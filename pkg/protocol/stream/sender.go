package stream

import (
	"net"
	"time"

	"github.com/pianoyeg94/multiplexed_udp/pkg/protocol/frame"
	"go.uber.org/zap"
)

type ackInfo struct {
	seqNum       uint16
	windowUpdate uint16
	isSack       bool
}

type windowUpdateInfo struct {
	seqNum       uint16
	windowUpdate uint16
}

type clientData struct {
	seqNum uint16
	data   []byte
}

type unackedData struct {
	dataSeqNum uint16
	partNum    uint16
	ts         int64
	data       []byte
	ackCount   int
	isSacked   bool
}

func newUnackedData(dataSeqNum uint16, partNum uint16, ts int64, data []byte) *unackedData {
	return &unackedData{
		dataSeqNum: dataSeqNum,
		partNum:    partNum,
		ts:         ts,
		data:       data,
	}
}

type sender struct {
	frameWriter     *frame.FrameWriter
	receivedAcksCh  chan ackInfo
	sendAcksCh      chan ackInfo
	windowUpdatesCh chan windowUpdateInfo
	clientDataCh    chan clientData

	// https://share.google/aimode/kVDIZK79mzZO4QmWZ
	oldestSendSequence          uint16
	nextSendSequence            uint16
	peerWindowSize              uint16
	lastWindowUpdateSequence    uint16
	lastWindowUpdateAckSequence uint16
	sendBuffer                  map[uint16]*unackedData
	rtoTimer                    *rtoTimer
}

func (s *Stream) sendDataFrame(data clientData) error {
	var n int
	var err error
	for partNum := uint16(1); len(data.data) != 0; partNum++ {
		dataSize := s.sender.frameWriter.CaclculateDataSize(data.data)
		if err = s.maybeWaitForPeerWindowUpdate(dataSize); err != nil {
			return err
		}
		if err = s.maybeSendAckFrame(); err != nil {
			return err
		}

		ts := time.Now().Unix()
		if n, err = s.sender.frameWriter.WriteData(
			s.id,
			s.sender.nextSendSequence,
			uint16(s.receiver.receiveWindowSize.Load()),
			false,
			data.seqNum,
			ts,
			partNum,
			data.data,
		); err != nil {
			return err
		}
		unackedData := newUnackedData(
			data.seqNum,
			partNum,
			ts,
			data.data[:n],
		)
		s.sender.sendBuffer[s.sender.nextSendSequence] = unackedData
		if !s.sender.rtoTimer.isSet() {
			if !s.sender.rtoTimer.reset(retransimssionBaseBackoff, s.sender.oldestSendSequence, s.closeCtx) {
				return net.ErrClosed
			}
		}

		s.logger.Debug("Sent data frame",
			zap.Uint16("stream_id", s.id),
			zap.Uint16("sequence_number", s.sender.nextSendSequence),
			zap.Uint16("data_sequence_number", data.seqNum),
			zap.Uint16("data_part_number", partNum),
			zap.Bool("has_data_end_flag", len(data.data) == 0),
		)

		s.sender.nextSendSequence++
		s.sender.peerWindowSize -= uint16(dataSize)
		data.data = data.data[n:]

		if err = s.maybeProcessAck(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Stream) sendAckFrame(ack ackInfo) error {
	if err := s.sender.frameWriter.WriteAck(s.id, ack.seqNum, ack.isSack, ack.windowUpdate); err != nil {
		return err
	}
	s.logger.Debug("Sent ack frame",
		zap.Uint16("stream_id", s.id),
		zap.Uint16("sequence_number", ack.seqNum),
		zap.Bool("is_sack", ack.isSack),
		zap.Uint16("window_update", ack.windowUpdate),
	)
	return nil
}

func (s *Stream) maybeSendAckFrame() error {
	select {
	case ack := <-s.sender.sendAcksCh:
		if err := s.sendAckFrame(ack); err != nil {
			return err
		}
	default:
	}
	return nil
}

func (s *Stream) maybeResendDataFrameRTO() error {
	select {
	case fired := <-s.sender.rtoTimer.fired():
		return s.resendDataFrameRTO(fired)
	case <-s.closeCtx.Done():
		return net.ErrClosed
	default:
	}
	return nil
}

func (s *Stream) resendDataFrameRTO(fired firedInfo) error {
	if fired.seqNum != s.sender.oldestSendSequence {
		if _, ok := s.sender.sendBuffer[s.sender.oldestSendSequence]; ok {
			if !s.sender.rtoTimer.reset(retransimssionBaseBackoff, s.sender.oldestSendSequence, s.closeCtx) {
				return net.ErrClosed
			}
		}
		return nil
	}

	if fired.retransmissionsLeft == 0 {
		return errLostFrame
	}

	return s.resendDataFrame(fired.seqNum, false)
}

func (s *Stream) resendDataFrame(seqNum uint16, isFastRetransmit bool) error {
	unackedData := s.sender.sendBuffer[seqNum]
	if unackedData.isSacked {
		return nil
	}

	dataSize := s.sender.frameWriter.CaclculateDataSize(unackedData.data)
	if err := s.maybeWaitForPeerWindowUpdate(dataSize); err != nil {
		return err
	}

	if !isFastRetransmit && seqNum < s.sender.oldestSendSequence {
		if _, ok := s.sender.sendBuffer[s.sender.oldestSendSequence]; ok {
			if !s.sender.rtoTimer.reset(retransimssionBaseBackoff, s.sender.oldestSendSequence, s.closeCtx) {
				return net.ErrClosed
			}
		}
		return nil
	}

	if _, err := s.sender.frameWriter.WriteData(
		s.id,
		seqNum,
		uint16(s.receiver.receiveWindowSize.Load()),
		false,
		unackedData.dataSeqNum,
		unackedData.ts,
		unackedData.partNum,
		unackedData.data,
	); err != nil {
		return err
	}
	return nil
}

func (s *Stream) maybeWaitForPeerWindowUpdate(sizeOfDataToBeSent int) error {
	for sizeOfDataToBeSent > int(s.sender.peerWindowSize) {
		s.logger.Debug("Waiting for peer window update", zap.Int("size_of_data_to_be_sent", sizeOfDataToBeSent))
		if err := s.maybeSendAckFrame(); err != nil {
			return err
		}
		select {
		case ack := <-s.sender.sendAcksCh:
			if err := s.sendAckFrame(ack); err != nil {
				return err
			}
		case ack := <-s.sender.receivedAcksCh:
			s.processAck(ack)
		case windowUpdate := <-s.sender.windowUpdatesCh:
			s.processSendWindowUpdate(windowUpdate)
		case <-s.closeCtx.Done():
			return net.ErrClosed
		}
	}
	return nil
}

func (s *Stream) maybeProcessAck() error {
	select {
	case ack := <-s.sender.sendAcksCh:
		if err := s.sendAckFrame(ack); err != nil {
			return err
		}
	case ack := <-s.sender.receivedAcksCh:
		s.processAck(ack)
	case windowUpdate := <-s.sender.windowUpdatesCh:
		s.processSendWindowUpdate(windowUpdate)
	case <-s.closeCtx.Done():
		return net.ErrClosed
	default:
	}
	return nil
}

func (s *Stream) processSendWindowUpdate(windowUpdate windowUpdateInfo) {
	if windowUpdate.seqNum > s.sender.lastWindowUpdateSequence &&
		windowUpdate.seqNum > s.sender.lastWindowUpdateAckSequence {
		s.sender.peerWindowSize = windowUpdate.windowUpdate
	}
}

func (s *Stream) processAck(ack ackInfo) error {
	if ack.seqNum > s.sender.lastWindowUpdateAckSequence &&
		ack.seqNum > s.sender.lastWindowUpdateSequence {
		s.sender.peerWindowSize = ack.windowUpdate
	}

	if ack.isSack && ack.seqNum >= s.sender.oldestSendSequence {
		if unackedData, ok := s.sender.sendBuffer[ack.seqNum]; ok {
			unackedData.isSacked = true
		}
		return nil
	}

	// TODO: handle if not in buffer - lost packet - is it a Connection error?
	if unackedData, ok := s.sender.sendBuffer[ack.seqNum]; ok {
		unackedData.ackCount++
		if unackedData.ackCount == maxRetransmissions {
			return s.resendDataFrame(ack.seqNum, true)
		} else if s.sender.sendBuffer[ack.seqNum].ackCount > maxRetransmissions {
			return errLostFrame
		}
	}

	if ack.seqNum > s.sender.oldestSendSequence {
		for i := s.sender.oldestSendSequence; i < ack.seqNum; i++ {
			delete(s.sender.sendBuffer, i)
		}
		s.sender.oldestSendSequence = ack.seqNum + 1
		if _, ok := s.sender.sendBuffer[s.sender.oldestSendSequence]; ok {
			if !s.sender.rtoTimer.reset(retransimssionBaseBackoff, s.sender.oldestSendSequence, s.closeCtx) {
				return net.ErrClosed
			}
		}
	}

	return nil
}
