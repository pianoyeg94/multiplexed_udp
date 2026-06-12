package stream

import (
	"net"
	"sync/atomic"

	"github.com/pianoyeg94/multiplexed_udp/pkg/protocol/frame"
	"go.uber.org/zap"
)

type receiver struct {
	frameParser    *frame.FrameParser
	dataFramePool  *frame.DataFramePool
	receiveFrameCh chan *frame.Frame

	nextExpectedSequence            uint16
	dupAckCountNextExpectedSequence uint16
	receiveWindowSize               atomic.Int32 // realy a uint16
	receiveWindow                   map[uint16]struct{}
	reassembleBuffer                reassembleBuffer

	lostDataSequences map[uint16]struct{} // clear somehow periodiaclly

	applicationLayerHandler Handler
}

func (s *Stream) processFrame(frm *frame.Frame) error {
	switch frm.Type {
	case frame.FrameData:
		return s.processDataFrame(frm)
	case frame.FrameAck:
		return s.processAckFrame(frm)
	default:
		return frame.ErrConnection
	}
}

// https://share.google/aimode/zOntMrtzoQE9vqm2G
// https://share.google/aimode/98nsaRZ9DOWvBYxy5
func (s *Stream) processDataFrame(frm *frame.Frame) (err error) {
	// TODO: If end stream flag is set
	defer func() {
		if err != nil {
			s.receiver.dataFramePool.PutFrame(frm.Frame)
		}
	}()

	var loggerDebugFields []zap.Field
	if s.logger.Level() == zap.DebugLevel {
		loggerDebugFields = append(loggerDebugFields,
			zap.Uint16("next_expected_sequence", s.receiver.nextExpectedSequence),
			zap.Uint16("stream_id", s.id),
			zap.Uint16("sequence_number", frm.SequenceNumber),
			zap.Uint16("peer_window_size", frm.WindowSize),
			zap.Uint16("length_of_frame", frm.Length),
			zap.Bool("has_flag_data_end", frm.Flags.Has(frame.FlagDataEnd)),
		)
	}
	s.logger.Debug("Starting to process data frame", loggerDebugFields...)

	if frm.SequenceNumber < s.receiver.nextExpectedSequence {
		s.logger.Debug("Received old duplicate data frame", loggerDebugFields...)
		s.receiver.dataFramePool.PutFrame(frm.Frame)
		return nil
	}
	if uint16(s.receiver.receiveWindowSize.Load()) < frm.Length && !(frm.Flags.Has(frame.FlagDataEnd) && frm.SequenceNumber == s.receiver.nextExpectedSequence) {
		s.logger.Debug("No more place in receive window", loggerDebugFields...)
		s.maybeFreeReassembleBufferFromLostData()
		if uint16(s.receiver.receiveWindowSize.Load()) < frm.Length {
			if err = s.sendAck(s.receiver.nextExpectedSequence, uint16(s.receiver.receiveWindowSize.Load())); err != nil {
				return err
			}
			err = s.signalPeerWindowSizeUpdate(frm.SequenceNumber, frm.WindowSize)
			s.receiver.dataFramePool.PutFrame(frm.Frame)
			return err
		}
	}
	if _, ok := s.receiver.receiveWindow[frm.SequenceNumber]; ok {
		s.logger.Debug("Receieved duplicate data within receive window", loggerDebugFields...)
		if err = s.sendAck(s.receiver.nextExpectedSequence, uint16(s.receiver.receiveWindowSize.Load())); err != nil {
			return err
		}
		err = s.signalPeerWindowSizeUpdate(frm.SequenceNumber, frm.WindowSize)
		s.receiver.dataFramePool.PutFrame(frm.Frame)
		return err
	}

	if err = s.signalPeerWindowSizeUpdate(frm.SequenceNumber, frm.WindowSize); err != nil {
		return err
	}

	// Only now parse the data frame
	var data *frame.DataFrame
	if data, err = s.receiver.frameParser.ParseDataFrame(frm); err != nil {
		return err
	}

	if s.logger.Level() == zap.DebugLevel {
		loggerDebugFields = append(loggerDebugFields,
			zap.Uint16("data_sequence_number", data.DataSequenceNumber),
			zap.Uint16("data_part_number", data.PartNumber),
			zap.Uint64("data_ts", data.Timestamp),
			zap.Int("length_of_data", len(data.Data)),
			zap.Int("length_of_checksum", len(data.Checksum)),
		)
	}

	if data.SequenceNumber == s.receiver.nextExpectedSequence {
		s.logger.Debug("Receieved next expected sequence data", loggerDebugFields...)
		if err = s.sendAck(s.receiver.nextExpectedSequence, uint16(s.receiver.receiveWindowSize.Load())); err != nil {
			return err
		}
		if data.Flags.Has(frame.FlagDataEnd) {
			if data.PartNumber == 1 {
				s.logger.Debug("Data has only one part, passing to application layer", loggerDebugFields...)
				s.passDataToApplicationLayer(data, true, false)
				s.receiver.dataFramePool.PutFrame(data.PooledFrame)
			} else {
				if hasReassembled, hasLostDataFrame := s.tryReassembleData(data); hasReassembled {
					s.logger.Debug("Reassembled data, passing to application layer", loggerDebugFields...)
					s.passDataToApplicationLayer(data, false, false)
				} else if hasLostDataFrame {
					s.logger.Debug("Data lost a frame, passing to application layer", loggerDebugFields...)
					s.passDataToApplicationLayer(data, false, true)
				} else {
					if uint16(s.receiver.receiveWindowSize.Load()) >= frm.Length {
						s.maybeFreeReassembleBufferFromLostData()
						if uint16(s.receiver.receiveWindowSize.Load()) >= frm.Length {
							s.logger.Debug("Not enough frames to reassemble data", loggerDebugFields...)
							s.receiver.reassembleBuffer.Set(data.SequenceNumber, data)
							s.receiver.receiveWindow[data.SequenceNumber] = struct{}{}
							s.receiver.receiveWindowSize.Add(-int32(frm.Length))
						}
					} else {
						s.logger.Debug("Not enough frames to reassemble data, low window size, dropping data frame", loggerDebugFields...)
						return nil
					}
				}
			}
		} else {
			s.logger.Debug("Data frame is not the last frame in the whole payload", loggerDebugFields...)
			s.receiver.reassembleBuffer.Set(data.SequenceNumber, data)
			s.receiver.receiveWindow[data.SequenceNumber] = struct{}{}
			s.receiver.receiveWindowSize.Add(-int32(frm.Length))
		}
		s.receiver.nextExpectedSequence++
		s.receiver.dupAckCountNextExpectedSequence = 0
	} else {
		if err = s.sendSack(data.SequenceNumber, uint16(s.receiver.receiveWindowSize.Load())); err != nil {
			return err
		}

		s.logger.Debug("Data frame is not the next expected in sequence", loggerDebugFields...)
		s.receiver.reassembleBuffer.Set(data.SequenceNumber, data)
		s.receiver.receiveWindow[data.SequenceNumber] = struct{}{}
		s.receiver.receiveWindowSize.Add(-int32(frm.Length))

		if s.receiver.dupAckCountNextExpectedSequence == maxDupAcksToSend {
			if dataChunk, ok := s.receiver.reassembleBuffer.Get(s.receiver.nextExpectedSequence); ok {
				s.receiver.lostDataSequences[dataChunk.DataSequenceNumber] = struct{}{}
				s.receiver.reassembleBuffer.Del(s.receiver.nextExpectedSequence)
			}
			s.receiver.nextExpectedSequence++
			s.receiver.dupAckCountNextExpectedSequence = 0
		} else {
			err = s.sendAck(s.receiver.nextExpectedSequence, uint16(s.receiver.receiveWindowSize.Load()))
			s.receiver.dupAckCountNextExpectedSequence++
		}
	}
	return err
}

func (s *Stream) processAckFrame(frm *frame.Frame) error {
	ack, err := s.receiver.frameParser.ParseAckFrame(frm)
	if err != nil {
		return err
	}

	s.logger.Debug("Processed ack frame",
		zap.Uint16("next_expected_sequence", s.receiver.nextExpectedSequence),
		zap.Uint16("stream_id", s.id),
		zap.Uint16("sequence_number", frm.SequenceNumber),
		zap.Uint16("peer_window_size", frm.WindowSize),
	)

	return s.signalAckReceived(ack.SequenceNumber, ack.WindowSize, ack.IsSack())
}

func (s *Stream) tryReassembleData(data *frame.DataFrame) (hasReassembled bool, hasLostDataFrame bool) {
	nextExpectedPartNum := uint16(1)
	var reassembleBuf []*frame.DataFrame
	var otherLostData []*frame.DataFrame
	for it := s.receiver.reassembleBuffer.Iterator(); it.Valid(); it.Next() {
		dt := it.Value()
		if _, ok := s.receiver.lostDataSequences[dt.DataSequenceNumber]; ok {
			if dt.DataSequenceNumber == data.DataSequenceNumber {
				hasLostDataFrame = true
				reassembleBuf = append(reassembleBuf, dt)
			} else {
				otherLostData = append(otherLostData, dt)
			}
		} else if dt.DataSequenceNumber == data.DataSequenceNumber && dt.PartNumber == nextExpectedPartNum {
			reassembleBuf = append(reassembleBuf, dt)
			nextExpectedPartNum++
			if nextExpectedPartNum == data.PartNumber {
				break
			}
		} else {
			break
		}
	}
	for _, dt := range otherLostData {
		dt.Data = nil
		dt.Checksum = nil
		dt.Timestamp = 0
		s.passDataToApplicationLayer(dt, false, true)
		s.receiver.dataFramePool.PutFrame(dt.PooledFrame)
	}
	if hasLostDataFrame {
		reassembleBuf = append(reassembleBuf, data)
		for _, dt := range reassembleBuf {
			dt.Data = nil
			data.Checksum = nil
			data.Timestamp = 0
			s.receiver.dataFramePool.PutFrame(dt.PooledFrame)
		}
		return false, true
	}
	if nextExpectedPartNum != data.PartNumber {
		return false, false
	}

	var reassembledDataLength int
	for _, dt := range reassembleBuf {
		s.receiver.receiveWindowSize.Add(int32(dt.Length))
		delete(s.receiver.receiveWindow, dt.SequenceNumber)
		s.receiver.reassembleBuffer.Del(dt.SequenceNumber)
		reassembledDataLength += len(dt.Data)

	}
	reassembledDataLength += len(data.Data)

	reassembledChecksum := make([]byte, len(reassembleBuf[0].Checksum))
	copy(reassembledChecksum, reassembleBuf[0].Checksum)

	reassembledData := make([]byte, 0, reassembledDataLength)
	for _, dt := range reassembleBuf {
		reassembledData = append(reassembledData, dt.Data...)
		s.receiver.dataFramePool.PutFrame(dt.PooledFrame)
	}
	reassembledData = append(reassembledData, data.Data...)
	s.receiver.dataFramePool.PutFrame(data.PooledFrame)

	data.Checksum = reassembledChecksum
	data.Data = reassembledData

	return true, false
}

func (s *Stream) maybeFreeReassembleBufferFromLostData() {
	if len(s.receiver.lostDataSequences) == 0 {
		return
	}
	var lostData []*frame.DataFrame
	for it := s.receiver.reassembleBuffer.Iterator(); it.Valid(); it.Next() {
		dt := it.Value()
		if _, ok := s.receiver.lostDataSequences[dt.DataSequenceNumber]; ok {
			lostData = append(lostData, dt)
		}
	}
	for _, dt := range lostData {
		s.receiver.reassembleBuffer.Del(dt.SequenceNumber)
		s.receiver.receiveWindowSize.Add(int32(dt.Length))
		s.receiver.dataFramePool.PutFrame(dt.PooledFrame)
	}
}

func (s *Stream) passDataToApplicationLayer(data *frame.DataFrame, requiresCopy bool, hasLostDataChunk bool) {
	appData := data.Data
	appChecksum := data.Checksum
	if requiresCopy {
		appData = make([]byte, len(data.Data))
		appChecksum = make([]byte, len(data.Checksum))
		copy(appData, data.Data)
		copy(appChecksum, data.Checksum)
	}
	s.receiver.applicationLayerHandler.Handle(&ServerMessage{
		SequenceNumber:   data.DataSequenceNumber,
		Timestamp:        int64(data.Timestamp),
		Data:             data.Data,
		Checksum:         data.Checksum,
		HasLostDataChunk: hasLostDataChunk,
	})
}

func (s *Stream) sendAck(seqNum uint16, windowUpdate uint16) error {
	select {
	case s.sender.sendAcksCh <- ackInfo{seqNum, windowUpdate, false}:
		return nil
	case <-s.closeCtx.Done():
		return net.ErrClosed
	}
}

func (s *Stream) sendSack(seqNum uint16, windowUpdate uint16) error {
	select {
	case s.sender.sendAcksCh <- ackInfo{seqNum, windowUpdate, true}:
		return nil
	case <-s.closeCtx.Done():
		return net.ErrClosed
	}
}

func (s *Stream) signalAckReceived(seqNum uint16, windowUpdate uint16, isSack bool) error {
	select {
	case s.sender.receivedAcksCh <- ackInfo{seqNum, windowUpdate, isSack}:
		return nil
	case <-s.closeCtx.Done():
		return net.ErrClosed
	}
}

func (s *Stream) signalPeerWindowSizeUpdate(seqNum uint16, availableSpace uint16) error {
	select {
	case s.sender.windowUpdatesCh <- windowUpdateInfo{seqNum, availableSpace}:
		return nil
	case <-s.closeCtx.Done():
		return net.ErrClosed
	}
}
