package stream

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pianoyeg94/multiplexed_udp/pkg/ctxt"
	"github.com/pianoyeg94/multiplexed_udp/pkg/protocol/frame"
	"go.uber.org/zap"
)

const (
	maxRetransmissions        = 4
	retransimssionBaseBackoff = 1 * time.Second

	maxDupAcksToSend = 4
)

var (
	errLostFrame = errors.New("protocol: lost frame")

	ErrStreamClosed = errors.New("protocol: stream is closed")
)

type (
	Handler interface {
		Handle(msg *ServerMessage)
	}

	HandleFunc func(msg *ServerMessage)
)

func (f HandleFunc) Handle(msg *ServerMessage) { f(msg) }

var _ Handler = HandleFunc(nil)

type ServerMessage struct {
	SequenceNumber   uint16
	Timestamp        int64
	Data             []byte
	Checksum         []byte
	HasLostDataChunk bool
}

type Stream struct {
	id       uint16
	receiver receiver
	sender   sender

	logger *zap.Logger

	closeCtx context.Context
	close    context.CancelFunc
	closeWg  sync.WaitGroup
}

func NewStream(
	id uint16,
	receiveWindowSize uint16,
	peerWindowSize uint16,
	frameWriter *frame.FrameWriter,
	dataFramePool *frame.DataFramePool,
	serverHandler Handler,
	logger *zap.Logger,
	ctx context.Context,
) *Stream {
	var clientDataCh chan clientData
	if serverHandler == nil {
		clientDataCh = make(chan clientData)
	}

	closeCtx, close := context.WithCancel(ctx)
	stream := Stream{
		id: id,
		receiver: receiver{
			frameParser:    frame.NewFrameParser(dataFramePool),
			dataFramePool:  dataFramePool,
			receiveFrameCh: make(chan *frame.Frame),

			receiveWindowSize: atomic.Int32{},
			receiveWindow:     make(map[uint16]struct{}),
			reassembleBuffer:  newReassembleBuffer(),

			lostDataSequences: make(map[uint16]struct{}),

			applicationLayerHandler: serverHandler,
		},
		sender: sender{
			frameWriter:     frameWriter,
			receivedAcksCh:  make(chan ackInfo, 100),
			sendAcksCh:      make(chan ackInfo),
			windowUpdatesCh: make(chan windowUpdateInfo),
			clientDataCh:    clientDataCh,

			peerWindowSize: peerWindowSize,
			sendBuffer:     make(map[uint16]*unackedData),
			rtoTimer:       newRtoTimer(maxRetransmissions),
		},

		logger: logger,

		closeCtx: closeCtx,
		close:    close,
	}
	stream.receiver.receiveWindowSize.Add(int32(receiveWindowSize))

	stream.startReceiving()
	stream.startSending()

	return &stream
}

func (s *Stream) Receieve(frm *frame.Frame) error {
	if ctxt.ContextDone(s.closeCtx) {
		return ErrStreamClosed
	}
	select {
	case s.receiver.receiveFrameCh <- frm:
		return nil
	case <-s.closeCtx.Done():
		return ErrStreamClosed
	}
}

func (s *Stream) Send(seqNum uint16, data []byte) error {
	if ctxt.ContextDone(s.closeCtx) {
		return ErrStreamClosed
	}
	select {
	case s.sender.clientDataCh <- clientData{seqNum, data}:
		return nil
	case <-s.closeCtx.Done():
		return ErrStreamClosed
	}
}

func (s *Stream) Close() {
	s.close()
	s.closeWg.Wait()

	s.sender.rtoTimer.stop()
	close(s.receiver.receiveFrameCh)
	close(s.sender.receivedAcksCh)
	close(s.sender.sendAcksCh)
	close(s.sender.windowUpdatesCh)
	if s.sender.clientDataCh != nil {
		close(s.sender.clientDataCh)
	}
}

func (s *Stream) startReceiving() {
	s.closeWg.Go(func() {
		for {
			select {
			case frm := <-s.receiver.receiveFrameCh:
				if err := s.processFrame(frm); err != nil {
					if err == frame.ErrConnection || err == net.ErrClosed {
						return
					}
					// TODO: handle other errors
				}
			case <-s.closeCtx.Done():
				return
			}
		}
	})
}

func (s *Stream) startSending() {
	s.closeWg.Go(func() {
		for {
			if err := s.maybeProcessAck(); err != nil {
				return
			}
			if err := s.maybeSendAckFrame(); err != nil {
				if err == net.ErrClosed {
					return
				}
				// handle other errors
			}
			if err := s.maybeResendDataFrameRTO(); err != nil {
				if err == net.ErrClosed {
					return
				}
				// TODO: handle other errors
			}
			select {
			case ack := <-s.sender.receivedAcksCh:
				if err := s.processAck(ack); err != nil {
					if err == net.ErrClosed {
						return
					}
				}
			case ack := <-s.sender.sendAcksCh:
				if err := s.sendAckFrame(ack); err != nil {
					if err == net.ErrClosed {
						return
					}
					// TODO: handle other errors
				}
			case windowUpdate := <-s.sender.windowUpdatesCh:
				s.processSendWindowUpdate(windowUpdate)
			case data := <-s.sender.clientDataCh:
				if err := s.sendDataFrame(data); err != nil {
					if err == net.ErrClosed {
						return
					}
					fmt.Println("GOT ERROR FROM SEND DATA FRAME", err)
				}
			case fired := <-s.sender.rtoTimer.fired():
				if err := s.resendDataFrameRTO(fired); err != nil {
					if err == net.ErrClosed {
						return
					}
					// TODO: handle other errors
				}
			case <-s.closeCtx.Done():
				return
			}
		}
	})
}
