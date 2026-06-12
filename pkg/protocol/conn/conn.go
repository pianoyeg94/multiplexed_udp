package conn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/pianoyeg94/multiplexed_udp/pkg/protocol/frame"
	"github.com/pianoyeg94/multiplexed_udp/pkg/protocol/reader"
	"github.com/pianoyeg94/multiplexed_udp/pkg/protocol/stream"
	"github.com/pianoyeg94/multiplexed_udp/pkg/protocol/writer"
	"github.com/pianoyeg94/multiplexed_udp/pkg/timer"
	"go.uber.org/zap"
)

const (
	defaultReadTimeout                = 3 * time.Second
	infiniteReadTimeout time.Duration = 1<<63 - 1

	pingInterval             = 15 * time.Second
	maxPingCountWithoutReply = 3
)

type connectionState uint8

const (
	stateIsServing         connectionState = 0x1
	statePeerSettingsAcked connectionState = 0x2
	stateSettingsReceieved connectionState = 0x4
)

func (s connectionState) isServing() bool {
	return (s & stateIsServing) == stateIsServing
}

func (s connectionState) peerSettingsAcked() bool {
	return (s & statePeerSettingsAcked) == statePeerSettingsAcked
}

func (s connectionState) settingsReceived() bool {
	return (s & stateSettingsReceieved) == stateSettingsReceieved
}

type (
	Handler       = stream.Handler
	HandleFunc    = stream.HandleFunc
	ServerMessage = stream.ServerMessage
)

type clientData struct {
	streamID       uint16
	sequenceNumber uint16
	data           []byte
}

type Connection struct {
	w writer.WriterWithSetRemoteAddr
	r reader.ReaderWithDeadlineWithRemoteAddr

	frameWriter   *frame.FrameWriter
	frameReader   *frame.FrameReader
	frameParser   *frame.FrameParser
	dataFramePool *frame.DataFramePool

	windowSize     uint16
	peerWindowSize uint16
	state          connectionState

	streamsSendCh chan []byte
	streams       map[uint16]*stream.Stream
	nextStreamID  uint16

	pingTicker          *time.Ticker
	sendPingAckSingal   chan struct{}
	frameReceivedSignal chan struct{}

	clientDataCh  chan clientData
	errsCh        chan error
	serverHandler stream.Handler

	logger *zap.Logger

	closer   io.Closer
	closeCtx context.Context
	close    context.CancelCauseFunc
}

func NewConnection(
	w writer.WriterWithSetRemoteAddr,
	r reader.ReaderWithDeadlineWithRemoteAddr,
	c io.Closer,
	windowSize uint16,
	maxStreamCount uint16,
	logger *zap.Logger,
) *Connection {
	dataFramePool := frame.NewDataFramePool()
	closeCtx, close := context.WithCancelCause(context.Background())
	return &Connection{
		w: w,
		r: r,

		frameWriter:   frame.NewFrameWriter(w, nil, context.Background()),
		frameReader:   frame.NewFrameReader(r, dataFramePool),
		frameParser:   frame.NewFrameParser(dataFramePool),
		dataFramePool: dataFramePool,

		windowSize: windowSize,

		streamsSendCh: make(chan []byte, maxStreamCount*1000),
		streams:       make(map[uint16]*stream.Stream, maxStreamCount),
		nextStreamID:  1,

		pingTicker:          time.NewTicker(timer.MaxDuration),
		sendPingAckSingal:   make(chan struct{}, 1),
		frameReceivedSignal: make(chan struct{}, 1000),

		clientDataCh: make(chan clientData, maxStreamCount),
		errsCh:       make(chan error, 2),

		logger: logger,

		closer:   c,
		closeCtx: closeCtx,
		close:    close,
	}
}

func (c *Connection) Connect() error {
	initialReadTimeout := 3 * time.Second
	readTimeout := initialReadTimeout
	maxRetransmissions := 3
	backoff := func() {
		if readTimeout == initialReadTimeout {
			readTimeout = 3 * time.Second
		} else {
			readTimeout *= 2
		}
	}
	for retransmissionsLeft := maxRetransmissions + 1; retransmissionsLeft > 0; retransmissionsLeft-- {
		if err := c.frameWriter.WriteSettings(c.windowSize); err != nil {
			return err
		}

		frm, err := c.frameReader.ReadFrame(defaultReadTimeout)
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				backoff()
				continue
			}
			return err
		}
		if frm.Type != frame.FrameSettings {
			return frame.ErrConnection
		}

		settings, err := c.frameParser.ParseSettingsFrame(frm)
		if err != nil {
			return err
		}
		if !settings.IsAck() {
			return frame.ErrConnection
		}

		c.peerWindowSize = settings.WindowSize
		c.state |= stateSettingsReceieved

		if err = c.frameWriter.WriteSettingsAck(0); err != nil {
			return err
		}
		c.startReceiver(retransmissionsLeft, nil)
		c.startSender()
		c.startHandlingClientData()
		fmt.Println("Creating reader and writer")
		return nil
	}
	return frame.ErrConnection
}

func (c *Connection) Write(streamID uint16, sequenceNumber uint16, data []byte) error {
	select {
	case err, ok := <-c.errsCh:
		if ok {
			return err
		}
		return net.ErrClosed
	default:
	}
	select {
	case c.clientDataCh <- clientData{streamID: streamID, sequenceNumber: sequenceNumber, data: data}:
	case <-c.closeCtx.Done():
		return c.closeCtx.Err()
	}
	return nil
}

func (c *Connection) ListenAndServe(handler Handler) error {
	c.state |= stateIsServing
	c.serverHandler = handler

	var attemp int
	readTimeout := infiniteReadTimeout
	maxRetransmissions := 3
	backoff := func() {
		if readTimeout == infiniteReadTimeout {
			readTimeout = 1 * time.Second
		} else {
			readTimeout *= 2
		}
	}
	for attemp = range 1 + maxRetransmissions {
		frm, err := c.frameReader.ReadFrame(readTimeout)
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				backoff()
				continue
			}
			return err
		}

		if frm.Type != frame.FrameSettings {
			if !c.state.settingsReceived() {
				return frame.ErrConnection
			}
			c.state |= statePeerSettingsAcked
			fmt.Println("Creating reader and writer and stream")
			c.startReceiver(-1, frm)
			c.startSender()
			break
		}

		settings, err := c.frameParser.ParseSettingsFrame(frm)
		if err != nil {
			return err
		}
		if !c.state.settingsReceived() && settings.IsAck() {
			return frame.ErrConnection
		}

		if !c.state.settingsReceived() {
			c.peerWindowSize = settings.WindowSize
			c.state |= stateSettingsReceieved
		} else if settings.IsAck() {
			c.state |= statePeerSettingsAcked
			fmt.Println("Creating reader and writer")
			c.startReceiver(-1, nil)
			c.startSender()
			break
		}

		if attemp == 0 {
			c.w.SetRemoteAddr(c.r.RemoteAddr())
		}
		if err := c.frameWriter.WriteSettingsAck(c.windowSize); err != nil {
			return err
		}
		backoff()
	}
	if attemp == 1+maxRetransmissions-1 {
		return frame.ErrConnection
	}

	err := <-c.errsCh
	return err
}

func (c *Connection) Close() error {
	return c.closer.Close()
}

func (c *Connection) startSender() {
	go func() {
		var pingCountWithoutReply int
		c.pingTicker.Reset(15 * time.Second)
		for {
			select {
			case <-c.frameReceivedSignal:
				c.pingTicker.Reset(pingInterval)
				pingCountWithoutReply = 0
			default:
			}
			select {
			case frm := <-c.streamsSendCh:
				c.logger.Debug("Got frame to write", zap.Int("frame_length", len(frm)))
				if n, err := c.w.Write(frm); err != nil {
					fmt.Println("Got error from connection sender Write data", err)
					c.errsCh <- err
					return
				} else if n < len(frm) {
					c.logger.Debug("Got short write")
					// handle short write
				}
				c.logger.Debug("Sent frame to connection", zap.Int("frame_length", len(frm)))
			default:
			}
			select {
			case <-c.pingTicker.C:
				if pingCountWithoutReply == maxPingCountWithoutReply {
					c.errsCh <- frame.ErrConnection
					return
				}
				if err := c.frameWriter.WritePing(); err != nil {
					c.errsCh <- err
					return
				}
				pingCountWithoutReply++
				select {
				case <-c.frameReceivedSignal:
					c.pingTicker.Reset(pingInterval)
					pingCountWithoutReply = 0
				default:
				}
				select {
				case frm := <-c.streamsSendCh:
					c.logger.Debug("Got frame to write", zap.Int("frame_length", len(frm)))
					if n, err := c.w.Write(frm); err != nil {
						fmt.Println("Got error from connection sender Write data", err)
						c.errsCh <- err
						return
					} else if n < len(frm) {
						c.logger.Debug("Got short write")
						// handle short write
					}
					c.logger.Debug("Sent frame to connection", zap.Int("frame_length", len(frm)))
				default:
				}
			case <-c.frameReceivedSignal:
				c.pingTicker.Reset(pingInterval)
				pingCountWithoutReply = 0
				select {
				case <-c.frameReceivedSignal:
					c.pingTicker.Reset(pingInterval)
					pingCountWithoutReply = 0
				default:
				}
				select {
				case frm := <-c.streamsSendCh:
					c.logger.Debug("Got frame to write", zap.Int("frame_length", len(frm)))
					if n, err := c.w.Write(frm); err != nil {
						fmt.Println("Got error from connection sender Write data", err)
						c.errsCh <- err
						return
					} else if n < len(frm) {
						c.logger.Debug("Got short write")
						// handle short write
					}
					c.logger.Debug("Sent frame to connection", zap.Int("frame_length", len(frm)))
				default:
				}
			case <-c.sendPingAckSingal:
				if err := c.frameWriter.WritePingAck(); err != nil {
					c.errsCh <- err
					return
				}
				select {
				case <-c.frameReceivedSignal:
					c.pingTicker.Reset(pingInterval)
					pingCountWithoutReply = 0
				default:
				}
				select {
				case frm := <-c.streamsSendCh:
					c.logger.Debug("Got frame to write", zap.Int("frame_length", len(frm)))
					if n, err := c.w.Write(frm); err != nil {
						fmt.Println("Got error from connection sender Write data", err)
						c.errsCh <- err
						return
					} else if n < len(frm) {
						c.logger.Debug("Got short write")
						// handle short write
					}
					c.logger.Debug("Sent frame to connection", zap.Int("frame_length", len(frm)))
				default:
				}
			case <-c.closeCtx.Done():
				c.errsCh <- c.closeCtx.Err()
				return
			}
		}
	}()
}

func (c *Connection) startReceiver(settingsRetransmissionsLeft int, frm *frame.Frame) {
	go func() {
		_ = settingsRetransmissionsLeft
		if frm != nil {
			if err := c.handleReceivedFrame(frm); err != nil {
				fmt.Println("Got error from connection receiver handleReceivedFrame", err)
				c.errsCh <- err
				return
			}
		}
		for {
			frm, err := c.frameReader.ReadFrame(timer.MaxDuration)
			if err != nil {
				fmt.Println("Got error from connection receiver ReadFrame", err)
				c.errsCh <- err
				return
			}
			c.logger.Debug("Read frame from connection", zap.Uint8("frame_type", uint8(frm.Type)), zap.Uint16("frame_length", frm.Length))

			select {
			case c.frameReceivedSignal <- struct{}{}:
			case <-c.closeCtx.Done():
				c.errsCh <- c.closeCtx.Err()
				return
			}

			if err := c.handleReceivedFrame(frm); err != nil {
				fmt.Println("Got error from connection receiver handleReceivedFrame", err)
				c.errsCh <- err
				return
			}
		}
	}()
}

func (c *Connection) startHandlingClientData() {
	go func() {
		for {
			select {
			case clientData := <-c.clientDataCh:
				if err := c.handleClientData(clientData); err != nil {
					fmt.Println("Got error from connection sender handleClientData", err)
					c.errsCh <- err
					return
				}
				c.logger.Debug("Got client data")
			case <-c.closeCtx.Done():
				return
			}
		}
	}()
}

func (c *Connection) handleReceivedFrame(frm *frame.Frame) error {
	switch frm.Type {
	case frame.FramePing:
		return c.handlePingFrame(frm)
	case frame.FrameGoaway:
		// TODO:
	default:
		return c.handleStreamFrame(frm)
	}
	return nil
}

func (c *Connection) handlePingFrame(frm *frame.Frame) error {
	if frm.Flags.Has(frame.FlagPingAck) {
		return nil
	}
	select {
	case c.sendPingAckSingal <- struct{}{}:
	case <-c.closeCtx.Done():
		return c.closeCtx.Err()
	}
	return nil
}

func (c *Connection) handleStreamFrame(frm *frame.Frame) error {
	if stream, ok := c.streams[frm.StreamID]; ok {
		return stream.Receieve(frm)
	}
	stream := stream.NewStream(
		frm.StreamID,
		c.windowSize,
		c.peerWindowSize,
		frame.NewFrameWriter(nil, c.streamsSendCh, c.closeCtx),
		c.dataFramePool,
		c.serverHandler,
		c.logger,
		c.closeCtx,
	)
	c.streams[frm.StreamID] = stream
	return stream.Receieve(frm)
}

func (c *Connection) handleClientData(clientData clientData) error {
	if stream, ok := c.streams[clientData.streamID]; ok {
		return stream.Send(clientData.sequenceNumber, clientData.data)
	}
	stream := stream.NewStream(
		clientData.streamID,
		c.windowSize,
		c.peerWindowSize,
		frame.NewFrameWriter(nil, c.streamsSendCh, c.closeCtx),
		c.dataFramePool,
		c.serverHandler,
		c.logger,
		c.closeCtx,
	)
	c.streams[clientData.streamID] = stream
	return stream.Send(clientData.sequenceNumber, clientData.data)
}
