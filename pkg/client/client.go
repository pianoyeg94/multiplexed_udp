package client

import (
	"fmt"
	"net"

	"github.com/pianoyeg94/multiplexed_udp/pkg/protocol/conn"
	"github.com/pianoyeg94/multiplexed_udp/pkg/protocol/reader"
	"github.com/pianoyeg94/multiplexed_udp/pkg/protocol/writer"
	"go.uber.org/zap"
)

const maxStreamCount = 10

type Client struct {
	remoteAddr string
	remotePort int
	windowSize uint16
	conn       *conn.Connection

	streamCycle *streamCycle

	logger *zap.Logger
}

func NewClient(remoteAddr string, remotePort int, windowSize uint16, logger *zap.Logger) *Client {
	return &Client{
		remoteAddr: remoteAddr,
		remotePort: remotePort,
		windowSize: windowSize,

		streamCycle: newStreamCycle(),

		logger: logger,
	}
}

func (c *Client) Connect() error {
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", c.remoteAddr, c.remotePort))
	if err != nil {
		return err
	}

	udpConn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return err
	}

	w := writer.NewClientWriter(udpConn)
	r := reader.NewUDPReader(udpConn)
	c.conn = conn.NewConnection(w, r, udpConn, c.windowSize, maxStreamCount, c.logger)

	return c.conn.Connect()
}

func (c *Client) Send(sequenceNumber int, data []byte) error {
	return c.conn.Write(c.streamCycle.nextStreamID(), uint16(sequenceNumber), data)
}

func (c *Client) Close() error {
	return c.conn.Close()
}
