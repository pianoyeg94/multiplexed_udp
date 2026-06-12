package server

import (
	"fmt"
	"net"

	"github.com/pianoyeg94/multiplexed_udp/pkg/protocol/conn"
	"github.com/pianoyeg94/multiplexed_udp/pkg/protocol/reader"
	"github.com/pianoyeg94/multiplexed_udp/pkg/protocol/writer"
	"go.uber.org/zap"
)

type (
	Handler       = conn.Handler
	HandleFunc    = conn.HandleFunc
	ServerMessage = conn.ServerMessage
)

type Server struct {
	port       int
	windowSize uint16
	conn       *conn.Connection

	logger *zap.Logger
}

func NewServer(port int, windowSize uint16, logger *zap.Logger) *Server {
	return &Server{
		port:       port,
		windowSize: windowSize,

		logger: logger,
	}
}

func (s *Server) ListenAndServe(handler Handler) error {
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return err
	}

	udpConn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}

	w := writer.NewServerWriter(udpConn)
	r := reader.NewUDPReader(udpConn)
	s.conn = conn.NewConnection(w, r, udpConn, s.windowSize, 10, s.logger)

	return s.conn.ListenAndServe(handler)
}

func (s *Server) Close() error {
	return s.conn.Close()
}
