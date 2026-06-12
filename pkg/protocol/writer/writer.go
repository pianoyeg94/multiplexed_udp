package writer

import (
	"io"
	"net"
)

type WriterWithSetRemoteAddr interface {
	io.Writer
	SetRemoteAddr(addr *net.UDPAddr)
}

type ServerWriter struct {
	remoteAddr *net.UDPAddr
	conn       *net.UDPConn
}

func NewServerWriter(conn *net.UDPConn) *ServerWriter {
	return &ServerWriter{
		conn: conn,
	}
}

func (sw *ServerWriter) Write(p []byte) (n int, err error) {
	return sw.conn.WriteToUDP(p, sw.remoteAddr)
}

func (sw *ServerWriter) SetRemoteAddr(addr *net.UDPAddr) {
	sw.remoteAddr = addr
}

type ClientWriter struct {
	conn *net.UDPConn
}

func NewClientWriter(conn *net.UDPConn) *ClientWriter {
	return &ClientWriter{
		conn: conn,
	}
}

func (cw *ClientWriter) Write(p []byte) (n int, err error) {
	return cw.conn.Write(p)
}

func (cw *ClientWriter) SetRemoteAddr(addr *net.UDPAddr) {}
