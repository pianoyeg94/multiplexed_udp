package reader

import (
	"io"
	"net"
	"time"
)

type ReaderWithDeadline interface {
	io.Reader
	SetDeadline(time.Time) error
}

type ReaderWithDeadlineWithRemoteAddr interface {
	ReaderWithDeadline
	RemoteAddr() *net.UDPAddr
}

type UDPReader struct {
	remoteAddr *net.UDPAddr
	conn       *net.UDPConn
}

func NewUDPReader(conn *net.UDPConn) *UDPReader {
	return &UDPReader{
		conn: conn,
	}
}

func (r *UDPReader) Read(p []byte) (n int, err error) {
	if r.remoteAddr == nil {
		n, r.remoteAddr, err = r.conn.ReadFromUDP(p)
	} else {
		n, err = r.conn.Read(p)
	}
	return n, err
}

func (r *UDPReader) SetDeadline(t time.Time) error {
	return r.conn.SetDeadline(t)
}

func (r *UDPReader) RemoteAddr() *net.UDPAddr {
	return r.remoteAddr
}
