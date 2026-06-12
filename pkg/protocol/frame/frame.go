package frame

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
	"time"
	"unsafe"

	"github.com/pianoyeg94/multiplexed_udp/pkg/protocol/reader"
)

const (
	frameHeaderLen = int(unsafe.Sizeof(FrameHeader{}))
	maxFrameSize   = 1450 // https://share.google/aimode/tBojQ7vJvFQcCDgxg
)

var (
	uint16Size = int(unsafe.Sizeof(uint16(0)))
	uint64Size = int(unsafe.Sizeof(uint64(0)))
	sha256Size = int(unsafe.Sizeof([32]byte{}))
)

var (
	ErrFrameTooLarge = errors.New("protocol: frame to large")

	ErrWritingSettings   = errors.New("protocol: error writing settings")
	ErrWritingSettingAck = errors.New("protocol: error writing settings ack")

	ErrReadingFrameHeader = errors.New("protocol: error reading frame header")
	ErrReadingFrame       = errors.New("protocol: error reading frame")

	ErrConnection = errors.New("protocol: connection error")
)

type FrameType uint8

const (
	FrameSettings FrameType = 0x0
	FrameAck      FrameType = 0x1
	FrameData     FrameType = 0x2
	FramePing     FrameType = 0x3
	FrameGoaway   FrameType = 0x4
)

type Flags uint8

const (
	FlagSettingsAck Flags = 0x1

	FlagDataEndStream Flags = 0x1
	FlagDataEnd       Flags = 0x2

	FlagAckIsSack Flags = 0x1

	FlagPingAck Flags = 0x1
)

func (f Flags) Has(v Flags) bool {
	return (f & v) == v
}

type DataFramePool struct {
	pool sync.Pool
}

func NewDataFramePool() *DataFramePool {
	return &DataFramePool{
		pool: sync.Pool{
			New: func() any {
				return make([]byte, maxFrameSize)
			},
		},
	}
}

func (fp *DataFramePool) GetFrame(size uint16) []byte {
	return fp.pool.Get().([]byte)[:size]
}

func (fp *DataFramePool) PutFrame(frame []byte) {
	fp.pool.Put(frame)
}

type FrameHeader struct {
	Type           FrameType
	Flags          Flags
	Length         uint16
	StreamID       uint16
	SequenceNumber uint16
	WindowSize     uint16
}

type Frame struct {
	FrameHeader
	Frame []byte
}

type SettingsFrame struct {
	FrameHeader
}

func (sf *SettingsFrame) IsAck() bool {
	return sf.FrameHeader.Flags.Has(FlagSettingsAck)
}

type AckFrame struct {
	FrameHeader
}

func (af *AckFrame) IsSack() bool {
	return af.FrameHeader.Flags.Has(FlagAckIsSack)
}

type DataFrame struct {
	FrameHeader
	DataSequenceNumber uint16
	Timestamp          uint64
	PartNumber         uint16
	Data               []byte
	Checksum           []byte

	PooledFrame []byte
}

type PingFrame struct {
	FrameHeader
}

func (pf *PingFrame) IsAck() bool {
	return pf.FrameHeader.Flags.Has(FlagPingAck)
}

type GoawayFrame struct {
}

type FrameWriter struct {
	w         io.Writer
	writerCh  chan<- []byte
	closeCtx  context.Context
	writerBuf []byte
}

func NewFrameWriter(w io.Writer, writerCh chan<- []byte, closeCtx context.Context) *FrameWriter {
	return &FrameWriter{
		w:        w,
		writerCh: writerCh,
		closeCtx: closeCtx,
	}
}

func (fw *FrameWriter) WriteSettings(windowSize uint16) error {
	fw.startWrite(FrameSettings, 0, 0, 0, windowSize)
	return fw.endWrite()
}

func (fw *FrameWriter) WriteSettingsAck(windowSize uint16) error {
	fw.startWrite(FrameSettings, FlagSettingsAck, 0, 0, windowSize)
	return fw.endWrite()
}

func (fw *FrameWriter) WriteAck(streamID uint16, seqNum uint16, isSack bool, windowSize uint16) error {
	var flags Flags
	if isSack {
		flags = FlagAckIsSack
	}

	fw.startWrite(FrameAck, flags, streamID, seqNum, windowSize)
	return fw.endWrite()
}

func (fw *FrameWriter) CaclculateDataSize(data []byte) int {
	n := maxFrameSize - frameHeaderLen - uint16Size - uint64Size - uint16Size - sha256Size
	return n + uint16Size + uint64Size + uint16Size + sha256Size
}

func (fw *FrameWriter) WriteData(
	streamID uint16,
	frameSeqNum uint16,
	windowSize uint16,
	endStream bool,
	seqNum uint16,
	ts int64,
	partNum uint16,
	data []byte,
) (n int, err error) {
	var flags Flags
	if endStream {
		flags |= FlagDataEndStream
	}
	if frameHeaderLen+uint16Size+uint64Size+uint16Size+sha256Size+len(data) <= maxFrameSize {
		flags |= FlagDataEnd
	}
	fw.startWrite(FrameData, flags, streamID, frameSeqNum, windowSize)

	fw.writeUint16(seqNum)
	fw.writeUint64(uint64(ts))
	fw.writeUint16(partNum)

	var checksum [32]byte
	if partNum == 1 {
		checksum = sha256.Sum256(data) // https://share.google/aimode/IjpcnVkh3WLysYSV3
	}

	n = len(data)
	if !flags.Has(FlagDataEnd) {
		n = maxFrameSize - frameHeaderLen - uint16Size - uint64Size - uint16Size - sha256Size
		data = data[:n:n]
	}

	fw.writeBytes(checksum[:])
	fw.writeBytes(data)

	if err := fw.endWrite(); err != nil {
		return 0, err
	}

	return n, nil
}

func (fw *FrameWriter) WritePing() error {
	fw.startWrite(FramePing, 0, 0, 0, 0)
	return fw.endWrite()
}

func (fw *FrameWriter) WritePingAck() error {
	fw.startWrite(FramePing, FlagPingAck, 0, 0, 0)
	return fw.endWrite()
}

func (fw *FrameWriter) startWrite(frameType FrameType, flags Flags, streamID uint16, seqNum uint16, windowSize uint16) {
	fw.writerBuf = fw.writerBuf[:0]
	fw.writerBuf = append(fw.writerBuf, 0, 0) // 2 bytes of length, filled in endWrite
	fw.writeByte(byte(frameType))
	fw.writeByte(byte(flags))
	fw.writeUint16(streamID)
	fw.writeUint16(seqNum)
	fw.writeUint16(windowSize)
}

func (fw *FrameWriter) endWrite() error {
	length := len(fw.writerBuf) - frameHeaderLen
	if length >= (1 << 16) {
		return ErrFrameTooLarge
	}

	_ = append(fw.writerBuf[:0], byte(length>>8), byte(length)) // big-endian 16-bit length field

	if fw.w == nil {
		data := make([]byte, len(fw.writerBuf))
		copy(data, fw.writerBuf)
		select {
		case fw.writerCh <- data:
		case <-fw.closeCtx.Done():
			return fw.closeCtx.Err()
		}
	} else {
		n, err := fw.w.Write(fw.writerBuf)
		if err != nil {
			return ErrConnection
		}
		if n != len(fw.writerBuf) {
			// return io.ErrShortWrite
			return ErrConnection
		}
	}

	return nil
}

func (fw *FrameWriter) writeByte(v byte)    { fw.writerBuf = append(fw.writerBuf, v) }
func (fw *FrameWriter) writeBytes(v []byte) { fw.writerBuf = append(fw.writerBuf, v...) }
func (fw *FrameWriter) writeUint16(v uint16) {
	fw.writerBuf = binary.BigEndian.AppendUint16(fw.writerBuf, v)
}
func (fw *FrameWriter) writeUint64(v uint64) {
	fw.writerBuf = binary.BigEndian.AppendUint64(fw.writerBuf, v)
}

type FrameReader struct {
	r             reader.ReaderWithDeadline
	headerBuf     []byte
	getReadBuf    func() []byte
	readBuf       []byte
	dataFramePool *DataFramePool
}

func NewFrameReader(r reader.ReaderWithDeadline, framePool *DataFramePool) *FrameReader {
	frameReader := FrameReader{
		r:             r,
		dataFramePool: framePool,
	}
	if r != nil {
		frameReader.headerBuf = make([]byte, frameHeaderLen)
	}
	frameReader.getReadBuf = func() []byte {
		if frameReader.readBuf == nil {
			frameReader.readBuf = make([]byte, maxFrameSize)
		}
		return frameReader.readBuf[:maxFrameSize]
	}

	return &frameReader
}

func (fr *FrameReader) ReadFrame(timeout time.Duration) (frame *Frame, err error) {
	if err = fr.r.SetDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	defer func() {
		_ = fr.r.SetDeadline(time.Time{})
		if err != nil {
			frame = nil
		}
	}()

	buf := fr.getReadBuf()
	n, err := fr.r.Read(buf)
	if err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) {
			return nil, err
		}
		return nil, err
	}
	if n < frameHeaderLen {
		return nil, ErrConnection
	}

	frameHeader := FrameHeader{
		Length:         fr.readUint16(buf[:2]),
		Type:           FrameType(buf[2]),
		Flags:          Flags(buf[3]),
		StreamID:       fr.readUint16(buf[4:6]),
		SequenceNumber: fr.readUint16(buf[6:8]),
		WindowSize:     fr.readUint16(buf[8:10]),
	}

	if n-frameHeaderLen != int(frameHeader.Length) {
		return nil, ErrConnection
	}
	buf = buf[frameHeaderLen : frameHeaderLen+int(frameHeader.Length)]

	var frm []byte
	if frameHeader.Type == FrameData {
		frm = fr.dataFramePool.GetFrame(frameHeader.Length)
	} else {
		frm = make([]byte, frameHeader.Length)
	}
	copy(frm, buf)

	frame = &Frame{FrameHeader: frameHeader, Frame: frm}
	return frame, err
}

func (fr *FrameReader) readUint16(buf []byte) uint16 {
	return binary.BigEndian.Uint16(buf)
}

type FrameParser struct {
	framePool *DataFramePool
}

func NewFrameParser(framePool *DataFramePool) *FrameParser {
	return &FrameParser{
		framePool: framePool,
	}
}

func (fp *FrameParser) ParseSettingsFrame(frame *Frame) (*SettingsFrame, error) {
	if frame.FrameHeader.StreamID != 0 {
		// TODO: Connection error
		return nil, ErrConnection
	}
	if len(frame.Frame) > 0 {
		// TODO: Connection error
		return nil, ErrConnection
	}

	settings := SettingsFrame{
		FrameHeader: frame.FrameHeader,
	}
	if !settings.IsAck() && settings.WindowSize == 0 {
		return nil, ErrConnection
	}

	return &settings, nil
}

func (fp *FrameParser) ParseAckFrame(frame *Frame) (*AckFrame, error) {
	if len(frame.Frame) > 0 {
		// TODO: Connection error
		return nil, ErrConnection
	}

	return &AckFrame{
		FrameHeader: frame.FrameHeader,
	}, nil
}

func (fp *FrameParser) ParseDataFrame(frame *Frame) (*DataFrame, error) {
	if frame.FrameHeader.StreamID == 0 {
		// TODO: Connection error
		return nil, ErrConnection
	}

	data := DataFrame{
		FrameHeader: frame.FrameHeader,
	}
	data.DataSequenceNumber = fp.readUint16(frame.Frame[:2])
	data.Timestamp = fp.readUint64(frame.Frame[2:10])
	data.PartNumber = fp.readUint16(frame.Frame[10:12])
	data.Data = frame.Frame[12+sha256Size:]
	data.Checksum = frame.Frame[12 : 12+sha256Size]

	data.PooledFrame = frame.Frame

	return &data, nil
}

func (fp *FrameParser) ParsePingFrame(frame *Frame) (*PingFrame, error) {
	defer func() { fp.framePool.PutFrame(frame.Frame) }()

	if frame.FrameHeader.StreamID != 0 {
		// TODO: Connection error
		return nil, ErrConnection
	}
	if len(frame.Frame) > 0 {
		// TODO: Connection error
		return nil, ErrConnection
	}

	return &PingFrame{
		FrameHeader: frame.FrameHeader,
	}, nil
}

func (fr *FrameParser) readUint16(buf []byte) uint16 {
	return binary.BigEndian.Uint16(buf)
}
func (fr *FrameParser) readUint64(buf []byte) uint64 {
	return binary.BigEndian.Uint64(buf)
}
