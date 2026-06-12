package client

type streamCycle struct {
	streamIDs []uint16
	currIdx   uint64
}

func newStreamCycle() *streamCycle {
	streamIDs := make([]uint16, 0, maxStreamCount)
	for id := uint16(1); id <= maxStreamCount; id++ {
		streamIDs = append(streamIDs, id)
	}
	return &streamCycle{
		streamIDs: streamIDs,
	}
}

func (c *streamCycle) nextStreamID() uint16 {
	streamID := c.streamIDs[c.currIdx%uint64(len(c.streamIDs))]
	c.currIdx++
	return streamID
}
