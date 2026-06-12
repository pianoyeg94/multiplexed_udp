package stream

import (
	"github.com/igrmk/treemap/v2"

	"github.com/pianoyeg94/multiplexed_udp/pkg/protocol/frame"
)

type reassembleBuffer struct {
	*treemap.TreeMap[uint16, *frame.DataFrame]
}

func newReassembleBuffer() reassembleBuffer {
	return reassembleBuffer{
		TreeMap: treemap.New[uint16, *frame.DataFrame](),
	}
}
