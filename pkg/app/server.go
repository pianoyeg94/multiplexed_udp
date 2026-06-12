package app

import (
	"fmt"

	"github.com/pianoyeg94/multiplexed_udp/pkg/server"
)

func HandleServerMessage(msg *server.ServerMessage) {
	fmt.Printf(
		"Handler got message: sequence_number %v, ts %v, data length %v, checksum len %v, has_lost_chunk %v\n",
		msg.SequenceNumber,
		msg.Timestamp,
		len(msg.Data),
		len(msg.Checksum),
		msg.HasLostDataChunk,
	)
}
