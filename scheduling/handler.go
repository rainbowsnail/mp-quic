package scheduling

import (
	"github.com/lucas-clemente/quic-go/internal/protocol"
)

// ScheduleHandler deal with scheduling
type ScheduleHandler interface {
	// called when a new stream write bytes
	AddStreamByte(streamID protocol.StreamID, bytes protocol.ByteCount)
	// called to cancel a stream write
	DelStreamByte(streamID protocol.StreamID)
	// called to get what to send on the path
	GetPathScheduling(pathID protocol.PathID) (protocol.StreamID, protocol.ByteCount)
	// called to consume bytes
	ConsumePathBytes(pathID protocol.PathID, streamID protocol.StreamID, bytes protocol.ByteCount)
	// called when path availability changed
	RefreshPath(paths []protocol.PathID)
}
