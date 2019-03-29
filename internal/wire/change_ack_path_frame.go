package wire

import (
	"bytes"
	"errors"
	"time"

	"github.com/lucas-clemente/quic-go/internal/protocol"
	"github.com/lucas-clemente/quic-go/internal/utils"
)

var (
	// ErrInvalidAckRanges occurs when a client sends inconsistent ACK ranges
	ErrInvalidAckRanges = errors.New("AckFrame: ACK frame contains invalid ACK ranges")
)

type ChangeAckPathFrame struct {
	ackPath map[protocol.PathID]protocol.PathID
	PacketReceivedTime time.Time
}

// parse Change Ack Path Frame after 
func ParseChangeAckPathFrame(r *bytes.Reader, version protocol.VersionNumber) (*ChangeAckPathFrame, error) {
	frame := &ChangeAckPathFrame{}
	pathID, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	return frame
}

// Write writes an Change Ack Path frame.
func (f *ChangeAckPathFrame) Write(b *bytes.Buffer, version protocol.VersionNumber) error {
}
