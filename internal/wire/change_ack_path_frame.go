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
	ErrInvalidPathID = errors.New("ChanegeAckPathFrame: frame contains invalid pathID")
)

type ChangeAckPathFrame struct {
	ackReturnPaths map[protocol.PathID]protocol.PathID
	//PacketReceivedTime time.Time
}

// parse Change Ack Path Frame after 
func ParseChangeAckPathFrame(r *bytes.Reader, version protocol.VersionNumber) (*ChangeAckPathFrame, error) {
	frame := &ChangeAckPathFrame{
		ackReturnPaths:	make(map[protocol.PathID]*path)
		//PacketReceivedTime: time.Now()
	}
	var numPathToChange uint8
	numPathToChange, err = r.ReadByte()
	if err != nil {
		return nil, err
	}
	for i := uint8(0); i < numPathToChange; i++ {
		pathID, err = r.ReadByte()
		if err != nil {
			return nil, err
		}
		ackReturnPathID, err = r.ReadByte()
		if err != nil {
			return nil, err
		}
		frame.ackReturnPaths[protocol.PathID(pathID)] = protocol.PathID(ackReturnPathID)
	}
	return frame
}

// Write writes an Change Ack Path frame.
func (f *ChangeAckPathFrame) Write(b *bytes.Buffer, version protocol.VersionNumber) error {
	var numPathToChange uint8
	numPathToChange = len(f.ackReturnPaths)
	b.WriteByte(numPathToChange)
	for pathID, ackReturnPathID := range f.ackReturnPaths {
		b.WriteByte(uint8(PathID))
		b.WriteByte(uint8(ackReturnPathID))
	}
	return nil
}

// MinLength of a written frame
func (f *ChangeAckPathFrame) MinLength(version protocol.VersionNumber) (protocol.ByteCount, error) {
	return 1 + 2, nil
}
