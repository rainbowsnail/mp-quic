package wire

import (
	"bytes"
	"errors"
	//"time"

	"github.com/lucas-clemente/quic-go/internal/protocol"
	//"github.com/lucas-clemente/quic-go/internal/utils"
)

var (
	// ErrInvalidAckRanges occurs when a client sends inconsistent ACK ranges
	ErrInvalidPathID = errors.New("ChanegeAckPathFrame: frame contains invalid pathID")
)

type ChangeAckPathFrame struct {
	AckReturnPaths map[protocol.PathID]protocol.PathID
	//PacketReceivedTime time.Time
}

// parse Change Ack Path Frame after 
func ParseChangeAckPathFrame(r *bytes.Reader, version protocol.VersionNumber) (*ChangeAckPathFrame, error) {
	frame := &ChangeAckPathFrame{
		//ackReturnPaths:	make(map[protocol.PathID] protocol.PathID)
		//PacketReceivedTime: time.Now()
	}
	frame.AckReturnPaths = make(map[protocol.PathID] protocol.PathID)
	var numPathToChange uint8
	var err error

	numPathToChange, err = r.ReadByte()
	if err != nil {
		return nil, err
	}
	var pathID protocol.PathID
	var uint8PathID uint8
	for i := uint8(0); i < numPathToChange; i++ {
		uint8PathID, err = r.ReadByte()
		if err != nil {
			return nil, err
		}
		pathID = protocol.PathID(uint8PathID)

		var ackReturnPathID protocol.PathID
		var uint8AckReturnPathID uint8
		uint8AckReturnPathID, err = r.ReadByte()
		if err != nil {
			return nil, err
		}
		ackReturnPathID = protocol.PathID(uint8AckReturnPathID)
		frame.AckReturnPaths[protocol.PathID(pathID)] = protocol.PathID(ackReturnPathID)
	}
	return frame, nil
}

// Write writes an Change Ack Path frame.
func (f *ChangeAckPathFrame) Write(b *bytes.Buffer, version protocol.VersionNumber) error {
	typeByte := uint8(0x13)
	b.WriteByte(typeByte)
	var numPathToChange uint8
	numPathToChange = uint8(len(f.AckReturnPaths))
	b.WriteByte(numPathToChange)
	for pathID, ackReturnPathID := range f.AckReturnPaths {
		b.WriteByte(uint8(pathID))
		b.WriteByte(uint8(ackReturnPathID))
	}
	return nil
}

// MinLength of a written frame
func (f *ChangeAckPathFrame) MinLength(version protocol.VersionNumber) (protocol.ByteCount, error) {
	return protocol.ByteCount(1 + 2 * len(f.AckReturnPaths)), nil
}
