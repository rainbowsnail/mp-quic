package quic

import (
	"github.com/lucas-clemente/quic-go/internal/protocol"
	"github.com/lucas-clemente/quic-go/internal/utils"
	"sort"
	"sync"
)

// ScheduleHandler deal with scheduling
type ScheduleHandler interface {
	// called when a new stream write bytes
	AddStreamByte(protocol.StreamID, protocol.ByteCount)
	// called to cancel a stream write
	DelStreamByte(protocol.StreamID)
	// returns the stream queue
	GetStreamQueue() []protocol.StreamID
	// returns stream limit of path
	GetPathStreamLimit(protocol.PathID, protocol.StreamID) protocol.ByteCount
	// called to consume bytes
	ConsumePathBytes(protocol.PathID, protocol.StreamID, protocol.ByteCount)
	// called when path availability changed
	RefreshPath([]protocol.PathID)
}

type streamInfo struct {
	bytes protocol.ByteCount
	alloc map[protocol.PathID]protocol.ByteCount
}

type epicScheduling struct {
	sync.RWMutex

	paths       []protocol.PathID
	streams     map[protocol.StreamID]*streamInfo
	streamQueue []protocol.StreamID
}

// NewEpicScheduling creates an epic scheduling handler
func NewEpicScheduling() ScheduleHandler {
	e := &epicScheduling{}
	e.setup()
	return e
}

func (e *epicScheduling) setup() {
	e.paths = make([]protocol.PathID, 0)
	e.streamQueue = make([]protocol.StreamID, 0)
	e.streams = make(map[protocol.StreamID]*streamInfo)
}

// Tiny: not thread safe
func (e *epicScheduling) updateStreamQueue() {
	// Tiny: we assume all priority are the same, so just sort by bytes
	e.streamQueue = e.streamQueue[:0]
	for sid := range e.streams {
		e.streamQueue = append(e.streamQueue, sid)
	}
	sort.Slice(e.streamQueue, func(i, j int) bool {
		ii, jj := e.streamQueue[i], e.streamQueue[j]
		return e.streams[ii].bytes < e.streams[jj].bytes
	})
}

// Tiny: not thread safe
func (e *epicScheduling) rearrangeStreams() {
	e.updateStreamQueue()

	if len(e.paths) == 0 {
		return
	}

	for _, sid := range e.streamQueue {
		s := e.streams[sid]

		for t := range s.alloc {
			delete(s.alloc, t)
		}

		// Tiny: TODO now we just simply average
		bytes := s.bytes
		cnt := len(e.paths)
		avg := s.bytes / protocol.ByteCount(cnt)
		for _, pth := range e.paths {
			cnt--
			if cnt > 0 {
				s.alloc[pth] = avg
				bytes -= avg
			} else {
				s.alloc[pth] = bytes
			}
		}
	}
}

func (e *epicScheduling) AddStreamByte(streamID protocol.StreamID, bytes protocol.ByteCount) {
	e.Lock()
	defer e.Unlock()

	utils.Infof("add stream %v %v bytes", streamID, bytes)

	if s, ok := e.streams[streamID]; ok {
		if s.bytes > 0 {
			utils.Errorf("try duplicate add stream %v bytes, ignore", streamID)
			return
		}
		s.bytes = bytes
	} else {
		e.streams[streamID] = &streamInfo{
			bytes: bytes,
			alloc: make(map[protocol.PathID]protocol.ByteCount),
		}
	}
	e.rearrangeStreams()
}

func (e *epicScheduling) DelStreamByte(streamID protocol.StreamID) {
	e.Lock()
	defer e.Unlock()
	if s, ok := e.streams[streamID]; ok {
		s.bytes = 0
		e.rearrangeStreams()
	} else {
		utils.Errorf("try delete non-existing stream %v, ignore", streamID)
	}
}

// Tiny: we must copy for thread safety
func (e *epicScheduling) GetStreamQueue() []protocol.StreamID {
	e.RLock()
	defer e.RUnlock()
	ret := make([]protocol.StreamID, len(e.streamQueue))
	copy(ret, e.streamQueue)
	return ret
}

func (e *epicScheduling) GetPathStreamLimit(pid protocol.PathID, sid protocol.StreamID) protocol.ByteCount {
	e.RLock()
	defer e.RUnlock()
	if s, ok := e.streams[sid]; ok {
		if s.bytes > 0 {
			return s.alloc[pid]
		}
	} else {
		utils.Errorf("try get path %v limit on non-existing stream %v, ignore", pid, sid)
	}
	return 0
}

// Tiny: we rearrange every time, i dont know if this will work
func (e *epicScheduling) ConsumePathBytes(pathID protocol.PathID, streamID protocol.StreamID, bytes protocol.ByteCount) {
	e.Lock()
	defer e.Unlock()
	if s, ok := e.streams[streamID]; ok {
		if b, ok := s.alloc[pathID]; ok && s.bytes >= bytes && b >= bytes {
			s.bytes -= bytes
			// s.alloc[pathID] -= bytes
			e.rearrangeStreams()
			return
		}
	}
	utils.Errorf("path %v try consume %v bytes on stream %v failed", pathID, bytes, streamID)
}

func (e *epicScheduling) RefreshPath(paths []protocol.PathID) {
	e.Lock()
	defer e.Unlock()
	// Tiny: ensure paths wont be used outside
	e.paths = paths
	e.rearrangeStreams()
}
