package scheduling

import (
	"github.com/lucas-clemente/quic-go/internal/protocol"
	"github.com/lucas-clemente/quic-go/internal/utils"
	"sort"
	"sync"
)

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

func (e *epicScheduling) GetPathScheduling(pathID protocol.PathID) (protocol.StreamID, protocol.ByteCount) {
	e.RLock()
	defer e.RUnlock()
	for _, sid := range e.streamQueue {
		if s, ok := e.streams[sid]; ok {
			if b, ok := s.alloc[pathID]; ok && s.bytes > 0 && b > 0 {
				return sid, b
			}
		}
	}
	utils.Infof("no stream to schedule on path %v", pathID)
	return 0, 0
}

// Tiny: we dont rearrange every time
func (e *epicScheduling) ConsumePathBytes(pathID protocol.PathID, streamID protocol.StreamID, bytes protocol.ByteCount) {
	e.Lock()
	defer e.Unlock()
	if s, ok := e.streams[streamID]; ok {
		if b, ok := s.alloc[pathID]; ok && s.bytes >= bytes && b >= bytes {
			s.bytes -= bytes
			s.alloc[pathID] -= bytes
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
