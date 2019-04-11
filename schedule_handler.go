package quic

import (
	"github.com/lucas-clemente/quic-go/internal/protocol"
	"github.com/lucas-clemente/quic-go/internal/utils"

	"math"
	"sort"
	"sync"
	"time"
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

type pathInfo struct {
	queue time.Duration
	rtt   time.Duration

	// bytes per second
	thr float64
	// queue time + rtt in second
	totQue float64
	// temporary size allocation (maybe not integer and maybe < 0)
	si float64
	// final size allocation
	size protocol.ByteCount
}

type depNode struct {
	id     protocol.StreamID
	parent *depNode
	child  []*depNode

	size      float64
	sumWeight float64
	weight    float64
	prop      float64
	delay     float64
}

type epicScheduling struct {
	sync.RWMutex

	sess *session

	paths       []protocol.PathID
	streams     map[protocol.StreamID]*streamInfo
	streamQueue []protocol.StreamID
}

// NewEpicScheduling creates an epic scheduling handler
func NewEpicScheduling(sess *session) ScheduleHandler {
	e := &epicScheduling{sess: sess}
	e.setup()
	return e
}

func (e *epicScheduling) setup() {
	e.paths = make([]protocol.PathID, 0)
	e.streamQueue = make([]protocol.StreamID, 0)
	e.streams = make(map[protocol.StreamID]*streamInfo)
}

func (n *depNode) dfs() {
	if n.parent != nil {
		if n.weight > 0 {
			n.prop = n.parent.prop * n.weight / n.parent.sumWeight
			n.delay = n.parent.delay + n.size/n.prop
		} else {
			utils.Errorf("zero weight for stream %v", n.id)
		}
	}
	for _, ch := range n.child {
		ch.dfs()
	}
}

func (e *epicScheduling) buildTree() map[protocol.StreamID]*depNode {
	ret := make(map[protocol.StreamID]*depNode)
	getNode := func(id protocol.StreamID) *depNode {
		var (
			node *depNode
			ok   bool
		)
		if node, ok = ret[id]; !ok {
			node = &depNode{id: id}
			ret[id] = node
		}
		return node
	}

	for sid, si := range e.streams {
		// there cannot be error
		s, _ := e.sess.streamsMap.GetOrOpenStream(sid)
		if s == nil {
			continue
		}

		cur := getNode(sid)
		cur.weight = float64(s.weight)
		if cur.weight <= 0 {
			cur.weight = 1
		}
		cur.size = float64(si.bytes)
		pa := getNode(s.parent)
		pa.child = append(pa.child, cur)
		pa.sumWeight += cur.weight
	}
	root := getNode(0)
	root.dfs()
	return ret
}

// Tiny: not thread safe
func (e *epicScheduling) updateStreamQueue() {
	// tree := e.buildTree()

	e.streamQueue = e.streamQueue[:0]
	for sid := range e.streams {
		e.streamQueue = append(e.streamQueue, sid)
	}
	sort.Slice(e.streamQueue, func(i, j int) bool {
		ii, jj := e.streamQueue[i], e.streamQueue[j]
		return e.streams[ii].bytes < e.streams[jj].bytes
	})
}

func (e *epicScheduling) updatePath() {
	// Tiny: it may cause race
	sort.Slice(e.paths, func(i, j int) bool {
		rtt1 := e.sess.paths[e.paths[i]].rttStats.SmoothedRTT()
		rtt2 := e.sess.paths[e.paths[j]].rttStats.SmoothedRTT()
		return rtt1 < rtt2
	})
}

func (e *epicScheduling) getPathInfo() map[protocol.PathID]*pathInfo {
	ret := make(map[protocol.PathID]*pathInfo)
	for _, pid := range e.paths {
		// Tiny: should this have lock?
		pth := e.sess.paths[pid]
		ret[pid] = &pathInfo{
			thr: float64(pth.sentPacketHandler.GetBandwidthEstimate()) / 8.0,
			rtt: pth.rttStats.SmoothedRTT(),
		}
	}
	return ret
}

// Tiny: not thread safe
func (e *epicScheduling) rearrangeStreams() {
	e.updatePath()
	e.updateStreamQueue()

	if len(e.paths) == 0 {
		return
	}

	pathInfo := e.getPathInfo()
	var bwSum float64
	for _, p := range pathInfo {
		bwSum += p.thr
	}

	for _, sid := range e.streamQueue {
		s := e.streams[sid]

		for t := range s.alloc {
			delete(s.alloc, t)
		}

		// Tiny: actually there are headers but we ignore them
		if s.bytes < protocol.MaxPacketSize {
			// Tiny: if the stream is small enough, find a path with minimal queue + rtt
			var (
				mini time.Duration = 1<<63 - 1
				k    protocol.PathID
			)
			for pid, p := range pathInfo {
				if mini > p.rtt+p.queue {
					mini = p.rtt + p.queue
					k = pid
				}
			}

			pathInfo[k].size = s.bytes
		} else {
			// Tiny: minimize total queue time
			bytes := float64(s.bytes)

			var queBDPSum float64
			for _, p := range pathInfo {
				p.totQue = float64(p.queue+p.rtt) / float64(time.Second)
				queBDPSum += p.thr * p.totQue
			}
			k := (bytes + queBDPSum) / bwSum

			for _, p := range pathInfo {
				p.si = p.thr * (k - p.totQue)
			}

			// Tiny: this part convert float size to integer & non-negative, i dont know if it works well
			delta := int64(s.bytes)
			for _, p := range pathInfo {
				p.size = protocol.ByteCount(math.Max(math.Floor(p.si), 0))
				delta -= int64(p.size)
			}

			for _, p := range pathInfo {
				if p.size > 0 && int64(p.size)+delta >= 0 {
					p.size = protocol.ByteCount(int64(p.size) + delta)
					break
				}
			}
		}

		for pid, p := range pathInfo {
			s.alloc[pid] = p.size
			if p.size > 0 {
				p.queue += p.rtt + time.Duration(float64(time.Second)*float64(p.size)/p.thr)
			}
		}
	}
}

func (e *epicScheduling) AddStreamByte(streamID protocol.StreamID, bytes protocol.ByteCount) {
	e.Lock()
	defer e.Unlock()

	utils.Debugf("add stream %v %v bytes", streamID, bytes)

	if s, ok := e.streams[streamID]; ok {
		// if s.bytes > 0 {
		// 	utils.Errorf("try duplicate add stream %v bytes, ignore", streamID)
		// 	return
		// }
		s.bytes += bytes
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
			utils.Debugf("stream %v consume %v from path %v", streamID, bytes, pathID)
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
