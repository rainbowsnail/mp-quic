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
	RefreshPath(bool)
	// called manually
	RearrangeStreams()
}

type streamInfo struct {
	que    bool
	parent protocol.StreamID
	weight int
	bytes  protocol.ByteCount
	alloc  map[protocol.PathID]protocol.ByteCount
}

type pathInfo struct {
	path  *path
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

	paths       []*pathInfo
	pathMap     map[protocol.PathID]*pathInfo
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
	e.pathMap = make(map[protocol.PathID]*pathInfo)
	e.paths = make([]*pathInfo, 0)
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
	} else {
		n.prop = 1
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
			node = &depNode{id: id, weight: 1}
			ret[id] = node
		}
		return node
	}

	e.streamQueue = e.streamQueue[:0]
	for sid, si := range e.streams {
		si.que = true
		// there cannot be error
		s, _ := e.sess.streamsMap.GetOrOpenStream(sid)
		if s == nil || (si.bytes == 0 && (!s.finishedWriting.Get() || s.finSent.Get())) {
			si.que = false
		}

		if si.que {
			e.streamQueue = append(e.streamQueue, sid)
		}

		cur := getNode(sid)
		cur.weight = float64(si.weight)
		if cur.weight <= 0 {
			cur.weight = 1
		}
		cur.size = float64(si.bytes)
		pa := getNode(si.parent)
		pa.child = append(pa.child, cur)
		cur.parent = pa
		pa.sumWeight += cur.weight
	}
	root := getNode(0)
	root.dfs()
	return ret
}

// Tiny: not thread safe
func (e *epicScheduling) updateStreamQueue() {
	tree := e.buildTree()

	sort.Slice(e.streamQueue, func(i, j int) bool {
		ii, jj := e.streamQueue[i], e.streamQueue[j]
		if ii == 3 || ii == 1 {
			return true
		} else if jj == 3 || jj == 1 {
			return false
		}
		return tree[ii].delay < tree[jj].delay
	})
}

func (e *epicScheduling) updatePath() {
	// Tiny: it may cause race
	sort.Slice(e.paths, func(i, j int) bool {
		rtt1 := e.paths[i].path.rttStats.SmoothedRTT()
		rtt2 := e.paths[j].path.rttStats.SmoothedRTT()
		return rtt1 < rtt2
	})
	for _, pth := range e.paths {
		utils.Infof("%v", pth.path.pathID)
	}
}

func (e *epicScheduling) getPathInfo() bool {
	for _, pi := range e.paths {
		pth := pi.path
		srtt := pth.rttStats.SmoothedRTT()
		if srtt == 0 {
			utils.Infof("no est path %v", pth.pathID)
			return false
		}
		pi.thr = float64(pth.sentPacketHandler.GetBandwidthEstimate()) / 8.0
		pi.rtt = srtt
		pi.queue = 0
		utils.Infof("path %v thr %v", pth.pathID, pi.thr)
	}
	return true
}

// Tiny: not thread safe
func (e *epicScheduling) rearrangeStreams() {
	// utils.Infof("rearrange called")
	// t := time.Now()
	// utils.Infof("rearrange")
	// debug.PrintStack()

	e.updatePath()
	e.updateStreamQueue()

	if len(e.paths) == 0 {
		return
	}

	// if some RTT is not estimated, we do not put on limit
	est := e.getPathInfo()
	if !est {
		for _, sid := range e.streamQueue {
			s := e.streams[sid]

			for t := range s.alloc {
				delete(s.alloc, t)
			}

			for _, p := range e.paths {
				s.alloc[p.path.pathID] = s.bytes
			}
		}
		return
	}

	var bwSum float64
	for _, p := range e.paths {
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
				k    *pathInfo
			)
			for _, p := range e.paths {
				if mini > p.rtt+p.queue {
					mini = p.rtt + p.queue
					k = p
				}
			}

			k.size = s.bytes
		} else {
			// Tiny: minimize total queue time
			bytes := float64(s.bytes)

			var queBDPSum float64
			for _, p := range e.paths {
				p.totQue = float64(p.queue+p.rtt) / float64(time.Second)
				queBDPSum += p.thr * p.totQue
			}
			k := (bytes + queBDPSum) / bwSum

			for _, p := range e.paths {
				p.si = p.thr * (k - p.totQue)
			}

			// Tiny: this part convert float size to integer & non-negative, i dont know if it works well
			delta := int64(s.bytes)
			for _, p := range e.paths {
				p.size = protocol.ByteCount(math.Max(math.Floor(p.si), 0))
				delta -= int64(p.size)
			}

			for _, p := range e.paths {
				if p.size > 0 && int64(p.size)+delta >= 0 {
					p.size = protocol.ByteCount(int64(p.size) + delta)
					break
				}
			}
		}

		for _, p := range e.paths {
			s.alloc[p.path.pathID] = p.size
			if p.size > 0 {
				p.queue += p.rtt + time.Duration(float64(time.Second)*float64(p.size)/p.thr)
			}
		}
	}

	// utils.Infof("rearrange %v", int(time.Since(t)/time.Microsecond))
}

func (e *epicScheduling) RearrangeStreams() {
	e.Lock()
	defer e.Unlock()

	e.rearrangeStreams()
}

func (e *epicScheduling) AddStreamByte(streamID protocol.StreamID, bytes protocol.ByteCount) {
	e.Lock()
	defer e.Unlock()

	if s, ok := e.streams[streamID]; ok {
		if bytes <= 0 && s.que {
			return
		}
		s.bytes += bytes
	} else {
		str, _ := e.sess.streamsMap.GetOrOpenStream(streamID)
		e.streams[streamID] = &streamInfo{
			bytes:  bytes,
			alloc:  make(map[protocol.PathID]protocol.ByteCount),
			parent: str.parent,
			weight: str.weight,
		}
	}
	utils.Debugf("add stream %v %v bytes", streamID, bytes)
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
			s.alloc[pathID] -= bytes
			// e.rearrangeStreams()
			return
		}
	}
	utils.Errorf("path %v try consume %v bytes on stream %v failed", pathID, bytes, streamID)
}

// Tiny: get all not failed & open paths
func (e *epicScheduling) getAlivePaths() {
	e.paths = e.paths[:0]
	if len(e.pathMap) <= 1 {
		pi := e.pathMap[protocol.InitialPathID]
		p := pi.path
		if p.open.Get() && !p.potentiallyFailed.Get() {
			e.paths = append(e.paths, pi)
			return
		}
	}

	for pid, pi := range e.pathMap {
		p := pi.path
		if pid != protocol.InitialPathID && p.open.Get() && !p.potentiallyFailed.Get() {
			e.paths = append(e.paths, pi)
		}
	}
	// utils.Infof("refresh paths %v", e.paths)
}

func (e *epicScheduling) RefreshPath(copyMap bool) {
	e.Lock()
	defer e.Unlock()

	// we assume there cannot be concurrent if copyMap = true
	if copyMap {
		for k := range e.pathMap {
			delete(e.pathMap, k)
		}
		for k, v := range e.sess.paths {
			e.pathMap[k] = &pathInfo{path: v}
		}
	}

	e.getAlivePaths()
	e.rearrangeStreams()
}
