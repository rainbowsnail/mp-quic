package quic

import (
	"github.com/lucas-clemente/quic-go/internal/protocol"
	"github.com/lucas-clemente/quic-go/internal/utils"

	"math"
	// "runtime/debug"
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
	id     protocol.StreamID
	que    bool
	parent protocol.StreamID
	weight float64
	bytes  protocol.ByteCount

	prop    float64
	quota   float64
	alloc   protocol.PathID
	alloced bool
	// alloc  map[protocol.PathID]protocol.ByteCount
}

type pathInfo struct {
	path  *path
	queue time.Duration
	rtt   time.Duration

	// bytes per second
	thr float64
	// queue time + rtt in second
	// totQue float64
	// temporary size allocation (maybe not integer and maybe < 0)
	// si float64
	// final size allocation
	// size protocol.ByteCount
}

type depNode struct {
	id     protocol.StreamID
	parent *depNode
	child  []*depNode

	// size      float64
	sumWeight float64
	weight    float64
	prop      float64
	// delay     float64
	// quota float64
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
	// e.pathMap = make(map[protocol.PathID]*pathInfo)
	// e.paths = make([]*pathInfo, 0)
	e.streamQueue = make([]protocol.StreamID, 0)
	e.streams = make(map[protocol.StreamID]*streamInfo)
}

func (n *depNode) dfs() {
	if n.parent != nil {
		if n.weight > 0 {
			n.prop = n.parent.prop * n.weight / n.parent.sumWeight
			// n.delay = n.parent.delay + n.size/n.prop
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

	for sid, si := range e.streams {
		s, _ := e.sess.streamsMap.GetOrOpenStream(sid)

		if sid > 9 && !shouldQueue(si, s) {
			continue
		}

		if sid <= 3 {
			continue
		}

		cur := getNode(sid)
		cur.weight = float64(si.weight)
		if cur.weight <= 0 {
			cur.weight = 1
		}
		// cur.size = float64(si.bytes)
		pa := getNode(si.parent)
		pa.child = append(pa.child, cur)
		cur.parent = pa
		pa.sumWeight += cur.weight
	}
	root := getNode(0)
	root.dfs()

	return ret
}

func shouldQueue(si *streamInfo, s *stream) bool {
	return s != nil && !s.finSent.Get()
}

// Tiny: not thread safe
func (e *epicScheduling) updateStreamQueue() {
	tree := e.buildTree()

	var sum, cnt float64
	for _, sid := range e.streamQueue {
		si := e.streams[sid]
		si.que = true
		sum += si.quota / si.prop
		cnt++
	}
	if cnt > 0 {
		sum /= cnt
	}

	e.streamQueue = e.streamQueue[:0]
	for sid, si := range e.streams {
		sn := tree[sid]
		s, _ := e.sess.streamsMap.GetOrOpenStream(sid)

		if !shouldQueue(si, s) || (sid > 3 && sn.prop <= 0) {
			// closed streams
			si.que = false
			continue
		}

		if sid > 3 {
			if !si.que {
				// new streams
				si.quota = sum / sn.prop
			} else {
				si.quota = si.quota / si.prop * sn.prop
			}
			si.prop = sn.prop
		}

		e.streamQueue = append(e.streamQueue, sid)
	}

	e.sortQueue()
}

// func (e *epicScheduling) updatePath() {
// 	// Tiny: it may cause race
// 	sort.Slice(e.paths, func(i, j int) bool {
// 		rtt1 := e.paths[i].path.rttStats.SmoothedRTT()
// 		rtt2 := e.paths[j].path.rttStats.SmoothedRTT()
// 		return rtt1 < rtt2
// 	})
// }

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

func (e *epicScheduling) sortQueue() {
	sort.Slice(e.streamQueue, func(i, j int) bool {
		ii, jj := e.streamQueue[i], e.streamQueue[j]
		if ii < 11 || jj < 11 {
			return ii < jj
		}
		return e.streams[ii].quota < e.streams[jj].quota
	})
}

// Tiny: not thread safe
func (e *epicScheduling) rearrangeStreams() {
	// utils.Infof("rearrange called")
	// t := time.Now()
	// utils.Infof("rearrange")
	// debug.PrintStack()

	// e.updatePath()
	utils.Infof("rearrange")
	// debug.PrintStack()
	e.updateStreamQueue()

	// if len(e.paths) == 0 {
	// 	return
	// }

	// if some RTT is not estimated, we do not put on limit
	// est := e.getPathInfo()
	// if !est {
	// 	for _, sid := range e.streamQueue {
	// 		s := e.streams[sid]

	// 		for t := range s.alloc {
	// 			delete(s.alloc, t)
	// 		}

	// 		for _, p := range e.paths {
	// 			s.alloc[p.path.pathID] = s.bytes
	// 		}
	// 	}
	// 	return
	// }

	// var bwSum float64
	// for _, p := range e.paths {
	// 	bwSum += p.thr
	// }

	// for _, sid := range e.streamQueue {
	// 	s := e.streams[sid]

	// 	for t := range s.alloc {
	// 		delete(s.alloc, t)
	// 	}

	// 	// Tiny: actually there are headers but we ignore them
	// 	if s.bytes < protocol.MaxPacketSize {
	// 		// Tiny: if the stream is small enough, find a path with minimal queue + rtt
	// 		var (
	// 			mini time.Duration = 1<<63 - 1
	// 			k    *pathInfo
	// 		)
	// 		for _, p := range e.paths {
	// 			if mini > p.rtt+p.queue {
	// 				mini = p.rtt + p.queue
	// 				k = p
	// 			}
	// 		}

	// 		k.size = s.bytes
	// 	} else {
	// 		// Tiny: minimize total queue time
	// 		bytes := float64(s.bytes)

	// 		var queBDPSum float64
	// 		for _, p := range e.paths {
	// 			p.totQue = float64(p.queue+p.rtt) / float64(time.Second)
	// 			queBDPSum += p.thr * p.totQue
	// 		}
	// 		k := (bytes + queBDPSum) / bwSum

	// 		for _, p := range e.paths {
	// 			p.si = p.thr * (k - p.totQue)
	// 		}

	// 		// Tiny: this part convert float size to integer & non-negative, i dont know if it works well
	// 		delta := int64(s.bytes)
	// 		for _, p := range e.paths {
	// 			p.size = protocol.ByteCount(math.Max(math.Floor(p.si), 0))
	// 			delta -= int64(p.size)
	// 		}

	// 		for _, p := range e.paths {
	// 			if p.size > 0 && int64(p.size)+delta >= 0 {
	// 				p.size = protocol.ByteCount(int64(p.size) + delta)
	// 				break
	// 			}
	// 		}
	// 	}

	// 	for _, p := range e.paths {
	// 		s.alloc[p.path.pathID] = p.size
	// 		if p.size > 0 {
	// 			p.queue += p.rtt + time.Duration(float64(time.Second)*float64(p.size)/p.thr)
	// 		}
	// 	}
	// }

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
			id:    streamID,
			bytes: bytes,
			// alloc:  make(map[protocol.PathID]protocol.ByteCount),
			parent: str.parent,
			weight: float64(str.weight),
		}
	}
	utils.Infof("add stream %v %v bytes", streamID, bytes)
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
		// if s.bytes > 0 {
		// 	return s.alloc[pid]
		// }
		return s.bytes
	}
	// } else {
	utils.Errorf("try get path %v limit on non-existing stream %v, ignore", pid, sid)
	// }
	return 0
}

// Tiny: we rearrange every time, i dont know if this will work
func (e *epicScheduling) ConsumePathBytes(pathID protocol.PathID, streamID protocol.StreamID, bytes protocol.ByteCount) {
	e.Lock()
	defer e.Unlock()
	if s, ok := e.streams[streamID]; ok && s.bytes >= bytes {
		// if b, ok := s.alloc[pathID]; ok && s.bytes >= bytes && b >= bytes {
		if bytes > 0 {
			utils.Infof("stream %v consume %v from path %v", streamID, bytes, pathID)
			s.bytes -= bytes
			s.quota += float64(bytes)
			e.sortQueue()
		}
		// s.alloc[pathID] -= bytes
		// e.rearrangeStreams()
		return
		// }
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

func (e *epicScheduling) selectPathFastest() (*pathInfo, *pathInfo) {
	e.getPathInfo()

	if len(e.paths) == 0 {
		return nil, nil
	}
	if len(e.paths) == 1 {
		return e.paths[0], nil
	}

	p1 := e.paths[0]
	r1 := p1.rtt
	var p2 *pathInfo
	for _, p := range e.paths {
		if p.rtt == 0 {
			continue
		}
		if p.rtt < r1 {
			p2 = p1
			p1 = p
			r1 = p.rtt
		} else if p2 == nil {
			p2 = p
		}
	}
	return p1, p2
}

func allocatePath(s *streamInfo, p *pathInfo) {
	utils.Infof("allocate stream %v path %v", s.id)
	s.alloced = true
	s.alloc = p.path.pathID
}

// Jing: select path for a stream
func (e *epicScheduling) streamSelectPath(streamID protocol.StreamID, weight float64, sumWeight float64) error {
	pathFast, pathSlow := e.selectPathFastest()
	//pathSlow := e.sess.scheduler.selecrPathFastAvailable(e.sess)
	s, ok := e.streams[streamID]
	if !ok {
		utils.Debugf("stream %v not exists", streamID)
	}
	if pathSlow == nil {
		allocatePath(s, pathFast)
		utils.Infof("path 2 %v not exists")
		return nil
	}

	// Jing: TODO: if fast path is available
	fastAvailable := pathFast.path.sentPacketHandler.SendingAllowed()
	if fastAvailable {
		allocatePath(s, pathFast)
		return nil
	}

	slowAvailable := pathSlow.path.sentPacketHandler.SendingAllowed()
	if !slowAvailable {
		return nil
	}
	// Jing: TODO: otherwise
	rttFast := pathFast.rtt
	rttSlow := pathSlow.rtt
	sigmaFast := float64(pathFast.path.rttStats.MeanDeviation())
	sigmaSlow := float64(pathSlow.path.rttStats.MeanDeviation())
	utils.Infof("rttSlow=%v, rttFast=%v", rttSlow, rttFast)

	k := float64(e.bytesUntilCompletion(streamID, weight, sumWeight))
	// Jing: n = 1+ k/cwnd_f
	cwndFast := pathFast.thr
	cwndSlow := pathSlow.thr
	n := float64(1+uint(k)) / float64(cwndFast)
	beta := float64(0.8)
	delta := math.Max(sigmaFast, sigmaSlow)

	if (n * rttFast) < (1+float64(s.waiting)*beta)*(rttSlow+delta) {
		if k/cwndSlow*rttSlow >= 2*rttFast+delta {
			s.waiting = 1
			utils.Infof("waiting = 1")
		} else {
			s.pathID = pathSlow.pathID
			utils.Infof("send on slowpath")
		}
	} else {
		s.waiting = 0
		s.pathID = pathSlow.pathID
		utils.Infof("send on slowpath")
	}
	return nil
}

func (e *epicScheduling) bytesUntilCompletion(streamID protocol.StreamID, weight float64, sumWeight float64) protocol.ByteCount {
	leftBytes := uint(e.streams[streamID].bytes)
	left := float64(leftBytes) / float64(bytesOpportunity)
	if leftBytes < lowestQuantum {
		return protocol.ByteCount(leftBytes)
	}
	nomarlizedWeight := weight / sumWeight
	g := (1 - nomarlizedWeight) / nomarlizedWeight
	k := float64(bytesOpportunity) * (g*(left-1) + left)
	return protocol.ByteCount(k)
}
