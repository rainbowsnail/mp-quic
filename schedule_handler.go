package quic

import (
	"github.com/lucas-clemente/quic-go/internal/protocol"
	"github.com/lucas-clemente/quic-go/internal/utils"

	"math"
	"sort"
	"sync"
	"time"
)

const (
	maxOpportunity = uint(1000)
	bytesOpportunity = uint(1000)
	lowestQuantum = uint(1000)
)

// ScheduleHandler deal with scheduling
type ScheduleHandler interface {
	// called when a new stream write bytes
	AddStreamByte(protocol.StreamID, protocol.ByteCount)
	// called to cancel a stream write
	DelStreamByte(protocol.StreamID)
	// returns the stream queue
	GetStreamQueue() []protocol.StreamID
	// Jing: return next stream to send
	UpdateOpportunity(streamID protocol.StreamID, bytes protocol.ByteCount) 
	// Jing: send date on active streams
	GetActiveStream() *map[protocol.StreamID]uint
	GetStreamOpportunity() *map[protocol.StreamID]uint
	Check(sid protocol.StreamID, pathID protocol.PathID) bool
	// returns stream limit of path
	GetPathStreamLimit(protocol.PathID, protocol.StreamID) protocol.ByteCount
	// called to consume bytes
	ConsumePathBytes(protocol.PathID, protocol.StreamID, protocol.ByteCount)
	// called when path availability changed
	RefreshPath([]protocol.PathID)
	// called manually
	RearrangeStreams()
}

type streamInfo struct {
	bytes protocol.ByteCount
	pathID protocol.PathID
	oppotunity uint
	waiting uint
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
	activeNodes []*depNode
	streamOpportunity map[protocol.StreamID]uint
	streamQueue []protocol.StreamID
	sumRemainOpportunity uint
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
	e.activeNodes = make([]*depNode, 0)
	e.streamOpportunity = make(map[protocol.StreamID]uint)
	e.streams = make(map[protocol.StreamID]*streamInfo)
	e.sumRemainOpportunity = int(0)
}
func (e *epicScheduling) Check(sid protocol.StreamID, pathID protocol.PathID) bool{
	if e.streams[sid].pathID != pathID || e.streams[sid].waiting == 1 {
		return false
	}
	return true
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
		// there cannot be error
		s, _ := e.sess.streamsMap.GetOrOpenStream(sid)
		if s == nil {
			continue
		}

		e.streamQueue = append(e.streamQueue, sid)
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

// Jing: get all ActiveNodes (have data and can send data) and return sum weight
func (e *epicScheduling) getActiveNodes(n *depNode) float64{
	sumWeight := float64(0)
	if n.size > 0 {
		e.activeNodes = append(e.activeNodes,n)
		//e.activeNodes.append(n)
		return n.weight
	}
	for _, ch := range n.child {
		sumWeight += e.getActiveNodes(ch)
	}
	return sumWeight
}

// Jing: select path for a stream
func (e *epicScheduling) streamSelectPath(streamID protocol.StreamID, weight float64, sumWeight float64) error{
	pathFast,pathSlow :=  e.sess.scheduler.selectPathFastest(e.sess)
	//pathSlow := e.sess.scheduler.selecrPathFastAvailable(e.sess)
	s, ok := e.streams[streamID]
	if !ok {
		utils.Debugf("stream %v not exists", streamID)
	}

	// Jing: TODO: if fast path is available 
	fastAvailable := e.sess.scheduler.PathAvailable(pathFast)
	if fastAvailable {
		s.pathID = pathSlow.pathID
	}
	// Jing: TODO: otherwise
	rttFast := float64(pathFast.rttStats.SmoothedRTT())
	rttSlow := float64(pathFast.rttStats.SmoothedRTT())
	sigmaFast := float64(pathFast.rttStats.MeanDeviation())
	sigmaSlow := float64(pathFast.rttStats.MeanDeviation())

	k := float64(e.bytesUntilCompletion(streamID, weight, sumWeight))
	// Jing: n = 1+ k/cwnd_f
	cwndFast := float64(pathFast.sentPacketHandler.GetBandwidthEstimate() / 8)
	cwndSlow := float64(pathSlow.sentPacketHandler.GetBandwidthEstimate() / 8)
	n := float64(1 + uint(k)) / float64(cwndFast)
	beta := float64(0.8)
	delta := math.Max(sigmaFast, sigmaSlow)

	
	if (n * rttFast) < (1 + float64(s.waiting) * beta) * (rttSlow + delta) {
		if k / cwndSlow * rttSlow >= 2 * rttFast + delta {
			s.waiting = 1
		} else {
			s.pathID = pathSlow.pathID
		}
	} else {
		s.waiting = 0
		s.pathID = pathSlow.pathID
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
	k := float64(bytesOpportunity) * ( g * (left - 1) + left )
	return protocol.ByteCount(k)
} 

func (e *epicScheduling) updateOpportunity(n *depNode) error{
	sumWeight := e.getActiveNodes(n)
	e.streamOpportunity = make(map[protocol.StreamID]uint)
	for _, n := range e.activeNodes {
		e.streamOpportunity[n.id] = uint(n.weight * float64(maxOpportunity)/sumWeight)
		e.streamSelectPath(n.id, n.weight, sumWeight)
		//if e.streamInfo[n.id].pathID == 255{
			//e.streamSelectPath(n.id, n.weight, sumWeight)
		//}
	}
	return nil
}

// Tiny: not thread safe
func (e *epicScheduling) updateStreamQueue() {
	tree := e.buildTree()
	if e.sumRemainOpportunity == 0{
		e.updateOpportunity(tree[0])
	}
	
	//sort.Slice(e.streamQueue, func(i, j int) bool {
	//	ii, jj := e.streamQueue[i], e.streamQueue[j]
	//	return tree[ii].delay < tree[jj].delay
	//})
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

func (e *epicScheduling) RearrangeStreams() {
	e.Lock()
	defer e.Unlock()

	e.rearrangeStreams()
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
			pathID: 255,
			waiting: 0,
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

// Jing: TODO: 
func (e *epicScheduling) GetActiveStream() *map[protocol.StreamID]uint {
	e.RLock()
	defer e.RUnlock()
	//ret := make(map[protocol.StreamID]uint)
	//ret := make([]protocol.StreamID, len(e.streamQueue))
	// copy(ret, e.streamOpportunity)
	return &e.streamOpportunity
}

func (e *epicScheduling) GetStreamOpportunity() *map[protocol.StreamID]uint {
	return e.GetActiveStream()
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

// Jing: update opportunity
func (e *epicScheduling) UpdateOpportunity(streamID protocol.StreamID, bytes protocol.ByteCount) {
	e.Lock()
	defer e.Unlock()
	if _, ok := e.streamOpportunity[streamID]; ok {
		if e.streamOpportunity[streamID] > 0 && e.sumRemainOpportunity > 0{
			e.streamOpportunity[streamID] -= 1
			sumRemainOpportunity -= 1
		}else{
			utils.Errorf("streamOpportunity for stream %v equals zero", streamID)
		}
	}
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

func (e *epicScheduling) RefreshPath(paths []protocol.PathID) {
	e.Lock()
	defer e.Unlock()
	// Tiny: ensure paths wont be used outside
	e.paths = paths
	e.rearrangeStreams()
}
