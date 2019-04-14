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
	lowestQuantum = uint(1500)
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
	RefreshPath(bool)
	// called manually
	RearrangeStreams()
}

type streamInfo struct {
	pathID protocol.PathID
	//oppotunity uint
	waiting uint

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
	e.pathMap = make(map[protocol.PathID]*pathInfo)
	e.paths = make([]*pathInfo, 0)
	e.streamQueue = make([]protocol.StreamID, 0)
	e.activeNodes = make([]*depNode, 0)
	e.streamOpportunity = make(map[protocol.StreamID]uint)
	e.streams = make(map[protocol.StreamID]*streamInfo)
	e.sumRemainOpportunity = uint(0)
}
func (e *epicScheduling) Check(sid protocol.StreamID, pathID protocol.PathID) bool{
	utils.Infof("Check for %v on path %v", sid, pathID)
	if e.streams[sid].pathID != pathID  {
		utils.Infof("false path ID ")
		e.RearrangeStreams()
	}

	if e.streams[sid].pathID != pathID || e.streamOpportunity[sid] == 0  {
		utils.Infof("False no opportunity")
		return false
	}
	utils.Infof("True")
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
			//utils.Infof("add node %v",id)
		}
		return node
	}

	e.streamQueue = e.streamQueue[:0]
	for sid, si := range e.streams {
		si.que = true
		// there cannot be error
		//utils.Infof("build tree sid=%v", sid)
		s, _ := e.sess.streamsMap.GetOrOpenStream(sid)
		if s == nil || (si.bytes == 0 && (!s.finishedWriting.Get() || s.finSent.Get())) {
			si.que = false
		}

		if si.que {
			e.streamQueue = append(e.streamQueue, sid)
		}
		//utils.Infof("build tree sid=%v not nil", sid)

		cur := getNode(sid)
		cur.weight = float64(si.weight)
		if cur.weight <= 0 {
			cur.weight = 1
		}
		cur.size = float64(si.bytes)
		//utils.Infof("%v", cur.size)
		pa := getNode(si.parent)
		//utils.Infof("parent = %v cur = %v", si.parent, sid)
		pa.child = append(pa.child, cur)
		cur.parent = pa
		pa.sumWeight += cur.weight
	}
	root := getNode(0)
	root.dfs()
	return ret
}

// Jing: get all ActiveNodes (have data and can send data) and return sum weight
func (e *epicScheduling) getActiveNodes(n *depNode) float64{
	sumWeight := float64(0)
	//if n == nil{
	//	return 0
	//}
	utils.Infof("stream id = %v", n.id)
	utils.Infof("stream id = %v", e.streams[n.id])
	s, ok := e.streams[n.id]
	//size := e.streams[n.id].bytes

	// only clear activeNodes when it is root node
	if n.id == 0{
		e.activeNodes = make([]*depNode, 0)
	}
	
	if ok && s.bytes > 0 {
		e.activeNodes = append(e.activeNodes,n)
		//e.activeNodes.append(n)
		sumWeight += n.weight
		return n.weight
	}else{
		utils.Infof("not in streams???")
		if n.id != 0{
			utils.Infof("size = %v", s.bytes)
		}
	}
	for _, ch := range n.child {
		sumWeight += e.getActiveNodes(ch)
	}
	utils.Infof("sumWeight = %v", sumWeight)
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
	if pathSlow == nil{
		s.pathID = pathFast.pathID
		utils.Debugf("path 2 %v not exists")
		return nil
	}

	// Jing: TODO: if fast path is available 
	fastAvailable := e.sess.scheduler.PathAvailable(pathFast)
	if fastAvailable {
		s.pathID = pathFast.pathID
		return nil
	}

	slowAvailable := e.sess.scheduler.PathAvailable(pathSlow)
	if slowAvailable == false{
		return nil
	}
	// Jing: TODO: otherwise
	rttFast := float64(pathFast.rttStats.SmoothedRTT())
	rttSlow := float64(pathSlow.rttStats.SmoothedRTT())
	sigmaFast := float64(pathFast.rttStats.MeanDeviation())
	sigmaSlow := float64(pathSlow.rttStats.MeanDeviation())
	utils.Infof("rttSlow=%v, rttFast=%v", rttSlow,rttFast)

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
	k := float64(bytesOpportunity) * ( g * (left - 1) + left )
	return protocol.ByteCount(k)
} 

func (e *epicScheduling) updateOpportunity(n *depNode) error{
	e.Lock()
	defer e.Unlock()
	utils.Debugf("newly created opportunity tree")
	sumWeight := e.getActiveNodes(n)
	e.streamOpportunity = make(map[protocol.StreamID]uint)
	e.sumRemainOpportunity = 0
	for _, n := range e.activeNodes {
		utils.Infof("one node stream id = %v for loop!", n.id)
		e.streamOpportunity[n.id] = uint(n.weight * float64(maxOpportunity)/sumWeight)
		utils.Infof("select path!")

		// Jing: TODO: whether to change path ID
		//if e.streams[n.id].pathID == 255{
		e.streamSelectPath(n.id, n.weight, sumWeight)
		//}
		e.sumRemainOpportunity += e.streamOpportunity[n.id]
		//if e.streamInfo[n.id].pathID == 255{
			//e.streamSelectPath(n.id, n.weight, sumWeight)
		//}
	}
	return nil
}

// Tiny: not thread safe
func (e *epicScheduling) updateStreamQueue() {
	tree := e.buildTree()
	//if e.sumRemainOpportunity == 0{
	e.updateOpportunity(tree[0])
	//}
	
	//sort.Slice(e.streamQueue, func(i, j int) bool {
	//	ii, jj := e.streamQueue[i], e.streamQueue[j]
	//	return tree[ii].delay < tree[jj].delay
	//})
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
	e.updateStreamQueue()
	return
}
func (e *epicScheduling) old_rearrangeStreams() {
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
			pathID: 255,
			waiting: 0,
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

// Jing: TODO: 
func (e *epicScheduling) GetActiveStream() *map[protocol.StreamID]uint {
	e.RLock()
	defer e.RUnlock()
	if len(e.streamOpportunity) == 0{
		e.rearrangeStreams()
	}
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
	if _, ok := e.streamOpportunity[streamID]; ok {
		if e.streamOpportunity[streamID] > 0 && e.sumRemainOpportunity > 0{
			utils.Infof("stream %v opportunity = %v before sending packets", streamID,e.sumRemainOpportunity)
			e.streamOpportunity[streamID] -= 1
			e.sumRemainOpportunity -= 1
			if e.streams[streamID].bytes == 0 {
				e.sumRemainOpportunity -= e.streamOpportunity[streamID]
				e.streamOpportunity[streamID] = 0
			}
			if e.sumRemainOpportunity == 0{
				e.updateStreamQueue()
			}
		}else{
			utils.Errorf("streamOpportunity for stream %v equals zero", streamID)
		}
	} else {
		utils.Errorf("stream not in opportunity map")
	}
}

// Tiny: we rearrange every time, i dont know if this will work
func (e *epicScheduling) ConsumePathBytes(pathID protocol.PathID, streamID protocol.StreamID, bytes protocol.ByteCount) {
	e.Lock()
	defer e.Unlock()
	if s, ok := e.streams[streamID]; ok {
		s.bytes -= bytes
		//if b, ok := s.alloc[pathID]; ok && s.bytes >= bytes && b >= bytes {
		//	utils.Debugf("stream %v consume %v from path %v", streamID, bytes, pathID)
		//	s.bytes -= bytes
			//s.alloc[pathID] -= bytes
			// e.rearrangeStreams()
		return
		//}
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
