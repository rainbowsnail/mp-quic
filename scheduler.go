package quic

import (
	"sort"
	"time"

	"github.com/lucas-clemente/quic-go/ackhandler"
	"github.com/lucas-clemente/quic-go/internal/protocol"
	"github.com/lucas-clemente/quic-go/internal/utils"
	"github.com/lucas-clemente/quic-go/internal/wire"
)
const (
	ackPathChangeTimer = 3 * time.Second
)
type scheduler struct {
	// XXX Currently round-robin based, inspired from MPTCP scheduler

	quotas map[protocol.PathID]uint

	handler ScheduleHandler

	lastAckDupTime 	time.Time
	delay 				map[protocol.PathID]time.Duration
	shouldSendDupAck	map[protocol.PathID]bool

	dupAckFrame			*wire.AckFrame
	lastDupAckTime		time.Time
	timer           	*utils.Timer
	shouldInstigateDupAck	utils.AtomicBool

}

func (sch *scheduler) setup(s *session) {
	sch.quotas = make(map[protocol.PathID]uint)
	sch.handler = NewEpicScheduling(s)

	sch.delay = make(map[protocol.PathID]time.Duration)
	sch.shouldSendDupAck = make(map[protocol.PathID]bool)
	sch.dupAckFrame = nil
	sch.shouldInstigateDupAck.Set(false)
	now := time.Now()
	sch.lastDupAckTime = now
	sch.timer = utils.NewTimer()
	go sch.run()
}

func (sch *scheduler) run() {
	// XXX (QDC): relay everything to the session, maybe not the most efficient
//runLoop:

	sch.maybeResetTimer()
	for {

		select {
		case <-sch.timer.Chan():
			sch.timer.SetRead()
			sch.shouldInstigateDupAck.Set(true)
			utils.Infof("scheduler timer timeout")

			now := time.Now()
			sch.lastDupAckTime = now
			sch.maybeResetTimer()
		}
	}
}

func (sch *scheduler) maybeResetTimer() {
	deadline := sch.lastDupAckTime.Add(ackPathChangeTimer)
	deadline = utils.MaxTime(deadline, time.Now())
	sch.timer.Reset(deadline)
}

func (sch *scheduler) getRetransmission(s *session) (hasRetransmission bool, retransmitPacket *ackhandler.Packet, pth *path) {
	// check for retransmissions first
	for {
		// TODO add ability to reinject on another path
		// XXX We need to check on ALL paths if any packet should be first retransmitted
		s.pathsLock.RLock()
	retransmitLoop:
		for _, pthTmp := range s.paths {
			retransmitPacket = pthTmp.sentPacketHandler.DequeuePacketForRetransmission()
			if retransmitPacket != nil {
				pth = pthTmp
				break retransmitLoop
			}
		}
		s.pathsLock.RUnlock()
		if retransmitPacket == nil {
			break
		}
		hasRetransmission = true

		if retransmitPacket.EncryptionLevel != protocol.EncryptionForwardSecure {
			if s.handshakeComplete {
				// Don't retransmit handshake packets when the handshake is complete
				continue
			}
			utils.Debugf("\tDequeueing handshake retransmission for packet 0x%x", retransmitPacket.PacketNumber)
			return
		}
		utils.Debugf("\tDequeueing retransmission of packet 0x%x from path %d", retransmitPacket.PacketNumber, pth.pathID)
		// resend the frames that were in the packet
		for _, frame := range retransmitPacket.GetFramesForRetransmission() {
			switch f := frame.(type) {
			case *wire.StreamFrame:
				s.streamFramer.AddFrameForRetransmission(f)
			case *wire.WindowUpdateFrame:
				// only retransmit WindowUpdates if the stream is not yet closed and the we haven't sent another WindowUpdate with a higher ByteOffset for the stream
				// XXX Should it be adapted to multiple paths?
				currentOffset, err := s.flowControlManager.GetReceiveWindow(f.StreamID)
				if err == nil && f.ByteOffset >= currentOffset {
					s.packer.QueueControlFrame(f, pth)
				}
			case *wire.PathsFrame:
				// Schedule a new PATHS frame to send
				s.schedulePathsFrame()
			default:
				s.packer.QueueControlFrame(frame, pth)
			}
		}
	}
	return
}

func (sch *scheduler) selectPathRoundRobin(s *session, hasRetransmission bool, hasStreamRetransmission bool, fromPth *path) *path {
	// Tiny: these code has no use
	// if sch.quotas == nil {
	// 	sch.setup()
	// }

	// XXX Avoid using PathID 0 if there is more than 1 path
	if len(s.paths) <= 1 {
		if !hasRetransmission && !s.paths[protocol.InitialPathID].SendingAllowed() {
			return nil
		}
		return s.paths[protocol.InitialPathID]
	}

	// TODO cope with decreasing number of paths (needed?)
	var selectedPath *path
	var lowerQuota, currentQuota uint
	var ok bool

	// Max possible value for lowerQuota at the beginning
	lowerQuota = ^uint(0)

pathLoop:
	for pathID, pth := range s.paths {
		// Don't block path usage if we retransmit, even on another path
		if !hasRetransmission && !pth.SendingAllowed() {
			continue pathLoop
		}

		// If this path is potentially failed, do no consider it for sending
		if pth.potentiallyFailed.Get() {
			continue pathLoop
		}

		// XXX Prevent using initial pathID if multiple paths
		if pathID == protocol.InitialPathID {
			continue pathLoop
		}

		// Tiny: if the path cannot send any more frame, ignore
		// _, limit := sch.handler.GetPathScheduling(pathID)
		// if limit == 0 {
		// 	continue pathLoop
		// }

		currentQuota, ok = sch.quotas[pathID]
		if !ok {
			sch.quotas[pathID] = 0
			currentQuota = 0
		}

		if currentQuota < lowerQuota {
			selectedPath = pth
			lowerQuota = currentQuota
		}
	}

	return selectedPath

}

func (sch *scheduler) selectPathLowLatency(s *session, hasRetransmission bool, hasStreamRetransmission bool, fromPth *path) *path {
	// XXX Avoid using PathID 0 if there is more than 1 path
	if len(s.paths) <= 1 {
		if !hasRetransmission && !s.paths[protocol.InitialPathID].SendingAllowed() {
			return nil
		}
		return s.paths[protocol.InitialPathID]
	}

	// FIXME Only works at the beginning... Cope with new paths during the connection
	if hasRetransmission && hasStreamRetransmission && fromPth.rttStats.SmoothedRTT() == 0 {
		// Is there any other path with a lower number of packet sent?
		currentQuota := sch.quotas[fromPth.pathID]
		for pathID, pth := range s.paths {
			if pathID == protocol.InitialPathID || pathID == fromPth.pathID {
				continue
			}
			// The congestion window was checked when duplicating the packet
			if sch.quotas[pathID] < currentQuota {
				return pth
			}
		}
	}

	var selectedPath *path
	var lowerRTT time.Duration
	var currentRTT time.Duration
	selectedPathID := protocol.PathID(255)

pathLoop:
	for pathID, pth := range s.paths {
		// Don't block path usage if we retransmit, even on another path
		if !hasRetransmission && !pth.SendingAllowed() {
			continue pathLoop
		}

		// If this path is potentially failed, do not consider it for sending
		if pth.potentiallyFailed.Get() {
			continue pathLoop
		}

		// XXX Prevent using initial pathID if multiple paths
		if pathID == protocol.InitialPathID {
			continue pathLoop
		}

		currentRTT = pth.rttStats.SmoothedRTT()

		// Prefer staying single-path if not blocked by current path
		// Don't consider this sample if the smoothed RTT is 0
		if lowerRTT != 0 && currentRTT == 0 {
			continue pathLoop
		}

		// Case if we have multiple paths unprobed
		if currentRTT == 0 {
			currentQuota, ok := sch.quotas[pathID]
			if !ok {
				sch.quotas[pathID] = 0
				currentQuota = 0
			}
			lowerQuota, _ := sch.quotas[selectedPathID]
			if selectedPath != nil && currentQuota > lowerQuota {
				continue pathLoop
			}
		}

		if currentRTT != 0 && lowerRTT != 0 && selectedPath != nil && currentRTT >= lowerRTT {
			continue pathLoop
		}

		// Update
		lowerRTT = currentRTT
		selectedPath = pth
		selectedPathID = pathID
	}

	return selectedPath
}

// Lock of s.paths must be held
func (sch *scheduler) selectPath(s *session, hasRetransmission bool, hasStreamRetransmission bool, fromPth *path) *path {
	// XXX Currently round-robin
	// TODO select the right scheduler dynamically
	// return sch.selectPathLowLatency(s, hasRetransmission, hasStreamRetransmission, fromPth)
	return sch.selectPathRoundRobin(s, hasRetransmission, hasStreamRetransmission, fromPth)
}

// Tiny: helper function to decide if a path is available
func pathAvailable(pth *path, hasRetransmission bool) bool {
	return pth != nil && pth.open.Get() && (hasRetransmission || pth.SendingAllowed())
}

// Tiny: filter & sort paths, locks
func (sch *scheduler) selectPaths(s *session, hasRetransmission bool) []*path {
	s.pathsLock.RLock()
	defer s.pathsLock.RUnlock()

	if len(s.paths) <= 1 {
		pth := s.paths[protocol.InitialPathID]
		if pathAvailable(pth, hasRetransmission) {
			return []*path{pth}
		}
		return nil
	}

	var ret []*path
	for pid, pth := range s.paths {
		if pid == protocol.InitialPathID || !pathAvailable(pth, hasRetransmission) {
			utils.Infof("not available %v", pid)
			continue
		}
		ret = append(ret, pth)
	}

	// Tiny: TODO we dont know how to sort now
	sort.Slice(ret, func(i, j int) bool {
		ii, jj := ret[i], ret[j]
		ri, rj := ii.rttStats.SmoothedRTT(), jj.rttStats.SmoothedRTT()
		if ri == 0 && rj == 0 {
			return sch.quotas[ii.pathID] < sch.quotas[jj.pathID]
		}
		if ri == 0 || rj == 0 {
			return ri > rj
		}
		return ri < rj
	})

	for _, pth := range ret {
		utils.Infof("select path %v", pth.pathID)
	}

	return ret
}

// Lock of s.paths must be free (in case of log print)
func (sch *scheduler) performPacketSending(s *session, windowUpdateFrames []*wire.WindowUpdateFrame, pth *path) (*ackhandler.Packet, bool, error) {
	// add a retransmittable frame
	if pth.sentPacketHandler.ShouldSendRetransmittablePacket() {
		s.packer.QueueControlFrame(&wire.PingFrame{}, pth)
	}
	packet, err := s.packer.PackPacket(pth)
	if err != nil || packet == nil {
		return nil, false, err
	}
	if err = s.sendPackedPacket(packet, pth); err != nil {
		return nil, false, err
	}

	// send every window update twice
	for _, f := range windowUpdateFrames {
		s.packer.QueueControlFrame(f, pth)
	}

	// Packet sent, so update its quota
	sch.quotas[pth.pathID]++

	// Provide some logging if it is the last packet
	for _, frame := range packet.frames {
		switch frame := frame.(type) {
		case *wire.StreamFrame:
			if frame.FinBit {
				// Last packet to send on the stream, print stats
				s.pathsLock.RLock()
				utils.Infof("Info for stream %x of %x", frame.StreamID, s.connectionID)
				for pathID, pth := range s.paths {
					sntPkts, sntRetrans, sntLost := pth.sentPacketHandler.GetStatistics()
					rcvPkts := pth.receivedPacketHandler.GetStatistics()
					utils.Infof("Path %x: sent %d retrans %d lost %d; rcv %d rtt %v", pathID, sntPkts, sntRetrans, sntLost, rcvPkts, pth.rttStats.SmoothedRTT())
				}
				s.pathsLock.RUnlock()
			}
		default:
		}
	}

	pkt := &ackhandler.Packet{
		PacketNumber:    packet.number,
		Frames:          packet.frames,
		Length:          protocol.ByteCount(len(packet.raw)),
		EncryptionLevel: packet.encryptionLevel,
	}

	return pkt, true, nil
}

// Lock of s.paths must be free
func (sch *scheduler) ackRemainingPaths(s *session, totalWindowUpdateFrames []*wire.WindowUpdateFrame) error {
	// Either we run out of data, or CWIN of usable paths are full
	// Send ACKs on paths not yet used, if needed. Either we have no data to send and
	// it will be a pure ACK, or we will have data in it, but the CWIN should then
	// not be an issue.
	s.pathsLock.RLock()
	defer s.pathsLock.RUnlock()
	// get WindowUpdate frames
	// this call triggers the flow controller to increase the flow control windows, if necessary
	windowUpdateFrames := totalWindowUpdateFrames
	if len(windowUpdateFrames) == 0 {
		windowUpdateFrames = s.getWindowUpdateFrames(s.peerBlocked)
	}
	for _, pthTmp := range s.paths {
		hasAck := false
		//ackTmp := pthTmp.GetAckFrame()
		for _, tmpPath := range s.paths{
			ackTmp := tmpPath.GetAckFrameOnPath(pthTmp.pathID)
			// TODO-Jing: ack packets on other path and dup ack 
			if ackTmp != nil {
				hasAck = true
				utils.Infof("Ack send on %x", pthTmp)
				s.packer.QueueControlFrame(ackTmp, pthTmp)
			}
		}
		if shouldSendDupAckOnPath, ok := sch.shouldSendDupAck[pthTmp.pathID]; ok {
			if pthTmp.pathID != protocol.InitialPathID {
				if shouldSendDupAckOnPath {
					hasAck = true
					utils.Infof("DupAck send on %x", pthTmp.pathID)
					if sch.dupAckFrame == nil {
						utils.Infof("DupAck is gone!!!!")
					}
					s.packer.QueueControlFrame(sch.dupAckFrame, pthTmp)
					sch.shouldSendDupAck[pthTmp.pathID] = false
				}
			}
		}
		
		for _, wuf := range windowUpdateFrames {
			s.packer.QueueControlFrame(wuf, pthTmp)
		}
		if hasAck || len(windowUpdateFrames) > 0 {
			if pthTmp.pathID == protocol.InitialPathID && !hasAck {
				continue
			}
			swf := pthTmp.GetStopWaitingFrame(false)
			if swf != nil {
				s.packer.QueueControlFrame(swf, pthTmp)
			}
			//s.packer.QueueControlFrame(ackTmp, pthTmp)
			// XXX (QDC) should we instead call PackPacket to provides WUFs?
			var packet *packedPacket
			var err error
			if hasAck {
				// Avoid internal error bug
				packet, err = s.packer.PackAckPacket(pthTmp)
			} else {
				packet, err = s.packer.PackPacket(pthTmp)
			}
			if err != nil {
				return err
			}
			err = s.sendPackedPacket(packet, pthTmp)
			if err != nil {
				return err
			}
		}
	}
	s.peerBlocked = false
	return nil
}

// Tiny: called when stream issue a write
func (sch *scheduler) allocateStream(s *session, str *stream, bytes protocol.ByteCount) {
	if bytes <= 0 && (!str.finishedWriting.Get() || str.finSent.Get()) {
		return
	}
	// utils.Infof("stream %v write %v bytes", str.streamID, bytes)
	sch.handler.AddStreamByte(str.streamID, bytes)
}

func (sch *scheduler) sendPacket(s *session) error {
	// var pth *path

	// Update leastUnacked value of paths
	s.pathsLock.RLock()
	for _, pthTmp := range s.paths {
		pthTmp.SetLeastUnacked(pthTmp.sentPacketHandler.GetLeastUnacked())
	}
	s.pathsLock.RUnlock()

	// get WindowUpdate frames
	// this call triggers the flow controller to increase the flow control windows, if necessary
	windowUpdateFrames := s.getWindowUpdateFrames(false)
	// Tiny: WTF??? pth must be nil at this moment
	for _, wuf := range windowUpdateFrames {
		s.packer.QueueControlFrame(wuf, nil)
	}

	// Tiny: it's ugly now, but i cannot think a better way

	// We first check for retransmissions
	hasRetransmission, retransmitHandshakePacket, fromPth := sch.getRetransmission(s)
	// XXX There might still be some stream frames to be retransmitted
	hasStreamRetransmission := s.streamFramer.HasFramesForRetransmission()

	// Tiny: get all available paths
	pths := sch.selectPaths(s, hasRetransmission)
	// Tiny: i'm confused with the logic of WUF frames and the purpose of ackRemainingPaths
	//		 but still keep the logic
	leastSent := false
	for i := 0; i < len(pths); {
		pth := pths[i]

		// If we have an handshake packet retransmission, do it directly
		if hasRetransmission && retransmitHandshakePacket != nil {
			s.packer.QueueControlFrame(pth.sentPacketHandler.GetStopWaitingFrame(true), pth)
			packet, err := s.packer.PackHandshakeRetransmission(retransmitHandshakePacket, pth)
			if err != nil {
				return err
			}
			if err = s.sendPackedPacket(packet, pth); err != nil {
				return err
			}
		} else {
			// XXX Some automatic ACK generation should be done someway
			
			// Jing: find ack frame that should be sent on current path 
			var ack *wire.AckFrame
			hasAck := false
			//ack = pth.GetAckFrame()
			for _, tmpPath := range s.paths{
				ack = tmpPath.GetAckFrameOnPath(pth.pathID)
				// TODO-Jing: ack packets on other path and dup ack 
				if s.perspective == protocol.PerspectiveClient {
				
					if ack != nil && sch.shouldInstigateDupAck.Get() {
						sch.shouldInstigateDupAck.Set(false)
						sch.dupAckFrame = ack
					
						for pathID, p := range s.paths{
							if p != pth {
								utils.Infof("shouldSendDupAck on Path %x", pathID)
								sch.shouldSendDupAck[pathID] = true
							}
						}
					}
				}
				if ack != nil {
					utils.Infof("Ack send on %x", pth.pathID)
					s.packer.QueueControlFrame(ack, pth)
					hasAck = true
				}
				
			}
			if s.perspective == protocol.PerspectiveClient {
				if shouldSendDupAckOnPath, ok := sch.shouldSendDupAck[pth.pathID]; ok {
					if pth.pathID != protocol.InitialPathID {
						if shouldSendDupAckOnPath {
							utils.Infof("DupAck send on %x", pth.pathID)
							s.packer.QueueControlFrame(sch.dupAckFrame, pth)
							hasAck = true
							sch.shouldSendDupAck[pth.pathID] = false
						}
					}
				}
			}
			if hasAck || hasStreamRetransmission {
				swf := pth.sentPacketHandler.GetStopWaitingFrame(hasStreamRetransmission)
				if swf != nil {
					s.packer.QueueControlFrame(swf, pth)
				}
			}
			// Also add ACK RETURN PATHS frames, if any
			if s.perspective == protocol.PerspectiveServer {
				for arpf := s.streamFramer.PopAckReturnPathsFrame(s); arpf != nil; arpf = s.streamFramer.PopAckReturnPathsFrame(s) {
					utils.Infof("ACK RETURN PATHS frames send on %x", pth.pathID)
					s.packer.QueueControlFrame(arpf, pth)
				}
			}
		// Also add CLOSE_PATH frames, if any
		for cpf := s.streamFramer.PopClosePathFrame(); cpf != nil; cpf = s.streamFramer.PopClosePathFrame() {
			s.packer.QueueControlFrame(cpf, pth)
		}

		// Also add ADD ADDRESS frames, if any
		for aaf := s.streamFramer.PopAddAddressFrame(); aaf != nil; aaf = s.streamFramer.PopAddAddressFrame() {
			s.packer.QueueControlFrame(aaf, pth)
		}

		// Also add CLOSE_PATH frames, if any
		for cpf := s.streamFramer.PopClosePathFrame(); cpf != nil; cpf = s.streamFramer.PopClosePathFrame() {
			s.packer.QueueControlFrame(cpf, pth)
		}


			// Also add PATHS frames, if any
			for pf := s.streamFramer.PopPathsFrame(); pf != nil; pf = s.streamFramer.PopPathsFrame() {
				s.packer.QueueControlFrame(pf, pth)
			}

			_, sent, err := sch.performPacketSending(s, windowUpdateFrames, pth)
			if err != nil {
				return err
			}
			windowUpdateFrames = nil
			if !sent {
				utils.Debugf("empty packet on %v, switch to next", pth.pathID)
				// Tiny: empty packet, we switch to next path
				i++
			} else {
				leastSent = true
			}

			// Tiny: we remove the duplicate sending for it cause serious bug
			// } else if pth.rttStats.SmoothedRTT() == 0 {
			// 	// Duplicate traffic when it was sent on an unknown performing path
			// 	// FIXME adapt for new paths coming during the connection
			// 	currentQuota := sch.quotas[pth.pathID]
			// 	// Was the packet duplicated on all potential paths?
			// duplicateLoop:
			// 	for pathID, tmpPth := range s.paths {
			// 		if pathID == protocol.InitialPathID || pathID == pth.pathID {
			// 			continue
			// 		}
			// 		if sch.quotas[pathID] < currentQuota && tmpPth.sentPacketHandler.SendingAllowed() {
			// 			// Duplicate it
			// 			// Tiny: WTF??
			// 			tmpPth.sentPacketHandler.DuplicatePacket(pkt)
			// 			break duplicateLoop
			// 		}
			// 	}
			// }

			// And try pinging on potentially failed paths
			if fromPth != nil && fromPth.potentiallyFailed.Get() {
				err = s.sendPing(fromPth)
				if err != nil {
					return err
				}
			}
		}

		// Tiny: copy
		hasRetransmission, retransmitHandshakePacket, fromPth = sch.getRetransmission(s)
		hasStreamRetransmission = s.streamFramer.HasFramesForRetransmission()
		// Tiny: if this path is not available switch to next
		if i < len(pths) && !pathAvailable(pths[i], hasRetransmission) {
			i++
		}
		//return sch.ackRemainingPaths(s, windowUpdateFrames)
	}
	if leastSent {
		sch.handler.RearrangeStreams()
	}
	windowUpdateFrames = s.getWindowUpdateFrames(false)
	return sch.ackRemainingPaths(s, windowUpdateFrames)
}
