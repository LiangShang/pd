// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"math"
	"math/rand"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
)

func init() {
	schedule.RegisterScheduler("hotRegion", func(opt schedule.Options, args []string) (schedule.Scheduler, error) {
		return newBalanceHotRegionScheduler(opt), nil
	})
}

const (
	hotRegionLimitFactor      = 0.75
	storeHotRegionsDefaultLen = 100
	hotRegionScheduleFactor   = 0.9
)

// BalanceType : the perspective of balance
type BalanceType int

const (
	byPeer BalanceType = iota
	byLeader
)

type balanceHotRegionScheduler struct {
	sync.RWMutex
	opt   schedule.Options
	limit uint64

	// store id -> hot regions statistics as the role of replica
	statisticsAsPeer map[uint64]*core.HotRegionsStat
	// store id -> hot regions statistics as the role of leader
	statisticsAsLeader map[uint64]*core.HotRegionsStat
	r                  *rand.Rand
}

// newBalanceHotRegionScheduler creates a scheduler that keeps hot regions on
// each stores balanced.
func newBalanceHotRegionScheduler(opt schedule.Options) schedule.Scheduler {
	return &balanceHotRegionScheduler{
		opt:                opt,
		limit:              1,
		statisticsAsPeer:   make(map[uint64]*core.HotRegionsStat),
		statisticsAsLeader: make(map[uint64]*core.HotRegionsStat),
		r:                  rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (h *balanceHotRegionScheduler) GetName() string {
	return "balance-hot-region-scheduler"
}

func (h *balanceHotRegionScheduler) GetResourceKind() core.ResourceKind {
	return core.PriorityKind
}

func (h *balanceHotRegionScheduler) GetResourceLimit() uint64 {
	return h.limit
}

func (h *balanceHotRegionScheduler) Prepare(cluster schedule.Cluster) error { return nil }

func (h *balanceHotRegionScheduler) Cleanup(cluster schedule.Cluster) {}

func (h *balanceHotRegionScheduler) Schedule(cluster schedule.Cluster) schedule.Operator {
	schedulerCounter.WithLabelValues(h.GetName(), "schedule").Inc()
	h.calcScore(cluster)

	// balance by peer
	srcRegion, srcPeer, destPeer := h.balanceByPeer(cluster)
	if srcRegion != nil {
		schedulerCounter.WithLabelValues(h.GetName(), "move_peer").Inc()
		return schedule.CreateMovePeerOperator(srcRegion, core.PriorityKind, srcPeer, destPeer)
	}

	// balance by leader
	srcRegion, newLeader := h.balanceByLeader(cluster)
	if srcRegion != nil {
		schedulerCounter.WithLabelValues(h.GetName(), "move_leader").Inc()
		return newPriorityTransferLeader(srcRegion, newLeader)
	}

	schedulerCounter.WithLabelValues(h.GetName(), "skip").Inc()
	return nil
}

func (h *balanceHotRegionScheduler) calcScore(cluster schedule.Cluster) {
	h.Lock()
	defer h.Unlock()

	h.statisticsAsPeer = make(map[uint64]*core.HotRegionsStat)
	h.statisticsAsLeader = make(map[uint64]*core.HotRegionsStat)
	items := cluster.RegionWriteStats()
	for _, r := range items {
		if r.HotDegree < h.opt.GetHotRegionLowThreshold() {
			continue
		}

		regionInfo := cluster.GetRegion(r.RegionID)
		leaderStoreID := regionInfo.Leader.GetStoreId()
		storeIDs := regionInfo.GetStoreIds()
		for storeID := range storeIDs {
			peerStat, ok := h.statisticsAsPeer[storeID]
			if !ok {
				peerStat = &core.HotRegionsStat{
					RegionsStat: make(core.RegionsStat, 0, storeHotRegionsDefaultLen),
				}
				h.statisticsAsPeer[storeID] = peerStat
			}
			leaderStat, ok := h.statisticsAsLeader[storeID]
			if !ok {
				leaderStat = &core.HotRegionsStat{
					RegionsStat: make(core.RegionsStat, 0, storeHotRegionsDefaultLen),
				}
				h.statisticsAsLeader[storeID] = leaderStat
			}

			stat := core.RegionStat{
				RegionID:       r.RegionID,
				WrittenBytes:   r.WrittenBytes,
				HotDegree:      r.HotDegree,
				LastUpdateTime: r.LastUpdateTime,
				StoreID:        storeID,
				AntiCount:      r.AntiCount,
				Version:        r.Version,
			}
			peerStat.WrittenBytes += r.WrittenBytes
			peerStat.RegionsCount++
			peerStat.RegionsStat = append(peerStat.RegionsStat, stat)

			if storeID == leaderStoreID {
				leaderStat.WrittenBytes += r.WrittenBytes
				leaderStat.RegionsCount++
				leaderStat.RegionsStat = append(leaderStat.RegionsStat, stat)
			}
		}
	}
}

func (h *balanceHotRegionScheduler) balanceByPeer(cluster schedule.Cluster) (*core.RegionInfo, *metapb.Peer, *metapb.Peer) {
	var (
		maxWrittenBytes        uint64
		srcStoreID             uint64
		maxHotStoreRegionCount int
	)

	// get the srcStoreId
	for storeID, statistics := range h.statisticsAsPeer {
		count, writtenBytes := statistics.RegionsStat.Len(), statistics.WrittenBytes
		if count >= 2 && (count > maxHotStoreRegionCount || (count == maxHotStoreRegionCount && writtenBytes > maxWrittenBytes)) {
			maxHotStoreRegionCount = count
			maxWrittenBytes = writtenBytes
			srcStoreID = storeID
		}
	}
	if srcStoreID == 0 {
		return nil, nil, nil
	}

	stores := cluster.GetStores()
	var destStoreID uint64
	for _, i := range h.r.Perm(h.statisticsAsPeer[srcStoreID].RegionsStat.Len()) {
		rs := h.statisticsAsPeer[srcStoreID].RegionsStat[i]
		srcRegion := cluster.GetRegion(rs.RegionID)
		if len(srcRegion.DownPeers) != 0 || len(srcRegion.PendingPeers) != 0 {
			continue
		}

		filters := []schedule.Filter{
			schedule.NewExcludedFilter(srcRegion.GetStoreIds(), srcRegion.GetStoreIds()),
			schedule.NewDistinctScoreFilter(h.opt.GetLocationLabels(), stores, cluster.GetLeaderStore(srcRegion)),
			schedule.NewStateFilter(h.opt),
			schedule.NewStorageThresholdFilter(h.opt),
		}
		destStoreIDs := make([]uint64, 0, len(stores))
		for _, store := range stores {
			if schedule.FilterTarget(store, filters) {
				continue
			}
			destStoreIDs = append(destStoreIDs, store.GetId())
		}

		destStoreID = h.selectDestStoreByPeer(destStoreIDs, srcRegion, srcStoreID)
		if destStoreID != 0 {
			srcRegion.WrittenBytes = rs.WrittenBytes
			h.adjustBalanceLimit(srcStoreID, byPeer)

			var srcPeer *metapb.Peer
			for _, peer := range srcRegion.GetPeers() {
				if peer.GetStoreId() == srcStoreID {
					srcPeer = peer
					break
				}
			}

			if srcPeer == nil {
				return nil, nil, nil
			}

			destPeer, err := cluster.AllocPeer(destStoreID)
			if err != nil {
				log.Errorf("failed to allocate peer: %v", err)
				return nil, nil, nil
			}

			return srcRegion, srcPeer, destPeer
		}
	}

	return nil, nil, nil
}

func (h *balanceHotRegionScheduler) selectDestStoreByPeer(candidateStoreIDs []uint64, srcRegion *core.RegionInfo, srcStoreID uint64) uint64 {
	sr := h.statisticsAsPeer[srcStoreID]
	srcWrittenBytes := sr.WrittenBytes
	srcHotRegionsCount := sr.RegionsStat.Len()

	var (
		destStoreID     uint64
		minWrittenBytes uint64 = math.MaxUint64
	)
	minRegionsCount := int(math.MaxInt32)
	for _, storeID := range candidateStoreIDs {
		if s, ok := h.statisticsAsPeer[storeID]; ok {
			if srcHotRegionsCount-s.RegionsStat.Len() > 1 && minRegionsCount > s.RegionsStat.Len() {
				destStoreID = storeID
				minWrittenBytes = s.WrittenBytes
				minRegionsCount = s.RegionsStat.Len()
				continue
			}
			if minRegionsCount == s.RegionsStat.Len() && minWrittenBytes > s.WrittenBytes &&
				uint64(float64(srcWrittenBytes)*hotRegionScheduleFactor) > s.WrittenBytes+2*srcRegion.WrittenBytes {
				minWrittenBytes = s.WrittenBytes
				destStoreID = storeID
			}
		} else {
			destStoreID = storeID
			break
		}
	}
	return destStoreID
}

func (h *balanceHotRegionScheduler) adjustBalanceLimit(storeID uint64, t BalanceType) {
	var srcStatistics *core.HotRegionsStat
	var allStatistics map[uint64]*core.HotRegionsStat
	switch t {
	case byPeer:
		srcStatistics = h.statisticsAsPeer[storeID]
		allStatistics = h.statisticsAsPeer
	case byLeader:
		srcStatistics = h.statisticsAsLeader[storeID]
		allStatistics = h.statisticsAsLeader
	}

	var hotRegionTotalCount float64
	for _, m := range allStatistics {
		hotRegionTotalCount += float64(m.RegionsStat.Len())
	}

	avgRegionCount := hotRegionTotalCount / float64(len(allStatistics))
	// Multiplied by hotRegionLimitFactor to avoid transfer back and forth
	limit := uint64((float64(srcStatistics.RegionsStat.Len()) - avgRegionCount) * hotRegionLimitFactor)
	h.limit = maxUint64(1, limit)
}

func (h *balanceHotRegionScheduler) balanceByLeader(cluster schedule.Cluster) (*core.RegionInfo, *metapb.Peer) {
	var (
		maxWrittenBytes        uint64
		srcStoreID             uint64
		maxHotStoreRegionCount int
	)

	// select srcStoreId by leader
	for storeID, statistics := range h.statisticsAsLeader {
		if statistics.RegionsStat.Len() < 2 {
			continue
		}

		if maxHotStoreRegionCount < statistics.RegionsStat.Len() {
			maxHotStoreRegionCount = statistics.RegionsStat.Len()
			maxWrittenBytes = statistics.WrittenBytes
			srcStoreID = storeID
			continue
		}

		if maxHotStoreRegionCount == statistics.RegionsStat.Len() && maxWrittenBytes < statistics.WrittenBytes {
			maxWrittenBytes = statistics.WrittenBytes
			srcStoreID = storeID
		}
	}
	if srcStoreID == 0 {
		return nil, nil
	}

	// select destPeer
	for _, i := range h.r.Perm(h.statisticsAsLeader[srcStoreID].RegionsStat.Len()) {
		rs := h.statisticsAsLeader[srcStoreID].RegionsStat[i]
		srcRegion := cluster.GetRegion(rs.RegionID)
		if len(srcRegion.DownPeers) != 0 || len(srcRegion.PendingPeers) != 0 {
			continue
		}

		destPeer := h.selectDestStoreByLeader(srcRegion)
		if destPeer != nil {
			h.adjustBalanceLimit(srcStoreID, byLeader)
			return srcRegion, destPeer
		}
	}
	return nil, nil
}

func (h *balanceHotRegionScheduler) selectDestStoreByLeader(srcRegion *core.RegionInfo) *metapb.Peer {
	sr := h.statisticsAsLeader[srcRegion.Leader.GetStoreId()]
	srcWrittenBytes := sr.WrittenBytes
	srcHotRegionsCount := sr.RegionsStat.Len()

	var (
		destPeer        *metapb.Peer
		minWrittenBytes uint64 = math.MaxUint64
	)
	minRegionsCount := int(math.MaxInt32)
	for storeID, peer := range srcRegion.GetFollowers() {
		if s, ok := h.statisticsAsLeader[storeID]; ok {
			if srcHotRegionsCount-s.RegionsStat.Len() > 1 && minRegionsCount > s.RegionsStat.Len() {
				destPeer = peer
				minWrittenBytes = s.WrittenBytes
				minRegionsCount = s.RegionsStat.Len()
				continue
			}
			if minRegionsCount == s.RegionsStat.Len() && minWrittenBytes > s.WrittenBytes &&
				uint64(float64(srcWrittenBytes)*hotRegionScheduleFactor) > s.WrittenBytes+2*srcRegion.WrittenBytes {
				minWrittenBytes = s.WrittenBytes
				destPeer = peer
			}
		} else {
			destPeer = peer
			break
		}
	}
	return destPeer
}

func newPriorityTransferLeader(region *core.RegionInfo, newLeader *metapb.Peer) schedule.Operator {
	transferLeader := schedule.NewTransferLeaderOperator(region.GetId(), region.Leader, newLeader)
	return schedule.NewRegionOperator(region, core.PriorityKind, transferLeader)
}

func (h *balanceHotRegionScheduler) GetStatus() *core.StoreHotRegionInfos {
	h.RLock()
	defer h.RUnlock()
	asPeer := make(map[uint64]*core.HotRegionsStat, len(h.statisticsAsPeer))
	for id, stat := range h.statisticsAsPeer {
		clone := *stat
		asPeer[id] = &clone
	}
	asLeader := make(map[uint64]*core.HotRegionsStat, len(h.statisticsAsLeader))
	for id, stat := range h.statisticsAsLeader {
		clone := *stat
		asLeader[id] = &clone
	}
	return &core.StoreHotRegionInfos{
		AsPeer:   asPeer,
		AsLeader: asLeader,
	}
}
