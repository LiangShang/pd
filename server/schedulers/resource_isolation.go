package schedulers

import (
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
)

func init() {
	schedule.RegisterScheduler("resourceIsolation", func(opt schedule.Options, args []string) (schedule.Scheduler, error){
		return newResourceIsolationScheduler(opt), nil
	})
}

type resourceIsolationScheduler struct {


}


func newResourceIsolationScheduler(opt schedule.Options) schedule.Scheduler {
	return &resourceIsolationScheduler{}
}

func (s *resourceIsolationScheduler) GetName() string {
	return "resource-isolation-scheduler"
}

func (s *resourceIsolationScheduler) GetResourceKind() core.ResourceKind {
	return core.RegionKind // ?
}

func (s *resourceIsolationScheduler) GetResourceLimit() uint64 {
	return minUint64(s.limit, s.opt.GetRegionScheduleLimit())
}

func (s *resourceIsolationScheduler) Prepare(cluster schedule.Cluster) error { return nil }

func (s *resourceIsolationScheduler) Cleanup(cluster schedule.Cluster) {}

func (s *resourceIsolationScheduler) Schedule(cluster schedule.Cluster) schedule.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()

	// Select a peer from the region that resides in the wrong store, according to the constraints
	constraint, sourceStore, sourceRegion, found := filterSource(cluster)
	if !found {
		return nil
	}
	sourcePeer := sourceRegion.GetStorePeer(sourceStore.GetId())

	// find the target
	targetStore := RandomPick(constraint.Stores())
	targetPeer, err := cluster.AllocPeer(targetStore.GetID())
	if err != nil {
		// TODO record in prometheus
		return nil
	}
	return schedule.CreateMovePeerOperator(sourceRegion, core.RegionKind, sourcePeer, targetPeer)



}

// self-defined filter function, not impl schedule.Filter
//
func filterSource(cluster schedule.Cluster) (*Constraint, *core.StoreInfo, *core.RegionInfo, bool) {
	for _, s := range cluster.GetStores() {
		for _, constraint := range cluster.Constraints() {
			if constraint.ContainsStore(s) {
				continue
			}
			// this store is not included in the constraint, check whether it contains inappropriate region
			for _, region := range s.AllRegions() {
				if constraint.ConstainsRegion(region) {
					return constraint, s, region, true
				}
			}
		}
	}
	return nil, nil, nil, false
}


type Constraint struct{
	// Name 唯一标识一个 Constraint
	Name string

	TableId int

	// MinKey, MaxKey 有 TableId 计算得来
	MinKey int
	MaxKey int
	Labels []string

}

func (c *Constraint) ContainsStore(s core.StoreInfo) bool {

	for _, l := range s.Labels{
		for _, constraintLabel := range c.Labels {
			if l == constraintLabel {
				return true
			}
		}
	}
	return false
}

func (c *Constraint) ContainsRegion(r core.RegionInfo) bool {

	startKey := r.StartKey
	endKey := r.EndKey

	// TODO given a tableid t_id, how to figure out whether it resides between startKey & endKey?


}