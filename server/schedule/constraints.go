package schedule

import "github.com/pingcap/pd/server/core"

type state int
const (
	_ state = iota
	NOT_READY
	READY
)

type Constraint struct{
	// Name 唯一标识一个 Constraint
	Name string

	TableId int

	// MinKey, MaxKey 有 TableId 计算得来
	MinKey int
	MaxKey int
	Labels []string


	TableName string
	State state

}


// Given a command like `pd_ctl config set region-constraint mbk_db_1.table_1 "rack=2"`,
// where mbk_db_1.table_1 is the tableName and rack=2 is the label(s)
func NewConstraint(constrtaintName string, tableName string, labels ...string) *Constraint {

	return &Constraint{
		Name: constrtaintName,
		Labels: labels,
		TableName: tableName,
		State: NOT_READY,
	}

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

// TODO this func convert tableName to tableId accroding to info from cluster
func (c *Constraint) ConvertTableNameToId(cluster Cluster) (bool, error) {

	if id, ok := cluster.FindTableIdByName(c.TableName); ok {

		// TODO mutex lock is needed or not?
		c.TableId = id
		// cal max and min key based on tableId
		c.MaxKey = calMaxKey(id)
		c.MinKey = calMinKey(id)
		c.State = READY
		return true, nil
	}
	return false, nil
}

