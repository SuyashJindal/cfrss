package scheduler

import (
	"time"

	"go.uber.org/zap"

	"github.com/variety-jones/cfrss/pkg/cfapi"
	"github.com/variety-jones/cfrss/pkg/models"
	"github.com/variety-jones/cfrss/pkg/store"
)

// CodeforcesScheduler is the scheduler that persists recent actions data to
// Codeforces periodically.
type CodeforcesScheduler struct {
	cfClient              cfapi.CodeforcesInterface
	cfStore               store.CodeforcesStore
	cooldown              time.Duration
	lastInsertedTimestamp int64
	batchSize             int
}

// filter scans the list of recent actions and removes the one that are stale,
// i,e, the ones who are already in the store.
func (sch *CodeforcesScheduler) filter(actions []models.RecentAction) (
	[]models.RecentAction, int64) {
	maxTimestampAfterInsertion := sch.lastInsertedTimestamp
	var newActions []models.RecentAction
	for _, action := range actions {
		now := action.TimeSeconds
		if now > maxTimestampAfterInsertion {
			maxTimestampAfterInsertion = now
		}
		if now > sch.lastInsertedTimestamp {
			newActions = append(newActions, action)
		}
	}
	return newActions, maxTimestampAfterInsertion
}

// Start is a blocking call that makes an API call to Codeforces and persists
// the results in MongoDB at fixed intervals.
func (sch *CodeforcesScheduler) Start() {
	for {
		actions, err := sch.cfClient.RecentActions(sch.batchSize)
		if err != nil {
			zap.S().Errorf("codeforces query failed with error %v", err)
		} else {
			newActions, maxTimestampAfterInsertion := sch.filter(actions)
			if err := sch.cfStore.AddRecentActions(newActions); err != nil {
				zap.S().Errorf("mongo insertion failed with error %v", err)
			} else {
				// Do an atomic swap only when insertion is successful.
				sch.lastInsertedTimestamp = maxTimestampAfterInsertion
				zap.S().Infof("Persisted activities till timestamp: %d",
					sch.lastInsertedTimestamp)
			}
		}
		zap.S().Infof("Sleeping for %v", sch.cooldown)
		time.Sleep(sch.cooldown)
	}
}

// NewScheduler creates a new instance of the scheduler.
func NewScheduler(cfClient cfapi.CodeforcesInterface,
	cfStore store.CodeforcesStore, batchSize int,
	lastInsertedTimestamp int64,
	coolDown time.Duration) *CodeforcesScheduler {
	sch := new(CodeforcesScheduler)
	sch.cfClient = cfClient
	sch.cfStore = cfStore
	sch.cooldown = coolDown
	sch.batchSize = batchSize
	sch.lastInsertedTimestamp = lastInsertedTimestamp

	return sch
}
