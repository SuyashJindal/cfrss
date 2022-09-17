package scheduler

import (
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/variety-jones/cfrss/pkg/cfapi"
	"github.com/variety-jones/cfrss/pkg/models"
	"github.com/variety-jones/cfrss/pkg/store"
)

type CodeforcesSchedulerInterface interface {
	// Sync makes a single API call to Codeforces and stores the result in store.
	Sync() error

	// Start runs Sync in an infinite loop with a cooldown period.
	Start()
}

// CodeforcesScheduler is the scheduler that persists recent actions data to
// Codeforces store periodically.
type CodeforcesScheduler struct {
	mutex                 sync.Mutex
	cfClient              cfapi.CodeforcesAPI
	cfStore               store.CodeforcesStore
	cooldown              time.Duration
	lastInsertedTimestamp int64
	batchSize             int
}

// filter scans the list of recent actions and removes the one that are stale,
// i,e, the ones that are already in the store.
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

func (sch *CodeforcesScheduler) Sync() error {
	sch.mutex.Lock()
	defer sch.mutex.Unlock()

	actions, err := sch.cfClient.RecentActions(sch.batchSize)
	if err != nil {
		return errors.Errorf("codeforces query failed with error [%v]", err)
	}

	newActions, maxTimestampAfterInsertion := sch.filter(actions)
	if err := sch.cfStore.AddRecentActions(newActions); err != nil {
		return errors.Errorf("mongo insertion failed with error [%v]", err)
	}

	// Do an atomic swap only when insertion is successful.
	sch.lastInsertedTimestamp = maxTimestampAfterInsertion
	zap.S().Infof("Persisted activities till timestamp: %d",
		sch.lastInsertedTimestamp)

	return nil
}

func (sch *CodeforcesScheduler) Start() {
	for {
		if err := sch.Sync(); err != nil {
			zap.S().Errorf("Failed to sync with codeforces with error [%+v]",
				err)
		}
		zap.S().Infof("Sleeping for %v", sch.cooldown)
		time.Sleep(sch.cooldown)
	}
}

// NewScheduler creates a new instance of the scheduler.
func NewScheduler(cfClient cfapi.CodeforcesAPI,
	cfStore store.CodeforcesStore, batchSize int,
	coolDown time.Duration) CodeforcesSchedulerInterface {
	sch := new(CodeforcesScheduler)
	sch.cfClient = cfClient
	sch.cfStore = cfStore
	sch.cooldown = coolDown
	sch.batchSize = batchSize
	sch.lastInsertedTimestamp = cfStore.LastRecordedTimestampForRecentActions()

	return sch
}
