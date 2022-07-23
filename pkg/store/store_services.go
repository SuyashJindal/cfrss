package store

import "github.com/variety-jones/cfrss/pkg/models"

// CodeforcesStore is the interface needed to persist data from Codeforces
// to MongoDB.
type CodeforcesStore interface {
	AddRecentActions(actions []models.RecentAction) error
	QueryRecentActions(timestamp int64) (actions []models.RecentAction, err error)
	LastRecordedTimestampForRecentActions() int64
}
