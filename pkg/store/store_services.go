package store

import "github.com/variety-jones/cfrss/pkg/models"

// CodeforcesStore is the interface needed to persist data from Codeforces
// to MongoDB.
type CodeforcesStore interface {
	// AddRecentActions adds a batch of actions to the store.
	AddRecentActions(actions []models.RecentAction) error

	// QueryRecentActions returns the list of actions that happened at or
	// after a fixed timestamp.
	QueryRecentActions(startTimestamp, limit int64) ([]models.RecentAction, error)

	// LastRecordedTimestampForRecentActions returns the latest activity
	// timestamp of any blog/comment in the store.
	// It returns zero if no document exists.
	LastRecordedTimestampForRecentActions() int64

	// QueryAllUniqueBlogs returns the metadata of all the unique blogs,
	// filtered by the blog creation time.
	QueryAllUniqueBlogs(startTimestamp, limit int64) ([]models.BlogEntry, error)

	// QueryCommentsFromBlog returns all the comments from a particular blog.
	// They are filtered by creation time and sorted in decreasing order of
	// creation time.
	QueryCommentsFromBlog(id int, startTimestamp, limit int64) (
		[]models.Comment, error)

	// AddUser adds the given user to the store.
	// TODO: Add uniqueness checks for username.
	AddUser(user *models.User) error

	// QueryUserByUuid returns the store user matching the uuid.
	QueryUserByUuid(uuid string) (*models.User, error)

	// QueryRecentActionsForUser returns the list of all activities on the
	// blogs that the user is subscribed to.
	// TODO: Sort it according to activity time and implement pagination.
	QueryRecentActionsForUser(uuid string, startTimestamp, limit int64) (
		[]models.RecentAction, error)

	// SubscribeToBlogs subscribes a user to the given blogs.
	SubscribeToBlogs(uuid string, ids ...int) error

	// UnsubscribeFromBlogs unsubscribes a user from the given blogs.
	UnsubscribeFromBlogs(uuid string, ids ...int) error
}
