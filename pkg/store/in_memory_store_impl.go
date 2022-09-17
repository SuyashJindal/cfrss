package store

import (
	"fmt"
	"sync"

	"github.com/variety-jones/cfrss/pkg/models"
)

type inMemoryCodeforcesStore struct {
	mutex sync.Mutex

	recentActions  []models.RecentAction
	uuidToUsersMap map[string]*models.User
}

func (store *inMemoryCodeforcesStore) AddRecentActions(
	actions []models.RecentAction) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	store.recentActions = append(store.recentActions, actions...)
	return nil
}

func (store *inMemoryCodeforcesStore) QueryRecentActions(
	startTimestamp, limit int64) (
	[]models.RecentAction, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	var res []models.RecentAction
	for _, action := range store.recentActions {
		if action.TimeSeconds >= startTimestamp {
			res = append(res, action)
		}
	}

	return res, nil
}

func (store *inMemoryCodeforcesStore) LastRecordedTimestampForRecentActions() int64 {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	res := int64(0)
	for _, action := range store.recentActions {
		if action.TimeSeconds > res {
			res = action.TimeSeconds
		}
	}

	return res
}

func (store *inMemoryCodeforcesStore) AddUser(user *models.User) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	// TODO: Add condition to reject duplicate uuid.
	store.uuidToUsersMap[user.Uuid] = user

	return nil
}

func (store *inMemoryCodeforcesStore) QueryUserByUuid(uuid string) (
	*models.User, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	user, ok := store.uuidToUsersMap[uuid]
	if !ok {
		return nil, fmt.Errorf("user does not exist")
	}

	return user, nil
}

func (store *inMemoryCodeforcesStore) QueryRecentActionsForUser(
	uuid string, startTimestamp, limit int64) ([]models.RecentAction, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	user, ok := store.uuidToUsersMap[uuid]
	if !ok {
		return nil, fmt.Errorf("user does not exist")
	}

	var res []models.RecentAction
	// TODO: Optimize the time complexity of search.
	ids := user.SubscribedBlogs
	for _, action := range store.recentActions {
		if action.TimeSeconds >= startTimestamp && action.BlogEntry != nil {
			for _, id := range ids {
				if action.BlogEntry.Id == id {
					res = append(res, action)
					break
				}
			}
		}
	}

	return res, nil
}

func (store *inMemoryCodeforcesStore) SubscribeToBlogs(
	uuid string, ids ...int) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	user, ok := store.uuidToUsersMap[uuid]
	if !ok {
		return fmt.Errorf("user does not exist")
	}

	// We are operating on a pointer, hence we don't need to overwrite it in
	// the map.
	user.SubscribedBlogs = append(user.SubscribedBlogs, ids...)

	return nil
}

func (store *inMemoryCodeforcesStore) UnsubscribeFromBlogs(
	uuid string, ids ...int) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	user, ok := store.uuidToUsersMap[uuid]
	if !ok {
		return fmt.Errorf("user does not exist")
	}

	// TODO: Improve the time complexity.
	var newBlogsList []int
	for _, old := range user.SubscribedBlogs {
		for _, toUnsubscribe := range ids {
			if old != toUnsubscribe {
				newBlogsList = append(newBlogsList, old)
			}
		}
	}

	// We are operating on a pointer, hence we don't need to overwrite it in
	// the map.
	user.SubscribedBlogs = newBlogsList

	return nil
}

func (store *inMemoryCodeforcesStore) QueryCommentsFromBlog(
	id int, startTimestamp, limit int64) (
	[]models.Comment, error) {
	// TODO: Implement it.
	return nil, nil
}

func (store *inMemoryCodeforcesStore) QueryAllUniqueBlogs(
	startTimestamp, limit int64) (
	[]models.BlogEntry, error) {
	// TODO: Implement it.
	return nil, nil
}

func NewInMemoryCodeforcesStore() CodeforcesStore {
	store := new(inMemoryCodeforcesStore)
	store.uuidToUsersMap = make(map[string]*models.User)

	return store
}
