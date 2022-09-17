package cfapi

import (
	"sync"

	"github.com/variety-jones/cfrss/pkg/models"
)

type dummyCodeforcesClient struct {
	mutex                sync.Mutex
	lastUnprocessedIndex int

	// Results are streamed from this golden dataset for each RecentActions call.
	goldenDataset []models.RecentAction
}

func (client *dummyCodeforcesClient) RecentActions(maxCount int) (
	[]models.RecentAction, error) {
	client.mutex.Lock()
	defer client.mutex.Unlock()

	length := len(client.goldenDataset)
	if client.lastUnprocessedIndex >= length {
		return nil, nil
	}

	nextUnprocessedIndex := client.lastUnprocessedIndex + maxCount
	if nextUnprocessedIndex >= length {
		nextUnprocessedIndex = length
	}

	var res []models.RecentAction
	for ind := client.lastUnprocessedIndex; ind < length &&
		ind < nextUnprocessedIndex; ind++ {
		res = append(res, client.goldenDataset[ind])
	}

	client.lastUnprocessedIndex = nextUnprocessedIndex
	return res, nil
}

func NewDummyCodeforcesClient() CodeforcesAPI {
	client := new(dummyCodeforcesClient)
	return client
}
