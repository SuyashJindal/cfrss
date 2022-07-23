package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"

	"github.com/variety-jones/cfrss/pkg/models"
	"github.com/variety-jones/cfrss/pkg/store"
)

const (
	kRecentActionsCollectionName = "recent_actions"

	kActivityCreationTimeKey = "timeSeconds"
)

// mongoStore is the concrete implementation of CodeforcesStore
type mongoStore struct {
	mongoClient             *mongo.Client
	recentActionsCollection *mongo.Collection
}

// AddRecentActions adds a batch of actions to the store.
func (store *mongoStore) AddRecentActions(actions []models.RecentAction) error {
	if actions == nil {
		return nil
	}
	zap.S().Infof("Persisting a batch of %d actions to the store",
		len(actions))

	// Convert the actions into generic interface to be compatible with
	// InsertMany call.
	var docs []interface{}
	for _, action := range actions {
		docs = append(docs, action)
	}

	// Bulk update all these documents.
	_, err := store.recentActionsCollection.InsertMany(context.TODO(), docs)
	if err != nil {
		// TODO: Add deep printing.
		zap.S().Debugf("actions: %+v", actions)
		return fmt.Errorf("bulk insert failed with error [%w]", err)
	}

	return nil
}

// QueryRecentActions returns the list of actions that happened after a fixed
// timestamp.
func (store *mongoStore) QueryRecentActions(timestamp int64) (
	[]models.RecentAction, error) {
	zap.S().Infof("Retrieving all actions after timestamp %d", timestamp)

	filter := bson.D{{kActivityCreationTimeKey, bson.D{{"$gte", timestamp}}}}
	cursor, err := store.recentActionsCollection.Find(context.TODO(), filter)
	if err != nil {
		zap.S().Debugf("Filter for querying recent actions: %+v", filter)
		return nil, fmt.Errorf("could not query recent actions with error [%w]",
			err)
	}

	var actions []models.RecentAction
	if err := cursor.All(context.TODO(), &actions); err != nil {
		return nil, fmt.Errorf("could not parse query actions to bson.M "+
			"with error [%w]", err)
	}

	zap.S().Infof("Retrieved a batch of %d activities", len(actions))
	return actions, nil
}

// LastRecordedTimestampForRecentActions returns the latest activity
// timestamp of any blog/comment in the store.
// It returns zero if no document exists.
func (store *mongoStore) LastRecordedTimestampForRecentActions() int64 {
	// Create the filter to compute the maximum value of a field.
	filter := []bson.M{{
		"$group": bson.M{
			"_id": nil,
			"max": bson.M{
				"$max": fmt.Sprintf("$%s", kActivityCreationTimeKey)},
		}},
	}

	// Make an aggregation call.
	cursor, err := store.recentActionsCollection.Aggregate(context.TODO(), filter)
	if err != nil {
		zap.S().Errorf("Querying the max recorded activity timestamp failed "+
			"with error %v", err)
		return 0
	}

	// The result set should only contain one document. Decode it.
	for cursor.Next(context.TODO()) {
		res := struct {
			Max int64 `bson:"max"`
		}{}
		if err := cursor.Decode(&res); err != nil {
			zap.S().Errorf("Decoding of max activity timestamp failed with error"+
				"%v", err)
			return 0
		}
		return res.Max
	}
	return 0
}

// NewMongoStore creates a new instance of the mongo store.
func NewMongoStore(mongoURI, databaseName string) (store.CodeforcesStore, error) {
	zap.S().Infof("Attempting to create a new mongo store. mongoURI: %s, "+
		"databaseName = %s", mongoURI, databaseName)

	// Create a new client and connect to the server
	client, err := mongo.Connect(
		context.TODO(),
		options.Client().ApplyURI(mongoURI),
	)
	if err != nil {
		return nil, fmt.Errorf("could not create mongo client with error [%w]",
			err)
	}

	// Ping the primary
	if err := client.Ping(context.TODO(), readpref.Primary()); err != nil {
		return nil, fmt.Errorf("could not ping primary with error [%w]", err)
	}

	mStore := new(mongoStore)
	mStore.mongoClient = client
	mStore.recentActionsCollection = client.Database(databaseName).
		Collection(kRecentActionsCollectionName)

	return mStore, nil
}
