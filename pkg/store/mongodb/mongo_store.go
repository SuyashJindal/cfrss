package mongodb

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"

	"github.com/pkg/errors"

	"github.com/variety-jones/cfrss/pkg/models"
	"github.com/variety-jones/cfrss/pkg/store"
	"github.com/variety-jones/cfrss/pkg/utils"
)

const (
	kRecentActionsCollectionName = "recent_actions"
	kUsersCollectionName         = "users"
)

// mongoStore is the concrete implementation of CodeforcesStore
type mongoStore struct {
	mongoClient             *mongo.Client
	recentActionsCollection *mongo.Collection
	usersCollection         *mongo.Collection
}

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
		return errors.Errorf("bulk insert failed with error [%v]", err)
	}

	return nil
}

func (store *mongoStore) QueryRecentActions(startTimestamp, limit int64) (
	[]models.RecentAction, error) {
	zap.S().Infof("Retrieving all actions after timestamp %d", startTimestamp)

	filter := bson.M{
		"timeSeconds": bson.M{
			"$gte": startTimestamp,
		},
		"blogEntry": bson.M{
			"$exists": true,
		},
		"comment": bson.M{
			"$exists": true,
		},
	}

	// Sort by decreasing order of activity time and add limits.
	opt := options.Find().SetSort(bson.M{"timeSeconds": -1})
	opt.SetLimit(limit)

	cursor, err := store.recentActionsCollection.Find(context.TODO(), filter, opt)
	if err != nil {
		zap.S().Debugf("Filter for querying recent actions: %+v", filter)
		return nil, errors.Errorf("could not query recent actions with error [%v]",
			err)
	}

	var actions []models.RecentAction
	if err := cursor.All(context.TODO(), &actions); err != nil {
		return nil, errors.Errorf("could not parse query actions "+
			"with error [%v]", err)
	}

	utils.ConvertRelativeLinksToAbsoluteLinks(actions)

	zap.S().Infof("Retrieved a batch of %d activities", len(actions))
	return actions, nil
}

func (store *mongoStore) QueryCommentsFromBlog(id int, startTimestamp, limit int64) (
	[]models.Comment, error) {
	zap.S().Infof("Retrieving comments from blog %d after timestamp %d",
		id, startTimestamp)

	// Create a filter to query all comments from a blog with timestamp greater
	// than or equal to the given timestamp.
	filter := bson.M{
		"timeSeconds": bson.M{
			"$gte": startTimestamp,
		},
		"blogEntry.id": id,
		"comment": bson.M{
			"$exists": true,
		},
	}

	// Only include the "comment" field in the output.
	opt := options.Find().SetProjection(bson.M{"comment": 1})

	// Sort by decreasing order of activity time and add limits.
	opt.SetSort(bson.M{"timeSeconds": -1})
	opt.SetLimit(limit)

	cursor, err := store.recentActionsCollection.Find(context.TODO(), filter, opt)
	if err != nil {
		zap.S().Debugf("Filter for querying comments from blogs: %+v", filter)
		return nil, errors.Errorf("could not query comments with error [%v]",
			err)
	}

	var actions []models.RecentAction
	if err := cursor.All(context.TODO(), &actions); err != nil {
		return nil, errors.Errorf("could not decode actions "+
			"with error [%v]", err)
	}

	utils.ConvertRelativeLinksToAbsoluteLinks(actions)

	// Extract all the comments from the recent actions.
	var comments []models.Comment
	for _, action := range actions {
		if action.Comment != nil {
			comments = append(comments, *action.Comment)
		}
	}
	zap.S().Infof("Retrieved a batch of %d comments for blog %d",
		len(comments), id)

	return comments, nil
}

func (store *mongoStore) QueryAllUniqueBlogs(startTimestamp, limit int64) (
	[]models.BlogEntry, error) {
	return nil, nil
}

func (store *mongoStore) LastRecordedTimestampForRecentActions() int64 {
	// Create the filter to compute the maximum value of a field.
	filter := []bson.M{{
		"$group": bson.M{
			"_id": nil,
			"max": bson.M{
				"$max": "$timeSeconds",
			},
		}},
	}

	// Make an aggregation call.
	cursor, err := store.recentActionsCollection.Aggregate(context.TODO(),
		filter)
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
			zap.S().Errorf("Decoding of max activity timestamp failed "+
				"with error %v", err)
			return 0
		}
		return res.Max
	}
	return 0
}

func (store *mongoStore) AddUser(user *models.User) error {
	if user == nil {
		return nil
	}
	zap.S().Infof("Adding user [username: %s, uuid: %s] to the store",
		user.Username, user.Uuid)

	if _, err := store.usersCollection.InsertOne(
		context.TODO(), user); err != nil {
		return errors.Errorf("could not insert user: %+v to the store "+
			"with error [%v]", *user, err)
	}
	return nil
}

func (store *mongoStore) QueryUserByUuid(uuid string) (*models.User, error) {
	zap.S().Infof("Querying the store for uuid %s", uuid)
	// Create the filter to query the user.
	filter := bson.M{
		"uuid": uuid,
	}

	// Query the store.
	res := store.usersCollection.FindOne(context.TODO(), filter)
	if res.Err() != nil {
		return nil, errors.Errorf("could not query user with uuid %s "+
			"with error [%v]", uuid, res.Err())
	}

	// Decode the store user.
	user := new(models.User)
	if err := res.Decode(user); err != nil {
		return nil, errors.Errorf("could not decode result to user "+
			"with error [%v], possibly the user does not exist", err)
	}

	return user, nil
}

func (store *mongoStore) QueryRecentActionsForUser(uuid string,
	startTimestamp, limit int64) ([]models.RecentAction, error) {
	zap.S().Infof("Retrieving all actions for user %s after timestamp %d",
		uuid, startTimestamp)

	user, err := store.QueryUserByUuid(uuid)
	if err != nil {
		return nil, errors.Errorf("uuid to user conversion failed with eror [%v]",
			err)
	}

	if len(user.SubscribedBlogs) == 0 {
		return nil, nil
	}

	// Create the filter to select only subscribed blogs sorted by time.
	filter := bson.M{
		"timeSeconds": bson.M{
			"$gte": startTimestamp,
		},
		"blogEntry.id": bson.M{
			"$in": user.SubscribedBlogs,
		},
		"comment": bson.M{
			"$exists": true,
		},
	}

	// Sort by decreasing order of activity time and add limits.
	opt := options.Find().SetSort(bson.M{"timeSeconds": -1})
	opt.SetLimit(limit)

	// Query all the documents.
	cursor, err := store.recentActionsCollection.Find(context.TODO(), filter, opt)
	if err != nil {
		zap.S().Debugf("Filter for querying recent actions: %+v", filter)
		return nil,
			errors.Errorf("could not query recent actions with error [%v]", err)
	}

	// Unmarshal the results.
	var actions []models.RecentAction
	if err := cursor.All(context.TODO(), &actions); err != nil {
		return nil, errors.Errorf("could not parse query actions "+
			"with error [%v]", err)
	}

	utils.ConvertRelativeLinksToAbsoluteLinks(actions)

	zap.S().Infof("Retrieved a batch of %d activities for user %s",
		len(actions), user.Uuid)
	return actions, nil
}

func (store *mongoStore) SubscribeToBlogs(uuid string, ids ...int) error {
	zap.S().Infof("User %s is subscribing to blogs %v", uuid, ids)

	// Create the filters to query and update the user's data.
	findFilter := bson.M{
		"uuid": uuid,
	}
	updateFilter := bson.M{
		"$push": bson.M{
			"subscribedBlogs": bson.M{
				"$each": ids,
			},
		},
	}

	_, err := store.updateSingleUser(findFilter, updateFilter)
	if err != nil {
		return errors.Errorf("user %s could not subscribe to blogs "+
			"with error [%v]", uuid, err)
	}

	return nil
}

func (store *mongoStore) UnsubscribeFromBlogs(uuid string, ids ...int) error {
	zap.S().Infof("User %s is unsubscribing from blogs %v", uuid, ids)

	// Create the filters to query and update the user's data.
	findFilter := bson.M{
		"uuid": uuid,
	}
	updateFilter := bson.M{
		"$pullAll": bson.M{
			"subscribedBlogs": ids,
		},
	}

	_, err := store.updateSingleUser(findFilter, updateFilter)
	if err != nil {
		return errors.Errorf("user %s could not unsubscribe from blogs "+
			"with error [%v]", uuid, err)
	}

	return nil
}

// updateSingleUser is a utility function to update a single user according to
// the filter provided.
//
// It returns the document as it was before the update.
func (store *mongoStore) updateSingleUser(findFilter, updateFilter interface{}) (
	oldUser *models.User, err error) {
	zap.S().Infof("Updating single user using the below filters")
	zap.S().Infof("find filter %+v", findFilter)
	zap.S().Infof("update filter %+v", updateFilter)

	// Find the user's entry and update it.
	res := store.usersCollection.FindOneAndUpdate(context.TODO(),
		findFilter, updateFilter)
	if res.Err() != nil {
		return nil, errors.Errorf("updation of single user failed "+
			"with error [%v]", res.Err())
	}

	// Unmarshal the result to confirm that we got a match.
	oldUser = new(models.User)
	if err := res.Decode(oldUser); err != nil {
		return nil, errors.Errorf("could not decode result to user "+
			"with error [%v], possibly the user does not exist", err)
	}

	return oldUser, nil
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
		return nil, errors.Errorf("could not create mongo client with error [%v]",
			err)
	}

	// Ping the primary
	if err := client.Ping(context.TODO(), readpref.Primary()); err != nil {
		return nil, errors.Errorf("could not ping primary with error [%v]", err)
	}

	mStore := new(mongoStore)
	mStore.mongoClient = client
	mStore.recentActionsCollection = client.Database(databaseName).
		Collection(kRecentActionsCollectionName)
	mStore.usersCollection = client.Database(databaseName).
		Collection(kUsersCollectionName)

	return mStore, nil
}
