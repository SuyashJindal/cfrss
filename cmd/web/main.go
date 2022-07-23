package main

import (
	"flag"
	"log"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/variety-jones/cfrss/pkg/cfapi"
	"github.com/variety-jones/cfrss/pkg/scheduler"
	"github.com/variety-jones/cfrss/pkg/store/mongodb"
)

const (
	kDevelopmentEnvironment = "dev"
	kDefaultCoolDownMinutes = 5
	kDefaultBatchSize       = 100
	kDefaultDatabaseName    = "cfrss-local"
	kDefaultMongoAddr       = "mongodb://localhost:27017"

	kDefaultCodeforcesTimeoutMinutes = 2
)

func main() {
	// Define the customizable flags.
	var mongoAddr, databaseName, environment string
	var coolDownInMinutes, batchSize int
	flag.StringVar(&environment, "environment", kDevelopmentEnvironment,
		"The current environment: dev/prod")
	flag.StringVar(&mongoAddr, "mongo-addr", kDefaultMongoAddr,
		"mongoDB address")
	flag.StringVar(&databaseName, "database-name", kDefaultDatabaseName,
		"The name of the MongoDB database")
	flag.IntVar(&coolDownInMinutes, "cooldown-minutes", kDefaultCoolDownMinutes,
		"The cooldown (in minutes) for contacting Codeforces API")
	flag.IntVar(&batchSize, "cf-batch-size", kDefaultBatchSize,
		"The number of recent actions to query on each API call")

	// Parse all the flags.
	flag.Parse()

	// Create the zap logger and replace the global logger.
	var logger *zap.Logger
	var loggerError error
	if environment == kDevelopmentEnvironment {
		if logger, loggerError = zap.NewDevelopment(); loggerError != nil {
			log.Fatalln(loggerError)
		}
	} else {
		if logger, loggerError = zap.NewProduction(); loggerError != nil {
			log.Fatalln(loggerError)
		}
	}
	defer logger.Sync()
	zap.ReplaceGlobals(logger)

	// Create the codeforces client to make API calls.
	cfClient := cfapi.NewCodeforcesClient(
		time.Duration(kDefaultCodeforcesTimeoutMinutes) * time.Minute)

	// Create the cfStore to persist data to MongoDB.
	// Also, query the last recorded timestamp.
	cfStore, err := mongodb.NewMongoStore(mongoAddr, databaseName)
	if err != nil {
		zap.S().Fatal(err)
	}
	lastRecordedTimestamp := cfStore.LastRecordedTimestampForRecentActions()

	// Create the schedule to contact CF and persist the result to MongoDB.
	sch := scheduler.NewScheduler(cfClient, cfStore, batchSize,
		lastRecordedTimestamp, time.Duration(coolDownInMinutes)*time.Minute)

	// Start the scheduler in a new goroutine.
	go sch.Start()

	// Wait forever.
	var wg sync.WaitGroup
	wg.Add(1)
	wg.Wait()
}
