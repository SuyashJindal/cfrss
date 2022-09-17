package main

import (
	"flag"
	"log"
	"sync"
	"time"

	"github.com/variety-jones/cfrss/pkg/web"
	"go.uber.org/zap"

	"github.com/variety-jones/cfrss/pkg/cfapi"
	"github.com/variety-jones/cfrss/pkg/scheduler"
	"github.com/variety-jones/cfrss/pkg/store/mongodb"
)

const (
	kDefaultEnvironment     = "dev"
	kDefaultCoolDownMinutes = 5
	kDefaultBatchSize       = 100
	kDefaultDatabaseName    = "cfrss-local"
	kDefaultMongoAddr       = "mongodb://localhost:27017"
	kDefaultServerAddr      = ":5000"

	kDefaultCodeforcesTimeoutMinutes = 2
)

func main() {
	// Define the customizable flags.
	var serverAddr, mongoAddr, databaseName, environment string
	var coolDownInMinutes, batchSize int
	var enableCodeforcesScheduler bool
	flag.StringVar(&serverAddr, "serverAddr", kDefaultServerAddr,
		"The address on which to run the web server")
	flag.StringVar(&environment, "environment", kDefaultEnvironment,
		"The current environment: dev/prod")
	flag.StringVar(&mongoAddr, "mongo-addr", kDefaultMongoAddr,
		"mongoDB address")
	flag.StringVar(&databaseName, "database-name", kDefaultDatabaseName,
		"The name of the MongoDB database")
	flag.IntVar(&coolDownInMinutes, "cooldown-minutes", kDefaultCoolDownMinutes,
		"The cooldown (in minutes) for contacting Codeforces API")
	flag.IntVar(&batchSize, "cf-batch-size", kDefaultBatchSize,
		"The number of recent actions to query on each API call")
	flag.BoolVar(&enableCodeforcesScheduler, "enable-cf-scheduler", false,
		"If set to true, DB is updated periodically with data from CF")

	// Parse all the flags.
	flag.Parse()

	// Create the zap logger and replace the global logger.
	var logger *zap.Logger
	var loggerError error
	if environment == kDefaultEnvironment {
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

	if enableCodeforcesScheduler {
		// Create the scheduler to contact CF and persist the result to MongoDB.
		sch := scheduler.NewScheduler(cfClient, cfStore, batchSize,
			time.Duration(coolDownInMinutes)*time.Minute)

		// Start the scheduler in a new goroutine.
		go sch.Start()
	}

	go func() {
		if err := web.CreateWebServer(cfStore).
			ListenAndServe(serverAddr); err != nil {
			zap.S().Fatal(err)
		}
	}()

	// Wait forever.
	var wg sync.WaitGroup
	wg.Add(1)
	wg.Wait()
}
