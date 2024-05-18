package main

import (
	"context"
	"log"
	"sync"

	"cloud.google.com/go/bigquery"
	"github.com/TFMV/GCS2Postgres/src/db"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	// Initialize context
	ctx := context.Background()

	// Load configuration
	config, err := db.LoadConfig("../config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	log.Println("Loaded configuration.")

	// Fetch secret
	secret, err := db.FetchSecret(ctx, config.Postgres.SecretName)
	if err != nil {
		log.Fatalf("Failed to fetch secret: %v", err)
	}
	log.Println("Fetched secret.")

	// Build PostgreSQL connection string
	connString := db.BuildConnString(config, secret)

	// Connect to PostgreSQL
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer pool.Close()
	log.Println("Connected to PostgreSQL.")

	// Create BigQuery client
	bqClient, err := bigquery.NewClient(ctx, config.GCS.ProjectID)
	if err != nil {
		log.Fatalf("Failed to create BigQuery client: %v", err)
	}
	defer bqClient.Close()
	log.Println("Created BigQuery client.")

	// Iterate over files in the config and process them
	for _, file := range config.GCS.Files {
		dataChan := make(chan []bigquery.Value)
		schemaChan := make(chan bigquery.Schema)

		columnTypes, columns, err := db.FetchColumns(ctx, pool, file.Table)
		if err != nil {
			log.Fatalf("Failed to fetch columns: %v", err)
		}

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			db.DataConsumer(ctx, pool, file.Table, columnTypes, columns, dataChan, schemaChan)
		}()

		go func() {
			defer wg.Done()
			db.DataProducer(ctx, bqClient, config, file, dataChan, schemaChan)
		}()

		wg.Wait()
	}

	log.Println("All tasks completed successfully.")
}
