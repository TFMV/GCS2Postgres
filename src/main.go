package main

import (
	"context"
	"log"
	"sync"

	"cloud.google.com/go/bigquery"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/semaphore"
	"google.golang.org/api/option"

	"github.com/TFMV/GCS2Postgres/src/db"
)

func main() {
	ctx := context.Background()

	log.Println("Starting application...")
	config, err := db.LoadConfig("../config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	log.Println("Loaded configuration.")

	password, err := db.FetchSecret(ctx, config.Postgres.SecretName)
	if err != nil {
		log.Fatalf("Failed to fetch secret: %v", err)
	}
	log.Println("Fetched secret.")

	postgresURL := "postgresql://" + config.Postgres.User + ":" + password + "@" + config.Postgres.Host + "/" + config.Postgres.DBName

	log.Println("Connecting to PostgreSQL...")
	pool, err := pgxpool.New(ctx, postgresURL)
	if err != nil {
		log.Fatalf("Unable to create connection pool: %v", err)
	}
	defer pool.Close()
	log.Println("Connected to PostgreSQL.")

	log.Println("Creating BigQuery client...")
	bigqueryClient, err := bigquery.NewClient(ctx, config.GCS.ProjectID, option.WithCredentialsFile("../sa.json"))
	if err != nil {
		log.Fatalf("Failed to create BigQuery client: %v", err)
	}
	log.Println("Created BigQuery client.")

	var wg sync.WaitGroup
	sem := semaphore.NewWeighted(int64(config.GCS.ConcurrentJobs))

	for _, file := range config.GCS.Files {
		wg.Add(1)
		dataChan := make(chan []bigquery.Value, 10000)
		go func(file db.File) {
			defer wg.Done()
			if err := sem.Acquire(ctx, 1); err != nil {
				log.Fatalf("Failed to acquire semaphore: %v", err)
			}
			defer sem.Release(1)

			log.Printf("Processing file: %s", file.Name)
			db.DataProducer(ctx, bigqueryClient, config, file, dataChan)

			columns, err := db.FetchColumns(ctx, pool, file.Table)
			if err != nil {
				log.Fatalf("Failed to fetch columns: %v", err)
			}
			db.DataConsumer(ctx, pool, file.Table, columns, dataChan)
		}(file)
	}

	wg.Wait()
	log.Println("All tasks completed successfully.")
}
