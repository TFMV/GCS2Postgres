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

	config, err := db.LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	postgresURL := "postgresql://" + config.Postgres.User + ":" + config.Postgres.Password + "@" + config.Postgres.Host + "/" + config.Postgres.DBName

	pool, err := pgxpool.New(ctx, postgresURL)
	if err != nil {
		log.Fatalf("Unable to create connection pool: %v", err)
	}
	defer pool.Close()

	bigqueryClient, err := bigquery.NewClient(ctx, config.GCS.ProjectID, option.WithCredentialsFile("path/to/credentials.json"))
	if err != nil {
		log.Fatalf("Failed to create BigQuery client: %v", err)
	}

	var wg sync.WaitGroup
	sem := semaphore.NewWeighted(int64(config.GCS.ConcurrentJobs))

	for _, file := range config.GCS.Files {
		wg.Add(1)
		dataChan := make(chan []bigquery.Value)
		if err := sem.Acquire(ctx, 1); err != nil {
			log.Fatalf("Failed to acquire semaphore: %v", err)
		}
		go func(file string) {
			defer sem.Release(1)
			defer wg.Done()
			db.DataProducer(ctx, bigqueryClient, config, file, dataChan, &wg)
			columns, err := db.FetchColumns(ctx, pool, "your_target_table") // Adjust target table as needed
			if err != nil {
				log.Fatalf("Failed to fetch columns: %v", err)
			}
			db.DataConsumer(ctx, pool, "your_target_table", columns, dataChan, &wg) // Adjust target table as needed
		}(file)
	}

	wg.Wait()
}
