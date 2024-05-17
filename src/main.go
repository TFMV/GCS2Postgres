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

	config, err := db.LoadConfig("../config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	password, err := db.FetchSecret(ctx, config.Postgres.SecretName)
	if err != nil {
		log.Fatalf("Failed to fetch secret: %v", err)
	}

	postgresURL := "postgresql://" + config.Postgres.User + ":" + password + "@" + config.Postgres.Host + "/" + config.Postgres.DBName

	pool, err := pgxpool.New(ctx, postgresURL)
	if err != nil {
		log.Fatalf("Unable to create connection pool: %v", err)
	}
	defer pool.Close()

	bigqueryClient, err := bigquery.NewClient(ctx, config.GCS.ProjectID, option.WithCredentialsFile("../sa.json"))
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
		go func(file db.File) {
			defer sem.Release(1)
			defer wg.Done()
			db.DataProducer(ctx, bigqueryClient, config, file, dataChan, &wg)
			columns, err := db.FetchColumns(ctx, pool, file.Table)
			if err != nil {
				log.Fatalf("Failed to fetch columns: %v", err)
			}
			db.DataConsumer(ctx, pool, file.Table, columns, dataChan, &wg)
		}(file)
	}

	wg.Wait()
}
