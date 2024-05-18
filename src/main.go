package main

import (
	"context"
	"log"

	"github.com/TFMV/GCS2Postgres/src/db"
	"github.com/TFMV/GCS2Postgres/src/utils"
)

func main() {
	ctx := context.Background()

	// Load the configuration
	config, err := utils.LoadConfig("../config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Fetch the secret (password)
	secret, err := utils.FetchSecret(ctx, config.Postgres.SecretName)
	if err != nil {
		log.Fatalf("Failed to fetch secret: %v", err)
	}

	// Build the connection string
	connString := utils.BuildConnString(config, secret)

	// Initialize PostgreSQL connection pool
	pool, err := db.InitializePostgresPool(ctx, connString)
	if err != nil {
		log.Fatalf("Failed to initialize PostgreSQL pool: %v", err)
	}
	defer pool.Close()

	// Initialize BigQuery client
	bigqueryClient, err := db.InitializeBigQueryClient(ctx, config.GCS.ProjectID)
	if err != nil {
		log.Fatalf("Failed to initialize BigQuery client: %v", err)
	}

	// Transfer data
	db.TransferData(ctx, config, pool, bigqueryClient)
}
