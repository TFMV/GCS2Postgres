package main

import (
	"context"
	"log"

	"github.com/TFMV/GCS2Postgres/src/db"
	"github.com/TFMV/GCS2Postgres/src/utils"
)

func main() {
	ctx := context.Background()

	config, err := utils.LoadConfig("../config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	secret, err := utils.FetchSecret(ctx, config.Postgres.SecretName)
	if err != nil {
		log.Fatalf("Failed to fetch secret: %v", err)
	}

	connString := utils.BuildConnString(config, secret)
	pool, err := utils.InitializePostgresPool(ctx, connString)
	if err != nil {
		log.Fatalf("Failed to initialize Postgres pool: %v", err)
	}
	defer pool.Close()

	bigqueryClient, err := utils.InitializeBigQueryClient(ctx, config.GCS.ProjectID)
	if err != nil {
		log.Fatalf("Failed to initialize BigQuery client: %v", err)
	}
	defer bigqueryClient.Close()

	db.TransferData(ctx, config, pool, bigqueryClient)
}
