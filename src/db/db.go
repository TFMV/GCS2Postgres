package db

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/TFMV/GCS2Postgres/src/utils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/api/iterator"
)

// DataProducer creates an external table in BigQuery and fetches data.
func DataProducer(ctx context.Context, bigqueryClient *bigquery.Client, config *utils.Config, file utils.File, dataChan chan<- []bigquery.Value, schemaChan chan bigquery.Schema) {
	defer close(dataChan)
	log.Printf("Starting DataProducer for file: %s...", file.Name)

	// Create an external table in BigQuery for the specified file
	gcsFilePath := "gs://" + config.GCS.BucketName + "/" + file.Name
	tableID := strings.TrimSuffix(filepath.Base(file.Name), filepath.Ext(file.Name))

	externalTable := &bigquery.TableMetadata{
		ExternalDataConfig: &bigquery.ExternalDataConfig{
			SourceFormat: bigquery.DataFormat(strings.ToUpper(filepath.Ext(file.Name)[1:])),
			SourceURIs:   []string{gcsFilePath},
		},
	}

	log.Printf("Creating external table: %s", tableID)

	// Check if the table already exists
	_, err := bigqueryClient.Dataset(config.GCS.Dataset).Table(tableID).Metadata(ctx)
	if err == nil {
		log.Printf("Table %s already exists, skipping creation.", tableID)
	} else {
		log.Printf("Creating new table %s.", tableID)
		if err := bigqueryClient.Dataset(config.GCS.Dataset).Table(tableID).Create(ctx, externalTable); err != nil {
			log.Fatalf("Failed to create external table in BigQuery: %v", err)
		}
	}

	log.Printf("Running query for table: %s", tableID)
	query := bigqueryClient.Query("SELECT * FROM `" + config.GCS.Dataset + "." + tableID + "`")
	job, err := query.Run(ctx)
	if err != nil {
		log.Fatalf("Failed to run query: %v", err)
	}

	log.Println("Waiting for query job to complete...")
	status, err := job.Wait(ctx)
	if err != nil {
		log.Fatalf("Failed to wait for job completion: %v", err)
	}
	if err := status.Err(); err != nil {
		log.Fatalf("Query job completed with error: %v", err)
	}

	log.Printf("Reading rows from table: %s", tableID)
	it, err := query.Read(ctx)
	if err != nil {
		log.Fatalf("Failed to read from BigQuery: %v", err)
	}

	// Log BigQuery table schema
	log.Println("BigQuery Table Schema:")
	for _, field := range it.Schema {
		log.Printf("Field Name: %s, Field Type: %s", field.Name, field.Type)
	}
	schemaChan <- it.Schema

	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatalf("Failed to read row from BigQuery: %v", err)
		}
		log.Printf("Row: %v", row)
		dataChan <- row
	}
	log.Printf("Finished reading data for file: %s", file.Name)
}

// DataConsumer receives data from a channel and writes it to the target database.
func DataConsumer(ctx context.Context, pool *pgxpool.Pool, tableName string, columnTypes map[string]string, columns []string, dataChan <-chan []bigquery.Value, schemaChan <-chan bigquery.Schema) {
	log.Printf("Starting DataConsumer for table: %s...", tableName)
	conn, err := pool.Acquire(ctx)
	if err != nil {
		log.Fatalf("Failed to acquire connection from pool: %v", err)
	}
	defer conn.Release()

	schema := <-schemaChan
	log.Printf("BigQuery Schema: %v", schema)

	rows := make([][]interface{}, 0)
	for row := range dataChan {
		rowInterface := make([]interface{}, len(columns))
		for i, colName := range columns {
			idx := utils.GetIndex(schema, colName)
			if idx == -1 {
				log.Printf("Column %s not found in BigQuery schema", colName)
				rowInterface[i] = nil
				continue
			}
			rowInterface[i] = utils.ConvertValue(row[idx], columnTypes[colName])
		}
		rows = append(rows, rowInterface)
	}

	copyCount, err := conn.CopyFrom(
		ctx,
		pgx.Identifier{tableName},
		columns,
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		log.Fatalf("Failed to copy data to target database: %v", err)
	}
	log.Printf("Copied %d rows to target database from table %s.", copyCount, tableName)
	log.Printf("Completed DataConsumer for table: %s.", tableName)
}

// TransferData orchestrates the data transfer from GCS to PostgreSQL.
func TransferData(ctx context.Context, config *utils.Config, pool *pgxpool.Pool, bigqueryClient *bigquery.Client) {
	log.Println("Starting data transfer...")

	for _, file := range config.GCS.Files {
		dataChan := make(chan []bigquery.Value, config.GCS.ConcurrentJobs)
		schemaChan := make(chan bigquery.Schema, 1)

		columnTypes, columns, err := utils.FetchColumns(ctx, pool, file.Table)
		if err != nil {
			log.Fatalf("Failed to fetch columns: %v", err)
		}

		go DataProducer(ctx, bigqueryClient, config, file, dataChan, schemaChan)
		DataConsumer(ctx, pool, file.Table, columnTypes, columns, dataChan, schemaChan)
	}

	log.Println("Data transfer completed.")
}

// InitializeBigQueryClient initializes a BigQuery client.
func InitializeBigQueryClient(ctx context.Context, projectID string) (*bigquery.Client, error) {
	return bigquery.NewClient(ctx, projectID)
}

// InitializePostgresPool initializes a PostgreSQL connection pool.
func InitializePostgresPool(ctx context.Context, connString string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %v", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %v", err)
	}

	return pool, nil
}
