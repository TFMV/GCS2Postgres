package db

import (
	"context"
	"log"
	"path/filepath"
	"strings"
	"sync"

	"os"

	"cloud.google.com/go/bigquery"
	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/api/iterator"
	"gopkg.in/yaml.v3"
)

// Config represents the YAML configuration structure
type Config struct {
	Postgres struct {
		Host       string `yaml:"host"`
		Port       int    `yaml:"port"`
		User       string `yaml:"user"`
		DBName     string `yaml:"dbname"`
		SSLMode    string `yaml:"sslmode"`
		SecretName string `yaml:"secret_name"`
		Password   string
	} `yaml:"postgres"`
	GCS struct {
		BucketName     string   `yaml:"bucket_name"`
		ProjectID      string   `yaml:"project_id"`
		Dataset        string   `yaml:"dataset"`
		Files          []string `yaml:"files"`
		ConcurrentJobs int      `yaml:"concurrent_jobs"`
	} `yaml:"gcs"`
}

// LoadConfig loads configuration from a YAML file and retrieves the secret from Secret Manager
func LoadConfig(filename string) (*Config, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var config Config
	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		return nil, err
	}

	// Retrieve the secret for the Postgres password
	ctx := context.Background()
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	accessRequest := &secretmanagerpb.AccessSecretVersionRequest{
		Name: config.Postgres.SecretName,
	}

	result, err := client.AccessSecretVersion(ctx, accessRequest)
	if err != nil {
		return nil, err
	}

	config.Postgres.Password = string(result.Payload.Data)

	return &config, nil
}

// FetchColumns fetches column names for the given table from the source database.
func FetchColumns(ctx context.Context, pool *pgxpool.Pool, tableName string) ([]string, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, "SELECT column_name FROM information_schema.columns WHERE table_name=$1", tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}

	return columns, nil
}

// DataProducer creates an external table in BigQuery and fetches data.
func DataProducer(ctx context.Context, bigqueryClient *bigquery.Client, config *Config, fileName string, dataChan chan<- []bigquery.Value, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(dataChan)

	// Create an external table in BigQuery for the specified file
	gcsFilePath := "gs://" + config.GCS.BucketName + "/" + fileName
	tableID := strings.TrimSuffix(filepath.Base(fileName), filepath.Ext(fileName))

	externalConfig := &bigquery.ExternalDataConfig{
		SourceFormat: bigquery.DataFormat(strings.ToUpper(filepath.Ext(fileName)[1:])),
		SourceURIs:   []string{gcsFilePath},
	}

	tableRef := bigqueryClient.Dataset(config.GCS.Dataset).Table(tableID)
	if err := tableRef.Create(ctx, &bigquery.TableMetadata{
		ExternalDataConfig: externalConfig,
	}); err != nil {
		log.Fatalf("Failed to create external table in BigQuery: %v", err)
	}

	query := bigqueryClient.Query("SELECT * FROM `" + config.GCS.Dataset + "." + tableID + "`")
	it, err := query.Read(ctx)
	if err != nil {
		log.Fatalf("Failed to read from BigQuery: %v", err)
	}

	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatalf("Failed to read row from BigQuery: %v", err)
		}
		dataChan <- row
	}
}

// DataConsumer receives data from a channel and writes it to the target database.
func DataConsumer(ctx context.Context, pool *pgxpool.Pool, tableName string, columns []string, dataChan <-chan []bigquery.Value, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := pool.Acquire(ctx)
	if err != nil {
		log.Fatalf("Failed to acquire connection from pool: %v", err)
	}
	defer conn.Release()

	rows := make([][]interface{}, 0)
	for row := range dataChan {
		rowInterface := make([]interface{}, len(row))
		for i, v := range row {
			rowInterface[i] = v
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
}
