package db

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"strings"

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
	} `yaml:"postgres"`
	GCS struct {
		BucketName     string `yaml:"bucket_name"`
		ProjectID      string `yaml:"project_id"`
		Dataset        string `yaml:"dataset"`
		Files          []File `yaml:"files"`
		ConcurrentJobs int    `yaml:"concurrent_jobs"`
	} `yaml:"gcs"`
}

type File struct {
	Name  string `yaml:"name"`
	Table string `yaml:"table"`
}

// LoadConfig loads configuration from a YAML file
func LoadConfig(filename string) (*Config, error) {
	log.Println("Starting LoadConfig...")
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
	log.Println("Completed LoadConfig.")
	return &config, nil
}

// FetchSecret retrieves the secret value from Google Secret Manager
func FetchSecret(ctx context.Context, secretName string) (string, error) {
	log.Println("Starting FetchSecret...")
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		return "", err
	}
	defer client.Close()

	req := &secretmanagerpb.AccessSecretVersionRequest{
		Name: secretName,
	}
	result, err := client.AccessSecretVersion(ctx, req)
	if err != nil {
		return "", err
	}
	log.Println("Completed FetchSecret.")
	return string(result.Payload.Data), nil
}

// FetchColumns fetches column names and types for the given table from the source database.
func FetchColumns(ctx context.Context, pool *pgxpool.Pool, tableName string) (map[string]string, []string, error) {
	log.Println("Starting FetchColumns...")
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, "SELECT column_name, data_type FROM information_schema.columns WHERE table_name=$1", tableName)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	columnTypes := make(map[string]string)
	var columns []string
	for rows.Next() {
		var columnName, dataType string
		if err := rows.Scan(&columnName, &dataType); err != nil {
			return nil, nil, err
		}
		columnTypes[columnName] = dataType
		columns = append(columns, columnName)
	}
	log.Println("Completed FetchColumns.")
	return columnTypes, columns, nil
}

// DataProducer creates an external table in BigQuery and fetches data.
func DataProducer(ctx context.Context, bigqueryClient *bigquery.Client, config *Config, file File, dataChan chan<- []bigquery.Value) {
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
func DataConsumer(ctx context.Context, pool *pgxpool.Pool, tableName string, columnTypes map[string]string, columns []string, dataChan <-chan []bigquery.Value) {
	log.Printf("Starting DataConsumer for table: %s...", tableName)
	conn, err := pool.Acquire(ctx)
	if err != nil {
		log.Fatalf("Failed to acquire connection from pool: %v", err)
	}
	defer conn.Release()

	rows := make([][]interface{}, 0)
	for row := range dataChan {
		rowInterface := make([]interface{}, len(row))
		for i, v := range row {
			columnName := columns[i]
			dataType := columnTypes[columnName]
			convertedValue, err := convertValue(v, dataType)
			if err != nil {
				log.Printf("Unexpected data type for column %s: %v", columnName, v)
				continue
			}
			rowInterface[i] = convertedValue
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

func convertValue(value bigquery.Value, dataType string) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	switch dataType {
	case "text", "varchar":
		if str, ok := value.(string); ok {
			return str, nil
		}
	case "int4", "integer":
		if num, ok := value.(int64); ok {
			return int32(num), nil
		}
	case "float8", "double precision":
		if num, ok := value.(float64); ok {
			return num, nil
		}
	// Add more cases as needed for other data types
	default:
		return value, fmt.Errorf("unsupported data type: %s", dataType)
	}
	return nil, fmt.Errorf("cannot convert value: %v to type: %s", value, dataType)
}
