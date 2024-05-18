package utils

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"cloud.google.com/go/bigquery"
	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/jackc/pgx/v5/pgxpool"
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

// BuildConnString builds the PostgreSQL connection string from the config and secret
func BuildConnString(config *Config, secret string) string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		config.Postgres.Host, config.Postgres.Port, config.Postgres.User, secret, config.Postgres.DBName, config.Postgres.SSLMode)
}

// FetchColumns fetches column names for the given table from the source database.
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
	log.Printf("PostgreSQL Table: %s, Columns: %v, Column Types: %v", tableName, columns, columnTypes)
	log.Println("Completed FetchColumns.")
	return columnTypes, columns, nil
}

// getIndex returns the index of a column name in the BigQuery schema
func GetIndex(schema bigquery.Schema, colName string) int {
	for i, field := range schema {
		if strings.ToLower(field.Name) == strings.ToLower(colName) {
			return i
		}
	}
	return -1
}

// convertValue converts BigQuery values to appropriate PostgreSQL types
func ConvertValue(value bigquery.Value, dataType string) interface{} {
	if value == nil {
		return nil
	}

	switch dataType {
	case "text", "varchar":
		if str, ok := value.(string); ok {
			return str
		}
	case "int4", "integer":
		if num, ok := value.(int64); ok {
			return int32(num)
		}
	case "float8", "double precision":
		if num, ok := value.(float64); ok {
			return num
		}
	default:
		return value
	}
	return value
}
