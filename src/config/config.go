package utils

import (
	"os"

	"gopkg.in/yaml.v2"
)

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
	BQ struct {
		ProjectID string  `yaml:"project_id"`
		Dataset   string  `yaml:"dataset"`
		Tables    []Table `yaml:"tables"`
	} `yaml:"bq"`
}

type File struct {
	Name  string `yaml:"name"`
	Table string `yaml:"table"`
}

type Table struct {
	Name  string `yaml:"name"`
	Table string `yaml:"table"`
}

func LoadConfig(configFile string) (*Config, error) {
	var config Config
	data, err := os.ReadFile(configFile)
	if err != nil {
		return nil, err
	}
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}
