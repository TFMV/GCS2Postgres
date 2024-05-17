package config

import (
	"os"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Postgres struct {
		Host       string `yaml:"host"`
		Port       int    `yaml:"port"`
		User       string `yaml:"user"`
		DbName     string `yaml:"dbname"`
		SslMode    string `yaml:"sslmode"`
		SecretName string `yaml:"secret_name"`
	} `yaml:"postgres"`
	GCS struct {
		BucketName string   `yaml:"bucket_name"`
		Files      []string `yaml:"files"`
	} `yaml:"gcs"`
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
