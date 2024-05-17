# GCS2Postgres

## Overview

GCS2Postgres is a Go-based solution designed to facilitate the loading of various open data formats stored in Google Cloud Storage (GCS) into a PostgreSQL database. This solution leverages Google BigQuery for data extraction and transformation, providing a seamless and scalable data pipeline.

![GCS2Postgres](assets/GCS2Postgres.webp)

## Features

- Supports multiple file formats: CSV, JSON, Parquet, Avro, Iceberg.
- Utilizes Google BigQuery to create external tables for data processing.
- Implements connection pooling with `pgx/v5` for efficient database operations.
- Configurable via YAML file for flexibility.
- Retrieves PostgreSQL credentials securely from Google Secret Manager.
- Concurrent data processing to maximize performance.

## Configuration

The configuration is managed through a `config.yaml` file. Below is an example configuration:

```yaml
postgres:
  host: "localhost"
  port: 5432
  user: "postgres"
  dbname: "tfmv"
  sslmode: "disable"
  secret_name: "projects/{project_id}/secrets/your_secret_name/versions/latest"

gcs:
  bucket_name: "your_bucket_name"
  project_id: "your_gcp_project_id"
  dataset: "your_bigquery_dataset"
  files:
    - "path/to/file1.csv"
    - "path/to/file2.csv"
    - "path/to/file3.json"
  concurrent_jobs: 3
```

## Usage

1. **Set up the YAML configuration file** with the necessary details, including PostgreSQL connection settings and GCS details.
2. **Ensure the necessary permissions** are granted for accessing GCS, BigQuery, and Google Secret Manager.
3. **Build the project** using the Go build tool:
```bash
go build -o GCS2Postgres
./GCS2Postgres
```

## Example

To load data from multiple files in a GCS bucket into PostgreSQL, ensure your config.yaml is properly set up and run the application. The application will create external tables in BigQuery for each file, fetch the data, and load it into the specified PostgreSQL database.

## Contributing

Contributions are welcome! Please fork this repository, make your changes, and submit a pull request.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.
