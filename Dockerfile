# Stage 1: Build the Go application
FROM golang:1.22-alpine AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy the go.mod and go.sum files
COPY go.mod go.sum ./

# Download the Go module dependencies
RUN go mod download

# Copy the rest of the application source code
COPY src/ ./src

# Build the Go application
RUN cd src && go build -o /app/GCS2Postgres

# Stage 2: Create the final runtime image
FROM alpine:latest

# Set environment variables
ENV GIN_MODE=release

# Set the working directory inside the container
WORKDIR /root/

# Copy the built Go application from the builder stage
COPY --from=builder /app/GCS2Postgres .

# Copy the configuration file
COPY config.yaml .

# Copy the credentials file
COPY sa.json .

# Expose the port the application will run on
EXPOSE 8080

# Run the Go application
CMD ["./GCS2Postgres"]
