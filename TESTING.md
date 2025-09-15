# Testing with Docker

This project includes Docker configuration to run tests in a complete environment with all dependencies, including Apache Spark.

## Prerequisites

- Docker
- Docker Compose

## Running Tests

### Option 1: Using npm scripts (Recommended)

```bash
# Run all tests in Docker environment
npm run test:docker

# Clean up Docker containers and volumes after testing
npm run test:docker:cleanup

# View logs from the test containers
npm run test:docker:logs
```

### Option 2: Using Docker Compose directly

```bash
# Build and run tests
docker compose -f docker-compose.test.yml up --build --abort-on-container-exit test-runner

# Clean up afterwards
docker compose -f docker-compose.test.yml down -v
```

## How it works

The Docker test setup includes:

1. **Spark Server Container**: Runs Apache Spark with Spark Connect enabled on port 15002
2. **Test Runner Container**: Runs the Node.js application with all tests

The test runner waits for the Spark server to be healthy before running tests. The containers share a Docker network so they can communicate with each other.

## Environment Variables

- `SPARK_CONNECT_URL`: Set to `sc://spark:15002` to connect to the Spark container
- Other environment variables can be added to the `docker-compose.test.yml` file

## Data and Certificates

- `./example_data` is copied into both containers as `/data` for test data access
- `./spark-server/certs` is copied into containers for SSL/TLS certificates
- All files are embedded in the container images for consistency and isolation

## Troubleshooting

### Tests failing to connect to Spark

- Check that the Spark container is healthy: `docker compose -f docker-compose.test.yml ps`
- View Spark logs: `docker compose -f docker-compose.test.yml logs spark`

### Build failures

- Ensure all dependencies are listed in `package.json`
- Check that TypeScript compilation succeeds: `npm run build`

### Permission issues

- The Spark container runs as user 1001, make sure files are accessible
