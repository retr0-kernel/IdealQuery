#!/bin/bash

# OptiQuery Podman Setup Script
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    case $1 in
        "SUCCESS") echo -e "${GREEN}✓ SUCCESS${NC}: $2" ;;
        "ERROR") echo -e "${RED}✗ ERROR${NC}: $2" ;;
        "INFO") echo -e "${BLUE}ℹ INFO${NC}: $2" ;;
        "WARN") echo -e "${YELLOW}⚠ WARN${NC}: $2" ;;
    esac
}

print_status "INFO" "Setting up OptiQuery with Podman..."

# Check if podman is installed
if ! command -v podman &> /dev/null; then
    print_status "ERROR" "Podman is not installed. Please install Podman Desktop first."
    exit 1
fi

# Load environment variables
if [ -f "backend/.env" ]; then
    export $(cat backend/.env | grep -v '#' | xargs)
    print_status "SUCCESS" "Loaded environment variables from backend/.env"
else
    print_status "WARN" "No .env file found. Using default values."
    export DB_HOST=localhost
    export DB_PORT=5432
    export DB_NAME=optiquery_test
    export DB_USER=test_user
    export DB_PASSWORD=test_password
fi

# Create network
print_status "INFO" "Creating Podman network..."
if podman network exists optiquery-network 2>/dev/null; then
    print_status "WARN" "Network 'optiquery-network' already exists"
else
    podman network create optiquery-network
    print_status "SUCCESS" "Created network 'optiquery-network'"
fi

# Stop and remove existing containers
print_status "INFO" "Cleaning up existing containers..."
podman stop optiquery-postgres 2>/dev/null || true
podman rm optiquery-postgres 2>/dev/null || true

# Start PostgreSQL container
print_status "INFO" "Starting PostgreSQL container..."
podman run -d \
    --name optiquery-postgres \
    --network optiquery-network \
    -e POSTGRES_DB=$DB_NAME \
    -e POSTGRES_USER=$DB_USER \
    -e POSTGRES_PASSWORD=$DB_PASSWORD \
    -p $DB_PORT:5432 \
    -v $(pwd)/examples/init.sql:/docker-entrypoint-initdb.d/init.sql:Z \
    postgres:15

print_status "SUCCESS" "PostgreSQL container started"

# Wait for PostgreSQL to be ready
print_status "INFO" "Waiting for PostgreSQL to be ready..."
for i in {1..30}; do
    if podman exec optiquery-postgres pg_isready -U $DB_USER -d $DB_NAME > /dev/null 2>&1; then
        print_status "SUCCESS" "PostgreSQL is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        print_status "ERROR" "PostgreSQL not ready after 30 seconds"
        podman logs optiquery-postgres
        exit 1
    fi
    sleep 1
done

# Test database connection
print_status "INFO" "Testing database connection..."
if podman exec optiquery-postgres psql -U $DB_USER -d $DB_NAME -c "SELECT 1;" > /dev/null 2>&1; then
    print_status "SUCCESS" "Database connection successful"
else
    print_status "ERROR" "Database connection failed"
    exit 1
fi

# Show connection information
echo
print_status "INFO" "Database setup complete!"
echo "Connection details for DBeaver:"
echo "  Host: localhost"
echo "  Port: $DB_PORT"
echo "  Database: $DB_NAME"
echo "  Username: $DB_USER"
echo "  Password: $DB_PASSWORD"
echo
echo "Connection URL: postgres://$DB_USER:$DB_PASSWORD@localhost:$DB_PORT/$DB_NAME"
echo
print_status "INFO" "Run './scripts/stop-podman.sh' to stop the containers"
print_status "INFO" "Run 'podman logs optiquery-postgres' to view database logs"