#!/bin/bash

# OptiQuery Podman Cleanup Script
set -e

GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

print_status() {
    case $1 in
        "SUCCESS") echo -e "${GREEN}✓ SUCCESS${NC}: $2" ;;
        "INFO") echo -e "${BLUE}ℹ INFO${NC}: $2" ;;
    esac
}

print_status "INFO" "Stopping OptiQuery containers..."

# Stop containers
podman stop optiquery-postgres 2>/dev/null || true
podman stop optiquery-backend 2>/dev/null || true

# Remove containers
podman rm optiquery-postgres 2>/dev/null || true
podman rm optiquery-backend 2>/dev/null || true

print_status "SUCCESS" "Containers stopped and removed"
print_status "INFO" "Network 'optiquery-network' preserved for future use"
print_status "INFO" "Run './scripts/setup-podman.sh' to restart"