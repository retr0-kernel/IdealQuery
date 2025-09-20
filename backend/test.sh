#!/bin/bash

# OptiQuery Comprehensive API Testing Script
BASE_URL="http://localhost:8080"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test counter
TESTS_RUN=0
TESTS_PASSED=0

# Function to print colored output
print_status() {
    case $1 in
        "PASS") echo -e "${GREEN}✓ PASS${NC}: $2" ;;
        "FAIL") echo -e "${RED}✗ FAIL${NC}: $2" ;;
        "INFO") echo -e "${BLUE}ℹ INFO${NC}: $2" ;;
        "WARN") echo -e "${YELLOW}⚠ WARN${NC}: $2" ;;
    esac
}

# Function to test API endpoint
test_endpoint() {
    local method=$1
    local endpoint=$2
    local data=$3
    local expected_status=$4
    local test_name=$5

    TESTS_RUN=$((TESTS_RUN + 1))

    if [ -n "$data" ]; then
        response=$(curl -s -w "HTTPSTATUS:%{http_code}" -X $method \
            -H "Content-Type: application/json" \
            -d "$data" \
            "$BASE_URL$endpoint")
    else
        response=$(curl -s -w "HTTPSTATUS:%{http_code}" -X $method "$BASE_URL$endpoint")
    fi

    http_code=$(echo $response | tr -d '\n' | sed -e 's/.*HTTPSTATUS://')
    body=$(echo $response | sed -e 's/HTTPSTATUS\:.*//g')

    if [ "$http_code" -eq "$expected_status" ]; then
        print_status "PASS" "$test_name (HTTP $http_code)"
        TESTS_PASSED=$((TESTS_PASSED + 1))
        return 0
    else
        print_status "FAIL" "$test_name (Expected HTTP $expected_status, got HTTP $http_code)"
        echo "Response: $body"
        return 1
    fi
}

echo "=== OptiQuery Comprehensive API Testing ==="
echo "Testing backend endpoints at $BASE_URL"
echo

# Wait for server to be ready
print_status "INFO" "Waiting for server to be ready..."
for i in {1..30}; do
    if curl -s "$BASE_URL/health" > /dev/null 2>&1; then
        print_status "PASS" "Server is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        print_status "FAIL" "Server not ready after 30 seconds"
        exit 1
    fi
    sleep 1
done

# Test 1: Health check
print_status "INFO" "Testing health check..."
test_endpoint "GET" "/health" "" 200 "Health check endpoint"

# Test 2: Load sample data
print_status "INFO" "Loading sample data..."
test_endpoint "POST" "/api/load-sample-data" "" 200 "Load sample data"

# Test 3: Catalog management
print_status "INFO" "Testing catalog management..."
test_endpoint "GET" "/api/catalog/tables" "" 200 "Get all tables"
test_endpoint "GET" "/api/catalog/table/customers/stats" "" 200 "Get customers table stats"

# Test 4: Add custom table
custom_table='{
  "name": "test_table",
  "row_count": 1000,
  "columns": [
    {"name": "id", "data_type": "int", "nullable": false},
    {"name": "name", "data_type": "string", "nullable": false}
  ]
}'
test_endpoint "POST" "/api/catalog/table" "$custom_table" 201 "Add custom table"

# Test 5: Simple SQL parsing
print_status "INFO" "Testing SQL parsing..."
simple_query='{
  "dialect": "sql",
  "query": "SELECT name, age FROM customers WHERE age > 25"
}'
test_endpoint "POST" "/api/parse" "$simple_query" 200 "Parse simple SELECT query"

# Test 6: Join query parsing
join_query='{
  "dialect": "sql",
  "query": "SELECT c.name, o.total_amount FROM customers c JOIN orders o ON c.customer_id = o.customer_id WHERE c.country = '\''USA'\''"
}'
test_endpoint "POST" "/api/parse" "$join_query" 200 "Parse JOIN query"

# Test 7: Complex query parsing
complex_query='{
  "dialect": "sql",
  "query": "SELECT c.name, COUNT(o.order_id) as order_count FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id GROUP BY c.customer_id, c.name ORDER BY order_count DESC LIMIT 10"
}'
test_endpoint "POST" "/api/parse" "$complex_query" 200 "Parse complex query with GROUP BY and ORDER BY"

# Test 8: Rule-based optimization
print_status "INFO" "Testing optimization..."
rule_optimization='{
  "strategy": "rule",
  "logicalPlan": {
    "id": "test_plan",
    "node_type": "filter",
    "predicate": {
      "expression": {
        "type": "binary_op",
        "value": ">",
        "left": {"type": "column", "value": "age"},
        "right": {"type": "literal", "value": 25}
      }
    },
    "children": [{
      "id": "scan_customers",
      "node_type": "scan",
      "table_name": "customers"
    }]
  }
}'
test_endpoint "POST" "/api/optimize" "$rule_optimization" 200 "Rule-based optimization"

# Test 9: Cost-based optimization
cost_optimization='{
  "strategy": "cost",
  "logicalPlan": {
    "id": "test_join",
    "node_type": "join",
    "join_type": "inner",
    "join_condition": {
      "left": {"type": "column", "value": "customers.customer_id"},
      "right": {"type": "column", "value": "orders.customer_id"},
      "operator": "="
    },
    "children": [
      {
        "id": "scan_customers",
        "node_type": "scan",
        "table_name": "customers"
      },
      {
        "id": "scan_orders",
        "node_type": "scan",
        "table_name": "orders"
      }
    ]
  }
}'
test_endpoint "POST" "/api/optimize" "$cost_optimization" 200 "Cost-based optimization"

# Test 10: Execution simulation
print_status "INFO" "Testing execution simulation..."
simulation_request='{
  "connector": "postgres",
  "plan": {
    "id": "test_scan",
    "node_type": "scan",
    "table_name": "customers",
    "estimated_rows": 50000
  },
  "options": {"explain_analyze": true}
}'
test_endpoint "POST" "/api/simulate" "$simulation_request" 200 "PostgreSQL execution simulation"

# Test 11: MongoDB simulation
mongo_simulation='{
  "connector": "mongo",
  "plan": {
    "id": "test_aggregate",
    "node_type": "aggregate",
    "group_by": [{"name": "country"}],
    "aggregates": [{"type": "count", "alias": "customer_count"}],
    "children": [{
      "id": "scan_customers",
      "node_type": "scan",
      "table_name": "customers"
    }]
  },
  "options": {}
}'
test_endpoint "POST" "/api/simulate" "$mongo_simulation" 200 "MongoDB execution simulation"

# Test 12: Error handling tests
print_status "INFO" "Testing error handling..."
invalid_query='{
  "dialect": "sql",
  "query": "INVALID SQL SYNTAX HERE"
}'
test_endpoint "POST" "/api/parse" "$invalid_query" 400 "Invalid SQL query handling"

invalid_optimization='{
  "strategy": "invalid_strategy",
  "logicalPlan": {}
}'
test_endpoint "POST" "/api/optimize" "$invalid_optimization" 400 "Invalid optimization strategy"

# Test 13: Edge cases
print_status "INFO" "Testing edge cases..."
empty_query='{
  "dialect": "sql",
  "query": ""
}'
test_endpoint "POST" "/api/parse" "$empty_query" 400 "Empty query handling"

nonexistent_table='{
  "connector": "postgres",
  "plan": {
    "id": "test_scan",
    "node_type": "scan",
    "table_name": "nonexistent_table"
  }
}'
test_endpoint "POST" "/api/simulate" "$nonexistent_table" 200 "Nonexistent table simulation"

# Summary
echo
echo "=== Test Results ==="
print_status "INFO" "Tests run: $TESTS_RUN"
print_status "INFO" "Tests passed: $TESTS_PASSED"
print_status "INFO" "Tests failed: $((TESTS_RUN - TESTS_PASSED))"

if [ $TESTS_PASSED -eq $TESTS_RUN ]; then
    print_status "PASS" "All tests passed! 🎉"
    exit 0
else
    print_status "FAIL" "Some tests failed"
    exit 1
fi