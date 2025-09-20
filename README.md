# IdealQuery: A Visual SQL Query Optimizer ⚡️

IdealQuery is a high-performance, teaching-grade query optimization engine designed to demystify database performance. It features a powerful Go backend that parses, analyzes, and optimizes SQL queries, coupled with an interactive React frontend to visualize and compare different execution plans. This tool provides invaluable insights into both rule-based and cost-based optimization strategies.

## Features

-   **SQL Parsing**: Transforms raw SQL queries into a structured logical plan representation.
-   **Rule-Based Optimization**: Applies a set of heuristic rules (like predicate pushdown) to improve the query plan.
-   **Cost-Based Optimization**: Utilizes table statistics and a cost model to find the most efficient execution plan by exploring different join orders and physical operators.
-   **Execution Simulation**: Simulates query execution against different database connectors (PostgreSQL, MongoDB) to provide realistic performance metrics without running the actual query.
-   **Interactive Plan Visualization**: Renders query plans as interactive tree diagrams using D3.js, allowing for easy comparison and node inspection.
-   **Database Catalog Management**: Manages table schemas and statistics, which are crucial for the cost-based optimizer.

## Technologies Used

| Technology                               | Description                                                     |
| ---------------------------------------- | --------------------------------------------------------------- |
| **[Go](https://golang.org/)**            | The core language for the high-performance backend API.           |
| **[Gin](https://gin-gonic.com/)**        | A lightweight and fast HTTP web framework for Go.               |
| **[PostgreSQL](https://www.postgresql.org/)** | The primary relational database used for testing and simulation. |
| **[Docker](https://www.docker.com/)**    | For containerizing and orchestrating the application services.    |
| **[React](https://reactjs.org/)**        | A JavaScript library for building the user interface.           |
| **[Vite](https://vitejs.dev/)**          | A modern, fast frontend build tool.                             |
| **[D3.js](https://d3js.org/)**           | A JavaScript library for producing dynamic data visualizations. |
| **[Zustand](https://zustand-demo.pmnd.rs/)** | A small, fast, and scalable state-management solution for React. |

## Usage

The simplest way to get IdealQuery running is with Docker. This will spin up the Go backend, the React frontend, and a PostgreSQL instance with pre-loaded sample data.

1.  **Clone the repository:**
    ```sh
    git clone https://github.com/your-username/IdealQuery.git
    cd IdealQuery
    ```

2.  **Start the services using Docker Compose:**
    ```sh
    docker-compose up --build
    ```

3.  **Access the application:**
    -   Frontend UI: [http://localhost:5173](http://localhost:5173)
    -   Backend API: [http://localhost:8080](http://localhost:8080)

Once the application is running, you can write a SQL query in the editor, click "Run Query", and explore the original, rule-based, and cost-based execution plans in the visualization panel.

---

# IdealQuery API

## Overview
This document outlines the API for the IdealQuery backend. The API is built with Go and the Gin framework, providing endpoints for parsing, optimizing, and simulating SQL queries, as well as managing the database catalog.

## Getting Started
### Environment Variables
Create a `.env` file in the `backend/` directory and populate it with the following variables. A `backend/.env.example` file is provided for reference.

```env
# Database Configuration
DB_HOST=postgres
DB_PORT=5432
DB_NAME=IdealQuery_test
DB_USER=test_user
DB_PASSWORD=test_password
DB_SSLMODE=disable

# App Config
GIN_MODE=debug
PORT=8080

# Connection String (auto-generated if not provided)
# DATABASE_URL=postgres://test_user:test_password@postgres:5432/IdealQuery_test?sslmode=disable

# Logging
LOG_LEVEL=debug
LOG_FORMAT=json

# IdealQuery Configuration
MAX_QUERY_PLANS=1000
OPTIMIZATION_TIMEOUT=30s
ENABLE_COST_BASED_OPTIMIZER=true
ENABLE_RULE_BASED_OPTIMIZER=true
```

## API Documentation
### Base URL
`http://localhost:8080`

### Endpoints
#### GET /health
Checks the health of the API server.

**Request**:
None.

**Response**:
```json
{
  "status": "healthy"
}
```

**Errors**:
- None.

---

#### POST /api/parse
Parses a query string from a specific dialect into a logical plan.

**Request**:
```json
{
  "dialect": "sql",
  "query": "SELECT name FROM customers WHERE age > 30"
}
```
*   `dialect` (string, required): The query language. Must be one of `sql`, `mongo`, `athena`.
*   `query` (string, required): The query string to parse.

**Response**:
```json
{
  "logicalPlan": {
    "id": "node_2",
    "node_type": "project",
    "children": [
      {
        "id": "node_1",
        "node_type": "filter",
        "children": [
          {
            "id": "node_0",
            "node_type": "scan",
            "table_name": "customers",
            "metadata": {}
          }
        ],
        "predicate": {
          "expression": {
            "type": "binary_op",
            "value": ">",
            "left": { "type": "column", "value": "age" },
            "right": { "type": "literal", "value": 30 }
          }
        },
        "metadata": {}
      }
    ],
    "projections": [{ "name": "name" }],
    "metadata": {}
  }
}
```

**Errors**:
- 400 Bad Request: If the request payload is invalid, the dialect is unsupported, or a parsing error occurs.

---

#### POST /api/optimize
Optimizes a given logical plan using a specified strategy.

**Request**:
```json
{
  "logicalPlan": {
    "id": "node_1",
    "node_type": "filter",
    "children": [
      {
        "id": "node_0",
        "node_type": "scan",
        "table_name": "customers"
      }
    ],
    "predicate": {
      "expression": {
        "type": "binary_op",
        "value": ">",
        "left": { "type": "column", "value": "age" },
        "right": { "type": "literal", "value": 30 }
      }
    }
  },
  "strategy": "cost"
}
```
*   `logicalPlan` (object, required): The logical plan structure to optimize.
*   `strategy` (string, required): The optimization strategy. Must be one of `cost`, `rule`.

**Response**:
```json
{
  "optimizedPlan": {
    "id": "node_...",
    "node_type": "scan",
    "table_name": "customers",
    "metadata": {
        "physical_operator": "sequential"
    }
  },
  "explain": {
    "applied_rules": ["PredicatePushdown", "CostBasedOptimization"],
    "steps": [...],
    "statistics": { "total_rules_applied": 2 }
  }
}
```

**Errors**:
- 400 Bad Request: If the request payload is invalid or the strategy is unsupported.
- 500 Internal Server Error: If an error occurs during the optimization process.

---

#### POST /api/simulate
Simulates the execution of a query plan for a specific data connector.

**Request**:
```json
{
  "plan": {
    "id": "node_0",
    "node_type": "scan",
    "table_name": "customers",
    "estimated_rows": 1000
  },
  "connector": "postgres",
  "options": {}
}
```
*   `plan` (object, required): The logical plan to simulate.
*   `connector` (string, required): The target connector. Must be one of `postgres`, `mongo`.
*   `options` (object, optional): Connector-specific simulation options.

**Response**:
```json
{
  "metrics": {
    "execution_time": 10000000,
    "rows_processed": 1000,
    "rows_returned": 1000,
    "cpu_time": 10000,
    "io_operations": 10,
    "memory_used": 100000,
    "network_traffic": 0,
    "operator_metrics": {},
    "connector": "postgres",
    "simulation_only": true
  }
}
```

**Errors**:
- 400 Bad Request: If the request payload is invalid.
- 500 Internal Server Error: If an error occurs during simulation.

---

#### POST /api/catalog/table
Adds a new table schema to the catalog.

**Request**:
```json
{
  "name": "new_table",
  "columns": [
    { "name": "id", "data_type": "int", "nullable": false },
    { "name": "data", "data_type": "string", "nullable": true }
  ],
  "row_count": 5000
}
```

**Response**:
```json
{
  "message": "Table added successfully"
}
```

**Errors**:
- 400 Bad Request: Invalid schema format.
- 409 Conflict: If a table with the same name already exists.

---

#### GET /api/catalog/tables
Retrieves a list of all table names in the catalog.

**Request**:
None.

**Response**:
```json
{
  "tables": [
    "customers",
    "orders",
    "products",
    "suppliers"
  ]
}
```

**Errors**:
- None.

---

#### GET /api/catalog/table/:name/stats
Retrieves the schema and statistics for a specific table.

**Request**:
None. URL parameter `name` is required. Example: `/api/catalog/table/customers/stats`.

**Response**:
```json
{
    "name": "customers",
    "columns": [
        { "name": "customer_id", "data_type": "int", "nullable": false },
        { "name": "name", "data_type": "string", "nullable": false }
    ],
    "row_count": 5000,
    "indexes": []
}
```

**Errors**:
- 404 Not Found: If the specified table does not exist.

---

#### POST /api/catalog/table/:name/stats
Updates the statistics for an existing table.

**Request**:
URL parameter `name` is required. Example: `/api/catalog/table/customers/stats`.
```json
{
  "row_count": 6000,
  "column_stats": {
    "name": {
      "ndv": 5500
    }
  }
}
```

**Response**:
```json
{
  "message": "Statistics updated successfully"
}
```

**Errors**:
- 400 Bad Request: If the update payload is invalid.
- 404 Not Found: If the specified table does not exist.
