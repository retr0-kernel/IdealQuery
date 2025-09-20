package catalog

import (
	"fmt"
	"sync"
)

type DataType string

const (
	DataTypeInt     DataType = "int"
	DataTypeFloat   DataType = "float"
	DataTypeString  DataType = "string"
	DataTypeBoolean DataType = "boolean"
	DataTypeDate    DataType = "date"
)

type Column struct {
	Name      string   `json:"name"`
	DataType  DataType `json:"data_type"`
	Nullable  bool     `json:"nullable"`
	NDV       *int64   `json:"ndv,omitempty"`
	MinValue  *string  `json:"min_value,omitempty"`
	MaxValue  *string  `json:"max_value,omitempty"`
	Histogram []Bucket `json:"histogram,omitempty"`
	NullCount *int64   `json:"null_count,omitempty"`
}

type Bucket struct {
	LowerBound string  `json:"lower_bound"`
	UpperBound string  `json:"upper_bound"`
	Count      int64   `json:"count"`
	Frequency  float64 `json:"frequency"`
}

type TableSchema struct {
	Name     string            `json:"name"`
	Columns  []Column          `json:"columns"`
	RowCount int64             `json:"row_count"`
	Indexes  []Index           `json:"indexes,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type Index struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Unique  bool     `json:"unique"`
	Type    string   `json:"type"` // btree, hash, etc.
}

type CatalogManager struct {
	tables map[string]*TableSchema
	mu     sync.RWMutex
}

func NewCatalogManager() *CatalogManager {
	return &CatalogManager{
		tables: make(map[string]*TableSchema),
	}
}

func (cm *CatalogManager) AddTable(schema *TableSchema) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if _, exists := cm.tables[schema.Name]; exists {
		return fmt.Errorf("table %s already exists", schema.Name)
	}

	cm.tables[schema.Name] = schema
	return nil
}

func (cm *CatalogManager) GetTable(tableName string) (*TableSchema, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	table, exists := cm.tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	tableCopy := *table
	return &tableCopy, nil
}

func (cm *CatalogManager) GetAllTables() []string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	tables := make([]string, 0, len(cm.tables))
	for name := range cm.tables {
		tables = append(tables, name)
	}
	return tables
}

func (cm *CatalogManager) UpdateTableStats(tableName string, rowCount int64, columnStats map[string]Column) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	table, exists := cm.tables[tableName]
	if !exists {
		return fmt.Errorf("table %s not found", tableName)
	}

	table.RowCount = rowCount
	for i, col := range table.Columns {
		if stats, hasStats := columnStats[col.Name]; hasStats {
			table.Columns[i].NDV = stats.NDV
			table.Columns[i].MinValue = stats.MinValue
			table.Columns[i].MaxValue = stats.MaxValue
			table.Columns[i].Histogram = stats.Histogram
			table.Columns[i].NullCount = stats.NullCount
		}
	}

	return nil
}

func (cm *CatalogManager) GetColumnStats(tableName, columnName string) (*Column, error) {
	table, err := cm.GetTable(tableName)
	if err != nil {
		return nil, err
	}

	for _, col := range table.Columns {
		if col.Name == columnName {
			return &col, nil
		}
	}

	return nil, fmt.Errorf("column %s not found in table %s", columnName, tableName)
}

func (cm *CatalogManager) EstimateSelectivity(tableName, columnName, operator string, value interface{}) (float64, error) {
	colStats, err := cm.GetColumnStats(tableName, columnName)
	if err != nil {
		return 0.0, err
	}

	switch operator {
	case "=":
		if colStats.NDV != nil && *colStats.NDV > 0 {
			return 1.0 / float64(*colStats.NDV), nil
		}
		return 0.1, nil
	case "<", ">", "<=", ">=":
		return 0.33, nil
	case "LIKE":
		return 0.1, nil
	default:
		return 0.5, nil
	}
}
