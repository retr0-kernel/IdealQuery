package api

import (
	"net/http"
	_ "strconv"

	"retr0-kernel/optiquery/catalog"

	"github.com/gin-gonic/gin"
)

func NewAddTableHandler(cm *catalog.CatalogManager) gin.HandlerFunc {
	return func(c *gin.Context) {
		var schema catalog.TableSchema
		if err := c.ShouldBindJSON(&schema); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		if err := cm.AddTable(&schema); err != nil {
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusCreated, gin.H{"message": "Table added successfully"})
	}
}

func NewGetTablesHandler(cm *catalog.CatalogManager) gin.HandlerFunc {
	return func(c *gin.Context) {
		tables := cm.GetAllTables()
		c.JSON(http.StatusOK, gin.H{"tables": tables})
	}
}

func NewGetTableStatsHandler(cm *catalog.CatalogManager) gin.HandlerFunc {
	return func(c *gin.Context) {
		tableName := c.Param("name")

		table, err := cm.GetTable(tableName)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, table)
	}
}

func NewUpdateStatsHandler(cm *catalog.CatalogManager) gin.HandlerFunc {
	return func(c *gin.Context) {
		tableName := c.Param("name")

		var updateReq struct {
			RowCount    int64                     `json:"row_count"`
			ColumnStats map[string]catalog.Column `json:"column_stats"`
		}

		if err := c.ShouldBindJSON(&updateReq); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		if err := cm.UpdateTableStats(tableName, updateReq.RowCount, updateReq.ColumnStats); err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"message": "Statistics updated successfully"})
	}
}
