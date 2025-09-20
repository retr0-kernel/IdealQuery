package api

import (
	"net/http"

	"retr0-kernel/optiquery/logical_plan"
	"retr0-kernel/optiquery/parser"

	"github.com/gin-gonic/gin"
)

type ParseRequest struct {
	Dialect string `json:"dialect" binding:"required,oneof=sql mongo athena"`
	Query   string `json:"query" binding:"required"`
}

type ParseResponse struct {
	LogicalPlan *logical_plan.LogicalPlan `json:"logicalPlan"`
	Error       string                    `json:"error,omitempty"`
}

func ParseHandler(c *gin.Context) {
	var req ParseRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ParseResponse{
			Error: "Invalid request: " + err.Error(),
		})
		return
	}

	var plan *logical_plan.LogicalPlan
	var err error

	switch req.Dialect {
	case "sql":
		plan, err = parser.ParseSQL(req.Query)
	case "mongo":
		plan, err = parser.ParseMongo(req.Query)
	case "athena":
		plan, err = parser.ParseAthena(req.Query)
	default:
		c.JSON(http.StatusBadRequest, ParseResponse{
			Error: "Unsupported dialect: " + req.Dialect,
		})
		return
	}

	if err != nil {
		c.JSON(http.StatusBadRequest, ParseResponse{
			Error: "Parse error: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, ParseResponse{
		LogicalPlan: plan,
	})
}
