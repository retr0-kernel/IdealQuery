package api

import (
	"net/http"

	"retr0-kernel/optiquery/logical_plan"
	"retr0-kernel/optiquery/optimizer"

	"github.com/gin-gonic/gin"
)

type OptimizeRequest struct {
	LogicalPlan *logical_plan.LogicalPlan `json:"logicalPlan" binding:"required"`
	Strategy    string                    `json:"strategy" binding:"required,oneof=cost rule"`
}

type OptimizeResponse struct {
	OptimizedPlan *logical_plan.LogicalPlan `json:"optimizedPlan"`
	Explain       *optimizer.ExplainResult  `json:"explain"`
	Error         string                    `json:"error,omitempty"`
}

func OptimizeHandler(c *gin.Context) {
	var req OptimizeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, OptimizeResponse{
			Error: "Invalid request: " + err.Error(),
		})
		return
	}

	var optimizedPlan *logical_plan.LogicalPlan
	var explain *optimizer.ExplainResult
	var err error

	switch req.Strategy {
	case "rule":
		optimizedPlan, explain, err = optimizer.OptimizeWithRules(req.LogicalPlan)
	case "cost":
		optimizedPlan, explain, err = optimizer.OptimizeWithCost(req.LogicalPlan)
	default:
		c.JSON(http.StatusBadRequest, OptimizeResponse{
			Error: "Unsupported strategy: " + req.Strategy,
		})
		return
	}

	if err != nil {
		c.JSON(http.StatusInternalServerError, OptimizeResponse{
			Error: "Optimization error: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, OptimizeResponse{
		OptimizedPlan: optimizedPlan,
		Explain:       explain,
	})
}
