package api

import (
	"net/http"

	"retr0-kernel/optiquery/logical_plan"
	"retr0-kernel/optiquery/simulator"

	"github.com/gin-gonic/gin"
)

type SimulateRequest struct {
	Plan      *logical_plan.LogicalPlan `json:"plan" binding:"required"`
	Connector string                    `json:"connector" binding:"required,oneof=postgres mongo"`
	Options   map[string]interface{}    `json:"options"`
}

type SimulateResponse struct {
	Metrics *simulator.ExecutionMetrics `json:"metrics"`
	Error   string                      `json:"error,omitempty"`
}

func SimulateHandler(c *gin.Context) {
	var req SimulateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, SimulateResponse{
			Error: "Invalid request: " + err.Error(),
		})
		return
	}

	metrics, err := simulator.SimulateExecution(req.Plan, req.Connector, req.Options)
	if err != nil {
		c.JSON(http.StatusInternalServerError, SimulateResponse{
			Error: "Simulation error: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, SimulateResponse{
		Metrics: metrics,
	})
}
