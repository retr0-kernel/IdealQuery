package main

import (
	"log"
	"net/http"

	"retr0-kernel/optiquery/api"
	"retr0-kernel/optiquery/catalog"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

func main() {

	catalogManager := catalog.NewCatalogManager()
	r := gin.Default()
	config := cors.DefaultConfig()
	config.AllowOrigins = []string{"http://localhost:5173"}
	config.AllowMethods = []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"}
	config.AllowHeaders = []string{"Origin", "Content-Type", "Accept", "Authorization"}
	r.Use(cors.New(config))

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "healthy"})
	})

	apiGroup := r.Group("/api")
	{
		apiGroup.POST("/parse", api.ParseHandler)
		apiGroup.POST("/optimize", api.OptimizeHandler)
		apiGroup.POST("/simulate", api.SimulateHandler)
		apiGroup.POST("/catalog/table", api.NewAddTableHandler(catalogManager))
		apiGroup.GET("/catalog/tables", api.NewGetTablesHandler(catalogManager))
		apiGroup.GET("/catalog/table/:name/stats", api.NewGetTableStatsHandler(catalogManager))
		apiGroup.POST("/catalog/table/:name/stats", api.NewUpdateStatsHandler(catalogManager))
	}
	log.Println("OptiQuery backend starting on :8080")
	log.Fatal(r.Run(":8080"))
}
