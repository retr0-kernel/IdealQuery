package config

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	// Database
	DBHost      string
	DBPort      string
	DBName      string
	DBUser      string
	DBPassword  string
	DBSSLMode   string
	DatabaseURL string

	// Application
	Port    string
	GinMode string

	// Logging
	LogLevel  string
	LogFormat string

	// OptiQuery specific
	MaxQueryPlans            int
	OptimizationTimeout      time.Duration
	EnableCostBasedOptimizer bool
	EnableRuleBasedOptimizer bool
}

func LoadConfig() (*Config, error) {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	config := &Config{
		// Database defaults
		DBHost:     getEnv("DB_HOST", "localhost"),
		DBPort:     getEnv("DB_PORT", "5432"),
		DBName:     getEnv("DB_NAME", "optiquery_test"),
		DBUser:     getEnv("DB_USER", "test_user"),
		DBPassword: getEnv("DB_PASSWORD", "test_password"),
		DBSSLMode:  getEnv("DB_SSLMODE", "disable"),

		// Application defaults
		Port:    getEnv("PORT", "8080"),
		GinMode: getEnv("GIN_MODE", "debug"),

		// Logging defaults
		LogLevel:  getEnv("LOG_LEVEL", "debug"),
		LogFormat: getEnv("LOG_FORMAT", "text"),

		// OptiQuery defaults
		MaxQueryPlans:            getEnvAsInt("MAX_QUERY_PLANS", 1000),
		OptimizationTimeout:      getEnvAsDuration("OPTIMIZATION_TIMEOUT", 30*time.Second),
		EnableCostBasedOptimizer: getEnvAsBool("ENABLE_COST_BASED_OPTIMIZER", true),
		EnableRuleBasedOptimizer: getEnvAsBool("ENABLE_RULE_BASED_OPTIMIZER", true),
	}

	config.DatabaseURL = getEnv("DATABASE_URL",
		fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
			config.DBUser, config.DBPassword, config.DBHost,
			config.DBPort, config.DBName, config.DBSSLMode))

	return config, nil
}

func (c *Config) Validate() error {
	if c.DBHost == "" {
		return fmt.Errorf("DB_HOST is required")
	}
	if c.DBPort == "" {
		return fmt.Errorf("DB_PORT is required")
	}
	if c.DBName == "" {
		return fmt.Errorf("DB_NAME is required")
	}
	if c.DBUser == "" {
		return fmt.Errorf("DB_USER is required")
	}
	if c.DBPassword == "" {
		return fmt.Errorf("DB_PASSWORD is required")
	}
	return nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvAsBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}

func getEnvAsDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}
