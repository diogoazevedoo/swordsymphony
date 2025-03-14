package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds all application configuration
type Config struct {
	Server   ServerConfig
	Database DatabaseConfig
	AI       AIConfig
	Medical  MedicalConfig
}

// ServerConfig holds server-related configuration
type ServerConfig struct {
	Port         string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// DatabaseConfig holds database-related configuration
type DatabaseConfig struct {
	Driver   string
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

// AIConfig holds AI-related configuration
type AIConfig struct {
	Provider    string
	APIKey      string
	ModelName   string
	MaxTokens   int
	Temperature float64
}

// MedicalConfig holds medical knowledge base configuration
type MedicalConfig struct {
	DataPath string
}

// LoadConfig loads configuration from environment variables
func LoadConfig() (*Config, error) {
	// Server config
	port := getEnv("PORT", "8080")
	readTimeoutStr := getEnv("SERVER_READ_TIMEOUT", "5")
	writeTimeoutStr := getEnv("SERVER_WRITE_TIMEOUT", "10")

	readTimeout, err := strconv.Atoi(readTimeoutStr)
	if err != nil {
		readTimeout = 5
	}

	writeTimeout, err := strconv.Atoi(writeTimeoutStr)
	if err != nil {
		writeTimeout = 10
	}

	dbDriver := getEnv("DB_DRIVER", "postgres")
	dbHost := getEnv("DB_HOST", "localhost")
	dbPortStr := getEnv("DB_PORT", "5432")
	dbPort, err := strconv.Atoi(dbPortStr)
	if err != nil {
		dbPort = 5432
	}
	dbUser := getEnv("DB_USER", "postgres")
	dbPassword := getEnv("DB_PASSWORD", "")
	dbName := getEnv("DB_NAME", "swordsymphony")

	aiProvider := getEnv("AI_PROVIDER", "openai")
	aiAPIKey := getEnv("OPENAI_API_KEY", "")
	aiModelName := getEnv("AI_MODEL_NAME", "gpt-4")
	aiMaxTokensStr := getEnv("AI_MAX_TOKENS", "1024")
	aiTemperatureStr := getEnv("AI_TEMPERATURE", "0.3")

	aiMaxTokens, err := strconv.Atoi(aiMaxTokensStr)
	if err != nil {
		aiMaxTokens = 1024
	}

	aiTemperature, err := strconv.ParseFloat(aiTemperatureStr, 64)
	if err != nil {
		aiTemperature = 0.3
	}

	medicalDataPath := getEnv("MEDICAL_DATA_PATH", "")

	return &Config{
		Server: ServerConfig{
			Port:         port,
			ReadTimeout:  time.Duration(readTimeout) * time.Second,
			WriteTimeout: time.Duration(writeTimeout) * time.Second,
		},
		Database: DatabaseConfig{
			Driver:   dbDriver,
			Host:     dbHost,
			Port:     dbPort,
			User:     dbUser,
			Password: dbPassword,
			Database: dbName,
		},
		AI: AIConfig{
			Provider:    aiProvider,
			APIKey:      aiAPIKey,
			ModelName:   aiModelName,
			MaxTokens:   aiMaxTokens,
			Temperature: aiTemperature,
		},
		Medical: MedicalConfig{
			DataPath: medicalDataPath,
		},
	}, nil
}

// Helper to get environment variable with fallback
func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
