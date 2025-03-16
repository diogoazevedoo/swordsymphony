package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/diogoazevedoo/swordsymphony/internal/app"
	"github.com/diogoazevedoo/swordsymphony/internal/config"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	"github.com/joho/godotenv"
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Println("Warning: No .env file found")
	}
}

func main() {
	logger.Init(logger.LogLevel("info"))
	logger.Info("Starting SwordSymphony API")

	cfg, err := config.LoadConfig()
	if err != nil {
		logger.Fatal("Failed to load configuration: %v", "error", err)
	}

	application, err := app.NewApplication(cfg)
	if err != nil {
		logger.Fatal("Failed to initialize application: %v", "error", err)
	}

	go func() {
		if err := application.Start(); err != nil {
			logger.Fatal("Failed to start application: %v", "error", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down gracefully...")
	if err := application.Stop(); err != nil {
		logger.Fatal("Error during shutdown: %v", "error", err)
		os.Exit(1)
	}

	logger.Info("Application stopped successfully")
}
