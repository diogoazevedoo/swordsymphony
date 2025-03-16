package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

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

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	sig := <-sigChan
	logger.Info("Received signal", "signal", sig.String())

	shutdownTimeout := 30 * time.Second
	logger.Info("Initiating graceful shutdown", "timeout", shutdownTimeout.String())

	shutdownComplete := make(chan struct{})

	go func() {
		if err := application.Stop(); err != nil {
			logger.Error("Error during shutdown", "error", err)
			os.Exit(1)
		}
		close(shutdownComplete)
	}()

	select {
	case <-shutdownComplete:
		logger.Info("Graceful shutdown completed")
	case <-time.After(shutdownTimeout):
		logger.Error("Shutdown timed out, forcing exit")
		os.Exit(1)
	}
}
