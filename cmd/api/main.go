package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/diogoazevedoo/swordsymphony/internal/app"
	"github.com/diogoazevedoo/swordsymphony/internal/config"
	"github.com/joho/godotenv"
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Println("Warning: No .env file found")
	}
}

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	application, err := app.NewApplication(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize application: %v", err)
	}

	go func() {
		if err := application.Start(); err != nil {
			log.Fatalf("Failed to start application: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down gracefully...")
	if err := application.Stop(); err != nil {
		log.Fatalf("Error during shutdown: %v", err)
	}
}
