package server

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/gin-gonic/gin"
)

// Server represents the HTTP server for the API
type Server struct {
	router *gin.Engine
	http   *http.Server
}

// NewServer creates a new http server
func NewServer(addr string) *Server {
	router := gin.Default()

	return &Server{
		router: router,
		http: &http.Server{
			Addr:    addr,
			Handler: router,
		},
	}
}

// Router returns the Gin router instance
func (s *Server) Router() *gin.Engine {
	return s.router
}

// Start begins listening for HTTP requests
func (s *Server) Start() error {
	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, os.Interrupt)
		<-quit

		log.Println("Server is shutting down...")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := s.http.Shutdown(ctx); err != nil {
			log.Fatalf("Server forced to shutdown: %v", err)
		}
	}()

	log.Printf("Server is running on %s", s.http.Addr)
	if err := s.http.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}

	return nil
}
