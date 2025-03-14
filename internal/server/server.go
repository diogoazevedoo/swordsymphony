package server

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// Server represents the HTTP server for the API
type Server struct {
	router          *gin.Engine
	http            *http.Server
	shutdownTimeout time.Duration
}

// ServerOptions contains configuration for the server
type ServerOptions struct {
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
}

// DefaultServerOptions returns default server options
func DefaultServerOptions() ServerOptions {
	return ServerOptions{
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    10 * time.Second,
		ShutdownTimeout: 5 * time.Second,
	}
}

// NewServer creates a new http server
func NewServer(addr string) *Server {
	return NewServerWithOptions(addr, DefaultServerOptions())
}

// NewServerWithOptions creates a new http server with custom options
func NewServerWithOptions(addr string, options ServerOptions) *Server {
	router := gin.Default()

	return &Server{
		router: router,
		http: &http.Server{
			Addr:         addr,
			Handler:      router,
			ReadTimeout:  options.ReadTimeout,
			WriteTimeout: options.WriteTimeout,
		},
		shutdownTimeout: options.ShutdownTimeout,
	}
}

// Router returns the Gin router instance
func (s *Server) Router() *gin.Engine {
	return s.router
}

// Start begins listening for HTTP requests
func (s *Server) Start() error {
	log.Printf("Server is running on %s", s.http.Addr)
	if err := s.http.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}

	return nil
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown() error {
	log.Println("Server is shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
	defer cancel()

	return s.http.Shutdown(ctx)
}
