package app

import (
	"context"
	"fmt"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/agent"
	"github.com/diogoazevedoo/swordsymphony/internal/config"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	"github.com/diogoazevedoo/swordsymphony/internal/orchestrator"
	"github.com/diogoazevedoo/swordsymphony/internal/server"
	"github.com/diogoazevedoo/swordsymphony/internal/server/handler"
)

// Application represents the main application
type Application struct {
	container    *Container
	orchestrator *orchestrator.Orchestrator
	server       *server.Server
	isShutdown   bool
}

// NewApplication creates and initializes a new application
func NewApplication(cfg *config.Config) (*Application, error) {
	container, err := NewContainer(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize container: %w", err)
	}

	app := &Application{
		container: container,
	}

	if err := app.initOrchestrator(); err != nil {
		return nil, fmt.Errorf("failed to initialize orchestrator: %w", err)
	}

	if err := app.initServer(); err != nil {
		return nil, fmt.Errorf("failed to initialize server: %w", err)
	}

	return app, nil
}

// Initialize orchestrator and agents
func (a *Application) initOrchestrator() error {
	a.orchestrator = orchestrator.NewOrchestrator()

	intakeAgent := agent.NewIntakeAgent()
	diagnosticAgent := agent.NewDiagnosticAgent(a.container.AIClient, a.container.KnowledgeBase)
	treatmentAgent := agent.NewTreatmentAgent(a.container.AIClient, a.container.KnowledgeBase, a.container.ResultRepo)

	a.orchestrator.RegisterAgent(intakeAgent)
	a.orchestrator.RegisterAgent(diagnosticAgent)
	a.orchestrator.RegisterAgent(treatmentAgent)

	a.orchestrator.StartProcessing()

	return nil
}

// Initialize HTTP server
func (a *Application) initServer() error {
	h := handler.NewHandler(a.orchestrator, a.container.CaseRepo, a.container.ResultRepo)

	address := fmt.Sprintf(":%s", a.container.Config.Server.Port)
	a.server = server.NewServer(address)
	a.server.SetupRoutes(h)

	return nil
}

// Start runs the application
func (a *Application) Start() error {
	logger.Info("Starting Sword Symphony API",
		"port", a.container.Config.Server.Port)
	return a.server.Start()
}

// Stop gracefully shuts down the application
func (a *Application) Stop() error {
	if a.isShutdown {
		logger.Warn("Application is already shutting down")
		return nil
	}

	a.isShutdown = true
	logger.Info("Starting graceful shutdown")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	serverDone := make(chan struct{})
	orchestratorDone := make(chan struct{})

	if a.server != nil {
		go func() {
			logger.Info("Shutting down HTTP server")
			if err := a.server.Shutdown(shutdownCtx); err != nil {
				logger.Error("Error shutting down server", "error", err)
			}
			close(serverDone)
		}()
	} else {
		close(serverDone)
	}

	if a.orchestrator != nil {
		go func() {
			logger.Info("Shutting down orchestrator")
			if err := a.orchestrator.Shutdown(shutdownCtx); err != nil {
				logger.Error("Error shutting down orchestrator", "error", err)
			}
			close(orchestratorDone)
		}()
	} else {
		close(orchestratorDone)
	}

	select {
	case <-shutdownCtx.Done():
		logger.Warn("Shutdown timed out, forcing exit")
		return fmt.Errorf("shutdown timed out")
	case <-serverDone:
		logger.Info("HTTP server shutdown complete")
		select {
		case <-shutdownCtx.Done():
			return fmt.Errorf("shutdown timed out after server")
		case <-orchestratorDone:
			logger.Info("Orchestrator shutdown complete")
		}
	}

	logger.Info("Application shutdown complete")
	return nil
}
