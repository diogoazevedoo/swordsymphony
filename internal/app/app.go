package app

import (
	"context"
	"fmt"

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
	logger.Info("Stopping application")

	ctx := context.Background()

	if a.server != nil {
		logger.Info("Shutting down server")
		if err := a.server.Shutdown(ctx); err != nil {
			logger.Error("Error shutting down server", "error", err)
		}
	}

	logger.Info("Application stopped")
	return nil
}
