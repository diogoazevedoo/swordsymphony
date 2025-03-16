package app

import (
	"fmt"
	"log"

	"github.com/diogoazevedoo/swordsymphony/internal/agent"
	"github.com/diogoazevedoo/swordsymphony/internal/ai"
	"github.com/diogoazevedoo/swordsymphony/internal/config"
	"github.com/diogoazevedoo/swordsymphony/internal/knowledge"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	"github.com/diogoazevedoo/swordsymphony/internal/orchestrator"
	"github.com/diogoazevedoo/swordsymphony/internal/repository"
	"github.com/diogoazevedoo/swordsymphony/internal/repository/memory"
	"github.com/diogoazevedoo/swordsymphony/internal/repository/postgres"
	"github.com/diogoazevedoo/swordsymphony/internal/server"
	"github.com/diogoazevedoo/swordsymphony/internal/server/handler"
)

// Application represents the main application
type Application struct {
	Config        *config.Config
	AIClient      ai.Client
	DB            *postgres.DB
	KnowledgeBase *knowledge.MedicalKnowledgeBase
	CaseRepo      repository.CaseRepository
	ResultRepo    repository.ResultRepository
	Orchestrator  *orchestrator.Orchestrator
	Server        *server.Server
}

// NewApplication creates and initializes a new application
func NewApplication(cfg *config.Config) (*Application, error) {
	app := &Application{
		Config: cfg,
	}

	if err := app.initDatabase(); err != nil {
		log.Printf("Warning: Failed to connect to database: %v. Using in-memory storage instead.", err)
	}

	if err := app.initAIClient(); err != nil {
		return nil, fmt.Errorf("failed to initialize AI client: %w", err)
	}

	if err := app.initKnowledgeBase(); err != nil {
		return nil, fmt.Errorf("failed to initialize knowledge base: %w", err)
	}

	if err := app.initRepositories(); err != nil {
		return nil, fmt.Errorf("failed to initialize repositories: %w", err)
	}

	if err := app.initOrchestrator(); err != nil {
		return nil, fmt.Errorf("failed to initialize orchestrator: %w", err)
	}

	if err := app.initServer(); err != nil {
		return nil, fmt.Errorf("failed to initialize server: %w", err)
	}

	return app, nil
}

// Initialize database connection
func (a *Application) initDatabase() error {
	var err error
	a.DB, err = postgres.NewDB(a.Config.Database)
	if err != nil {
		logger.Warn("Failed to connect to database, using in-memory storage",
			"error", err)
		return err
	}

	logger.Info("Successfully connected to database")
	return nil
}

// Initialize AI client
func (a *Application) initAIClient() error {
	var err error
	a.AIClient, err = ai.NewClient(ai.Provider(a.Config.AI.Provider), a.Config.AI.APIKey)
	return err
}

// Initialize medical knowledge base
func (a *Application) initKnowledgeBase() error {
	var err error
	a.KnowledgeBase, err = knowledge.NewMedicalKnowledgeBase(a.Config.Medical.DataPath)
	return err
}

// Initialize repositories
func (a *Application) initRepositories() error {
	if a.DB != nil {
		a.CaseRepo = postgres.NewCaseRepository(a.DB)
		a.ResultRepo = postgres.NewResultRepository(a.DB)

		logger.Info("Using PostgreSQL repositories")
	} else {
		a.CaseRepo = memory.NewCaseRepository()
		a.ResultRepo = memory.NewResultRepository()

		logger.Info("Using in-memory repositories")
	}

	if err := a.CaseRepo.InitializeDemoCases(); err != nil {
		return fmt.Errorf("failed to initialize demo cases: %w", err)
	}

	return nil
}

// Initialize orchestrator and agents
func (a *Application) initOrchestrator() error {
	a.Orchestrator = orchestrator.NewOrchestrator()

	intakeAgent := agent.NewIntakeAgent()
	diagnosticAgent := agent.NewDiagnosticAgent(a.AIClient, a.KnowledgeBase)
	treatmentAgent := agent.NewTreatmentAgent(a.AIClient, a.KnowledgeBase, a.ResultRepo)

	a.Orchestrator.RegisterAgent(intakeAgent)
	a.Orchestrator.RegisterAgent(diagnosticAgent)
	a.Orchestrator.RegisterAgent(treatmentAgent)

	a.Orchestrator.StartProcessing()

	return nil
}

// Initialize HTTP server
func (a *Application) initServer() error {
	h := handler.NewHandler(a.Orchestrator, a.CaseRepo, a.ResultRepo)

	address := fmt.Sprintf(":%s", a.Config.Server.Port)
	a.Server = server.NewServer(address)
	a.Server.SetupRoutes(h)

	return nil
}

// Start runs the application
func (a *Application) Start() error {
	log.Printf("Starting Sword Symphony API on port %s", a.Config.Server.Port)
	return a.Server.Start()
}

// Stop gracefully shuts down the application
func (a *Application) Stop() error {
	logger.Info("Stopping application")

	if a.DB != nil {
		logger.Info("Closing database connection")
		if err := a.DB.Close(); err != nil {
			logger.Error("Error closing database connection", "error", err)
		}
	}

	if a.Server != nil {
		logger.Info("Shutting down server")
		if err := a.Server.Shutdown(); err != nil {
			logger.Error("Error shutting down server", "error", err)
		}
	}

	logger.Info("Application stopped")
	return nil
}
