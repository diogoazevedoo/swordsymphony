package app

import (
	"fmt"
	"log"

	"github.com/diogoazevedoo/swordsymphony/internal/agent"
	"github.com/diogoazevedoo/swordsymphony/internal/ai"
	"github.com/diogoazevedoo/swordsymphony/internal/config"
	"github.com/diogoazevedoo/swordsymphony/internal/knowledge"
	"github.com/diogoazevedoo/swordsymphony/internal/orchestrator"
	"github.com/diogoazevedoo/swordsymphony/internal/repository"
	"github.com/diogoazevedoo/swordsymphony/internal/repository/memory"
	"github.com/diogoazevedoo/swordsymphony/internal/server"
	"github.com/diogoazevedoo/swordsymphony/internal/server/handler"
)

// Application represents the main application
type Application struct {
	Config        *config.Config
	AIClient      ai.Client
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
	a.CaseRepo = memory.NewCaseRepository()
	a.ResultRepo = memory.NewResultRepository()

	if err := a.CaseRepo.InitializeDemoCases(); err != nil {
		return fmt.Errorf("failed to initialize demo cases: %w", err)
	}

	return nil
}

// Initialize orchestrator and agents
func (a *Application) initOrchestrator() error {
	a.Orchestrator = orchestrator.NewOrchestrator()

	intakeAgent := agent.NewIntakeAgent("intake_agent", "Patient Intake Agent")
	diagnosticAgent := agent.NewDiagnosticAgent("diagnostic_agent", "Diagnostic Agent", a.AIClient, a.KnowledgeBase)
	treatmentAgent := agent.NewTreatmentAgent("treatment_agent", "Treatment Agent", a.AIClient, a.KnowledgeBase, a.ResultRepo)

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
	// TODO
	return nil
}
