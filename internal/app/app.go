package app

import (
	"context"
	"fmt"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/actor"
	actorAgent "github.com/diogoazevedoo/swordsymphony/internal/agent/actor"
	"github.com/diogoazevedoo/swordsymphony/internal/config"
	"github.com/diogoazevedoo/swordsymphony/internal/domain"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	"github.com/diogoazevedoo/swordsymphony/internal/server"
	"github.com/diogoazevedoo/swordsymphony/internal/server/handler"
	"github.com/diogoazevedoo/swordsymphony/internal/workflow"
)

// Application represents the main application
type Application struct {
	container          *Container
	actorSystem        actor.ActorSystem
	actorRegistry      *actor.Registry
	workflowEngine     *workflow.WorkflowEngine
	server             *server.Server
	isShutdown         bool
	agentConfigService *config.AgentConfigService
	workflowService    *workflow.WorkflowService
}

// NewApplication creates and initializes a new application
func NewApplication(cfg *config.Config) (*Application, error) {
	container, err := NewContainer(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize container: %w", err)
	}

	app := &Application{
		container:      container,
		actorSystem:    actor.NewActorSystem(),
		actorRegistry:  actor.NewRegistry(),
		workflowEngine: workflow.NewWorkflowEngine(),
	}

	app.agentConfigService = config.NewAgentConfigService(
		"./configs/agents",
		app.actorRegistry,
		app.actorSystem,
	)

	app.workflowService = workflow.NewWorkflowService(
		app.workflowEngine,
		"./configs/workflows",
	)

	app.workflowEngine.SetAgentConfigService(app.agentConfigService)

	if err := app.initActorSystem(); err != nil {
		return nil, fmt.Errorf("failed to initialize actor system: %w", err)
	}

	if err := app.initServer(); err != nil {
		return nil, fmt.Errorf("failed to initialize server: %w", err)
	}

	return app, nil
}

// Initialize actor system and agents
func (a *Application) initActorSystem() error {
	if err := a.actorSystem.Start(); err != nil {
		return fmt.Errorf("failed to start actor system: %w", err)
	}

	ctx := context.Background()

	if err := actorAgent.CreateStandardActors(
		a.actorRegistry,
		a.container.AIClient,
		a.container.KnowledgeBase,
		a.container.ResultRepo,
	); err != nil {
		return fmt.Errorf("failed to register standard agents: %w", err)
	}

	if err := actorAgent.CreateSystemActors(ctx, a.actorRegistry, a.actorSystem); err != nil {
		return fmt.Errorf("failed to create system actors: %w", err)
	}

	for _, agentType := range []domain.AgentType{
		domain.IntakeAgentType,
		domain.DiagnosticAgentType,
		domain.TreatmentAgentType,
	} {
		actorConfig := actor.ActorConfig{
			ID:          string(agentType),
			Type:        string(agentType),
			Name:        domain.GetAgentName(agentType),
			Description: fmt.Sprintf("%s for processing medical data", domain.GetAgentName(agentType)),
			Properties:  map[string]any{},
		}

		actorConfig.Properties["result_repository"] = a.container.ResultRepo

		agent, err := a.actorRegistry.Create(ctx, actorConfig, a.actorSystem)
		if err != nil {
			logger.Error("Failed to create agent", "agent", string(agentType), "error", err)
			continue
		}

		if err := a.actorSystem.Register(agent); err != nil {
			logger.Error("Failed to register agent", "agent", string(agentType), "error", err)
			continue
		}

		logger.Info("Successfully registered agent", "agent", string(agentType))
	}

	if err := a.agentConfigService.Initialize(); err != nil {
		logger.Warn("Failed to initialize agent configurations", "error", err)
	}

	if err := a.workflowService.Initialize(); err != nil {
		logger.Warn("Failed to initialize workflows", "error", err)
	}

	orchestratorAddr := actor.Address(domain.OrchestratorAgentType)
	orchestrator, exists := a.actorSystem.GetActor(orchestratorAddr)
	if exists {
		a.workflowEngine.SetOrchestrator(orchestrator)
		a.workflowEngine.SetResultRepository(a.container.ResultRepo)
		a.workflowEngine.SetDocumentRepository(a.container.DocumentRepo)
		logger.Info("Set document repository in workflow engine")
	}

	logger.Info("Listing all registered actors")
	actors := a.actorSystem.GetAllActors()
	for addr := range actors {
		logger.Info("Registered actor", "address", addr)
	}

	return nil
}

// Initialize HTTP server
func (a *Application) initServer() error {
	orchestratorAddr := actor.Address(domain.OrchestratorAgentType)

	h := handler.NewActorHandler(
		a.actorSystem,
		orchestratorAddr,
		a.container.CaseRepo,
		a.container.ResultRepo,
		a.workflowEngine,
	)

	adminH := handler.NewManagementHandler(
		a.agentConfigService,
		a.workflowService,
	)

	callH := handler.NewCallController(
		a.container.CallService,
	)

	docH := handler.NewDocumentHandler(
		a.container.DocumentService,
	)

	address := fmt.Sprintf(":%s", a.container.Config.Server.Port)
	a.server = server.NewServer(address)
	a.server.SetupRoutes(h, adminH, callH, docH)

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
	actorSystemDone := make(chan struct{})

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

	if a.actorSystem != nil {
		go func() {
			logger.Info("Shutting down actor system")
			if err := a.actorSystem.Stop(shutdownCtx); err != nil {
				logger.Error("Error shutting down actor system", "error", err)
			}
			close(actorSystemDone)
		}()
	} else {
		close(actorSystemDone)
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
		case <-actorSystemDone:
			logger.Info("Actor system shutdown complete")
		}
	}

	logger.Info("Application shutdown complete")
	return nil
}
