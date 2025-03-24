package actor

import (
	"context"

	"github.com/diogoazevedoo/swordsymphony/internal/actor"
	"github.com/diogoazevedoo/swordsymphony/internal/ai"
	"github.com/diogoazevedoo/swordsymphony/internal/domain"
	"github.com/diogoazevedoo/swordsymphony/internal/knowledge"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	orchestratorActor "github.com/diogoazevedoo/swordsymphony/internal/orchestrator/actor"
	"github.com/diogoazevedoo/swordsymphony/internal/repository"
)

// CreateStandardActors registers all standard agent actors in the registry
func CreateStandardActors(
	registry *actor.Registry,
	aiClient ai.Client,
	kb *knowledge.MedicalKnowledgeBase,
	resultRepo repository.ResultRepository,
) error {
	logger.Info("Creating standard actors")

	logger.Info("Current registered types", "types", registry.GetTypes())

	if err := registry.Register("intake_agent", func(ctx context.Context, config actor.ActorConfig, system actor.ActorSystem) (actor.Actor, error) {
		logger.Info("Creating intake actor")
		return NewIntakeActor(ctx, config, system)
	}); err != nil {
		logger.Error("Failed to register intake actor", "error", err)
		return err
	}
	logger.Info("Registered intake actor")

	if err := registry.Register("diagnostic_agent", func(ctx context.Context, config actor.ActorConfig, system actor.ActorSystem) (actor.Actor, error) {
		logger.Info("Creating diagnostic actor")
		return NewDiagnosticActor(ctx, config, system, aiClient, kb)
	}); err != nil {
		logger.Error("Failed to register diagnostic actor", "error", err)
		return err
	}
	logger.Info("Registered diagnostic actor")

	if err := registry.Register("treatment_agent", func(ctx context.Context, config actor.ActorConfig, system actor.ActorSystem) (actor.Actor, error) {
		logger.Info("Creating treatment actor")
		return NewTreatmentActor(ctx, config, system, aiClient, kb, resultRepo)
	}); err != nil {
		logger.Error("Failed to register treatment actor", "error", err)
		return err
	}
	logger.Info("Registered treatment actor")

	logger.Info("Updated registered types", "types", registry.GetTypes())

	return nil
}

// CreateSystemActors creates and registers the built-in system actors
func CreateSystemActors(
	ctx context.Context,
	registry *actor.Registry,
	system actor.ActorSystem,
) error {
	orchestratorConfig := actor.ActorConfig{
		ID:          string(domain.OrchestratorAgentType),
		Type:        "orchestrator",
		Name:        domain.GetAgentName(domain.OrchestratorAgentType),
		Description: "Manages communication between agents",
		Properties:  map[string]any{},
	}

	orchestratorActor, err := orchestratorActor.NewOrchestratorActor(ctx, orchestratorConfig, system)
	if err != nil {
		return err
	}

	if err := system.Register(orchestratorActor); err != nil {
		return err
	}

	return nil
}
