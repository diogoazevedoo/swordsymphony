package actor

import (
	"context"

	"github.com/diogoazevedoo/swordsymphony/internal/actor"
	"github.com/diogoazevedoo/swordsymphony/internal/ai"
	"github.com/diogoazevedoo/swordsymphony/internal/domain"
	"github.com/diogoazevedoo/swordsymphony/internal/knowledge"
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
	if err := registry.Register("intake", func(ctx context.Context, config actor.ActorConfig, system actor.ActorSystem) (actor.Actor, error) {
		return NewIntakeActor(ctx, config, system)
	}); err != nil {
		return err
	}

	if err := registry.Register("diagnostic", func(ctx context.Context, config actor.ActorConfig, system actor.ActorSystem) (actor.Actor, error) {
		return NewDiagnosticActor(ctx, config, system, aiClient, kb)
	}); err != nil {
		return err
	}

	if err := registry.Register("treatment", func(ctx context.Context, config actor.ActorConfig, system actor.ActorSystem) (actor.Actor, error) {
		return NewTreatmentActor(ctx, config, system, aiClient, kb, resultRepo)
	}); err != nil {
		return err
	}

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
