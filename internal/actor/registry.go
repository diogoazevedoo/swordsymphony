package actor

import (
	"context"
	"fmt"
	"sync"
)

// Registry manages actor types and their factories
type Registry struct {
	creators map[string]ActorCreator
	mu       sync.RWMutex
}

// NewRegistry creates a new actor registry
func NewRegistry() *Registry {
	return &Registry{
		creators: make(map[string]ActorCreator),
	}
}

// Register adds a new actor type to the registry
func (r *Registry) Register(typeName string, creator ActorCreator) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.creators[typeName]; exists {
		return fmt.Errorf("actor type %s is already registered", typeName)
	}

	r.creators[typeName] = creator
	return nil
}

// Create instantiates a new actor from a configuration
func (r *Registry) Create(ctx context.Context, config ActorConfig, system ActorSystem) (Actor, error) {
	r.mu.RLock()
	creator, exists := r.creators[config.Type]
	r.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no creator registered for actor type %s", config.Type)
	}

	return creator(ctx, config, system)
}

// GetTypes returns all registered actor types
func (r *Registry) GetTypes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	types := make([]string, 0, len(r.creators))
	for typeName := range r.creators {
		types = append(types, typeName)
	}

	return types
}
