package actor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/domain"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
)

// ActorSystem manages the lifecycle of actors and delivers messages
type ActorSystem interface {
	// Register adds an actor to the system
	Register(actor Actor) error

	// Unregister removes an actor from the system
	Unregister(address Address) error

	// Send delivers a message to an actor
	Send(envelope *Envelope) error

	// Broadcast sends a message to all actors
	Broadcast(msg domain.Message) error

	// AddressForName looks up an actor address by name
	AddressForName(name string) (Address, bool)

	// GetActor retrieves an actor by address
	GetActor(address Address) (Actor, bool)

	// GetAllActors returns all registered actors
	GetAllActors() map[Address]Actor

	// Start initializes the actor system
	Start() error

	// Stop gracefully shuts down the actor system
	Stop(ctx context.Context) error
}

// DefaultActorSystem is the standard implementation of ActorSystem
type DefaultActorSystem struct {
	actors      map[Address]Actor
	mailboxes   map[Address]chan *Envelope
	deadLetters chan *Envelope
	registry    map[string]Address
	mu          sync.RWMutex
	isRunning   bool
	wg          sync.WaitGroup
}

// NewActorSystem creates a new actor system
func NewActorSystem() ActorSystem {
	return &DefaultActorSystem{
		actors:      make(map[Address]Actor),
		mailboxes:   make(map[Address]chan *Envelope),
		deadLetters: make(chan *Envelope, 100),
		registry:    make(map[string]Address),
		isRunning:   false,
	}
}

// Register adds an actor to the system
func (s *DefaultActorSystem) Register(actor Actor) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.isRunning {
		return errors.New("actor system is not running")
	}

	address := actor.Address()

	if _, exists := s.actors[address]; exists {
		return fmt.Errorf("actor with address %s already exists", address)
	}

	s.actors[address] = actor
	s.mailboxes[address] = make(chan *Envelope, 100)

	config, ok := actor.(interface{ Config() ActorConfig })
	if ok {
		actorConfig := config.Config()
		s.registry[actorConfig.Name] = address
	}

	if err := actor.Start(); err != nil {
		delete(s.actors, address)
		delete(s.mailboxes, address)
		return fmt.Errorf("failed to start actor: %w", err)
	}

	s.wg.Add(1)
	go s.processMessages(address)

	logger.Info("Actor registered", "address", address)
	return nil
}

// Unregister removes an actor from the system
func (s *DefaultActorSystem) Unregister(address Address) error {
	s.mu.Lock()
	actor, exists := s.actors[address]
	if !exists {
		s.mu.Unlock()
		return fmt.Errorf("no actor found with address %s", address)
	}

	mailbox := s.mailboxes[address]
	delete(s.actors, address)
	delete(s.mailboxes, address)

	for name, addr := range s.registry {
		if addr == address {
			delete(s.registry, name)
		}
	}
	s.mu.Unlock()

	if err := actor.Stop(); err != nil {
		logger.Warn("Error stopping actor", "address", address, "error", err)
	}

	close(mailbox)
	logger.Info("Actor unregistered", "address", address)
	return nil
}

// Send delivers a message to an actor
func (s *DefaultActorSystem) Send(envelope *Envelope) error {
	if envelope == nil {
		return errors.New("envelope cannot be nil")
	}

	s.mu.RLock()
	mailbox, exists := s.mailboxes[envelope.Recipient]
	s.mu.RUnlock()

	if !exists {
		s.deadLetters <- envelope
		return fmt.Errorf("no actor found with address %s", envelope.Recipient)
	}

	select {
	case mailbox <- envelope:
		return nil
	default:
		s.deadLetters <- envelope
		return fmt.Errorf("mailbox full for actor %s", envelope.Recipient)
	}
}

// Broadcast sends a message to all actors
func (s *DefaultActorSystem) Broadcast(msg domain.Message) error {
	s.mu.RLock()
	addresses := make([]Address, 0, len(s.actors))
	for addr := range s.actors {
		addresses = append(addresses, addr)
	}
	s.mu.RUnlock()

	for _, addr := range addresses {
		envelope := NewEnvelope(Address("system"), addr, msg)
		if err := s.Send(envelope); err != nil {
			logger.Warn("Error broadcasting message", "recipient", addr, "error", err)
		}
	}

	return nil
}

// AddressForName looks up an actor address by name
func (s *DefaultActorSystem) AddressForName(name string) (Address, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	address, exists := s.registry[name]
	return address, exists
}

// GetActor retrieves an actor by address
func (s *DefaultActorSystem) GetActor(address Address) (Actor, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	actor, exists := s.actors[address]
	return actor, exists
}

// GetAllActors returns all registered actors
func (s *DefaultActorSystem) GetAllActors() map[Address]Actor {
	s.mu.RLock()
	defer s.mu.RUnlock()

	actors := make(map[Address]Actor, len(s.actors))
	for addr, actor := range s.actors {
		actors[addr] = actor
	}

	return actors
}

// Start initializes the actor system
func (s *DefaultActorSystem) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isRunning {
		return errors.New("actor system is already running")
	}

	s.isRunning = true
	go s.processDeadLetters()

	logger.Info("Actor system started")
	return nil
}

// Stop gracefully shuts down the actor system
func (s *DefaultActorSystem) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.isRunning {
		s.mu.Unlock()
		return errors.New("actor system is not running")
	}

	addresses := make([]Address, 0, len(s.actors))
	for addr := range s.actors {
		addresses = append(addresses, addr)
	}
	s.isRunning = false
	s.mu.Unlock()

	for _, addr := range addresses {
		if err := s.Unregister(addr); err != nil {
			logger.Warn("Error unregistering actor", "address", addr, "error", err)
		}
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("Actor system stopped")
		return nil
	case <-ctx.Done():
		return fmt.Errorf("timeout waiting for actors to stop: %w", ctx.Err())
	}
}

// processMessages handles message delivery for an actor
func (s *DefaultActorSystem) processMessages(address Address) {
	defer s.wg.Done()

	s.mu.RLock()
	mailbox := s.mailboxes[address]
	actor := s.actors[address]
	s.mu.RUnlock()

	for envelope := range mailbox {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := actor.Receive(ctx, envelope)
		cancel()

		if err != nil {
			logger.Error("Error processing message",
				"actor", address,
				"error", err,
				"message_type", envelope.Message.MessageType)
		}
	}

	logger.Info("Actor message processor stopped", "address", address)
}

// processDeadLetters handles messages that couldn't be delivered
func (s *DefaultActorSystem) processDeadLetters() {
	for envelope := range s.deadLetters {
		logger.Warn("Dead letter",
			"sender", envelope.Sender,
			"recipient", envelope.Recipient,
			"message_type", envelope.Message.MessageType)
	}
}
