package app

import (
	"github.com/diogoazevedoo/swordsymphony/internal/ai"
	"github.com/diogoazevedoo/swordsymphony/internal/config"
	"github.com/diogoazevedoo/swordsymphony/internal/knowledge"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	"github.com/diogoazevedoo/swordsymphony/internal/repository"
	"github.com/diogoazevedoo/swordsymphony/internal/repository/memory"
	"github.com/diogoazevedoo/swordsymphony/internal/repository/postgres"
)

// Container manages application dependencies
type Container struct {
	Config        *config.Config
	AIClient      ai.Client
	AIFactory     ai.Factory
	KnowledgeBase *knowledge.MedicalKnowledgeBase
	CaseRepo      repository.CaseRepository
	ResultRepo    repository.ResultRepository
}

// NewContainer creates a new dependency container
func NewContainer(cfg *config.Config) (*Container, error) {
	container := &Container{
		Config:    cfg,
		AIFactory: &ai.DefaultFactory{},
	}

	if err := container.initKnowledgeBase(); err != nil {
		return nil, err
	}

	if err := container.initAIClient(); err != nil {
		return nil, err
	}

	if err := container.initRepositories(); err != nil {
		return nil, err
	}

	return container, nil
}

// Initialize AI client
func (c *Container) initAIClient() error {
	var err error
	c.AIClient, err = c.AIFactory.CreateClient(ai.Provider(c.Config.AI.Provider), c.Config.AI.APIKey)
	return err
}

// Initialize medical knowledge base
func (c *Container) initKnowledgeBase() error {
	var err error
	c.KnowledgeBase, err = knowledge.NewMedicalKnowledgeBase(c.Config.Medical.DataPath)
	return err
}

// Initialize repositories
func (c *Container) initRepositories() error {
	if c.Config.Database.Driver == "postgres" {
		db, err := postgres.NewDB(c.Config.Database)
		if err == nil {
			c.CaseRepo = postgres.NewCaseRepository(db)
			c.ResultRepo = postgres.NewResultRepository(db)
			logger.Info("Using PostgreSQL repositories")

			if err := c.CaseRepo.InitializeDemoCases(); err != nil {
				return err
			}

			return nil
		}

		logger.Warn("Failed to connect to database, falling back to in-memory storage",
			"error", err)
	}

	c.CaseRepo = memory.NewCaseRepository()
	c.ResultRepo = memory.NewResultRepository()
	logger.Info("Using in-memory repositories")

	if err := c.CaseRepo.InitializeDemoCases(); err != nil {
		return err
	}

	return nil
}
