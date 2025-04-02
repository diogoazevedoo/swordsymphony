package app

import (
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/ai"
	"github.com/diogoazevedoo/swordsymphony/internal/communication/call"
	"github.com/diogoazevedoo/swordsymphony/internal/communication/deepgram"
	"github.com/diogoazevedoo/swordsymphony/internal/communication/elevenlabs"
	"github.com/diogoazevedoo/swordsymphony/internal/communication/email"
	"github.com/diogoazevedoo/swordsymphony/internal/communication/twilio"
	"github.com/diogoazevedoo/swordsymphony/internal/config"
	"github.com/diogoazevedoo/swordsymphony/internal/conversation"
	"github.com/diogoazevedoo/swordsymphony/internal/knowledge"
	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	"github.com/diogoazevedoo/swordsymphony/internal/repository"
	"github.com/diogoazevedoo/swordsymphony/internal/repository/memory"
	"github.com/diogoazevedoo/swordsymphony/internal/repository/postgres"
	"github.com/diogoazevedoo/swordsymphony/internal/workflow"
)

// Container manages application dependencies
type Container struct {
	Config              *config.Config
	AIClient            ai.Client
	AIFactory           ai.Factory
	KnowledgeBase       *knowledge.MedicalKnowledgeBase
	CaseRepo            repository.CaseRepository
	ResultRepo          repository.ResultRepository
	TwilioClient        *twilio.Client
	ElevenLabsClient    *elevenlabs.Client
	DeepgramClient      *deepgram.Client
	EmailSender         *email.Sender
	ConversationManager *conversation.ConversationManager
	WorkflowService     *conversation.WorkflowService
	CallService         *call.Service
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

	if err := container.initCommunicationServices(); err != nil {
		return nil, err
	}

	if err := container.initConversationServices(); err != nil {
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
	dataPath := c.Config.Medical.DataPath
	if dataPath == "" {
		dataPath = "./data/medical"
	}

	c.KnowledgeBase, err = knowledge.NewMedicalKnowledgeBase(dataPath)
	if err != nil {
		logger.Warn("Failed to load medical knowledge base from path, using embedded data",
			"path", dataPath, "error", err)
		c.KnowledgeBase, err = knowledge.NewMedicalKnowledgeBase("embedded")
	}

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

// Initialize communication services (Twilio, ElevenLabs, Deepgram, Email)
func (c *Container) initCommunicationServices() error {
	// Initialize Twilio client
	c.TwilioClient = twilio.NewClient(
		c.Config.Twilio.AccountSID,
		c.Config.Twilio.AuthToken,
		c.Config.Twilio.PhoneNumber,
	)

	// Initialize ElevenLabs client
	c.ElevenLabsClient = elevenlabs.NewClient(
		c.Config.ElevenLabs.APIKey,
		elevenlabs.WithVoiceID(c.Config.ElevenLabs.VoiceID),
		elevenlabs.WithModelID(c.Config.ElevenLabs.ModelID),
		elevenlabs.WithStability(c.Config.ElevenLabs.Stability),
		elevenlabs.WithSimilarity(c.Config.ElevenLabs.SimilarityBoost),
	)

	// Initialize Deepgram client
	c.DeepgramClient = deepgram.NewClient(
		c.Config.Deepgram.APIKey,
		deepgram.WithLanguage(c.Config.Deepgram.Language),
		deepgram.WithModel(c.Config.Deepgram.Model),
		deepgram.WithTier(c.Config.Deepgram.Tier),
	)

	// Initialize Email sender
	c.EmailSender = email.NewSender(
		c.Config.Email.SMTPServer,
		c.Config.Email.SMTPPort,
		c.Config.Email.Username,
		c.Config.Email.Password,
		c.Config.Email.FromAddress,
		c.Config.Email.FromName,
		c.Config.Email.TemplatesDir,
	)

	// Load email templates
	if err := c.EmailSender.LoadTemplates(); err != nil {
		logger.Warn("Failed to load email templates", "error", err)
	}

	logger.Info("Communication services initialized")
	return nil
}

// Initialize conversation and workflow services
func (c *Container) initConversationServices() error {
	// Initialize conversation manager
	c.ConversationManager = conversation.NewConversationManager(c.AIClient)

	// Initialize workflow service
	workflowEngine := workflow.NewWorkflowEngine()
	workflowService := workflow.NewWorkflowService(workflowEngine, c.Config.Medical.DataPath)

	if err := workflowService.Initialize(); err != nil {
		logger.Warn("Failed to initialize workflow service", "error", err)
	}

	c.WorkflowService = conversation.NewWorkflowService(workflowEngine, workflowService)

	// Initialize call service
	c.CallService = call.NewService(
		c.TwilioClient,
		c.ElevenLabsClient,
		c.DeepgramClient,
		c.EmailSender,
		c.ConversationManager,
		c.WorkflowService,
		c.ResultRepo,
		c.AIClient,
		c.Config.Twilio.WebhookBaseURL,
	)

	// Start the cleanup scheduler for call sessions
	c.CallService.StartCleanupScheduler(5 * time.Minute)

	logger.Info("Conversation services initialized")
	return nil
}
