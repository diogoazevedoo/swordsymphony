package server

import (
	"github.com/diogoazevedoo/swordsymphony/internal/server/handler"
	"github.com/diogoazevedoo/swordsymphony/internal/server/middleware"
)

// SetupRoutes configures all the routes for the API
func (s *Server) SetupRoutes(h *handler.ActorHandler, adminH *handler.AdminHandler) {
	api := s.router.Group("/api")
	api.Use(middleware.CORS())
	api.Use(middleware.RequestLogger())
	api.Use(middleware.Recovery())
	{
		api.GET("/demo-cases", h.GetDemoCases)
		api.POST("/start-case/:case_id", h.StartCase)
		api.GET("/case-status", h.GetCaseStatus)
		api.GET("/results/:case_id", h.GetResults)

		api.POST("/upload", h.UploadPatientData)

		api.POST("/workflows/:workflow_id/start/:case_id", h.StartWorkflow)
		api.GET("/workflows", h.GetWorkflows)
		api.GET("/workflows/:workflow_id", h.GetWorkflowDetails)
		api.GET("/workflow-instances/:instance_id", h.GetWorkflowInstance)

		api.GET("/agents", h.GetAgents)
		api.GET("/agents/:agent_id", h.GetAgentDetails)

		api.GET("/messages", h.GetMessages)

		admin := api.Group("/admin")
		{
			admin.GET("/agents", adminH.GetAgentConfigs)
			admin.GET("/agents/:id", adminH.GetAgentConfig)
			admin.POST("/agents", adminH.CreateAgentConfig)
			admin.PUT("/agents/:id", adminH.UpdateAgentConfig)
			admin.POST("/agents/:id/deploy", adminH.DeployAgent)

			admin.GET("/workflows", adminH.GetWorkflows)
			admin.GET("/workflows/:id", adminH.GetWorkflow)
			admin.POST("/workflows", adminH.CreateWorkflow)
			admin.PUT("/workflows/:id", adminH.UpdateWorkflow)
			admin.DELETE("/workflows/:id", adminH.DeleteWorkflow)
			admin.GET("/workflows/:id/instances", adminH.GetWorkflowInstances)
			admin.GET("/workflow-instances/:instance_id", adminH.GetWorkflowInstance)
			admin.POST("/workflows/:id/instances", adminH.StartWorkflowInstance)
		}
	}

	s.router.GET("/ws", h.WebSocketHandler)

	s.router.GET("/health", h.HealthCheck)
}
