package server

import (
	"github.com/diogoazevedoo/swordsymphony/internal/server/handler"
	"github.com/diogoazevedoo/swordsymphony/internal/server/middleware"
)

// SetupRoutes configures all the routes for the API
func (s *Server) SetupRoutes(h *handler.ActorHandler, managementH *handler.ManagementHandler, callH *handler.CallController, docH *handler.DocumentHandler) {
	api := s.router.Group("/api")
	api.Use(middleware.CORS())
	api.Use(middleware.RequestLogger())
	api.Use(middleware.Recovery())
	{
		api.GET("/cases", h.GetAllCases)
		api.GET("/cases/:case_id", h.GetCaseByID)
		api.GET("/demo-cases", h.GetDemoCases)
		api.POST("/start-case/:case_id", h.StartCase)
		api.GET("/case-status", h.GetCaseStatus)
		api.GET("/results/:case_id", h.GetResults)
		api.GET("/debug/repository/:case_id", h.DebugResultRepository)

		api.POST("/upload", h.UploadPatientData)

		api.POST("/workflows/:workflow_id/start/:case_id", h.StartWorkflow)
		api.GET("/workflows", h.GetWorkflows)
		api.GET("/workflows/:workflow_id", h.GetWorkflowDetails)
		api.GET("/workflow-instances/:instance_id", h.GetWorkflowInstance)

		api.GET("/agents", h.GetAgents)
		api.GET("/agents/:agent_id", h.GetAgentDetails)

		api.GET("/messages", h.GetMessages)

		api.POST("/documents/upload/:case_id", docH.UploadDocument)
		api.GET("/documents/case/:case_id", docH.GetDocuments)
		api.GET("/documents/:document_id", docH.GetDocument)
		api.GET("/documents/:document_id/file", docH.GetDocumentFile)
		api.GET("/documents/:document_id/analysis", docH.GetDocumentAnalysis)
		api.DELETE("/documents/:document_id", docH.DeleteDocument)

		call := api.Group("/call")
		{
			call.POST("/start", callH.StartCall)
			call.POST("/process/call/:call_sid", callH.ProcessCallConversation)
			call.POST("/end/:call_sid", callH.EndCall)
			call.GET("/results/:call_sid", callH.GetCallResults)

			call.POST("/webhook", callH.HandleWebhook)
			call.POST("/status", callH.HandleStatusCallback)
			call.POST("/speech", callH.HandleSpeechCallback)

			call.GET("/audio/:call_sid", callH.GetAudio)
		}

		admin := api.Group("/management")
		{
			admin.GET("/agents", managementH.GetAgentConfigs)
			admin.GET("/agents/:id", managementH.GetAgentConfig)
			admin.POST("/agents", managementH.CreateAgentConfig)
			admin.PUT("/agents/:id", managementH.UpdateAgentConfig)
			admin.POST("/agents/:id/deploy", managementH.DeployAgent)

			admin.GET("/workflows", managementH.GetWorkflows)
			admin.GET("/workflows/:id", managementH.GetWorkflow)
			admin.POST("/workflows", managementH.CreateWorkflow)
			admin.PUT("/workflows/:id", managementH.UpdateWorkflow)
			admin.DELETE("/workflows/:id", managementH.DeleteWorkflow)
			admin.GET("/workflows/:id/instances", managementH.GetWorkflowInstances)
			admin.GET("/workflow-instances/:instance_id", managementH.GetWorkflowInstance)
			admin.POST("/workflows/:id/instances", managementH.StartWorkflowInstance)
		}
	}

	s.router.GET("/ws", h.WebSocketHandler)

	s.router.GET("/health", h.HealthCheck)
}
