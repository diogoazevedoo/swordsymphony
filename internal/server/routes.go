package server

import (
	"github.com/diogoazevedoo/swordsymphony/internal/server/handler"
	"github.com/diogoazevedoo/swordsymphony/internal/server/middleware"
)

// SetupRoutes configures all the routes for the API
func (s *Server) SetupRoutes(h *handler.Handler) {
	s.router.Use(middleware.CORS())
	s.router.Use(middleware.RequestLogger())
	s.router.Use(middleware.Recovery())

	s.router.GET("/health", h.HealthCheck)

	api := s.router.Group("/api")
	{
		api.GET("/demo-cases", h.GetDemoCases)

		api.POST("/start-case/:case_id", h.StartCase)
		api.GET("/case-status", h.GetCaseStatus)
		api.GET("/results/:case_id", h.GetResults)

		api.GET("/messages", h.GetMessages)
	}

	s.router.GET("/ws", h.WebSocketHandler)
}
