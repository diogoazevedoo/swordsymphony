package middleware

import (
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/logger"
	"github.com/gin-gonic/gin"
)

// RequestLogger logs HTTP request details
func RequestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		method := c.Request.Method
		clientIP := c.ClientIP()
		userAgent := c.Request.UserAgent()

		c.Next()

		latency := time.Since(start)
		status := c.Writer.Status()

		if status >= 500 {
			logger.Error("HTTP request",
				"status", status,
				"method", method,
				"path", path,
				"client_ip", clientIP,
				"user_agent", userAgent,
				"latency", latency.String())
		} else if status >= 400 {
			logger.Warn("HTTP request",
				"status", status,
				"method", method,
				"path", path,
				"client_ip", clientIP,
				"latency", latency.String())
		} else {
			logger.Info("HTTP request",
				"status", status,
				"method", method,
				"path", path,
				"client_ip", clientIP,
				"latency", latency.String())
		}
	}
}
