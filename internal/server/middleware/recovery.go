package middleware

import (
	"fmt"
	"net/http"
	"runtime/debug"

	"github.com/diogoazevedoo/swordsymphony/internal/errors"
	"github.com/gin-gonic/gin"
)

// Recovery middleware handles panics and provides graceful recovery
func Recovery() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if r := recover(); r != nil {
				stackTrace := debug.Stack()

				fmt.Printf("Panic recovered: %v\n%s", r, stackTrace)

				var statusCode int
				var errorResponse gin.H

				switch err := r.(type) {
				case *errors.AppError:
					statusCode = err.HTTPStatusCode()
					errorResponse = gin.H{
						"error": err.Message,
						"code":  err.Code,
					}
				default:
					statusCode = http.StatusInternalServerError
					errorResponse = gin.H{
						"error": "An internal server error occurred",
						"code":  "internal_error",
					}
				}

				c.JSON(statusCode, errorResponse)
				c.Abort()
			}
		}()
		c.Next()
	}
}
