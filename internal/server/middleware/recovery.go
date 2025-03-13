package middleware

import (
	"fmt"
	"net/http"
	"runtime/debug"

	"github.com/gin-gonic/gin"
)

// Recovery middleware handles panics and provides graceful recovery
func Recovery() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				stackTrace := debug.Stack()

				fmt.Printf("Panic recovered: %v\n%s", err, stackTrace)

				ctx.JSON(http.StatusInternalServerError, gin.H{
					"error": "An internal server error occurred",
					"code":  "internal_error",
				})

				ctx.Abort()
			}
		}()
		ctx.Next()
	}
}
