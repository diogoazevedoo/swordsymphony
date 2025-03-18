package response

import (
	"net/http"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/errors"
	"github.com/gin-gonic/gin"
)

// Response is the standard API response format
type Response struct {
	Success    bool      `json:"success"`
	Data       any       `json:"data,omitempty"`
	Error      *ApiError `json:"error,omitempty"`
	StatusCode int       `json:"status_code"`
	Timestamp  int64     `json:"timestamp"`
}

// Error represents an error in the API response
type ApiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details string `json:"details,omitempty"`
}

// New creates a new success response
func New(data any) Response {
	return Response{
		Success:    true,
		Data:       data,
		StatusCode: http.StatusOK,
		Timestamp:  time.Now().Unix(),
	}
}

// Error creates a new error response
func NewError(err error) Response {
	statusCode := http.StatusInternalServerError
	errorCode := "internal_error"
	errorMessage := "An unexpected error occurred"
	errorDetails := ""

	if appErr, ok := err.(*errors.AppError); ok {
		statusCode = appErr.HTTPStatusCode()
		errorCode = appErr.Code
		errorMessage = appErr.Message
		if appErr.Err != nil {
			errorDetails = appErr.Err.Error()
		}
	} else if err != nil {
		errorDetails = err.Error()
	}

	return Response{
		Success:    false,
		StatusCode: statusCode,
		Error: &ApiError{
			Code:    errorCode,
			Message: errorMessage,
			Details: errorDetails,
		},
		Timestamp: time.Now().Unix(),
	}
}

// Send sends the response with the appropriate status code
func (r Response) Send(c *gin.Context) {
	c.JSON(r.StatusCode, r)
}

// JSON is a shorthand for creating and sending a success response
func JSON(c *gin.Context, data any) {
	New(data).Send(c)
}

// Error is a shorthand for creating and sending an error response
func Error(c *gin.Context, err error) {
	NewError(err).Send(c)
}
