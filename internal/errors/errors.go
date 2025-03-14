package errors

import (
	"fmt"
	"net/http"
)

// ErrorType represents the type of an error
type ErrorType string

const (
	// ErrorTypeInternal represents an internal server error
	ErrorTypeInternal ErrorType = "internal"

	// ErrorTypeValidation represents a validation error
	ErrorTypeValidation ErrorType = "validation"

	// ErrorTypeNotFound represents a not found error
	ErrorTypeNotFound ErrorType = "not_found"

	// ErrorTypeUnauthorized represents an unauthorized error
	ErrorTypeUnauthorized ErrorType = "unauthorized"

	// ErrorTypeConflict represents a conflict error
	ErrorTypeConflict ErrorType = "conflict"

	// ErrorTypeExternal represents an error from an external service
	ErrorTypeExternal ErrorType = "external"
)

// AppError represents an application error
type AppError struct {
	Type    ErrorType
	Message string
	Err     error
	Code    string
}

// Error implements the error interface
func (e *AppError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

// Unwrap returns the wrapped error
func (e *AppError) Unwrap() error {
	return e.Err
}

// HTTPStatusCode returns the appropriate HTTP status code for the error
func (e *AppError) HTTPStatusCode() int {
	switch e.Type {
	case ErrorTypeValidation:
		return http.StatusBadRequest
	case ErrorTypeNotFound:
		return http.StatusNotFound
	case ErrorTypeUnauthorized:
		return http.StatusUnauthorized
	case ErrorTypeConflict:
		return http.StatusConflict
	case ErrorTypeExternal:
		return http.StatusBadGateway
	default:
		return http.StatusInternalServerError
	}
}

// New creates a new AppError
func New(errType ErrorType, message string, code string) *AppError {
	return &AppError{
		Type:    errType,
		Message: message,
		Code:    code,
	}
}

// Wrap wraps an existing error in an AppError
func Wrap(err error, errType ErrorType, message string, code string) *AppError {
	return &AppError{
		Type:    errType,
		Message: message,
		Err:     err,
		Code:    code,
	}
}

// NotFound creates a not found error
func NotFound(message, code string) *AppError {
	return New(ErrorTypeNotFound, message, code)
}

// Validation creates a validation error
func Validation(message, code string) *AppError {
	return New(ErrorTypeValidation, message, code)
}

// Internal creates an internal server error
func Internal(message, code string) *AppError {
	return New(ErrorTypeInternal, message, code)
}

// External creates an external service error
func External(err error, message, code string) *AppError {
	return Wrap(err, ErrorTypeExternal, message, code)
}
