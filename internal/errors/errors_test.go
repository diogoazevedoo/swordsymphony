package errors

import (
	"errors"
	"net/http"
	"testing"
)

func TestAppError_Error(t *testing.T) {
	tests := []struct {
		name     string
		appError *AppError
		want     string
	}{
		{
			name: "with wrapped error",
			appError: &AppError{
				Type:    ErrorTypeInternal,
				Message: "Something went wrong",
				Err:     errors.New("database connection failed"),
				Code:    "db_error",
			},
			want: "Something went wrong: database connection failed",
		},
		{
			name: "without wrapped error",
			appError: &AppError{
				Type:    ErrorTypeValidation,
				Message: "Invalid input",
				Code:    "invalid_input",
			},
			want: "Invalid input",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.appError.Error(); got != tt.want {
				t.Errorf("AppError.Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAppError_HTTPStatusCode(t *testing.T) {
	tests := []struct {
		name      string
		errorType ErrorType
		want      int
	}{
		{
			name:      "validation error",
			errorType: ErrorTypeValidation,
			want:      http.StatusBadRequest,
		},
		{
			name:      "not found error",
			errorType: ErrorTypeNotFound,
			want:      http.StatusNotFound,
		},
		{
			name:      "unauthorized error",
			errorType: ErrorTypeUnauthorized,
			want:      http.StatusUnauthorized,
		},
		{
			name:      "conflict error",
			errorType: ErrorTypeConflict,
			want:      http.StatusConflict,
		},
		{
			name:      "external error",
			errorType: ErrorTypeExternal,
			want:      http.StatusBadGateway,
		},
		{
			name:      "internal error",
			errorType: ErrorTypeInternal,
			want:      http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &AppError{Type: tt.errorType}
			if got := err.HTTPStatusCode(); got != tt.want {
				t.Errorf("AppError.HTTPStatusCode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNew(t *testing.T) {
	err := New(ErrorTypeValidation, "Invalid input", "invalid_input")

	if err.Type != ErrorTypeValidation {
		t.Errorf("Expected error type %v, got %v", ErrorTypeValidation, err.Type)
	}

	if err.Message != "Invalid input" {
		t.Errorf("Expected message 'Invalid input', got %v", err.Message)
	}

	if err.Code != "invalid_input" {
		t.Errorf("Expected code 'invalid_input', got %v", err.Code)
	}

	if err.Err != nil {
		t.Errorf("Expected nil wrapped error, got %v", err.Err)
	}
}

func TestWrap(t *testing.T) {
	originalErr := errors.New("original error")
	err := Wrap(originalErr, ErrorTypeExternal, "Something failed", "api_error")

	if err.Type != ErrorTypeExternal {
		t.Errorf("Expected error type %v, got %v", ErrorTypeExternal, err.Type)
	}

	if err.Message != "Something failed" {
		t.Errorf("Expected message 'Something failed', got %v", err.Message)
	}

	if err.Code != "api_error" {
		t.Errorf("Expected code 'api_error', got %v", err.Code)
	}

	if err.Err != originalErr {
		t.Errorf("Expected wrapped error to be original error")
	}
}

func TestHelperFunctions(t *testing.T) {
	tests := []struct {
		name     string
		errFunc  func(string, string) *AppError
		wantType ErrorType
	}{
		{
			name:     "NotFound",
			errFunc:  NotFound,
			wantType: ErrorTypeNotFound,
		},
		{
			name:     "Validation",
			errFunc:  Validation,
			wantType: ErrorTypeValidation,
		},
		{
			name:     "Internal",
			errFunc:  Internal,
			wantType: ErrorTypeInternal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.errFunc("Test message", "test_code")
			if err.Type != tt.wantType {
				t.Errorf("Expected error type %v, got %v", tt.wantType, err.Type)
			}
		})
	}
}
