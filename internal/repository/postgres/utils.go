package postgres

import (
	"context"
	"time"
)

// withTimeout creates a context with timeout and returns a function to cancel it
func withTimeout(duration time.Duration) (context.Context, func()) {
	return context.WithTimeout(context.Background(), duration)
}

// defaultQueryTimeout is the default timeout for database queries
const defaultQueryTimeout = 5 * time.Second

// defaultTxTimeout is the default timeout for database transactions
const defaultTxTimeout = 10 * time.Second
