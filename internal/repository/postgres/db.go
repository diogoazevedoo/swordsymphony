package postgres

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/diogoazevedoo/swordsymphony/internal/config"
	"github.com/diogoazevedoo/swordsymphony/internal/errors"
	_ "github.com/lib/pq"
)

// DB is a wrapper around a SQL database connection
type DB struct {
	*sql.DB
}

// NewDB creates a new database connection
func NewDB(cfg config.DatabaseConfig) (*DB, error) {
	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Database,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, errors.External(err, "Failed to connect to database", "db_connection_error")
	}

	if err := db.Ping(); err != nil {
		return nil, errors.External(err, "Failed to ping database", "db_ping_error")
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	return &DB{db}, nil
}
