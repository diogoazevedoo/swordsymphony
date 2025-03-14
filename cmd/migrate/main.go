package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/diogoazevedoo/swordsymphony/internal/config"
	_ "github.com/lib/pq"
)

func main() {
	configFile := flag.String("config", ".env", "Path to configuration file")
	migrationsDir := flag.String("migrations", "migrations", "Path to migrations directory")
	flag.Parse()

	if err := loadEnv(*configFile); err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	dbConfig := config.DatabaseConfig{
		Driver:   os.Getenv("DB_DRIVER"),
		Host:     os.Getenv("DB_HOST"),
		Port:     getEnvAsInt("DB_PORT", 5432),
		User:     os.Getenv("DB_USER"),
		Password: os.Getenv("DB_PASSWORD"),
		Database: os.Getenv("DB_NAME"),
	}

	db, err := connectDB(dbConfig)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	if err := ensureMigrationsTable(db); err != nil {
		log.Fatalf("Failed to create migrations table: %v", err)
	}

	appliedMigrations, err := getAppliedMigrations(db)
	if err != nil {
		log.Fatalf("Failed to get applied migrations: %v", err)
	}

	files, err := getMigrationFiles(*migrationsDir)
	if err != nil {
		log.Fatalf("Failed to read migrations directory: %v", err)
	}

	for _, file := range files {
		migrationName := filepath.Base(file)
		if _, applied := appliedMigrations[migrationName]; applied {
			log.Printf("Migration %s already applied, skipping", migrationName)
			continue
		}

		log.Printf("Applying migration %s", migrationName)

		content, err := os.ReadFile(file)
		if err != nil {
			log.Fatalf("Failed to read migration file %s: %v", file, err)
		}

		if err := applyMigration(db, migrationName, string(content)); err != nil {
			log.Fatalf("Failed to apply migration %s: %v", migrationName, err)
		}

		log.Printf("Successfully applied migration %s", migrationName)
	}

	log.Println("Migrations completed successfully")
}

// loadEnv loads environment variables from a file
func loadEnv(path string) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("error reading file: %w", err)
	}

	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		if len(value) > 1 && (value[0] == '"' || value[0] == '\'') && value[0] == value[len(value)-1] {
			value = value[1 : len(value)-1]
		}

		os.Setenv(key, value)
	}

	return nil
}

// getEnvAsInt gets an environment variable as an integer
func getEnvAsInt(key string, defaultVal int) int {
	if value, exists := os.LookupEnv(key); exists {
		intVal, err := strconv.Atoi(value)
		if err == nil {
			return intVal
		}
	}
	return defaultVal
}

// connectDB creates a database connection
func connectDB(cfg config.DatabaseConfig) (*sql.DB, error) {
	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Database,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

// ensureMigrationsTable creates the migrations table if it doesn't exist
func ensureMigrationsTable(db *sql.DB) error {
	query := `
		CREATE TABLE IF NOT EXISTS migrations (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			applied_at TIMESTAMP NOT NULL DEFAULT NOW(),
			UNIQUE(name)
		);
	`

	_, err := db.Exec(query)
	return err
}

// getAppliedMigrations gets a list of already applied migrations
func getAppliedMigrations(db *sql.DB) (map[string]bool, error) {
	query := `SELECT name FROM migrations ORDER BY id;`

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	migrations := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		migrations[name] = true
	}

	return migrations, rows.Err()
}

// getMigrationFiles gets a sorted list of migration files
func getMigrationFiles(dir string) ([]string, error) {
	fileInfos, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, fi := range fileInfos {
		if fi.IsDir() || !strings.HasSuffix(fi.Name(), ".sql") {
			continue
		}
		files = append(files, filepath.Join(dir, fi.Name()))
	}

	sort.Strings(files)
	return files, nil
}

// applyMigration applies a migration to the database
func applyMigration(db *sql.DB, name, content string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(content); err != nil {
		return err
	}

	if _, err := tx.Exec("INSERT INTO migrations (name) VALUES ($1);", name); err != nil {
		return err
	}

	return tx.Commit()
}
