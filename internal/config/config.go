package config

import (
	"os"
	"path/filepath"
)

// Config holds the application configuration
type Config struct {
	// DataDir is the directory where data files are stored (for exports/imports)
	DataDir string
	// DatabaseFile is the path to the BoltDB database file
	DatabaseFile string
	// ServerPort is the port the server listens on
	ServerPort string
	// JWTSecret is the secret key for signing JWTs
	JWTSecret string
}

// Default values
const (
	DefaultDataDir     = "./aidb_data"
	DefaultDatabaseFile = "./aidb_data/aidb.db"
	DefaultServerPort  = "11111"
	DefaultJWTSecret   = "change-me-in-production-very-secret-key"
)

// Load reads configuration from environment variables
func Load() *Config {
	dataDir := getEnv("AIDB_DATA_DIR", DefaultDataDir)
	databaseFile := getEnv("AIDB_DATABASE_FILE", DefaultDatabaseFile)
	
	// Ensure absolute paths
	if !filepath.IsAbs(dataDir) {
		absPath, err := filepath.Abs(dataDir)
		if err == nil {
			dataDir = absPath
		}
	}
	
	if !filepath.IsAbs(databaseFile) {
		absPath, err := filepath.Abs(databaseFile)
		if err == nil {
			databaseFile = absPath
		}
	}

	return &Config{
		DataDir:      dataDir,
		DatabaseFile: databaseFile,
		ServerPort:   getEnv("AIDB_SERVER_PORT", DefaultServerPort),
		JWTSecret:    getEnv("AIDB_JWT_SECRET", DefaultJWTSecret),
	}
}

// getEnv gets an environment variable or returns the default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// EnsureDataDir creates the data directory if it doesn't exist
func (c *Config) EnsureDataDir() error {
	// Ensure the directory for the database file exists
	dir := filepath.Dir(c.DatabaseFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	// Also ensure data dir for exports
	return os.MkdirAll(c.DataDir, 0755)
}

// ExportFile returns the path for exporting a collection
func (c *Config) ExportFile(collectionName string) string {
	return filepath.Join(c.DataDir, collectionName+".export.json")
}

// SchemaExportFile returns the path for exporting a collection's schema
func (c *Config) SchemaExportFile(collectionName string) string {
	return filepath.Join(c.DataDir, collectionName+".schema.json")
}
