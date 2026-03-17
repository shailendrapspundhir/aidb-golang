package main

import (
	"aidb/internal/api"
	"aidb/internal/auth"
	"aidb/internal/collection"
	"aidb/internal/config"
	"aidb/internal/rbac"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// Load configuration from environment variables
	cfg := config.Load()

	// Ensure data directory exists
	if err := cfg.EnsureDataDir(); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	log.Printf("Data directory: %s", cfg.DataDir)
	log.Printf("Database file: %s", cfg.DatabaseFile)

	// Initialize the collection manager with persistence
	collectionManager, err := collection.NewPersistentManager(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize collection manager: %v", err)
	}

	// Ensure database is closed on exit
	defer func() {
		if err := collectionManager.Close(); err != nil {
			log.Printf("Error closing database: %v", err)
		}
	}()

	// Initialize Auth Service
	authService := auth.NewService(collectionManager, cfg)

	// Initialize RBAC Enforcer
	enforcer := rbac.NewEnforcer(collectionManager)

	// Create the API handler
	handler := api.NewHandler(collectionManager, authService, enforcer)

	// Create a new serve mux
	mux := http.NewServeMux()

	// Register all routes
	handler.RegisterRoutes(mux)

	// Add a root handler
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{
  "name": "AIDB",
  "version": "0.3.0",
  "description": "AI-Native Database with Auth, RBAC & Multi-tenancy",
  "dataDir": "` + cfg.DataDir + `",
  "database": "` + cfg.DatabaseFile + `",
  "endpoints": {
    "auth": {
      "login": "POST /api/v1/login",
      "register": "POST /api/v1/register",
      "apikeys": "POST /api/v1/apikeys",
      "roles": "POST /api/v1/roles"
    },
    "collections": {
      "list": "GET /api/v1/collections",
      "create": "POST /api/v1/collections",
      "get": "GET /api/v1/collections/{name}",
      "delete": "DELETE /api/v1/collections/{name}"
    },
    "documents": {
      "insert": "POST /api/v1/collections/{name}/documents",
      "list": "GET /api/v1/collections/{name}/documents",
      "get": "GET /api/v1/collections/{name}/documents/{id}",
      "update": "PUT /api/v1/collections/{name}/documents/{id}",
      "patch": "PATCH /api/v1/collections/{name}/documents/{id}",
      "delete": "DELETE /api/v1/collections/{name}/documents/{id}"
    },
    "schema": {
      "get": "GET /api/v1/collections/{name}/schema",
      "set": "PUT /api/v1/collections/{name}/schema"
    },
    "export_import": {
      "export": "GET /api/v1/collections/{name}/export",
      "export_download": "GET /api/v1/collections/{name}/export?download=true",
      "import": "POST /api/v1/collections/{name}/import?overwrite=true"
    },
    "health": "GET /api/v1/health"
  }
}`))
	})

	// Start the server
	addr := fmt.Sprintf(":%s", cfg.ServerPort)
	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	// Start server in goroutine
	go func() {
		log.Printf("AIDB server starting on %s", addr)
		log.Printf("API documentation available at http://localhost%s/", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed to start: %v", err)
		}
	}()

	// Wait for interrupt signal for graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	// Give outstanding requests 30 seconds to complete
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited")
}