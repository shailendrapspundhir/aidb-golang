package main

import (
	"aidb/internal/api"
	"aidb/internal/auth"
	"aidb/internal/collection"
	"aidb/internal/config"
	"aidb/internal/rbac"
	"aidb/internal/transaction"
	"aidb/internal/vector"
	"aidb/internal/wal"
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
	log.Printf("Storage engine: %s", cfg.StorageEngine)
	log.Printf("Cache enabled: %v, size: %d MB", cfg.CacheEnabled, cfg.CacheSizeMB)
	if cfg.StorageEngine == "rocksdb" {
		log.Printf("RocksDB path: %s", cfg.RocksDBPath)
	}

	// Initialize the collection manager with persistence
	collectionManager, err := collection.NewPersistentManager(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize collection manager: %v", err)
	}

	// Initialize the unified collection manager using the same database connection
	unifiedManager, err := collection.NewUnifiedManagerWithDB(cfg, collectionManager.GetDB())
	if err != nil {
		log.Fatalf("Failed to initialize unified collection manager: %v", err)
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

	// Initialize Vector Manager (using the same BoltDB instance)
	vectorManager, err := vector.NewPersistentVectorManager(collectionManager.GetDB())
	if err != nil {
		log.Fatalf("Failed to initialize vector manager: %v", err)
	}

	// Create the API handler
	handler := api.NewHandler(collectionManager, authService, enforcer)

	// Initialize WAL for transaction support
	if cfg.TransactionsEnabled {
		walConfig := wal.DefaultConfig(cfg.DataDir)
		walConfig.SegmentSize = int64(cfg.WALMaxSegmentSizeMB) * 1024 * 1024

		// Set sync policy based on config
		switch cfg.WALSyncPolicy {
		case "every_write":
			walConfig.SyncPolicy = wal.SyncOnEveryWrite
		case "async":
			walConfig.SyncPolicy = wal.SyncAsync
		default:
			walConfig.SyncPolicy = wal.SyncOnCommit
		}

		writeAheadLog, err := wal.NewFileWAL(walConfig)
		if err != nil {
			log.Fatalf("Failed to initialize WAL: %v", err)
		}
		defer writeAheadLog.Close()

		// Initialize Transaction Manager
		txManagerConfig := transaction.DefaultManagerConfig()
		txManagerConfig.AutoCommitEnabled = cfg.TransactionAutoCommit
		txManagerConfig.DefaultTimeout = time.Duration(cfg.TransactionTimeoutSec) * time.Second

		txManager := transaction.NewManager(writeAheadLog, txManagerConfig)
		defer txManager.Close()

		// Set transaction manager on collection manager
		collectionManager.SetTransactionManager(txManager)

		// Wire deferred-write StorageApplier so commits flush to storage
		storageApplier := collection.NewStorageApplier(collectionManager)
		txManager.SetStorageApplier(storageApplier)

		// --- Crash Recovery ---
		if !cfg.SkipRecovery {
			recoveryApplier := collection.NewRecoveryApplier(collectionManager)
			recoveryMgr := wal.NewRecoveryManager(writeAheadLog, recoveryApplier)
			recoveryResult, recoveryErr := recoveryMgr.Recover()
			if recoveryErr != nil {
				log.Fatalf("WAL recovery failed: %v", recoveryErr)
			}
			if recoveryResult.RedoneOps > 0 || recoveryResult.UndoneOps > 0 {
				log.Printf("Recovery applied: %d ops redone, %d ops undone, %d committed tx, %d in-flight tx aborted",
					recoveryResult.RedoneOps, recoveryResult.UndoneOps,
					recoveryResult.CommittedTx, recoveryResult.InFlightTx)
			}
		} else {
			log.Println("WAL recovery skipped (AIDB_SKIP_RECOVERY=true)")
		}

		// Set transaction manager on API handler (for async transactions)
		handler.SetTransactionManager(txManager)

		log.Printf("Transaction support enabled: auto-commit=%v, timeout=%ds, wal-sync=%s",
			txManagerConfig.AutoCommitEnabled, cfg.TransactionTimeoutSec, cfg.WALSyncPolicy)
	} else {
		log.Println("Transaction support disabled")
	}

	// Create the Vector API handler
	vectorHandler := api.NewVectorHandler(vectorManager, authService, enforcer)

	// Create the Unified API handler
	unifiedHandler := api.NewUnifiedHandler(unifiedManager, authService, enforcer)

	// Create a new serve mux
	mux := http.NewServeMux()

	// Register all routes
	handler.RegisterRoutes(mux)

	// Helper to chain middleware
	protected := func(handlerFunc http.HandlerFunc) http.Handler {
		return handler.AuthMiddleware(handler.RBACMiddleware(http.HandlerFunc(handlerFunc)))
	}

	// Register vector routes
	vectorHandler.RegisterVectorRoutes(mux, protected)

	// Register unified routes (API v2)
	unifiedHandler.RegisterUnifiedRoutes(mux, protected)

	// WebSocket streaming endpoint
	mux.HandleFunc("GET /api/v1/ws", api.HandleWebSocket)

	// Query management endpoints
	mux.HandleFunc("GET /api/v1/queries", api.ListQueries)
	mux.HandleFunc("GET /api/v1/queries/{id}", api.GetQueryStatus)
	mux.HandleFunc("POST /api/v1/queries/{id}/cancel", api.CancelQuery)

	// Add a root handler
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{
  "name": "AIDB",
  "version": "0.5.0",
  "description": "AI-Native Database with Auth, RBAC, Multi-tenancy, Vector Support & HNSW Indexing",
  "dataDir": "` + cfg.DataDir + `",
  "database": "` + cfg.DatabaseFile + `",
  "storageEngine": "` + cfg.StorageEngine + `",
  "cacheEnabled": ` + fmt.Sprintf("%v", cfg.CacheEnabled) + `,
  "cacheSizeMB": ` + fmt.Sprintf("%d", cfg.CacheSizeMB) + `,
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
      "delete": "DELETE /api/v1/collections/{name}",
      "createIndex": "POST /api/v1/collections/{name}/indexes"
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
    "vectors": {
      "list": "GET /api/v1/vectors",
      "create": "POST /api/v1/vectors",
      "get": "GET /api/v1/vectors/{name}",
      "delete": "DELETE /api/v1/vectors/{name}",
      "insert_document": "POST /api/v1/vectors/{name}/documents",
      "list_documents": "GET /api/v1/vectors/{name}/documents",
      "get_document": "GET /api/v1/vectors/{name}/documents/{id}",
      "update_document": "PUT /api/v1/vectors/{name}/documents/{id}",
      "patch_document": "PATCH /api/v1/vectors/{name}/documents/{id}",
      "delete_document": "DELETE /api/v1/vectors/{name}/documents/{id}",
      "search": "POST /api/v1/vectors/{name}/search",
      "export": "GET /api/v1/vectors/{name}/export",
      "import": "POST /api/v1/vectors/{name}/import"
    },
    "websocket": {
      "connect": "WS /api/v1/ws",
      "subscribe": "{\"type\": \"subscribe\", \"queryIds\": [\"id1\", \"id2\"]}",
      "subscribe_all": "{\"type\": \"subscribe_all\"}",
      "cancel": "{\"type\": \"cancel\", \"queryId\": \"id\"}",
      "ping": "{\"type\": \"ping\"}"
    },
    "queries": {
      "list": "GET /api/v1/queries",
      "get": "GET /api/v1/queries/{id}",
      "cancel": "POST /api/v1/queries/{id}/cancel"
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