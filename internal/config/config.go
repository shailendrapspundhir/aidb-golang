package config

import (
	"os"
	"path/filepath"
	"strconv"
)

// Config holds the application configuration
type Config struct {
	// DataDir is the directory where data files are stored (for exports/imports)
	DataDir string
	// DatabaseFile is the path to the BoltDB database file (legacy, kept for compatibility)
	DatabaseFile string
	// ServerPort is the port the server listens on
	ServerPort string
	// JWTSecret is the secret key for signing JWTs
	JWTSecret string
	
	// Storage configuration
	StorageEngine  string // "rocksdb" or "boltdb"
	RocksDBPath    string // Path to RocksDB data directory
	CacheSizeMB    int    // Cache size in megabytes
	CacheEnabled   bool   // Enable/disable cache
	IndexEnabled   bool   // Enable/disable indexing

	// Memory Controller configuration
	MemoryLimitMB         int64  // Total memory limit in MB (0 = auto-detect)
	MemorySafetyPercent   int    // Safety margin percent (default 10)
	MemoryHighPressurePercent   int // High pressure threshold (default 70)
	MemoryCriticalPressurePercent int // Critical pressure threshold (default 90)
	MemoryEnableForcedGC  bool   // Enable forced GC under pressure

	// Adaptive Cursor configuration
	BatchInitialSize   int     // Initial batch size (default 1000)
	BatchMinSize       int     // Minimum batch size (default 1)
	BatchMaxSize       int     // Maximum batch size (default 50000)
	BatchGrowthFactor  float64 // Growth factor on success (default 1.5)
	BatchShrinkFactor  float64 // Shrink factor on failure (default 0.5)
	BatchTimeoutSec    int     // Batch fetch timeout in seconds (default 30)

	// Temp Storage (Spill) configuration
	SpillDir           string // Directory for spill files
	SpillMaxBytes      int64  // Max total spill bytes (default 10GB)
	SpillMaxFiles      int    // Max number of spill files (default 1000)
	SpillTTLSec        int    // Default TTL for spill files in seconds (default 3600)

	// External Merge Sort configuration
	SortMaxMemoryMB    int64  // Max memory for in-memory sort (default 100MB)
	SortMergeFanout    int    // Number of runs to merge at once (default 10)

	// Hash Partitioning configuration
	PartitionCount     int   // Number of partitions (default 16)
	PartitionMaxMemMB  int64 // Max memory per partition (default 50MB)

	// Transaction configuration
	TransactionsEnabled       bool   // Enable/disable transaction support
	TransactionAutoCommit     bool   // Auto-commit single operations (default true)
	TransactionTimeoutSec     int    // Transaction timeout in seconds (default 30)
	WALSyncPolicy             string // WAL sync policy: "every_write", "on_commit", "async" (default "on_commit")
	WALMaxSegmentSizeMB       int    // Max WAL segment size in MB (default 100)

	// Recovery configuration
	SkipRecovery bool // If true, skip WAL recovery on startup (default false)

	// PITR / Backup configuration
	BackupDir          string // Directory for base backups
	WALArchiveDir      string // Directory for archived WAL segments
	EnablePITR         bool   // Enable PITR features
	CheckpointInterval int    // Seconds between automatic checkpoints

	// Auto-indexing configuration
	AutoIndexEnabled            bool    // Enable/disable automatic index creation
	AutoIndexMinQueryCount      int64   // Min queries on a field before considering index
	AutoIndexMinFullScanRatio   float64 // Min full-scan ratio to consider indexing
	AutoIndexMaxSingleIndexes   int     // Max auto-created single-field indexes per collection
	AutoIndexMaxComposite       int     // Max auto-created composite indexes per collection
	AutoIndexConfidence         float64 // Min confidence score (0-1) to auto-create
	AutoIndexReanalysisPeriodMs int     // Milliseconds between re-analysis (0 = disabled)
}

// Default values
const (
	DefaultDataDir      = "./aidb_data"
	DefaultDatabaseFile = "./aidb_data/aidb.db"
	DefaultServerPort   = "11111"
	DefaultJWTSecret    = "change-me-in-production-very-secret-key"
	
	// Storage defaults
	DefaultStorageEngine = "rocksdb"
	DefaultRocksDBPath   = "./aidb_data/rocksdb"
	DefaultCacheSizeMB   = 256  // 256 MB default cache
	DefaultCacheEnabled  = true
	DefaultIndexEnabled  = true

	// Auto-indexing defaults
	DefaultAutoIndexEnabled          = true
	DefaultAutoIndexMinQueryCount    = 100
	DefaultAutoIndexMinFullScanRatio = 0.5
	DefaultAutoIndexMaxSingle        = 5
	DefaultAutoIndexMaxComposite     = 3
	DefaultAutoIndexConfidence       = 0.6
	DefaultAutoIndexReanalysisMs     = 600000 // 10 minutes

	// Memory Controller defaults
	DefaultMemorySafetyPercent   = 10
	DefaultMemoryHighPressurePercent   = 70
	DefaultMemoryCriticalPressurePercent = 90
	DefaultMemoryEnableForcedGC  = true

	// Adaptive Cursor defaults
	DefaultBatchInitialSize  = 1000
	DefaultBatchMinSize      = 1
	DefaultBatchMaxSize      = 50000
	DefaultBatchGrowthFactor = 1.5
	DefaultBatchShrinkFactor = 0.5
	DefaultBatchTimeoutSec   = 30

	// Temp Storage defaults
	DefaultSpillDir      = "/tmp/aidb_spill"
	DefaultSpillMaxBytes = 10 * 1024 * 1024 * 1024 // 10GB
	DefaultSpillMaxFiles = 1000
	DefaultSpillTTLSec   = 3600 // 1 hour

	// External Merge Sort defaults
	DefaultSortMaxMemoryMB = 100
	DefaultSortMergeFanout = 10

	// Hash Partitioning defaults
	DefaultPartitionCount    = 16
	DefaultPartitionMaxMemMB = 50

	// Transaction defaults
	DefaultTransactionsEnabled   = true
	DefaultTransactionAutoCommit = true
	DefaultTransactionTimeoutSec = 30
	DefaultWALSyncPolicy         = "on_commit"
	DefaultWALMaxSegmentSizeMB   = 100
	DefaultSkipRecovery          = false
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
	
	// Storage configuration
	storageEngine := getEnv("AIDB_STORAGE_ENGINE", DefaultStorageEngine)
	rocksDBPath := getEnv("AIDB_ROCKSDB_PATH", DefaultRocksDBPath)
	
	// Ensure absolute path for RocksDB
	if !filepath.IsAbs(rocksDBPath) {
		absPath, err := filepath.Abs(rocksDBPath)
		if err == nil {
			rocksDBPath = absPath
		}
	}
	
	cacheSizeMB := getEnvInt("AIDB_CACHE_SIZE_MB", DefaultCacheSizeMB)
	cacheEnabled := getEnvBool("AIDB_CACHE_ENABLED", DefaultCacheEnabled)
	indexEnabled := getEnvBool("AIDB_INDEX_ENABLED", DefaultIndexEnabled)

	// Auto-indexing configuration
	autoIndexEnabled := getEnvBool("AIDB_AUTO_INDEX_ENABLED", DefaultAutoIndexEnabled)
	autoIndexMinQueryCount := getEnvInt64("AIDB_AUTO_INDEX_MIN_QUERY_COUNT", DefaultAutoIndexMinQueryCount)
	autoIndexMinFullScanRatio := getEnvFloat64("AIDB_AUTO_INDEX_MIN_FULL_SCAN_RATIO", DefaultAutoIndexMinFullScanRatio)
	autoIndexMaxSingle := getEnvInt("AIDB_AUTO_INDEX_MAX_SINGLE", DefaultAutoIndexMaxSingle)
	autoIndexMaxComposite := getEnvInt("AIDB_AUTO_INDEX_MAX_COMPOSITE", DefaultAutoIndexMaxComposite)
	autoIndexConfidence := getEnvFloat64("AIDB_AUTO_INDEX_CONFIDENCE", DefaultAutoIndexConfidence)
	autoIndexReanalysisMs := getEnvInt("AIDB_AUTO_INDEX_REANALYSIS_MS", DefaultAutoIndexReanalysisMs)

	// Memory Controller configuration
	memoryLimitMB := getEnvInt64("AIDB_MEMORY_LIMIT_MB", 0) // 0 = auto-detect
	memorySafetyPercent := getEnvInt("AIDB_MEMORY_SAFETY_PERCENT", DefaultMemorySafetyPercent)
	memoryHighPressurePercent := getEnvInt("AIDB_MEMORY_HIGH_PRESSURE_PERCENT", DefaultMemoryHighPressurePercent)
	memoryCriticalPressurePercent := getEnvInt("AIDB_MEMORY_CRITICAL_PRESSURE_PERCENT", DefaultMemoryCriticalPressurePercent)
	memoryEnableForcedGC := getEnvBool("AIDB_MEMORY_ENABLE_FORCED_GC", DefaultMemoryEnableForcedGC)

	// Adaptive Cursor configuration
	batchInitialSize := getEnvInt("AIDB_BATCH_INITIAL_SIZE", DefaultBatchInitialSize)
	batchMinSize := getEnvInt("AIDB_BATCH_MIN_SIZE", DefaultBatchMinSize)
	batchMaxSize := getEnvInt("AIDB_BATCH_MAX_SIZE", DefaultBatchMaxSize)
	batchGrowthFactor := getEnvFloat64("AIDB_BATCH_GROWTH_FACTOR", DefaultBatchGrowthFactor)
	batchShrinkFactor := getEnvFloat64("AIDB_BATCH_SHRINK_FACTOR", DefaultBatchShrinkFactor)
	batchTimeoutSec := getEnvInt("AIDB_BATCH_TIMEOUT_SEC", DefaultBatchTimeoutSec)

	// Temp Storage (Spill) configuration
	spillDir := getEnv("AIDB_SPILL_DIR", DefaultSpillDir)
	spillMaxBytes := getEnvInt64("AIDB_SPILL_MAX_BYTES", DefaultSpillMaxBytes)
	spillMaxFiles := getEnvInt("AIDB_SPILL_MAX_FILES", DefaultSpillMaxFiles)
	spillTTLSec := getEnvInt("AIDB_SPILL_TTL_SEC", DefaultSpillTTLSec)

	// External Merge Sort configuration
	sortMaxMemoryMB := getEnvInt64("AIDB_SORT_MAX_MEMORY_MB", DefaultSortMaxMemoryMB)
	sortMergeFanout := getEnvInt("AIDB_SORT_MERGE_FANOUT", DefaultSortMergeFanout)

	// Hash Partitioning configuration
	partitionCount := getEnvInt("AIDB_PARTITION_COUNT", DefaultPartitionCount)
	partitionMaxMemMB := getEnvInt64("AIDB_PARTITION_MAX_MEM_MB", DefaultPartitionMaxMemMB)

	// Transaction configuration
	transactionsEnabled := getEnvBool("AIDB_TRANSACTIONS_ENABLED", DefaultTransactionsEnabled)
	transactionAutoCommit := getEnvBool("AIDB_TRANSACTION_AUTO_COMMIT", DefaultTransactionAutoCommit)
	transactionTimeoutSec := getEnvInt("AIDB_TRANSACTION_TIMEOUT_SEC", DefaultTransactionTimeoutSec)
	walSyncPolicy := getEnv("AIDB_WAL_SYNC_POLICY", DefaultWALSyncPolicy)
	walMaxSegmentSizeMB := getEnvInt("AIDB_WAL_MAX_SEGMENT_SIZE_MB", DefaultWALMaxSegmentSizeMB)
	skipRecovery := getEnvBool("AIDB_SKIP_RECOVERY", DefaultSkipRecovery)

	return &Config{
		DataDir:        dataDir,
		DatabaseFile:   databaseFile,
		ServerPort:     getEnv("AIDB_SERVER_PORT", DefaultServerPort),
		JWTSecret:      getEnv("AIDB_JWT_SECRET", DefaultJWTSecret),
		StorageEngine:  storageEngine,
		RocksDBPath:    rocksDBPath,
		CacheSizeMB:    cacheSizeMB,
		CacheEnabled:   cacheEnabled,
		IndexEnabled:   indexEnabled,

		MemoryLimitMB:                   memoryLimitMB,
		MemorySafetyPercent:             memorySafetyPercent,
		MemoryHighPressurePercent:       memoryHighPressurePercent,
		MemoryCriticalPressurePercent:   memoryCriticalPressurePercent,
		MemoryEnableForcedGC:            memoryEnableForcedGC,

		BatchInitialSize:  batchInitialSize,
		BatchMinSize:      batchMinSize,
		BatchMaxSize:      batchMaxSize,
		BatchGrowthFactor: batchGrowthFactor,
		BatchShrinkFactor: batchShrinkFactor,
		BatchTimeoutSec:   batchTimeoutSec,

		SpillDir:      spillDir,
		SpillMaxBytes: spillMaxBytes,
		SpillMaxFiles: spillMaxFiles,
		SpillTTLSec:   spillTTLSec,

		SortMaxMemoryMB: sortMaxMemoryMB,
		SortMergeFanout: sortMergeFanout,

		PartitionCount:   partitionCount,
		PartitionMaxMemMB: partitionMaxMemMB,

		TransactionsEnabled:       transactionsEnabled,
		TransactionAutoCommit:     transactionAutoCommit,
		TransactionTimeoutSec:     transactionTimeoutSec,
		WALSyncPolicy:             walSyncPolicy,
		WALMaxSegmentSizeMB:       walMaxSegmentSizeMB,

		SkipRecovery: skipRecovery,

		AutoIndexEnabled:            autoIndexEnabled,
		AutoIndexMinQueryCount:      autoIndexMinQueryCount,
		AutoIndexMinFullScanRatio:   autoIndexMinFullScanRatio,
		AutoIndexMaxSingleIndexes:   autoIndexMaxSingle,
		AutoIndexMaxComposite:       autoIndexMaxComposite,
		AutoIndexConfidence:         autoIndexConfidence,
		AutoIndexReanalysisPeriodMs: autoIndexReanalysisMs,
	}
}

// getEnv gets an environment variable or returns the default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getEnvInt gets an environment variable as int or returns the default value
func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}

// getEnvBool gets an environment variable as bool or returns the default value
func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolVal, err := strconv.ParseBool(value); err == nil {
			return boolVal
		}
	}
	return defaultValue
}

// getEnvInt64 gets an environment variable as int64 or returns the default value
func getEnvInt64(key string, defaultValue int64) int64 {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
			return intVal
		}
	}
	return defaultValue
}

// getEnvFloat64 gets an environment variable as float64 or returns the default value
func getEnvFloat64(key string, defaultValue float64) float64 {
	if value := os.Getenv(key); value != "" {
		if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			return floatVal
		}
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
	if err := os.MkdirAll(c.DataDir, 0755); err != nil {
		return err
	}
	// Ensure RocksDB directory exists
	if c.StorageEngine == "rocksdb" {
		if err := os.MkdirAll(c.RocksDBPath, 0755); err != nil {
			return err
		}
	}
	return nil
}

// ExportFile returns the path for exporting a collection
func (c *Config) ExportFile(collectionName string) string {
	return filepath.Join(c.DataDir, collectionName+".export.json")
}

// SchemaExportFile returns the path for exporting a collection's schema
func (c *Config) SchemaExportFile(collectionName string) string {
	return filepath.Join(c.DataDir, collectionName+".schema.json")
}
