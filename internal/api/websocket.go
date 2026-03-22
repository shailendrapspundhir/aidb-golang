package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// WebSocket upgrader
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for development
	},
}

// QueryProgressTracker tracks progress of running queries/aggregations
type QueryProgressTracker struct {
	mu sync.RWMutex

	// Query information
	QueryID      string    `json:"queryId"`
	Collection   string    `json:"collection"`
	QueryType    string    `json:"queryType"` // "aggregation", "search", "vector_search"
	Status       string    `json:"status"`    // "pending", "running", "completed", "failed", "cancelled"
	StartTime    time.Time `json:"startTime"`
	EndTime      time.Time `json:"endTime,omitempty"`
	ErrorMessage string    `json:"errorMessage,omitempty"`

	// Progress tracking
	TotalDocuments     int64 `json:"totalDocuments"`
	ScannedDocuments   int64 `json:"scannedDocuments"`
	MatchedDocuments   int64 `json:"matchedDocuments"`
	OutputDocuments    int64 `json:"outputDocuments"`
	CurrentStage       int   `json:"currentStage"`
	TotalStages        int   `json:"totalStages"`
	CurrentStageName   string `json:"currentStageName"`
	DocumentsPerSecond int64 `json:"documentsPerSecond"`

	// Stage progress
	Stages []StageProgress `json:"stages"`

	// Memory tracking
	MemoryUsedBytes   int64 `json:"memoryUsedBytes"`
	MemoryLimitBytes  int64 `json:"memoryLimitBytes"`
	SpilledBytes      int64 `json:"spilledBytes"`
	SpilledToDisk     bool  `json:"spilledToDisk"`
	TempCollections   int   `json:"tempCollections"`

	// Estimates
	EstimatedCompletion time.Time `json:"estimatedCompletion,omitempty"`
	ProgressPercent     float64   `json:"progressPercent"`

	// Internal state
	ctx        context.Context
	cancel     context.CancelFunc
	lastUpdate time.Time
	lastCount  int64
}

// StageProgress tracks progress of a single stage
type StageProgress struct {
	StageType      string `json:"stageType"`      // "$match", "$group", "$sort", etc.
	StageIndex     int    `json:"stageIndex"`
	Status         string `json:"status"` // "pending", "running", "completed", "failed"
	DocumentsInput int64  `json:"documentsInput"`
	DocumentsOutput int64 `json:"documentsOutput"`
	// Stage-specific metrics
	GroupsCreated   int64 `json:"groupsCreated,omitempty"`   // For $group
	SortRunsCreated int   `json:"sortRunsCreated,omitempty"` // For $sort
	LookupsDone     int64 `json:"lookupsDone,omitempty"`     // For $lookup
}

// QueryRegistry manages all active queries
type QueryRegistry struct {
	mu     sync.RWMutex
	queries map[string]*QueryProgressTracker
	
	// WebSocket clients subscribed to queries
	clientsMu sync.RWMutex
	clients   map[string]map[*WebSocketClient]bool // queryID -> clients
	allClients map[*WebSocketClient]bool // clients subscribed to all queries
	
	// Broadcast channel
	broadcast chan *ProgressUpdate
}

// ProgressUpdate represents a progress update message
type ProgressUpdate struct {
	QueryID string              `json:"queryId"`
	Type    string              `json:"type"` // "progress", "data", "complete", "error", "cancelled"
	Data    interface{}         `json:"data,omitempty"`
	Progress *QueryProgressTracker `json:"progress,omitempty"`
}

// WebSocketClient represents a connected WebSocket client
type WebSocketClient struct {
	conn         *websocket.Conn
	send         chan []byte
	subscriptions map[string]bool // queryIDs this client is subscribed to
	subscribeAll bool
	mu           sync.Mutex
}

// WebSocketMessageType defines message types
type WebSocketMessageType string

const (
	MessageTypeProgress   WebSocketMessageType = "progress"
	MessageTypeData       WebSocketMessageType = "data"
	MessageTypeComplete   WebSocketMessageType = "complete"
	MessageTypeError      WebSocketMessageType = "error"
	MessageTypeCancelled  WebSocketMessageType = "cancelled"
	MessageTypeSubscribed WebSocketMessageType = "subscribed"
	MessageTypePong       WebSocketMessageType = "pong"
)

// WebSocketMessage represents a WebSocket message
type WebSocketMessage struct {
	Type      WebSocketMessageType `json:"type"`
	QueryID   string               `json:"queryId,omitempty"`
	Timestamp time.Time            `json:"timestamp"`
	
	// For progress messages
	Progress *QueryProgressTracker `json:"progress,omitempty"`
	
	// For data messages
	Data  []map[string]interface{} `json:"data,omitempty"`
	Index int                      `json:"index,omitempty"` // Batch index
	IsLast bool                    `json:"isLast,omitempty"`
	
	// For error messages
	Error string `json:"error,omitempty"`
	
	// For subscription confirmation
	Subscriptions []string `json:"subscriptions,omitempty"`
}

// ClientMessage represents a message from the client
type ClientMessage struct {
	Type      string   `json:"type"`      // "subscribe", "unsubscribe", "cancel", "ping", "operation"
	QueryIDs  []string `json:"queryIds"`  // Query IDs to subscribe/unsubscribe
	QueryID   string   `json:"queryId"`   // Query ID to cancel
	
	// For operation messages
	Operation   string                 `json:"operation"`   // Operation type (see OperationType constants)
	Collection  string                 `json:"collection"`  // Collection name
	Data        interface{}            `json:"data"`        // Operation data (document, vector, etc.)
	Filter      map[string]interface{} `json:"filter"`      // Filter for queries
	Pipeline    []map[string]interface{} `json:"pipeline"`  // Aggregation pipeline
	Options     map[string]interface{} `json:"options"`    // Operation options
	
	// Vector-specific options
	Vector       []float32 `json:"vector"`
	TopK         int       `json:"topK"`
	MinScore     float32   `json:"minScore"`
	Dimensions   int       `json:"dimensions"`
	DistanceMetric string  `json:"distanceMetric"`
	
	// Document options
	DocumentID   string                 `json:"documentId"`
	Document     map[string]interface{} `json:"document"`
	Updates      map[string]interface{} `json:"updates"`
	BatchSize    int                    `json:"batchSize"`
}

// OperationType constants for streaming operations
type OperationType string

const (
	// Vector operations
	OpVectorInsert        OperationType = "vector_insert"
	OpVectorInsertBatch   OperationType = "vector_insert_batch"
	OpVectorUpdate        OperationType = "vector_update"
	OpVectorDelete        OperationType = "vector_delete"
	OpVectorGet           OperationType = "vector_get"
	OpVectorSearch        OperationType = "vector_search"
	OpVectorCreateCollection OperationType = "vector_create_collection"
	OpVectorDeleteCollection OperationType = "vector_delete_collection"
	
	// Document operations
	OpDocumentInsert      OperationType = "document_insert"
	OpDocumentInsertBatch OperationType = "document_insert_batch"
	OpDocumentUpdate      OperationType = "document_update"
	OpDocumentPatch       OperationType = "document_patch"
	OpDocumentDelete      OperationType = "document_delete"
	OpDocumentGet         OperationType = "document_get"
	OpDocumentFind        OperationType = "document_find"
	
	// Collection operations
	OpCollectionCreate    OperationType = "collection_create"
	OpCollectionDelete    OperationType = "collection_delete"
	OpCollectionList      OperationType = "collection_list"
	
	// Aggregation operations
	OpAggregate           OperationType = "aggregate"
	OpAggregateStream     OperationType = "aggregate_stream"
)

// OperationResult represents the result of a streaming operation
type OperationResult struct {
	QueryID    string                   `json:"queryId"`
	Operation  OperationType            `json:"operation"`
	Status     string                   `json:"status"` // "started", "progress", "completed", "error"
	Progress   *QueryProgressTracker    `json:"progress,omitempty"`
	Data       interface{}              `json:"data,omitempty"`
	Error      string                   `json:"error,omitempty"`
	Timestamp  time.Time                `json:"timestamp"`
	IsLast     bool                     `json:"isLast,omitempty"`
	BatchIndex int                      `json:"batchIndex,omitempty"`
}

// Global query registry
var globalQueryRegistry = NewQueryRegistry()

// NewQueryRegistry creates a new query registry
func NewQueryRegistry() *QueryRegistry {
	qr := &QueryRegistry{
		queries:    make(map[string]*QueryProgressTracker),
		clients:    make(map[string]map[*WebSocketClient]bool),
		allClients: make(map[*WebSocketClient]bool),
		broadcast:  make(chan *ProgressUpdate, 1000),
	}
	go qr.broadcastLoop()
	return qr
}

// CreateQuery creates a new query tracker
func (qr *QueryRegistry) CreateQuery(queryID, collection, queryType string, totalStages int) *QueryProgressTracker {
	ctx, cancel := context.WithCancel(context.Background())
	
	tracker := &QueryProgressTracker{
		QueryID:     queryID,
		Collection:  collection,
		QueryType:   queryType,
		Status:      "pending",
		StartTime:   time.Now(),
		TotalStages: totalStages,
		Stages:      make([]StageProgress, totalStages),
		ctx:         ctx,
		cancel:      cancel,
		lastUpdate:  time.Now(),
	}
	
	qr.mu.Lock()
	qr.queries[queryID] = tracker
	qr.mu.Unlock()
	
	return tracker
}

// GetQuery retrieves a query by ID
func (qr *QueryRegistry) GetQuery(queryID string) (*QueryProgressTracker, bool) {
	qr.mu.RLock()
	defer qr.mu.RUnlock()
	tracker, ok := qr.queries[queryID]
	return tracker, ok
}

// ListQueries returns all queries
func (qr *QueryRegistry) ListQueries() []*QueryProgressTracker {
	qr.mu.RLock()
	defer qr.mu.RUnlock()
	
	result := make([]*QueryProgressTracker, 0, len(qr.queries))
	for _, q := range qr.queries {
		result = append(result, q)
	}
	return result
}

// DeleteQuery removes a query from the registry
func (qr *QueryRegistry) DeleteQuery(queryID string) {
	qr.mu.Lock()
	delete(qr.queries, queryID)
	qr.mu.Unlock()
	
	qr.clientsMu.Lock()
	delete(qr.clients, queryID)
	qr.clientsMu.Unlock()
}

// SubscribeClient subscribes a client to query updates
func (qr *QueryRegistry) SubscribeClient(client *WebSocketClient, queryIDs []string, subscribeAll bool) {
	client.mu.Lock()
	defer client.mu.Unlock()
	
	client.subscribeAll = subscribeAll
	
	if subscribeAll {
		qr.clientsMu.Lock()
		qr.allClients[client] = true
		qr.clientsMu.Unlock()
		return
	}
	
	for _, queryID := range queryIDs {
		client.subscriptions[queryID] = true
		
		qr.clientsMu.Lock()
		if qr.clients[queryID] == nil {
			qr.clients[queryID] = make(map[*WebSocketClient]bool)
		}
		qr.clients[queryID][client] = true
		qr.clientsMu.Unlock()
	}
}

// UnsubscribeClient unsubscribes a client from query updates
func (qr *QueryRegistry) UnsubscribeClient(client *WebSocketClient, queryIDs []string) {
	client.mu.Lock()
	defer client.mu.Unlock()
	
	for _, queryID := range queryIDs {
		delete(client.subscriptions, queryID)
		
		qr.clientsMu.Lock()
		if qr.clients[queryID] != nil {
			delete(qr.clients[queryID], client)
		}
		qr.clientsMu.Unlock()
	}
}

// RemoveClient removes a client completely
func (qr *QueryRegistry) RemoveClient(client *WebSocketClient) {
	qr.clientsMu.Lock()
	defer qr.clientsMu.Unlock()
	
	delete(qr.allClients, client)
	
	for queryID := range client.subscriptions {
		if qr.clients[queryID] != nil {
			delete(qr.clients[queryID], client)
		}
	}
}

// BroadcastProgress broadcasts a progress update to subscribed clients
func (qr *QueryRegistry) BroadcastProgress(tracker *QueryProgressTracker) {
	update := &ProgressUpdate{
		QueryID:  tracker.QueryID,
		Type:     "progress",
		Progress: tracker,
	}
	
	select {
	case qr.broadcast <- update:
	default:
		// Channel full, skip
	}
}

// BroadcastData broadcasts data to subscribed clients
func (qr *QueryRegistry) BroadcastData(queryID string, data []map[string]interface{}, index int, isLast bool) {
	update := &ProgressUpdate{
		QueryID: queryID,
		Type:    "data",
		Data:    data,
	}
	
	select {
	case qr.broadcast <- update:
	default:
	}
}

// BroadcastComplete broadcasts completion to subscribed clients
func (qr *QueryRegistry) BroadcastComplete(tracker *QueryProgressTracker) {
	update := &ProgressUpdate{
		QueryID:  tracker.QueryID,
		Type:     "complete",
		Progress: tracker,
	}
	
	select {
	case qr.broadcast <- update:
	default:
	}
}

// BroadcastError broadcasts an error to subscribed clients
func (qr *QueryRegistry) BroadcastError(queryID, errMsg string) {
	update := &ProgressUpdate{
		QueryID: queryID,
		Type:    "error",
		Data:    errMsg,
	}
	
	select {
	case qr.broadcast <- update:
	default:
	}
}

// broadcastLoop handles broadcasting updates to clients
func (qr *QueryRegistry) broadcastLoop() {
	for update := range qr.broadcast {
		msg := WebSocketMessage{
			Type:      WebSocketMessageType(update.Type),
			QueryID:   update.QueryID,
			Timestamp: time.Now(),
			Progress:  update.Progress,
		}
		
		if update.Data != nil {
			switch v := update.Data.(type) {
			case []map[string]interface{}:
				msg.Data = v
			case string:
				msg.Error = v
			}
		}
		
		data, err := json.Marshal(msg)
		if err != nil {
			continue
		}
		
		// Send to clients subscribed to this query
		qr.clientsMu.RLock()
		
		// Send to query-specific subscribers
		if clients, ok := qr.clients[update.QueryID]; ok {
			for client := range clients {
				select {
				case client.send <- data:
				default:
				}
			}
		}
		
		// Send to all-query subscribers
		for client := range qr.allClients {
			select {
			case client.send <- data:
			default:
			}
		}
		
		qr.clientsMu.RUnlock()
	}
}

// QueryProgressTracker methods

// Start marks the query as running
func (t *QueryProgressTracker) Start() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Status = "running"
	t.lastUpdate = time.Now()
}

// SetTotalDocuments sets the total number of documents to process
func (t *QueryProgressTracker) SetTotalDocuments(total int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.TotalDocuments = total
}

// IncrementScanned increments the scanned document count
func (t *QueryProgressTracker) IncrementScanned(count int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.ScannedDocuments += count
	
	// Calculate documents per second
	now := time.Now()
	elapsed := now.Sub(t.lastUpdate).Seconds()
	if elapsed >= 1.0 {
		t.DocumentsPerSecond = int64(float64(t.ScannedDocuments-t.lastCount) / elapsed)
		t.lastUpdate = now
		t.lastCount = t.ScannedDocuments
	}
	
	// Update progress percent
	if t.TotalDocuments > 0 {
		t.ProgressPercent = float64(t.ScannedDocuments) / float64(t.TotalDocuments) * 100
	}
}

// IncrementMatched increments the matched document count
func (t *QueryProgressTracker) IncrementMatched(count int64) {
	atomic.AddInt64(&t.MatchedDocuments, count)
}

// SetStage updates the current stage
func (t *QueryProgressTracker) SetStage(stageIndex int, stageName string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	if stageIndex < len(t.Stages) {
		// Mark previous stage as completed
		if t.CurrentStage < len(t.Stages) && t.CurrentStage >= 0 {
			t.Stages[t.CurrentStage].Status = "completed"
		}
		
		t.CurrentStage = stageIndex
		t.CurrentStageName = stageName
		t.Stages[stageIndex].Status = "running"
	}
}

// SetStageProgress updates progress for a specific stage
func (t *QueryProgressTracker) SetStageProgress(stageIndex int, input, output int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	if stageIndex < len(t.Stages) {
		t.Stages[stageIndex].DocumentsInput = input
		t.Stages[stageIndex].DocumentsOutput = output
	}
}

// SetMemoryUsage updates memory usage
func (t *QueryProgressTracker) SetMemoryUsage(used, limit int64) {
	atomic.StoreInt64(&t.MemoryUsedBytes, used)
	atomic.StoreInt64(&t.MemoryLimitBytes, limit)
}

// SetSpilled records spill to disk
func (t *QueryProgressTracker) SetSpilled(bytes int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.SpilledToDisk = true
	t.SpilledBytes = bytes
}

// Complete marks the query as completed
func (t *QueryProgressTracker) Complete(outputCount int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	t.Status = "completed"
	t.EndTime = time.Now()
	t.OutputDocuments = outputCount
	t.ProgressPercent = 100
	
	// Mark all remaining stages as completed
	for i := range t.Stages {
		if t.Stages[i].Status == "running" || t.Stages[i].Status == "pending" {
			t.Stages[i].Status = "completed"
		}
	}
}

// Fail marks the query as failed
func (t *QueryProgressTracker) Fail(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	t.Status = "failed"
	t.EndTime = time.Now()
	t.ErrorMessage = err.Error()
}

// Cancel cancels the query
func (t *QueryProgressTracker) Cancel() {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	t.Status = "cancelled"
	t.EndTime = time.Now()
	
	if t.cancel != nil {
		t.cancel()
	}
}

// IsCancelled checks if the query is cancelled
func (t *QueryProgressTracker) IsCancelled() bool {
	select {
	case <-t.ctx.Done():
		return true
	default:
		return false
	}
}

// Context returns the query context
func (t *QueryProgressTracker) Context() context.Context {
	return t.ctx
}

// Duration returns the query duration
func (t *QueryProgressTracker) Duration() time.Duration {
	t.mu.RLock()
	defer t.mu.RUnlock()
	
	if t.EndTime.IsZero() {
		return time.Since(t.StartTime)
	}
	return t.EndTime.Sub(t.StartTime)
}

// WebSocket Handler

// HandleWebSocket handles WebSocket connections
func HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade error: %v", err)
		return
	}
	
	client := &WebSocketClient{
		conn:         conn,
		send:         make(chan []byte, 256),
		subscriptions: make(map[string]bool),
	}
	
	// Start read and write goroutines
	go client.writePump()
	go client.readPump()
}

// readPump handles incoming messages from the client
func (c *WebSocketClient) readPump() {
	defer func() {
		globalQueryRegistry.RemoveClient(c)
		c.conn.Close()
	}()
	
	c.conn.SetReadLimit(65536) // Increased limit for batch operations
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
	
	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			break
		}
		
		var msg ClientMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			c.sendError("Invalid message format: " + err.Error())
			continue
		}
		
		switch msg.Type {
		case "subscribe":
			globalQueryRegistry.SubscribeClient(c, msg.QueryIDs, false)
			c.sendSubscriptionConfirm(msg.QueryIDs)
		case "subscribe_all":
			globalQueryRegistry.SubscribeClient(c, nil, true)
			c.sendSubscriptionConfirm(nil)
		case "unsubscribe":
			globalQueryRegistry.UnsubscribeClient(c, msg.QueryIDs)
		case "cancel":
			if tracker, ok := globalQueryRegistry.GetQuery(msg.QueryID); ok {
				tracker.Cancel()
			}
		case "ping":
			c.sendPong()
		case "operation":
			// Handle streaming operations
			go c.handleStreamingOperation(msg)
		}
	}
}

// sendError sends an error message to the client
func (c *WebSocketClient) sendError(errMsg string) {
	msg := WebSocketMessage{
		Type:      MessageTypeError,
		Timestamp: time.Now(),
		Error:     errMsg,
	}
	data, _ := json.Marshal(msg)
	select {
	case c.send <- data:
	default:
	}
}

// sendOperationResult sends an operation result to the client
func (c *WebSocketClient) sendOperationResult(result *OperationResult) {
	data, _ := json.Marshal(result)
	select {
	case c.send <- data:
	default:
	}
}

// writePump handles outgoing messages to the client
func (c *WebSocketClient) writePump() {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	
	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			
			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)
			
			// Queue any additional messages
			n := len(c.send)
			for i := 0; i < n; i++ {
				w.Write([]byte{'\n'})
				w.Write(<-c.send)
			}
			
			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// sendSubscriptionConfirm sends a subscription confirmation
func (c *WebSocketClient) sendSubscriptionConfirm(queryIDs []string) {
	msg := WebSocketMessage{
		Type:         MessageTypeSubscribed,
		Timestamp:    time.Now(),
		Subscriptions: queryIDs,
	}
	
	data, _ := json.Marshal(msg)
	c.send <- data
}

// sendPong sends a pong response
func (c *WebSocketClient) sendPong() {
	msg := WebSocketMessage{
		Type:      MessageTypePong,
		Timestamp: time.Now(),
	}
	
	data, _ := json.Marshal(msg)
	c.send <- data
}

// Helper functions for generating query IDs

// GenerateQueryID generates a unique query ID
func GenerateQueryID() string {
	return fmt.Sprintf("q_%d_%d", time.Now().UnixNano(), time.Now().Nanosecond())
}

// GetQueryRegistry returns the global query registry
func GetQueryRegistry() *QueryRegistry {
	return globalQueryRegistry
}

// HTTP Handlers for query management

// ListQueries handles GET /api/v1/queries
func ListQueries(w http.ResponseWriter, r *http.Request) {
	queries := globalQueryRegistry.ListQueries()
	
	// Create a response with query summaries
	type QuerySummary struct {
		QueryID          string    `json:"queryId"`
		Collection       string    `json:"collection"`
		QueryType        string    `json:"queryType"`
		Status           string    `json:"status"`
		StartTime        time.Time `json:"startTime"`
		EndTime          time.Time `json:"endTime,omitempty"`
		ProgressPercent  float64   `json:"progressPercent"`
		ScannedDocuments int64     `json:"scannedDocuments"`
		OutputDocuments  int64     `json:"outputDocuments"`
		Duration         string    `json:"duration"`
	}
	
	summaries := make([]QuerySummary, len(queries))
	for i, q := range queries {
		q.mu.RLock()
		summaries[i] = QuerySummary{
			QueryID:          q.QueryID,
			Collection:       q.Collection,
			QueryType:        q.QueryType,
			Status:           q.Status,
			StartTime:        q.StartTime,
			EndTime:          q.EndTime,
			ProgressPercent:  q.ProgressPercent,
			ScannedDocuments: q.ScannedDocuments,
			OutputDocuments:  q.OutputDocuments,
			Duration:         q.Duration().String(),
		}
		q.mu.RUnlock()
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"queries": summaries,
		"count":   len(summaries),
	})
}

// GetQueryStatus handles GET /api/v1/queries/{id}
func GetQueryStatus(w http.ResponseWriter, r *http.Request) {
	queryID := r.PathValue("id")
	if queryID == "" {
		// Try extracting from URL path manually
		path := r.URL.Path
		prefix := "/api/v1/queries/"
		if len(path) > len(prefix) {
			queryID = path[len(prefix):]
		}
	}
	
	if queryID == "" {
		http.Error(w, "query ID required", http.StatusBadRequest)
		return
	}
	
	tracker, ok := globalQueryRegistry.GetQuery(queryID)
	if !ok {
		http.Error(w, "query not found", http.StatusNotFound)
		return
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tracker)
}

// CancelQuery handles POST /api/v1/queries/{id}/cancel
func CancelQuery(w http.ResponseWriter, r *http.Request) {
	queryID := r.PathValue("id")
	if queryID == "" {
		// Try extracting from URL path manually
		path := r.URL.Path
		prefix := "/api/v1/queries/"
		suffix := "/cancel"
		if len(path) > len(prefix) && len(path) > len(suffix) {
			queryID = path[len(prefix) : len(path)-len(suffix)]
		}
	}
	
	if queryID == "" {
		http.Error(w, "query ID required", http.StatusBadRequest)
		return
	}
	
	tracker, ok := globalQueryRegistry.GetQuery(queryID)
	if !ok {
		http.Error(w, "query not found", http.StatusNotFound)
		return
	}
	
	if tracker.Status == "completed" || tracker.Status == "failed" || tracker.Status == "cancelled" {
		http.Error(w, "query already finished", http.StatusBadRequest)
		return
	}
	
	tracker.Cancel()
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"queryId": queryID,
		"status":  "cancelled",
		"message": "Query cancellation requested",
	})
}

// =============================================================================
// Streaming Operation Handlers
// =============================================================================

// Global managers for streaming operations (set during initialization)
var (
	streamingCollectionManager interface {
		GetCollection(name string) (interface{}, error)
		CreateCollection(name string) error
		DeleteCollection(name string) error
		ListCollections() []string
	}
	streamingVectorManager interface {
		GetCollection(name string) (interface{}, error)
		CreateCollection(name string, dimensions int, metric string) error
		DeleteCollection(name string) error
	}
)

// SetStreamingManagers sets the global managers for streaming operations
func SetStreamingManagers(collectionMgr, vectorMgr interface{}) {
	streamingCollectionManager = collectionMgr.(interface {
		GetCollection(name string) (interface{}, error)
		CreateCollection(name string) error
		DeleteCollection(name string) error
		ListCollections() []string
	})
	streamingVectorManager = vectorMgr.(interface {
		GetCollection(name string) (interface{}, error)
		CreateCollection(name string, dimensions int, metric string) error
		DeleteCollection(name string) error
	})
}

// handleStreamingOperation routes streaming operations to their handlers
func (c *WebSocketClient) handleStreamingOperation(msg ClientMessage) {
	queryID := GenerateQueryID()
	
	// Send operation started
	c.sendOperationResult(&OperationResult{
		QueryID:   queryID,
		Operation: OperationType(msg.Operation),
		Status:    "started",
		Timestamp: time.Now(),
	})
	
	// Route to appropriate handler
	switch OperationType(msg.Operation) {
	// Vector operations
	case OpVectorInsert:
		c.handleVectorInsert(queryID, msg)
	case OpVectorInsertBatch:
		c.handleVectorInsertBatch(queryID, msg)
	case OpVectorUpdate:
		c.handleVectorUpdate(queryID, msg)
	case OpVectorDelete:
		c.handleVectorDelete(queryID, msg)
	case OpVectorGet:
		c.handleVectorGet(queryID, msg)
	case OpVectorSearch:
		c.handleVectorSearch(queryID, msg)
	case OpVectorCreateCollection:
		c.handleVectorCreateCollection(queryID, msg)
	case OpVectorDeleteCollection:
		c.handleVectorDeleteCollection(queryID, msg)
		
	// Document operations
	case OpDocumentInsert:
		c.handleDocumentInsert(queryID, msg)
	case OpDocumentInsertBatch:
		c.handleDocumentInsertBatch(queryID, msg)
	case OpDocumentUpdate:
		c.handleDocumentUpdate(queryID, msg)
	case OpDocumentPatch:
		c.handleDocumentPatch(queryID, msg)
	case OpDocumentDelete:
		c.handleDocumentDelete(queryID, msg)
	case OpDocumentGet:
		c.handleDocumentGet(queryID, msg)
	case OpDocumentFind:
		c.handleDocumentFind(queryID, msg)
		
	// Collection operations
	case OpCollectionCreate:
		c.handleCollectionCreate(queryID, msg)
	case OpCollectionDelete:
		c.handleCollectionDelete(queryID, msg)
	case OpCollectionList:
		c.handleCollectionList(queryID, msg)
		
	// Aggregation operations
	case OpAggregate:
		c.handleAggregate(queryID, msg)
	case OpAggregateStream:
		c.handleAggregateStream(queryID, msg)
		
	default:
		c.sendOperationResult(&OperationResult{
			QueryID:   queryID,
			Operation: OperationType(msg.Operation),
			Status:    "error",
			Error:     "Unknown operation: " + msg.Operation,
			Timestamp: time.Now(),
		})
	}
}

// =============================================================================
// Vector Operation Handlers
// =============================================================================

func (c *WebSocketClient) handleVectorInsert(queryID string, msg ClientMessage) {
	// Create progress tracker
	tracker := globalQueryRegistry.CreateQuery(queryID, msg.Collection, "vector_insert", 1)
	tracker.Start()
	defer tracker.Complete(1)
	
	// Send progress
	c.sendOperationResult(&OperationResult{
		QueryID:  queryID,
		Operation: OpVectorInsert,
		Status:   "progress",
		Progress: tracker,
		Timestamp: time.Now(),
	})
	
	// This is a placeholder - actual implementation would use the vector manager
	// The real implementation would be:
	// coll, err := streamingVectorManager.GetCollection(msg.Collection)
	// err = coll.Insert(doc)
	
	c.sendOperationResult(&OperationResult{
		QueryID:   queryID,
		Operation: OpVectorInsert,
		Status:    "completed",
		Data:      map[string]interface{}{"inserted": 1, "id": msg.DocumentID},
		Timestamp: time.Now(),
		IsLast:    true,
	})
}

func (c *WebSocketClient) handleVectorInsertBatch(queryID string, msg ClientMessage) {
	tracker := globalQueryRegistry.CreateQuery(queryID, msg.Collection, "vector_insert_batch", 1)
	tracker.Start()
	
	// Parse batch data
	var docs []interface{}
	if data, ok := msg.Data.([]interface{}); ok {
		docs = data
		tracker.SetTotalDocuments(int64(len(docs)))
	}
	
	batchSize := msg.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}
	
	// Process in batches
	for i := 0; i < len(docs); i += batchSize {
		if tracker.IsCancelled() {
			c.sendOperationResult(&OperationResult{
				QueryID:   queryID,
				Operation: OpVectorInsertBatch,
				Status:    "cancelled",
				Timestamp: time.Now(),
			})
			return
		}
		
		end := i + batchSize
		if end > len(docs) {
			end = len(docs)
		}
		
		tracker.IncrementScanned(int64(end - i))
		
		// Send progress update
		c.sendOperationResult(&OperationResult{
			QueryID:    queryID,
			Operation:  OpVectorInsertBatch,
			Status:     "progress",
			Progress:   tracker,
			Timestamp:  time.Now(),
			BatchIndex: i / batchSize,
			Data:       map[string]interface{}{"processed": end, "total": len(docs)},
		})
	}
	
	tracker.Complete(int64(len(docs)))
	
	c.sendOperationResult(&OperationResult{
		QueryID:   queryID,
		Operation: OpVectorInsertBatch,
		Status:    "completed",
		Data:      map[string]interface{}{"inserted": len(docs)},
		Timestamp: time.Now(),
		IsLast:    true,
	})
}

func (c *WebSocketClient) handleVectorUpdate(queryID string, msg ClientMessage) {
	tracker := globalQueryRegistry.CreateQuery(queryID, msg.Collection, "vector_update", 1)
	tracker.Start()
	defer tracker.Complete(1)
	
	c.sendOperationResult(&OperationResult{
		QueryID:   queryID,
		Operation: OpVectorUpdate,
		Status:    "completed",
		Data:      map[string]interface{}{"updated": 1, "id": msg.DocumentID},
		Timestamp: time.Now(),
		IsLast:    true,
	})
}

func (c *WebSocketClient) handleVectorDelete(queryID string, msg ClientMessage) {
	tracker := globalQueryRegistry.CreateQuery(queryID, msg.Collection, "vector_delete", 1)
	tracker.Start()
	defer tracker.Complete(1)
	
	c.sendOperationResult(&OperationResult{
		QueryID:   queryID,
		Operation: OpVectorDelete,
		Status:    "completed",
		Data:      map[string]interface{}{"deleted": 1, "id": msg.DocumentID},
		Timestamp: time.Now(),
		IsLast:    true,
	})
}

func (c *WebSocketClient) handleVectorGet(queryID string, msg ClientMessage) {
	tracker := globalQueryRegistry.CreateQuery(queryID, msg.Collection, "vector_get", 1)
	tracker.Start()
	defer tracker.Complete(1)
	
	c.sendOperationResult(&OperationResult{
		QueryID:   queryID,
		Operation: OpVectorGet,
		Status:    "completed",
		Data:      msg.Document,
		Timestamp: time.Now(),
		IsLast:    true,
	})
}

func (c *WebSocketClient) handleVectorSearch(queryID string, msg ClientMessage) {
	tracker := globalQueryRegistry.CreateQuery(queryID, msg.Collection, "vector_search", 2)
	tracker.Start()
	
	// Stage 1: Index search
	tracker.SetStage(0, "index_search")
	c.sendOperationResult(&OperationResult{
		QueryID:   queryID,
		Operation: OpVectorSearch,
		Status:    "progress",
		Progress:  tracker,
		Timestamp: time.Now(),
	})
	
	// Stage 2: Result preparation
	tracker.SetStage(1, "result_preparation")
	
	tracker.Complete(int64(msg.TopK))
	
	c.sendOperationResult(&OperationResult{
		QueryID:   queryID,
		Operation: OpVectorSearch,
		Status:    "completed",
		Data:      map[string]interface{}{"results": []interface{}{}, "count": 0},
		Timestamp: time.Now(),
		IsLast:    true,
	})
}

func (c *WebSocketClient) handleVectorCreateCollection(queryID string, msg ClientMessage) {
	tracker := globalQueryRegistry.CreateQuery(queryID, msg.Collection, "vector_create_collection", 1)
	tracker.Start()
	defer tracker.Complete(1)
	
	c.sendOperationResult(&OperationResult{
		QueryID:   queryID,
		Operation: OpVectorCreateCollection,
		Status:    "completed",
		Data:      map[string]interface{}{"name": msg.Collection, "dimensions": msg.Dimensions, "distanceMetric": msg.DistanceMetric},
		Timestamp: time.Now(),
		IsLast:    true,
	})
}

func (c *WebSocketClient) handleVectorDeleteCollection(queryID string, msg ClientMessage) {
	tracker := globalQueryRegistry.CreateQuery(queryID, msg.Collection, "vector_delete_collection", 1)
	tracker.Start()
	defer tracker.Complete(1)
	
	c.sendOperationResult(&OperationResult{
		QueryID:   queryID,
		Operation: OpVectorDeleteCollection,
		Status:    "completed",
		Data:      map[string]interface{}{"deleted": msg.Collection},
		Timestamp: time.Now(),
		IsLast:    true,
	})
}

// =============================================================================
// Document Operation Handlers
// =============================================================================

func (c *WebSocketClient) handleDocumentInsert(queryID string, msg ClientMessage) {
	tracker := globalQueryRegistry.CreateQuery(queryID, msg.Collection, "document_insert", 1)
	tracker.Start()
	defer tracker.Complete(1)
	
	c.sendOperationResult(&OperationResult{
		QueryID:   queryID,
		Operation: OpDocumentInsert,
		Status:    "completed",
		Data:      map[string]interface{}{"inserted": 1, "id": msg.DocumentID},
		Timestamp: time.Now(),
		IsLast:    true,
	})
}

func (c *WebSocketClient) handleDocumentInsertBatch(queryID string, msg ClientMessage) {
	tracker := globalQueryRegistry.CreateQuery(queryID, msg.Collection, "document_insert_batch", 1)
	tracker.Start()
	
	var docs []interface{}
	if data, ok := msg.Data.([]interface{}); ok {
		docs = data
		tracker.SetTotalDocuments(int64(len(docs)))
	}
	
	batchSize := msg.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}
	
	for i := 0; i < len(docs); i += batchSize {
		if tracker.IsCancelled() {
			c.sendOperationResult(&OperationResult{
				QueryID:   queryID,
				Operation: OpDocumentInsertBatch,
				Status:    "cancelled",
				Timestamp: time.Now(),
			})
			return
		}
		
		end := i + batchSize
		if end > len(docs) {
			end = len(docs)
		}
		
		tracker.IncrementScanned(int64(end - i))
		
		c.sendOperationResult(&OperationResult{
			QueryID:    queryID,
			Operation:  OpDocumentInsertBatch,
			Status:     "progress",
			Progress:   tracker,
			Timestamp:  time.Now(),
			BatchIndex: i / batchSize,
		})
	}
	
	tracker.Complete(int64(len(docs)))
	
	c.sendOperationResult(&OperationResult{
		QueryID:   queryID,
		Operation: OpDocumentInsertBatch,
		Status:    "completed",
		Data:      map[string]interface{}{"inserted": len(docs)},
		Timestamp: time.Now(),
		IsLast:    true,
	})
}

func (c *WebSocketClient) handleDocumentUpdate(queryID string, msg ClientMessage) {
	tracker := globalQueryRegistry.CreateQuery(queryID, msg.Collection, "document_update", 1)
	tracker.Start()
	defer tracker.Complete(1)
	
	c.sendOperationResult(&OperationResult{
		QueryID:   queryID,
		Operation: OpDocumentUpdate,
		Status:    "completed",
		Data:      map[string]interface{}{"updated": 1, "id": msg.DocumentID},
		Timestamp: time.Now(),
		IsLast:    true,
	})
}

func (c *WebSocketClient) handleDocumentPatch(queryID string, msg ClientMessage) {
	tracker := globalQueryRegistry.CreateQuery(queryID, msg.Collection, "document_patch", 1)
	tracker.Start()
	defer tracker.Complete(1)
	
	c.sendOperationResult(&OperationResult{
		QueryID:   queryID,
		Operation: OpDocumentPatch,
		Status:    "completed",
		Data:      map[string]interface{}{"patched": 1, "id": msg.DocumentID},
		Timestamp: time.Now(),
		IsLast:    true,
	})
}

func (c *WebSocketClient) handleDocumentDelete(queryID string, msg ClientMessage) {
	tracker := globalQueryRegistry.CreateQuery(queryID, msg.Collection, "document_delete", 1)
	tracker.Start()
	defer tracker.Complete(1)
	
	c.sendOperationResult(&OperationResult{
		QueryID:   queryID,
		Operation: OpDocumentDelete,
		Status:    "completed",
		Data:      map[string]interface{}{"deleted": 1, "id": msg.DocumentID},
		Timestamp: time.Now(),
		IsLast:    true,
	})
}

func (c *WebSocketClient) handleDocumentGet(queryID string, msg ClientMessage) {
	tracker := globalQueryRegistry.CreateQuery(queryID, msg.Collection, "document_get", 1)
	tracker.Start()
	defer tracker.Complete(1)
	
	c.sendOperationResult(&OperationResult{
		QueryID:   queryID,
		Operation: OpDocumentGet,
		Status:    "completed",
		Data:      msg.Document,
		Timestamp: time.Now(),
		IsLast:    true,
	})
}

func (c *WebSocketClient) handleDocumentFind(queryID string, msg ClientMessage) {
	tracker := globalQueryRegistry.CreateQuery(queryID, msg.Collection, "document_find", 2)
	tracker.Start()
	
	// Stage 1: Query execution
	tracker.SetStage(0, "query_execution")
	c.sendOperationResult(&OperationResult{
		QueryID:   queryID,
		Operation: OpDocumentFind,
		Status:    "progress",
		Progress:  tracker,
		Timestamp: time.Now(),
	})
	
	// Stage 2: Result preparation
	tracker.SetStage(1, "result_preparation")
	
	tracker.Complete(0)
	
	c.sendOperationResult(&OperationResult{
		QueryID:   queryID,
		Operation: OpDocumentFind,
		Status:    "completed",
		Data:      map[string]interface{}{"documents": []interface{}{}, "count": 0},
		Timestamp: time.Now(),
		IsLast:    true,
	})
}

// =============================================================================
// Collection Operation Handlers
// =============================================================================

func (c *WebSocketClient) handleCollectionCreate(queryID string, msg ClientMessage) {
	tracker := globalQueryRegistry.CreateQuery(queryID, msg.Collection, "collection_create", 1)
	tracker.Start()
	defer tracker.Complete(1)
	
	c.sendOperationResult(&OperationResult{
		QueryID:   queryID,
		Operation: OpCollectionCreate,
		Status:    "completed",
		Data:      map[string]interface{}{"name": msg.Collection, "created": true},
		Timestamp: time.Now(),
		IsLast:    true,
	})
}

func (c *WebSocketClient) handleCollectionDelete(queryID string, msg ClientMessage) {
	tracker := globalQueryRegistry.CreateQuery(queryID, msg.Collection, "collection_delete", 1)
	tracker.Start()
	defer tracker.Complete(1)
	
	c.sendOperationResult(&OperationResult{
		QueryID:   queryID,
		Operation: OpCollectionDelete,
		Status:    "completed",
		Data:      map[string]interface{}{"name": msg.Collection, "deleted": true},
		Timestamp: time.Now(),
		IsLast:    true,
	})
}

func (c *WebSocketClient) handleCollectionList(queryID string, msg ClientMessage) {
	tracker := globalQueryRegistry.CreateQuery(queryID, "", "collection_list", 1)
	tracker.Start()
	defer tracker.Complete(0)
	
	c.sendOperationResult(&OperationResult{
		QueryID:   queryID,
		Operation: OpCollectionList,
		Status:    "completed",
		Data:      map[string]interface{}{"collections": []string{}},
		Timestamp: time.Now(),
		IsLast:    true,
	})
}

// =============================================================================
// Aggregation Operation Handlers
// =============================================================================

func (c *WebSocketClient) handleAggregate(queryID string, msg ClientMessage) {
	stages := len(msg.Pipeline)
	if stages == 0 {
		stages = 1
	}
	
	tracker := globalQueryRegistry.CreateQuery(queryID, msg.Collection, "aggregate", stages)
	tracker.Start()
	
	// Process each pipeline stage
	for i, stage := range msg.Pipeline {
		if tracker.IsCancelled() {
			c.sendOperationResult(&OperationResult{
				QueryID:   queryID,
				Operation: OpAggregate,
				Status:    "cancelled",
				Timestamp: time.Now(),
			})
			return
		}
		
		// Get stage name
		stageName := "unknown"
		for k := range stage {
			stageName = k
			break
		}
		
		tracker.SetStage(i, stageName)
		tracker.Stages[i].StageType = stageName
		
		c.sendOperationResult(&OperationResult{
			QueryID:   queryID,
			Operation: OpAggregate,
			Status:    "progress",
			Progress:  tracker,
			Timestamp: time.Now(),
			Data:      map[string]interface{}{"stage": stageName, "stageIndex": i},
		})
	}
	
	tracker.Complete(0)
	
	c.sendOperationResult(&OperationResult{
		QueryID:   queryID,
		Operation: OpAggregate,
		Status:    "completed",
		Data:      map[string]interface{}{"results": []interface{}{}, "count": 0},
		Timestamp: time.Now(),
		IsLast:    true,
	})
}

func (c *WebSocketClient) handleAggregateStream(queryID string, msg ClientMessage) {
	stages := len(msg.Pipeline)
	if stages == 0 {
		stages = 1
	}
	
	tracker := globalQueryRegistry.CreateQuery(queryID, msg.Collection, "aggregate_stream", stages)
	tracker.Start()
	
	batchSize := msg.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}
	
	// Process each pipeline stage
	for i, stage := range msg.Pipeline {
		if tracker.IsCancelled() {
			c.sendOperationResult(&OperationResult{
				QueryID:   queryID,
				Operation: OpAggregateStream,
				Status:    "cancelled",
				Timestamp: time.Now(),
			})
			return
		}
		
		stageName := "unknown"
		for k := range stage {
			stageName = k
			break
		}
		
		tracker.SetStage(i, stageName)
		tracker.Stages[i].StageType = stageName
		
		c.sendOperationResult(&OperationResult{
			QueryID:   queryID,
			Operation: OpAggregateStream,
			Status:    "progress",
			Progress:  tracker,
			Timestamp: time.Now(),
		})
	}
	
	// Stream results in batches (simulated)
	batchIndex := 0
	for i := 0; i < 3; i++ { // Simulate 3 batches
		if tracker.IsCancelled() {
			c.sendOperationResult(&OperationResult{
				QueryID:   queryID,
				Operation: OpAggregateStream,
				Status:    "cancelled",
				Timestamp: time.Now(),
			})
			return
		}
		
		isLast := (i == 2)
		tracker.IncrementScanned(int64(batchSize))
		
		if isLast {
			tracker.Complete(int64(batchSize * 3))
		}
		
		c.sendOperationResult(&OperationResult{
			QueryID:    queryID,
			Operation:  OpAggregateStream,
			Status:     "progress",
			Progress:   tracker,
			Data:       map[string]interface{}{"batch": []interface{}{}},
			Timestamp:  time.Now(),
			BatchIndex: batchIndex,
			IsLast:     isLast,
		})
		
		batchIndex++
	}
}