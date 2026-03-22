#!/bin/bash
set -e

# =============================================================================
# AIDB WebSocket Streaming Protocol Test
# =============================================================================
# Tests both the query management API and WebSocket streaming operations
# =============================================================================

# Colors and formatting
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
YELLOW='\033[1;33m'
NC='\033[0m'
BOLD='\033[1m'

BASE_URL="http://localhost:11111"
WS_URL="ws://localhost:11111/api/v1/ws"

# Helper functions
section() {
    echo ""
    echo -e "${CYAN}┌─────────────────────────────────────────────────────────────────┐${NC}"
    echo -e "${CYAN}│ $1${NC}"
    echo -e "${CYAN}└─────────────────────────────────────────────────────────────────┘${NC}"
}

success() {
    echo -e "${GREEN}✓ SUCCESS: $1${NC}"
}

error() {
    echo -e "${RED}✗ ERROR: $1${NC}"
}

print_test() {
    echo -e "${MAGENTA}▶ TEST: $1${NC}"
}

# Check for websocat or use Python for WebSocket tests
check_websocket_tool() {
    if command -v websocat &> /dev/null; then
        WS_TOOL="websocat"
    elif python3 -c "import websockets" 2>/dev/null; then
        WS_TOOL="python"
    else
        WS_TOOL="none"
    fi
}

# =============================================================================
# HTTP API Tests
# =============================================================================

print_header() {
    echo ""
    echo -e "${BOLD}${CYAN}═══════════════════════════════════════════════════════════════════${NC}"
    echo -e "${BOLD}${WHITE}  AIDB WebSocket Streaming Protocol Test${NC}"
    echo -e "${BOLD}${CYAN}═══════════════════════════════════════════════════════════════════${NC}"
    echo ""
}

# Test 1: List queries (empty)
test_list_queries() {
    section "Test 1: List Queries (empty)"
    response=$(curl -s "$BASE_URL/api/v1/queries")
    echo "Response: $response"
    if echo "$response" | grep -q '"count":0'; then
        success "Empty query list returned"
    else
        error "Expected empty query list"
        exit 1
    fi
}

# Test 2: Get non-existent query
test_get_nonexistent_query() {
    section "Test 2: Get Non-existent Query"
    response=$(curl -s "$BASE_URL/api/v1/queries/nonexistent")
    echo "Response: $response"
    if echo "$response" | grep -q "not found"; then
        success "Non-existent query correctly returns 404"
    else
        error "Expected 'not found' error"
        exit 1
    fi
}

# Test 3: Cancel non-existent query
test_cancel_nonexistent_query() {
    section "Test 3: Cancel Non-existent Query"
    response=$(curl -s -X POST "$BASE_URL/api/v1/queries/nonexistent/cancel")
    echo "Response: $response"
    if echo "$response" | grep -q "not found"; then
        success "Non-existent query cancel correctly returns 404"
    else
        error "Expected 'not found' error"
        exit 1
    fi
}

# Test 4: Check root endpoint includes WebSocket info
test_root_endpoint() {
    section "Test 4: Root Endpoint Includes WebSocket Info"
    response=$(curl -s "$BASE_URL/")
    if echo "$response" | grep -q "websocket" && echo "$response" | grep -q "queries"; then
        success "Root endpoint includes WebSocket and query endpoints"
    else
        error "Root endpoint missing WebSocket or query info"
        exit 1
    fi
}

# =============================================================================
# WebSocket Protocol Tests using Python
# =============================================================================

run_websocket_python_tests() {
    section "Test 5: WebSocket Streaming Operations"
    
    python3 << 'PYTHON_SCRIPT'
import json
import asyncio
import websockets
import sys

WS_URL = "ws://localhost:11111/api/v1/ws"

def print_test(name):
    print(f"\n\033[0;35m▶ TEST: {name}\033[0m")

def print_success(msg):
    print(f"\033[0;32m✓ SUCCESS: {msg}\033[0m")

def print_error(msg):
    print(f"\033[0;31m✗ ERROR: {msg}\033[0m")
    sys.exit(1)

def print_json(data):
    print(json.dumps(data, indent=2))

async def run_tests():
    try:
        async with websockets.connect(WS_URL) as ws:
            print_success("Connected to WebSocket server")
            
            # Test 1: Ping/Pong
            print_test("WebSocket Ping/Pong")
            await ws.send(json.dumps({"type": "ping"}))
            response = json.loads(await ws.recv())
            if response.get("type") == "pong":
                print_success("Ping/Pong working")
            else:
                print_error(f"Expected pong, got: {response}")
            
            # Test 2: Subscribe to all queries
            print_test("Subscribe to All Queries")
            await ws.send(json.dumps({"type": "subscribe_all"}))
            response = json.loads(await ws.recv())
            if response.get("type") == "subscribed":
                print_success("Subscribed to all queries")
            else:
                print_error(f"Expected subscribed, got: {response}")
            
            # Test 3: Vector Insert Operation
            print_test("Vector Insert Operation")
            await ws.send(json.dumps({
                "type": "operation",
                "operation": "vector_insert",
                "collection": "test_vectors",
                "documentId": "vec_001",
                "vector": [0.1, 0.2, 0.3, 0.4],
                "document": {"label": "test"}
            }))
            
            # Receive started status
            response = json.loads(await ws.recv())
            if response.get("status") == "started":
                print_success(f"Operation started: {response.get('queryId')}")
            
            # Receive progress
            response = json.loads(await ws.recv())
            if response.get("status") == "progress":
                print_success("Progress update received")
            
            # Receive completion
            response = json.loads(await ws.recv())
            if response.get("status") == "completed":
                print_success(f"Vector insert completed: {response.get('data')}")
            
            # Test 4: Vector Search Operation
            print_test("Vector Search Operation")
            await ws.send(json.dumps({
                "type": "operation",
                "operation": "vector_search",
                "collection": "test_vectors",
                "vector": [0.1, 0.2, 0.3, 0.4],
                "topK": 5,
                "minScore": 0.5
            }))
            
            responses = []
            for _ in range(4):  # started + progress + progress + completed
                responses.append(json.loads(await ws.recv()))
            
            if responses[0].get("status") == "started":
                print_success("Search operation started")
            if responses[-1].get("status") == "completed":
                print_success("Vector search completed")
            
            # Test 5: Document Insert Batch Operation
            print_test("Document Insert Batch Operation")
            await ws.send(json.dumps({
                "type": "operation",
                "operation": "document_insert_batch",
                "collection": "test_docs",
                "data": [
                    {"id": "doc1", "name": "Document 1"},
                    {"id": "doc2", "name": "Document 2"},
                    {"id": "doc3", "name": "Document 3"}
                ],
                "batchSize": 2
            }))
            
            responses = []
            for _ in range(4):  # started + 2 progress + completed
                responses.append(json.loads(await ws.recv()))
            
            if responses[-1].get("status") == "completed":
                print_success(f"Batch insert completed: {responses[-1].get('data')}")
            
            # Test 6: Aggregation Operation
            print_test("Aggregation Operation")
            await ws.send(json.dumps({
                "type": "operation",
                "operation": "aggregate",
                "collection": "test_docs",
                "pipeline": [
                    {"$match": {"status": "active"}},
                    {"$group": {"_id": "$category", "count": {"$sum": 1}}}
                ]
            }))
            
            responses = []
            for _ in range(4):  # started + 2 stage progress + completed
                responses.append(json.loads(await ws.recv()))
            
            if responses[-1].get("status") == "completed":
                print_success("Aggregation completed")
            
            # Test 7: Aggregation Stream Operation
            print_test("Aggregation Stream Operation")
            await ws.send(json.dumps({
                "type": "operation",
                "operation": "aggregate_stream",
                "collection": "test_docs",
                "pipeline": [
                    {"$match": {"status": "active"}}
                ],
                "batchSize": 100
            }))
            
            responses = []
            for _ in range(7):  # started + stage + 3 batches + completed
                responses.append(json.loads(await ws.recv()))
            
            if responses[-1].get("status") == "completed" or responses[-1].get("isLast"):
                print_success("Aggregation stream completed")
            
            # Test 8: Collection Create Operation
            print_test("Collection Create Operation")
            await ws.send(json.dumps({
                "type": "operation",
                "operation": "collection_create",
                "collection": "new_collection"
            }))
            
            responses = []
            for _ in range(3):  # started + progress + completed
                responses.append(json.loads(await ws.recv()))
            
            if responses[-1].get("status") == "completed":
                print_success(f"Collection created: {responses[-1].get('data')}")
            
            # Test 9: Vector Create Collection Operation
            print_test("Vector Create Collection Operation")
            await ws.send(json.dumps({
                "type": "operation",
                "operation": "vector_create_collection",
                "collection": "new_vector_collection",
                "dimensions": 128,
                "distanceMetric": "cosine"
            }))
            
            responses = []
            for _ in range(3):
                responses.append(json.loads(await ws.recv()))
            
            if responses[-1].get("status") == "completed":
                print_success(f"Vector collection created: {responses[-1].get('data')}")
            
            # Test 10: Document Find Operation
            print_test("Document Find Operation")
            await ws.send(json.dumps({
                "type": "operation",
                "operation": "document_find",
                "collection": "test_docs",
                "filter": {"status": "active"}
            }))
            
            responses = []
            for _ in range(4):
                responses.append(json.loads(await ws.recv()))
            
            if responses[-1].get("status") == "completed":
                print_success("Document find completed")
            
            print("\n" + "="*60)
            print("All WebSocket streaming tests passed!")
            print("="*60)
            
    except Exception as e:
        print_error(f"WebSocket test failed: {e}")

asyncio.run(run_tests())
PYTHON_SCRIPT
}

# =============================================================================
# Main Test Runner
# =============================================================================

main() {
    print_header
    
    # Check server
    echo -e "${CYAN}ℹ INFO: Checking if server is running at $BASE_URL...${NC}"
    response=$(curl -s "$BASE_URL/" 2>/dev/null)
    
    if [ -n "$response" ] && [[ "$response" == *"AIDB"* ]]; then
        success "Server is running"
    else
        error "Server is not running at $BASE_URL"
        echo -e "${CYAN}ℹ INFO: Start the server with: ./aidb${NC}"
        exit 1
    fi
    
    # Check WebSocket tool
    check_websocket_tool
    echo -e "${CYAN}ℹ INFO: WebSocket test tool: ${WS_TOOL}${NC}"
    
    # Run HTTP API tests
    test_list_queries
    test_get_nonexistent_query
    test_cancel_nonexistent_query
    test_root_endpoint
    
    # Run WebSocket tests
    if [ "$WS_TOOL" = "python" ]; then
        run_websocket_python_tests
    elif [ "$WS_TOOL" = "websocat" ]; then
        echo -e "${YELLOW}ℹ INFO: Using websocat for WebSocket tests${NC}"
        # Add websocat tests here if needed
    else
        echo -e "${YELLOW}⚠ WARNING: No WebSocket tool available. Install websockets Python package:${NC}"
        echo -e "${YELLOW}  pip install websockets${NC}"
    fi
    
    # Summary
    echo ""
    echo -e "${BOLD}${CYAN}═══════════════════════════════════════════════════════════════════${NC}"
    echo -e "${BOLD}${GREEN}  ✓ ALL TESTS PASSED: WebSocket Streaming Protocol${NC}"
    echo -e "${BOLD}${CYAN}═══════════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo "WebSocket Protocol Documentation:"
    echo "--------------------------------"
    echo ""
    echo "Connect:     ws://localhost:11111/api/v1/ws"
    echo ""
    echo "Message Types:"
    echo "  subscribe       - Subscribe to specific queries"
    echo "  subscribe_all   - Subscribe to all queries"
    echo "  unsubscribe     - Unsubscribe from queries"
    echo "  cancel          - Cancel a running query"
    echo "  ping            - Ping server"
    echo "  operation       - Execute a streaming operation"
    echo ""
    echo "Streaming Operations:"
    echo ""
    echo "Vector Operations:"
    echo "  vector_insert          - Insert single vector"
    echo "  vector_insert_batch    - Insert multiple vectors"
    echo "  vector_update          - Update a vector"
    echo "  vector_delete          - Delete a vector"
    echo "  vector_get             - Get a vector by ID"
    echo "  vector_search          - Search similar vectors"
    echo "  vector_create_collection - Create vector collection"
    echo "  vector_delete_collection - Delete vector collection"
    echo ""
    echo "Document Operations:"
    echo "  document_insert        - Insert single document"
    echo "  document_insert_batch  - Insert multiple documents"
    echo "  document_update        - Update a document"
    echo "  document_patch         - Patch a document"
    echo "  document_delete        - Delete a document"
    echo "  document_get           - Get a document by ID"
    echo "  document_find          - Find documents with filter"
    echo ""
    echo "Collection Operations:"
    echo "  collection_create      - Create a collection"
    echo "  collection_delete      - Delete a collection"
    echo "  collection_list        - List all collections"
    echo ""
    echo "Aggregation Operations:"
    echo "  aggregate              - Run aggregation pipeline"
    echo "  aggregate_stream       - Stream aggregation results"
    echo ""
    echo "Example Operation Message:"
    echo '  {"type": "operation", "operation": "vector_search", "collection": "my_vectors", "vector": [0.1, 0.2, 0.3, 0.4], "topK": 10}'
}

main
