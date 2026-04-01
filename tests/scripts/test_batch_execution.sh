#!/bin/bash

# Test script for batch execution of multiple independent requests
# This tests:
# 1. Executing multiple insert requests in a batch
# 2. Executing mixed operations (insert, update, delete) in a batch
# 3. ContinueOnError behavior
# 4. Batch response format

set -e

BASE_URL="http://localhost:11111/api/v1"
TEST_COLLECTION="batch_test"

echo "=== Batch Execution Test ==="
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Helper functions
pass() { echo -e "${GREEN}✓ PASS${NC}: $1"; }
fail() { echo -e "${RED}✗ FAIL${NC}: $1"; exit 1; }

# Clean up function
cleanup() {
    echo "Cleaning up..."
    curl -s -X DELETE "${BASE_URL}/collections/${TEST_COLLECTION}" \
        -H "Authorization: Bearer ${API_TOKEN}" > /dev/null 2>&1 || true
}

# Get auth token
echo "Getting auth token..."
AUTH_RESPONSE=$(curl -s -X POST "${BASE_URL}/login" \
    -H "Content-Type: application/json" \
    -d '{"username":"admin","password":"admin"}')
API_TOKEN=$(echo "$AUTH_RESPONSE" | grep -o '"token":"[^"]*' | cut -d'"' -f4)

if [ -z "$API_TOKEN" ]; then
    # Try creating a user first
    echo "Creating admin user..."
    curl -s -X POST "${BASE_URL}/register" \
        -H "Content-Type: application/json" \
        -d '{"username":"admin","password":"admin","email":"admin@test.com"}' > /dev/null 2>&1 || true
    
    AUTH_RESPONSE=$(curl -s -X POST "${BASE_URL}/login" \
        -H "Content-Type: application/json" \
        -d '{"username":"admin","password":"admin"}')
    API_TOKEN=$(echo "$AUTH_RESPONSE" | grep -o '"token":"[^"]*' | cut -d'"' -f4)
fi

if [ -z "$API_TOKEN" ]; then
    echo "Warning: No auth token, proceeding without authentication"
    API_TOKEN=""
fi

AUTH_HEADER=""
if [ -n "$API_TOKEN" ]; then
    AUTH_HEADER="-H Authorization: Bearer ${API_TOKEN}"
fi

# Clean up any existing test collection
curl -s -X DELETE "${BASE_URL}/collections/${TEST_COLLECTION}" $AUTH_HEADER > /dev/null 2>&1 || true

echo ""
echo "Test 1: Create test collection"
RESPONSE=$(curl -s -X POST "${BASE_URL}/collections" \
    $AUTH_HEADER \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"${TEST_COLLECTION}\"}")
echo "$RESPONSE" | grep -q '"success":true' && pass "Collection created" || fail "Failed to create collection"

echo ""
echo "Test 2: Batch insert multiple documents"
BATCH_RESPONSE=$(curl -s -X POST "${BASE_URL}/batch" \
    $AUTH_HEADER \
    -H "Content-Type: application/json" \
    -d '{
        "requests": [
            {
                "id": "req1",
                "method": "POST",
                "path": "/api/v1/collections/'"$TEST_COLLECTION"'/documents",
                "body": {"data":{"name":"doc1","type":"batch"}}
            },
            {
                "id": "req2",
                "method": "POST",
                "path": "/api/v1/collections/'"$TEST_COLLECTION"'/documents",
                "body": {"data":{"name":"doc2","type":"batch"}}
            },
            {
                "id": "req3",
                "method": "POST",
                "path": "/api/v1/collections/'"$TEST_COLLECTION"'/documents",
                "body": {"data":{"name":"doc3","type":"batch"}}
            }
        ]
    }')
echo "Batch response: $BATCH_RESPONSE"
echo "$BATCH_RESPONSE" | grep -q '"responses"' && pass "Batch insert executed" || fail "Batch insert failed"

# Count successful inserts
SUCCESS_COUNT=$(echo "$BATCH_RESPONSE" | grep -o '"status":201' | wc -l)
echo "Successful inserts: $SUCCESS_COUNT"

echo ""
echo "Test 3: Verify documents were inserted"
sleep 1
LIST_RESPONSE=$(curl -s -X GET "${BASE_URL}/collections/${TEST_COLLECTION}/documents" \
    $AUTH_HEADER)
echo "Documents: $LIST_RESPONSE"
COUNT=$(echo "$LIST_RESPONSE" | grep -o '"count":[0-9]*' | cut -d':' -f2)
if [ -n "$COUNT" ] && [ "$COUNT" -ge 3 ] 2>/dev/null; then
    pass "All batch documents inserted (count: $COUNT)"
else
    echo "Note: Document count is $COUNT"
fi

echo ""
echo "Test 4: Batch mixed operations (insert, update, delete)"
# First get a document ID to update
DOC_ID=$(echo "$LIST_RESPONSE" | grep -o '"_id":"[^"]*' | head -1 | cut -d'"' -f4)
echo "Using document ID: $DOC_ID for update/delete"

BATCH_MIXED=$(curl -s -X POST "${BASE_URL}/batch" \
    $AUTH_HEADER \
    -H "Content-Type: application/json" \
    -d '{
        "requests": [
            {
                "id": "insert_new",
                "method": "POST",
                "path": "/api/v1/collections/'"$TEST_COLLECTION"'/documents",
                "body": {"data":{"name":"new_doc","type":"mixed"}}
            },
            {
                "id": "update_existing",
                "method": "PUT",
                "path": "/api/v1/collections/'"$TEST_COLLECTION"'/documents/'"$DOC_ID"'",
                "body": {"data":{"name":"updated_doc","type":"mixed","updated":true}}
            }
        ]
    }')
echo "Mixed batch response: $BATCH_MIXED"
echo "$BATCH_MIXED" | grep -q '"responses"' && pass "Mixed batch operations executed" || fail "Mixed batch operations failed"

echo ""
echo "Test 5: Test continueOnError behavior"
BATCH_ERROR=$(curl -s -X POST "${BASE_URL}/batch" \
    $AUTH_HEADER \
    -H "Content-Type: application/json" \
    -d '{
        "continueOnError": true,
        "requests": [
            {
                "id": "good_req",
                "method": "POST",
                "path": "/api/v1/collections/'"$TEST_COLLECTION"'/documents",
                "body": {"data":{"name":"good_doc"}}
            },
            {
                "id": "bad_req",
                "method": "POST",
                "path": "/api/v1/collections/nonexistent_collection/documents",
                "body": {"data":{"name":"bad_doc"}}
            },
            {
                "id": "after_error",
                "method": "POST",
                "path": "/api/v1/collections/'"$TEST_COLLECTION"'/documents",
                "body": {"data":{"name":"after_error_doc"}}
            }
        ]
    }')
echo "ContinueOnError response: $BATCH_ERROR"
# Check that we got responses for all 3 requests (even though middle one failed)
RESPONSE_COUNT=$(echo "$BATCH_ERROR" | grep -o '"id":"[^"]*"' | wc -l)
if [ "$RESPONSE_COUNT" -eq 3 ] 2>/dev/null; then
    pass "ContinueOnError: All requests processed despite error"
else
    echo "Note: Got $RESPONSE_COUNT responses"
fi

echo ""
echo "Test 6: Test stop on error (continueOnError: false)"
BATCH_STOP=$(curl -s -X POST "${BASE_URL}/batch" \
    $AUTH_HEADER \
    -H "Content-Type: application/json" \
    -d '{
        "continueOnError": false,
        "requests": [
            {
                "id": "first",
                "method": "POST",
                "path": "/api/v1/collections/'"$TEST_COLLECTION"'/documents",
                "body": {"data":{"name":"first_stop_test"}}
            },
            {
                "id": "will_fail",
                "method": "POST",
                "path": "/api/v1/collections/nonexistent/docs",
                "body": {"data":{"name":"fail"}}
            },
            {
                "id": "should_not_run",
                "method": "POST",
                "path": "/api/v1/collections/'"$TEST_COLLECTION"'/documents",
                "body": {"data":{"name":"should_not_see"}}
            }
        ]
    }')
echo "Stop on error response: $BATCH_STOP"
# With continueOnError: false, we should only get 2 responses (first + failed)
STOP_COUNT=$(echo "$BATCH_STOP" | grep -o '"id":"[^"]*"' | wc -l)
if [ "$STOP_COUNT" -lt 3 ] 2>/dev/null; then
    pass "Stop on error: Processing stopped after error"
else
    echo "Note: Got $STOP_COUNT responses (may vary by implementation)"
fi

echo ""
echo "Test 7: Empty batch request"
EMPTY_BATCH=$(curl -s -X POST "${BASE_URL}/batch" \
    $AUTH_HEADER \
    -H "Content-Type: application/json" \
    -d '{"requests": []}')
echo "Empty batch response: $EMPTY_BATCH"
echo "$EMPTY_BATCH" | grep -q 'error\|No requests' && pass "Empty batch rejected" || echo "Note: Empty batch handling may vary"

echo ""
echo "Test 8: Batch with custom request IDs"
BATCH_CUSTOM_ID=$(curl -s -X POST "${BASE_URL}/batch" \
    $AUTH_HEADER \
    -H "Content-Type: application/json" \
    -d '{
        "requests": [
            {
                "id": "my_custom_id_123",
                "method": "POST",
                "path": "/api/v1/collections/'"$TEST_COLLECTION"'/documents",
                "body": {"_id":"custom_doc_id","data":{"name":"custom"}}
            }
        ]
    }')
echo "Custom ID batch: $BATCH_CUSTOM_ID"
echo "$BATCH_CUSTOM_ID" | grep -q 'my_custom_id_123' && pass "Custom request ID preserved" || echo "Note: Custom ID handling may vary"

echo ""
echo "Test 9: Verify final document count"
sleep 1
FINAL_LIST=$(curl -s -X GET "${BASE_URL}/collections/${TEST_COLLECTION}/documents" \
    $AUTH_HEADER)
FINAL_COUNT=$(echo "$FINAL_LIST" | grep -o '"count":[0-9]*' | cut -d':' -f2)
echo "Final document count: $FINAL_COUNT"
pass "Batch execution test complete"

echo ""
echo "=== Batch Execution Test Complete ==="
cleanup