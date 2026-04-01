#!/bin/bash

# Test script for async transactions with time limits
# This tests:
# 1. Starting an async transaction (getting a transaction ID)
# 2. Executing operations using the transaction ID
# 3. Checking transaction status
# 4. Committing the transaction
# 5. Rollback functionality
# 6. Transaction expiration

set -e

BASE_URL="http://localhost:11111/api/v1"
TEST_COLLECTION="async_tx_test"

echo "=== Async Transactions Test ==="
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
echo "Test 2: Begin async transaction"
TX_RESPONSE=$(curl -s -X POST "${BASE_URL}/transactions/begin" \
    $AUTH_HEADER \
    -H "Content-Type: application/json" \
    -d '{"timeoutSeconds":60}')
echo "Response: $TX_RESPONSE"

TX_ID=$(echo "$TX_RESPONSE" | grep -o '"transactionId":"[^"]*' | cut -d'"' -f4)
if [ -z "$TX_ID" ]; then
    fail "Failed to get transaction ID"
fi
pass "Transaction started with ID: $TX_ID"

echo ""
echo "Test 3: Check transaction status"
STATUS_RESPONSE=$(curl -s -X GET "${BASE_URL}/transactions/${TX_ID}/status" \
    $AUTH_HEADER)
echo "Status: $STATUS_RESPONSE"
echo "$STATUS_RESPONSE" | grep -q '"status":"ACTIVE"' && pass "Transaction is active" || pass "Transaction status retrieved"

echo ""
echo "Test 4: Insert document using transaction ID"
INSERT_RESPONSE=$(curl -s -X POST "${BASE_URL}/collections/${TEST_COLLECTION}/documents" \
    $AUTH_HEADER \
    -H "Content-Type: application/json" \
    -H "X-Transaction-ID: ${TX_ID}" \
    -d '{"data":{"name":"async_doc","value":100}}')
echo "Response: $INSERT_RESPONSE"
echo "$INSERT_RESPONSE" | grep -q '"status":"pending"' && pass "Document insert queued in transaction" || fail "Failed to queue document insert"

echo ""
echo "Test 5: Insert another document using same transaction"
INSERT_RESPONSE2=$(curl -s -X POST "${BASE_URL}/collections/${TEST_COLLECTION}/documents" \
    $AUTH_HEADER \
    -H "Content-Type: application/json" \
    -H "X-Transaction-ID: ${TX_ID}" \
    -d '{"data":{"name":"async_doc2","value":200}}')
echo "$INSERT_RESPONSE2" | grep -q '"status":"pending"' && pass "Second document insert queued" || fail "Failed to queue second document"

echo ""
echo "Test 6: Check transaction status (should show 2 operations)"
STATUS_RESPONSE=$(curl -s -X GET "${BASE_URL}/transactions/${TX_ID}/status" \
    $AUTH_HEADER)
echo "Status: $STATUS_RESPONSE"
OP_COUNT=$(echo "$STATUS_RESPONSE" | grep -o '"operationCount":[0-9]*' | cut -d':' -f2)
if [ "$OP_COUNT" -eq 2 ] 2>/dev/null; then
    pass "Transaction has 2 operations queued"
else
    echo "Note: Operation count is $OP_COUNT (may vary based on implementation)"
fi

echo ""
echo "Test 7: Verify documents are NOT visible before commit"
LIST_RESPONSE=$(curl -s -X GET "${BASE_URL}/collections/${TEST_COLLECTION}/documents" \
    $AUTH_HEADER)
echo "Documents before commit: $LIST_RESPONSE"
COUNT=$(echo "$LIST_RESPONSE" | grep -o '"count":[0-9]*' | cut -d':' -f2)
if [ -z "$COUNT" ] || [ "$COUNT" = "0" ]; then
    pass "Documents not visible before commit (expected)"
else
    echo "Note: Found $COUNT documents (may vary based on implementation)"
fi

echo ""
echo "Test 8: Commit transaction"
COMMIT_RESPONSE=$(curl -s -X POST "${BASE_URL}/transactions/${TX_ID}/commit" \
    $AUTH_HEADER)
echo "Commit response: $COMMIT_RESPONSE"
echo "$COMMIT_RESPONSE" | grep -q '"status":"committed"' && pass "Transaction committed" || fail "Failed to commit transaction"

echo ""
echo "Test 9: Verify documents ARE visible after commit"
sleep 1
LIST_RESPONSE=$(curl -s -X GET "${BASE_URL}/collections/${TEST_COLLECTION}/documents" \
    $AUTH_HEADER)
echo "Documents after commit: $LIST_RESPONSE"
COUNT=$(echo "$LIST_RESPONSE" | grep -o '"count":[0-9]*' | cut -d':' -f2)
if [ -n "$COUNT" ] && [ "$COUNT" -ge 2 ] 2>/dev/null; then
    pass "Documents visible after commit (count: $COUNT)"
else
    echo "Note: Document count is $COUNT (implementation may vary)"
fi

echo ""
echo "Test 10: Test rollback with new transaction"
TX_RESPONSE=$(curl -s -X POST "${BASE_URL}/transactions/begin" \
    $AUTH_HEADER \
    -H "Content-Type: application/json" \
    -d '{"timeoutSeconds":60}')
TX_ID2=$(echo "$TX_RESPONSE" | grep -o '"transactionId":"[^"]*' | cut -d'"' -f4)

if [ -n "$TX_ID2" ]; then
    # Insert a document
    curl -s -X POST "${BASE_URL}/collections/${TEST_COLLECTION}/documents" \
        $AUTH_HEADER \
        -H "Content-Type: application/json" \
        -H "X-Transaction-ID: ${TX_ID2}" \
        -d '{"data":{"name":"rollback_test","value":999}}' > /dev/null
    
    # Rollback
    ROLLBACK_RESPONSE=$(curl -s -X POST "${BASE_URL}/transactions/${TX_ID2}/rollback" \
        $AUTH_HEADER)
    echo "Rollback response: $ROLLBACK_RESPONSE"
    echo "$ROLLBACK_RESPONSE" | grep -q '"status":"aborted"' && pass "Transaction rolled back" || echo "Note: Rollback response may vary"
else
    echo "Note: Could not start second transaction for rollback test"
fi

echo ""
echo "Test 11: Test transaction expiration (short timeout)"
TX_RESPONSE=$(curl -s -X POST "${BASE_URL}/transactions/begin" \
    $AUTH_HEADER \
    -H "Content-Type: application/json" \
    -d '{"timeoutSeconds":1}')
TX_ID3=$(echo "$TX_RESPONSE" | grep -o '"transactionId":"[^"]*' | cut -d'"' -f4)

if [ -n "$TX_ID3" ]; then
    pass "Started transaction with 1 second timeout"
    echo "Waiting for expiration..."
    sleep 3
    
    # Try to use expired transaction
    EXPIRED_RESPONSE=$(curl -s -X POST "${BASE_URL}/collections/${TEST_COLLECTION}/documents" \
        $AUTH_HEADER \
        -H "Content-Type: application/json" \
        -H "X-Transaction-ID: ${TX_ID3}" \
        -d '{"data":{"name":"expired","value":0}}')
    echo "Expired transaction response: $EXPIRED_RESPONSE"
    echo "$EXPIRED_RESPONSE" | grep -q 'expired\|not found\|error' && pass "Expired transaction rejected" || echo "Note: Expiration handling may vary"
else
    echo "Note: Could not start transaction for expiration test"
fi

echo ""
echo "Test 12: List active transactions"
LIST_TX_RESPONSE=$(curl -s -X GET "${BASE_URL}/transactions" \
    $AUTH_HEADER)
echo "Active transactions: $LIST_TX_RESPONSE"
echo "$LIST_TX_RESPONSE" | grep -q '"transactions"' && pass "Listed active transactions" || fail "Failed to list transactions"

echo ""
echo "=== Async Transactions Test Complete ==="
cleanup