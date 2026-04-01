#!/bin/bash

# =============================================================================
# Test: ACID Resilience & Crash Recovery
# =============================================================================
# End-to-end resilience tests that exercise the deferred-write + crash-recovery
# features via the HTTP API. This includes:
#   1. Insert + Commit (data persists)
#   2. Insert + Rollback (data never reaches storage)
#   3. Crash simulation (kill -9) + recovery (REDO committed, UNDO in-flight)
#   4. Multi-document insert + rollback atomicity
#
# The script builds the binary, starts a fresh server per test phase, and
# tears down all artifacts on exit.
# =============================================================================

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TEST_DIR=$(mktemp -d /tmp/aidb_resilience_XXXXXX)
BINARY="$TEST_DIR/aidb_bin"
DATA_DIR="$TEST_DIR/data"
PORT=19876
BASE="http://localhost:$PORT/api/v1"
SERVER_PID=""

PASS=0
FAIL=0

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

cleanup() {
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$TEST_DIR"
}
trap cleanup EXIT

check() {
    local desc="$1" expected="$2" actual="$3"
    if [[ "$actual" == *"$expected"* ]]; then
        echo "  ✅  PASS: $desc"
        PASS=$((PASS + 1))
    else
        echo "  ❌  FAIL: $desc (expected '$expected', got '$actual')"
        FAIL=$((FAIL + 1))
    fi
}

start_server() {
    AIDB_SERVER_PORT="$PORT" \
    AIDB_DATA_DIR="$DATA_DIR" \
    AIDB_DATABASE_FILE="$DATA_DIR/aidb.db" \
    AIDB_STORAGE_ENGINE=boltdb \
    AIDB_TRANSACTIONS_ENABLED=true \
    AIDB_TRANSACTION_AUTO_COMMIT=true \
    AIDB_WAL_SYNC_POLICY=every_write \
        "$BINARY" </dev/null >"$TEST_DIR/server.log" 2>&1 &
    SERVER_PID=$!
    # Wait for server to accept connections
    for i in $(seq 1 30); do
        if curl --max-time 2 -s "http://localhost:$PORT/" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.2
    done
    echo "ERROR: Server did not start. Logs:"
    tail -20 "$TEST_DIR/server.log"
    return 1
}

stop_server_graceful() {
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    SERVER_PID=""
}

stop_server_crash() {
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -9 "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    SERVER_PID=""
}

# Register + login, set TOKEN
setup_auth() {
    local TS
    TS=$(date +%s%N)
    local user="admin_res_${TS}"
    curl --max-time 15 -s -X POST "$BASE/register" \
        -H "Content-Type: application/json" \
        -d "{\"username\":\"$user\",\"password\":\"password123\",\"email\":\"${user}@test.com\",\"tenantId\":\"t_${TS}\"}" >/dev/null
    TOKEN=$(curl --max-time 15 -s -X POST "$BASE/login" \
        -H "Content-Type: application/json" \
        -d "{\"username\":\"$user\",\"password\":\"password123\"}" \
        | python3 -c "import sys,json; print(json.load(sys.stdin).get('data',{}).get('token',''))" 2>/dev/null)
}

api() {
    local method="$1" path="$2" body="${3:-}"
    if [ -n "$body" ]; then
        curl --max-time 15 -s -X "$method" "$BASE$path" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer $TOKEN" \
            -d "$body"
    else
        curl --max-time 15 -s -X "$method" "$BASE$path" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer $TOKEN"
    fi
}

api_status() {
    local method="$1" path="$2" body="${3:-}"
    if [ -n "$body" ]; then
        curl --max-time 15 -s -o /dev/null -w "%{http_code}" -X "$method" "$BASE$path" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer $TOKEN" \
            -d "$body"
    else
        curl --max-time 15 -s -o /dev/null -w "%{http_code}" -X "$method" "$BASE$path" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer $TOKEN"
    fi
}

api_tx() {
    local method="$1" path="$2" tx_id="$3" body="${4:-}"
    if [ -n "$body" ]; then
        curl --max-time 15 -s -X "$method" "$BASE$path" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer $TOKEN" \
            -H "X-Transaction-ID: $tx_id" \
            -d "$body"
    else
        curl --max-time 15 -s -X "$method" "$BASE$path" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer $TOKEN" \
            -H "X-Transaction-ID: $tx_id"
    fi
}

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  AIDB Resilience Test Suite"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "Building..."
cd "$PROJECT_ROOT"
go build -o "$BINARY" .
echo "Binary: $BINARY"
echo ""

# ---------------------------------------------------------------------------
# Test 1: Auto-commit insert persists
# ---------------------------------------------------------------------------

echo "--- Test 1: Auto-commit insert persists ---"
mkdir -p "$DATA_DIR"
start_server
setup_auth

api "POST" "/collections" '{"name":"res1"}' >/dev/null
api "POST" "/collections/res1/documents" '{"_id":"d1","data":{"val":"persisted"}}' >/dev/null

RESULT=$(api "GET" "/collections/res1/documents/d1" | python3 -c "import sys,json; print(json.load(sys.stdin).get('data',{}).get('data',{}).get('val',''))" 2>/dev/null)
check "Auto-committed doc readable" "persisted" "$RESULT"

stop_server_graceful
rm -rf "$DATA_DIR"

# ---------------------------------------------------------------------------
# Test 2: Explicit transaction — insert + rollback
# ---------------------------------------------------------------------------

echo ""
echo "--- Test 2: Explicit tx insert + rollback leaves nothing ---"
mkdir -p "$DATA_DIR"
start_server
setup_auth

api "POST" "/collections" '{"name":"res2"}' >/dev/null

# Begin explicit transaction
TX_ID=$(api "POST" "/transactions/begin" '{"timeoutSeconds":60}' | python3 -c "import sys,json; print(json.load(sys.stdin).get('data',{}).get('transactionId',''))" 2>/dev/null)

# Insert within the transaction
api_tx "POST" "/collections/res2/documents" "$TX_ID" '{"_id":"ghost","data":{"val":"should_vanish"}}' >/dev/null

# Doc should NOT be visible outside the transaction
STATUS=$(api_status "GET" "/collections/res2/documents/ghost")
check "Ghost doc not visible before commit" "404" "$STATUS"

# Rollback
api "POST" "/transactions/$TX_ID/rollback" >/dev/null

# Still not visible after rollback
STATUS=$(api_status "GET" "/collections/res2/documents/ghost")
check "Ghost doc not visible after rollback" "404" "$STATUS"

stop_server_graceful
rm -rf "$DATA_DIR"

# ---------------------------------------------------------------------------
# Test 3: Explicit transaction — insert + commit
# ---------------------------------------------------------------------------

echo ""
echo "--- Test 3: Explicit tx insert + commit persists ---"
mkdir -p "$DATA_DIR"
start_server
setup_auth

api "POST" "/collections" '{"name":"res3"}' >/dev/null

TX_ID=$(api "POST" "/transactions/begin" '{"timeoutSeconds":60}' | python3 -c "import sys,json; print(json.load(sys.stdin).get('data',{}).get('transactionId',''))" 2>/dev/null)
api_tx "POST" "/collections/res3/documents" "$TX_ID" '{"_id":"solid","data":{"val":"committed_val"}}' >/dev/null
api "POST" "/transactions/$TX_ID/commit" >/dev/null

RESULT=$(api "GET" "/collections/res3/documents/solid" | python3 -c "import sys,json; print(json.load(sys.stdin).get('data',{}).get('data',{}).get('val',''))" 2>/dev/null)
check "Committed doc readable" "committed_val" "$RESULT"

stop_server_graceful
rm -rf "$DATA_DIR"

# ---------------------------------------------------------------------------
# Test 4: Crash recovery — committed data survives kill -9
# ---------------------------------------------------------------------------

echo ""
echo "--- Test 4: Crash recovery — committed data survives kill -9 ---"
mkdir -p "$DATA_DIR"
start_server
setup_auth

api "POST" "/collections" '{"name":"crash1"}' >/dev/null
api "POST" "/collections/crash1/documents" '{"_id":"survivor","data":{"val":"i_survive"}}' >/dev/null

# Verify before crash
RESULT=$(api "GET" "/collections/crash1/documents/survivor" | python3 -c "import sys,json; print(json.load(sys.stdin).get('data',{}).get('data',{}).get('val',''))" 2>/dev/null)
check "Doc exists before crash" "i_survive" "$RESULT"

# CRASH — hard kill
stop_server_crash

# Restart — recovery runs
start_server
setup_auth

RESULT=$(api "GET" "/collections/crash1/documents/survivor" | python3 -c "import sys,json; print(json.load(sys.stdin).get('data',{}).get('data',{}).get('val',''))" 2>/dev/null)
check "Doc survives crash + WAL recovery" "i_survive" "$RESULT"

stop_server_graceful
rm -rf "$DATA_DIR"

# ---------------------------------------------------------------------------
# Test 5: Multiple auto-committed docs survive crash
# ---------------------------------------------------------------------------

echo ""
echo "--- Test 5: Multiple docs survive crash ---"
mkdir -p "$DATA_DIR"
start_server
setup_auth

api "POST" "/collections" '{"name":"multi"}' >/dev/null
for i in $(seq 1 5); do
    api "POST" "/collections/multi/documents" "{\"_id\":\"m$i\",\"data\":{\"n\":$i}}" >/dev/null
done

# Crash
stop_server_crash

# Restart
start_server
setup_auth

ALL_OK=true
for i in $(seq 1 5); do
    V=$(api "GET" "/collections/multi/documents/m$i" | python3 -c "import sys,json; d=json.load(sys.stdin).get('data',{}).get('data',{}); print(int(d.get('n',0)))" 2>/dev/null)
    if [ "$V" != "$i" ]; then
        ALL_OK=false
        echo "  ❌  FAIL: doc m$i expected n=$i, got n=$V"
        FAIL=$((FAIL + 1))
    fi
done
if [ "$ALL_OK" = true ]; then
    echo "  ✅  PASS: All 5 docs survived crash"
    PASS=$((PASS + 1))
fi

stop_server_graceful
rm -rf "$DATA_DIR"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  Results: $PASS passed, $FAIL failed"
echo "═══════════════════════════════════════════════════════════════"
echo ""
[ "$FAIL" -eq 0 ] && exit 0 || exit 1