#!/bin/bash

# AIDB API Test Script
# This script tests all API endpoints of the AIDB server including Auth & RBAC
# Run with: ./test_api.sh

# Configuration
BASE_URL="${BASE_URL:-http://localhost:11111}"
API_URL="${BASE_URL}/api/v1"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test counters
TESTS_PASSED=0
TESTS_FAILED=0

# Helper functions
print_header() {
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}\n"
}

print_test() {
    echo -e "${YELLOW}TEST: $1${NC}"
}

print_success() {
    echo -e "${GREEN}✓ PASS: $1${NC}"
    ((TESTS_PASSED++))
}

print_failure() {
    echo -e "${RED}✗ FAIL: $1${NC}"
    echo -e "${RED}  Response: $2${NC}"
    ((TESTS_FAILED++))
}

check_response() {
    local response="$1"
    local expected="$2"
    local test_name="$3"
    
    # Use grep with flexible whitespace matching
    if echo "$response" | grep -qE "$expected"; then
        print_success "$test_name"
        return 0
    else
        print_failure "$test_name" "$response"
        return 1
    fi
}

check_success() {
    local response="$1"
    local test_name="$2"
    check_response "$response" '"success":[[:space:]]*true' "$test_name"
}

check_error() {
    local response="$1"
    local test_name="$2"
    check_response "$response" '"success":[[:space:]]*false' "$test_name"
}

# Extract document ID from response
extract_doc_id() {
    echo "$1" | grep -oE '"_id":[[:space:]]*"[^"]*"' | head -1 | grep -oE '"[^"]*"$' | tr -d '"'
}

# Extract Token from login response
extract_token() {
    echo "$1" | grep -oE '"token":[[:space:]]*"[^"]*"' | head -1 | sed 's/"token":[[:space:]]*"//' | sed 's/"//'
}

# ============================================
# TESTS START HERE
# ============================================

print_header "AIDB API Test Suite"
echo -e "Testing server at: ${YELLOW}${BASE_URL}${NC}"
echo ""

# --------------------------------------------
# 1. Health Check (Public)
# --------------------------------------------
print_header "1. Health Check"

print_test "Health endpoint should return healthy status"
RESPONSE=$(curl -s "${API_URL}/health" 2>/dev/null || echo '{"success":false}')
check_success "$RESPONSE" "Health check"

# --------------------------------------------
# 2. Authentication
# --------------------------------------------
print_header "2. Authentication"

# 2.1 Register Admin User
print_test "Register Admin User"
# Use a random username to avoid conflicts on re-runs if DB persists
RANDOM_SUFFIX=$RANDOM
ADMIN_USER="admin_${RANDOM_SUFFIX}"
RESPONSE=$(curl -s -X POST "${API_URL}/register" \
    -H "Content-Type: application/json" \
    -d "{
        \"username\": \"${ADMIN_USER}\",
        \"password\": \"password123\",
        \"email\": \"admin@example.com\",
        \"tenantId\": \"tenant-1\"
    }" 2>/dev/null || echo '{"success":false}')
check_success "$RESPONSE" "Register Admin User"

# 2.2 Login Admin User
print_test "Login Admin User"
RESPONSE=$(curl -s -X POST "${API_URL}/login" \
    -H "Content-Type: application/json" \
    -d "{
        \"username\": \"${ADMIN_USER}\",
        \"password\": \"password123\"
    }" 2>/dev/null || echo '{"success":false}')
check_success "$RESPONSE" "Login Admin User"

TOKEN=$(extract_token "$RESPONSE")
if [ -z "$TOKEN" ]; then
    print_failure "Extract Token" "Token not found in response"
    exit 1
fi
echo -e "  Token: ${GREEN}${TOKEN:0:20}...${NC}"

AUTH_HEADER="Authorization: Bearer $TOKEN"

# --------------------------------------------
# 3. RBAC Management
# --------------------------------------------
print_header "3. RBAC Management"

# 3.1 Create Role 'collection_manager'
print_test "Create Role 'collection_manager'"
ROLE_NAME="collection_manager_${RANDOM_SUFFIX}"
RESPONSE=$(curl -s -X POST "${API_URL}/roles" \
    -H "Content-Type: application/json" \
    -H "$AUTH_HEADER" \
    -d "{
        \"name\": \"${ROLE_NAME}\",
        \"description\": \"Can manage collections\",
        \"policies\": [
            {
                \"effect\": \"allow\",
                \"actions\": [\"create\", \"delete\", \"read\"],
                \"resources\": [\"tenant/tenant-1/region/*/env/*/collection/*\"]
            }
        ]
    }" 2>/dev/null || echo '{"success":false}')
check_success "$RESPONSE" "Create Role"

# 3.2 Create API Key
print_test "Create API Key"
RESPONSE=$(curl -s -X POST "${API_URL}/apikeys" \
    -H "Content-Type: application/json" \
    -H "$AUTH_HEADER" \
    -d '{
        "name": "test-key",
        "expiresIn": 3600
    }' 2>/dev/null || echo '{"success":false}')
check_success "$RESPONSE" "Create API Key"

# --------------------------------------------
# 4. Collection Operations (Authenticated)
# --------------------------------------------
print_header "4. Collection Operations"

# Clean up any existing test collections first
curl -s -X DELETE "${API_URL}/collections/test_users" -H "$AUTH_HEADER" > /dev/null 2>&1
curl -s -X DELETE "${API_URL}/collections/test_products" -H "$AUTH_HEADER" > /dev/null 2>&1

# 4.1 List collections
print_test "List all collections"
RESPONSE=$(curl -s "${API_URL}/collections" -H "$AUTH_HEADER" 2>/dev/null || echo '{"success":false}')
check_success "$RESPONSE" "List collections"

# 4.2 Create schema-less collection
print_test "Create schema-less collection 'test_users'"
RESPONSE=$(curl -s -X POST "${API_URL}/collections" \
    -H "Content-Type: application/json" \
    -H "$AUTH_HEADER" \
    -d '{"name": "test_users"}' 2>/dev/null || echo '{"success":false}')
check_success "$RESPONSE" "Create schema-less collection"

# 4.3 Create collection with schema
print_test "Create schema-full collection 'test_products'"
RESPONSE=$(curl -s -X POST "${API_URL}/collections" \
    -H "Content-Type: application/json" \
    -H "$AUTH_HEADER" \
    -d '{
        "name": "test_products",
        "schema": {
            "name": "test_products",
            "strict": true,
            "fields": {
                "name": {"type": "string", "required": true},
                "price": {"type": "number", "required": true}
            }
        }
    }' 2>/dev/null || echo '{"success":false}')
check_success "$RESPONSE" "Create schema-full collection"

# --------------------------------------------
# 5. Document Operations (Authenticated)
# --------------------------------------------
print_header "5. Document Operations"

# 5.1 Insert document
print_test "Insert document into 'test_users'"
RESPONSE=$(curl -s -X POST "${API_URL}/collections/test_users/documents" \
    -H "Content-Type: application/json" \
    -H "$AUTH_HEADER" \
    -d '{
        "data": {
            "name": "John Doe",
            "email": "john@example.com"
        }
    }' 2>/dev/null || echo '{"success":false}')
check_success "$RESPONSE" "Insert document"
USER_ID=$(extract_doc_id "$RESPONSE")

# 5.2 Get document
print_test "Get document by ID"
RESPONSE=$(curl -s "${API_URL}/collections/test_users/documents/${USER_ID}" \
    -H "$AUTH_HEADER" 2>/dev/null || echo '{"success":false}')
check_success "$RESPONSE" "Get document by ID"

# --------------------------------------------
# 6. Unauthorized Access Test
# --------------------------------------------
print_header "6. Unauthorized Access Test"

print_test "Access without token should fail"
RESPONSE=$(curl -s "${API_URL}/collections" 2>/dev/null || echo '{"success":false}')
check_error "$RESPONSE" "Access without token"

# --------------------------------------------
# 7. Cleanup
# --------------------------------------------
print_header "7. Cleanup"

print_test "Cleaning up test collections..."
curl -s -X DELETE "${API_URL}/collections/test_users" -H "$AUTH_HEADER" > /dev/null 2>&1
curl -s -X DELETE "${API_URL}/collections/test_products" -H "$AUTH_HEADER" > /dev/null 2>&1
echo -e "${GREEN}✓ Cleanup complete${NC}"

# --------------------------------------------
# SUMMARY
# --------------------------------------------
print_header "Test Summary"

echo -e "${GREEN}Tests Passed: ${TESTS_PASSED}${NC}"
echo -e "${RED}Tests Failed: ${TESTS_FAILED}${NC}"

if [ $TESTS_FAILED -eq 0 ]; then
    echo -e "\n${GREEN}All tests passed! ✓${NC}"
    exit 0
else
    echo -e "\n${RED}Some tests failed. ✗${NC}"
    exit 1
fi
