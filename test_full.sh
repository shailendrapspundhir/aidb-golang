#!/bin/bash

# AIDB Comprehensive Test Suite
# Logs requests/responses to requests.log
# Run with: ./test_full.sh

LOG_FILE="requests.log"
rm -f "$LOG_FILE"
touch "$LOG_FILE"

# Configuration
BASE_URL="${BASE_URL:-http://localhost:11111}"
API_URL="${BASE_URL}/api/v1"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Counters
TESTS_PASSED=0
TESTS_FAILED=0

# Helpers
print_header() {
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}\n"
    echo "========================================" >> "$LOG_FILE"
    echo "SECTION: $1" >> "$LOG_FILE"
    echo "========================================" >> "$LOG_FILE"
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
    ((TESTS_FAILED++))
}

# Python helper for JSON extraction
extract_data_field() {
    local json="$1"
    local field="$2"
    echo "$json" | python3 -c "import sys, json; 
try:
    data = json.load(sys.stdin)
    val = data.get('data', {}).get('$field')
    if val is None: val = data.get('$field')
    if val is not None: print(val)
except:
    pass"
}

extract_id() {
    extract_data_field "$1" "_id"
}

extract_token() {
    extract_data_field "$1" "token"
}

# Make Request
# Usage: make_request METHOD URL BODY [AUTH_HEADER] [DESCRIPTION]
make_request() {
    local method="$1"
    local url="$2"
    local body="$3"
    local auth="$4"
    local desc="$5"

    echo "------------------------------------------------" >> "$LOG_FILE"
    echo "TEST: $desc" >> "$LOG_FILE"
    echo "REQUEST: $method $url" >> "$LOG_FILE"
    
    local cmd=(curl -s -X "$method" "$url" -H "Content-Type: application/json")
    
    if [ ! -z "$auth" ]; then
        cmd+=(-H "$auth")
        echo "AUTH: $auth" >> "$LOG_FILE"
    fi
    
    if [ ! -z "$body" ]; then
        cmd+=(-d "$body")
        echo "BODY: $body" >> "$LOG_FILE"
    fi

    local response=$("${cmd[@]}")
    
    echo "RESPONSE: $response" >> "$LOG_FILE"
    if command -v python3 &> /dev/null; then
        echo "$response" | python3 -m json.tool >> "$LOG_FILE" 2>/dev/null
    fi
    echo "" >> "$LOG_FILE"
    
    echo "$response"
}

check_success() {
    local response="$1"
    local test_name="$2"
    
    if echo "$response" | grep -q '"success":[[:space:]]*true'; then
        print_success "$test_name"
        return 0
    else
        print_failure "$test_name"
        return 1
    fi
}

check_error() {
    local response="$1"
    local test_name="$2"
    
    if echo "$response" | grep -q '"success":[[:space:]]*false'; then
        print_success "$test_name (Expected Failure)"
        return 0
    else
        print_failure "$test_name (Expected Failure but got Success)"
        return 1
    fi
}

# ============================================
# TESTS START HERE
# ============================================

print_header "AIDB API Test Suite"
echo -e "Testing server at: ${YELLOW}${BASE_URL}${NC}"
echo "Full logs in: ${YELLOW}${LOG_FILE}${NC}"

# Generate Random Suffix for this run
RANDOM_SUFFIX=$RANDOM
TENANT_NAME="tenant_${RANDOM_SUFFIX}"
REGION_NAME="us-east_${RANDOM_SUFFIX}"
ENV_NAME="prod_${RANDOM_SUFFIX}"
COLLECTION_NAME="products_${RANDOM_SUFFIX}"

ROLE_WRITER="writer_${RANDOM_SUFFIX}"
ROLE_READER="reader_${RANDOM_SUFFIX}"
ROLE_RESTRICTED="restricted_${RANDOM_SUFFIX}"

# 1. Health Check
print_header "1. Health Check"
RESPONSE=$(make_request "GET" "${API_URL}/health" "" "" "Health Check")
check_success "$RESPONSE" "Health endpoint"


# 2. Authentication (Admin)
print_header "2. Authentication (Admin)"

ADMIN_USER="admin_${RANDOM_SUFFIX}"
RESPONSE=$(make_request "POST" "${API_URL}/register"     "{\"username\": \"${ADMIN_USER}\", \"password\": \"password123\", \"email\": \"admin@example.com\", \"tenantId\": \"tenant-1\"}"     "" "Register Admin User")
check_success "$RESPONSE" "Register Admin"

RESPONSE=$(make_request "POST" "${API_URL}/login"     "{\"username\": \"${ADMIN_USER}\", \"password\": \"password123\"}"     "" "Login Admin User")
check_success "$RESPONSE" "Login Admin"

ADMIN_TOKEN=$(extract_token "$RESPONSE")
ADMIN_AUTH="Authorization: Bearer $ADMIN_TOKEN"

if [ -z "$ADMIN_TOKEN" ]; then
    echo -e "${RED}Failed to get Admin Token. Aborting.${NC}"
    exit 1
fi


# 3. Hierarchy Management
print_header "3. Hierarchy Management"

# 3.1 Create Tenant
RESPONSE=$(make_request "POST" "${API_URL}/tenants"     "{\"name\": \"${TENANT_NAME}\"}"     "$ADMIN_AUTH" "Create Tenant '${TENANT_NAME}'")
check_success "$RESPONSE" "Create Tenant"

# 3.2 List Tenants
RESPONSE=$(make_request "GET" "${API_URL}/tenants" "" "$ADMIN_AUTH" "List Tenants")
check_success "$RESPONSE" "List Tenants"

# 3.3 Create Region
RESPONSE=$(make_request "POST" "${API_URL}/regions"     "{\"name\": \"${REGION_NAME}\"}"     "$ADMIN_AUTH" "Create Region '${REGION_NAME}'")
check_success "$RESPONSE" "Create Region"
REGION_ID=$(extract_id "$RESPONSE")

# 3.4 Create Environment
RESPONSE=$(make_request "POST" "${API_URL}/environments"     "{\"name\": \"${ENV_NAME}\", \"regionId\": \"$REGION_ID\"}"     "$ADMIN_AUTH" "Create Environment '${ENV_NAME}'")
check_success "$RESPONSE" "Create Environment"
ENV_ID=$(extract_id "$RESPONSE")


# 4. RBAC Setup
print_header "4. RBAC Setup"

# 4.1 Create Roles
RESPONSE=$(make_request "POST" "${API_URL}/roles"     "{
        \"name\": \"${ROLE_WRITER}\",
        \"description\": \"Can write products\",
        \"policies\": [
            {
                \"effect\": \"allow\",
                \"actions\": [\"create\", \"read\", \"update\", \"delete\"],
                \"resources\": [\"tenant/tenant-1/region/*/env/*/collection/${COLLECTION_NAME}\"]
            }
        ]
    }"     "$ADMIN_AUTH" "Create Role '${ROLE_WRITER}'")
check_success "$RESPONSE" "Create Writer Role"

RESPONSE=$(make_request "POST" "${API_URL}/roles"     "{
        \"name\": \"${ROLE_READER}\",
        \"description\": \"Can read products\",
        \"policies\": [
            {
                \"effect\": \"allow\",
                \"actions\": [\"read\"],
                \"resources\": [\"tenant/tenant-1/region/*/env/*/collection/${COLLECTION_NAME}\"]
            }
        ]
    }"     "$ADMIN_AUTH" "Create Role '${ROLE_READER}'")
check_success "$RESPONSE" "Create Reader Role"

RESPONSE=$(make_request "POST" "${API_URL}/roles"     "{
        \"name\": \"${ROLE_RESTRICTED}\",
        \"description\": \"Can read products name only\",
        \"policies\": [
            {
                \"effect\": \"allow\",
                \"actions\": [\"read\"],
                \"resources\": [\"tenant/tenant-1/region/*/env/*/collection/${COLLECTION_NAME}\"],
                \"fields\": [\"name\"]
            }
        ]
    }"     "$ADMIN_AUTH" "Create Role '${ROLE_RESTRICTED}'")
check_success "$RESPONSE" "Create Restricted Role"

# 4.2 Create Users
RESPONSE=$(make_request "POST" "${API_URL}/register"     "{
        \"username\": \"user_writer_${RANDOM_SUFFIX}\",
        \"password\": \"password123\",
        \"email\": \"writer@example.com\",
        \"tenantId\": \"tenant-1\"
    }"     "" "Register User 'user_writer'")
check_success "$RESPONSE" "Register Writer"
WRITER_ID=$(extract_id "$RESPONSE")
WRITER_USER="user_writer_${RANDOM_SUFFIX}"

RESPONSE=$(make_request "POST" "${API_URL}/register"     "{
        \"username\": \"user_reader_${RANDOM_SUFFIX}\",
        \"password\": \"password123\",
        \"email\": \"reader@example.com\",
        \"tenantId\": \"tenant-1\"
    }"     "" "Register User 'user_reader'")
check_success "$RESPONSE" "Register Reader"
READER_ID=$(extract_id "$RESPONSE")
READER_USER="user_reader_${RANDOM_SUFFIX}"

RESPONSE=$(make_request "POST" "${API_URL}/register"     "{
        \"username\": \"user_restricted_${RANDOM_SUFFIX}\",
        \"password\": \"password123\",
        \"email\": \"restricted@example.com\",
        \"tenantId\": \"tenant-1\"
    }"     "" "Register User 'user_restricted'")
check_success "$RESPONSE" "Register Restricted"
RESTRICTED_ID=$(extract_id "$RESPONSE")
RESTRICTED_USER="user_restricted_${RANDOM_SUFFIX}"

# 4.3 Assign Roles
RESPONSE=$(make_request "POST" "${API_URL}/users/roles"     "{\"userId\": \"$WRITER_ID\", \"roleId\": \"${ROLE_WRITER}\"}"     "$ADMIN_AUTH" "Assign '${ROLE_WRITER}' role to user_writer")
check_success "$RESPONSE" "Assign Writer Role"

RESPONSE=$(make_request "POST" "${API_URL}/users/roles"     "{\"userId\": \"$READER_ID\", \"roleId\": \"${ROLE_READER}\"}"     "$ADMIN_AUTH" "Assign '${ROLE_READER}' role to user_reader")
check_success "$RESPONSE" "Assign Reader Role"

RESPONSE=$(make_request "POST" "${API_URL}/users/roles"     "{\"userId\": \"$RESTRICTED_ID\", \"roleId\": \"${ROLE_RESTRICTED}\"}"     "$ADMIN_AUTH" "Assign '${ROLE_RESTRICTED}' role to user_restricted")
check_success "$RESPONSE" "Assign Restricted Role"

# 4.4 Login Users to get Tokens
RESPONSE=$(make_request "POST" "${API_URL}/login"     "{\"username\": \"$WRITER_USER\", \"password\": \"password123\"}"     "" "Login Writer")
check_success "$RESPONSE" "Login Writer"
WRITER_TOKEN=$(extract_token "$RESPONSE")
WRITER_AUTH="Authorization: Bearer $WRITER_TOKEN"

RESPONSE=$(make_request "POST" "${API_URL}/login"     "{\"username\": \"$READER_USER\", \"password\": \"password123\"}"     "" "Login Reader")
check_success "$RESPONSE" "Login Reader"
READER_TOKEN=$(extract_token "$RESPONSE")
READER_AUTH="Authorization: Bearer $READER_TOKEN"

RESPONSE=$(make_request "POST" "${API_URL}/login"     "{\"username\": \"$RESTRICTED_USER\", \"password\": \"password123\"}"     "" "Login Restricted")
check_success "$RESPONSE" "Login Restricted"
RESTRICTED_TOKEN=$(extract_token "$RESPONSE")
RESTRICTED_AUTH="Authorization: Bearer $RESTRICTED_TOKEN"


# 5. Data Access Control
print_header "5. Data Access Control"

# 5.1 Admin Creates Collection
RESPONSE=$(make_request "POST" "${API_URL}/collections"     "{
        \"name\": \"${COLLECTION_NAME}\",
        \"schema\": {
            \"name\": \"${COLLECTION_NAME}\",
            \"strict\": true,
            \"fields\": {
                \"name\": {\"type\": \"string\", \"required\": true},
                \"price\": {\"type\": \"number\", \"required\": true}
            }
        }
    }"     "$ADMIN_AUTH" "Admin creates collection '${COLLECTION_NAME}'")
check_success "$RESPONSE" "Create Collection"

# 5.2 Writer Inserts Document
RESPONSE=$(make_request "POST" "${API_URL}/collections/${COLLECTION_NAME}/documents"     '{
        "data": {
            "name": "Laptop",
            "price": 1200
        }
    }'     "$WRITER_AUTH" "Writer inserts document")
check_success "$RESPONSE" "Writer Insert"
PRODUCT_ID=$(extract_id "$RESPONSE")

# 5.3 Reader Tries to Insert (Should Fail)
RESPONSE=$(make_request "POST" "${API_URL}/collections/${COLLECTION_NAME}/documents"     '{
        "data": {
            "name": "Mouse",
            "price": 20
        }
    }'     "$READER_AUTH" "Reader tries to insert (Should Fail)")
check_error "$RESPONSE" "Reader Insert Fails"

# 5.4 Reader Reads Document
RESPONSE=$(make_request "GET" "${API_URL}/collections/${COLLECTION_NAME}/documents/$PRODUCT_ID"     "" "$READER_AUTH" "Reader reads document")
check_success "$RESPONSE" "Reader Read"

# Check if price is present (Reader has full read access)
if echo "$RESPONSE" | grep -q '"price"'; then
    print_success "Reader sees price"
else
    print_failure "Reader sees price"
fi

# 5.5 Restricted User Reads Document (Field Level Security)
RESPONSE=$(make_request "GET" "${API_URL}/collections/${COLLECTION_NAME}/documents/$PRODUCT_ID"     "" "$RESTRICTED_AUTH" "Restricted user reads document")
check_success "$RESPONSE" "Restricted Read"

# Check if name is present
if echo "$RESPONSE" | grep -q '"name"'; then
    print_success "Restricted sees name"
else
    print_failure "Restricted sees name"
fi

# Check if price is ABSENT
if echo "$RESPONSE" | grep -q '"price"'; then
    print_failure "Restricted should NOT see price"
else
    print_success "Restricted does NOT see price"
fi

# 6. Unauthorized Access
print_header "6. Unauthorized Access"
RESPONSE=$(make_request "GET" "${API_URL}/tenants" "" "" "Access without token")
check_error "$RESPONSE" "Access without token"

# 7. Cleanup
print_header "7. Cleanup"
print_test "Cleaning up..."
# Delete collection - Using curl directly to avoid cluttering logs for cleanup
curl -s -X DELETE "${API_URL}/collections/${COLLECTION_NAME}" -H "$ADMIN_AUTH" > /dev/null
# Delete environment
curl -s -X DELETE "${API_URL}/environments/$ENV_ID" -H "$ADMIN_AUTH" > /dev/null
# Delete region
curl -s -X DELETE "${API_URL}/regions/$REGION_ID" -H "$ADMIN_AUTH" > /dev/null
# Delete tenant
# We don't have tenant ID easily available from Create Tenant response in var, but we can list.
# For now skip deleting tenant.
echo -e "${GREEN}✓ Cleanup complete${NC}"

# Summary
print_header "Test Summary"
echo -e "${GREEN}Tests Passed: ${TESTS_PASSED}${NC}"
echo -e "${RED}Tests Failed: ${TESTS_FAILED}${NC}"
echo "See ${LOG_FILE} for full details."

if [ $TESTS_FAILED -eq 0 ]; then
    echo -e "\n${GREEN}All tests passed! ✓${NC}"
    exit 0
else
    echo -e "\n${RED}Some tests failed. ✗${NC}"
    exit 1
fi
