#!/bin/bash

# =============================================================================
# AIDB Test Common Library
# =============================================================================
# This file contains reusable functions for AIDB testing
# Source this file in other test scripts: source tests/common.sh
# =============================================================================

# Configuration
BASE_URL="${BASE_URL:-http://localhost:11111}"
DATA_DIR="${DATA_DIR:-/tmp/aidb_test_data}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# Global variables to store state
TEST_TOKEN=""
TEST_USER_ID=""
TEST_TENANT_ID=""
TEST_COLLECTION=""
TEST_VECTOR_COLLECTION=""

# =============================================================================
# Output Functions
# =============================================================================

print_header() {
    echo ""
    echo -e "${BOLD}${CYAN}═══════════════════════════════════════════════════════════════════${NC}"
    echo -e "${BOLD}${WHITE}  $1${NC}"
    echo -e "${BOLD}${CYAN}═══════════════════════════════════════════════════════════════════${NC}"
    echo ""
}

print_step() {
    local step_num=$1
    local total_steps=$2
    local description=$3
    echo ""
    echo -e "${BOLD}${BLUE}┌─────────────────────────────────────────────────────────────────┐${NC}"
    echo -e "${BOLD}${BLUE}│${NC} ${YELLOW}Step $step_num/$total_steps:${NC} ${WHITE}$description${NC}"
    echo -e "${BOLD}${BLUE}└─────────────────────────────────────────────────────────────────┘${NC}"
    echo ""
}

print_request() {
    local method=$1
    local endpoint=$2
    local body=$3
    
    echo -e "${MAGENTA}▶ REQUEST:${NC}"
    echo -e "${CYAN}Method:${NC} $method"
    echo -e "${CYAN}Endpoint:${NC} $endpoint"
    if [ -n "$body" ]; then
        echo -e "${CYAN}Body:${NC}"
        echo "$body" | python3 -m json.tool 2>/dev/null || echo "$body"
    fi
    echo ""
}

print_response() {
    local response=$1
    
    echo -e "${GREEN}◀ RESPONSE:${NC}"
    echo "$response" | python3 -m json.tool 2>/dev/null || echo "$response"
    echo ""
}

print_success() {
    echo -e "${GREEN}✓ SUCCESS:${NC} $1"
}

print_error() {
    echo -e "${RED}✗ ERROR:${NC} $1"
}

print_info() {
    echo -e "${CYAN}ℹ INFO:${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠ WARNING:${NC} $1"
}

# =============================================================================
# Interactive Functions
# =============================================================================

wait_for_input() {
    local message="${1:-Press Y to continue to next step...}"
    local response
    
    while true; do
        echo -e -n "${YELLOW}$message${NC}"
        read -r response
        case "$response" in
            [Yy]*)
                return 0
                ;;
            [Nn]*)
                return 1
                ;;
            [Qq]*)
                echo -e "${RED}Test aborted by user.${NC}"
                exit 0
                ;;
            *)
                echo -e "${YELLOW}Please press Y to continue, N to skip, or Q to quit.${NC}"
                ;;
        esac
    done
}

pause_for_review() {
    echo ""
    echo -e "${BOLD}${YELLOW}═══════════════════════════════════════════════════════════════════${NC}"
    echo -e "${BOLD}${YELLOW}  Press ENTER to continue...${NC}"
    echo -e "${BOLD}${YELLOW}═══════════════════════════════════════════════════════════════════${NC}"
    read -r
}

# =============================================================================
# API Functions
# =============================================================================

# Make an API call and print request/response
api_call() {
    local method=$1
    local endpoint=$2
    local body=$3
    local expected_status=${4:-200}
    
    # Print request to stderr so it shows even when response is captured
    { print_request "$method" "$endpoint" "$body"; } >&2
    
    local response
    local http_code
    
    if [ -n "$body" ]; then
        response=$(curl -s -w "\n%{http_code}" -X "$method" "${BASE_URL}${endpoint}" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer $TEST_TOKEN" \
            -d "$body" 2>/dev/null)
    else
        response=$(curl -s -w "\n%{http_code}" -X "$method" "${BASE_URL}${endpoint}" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer $TEST_TOKEN" 2>/dev/null)
    fi
    
    http_code=$(echo "$response" | tail -n1)
    response=$(echo "$response" | sed '$d')
    
    # Print response to stderr so it shows even when captured
    { print_response "$response"; } >&2
    
    if [ "$http_code" == "$expected_status" ]; then
        print_success "HTTP $http_code (Expected: $expected_status)"
        # Echo response to stdout for caller to capture
        echo "$response"
        return 0
    else
        print_error "HTTP $http_code (Expected: $expected_status)"
        # Echo response to stdout for caller to capture
        echo "$response"
        return 1
    fi
}

# Make API call without printing (for internal use)
api_call_silent() {
    local method=$1
    local endpoint=$2
    local body=$3
    
    if [ -n "$body" ]; then
        curl -s -X "$method" "${BASE_URL}${endpoint}" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer $TEST_TOKEN" \
            -d "$body" 2>/dev/null
    else
        curl -s -X "$method" "${BASE_URL}${endpoint}" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer $TEST_TOKEN" 2>/dev/null
    fi
}

# Extract value from JSON response
json_value() {
    local json=$1
    local key=$2
    echo "$json" | grep -o "\"$key\"[[:space:]]*:[[:space:]]*\"[^\"]*\"" | sed "s/\"$key\"[[:space:]]*:[[:space:]]*\"//" | sed 's/"$//' | head -1
}

# Extract nested value from JSON response
json_nested_value() {
    local json=$1
    local path=$2
    echo "$json" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data$path)" 2>/dev/null
}

# Check if response contains success
is_success() {
    local json=$1
    local success=$(echo "$json" | grep -o '"success"[[:space:]]*:[[:space:]]*[a-z]*' | sed 's/"success"[[:space:]]*:[[:space:]]*//')
    [ "$success" == "true" ]
}

# =============================================================================
# Setup Functions
# =============================================================================

# Register a new user
register_user() {
    local username=${1:-"test_user_$(date +%s)"}
    local password=${2:-"password123"}
    local email=${3:-"${username}@test.com"}
    local tenant_id=${4:-"tenant_$(date +%s)"}
    
    print_step "1" "7" "Registering User"
    
    local body="{\"username\":\"$username\",\"password\":\"$password\",\"email\":\"$email\",\"tenantId\":\"$tenant_id\"}"
    local response
    response=$(api_call "POST" "/api/v1/register" "$body" 200)
    
    if is_success "$response"; then
        TEST_USER_ID=$(json_value "$response" "_id")
        TEST_TENANT_ID=$tenant_id
        print_info "User ID: $TEST_USER_ID"
        print_info "Tenant ID: $TEST_TENANT_ID"
        return 0
    else
        print_error "Failed to register user"
        return 1
    fi
}

# Login and get token
login_user() {
    local username=$1
    local password=${2:-"password123"}
    
    print_step "2" "7" "Logging In"
    
    local body="{\"username\":\"$username\",\"password\":\"$password\"}"
    local response
    response=$(api_call "POST" "/api/v1/login" "$body" 200)
    
    if is_success "$response"; then
        TEST_TOKEN=$(json_value "$response" "token")
        TEST_USER_ID=$(json_value "$response" "_id")
        print_info "Token obtained: ${TEST_TOKEN:0:50}..."
        return 0
    else
        print_error "Failed to login"
        return 1
    fi
}

# Create a tenant
create_tenant() {
    local tenant_id=${1:-$TEST_TENANT_ID}
    local name=${2:-"Test Tenant"}
    
    print_step "3" "7" "Creating Tenant"
    
    local body="{\"id\":\"$tenant_id\",\"name\":\"$name\"}"
    local response
    response=$(api_call "POST" "/api/v1/tenants" "$body" 200)
    
    if is_success "$response"; then
        print_info "Tenant created: $tenant_id"
        return 0
    else
        print_warning "Tenant may already exist or creation failed"
        return 0  # Continue anyway
    fi
}

# Create a region
create_region() {
    local region_id=${1:-"region_$(date +%s)"}
    local name=${2:-"Test Region"}
    
    print_step "4" "7" "Creating Region"
    
    local body="{\"id\":\"$region_id\",\"name\":\"$name\",\"tenantId\":\"$TEST_TENANT_ID\"}"
    local response
    response=$(api_call "POST" "/api/v1/regions" "$body" 200)
    
    if is_success "$response"; then
        print_info "Region created: $region_id"
        return 0
    else
        print_warning "Region creation may have failed"
        return 0
    fi
}

# Create an environment
create_environment() {
    local env_id=${1:-"env_$(date +%s)"}
    local name=${2:-"Test Environment"}
    local region_id=${3:-"region_$(date +%s)"}
    
    print_step "5" "7" "Creating Environment"
    
    local body="{\"id\":\"$env_id\",\"name\":\"$name\",\"regionId\":\"$region_id\",\"tenantId\":\"$TEST_TENANT_ID\"}"
    local response
    response=$(api_call "POST" "/api/v1/environments" "$body" 200)
    
    if is_success "$response"; then
        print_info "Environment created: $env_id"
        return 0
    else
        print_warning "Environment creation may have failed"
        return 0
    fi
}

# Create a document collection
create_collection() {
    local name=${1:-"test_collection_$(date +%s)"}
    
    print_step "6" "7" "Creating Document Collection"
    
    local body="{\"name\":\"$name\"}"
    local response
    response=$(api_call "POST" "/api/v1/collections" "$body" 201)
    
    if is_success "$response"; then
        TEST_COLLECTION=$name
        print_info "Collection created: $name"
        return 0
    else
        print_error "Failed to create collection"
        return 1
    fi
}

# Create a vector collection
create_vector_collection() {
    local name=${1:-"test_vectors_$(date +%s)"}
    local dimensions=${2:-4}
    local metric=${3:-"cosine"}
    
    print_step "6" "7" "Creating Vector Collection"
    
    local body="{\"name\":\"$name\",\"dimensions\":$dimensions,\"distanceMetric\":\"$metric\"}"
    local response
    response=$(api_call "POST" "/api/v1/vectors" "$body" 201)
    
    if is_success "$response"; then
        TEST_VECTOR_COLLECTION=$name
        print_info "Vector collection created: $name (dimensions: $dimensions, metric: $metric)"
        return 0
    else
        print_error "Failed to create vector collection"
        return 1
    fi
}

# Full setup for document tests
setup_document_test() {
    local test_name=$1
    local total_steps=${2:-7}
    
    print_header "$test_name"
    
    local timestamp=$(date +%s)
    # Use admin_ prefix to get super_admin role
    local username="admin_doc_${timestamp}"
    local tenant_id="tenant_${timestamp}"
    
    # Step 1: Register
    print_step "1" "$total_steps" "Registering User (with admin privileges)"
    local body="{\"username\":\"$username\",\"password\":\"password123\",\"email\":\"${username}@test.com\",\"tenantId\":\"$tenant_id\"}"
    local response
    response=$(api_call "POST" "/api/v1/register" "$body" 200)
    
    if ! is_success "$response"; then
        print_error "Registration failed"
        return 1
    fi
    TEST_USER_ID=$(json_value "$response" "_id")
    TEST_TENANT_ID=$tenant_id
    
    # Step 2: Login
    print_step "2" "$total_steps" "Logging In"
    body="{\"username\":\"$username\",\"password\":\"password123\"}"
    response=$(api_call "POST" "/api/v1/login" "$body" 200)
    
    if ! is_success "$response"; then
        print_error "Login failed"
        return 1
    fi
    TEST_TOKEN=$(json_value "$response" "token")
    
    # Step 3: Create Tenant
    print_step "3" "$total_steps" "Creating Tenant"
    body="{\"id\":\"$tenant_id\",\"name\":\"Test Tenant\"}"
    api_call "POST" "/api/v1/tenants" "$body" 200
    
    # Step 4: Create Region
    print_step "4" "$total_steps" "Creating Region"
    body="{\"id\":\"region_${timestamp}\",\"name\":\"Test Region\",\"tenantId\":\"$tenant_id\"}"
    api_call "POST" "/api/v1/regions" "$body" 200
    
    # Step 5: Create Environment
    print_step "5" "$total_steps" "Creating Environment"
    body="{\"id\":\"env_${timestamp}\",\"name\":\"Test Env\",\"regionId\":\"region_${timestamp}\",\"tenantId\":\"$tenant_id\"}"
    api_call "POST" "/api/v1/environments" "$body" 200
    
    # Step 6: Create Collection
    print_step "6" "$total_steps" "Creating Document Collection"
    TEST_COLLECTION="docs_${timestamp}"
    body="{\"name\":\"$TEST_COLLECTION\"}"
    response=$(api_call "POST" "/api/v1/collections" "$body" 201)
    
    if ! is_success "$response"; then
        print_error "Collection creation failed"
        return 1
    fi
    
    print_success "Setup completed successfully!"
    return 0
}

# Full setup for vector tests
setup_vector_test() {
    local test_name=$1
    local total_steps=${2:-7}
    local dimensions=${3:-4}
    
    print_header "$test_name"
    
    local timestamp=$(date +%s)
    # Use admin_ prefix to get super_admin role
    local username="admin_vec_${timestamp}"
    local tenant_id="tenant_${timestamp}"
    
    # Step 1: Register
    print_step "1" "$total_steps" "Registering User (with admin privileges)"
    local body="{\"username\":\"$username\",\"password\":\"password123\",\"email\":\"${username}@test.com\",\"tenantId\":\"$tenant_id\"}"
    local response
    response=$(api_call "POST" "/api/v1/register" "$body" 200)
    
    if ! is_success "$response"; then
        print_error "Registration failed"
        return 1
    fi
    TEST_USER_ID=$(json_value "$response" "_id")
    TEST_TENANT_ID=$tenant_id
    
    # Step 2: Login
    print_step "2" "$total_steps" "Logging In"
    body="{\"username\":\"$username\",\"password\":\"password123\"}"
    response=$(api_call "POST" "/api/v1/login" "$body" 200)
    
    if ! is_success "$response"; then
        print_error "Login failed"
        return 1
    fi
    TEST_TOKEN=$(json_value "$response" "token")
    
    # Step 3: Create Tenant
    print_step "3" "$total_steps" "Creating Tenant"
    body="{\"id\":\"$tenant_id\",\"name\":\"Test Tenant\"}"
    api_call "POST" "/api/v1/tenants" "$body" 200
    
    # Step 4: Create Region
    print_step "4" "$total_steps" "Creating Region"
    body="{\"id\":\"region_${timestamp}\",\"name\":\"Test Region\",\"tenantId\":\"$tenant_id\"}"
    api_call "POST" "/api/v1/regions" "$body" 200
    
    # Step 5: Create Environment
    print_step "5" "$total_steps" "Creating Environment"
    body="{\"id\":\"env_${timestamp}\",\"name\":\"Test Env\",\"regionId\":\"region_${timestamp}\",\"tenantId\":\"$tenant_id\"}"
    api_call "POST" "/api/v1/environments" "$body" 200
    
    # Step 6: Create Vector Collection
    print_step "6" "$total_steps" "Creating Vector Collection"
    TEST_VECTOR_COLLECTION="vectors_${timestamp}"
    body="{\"name\":\"$TEST_VECTOR_COLLECTION\",\"dimensions\":$dimensions,\"distanceMetric\":\"cosine\"}"
    response=$(api_call "POST" "/api/v1/vectors" "$body" 201)
    
    if ! is_success "$response"; then
        print_error "Vector collection creation failed"
        return 1
    fi
    
    print_success "Setup completed successfully!"
    return 0
}

# =============================================================================
# Cleanup Functions
# =============================================================================

cleanup() {
    print_info "Cleaning up test data..."
    # Add cleanup logic if needed
}

# =============================================================================
# Assertion Functions
# =============================================================================

assert_equals() {
    local expected=$1
    local actual=$2
    local message=${3:-"Values should be equal"}
    
    if [ "$expected" == "$actual" ]; then
        print_success "$message"
        return 0
    else
        print_error "$message (Expected: $expected, Got: $actual)"
        return 1
    fi
}

assert_contains() {
    local haystack=$1
    local needle=$2
    local message=${3:-"String should contain substring"}
    
    if [[ "$haystack" == *"$needle"* ]]; then
        print_success "$message"
        return 0
    else
        print_error "$message (Looking for: $needle)"
        return 1
    fi
}

assert_success() {
    local response=$1
    local message=${2:-"Response should indicate success"}
    
    if is_success "$response"; then
        print_success "$message"
        return 0
    else
        print_error "$message"
        return 1
    fi
}

# =============================================================================
# Utility Functions
# =============================================================================

generate_random_string() {
    local length=${1:-8}
    cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w "$length" | head -n 1
}

generate_unique_id() {
    # Generate a unique ID using timestamp + random string
    echo "$(date +%s)_$(generate_random_string 6)"
}

generate_random_vector() {
    local dimensions=${1:-4}
    local vector="["
    for i in $(seq 1 $dimensions); do
        if [ $i -gt 1 ]; then
            vector+=","
        fi
        vector+="$(awk -v seed=$RANDOM 'BEGIN{srand(seed); printf "%.2f", rand()}')"
    done
    vector+="]"
    echo "$vector"
}

get_timestamp() {
    date +%s
}

# Check if server is running
check_server() {
    print_info "Checking if server is running at $BASE_URL..."
    local response
    response=$(curl -s "${BASE_URL}/" 2>/dev/null)
    
    if [ -n "$response" ] && [[ "$response" == *"AIDB"* ]]; then
        print_success "Server is running"
        return 0
    else
        print_error "Server is not running at $BASE_URL"
        print_info "Start the server with: ./aidb"
        return 1
    fi
}

# Print test summary
print_test_summary() {
    local test_name=$1
    local result=$2
    
    echo ""
    echo -e "${BOLD}${CYAN}═══════════════════════════════════════════════════════════════════${NC}"
    if [ "$result" == "PASS" ]; then
        echo -e "${BOLD}${GREEN}  ✓ TEST PASSED: $test_name${NC}"
    else
        echo -e "${BOLD}${RED}  ✗ TEST FAILED: $test_name${NC}"
    fi
    echo -e "${BOLD}${CYAN}═══════════════════════════════════════════════════════════════════${NC}"
    echo ""
}

# Export functions for use in other scripts
export -f print_header print_step print_request print_response
export -f print_success print_error print_info print_warning
export -f wait_for_input pause_for_review
export -f api_call api_call_silent json_value json_nested_value is_success
export -f register_user login_user create_tenant create_region create_environment
export -f create_collection create_vector_collection
export -f setup_document_test setup_vector_test
export -f cleanup assert_equals assert_contains assert_success
export -f generate_random_string generate_random_vector get_timestamp check_server
export -f print_test_summary

export BASE_URL DATA_DIR
