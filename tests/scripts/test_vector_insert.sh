#!/bin/bash

# =============================================================================
# Test: Vector Insert
# =============================================================================
# This test verifies vector document insertion functionality
# Steps:
#   1. Register User
#   2. Login
#   3. Create Tenant
#   4. Create Region
#   5. Create Environment
#   6. Create Vector Collection
#   7. Insert Vector Document
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common.sh"

TEST_NAME="Vector Insert"
TOTAL_STEPS=7
INTERACTIVE="${INTERACTIVE:-false}"

run_test() {
    local timestamp=$(date +%s)
    local username="admin_vec_insert_${timestamp}"
    local tenant_id="tenant_${timestamp}"
    local doc_id="doc_${timestamp}"
    
    # Step 1: Register
    print_step "1" "$TOTAL_STEPS" "Registering User"
    local body="{\"username\":\"$username\",\"password\":\"password123\",\"email\":\"${username}@test.com\",\"tenantId\":\"$tenant_id\"}"
    local response
    response=$(api_call "POST" "/api/v1/register" "$body" 200)
    
    if [ "$INTERACTIVE" == "true" ]; then
        if ! wait_for_input "Press Y to continue to login step... "; then
            return 1
        fi
    fi
    
    if ! is_success "$response"; then
        print_error "Registration failed"
        return 1
    fi
    TEST_USER_ID=$(json_value "$response" "_id")
    TEST_TENANT_ID=$tenant_id
    
    # Step 2: Login
    print_step "2" "$TOTAL_STEPS" "Logging In"
    body="{\"username\":\"$username\",\"password\":\"password123\"}"
    response=$(api_call "POST" "/api/v1/login" "$body" 200)
    
    if [ "$INTERACTIVE" == "true" ]; then
        if ! wait_for_input "Press Y to continue to tenant creation... "; then
            return 1
        fi
    fi
    
    if ! is_success "$response"; then
        print_error "Login failed"
        return 1
    fi
    TEST_TOKEN=$(json_value "$response" "token")
    
    # Step 3: Create Tenant
    print_step "3" "$TOTAL_STEPS" "Creating Tenant"
    body="{\"id\":\"$tenant_id\",\"name\":\"Test Tenant\"}"
    api_call "POST" "/api/v1/tenants" "$body" 200
    
    if [ "$INTERACTIVE" == "true" ]; then
        if ! wait_for_input "Press Y to continue to region creation... "; then
            return 1
        fi
    fi
    
    # Step 4: Create Region
    print_step "4" "$TOTAL_STEPS" "Creating Region"
    body="{\"id\":\"region_${timestamp}\",\"name\":\"Test Region\",\"tenantId\":\"$tenant_id\"}"
    api_call "POST" "/api/v1/regions" "$body" 200
    
    if [ "$INTERACTIVE" == "true" ]; then
        if ! wait_for_input "Press Y to continue to environment creation... "; then
            return 1
        fi
    fi
    
    # Step 5: Create Environment
    print_step "5" "$TOTAL_STEPS" "Creating Environment"
    body="{\"id\":\"env_${timestamp}\",\"name\":\"Test Env\",\"regionId\":\"region_${timestamp}\",\"tenantId\":\"$tenant_id\"}"
    api_call "POST" "/api/v1/environments" "$body" 200
    
    if [ "$INTERACTIVE" == "true" ]; then
        if ! wait_for_input "Press Y to continue to vector collection creation... "; then
            return 1
        fi
    fi
    
    # Step 6: Create Vector Collection
    print_step "6" "$TOTAL_STEPS" "Creating Vector Collection"
    TEST_VECTOR_COLLECTION="vectors_${timestamp}"
    body="{\"name\":\"$TEST_VECTOR_COLLECTION\",\"dimensions\":4,\"distanceMetric\":\"cosine\"}"
    response=$(api_call "POST" "/api/v1/vectors" "$body" 201)
    
    if ! is_success "$response"; then
        print_error "Vector collection creation failed"
        return 1
    fi
    
    if [ "$INTERACTIVE" == "true" ]; then
        if ! wait_for_input "Press Y to continue to vector insert... "; then
            return 1
        fi
    fi
    
    # Step 7: Insert Vector Document
    print_step "7" "$TOTAL_STEPS" "Inserting Vector Document"
    body="{\"_id\":\"$doc_id\",\"vector\":[1.0,0.0,0.0,0.0],\"metadata\":{\"label\":\"test-vector\",\"category\":\"test\"}}"
    response=$(api_call "POST" "/api/v1/vectors/${TEST_VECTOR_COLLECTION}/documents" "$body" 201)
    
    if ! is_success "$response"; then
        print_error "Vector document insert failed"
        return 1
    fi
    
    # Verify the inserted document
    local inserted_id=$(json_value "$response" "_id")
    if [ "$inserted_id" == "$doc_id" ]; then
        print_success "Vector document inserted with correct ID: $doc_id"
    else
        print_error "Inserted ID mismatch"
        return 1
    fi
    
    return 0
}

# Main execution
main() {
    if ! check_server; then
        exit 1
    fi
    
    print_header "$TEST_NAME"
    
    if run_test; then
        print_test_summary "$TEST_NAME" "PASS"
        exit 0
    else
        print_test_summary "$TEST_NAME" "FAIL"
        exit 1
    fi
}

# Run main if script is executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
