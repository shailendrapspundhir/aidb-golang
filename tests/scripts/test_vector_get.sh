#!/bin/bash

# =============================================================================
# Test: Vector Get
# =============================================================================
# This test verifies vector document retrieval functionality
# Steps:
#   1. Register User
#   2. Login
#   3. Create Tenant
#   4. Create Region
#   5. Create Environment
#   6. Create Vector Collection
#   7. Insert Vector Document
#   8. Get Vector Document
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common.sh"

TEST_NAME="Vector Get"
TOTAL_STEPS=8
INTERACTIVE="${INTERACTIVE:-false}"

run_test() {
    local timestamp=$(date +%s)
    local username="admin_vec_get_${timestamp}"
    local tenant_id="tenant_${timestamp}"
    local doc_id="doc_${timestamp}"
    
    # Step 1: Register
    print_step "1" "$TOTAL_STEPS" "Registering User"
    local body="{\"username\":\"$username\",\"password\":\"password123\",\"email\":\"${username}@test.com\",\"tenantId\":\"$tenant_id\"}"
    local response
    response=$(api_call "POST" "/api/v1/register" "$body" 200)
    
    if [ "$INTERACTIVE" == "true" ]; then
        if ! wait_for_input "Press Y to continue... "; then return 1; fi
    fi
    
    if ! is_success "$response"; then return 1; fi
    TEST_USER_ID=$(json_value "$response" "_id")
    TEST_TENANT_ID=$tenant_id
    
    # Step 2: Login
    print_step "2" "$TOTAL_STEPS" "Logging In"
    body="{\"username\":\"$username\",\"password\":\"password123\"}"
    response=$(api_call "POST" "/api/v1/login" "$body" 200)
    
    if [ "$INTERACTIVE" == "true" ]; then
        if ! wait_for_input "Press Y to continue... "; then return 1; fi
    fi
    
    if ! is_success "$response"; then return 1; fi
    TEST_TOKEN=$(json_value "$response" "token")
    
    # Step 3: Create Tenant
    print_step "3" "$TOTAL_STEPS" "Creating Tenant"
    body="{\"id\":\"$tenant_id\",\"name\":\"Test Tenant\"}"
    api_call "POST" "/api/v1/tenants" "$body" 200
    
    if [ "$INTERACTIVE" == "true" ]; then
        if ! wait_for_input "Press Y to continue... "; then return 1; fi
    fi
    
    # Step 4: Create Region
    print_step "4" "$TOTAL_STEPS" "Creating Region"
    body="{\"id\":\"region_${timestamp}\",\"name\":\"Test Region\",\"tenantId\":\"$tenant_id\"}"
    api_call "POST" "/api/v1/regions" "$body" 200
    
    if [ "$INTERACTIVE" == "true" ]; then
        if ! wait_for_input "Press Y to continue... "; then return 1; fi
    fi
    
    # Step 5: Create Environment
    print_step "5" "$TOTAL_STEPS" "Creating Environment"
    body="{\"id\":\"env_${timestamp}\",\"name\":\"Test Env\",\"regionId\":\"region_${timestamp}\",\"tenantId\":\"$tenant_id\"}"
    api_call "POST" "/api/v1/environments" "$body" 200
    
    if [ "$INTERACTIVE" == "true" ]; then
        if ! wait_for_input "Press Y to continue... "; then return 1; fi
    fi
    
    # Step 6: Create Vector Collection
    print_step "6" "$TOTAL_STEPS" "Creating Vector Collection"
    TEST_VECTOR_COLLECTION="vectors_${timestamp}"
    body="{\"name\":\"$TEST_VECTOR_COLLECTION\",\"dimensions\":4,\"distanceMetric\":\"cosine\"}"
    response=$(api_call "POST" "/api/v1/vectors" "$body" 201)
    
    if ! is_success "$response"; then return 1; fi
    
    if [ "$INTERACTIVE" == "true" ]; then
        if ! wait_for_input "Press Y to continue... "; then return 1; fi
    fi
    
    # Step 7: Insert Vector Document
    print_step "7" "$TOTAL_STEPS" "Inserting Vector Document"
    body="{\"_id\":\"$doc_id\",\"vector\":[1.0,2.0,3.0,4.0],\"metadata\":{\"label\":\"get-test\",\"value\":42}}"
    response=$(api_call "POST" "/api/v1/vectors/${TEST_VECTOR_COLLECTION}/documents" "$body" 201)
    
    if ! is_success "$response"; then return 1; fi
    
    if [ "$INTERACTIVE" == "true" ]; then
        if ! wait_for_input "Press Y to continue to get operation... "; then return 1; fi
    fi
    
    # Step 8: Get Vector Document (without vector)
    print_step "8" "$TOTAL_STEPS" "Getting Vector Document (without vector data)"
    response=$(api_call "GET" "/api/v1/vectors/${TEST_VECTOR_COLLECTION}/documents/${doc_id}" "" 200)
    
    if ! is_success "$response"; then
        print_error "Failed to get vector document"
        return 1
    fi
    
    # Verify document ID matches
    local retrieved_id=$(json_value "$response" "_id")
    if [ "$retrieved_id" == "$doc_id" ]; then
        print_success "Retrieved document with correct ID: $doc_id"
    else
        print_error "Retrieved ID mismatch"
        return 1
    fi
    
    if [ "$INTERACTIVE" == "true" ]; then
        if ! wait_for_input "Press Y to get document with vector data... "; then return 1; fi
    fi
    
    # Get Vector Document (with vector)
    print_step "8" "$TOTAL_STEPS" "Getting Vector Document (with vector data)"
    response=$(api_call "GET" "/api/v1/vectors/${TEST_VECTOR_COLLECTION}/documents/${doc_id}?includeVector=true" "" 200)
    
    if ! is_success "$response"; then
        print_error "Failed to get vector document with vector"
        return 1
    fi
    
    # Verify vector is present
    if echo "$response" | grep -q '"vector"'; then
        print_success "Vector data included in response"
    else
        print_error "Vector data not found in response"
        return 1
    fi
    
    return 0
}

main() {
    if ! check_server; then exit 1; fi
    print_header "$TEST_NAME"
    
    if run_test; then
        print_test_summary "$TEST_NAME" "PASS"
        exit 0
    else
        print_test_summary "$TEST_NAME" "FAIL"
        exit 1
    fi
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
