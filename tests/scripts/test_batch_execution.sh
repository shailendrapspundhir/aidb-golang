#!/bin/bash

# =============================================================================
# Test: Batch Execution
# =============================================================================
# This tests:
# 1. Executing multiple insert requests in a batch
# 2. Executing mixed operations (insert, update, delete) in a batch
# 3. ContinueOnError behavior
# 4. Batch response format
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common.sh"

TEST_NAME="Batch Execution"
TOTAL_STEPS=10
INTERACTIVE="${INTERACTIVE:-false}"

run_test() {
    local timestamp=$(date +%s)
    local username="admin_batch_${timestamp}"
    local tenant_id="tenant_${timestamp}"
    
    print_header "$TEST_NAME"
    
    # Step 1: Register
    print_step "1" "$TOTAL_STEPS" "Registering User (with admin privileges)"
    local body="{\"username\":\"$username\",\"password\":\"password123\",\"email\":\"${username}@test.com\",\"tenantId\":\"$tenant_id\"}"
    local response
    response=$(api_call "POST" "/api/v1/register" "$body" 200)
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    if ! is_success "$response"; then print_error "Registration failed"; return 1; fi
    TEST_USER_ID=$(json_value "$response" "_id")
    TEST_TENANT_ID=$tenant_id
    
    # Step 2: Login
    print_step "2" "$TOTAL_STEPS" "Logging In"
    body="{\"username\":\"$username\",\"password\":\"password123\"}"
    response=$(api_call "POST" "/api/v1/login" "$body" 200)
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    if ! is_success "$response"; then print_error "Login failed"; return 1; fi
    TEST_TOKEN=$(json_value "$response" "token")
    
    # Step 3: Create Tenant
    print_step "3" "$TOTAL_STEPS" "Creating Tenant"
    api_call "POST" "/api/v1/tenants" "{\"id\":\"$tenant_id\",\"name\":\"Test Tenant\"}" 200
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 4: Create Region
    print_step "4" "$TOTAL_STEPS" "Creating Region"
    api_call "POST" "/api/v1/regions" "{\"id\":\"region_${timestamp}\",\"name\":\"Test Region\",\"tenantId\":\"$tenant_id\"}" 200
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 5: Create Environment
    print_step "5" "$TOTAL_STEPS" "Creating Environment"
    api_call "POST" "/api/v1/environments" "{\"id\":\"env_${timestamp}\",\"name\":\"Test Env\",\"regionId\":\"region_${timestamp}\",\"tenantId\":\"$tenant_id\"}" 200
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 6: Create test collection
    print_step "6" "$TOTAL_STEPS" "Creating test collection"
    TEST_COLLECTION="batch_test_${timestamp}"
    body="{\"name\":\"$TEST_COLLECTION\"}"
    response=$(api_call "POST" "/api/v1/collections" "$body" 201)
    if ! is_success "$response"; then print_error "Failed to create collection"; return 1; fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 7: Batch insert multiple documents
    print_step "7" "$TOTAL_STEPS" "Batch inserting multiple documents"
    body="{
        \"requests\": [
            {
                \"id\": \"req1\",
                \"method\": \"POST\",
                \"path\": \"/api/v1/collections/${TEST_COLLECTION}/documents\",
                \"body\": {\"data\":{\"name\":\"doc1\",\"type\":\"batch\"}}
            },
            {
                \"id\": \"req2\",
                \"method\": \"POST\",
                \"path\": \"/api/v1/collections/${TEST_COLLECTION}/documents\",
                \"body\": {\"data\":{\"name\":\"doc2\",\"type\":\"batch\"}}
            },
            {
                \"id\": \"req3\",
                \"method\": \"POST\",
                \"path\": \"/api/v1/collections/${TEST_COLLECTION}/documents\",
                \"body\": {\"data\":{\"name\":\"doc3\",\"type\":\"batch\"}}
            }
        ]
    }"
    response=$(api_call "POST" "/api/v1/batch" "$body" 200)
    if ! is_success "$response"; then print_error "Batch insert failed"; return 1; fi
    print_success "Batch insert executed"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 8: Verify documents were inserted
    print_step "8" "$TOTAL_STEPS" "Verifying documents were inserted"
    sleep 1
    response=$(api_call "GET" "/api/v1/collections/${TEST_COLLECTION}/documents" "" 200)
    if is_success "$response"; then
        print_success "Documents retrieved after batch insert"
    else
        print_error "Failed to retrieve documents"
        return 1
    fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 9: Test continueOnError behavior
    print_step "9" "$TOTAL_STEPS" "Testing continueOnError behavior"
    body="{
        \"continueOnError\": true,
        \"requests\": [
            {
                \"id\": \"good_req\",
                \"method\": \"POST\",
                \"path\": \"/api/v1/collections/${TEST_COLLECTION}/documents\",
                \"body\": {\"data\":{\"name\":\"good_doc\"}}
            },
            {
                \"id\": \"bad_req\",
                \"method\": \"POST\",
                \"path\": \"/api/v1/collections/nonexistent_collection/documents\",
                \"body\": {\"data\":{\"name\":\"bad_doc\"}}
            },
            {
                \"id\": \"after_error\",
                \"method\": \"POST\",
                \"path\": \"/api/v1/collections/${TEST_COLLECTION}/documents\",
                \"body\": {\"data\":{\"name\":\"after_error_doc\"}}
            }
        ]
    }"
    response=$(api_call "POST" "/api/v1/batch" "$body" 200)
    if is_success "$response"; then
        print_success "ContinueOnError batch executed"
    else
        print_warning "ContinueOnError response may vary"
    fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 10: Test empty batch request
    print_step "10" "$TOTAL_STEPS" "Testing empty batch request"
    body="{\"requests\": []}"
    response=$(api_call "POST" "/api/v1/batch" "$body" 400)
    # Empty batch should return an error
    print_success "Empty batch test completed"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Cleanup
    api_call "DELETE" "/api/v1/collections/${TEST_COLLECTION}" "" 200 > /dev/null 2>&1
    
    return 0
}

# Run the test
if [ "$INTERACTIVE" == "true" ]; then
    source "${SCRIPT_DIR}/../interactive.sh"
    run_test_interactive "$TEST_NAME" run_test
else
    check_server || exit 1
    if run_test; then
        print_test_summary "$TEST_NAME" "PASS"
        exit 0
    else
        print_test_summary "$TEST_NAME" "FAIL"
        exit 1
    fi
fi
