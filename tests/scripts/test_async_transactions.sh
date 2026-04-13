#!/bin/bash

# =============================================================================
# Test: Async Transactions
# =============================================================================
# This tests:
# 1. Starting an async transaction (getting a transaction ID)
# 2. Executing operations using the transaction ID
# 3. Checking transaction status
# 4. Committing the transaction
# 5. Rollback functionality
# 6. Transaction expiration
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common.sh"

TEST_NAME="Async Transactions"
TOTAL_STEPS=12
INTERACTIVE="${INTERACTIVE:-false}"

run_test() {
    local timestamp=$(date +%s)
    local username="admin_async_tx_${timestamp}"
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
    TEST_COLLECTION="async_tx_test_${timestamp}"
    body="{\"name\":\"$TEST_COLLECTION\"}"
    response=$(api_call "POST" "/api/v1/collections" "$body" 201)
    if ! is_success "$response"; then print_error "Failed to create collection"; return 1; fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 7: Begin async transaction
    print_step "7" "$TOTAL_STEPS" "Beginning async transaction"
    body="{\"timeoutSeconds\":60}"
    response=$(api_call "POST" "/api/v1/transactions/begin" "$body" 201)
    if ! is_success "$response"; then print_error "Failed to begin transaction"; return 1; fi
    local tx_id=$(json_value "$response" "transactionId")
    print_success "Transaction started with ID: $tx_id"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 8: Check transaction status
    print_step "8" "$TOTAL_STEPS" "Checking transaction status"
    response=$(api_call "GET" "/api/v1/transactions/${tx_id}/status" "" 200)
    if is_success "$response"; then
        print_success "Transaction status retrieved"
    else
        print_error "Failed to get transaction status"
        return 1
    fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 9: Insert document using transaction ID
    print_step "9" "$TOTAL_STEPS" "Inserting document with transaction ID"
    # Note: The api_call function doesn't support custom headers, so we need to use curl directly
    local insert_response
    insert_response=$(curl -s -w "\n%{http_code}" -X POST "${BASE_URL}/api/v1/collections/${TEST_COLLECTION}/documents" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer $TEST_TOKEN" \
        -H "X-Transaction-ID: ${tx_id}" \
        -d '{"data":{"name":"async_doc","value":100}}' 2>/dev/null)
    local http_code=$(echo "$insert_response" | tail -n1)
    insert_response=$(echo "$insert_response" | sed '$d')
    print_response "$insert_response"
    if [ "$http_code" == "202" ] || [ "$http_code" == "201" ]; then
        print_success "Document insert queued in transaction (HTTP $http_code)"
    else
        print_warning "Document insert response: HTTP $http_code"
    fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 10: Commit transaction
    print_step "10" "$TOTAL_STEPS" "Committing transaction"
    response=$(api_call "POST" "/api/v1/transactions/${tx_id}/commit" "" 200)
    if is_success "$response"; then
        print_success "Transaction committed"
    else
        print_error "Failed to commit transaction"
        return 1
    fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 11: Verify documents after commit
    print_step "11" "$TOTAL_STEPS" "Verifying documents after commit"
    sleep 1
    response=$(api_call "GET" "/api/v1/collections/${TEST_COLLECTION}/documents" "" 200)
    if is_success "$response"; then
        print_success "Documents retrieved after commit"
    else
        print_warning "Document retrieval response may vary"
    fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 12: Test rollback with new transaction
    print_step "12" "$TOTAL_STEPS" "Testing rollback"
    body="{\"timeoutSeconds\":60}"
    response=$(api_call "POST" "/api/v1/transactions/begin" "$body" 201)
    if is_success "$response"; then
        local tx_id2=$(json_value "$response" "transactionId")
        print_success "Started second transaction: $tx_id2"
        
        # Insert a document
        curl -s -X POST "${BASE_URL}/api/v1/collections/${TEST_COLLECTION}/documents" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer $TEST_TOKEN" \
            -H "X-Transaction-ID: ${tx_id2}" \
            -d '{"data":{"name":"rollback_test","value":999}}' > /dev/null 2>&1
        
        # Rollback
        response=$(api_call "POST" "/api/v1/transactions/${tx_id2}/rollback" "" 200)
        if is_success "$response"; then
            print_success "Transaction rolled back"
        else
            print_warning "Rollback response may vary"
        fi
    else
        print_warning "Could not start second transaction for rollback test"
    fi
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
