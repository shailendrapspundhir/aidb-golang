#!/bin/bash

# =============================================================================
# Test: Basic Transaction Support
# =============================================================================
# This test verifies that transactions are working correctly by testing
# document operations that use auto-transaction mode
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common.sh"

TEST_NAME="Basic Transaction Support"
TOTAL_STEPS=10
INTERACTIVE="${INTERACTIVE:-false}"

run_test() {
    local timestamp=$(date +%s)
    local username="admin_tx_${timestamp}"
    local tenant_id="tenant_${timestamp}"
    
    print_step "1" "$TOTAL_STEPS" "Registering User"
    local body="{\"username\":\"$username\",\"password\":\"password123\",\"email\":\"${username}@test.com\",\"tenantId\":\"$tenant_id\"}"
    local response
    response=$(api_call "POST" "/api/v1/register" "$body" 200)
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    if ! is_success "$response"; then return 1; fi
    TEST_USER_ID=$(json_value "$response" "_id")
    TEST_TENANT_ID=$tenant_id
    
    print_step "2" "$TOTAL_STEPS" "Logging In"
    body="{\"username\":\"$username\",\"password\":\"password123\"}"
    response=$(api_call "POST" "/api/v1/login" "$body" 200)
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    if ! is_success "$response"; then return 1; fi
    TEST_TOKEN=$(json_value "$response" "token")
    
    print_step "3" "$TOTAL_STEPS" "Creating Collection for Transaction Tests"
    TEST_COLLECTION="tx_test_${timestamp}"
    response=$(api_call "POST" "/api/v1/collections" "{\"name\":\"$TEST_COLLECTION\"}" 201)
    if ! is_success "$response"; then return 1; fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "4" "$TOTAL_STEPS" "Inserting Document (Transactional)"
    body="{\"data\":{\"name\":\"Transaction Test User\",\"email\":\"tx@example.com\",\"balance\":1000}}"
    response=$(api_call "POST" "/api/v1/collections/${TEST_COLLECTION}/documents" "$body" 201)
    if ! is_success "$response"; then
        print_error "Transactional insert failed"
        return 1
    fi
    local doc_id=$(json_value "$response" "_id")
    print_success "Document inserted with ID: $doc_id"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "5" "$TOTAL_STEPS" "Verifying Document Exists"
    response=$(api_call "GET" "/api/v1/collections/${TEST_COLLECTION}/documents/${doc_id}" "" 200)
    if ! is_success "$response"; then
        print_error "Failed to retrieve inserted document"
        return 1
    fi
    local retrieved_name=$(json_value "$response" "name")
    if [ "$retrieved_name" != "Transaction Test User" ]; then
        print_error "Retrieved document data mismatch: expected 'Transaction Test User', got '$retrieved_name'"
        return 1
    fi
    print_success "Document verified in collection"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "6" "$TOTAL_STEPS" "Updating Document (Transactional)"
    body="{\"data\":{\"name\":\"Transaction Test User\",\"email\":\"tx@example.com\",\"balance\":2000}}"
    response=$(api_call "PUT" "/api/v1/collections/${TEST_COLLECTION}/documents/${doc_id}" "$body" 200)
    if ! is_success "$response"; then
        print_error "Transactional update failed"
        return 1
    fi
    print_success "Document updated"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "7" "$TOTAL_STEPS" "Verifying Updated Document"
    response=$(api_call "GET" "/api/v1/collections/${TEST_COLLECTION}/documents/${doc_id}" "" 200)
    if ! is_success "$response"; then
        print_error "Failed to retrieve updated document"
        return 1
    fi
    local balance=$(echo "$response" | grep -o '"balance":[0-9]*' | head -1 | cut -d':' -f2)
    if [ "$balance" != "2000" ]; then
        print_error "Updated document has incorrect balance: '$balance'"
        return 1
    fi
    print_success "Update verified - balance is $balance"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "8" "$TOTAL_STEPS" "Patching Document (Transactional)"
    body="{\"data\":{\"status\":\"active\"}}"
    response=$(api_call "PATCH" "/api/v1/collections/${TEST_COLLECTION}/documents/${doc_id}" "$body" 200)
    if ! is_success "$response"; then
        print_error "Transactional patch failed"
        return 1
    fi
    print_success "Document patched"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "9" "$TOTAL_STEPS" "Verifying Patched Document"
    response=$(api_call "GET" "/api/v1/collections/${TEST_COLLECTION}/documents/${doc_id}" "" 200)
    if ! is_success "$response"; then
        print_error "Failed to retrieve patched document"
        return 1
    fi
    local status=$(json_value "$response" "status")
    if [ "$status" != "active" ]; then
        print_error "Patched document has incorrect status: $status"
        return 1
    fi
    # Verify balance is still there from update
    balance=$(echo "$response" | grep -o '"balance":[0-9]*' | head -1 | cut -d':' -f2)
    if [ "$balance" != "2000" ]; then
        print_error "Patched document lost previous data - balance: '$balance'"
        return 1
    fi
    print_success "Patch verified - status=$status, balance=$balance"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "10" "$TOTAL_STEPS" "Deleting Document (Transactional)"
    response=$(api_call "DELETE" "/api/v1/collections/${TEST_COLLECTION}/documents/${doc_id}" "" 200)
    if ! is_success "$response"; then
        print_error "Transactional delete failed"
        return 1
    fi
    print_success "Document deleted"
    
    print_step "10" "$TOTAL_STEPS" "Verifying Document Deleted"
    response=$(api_call "GET" "/api/v1/collections/${TEST_COLLECTION}/documents/${doc_id}" "" 404)
    if [ $? -ne 0 ]; then
        print_error "Deleted document still exists"
        return 1
    fi
    print_success "Document deletion verified"
    
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

main "$@"
