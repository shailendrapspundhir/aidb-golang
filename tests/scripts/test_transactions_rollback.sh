#!/bin/bash

# =============================================================================
# Test: Transaction Rollback on Schema Validation Failure
# =============================================================================
# This test verifies that transactions properly roll back when schema
# validation fails, ensuring no partial data is committed
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common.sh"

TEST_NAME="Transaction Rollback (Schema Validation)"
TOTAL_STEPS=8
INTERACTIVE="${INTERACTIVE:-false}"

run_test() {
    local timestamp=$(date +%s)
    local username="admin_txrb_${timestamp}"
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
    
    print_step "3" "$TOTAL_STEPS" "Creating Schema-Strict Collection"
    TEST_COLLECTION="tx_rollback_${timestamp}"
    body="{\"name\":\"$TEST_COLLECTION\",\"schema\":{\"name\":\"$TEST_COLLECTION\",\"strict\":true,\"fields\":{\"name\":{\"type\":\"string\",\"required\":true},\"email\":{\"type\":\"string\",\"required\":true},\"age\":{\"type\":\"integer\",\"required\":true}}}}"
    response=$(api_call "POST" "/api/v1/collections" "$body" 201)
    if ! is_success "$response"; then
        print_error "Failed to create schema-strict collection"
        return 1
    fi
    print_success "Created schema-strict collection"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "4" "$TOTAL_STEPS" "Inserting Valid Document (Should Succeed)"
    body="{\"data\":{\"name\":\"Valid User\",\"email\":\"valid@example.com\",\"age\":25}}"
    response=$(api_call "POST" "/api/v1/collections/${TEST_COLLECTION}/documents" "$body" 201)
    if ! is_success "$response"; then
        print_error "Valid document insert failed"
        return 1
    fi
    local valid_doc_id=$(json_value "$response" "_id")
    print_success "Valid document inserted with ID: $valid_doc_id"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "5" "$TOTAL_STEPS" "Attempting Invalid Insert (Missing Required Field - Should Fail)"
    body="{\"data\":{\"name\":\"Invalid User\",\"email\":\"invalid@example.com\"}}"
    response=$(api_call "POST" "/api/v1/collections/${TEST_COLLECTION}/documents" "$body" 400)
    # Expecting 400 Bad Request due to validation failure
    if [ $? -eq 0 ]; then
        print_success "Invalid insert correctly rejected with 400"
    else
        print_error "Expected 400 for invalid insert, got different response"
        return 1
    fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "6" "$TOTAL_STEPS" "Verifying No Partial Document Was Created"
    # List all documents - should only find the valid one
    response=$(api_call "GET" "/api/v1/collections/${TEST_COLLECTION}/documents" "" 200)
    if ! is_success "$response"; then
        print_error "Failed to list documents"
        return 1
    fi
    local doc_count=$(echo "$response" | grep -o '"_id"' | wc -l)
    if [ "$doc_count" -ne 1 ]; then
        print_error "Expected 1 document, found $doc_count (rollback may have failed)"
        return 1
    fi
    print_success "Only 1 document exists - rollback worked correctly"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "7" "$TOTAL_STEPS" "Attempting Invalid Update (Wrong Type - Should Fail)"
    body="{\"data\":{\"name\":\"Valid User\",\"email\":\"valid@example.com\",\"age\":\"not-a-number\"}}"
    response=$(api_call "PUT" "/api/v1/collections/${TEST_COLLECTION}/documents/${valid_doc_id}" "$body" 400)
    if [ $? -eq 0 ]; then
        print_success "Invalid update correctly rejected with 400"
    else
        print_error "Expected 400 for invalid update"
        return 1
    fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "8" "$TOTAL_STEPS" "Verifying Original Document Unchanged"
    response=$(api_call "GET" "/api/v1/collections/${TEST_COLLECTION}/documents/${valid_doc_id}" "" 200)
    if ! is_success "$response"; then
        print_error "Failed to retrieve document"
        return 1
    fi
    local age=$(echo "$response" | grep -o '"age":[0-9]*' | head -1 | cut -d':' -f2)
    if [ "$age" != "25" ]; then
        print_error "Document was modified despite failed update! Age='$age'"
        return 1
    fi
    print_success "Document unchanged - age still $age"
    
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
