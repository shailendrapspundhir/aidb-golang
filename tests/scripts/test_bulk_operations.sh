#!/bin/bash

# =============================================================================
# Test: Bulk Operations with Transactions
# =============================================================================
# This test verifies bulk insert, update, and delete operations with
# full transaction support (all succeed or all rollback)
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common.sh"

TEST_NAME="Bulk Operations with Transactions"
TOTAL_STEPS=12
INTERACTIVE="${INTERACTIVE:-false}"

run_test() {
    local timestamp=$(date +%s)
    local username="admin_bulk_${timestamp}"
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
    
    print_step "3" "$TOTAL_STEPS" "Creating Collection for Bulk Tests"
    TEST_COLLECTION="bulk_test_${timestamp}"
    response=$(api_call "POST" "/api/v1/collections" "{\"name\":\"$TEST_COLLECTION\"}" 201)
    if ! is_success "$response"; then return 1; fi
    print_success "Created collection: $TEST_COLLECTION"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "4" "$TOTAL_STEPS" "Bulk Insert 10 Documents"
    body='{"documents":[
        {"name":"User 1","email":"user1@test.com","status":"active"},
        {"name":"User 2","email":"user2@test.com","status":"active"},
        {"name":"User 3","email":"user3@test.com","status":"inactive"},
        {"name":"User 4","email":"user4@test.com","status":"active"},
        {"name":"User 5","email":"user5@test.com","status":"inactive"},
        {"name":"User 6","email":"user6@test.com","status":"active"},
        {"name":"User 7","email":"user7@test.com","status":"active"},
        {"name":"User 8","email":"user8@test.com","status":"inactive"},
        {"name":"User 9","email":"user9@test.com","status":"active"},
        {"name":"User 10","email":"user10@test.com","status":"active"}
    ]}'
    response=$(api_call "POST" "/api/v1/collections/${TEST_COLLECTION}/bulk/insert" "$body" 201)
    if ! is_success "$response"; then
        print_error "Bulk insert failed"
        return 1
    fi
    local inserted_count=$(echo "$response" | grep -o '"count":[0-9]*' | head -1 | cut -d':' -f2)
    if [ "$inserted_count" != "10" ]; then
        print_error "Expected 10 inserted documents, got $inserted_count"
        return 1
    fi
    print_success "Bulk inserted $inserted_count documents"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "5" "$TOTAL_STEPS" "Verifying All Documents Exist"
    response=$(api_call "GET" "/api/v1/collections/${TEST_COLLECTION}/documents" "" 200)
    if ! is_success "$response"; then
        print_error "Failed to list documents"
        return 1
    fi
    local doc_count=$(echo "$response" | grep -o '"_id"' | wc -l)
    if [ "$doc_count" -ne 10 ]; then
        print_error "Expected 10 documents, found $doc_count"
        return 1
    fi
    print_success "All $doc_count documents verified"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "6" "$TOTAL_STEPS" "Bulk Update Status for All Documents"
    # Get document IDs first
    local doc_ids=$(echo "$response" | grep -o '"_id":"[^"]*"' | head -5 | cut -d'"' -f4)
    
    # Build bulk update request
    local updates="["
    local first=true
    for doc_id in $doc_ids; do
        if [ "$first" == "true" ]; then
            first=false
        else
            updates="${updates},"
        fi
        updates="${updates}{\"_id\":\"${doc_id}\",\"data\":{\"status\":\"updated\"}}"
    done
    updates="${updates}]"
    
    body="{\"updates\":$updates}"
    response=$(api_call "POST" "/api/v1/collections/${TEST_COLLECTION}/bulk/update" "$body" 200)
    if ! is_success "$response"; then
        print_error "Bulk update failed"
        return 1
    fi
    local updated_count=$(echo "$response" | grep -o '"count":[0-9]*' | head -1 | cut -d':' -f2)
    if [ "$updated_count" -ne 5 ]; then
        print_error "Expected 5 updated documents, got $updated_count"
        return 1
    fi
    print_success "Bulk updated $updated_count documents"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "7" "$TOTAL_STEPS" "Verifying Updates Applied"
    response=$(api_call "GET" "/api/v1/collections/${TEST_COLLECTION}/documents" "" 200)
    if ! is_success "$response"; then
        print_error "Failed to list documents"
        return 1
    fi
    local updated_status_count=$(echo "$response" | grep -o '"status":"updated"' | wc -l)
    if [ "$updated_status_count" -ne 5 ]; then
        print_error "Expected 5 documents with status=updated, found $updated_status_count"
        return 1
    fi
    print_success "$updated_status_count documents have status=updated"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "8" "$TOTAL_STEPS" "Bulk Delete 3 Documents"
    # Get 3 document IDs to delete
    local delete_ids=$(echo "$response" | grep -o '"_id":"[^"]*"' | head -3 | cut -d'"' -f4)
    local ids_json="["
    first=true
    for doc_id in $delete_ids; do
        if [ "$first" == "true" ]; then
            first=false
        else
            ids_json="${ids_json},"
        fi
        ids_json="${ids_json}\"${doc_id}\""
    done
    ids_json="${ids_json}]"
    
    body="{\"ids\":$ids_json}"
    response=$(api_call "POST" "/api/v1/collections/${TEST_COLLECTION}/bulk/delete" "$body" 200)
    if ! is_success "$response"; then
        print_error "Bulk delete failed"
        return 1
    fi
    local deleted_count=$(echo "$response" | grep -o '"count":[0-9]*' | head -1 | cut -d':' -f2)
    if [ "$deleted_count" -ne 3 ]; then
        print_error "Expected 3 deleted documents, got $deleted_count"
        return 1
    fi
    print_success "Bulk deleted $deleted_count documents"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "9" "$TOTAL_STEPS" "Verifying Documents Deleted"
    response=$(api_call "GET" "/api/v1/collections/${TEST_COLLECTION}/documents" "" 200)
    if ! is_success "$response"; then
        print_error "Failed to list documents"
        return 1
    fi
    local remaining_count=$(echo "$response" | grep -o '"_id"' | wc -l)
    if [ "$remaining_count" -ne 7 ]; then
        print_error "Expected 7 remaining documents, found $remaining_count"
        return 1
    fi
    print_success "$remaining_count documents remaining after bulk delete"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "10" "$TOTAL_STEPS" "Testing Bulk Insert with Validation Failure (Should Rollback)"
    # Create schema-strict collection
    local strict_collection="bulk_strict_${timestamp}"
    body="{\"name\":\"$strict_collection\",\"schema\":{\"name\":\"$strict_collection\",\"strict\":true,\"fields\":{\"name\":{\"type\":\"string\",\"required\":true},\"email\":{\"type\":\"string\",\"required\":true},\"age\":{\"type\":\"integer\",\"required\":true}}}}"
    response=$(api_call "POST" "/api/v1/collections" "$body" 201)
    if ! is_success "$response"; then
        print_error "Failed to create strict collection"
        return 1
    fi
    
    # Try bulk insert with one invalid document (missing age)
    body='{"documents":[
        {"name":"Valid 1","email":"valid1@test.com","age":25},
        {"name":"Invalid","email":"invalid@test.com"},
        {"name":"Valid 2","email":"valid2@test.com","age":30}
    ]}'
    response=$(api_call "POST" "/api/v1/collections/${strict_collection}/bulk/insert" "$body" 400)
    if [ $? -ne 0 ]; then
        print_error "Expected 400 for invalid bulk insert"
        return 1
    fi
    print_success "Bulk insert correctly rejected due to validation failure"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "11" "$TOTAL_STEPS" "Verifying No Documents Created After Failed Bulk Insert"
    response=$(api_call "GET" "/api/v1/collections/${strict_collection}/documents" "" 200)
    if ! is_success "$response"; then
        print_error "Failed to list documents"
        return 1
    fi
    local strict_count=$(echo "$response" | grep -o '"_id"' | wc -l)
    if [ "$strict_count" -ne 0 ]; then
        print_error "Expected 0 documents after rollback, found $strict_count"
        return 1
    fi
    print_success "No documents created - transaction rolled back correctly"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "12" "$TOTAL_STEPS" "Testing Valid Bulk Insert in Strict Collection"
    body='{"documents":[
        {"name":"Valid 1","email":"valid1@test.com","age":25},
        {"name":"Valid 2","email":"valid2@test.com","age":30},
        {"name":"Valid 3","email":"valid3@test.com","age":35}
    ]}'
    response=$(api_call "POST" "/api/v1/collections/${strict_collection}/bulk/insert" "$body" 201)
    if ! is_success "$response"; then
        print_error "Valid bulk insert failed"
        return 1
    fi
    local valid_count=$(echo "$response" | grep -o '"count":[0-9]*' | head -1 | cut -d':' -f2)
    if [ "$valid_count" -ne 3 ]; then
        print_error "Expected 3 documents, got $valid_count"
        return 1
    fi
    print_success "Bulk inserted $valid_count documents in strict collection"
    
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
