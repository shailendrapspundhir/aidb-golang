#!/bin/bash

# =============================================================================
# Test: Concurrent Transaction Operations
# =============================================================================
# This test verifies that multiple concurrent operations work correctly
# with transaction isolation
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common.sh"

TEST_NAME="Concurrent Transaction Operations"
TOTAL_STEPS=7
INTERACTIVE="${INTERACTIVE:-false}"

# Function to insert documents in parallel
insert_documents_parallel() {
    local collection=$1
    local start=$2
    local count=$3
    local token=$4
    
    for i in $(seq $start $((start + count - 1))); do
        local body="{\"data\":{\"index\":$i,\"batch\":$start,\"timestamp\":$(date +%s%N)}}"
        curl -s -X POST "${BASE_URL}/api/v1/collections/${collection}/documents" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer ${token}" \
            -d "$body" > /dev/null 2>&1
    done
}

run_test() {
    local timestamp=$(date +%s)
    local username="admin_txc_${timestamp}"
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
    
    print_step "3" "$TOTAL_STEPS" "Creating Collection for Concurrent Tests"
    TEST_COLLECTION="tx_concurrent_${timestamp}"
    response=$(api_call "POST" "/api/v1/collections" "{\"name\":\"$TEST_COLLECTION\"}" 201)
    if ! is_success "$response"; then return 1; fi
    print_success "Created collection: $TEST_COLLECTION"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "4" "$TOTAL_STEPS" "Inserting Documents Concurrently (3 parallel batches)"
    print_info "Starting 3 parallel insertion batches..."
    
    # Start 3 parallel insertion processes
    insert_documents_parallel "$TEST_COLLECTION" 1 10 "$TEST_TOKEN" &
    local pid1=$!
    insert_documents_parallel "$TEST_COLLECTION" 11 10 "$TEST_TOKEN" &
    local pid2=$!
    insert_documents_parallel "$TEST_COLLECTION" 21 10 "$TEST_TOKEN" &
    local pid3=$!
    
    # Wait for all to complete
    wait $pid1
    local status1=$?
    wait $pid2
    local status2=$?
    wait $pid3
    local status3=$?
    
    if [ $status1 -ne 0 ] || [ $status2 -ne 0 ] || [ $status3 -ne 0 ]; then
        print_error "One or more parallel insertions failed"
        return 1
    fi
    print_success "All 30 documents inserted concurrently"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "5" "$TOTAL_STEPS" "Verifying All Documents Exist"
    response=$(api_call "GET" "/api/v1/collections/${TEST_COLLECTION}/documents" "" 200)
    if ! is_success "$response"; then
        print_error "Failed to list documents"
        return 1
    fi
    local doc_count=$(echo "$response" | grep -o '"_id"' | wc -l)
    if [ "$doc_count" -ne 30 ]; then
        print_error "Expected 30 documents, found $doc_count"
        return 1
    fi
    print_success "All $doc_count documents verified"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "6" "$TOTAL_STEPS" "Performing Concurrent Updates"
    print_info "Updating documents while reading them..."
    
    # Get first 5 document IDs
    local doc_ids=$(echo "$response" | grep -o '"_id":"[^"]*"' | head -5 | cut -d'"' -f4)
    
    # Update documents in background
    for doc_id in $doc_ids; do
        local update_body="{\"data\":{\"status\":\"updated\"}}"
        curl -s -X PATCH "${BASE_URL}/api/v1/collections/${TEST_COLLECTION}/documents/${doc_id}" \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer ${TEST_TOKEN}" \
            -d "$update_body" > /dev/null 2>&1 &
    done
    
    # Immediately try to read them (concurrent read during update)
    local read_count=0
    for doc_id in $doc_ids; do
        local read_response=$(curl -s -X GET "${BASE_URL}/api/v1/collections/${TEST_COLLECTION}/documents/${doc_id}" \
            -H "Authorization: Bearer ${TEST_TOKEN}")
        if echo "$read_response" | grep -q '"success":true'; then
            ((read_count++))
        fi
    done
    
    wait  # Wait for all background updates
    
    if [ $read_count -lt 5 ]; then
        print_error "Could only read $read_count/5 documents during concurrent updates"
        return 1
    fi
    print_success "Successfully read $read_count documents during concurrent updates"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "7" "$TOTAL_STEPS" "Verifying Updates Applied"
    response=$(api_call "GET" "/api/v1/collections/${TEST_COLLECTION}/documents" "" 200)
    if ! is_success "$response"; then
        print_error "Failed to list documents"
        return 1
    fi
    local updated_count=$(echo "$response" | grep -o '"status":"updated"' | wc -l)
    if [ "$updated_count" -lt 5 ]; then
        print_error "Expected at least 5 updated documents, found $updated_count"
        return 1
    fi
    print_success "$updated_count documents have status=updated"
    
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
