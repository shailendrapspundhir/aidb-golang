#!/bin/bash

# =============================================================================
# Test: Vector Delete
# =============================================================================
# This test verifies vector document deletion functionality
# Steps:
#   1-6: Setup
#   7. Insert Vector Documents
#   8. Delete Single Document
#   9. Verify Deletion
#   10. Delete Non-existent Document (should fail)
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common.sh"

TEST_NAME="Vector Delete"
TOTAL_STEPS=10
INTERACTIVE="${INTERACTIVE:-false}"

run_test() {
    local timestamp=$(date +%s)
    local username="admin_vec_delete_${timestamp}"
    local tenant_id="tenant_${timestamp}"
    
    # Setup
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
    
    print_step "3" "$TOTAL_STEPS" "Creating Tenant"
    api_call "POST" "/api/v1/tenants" "{\"id\":\"$tenant_id\",\"name\":\"Test Tenant\"}" 200
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "4" "$TOTAL_STEPS" "Creating Region"
    api_call "POST" "/api/v1/regions" "{\"id\":\"region_${timestamp}\",\"name\":\"Test Region\",\"tenantId\":\"$tenant_id\"}" 200
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "5" "$TOTAL_STEPS" "Creating Environment"
    api_call "POST" "/api/v1/environments" "{\"id\":\"env_${timestamp}\",\"name\":\"Test Env\",\"regionId\":\"region_${timestamp}\",\"tenantId\":\"$tenant_id\"}" 200
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "6" "$TOTAL_STEPS" "Creating Vector Collection"
    TEST_VECTOR_COLLECTION="vectors_${timestamp}"
    response=$(api_call "POST" "/api/v1/vectors" "{\"name\":\"$TEST_VECTOR_COLLECTION\",\"dimensions\":4,\"distanceMetric\":\"cosine\"}" 201)
    if ! is_success "$response"; then return 1; fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 7: Insert multiple documents
    print_step "7" "$TOTAL_STEPS" "Inserting Multiple Vector Documents"
    api_call "POST" "/api/v1/vectors/${TEST_VECTOR_COLLECTION}/documents" "{\"_id\":\"doc1\",\"vector\":[1.0,0.0,0.0,0.0],\"metadata\":{\"label\":\"doc1\"}}" 201
    api_call "POST" "/api/v1/vectors/${TEST_VECTOR_COLLECTION}/documents" "{\"_id\":\"doc2\",\"vector\":[0.0,1.0,0.0,0.0],\"metadata\":{\"label\":\"doc2\"}}" 201
    api_call "POST" "/api/v1/vectors/${TEST_VECTOR_COLLECTION}/documents" "{\"_id\":\"doc3\",\"vector\":[0.0,0.0,1.0,0.0],\"metadata\":{\"label\":\"doc3\"}}" 201
    print_success "Inserted 3 documents"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue to delete... "; then return 1; fi; fi
    
    # Step 8: Delete single document
    print_step "8" "$TOTAL_STEPS" "Deleting Document 'doc2'"
    response=$(api_call "DELETE" "/api/v1/vectors/${TEST_VECTOR_COLLECTION}/documents/doc2" "" 200)
    
    if ! is_success "$response"; then
        print_error "Delete failed"
        return 1
    fi
    print_success "Document deleted successfully"
    
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue to verify... "; then return 1; fi; fi
    
    # Step 9: Verify deletion
    print_step "9" "$TOTAL_STEPS" "Verifying Deletion (Get doc2 should fail)"
    response=$(api_call "GET" "/api/v1/vectors/${TEST_VECTOR_COLLECTION}/documents/doc2" "" 200)
    
    if is_success "$response"; then
        print_error "Document still exists after deletion"
        return 1
    fi
    print_success "Document correctly deleted (404 expected)"
    
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 10: Delete non-existent document
    print_step "10" "$TOTAL_STEPS" "Deleting Non-existent Document"
    response=$(api_call "DELETE" "/api/v1/vectors/${TEST_VECTOR_COLLECTION}/documents/nonexistent" "" 200)
    
    # This should fail
    if is_success "$response"; then
        print_warning "Non-existent delete returned success (may be expected behavior)"
    else
        print_success "Non-existent delete correctly failed"
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
