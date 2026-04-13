#!/bin/bash

# =============================================================================
# Test: Vector Update
# =============================================================================
# This test verifies vector document update functionality
# Steps:
#   1-6: Setup (Register, Login, Tenant, Region, Env, Collection)
#   7. Insert Vector Document
#   8. Update Vector Document (full update)
#   9. Patch Vector Document (partial update - metadata)
#   10. Patch Vector Document (partial update - vector only)
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common.sh"

TEST_NAME="Vector Update"
TOTAL_STEPS=10
INTERACTIVE="${INTERACTIVE:-false}"

run_test() {
    local unique_id=$(generate_unique_id)
    local timestamp=$(echo "$unique_id" | cut -d'_' -f1)
    local random_suffix=$(echo "$unique_id" | cut -d'_' -f2)
    local username="admin_vec_upd_${random_suffix}"
    local tenant_id="tenant_${random_suffix}"
    local region_id="region_${random_suffix}"
    local env_id="env_${random_suffix}"
    local doc_id="doc_${random_suffix}"
    
    # Setup Steps 1-5
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
    api_call "POST" "/api/v1/regions" "{\"id\":\"$region_id\",\"name\":\"Test Region\",\"tenantId\":\"$tenant_id\"}" 200
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "5" "$TOTAL_STEPS" "Creating Environment"
    api_call "POST" "/api/v1/environments" "{\"id\":\"$env_id\",\"name\":\"Test Env\",\"regionId\":\"$region_id\",\"tenantId\":\"$tenant_id\"}" 200
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "6" "$TOTAL_STEPS" "Creating Vector Collection"
    TEST_VECTOR_COLLECTION="vectors_${random_suffix}"
    response=$(api_call "POST" "/api/v1/vectors" "{\"name\":\"$TEST_VECTOR_COLLECTION\",\"dimensions\":4,\"distanceMetric\":\"cosine\"}" 201)
    if ! is_success "$response"; then return 1; fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 7: Insert
    print_step "7" "$TOTAL_STEPS" "Inserting Vector Document"
    body="{\"_id\":\"$doc_id\",\"vector\":[1.0,0.0,0.0,0.0],\"metadata\":{\"label\":\"original\",\"version\":1}}"
    response=$(api_call "POST" "/api/v1/vectors/${TEST_VECTOR_COLLECTION}/documents" "$body" 201)
    if ! is_success "$response"; then return 1; fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue to update... "; then return 1; fi; fi
    
    # Step 8: Full Update
    print_step "8" "$TOTAL_STEPS" "Updating Vector Document (Full Update)"
    body="{\"vector\":[0.0,1.0,0.0,0.0],\"metadata\":{\"label\":\"updated\",\"version\":2}}"
    response=$(api_call "PUT" "/api/v1/vectors/${TEST_VECTOR_COLLECTION}/documents/${doc_id}" "$body" 200)
    
    if ! is_success "$response"; then
        print_error "Full update failed"
        return 1
    fi
    
    print_success "Vector document updated successfully"
    
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue to patch metadata... "; then return 1; fi; fi
    
    # Step 9: Patch Metadata
    print_step "9" "$TOTAL_STEPS" "Patching Vector Document (Metadata Only)"
    body="{\"metadata\":{\"label\":\"patched\",\"extra\":\"field\"}}"
    response=$(api_call "PATCH" "/api/v1/vectors/${TEST_VECTOR_COLLECTION}/documents/${doc_id}" "$body" 200)
    
    if ! is_success "$response"; then
        print_error "Metadata patch failed"
        return 1
    fi
    
    print_success "Metadata patched successfully"
    
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue to patch vector... "; then return 1; fi; fi
    
    # Step 10: Patch Vector
    print_step "10" "$TOTAL_STEPS" "Patching Vector Document (Vector Only)"
    body="{\"vector\":[0.5,0.5,0.5,0.5]}"
    response=$(api_call "PATCH" "/api/v1/vectors/${TEST_VECTOR_COLLECTION}/documents/${doc_id}" "$body" 200)
    
    if ! is_success "$response"; then
        print_error "Vector patch failed"
        return 1
    fi
    
    print_success "Vector patched successfully"
    
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
