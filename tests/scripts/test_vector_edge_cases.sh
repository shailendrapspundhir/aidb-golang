#!/bin/bash

# =============================================================================
# Test: Vector Edge Cases
# =============================================================================
# This test verifies edge cases and error handling for vectors
# Steps:
#   1-6: Setup
#   7. Test dimension mismatch
#   8. Test empty vector
#   9. Test duplicate ID
#   10. Test invalid distance metric
#   11. Test zero dimensions
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common.sh"

TEST_NAME="Vector Edge Cases"
TOTAL_STEPS=11
INTERACTIVE="${INTERACTIVE:-false}"

run_test() {
    local timestamp=$(date +%s)
    local username="admin_vec_edge_${timestamp}"
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
    
    print_step "6" "$TOTAL_STEPS" "Creating Vector Collection (4 dimensions)"
    TEST_VECTOR_COLLECTION="vectors_${timestamp}"
    response=$(api_call "POST" "/api/v1/vectors" "{\"name\":\"$TEST_VECTOR_COLLECTION\",\"dimensions\":4,\"distanceMetric\":\"cosine\"}" 201)
    if ! is_success "$response"; then return 1; fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 7: Test dimension mismatch
    print_step "7" "$TOTAL_STEPS" "Testing Dimension Mismatch (insert 3D vector into 4D collection)"
    body="{\"_id\":\"dim_mismatch\",\"vector\":[1.0,0.0,0.0],\"metadata\":{}}"
    response=$(api_call "POST" "/api/v1/vectors/${TEST_VECTOR_COLLECTION}/documents" "$body" 400)
    
    if ! is_success "$response"; then
        print_success "Dimension mismatch correctly rejected"
    else
        print_error "Dimension mismatch should have been rejected"
        return 1
    fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 8: Test empty vector
    print_step "8" "$TOTAL_STEPS" "Testing Empty Vector"
    body="{\"_id\":\"empty_vec\",\"vector\":[],\"metadata\":{}}"
    response=$(api_call "POST" "/api/v1/vectors/${TEST_VECTOR_COLLECTION}/documents" "$body" 400)
    
    if ! is_success "$response"; then
        print_success "Empty vector correctly rejected"
    else
        print_error "Empty vector should have been rejected"
        return 1
    fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 9: Test duplicate ID
    print_step "9" "$TOTAL_STEPS" "Testing Duplicate ID"
    # First insert
    body="{\"_id\":\"duplicate_test\",\"vector\":[1.0,0.0,0.0,0.0],\"metadata\":{}}"
    api_call "POST" "/api/v1/vectors/${TEST_VECTOR_COLLECTION}/documents" "$body" 201
    
    # Try duplicate
    body="{\"_id\":\"duplicate_test\",\"vector\":[0.0,1.0,0.0,0.0],\"metadata\":{}}"
    response=$(api_call "POST" "/api/v1/vectors/${TEST_VECTOR_COLLECTION}/documents" "$body" 409)
    
    if ! is_success "$response"; then
        print_success "Duplicate ID correctly rejected"
    else
        print_error "Duplicate ID should have been rejected"
        return 1
    fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 10: Test invalid distance metric
    print_step "10" "$TOTAL_STEPS" "Testing Invalid Distance Metric"
    body="{\"name\":\"invalid_metric_${timestamp}\",\"dimensions\":4,\"distanceMetric\":\"invalid_metric\"}"
    response=$(api_call "POST" "/api/v1/vectors" "$body" 400)
    
    if ! is_success "$response"; then
        print_success "Invalid distance metric correctly rejected"
    else
        print_error "Invalid distance metric should have been rejected"
        return 1
    fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 11: Test zero dimensions
    print_step "11" "$TOTAL_STEPS" "Testing Zero Dimensions"
    body="{\"name\":\"zero_dim_${timestamp}\",\"dimensions\":0,\"distanceMetric\":\"cosine\"}"
    response=$(api_call "POST" "/api/v1/vectors" "$body" 400)
    
    if ! is_success "$response"; then
        print_success "Zero dimensions correctly rejected"
    else
        print_error "Zero dimensions should have been rejected"
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
