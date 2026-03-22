#!/bin/bash

# =============================================================================
# Test: Distance Metrics
# =============================================================================
# This test verifies different distance metrics for vector search
# Steps:
#   1-6: Setup
#   7. Create collection with cosine similarity
#   8. Insert vectors and test cosine search
#   9. Create collection with euclidean distance
#   10. Insert vectors and test euclidean search
#   11. Create collection with dot product
#   12. Insert vectors and test dot product search
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common.sh"

TEST_NAME="Distance Metrics"
TOTAL_STEPS=12
INTERACTIVE="${INTERACTIVE:-false}"

run_test() {
    local unique_id=$(generate_unique_id)
    local random_suffix=$(echo "$unique_id" | cut -d'_' -f2)
    local username="admin_dist_${random_suffix}"
    local tenant_id="tenant_${random_suffix}"
    local region_id="region_${random_suffix}"
    local env_id="env_${random_suffix}"
    
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
    api_call "POST" "/api/v1/regions" "{\"id\":\"$region_id\",\"name\":\"Test Region\",\"tenantId\":\"$tenant_id\"}" 200
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "5" "$TOTAL_STEPS" "Creating Environment"
    api_call "POST" "/api/v1/environments" "{\"id\":\"$env_id\",\"name\":\"Test Env\",\"regionId\":\"$region_id\",\"tenantId\":\"$tenant_id\"}" 200
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 6: Create Cosine collection
    print_step "6" "$TOTAL_STEPS" "Creating Vector Collection (Cosine Similarity)"
    local cosine_collection="cosine_${random_suffix}"
    body="{\"name\":\"$cosine_collection\",\"dimensions\":3,\"distanceMetric\":\"cosine\"}"
    response=$(api_call "POST" "/api/v1/vectors" "$body" 201)
    if ! is_success "$response"; then return 1; fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 7: Insert vectors for cosine test
    print_step "7" "$TOTAL_STEPS" "Inserting Vectors for Cosine Test"
    # Normalized vectors for cosine similarity
    api_call "POST" "/api/v1/vectors/${cosine_collection}/documents" "{\"_id\":\"cos_a\",\"vector\":[1,0,0],\"metadata\":{\"label\":\"A\"}}" 201
    api_call "POST" "/api/v1/vectors/${cosine_collection}/documents" "{\"_id\":\"cos_b\",\"vector\":[0.707,0.707,0],\"metadata\":{\"label\":\"B\"}}" 201
    api_call "POST" "/api/v1/vectors/${cosine_collection}/documents" "{\"_id\":\"cos_c\",\"vector\":[0,1,0],\"metadata\":{\"label\":\"C\"}}" 201
    print_success "Inserted 3 vectors for cosine test"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 8: Search with cosine
    print_step "8" "$TOTAL_STEPS" "Searching with Cosine Similarity (query: [1,0,0])"
    body="{\"vector\":[1,0,0],\"topK\":3,\"includeVector\":true}"
    response=$(api_call "POST" "/api/v1/vectors/${cosine_collection}/search" "$body" 200)
    if ! is_success "$response"; then return 1; fi
    print_success "Cosine search completed"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 9: Create Euclidean collection
    print_step "9" "$TOTAL_STEPS" "Creating Vector Collection (Euclidean Distance)"
    local euclidean_collection="euclidean_${random_suffix}"
    body="{\"name\":\"$euclidean_collection\",\"dimensions\":3,\"distanceMetric\":\"euclidean\"}"
    response=$(api_call "POST" "/api/v1/vectors" "$body" 201)
    if ! is_success "$response"; then return 1; fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 10: Insert vectors for euclidean test
    print_step "10" "$TOTAL_STEPS" "Inserting Vectors for Euclidean Test"
    api_call "POST" "/api/v1/vectors/${euclidean_collection}/documents" "{\"_id\":\"euc_a\",\"vector\":[0,0,0],\"metadata\":{\"label\":\"origin\"}}" 201
    api_call "POST" "/api/v1/vectors/${euclidean_collection}/documents" "{\"_id\":\"euc_b\",\"vector\":[1,1,1],\"metadata\":{\"label\":\"diag\"}}" 201
    api_call "POST" "/api/v1/vectors/${euclidean_collection}/documents" "{\"_id\":\"euc_c\",\"vector\":[2,2,2],\"metadata\":{\"label\":\"far\"}}" 201
    print_success "Inserted 3 vectors for euclidean test"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Search with euclidean
    body="{\"vector\":[0,0,0],\"topK\":3,\"includeVector\":true}"
    response=$(api_call "POST" "/api/v1/vectors/${euclidean_collection}/search" "$body" 200)
    if ! is_success "$response"; then return 1; fi
    print_success "Euclidean search completed"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 11: Create Dot Product collection
    print_step "11" "$TOTAL_STEPS" "Creating Vector Collection (Dot Product)"
    local dot_collection="dot_${random_suffix}"
    body="{\"name\":\"$dot_collection\",\"dimensions\":3,\"distanceMetric\":\"dot\"}"
    response=$(api_call "POST" "/api/v1/vectors" "$body" 201)
    if ! is_success "$response"; then return 1; fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 12: Insert vectors for dot product test
    print_step "12" "$TOTAL_STEPS" "Inserting Vectors for Dot Product Test"
    api_call "POST" "/api/v1/vectors/${dot_collection}/documents" "{\"_id\":\"dot_a\",\"vector\":[1,0,0],\"metadata\":{\"label\":\"A\"}}" 201
    api_call "POST" "/api/v1/vectors/${dot_collection}/documents" "{\"_id\":\"dot_b\",\"vector\":[1,1,0],\"metadata\":{\"label\":\"B\"}}" 201
    api_call "POST" "/api/v1/vectors/${dot_collection}/documents" "{\"_id\":\"dot_c\",\"vector\":[1,1,1],\"metadata\":{\"label\":\"C\"}}" 201
    print_success "Inserted 3 vectors for dot product test"
    
    # Search with dot product
    body="{\"vector\":[1,1,1],\"topK\":3,\"includeVector\":true}"
    response=$(api_call "POST" "/api/v1/vectors/${dot_collection}/search" "$body" 200)
    if ! is_success "$response"; then return 1; fi
    print_success "Dot product search completed"
    
    print_success "Distance metrics test completed"
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
