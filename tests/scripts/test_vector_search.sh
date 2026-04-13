#!/bin/bash

# =============================================================================
# Test: Vector Search (Similarity Search)
# =============================================================================
# This test verifies vector similarity search functionality
# Steps:
#   1. Register User
#   2. Login
#   3. Create Tenant
#   4. Create Region
#   5. Create Environment
#   6. Create Vector Collection
#   7. Insert Multiple Vector Documents
#   8. Search Vectors
#   9. Search with Metadata Filter
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common.sh"

TEST_NAME="Vector Search"
TOTAL_STEPS=9
INTERACTIVE="${INTERACTIVE:-false}"

run_test() {
    local unique_id=$(generate_unique_id)
    local timestamp=$(echo "$unique_id" | cut -d'_' -f1)
    local random_suffix=$(echo "$unique_id" | cut -d'_' -f2)
    local username="admin_vec_srch_${random_suffix}"
    local tenant_id="tenant_${random_suffix}"
    local region_id="region_${random_suffix}"
    local env_id="env_${random_suffix}"
    
    # Step 1: Register
    print_step "1" "$TOTAL_STEPS" "Registering User"
    local body="{\"username\":\"$username\",\"password\":\"password123\",\"email\":\"${username}@test.com\",\"tenantId\":\"$tenant_id\"}"
    local response
    response=$(api_call "POST" "/api/v1/register" "$body" 200)
    
    if [ "$INTERACTIVE" == "true" ]; then
        if ! wait_for_input "Press Y to continue... "; then return 1; fi
    fi
    
    if ! is_success "$response"; then return 1; fi
    TEST_USER_ID=$(json_value "$response" "_id")
    TEST_TENANT_ID=$tenant_id
    
    # Step 2: Login
    print_step "2" "$TOTAL_STEPS" "Logging In"
    body="{\"username\":\"$username\",\"password\":\"password123\"}"
    response=$(api_call "POST" "/api/v1/login" "$body" 200)
    
    if [ "$INTERACTIVE" == "true" ]; then
        if ! wait_for_input "Press Y to continue... "; then return 1; fi
    fi
    
    if ! is_success "$response"; then return 1; fi
    TEST_TOKEN=$(json_value "$response" "token")
    
    # Step 3: Create Tenant
    print_step "3" "$TOTAL_STEPS" "Creating Tenant"
    body="{\"id\":\"$tenant_id\",\"name\":\"Test Tenant\"}"
    api_call "POST" "/api/v1/tenants" "$body" 200
    
    if [ "$INTERACTIVE" == "true" ]; then
        if ! wait_for_input "Press Y to continue... "; then return 1; fi
    fi
    
    # Step 4: Create Region
    print_step "4" "$TOTAL_STEPS" "Creating Region"
    body="{\"id\":\"$region_id\",\"name\":\"Test Region\",\"tenantId\":\"$tenant_id\"}"
    api_call "POST" "/api/v1/regions" "$body" 200
    
    if [ "$INTERACTIVE" == "true" ]; then
        if ! wait_for_input "Press Y to continue... "; then return 1; fi
    fi
    
    # Step 5: Create Environment
    print_step "5" "$TOTAL_STEPS" "Creating Environment"
    body="{\"id\":\"$env_id\",\"name\":\"Test Env\",\"regionId\":\"$region_id\",\"tenantId\":\"$tenant_id\"}"
    api_call "POST" "/api/v1/environments" "$body" 200
    
    if [ "$INTERACTIVE" == "true" ]; then
        if ! wait_for_input "Press Y to continue... "; then return 1; fi
    fi
    
    # Step 6: Create Vector Collection
    print_step "6" "$TOTAL_STEPS" "Creating Vector Collection"
    TEST_VECTOR_COLLECTION="vectors_${random_suffix}"
    body="{\"name\":\"$TEST_VECTOR_COLLECTION\",\"dimensions\":4,\"distanceMetric\":\"cosine\"}"
    response=$(api_call "POST" "/api/v1/vectors" "$body" 201)
    
    if ! is_success "$response"; then return 1; fi
    
    if [ "$INTERACTIVE" == "true" ]; then
        if ! wait_for_input "Press Y to continue to insert vectors... "; then return 1; fi
    fi
    
    # Step 7: Insert Multiple Vector Documents
    print_step "7" "$TOTAL_STEPS" "Inserting Multiple Vector Documents"
    
    # Insert doc1: x-axis vector
    body="{\"_id\":\"doc1_${random_suffix}\",\"vector\":[1.0,0.0,0.0,0.0],\"metadata\":{\"label\":\"x-axis\",\"category\":\"axis\"}}"
    api_call "POST" "/api/v1/vectors/${TEST_VECTOR_COLLECTION}/documents" "$body" 201
    
    # Insert doc2: y-axis vector
    body="{\"_id\":\"doc2_${random_suffix}\",\"vector\":[0.0,1.0,0.0,0.0],\"metadata\":{\"label\":\"y-axis\",\"category\":\"axis\"}}"
    api_call "POST" "/api/v1/vectors/${TEST_VECTOR_COLLECTION}/documents" "$body" 201
    
    # Insert doc3: z-axis vector
    body="{\"_id\":\"doc3_${random_suffix}\",\"vector\":[0.0,0.0,1.0,0.0],\"metadata\":{\"label\":\"z-axis\",\"category\":\"axis\"}}"
    api_call "POST" "/api/v1/vectors/${TEST_VECTOR_COLLECTION}/documents" "$body" 201
    
    # Insert doc4: diagonal vector
    body="{\"_id\":\"doc4_${random_suffix}\",\"vector\":[1.0,1.0,0.0,0.0],\"metadata\":{\"label\":\"xy-diagonal\",\"category\":\"diagonal\"}}"
    api_call "POST" "/api/v1/vectors/${TEST_VECTOR_COLLECTION}/documents" "$body" 201
    
    print_success "Inserted 4 vector documents"
    
    if [ "$INTERACTIVE" == "true" ]; then
        if ! wait_for_input "Press Y to continue to search... "; then return 1; fi
    fi
    
    # Step 8: Search Vectors
    print_step "8" "$TOTAL_STEPS" "Searching for Similar Vectors (query: [1,0,0,0])"
    body="{\"vector\":[1.0,0.0,0.0,0.0],\"topK\":3,\"includeVector\":true}"
    response=$(api_call "POST" "/api/v1/vectors/${TEST_VECTOR_COLLECTION}/search" "$body" 200)
    
    if ! is_success "$response"; then
        print_error "Vector search failed"
        return 1
    fi
    
    # Verify results contain doc1 (should be most similar)
    if echo "$response" | grep -q "doc1_${random_suffix}"; then
        print_success "Search returned doc1 (most similar to [1,0,0,0])"
    else
        print_error "Search did not return expected document"
        return 1
    fi
    
    if [ "$INTERACTIVE" == "true" ]; then
        if ! wait_for_input "Press Y to continue to filtered search... "; then return 1; fi
    fi
    
    # Step 9: Search with Metadata Filter
    print_step "9" "$TOTAL_STEPS" "Searching with Metadata Filter (category=diagonal)"
    body="{\"vector\":[1.0,0.0,0.0,0.0],\"topK\":10,\"filter\":{\"category\":\"diagonal\"},\"includeVector\":true}"
    response=$(api_call "POST" "/api/v1/vectors/${TEST_VECTOR_COLLECTION}/search" "$body" 200)
    
    if ! is_success "$response"; then
        print_error "Filtered search failed"
        return 1
    fi
    
    # Verify only diagonal documents returned
    if echo "$response" | grep -q "doc4_${random_suffix}" && ! echo "$response" | grep -q "doc1_${random_suffix}"; then
        print_success "Filter correctly returned only diagonal documents"
    else
        print_error "Filter did not work correctly"
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
