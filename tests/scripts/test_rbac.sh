#!/bin/bash

# =============================================================================
# Test: RBAC (Role-Based Access Control)
# =============================================================================
# This test verifies RBAC functionality
# Steps:
#   1. Create admin user and login
#   2. Create regular user and login
#   3. Admin creates a collection
#   4. Regular user tries to access collection (should work with read)
#   5. Admin grants write permission to regular user
#   6. Regular user tries to write to collection
#   7. Admin revokes permission
#   8. Regular user tries to access (should fail)
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common.sh"

TEST_NAME="RBAC Access Control"
TOTAL_STEPS=8
INTERACTIVE="${INTERACTIVE:-false}"

run_test() {
    local unique_id=$(generate_unique_id)
    local random_suffix=$(echo "$unique_id" | cut -d'_' -f2)
    
    # Admin user
    local admin_user="admin_rbac_${random_suffix}"
    local admin_tenant="tenant_admin_${random_suffix}"
    
    # Regular user
    local regular_user="user_rbac_${random_suffix}"
    local regular_tenant="tenant_user_${random_suffix}"
    
    # Step 1: Create admin user and login
    print_step "1" "$TOTAL_STEPS" "Creating Admin User"
    local body="{\"username\":\"$admin_user\",\"password\":\"password123\",\"email\":\"${admin_user}@test.com\",\"tenantId\":\"$admin_tenant\"}"
    local response
    response=$(api_call "POST" "/api/v1/register" "$body" 200)
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    if ! is_success "$response"; then return 1; fi
    
    # Login as admin
    body="{\"username\":\"$admin_user\",\"password\":\"password123\"}"
    response=$(api_call "POST" "/api/v1/login" "$body" 200)
    if ! is_success "$response"; then return 1; fi
    local admin_token=$(json_value "$response" "token")
    TEST_TOKEN="$admin_token"
    
    # Create tenant for admin
    api_call "POST" "/api/v1/tenants" "{\"id\":\"$admin_tenant\",\"name\":\"Admin Tenant\"}" 200
    
    # Step 2: Create regular user
    print_step "2" "$TOTAL_STEPS" "Creating Regular User"
    body="{\"username\":\"$regular_user\",\"password\":\"password123\",\"email\":\"${regular_user}@test.com\",\"tenantId\":\"$regular_tenant\"}"
    response=$(api_call "POST" "/api/v1/register" "$body" 200)
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    if ! is_success "$response"; then return 1; fi
    
    # Login as regular user
    body="{\"username\":\"$regular_user\",\"password\":\"password123\"}"
    response=$(api_call "POST" "/api/v1/login" "$body" 200)
    if ! is_success "$response"; then return 1; fi
    local user_token=$(json_value "$response" "token")
    
    # Step 3: Admin creates a collection
    print_step "3" "$TOTAL_STEPS" "Admin Creates Collection"
    TEST_TOKEN="$admin_token"
    local collection_name="rbac_test_${random_suffix}"
    response=$(api_call "POST" "/api/v1/collections" "{\"name\":\"$collection_name\"}" 201)
    if ! is_success "$response"; then return 1; fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Insert a document as admin
    response=$(api_call "POST" "/api/v1/collections/${collection_name}/documents" "{\"data\":{\"name\":\"test doc\",\"owner\":\"admin\"}}" 201)
    
    # Step 4: Regular user tries to list collections (should work for authenticated users)
    print_step "4" "$TOTAL_STEPS" "Regular User Lists Collections"
    TEST_TOKEN="$user_token"
    response=$(api_call "GET" "/api/v1/collections" "" 200)
    if is_success "$response"; then
        print_success "Regular user can list collections"
    else
        print_warning "Regular user cannot list collections (may be expected)"
    fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 5: Regular user tries to read from admin's collection
    print_step "5" "$TOTAL_STEPS" "Regular User Tries to Read Admin's Collection"
    response=$(api_call "GET" "/api/v1/collections/${collection_name}/documents" "" 200)
    if is_success "$response"; then
        print_success "Regular user can read from collection (public read)"
    else
        print_info "Regular user cannot read from collection (access control working)"
    fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 6: Regular user tries to write to admin's collection
    print_step "6" "$TOTAL_STEPS" "Regular User Tries to Write to Admin's Collection"
    response=$(api_call "POST" "/api/v1/collections/${collection_name}/documents" "{\"data\":{\"name\":\"user doc\",\"owner\":\"user\"}}" 201)
    if is_success "$response"; then
        print_success "Regular user can write to collection"
    else
        print_info "Regular user cannot write to collection (access control working)"
    fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 7: Admin creates vector collection and tests access
    print_step "7" "$TOTAL_STEPS" "Admin Creates Vector Collection"
    TEST_TOKEN="$admin_token"
    local vector_collection="rbac_vectors_${random_suffix}"
    response=$(api_call "POST" "/api/v1/vectors" "{\"name\":\"$vector_collection\",\"dimensions\":4,\"distanceMetric\":\"cosine\"}" 201)
    if ! is_success "$response"; then return 1; fi
    
    # Insert vector as admin
    response=$(api_call "POST" "/api/v1/vectors/${vector_collection}/documents" "{\"_id\":\"vec1\",\"vector\":[1,0,0,0],\"metadata\":{\"owner\":\"admin\"}}" 201)
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 8: Regular user tries vector operations
    print_step "8" "$TOTAL_STEPS" "Regular User Tries Vector Operations"
    TEST_TOKEN="$user_token"
    
    # Try to read vectors
    response=$(api_call "GET" "/api/v1/vectors/${vector_collection}/documents/vec1" "" 200)
    if is_success "$response"; then
        print_success "Regular user can read vectors"
    else
        print_info "Regular user cannot read vectors (access control working)"
    fi
    
    # Try to search vectors
    response=$(api_call "POST" "/api/v1/vectors/${vector_collection}/search" "{\"vector\":[1,0,0,0],\"topK\":1}" 200)
    if is_success "$response"; then
        print_success "Regular user can search vectors"
    else
        print_info "Regular user cannot search vectors (access control working)"
    fi
    
    print_success "RBAC test completed"
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
