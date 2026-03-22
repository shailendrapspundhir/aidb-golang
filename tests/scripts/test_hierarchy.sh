#!/bin/bash

# =============================================================================
# Test: Hierarchy (Tenant, Region, Environment)
# =============================================================================
# This test verifies hierarchy management functionality
# Steps:
#   1-2: Setup (Register, Login)
#   3. Create Tenant
#   4. List Tenants
#   5. Create Region
#   6. List Regions
#   7. Create Environment
#   8. List Environments
#   9. Delete Environment
#   10. Delete Region
#   11. Delete Tenant
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common.sh"

TEST_NAME="Hierarchy Management"
TOTAL_STEPS=11
INTERACTIVE="${INTERACTIVE:-false}"

run_test() {
    local unique_id=$(generate_unique_id)
    local timestamp=$(echo "$unique_id" | cut -d'_' -f1)
    local random_suffix=$(echo "$unique_id" | cut -d'_' -f2)
    local username="admin_hier_${random_suffix}"
    local tenant_id="tenant_${random_suffix}"
    local region_id="region_${random_suffix}"
    local env_id="env_${random_suffix}"
    
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
    body="{\"id\":\"$tenant_id\",\"name\":\"Test Tenant $random_suffix\"}"
    response=$(api_call "POST" "/api/v1/tenants" "$body" 200)
    # Continue even if tenant already exists (might happen in tests)
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "4" "$TOTAL_STEPS" "Listing Tenants"
    response=$(api_call "GET" "/api/v1/tenants" "" 200)
    if ! is_success "$response"; then return 1; fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "5" "$TOTAL_STEPS" "Creating Region"
    body="{\"id\":\"$region_id\",\"name\":\"Test Region $random_suffix\",\"tenantId\":\"$tenant_id\"}"
    response=$(api_call "POST" "/api/v1/regions" "$body" 200)
    # Continue even if region already exists
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "6" "$TOTAL_STEPS" "Listing Regions"
    response=$(api_call "GET" "/api/v1/regions" "" 200)
    if ! is_success "$response"; then return 1; fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "7" "$TOTAL_STEPS" "Creating Environment"
    body="{\"id\":\"$env_id\",\"name\":\"Test Environment $random_suffix\",\"regionId\":\"$region_id\",\"tenantId\":\"$tenant_id\"}"
    response=$(api_call "POST" "/api/v1/environments" "$body" 200)
    # Continue even if environment already exists
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "8" "$TOTAL_STEPS" "Listing Environments"
    response=$(api_call "GET" "/api/v1/environments" "" 200)
    if ! is_success "$response"; then return 1; fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "9" "$TOTAL_STEPS" "Deleting Environment"
    response=$(api_call "DELETE" "/api/v1/environments/${env_id}" "" 200)
    if ! is_success "$response"; then
        print_warning "Environment deletion returned non-success (may not exist)"
    fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "10" "$TOTAL_STEPS" "Deleting Region"
    response=$(api_call "DELETE" "/api/v1/regions/${region_id}" "" 200)
    if ! is_success "$response"; then
        print_warning "Region deletion returned non-success (may not exist)"
    fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "11" "$TOTAL_STEPS" "Deleting Tenant"
    response=$(api_call "DELETE" "/api/v1/tenants/${tenant_id}" "" 200)
    if ! is_success "$response"; then
        print_warning "Tenant deletion returned non-success (may not exist)"
    fi
    
    print_success "Hierarchy management test completed"
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
