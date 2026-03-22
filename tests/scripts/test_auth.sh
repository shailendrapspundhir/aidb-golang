#!/bin/bash

# =============================================================================
# Test: Authentication
# =============================================================================
# This test verifies authentication functionality
# Steps:
#   1. Register new user
#   2. Login with correct credentials
#   3. Login with wrong password (should fail)
#   4. Access protected endpoint without token (should fail)
#   5. Access protected endpoint with token
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common.sh"

TEST_NAME="Authentication"
TOTAL_STEPS=5
INTERACTIVE="${INTERACTIVE:-false}"

run_test() {
    local timestamp=$(date +%s)
    local username="admin_auth_test_${timestamp}"
    local password="password123"
    local tenant_id="tenant_${timestamp}"
    
    # Step 1: Register
    print_step "1" "$TOTAL_STEPS" "Registering New User"
    local body="{\"username\":\"$username\",\"password\":\"$password\",\"email\":\"${username}@test.com\",\"tenantId\":\"$tenant_id\"}"
    local response
    response=$(api_call "POST" "/api/v1/register" "$body" 200)
    
    if [ "$INTERACTIVE" == "true" ]; then
        if ! wait_for_input "Press Y to continue... "; then return 1; fi
    fi
    
    if ! is_success "$response"; then
        print_error "Registration failed"
        return 1
    fi
    
    TEST_USER_ID=$(json_value "$response" "_id")
    TEST_TENANT_ID=$tenant_id
    print_success "User registered with ID: $TEST_USER_ID"
    
    # Step 2: Login with correct credentials
    print_step "2" "$TOTAL_STEPS" "Logging In with Correct Credentials"
    body="{\"username\":\"$username\",\"password\":\"$password\"}"
    response=$(api_call "POST" "/api/v1/login" "$body" 200)
    
    if [ "$INTERACTIVE" == "true" ]; then
        if ! wait_for_input "Press Y to continue... "; then return 1; fi
    fi
    
    if ! is_success "$response"; then
        print_error "Login with correct credentials failed"
        return 1
    fi
    
    TEST_TOKEN=$(json_value "$response" "token")
    print_success "Login successful, token obtained"
    
    # Step 3: Login with wrong password
    print_step "3" "$TOTAL_STEPS" "Logging In with Wrong Password (should fail)"
    body="{\"username\":\"$username\",\"password\":\"wrongpassword\"}"
    response=$(api_call "POST" "/api/v1/login" "$body" 401)
    
    if [ "$INTERACTIVE" == "true" ]; then
        if ! wait_for_input "Press Y to continue... "; then return 1; fi
    fi
    
    if ! is_success "$response"; then
        print_success "Wrong password correctly rejected"
    else
        print_error "Wrong password should have been rejected"
        return 1
    fi
    
    # Step 4: Access protected endpoint without token
    print_step "4" "$TOTAL_STEPS" "Accessing Protected Endpoint Without Token (should fail)"
    
    # Temporarily clear token
    local saved_token="$TEST_TOKEN"
    TEST_TOKEN=""
    response=$(api_call "GET" "/api/v1/collections" "" 401)
    TEST_TOKEN="$saved_token"
    
    if [ "$INTERACTIVE" == "true" ]; then
        if ! wait_for_input "Press Y to continue... "; then return 1; fi
    fi
    
    if ! is_success "$response"; then
        print_success "Unauthenticated access correctly rejected"
    else
        print_error "Unauthenticated access should have been rejected"
        return 1
    fi
    
    # Step 5: Access protected endpoint with token
    print_step "5" "$TOTAL_STEPS" "Accessing Protected Endpoint With Token"
    response=$(api_call "GET" "/api/v1/collections" "" 200)
    
    if [ "$INTERACTIVE" == "true" ]; then
        if ! wait_for_input "Press Y to continue... "; then return 1; fi
    fi
    
    if is_success "$response"; then
        print_success "Authenticated access successful"
    else
        print_error "Authenticated access failed"
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
