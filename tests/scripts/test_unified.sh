#!/bin/bash

# =============================================================================
# Test: Unified Collection Architecture
# =============================================================================
# This test verifies the unified collection functionality with vectors, 
# full-text search, and scalar data in a single collection.
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common.sh"

TEST_NAME="Unified Collection Architecture"
TOTAL_STEPS=15
INTERACTIVE="${INTERACTIVE:-false}"

run_test() {
    local timestamp=$(date +%s)
    local username="admin_unified_${timestamp}"
    local tenant_id="tenant_${timestamp}"
    
    print_header "$TEST_NAME"
    
    # Step 1: Register
    print_step "1" "$TOTAL_STEPS" "Registering User (with admin privileges)"
    local body="{\"username\":\"$username\",\"password\":\"password123\",\"email\":\"${username}@test.com\",\"tenantId\":\"$tenant_id\"}"
    local response
    response=$(api_call "POST" "/api/v1/register" "$body" 200)
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    if ! is_success "$response"; then print_error "Registration failed"; return 1; fi
    TEST_USER_ID=$(json_value "$response" "_id")
    TEST_TENANT_ID=$tenant_id
    
    # Step 2: Login
    print_step "2" "$TOTAL_STEPS" "Logging In"
    body="{\"username\":\"$username\",\"password\":\"password123\"}"
    response=$(api_call "POST" "/api/v1/login" "$body" 200)
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    if ! is_success "$response"; then print_error "Login failed"; return 1; fi
    TEST_TOKEN=$(json_value "$response" "token")
    
    # Step 3: Create Tenant (may already exist from registration)
    print_step "3" "$TOTAL_STEPS" "Creating Tenant"
    response=$(api_call "POST" "/api/v1/tenants" "{\"id\":\"$tenant_id\",\"name\":\"Test Tenant\"}" 200)
    # Tenant may already exist from registration, which is fine
    if ! is_success "$response"; then
        print_info "Tenant may already exist, continuing..."
    fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 4: Create Region
    print_step "4" "$TOTAL_STEPS" "Creating Region"
    api_call "POST" "/api/v1/regions" "{\"id\":\"region_${timestamp}\",\"name\":\"Test Region\",\"tenantId\":\"$tenant_id\"}" 200
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 5: Create Environment
    print_step "5" "$TOTAL_STEPS" "Creating Environment"
    api_call "POST" "/api/v1/environments" "{\"id\":\"env_${timestamp}\",\"name\":\"Test Env\",\"regionId\":\"region_${timestamp}\",\"tenantId\":\"$tenant_id\"}" 200
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 6: Create Unified Collection with mixed schema
    print_step "6" "$TOTAL_STEPS" "Creating Unified Collection with mixed schema"
    local collection_name="unified_test_${timestamp}"
    body="{
        \"name\": \"$collection_name\",
        \"schema\": {
            \"name\": \"$collection_name\",
            \"strict\": false,
            \"fields\": {
                \"name\": {\"type\": \"string\", \"required\": true},
                \"description\": {\"type\": \"fulltext\", \"analyzer\": \"standard\"},
                \"price\": {\"type\": \"number\", \"index\": true},
                \"category\": {\"type\": \"string\", \"index\": true},
                \"embedding\": {\"type\": \"vector\", \"dimensions\": 4, \"distanceMetric\": \"cosine\"}
            }
        }
    }"
    response=$(api_call "POST" "/api/v2/collections" "$body" 201)
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    if ! is_success "$response"; then print_error "Failed to create unified collection"; return 1; fi
    print_success "Unified collection created with vector, fulltext, and scalar fields"
    
    # Step 7: Insert document with all field types
    print_step "7" "$TOTAL_STEPS" "Inserting document with all field types"
    body="{
        \"fields\": {
            \"name\": \"Wireless Headphones\",
            \"description\": \"High-quality wireless headphones with active noise cancellation and premium sound quality\",
            \"price\": 299.99,
            \"category\": \"electronics\",
            \"embedding\": [0.1, 0.2, 0.3, 0.4]
        }
    }"
    response=$(api_call "POST" "/api/v2/collections/${collection_name}/documents" "$body" 201)
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    if ! is_success "$response"; then print_error "Failed to insert document"; return 1; fi
    local doc1_id=$(json_value "$response" "_id")
    print_success "Document inserted with ID: $doc1_id"
    
    # Step 8: Insert more documents
    print_step "8" "$TOTAL_STEPS" "Inserting additional documents"
    body="{
        \"fields\": {
            \"name\": \"Bluetooth Speaker\",
            \"description\": \"Portable bluetooth speaker with deep bass and long battery life\",
            \"price\": 149.99,
            \"category\": \"electronics\",
            \"embedding\": [0.15, 0.25, 0.35, 0.45]
        }
    }"
    response=$(api_call "POST" "/api/v2/collections/${collection_name}/documents" "$body" 201)
    
    body="{
        \"fields\": {
            \"name\": \"Running Shoes\",
            \"description\": \"Lightweight running shoes with excellent cushioning\",
            \"price\": 89.99,
            \"category\": \"sports\",
            \"embedding\": [0.5, 0.6, 0.7, 0.8]
        }
    }"
    response=$(api_call "POST" "/api/v2/collections/${collection_name}/documents" "$body" 201)
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    print_success "Additional documents inserted"
    
    # Step 9: Test filter query
    print_step "9" "$TOTAL_STEPS" "Testing filter query"
    # URL-encode the filter
    local encoded_filter=$(python3 -c "import urllib.parse; print(urllib.parse.quote('{\"category\":\"electronics\"}'))")
    response=$(api_call "GET" "/api/v2/collections/${collection_name}/documents?filter=${encoded_filter}" "" 200)
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    if is_success "$response"; then
        print_success "Filter query returned electronics documents"
    else
        print_error "Filter query failed"
        return 1
    fi
    
    # Step 10: Test vector search
    print_step "10" "$TOTAL_STEPS" "Testing vector similarity search"
    body="{
        \"vector\": [0.1, 0.2, 0.3, 0.4],
        \"field\": \"embedding\",
        \"topK\": 3,
        \"minScore\": 0.0
    }"
    response=$(api_call "POST" "/api/v2/collections/${collection_name}/search/vector" "$body" 200)
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    if is_success "$response"; then
        print_success "Vector search returned results"
    else
        print_error "Vector search failed"
        return 1
    fi
    
    # Step 11: Test text search
    print_step "11" "$TOTAL_STEPS" "Testing full-text search"
    body="{
        \"query\": \"headphones noise cancellation\",
        \"field\": \"description\",
        \"limit\": 5
    }"
    response=$(api_call "POST" "/api/v2/collections/${collection_name}/search/text" "$body" 200)
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    if is_success "$response"; then
        print_success "Text search returned results"
    else
        print_error "Text search failed"
        return 1
    fi
    
    # Step 12: Test unified query (hybrid search)
    print_step "12" "$TOTAL_STEPS" "Testing unified hybrid query"
    body="{
        \"filter\": {\"category\": \"electronics\"},
        \"vector\": [0.1, 0.2, 0.3, 0.4],
        \"vectorField\": \"embedding\",
        \"topK\": 5,
        \"limit\": 10
    }"
    response=$(api_call "POST" "/api/v2/collections/${collection_name}/query" "$body" 200)
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    if is_success "$response"; then
        print_success "Unified hybrid query returned results"
    else
        print_error "Unified query failed"
        return 1
    fi
    
    # Step 13: Test update document
    print_step "13" "$TOTAL_STEPS" "Testing document update"
    body="{
        \"fields\": {
            \"name\": \"Premium Wireless Headphones\",
            \"description\": \"High-quality wireless headphones with active noise cancellation\",
            \"price\": 349.99,
            \"category\": \"electronics\",
            \"embedding\": [0.1, 0.2, 0.3, 0.4]
        }
    }"
    response=$(api_call "PUT" "/api/v2/collections/${collection_name}/documents/${doc1_id}" "$body" 200)
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    if is_success "$response"; then
        print_success "Document updated successfully"
    else
        print_error "Document update failed"
        return 1
    fi
    
    # Step 14: Test collection stats
    print_step "14" "$TOTAL_STEPS" "Testing collection stats"
    response=$(api_call "GET" "/api/v2/collections/${collection_name}/stats" "" 200)
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    if is_success "$response"; then
        print_success "Collection stats retrieved"
    else
        print_error "Failed to get collection stats"
        return 1
    fi
    
    # Step 15: Drop collection
    print_step "15" "$TOTAL_STEPS" "Dropping unified collection"
    response=$(api_call "DELETE" "/api/v2/collections/${collection_name}" "" 200)
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    if is_success "$response"; then
        print_success "Collection dropped successfully"
    else
        print_error "Failed to drop collection"
        return 1
    fi
    
    return 0
}

# Run the test
if [ "$INTERACTIVE" == "true" ]; then
    source "${SCRIPT_DIR}/../interactive.sh"
    run_test_interactive "$TEST_NAME" run_test
else
    check_server || exit 1
    if run_test; then
        print_test_summary "$TEST_NAME" "PASS"
        exit 0
    else
        print_test_summary "$TEST_NAME" "FAIL"
        exit 1
    fi
fi
