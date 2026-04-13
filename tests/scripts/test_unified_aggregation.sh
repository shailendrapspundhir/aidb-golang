#!/bin/bash

# =============================================================================
# Test: Unified Aggregation with Vector Support
# =============================================================================
# This test verifies the aggregation pipeline with vector-specific stages
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common.sh"

TEST_NAME="Unified Aggregation with Vectors"
TOTAL_STEPS=10
INTERACTIVE="${INTERACTIVE:-false}"

run_test() {
    local timestamp=$(date +%s)
    local username="admin_agg_${timestamp}"
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
    local collection_name="agg_test_${timestamp}"
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
                \"brand\": {\"type\": \"string\", \"index\": true},
                \"embedding\": {\"type\": \"vector\", \"dimensions\": 4, \"distanceMetric\": \"cosine\"}
            }
        }
    }"
    response=$(api_call "POST" "/api/v2/collections" "$body" 201)
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    if ! is_success "$response"; then print_error "Failed to create unified collection"; return 1; fi
    print_success "Unified collection created"
    
    # Step 7: Insert documents
    print_step "7" "$TOTAL_STEPS" "Inserting test documents"
    # Electronics - Brand A
    body="{\"fields\":{\"name\":\"Laptop A\",\"description\":\"High performance laptop for gaming\",\"price\":1299.99,\"category\":\"electronics\",\"brand\":\"BrandA\",\"embedding\":[0.1,0.2,0.3,0.4]}}"
    api_call "POST" "/api/v2/collections/${collection_name}/documents" "$body" 201
    
    body="{\"fields\":{\"name\":\"Phone A\",\"description\":\"Smartphone with excellent camera\",\"price\":899.99,\"category\":\"electronics\",\"brand\":\"BrandA\",\"embedding\":[0.15,0.25,0.35,0.45]}}"
    api_call "POST" "/api/v2/collections/${collection_name}/documents" "$body" 201
    
    # Electronics - Brand B
    body="{\"fields\":{\"name\":\"Laptop B\",\"description\":\"Business laptop with long battery\",\"price\":1099.99,\"category\":\"electronics\",\"brand\":\"BrandB\",\"embedding\":[0.2,0.3,0.4,0.5]}}"
    api_call "POST" "/api/v2/collections/${collection_name}/documents" "$body" 201
    
    body="{\"fields\":{\"name\":\"Phone B\",\"description\":\"Budget smartphone\",\"price\":499.99,\"category\":\"electronics\",\"brand\":\"BrandB\",\"embedding\":[0.25,0.35,0.45,0.55]}}"
    api_call "POST" "/api/v2/collections/${collection_name}/documents" "$body" 201
    
    # Clothing
    body="{\"fields\":{\"name\":\"T-Shirt\",\"description\":\"Cotton t-shirt comfortable fit\",\"price\":29.99,\"category\":\"clothing\",\"brand\":\"BrandC\",\"embedding\":[0.5,0.6,0.7,0.8]}}"
    api_call "POST" "/api/v2/collections/${collection_name}/documents" "$body" 201
    
    body="{\"fields\":{\"name\":\"Jeans\",\"description\":\"Denim jeans classic style\",\"price\":79.99,\"category\":\"clothing\",\"brand\":\"BrandC\",\"embedding\":[0.55,0.65,0.75,0.85]}}"
    api_call "POST" "/api/v2/collections/${collection_name}/documents" "$body" 201
    
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    print_success "Test documents inserted"
    
    # Step 8: Test $vectorMatch + $group aggregation
    print_step "8" "$TOTAL_STEPS" "Testing \$vectorMatch + \$group aggregation"
    body='{
        "pipeline": [
            {
                "$vectorMatch": {
                    "field": "embedding",
                    "vector": [0.1, 0.2, 0.3, 0.4],
                    "topK": 10,
                    "minScore": 0.5
                }
            },
            {
                "$group": {
                    "_id": "$category",
                    "avgPrice": {"$avg": "$price"},
                    "avgVectorScore": {"$avgVectorScore": 1},
                    "count": {"$sum": 1},
                    "topProduct": {"$first": "$name"}
                }
            },
            {"$sort": {"avgVectorScore": -1}}
        ]
    }'
    response=$(api_call "POST" "/api/v2/collections/${collection_name}/aggregate" "$body" 200)
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    if is_success "$response"; then
        print_success "Vector match + group aggregation succeeded"
    else
        print_error "Vector match + group aggregation failed"
        return 1
    fi
    
    # Step 9: Test $hybridSearch aggregation
    print_step "9" "$TOTAL_STEPS" "Testing \$hybridSearch aggregation"
    body='{
        "pipeline": [
            {
                "$hybridSearch": {
                    "vector": {"field": "embedding", "vector": [0.1, 0.2, 0.3, 0.4], "topK": 10},
                    "text": {"field": "description", "query": "laptop", "limit": 10},
                    "scoring": {"method": "weighted", "weights": {"vector": 0.6, "text": 0.4}}
                }
            },
            {"$limit": 5}
        ]
    }'
    response=$(api_call "POST" "/api/v2/collections/${collection_name}/aggregate" "$body" 200)
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    if is_success "$response"; then
        print_success "Hybrid search aggregation succeeded"
    else
        print_error "Hybrid search aggregation failed"
        return 1
    fi
    
    # Step 10: Test standard aggregation with $match and $group
    print_step "10" "$TOTAL_STEPS" "Testing standard aggregation with \$match and \$group"
    body='{
        "pipeline": [
            {
                "$match": {
                    "$eq": {"category": "electronics"}
                }
            },
            {
                "$group": {
                    "_id": "$brand",
                    "$sum": {"totalRevenue": "$price", "productCount": 1},
                    "$avg": {"avgPrice": "$price"},
                    "$push": {"products": "$name"}
                }
            },
            {"$sort": {"totalRevenue": -1}}
        ]
    }'
    response=$(api_call "POST" "/api/v2/collections/${collection_name}/aggregate" "$body" 200)
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    if is_success "$response"; then
        print_success "Standard aggregation succeeded"
    else
        print_error "Standard aggregation failed"
        return 1
    fi
    
    # Cleanup
    api_call "DELETE" "/api/v2/collections/${collection_name}" "" 200 > /dev/null 2>&1
    
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
