#!/bin/bash

# =============================================================================
# Test: Aggregation Pipeline
# =============================================================================
# This test verifies MongoDB-like aggregation pipeline functionality
# Tests: $match, $group, $sort, $project, $limit, $skip, $count, $addFields,
#        $sortByCount, $avg, distinct, stats, swagger
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common.sh"

TEST_NAME="Aggregation Pipeline"
TOTAL_STEPS=20
INTERACTIVE="${INTERACTIVE:-false}"

run_test() {
    local timestamp=$(date +%s)
    local username="admin_agg_${timestamp}"
    local tenant_id="tenant_${timestamp}"
    local collection="agg_test_${timestamp}"
    
    # Step 1: Register
    print_step "1" "$TOTAL_STEPS" "Registering User (with admin privileges)"
    local body="{\"username\":\"$username\",\"password\":\"password123\",\"email\":\"${username}@test.com\",\"tenantId\":\"$tenant_id\"}"
    local response
    response=$(api_call "POST" "/api/v1/register" "$body" 200)
    if ! is_success "$response"; then return 1; fi
    TEST_USER_ID=$(json_value "$response" "_id")
    TEST_TENANT_ID=$tenant_id
    
    # Step 2: Login
    print_step "2" "$TOTAL_STEPS" "Logging In"
    body="{\"username\":\"$username\",\"password\":\"password123\"}"
    response=$(api_call "POST" "/api/v1/login" "$body" 200)
    if ! is_success "$response"; then return 1; fi
    TEST_TOKEN=$(json_value "$response" "token")
    
    # Step 3: Create Tenant
    print_step "3" "$TOTAL_STEPS" "Creating Tenant"
    api_call "POST" "/api/v1/tenants" "{\"id\":\"$tenant_id\",\"name\":\"Test Tenant\"}" 200
    
    # Step 4: Create Region
    print_step "4" "$TOTAL_STEPS" "Creating Region"
    api_call "POST" "/api/v1/regions" "{\"id\":\"region_${timestamp}\",\"name\":\"Test Region\",\"tenantId\":\"$tenant_id\"}" 200
    
    # Step 5: Create Environment
    print_step "5" "$TOTAL_STEPS" "Creating Environment"
    api_call "POST" "/api/v1/environments" "{\"id\":\"env_${timestamp}\",\"name\":\"Test Env\",\"regionId\":\"region_${timestamp}\",\"tenantId\":\"$tenant_id\"}" 200
    
    # Step 6: Create Collection
    print_step "6" "$TOTAL_STEPS" "Creating Document Collection"
    response=$(api_call "POST" "/api/v1/collections" "{\"name\":\"$collection\"}" 201)
    if ! is_success "$response"; then return 1; fi
    
    # Step 7: Insert test documents
    print_step "7" "$TOTAL_STEPS" "Inserting test documents"
    for i in 1 2 3 4 5; do
        local category="electronics"
        if [ $i -gt 2 ]; then category="clothing"; fi
        if [ $i -gt 4 ]; then category="books"; fi
        api_call_silent "POST" "/api/v1/collections/$collection/documents" "{\"data\": {\"name\": \"Product $i\", \"price\": $((i * 100)), \"category\": \"$category\", \"quantity\": $i}}"
    done
    print_success "Inserted 5 test documents"
    
    # Step 8: Test $match
    print_step "8" "$TOTAL_STEPS" "Testing \$match stage"
    response=$(api_call "POST" "/api/v1/collections/$collection/aggregate" '{"pipeline": [{"$match": {"category": "electronics"}}]}' 200)
    if ! is_success "$response"; then return 1; fi
    if ! echo "$response" | grep -q "Product 1"; then print_error "Expected Product 1 in results"; return 1; fi
    
    # Step 9: Test $group with $sum
    print_step "9" "$TOTAL_STEPS" "Testing \$group with \$sum"
    response=$(api_call "POST" "/api/v1/collections/$collection/aggregate" '{"pipeline": [{"$group": {"_id": "$category", "totalPrice": {"$sum": "$price"}, "count": {"$sum": 1}}} ]}' 200)
    if ! is_success "$response"; then return 1; fi
    if ! echo "$response" | grep -q "totalPrice"; then print_error "Expected totalPrice in results"; return 1; fi
    
    # Step 10: Test $sort
    print_step "10" "$TOTAL_STEPS" "Testing \$sort stage"
    response=$(api_call "POST" "/api/v1/collections/$collection/aggregate" '{"pipeline": [{"$sort": {"price": -1}}, {"$limit": 2}]}' 200)
    if ! is_success "$response"; then return 1; fi
    if ! echo "$response" | grep -q "Product 5"; then print_error "Expected Product 5 in results"; return 1; fi
    
    # Step 11: Test $project
    print_step "11" "$TOTAL_STEPS" "Testing \$project stage"
    response=$(api_call "POST" "/api/v1/collections/$collection/aggregate" '{"pipeline": [{"$project": {"name": 1, "category": 1}}]}' 200)
    if ! is_success "$response"; then return 1; fi
    
    # Step 12: Test $limit and $skip
    print_step "12" "$TOTAL_STEPS" "Testing \$limit and \$skip"
    response=$(api_call "POST" "/api/v1/collections/$collection/aggregate" '{"pipeline": [{"$sort": {"price": 1}}, {"$skip": 2}, {"$limit": 2}]}' 200)
    if ! is_success "$response"; then return 1; fi
    
    # Step 13: Test $count
    print_step "13" "$TOTAL_STEPS" "Testing \$count stage"
    response=$(api_call "POST" "/api/v1/collections/$collection/aggregate" '{"pipeline": [{"$match": {"category": "electronics"}}, {"$count": "total"}]}' 200)
    if ! is_success "$response"; then return 1; fi
    if ! echo "$response" | grep -q '"total"'; then print_error "Expected total count in results"; return 1; fi
    
    # Step 14: Test $addFields
    print_step "14" "$TOTAL_STEPS" "Testing \$addFields stage"
    response=$(api_call "POST" "/api/v1/collections/$collection/aggregate" '{"pipeline": [{"$limit": 1}, {"$addFields": {"doublePrice": {"$multiply": ["$price", 2]}}}]}' 200)
    if ! is_success "$response"; then return 1; fi
    if ! echo "$response" | grep -q "doublePrice"; then print_error "Expected doublePrice in results"; return 1; fi
    
    # Step 15: Test $sortByCount
    print_step "15" "$TOTAL_STEPS" "Testing \$sortByCount stage"
    response=$(api_call "POST" "/api/v1/collections/$collection/aggregate" '{"pipeline": [{"$sortByCount": "$category"}]}' 200)
    if ! is_success "$response"; then return 1; fi
    
    # Step 16: Test $group with $avg
    print_step "16" "$TOTAL_STEPS" "Testing \$group with \$avg"
    response=$(api_call "POST" "/api/v1/collections/$collection/aggregate" '{"pipeline": [{"$group": {"_id": "$category", "avgPrice": {"$avg": "$price"}}}]}' 200)
    if ! is_success "$response"; then return 1; fi
    if ! echo "$response" | grep -q "avgPrice"; then print_error "Expected avgPrice in results"; return 1; fi
    
    # Step 17: Test distinct values
    print_step "17" "$TOTAL_STEPS" "Testing distinct values endpoint"
    response=$(api_call "GET" "/api/v1/collections/$collection/distinct/category" "" 200)
    if ! is_success "$response"; then return 1; fi
    if ! echo "$response" | grep -q "electronics"; then print_error "Expected electronics in distinct values"; return 1; fi
    
    # Step 18: Test collection stats
    print_step "18" "$TOTAL_STEPS" "Testing collection stats endpoint"
    response=$(api_call "GET" "/api/v1/collections/$collection/stats" "" 200)
    if ! is_success "$response"; then return 1; fi
    
    # Step 19: Test swagger.json endpoint
    print_step "19" "$TOTAL_STEPS" "Testing swagger.json endpoint"
    response=$(curl -s "${BASE_URL}/api/v1/swagger.json")
    if ! echo "$response" | grep -q "openapi"; then print_error "Expected openapi in swagger.json"; return 1; fi
    if ! echo "$response" | grep -q "AIDB API"; then print_error "Expected AIDB API in swagger.json"; return 1; fi
    print_success "Swagger.json endpoint working"
    
    # Step 20: Test swagger UI endpoint
    print_step "20" "$TOTAL_STEPS" "Testing swagger UI endpoint"
    response=$(curl -s "${BASE_URL}/api/v1/docs")
    if ! echo "$response" | grep -q "swagger-ui"; then print_error "Expected swagger-ui in docs"; return 1; fi
    print_success "Swagger UI endpoint working"
    
    # Cleanup
    print_info "Cleaning up..."
    api_call_silent "DELETE" "/api/v1/collections/$collection"
    
    print_test_summary "$TEST_NAME" "PASS"
    return 0
}

# Run the test
run_test