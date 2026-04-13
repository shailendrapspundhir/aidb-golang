#!/bin/bash

# =============================================================================
# Test: Query Statistics and Explain
# =============================================================================
# This test verifies query statistics collection and explain functionality
# Steps:
#   1. Setup (auth, tenant, etc.)
#   2. Create collection
#   3. Insert documents
#   4. Create index
#   5. Run queries (records stats)
#   6. Test includeStats parameter
#   7. Test explain endpoint
#   8. Test query-stats endpoint
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common.sh"

TEST_NAME="Query Statistics, Explain, and Auto-Indexing"
TOTAL_STEPS=11
INTERACTIVE="${INTERACTIVE:-false}"

run_test() {
    local timestamp=$(date +%s)
    local username="admin_stats_test_${timestamp}"
    local tenant_id="tenant_stats_${timestamp}"
    
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
    api_call "POST" "/api/v1/tenants" "{\"id\":\"$tenant_id\",\"name\":\"Stats Test Tenant\"}" 200
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "4" "$TOTAL_STEPS" "Creating Collection"
    TEST_COLLECTION="stats_test_${timestamp}"
    body="{\"name\":\"$TEST_COLLECTION\"}"
    response=$(api_call "POST" "/api/v1/collections" "$body" 201)
    if ! is_success "$response"; then return 1; fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "5" "$TOTAL_STEPS" "Inserting Test Documents"
    for i in 1 2 3; do
        body="{\"data\":{\"name\":\"User$i\",\"age\":$((20 + i))}}"
        response=$(api_call "POST" "/api/v1/collections/${TEST_COLLECTION}/documents" "$body" 201)
        if ! is_success "$response"; then return 1; fi
    done
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "6" "$TOTAL_STEPS" "Creating Index on 'name'"
    body="{\"field\":\"name\",\"type\":\"btree\"}"
    response=$(api_call "POST" "/api/v1/collections/${TEST_COLLECTION}/indexes" "$body" 200)
    if ! is_success "$response"; then return 1; fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "7" "$TOTAL_STEPS" "Running Query (records stats)"
    response=$(api_call "GET" "/api/v1/collections/${TEST_COLLECTION}/documents?name=User1" "" 200)
    if ! is_success "$response"; then return 1; fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "8" "$TOTAL_STEPS" "Running Query with includeStats=true"
    response=$(api_call "GET" "/api/v1/collections/${TEST_COLLECTION}/documents?name=User2&includeStats=true" "" 200)
    if ! is_success "$response"; then return 1; fi
    # Check if stats field is present
    if echo "$response" | grep -q '"stats"'; then
        print_success "Stats field present in response"
    else
        print_error "Stats field not present in response"
        return 1
    fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "9" "$TOTAL_STEPS" "Testing Explain Endpoint"
    response=$(api_call "GET" "/api/v1/collections/${TEST_COLLECTION}/explain?name=User1" "" 200)
    if ! is_success "$response"; then return 1; fi
    # Check if plan field is present
    if echo "$response" | grep -q '"plan"'; then
        print_success "Explain plan returned"
    else
        print_error "Explain plan not returned"
        return 1
    fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "10" "$TOTAL_STEPS" "Testing Query Stats Endpoint"
    response=$(api_call "GET" "/api/v1/collections/${TEST_COLLECTION}/query-stats" "" 200)
    if ! is_success "$response"; then return 1; fi
    # Check for summary and recent fields
    if echo "$response" | grep -q '"summary"' && echo "$response" | grep -q '"recent"'; then
        print_success "Query stats returned"
    else
        print_error "Query stats not properly returned"
        return 1
    fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    print_step "11" "$TOTAL_STEPS" "Testing Auto-Index Recommendations Endpoint"
    response=$(api_call "GET" "/api/v1/collections/${TEST_COLLECTION}/auto-index/recommendations" "" 200)
    if ! is_success "$response"; then return 1; fi
    # Check for recommendations field
    if echo "$response" | grep -q '"recommendations"'; then
        print_success "Auto-index recommendations returned"
    else
        print_error "Auto-index recommendations not returned"
        return 1
    fi
    # Print recommendations for visibility
    echo "$response" | python3 -c "
import sys,json
data=json.load(sys.stdin)
recs=data.get('data',{}).get('recommendations',[])
print(f'  Recommendations count: {len(recs)}')
for r in recs[:3]:
    print(f'    - {r.get(\"fields\",[])}: score={r.get(\"score\",0):.2f}, {r.get(\"reason\",\"\")[:60]}...')
" 2>/dev/null || echo "  (Recommendations parsed)"
    
    print_success "All Query Statistics, Explain, and Auto-Index tests passed!"
    return 0
}

# Run the test
run_test
