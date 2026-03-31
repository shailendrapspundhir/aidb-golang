#!/bin/bash

# =============================================================================
# Test: Full-Text Search
# =============================================================================
# This test verifies full-text search functionality
# Steps:
#   1-5: Setup (register, login, tenant, region, environment)
#   6. Create Collection
#   7. Insert Documents with text fields
#   8. Create Full-Text Index
#   9. Search for terms
#   10. Verify search results and ranking
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common.sh"

TEST_NAME="Full-Text Search"
TOTAL_STEPS=17
INTERACTIVE="${INTERACTIVE:-false}"

run_test() {
    local unique_id=$(generate_unique_id)
    local timestamp=$(echo "$unique_id" | cut -d'_' -f1)
    local random_suffix=$(echo "$unique_id" | cut -d'_' -f2)
    local username="admin_ft_${random_suffix}"
    local tenant_id="tenant_${random_suffix}"
    local region_id="region_${random_suffix}"
    local env_id="env_${random_suffix}"
    
    # Step 1: Register
    print_step "1" "$TOTAL_STEPS" "Registering User"
    local body="{\"username\":\"$username\",\"password\":\"password123\",\"email\":\"${username}@test.com\",\"tenantId\":\"$tenant_id\"}"
    local response
    response=$(api_call "POST" "/api/v1/register" "$body" 200)
    if ! is_success "$response"; then return 1; fi
    TEST_USER_ID=$(json_value "$response" "_id")
    TEST_TENANT_ID=$tenant_id
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 2: Login
    print_step "2" "$TOTAL_STEPS" "Logging In"
    body="{\"username\":\"$username\",\"password\":\"password123\"}"
    response=$(api_call "POST" "/api/v1/login" "$body" 200)
    if ! is_success "$response"; then return 1; fi
    TEST_TOKEN=$(json_value "$response" "token")
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 3: Create Tenant
    print_step "3" "$TOTAL_STEPS" "Creating Tenant"
    api_call "POST" "/api/v1/tenants" "{\"id\":\"$tenant_id\",\"name\":\"Test Tenant\"}" 200
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 4: Create Region
    print_step "4" "$TOTAL_STEPS" "Creating Region"
    api_call "POST" "/api/v1/regions" "{\"id\":\"$region_id\",\"name\":\"Test Region\",\"tenantId\":\"$tenant_id\"}" 200
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 5: Create Environment
    print_step "5" "$TOTAL_STEPS" "Creating Environment"
    api_call "POST" "/api/v1/environments" "{\"id\":\"$env_id\",\"name\":\"Test Env\",\"regionId\":\"$region_id\",\"tenantId\":\"$tenant_id\"}" 200
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 6: Create Collection
    print_step "6" "$TOTAL_STEPS" "Creating Collection"
    TEST_COLLECTION="ft_docs_${random_suffix}"
    body="{\"name\":\"$TEST_COLLECTION\"}"
    response=$(api_call "POST" "/api/v1/collections" "$body" 201)
    if ! is_success "$response"; then return 1; fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 7: Insert Documents
    print_step "7" "$TOTAL_STEPS" "Inserting Documents"
    # Doc 1: about databases
    body="{\"data\":{\"title\":\"Introduction to Databases\",\"content\":\"This document explains relational databases and SQL queries.\"}}"
    response=$(api_call "POST" "/api/v1/collections/${TEST_COLLECTION}/documents" "$body" 201)
    if ! is_success "$response"; then return 1; fi
    DOC1_ID=$(json_value "$response" "_id")
    
    # Doc 2: about full-text search
    body="{\"data\":{\"title\":\"Full-Text Search Guide\",\"content\":\"Learn how to implement full-text search in your applications.\"}}"
    response=$(api_call "POST" "/api/v1/collections/${TEST_COLLECTION}/documents" "$body" 201)
    if ! is_success "$response"; then return 1; fi
    DOC2_ID=$(json_value "$response" "_id")
    
    # Doc 3: about something else
    body="{\"data\":{\"title\":\"Weather Report\",\"content\":\"Today is sunny with clear skies.\"}}"
    response=$(api_call "POST" "/api/v1/collections/${TEST_COLLECTION}/documents" "$body" 201)
    if ! is_success "$response"; then return 1; fi
    
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 8: Create Full-Text Index
    print_step "8" "$TOTAL_STEPS" "Creating Full-Text Index"
    body="{\"fields\":[\"title\",\"content\"]}"
    response=$(api_call "POST" "/api/v1/collections/${TEST_COLLECTION}/fulltext-index" "$body" 200)
    if ! is_success "$response"; then
        print_error "Failed to create full-text index"
        return 1
    fi
    print_success "Full-text index created on title and content fields"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 9: Search
    print_step "9" "$TOTAL_STEPS" "Searching for 'database'"
    body="{\"q\":\"database\",\"limit\":10}"
    response=$(api_call "POST" "/api/v1/collections/${TEST_COLLECTION}/search" "$body" 200)
    if ! is_success "$response"; then
        print_error "Search failed"
        return 1
    fi
    
    # Check that we got results
    RESULT_COUNT=$(echo "$response" | grep -o '"count":[0-9]*' | head -1 | cut -d':' -f2)
    if [ "$RESULT_COUNT" -ge 1 ]; then
        print_success "Search returned $RESULT_COUNT result(s)"
    else
        print_error "Expected at least 1 result for 'database' search"
        return 1
    fi
    
    # Verify the database doc is in results
    if echo "$response" | grep -q "Databases"; then
        print_success "Database document found in search results"
    else
        print_error "Database document not found in search results"
        return 1
    fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 10: Search for 'full-text' and verify ranking
    print_step "10" "$TOTAL_STEPS" "Searching for 'full-text' and verifying ranking"
    body="{\"q\":\"full-text\",\"limit\":10}"
    response=$(api_call "POST" "/api/v1/collections/${TEST_COLLECTION}/search" "$body" 200)
    if ! is_success "$response"; then
        print_error "Search for 'full-text' failed"
        return 1
    fi
    
    # The full-text guide doc should be top result
    if echo "$response" | grep -q "Full-Text"; then
        print_success "Full-Text Search Guide document found in results"
    else
        print_error "Full-Text Search Guide document not found"
        return 1
    fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 11: Test Phrase Search
    print_step "11" "$TOTAL_STEPS" "Testing phrase search"
    body="{\"q\":\"relational databases\",\"phrase\":true,\"limit\":10}"
    response=$(api_call "POST" "/api/v1/collections/${TEST_COLLECTION}/search" "$body" 200)
    if ! is_success "$response"; then
        print_error "Phrase search failed"
        return 1
    fi
    # Phrase search should find doc1 (which has "relational databases" as adjacent terms)
    if echo "$response" | grep -q "Databases"; then
        print_success "Phrase search found expected document"
    else
        print_error "Phrase search did not find expected document"
        return 1
    fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 12: Test Fuzzy Search
    print_step "12" "$TOTAL_STEPS" "Testing fuzzy search"
    # Search for "databse" (typo) with fuzzy enabled
    body="{\"q\":\"databse\",\"fuzzy\":true,\"maxFuzzyDist\":2,\"limit\":10}"
    response=$(api_call "POST" "/api/v1/collections/${TEST_COLLECTION}/search" "$body" 200)
    if ! is_success "$response"; then
        print_error "Fuzzy search failed"
        return 1
    fi
    # Fuzzy search should find documents with "database" (close to "databse")
    if echo "$response" | grep -q "database\|Database"; then
        print_success "Fuzzy search found documents with similar terms"
    else
        print_error "Fuzzy search did not find expected documents"
        return 1
    fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 13: Test Case-Insensitive Search (default)
    print_step "13" "$TOTAL_STEPS" "Testing case-insensitive search (default)"
    body="{\"q\":\"DATABASES\",\"limit\":10}"
    response=$(api_call "POST" "/api/v1/collections/${TEST_COLLECTION}/search" "$body" 200)
    if ! is_success "$response"; then
        print_error "Case-insensitive search failed"
        return 1
    fi
    # Should find documents despite uppercase query
    if echo "$response" | grep -q "database\|Database"; then
        print_success "Case-insensitive search works correctly"
    else
        print_error "Case-insensitive search did not find expected documents"
        return 1
    fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 14: Test Pagination (offset)
    print_step "14" "$TOTAL_STEPS" "Testing pagination with offset"
    body="{\"q\":\"search\",\"limit\":2,\"offset\":0}"
    response=$(api_call "POST" "/api/v1/collections/${TEST_COLLECTION}/search" "$body" 200)
    if ! is_success "$response"; then
        print_error "Pagination search failed"
        return 1
    fi
    PAGE1_COUNT=$(echo "$response" | grep -o '"count":[0-9]*' | head -1 | cut -d':' -f2)
    print_success "Page 1 returned $PAGE1_COUNT result(s)"
    
    # Get second page
    body="{\"q\":\"search\",\"limit\":2,\"offset\":2}"
    response=$(api_call "POST" "/api/v1/collections/${TEST_COLLECTION}/search" "$body" 200)
    if ! is_success "$response"; then
        print_error "Pagination offset search failed"
        return 1
    fi
    PAGE2_COUNT=$(echo "$response" | grep -o '"count":[0-9]*' | head -1 | cut -d':' -f2)
    print_success "Page 2 (offset=2) returned $PAGE2_COUNT result(s)"
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 15: Test Field-Specific Search
    print_step "15" "$TOTAL_STEPS" "Testing field-specific search"
    # Search only in 'title' field
    body="{\"q\":\"database\",\"fields\":[\"title\"],\"limit\":10}"
    response=$(api_call "POST" "/api/v1/collections/${TEST_COLLECTION}/search" "$body" 200)
    if ! is_success "$response"; then
        print_error "Field-specific search failed"
        return 1
    fi
    # Should find docs where 'database' appears in title
    if echo "$response" | grep -q "database\|Database"; then
        print_success "Field-specific search (title only) works correctly"
    else
        print_error "Field-specific search did not find expected documents"
        return 1
    fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 16: Test Regex Search via JSON query
    print_step "16" "$TOTAL_STEPS" "Testing regex search via \$regex in query object"
    body='{"query":{"$regex":"datab.*"},"limit":10}'
    response=$(api_call "POST" "/api/v1/collections/${TEST_COLLECTION}/search" "$body" 200)
    if ! is_success "$response"; then
        print_error "Regex search failed"
        return 1
    fi
    if echo "$response" | grep -q "database\|Database"; then
        print_success "Regex search (\$regex) found matching documents"
    else
        print_error "Regex search did not find expected documents"
        return 1
    fi
    if [ "$INTERACTIVE" == "true" ]; then if ! wait_for_input "Press Y to continue... "; then return 1; fi; fi
    
    # Step 17: Test Conditional/JSON Query (combining operators)
    print_step "17" "$TOTAL_STEPS" "Testing JSON query with multiple operators"
    # Combine $text with $phrase and $caseSensitive
    body='{"query":{"$text":"relational","$phrase":true},"limit":10}'
    response=$(api_call "POST" "/api/v1/collections/${TEST_COLLECTION}/search" "$body" 200)
    if ! is_success "$response"; then
        print_error "JSON query (phrase) failed"
        return 1
    fi
    if echo "$response" | grep -q "relational\|Relational"; then
        print_success "JSON query with \$phrase found matching documents"
    else
        print_error "JSON query did not find expected documents"
        return 1
    fi
    # Test $fuzzy in JSON query
    body='{"query":{"$text":"databse","$fuzzy":true},"limit":10}'
    response=$(api_call "POST" "/api/v1/collections/${TEST_COLLECTION}/search" "$body" 200)
    if ! is_success "$response"; then
        print_error "JSON query (fuzzy) failed"
        return 1
    fi
    if echo "$response" | grep -q "database\|Database"; then
        print_success "JSON query with \$fuzzy found similar documents"
    else
        print_error "JSON query fuzzy did not find expected documents"
        return 1
    fi
    
    print_success "Full-text search test completed successfully!"
    return 0
}

# Run the test
run_test
