#!/bin/bash

# =============================================================================
# AIDB Test Runner
# =============================================================================
# This script runs all tests in the scripts directory
# Usage: ./testall.sh [--verbose] [--stop-on-fail]
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'
BOLD='\033[1m'

# Options
VERBOSE=false
STOP_ON_FAIL=false
INTERACTIVE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -s|--stop-on-fail)
            STOP_ON_FAIL=true
            shift
            ;;
        -i|--interactive)
            INTERACTIVE=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  -v, --verbose      Show detailed output"
            echo "  -s, --stop-on-fail Stop on first test failure"
            echo "  -i, --interactive  Run tests in interactive mode"
            echo "  -h, --help         Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Test counters
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0
SKIPPED_TESTS=0

# Arrays to track results
declare -a PASSED_LIST
declare -a FAILED_LIST

# Function to run a single test
run_test() {
    local test_file=$1
    local test_name=$(basename "$test_file" .sh)
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    echo -e "${CYAN}▶ Running: ${test_name}${NC}"
    
    local output
    local exit_code
    
    if [ "$VERBOSE" == "true" ]; then
        INTERACTIVE="$INTERACTIVE" bash "$test_file"
        exit_code=$?
    else
        output=$(INTERACTIVE="$INTERACTIVE" bash "$test_file" 2>&1)
        exit_code=$?
    fi
    
    if [ $exit_code -eq 0 ]; then
        echo -e "  ${GREEN}✓ PASSED${NC}"
        PASSED_TESTS=$((PASSED_TESTS + 1))
        PASSED_LIST+=("$test_name")
    else
        echo -e "  ${RED}✗ FAILED${NC}"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        FAILED_LIST+=("$test_name")
        
        if [ "$VERBOSE" == "false" ] && [ -n "$output" ]; then
            echo -e "  ${YELLOW}Output:${NC}"
            echo "$output" | sed 's/^/    /'
        fi
        
        if [ "$STOP_ON_FAIL" == "true" ]; then
            echo -e "\n${RED}Stopping on first failure${NC}"
            exit 1
        fi
    fi
    
    echo ""
}

# Main execution
echo ""
echo -e "${BOLD}${CYAN}═══════════════════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}${WHITE}  AIDB Test Suite${NC}"
echo -e "${BOLD}${CYAN}═══════════════════════════════════════════════════════════════════${NC}"
echo ""

# Check if server is running
if ! check_server; then
    echo -e "${RED}Server is not running. Please start the server first.${NC}"
    exit 1
fi

echo ""

# Find and run all tests
TEST_FILES=$(find "${SCRIPT_DIR}/scripts" -name "test_*.sh" -type f | sort)

if [ -z "$TEST_FILES" ]; then
    echo -e "${YELLOW}No test files found in ${SCRIPT_DIR}/scripts${NC}"
    exit 0
fi

# Run each test
for test_file in $TEST_FILES; do
    run_test "$test_file"
done

# Print summary
echo ""
echo -e "${BOLD}${CYAN}═══════════════════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}${WHITE}  Test Summary${NC}"
echo -e "${BOLD}${CYAN}═══════════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "  Total:   ${TOTAL_TESTS}"
echo -e "  ${GREEN}Passed:  ${PASSED_TESTS}${NC}"
echo -e "  ${RED}Failed:  ${FAILED_TESTS}${NC}"
echo ""

if [ ${#PASSED_LIST[@]} -gt 0 ]; then
    echo -e "${GREEN}Passed Tests:${NC}"
    for test in "${PASSED_LIST[@]}"; do
        echo -e "  ${GREEN}✓${NC} $test"
    done
    echo ""
fi

if [ ${#FAILED_LIST[@]} -gt 0 ]; then
    echo -e "${RED}Failed Tests:${NC}"
    for test in "${FAILED_LIST[@]}"; do
        echo -e "  ${RED}✗${NC} $test"
    done
    echo ""
fi

# Exit with appropriate code
if [ $FAILED_TESTS -gt 0 ]; then
    echo -e "${BOLD}${RED}Some tests failed!${NC}"
    exit 1
else
    echo -e "${BOLD}${GREEN}All tests passed!${NC}"
    exit 0
fi
