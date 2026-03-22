#!/bin/bash

# =============================================================================
# AIDB Interactive Test Runner
# =============================================================================
# This script provides an interactive interface to browse, search, and run tests
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
NC='\033[0m'
BOLD='\033[1m'

# Global variables
declare -a TEST_FILES
declare -a TEST_NAMES
declare -a TEST_DESCRIPTIONS
SEARCH_QUERY=""

# Clear screen and show header
show_header() {
    clear
    echo -e "${BOLD}${CYAN}══════════════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${BOLD}${WHITE}                    AIDB Interactive Test Runner${NC}"
    echo -e "${BOLD}${CYAN}══════════════════════════════════════════════════════════════════════════════${NC}"
    echo ""
}

# Load all tests
load_tests() {
    local files=$(find "${SCRIPT_DIR}/scripts" -name "test_*.sh" -type f | sort)
    local i=0
    
    for file in $files; do
        TEST_FILES[$i]="$file"
        TEST_NAMES[$i]=$(basename "$file" .sh)
        
        # Extract description from file
        local desc=$(grep -m1 "^# Test:" "$file" | sed 's/# Test://')
        if [ -z "$desc" ]; then
            desc=$(grep -m1 "^# ====" "$file" | head -1)
        fi
        TEST_DESCRIPTIONS[$i]="${desc:-No description available}"
        
        i=$((i + 1))
    done
}

# Display test list (all on one page)
display_tests() {
    show_header
    
    local total_tests=${#TEST_NAMES[@]}
    
    # Show search status
    if [ -n "$SEARCH_QUERY" ]; then
        echo -e "${YELLOW}Search: \"$SEARCH_QUERY\"${NC}"
        echo ""
    fi
    
    echo -e "${WHITE}Available Tests (${total_tests} total)${NC}"
    echo -e "${CYAN}────────────────────────────────────────────────────────────────────────────${NC}"
    
    local num=1
    local displayed=0
    for ((i=0; i<total_tests; i++)); do
        local name="${TEST_NAMES[$i]}"
        local desc="${TEST_DESCRIPTIONS[$i]}"
        
        # Filter by search if query exists
        if [ -n "$SEARCH_QUERY" ]; then
            if [[ ! "${name,,}" == *"${SEARCH_QUERY,,}"* ]] && [[ ! "${desc,,}" == *"${SEARCH_QUERY,,}"* ]]; then
                continue
            fi
        fi
        
        printf "${GREEN}%3d${NC}. ${BOLD}${WHITE}%-35s${NC} %s\n" "$num" "$name" "${desc:0:45}"
        num=$((num + 1))
        displayed=$((displayed + 1))
    done
    
    if [ $displayed -eq 0 ]; then
        echo -e "${YELLOW}No tests found matching your search.${NC}"
    fi
    
    echo ""
    echo -e "${CYAN}────────────────────────────────────────────────────────────────────────────${NC}"
    echo -e "Showing ${displayed} of ${total_tests} tests"
    echo ""
    echo -e "${WHITE}Commands:${NC}"
    echo -e "  ${GREEN}[number]${NC}  Run test by number (with verbose output)"
    echo -e "  ${GREEN}q[number]${NC}  Run test by number (quiet - only show queries/responses)"
    echo -e "  ${GREEN}s/search${NC} Search tests"
    echo -e "  ${GREEN}c/clear${NC}  Clear search"
    echo -e "  ${GREEN}a/all${NC}    Run all tests"
    echo -e "  ${GREEN}r/refresh${NC} Refresh list"
    echo -e "  ${GREEN}x/quit${NC}   Exit"
    echo ""
    echo -e -n "${YELLOW}Enter command: ${NC}"
}

# Run a single test interactively
run_test_interactive() {
    local test_index=$1
    local quiet_mode=${2:-false}
    
    # Convert display number to actual array index
    local actual_index=$((test_index - 1))
    
    # Account for filtered tests if searching
    if [ -n "$SEARCH_QUERY" ]; then
        local count=0
        for ((i=0; i<${#TEST_NAMES[@]}; i++)); do
            local name="${TEST_NAMES[$i]}"
            local desc="${TEST_DESCRIPTIONS[$i]}"
            if [[ "${name,,}" == *"${SEARCH_QUERY,,}"* ]] || [[ "${desc,,}" == *"${SEARCH_QUERY,,}"* ]]; then
                count=$((count + 1))
                if [ $count -eq $test_index ]; then
                    actual_index=$i
                    break
                fi
            fi
        done
    fi
    
    if [ $actual_index -lt 0 ] || [ $actual_index -ge ${#TEST_FILES[@]} ]; then
        echo -e "${RED}Invalid test number${NC}"
        sleep 1
        return
    fi
    
    local test_file="${TEST_FILES[$actual_index]}"
    local test_name="${TEST_NAMES[$actual_index]}"
    
    show_header
    echo -e "${BOLD}${WHITE}Running Test: ${test_name}${NC}"
    echo -e "${CYAN}────────────────────────────────────────────────────────────────────────────${NC}"
    echo ""
    
    # Show test file path
    echo -e "${MAGENTA}Test File:${NC} $test_file"
    echo ""
    
    # Show test steps
    echo -e "${WHITE}Test Steps:${NC}"
    grep -E "^#.*Step|^#   [0-9]" "$test_file" | sed 's/^#/  /' | head -15
    echo ""
    echo -e "${CYAN}────────────────────────────────────────────────────────────────────────────${NC}"
    echo ""
    
    if [ "$quiet_mode" = "true" ]; then
        echo -e "${YELLOW}Running in QUIET mode - showing only queries/responses...${NC}"
        echo ""
        # Run the test with verbose output (shows all curl commands)
        VERBOSE=true bash "$test_file"
    else
        echo -e "${YELLOW}Press Y to start the test (each step will require confirmation)...${NC}"
        echo -e "${YELLOW}Press Q to run in quiet mode (only show queries/responses)${NC}"
        echo -e "${YELLOW}Press any other key to go back${NC}"
        read -r response
        
        if [[ "$response" =~ ^[Qq]$ ]]; then
            quiet_mode=true
        elif [[ ! "$response" =~ ^[Yy]$ ]]; then
            return
        fi
        
        if [ "$quiet_mode" = "true" ]; then
            # Run the test with verbose output (shows all curl commands)
            VERBOSE=true bash "$test_file"
        else
            # Run the test in interactive mode
            INTERACTIVE=true bash "$test_file"
        fi
    fi
    
    local exit_code=$?
    
    echo ""
    echo -e "${CYAN}────────────────────────────────────────────────────────────────────────────${NC}"
    if [ $exit_code -eq 0 ]; then
        echo -e "${BOLD}${GREEN}✓ Test Completed Successfully${NC}"
    else
        echo -e "${BOLD}${RED}✗ Test Failed${NC}"
    fi
    echo ""
    echo -e "${YELLOW}Press ENTER to return to test list...${NC}"
    read -r
}

# Run all tests
run_all_tests() {
    show_header
    echo -e "${BOLD}${WHITE}Running All Tests...${NC}"
    echo -e "${CYAN}────────────────────────────────────────────────────────────────────────────${NC}"
    echo ""
    
    bash "${SCRIPT_DIR}/testall.sh" -v
    
    echo ""
    echo -e "${YELLOW}Press ENTER to return to test list...${NC}"
    read -r
}

# Search tests
search_tests() {
    echo -e -n "${YELLOW}Enter search query: ${NC}"
    read -r SEARCH_QUERY
}

# Main loop
main() {
    # Check server
    show_header
    if ! check_server; then
        echo -e "${RED}Server is not running. Please start the server first.${NC}"
        echo ""
        echo -e "${YELLOW}Press ENTER to exit...${NC}"
        read -r
        exit 1
    fi
    
    load_tests
    
    while true; do
        display_tests
        read -r command
        
        case "$command" in
            [0-9]*)
                run_test_interactive "$command" "false"
                ;;
            q[0-9]*|Q[0-9]*)
                # Quiet mode - extract number after 'q'
                local num="${command:1}"
                run_test_interactive "$num" "true"
                ;;
            s|search|S|SEARCH)
                search_tests
                ;;
            c|clear|C|CLEAR)
                SEARCH_QUERY=""
                ;;
            a|all|A|ALL)
                run_all_tests
                ;;
            r|refresh|R|REFRESH)
                load_tests
                ;;
            x|quit|X|QUIT|exit|EXIT)
                clear
                echo -e "${GREEN}Goodbye!${NC}"
                exit 0
                ;;
            *)
                echo -e "${RED}Unknown command: $command${NC}"
                sleep 1
                ;;
        esac
    done
}

# Run main
main
