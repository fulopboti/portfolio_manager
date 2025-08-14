#!/bin/bash
set -euo pipefail

# Portfolio Manager CI/CD Local Validation Script (Linux/macOS)
#
# This script runs the same quality checks that are executed in the GitHub Actions CI/CD pipeline.
# Use this to validate your code locally before pushing to ensure CI will pass.
#
# Usage:
#   ./scripts/ci-check.sh                    # Run all checks
#   ./scripts/ci-check.sh --skip-tests       # Skip tests
#   ./scripts/ci-check.sh --coverage         # Run with coverage
#   ./scripts/ci-check.sh --verbose          # Verbose output
#   ./scripts/ci-check.sh --help             # Show help

# Default options
SKIP_TESTS=false
COVERAGE=false
VERBOSE=false
HELP=false

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
GRAY='\033[0;37m'
NC='\033[0m' # No Color

# Output functions
print_success() {
    echo -e "✅ ${GREEN}$1${NC}"
}

print_error() {
    echo -e "❌ ${RED}$1${NC}"
}

print_info() {
    echo -e "ℹ️  ${CYAN}$1${NC}"
}

print_warning() {
    echo -e "⚠️  ${YELLOW}$1${NC}"
}

print_step() {
    echo -e "\n🔄 ${BLUE}$1${NC}"
    echo -e "${BLUE}$(printf '=%.0s' $(seq 1 $((${#1} + 3))))${NC}"
}

print_command() {
    echo -e "  ${GRAY}Running: $1${NC}"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to run command with error handling
run_check() {
    local cmd="$1"
    local description="$2"
    local ignore_errors="${3:-false}"
    
    print_command "$cmd"
    
    if [ "$VERBOSE" = true ]; then
        if eval "$cmd"; then
            print_success "$description"
            return 0
        else
            if [ "$ignore_errors" = false ]; then
                print_error "$description failed"
                return 1
            else
                print_warning "$description had issues (warnings only)"
                return 0
            fi
        fi
    else
        if output=$(eval "$cmd" 2>&1); then
            print_success "$description"
            return 0
        else
            if [ "$ignore_errors" = false ]; then
                print_error "$description failed:"
                echo "$output" | head -20
                return 1
            else
                print_warning "$description had issues (warnings only)"
                return 0
            fi
        fi
    fi
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-tests)
            SKIP_TESTS=true
            shift
            ;;
        --coverage)
            COVERAGE=true
            shift
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --help|-h)
            HELP=true
            shift
            ;;
        *)
            echo "Unknown option $1"
            HELP=true
            shift
            ;;
    esac
done

# Show help
if [ "$HELP" = true ]; then
    cat << 'EOF'
Portfolio Manager CI/CD Local Validation Script

USAGE:
    ./scripts/ci-check.sh [OPTIONS]

OPTIONS:
    --skip-tests     Skip running the test suite
    --coverage       Run tests with coverage reporting  
    --verbose        Enable verbose output for debugging
    --help, -h       Show this help message

EXAMPLES:
    ./scripts/ci-check.sh                    # Run all CI checks
    ./scripts/ci-check.sh --skip-tests       # Run only linting and type checking  
    ./scripts/ci-check.sh --coverage         # Run all checks with coverage
    ./scripts/ci-check.sh --verbose          # Run with detailed output

DESCRIPTION:
    This script executes the same quality checks that run in GitHub Actions CI/CD:
    • Code linting with Ruff
    • Code formatting with Black
    • Import sorting with isort  
    • Type checking with MyPy
    • Security scanning with Bandit
    • Test execution with pytest
    • Coverage reporting (optional)

    Use this before pushing to ensure CI will pass.
EOF
    exit 0
fi

# Main execution
echo -e "${MAGENTA}"
cat << 'EOF'
╔═══════════════════════════════════════════════════════════════════════╗
║                    Portfolio Manager CI/CD Checks                    ║
║                          Local Validation                            ║
╚═══════════════════════════════════════════════════════════════════════╝
EOF
echo -e "${NC}"

START_TIME=$(date +%s)
FAILED_CHECKS=()

# Check if we're in the right directory
if [ ! -d "portfolio_manager" ]; then
    print_error "portfolio_manager directory not found. Please run this script from the project root."
    exit 1
fi

print_info "Starting CI/CD validation checks..."
print_info "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
print_info "Shell: $SHELL"
print_info "OS: $(uname -s) $(uname -m)"

# Environment checks
print_step "Environment Validation"

# Check Python
if ! command_exists python3; then
    if ! command_exists python; then
        print_error "Python not found in PATH"
        exit 1
    else
        PYTHON_CMD="python"
    fi
else
    PYTHON_CMD="python3"
fi

PYTHON_VERSION=$($PYTHON_CMD --version 2>&1)
print_info "Python version: $PYTHON_VERSION"

# Check pip
if ! command_exists pip3 && ! command_exists pip; then
    print_error "pip not found in PATH"
    exit 1
fi

# Check if development dependencies are installed
print_step "Dependency Validation"

REQUIRED_PACKAGES=("ruff" "black" "isort" "mypy" "pytest" "bandit")
MISSING_PACKAGES=()

for package in "${REQUIRED_PACKAGES[@]}"; do
    if ! $PYTHON_CMD -c "import $package" 2>/dev/null; then
        MISSING_PACKAGES+=("$package")
    fi
done

if [ ${#MISSING_PACKAGES[@]} -gt 0 ]; then
    print_warning "Missing packages detected: ${MISSING_PACKAGES[*]}"
    print_info "Installing missing development dependencies..."
    
    if ! run_check "$PYTHON_CMD -m pip install -e \".[dev]\"" "Install development dependencies"; then
        print_error "Failed to install dependencies"
        exit 1
    fi
fi

# Code Quality Checks
print_step "Code Quality Checks"

# Ruff linting
if ! run_check "ruff check portfolio_manager/" "Ruff linting"; then
    FAILED_CHECKS+=("Ruff linting")
fi

# Black formatting check
if ! run_check "black --check portfolio_manager/" "Black formatting"; then
    FAILED_CHECKS+=("Black formatting")
fi

# Import sorting check
if ! run_check "isort --check-only portfolio_manager/" "Import sorting"; then
    FAILED_CHECKS+=("Import sorting")
fi

# Type checking
print_step "Type Checking"

if ! run_check "mypy portfolio_manager/" "MyPy type checking"; then
    FAILED_CHECKS+=("Type checking")
fi

# Security scanning
print_step "Security Scanning"

if ! run_check "bandit -r portfolio_manager/" "Security scan" true; then
    print_warning "Security scan found issues (warnings only)"
fi

# Test execution
if [ "$SKIP_TESTS" = false ]; then
    print_step "Test Execution"
    
    if [ "$COVERAGE" = true ]; then
        print_info "Running tests with coverage reporting..."
        if ! run_check "pytest --cov=portfolio_manager --cov-report=term-missing --cov-report=html" "Tests with coverage"; then
            FAILED_CHECKS+=("Tests with coverage")
        else
            print_success "Coverage report generated in htmlcov/"
        fi
    else
        print_info "Running test suite..."
        if ! run_check "pytest -v" "Unit and integration tests"; then
            FAILED_CHECKS+=("Tests")
        fi
    fi
    
    # Run comprehensive test suite if available
    if [ -f "tests/run_tests.py" ]; then
        print_info "Running comprehensive test suite..."
        if ! run_check "$PYTHON_CMD tests/run_tests.py all" "Comprehensive test suite" true; then
            print_warning "Comprehensive test suite had issues (warnings only)"
        fi
    fi
else
    print_info "Skipping tests (--skip-tests flag enabled)"
fi

# Build validation
print_step "Build Validation"

if [ -f "pyproject.toml" ]; then
    print_info "Validating package build..."
    
    # Check if build module is available
    if $PYTHON_CMD -c "import build" 2>/dev/null; then
        if ! run_check "$PYTHON_CMD -m build --wheel --no-isolation" "Package build" true; then
            print_warning "Package build had issues (warnings only)"
        fi
    else
        print_info "Build module not available, skipping build validation"
    fi
fi

# Summary
print_step "Validation Summary"

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
MINUTES=$((DURATION / 60))
SECONDS=$((DURATION % 60))

echo
echo -e "${MAGENTA}╔═══════════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${MAGENTA}║                            SUMMARY                                   ║${NC}"
echo -e "${MAGENTA}╚═══════════════════════════════════════════════════════════════════════╝${NC}"

print_info "Total execution time: $(printf '%02d:%02d' $MINUTES $SECONDS)"

if [ ${#FAILED_CHECKS[@]} -eq 0 ]; then
    print_success "All CI/CD checks passed! ✨"
    print_success "Your code is ready for commit and push."
    
    echo -e "\n${GREEN}🚀 Next steps:${NC}"
    echo -e "  ${GRAY}• git add -A${NC}"
    echo -e "  ${GRAY}• git commit -m \"your commit message\"${NC}"
    echo -e "  ${GRAY}• git push origin your-branch-name${NC}"
    
    exit 0
else
    print_error "The following checks failed:"
    for check in "${FAILED_CHECKS[@]}"; do
        echo -e "  ${RED}• $check${NC}"
    done
    
    echo -e "\n${YELLOW}🔧 Fix the issues above before committing.${NC}"
    echo -e "${YELLOW}💡 You can also run individual tools to fix issues:${NC}"
    echo -e "  ${GRAY}• black portfolio_manager/                    # Auto-format code${NC}"
    echo -e "  ${GRAY}• isort portfolio_manager/                    # Auto-sort imports${NC}"
    echo -e "  ${GRAY}• ruff check portfolio_manager/ --fix         # Auto-fix some linting issues${NC}"
    
    exit 1
fi