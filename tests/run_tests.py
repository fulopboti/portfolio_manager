#!/usr/bin/env python3
"""Test runner script for Portfolio Manager.

This script provides convenient commands for running different categories of tests
with appropriate configurations.
"""

import subprocess
import sys
from pathlib import Path


def run_command(cmd: list[str], description: str) -> bool:
    """Run a command and return success status."""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {' '.join(cmd)}")
    print(f"{'='*60}")

    try:
        result = subprocess.run(cmd, check=True, cwd=Path(__file__).parent.parent)
        print(f"\n✅ {description} - PASSED")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n❌ {description} - FAILED (exit code: {e.returncode})")
        return False


def main():
    """Main test runner."""
    if len(sys.argv) < 2:
        print("""
Portfolio Manager Test Runner

Usage: python run_tests.py <test_type>

Available test types:
  unit         - Run all unit tests (fast)
  integration  - Run all integration tests (slower)
  all          - Run all tests
  coverage     - Run all tests with coverage report
  duckdb       - Run DuckDB-specific tests
  domain       - Run domain layer tests
  application  - Run application layer tests
  infrastructure - Run infrastructure layer tests

Examples:
  python run_tests.py unit
  python run_tests.py coverage
  python run_tests.py duckdb
""")
        return

    test_type = sys.argv[1].lower()
    success = True

    if test_type == "unit":
        success = run_command([
            "python", "-m", "pytest", 
            "tests/unit/", 
            "-v", 
            "-m", "unit"
        ], "Unit Tests")

    elif test_type == "integration":
        success = run_command([
            "python", "-m", "pytest", 
            "tests/integration/", 
            "-v", 
            "-m", "integration"
        ], "Integration Tests")

    elif test_type == "all":
        success = run_command([
            "python", "-m", "pytest", 
            "tests/unit/", 
            "tests/integration/",
            "-v"
        ], "All Tests")

    elif test_type == "coverage":
        success = run_command([
            "python", "-m", "pytest", 
            "tests/unit/", 
            "tests/integration/",
            "--cov=portfolio_manager",
            "--cov-report=term-missing",
            "--cov-report=html",
            "-v"
        ], "All Tests with Coverage")

    elif test_type == "duckdb":
        success = run_command([
            "python", "-m", "pytest", 
            "-m", "duckdb",
            "-v"
        ], "DuckDB Tests")

    elif test_type == "domain":
        success = run_command([
            "python", "-m", "pytest", 
            "tests/unit/domain/",
            "-v"
        ], "Domain Layer Tests")

    elif test_type == "application":
        success = run_command([
            "python", "-m", "pytest", 
            "tests/unit/application/",
            "tests/integration/application/",
            "-v"
        ], "Application Layer Tests")

    elif test_type == "infrastructure":
        success = run_command([
            "python", "-m", "pytest", 
            "tests/unit/infrastructure/",
            "tests/integration/infrastructure/",
            "-v"
        ], "Infrastructure Layer Tests")

    else:
        print(f"Unknown test type: {test_type}")
        return

    if success:
        print(f"\n🎉 All tests completed successfully!")
        sys.exit(0)
    else:
        print(f"\n💥 Some tests failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
