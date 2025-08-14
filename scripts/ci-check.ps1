#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Local CI/CD validation script for Portfolio Manager (Windows/PowerShell)
    
.DESCRIPTION
    This script runs the same quality checks that are executed in the GitHub Actions CI/CD pipeline.
    Use this to validate your code locally before pushing to ensure CI will pass.
    
.PARAMETER SkipTests
    Skip running the test suite (useful for quick linting checks)
    
.PARAMETER Coverage
    Run tests with coverage reporting
    
.PARAMETER VerboseOutput
    Enable verbose output for debugging
    
.EXAMPLE
    .\scripts\ci-check.ps1
    Run all CI checks
    
.EXAMPLE  
    .\scripts\ci-check.ps1 -SkipTests
    Run only linting and type checking
    
.EXAMPLE
    .\scripts\ci-check.ps1 -Coverage -VerboseOutput
    Run all checks with coverage and verbose output
#>

[CmdletBinding()]
param(
    [switch]$SkipTests,
    [switch]$Coverage,
    [switch]$VerboseOutput
)

# Enable strict error handling
$ErrorActionPreference = "Continue"

# Color functions for output
function Write-Success { 
    param($Message)
    Write-Host "[SUCCESS] $Message" -ForegroundColor Green 
}

function Write-Error-Custom { 
    param($Message)
    Write-Host "[ERROR] $Message" -ForegroundColor Red 
}

function Write-Info { 
    param($Message)
    Write-Host "[INFO] $Message" -ForegroundColor Cyan 
}

function Write-Warning-Custom { 
    param($Message)
    Write-Host "[WARNING] $Message" -ForegroundColor Yellow 
}

function Write-Step {
    param($Message)
    Write-Host "`n[STEP] $Message" -ForegroundColor Blue
    Write-Host ("=" * ($Message.Length + 8)) -ForegroundColor Blue
}

# Function to check if command exists
function Test-Command {
    param($Command)
    try {
        Get-Command $Command -ErrorAction Stop | Out-Null
        return $true
    }
    catch {
        return $false
    }
}

# Function to run command with error handling
function Invoke-Check {
    param(
        [string]$Command,
        [string]$Description,
        [switch]$IgnoreErrors
    )
    
    Write-Host "  Running: $Command" -ForegroundColor Gray
    
    try {
        if ($VerboseOutput) {
            Invoke-Expression $Command
        } else {
            $output = Invoke-Expression $Command 2>&1
            if ($LASTEXITCODE -ne 0 -and -not $IgnoreErrors) {
                Write-Host $output -ForegroundColor Red
                throw "Command failed with exit code $LASTEXITCODE"
            }
        }
        Write-Success $Description
        return $true
    }
    catch {
        Write-Error-Custom "$Description failed: $($_.Exception.Message)"
        return $false
    }
}

# Main execution
Write-Host @"
===================================================================
                Portfolio Manager CI/CD Checks                    
                        Local Validation                          
===================================================================
"@ -ForegroundColor Magenta

$startTime = Get-Date
$failedChecks = @()

# Check if we're in the right directory
if (-not (Test-Path "portfolio_manager")) {
    Write-Error-Custom "portfolio_manager directory not found. Please run this script from the project root."
    Write-Host "Current directory: $(Get-Location)" -ForegroundColor Gray
    exit 1
}

Write-Success "Found portfolio_manager directory"
Write-Info "Starting CI/CD validation checks..."
Write-Info "Timestamp: $((Get-Date).ToString('yyyy-MM-dd HH:mm:ss'))"
Write-Info "PowerShell Version: $($PSVersionTable.PSVersion)"

# Environment checks
Write-Step "Environment Validation"

# Check Python
if (-not (Test-Command "python")) {
    Write-Error-Custom "Python not found in PATH"
    exit 1
}

$pythonVersion = python --version 2>&1
Write-Info "Python version: $pythonVersion"

# Check if development dependencies are installed
Write-Step "Dependency Validation"

$requiredPackages = @("ruff", "black", "isort", "mypy", "pytest", "bandit")
$missingPackages = @()

foreach ($package in $requiredPackages) {
    try {
        python -c "import $package" 2>$null
        if ($LASTEXITCODE -ne 0) {
            $missingPackages += $package
        }
    }
    catch {
        $missingPackages += $package
    }
}

if ($missingPackages.Count -gt 0) {
    Write-Warning-Custom "Missing packages detected: $($missingPackages -join ', ')"
    Write-Info "Installing missing development dependencies..."
    
    if (-not (Invoke-Check "python -m pip install -e `".[dev]`"" "Install development dependencies")) {
        Write-Error-Custom "Failed to install dependencies"
        exit 1
    }
}

# Code Quality Checks
Write-Step "Code Quality Checks"

# Ruff linting
if (-not (Invoke-Check "ruff check portfolio_manager/" "Ruff linting")) {
    $failedChecks += "Ruff linting"
}

# Black formatting check
if (-not (Invoke-Check "black --check portfolio_manager/" "Black formatting")) {
    $failedChecks += "Black formatting"
}

# Import sorting check  
if (-not (Invoke-Check "isort --check-only portfolio_manager/" "Import sorting")) {
    $failedChecks += "Import sorting"
}

# Type checking
Write-Step "Type Checking"

if (-not (Invoke-Check "mypy portfolio_manager/" "MyPy type checking")) {
    $failedChecks += "Type checking"
}

# Security scanning
Write-Step "Security Scanning"

if (-not (Invoke-Check "bandit -r portfolio_manager/" "Security scan" -IgnoreErrors)) {
    Write-Warning-Custom "Security scan found issues (warnings only)"
}

# Test execution
if (-not $SkipTests) {
    Write-Step "Test Execution"
    
    if ($Coverage) {
        Write-Info "Running tests with coverage reporting..."
        if (-not (Invoke-Check "pytest --cov=portfolio_manager --cov-report=term-missing --cov-report=html" "Tests with coverage")) {
            $failedChecks += "Tests with coverage"
        } else {
            Write-Success "Coverage report generated in htmlcov/"
        }
    } else {
        Write-Info "Running test suite..."
        if (-not (Invoke-Check "pytest -v" "Unit and integration tests")) {
            $failedChecks += "Tests"
        }
    }
} else {
    Write-Info "Skipping tests (SkipTests flag enabled)"
}

# Summary
Write-Step "Validation Summary"

$endTime = Get-Date
$duration = $endTime - $startTime

Write-Host "`n" -NoNewline
Write-Host "===================================================================" -ForegroundColor Magenta
Write-Host "                            SUMMARY                               " -ForegroundColor Magenta  
Write-Host "===================================================================" -ForegroundColor Magenta

Write-Info "Total execution time: $($duration.ToString('mm\:ss'))"

if ($failedChecks.Count -eq 0) {
    Write-Success "All CI/CD checks passed!"
    Write-Success "Your code is ready for commit and push."
    
    Write-Host "`nNext steps:" -ForegroundColor Green
    Write-Host "  • git add -A" -ForegroundColor Gray
    Write-Host "  • git commit -m `"your commit message`"" -ForegroundColor Gray  
    Write-Host "  • git push origin your-branch-name" -ForegroundColor Gray
    
    exit 0
} else {
    Write-Error-Custom "The following checks failed:"
    foreach ($check in $failedChecks) {
        Write-Host "  • $check" -ForegroundColor Red
    }
    
    Write-Host "`nFix the issues above before committing." -ForegroundColor Yellow
    Write-Host "You can also run individual tools to fix issues:" -ForegroundColor Yellow
    Write-Host "  • black portfolio_manager/          # Auto-format code" -ForegroundColor Gray
    Write-Host "  • isort portfolio_manager/          # Auto-sort imports" -ForegroundColor Gray
    Write-Host "  • ruff check portfolio_manager/ --fix  # Auto-fix some linting issues" -ForegroundColor Gray
    
    exit 1
}