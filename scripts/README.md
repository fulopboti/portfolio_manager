# Development Scripts

This directory contains utility scripts for development and CI/CD validation.

## CI/CD Validation Scripts

These scripts run the same quality checks that are executed in the GitHub Actions CI/CD pipeline, allowing you to validate your code locally before pushing.

### Windows (PowerShell)

**File**: `ci-check.ps1`

```powershell
# Run all checks
.\scripts\ci-check.ps1

# Skip tests (quick linting only)
.\scripts\ci-check.ps1 -SkipTests

# Run with coverage reporting
.\scripts\ci-check.ps1 -Coverage

# Verbose output for debugging
.\scripts\ci-check.ps1 -VerboseOutput -Coverage
```

**Requirements**:
- PowerShell 5.1+ (Windows) or PowerShell Core 7+ (cross-platform)
- Python 3.11+ with development dependencies installed

### Linux/macOS (Bash)

**File**: `ci-check.sh`

```bash
# Make script executable (first time only)
chmod +x scripts/ci-check.sh

# Run all checks
./scripts/ci-check.sh

# Skip tests (quick linting only)  
./scripts/ci-check.sh --skip-tests

# Run with coverage reporting
./scripts/ci-check.sh --coverage

# Verbose output for debugging
./scripts/ci-check.sh --verbose --coverage

# Show help
./scripts/ci-check.sh --help
```

**Requirements**:
- Bash 4.0+
- Python 3.11+ with development dependencies installed

## What the Scripts Check

Both scripts perform the same validation as the GitHub Actions workflow:

### 1. **Environment Validation**
- ✅ Python version check
- ✅ Development dependencies verification
- ✅ Auto-install missing packages

### 2. **Code Quality Checks**  
- ✅ **Ruff linting**: Code quality and style
- ✅ **Black formatting**: Consistent code formatting
- ✅ **isort**: Import statement sorting
- ✅ **MyPy**: Static type checking
- ✅ **Bandit**: Security vulnerability scanning

### 3. **Test Execution**
- ✅ **pytest**: Unit and integration tests
- ✅ **Coverage reporting**: Code coverage analysis (optional)
- ✅ **Comprehensive test suite**: Full test runner (if available)

### 4. **Build Validation**
- ✅ **Package build**: Validate package can be built
- ✅ **Distribution check**: Verify package integrity

## Usage Recommendations

### Before Committing
```bash
# Quick pre-commit check (skips tests for speed)
./scripts/ci-check.sh --skip-tests

# Full validation before push
./scripts/ci-check.sh --coverage
```

### Fixing Issues
The scripts provide helpful commands to fix common issues:

```bash
# Auto-format code
black portfolio_manager/

# Auto-sort imports  
isort portfolio_manager/

# Auto-fix linting issues
ruff check portfolio_manager/ --fix
```

### Integration with Git Hooks

You can integrate these scripts with git hooks for automatic validation:

#### Pre-commit Hook
```bash
# .git/hooks/pre-commit
#!/bin/bash
exec ./scripts/ci-check.sh --skip-tests
```

#### Pre-push Hook
```bash  
# .git/hooks/pre-push
#!/bin/bash
exec ./scripts/ci-check.sh
```

## Output Examples

### ✅ Success Output
```
✅ All CI/CD checks passed! ✨
✅ Your code is ready for commit and push.

🚀 Next steps:
  • git add -A
  • git commit -m "your commit message"  
  • git push origin your-branch-name
```

### ❌ Failure Output
```
❌ The following checks failed:
  • Black formatting
  • MyPy type checking

🔧 Fix the issues above before committing.
💡 You can also run individual tools to fix issues:
  • black portfolio_manager/          # Auto-format code
  • isort portfolio_manager/          # Auto-sort imports
  • ruff check portfolio_manager/ --fix  # Auto-fix linting issues
```

## Performance

- **Quick check** (--skip-tests): ~30-60 seconds
- **Full validation**: ~2-5 minutes depending on test suite size
- **With coverage**: +30-60 seconds for coverage generation

## Troubleshooting

### Common Issues

**"Command not found" errors**:
- Ensure Python 3.11+ is installed and in PATH
- Run `pip install -e ".[dev]"` to install development dependencies

**Permission errors (Linux/macOS)**:
- Run `chmod +x scripts/ci-check.sh` to make script executable

**PowerShell execution policy (Windows)**:  
- Run `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`

**Missing dependencies**:
- Scripts auto-detect and install missing packages
- If issues persist, run `pip install -e ".[dev]"` manually

### Debug Mode
Use verbose flags to see detailed output for debugging:
- PowerShell: `-VerboseOutput`  
- Bash: `--verbose`

## CI/CD Alignment

These scripts are designed to match the GitHub Actions workflow exactly:
- Same tool versions and configurations
- Same command-line arguments
- Same error handling and reporting

Running these scripts locally should predict CI/CD results with high accuracy.

---

## Version Update Script

The `update_version.py` script provides a secure and robust way to update version numbers across the project following semantic versioning principles.

### Features

- **Semantic Versioning**: Supports major, minor, and patch version bumps
- **Security**: Validates regex patterns to prevent injection attacks
- **Backup & Recovery**: Creates backups before making changes and can restore on failure
- **Validation**: Ensures new versions are greater than current versions
- **Dry Run Mode**: Preview changes without applying them
- **Comprehensive Logging**: Uses structured logging for monitoring and auditing
- **Test File Validation**: Checks for version conflicts in test files
- **Force Mode**: Override validation checks when needed

### Usage

```bash
# Bump patch version (0.1.0 -> 0.1.1)
python scripts/update_version.py patch

# Bump minor version (0.1.0 -> 0.2.0)
python scripts/update_version.py minor

# Bump major version (0.1.0 -> 1.0.0)
python scripts/update_version.py major

# Preview changes without applying them
python scripts/update_version.py patch --dry-run

# Force update even with validation failures
python scripts/update_version.py minor --force

# Specify custom project root
python scripts/update_version.py patch --project-root /path/to/project
```

### Files Updated

The script updates version numbers in the following files:

- `pyproject.toml` - Project metadata and dependencies
- `portfolio_manager/__init__.py` - Package version information

### Security Features

- **Pattern Validation**: All regex patterns are validated to prevent dangerous constructs
- **Input Sanitization**: All user inputs are validated and sanitized
- **Backup Creation**: Files are backed up before modification
- **Atomic Operations**: Changes are applied atomically with rollback capability
- **Structured Logging**: All actions are logged for audit purposes

### Error Handling

- Graceful handling of missing files
- Validation of version format and relationships
- Automatic rollback on failure
- Comprehensive error messages and logging

### Testing

Run the unit tests for the version update script:

```bash
pytest tests/unit/scripts/test_update_version.py -v
```

### Examples

```bash
# Current version: 0.1.0

# Patch update (bug fixes)
python scripts/update_version.py patch
# Result: 0.1.1

# Minor update (new features, backward compatible)
python scripts/update_version.py minor
# Result: 0.2.0

# Major update (breaking changes)
python scripts/update_version.py major
# Result: 1.0.0
```

### Integration

The script is designed to integrate with CI/CD pipelines and can be used in automated release processes. It provides appropriate exit codes (0 for success, 1 for failure) for automation.
