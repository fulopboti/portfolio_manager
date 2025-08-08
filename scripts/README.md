# Scripts Directory

This directory contains utility scripts for the Portfolio Manager project.

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
