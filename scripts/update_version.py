#!/usr/bin/env python3
"""
Version Update Script for Portfolio Manager

This script updates version numbers across the project following semantic versioning.
It includes proper validation, logging, and security measures.

Usage:
    python scripts/update_version.py [major|minor|patch] [--dry-run] [--force]
"""

import argparse
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple
from dataclasses import dataclass
import structlog

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


@dataclass
class VersionInfo:
    """Represents a semantic version with validation."""
    major: int
    minor: int
    patch: int

    def __post_init__(self):
        """Validate version components."""
        if not all(isinstance(x, int) and x >= 0 for x in [self.major, self.minor, self.patch]):
            raise ValueError("Version components must be non-negative integers")

    @classmethod
    def from_string(cls, version_str: str) -> 'VersionInfo':
        """Create VersionInfo from version string."""
        if not version_str or not isinstance(version_str, str):
            raise ValueError("Version string must be a non-empty string")

        # Validate format: x.y.z
        pattern = r'^(\d+)\.(\d+)\.(\d+)$'
        match = re.match(pattern, version_str)
        if not match:
            raise ValueError(f"Invalid version format: {version_str}. Expected format: x.y.z")

        major, minor, patch = map(int, match.groups())
        return cls(major, minor, patch)

    def __str__(self) -> str:
        """Return version as string."""
        return f"{self.major}.{self.minor}.{self.patch}"

    def bump_major(self) -> 'VersionInfo':
        """Bump major version, reset minor and patch."""
        return VersionInfo(self.major + 1, 0, 0)

    def bump_minor(self) -> 'VersionInfo':
        """Bump minor version, reset patch."""
        return VersionInfo(self.major, self.minor + 1, 0)

    def bump_patch(self) -> 'VersionInfo':
        """Bump patch version."""
        return VersionInfo(self.major, self.minor, self.patch + 1)


class VersionUpdater:
    """Handles version updates across the project with security measures."""

    def __init__(self, project_root: Path):
        """Initialize the version updater."""
        self.project_root = Path(project_root).resolve()
        self.logger = structlog.get_logger()

        # Files that contain version information
        self.version_files = {
            'pyproject.toml': [
                (r'version\s*=\s*["\']([^"\']+)["\']', 'version = "{new_version}"'),
            ],
            'portfolio_manager/__init__.py': [
                (r'__version__\s*=\s*["\']([^"\']+)["\']', '__version__ = "{new_version}"'),
            ],
        }

        # Test files that might contain version references (for validation)
        self.test_files_patterns = [
            'tests/**/*.py',
        ]

    def validate_project_structure(self) -> bool:
        """Validate that we're in the correct project directory."""
        required_files = ['pyproject.toml', 'portfolio_manager/__init__.py']

        for file_path in required_files:
            if not (self.project_root / file_path).exists():
                self.logger.error("Missing required file", file=file_path)
                return False

        self.logger.info("Project structure validation passed")
        return True

    def get_current_version(self) -> VersionInfo:
        """Extract current version from pyproject.toml."""
        pyproject_path = self.project_root / 'pyproject.toml'

        try:
            content = pyproject_path.read_text(encoding='utf-8')
            match = re.search(r'version\s*=\s*["\']([^"\']+)["\']', content)
            if not match:
                raise ValueError("Could not find version in pyproject.toml")

            version_str = match.group(1)
            return VersionInfo.from_string(version_str)

        except Exception as e:
            self.logger.error("Failed to read current version", error=str(e))
            raise

    def validate_new_version(self, new_version: VersionInfo, current_version: VersionInfo) -> bool:
        """Validate that the new version is appropriate."""
        # Ensure new version is greater than current
        if (new_version.major, new_version.minor, new_version.patch) <= (current_version.major, current_version.minor, current_version.patch):
            self.logger.error("New version must be greater than current version", 
                            current=str(current_version), new=str(new_version))
            return False

        self.logger.info("Version validation passed", 
                        current=str(current_version), new=str(new_version))
        return True

    def update_file_version(self, file_path: Path, patterns: List[Tuple[str, str]], 
                          new_version: str, dry_run: bool = False) -> bool:
        """Update version in a single file."""
        try:
            if not file_path.exists():
                self.logger.warning("File does not exist", file=str(file_path))
                return False

            content = file_path.read_text(encoding='utf-8')
            original_content = content
            updated = False

            for pattern, replacement in patterns:
                # Validate the pattern to prevent injection
                if not self._is_safe_pattern(pattern):
                    self.logger.error("Unsafe pattern detected", pattern=pattern)
                    return False

                new_content = re.sub(pattern, replacement.format(new_version=new_version), content)
                if new_content != content:
                    content = new_content
                    updated = True

            if updated:
                if dry_run:
                    self.logger.info("Would update file (dry run)", file=str(file_path))
                else:
                    # Create backup before writing
                    backup_path = file_path.with_suffix(file_path.suffix + '.backup')
                    file_path.rename(backup_path)

                    try:
                        file_path.write_text(content, encoding='utf-8')
                        self.logger.info("Updated file", file=str(file_path))

                        # Verify the write was successful
                        if file_path.read_text(encoding='utf-8') != content:
                            raise RuntimeError("File content verification failed")

                        # Remove backup on success
                        backup_path.unlink()

                    except Exception as e:
                        # Restore from backup on failure
                        if backup_path.exists():
                            backup_path.rename(file_path)
                        self.logger.error("Failed to update file, restored backup", 
                                        file=str(file_path), error=str(e))
                        raise
                return True
            else:
                self.logger.warning("No version patterns found in file", file=str(file_path))
                return False

        except Exception as e:
            self.logger.error("Error updating file", file=str(file_path), error=str(e))
            return False

    def _is_safe_pattern(self, pattern: str) -> bool:
        """Validate that the regex pattern is safe (no dangerous constructs)."""
        dangerous_patterns = [
            r'\(\?<',  # Named groups
            r'\(\?P<',  # Named groups
            r'\(\?=',   # Positive lookahead
            r'\(\?!',   # Negative lookahead
            r'\(\?<=',  # Positive lookbehind
            r'\(\?<!',  # Negative lookbehind
            r'\(\?[imsxUX]',  # Flags
        ]

        for dangerous in dangerous_patterns:
            if re.search(dangerous, pattern):
                return False

        return True

    def update_all_versions(self, new_version: VersionInfo, dry_run: bool = False) -> Dict[str, bool]:
        """Update version in all relevant files."""
        results = {}
        new_version_str = str(new_version)

        for file_path_str, patterns in self.version_files.items():
            file_path = self.project_root / file_path_str
            success = self.update_file_version(file_path, patterns, new_version_str, dry_run)
            results[file_path_str] = success

        return results

    def validate_test_files(self, new_version: str) -> bool:
        """Validate that test files don't have hardcoded version conflicts."""
        test_files = []
        for pattern in self.test_files_patterns:
            test_files.extend(self.project_root.glob(pattern))

        conflicts = []
        for test_file in test_files:
            try:
                content = test_file.read_text(encoding='utf-8')
                # Look for hardcoded version strings that might conflict
                version_pattern = r'["\']0\.\d+\.\d+["\']'
                matches = re.findall(version_pattern, content)

                for match in matches:
                    if match != f'"{new_version}"':
                        conflicts.append((str(test_file), match))

            except Exception as e:
                self.logger.warning("Could not read test file", file=str(test_file), error=str(e))

        if conflicts:
            self.logger.warning("Found potential version conflicts in test files", conflicts=conflicts)
            return False

        return True

    def run_update(self, bump_type: str, dry_run: bool = False, force: bool = False) -> bool:
        """Main method to run the version update process."""
        try:
            self.logger.info("Starting version update process", bump_type=bump_type, dry_run=dry_run)

            # Validate project structure
            if not self.validate_project_structure():
                return False

            # Get current version
            current_version = self.get_current_version()
            self.logger.info("Current version", version=str(current_version))

            # Calculate new version
            if bump_type == 'major':
                new_version = current_version.bump_major()
            elif bump_type == 'minor':
                new_version = current_version.bump_minor()
            elif bump_type == 'patch':
                new_version = current_version.bump_patch()
            else:
                self.logger.error("Invalid bump type", bump_type=bump_type)
                return False

            # Validate new version
            if not force and not self.validate_new_version(new_version, current_version):
                return False

            # Update all files
            results = self.update_all_versions(new_version, dry_run)

            # Check for failures
            failed_files = [file for file, success in results.items() if not success]
            if failed_files:
                self.logger.error("Failed to update some files", failed_files=failed_files)
                return False

            # Validate test files (only if not dry run)
            if not dry_run and not self.validate_test_files(str(new_version)):
                if not force:
                    self.logger.error("Test file validation failed. Use --force to override.")
                    return False

            self.logger.info("Version update completed successfully", 
                           old_version=str(current_version), 
                           new_version=str(new_version),
                           dry_run=dry_run)

            return True

        except Exception as e:
            self.logger.error("Version update failed", error=str(e))
            return False


def main():
    """Main entry point for the version update script."""
    parser = argparse.ArgumentParser(
        description="Update version numbers across the Portfolio Manager project",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/update_version.py patch          # Bump patch version (0.1.0 -> 0.1.1)
  python scripts/update_version.py minor          # Bump minor version (0.1.0 -> 0.2.0)
  python scripts/update_version.py major          # Bump major version (0.1.0 -> 1.0.0)
  python scripts/update_version.py patch --dry-run # Preview changes without applying
  python scripts/update_version.py minor --force  # Force update even with conflicts
        """
    )

    parser.add_argument(
        'bump_type',
        choices=['major', 'minor', 'patch'],
        help='Type of version bump to perform'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview changes without applying them'
    )

    parser.add_argument(
        '--force',
        action='store_true',
        help='Force update even if validation fails'
    )

    parser.add_argument(
        '--project-root',
        type=Path,
        default=Path.cwd(),
        help='Project root directory (default: current directory)'
    )

    args = parser.parse_args()

    # Initialize updater
    updater = VersionUpdater(args.project_root)

    # Run update
    success = updater.run_update(args.bump_type, args.dry_run, args.force)

    if success:
        if args.dry_run:
            print("✅ Version update preview completed successfully")
        else:
            print("✅ Version update completed successfully")
        sys.exit(0)
    else:
        print("❌ Version update failed")
        sys.exit(1)


if __name__ == '__main__':
    main()
