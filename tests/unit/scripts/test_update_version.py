"""
Unit tests for the version update script.

These tests follow TDD principles and ensure the version update functionality
works correctly with proper validation and security measures.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock
import sys
import os

# Add the scripts directory to the path for testing
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / 'scripts'))

from update_version import VersionInfo, VersionUpdater


class TestVersionInfo:
    """Test the VersionInfo dataclass."""

    def test_version_info_creation(self):
        """Test creating VersionInfo with valid components."""
        version = VersionInfo(1, 2, 3)
        assert version.major == 1
        assert version.minor == 2
        assert version.patch == 3
        assert str(version) == "1.2.3"

    def test_version_info_from_string_valid(self):
        """Test creating VersionInfo from valid string."""
        version = VersionInfo.from_string("1.2.3")
        assert version.major == 1
        assert version.minor == 2
        assert version.patch == 3

    def test_version_info_from_string_invalid_format(self):
        """Test creating VersionInfo from invalid string format."""
        with pytest.raises(ValueError, match="Invalid version format"):
            VersionInfo.from_string("1.2")

        with pytest.raises(ValueError, match="Invalid version format"):
            VersionInfo.from_string("1.2.3.4")

        with pytest.raises(ValueError, match="Invalid version format"):
            VersionInfo.from_string("a.b.c")

    def test_version_info_from_string_empty(self):
        """Test creating VersionInfo from empty string."""
        with pytest.raises(ValueError, match="Version string must be a non-empty string"):
            VersionInfo.from_string("")

    def test_version_info_from_string_none(self):
        """Test creating VersionInfo from None."""
        with pytest.raises(ValueError, match="Version string must be a non-empty string"):
            VersionInfo.from_string(None)

    def test_version_info_validation_negative(self):
        """Test VersionInfo validation with negative components."""
        with pytest.raises(ValueError, match="Version components must be non-negative integers"):
            VersionInfo(-1, 0, 0)

    def test_version_bump_major(self):
        """Test major version bump."""
        version = VersionInfo(1, 2, 3)
        new_version = version.bump_major()
        assert new_version.major == 2
        assert new_version.minor == 0
        assert new_version.patch == 0
        assert str(new_version) == "2.0.0"

    def test_version_bump_minor(self):
        """Test minor version bump."""
        version = VersionInfo(1, 2, 3)
        new_version = version.bump_minor()
        assert new_version.major == 1
        assert new_version.minor == 3
        assert new_version.patch == 0
        assert str(new_version) == "1.3.0"

    def test_version_bump_patch(self):
        """Test patch version bump."""
        version = VersionInfo(1, 2, 3)
        new_version = version.bump_patch()
        assert new_version.major == 1
        assert new_version.minor == 2
        assert new_version.patch == 4
        assert str(new_version) == "1.2.4"


class TestVersionUpdater:
    """Test the VersionUpdater class."""

    @pytest.fixture
    def temp_project(self):
        """Create a temporary project structure for testing."""
        temp_dir = tempfile.mkdtemp()
        project_root = Path(temp_dir)

        # Create pyproject.toml
        pyproject_content = '''[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "portfolio-manager"
version = "0.1.0"
description = "A comprehensive, privacy-focused investment research platform"
'''
        (project_root / 'pyproject.toml').write_text(pyproject_content)

        # Create portfolio_manager/__init__.py
        init_content = '''"""Stock Analysis & Simulation Platform."""
__version__ = "0.1.0"
__author__ = "Portfolio Manager Team"
'''
        (project_root / 'portfolio_manager').mkdir()
        (project_root / 'portfolio_manager' / '__init__.py').write_text(init_content)

        yield project_root

        # Cleanup
        shutil.rmtree(temp_dir)

    def test_validate_project_structure_success(self, temp_project):
        """Test successful project structure validation."""
        updater = VersionUpdater(temp_project)
        assert updater.validate_project_structure() is True

    def test_validate_project_structure_missing_files(self):
        """Test project structure validation with missing files."""
        temp_dir = tempfile.mkdtemp()
        try:
            updater = VersionUpdater(temp_dir)
            assert updater.validate_project_structure() is False
        finally:
            shutil.rmtree(temp_dir)

    def test_get_current_version_success(self, temp_project):
        """Test getting current version from pyproject.toml."""
        updater = VersionUpdater(temp_project)
        version = updater.get_current_version()
        assert version.major == 0
        assert version.minor == 1
        assert version.patch == 0
        assert str(version) == "0.1.0"

    def test_get_current_version_missing_version(self, temp_project):
        """Test getting current version when version is missing."""
        # Remove version from pyproject.toml
        pyproject_path = temp_project / 'pyproject.toml'
        content = pyproject_path.read_text()
        content = content.replace('version = "0.1.0"', '')
        pyproject_path.write_text(content)

        updater = VersionUpdater(temp_project)
        with pytest.raises(ValueError, match="Could not find version in pyproject.toml"):
            updater.get_current_version()

    def test_validate_new_version_success(self, temp_project):
        """Test successful version validation."""
        updater = VersionUpdater(temp_project)
        current_version = VersionInfo(0, 1, 0)
        new_version = VersionInfo(0, 1, 1)
        assert updater.validate_new_version(new_version, current_version) is True

    def test_validate_new_version_failure(self, temp_project):
        """Test version validation failure when new version is not greater."""
        updater = VersionUpdater(temp_project)
        current_version = VersionInfo(0, 1, 1)
        new_version = VersionInfo(0, 1, 0)  # Lower version
        assert updater.validate_new_version(new_version, current_version) is False

    def test_validate_new_version_equal(self, temp_project):
        """Test version validation failure when versions are equal."""
        updater = VersionUpdater(temp_project)
        current_version = VersionInfo(0, 1, 0)
        new_version = VersionInfo(0, 1, 0)  # Same version
        assert updater.validate_new_version(new_version, current_version) is False

    def test_is_safe_pattern_safe(self, temp_project):
        """Test pattern safety validation with safe patterns."""
        updater = VersionUpdater(temp_project)
        safe_patterns = [
            r'version\s*=\s*["\']([^"\']+)["\']',
            r'__version__\s*=\s*["\']([^"\']+)["\']',
            r'(\d+)\.(\d+)\.(\d+)',
        ]

        for pattern in safe_patterns:
            assert updater._is_safe_pattern(pattern) is True

    def test_is_safe_pattern_dangerous(self, temp_project):
        """Test pattern safety validation with dangerous patterns."""
        updater = VersionUpdater(temp_project)
        dangerous_patterns = [
            r'(?P<name>\d+)',  # Named group
            r'(?=lookahead)',  # Positive lookahead
            r'(?!negative)',    # Negative lookahead
            r'(?<=behind)',     # Positive lookbehind
            r'(?<!negative)',   # Negative lookbehind
            r'(?i)case',        # Flag
        ]

        for pattern in dangerous_patterns:
            assert updater._is_safe_pattern(pattern) is False

    def test_update_file_version_success(self, temp_project):
        """Test successful file version update."""
        updater = VersionUpdater(temp_project)
        file_path = temp_project / 'portfolio_manager' / '__init__.py'
        patterns = [(r'__version__\s*=\s*["\']([^"\']+)["\']', '__version__ = "{new_version}"')]

        # Test dry run
        result = updater.update_file_version(file_path, patterns, "0.1.1", dry_run=True)
        assert result is True

        # Verify file wasn't actually changed in dry run
        content = file_path.read_text()
        assert '__version__ = "0.1.0"' in content

        # Test actual update
        result = updater.update_file_version(file_path, patterns, "0.1.1", dry_run=False)
        assert result is True

        # Verify file was changed
        content = file_path.read_text()
        assert '__version__ = "0.1.1"' in content

    def test_update_file_version_no_match(self, temp_project):
        """Test file version update when no patterns match."""
        updater = VersionUpdater(temp_project)
        file_path = temp_project / 'portfolio_manager' / '__init__.py'
        patterns = [(r'nonexistent\s*=\s*["\']([^"\']+)["\']', 'nonexistent = "{new_version}"')]

        result = updater.update_file_version(file_path, patterns, "0.1.1", dry_run=False)
        assert result is False

    def test_update_file_version_missing_file(self, temp_project):
        """Test file version update with missing file."""
        updater = VersionUpdater(temp_project)
        file_path = temp_project / 'nonexistent.py'
        patterns = [(r'version\s*=\s*["\']([^"\']+)["\']', 'version = "{new_version}"')]

        result = updater.update_file_version(file_path, patterns, "0.1.1", dry_run=False)
        assert result is False

    def test_update_all_versions_success(self, temp_project):
        """Test updating all version files successfully."""
        updater = VersionUpdater(temp_project)
        new_version = VersionInfo(0, 1, 1)

        results = updater.update_all_versions(new_version, dry_run=False)

        # Check that all files were updated successfully
        assert all(results.values())

        # Verify pyproject.toml was updated
        pyproject_content = (temp_project / 'pyproject.toml').read_text()
        assert 'version = "0.1.1"' in pyproject_content

        # Verify __init__.py was updated
        init_content = (temp_project / 'portfolio_manager' / '__init__.py').read_text()
        assert '__version__ = "0.1.1"' in init_content

    def test_validate_test_files_no_conflicts(self, temp_project):
        """Test test file validation with no conflicts."""
        updater = VersionUpdater(temp_project)

        # Create a test file with matching version
        test_file = temp_project / 'test_file.py'
        test_content = '''
def test_version():
    assert version == "0.1.1"
'''
        test_file.write_text(test_content)

        # Update test files patterns to include our test file
        updater.test_files_patterns = ['test_file.py']

        result = updater.validate_test_files("0.1.1")
        assert result is True

    def test_validate_test_files_with_conflicts(self, temp_project):
        """Test test file validation with version conflicts."""
        updater = VersionUpdater(temp_project)

        # Create a test file with conflicting version
        test_file = temp_project / 'test_file.py'
        test_content = '''
def test_version():
    assert version == "0.1.0"  # This conflicts with new version 0.1.1
'''
        test_file.write_text(test_content)

        # Update test files patterns to include our test file
        updater.test_files_patterns = ['test_file.py']

        result = updater.validate_test_files("0.1.1")
        assert result is False


class TestVersionUpdaterIntegration:
    """Integration tests for the VersionUpdater."""

    @pytest.fixture
    def temp_project_with_tests(self):
        """Create a temporary project with test files."""
        temp_dir = tempfile.mkdtemp()
        project_root = Path(temp_dir)

        # Create pyproject.toml
        pyproject_content = '''[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "portfolio-manager"
version = "0.1.0"
description = "A comprehensive, privacy-focused investment research platform"
'''
        (project_root / 'pyproject.toml').write_text(pyproject_content)

        # Create portfolio_manager/__init__.py
        init_content = '''"""Stock Analysis & Simulation Platform."""
__version__ = "0.1.0"
__author__ = "Portfolio Manager Team"
'''
        (project_root / 'portfolio_manager').mkdir()
        (project_root / 'portfolio_manager' / '__init__.py').write_text(init_content)

        # Create tests directory with version references
        tests_dir = project_root / 'tests'
        tests_dir.mkdir()

        test_file = tests_dir / 'test_version.py'
        test_content = '''
def test_current_version():
    # This test will be updated by the version script
    assert version == "0.1.0"
'''
        test_file.write_text(test_content)

        yield project_root

        # Cleanup
        shutil.rmtree(temp_dir)

    def test_run_update_patch_success(self, temp_project_with_tests):
        """Test successful patch version update."""
        updater = VersionUpdater(temp_project_with_tests)

        # Update test files patterns to include our test file
        updater.test_files_patterns = ['tests/test_version.py']

        # Use force flag to handle test file conflicts
        result = updater.run_update('patch', dry_run=False, force=True)
        assert result is True

        # Verify versions were updated
        pyproject_content = (temp_project_with_tests / 'pyproject.toml').read_text()
        assert 'version = "0.1.1"' in pyproject_content

        init_content = (temp_project_with_tests / 'portfolio_manager' / '__init__.py').read_text()
        assert '__version__ = "0.1.1"' in init_content

    def test_run_update_minor_success(self, temp_project_with_tests):
        """Test successful minor version update."""
        updater = VersionUpdater(temp_project_with_tests)

        # Update test files patterns to include our test file
        updater.test_files_patterns = ['tests/test_version.py']

        # Use force flag to handle test file conflicts
        result = updater.run_update('minor', dry_run=False, force=True)
        assert result is True

        # Verify versions were updated
        pyproject_content = (temp_project_with_tests / 'pyproject.toml').read_text()
        assert 'version = "0.2.0"' in pyproject_content

        init_content = (temp_project_with_tests / 'portfolio_manager' / '__init__.py').read_text()
        assert '__version__ = "0.2.0"' in init_content

    def test_run_update_major_success(self, temp_project_with_tests):
        """Test successful major version update."""
        updater = VersionUpdater(temp_project_with_tests)

        # Update test files patterns to include our test file
        updater.test_files_patterns = ['tests/test_version.py']

        # Use force flag to handle test file conflicts
        result = updater.run_update('major', dry_run=False, force=True)
        assert result is True

        # Verify versions were updated
        pyproject_content = (temp_project_with_tests / 'pyproject.toml').read_text()
        assert 'version = "1.0.0"' in pyproject_content

        init_content = (temp_project_with_tests / 'portfolio_manager' / '__init__.py').read_text()
        assert '__version__ = "1.0.0"' in init_content

    def test_run_update_dry_run(self, temp_project_with_tests):
        """Test dry run mode."""
        updater = VersionUpdater(temp_project_with_tests)

        # Update test files patterns to include our test file
        updater.test_files_patterns = ['tests/test_version.py']

        result = updater.run_update('patch', dry_run=True, force=False)
        assert result is True

        # Verify versions were NOT updated in dry run
        pyproject_content = (temp_project_with_tests / 'pyproject.toml').read_text()
        assert 'version = "0.1.0"' in pyproject_content

        init_content = (temp_project_with_tests / 'portfolio_manager' / '__init__.py').read_text()
        assert '__version__ = "0.1.0"' in init_content

    def test_run_update_invalid_bump_type(self, temp_project_with_tests):
        """Test update with invalid bump type."""
        updater = VersionUpdater(temp_project_with_tests)

        result = updater.run_update('invalid', dry_run=False, force=False)
        assert result is False

    def test_run_update_with_force(self, temp_project_with_tests):
        """Test update with force flag."""
        updater = VersionUpdater(temp_project_with_tests)

        # Create a test file with conflicting version
        test_file = temp_project_with_tests / 'tests' / 'test_conflict.py'
        test_content = '''
def test_version():
    assert version == "0.1.0"  # This conflicts with new version
'''
        test_file.write_text(test_content)

        # Update test files patterns to include our test file
        updater.test_files_patterns = ['tests/test_conflict.py']

        # Should fail without force
        result = updater.run_update('patch', dry_run=False, force=False)
        assert result is False

        # Should succeed with force
        result = updater.run_update('patch', dry_run=False, force=True)
        assert result is True
