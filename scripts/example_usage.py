#!/usr/bin/env python3
"""
Example usage of the version update script.

This script demonstrates how to use the VersionUpdater class programmatically
for automated version management in CI/CD pipelines.
"""

import sys
from pathlib import Path

# Add the scripts directory to the path
sys.path.insert(0, str(Path(__file__).parent))

from update_version import VersionUpdater, VersionInfo


def example_version_update():
    """Example of programmatic version update."""
    print("🔧 Version Update Script Example")
    print("=" * 40)

    # Initialize the updater
    project_root = Path.cwd()
    updater = VersionUpdater(project_root)

    # Get current version
    try:
        current_version = updater.get_current_version()
        print(f"📋 Current version: {current_version}")

        # Calculate new patch version
        new_version = current_version.bump_patch()
        print(f"🆕 New version: {new_version}")

        # Preview the update (dry run)
        print("\n🔍 Previewing changes (dry run)...")
        success = updater.run_update('patch', dry_run=True, force=False)

        if success:
            print("✅ Dry run completed successfully")

            # Ask user if they want to proceed
            response = input("\n❓ Do you want to apply the changes? (y/N): ")
            if response.lower() in ['y', 'yes']:
                print("\n🚀 Applying version update...")
                success = updater.run_update('patch', dry_run=False, force=False)

                if success:
                    print("✅ Version update completed successfully!")

                    # Verify the update
                    updated_version = updater.get_current_version()
                    print(f"📋 Updated version: {updated_version}")
                else:
                    print("❌ Version update failed")
            else:
                print("⏭️  Update cancelled")
        else:
            print("❌ Dry run failed")

    except Exception as e:
        print(f"❌ Error: {e}")
        return False

    return True


def example_version_validation():
    """Example of version validation."""
    print("\n🔍 Version Validation Example")
    print("=" * 40)

    # Test various version strings
    test_versions = [
        "1.2.3",      # Valid
        "0.1.0",      # Valid
        "10.20.30",   # Valid
        "1.2",        # Invalid
        "1.2.3.4",    # Invalid
        "a.b.c",      # Invalid
        "",           # Invalid
    ]

    for version_str in test_versions:
        try:
            version = VersionInfo.from_string(version_str)
            print(f"✅ '{version_str}' -> {version}")
        except ValueError as e:
            print(f"❌ '{version_str}' -> {e}")

    # Test version bumping
    print("\n🔄 Version Bumping Examples:")
    original = VersionInfo(1, 2, 3)
    print(f"Original: {original}")
    print(f"Patch bump: {original.bump_patch()}")
    print(f"Minor bump: {original.bump_minor()}")
    print(f"Major bump: {original.bump_major()}")


def example_security_features():
    """Example of security features."""
    print("\n🔒 Security Features Example")
    print("=" * 40)

    project_root = Path.cwd()
    updater = VersionUpdater(project_root)

    # Test safe patterns
    safe_patterns = [
        r'version\s*=\s*["\']([^"\']+)["\']',
        r'__version__\s*=\s*["\']([^"\']+)["\']',
    ]

    print("✅ Safe patterns:")
    for pattern in safe_patterns:
        is_safe = updater._is_safe_pattern(pattern)
        print(f"  {pattern}: {'✅ Safe' if is_safe else '❌ Unsafe'}")

    # Test dangerous patterns
    dangerous_patterns = [
        r'(?P<name>\d+)',  # Named group
        r'(?=lookahead)',  # Positive lookahead
        r'(?i)case',       # Flag
    ]

    print("\n❌ Dangerous patterns:")
    for pattern in dangerous_patterns:
        is_safe = updater._is_safe_pattern(pattern)
        print(f"  {pattern}: {'✅ Safe' if is_safe else '❌ Unsafe'}")


if __name__ == '__main__':
    print("🚀 Portfolio Manager Version Update Script Examples")
    print("=" * 60)

    # Run examples
    example_version_validation()
    example_security_features()

    # Interactive example (commented out to avoid accidental updates)
    # example_version_update()

    print("\n✨ Examples completed!")
    print("\n💡 To run the interactive example, uncomment the last line in the script.")
