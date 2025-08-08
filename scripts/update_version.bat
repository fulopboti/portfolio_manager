@echo off
REM Version Update Script for Portfolio Manager
REM This batch script provides an easy way to update version numbers on Windows

echo.
echo ========================================
echo   Portfolio Manager Version Updater
echo ========================================
echo.

if "%1"=="" (
    echo Usage: update_version.bat [major^|minor^|patch] [--dry-run] [--force]
    echo.
    echo Examples:
    echo   update_version.bat patch          # Bump patch version (0.1.0 -^> 0.1.1)
    echo   update_version.bat minor          # Bump minor version (0.1.0 -^> 0.2.0)
    echo   update_version.bat major          # Bump major version (0.1.0 -^> 1.0.0)
    echo   update_version.bat patch --dry-run # Preview changes without applying
    echo   update_version.bat minor --force  # Force update even with conflicts
    echo.
    exit /b 1
)

echo Running version update...
python "%~dp0update_version.py" %*

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ✅ Version update completed successfully!
) else (
    echo.
    echo ❌ Version update failed!
    echo.
    echo For help, run: update_version.bat
)

pause
