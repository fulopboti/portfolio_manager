# GitHub Development Workflow Guide

This document provides comprehensive guidelines for using GitHub effectively in the Portfolio Manager project, covering branching strategies, versioning, releases, and pull request workflows.

## 🌳 Branching Strategy

### Branch Types & Naming Conventions

The project follows a GitFlow-inspired branching model with clear naming conventions:

```bash
# Main branches
master              # Production-ready code
develop            # Integration branch for features (optional)

# Feature branches
feature/symbol-mapping        # New features
feature/yfinance-provider    
feature/portfolio-analytics  
feature/risk-metrics

# Bug fixes
fix/decimal-precision        # Bug fixes
fix/repository-error-handling
fix/duckdb-connection-pool

# Hotfixes (urgent production fixes)
hotfix/security-patch       # Critical production fixes
hotfix/data-corruption-fix
hotfix/memory-leak

# Release branches
release/v1.2.0             # Prepare releases
release/v2.0.0-beta        
release/v1.3.0-rc1

# Maintenance and documentation
chore/update-dependencies   # Maintenance tasks
chore/refactor-tests       
chore/cleanup-imports
docs/api-documentation     # Documentation updates
docs/deployment-guide
```

### Branching Workflow

#### Creating a Feature Branch

```bash
# 1. Start from the latest master
git checkout master
git pull origin master

# 2. Create and switch to feature branch
git checkout -b feature/portfolio-rebalancing

# 3. Work on your feature with atomic commits
git add portfolio_manager/application/services/rebalancing.py
git commit -m "feat: implement portfolio rebalancing algorithm

- Add RebalancingService with target allocation logic
- Implement trade suggestion generation based on current vs target weights
- Add comprehensive unit tests with 98% coverage
- Include proper exception handling with domain-specific errors"

# 4. Keep branch updated with master
git fetch origin
git rebase origin/master  # or git merge origin/master

# 5. Push branch and create PR
git push -u origin feature/portfolio-rebalancing
# Create PR via GitHub UI or CLI: gh pr create
```

#### Branch Protection Rules

Configure the following branch protection rules for `master`:

- Require pull request reviews before merging
- Require status checks to pass before merging
- Require branches to be up to date before merging  
- Restrict pushes that create files larger than 100MB
- Require signed commits (recommended)

## 🏷️ Versioning & Tagging

### Semantic Versioning (SemVer)

The project follows semantic versioning principles:

```
v1.2.3
│ │ └── PATCH: Bug fixes, security patches, documentation
│ └──── MINOR: New features, backwards compatible changes  
└────── MAJOR: Breaking changes, API modifications

Examples for Portfolio Manager:
v0.1.0  - Initial development release
v1.0.0  - First stable release with core functionality
v1.1.0  - Add new investment strategies (VALUE, GROWTH)
v1.1.1  - Fix decimal precision bug in trade calculations
v1.2.0  - Add portfolio analytics dashboard
v2.0.0  - Breaking: Migrate to hexagonal architecture
```

### Version Determination Guidelines

**MAJOR version** increment when:
- Breaking API changes
- Database schema breaking changes
- Architectural changes requiring migration
- Removal of deprecated features

**MINOR version** increment when:
- New features added (backwards compatible)
- New investment strategies
- New API endpoints
- Performance improvements
- New configuration options

**PATCH version** increment when:
- Bug fixes
- Security patches
- Documentation updates
- Dependency updates (security)
- Code refactoring (no functional changes)

### Release Process

```bash
# 1. Create release branch from master
git checkout master
git pull origin master  
git checkout -b release/v1.2.0

# 2. Update version in relevant files
# - pyproject.toml: version = "1.2.0"
# - portfolio_manager/__init__.py: __version__ = "1.2.0"
# - Update CHANGELOG.md with release notes

# 3. Run comprehensive testing
pytest --cov=portfolio_manager --cov-report=term-missing
python tests/run_tests.py all
ruff check portfolio_manager/
black --check portfolio_manager/
mypy portfolio_manager/

# 4. Commit version updates
git add .
git commit -m "chore: bump version to v1.2.0

- Update version strings across project
- Update CHANGELOG.md with release notes
- Prepare for v1.2.0 release"

# 5. Create and push annotated tag
git tag -a v1.2.0 -m "Release v1.2.0: Portfolio Analytics Engine

Features:
- Advanced portfolio analytics dashboard with performance metrics
- Risk analysis engine with VaR and Sharpe ratio calculations
- Strategy backtesting framework for historical analysis
- Enhanced data visualization components

Improvements:
- 40% performance improvement in portfolio calculations
- Enhanced error handling with domain-specific exceptions
- Improved test coverage to 97%
- Optimized database queries for large portfolios

Bug Fixes:
- Fixed decimal precision loss in trade calculations (#234)
- Resolved DuckDB connection pool exhaustion under load (#245)
- Fixed portfolio rebalancing edge cases with zero positions (#256)

Breaking Changes:
- PortfolioService.calculate_returns() now returns ReturnsAnalysis object
- Database schema migration required (see MIGRATION.md)"

git push origin v1.2.0

# 6. Merge release branch to master
git checkout master
git merge --no-ff release/v1.2.0
git push origin master

# 7. Clean up release branch (optional)
git branch -d release/v1.2.0
git push origin --delete release/v1.2.0
```

## 📋 Pull Request Workflow

### PR Creation Process

1. **Pre-PR Checklist**:
   - All tests pass locally
   - Code follows style guidelines
   - Documentation updated if needed
   - Self-review completed

2. **Create PR**: Use GitHub UI or CLI
   ```bash
   gh pr create --title "feat: implement portfolio rebalancing service" \
                --body-file PR_DESCRIPTION.md \
                --reviewer @teammate \
                --label "enhancement"
   ```

3. **PR Review Cycle**:
   - Automated CI checks run
   - Peer review and feedback
   - Address review comments
   - Final approval and merge

### PR Merge Strategies

**Squash and Merge** (Recommended):
- Creates clean, linear history
- Combines all commits into single commit on master
- Use for feature branches and bug fixes

**Merge Commit**:
- Preserves branch structure
- Use for release branches and important milestones

**Rebase and Merge**:
- Creates linear history without merge commits
- Use when individual commits in PR are valuable

### PR Review Guidelines

#### For Authors:
- Keep PRs focused and reasonably sized (< 400 lines changed)
- Write clear PR descriptions with context
- Respond promptly to review feedback
- Ensure CI passes before requesting review
- Self-review code before submission

#### For Reviewers:
- Review promptly (within 24-48 hours)
- Focus on logic, architecture, and maintainability
- Check for security issues and performance impacts
- Verify test coverage for new code
- Provide constructive feedback with suggestions

### Code Review Checklist

**Architecture & Design**:
- [ ] Changes follow hexagonal architecture principles
- [ ] Domain logic properly isolated in domain layer
- [ ] Repository pattern correctly implemented
- [ ] Event-driven architecture maintained

**Code Quality**:
- [ ] Domain-specific exceptions used appropriately
- [ ] Financial precision maintained (Decimal usage)
- [ ] Proper logging with structured context
- [ ] Type hints present and accurate
- [ ] Code follows project style guidelines

**Testing**:
- [ ] Unit tests for business logic
- [ ] Integration tests for cross-layer interactions
- [ ] Edge cases and error scenarios covered
- [ ] Test coverage maintained/improved

**Security & Performance**:
- [ ] Input validation implemented
- [ ] SQL injection prevention (parameterized queries)
- [ ] No hardcoded secrets or sensitive data
- [ ] No obvious performance regressions

## 🚀 Release Management

### GitHub Releases

#### Creating Releases via CLI

```bash
# Create release with notes file
gh release create v1.2.0 \
  --title "Portfolio Manager v1.2.0" \
  --notes-file RELEASE_NOTES.md \
  --generate-notes  # Auto-generate from PRs

# Create pre-release (beta/RC)
gh release create v1.3.0-beta.1 \
  --title "Portfolio Manager v1.3.0 Beta 1" \
  --prerelease \
  --notes "Beta release for testing new features"
```

#### Release Assets

Include relevant artifacts in releases:
```bash
# Build and attach distribution files
python -m build
gh release upload v1.2.0 dist/portfolio_manager-1.2.0-*.whl
gh release upload v1.2.0 dist/portfolio_manager-1.2.0.tar.gz
```

### Changelog Management

Maintain `CHANGELOG.md` following [Keep a Changelog](https://keepachangelog.com/) format:

```markdown
# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]
### Added
- New portfolio optimization algorithms

### Changed
- Improved error messages in validation

### Fixed
- Memory leak in data ingestion service

## [1.2.0] - 2024-03-15
### Added
- Portfolio analytics dashboard with performance metrics
- Risk analysis engine (VaR, Sharpe ratio, drawdowns)
- Strategy backtesting framework
- Data visualization components

### Changed
- Enhanced error handling with domain-specific exceptions
- Optimized database queries (40% performance improvement)
- Updated test coverage to 97%

### Fixed
- Decimal precision loss in trade calculations (#234)
- DuckDB connection pool exhaustion (#245)
- Portfolio rebalancing edge cases (#256)

### Breaking Changes
- `PortfolioService.calculate_returns()` returns `ReturnsAnalysis` object
- Database migration required (see MIGRATION.md)

## [1.1.1] - 2024-02-28
### Fixed
- Fixed string-to-Decimal conversion in repository layer
- Resolved race condition in event handling
```

## 🔄 Daily Development Workflow

### Morning Routine

```bash
# 1. Sync with remote changes
git checkout master
git pull origin master

# 2. Update your feature branch
git checkout feature/your-feature-name
git rebase master  # Incorporate latest changes

# 3. Check CI status and PR reviews  
gh pr status
gh pr list --assignee @me
```

### Commit Best Practices

Follow [Conventional Commits](https://www.conventionalcommits.org/) specification:

```bash
# Format: <type>[optional scope]: <description>
# 
# [optional body]
#
# [optional footer(s)]

git commit -m "feat: add portfolio rebalancing service

- Implement target allocation algorithm with optimization
- Add trade suggestion generation with cost analysis
- Include comprehensive error handling for edge cases
- Add performance benchmarks and monitoring"

git commit -m "fix: resolve decimal precision in trade calculations

- Convert all financial values to string before database storage
- Add validation for Decimal conversion from database
- Update repository tests for precision requirements
- Add migration script for existing data

Fixes #234"

git commit -m "docs: update API documentation for portfolio endpoints

- Add OpenAPI specifications for new endpoints
- Include request/response examples
- Document authentication requirements"

# Commit types:
# feat: New feature
# fix: Bug fix  
# docs: Documentation changes
# style: Code style changes (formatting, etc.)
# refactor: Code refactoring
# test: Adding/updating tests
# chore: Maintenance tasks
```

### Atomic Commits

Create focused, atomic commits that:
- Address a single concern or change
- Include related tests and documentation
- Pass all CI checks independently
- Have clear, descriptive commit messages

**Good Example**:
```bash
git commit -m "feat: add portfolio performance metrics calculation

- Implement returns calculation with time-weighted methodology
- Add Sharpe ratio and maximum drawdown calculations  
- Include comprehensive unit tests with edge cases
- Add performance benchmarks for large portfolios"
```

**Bad Example**:
```bash
git commit -m "WIP: lots of changes

- fixed some bugs
- added new features
- updated tests
- misc cleanup"
```

## 📈 Advanced GitHub Features

### Issue Management

#### Labels

Use consistent labels for issue management:

**Type Labels**:
- `bug` - Something isn't working
- `enhancement` - New feature or improvement
- `documentation` - Documentation improvements
- `question` - Further information requested
- `duplicate` - Duplicate issue
- `invalid` - Invalid issue

**Priority Labels**:  
- `priority: critical` - Security issues, data corruption
- `priority: high` - Important features, significant bugs
- `priority: medium` - Standard features and improvements
- `priority: low` - Nice-to-have features

**Component Labels**:
- `domain` - Domain layer changes
- `application` - Application layer changes  
- `infrastructure` - Infrastructure layer changes
- `database` - Database-related changes
- `ci/cd` - Build and deployment changes

### Project Management

#### Milestones

Create milestones for major releases:
```
v1.2.0 - Portfolio Analytics
- Due: March 15, 2024
- Description: Major release adding analytics dashboard and risk metrics

v1.3.0 - Strategy Engine Enhancement  
- Due: May 1, 2024
- Description: Enhanced strategy engine with ML-based recommendations
```

#### GitHub Projects

Use GitHub Projects for sprint planning:
- **Backlog**: Prioritized list of issues/features
- **Sprint Planning**: Current sprint items
- **In Progress**: Actively being worked on
- **Review**: Pending code review
- **Done**: Completed items

### Automation

#### GitHub Actions for Project Management

```yaml
# .github/workflows/project-management.yml
name: Project Management

on:
  issues:
    types: [opened, closed, reopened]
  pull_request:
    types: [opened, closed, merged]

jobs:
  auto-assign:
    runs-on: ubuntu-latest
    steps:
    - name: Auto-assign issues
      uses: pozil/auto-assign-issue@v1
      with:
        assignees: maintainer1,maintainer2
        numOfAssignee: 1
```

## 🔧 Repository Configuration

### Branch Protection Rules

Configure these settings for the `master` branch:

**General**:
- ☑️ Restrict pushes that create files larger than 100MB
- ☑️ Require signed commits

**Branch Protection Rules**:
- ☑️ Require a pull request before merging
  - ☑️ Require approvals (1-2 reviewers)
  - ☑️ Dismiss stale PR approvals when new commits are pushed
  - ☑️ Require review from code owners
- ☑️ Require status checks to pass before merging
  - ☑️ Require branches to be up to date before merging
  - Required status checks: `test`, `lint`, `type-check`
- ☑️ Require conversation resolution before merging
- ☑️ Require linear history (optional)
- ☑️ Include administrators (apply rules to admins too)

### Security Settings

**Vulnerability Alerts**:
- Enable Dependabot alerts
- Enable Dependabot security updates
- Configure automated dependency updates

**Code Scanning**:
- Enable CodeQL analysis
- Configure custom security policies
- Set up secret scanning

This comprehensive GitHub workflow guide ensures consistent, high-quality development practices that align with the Portfolio Manager's architectural principles and quality standards.