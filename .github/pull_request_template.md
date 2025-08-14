## Summary
<!-- Provide a brief description of the changes and the motivation behind them -->

## Type of Change
<!-- Mark the relevant option with an "x" -->
- [ ] 🐛 Bug fix (non-breaking change that fixes an issue)
- [ ] ✨ New feature (non-breaking change that adds functionality)  
- [ ] 💥 Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] 📚 Documentation update
- [ ] 🔧 Refactoring (no functional changes)
- [ ] ⚡ Performance improvement
- [ ] 🧪 Test improvements

## Changes Made
<!-- List specific changes made in this PR -->
- 
- 
- 

## Testing
<!-- Mark completed testing activities -->
- [ ] Unit tests pass (`pytest tests/unit/`)
- [ ] Integration tests pass (`pytest tests/integration/`)
- [ ] Coverage maintained or improved
- [ ] Manual testing completed
- [ ] Edge cases tested
- [ ] Error scenarios tested

## Code Quality Checklist
<!-- Ensure all quality standards are met -->
- [ ] Code follows project style guidelines (`black`, `ruff`, `isort`)
- [ ] Type hints added and `mypy` passes
- [ ] Exception handling follows domain patterns (domain-specific exceptions)
- [ ] Financial precision maintained (Decimal usage for financial calculations)
- [ ] Structured logging added for significant operations using `self._logger`
- [ ] Docstrings added for new public methods/classes

## Architecture Compliance
<!-- Verify adherence to hexagonal architecture -->
- [ ] Changes follow hexagonal architecture principles
- [ ] Domain logic isolated in domain layer
- [ ] Repository pattern properly implemented
- [ ] Event-driven architecture maintained where applicable
- [ ] Layer separation respected (no circular dependencies)

## Database Changes
<!-- Select one and provide details if applicable -->
- [ ] No database changes
- [ ] Schema migration included and tested
- [ ] Data migration scripts provided (if needed)
- [ ] Migration tested on sample data
- [ ] Backward compatibility considered

## Performance Impact
<!-- Assess performance implications -->
- [ ] No performance impact expected
- [ ] Performance improvements included
- [ ] Performance impact analyzed and acceptable
- [ ] Load testing completed (for significant changes)

## Security Considerations
<!-- Address security aspects -->
- [ ] No security implications
- [ ] Input validation implemented
- [ ] SQL injection prevention maintained (parameterized queries)
- [ ] No hardcoded secrets or sensitive data
- [ ] Authentication/authorization properly handled

## Breaking Changes
<!-- If this is a breaking change, describe what breaks and how to migrate -->
None.

<!-- If there are breaking changes, replace "None." with:
### What breaks:
- 

### Migration steps:
1. 
2. 

### Backward compatibility:
- [ ] Backward compatibility maintained
- [ ] Deprecation warnings added
- [ ] Migration guide provided
-->

## Dependencies
<!-- List any new dependencies or version updates -->
- [ ] No new dependencies
- [ ] New dependencies justified and documented
- [ ] Dependency versions pinned appropriately
- [ ] Security implications of new dependencies reviewed

## Documentation
<!-- Ensure proper documentation -->
- [ ] Code changes are self-documenting
- [ ] API documentation updated (if applicable)
- [ ] README updated (if applicable)  
- [ ] CHANGELOG.md updated
- [ ] Migration guide provided (for breaking changes)

## Screenshots/Logs
<!-- Include relevant screenshots, logs, or output examples if applicable -->

## Related Issues
<!-- Link related issues using GitHub keywords -->
<!-- Examples:
Closes #123
Fixes #456
Related to #789
-->

## Additional Context
<!-- Add any other context about the PR here -->

---

## Reviewer Checklist
<!-- For reviewers - do not modify -->
- [ ] **Architecture**: Changes follow hexagonal architecture principles
- [ ] **Domain Logic**: Business rules properly implemented in domain layer
- [ ] **Exception Handling**: Domain-specific exceptions used, proper logging
- [ ] **Financial Precision**: Decimal used for financial calculations, no float usage
- [ ] **Tests**: Comprehensive test coverage for new/changed code
- [ ] **Performance**: No obvious performance regressions
- [ ] **Security**: Input validation, SQL injection prevention
- [ ] **Documentation**: Code comments and docstrings where needed
- [ ] **Style**: Code follows project conventions and passes all linting
- [ ] **Dependencies**: New dependencies are justified and secure