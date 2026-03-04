# Contributing to DAL Prompt Generator

Thank you for your interest in contributing to DAL Prompt Generator! This document provides guidelines and instructions for contributing to the project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [How to Contribute](#how-to-contribute)
- [Coding Standards](#coding-standards)
- [Testing Guidelines](#testing-guidelines)
- [Pull Request Process](#pull-request-process)
- [Reporting Issues](#reporting-issues)

## Code of Conduct

By participating in this project, you agree to maintain a respectful and inclusive environment for all contributors.

## Getting Started

1. Fork the repository on GitHub
2. Clone your fork locally:
   ```bash
   git clone https://github.com/your-username/DAL_PROMPT_GENERATOR.git
   cd DAL_PROMPT_GENERATOR
   ```

3. Add the upstream repository:
   ```bash
   git remote add upstream https://github.com/original-owner/DAL_PROMPT_GENERATOR.git
   ```

## Development Setup

1. Create and activate a virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Install development dependencies:
   ```bash
   pip install pytest pytest-cov pytest-mock flake8 pylint black
   ```

4. Verify installation:
   ```bash
   python3 -c "import pandas; print('Setup successful!')"
   ```

## How to Contribute

### Types of Contributions

We welcome various types of contributions:

- Bug fixes
- New features
- Documentation improvements
- Template enhancements
- Test coverage improvements
- Performance optimizations
- Code refactoring

### Contribution Workflow

1. Create a feature branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Make your changes following our coding standards

3. Add or update tests as needed

4. Run tests to ensure everything works:
   ```bash
   pytest
   ```

5. Commit your changes:
   ```bash
   git add .
   git commit -m "Add: Brief description of your changes"
   ```

6. Push to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```

7. Create a Pull Request on GitHub

## Coding Standards

### Python Style Guide

Follow PEP 8 guidelines and the project-specific rules:

- Use 4 spaces for indentation (no tabs)
- Maximum line length: 100 characters
- Use snake_case for functions and variables
- Use CamelCase for class names
- Use UPPER_CASE for constants

### SOLID Principles

Apply SOLID principles when writing code:

- Single Responsibility Principle
- Open/Closed Principle
- Liskov Substitution Principle
- Interface Segregation Principle
- Dependency Inversion Principle

### Logging

Use `aws_lambda_powertools` logger for all logging:

```python
from aws_lambda_powertools import Logger

logger = Logger()

# Use appropriate log levels
logger.info("Important event occurred")
logger.error("Error occurred", exc_info=True)
logger.debug("Debugging information")
```

### Documentation

All classes and functions must include docstrings:

```python
class ExampleClass:
    """
    Brief description of the class.
    
    This class handles specific functionality for the project.
    
    Attributes:
        attribute_name (type): Description of the attribute
        another_attribute (type): Description of another attribute
    
    Args:
        param1 (type): Description of parameter
        param2 (type): Description of parameter
    """
    
    def example_method(self, param1: str, param2: int) -> bool:
        """
        Brief description of what the method does.
        
        Detailed explanation of the method's purpose and behavior.
        
        Args:
            param1 (str): Description of first parameter
            param2 (int): Description of second parameter
        
        Returns:
            bool: Description of return value
        
        Raises:
            ValueError: When param1 is empty
            TypeError: When param2 is not an integer
        
        Example:
            >>> obj = ExampleClass()
            >>> result = obj.example_method("test", 42)
            >>> print(result)
            True
        """
        pass
```

### Code Formatting

Use `black` for automatic code formatting:

```bash
black .
```

Use `flake8` for linting:

```bash
flake8 .
```

## Testing Guidelines

### Writing Tests

- Write unit tests for all new functionality
- Aim for at least 80% code coverage
- Use descriptive test names that explain what is being tested
- Follow the Arrange-Act-Assert pattern

### Test Structure

```python
import pytest
from unittest.mock import Mock, patch

class TestExampleClass:
    """Test suite for ExampleClass."""
    
    def test_method_returns_expected_value(self):
        """Test that method returns correct value for valid input."""
        # Arrange
        obj = ExampleClass()
        expected = True
        
        # Act
        result = obj.example_method("test", 42)
        
        # Assert
        assert result == expected
    
    def test_method_raises_exception_for_invalid_input(self):
        """Test that method raises ValueError for empty string."""
        # Arrange
        obj = ExampleClass()
        
        # Act & Assert
        with pytest.raises(ValueError):
            obj.example_method("", 42)
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov=utils --cov=. --cov-report=html

# Run specific test file
pytest tests/test_excel_utils.py

# Run specific test
pytest tests/test_excel_utils.py::TestExcelToDateframe::test_read_from_excel
```

## Pull Request Process

### Before Submitting

1. Ensure all tests pass
2. Update documentation if needed
3. Add your changes to the relevant section
4. Verify code follows style guidelines
5. Rebase on latest upstream main:
   ```bash
   git fetch upstream
   git rebase upstream/main
   ```

### PR Description Template

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Documentation update
- [ ] Code refactoring
- [ ] Performance improvement

## Changes Made
- Change 1
- Change 2
- Change 3

## Testing
Describe testing performed

## Checklist
- [ ] Code follows project style guidelines
- [ ] Self-review completed
- [ ] Comments added for complex code
- [ ] Documentation updated
- [ ] Tests added/updated
- [ ] All tests pass
- [ ] No new warnings generated
```

### Review Process

1. Maintainers will review your PR
2. Address any requested changes
3. Once approved, your PR will be merged
4. Your contribution will be acknowledged

## Reporting Issues

### Bug Reports

When reporting bugs, include:

- Clear, descriptive title
- Steps to reproduce the issue
- Expected behavior
- Actual behavior
- Excel file content (sanitized)
- Error messages and stack traces
- Log file excerpts
- Environment details (OS, Python version)

### Feature Requests

When requesting features, include:

- Clear description of the feature
- Use case and benefits
- Proposed implementation (if applicable)
- Examples of similar features in other tools

### Issue Template

```markdown
## Issue Type
- [ ] Bug Report
- [ ] Feature Request
- [ ] Documentation Issue

## Description
Clear description of the issue

## Steps to Reproduce (for bugs)
1. Step 1
2. Step 2
3. Step 3

## Expected Behavior
What should happen

## Actual Behavior
What actually happens

## Environment
- OS: [e.g., macOS 12.0]
- Python Version: [e.g., 3.9.7]
- Project Version: [e.g., 2.0.0]

## Additional Context
Any other relevant information
```

## Template Contributions

### Modifying Existing Templates

1. Templates are in `prompt_templates/`
2. Use `{{variable_name}}` for placeholders
3. Test with sample Excel files
4. Document new variables in template comments
5. Update relevant utility functions

### Adding New Templates

1. Create template file in `prompt_templates/`
2. Add template loading logic in `PromptGenerator`
3. Add parameter generation in `PromptGeneratorParam`
4. Update documentation
5. Add tests for new template

## Documentation Contributions

- Fix typos and grammatical errors
- Improve clarity and readability
- Add examples and use cases
- Update outdated information
- Translate documentation (if applicable)

## Questions?

If you have questions about contributing:

1. Check existing documentation
2. Search closed issues for similar questions
3. Open a new issue with the "question" label
4. Reach out to maintainers

## Recognition

Contributors will be recognized in:

- Project README
- Release notes
- Contributors list

Thank you for contributing to DAL Prompt Generator!
