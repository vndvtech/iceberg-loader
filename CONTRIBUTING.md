# Contributing to iceberg-loader

Thank you for your interest in contributing to the project! This guide will help you get started quickly.

## Quick Start

1. **Fork and clone the repository**
   ```bash
   git clone https://github.com/YOUR_USERNAME/iceberg-loader.git
   cd iceberg-loader
   ```

2. **Install uv**

   We use [uv](https://docs.astral.sh/uv/) for environment management:
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

   **Note**: This project requires Python 3.10-3.14. If you need a specific version:
   ```bash
   uv venv --python 3.12
   ```

3. **Set up the development environment**
   ```bash
   uv sync
   ```

4. **Run linters and tests**
   ```bash
   uv run ruff check . && uv run ruff format --check . && uv run ty check
   uv run pytest
   ```

## Development Workflow

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage report
uv run pytest --cov=iceberg_loader --cov-report=html

# Run specific test file
uv run pytest tests/test_iceberg_loader.py

# Run tests with verbose output
uv run pytest -v
```

### Code Quality

```bash
# Run all checks (lint + format check + types)
uv run ruff check . && uv run ruff format --check . && uv run ty check

# Auto-fix linting issues
uv run ruff check --fix .

# Format code
uv run ruff format .

# Type checking
uv run ty check
```

### Running Examples

Examples require a local Iceberg environment with Docker:

```bash
cd examples
docker-compose up -d
cd ..

# Run individual examples
uv run python examples/load_upsert.py
uv run python examples/advanced_scenarios.py
uv run python examples/load_with_commits.py

# Run the Docker-backed smoke subset
bash tools/run_examples_smoke.sh
```

The smoke subset intentionally excludes `examples/load_from_api.py` because it depends on an external API,
and `examples/load_stream.py` because it is much heavier than a normal CI smoke run.

## Code Style Guidelines

- **Python Version**: 3.10+ (3.12 recommended)
- **Linter**: `ruff` for linting and formatting
- **Type Hints**: Add type hints to all public functions and classes
- **Docstrings**: Use Google-style docstrings for public APIs
- **Logging**: Use `logging.getLogger(__name__)` pattern
- **No Comments**: Code should be self-explanatory (unless complex logic requires explanation)

### Example

```python
import logging
from typing import Any

logger = logging.getLogger(__name__)


def load_data(table_id: tuple[str, str], data: Any) -> dict[str, Any]:
    """
    Load data into an Iceberg table.

    Args:
        table_id: Table identifier as (namespace, table_name)
        data: Data to load

    Returns:
        Dictionary with loading results
    """
    logger.info('Loading data into table %s', table_id)
    # Implementation
    return {'rows_loaded': 100}
```

## Testing Guidelines

- **New Feature → New Test**: Every new feature must include corresponding tests
- **Test Coverage**: Aim for 90%+ coverage for new code
- **Test Structure**: Use descriptive test names that explain what is being tested
- **Fixtures**: Use pytest fixtures for common test setups
- **Mocking**: Mock external dependencies (catalog, tables) appropriately

### Example Test

```python
def test_load_data_with_schema_evolution():
    """Test that schema evolution adds new columns correctly."""
    # Arrange
    catalog = get_mock_catalog()
    data = create_test_data()

    # Act
    from iceberg_loader import LoaderConfig

    config = LoaderConfig(schema_evolution=True)
    result = load_data_to_iceberg(data, ("db", "table"), catalog, config=config)

    # Assert
    assert result['rows_loaded'] == 3
```

## Pull Request Process

1. **Branch Naming**: Use descriptive branch names
   - `feature/add-streaming-support`
   - `fix/schema-evolution-bug`
   - `docs/update-api-reference`

2. **Commit Messages**: Write clear commit messages
   - Use present tense: "Add feature" not "Added feature"
   - Keep first line under 72 characters
   - Add detailed description if needed

3. **Pull Request Description**
   - Describe the changes and motivation
   - Link to related issues
   - Include test results
   - Add screenshots/logs if relevant

4. **Keep PRs Focused**:
   - One feature/fix per PR
   - Break large changes into smaller PRs
   - Rebase on main before submitting

5. **Code Review**
   - Address all review comments
   - Be open to feedback
   - Update PR based on suggestions

## Release Process

Releases are automated via GitHub Actions:

1. Ensure all tests pass on `main`
2. Update version in `src/iceberg_loader/__about__.py`
3. Create and push a git tag:
   ```bash
   git tag v0.1.0
   git push origin v0.1.0
   ```
4. GitHub Actions will automatically:
   - Build the package
   - Run tests
   - Publish to PyPI

## Getting Help

- **Questions**: Open a [Discussion](https://github.com/vndvtech/iceberg-loader/discussions)
- **Bugs**: Open an [Issue](https://github.com/vndvtech/iceberg-loader/issues)
- **Features**: Open an [Issue](https://github.com/vndvtech/iceberg-loader/issues) with the `enhancement` label

## Code of Conduct

- Be respectful and inclusive
- Welcome newcomers
- Accept constructive criticism
- Focus on what is best for the community

## Recognition

All contributors will be recognized in the project README. Thank you for making iceberg-loader better! 🎉
