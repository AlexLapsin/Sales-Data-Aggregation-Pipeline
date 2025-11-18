# Contributing

Contributions are welcome and appreciated. This document outlines how to contribute to this project.

## Getting Started

Before contributing:
- Read the [Getting Started Tutorial](docs/tutorial/getting-started.md)
- Review the [Architecture Documentation](docs/reference/architecture.md)
- Check [existing issues](https://github.com/your-org/sales-data-aggregation-pipeline/issues)

## How to Contribute

### Reporting Issues

Use GitHub Issues for bug reports and feature requests. Include:
- Clear description of the issue or feature
- Steps to reproduce (for bugs)
- Environment details (OS, Python version, Docker version)
- Error messages and logs (if applicable)

### Submitting Pull Requests

1. Fork the repository and create a feature branch
2. Make your changes with appropriate tests
3. Ensure all tests pass: `python tools/testing/run_tests.py`
4. Run code formatting and linting:
   ```bash
   black .
   flake8 .
   mypy src/
   ```
5. Update documentation if needed
6. Submit a pull request with a clear description

## Development Setup

See the [Getting Started Tutorial](docs/tutorial/getting-started.md) for complete setup instructions.

Quick setup:
```bash
cp .env.example .env
# Edit .env with your credentials
docker-compose up -d
python tools/validation/setup_doctor.py
```

## Code Standards

- **Python**: Black formatting, Flake8 linting, MyPy type hints
- **SQL/dbt**: Standard dbt conventions with documented models
- **Testing**: Minimum 90% coverage target
- **Documentation**: Update `docs/` for user-facing changes

## Testing

Run tests before submitting:
```bash
# Full test suite
python tools/testing/run_tests.py

# Unit tests
pytest tests/bronze/ tests/silver/ -v

# dbt tests
cd dbt && dbt test --target dev
```

## Pull Request Guidelines

- Follow existing code style
- Add tests for new functionality
- Update documentation for user-facing changes
- Ensure all CI/CD checks pass
- Use clear commit messages: `type: description` (feat, fix, docs, refactor, test)

## Questions?

For questions about contributing:
- Check the [documentation](docs/)
- Review the [Troubleshooting Guide](docs/how-to/troubleshooting.md)
- Open a GitHub issue or discussion

Thank you for contributing!
