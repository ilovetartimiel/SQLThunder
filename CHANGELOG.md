# Changelog

All notable changes to this project will be documented in this file.

This project follows [Semantic Versioning](https://semver.org/).

---

## [1.0.0] – 2025-07-05

### Added
- Initial stable release to PyPI
- Core `DBClient` interface for MySQL, PostgreSQL, and SQLite
- Core READ functions: `query`, `query_keyed`.
- Core EXECUTE functions: `execute`, `execute_many`
- Core INSERT functions: `insert_many`
- Threaded `query_batch`, `execute_batch`, and `insert_batch` functions
- `DBSession` wrapper to support the use of a context manager
- Flexible return types and transaction error handling
- Custom exceptions
- YAML config loading
- CLI tool via `sqlthunder` entry point
- Type hints (`py.typed`), full test suite, and CI-ready packaging