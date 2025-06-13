# Installation

**SQLThunder** is a flexible, high-level SQL client for Python that simplifies and accelerates interactions with SQLite, MySQL, and PostgreSQL databases. It uses SQLAlchemy under the hood, but exposes a cleaner, faster API with threading support and YAML-based configuration.

---

## Requirements

- Python ≥ 3.9
- [SQLAlchemy](https://www.sqlalchemy.org/)
- [pandas](https://pandas.pydata.org/)
- [PyYAML](https://pypi.org/project/PyYAML/)
- [sqlparse](https://pypi.org/project/sqlparse/)
- Database drivers (PyMySQL, psycopg2)

---

## Install via pip

```bash
pip install sqlthunder
```

This installs the core SQLThunder package with support for all core features.

---

## Development install (editable + dev tools)

```bash
git clone https://github.com/ilovetartimiel/SQLThunder
cd sqlthunder
pip install -e .[dev]
```

This will install SQLThunder in editable mode with test, linting, and formatting dependencies.

---

## Verify your installation

Check that the CLI is working:

```bash
sqlthunder --help
```

You should see the list of supported subcommands: `query`, `insert`, and `execute`.

## Next Steps

- [Quickstart](quickstart.md)
- [Configuration](configuration.md)
- [Querying (READ)](querying.md)
- [Executing (DDL/DML)](execution.md)
- [CLI usage](cli.md)
- [Examples](examples.md)