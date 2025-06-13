# SQLThunder

[![PyPI Version](https://img.shields.io/pypi/v/sqlthunder.svg)](https://pypi.org/project/sqlthunder/)
[![Python Versions](https://img.shields.io/pypi/pyversions/sqlthunder.svg)](https://pypi.org/project/sqlthunder/)
[![License](https://img.shields.io/github/license/ilovetartimiel/SQLThunder)](https://github.com/ilovetartimiel/SQLThunder/blob/main/LICENSE)
[![Docs](https://readthedocs.org/projects/sqlthunder/badge/?version=latest)](https://sqlthunder.readthedocs.io/en/latest/)
[![codecov](https://codecov.io/gh/ilovetartimiel/SQLThunder/branch/main/graph/badge.svg)](https://codecov.io/gh/ilovetartimiel/SQLThunder)
[![Integration and Unit Tests](https://github.com/ilovetartimiel/SQLThunder/actions/workflows/tests.yaml/badge.svg)](https://github.com/ilovetartimiel/SQLThunder/actions/workflows/tests.yaml)
[![Pre-Commit Checks](https://github.com/ilovetartimiel/SQLThunder/actions/workflows/pre-commit.yaml/badge.svg)](https://github.com/ilovetartimiel/SQLThunder/actions/workflows/pre-commit.yaml)

---

## What is it ?

**SQLThunder** is a fast, flexible SQL client for Python designed for real-world workloads across **PostgreSQL**, **MySQL**, and **SQLite**. It offers a cleaner, high-level API over SQLAlchemy, with support for multi-threaded operations, flexible result formats, YAML-based configuration, and a built-in CLI.

The best thing about **SQLThunder**: no boilerplate code !

---

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Example Usage](#example-usage)
- [CLI Usage](#cli-usage)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

---

## Features

- **Threaded execution** for fast inserts, updates, and deletes — designed for large-scale data workflows
- **Declarative inserts** with automatic SQL generation from dictionaries or DataFrames
- **Key-based pagination** and **chunked queries** for scalable data extraction
- **Flexible result formats**: return data as `pandas.DataFrame`, list of rows, or raw SQLAlchemy objects
- **Graceful error handling** with optional success flags and failure DataFrames
- **YAML-based configuration** for clean, environment-agnostic connection setup
- **Built-in CLI** for quick querying, inserting, and executing SQL without writing code
- **Full test coverage** including unit tests, integration tests, and Dockerized database backends
- **Minimal boilerplate** — write less code and get more done

---

## Installation

```bash
pip install sqlthunder
```

---

## Example Usage

```python
from SQLThunder import DBClient

client = DBClient("config.yaml")
df = client.query("SELECT * FROM trades WHERE symbol = 'AAPL')
```

```python
import pandas as pd
from SQLThunder import DBClient

df = pd.read_excel("data.xlsx")
client = DBClient("config.yaml")
failed_rows, success_flag = client.insert_batch(df, "my_schema.my_table", on_duplicate="ignore", return_status=True)
```

```python
from SQLThunder import DBClient

client = DBClient("config.yaml")
client.execute(
    """
    CREATE TABLE IF NOT EXISTS trades (
        id INT PRIMARY KEY,
        symbol VARCHAR(10) NOT NULL,
        price DECIMAL(10, 2) NOT NULL,
        trade_time DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """
)
```
---

## CLI Usage

```bash
# Run a query
sqlthunder query "SELECT * FROM trades" -c config.yaml --print

# Insert from file
sqlthunder insert data.xlsx schema.table -c config.yaml

# Run DDL or DML
sqlthunder execute "DELETE FROM logs" -c config.yaml
```

---

## Documentation

https://sqlthunder.readthedocs.io

---

## Contributing

See [CONTRIBUTE.txt](CONTRIBUTE.txt) to get started. PRs and feedback are welcome!

---

## License

MIT — see [LICENSE](LICENSE) for details.
