# Quickstart

SQLThunder is designed to be fast, simple, and robust — without the overhead of full ORMs or manual SQL boilerplate.

This guide helps you get started with basic configuration, querying, and data insertion in just a few lines.

After that, go explore the documentation, as SQLThunder as much more to offer !

---

## Step 1 — Write a config file

Create a YAML file to store your database connection info.

### SQLite (local file)

```yaml
db_type: "sqlite"
path: "data/mydb.sqlite"
```

### MySQL

```yaml
db_type: "mysql"
user: "root"
password: "mypassword"
host: "localhost"
database: "test_db"
port: 3306
```

### PostgreSQL

```yaml
db_type: "postgresql"
user: "postgres"
password: "mypassword"
host: "localhost"
database: "mydb"
port: 5432
connect_timeout: 10
```

---

## Step 2 — Connect to the database

### From a YAML config

```python
from sqlthunder import DBClient

client = DBClient("config/mysql.yaml")
```

---

## Step 3 — Create table

```python
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

## Step 4 — Insert rows

Besides what is shown below for INSERT, you can check out execute_many and execute_batch for other DML operations with bulk arguments.

### Insert multiple rows in one transaction

```python
data = [
    {"id": 2, "symbol": "GOOG", "price": 2750.0},
    {"id": 3, "symbol": "MSFT", "price": 299.0}
]
client.insert_many("trades", data)
```

### Or use multiple transactions in chunks with threaded batch insert

```python
client.insert_batch("trades", data * 1000, chunk_size=500, max_workers=4)
```

Threads make large inserts faster and more fault-tolerant.

---

## Step 5 — Run a query

```python
df = client.query("SELECT * FROM trades LIMIT 10")
print(df)
```

You can choose your return type: `dataframe` (default), `list`, or `raw`.

You also have other query options for larger tables (query_keyed and query_batch).

---

## Step 6 — Use the CLI

SQLThunder includes a built-in CLI for fast, scriptable access:

```bash
sqlthunder query "SELECT COUNT(*) FROM trades" -c config/mysql.yaml
```

You can also insert (from CSV or Excel file) or execute statements from the terminal.

---

## Next Steps

- [Configuration](configuration.md)
- [Querying (READ)](querying.md)
- [Executing (DDL/DML)](execution.md)
- [CLI usage](cli.md)
- [Examples](examples.md)
