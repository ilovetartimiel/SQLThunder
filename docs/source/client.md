# DBClient

The `DBClient` class is the central interface for all database interactions in SQLThunder. It wraps SQLAlchemy with built-in thread pooling, error handling, and configuration loading.

SQLThunder supports **MySQL**, **PostgreSQL**, and **SQLite** through a single unified client.

---

## Client Initialization

```python
from sqlthunder import DBClient

client = DBClient("config/postgres.yaml")
```

### Required parameters:

| Argument        | Description                                                                 |
|----------------|-----------------------------------------------------------------------------|
| `config_file_path` | Path to the YAML config file. Required.                                |

### Optional parameters:

| Argument        | Description                                                                                                                    |
|----------------|--------------------------------------------------------------------------------------------------------------------------------|
| `db_type`       | Optional override for DB type (`mysql`, `postgresql`, `sqlite`) if not specified in config file.                               |
| `pool_size`     | SQLAlchemy connection pool size. Default: 10.                                                                                  |
| `max_overflow`  | Max overflow connections beyond pool. Default: 5.                                                                              |
| `max_workers`   | Thread pool size for parallel operations. Defaults to `pool_size + max_overflow`. Must be less than `pool_size + max_overflow` |

---

## Connection Lifecycle

SQLThunder opens a persistent connection pool and thread executor at startup. You are responsible for managing the client lifecycle.

### Close the client:

```python
client.close()
```

- Shuts down the engine and thread pool.
- After closing, all operations will raise a `DBClientClosedError`.

### Reopen a closed client:

```python
client.reopen_connection()
```

- Re-initializes the engine and thread pool.

### Check if closed:

```python
client.is_closed  # True or False
```

### Validate the connection:

```python
client.test_connection()
```

---

## Using DBSession (Context Manager)

For more structured workflows or temporary usage, wrap the client in a `DBSession`:

```python
from sqlthunder import DBSession

with DBSession(client, label="load-prices", auto_close=True) as db:
    df = db.query("SELECT * FROM prices")
```

### DBSession Options

| Argument        | Description                                                             |
|----------------|-------------------------------------------------------------------------|
| `client`        | A `DBClient` instance.                                                  |
| `label`         | Optional name for logging/debugging. Default: `"UnnamedSession"`.       |
| `auto_close`    | Whether to automatically call `.close()` on exit. Default: `False`.     |
| `auto_reopen`   | Whether to automatically reopen a closed client on entry. Default: `False`. |

If `auto_reopen=False` and the client is closed, `DBClientSessionClosedError` will be raised.

---

## Public Methods Overview

These are the key public methods for managing DBClient lifecycle:

### `test_connection() -> bool`
Checks if the client can successfully connect to the database.

### `reopen_connection() -> None`
Recreates engine and thread pool after shutdown. Use after `close()`.

### `close() -> None`
Shuts down the engine and thread executor. Marks the client as unusable.

### `is_closed: bool`
Property indicating whether the client has been closed.

---

## Notes

- All operations on a closed client will raise `DBClientClosedError`.
- Use `DBSession` when possible for cleaner control of session lifetimes.
- The client automatically validates config paths, SSL certificates, and connection URLs during init.

For querying, executing DDL/DML statements, inserting, batch operations, and CLI usage, see:

- [Querying](querying.md)
- [Execution (DDL/DML)](execution.md)
- [CLI Guide](cli.md)
