# Querying

SQLThunder provides three methods for executing `SELECT` queries, each optimized for different use cases and data volumes:

---

## Overview

| Method                                                      | Description                                               | Best For                               |
|-------------------------------------------------------------|-----------------------------------------------------------|----------------------------------------|
| {py:meth}`query <SQLThunder.core.client.DBClient.query>`    | One-shot SELECT query                      | Small to medium result sets            |
| {py:meth}`query_batch <SQLThunder.core.client.DBClient.query_batch>` | Parallel chunking with LIMIT/OFFSET        | Large tables without a primary key     |
| {py:meth}`query_keyed <SQLThunder.core.client.DBClient.query_keyed>` | Key-based pagination                       | Very large tables with a sortable key  |

---

## `query` — One-shot SELECT

{py:meth}`SQLThunder.core.client.DBClient.query`

Use this for standard SELECT queries that return manageable amounts of data.

```python
df = client.query("SELECT * FROM trades WHERE symbol = :symbol", args={"symbol": "AAPL"})
```

or equivalently:

```python
df = client.query("SELECT * FROM trades WHERE symbol = 'AAPL'")
```

### Arguments

| Name           | Default     | Description                                               |
|----------------|-------------|-----------------------------------------------------------|
| `sql`          | —           | A valid `SELECT` query. Must not be DDL or DML.           |
| `args`         | `None`      | Query parameters (dict or tuple).                         |
| `return_type`  | `"df"`      | `"df"`, `"list"`, `"raw"`, or `"none"`.                   |
| `print_result` | `False`     | Whether to print a preview to stdout.                     |
| `print_limit`  | `5`         | Rows to print if `print_result=True`.                     |

### Returns

- results

### Use case
- Fastest method for complex queries or queries returning up to a couple million rows.
- Ideal for filters, aggregations, and data previews.

---

## `query_batch` — Parallelized Chunked SELECT

{py:meth}`SQLThunder.core.client.DBClient.query_batch`

Splits your SELECT query into chunks using `LIMIT` and `OFFSET` and executes them in parallel.

```python
df = client.query_batch("SELECT * FROM orders WHERE client_type = :client_type", args={"client_type": "vip"}, chunk_size=10000, max_workers=10)
```

or equivalently:

```python
df = client.query_batch("SELECT * FROM orders WHERE client_type = 'vip'", chunk_size=10000, max_workers=10)
```

### Arguments

| Name           | Default     | Description                                                                                                                                                  |
|----------------|-------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `sql`          | —           | A valid `SELECT` query. Must not be DDL or DML.                                                                                                              |
| `args`         | `None`      | Query parameters (dict or tuple).                                                                                                                            |
| `chunk_size`   | `10000`     | Rows per thread per fetch.                                                                                                                                   |
| `max_workers`  | `15`        | Number of threads. Must be less than `pool_size + max_overflow` (default is 15 if `pool_size`and `max_overflow` were not specified at client initilization). |
| `return_type`  | `"df"`      | `"df"`, `"list"`, `"raw"`, or `"none"`.                                                                                                                      |
| `print_result` | `False`     | Whether to print a preview to stdout.                                                                                                                        |
| `print_limit`  | `10`        | Rows to print if `print_result=True`.                                                                                                                        |
| `return_status`| `False`     | Whether to return a success flag. If a single or more chunk failed, then it becomes `False`, otherwise `True`. Return value becomes (result, success)        |

### Returns

- `results` if `return_status` is `False`
- Tuple of `(results, success_flag)` if `return_status` is `True`

### Why use it?

- Querying very large tables (>3M rows) with no usable key column.
- Useful even if speed is not a concern:
  - Prevents memory overload by streaming in parts.
  - Reduces risk of query timeouts or network issues.
  - Threads allow partial failure recovery.

### ⚠️ Caution

- OFFSET becomes slower with higher chunk index — avoid on huge datasets if possible.
- Always ensure your SQL **does not include LIMIT or OFFSET** manually, otherwise SQLThunder will raise QueryDisallowedClauseError.
- For long-running queries, configure timeouts:

#### MySQL example:

```yaml
read_timeout: 120
connect_timeout: 20
```

#### PostgreSQL example:

```yaml
pg_options: "-c statement_timeout=60000 -c idle_in_transaction_session_timeout=30000"
```

---

## `query_keyed` — Key-based Chunked SELECT

{py:meth}`SQLThunder.core.client.DBClient.query_keyed`

Splits the query by incrementing a key column (e.g. `id`, `timestamp`), avoiding `OFFSET`.

```python
df = client.query_keyed(
    sql="SELECT * FROM trades WHERE symbol = :symbol",
    key_column="id",
    key_column_type="int",
    start_key=0,
    chunk_size=10000,
    args={"symbol": "AAPL"}
)
```

or equivalently:

```python
df = client.query_keyed(
    sql="SELECT * FROM trades WHERE symbol = 'AAPL'",
    key_column="id",
    key_column_type="int",
    start_key=0,
    chunk_size=10000,
)
```

### Arguments

| Name              | Default     | Description                                                                                                                                                                                   |
|-------------------|-------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `sql`             | —           | A valid `SELECT` query. Must not be DDL or DML.                                                                                                                                               |
| `key_column`      | —           | The column to paginate by. Must be sortable and ideally indexed.                                                                                                                              |
| `key_column_type` | —           | `"int"`, `"string"`, or `"date"` to validate and format keys properly.                                                                                                                        |
| `args`            | `None`      | Query parameters (dict or tuple).                                                                                                                                                             |
| `start_key`       | `None`      | Lower bound (inclusive). Required for `"string"` or `"date"` keys.                                                                                                                            |
| `end_key`         | `None`      | Upper bound (inclusive). Optional.                                                                                                                                                            |
| `order`           | `"asc"`     | `"asc"` or `"desc"`. Sort direction.                                                                                                                                                          |
| `chunk_size`      | `10000`     | Rows per chunk.                                                                                                                                                                               |
| `return_type`     | `"df"`      | `"df"`, `"list"`, `"raw"`, or `"none"`.                                                                                                                                                       |
| `return_last_key` | `False`     | Return last key from result set. Useful for resuming pagination. Return value becomes (result, last_key) or (result, success, last_key)                                                       |
| `return_status`   | `False`     | Whether to return a success flag. If the query fails before reaching the last key it returns `False`, otherwise `True`. Return value becomes (result, success) or (result, success, last_key) |
| `print_result`    | `False`     | Whether to print a preview to stdout.                                                                                                                                                         |
| `print_limit`     | `10`        | Rows to print if `print_result=True`.                                                                                                                                                         |

### Returns

- `results` if `return_status` is `False`
- Tuple of `(results, success_flag)` if `return_status` is `True`
- Tuple of `(results, last_key)` if `return_last_key` is `True`
- Tuple of `(results, success_flag, last_key)` if `return_last_key` is `True` and `return_status` is `True`

### Why use it?

- Highly efficient for huge tables (millions+ rows).
- Avoids the cost of OFFSET — queries remain fast even in deep pagination.
- Recovers partial results on failure.
- Returns last key to allow seamless resume or incremental ingestion.

### Requirements

- A **unique** or **monotonically increasing** key (e.g. primary key, created_at).
- No `LIMIT` or `OFFSET` in your query, otherwise SQLThunder will raise QueryDisallowedClauseError.
- Key column type must be explicitly set to validate keys.

---

## Which Should I Use?

| If...                                     | Use             |
|-------------------------------------------|-----------------|
| Small to medium result set                | `query()`       |
| Large table with no primary key           | `query_batch()` |
| Large table with indexed primary/sort key | `query_keyed()` |

When performance isn't the goal, **querying in chunks** helps mitigate:
- Database connection timeouts
- Network instability
- Memory pressure
- Large unbuffered result sets

---

## Logging and Error Handling

By default, SQLThunder does **not raise exceptions** for query_batch() and query_keyed() during query failures caused by database errors (e.g. invalid table name, malformed SQL, or permission issues). Instead:

- The function will return normally.
- If `return_status=True` was set, the second return value (`success_flag`) will be `False`.
- The error message will be logged via Python logging.

To monitor query issues, **you should configure logging**:

```python
from SQLThunder import configure_logging
import logging

configure_logging(level=logging.WARNING)
```

This will ensure that any query failure (e.g. SQL syntax issues) will be visible in the console logs. If you want to **manually raise an exception**, you can check the `success_flag` like so:

```python
result, success = client.query_batch(..., return_status=True)
if not success:
    raise RuntimeError("Query failed. Check logs for details.")
```

The original database error will be available in the logs (stdout/stderr).

---

## Testing and Debugging Tips

- Use `print_result=True` during development to inspect query output.
- Use `print_limit=10` to preview the first few rows.
- Set `return_status=True` to confirm success without crashing pipelines.
- Log or store `return_last_key` from `query_keyed()` to resume ingestion jobs.

---

## Next Steps

- API Reference: {py:meth}`query <SQLThunder.core.client.DBClient.query>`
- API Reference: {py:meth}`query_batch <SQLThunder.core.client.DBClient.query_batch>` 
- API Reference: {py:meth}`query_keyed <SQLThunder.core.client.DBClient.query_keyed>`
- [Executing (DDL/DML)](execution.md)
- [CLI usage](cli.md)
- [Examples](examples.md)
