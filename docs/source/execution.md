# Execution

SQLThunder supports robust and configurable write operations for inserting, updating, or deleting rows — with or without threading. These methods range from single-row execution to fully parallelized ingestion pipelines.

It supports DDL operations (create, alter, drop, ...) through the execute() function.

---

## Overview

| Method                                                                   | Description                                   | Best For                                                                                        |
|--------------------------------------------------------------------------|-----------------------------------------------|-------------------------------------------------------------------------------------------------|
| {py:meth}`execute <SQLThunder.core.client.DBClient.execute>`             | One-row SQL statement in a single transaction | One-off DML (insert, update, delete), DDL (ex: create, alter, drop, ...)                        |
| {py:meth}`execute_many <SQLThunder.core.client.DBClient.execute_many>`   | Bulk SQL execution in one atomic transaction  | Multi-row updates/deletes (custom SQL), all-or-nothing operations                               |
| {py:meth}`insert_many <SQLThunder.core.client.DBClient.insert_many>`     | SQL-free version of `execute_many`            | Multi-row inserts (easier, auto SQL), all-or-nothing operations                                 |
| {py:meth}`execute_batch <SQLThunder.core.client.DBClient.execute_batch>` | Parallelized SQL execution (not atomic)       | Very large, multi-row inserts/deletes/updates, flexible error handling and retry logics         |
| {py:meth}`insert_batch <SQLThunder.core.client.DBClient.insert_batch>`   | SQL-free version of `execute_batch`           | Very large, multi-row inserts (faster, easier syntax), flexible error handling and retry logics |

---

## `execute` — One-statement, One-transaction

{py:meth}`SQLThunder.core.client.DBClient.execute`

Executes a single non-SELECT SQL statement (e.g. INSERT, UPDATE, DELETE, CREATE).

```python
client.execute("DELETE FROM users WHERE id = :id", args={"id": 42})
client.execute("DELETE FROM users WHERE id = %s", args=(42,))
client.execute("CREATE TABLE trades (id INT, symbol TEXT)")
client.execute("DELETE FROM trades WHERE id = 1")
```

### Arguments

| Name             | Default  | Description                                                                                         |
|------------------|----------|-----------------------------------------------------------------------------------------------------|
| `sql`            | —        | SQL string                                                                                          |
| `args`           | `None`   | A single row of bind parameters: dictionary (named placeholders) or tuple (positional placeholders) |
| `on_duplicate`   | `None`   | Optional conflict handling for insert (`"ignore"`, `"replace"`)                                     |
| `return_failures`| `True`   | If `True`, return failure detail as DataFrame if execution fails                                    |
| `return_status`  | `False`  | If `True`, return a boolean success flag                                                            |

### Returns

- Tuple of `(failures_df or None, success_flag or None)`

---

## `execute_many` — Bulk SQL in a Single Transaction

{py:meth}`SQLThunder.core.client.DBClient.execute_many`

Executes the same SQL command over multiple rows — atomically.

```python
rows = [{"id": 1}, {"id": 2}, {"id": 3}]
client.execute_many("DELETE FROM trades WHERE id = :id", rows)
```

### Arguments

| Name             | Default  | Description                                                                                                                                                             |
|------------------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `sql`            | —        | SQL string with named or positional placeholder placeholders                                                                                                            |
| `args`           | —        | List of dictionaries (for named parameters), tuples (for positional paremeters), or a DataFrame (for named placeholders, must have same column_name in df and db table) |
| `on_duplicate`   | `None`   | Optional conflict handling for insert (`"ignore"`, `"replace"`)                                                                                                         |
| `return_failures`| `True`   | If `True`, returns a DataFrame of failed rows if any, including `error_message` and `sql`statement                                                                      |
| `return_status`  | `False`  | If `True`, return a boolean success flag                                                                                                                                |

### Returns

- Tuple of `(failures_df or None, success_flag or None)`

### Behavior

- All rows are committed in a single transaction.
- If one row fails, the entire batch fails.

---

## `insert_many` — Simplified Atomic Inserts

{py:meth}`SQLThunder.core.client.DBClient.insert_many`

Bulk insert from a Pandas DataFrame into a table, atomically.

```python
df = pd.DataFrame([{"id": 1, "symbol": "AAPL"}, {"id": 2, "symbol": "TSLA"}])
client.insert_many(df, table_name="trades")
```

### Arguments

| Name             | Default  | Description                                                                |
|------------------|----------|----------------------------------------------------------------------------|
| `df`             | —        | DataFrame of rows to insert                                                |
| `table_name`     | —        | Target table name (e.g. `"schema.table"`)                                  |
| `on_duplicate`   | `None`   | Optional conflict handling (`"ignore"`, `"replace"`)                                   |
| `return_failures`| `True`   | If `True`, returns a DataFrame of failed rows if any, including `error_message` and `sql`statement       |
| `return_status`  | `False`  | If `True`, return a boolean success flag                                       |

### Returns

- Tuple of `(failures_df or None, success_flag or None)`

### Behavior

- Internally calls `execute_many()` after auto-generating `INSERT` SQL.
- Transaction is all-or-nothing.

---

## `execute_batch` — Threaded DML (Multi-transaction)

{py:meth}`SQLThunder.core.client.DBClient.execute_batch`

Executes a large write operation (INSERT/UPDATE/DELETE) using threads.

```python
client.execute_batch(
    "UPDATE orders SET status = 'archived' WHERE id = :id",
    args=[{"id": i} for i in range(10000)],
    chunk_size=1000,
    max_workers=5
)
```

### Arguments

| Name             | Default  | Description                                                                                        |
|------------------|----------|----------------------------------------------------------------------------------------------------|
| `sql`            | —        | SQL string with named or positional placeholder placeholders                                                                    |
| `args`           | —        | List of dictionaries (for named parameters), tuples (for positional paremeters), or a DataFrame (for named placeholders, must have same column_name in df and db table)       |
| `chunk_size`     | `512`    | Number of rows per batch                                                                         |
| `max_workers`    | `None`   | Number of threads. Must be less than `pool_size + max_overflow`. Defaults to `pool_size + max_overflow` (default is 15 if `pool_size`and `max_overflow` were not specified at client initilization).       |
| `on_duplicate`   | `None`   | Optional conflict handling for insert (`"ignore"`, `"replace"`)                                    |
| `return_failures`| `True`   | If `True`, returns a DataFrame of failed rows if any, including `error_message` and `sql`statement |
| `return_status`  | `False`  | If `True`, return a boolean success flag                                                           |

### Returns

- Tuple of `(failures_df or None, success_flag or None)`

### Behavior

- Not atomic — each chunk commits independently.
- Suitable for large ETL operations where performance > rollback.
- Useful for custom error handling of subsets of failed rows.

---

## `insert_batch` — Threaded Bulk Insert

{py:meth}`SQLThunder.core.client.DBClient.insert_batch`

Inserts a DataFrame into a SQL table in parallel chunks.

```python
client.insert_batch(df, table_name="trades", chunk_size=1000, max_workers=8)
```

### Arguments

| Name             | Default  | Description                                                                                        |
|------------------|----------|----------------------------------------------------------------------------------------------------|
| `df`             | —        | DataFrame of rows to insert                                                                        |
| `table_name`     | —        | Target table name (e.g. `"schema.table"`)                                                                       |
| `chunk_size`     | `512`    | Number of rows per batch                                                                           |
| `max_workers`    | `None`   | Number of threads. Must be less than `pool_size + max_overflow`. Defaults to `pool_size + max_overflow` (default is 15 if `pool_size`and `max_overflow` were not specified at client initilization).       |
| `on_duplicate`   | `None`   | Optional conflict handling (`"ignore"`, `"replace"`)                                                                |
| `return_failures`| `True`   | If `True`, returns a DataFrame of failed rows if any, including `error_message` and `sql`statement |
| `return_status`  | `False`  | If `True`, return a boolean success flag                                                           |

### Returns

- Tuple of `(failures_df or None, success_flag or None)`

### Behavior

- Internally wraps `execute_batch()` with autogenerated `INSERT` SQL.
- Best performance for high-volume inserts.
- Useful for custom error handling of subsets of failed rows.

---

## Summary: Atomic vs Threaded

| Method           | Atomic | Threads | Auto SQL | Best For                                                                    |
|------------------|--------|---------|----------|-----------------------------------------------------------------------------|
| `execute`        | ✅      | ❌       | ❌        | Single statement, One-row DDL/DML                                           |
| `execute_many`   | ✅      | ❌       | ❌        | Multi-row DML, all-or-nothing                                               |
| `insert_many`    | ✅      | ❌       | ✅        | Easy-to-use Multi-row INSERT (DataFrame), all-or-nothing                    |
| `execute_batch`  | ❌      | ✅       | ❌        | High-performance batch DML, custom error handling, flexible                 |
| `insert_batch`   | ❌      | ✅       | ✅        | Easy-to-use, Fastest INSERT from DataFrame, custom error handling, flexible |

---

## Error Handling in Execution

SQLThunder's execution functions (`execute`, `execute_many`, `insert_many`, `execute_batch`, `insert_batch`) are designed to **not raise exceptions by default** on database-level errors (e.g. syntax error, bad column, constraint violation). Instead:

- The function will return normally.
- If `return_status=True`, you’ll get a `False` success flag.
- If `return_failures=True`, a DataFrame will be returned with one or more rows containing an `error_message` column with the error details.

### Example

```python
df_failed, success = client.execute_many(
    sql="UPDATE trades SET price = :price WHERE id = :id",
    args=[{"id": 1, "price": 120}],
    return_failures=True,
    return_status=True,
)

if not success:
    print("Operation failed:", df_failed["error_message"].iloc[0])
    raise RuntimeError("Write operation failed.")
```

This design allows for **graceful error recovery** and **manual control** over exception handling — especially useful in pipelines or automated systems.

---

## Note

All the execute methods support both positional and named arguments in the SQL statement. They support:
Named placeholders:
- `:name`
- `%(name)s`

Positional placeholder:
- `%s`
- `?` 
- `:param1, :param2, ...`
---

## Next Steps

- API Reference: {py:meth}`execute <SQLThunder.core.client.DBClient.execute>`
- API Reference: {py:meth}`execute_many <SQLThunder.core.client.DBClient.execute_many>`
- API Reference: {py:meth}`insert_many <SQLThunder.core.client.DBClient.insert_many>`
- API Reference: {py:meth}`execute_batch <SQLThunder.core.client.DBClient.execute_batch>`
- API Reference: {py:meth}`insert_batch <SQLThunder.core.client.DBClient.insert_batch>`
- [CLI usage](cli.md)
- [Examples](examples.md)
