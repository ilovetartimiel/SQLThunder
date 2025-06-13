# CLI

SQLThunder includes a powerful CLI for database interactions without writing Python code.

You can use it to:
- Run SELECT queries with or without chunking
- Insert data from CSV/Excel files
- Execute DDL or DML statements

---

## Overview

| Command      | Description                                         | Typical Use Case                                |
|--------------|-----------------------------------------------------|-------------------------------------------------|
| `query`      | Run SELECT statements (full table, batch, or keyed) | Exports, dashboards, data previews              |
| `insert`     | Insert rows from file (batch or atomic)             | Ingesting data into SQL from Excel/CSV          |
| `execute`    | Run one non-SELECT SQL command                      | Table creation, delete, truncate, alter, etc.   |

Use via:

```bash
sqlthunder <command> [options]
```

---

## `query` — Run a SELECT Query

Supports standard queries, offset-based chunking (`--batch`), or key-based pagination (`--key_based`).

```bash
sqlthunder query "SELECT * FROM trades" -c config.yaml
```

### Common options

| Option             | Default       | Description                                                   |
|--------------------|---------------|---------------------------------------------------------------|
| `sql`              | —             | SELECT query string                                           |
| `-c, --config_path`| —             | Path to YAML config                                           |
| `--print`          | `False`       | Print result to stdout                                        |
| `--print_limit`    | `10`          | Max rows to print                                             |
| `--output`         | —             | Save format: `"csv"` or `"excel"`                             |
| `--output_path`    | —             | Path to save result file                                      |

### Batch mode options (`--batch`)

| Option             | Default | Description                                                                          |
|--------------------|---------|--------------------------------------------------------------------------------------|
| `--batch`          | `False` | Use offset-based chunking                                                            |
| `--chunk_size`     | `10000` | Rows per chunk                                                                       |
| `--max_workers`    | `15`    | Max threads for parallel reads  (must be less than `--pool_size` + `--max_overflow`) |
| `--pool_size`      | `10`    | SQLAlchemy connection pool size.                                                                      |
| `--max_overflow`   | `5`      | Max overflow connections beyond pool.                                                                |

### Key-based mode options (`--key_based`)

| Option                 | Default   | Description                                                    |
|------------------------|-----------|----------------------------------------------------------------|
| `--key_based`          | `False`   | Use key-based pagination                                       |
| `--key_column`         | —         | Primary key column name                                        |
| `--key_column_type`    | —         | `"int"`, `"string"`, or `"date"`                               |
| `--start_key`          | —         | Start value for key-based pagination                           |
| `--order`              | `"asc"`   | Sort direction                                                 |

---

## `insert` — Insert from CSV or Excel

```bash
sqlthunder insert data.xlsx my_schema.my_table -c config.yaml
```

### Common options

| Option             | Default       | Description                                                   |
|--------------------|---------------|---------------------------------------------------------------|
| `file_path`        | —             | Input CSV or Excel file                                       |
| `table_name`       | —             | Target table name (e.g. `"schema.table"`)                        |
| `--on_duplicate`   | —             | Optional conflict handling (`"ignore"`, `"replace"`)           |
| `--output`         | —             | `"csv"` or `"excel"` for failed row output                    |
| `--output_path`    | —             | Path to write failed rows                                     |

### Batch mode options (`--batch`)

| Option             | Default | Description                                                                            |
|--------------------|---------|----------------------------------------------------------------------------------------|
| `--batch`          | `False` | Use threaded insert                                                                    |
| `--chunk_size`     | `512`   | Rows per chunk                                                                         |
| `--max_workers`    | `15`    | Max threads for parallel inserts  (must be less than `--pool_size` + `--max_overflow`) |
| `--pool_size`      | `10`    | SQLAlchemy connection pool size.                                                                    |
| `--max_overflow`   | `5`     | Max overflow connections beyond pool.                                                         |

---

## `execute` — Run a single DDL or DML

```bash
sqlthunder execute "CREATE TABLE users (id INT)" -c config.yaml
```

| Option             | Default       | Description                                                   |
|--------------------|---------------|---------------------------------------------------------------|
| `sql`              | —             | Any SQL statement (non-SELECT)                                |
| `-c, --config_path`| —             | Path to YAML config                                           |

---

## Global Flags

| Flag               | Description                                                   |
|--------------------|---------------------------------------------------------------|
| `--verbose`        | Enables `DEBUG` logging for detailed runtime feedback         |

---

## Error Handling

- Invalid combinations of `--output` and `--output_path` will raise CLI errors.
- For `--key_based`, `--key_column` and `--key_column_type` are required.
- For `"string"` and `"date"` keys, `--start_key` is mandatory.
- `--max_workers` is validated against `pool_size + max_overflow`.

---

## Next Steps

- [Examples](examples.md)
