# Examples

This guide demonstrates typical SQLThunder workflows using both **Python code** and the **CLI interface**. These examples assume you have a valid YAML config file (see [Configuration](configuration.md)) and a running SQL database.

---

## Querying Data

### Python (basic)

```python
from SQLThunder import DBClient

client = DBClient("config.yaml")
df = client.query("SELECT * FROM trades WHERE symbol = 'AAPL'")
print(df.head())
```

### CLI

```bash
sqlthunder query "SELECT * FROM trades WHERE symbol = 'AAPL'" -c config.yaml --print
```

---

## Key-based Chunked Query

Useful for reading very large tables in slices based on a primary key.

### Python

```python
df, success = client.query_keyed(
    sql="SELECT * FROM trades",
    key_column="id",
    key_column_type="int",
    start_key=0,
    chunk_size=10000,
    return_status=True
)

if not success:
    raise RuntimeError("Keyed query failed.")
```

### CLI

```bash
sqlthunder query "SELECT * FROM trades" -c config.yaml --key_based --key_column id --key_column_type int --start_key 0
```

---

## Offset-based Batch Query

### Python

```python
df = client.query_batch(
    "SELECT * FROM orders WHERE status = 'open'"
)
```

### CLI

```bash
sqlthunder query "SELECT * FROM orders WHERE status = 'open'" -c config.yaml --batch --chunk_size 5000 --max_workers 10
```

---

## Insert Data from Excel

### CLI

```bash
sqlthunder insert data.xlsx my_schema.my_table -c config.yaml --on_duplicate ignore
```

### Python

```python
import pandas as pd
from SQLThunder import DBClient

df = pd.read_excel("data.xlsx")
client = DBClient("config.yaml")
client.insert_many(df, "my_schema.my_table", on_duplicate="ignore")
```

---

## Fast Insert (Threaded)

### Python

```python
client.insert_batch(
    df,
    table_name="my_schema.my_table",
    chunk_size=1000,
    max_workers=8
)
```

### CLI

```bash
sqlthunder insert data.xlsx my_schema.my_table -c config.yaml --batch --chunk_size 1000 --max_workers 8
```

---

## Run a DDL or DML Statement

### Python

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

### CLI

```bash
sqlthunder execute "DELETE FROM logs WHERE level = 'DEBUG'" -c config.yaml
```

---

## Save Query Output to File

```bash
sqlthunder query "SELECT * FROM users" -c config.yaml --output csv --output_path results/users.csv
```

---

## Save Failed Inserts to File

```bash
sqlthunder insert data.xlsx my_schema.my_table -c config.yaml --output excel --output_path errors/failures.xlsx
```

---

## See Also

- [Querying](querying.md)
- [Execution](execution.md)
- [CLI](cli.md)
