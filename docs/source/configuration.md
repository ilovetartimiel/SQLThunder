# Configuration

SQLThunder uses YAML configuration files to manage database connection settings across SQLite, MySQL, and PostgreSQL. This allows clean separation of credentials and connection logic from your code.

---

## General Structure

At a minimum, each config file must include the database type and required connection fields for that database. Optional fields such as SSL certificates, timeouts, and PostgreSQL options can be added for advanced use cases.

---

## Supported Fields

| Field              | Description                                  | Required         |
|-------------------|----------------------------------------------|------------------|
| `db_type`         | `"sqlite"`, `"mysql"`, or `"postgresql"`     | ✅                |
| `path`            | Path to SQLite file (or `":memory:"`)        | ✅ (sqlite)       |
| `user`            | Database username                            | ✅ (mysql/pgsql)  |
| `password`        | Database password                            | ✅ (mysql/pgsql)  |
| `host`            | Database host                                | ✅ (mysql/pgsql)  |
| `port`            | Port (default: 3306 for MySQL, 5432 for PG)  | optional (mysql/pgsql) |
| `database`        | Database name                                | ✅ (mysql/pgsql)  |
| `ssl_mode`        | PostgreSQL SSL mode (`disable`, `require`, `verify-ca`, etc.) | optional (pgsql) |
| `ssl_ca`          | Path to CA certificate (SSL)                 | optional (mysql/pgsql) |
| `ssl_cert`        | Path to client certificate (SSL)             | optional (mysql/pgsql) |
| `ssl_key`         | Path to client private key (SSL)             | optional (mysql/pgsql) |
| `connect_timeout` | Connection timeout in seconds                | optional (mysql/pgsql) |
| `read_timeout`    | MySQL read timeout                           | optional (mysql) |
| `write_timeout`   | MySQL write timeout                          | optional (mysql) |
| `application_name`| PostgreSQL application name                  | optional (pgsql) |
| `pg_options`      | PostgreSQL connection options                | optional (pgsql) |

---

## SQLite Example

```yaml
db_type: "sqlite"
path: "data/my_database.sqlite"
```

For in-memory databases:

```yaml
db_type: "sqlite"
path: ":memory:"
```

---

## Basic MySQL Example

```yaml
db_type: "mysql"
user: "admin"
password: "secure123"
host: "localhost"
port: 3306
database: "trades"
```

---

## MySQL Example with SSL and Options

```yaml
db_type: "mysql"
user: "admin"
password: "secure123"
host: "localhost"
port: 3306
database: "trades"
connect_timeout: 15
read_timeout: 60
write_timeout: 60
ssl_ca: "~/certs/ca.pem"
ssl_cert: "~/certs/client-cert.pem"
ssl_key: "~/certs/client-key.pem"
```

---

## PostgreSQL Example with Advanced Options

```yaml
db_type: "postgresql"
user: "postgres"
password: "secure123"
host: "db.prod.local"
port: 5432
database: "analytics"
connect_timeout: 10
ssl_mode: "verify-ca"
ssl_ca: "~/certs/ca.pem"
ssl_cert: "~/certs/client-cert.pem"
ssl_key: "~/certs/client-key.pem"
application_name: "sqlthunder-worker"
pg_options: "-c statement_timeout=30000 -c search_path=myschema"
```

You can pass multiple `pg_options` like setting a custom search path or statement timeout.

---

## Notes

- Paths like `~/certs/ca.pem` are automatically expanded to absolute paths.
- Missing or malformed fields will raise helpful errors at runtime.
- SSL certificates are only validated if explicitly provided.
- PostgreSQL `ssl_mode` overrides psycopg2 `prefer` default behavior (e.g. `require`, `verify-ca`, `verify-full`).

## Next Steps

- [Querying](querying.md)
- [Executing (DDL/DML)](execution.md)
- [CLI usage](cli.md)
- [Examples](examples.md)