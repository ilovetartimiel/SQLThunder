### --- Standard library imports --- ###
import argparse
import logging
import sys

### --- Internal package imports --- ###
from .core import DBClient
from .exceptions import (
    BaseSQLConversionError,
    ConfigFileError,
    DataFileLoadError,
    DBClientError,
    FileSaveError,
    SQLExecutionError,
    ThreadPoolLimitError,
)
from .logging_config import configure_logging
from .utils.file_io import load_data, save_dataframe

KNOWN_ERRORS = (
    ConfigFileError,
    ThreadPoolLimitError,
    DBClientError,
    SQLExecutionError,
    BaseSQLConversionError,
    FileSaveError,
    DataFileLoadError,
)

### --- CLI --- ###


def main() -> None:
    """
    Command-line interface for SQLThunder.

    Supports the following subcommands:
        - query
        - insert
        - execute

    Each command operates on a YAML database config file and optionally supports
    threaded batch operations.

    Example usage:
        $ sqlthunder query 'SELECT * FROM table' -c config.yaml
        $ sqlthunder query 'SELECT * FROM table' -c config.yaml --verbose
        $ sqlthunder insert data.xlsx schema.table -c config.yaml --batch
        $ sqlthunder execute 'DELETE FROM logs' -c config.yaml
    """
    ### --- Main parser --- ###
    # Pre-parse global flags anywhere in the CLI (--verbose)
    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument(
        "--verbose", action="store_true", help="Enable DEBUG logging."
    )

    known_args, remaining_args = pre_parser.parse_known_args()
    log_level = logging.DEBUG if known_args.verbose else logging.WARNING
    configure_logging(level=log_level)

    # Main parser
    parser = argparse.ArgumentParser(
        description="Threaded SQL Database CLI for querying, inserting, and executing SQL.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        parents=[pre_parser],  # Inherit global flags like --verbose
    )

    ### --- Subparsers --- ###

    subparsers = parser.add_subparsers(dest="command", required=True)

    ### --- Query parser (query, query_keyed & query_batch) --- ###

    # Base
    query_parser = subparsers.add_parser(
        "query", help="Run SELECT query (with batch/key-based options)."
    )
    query_parser.add_argument("sql", type=str, help="SQL SELECT statement.")
    query_parser.add_argument(
        "-c", "--config_path", type=str, required=True, help="Path to DB config YAML."
    )
    query_parser.add_argument(
        "--print", action="store_true", help="Print the result to stdout."
    )
    query_parser.add_argument(
        "--print_limit", type=int, default=10, help="Max rows to print. Default: 10."
    )
    query_parser.add_argument(
        "--output", choices=["csv", "excel"], help="Output format."
    )
    query_parser.add_argument(
        "--output_path", type=str, help="Where to save output file."
    )

    # Key-based pagination
    query_parser.add_argument(
        "--key_based", action="store_true", help="Use key-based pagination."
    )
    query_parser.add_argument(
        "--key_column",
        type=str,
        default=None,
        help="For key_based mode, Primary key column name.",
    )
    query_parser.add_argument(
        "--key_column_type",
        choices=["int", "string", "date"],
        help="For key_based mode, Primary Key column type.",
    )
    query_parser.add_argument(
        "--start_key",
        default=None,
        help="For key_based mode, Start value for key-based pagination.",
    )
    query_parser.add_argument(
        "--order",
        choices=["asc", "desc"],
        default="asc",
        help="For key_based mode, Sort direction for the key-based pagination.",
    )

    # Key-based and batch
    query_parser.add_argument(
        "--chunk_size",
        type=int,
        default=None,
        help="Rows per chunk (batch/key_based mode only). Default: 10,000",
    )

    # Batch
    query_parser.add_argument(
        "--batch", action="store_true", help="Use offset-based chunking."
    )
    query_parser.add_argument(
        "--pool_size",
        type=int,
        default=None,
        help="For batch mode, Size of connection pool_size.",
    )
    query_parser.add_argument(
        "--max_overflow",
        type=int,
        default=None,
        help="For batch mode, Size of connection max_overflow.",
    )
    query_parser.add_argument(
        "--max_workers",
        type=int,
        default=15,
        help="For batch mode, Thread count. Should not be greater than pool_size+max_overflow",
    )

    ### --- Insert parser (insert_many & insert_batch) --- ###

    # Base
    insert_parser = subparsers.add_parser(
        "insert", help="Insert data from file into SQL table."
    )
    insert_parser.add_argument(
        "file_path", type=str, help="Path to input CSV or Excel file."
    )
    insert_parser.add_argument(
        "table_name",
        type=str,
        help="Target table name. Format is schema.table_name (just table name if no schema)",
    )
    insert_parser.add_argument(
        "-c", "--config_path", type=str, required=True, help="Path to DB config YAML."
    )

    insert_parser.add_argument(
        "--on_duplicate",
        default=None,
        choices=["ignore", "replace"],
        help="Behavior for duplicate handling during insert. Default: None.",
    )
    insert_parser.add_argument(
        "--output",
        choices=["csv", "excel"],
        help="Save failed rows to file. Requires --output_path to be given",
    )
    insert_parser.add_argument(
        "--output_path",
        type=str,
        help="Path to save failed rows. Requires --output argument to be given.",
    )

    # Batch
    insert_parser.add_argument(
        "--batch", action="store_true", help="Enable threaded chunked insert."
    )
    insert_parser.add_argument(
        "--chunk_size",
        type=int,
        default=None,
        help="Rows per chunk (batch mode only), Default: 512",
    )
    insert_parser.add_argument(
        "--pool_size",
        type=int,
        default=None,
        help="For batch mode, Size of connection pool_size.",
    )
    insert_parser.add_argument(
        "--max_overflow",
        type=int,
        default=None,
        help="For batch mode, Size of connection max_overflow.",
    )
    insert_parser.add_argument(
        "--max_workers",
        type=int,
        default=None,
        help="For batch mode, Thread count. Should not be greater than pool_size+max_overflow",
    )

    ### --- Execute parser (Other SQL ops) --- ###

    execute_parser = subparsers.add_parser(
        "execute", help="Run one non-SELECT SQL statement."
    )
    execute_parser.add_argument("sql", type=str, help="SQL statement.")
    execute_parser.add_argument(
        "-c", "--config_path", type=str, required=True, help="Path to DB config YAML."
    )

    ### --- Check user arguments --- ###

    # Parse CLI args
    args = parser.parse_args()

    # Check max_worker and pool_size + max_overflow logic
    pool_given = False
    pool_size_: int
    max_overflow_: int

    if args.command in {"query", "insert"}:

        if (args.pool_size is None) != (args.max_overflow is None):
            parser.error(
                "Both --pool_size and --max_overflow must be provided together."
            )

        if args.pool_size is not None and args.max_overflow is not None:
            pool_size_ = args.pool_size
            max_overflow_ = args.max_overflow
            max_allowed = pool_size_ + max_overflow_
            pool_given = True
        else:
            max_allowed = 15

        # Only check this if the command uses workers
        if hasattr(args, "max_workers") and args.max_workers is not None:
            if args.max_workers > max_allowed:
                parser.error(
                    f"--max_workers={args.max_workers} exceeds allowed maximum of {max_allowed} "
                    f"(pool_size + max_overflow)."
                )

        # Enforce conditional argument requirements for output
        if (args.output and not args.output_path) or (
            args.output_path and not args.output
        ):
            parser.error("--output and --output_path must be used together.")

    # Enforce start_key if --key_based for query and key_column_type is "string".
    # Enforce key_column + key_column_type
    if (args.command in {"query"}) and args.key_based:
        if not args.key_column:
            parser.error("--key_based requires --key_column to be given.")
        if not args.key_column_type:
            parser.error("--key_based requires --key_column_type to be given.")
        if (args.key_column_type in {"string", "date"}) and not args.start_key:
            parser.error(
                "--key_based requires --start_key to be given when used with --key_column_type 'string'."
            )

    ### --- Run Command --- ###

    # Run command
    try:
        # Setup logger
        log_level = logging.DEBUG if args.verbose else logging.WARNING
        configure_logging(level=log_level)

        # Setup DBClient
        if pool_given:
            client = DBClient(
                config_file_path=args.config_path,
                pool_size=args.pool_size,
                max_overflow=args.max_overflow,
            )
        else:
            client = DBClient(config_file_path=args.config_path)

        ### --- Query --- ###

        if args.command == "query":
            if args.batch:
                result = client.query_batch(
                    sql=args.sql,
                    chunk_size=args.chunk_size,
                    max_workers=args.max_workers,
                    return_type="df",
                    print_result=args.print,
                    print_limit=args.print_limit,
                )
            elif args.key_based:
                result = client.query_keyed(
                    sql=args.sql,
                    key_column=args.key_column,
                    key_column_type=args.key_column_type,
                    order=args.order,
                    start_key=args.start_key,
                    return_type="df",
                    print_result=args.print,
                    print_limit=args.print_limit,
                )
            else:
                result = client.query(
                    sql=args.sql,
                    return_type="df",
                    print_result=args.print,
                    print_limit=args.print_limit,
                )
            # Save df to excel or csv at specified path
            if args.output and args.output_path:
                save_dataframe(result, args.output, args.output_path)

        ### --- Insert --- ###

        elif args.command == "insert":
            data = load_data(args.file_path)
            if args.batch:
                failed, _ = client.insert_batch(
                    df=data,
                    table_name=args.table_name,
                    chunk_size=args.chunk_size,
                    max_workers=args.max_workers,
                    on_duplicate=args.on_duplicate,
                )
            else:
                failed, _ = client.insert_many(
                    df=data, table_name=args.table_name, on_duplicate=args.on_duplicate
                )

            if (
                args.output
                and args.output_path
                and failed is not None
                and not failed.empty
            ):
                save_dataframe(failed, args.output, args.output_path)

        ### --- Execute --- ###

        elif args.command == "execute":
            client.execute(sql=args.sql)
            print("SQL statement executed successfully.")

    # Errors
    except KNOWN_ERRORS as e:
        print(f"Error: {e}")
        sys.exit(1)

    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


# Run logic
if __name__ == "__main__":
    main()
