import argparse
import subprocess
import sys
from pathlib import Path
from typing import Union


def load_sql_file(
    connection_uri: str,
    sql_file_path: Union[str, Path],
    verbose: bool = False,
) -> None:
    """
    Loads and executes a SQL file into a PostgreSQL database using psql.

    Uses the psql command-line tool to properly handle psql meta-commands
    like \\set, \\connect, \\restrict, etc. that psycopg2 cannot execute.

    Args:
        connection_uri: PostgreSQL connection URI string
        sql_file_path: Path to the .sql file to execute
        verbose: If True, prints progress

    Raises:
        FileNotFoundError: If the SQL file doesn't exist
        RuntimeError: If psql execution fails
    """
    sql_file_path = Path(sql_file_path)

    if not sql_file_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file_path}")

    if verbose:
        print(f"Loading SQL file via psql: {sql_file_path}")

    # Build psql command with connection URI
    # Use -a (echo all) or -e (echo queries) for progress when verbose
    cmd = [
        "psql",
        connection_uri,
        "-f",
        str(sql_file_path),
        "-v",
        "ON_ERROR_STOP=1",  # Stop on first error
    ]

    if verbose:
        cmd.extend(["-a"])  # Echo all input from script
    else:
        cmd.append("-q")  # Quiet mode

    try:
        # Use Popen for real-time output streaming
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,  # Line buffered
        )

        # Stream stdout in real-time
        if verbose:
            for line in process.stdout:
                print(line, end="")

        # Wait for completion and get return code
        process.wait()

        if process.returncode != 0:
            stderr = process.stderr.read()
            print(f"✗ Error executing SQL file: {stderr}", file=sys.stderr)
            raise RuntimeError(f"psql failed: {stderr}")

        if verbose:
            print(f"✓ Successfully executed SQL file: {sql_file_path}")

    except FileNotFoundError:
        raise RuntimeError(
            "psql command not found. Please ensure PostgreSQL client is installed."
        )


def main():
    """
    Standalone script entry point for loading SQL files into PostgreSQL.

    Usage:
        python -m util.import_db <sql_file> --host <host> --port <port> --user <user> --password <password> --database <db_name>
    """
    parser = argparse.ArgumentParser(
        description="Load a SQL file into a PostgreSQL database"
    )
    parser.add_argument("sql_file", type=str, help="Path to the .sql file")
    parser.add_argument(
        "--host", type=str, default="localhost", help="Database host"
    )
    parser.add_argument("--port", type=int, default=5432, help="Database port")
    parser.add_argument("--user", type=str, required=True, help="Database user")
    parser.add_argument(
        "--password", type=str, default="", help="Database password"
    )
    parser.add_argument(
        "--database", "-d", type=str, required=True, help="Database name"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Verbose output"
    )

    args = parser.parse_args()

    # Build connection URI
    if args.password:
        connection_uri = f"postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.database}"
    else:
        connection_uri = (
            f"postgresql://{args.user}@{args.host}:{args.port}/{args.database}"
        )

    try:
        if args.verbose:
            print(
                f"Connecting to database: {args.database} at {args.host}:{args.port}"
            )

        # Load the SQL file using psql
        load_sql_file(connection_uri, args.sql_file, verbose=args.verbose)

        sys.exit(0)

    except FileNotFoundError as e:
        print(f"File error: {e}", file=sys.stderr)
        sys.exit(1)
    except RuntimeError as e:
        print(f"Database error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
