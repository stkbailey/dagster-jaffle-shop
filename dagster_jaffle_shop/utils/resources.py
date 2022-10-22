import duckdb
import pathlib

from dagster import resource, InitResourceContext

DUCKDB_FILE = "/tmp/duckdb/dagster.duckdb"


@resource
def duckdb_resource(context: InitResourceContext) -> duckdb.DuckDBPyConnection:
    "The DuckDB resource yields a read-only connection to a local database."

    db_file = pathlib.Path(DUCKDB_FILE)

    if not db_file.exists():
        context.log.info("Database file does not exist. Creating...")
        con = duckdb.connect(database=db_file.as_posix())
        con.close()

    yield duckdb.connect(database=db_file.as_posix(), read_only=True)
