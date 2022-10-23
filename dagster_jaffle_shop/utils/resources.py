import duckdb
import pandas
import pathlib

from dagster import resource, get_dagster_logger, InitResourceContext

DUCKDB_FILE = "/tmp/duckdb/dagster.duckdb"
logger = get_dagster_logger()


class DuckDBResource:
    """
    Utility class for executing queries against a DuckDB database.
    This is mainly useful so that we know connections are made within
    the operations actually executed.
    """

    def __init__(self, database: str):
        self.database = database

    def execute_query(self, query: str, read_only=True) -> pandas.DataFrame:
        with duckdb.connect(database=self.database, read_only=read_only) as conn:
            logger.info("Executing query: %s", query)
            df = conn.execute(query).fetch_df()

        if not "df" in locals():
            raise Exception("There was an error with the query!")

        return df


@resource
def duckdb_resource(context: InitResourceContext) -> duckdb.DuckDBPyConnection:
    "The DuckDB resource yields a read-only connection to a local database."

    db_file = pathlib.Path(DUCKDB_FILE)
    yield DuckDBResource(database=db_file.as_posix())
