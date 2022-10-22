import duckdb
import pathlib

from dagster import resource, get_dagster_logger, InitResourceContext

logger = get_dagster_logger()

DUCKDB_FILE = "/tmp/duckdb/dagster.duckdb"


@resource
def duckdb_resource(init_context: InitResourceContext):

    db_file = pathlib.Path(DUCKDB_FILE)

    if not db_file.exists():
        logger.info("Database file does not exist. Creating...")
        con = duckdb.connect(database=db_file.as_posix())
        con.close()

    return duckdb.connect(database=db_file.as_posix(), read_only=False)
