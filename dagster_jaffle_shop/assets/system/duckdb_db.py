import pathlib
import duckdb

from dagster import asset, get_dagster_logger

from dagster_jaffle_shop.utils.resources import DUCKDB_FILE


logger = get_dagster_logger()


@asset(group_name="system")
def duckdb_db() -> str:
    "Creates the local DuckDB database file and directory path."

    db_file = pathlib.Path(DUCKDB_FILE)

    if not db_file.parent.exists():
        db_file.parent.mkdir(parents=True)

    if not db_file.exists():
        logger.info("Database file does not exist. Creating...")
        con = duckdb.connect(database=db_file.as_posix())
        con.close()

    return DUCKDB_FILE
