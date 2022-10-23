import pathlib
import duckdb

from dagster import asset, OpExecutionContext

from dagster_jaffle_shop.utils.resources import DUCKDB_FILE


@asset(group_name="system")
def duckdb_db(context: OpExecutionContext) -> dict:
    "Creates the local DuckDB database file and directory path."

    db_file = pathlib.Path(DUCKDB_FILE)
    if not db_file.parent.exists():
        db_file.parent.mkdir(parents=True)

    if not db_file.exists():
        context.log.info("Database file does not exist. Creating...")
        con = duckdb.connect(database=db_file.as_posix())
        con.close()

    # list the tables currently available in the database
    with duckdb.connect(database=db_file.as_posix()) as con:
        table_df = con.execute("select * from pg_tables").fetchdf()
        table_list = ", ".join(table_df["tablename"].values.tolist())
    context.add_output_metadata({"tables": table_list})

    return {"path": DUCKDB_FILE}
