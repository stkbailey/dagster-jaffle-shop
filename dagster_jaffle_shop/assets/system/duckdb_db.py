import duckdb
import pathlib

from dagster import asset, OpExecutionContext

from dagster_jaffle_shop.resources import duckdb_resource


@asset(
    group_name="system",
    required_resource_keys={"duckdb"},
    resource_defs={"duckdb": duckdb_resource},
)
def duckdb_db(context: OpExecutionContext) -> dict:
    """
    Creates the local DuckDB database file and directory path.
    Not required since the `io_manager` will create these, but included
    as an example and a way to log database-level metadata.
    """

    db_file = pathlib.Path(context.resources.duckdb.database)
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
        context.add_output_metadata(
            {"tables": table_list, "count_tables": table_df.shape[0]}
        )

    return {"path": db_file.as_posix()}
