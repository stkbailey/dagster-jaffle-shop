import duckdb
import time

from dagster import IOManager, io_manager, OutputContext, InputContext

from dagster_jaffle_shop.resources import (
    DUCKDB_FILE,
    DuckDBResource,
    DuckDBAssetMetadata,
)
from dagster_jaffle_shop.utils import render_jinja_template


class DuckDBIOManager(IOManager):
    "The DuckDB IO Manager takes query text and creates tables based on it."

    def __init__(self, db_file: str):
        self.db_file = db_file
        self.resource = DuckDBResource(db_file)

    def handle_output(self, context: OutputContext, obj: str):
        """
        The handle_output function takes the asset output and does something
        with it. In this case, we are going to take a Jinja SQL string, render
        it, and then write it into the DuckDB database.
        """
        # name is the name given to the Out that we're storing for
        table_name = context.asset_key.to_user_string()
        rendered_query = render_jinja_template(obj)
        query = f"CREATE OR REPLACE TABLE {table_name} AS {rendered_query}"

        # we try multiple times to insert the data
        max_attempts = 5
        counter = 0
        while counter < max_attempts:
            try:
                result_df = self.resource.execute_query(query, read_only=False)
                counter = max_attempts
            except duckdb.IOException:
                counter += 1
                time.sleep(1)

        if "result_df" not in vars():
            raise Exception("Query was unsuccessful.")

        context.add_output_metadata(
            {
                "query": rendered_query,
                "table_name": table_name,
                "count_records": int(result_df.iloc[0]["Count"]),
            }
        )

    def load_input(self, context: InputContext) -> DuckDBAssetMetadata:
        """
        The load_input function determines how this asset will be loaded by any
        assets that reference it. In our case, we are not loading a dataframe
        directly, we are loading the table name, which can then be referenced
        in the SQL query.
        """
        # upstream_output.name is the name given to the Out that we're loading for
        return DuckDBAssetMetadata(table_name=context.upstream_output.name)


@io_manager
def duckdb_io_manager(_):
    yield DuckDBIOManager(DUCKDB_FILE)
