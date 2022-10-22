import duckdb

from dagster import IOManager, io_manager, OutputContext, InputContext

from dagster_jaffle_shop.utils.resources import DUCKDB_FILE
from dagster_jaffle_shop.utils.queries import render_jinja_template


class DuckDBIOManager(IOManager):
    "The DuckDB IO Manager takes query text and creates tables based on it."

    def __init__(self, db_file: str):
        self.db_file = db_file

    def handle_output(self, context: OutputContext, obj: str):
        """
        The handle_output function takes the asset output and does something
        with it. In this case, we are going to take a Jinja SQL string, render
        it, and then write it into the DuckDB database.
        """
        # name is the name given to the Out that we're storing for
        table_name = context.name
        rendered_query = render_jinja_template(obj)
        with duckdb.connect(self.db_file, read_only=False) as conn:
            query = f"CREATE OR REPLACE TABLE {table_name} AS {rendered_query}"
            conn.execute(query)

            # log metadata based on the table
            # context.log.info(df)
            metadata = {"query": rendered_query} #, "records": df.shape[0]}
            context.add_output_metadata(metadata)

    def load_input(self, context: InputContext) -> str:
        """
        The load_input function determines how this asset will be loaded by any
        assets that reference it. In our case, we are not loading a dataframe
        directly, we are loading the table name, which can then be referenced
        in the SQL query.
        """
        # upstream_output.name is the name given to the Out that we're loading for
        table_name = context.upstream_output.name
        return table_name


@io_manager
def duckdb_io_manager(_):
    yield DuckDBIOManager(DUCKDB_FILE)
