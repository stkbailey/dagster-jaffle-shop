import duckdb

from dagster import IOManager, io_manager

from .resources import DUCKDB_FILE
from .queries import render_jinja_template


class DuckdbIOManager(IOManager):
    def __init__(self, db_file: str):
        self.db_file = db_file

    def handle_output(self, context, obj: str):
        # name is the name given to the Out that we're storing for
        table_name = context.name
        rendered_query = render_jinja_template(obj)
        with duckdb.connect(self.db_file, read_only=False) as conn:
            query = f"CREATE OR REPLACE TABLE {table_name} AS {rendered_query}"
            conn.execute(query)

    def load_input(self, context) -> str:
        # upstream_output.name is the name given to the Out that we're loading for
        table_name = context.upstream_output.name
        return table_name


@io_manager
def duckdb_io_manager(_):
    return DuckdbIOManager(DUCKDB_FILE)
