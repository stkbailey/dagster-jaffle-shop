from dagster import asset, OpExecutionContext

from dagster_jaffle_shop.assets.system.duckdb_db import duckdb_db
from dagster_jaffle_shop.utils import get_seed_filepath
from dagster_jaffle_shop.utils.io_managers import duckdb_io_manager


@asset(non_argument_deps={duckdb_db.key}, io_manager_def=duckdb_io_manager)
def raw_customers(context: OpExecutionContext) -> str:
    "Example customer data."

    f = get_seed_filepath("raw_customers.csv")
    query = f"SELECT * FROM read_csv_auto('{f}')"
    context.add_output_metadata({"query": query})

    return query
