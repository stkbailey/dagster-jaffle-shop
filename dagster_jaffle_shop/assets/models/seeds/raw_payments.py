from dagster import asset, OpExecutionContext

from dagster_jaffle_shop.assets.system.duckdb_db import duckdb_db
from dagster_jaffle_shop.utils import get_package_root_path
from dagster_jaffle_shop.io_managers import duckdb_io_manager


@asset(
    group_name="models",
    non_argument_deps={duckdb_db.key},
    io_manager_def=duckdb_io_manager,
)
def raw_payments() -> str:
    "Raw payments data loaded from a CSV."

    p = get_package_root_path() / "assets" / "include" / "raw_payments.csv"
    query = f"SELECT * FROM read_csv_auto('{p.as_posix()}')"

    return query
