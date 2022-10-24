from dagster import repository, load_assets_from_package_module

from dagster_jaffle_shop import assets
from dagster_jaffle_shop.jobs.models import materialize_all_job
from dagster_jaffle_shop.jobs.profiling import evaluate_duckdb_tables_job


# load all of the assets from the assets folder into a single list
asset_list = load_assets_from_package_module(assets)
job_list = [materialize_all_job, evaluate_duckdb_tables_job]


@repository
def jaffle_shop_repo():
    "Repo (like a folder) for Jaffle Shop assets, jobs, sensors, schedules."
    return asset_list + job_list
