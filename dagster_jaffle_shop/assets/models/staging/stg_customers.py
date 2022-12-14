from dagster import asset

from dagster_jaffle_shop.io_managers import duckdb_io_manager
from dagster_jaffle_shop.resources import DuckDBAssetMetadata


@asset(group_name="models", io_manager_def=duckdb_io_manager)
def stg_customers(raw_customers: DuckDBAssetMetadata) -> str:
    "An intermediate staging table for customers"

    jinja_query = """
    with source as (

        {#-
        Normally we would select from the table here, but we are using seeds to load
        our data in this project
        #}
        select * from {{ ref('raw_customers') }}

    ),

    renamed as (

        select
            id as customer_id,
            first_name,
            last_name

        from source

    )

    select * from renamed

    """
    return jinja_query
