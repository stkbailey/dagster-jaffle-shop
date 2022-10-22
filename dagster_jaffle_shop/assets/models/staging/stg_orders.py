from dagster import asset, OpExecutionContext

from dagster_jaffle_shop.utils.io_managers import duckdb_io_manager


@asset(io_manager_def=duckdb_io_manager)
def stg_orders(context: OpExecutionContext, raw_orders: str) -> str:
    "An intermediate staging table for orders"

    jinja_query = """
    with source as (

        {#-
        Normally we would select from the table here, but we are using seeds to load
        our data in this project
        #}
        select * from {{ ref('raw_orders') }}

    ),

    renamed as (

        select
            id as order_id,
            user_id as customer_id,
            order_date,
            status

        from source

    )

    select * from renamed
    """
    return jinja_query
