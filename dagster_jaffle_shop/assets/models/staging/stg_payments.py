from dagster import asset

from dagster_jaffle_shop.io_managers import duckdb_io_manager


@asset(group_name="models", io_manager_def=duckdb_io_manager)
def stg_payments(raw_payments: str) -> str:
    "An intermediate staging table for payments"

    jinja_query = """
    with source as (
        
        {#-
        Normally we would select from the table here, but we are using seeds to load
        our data in this project
        #}
        select * from {{ ref('raw_payments') }}

    ),

    renamed as (

        select
            id as payment_id,
            order_id,
            payment_method,

            -- `amount` is currently stored in cents, so we convert it to dollars
            amount / 100 as amount

        from source

    )

    select * from renamed
    """

    return jinja_query
