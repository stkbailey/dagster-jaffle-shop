"""
Creates a job that is not directly associated with assets, which can
be run to log metadata to those assets, based on their names.
"""
import json

from dagster import op, job, OpExecutionContext, AssetObservation
from pandas_profiling import ProfileReport

from dagster_jaffle_shop.resources import duckdb_resource


PRIMARY_KEY_DICT = {
    "customers": "customer_id",
    "stg_customers": "customer_id",
    "orders": "order_id",
    "stg_orders": "order_id",
}


@op(required_resource_keys={"duckdb"})
def profile_duckdb_tables(context: OpExecutionContext) -> None:
    "Generates `pandas_profiling` observation for each table in the db."

    # list all tables in the duckdb database
    ddb = context.resources.duckdb
    df = ddb.execute_query("select * from pg_tables")
    table_list = df["tablename"].values.tolist()
    context.log.info("Found %s tables in DuckDB database.", len(table_list))

    # for each table, read in the data and create profile
    for t in table_list:
        context.log.info("Creating profile for table %s", t)
        df = ddb.execute_query(f"select * from {t}")
        profile = ProfileReport(df, minimal=True)
        profile_dict = json.loads(profile.to_json())
        metadata = {**profile_dict["analysis"], **profile_dict["table"]}

        # log event
        observation = AssetObservation(
            asset_key=t,
            description="Auto-logged statistics by pandas-profiling job.",
            metadata=metadata,
        )
        context.log_event(event=observation)


@op(required_resource_keys={"duckdb"})
def test_duckdb_table_primary_keys(context: OpExecutionContext) -> None:
    "Tests that there are no null values in a declared primary key col."

    # list all tables in the duckdb database
    ddb = context.resources.duckdb
    df = ddb.execute_query("select * from pg_tables")
    table_list = df["tablename"].values.tolist()
    context.log.info("Found %s tables in DuckDB database.", len(table_list))

    # for each table, read in the data and create profile
    for table_name, col_name in PRIMARY_KEY_DICT.items():
        context.log.info("Checking for primary key validity of table %s", table_name)
        test = f"""
            select count(*) as count_null
            from {table_name}
            where {col_name} is null
        """
        df = ddb.execute_query(test)
        success = df.iloc[0]["count_null"] == 0

        # log event
        observation = AssetObservation(
            asset_key=table_name,
            description="TEST: Primary key is not null",
            metadata={"success": success},
        )
        context.log_event(event=observation)


@job(resource_defs={"duckdb": duckdb_resource})
def evaluate_duckdb_tables_job():
    "This job runs both the table profiling and testing steps."

    profile_duckdb_tables()
    test_duckdb_table_primary_keys()
