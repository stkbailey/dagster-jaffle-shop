import json

from dagster import op, job, OpExecutionContext, AssetObservation, MetadataEntry
from pandas_profiling import ProfileReport

from dagster_jaffle_shop.utils.resources import duckdb_resource


@op(required_resource_keys={"duckdb"})
def profile_duckdb_tables(context: OpExecutionContext):
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
            metadata=metadata
        )
        context.log_event(event=observation)


# @op(required_resource_keys={"duckdb"})
# def test_duckdb_tables(context: OpExecutionContext):
#     "This op tests that there are no null values in any column."

#     conn = context.resources.duckdb
#     table_list = ["customers"]

#     for t in table_list:
#         df = conn.execute(f"select * from {t}").fetchdf()
#         profile = ProfileReport(df, title="Pandas Profiling Report")
#         profile_metadata = profile.to_json()

#     # context.add_output_metadata({"report_json": json.loads(profile_metadata)})


@job(resource_defs={"duckdb": duckdb_resource})
def evaluate_duckdb_tables_job():
    "This job runs both the table profiling and testing steps."

    profile_duckdb_tables()
    # test_duckdb_tables()
