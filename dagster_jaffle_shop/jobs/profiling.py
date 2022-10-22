import json

from dagster import op, job, OpExecutionContext
from pandas_profiling import ProfileReport

from dagster_jaffle_shop.utils.resources import duckdb_resource


@op(required_resource_keys={"duckdb"})
def profile_duckdb_tables(context: OpExecutionContext):
    "This op generates `pandas_profiling` observation for each table in the db."
    conn = context.resources.duckdb
    table_list = ["customers"]

    for t in table_list:
        df = conn.execute(f"select * from {t}").fetchdf()
        profile = ProfileReport(df, title="Pandas Profiling Report")
        profile_metadata = json.loads(profile.to_json())
        
    context.add_output_metadata(profile_metadata)


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
