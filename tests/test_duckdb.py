import duckdb
import pathlib


def test_duckdb_create():
    cursor = duckdb.connect()
    p = (
        pathlib.Path().cwd()
        / "dagster_jaffle_shop"
        / "include"
        / "seeds"
        / "raw_customers.csv"
    )

    QUERY = f"CREATE TABLE new_tbl AS SELECT * FROM read_csv_auto('{p.as_posix()}');"
    print(cursor.execute(QUERY).fetchall())

    assert False


def test_duckdb_select():
    cursor = duckdb.connect()
    QUERY = "select * from new_tbl;"
    print(cursor.execute(QUERY).fetchall())

    assert False
