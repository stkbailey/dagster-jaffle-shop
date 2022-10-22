import duckdb
import pytest

DATA_CSV = """\
id,value
1,101
2,202
3,303"""


@pytest.fixture
def duckdb_cursor(tmp_path):
    # create duckdb database and csv
    cursor = duckdb.connect()
    f = tmp_path / "foo.csv"
    f.write_text(DATA_CSV)

    # load table
    query = f"""
    CREATE TABLE new_tbl AS (
        SELECT * FROM read_csv_auto('{f.as_posix()}')
    );"""
    cursor.execute(query).fetchdf()

    yield cursor


def test_duckdb_create(tmp_path):
    # given
    cursor = duckdb.connect()
    f = tmp_path / "foo.csv"
    f.write_text(DATA_CSV)

    # when
    QUERY = f"""
    CREATE TABLE new_tbl AS (
        SELECT * FROM read_csv_auto('{f.as_posix()}')
    );"""
    df = cursor.execute(QUERY).fetchdf()

    # then
    assert df.iloc[0]["Count"] == 3


def test_duckdb_select(duckdb_cursor):
    # given
    QUERY = "select * from new_tbl;"

    # when
    df = duckdb_cursor.execute(QUERY).fetchdf()

    # then
    print(df)
    assert df.shape == (3, 2)
