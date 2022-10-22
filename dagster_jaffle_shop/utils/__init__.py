import pathlib


DATABASE = "dagster"
SCHEMA = "public"


def get_seed_filepath(name):
    root_dir = pathlib.Path(__file__).parent
    f = root_dir / "include" / name
    return f.as_posix()
