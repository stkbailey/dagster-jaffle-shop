import jinja2
import pathlib
import warnings

from dagster import ExperimentalWarning


def resolve_duckdb_ref(name):
    """
    Function that will be used in `render_jinja_template` to pass to the
    `ref` function that is common in dbt-formatted queries. In DuckDB, there
    are no database or schema designations, so we simply pass through the name.
    """
    return name


def render_jinja_template(q: str) -> str:
    "Takes a dbt-jinja-formatted query and resolves functions."
    template = jinja2.Template(q)
    result = template.render(ref=resolve_duckdb_ref)
    return result


def get_seed_filepath(name) -> str:
    "Get the absolute path to a seed file"
    root_dir = pathlib.Path(__file__).parent
    f = root_dir / "include" / name
    return f.absolute().as_posix()


def mute_dagster_warnings() -> None:
    "Filter extraneous warnings that Dagster emits. Must be invoked."
    warnings.filterwarnings("ignore", category=ExperimentalWarning)
