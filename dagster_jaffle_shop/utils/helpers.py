import jinja2
import pathlib
import warnings

from dagster import ExperimentalWarning


def resolve_duckdb_ref(name):
    return name


def render_jinja_template(q: str):
    template = jinja2.Template(q)
    result = template.render(ref=resolve_duckdb_ref)
    return result


def get_seed_filepath(name):
    root_dir = pathlib.Path(__file__).parent.parent
    f = root_dir / "include" / name
    return f.absolute().as_posix()


def mute_dagster_warnings():
    "Filter extraneous warnings that Dagster emits"
    warnings.filterwarnings("ignore", category=ExperimentalWarning)
