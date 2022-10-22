import jinja2


def resolve_duckdb_ref(name):
    return name


def render_jinja_template(q: str):
    template = jinja2.Template(q)
    result = template.render(ref=resolve_duckdb_ref)
    return result
