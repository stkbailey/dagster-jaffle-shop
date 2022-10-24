# Dagster's Jaffle Shop

Dagster's Software Defined Assets technology is very cool. It can also be
extremely hard to understand -- not unlike its common partner, dbt.

This repo is intended to help dbt users understand what Dagster assets are,
and how Dagster uses other object classes to declaratively create resources,
in much the same way dbt does.

## dbt _is_ software-defined assets

The core insight this package aims to help users understand is that dbt was
the OG software-defined assets platform for the modern data stack. The beauty
is that dbt made the statement: "you know what, 80% of the highest impact
data work you do can be done simply by writing queries. You give us the SQL,
we'll do the rest."

It's a very neat framework: you just have a directory of SQL files (with Jinja, of
course), and a few "types" of assets like snapshots and seeds and models.

Dagster's software-defined assets framework does not have the benefit of dbt's
simple worldview. But, one could easily rebuild a simple dbt worldview with the
tools provided. That's what I'm doing here, with DuckDB and Dagster.

## Jaffle Shop

Here are a few of the opinions this directory provides:

- All assets live in the dagster_jaffle_shop/assets folder.
- Only one asset is defined per .py file.
- DuckDB assets return a query string. The DuckDB IO Manager renders the Jinja and executes the query.
- DuckDB assets are loaded as just a table name.

