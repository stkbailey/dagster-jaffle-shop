# Dagster's Jaffle Shop

Dagster's Software Defined Assets technology is very cool. It can also be
extremely hard to understand -- not unlike its common partner, dbt.

This repo is intended to help dbt users understand what Dagster assets are,
and how Dagster uses other object classes to declaratively create resources,
in much the same way dbt does.

## Dagster vs. dbt

The core difference is that Dagster needs to be able to create many different
types of assets, whereas dbt is able to neatly hide them.