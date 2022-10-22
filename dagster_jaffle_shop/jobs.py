from dagster import define_asset_job, load_assets_from_package_module

from dagster_jaffle_shop import assets


asset_list = [x.key.to_user_string() for x in load_assets_from_package_module(assets)]

materialize_all_job = define_asset_job(
    name="materialize_all_assets_job",
    description="Materialize all assets in the project.",
    selection=asset_list,
)
