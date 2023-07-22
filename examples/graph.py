from dagster import asset, graph_asset, op


@asset
def upstream_asset():
    return 1


@op
def add_one(input_num):
    return input_num + 1


@op
def multiply_by_two(input_num):
    return input_num * 2


@graph_asset
def middle_asset(upstream_asset):
    return multiply_by_two(add_one(upstream_asset))


@asset
def downstream_asset(middle_asset):
    return middle_asset + 7


from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_package_module,
)

defs = Definitions(
    assets=[upstream_asset, middle_asset, downstream_asset],
    #schedules=[daily_refresh_schedule]
)