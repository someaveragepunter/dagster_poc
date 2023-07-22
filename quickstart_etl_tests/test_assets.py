
from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_package_module,
)

from quickstart_etl import assets

daily_refresh_schedule = ScheduleDefinition(
    job=define_asset_job(name="all_assets_job"), cron_schedule="* * * * *"
)

defs = Definitions(
    assets=load_assets_from_package_module(assets), schedules=[daily_refresh_schedule]
)