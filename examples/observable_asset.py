import os
from datetime import datetime
from dagster import (
    DataVersion,
    ScheduleDefinition,
    define_asset_job,
    observable_source_asset, StaticPartitionsDefinition, DataVersionsByPartition, Definitions, RunConfig,
    in_process_executor
)

@observable_source_asset(auto_observe_interval_minutes=1)
def foo_source_asset(context):
    partition_name = 'b' #context.asset_partition_key_for_output()
    filename = f'c:/temp/{partition_name}.csv'
    print(f'filename is {filename}')
    version = filename if os.path.exists(filename) else datetime.today().isoformat()
    return DataVersion(version)

@observable_source_asset(
    partitions_def=StaticPartitionsDefinition(["apple", "orange", "kiwi"]),
    auto_observe_interval_minutes=1,
)
def foo():
    return DataVersionsByPartition({"apple": "one", "orange": "two"})

# observation_job = define_asset_job("observation_job", [foo_source_asset])
#
# observation_schedule = ScheduleDefinition(
#     name="observation_schedule",
#     cron_schedule="* * * * *",
#     job=observation_job,
# )

defn = Definitions(
    #jobs=[observation_job],
    #schedules=[observation_schedule],
    assets=[foo_source_asset, foo],
    executor=in_process_executor,
)