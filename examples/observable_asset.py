import os
from dagster import (
    DataVersion,
    ScheduleDefinition,
    define_asset_job,
    observable_source_asset, StaticPartitionsDefinition, DataVersionsByPartition
)

@observable_source_asset(
    partitions_def=StaticPartitionsDefinition(['b1', 'b2', 'b3']),
)
def foo_source_asset(context):
    partition_name = 'b' #context.asset_partition_key_for_output()
    filename = f'c:/temp/{partition_name}.csv'
    print(f'filename is {filename}')
    version = filename if os.path.exists(filename) else 'none'
    return DataVersionsByPartition(version)

# observation_schedule = ScheduleDefinition(
#     name="observation_schedule",
#     cron_schedule="* * * * *",
# )