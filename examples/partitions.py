from datetime import date, timedelta
from itertools import product
from dagster import (
    DailyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    asset, define_asset_job,
    Definitions, AutoMaterializePolicy, AssetSelection, in_process_executor,
)

start = (date.today() - timedelta(days=3)).isoformat()
colors = ["red", "yellow", "blue"]
shapes = ["square", "round"]
color_shapes = [f'{c}/{s}' for c, s in product(colors, shapes)]

datep = DailyPartitionsDefinition(start_date=start)
colorp = MultiPartitionsDefinition({
    'color': StaticPartitionsDefinition(colors),
    'date': datep
})
shapesp = MultiPartitionsDefinition({
    'shapes': StaticPartitionsDefinition(shapes),
    'date': datep
})

multip = MultiPartitionsDefinition({
    "colored_shapes": StaticPartitionsDefinition(color_shapes),
    "date": datep,

})

@asset(partitions_def=colorp)
def up_a():
    return [1, 2, 3]

@asset(partitions_def=shapesp)
def up_b():
    return ['a', 'b']

@asset(
    partitions_def=multip,
    #auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def multi_partitions_asset(context, up_a, up_b):
    return up_a + up_b


myjob = define_asset_job('test_job',
    # selection=AssetSelection.assets(multi_partitions_asset, up_a, up_b),
    # partitions_def=multip,
    executor_def=in_process_executor,
)

defs = Definitions(
    #jobs=[myjob],
    assets=[up_a, up_b, multi_partitions_asset],
)

