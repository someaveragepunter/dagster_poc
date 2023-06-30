from datetime import date, timedelta
from itertools import product
from dagster import (
    DailyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    asset, define_asset_job,
    Definitions, AutoMaterializePolicy, AssetSelection, in_process_executor,
)

colors = ["red", "yellow", "blue"]
shapes = ["square", "round"]
colorshapes = [f'{clr}/{shp}' for clr, shp in product(colors, shapes)]
start = (date.today() - timedelta(days=3)).isoformat()

datep = DailyPartitionsDefinition(start_date=start)
colorp = StaticPartitionsDefinition(colors)
shapesp = StaticPartitionsDefinition(shapes)

multip = MultiPartitionsDefinition({
    "date": datep,
    "cs": StaticPartitionsDefinition(colorshapes),
})

@asset(
    partitions_def=datep
)
def up_a():
    return [1, 2, 3]

@asset(partitions_def=shapesp)
def up_c():
    return [11, 22, 33]

@asset(
    partitions_def=colorp
)
def up_b():
    return ['a', 'b']

@asset(
    partitions_def=multip,
    #auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def multi_partitions_asset(context, up_a, up_c, up_b):
    return up_a + up_c + up_b


myjob = define_asset_job('test_job',
    # selection=AssetSelection.assets(multi_partitions_asset, up_a, up_b),
    # partitions_def=multip,
    executor_def=in_process_executor,
)

defs = Definitions(
    #jobs=[myjob],
    assets=[up_a, up_b, up_c, multi_partitions_asset],
)

