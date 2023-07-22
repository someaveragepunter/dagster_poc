from itertools import product

from dagster import (
    AssetIn,
    MultiPartitionsDefinition,
    StaticPartitionMapping,
    StaticPartitionsDefinition,
    asset, AssetSelection, in_process_executor, define_asset_job, Definitions,
    AutoMaterializePolicy,
    MultiPartitionMapping, DailyPartitionsDefinition, DimensionPartitionMapping, IdentityPartitionMapping,
)

DateP = DailyPartitionsDefinition(start_date="2023-07-16")
DateMap = DimensionPartitionMapping(
    dimension_name="dates",
    partition_mapping=IdentityPartitionMapping(),
)

################################## HELPER FUNCS ##############################################
def cross(p1: list, p2: list) -> list:
    return ['.'.join(x) for x in product(p1, p2)]

def map_partition(k1: str, k2: str) -> AssetIn:
    return AssetIn(partition_mapping=MultiPartitionMapping({
        "dates": DateMap,
        k1: DimensionPartitionMapping(
            dimension_name=k2,
            partition_mapping=StaticPartitionMapping({
                v1: {v2 for v2 in partition_vals[k2] if v1 in v2}
            for v1 in partition_vals[k1]})
        )
    }))

################################## PARTITIONS ##############################################

partition_vals = {key: [f'{key}{i}' for i in range(3)] for key in ('a', 'b', 'c')}
partition_vals['bc'] = cross(partition_vals['b'], partition_vals['c'])
partition_vals['abc'] = cross(partition_vals['a'], partition_vals['bc'])

static_partitions = {k: StaticPartitionsDefinition(v) for k, v in partition_vals.items()}
multi_partitions = {k: MultiPartitionsDefinition({'dates': DateP, k: v}) for k, v in static_partitions.items()}

#################################### ASSET DEF ############################################

@asset(partitions_def=multi_partitions['a'])
def assetA(): return [1, 2, 3]
@asset(partitions_def=multi_partitions['b'])
def assetB(): return [11, 22, 33]
@asset(partitions_def=multi_partitions['c'])
def assetC(): return [12, 22, 32]


@asset(
    partitions_def=multi_partitions['bc'],
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    ins={
        "assetB": map_partition('b', 'bc'),
        "assetC": map_partition('c', 'bc'),
    },
)
def assetBC(assetB, assetC):
    return assetB + assetC

@asset(
    partitions_def=multi_partitions['abc'],
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    ins={
        "assetA": map_partition('a', 'abc'),
        "assetBC": map_partition('bc', 'abc'),
    },
)
def assetABC(assetA, assetBC):
    return assetA + assetBC

#
# partitioned_asset_job = define_asset_job(
#     name="all_job",
#     selection=AssetSelection.assets(assetA, assetB, assetC, assetBC), #, asset1, asset2
#     #partitions_def=multpar,
#     executor_def=in_process_executor,
# )

# defn = Definitions(
#     assets=[assetB, assetC, assetBC],
#     jobs=[partitioned_asset_job],
# )
