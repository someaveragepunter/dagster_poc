from itertools import chain
import pandas as pd
from dagster import (
    multi_asset, asset,
    AssetIn, AssetOut,
    MultiPartitionsDefinition,
    StaticPartitionMapping,
    StaticPartitionsDefinition,
    AutoMaterializePolicy, Definitions, in_process_executor, Output
)

############## mock funcs ##############

def load(key: str) -> pd.DataFrame:
    return pd.DataFrame({'value': [1, 2, 3]})

def clean(df: pd.DataFrame) -> pd.DataFrame:
    return df.fillna(0)

############### Build DAG #############

regions = ['DE_50HZ', 'DE_TENNET', 'DE_AMPRION', 'DE_TRANSNET', 'DE_LU']
regions_partition = StaticPartitionsDefinition(regions)
de_partition = StaticPartitionsDefinition(['DE'])


def factory(prefix):
    kwargs = dict(
        key_prefix=prefix, group_name=prefix,
        auto_materialize_policy=AutoMaterializePolicy.eager(100),
    )
    @asset(partitions_def=regions_partition, **kwargs)
    def de_regions_load(context):
        key = context.asset_partition_key_for_output()
        res = load(key)
        return res

    @asset(partitions_def=regions_partition, **kwargs)
    def de_regions_clean(de_regions_load):
        res = clean(de_regions_load)
        return res

    @asset(
        partitions_def=de_partition,
        ins={
            "de_regions_clean": AssetIn(
                partition_mapping=StaticPartitionMapping({r: {'DE'} for r in regions})
        )},
        **kwargs
    )
    def de_combine(de_regions_clean):
        res = pd.concat(list(de_regions_clean.values()))
        return Output(res,
            metadata={"upstream": list(de_regions_clean.keys())}
        )

    @asset(partitions_def=de_partition, **kwargs)
    def de_clean(de_combine):
        return clean(de_combine)

    return [de_regions_load, de_regions_clean, de_combine, de_clean]


defs = Definitions(
    assets=list(chain(
        *[factory(x) for x in ('forecast', 'actual')],
    )),
    # executor=in_process_executor,
)

# @multi_asset(partitions_def=regions_partition,
#     outs={
#         "fcts": AssetOut(),
#         "acts": AssetOut(),
#     }
# )
# def de_regions_load2(context):
#     key = context.asset_partition_key_for_output()
#     res = load(key)
#     return res, res + 1
#
# @multi_asset(partitions_def=regions_partition,
#     outs={
#         "fcts_cleaned": AssetOut(),
#         "acts_cleaned": AssetOut(),
#     },
# )
# def de_regions_clean2(fcts, acts):
#     return clean(fcts), clean(acts)
#
