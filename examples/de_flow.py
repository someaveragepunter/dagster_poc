import pandas as pd
from dagster import (
    multi_asset, asset,
    AssetIn, AssetOut,
    MultiPartitionsDefinition,
    StaticPartitionMapping,
    StaticPartitionsDefinition,
    AutoMaterializePolicy, Definitions, in_process_executor
)

############## model funcs ##############

def load(key: str) -> pd.DataFrame:
    return pd.DataFrame({'value': [1, 2, 3]})

def clean(df: pd.DataFrame) -> pd.DataFrame:
    return df.fillna(0)

############### Build DAG #############

regions = ['DE_50HZ', 'DE_TENNET', 'DE_AMPRION', 'DE_TRANSNET', 'DE_LU']
regions_partition = StaticPartitionsDefinition(regions)
de_partition = StaticPartitionsDefinition(['DE'])
kwargs = dict(
    auto_materialize_policy=AutoMaterializePolicy.eager(100),
)
kwargs_multi = dict(
    outs={
        "fcts": AssetOut(),
        "acts": AssetOut(),
    }
) # | kwargs

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
    return pd.concat(list(de_regions_clean.values()))

@asset(partitions_def=de_partition, **kwargs)
def de_clean(de_combine):
    return clean(de_combine)



@multi_asset(partitions_def=regions_partition,
    outs={
        "fcts": AssetOut(),
        "acts": AssetOut(),
    }
)
def de_regions_load2(context):
    key = context.asset_partition_key_for_output()
    res = load(key)
    return res, res + 1

@multi_asset(partitions_def=regions_partition,
    outs={
        "fcts_cleaned": AssetOut(),
        "acts_cleaned": AssetOut(),
    },
)
def de_regions_clean2(fcts, acts):
    return clean(fcts), clean(acts)

