import itertools
from dagster import (
    AssetIn,
    MultiPartitionsDefinition,
    StaticPartitionMapping,
    StaticPartitionsDefinition,
    asset, AutoMaterializePolicy,
)
A = StaticPartitionsDefinition(["a1", "a2", "a3"])
B = StaticPartitionsDefinition(["b1", "b2", "b3"])
C = StaticPartitionsDefinition(["c1", "c2", "c3"])
BC = StaticPartitionsDefinition(
    # instead of actual multi-partitions, just create strings of the form b1.c1
    [f"{b}.{c}" for b, c in itertools.product(B.get_partition_keys(), C.get_partition_keys())]
)
ABC = MultiPartitionsDefinition(
    {
        "a": A,
        "bc": BC,
    }
)
@asset(partitions_def=A)
def assetA():
    return [1,2,3]
@asset(partitions_def=B)
def assetB():
    return ['a', 'b']
@asset(partitions_def=C)
def assetC():
    return [11, 22, 33]
@asset(
    partitions_def=BC,
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    ins={
        "assetB": AssetIn(
            partition_mapping=StaticPartitionMapping(
                {
                    # each partition of b maps to...
                    b_partition: {
                        # all multi partition keys that contain that partition
                        f"{b_partition}.{c_partition}"
                        for c_partition in C.get_partition_keys()
                    }
                    for b_partition in B.get_partition_keys()
                }
            ),
        ),
        "assetC": AssetIn(
            partition_mapping=StaticPartitionMapping(
                {
                    # each partition of c maps to...
                    c_partition: {
                        # all multi partition keys that contain that partition
                        f"{b_partition}.{c_partition}"
                        for b_partition in B.get_partition_keys()
                    }
                    for c_partition in C.get_partition_keys()
                }
            ),
        ),
    },
)
def assetBC(assetB, assetC):
    return assetB + assetC
@asset(partitions_def=ABC,
       auto_materialize_policy=AutoMaterializePolicy.eager(),
       )
def assetABC(assetA, assetBC):
    return assetA + assetBC




