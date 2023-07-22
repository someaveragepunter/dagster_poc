from dagster import DailyPartitionsDefinition
from dagster import asset, OpExecutionContext, Definitions, AssetKey

from itertools import permutations


# Root Nodes

date = DailyPartitionsDefinition(start_date="2023-06-01")
colors = ["blue", "red"]
shapes = ["circle", "square"]


def root_asset_factory(node, partition_def, group):
    @asset(
        name=node,
        key_prefix=group,
        partitions_def=partition_def,
        group_name=group,
    )
    def the_asset(context: OpExecutionContext):
        # example of how to construct an appropriate query in the resulting asset
        partition = context.partition_key
        query = f"""
            SELECT * 
            FROM table 
            WHERE 
                {group} = {node} 
                AND date = {partition}
        """
        context.log.info(query)
        return

    return the_asset


color_assets = [root_asset_factory(color, date, "color") for color in colors]
shape_assets = [root_asset_factory(shape, date, "shape") for shape in shapes]


# Mixed Nodes
color_shapes = []
colors_with_prefix = [f"color/{c}" for c in colors]
shapes_with_prefix = [f"shape/{s}" for s in shapes]

permut = permutations(colors_with_prefix, len(shapes_with_prefix))
for comb in permut:
    zipped = zip(comb, shapes_with_prefix)
    color_shapes.append(list(zipped))

color_shapes = [pair for combo in color_shapes for pair in combo]


def mixed_asset_factory(ins, partition_def):
    asset_key_ins = set([AssetKey.from_user_string(i) for i in ins])
    # ins of ("color/blue", "shape/circle") becomes name blue_circle, group color_shape
    name = "_".join([key_value[1] for key_value in [i.split("/") for i in ins]])
    group = "_".join([key_value[0] for key_value in [i.split("/") for i in ins]])

    @asset(
        name=name,
        group_name=group,
        partitions_def=partition_def,
        non_argument_deps=asset_key_ins,
    )
    def the_asset(context: OpExecutionContext):
        # example of how to construct an appropriate query in the resulting asset
        partition = context.partition_key

        node_where_clause = ""
        for i in ins:
            kv = i.split("/")
            node_where_clause += f"""
                AND {kv[0]} = \"{kv[1]}\"
            """

        query = f"""
            SELECT * 
            FROM table 
            WHERE 
                date = {partition}
                {node_where_clause}            
        """
        context.log.info(query)
        return

    return the_asset


color_shape_assets = [
    mixed_asset_factory(upstreams, date) for upstreams in color_shapes
]

defs = Definitions(assets=[*color_assets, *shape_assets, *color_shape_assets])