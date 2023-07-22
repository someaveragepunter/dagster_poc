from typing import Any

from dagster._utils.cached_method import cached_method
from dagster_aws.s3 import ConfigurablePickledObjectS3IOManager, S3Resource, PickledObjectS3IOManager
from dagster import Definitions, asset
import pandas as pd
from io import BytesIO
import s3fs
import aiobotocore

# import boto3
# boto3.setup_default_session(profile_name='ara', region_name="eu-west-2")

def test():
    import boto3
    import s3fs
    import aiobotocore
    profile_session = aiobotocore.session.AioSession(profile='ara')
    boto3.setup_default_session(profile_name='ara', region_name="eu-west-2")
    session = boto3.session.Session(profile_name='ara', region_name="eu-west-2")
    fs = s3fs.S3FileSystem(profile_name='ara') #session=profile_session)
    fs = s3fs.core.S3FileSystem(profile="ara")
    with fs.open('esop-testbucket/test_parquet', 'rb') as f:
        df3 = pd.read_parquet(f)

    client = boto3.client('s3')
    bucket = 'esop-testbucket'
    buffer = BytesIO()
    s3 = boto3.resource('s3')
    s3.Object(bucket, 'test_parquet').download_fileobj(buffer)

    downloaded = client.get_object(Bucket='esop-testbucket', Key='test_parquet')["Body"].read()

    df2 = pd.read_parquet(buffer)
    import polars as pl
    pl.read_parquet(buffer)

    fs = s3fs.S3FileSystem(anon=True)

    client.upload_fileobj(obj_bytes, 'esop-testbucket', 'test_parquet')

class ParquetS3IOManager(PickledObjectS3IOManager):
    @property
    def fs(self):
        fs = s3fs.core.S3FileSystem(profile="ara")
        return fs

    def load_from_path(self, context, path) -> Any:
        with self.fs.open(f'{self.bucket}/{path}', 'rb') as f:
            pd.read_parquet(f)

    def dump_to_path(self, context, obj, path):
        if self.path_exists(path):
            context.log.warning(f"Removing existing S3 object: {path}")
            self.unlink(path)

        with self.fs.open(f'{self.bucket}/{path}', 'wb') as f:
            obj.to_parquet(f)


class ConfigurableParquetS3IOManager(ConfigurablePickledObjectS3IOManager):
    @cached_method
    def inner_io_manager(self) -> ParquetS3IOManager:
        return ParquetS3IOManager(
            s3_bucket=self.s3_bucket,
            s3_session=self.s3_resource.get_client(),
            s3_prefix=self.s3_prefix,
        )


@asset
def upstream_asset():
    return pd.DataFrame({'a': [1, 2, 3]})


@asset
def downstream_asset(upstream_asset):
    return upstream_asset * 2

print('padnas ')
defs = Definitions(
    assets=[upstream_asset, downstream_asset],
    resources={
        "io_manager": ConfigurableParquetS3IOManager(
            s3_resource=S3Resource(profile_name='ara', region_name="eu-west-2"), s3_bucket="esop-testbucket"
        ),
    },
)

from dagster_duckdb_polars import DuckDBPolarsIOManager