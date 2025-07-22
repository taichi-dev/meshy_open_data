# -*- coding: utf-8 -*-

# -- stdlib --
# -- third party --
import asyncio
import os
import random
from contextlib import asynccontextmanager, contextmanager
from io import BytesIO
from pathlib import Path
from typing import AsyncGenerator

import aiofiles
import boto3
import botocore.exceptions
import pandas as pd
from aiobotocore.session import get_session

# -- own --

# -- code --
AWS_S3_BUCKET = "meshy-datasets"

def make_s3_client():
    # Use IAM Role instead of Access Key and Secret Key
    from botocore.config import Config

    return boto3.resource(
        "s3",
        config=Config(s3={"use_accelerate_endpoint": True}),
    )


class S3IO:
    def __init__(
        self,
        bucket_name=AWS_S3_BUCKET,
        rgw_hosts=[],  # ["http://10.200.1.43", "http://10.200.1.41", "http://10.200.1.15"],
        rgw_access_key="1SYG6UUONTYL7RTOOT30",
        rgw_secret_key="OqpwyvLTffLCohtX5ANjID3twKFDIAamRs32f8et",
    ):
        self.bucket_name = bucket_name
        self.s3_client = make_s3_client()
        self.rgw_clients = []
        self.rgw_hosts = rgw_hosts
        self.rgw_access_key = rgw_access_key
        self.rgw_secret_key = rgw_secret_key

        if len(rgw_hosts) > 0:
            for host in rgw_hosts:
                self.rgw_clients.append(
                    boto3.client(
                        "s3",
                        endpoint_url=host,
                        aws_access_key_id=rgw_access_key,
                        aws_secret_access_key=rgw_secret_key,
                    )
                )

            buckets = random.choice(self.rgw_clients).list_buckets()
            buckets = [bucket["Name"] for bucket in buckets["Buckets"]]
            if bucket_name not in buckets:
                print(f"Bucket {bucket_name} not found in RGW, creating...")
                random.choice(self.rgw_clients).create_bucket(Bucket=bucket_name)

    def doesKeyExist(self, key: str):
        try:
            self.s3_client.Object(bucket_name=self.bucket_name, key=key).get()
            return True
        except Exception as e:
            return False

    async def doesKeyExistAsync(self, key: str):
        assert getattr(self, "async_cache_client") is not None
        try:
            await self.async_cache_client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except Exception as e:
            return False

    def doesDirExists(self, dir_key: str):
        try:
            bucket = self.s3_client.Bucket(self.bucket_name)
            if not dir_key.endswith("/"):
                dir_key += "/"
            response = list(bucket.objects.filter(Prefix=dir_key))
            return len(response) > 0
        except Exception as e:
            print(e)
            return False

    @contextmanager
    def getFileCached(self, key, update_cache=False):
        if len(self.rgw_clients) == 0:
            try:
                filebytes = self.fetchBytes(key)
            except Exception as e:
                print(f"Error fetching {key}: {e}")
                raise e
            yield filebytes
        else:
            cache_client = random.choice(self.rgw_clients)

            with BytesIO() as file_obj:
                try:
                    if update_cache:
                        raise FileNotFoundError(
                            f"File {key} will be force updated in cache"
                        )
                    else:
                        cache_client.download_fileobj(self.bucket_name, key, file_obj)
                except Exception as e:
                    file_obj.seek(0)
                    self.s3_client.Object(
                        bucket_name=self.bucket_name, key=key
                    ).download_fileobj(file_obj)

                    with BytesIO(file_obj.getvalue()) as duplicate_file_obj:
                        cache_client.upload_fileobj(
                            duplicate_file_obj, self.bucket_name, key
                        )

                file_obj.seek(0)
                yield file_obj

    @asynccontextmanager
    async def prepAsyncCacheClient(self):
        if len(self.rgw_hosts) == 0:
            self.async_cache_client = None
            self.async_cache_session = get_session()
            async with self.async_cache_session.create_client("s3") as s3_async_client:
                self.async_cache_client = s3_async_client
                yield
                self.async_cache_client = None
            del self.async_cache_session
        else:
            self.async_cache_client = None
            self.async_cache_session = get_session()
            endpoint = random.choice(self.rgw_hosts)
            async with self.async_cache_session.create_client(
                "s3",
                endpoint_url=endpoint,
                aws_access_key_id=self.rgw_access_key,
                aws_secret_access_key=self.rgw_secret_key,
            ) as client:
                self.async_cache_client = client
                yield
                self.async_cache_client = None

            del self.async_cache_session

    @asynccontextmanager
    async def asyncGetFileCached(self, key) -> AsyncGenerator[BytesIO, None]:
        if len(self.rgw_hosts) == 0:
            yield await self.asyncFetchBytes(self.async_cache_client, key)
        else:
            assert getattr(self, "async_cache_client") is not None
            try:
                resp = await self.async_cache_client.get_object(
                    Bucket=self.bucket_name, Key=key
                )
                async with resp["Body"] as stream:
                    fileobj = BytesIO(await stream.read())
            except Exception:
                fileobj = await self.asyncFetchBytes(self.async_cache_client, key)
                await self.async_cache_client.put_object(
                    Bucket=self.bucket_name, Key=key, Body=fileobj.getvalue()
                )

            yield fileobj

    @asynccontextmanager
    async def asyncGetFileUpdateCache(
        self, s3_client, key
    ) -> AsyncGenerator[BytesIO, None]:
        if len(self.rgw_hosts) == 0:
            yield await self.asyncFetchBytes(s3_client, key)
        else:
            assert getattr(self, "async_cache_client") is not None
            fileobj = await self.asyncFetchBytes(s3_client, key)
            await self.async_cache_client.put_object(
                Bucket=self.bucket_name, Key=key, Body=fileobj.getvalue()
            )

            yield fileobj

    def cacheEnabled(self):
        return len(self.rgw_clients) > 0

    def fetchBytes(self, key: str):
        obj = self.s3_client.Object(bucket_name=self.bucket_name, key=key)
        response = obj.get()
        return BytesIO(response["Body"].read())

    async def asyncFetchBytes(self, s3_client, key: str):
        resp = await s3_client.get_object(Bucket=self.bucket_name, Key=key)
        async with resp["Body"] as stream:
            return BytesIO(await stream.read())

    def downloadFile(self, key: str, out_path: str | Path):
        self.s3_client.Object(bucket_name=self.bucket_name, key=key).download_file(
            out_path
        )
        return out_path

    def uploadFile(self, filepath: str | Path | bytes, key: str):
        try:
            if isinstance(filepath, bytes):
                self.s3_client.Object(bucket_name=self.bucket_name, key=key).put(
                    Body=filepath
                )
            else:
                self.s3_client.Object(
                    bucket_name=self.bucket_name, key=key
                ).upload_file(filepath)
            return True
        except Exception as e:
            print(f"[S3IO] Upload failed: {e}")
            return False

    def getTableParquet(self, key: str) -> pd.DataFrame:
        filebytes = self.fetchBytes(key)
        table = pd.read_parquet(filebytes)
        return table

    def listdir(self, dir):
        files = []
        try:
            s3 = boto3.client("s3")
            paginator = s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=dir)
            for page in pages:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        files.append(obj["Key"])
        except Exception:
            bucket = self.s3_client.Bucket(self.bucket_name)
            for obj in bucket.objects.filter(Prefix=dir):
                files.append(obj.key)
        return files

    async def _asyncUploadFile(self, s3_client, local_path, s3_key):
        try:
            async with aiofiles.open(local_path, "rb") as file_data:
                resp = await s3_client.put_object(
                    Bucket=self.bucket_name, Key=s3_key, Body=await file_data.read()
                )
                success = resp["ResponseMetadata"]["HTTPStatusCode"] == 200
                return (s3_key, success)
        except Exception as e:
            print(f"[S3IO][Upload] Failed to upload '{local_path}' to '{s3_key}': {e}")
            return (s3_key, False)

    async def asyncUploadFiles(self, upload_tasks):
        async_session = get_session()
        async with async_session.create_client("s3") as s3_async_client:
            results = await asyncio.gather(
                *[
                    self._asyncUploadFile(s3_async_client, local_path, s3_key)
                    for (local_path, s3_key) in upload_tasks
                ],
                return_exceptions=True,
            )
            return results

    async def asyncUploadBytes(self, bytes: bytes, s3_key: str):
        try:
            async_session = get_session()
            async with async_session.create_client("s3") as s3_async_client:
                resp = await s3_async_client.put_object(
                    Bucket=self.bucket_name, Key=s3_key, Body=bytes
                )
            success = resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            return (s3_key, success)
        except Exception as e:
            print(f"[S3IO][Upload] Failed to upload '{s3_key}': {e}")
            return (s3_key, False)


class S3IOFileCache(S3IO):
    def __init__(self, *args, local_cache_dir=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.local_cache_dir = local_cache_dir

    @contextmanager
    def getFileCached(self, key):
        local_path = None
        if self.local_cache_dir is not None:
            local_path = Path(self.local_cache_dir) / key
            if local_path.exists():
                yield local_path.open("rb")
                return

        try:
            filebytes = self.fetchBytes(key)
        except Exception as e:
            print(f"Error fetching {key}: {e}")
            raise e

        if self.local_cache_dir is not None:
            assert local_path is not None
            with open(local_path, "wb") as f:
                f.write(filebytes.read())
            filebytes.seek(0)
            yield filebytes
        else:
            yield filebytes

    @asynccontextmanager
    async def prepAsyncCacheClient(self):
        yield

    @asynccontextmanager
    async def asyncGetFileCached(self, s3_client, key) -> AsyncGenerator[BytesIO, None]:
        import aiofiles

        is_cached = False

        local_path = None
        if self.local_cache_dir is not None:
            local_path = Path(self.local_cache_dir) / key
            if local_path.exists():
                async with aiofiles.open(str(local_path), "rb") as f:
                    yield BytesIO(await f.read())
                is_cached = True

        if not is_cached:
            try:
                bytesio = await self.asyncFetchBytes(s3_client, key)
            except Exception as e:
                print(f"Error fetching {key}: {e}")
                # traceback.print_exc()
                raise e
            if self.local_cache_dir is not None:
                assert local_path is not None
                local_path.parent.mkdir(parents=True, exist_ok=True)
                async with aiofiles.open(str(local_path), "wb") as f:
                    await f.write(bytesio.getvalue())
            yield bytesio


def get_s3io(local_cache_dir=None, rgw_hosts=None) -> S3IO:
    if os.environ.get("AWS_SECRET_ACCESS_KEY") is None:
        print(f"[Warning] AWS_SECRET_ACCESS_KEY is not set, please check if you have setup the credentials to access the S3 bucket")

    if local_cache_dir is None:
        local_cache_dir = os.environ.get("LOCAL_CACHE_DIR")
    if local_cache_dir is not None:
        return S3IOFileCache(local_cache_dir=local_cache_dir)

    if rgw_hosts is None:
        rgw_hosts = os.environ.get("RGW_HOSTS")
        if rgw_hosts is not None:
            rgw_hosts = rgw_hosts.split(",")

    if rgw_hosts is None:
        return S3IO()
    return S3IO(rgw_hosts=rgw_hosts)