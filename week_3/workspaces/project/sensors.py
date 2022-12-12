import boto3
from dagster import sensor, SkipReason, RunRequest

# from .week_3 import week_3_pipeline_docker


def get_s3_keys(bucket: str, prefix: str = "", endpoint_url: str = None, since_key: str = None, max_keys: int = 1000):
    """Get S3 keys"""
    config = {"service_name": "s3"}
    if endpoint_url:
        config["endpoint_url"] = endpoint_url

    client = boto3.client(**config)

    cursor = ""
    contents = []

    while True:
        response = client.list_objects_v2(
            Bucket=bucket,
            Delimiter="",
            MaxKeys=max_keys,
            Prefix=prefix,
            StartAfter=cursor,
        )
        contents.extend(response.get("Contents", []))
        if response["KeyCount"] < max_keys:
            break

        cursor = response["Contents"][-1]["Key"]

    sorted_keys = [obj["Key"] for obj in sorted(contents, key=lambda x: x["LastModified"])]

    if not since_key or since_key not in sorted_keys:
        return sorted_keys

    for idx, key in enumerate(sorted_keys):
        if key == since_key:
            return sorted_keys[idx + 1 :]

    return []


# @sensor(job=week_3_pipeline_docker, minimum_interval_seconds=30)
# def docker_sensor(context):
#     new_files = get_s3_keys(
#         bucket=context.resources.s3.bucket, prefix="prefix", endpoint_url=context.resources.s3.endpoint_url
#     )
#     if not new_files:
#         yield SkipReason("No new s3 files found in bucket")
#         return
#     for file in new_files:
#         yield RunRequest(
#             run_key=file,
#             run_config={
#                 "resources": {
#                     "s3": {
#                         "config": {
#                             "bucket": "dagster",
#                             "access_key": "test",
#                             "secret_key": "test",
#                             "endpoint_url": "http://localstack:4566",
#                         }
#                     },
#                     "redis": {
#                         "config": {
#                             "host": "redis",
#                             "port": 6379,
#                         }
#                     },
#                 },
#                 "ops": {"get_s3_data": {"config": {"s3_key": file}}},
#             },
#         )
