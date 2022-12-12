from typing import List
import os

from dagster import (
    In,
    Nothing,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    graph,
    op,
    schedule,
    sensor,
    static_partitioned_config,
    String,
)
from workspaces.project.sensors import get_s3_keys
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(config_schema={"s3_key": String}, required_resource_keys={"s3"})
def get_s3_data(context) -> List[Stock]:
    s3_key = context.op_config["s3_key"]
    data = context.resources.s3.get_data(s3_key)
    return [Stock.from_list(row) for row in data]


@op
def process_data(context, stocks: List[Stock]) -> Aggregation:
    biggest_baddest_stock = sorted(stocks, key=lambda x: x.high, reverse=True)[0]
    return Aggregation(date=biggest_baddest_stock.date, high=biggest_baddest_stock.high)


@op(required_resource_keys={"redis"})
def put_redis_data(context, aggregation: Aggregation) -> Nothing:
    context.resources.redis.put_data(name=aggregation.date.strftime("%Y-%m-%d"), value=str(aggregation.high))


@op(required_resource_keys={"s3"})
def put_s3_data(context, aggregation: Aggregation) -> Nothing:
    s3_key = f"aggregations/{aggregation.date.strftime('%Y-%m-%d')}.json"
    context.resources.s3.put_data(key_name=s3_key, data=aggregation)


@graph
def week_3_pipeline():
    aggregation = process_data(get_s3_data())
    put_redis_data(aggregation)
    put_s3_data(aggregation)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


@static_partitioned_config(partition_keys=[str(i) for i in range(1, 11)])
def docker_config(partition_key):
    s3_key = f"prefix/stock_{partition_key}.csv"
    config = {
        "resources": {
            "s3": {
                "config": {
                    "bucket": "dagster",
                    "access_key": "test",
                    "secret_key": "test",
                    "endpoint_url": "http://localstack:4566",
                }
            },
            "redis": {
                "config": {
                    "host": "redis",
                    "port": 6379,
                }
            },
        },
        "ops": {
            "get_s3_data": {"config": {"s3_key": s3_key}},
        },
    }
    return config


week_3_pipeline_local = week_3_pipeline.to_job(
    name="week_3_pipeline_local",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

week_3_pipeline_docker = week_3_pipeline.to_job(
    name="week_3_pipeline_docker",
    config=docker_config,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
    op_retry_policy=RetryPolicy(max_retries=10, delay=1),
)


week_3_schedule_local = ScheduleDefinition(job=week_3_pipeline_local, cron_schedule="*/15 * * * *")


# So, this part is a little funny. I'm pretty sure this is incorrect...
@schedule(cron_schedule="0 * * * *", job=week_3_pipeline_docker)
def week_3_schedule_docker(context):
    new_files = get_s3_keys(bucket="dagster", prefix="prefix", endpoint_url="http://localstack:4566")
    if not new_files:
        yield SkipReason("No new s3 files found in bucket.")
        return
    for file in new_files:
        yield week_3_pipeline_docker.run_request_for_partition(partition_key=os.path.basename(file).split(".")[0][-1])


@sensor(job=week_3_pipeline_docker, minimum_interval_seconds=30)
def week_3_sensor_docker(context):
    new_files = get_s3_keys(bucket="dagster", prefix="prefix", endpoint_url="http://localstack:4566")
    if not new_files:
        yield SkipReason("No new s3 files found in bucket.")
        return
    for file in new_files:
        yield RunRequest(
            run_key=file,
            run_config={
                "resources": {
                    "s3": {
                        "config": {
                            "bucket": "dagster",
                            "access_key": "test",
                            "secret_key": "test",
                            "endpoint_url": "http://localstack:4566",
                        }
                    },
                    "redis": {
                        "config": {
                            "host": "redis",
                            "port": 6379,
                        }
                    },
                },
                "ops": {"get_s3_data": {"config": {"s3_key": file}}},
            },
        )
