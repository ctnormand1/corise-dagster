import csv
from datetime import datetime
from typing import Iterator, List

from dagster import In, Nothing, Out, String, job, op, usable_as_dagster_type
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: List[List]):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


def csv_helper(file_name: str) -> Iterator[Stock]:
    with open(file_name) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield Stock.from_list(row)


@op
def get_s3_data(context) -> List[Stock]:
    file_name = context.op_config["s3_key"]
    return [stock for stock in csv_helper(file_name)]


@op
def process_data(context, stocks: List[Stock]) -> Aggregation:
    biggest_baddest_stock = sorted(stocks, key=lambda x: x.high, reverse=True)[0]
    return Aggregation(date=biggest_baddest_stock.date, high=biggest_baddest_stock.high)


@op
def put_redis_data(context, aggregation: Aggregation):
    pass


@job
def week_1_pipeline():
    put_redis_data(process_data(get_s3_data()))
