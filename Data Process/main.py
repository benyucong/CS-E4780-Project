from quixstreams import Application
from quixstreams.models import TimestampType
import os
import time
from typing import Any, Optional, List, Tuple
from datetime import timedelta
from query2 import query2_calculation

docs_topic_name = 'dbs2022-trading'

inputtopicname = docs_topic_name
outputtopicname = "output-debs2022-trading"
consumergroup_name = "dashboard"

# Define the consumer application and settings
app = Application(
    broker_address="127.0.0.1:9092",
    consumer_group=consumergroup_name,
    auto_offset_reset="earliest",
    consumer_extra_config={"allow.auto.create.topics": "true"},
)
price_list = []

# Timestamp extractor must always return timestamp as an integer in milliseconds.
def timestamp_extractor(value: Any) -> int:
    time_str=value['Trading time'] if value['Trading time'] else value['Time']
    hours, minutes, seconds = map(int, time_str.split(':'))
    milliseconds = (hours * 3600 + minutes * 60 + seconds) * 1000
    return milliseconds

input_topic = app.topic(inputtopicname, value_deserializer="json", timestamp_extractor=timestamp_extractor)
print(f"Consuming from input topic: {inputtopicname}")

output_topic = app.topic(outputtopicname, value_serializer="json")
print(f"Producing to output topic: {outputtopicname}")

sdf = app.dataframe(topic=input_topic)
# sdf = sdf.update(lambda val: print(f"Received update: {val}"))

#my_stock = sdf.filter(lambda row: row['ID'] == 'A1EXZE.ETR')

# main logic for query processing, recall the definitions from Haskell/Clean :)
def initializer(value: dict) -> dict:
    """
    Initialize the state for aggregation when a new window starts.

    It will prime the aggregation when the first record arrives 
    in the window.
    """
    return {}

def reducer(aggregated: dict, value: dict) -> dict:
    """
    Reducer always receives two arguments:
    - previously aggregated value (the "aggregated" argument)
    - current value (the "value" argument)
    It combines them into a new aggregated value and returns it.
    This aggregated value will be also returned as a result of the window.
    """
    stock_id = value.get("doc_id")
    if stock_id is None:
        return aggregated  # Skip if no stock ID is present

    # Initialize stock entry if not already present in `aggregated`
    if stock_id not in aggregated:
        aggregated[stock_id] = {
            "prices": [],
            "cumulative_price": 0,
            "price_count": 0,
            "last_price": None,
            "ema_38": 0,
            "ema_100": 0
        }
    stock_data = aggregated[stock_id]

    if "Open" in value and value["Open"] is not None:
        price = value["Open"]
        stock_data["last_price"] = price
        stock_data["cumulative_price"] += price
        stock_data["price_count"] += 1
        print(f"Price for {stock_id}: {price}")

    return aggregated

def calculate_ema(previous_ema, price, smooth_fac):
    alpha = 2 / (smooth_fac + 1)
    ema = (price * alpha) + previous_ema  * (1 - alpha)
    return ema

def process_window(window_data):
    stock_data_dict = window_data.get("value", {})
    for stock_id, stock_data in stock_data_dict.items():
        print(f"Processing stock ID: {stock_id}")
        last_price = stock_data["last_price"]

        if last_price is None:
            continue

        stock_data["ema_38"] = calculate_ema(stock_data["ema_38"], last_price, 38)
        stock_data["ema_100"] = calculate_ema(stock_data["ema_100"], last_price, 100)

        print(f"EMA_38: {stock_data['ema_38']}, EMA_100: {stock_data['ema_100']}")

        previous_ema_38 = stock_data.get("previous_ema_38", stock_data["ema_38"])
        previous_ema_100 = stock_data.get("previous_ema_100", stock_data["ema_100"])

        query2_calculation(stock_data["ema_38"], stock_data["ema_100"], previous_ema_38, previous_ema_100)

        stock_data["previous_ema_38"] = stock_data["ema_38"]
        stock_data["previous_ema_100"] = stock_data["ema_100"]

#tumbling window, emitting results for each incoming message
sdf = (
    # Define a tumbling window of 5 minutes
    sdf.tumbling_window(duration_ms=timedelta(minutes=5))
    .reduce(reducer=reducer, initializer=initializer)
    # Emit updates for each incoming message
    .current()
    .apply(process_window)
)

sdf = sdf.to_topic(output_topic)
app.run(sdf)