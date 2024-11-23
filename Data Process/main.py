from quixstreams import Application
from quixstreams.models import TimestampType
import os
import time
from typing import Any, Optional, List, Tuple
from datetime import timedelta
from query2 import query2_calculation
import copy

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

# Timestamp extractor must always return timestamp as an integer in milliseconds.
def timestamp_extractor(
    value: Any,
    headers: Optional[List[Tuple[str, bytes]]],
    timestamp: float,
    timestamp_type: TimestampType,
) -> int:
    if value['Trading time']:
        time_str = value['Trading time']
        print(f"Timestamp: {time_str}")
    else:
        return 0
    hours, minutes, seconds = time_str.split(':')
    hours = int(hours)
    minutes = int(minutes)
    seconds = int(float(seconds))
    milliseconds = (hours * 3600 + minutes * 60 + seconds) * 1000
    return milliseconds

input_topic = app.topic(inputtopicname, value_deserializer="json", timestamp_extractor=timestamp_extractor)
print(f"Consuming from input topic: {inputtopicname}")

output_topic = app.topic(outputtopicname, value_serializer="json")
print(f"Producing to output topic: {outputtopicname}")

sdf = app.dataframe(topic=input_topic)
# sdf = sdf.update(lambda val: print(f"Received update: {val}"))

# filter the stock by timestamp
sdf = sdf.filter(lambda val: val['Trading time'] is not None and val['Last'] is not None)

# store the emas for each stock of prev window
known_stock_id_emas = dict()
# store the emas for each stock of current window
window_buffer = dict()

# main logic for query processing, recall the definitions from Haskell/Clean :)
def initializer(value: dict) -> dict:
    """
    Initialize the state for aggregation when a new window starts.

    It will prime the aggregation when the first record arrives 
    in the window.
    1, move the items from window_buffer into the known_stock_id_emas
    2, Initiate EMA38 and EMA100 to 0 if stock not seen before. Otherwise, keep the previous windows' values.
    """
    global window_buffer
    global known_stock_id_emas
    # shallow copy the window buffer
    known_stock_id_emas = window_buffer.copy()
    if value['ID'] not in known_stock_id_emas:
        known_stock_id_emas[value['ID']] = {'EMA38': 0, 'EMA100': 0}
    print(value['Last'])
    new_ema38 = calculate_ema(known_stock_id_emas[value['ID']]['EMA38'], value['Last'], 38)
    new_ema100 = calculate_ema(known_stock_id_emas[value['ID']]['EMA100'], value['Last'], 100)
    # new window buffer starts with the new emas
    window_buffer = {
        value['ID']: {'EMA38': new_ema38, 'EMA100': new_ema100}
    }
    print(f"EMA38: {new_ema38}, EMA100: {new_ema100}") 
    return window_buffer

def reducer(aggregated: dict, value: dict) -> dict:
    """
    Reducer always receives two arguments:
    - previously aggregated value (the "aggregated" argument)
    - current value (the "value" argument)
    It combines them into a new aggregated value and returns it.
    This aggregated value will be also returned as a result of the window.
    """
    global known_stock_id_emas
    if value['ID'] not in known_stock_id_emas:
        known_stock_id_emas[value['ID']] = {'EMA38': 0, 'EMA100': 0}
    print(value['Last'])
    new_ema38 = calculate_ema(known_stock_id_emas[value['ID']]['EMA38'], value['Last'], 38)
    new_ema100 = calculate_ema(known_stock_id_emas[value['ID']]['EMA100'], value['Last'], 100) 
    print(f"EMA38: {new_ema38}, EMA100: {new_ema100}")

    aggregated.update({value['ID']: {'EMA38': new_ema38, 'EMA100': new_ema100}})    
    return aggregated

def calculate_ema(previous_ema, price, smooth_fac):
    alpha = 2 / (smooth_fac + 1)
    ema = (price * alpha) + previous_ema  * (1 - alpha)
    return ema

#tumbling window, emitting results for each incoming message
sdf = (
    # Define a tumbling window of 5 minutes
    sdf.tumbling_window(duration_ms=timedelta(minutes=5))
    .reduce(reducer=reducer, initializer=initializer)
    # Emit updates for each incoming message
    .current()
)

# Query 2 starts here

sdf = sdf.to_topic(output_topic)
app.run(sdf)