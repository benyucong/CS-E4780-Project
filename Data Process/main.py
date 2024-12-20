from quixstreams import Application
from quixstreams.models import TimestampType
from quixstreams.kafka import Producer
import json
import uuid
import os
import time
from typing import Any, Optional, List, Tuple
from datetime import timedelta
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

app.clear_state()

# Timestamp extractor must always return timestamp as an integer in milliseconds.
def timestamp_extractor(
    value: Any,
    headers: Optional[List[Tuple[str, bytes]]],
    timestamp: float,
    timestamp_type: TimestampType,
) -> int:
    if value['Trading time']:
        time_str = value['Trading time']
    else:
        return 0
    hours, minutes, seconds = time_str.split(':')
    hours = int(hours)
    minutes = int(minutes)
    seconds = int(float(seconds))
    milliseconds = (hours * 3600 + minutes * 60 + seconds) * 1000
    return milliseconds

input_topic = app.topic(name=inputtopicname, value_deserializer="json", timestamp_extractor=timestamp_extractor)
print(f"Consuming from input topic: {inputtopicname}")

output_topic = app.topic(name=outputtopicname, value_serializer="json")
print(f"Producing to output topic: {outputtopicname}")

sdf = app.dataframe(topic=input_topic)
# sdf = sdf.update(lambda val: print(f"Received update: {val}"))

# filter the stock by timestamp
sdf = sdf.filter(lambda val: val['Trading time'] is not None and val['Last'] is not None)

# store the emas for each stock of prev window
known_stock_id_emas = dict()
# store the emas for each stock of current window
window_buffer = dict()
output_data = dict()

# main logic for query processing, recall the definitions from Haskell/Clean :)
def initializer(value: dict) -> dict:
    """
    Initialize the state for aggregation when a new window starts.

    It will prime the aggregation when the first record arrives 
    in the window.
    1, move the items from window_buffer into the known_stock_id_emas
    2, Initiate EMA38 and EMA100 to 0 if stock not seen before. Otherwise, keep the previous windows' values.
    """
    start_time = time.time_ns()
    queue_time = start_time - value['Arrival Time']
    global window_buffer
    global known_stock_id_emas
    # known_stock_id_emas update the values from window_buffer
    known_stock_id_emas.update(window_buffer)
    if value['ID'] not in known_stock_id_emas:
        known_stock_id_emas[value['ID']] = {'EMA38': 0, 'EMA100': 0}
    new_ema38 = calculate_ema(known_stock_id_emas[value['ID']]['EMA38'], value['Last'], 38)
    new_ema100 = calculate_ema(known_stock_id_emas[value['ID']]['EMA100'], value['Last'], 100)
    # new window buffer starts with the new emas
    window_buffer = {
        value['ID']: {'EMA38': new_ema38, 'EMA100': new_ema100}
    }
    advice = query2_calculation(new_ema38, new_ema100, known_stock_id_emas[value['ID']]['EMA38'], known_stock_id_emas[value['ID']]['EMA100'])
    # window_buffer[value['ID']]['advice'] = advice
    process_time = time.time_ns() - start_time
    return {
        "Stock ID": value['ID'],
        "Advice": advice,
        "Process Time": process_time,
        "Queue Time": queue_time
    }

def reducer(aggregated: dict, value: dict) -> dict:
    """
    Reducer always receives two arguments:
    - previously aggregated value (the "aggregated" argument)
    - current value (the "value" argument)
    It combines them into a new aggregated value and returns it.
    This aggregated value will be also returned as a result of the window.
    """
    start_time = time.time_ns()
    queue_time = start_time - value['Arrival Time']
    global known_stock_id_emas
    global window_buffer
    if value['ID'] not in known_stock_id_emas:
        known_stock_id_emas[value['ID']] = {'EMA38': 0, 'EMA100': 0}
    new_ema38 = calculate_ema(known_stock_id_emas[value['ID']]['EMA38'], value['Last'], 38)
    new_ema100 = calculate_ema(known_stock_id_emas[value['ID']]['EMA100'], value['Last'], 100)
    advice = query2_calculation(new_ema38, new_ema100, known_stock_id_emas[value['ID']]['EMA38'], known_stock_id_emas[value['ID']]['EMA100'])
    # global output_data
    # output_data = {
    #     "Timestamp": value['Trading time'],
    #     "Stock ID": value['ID'],
    #     "EMA38": new_ema38,
    #     "EMA100": new_ema100,
    #     "Advice": advice,
    # }
    # print(f"Advice for stock {value['ID']}: {advice}")
    # with Producer(
    #     broker_address="127.0.0.1:9092",
    #     extra_config={"allow.auto.create.topics": "true"},
    # ) as producer:
    #     producer.produce(
    #         topic=outputtopicname,
    #         headers=[("uuid", str(uuid.uuid4()))],
    #         key=value['ID'],
    #         value=json.dumps(output_data),
    #     )
    window_buffer.update({value['ID']: {'EMA38': new_ema38, 'EMA100': new_ema100}})   
    process_time = time.time_ns() - start_time
    return {
        "Stock ID": value['ID'],
        "Advice": advice,
        "Process Time": process_time,
        "Queue Time": queue_time
    }

def calculate_ema(previous_ema, price, smooth_fac):
    alpha = 2 / (smooth_fac + 1)
    ema = (price * alpha) + previous_ema  * (1 - alpha)
    return ema

#tumbling window, emitting results for each incoming message
sdf = (
    sdf.tumbling_window(duration_ms=timedelta(minutes=5))
    .reduce(reducer=reducer, initializer=initializer)
    .current()
    .apply(lambda val: val['value'])
    .filter(lambda val: val['Advice'] != 0 if 'Advice' in val else False)
    .update(print)
)

# Query 2 starts here
def query2_calculation(ema_38, ema_100, previous_ema_38, previous_ema_100):
    # print(f"EMA_38: {ema_38}, EMA_100: {ema_100}, previous_EMA_38: {previous_ema_38}, previous_EMA_100: {previous_ema_100}")
    if ema_38 > ema_100 and previous_ema_38 <= previous_ema_100:
        # print(f"Buy Detected")
        return 1
    elif ema_38 < ema_100 and previous_ema_38 >= previous_ema_100:
        # print(f"Sell Detected")
        return -1
    else:
        return 0

sdf = sdf.to_topic(output_topic)
app.run(sdf)