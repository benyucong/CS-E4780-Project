from quixstreams import Application
import os
import time
from datetime import timedelta

docs_topic_name = 'debs2022-trading'

inputtopicname = docs_topic_name
outputtopicname = docs_topic_name
consumergroup_name = "dashboard"

# Define the consumer application and settings
app = Application(
    broker_address="127.0.0.1:9092",
    consumer_group=consumergroup_name,
    auto_offset_reset="earliest",
    consumer_extra_config={"allow.auto.create.topics": "true"},
)

# Define an input topic with JSON deserializer
input_topic = app.topic(inputtopicname, value_deserializer="json")
print(f"Consuming from input topic: {inputtopicname}")

# Define an output topic with JSON serializer
output_topic = app.topic(outputtopicname, value_serializer="json")
print(f"Producing to output topic: {outputtopicname}")

# Initialize a streaming dataframe based on the stream of messages from the input topic:
sdf = app.dataframe(topic=input_topic)
sdf = sdf.update(lambda val: print(f"Received update: {val}"))

# main logic for query processing, recall the definitions from Haskell/Clean :)
def initializer(value: dict) -> dict:
    """
    Initialize the state for aggregation when a new window starts.

    It will prime the aggregation when the first record arrives 
    in the window.
    """
    pass

def reducer(aggregated: dict, value: dict) -> dict:
    """
    Reducer always receives two arguments:
    - previously aggregated value (the "aggregated" argument)
    - current value (the "value" argument)
    It combines them into a new aggregated value and returns it.
    This aggregated value will be also returned as a result of the window.
    """
    pass



#tumbling window, emitting results for each incoming message
sdf = (
    # Define a tumbling window of 5 minutes
    sdf.tumbling_window(duration_ms=timedelta(minutes=5))
    .reduce(reducer=reducer, initializer=initializer)
    # Emit updates for each incoming message
    .current()
)

sdf = sdf.to_topic(output_topic)
app.run(sdf)