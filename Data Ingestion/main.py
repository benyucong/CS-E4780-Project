# This code will publish the CSV data to a stream as if the data were being generated in real-time.
import json
import time
import uuid
import os
import pandas as pd
import orjson

# Quix stuff
from quixstreams.kafka import Producer
from quixstreams import Application, State 

docs_topic_name = 'dbs2022-trading'

df = pd.read_csv('../datasets/filtered_trading_data_Monday.csv', comment='#', delimiter=",",index_col=False)
#df = pd.read_csv('../datasets/demo.csv', comment='#')

df.columns = df.columns.str.strip()

outputtopicname = docs_topic_name
# offsetlimit = len(df)-2
print(f"Producing to output topic: {outputtopicname}...\n\n")

with Producer(
    broker_address="127.0.0.1:9092",
    extra_config={"allow.auto.create.topics": "true"},
) as producer:
    for index, row in df.iterrows():
        doc_id = index
        doc_key = f"A{'0'*(10-len(str(doc_id)))}{doc_id}"
        doc_uuid = str(uuid.uuid4())
        row = row.replace({pd.NA: None})
        # Read all relevant columns
        value = {
            "Timestamp": time.time_ns(),
            "doc_id": doc_id,
            "doc_uuid": doc_uuid,
            "ID": row['ID'],
            "SecType": row['SecType'],
            "Time": row['Time'],
            "Last": row['Last'],
            "Trading time": row['Trading time'],
            "Trading date": row['Trading date'],
        }
        #print(f"Producing value: {value}")
        
        producer.produce(
            topic=outputtopicname,
            headers=[("uuid", doc_uuid)],  # a dict is also allowed here
            key=doc_key,
            value=json.dumps(value),  # needs to be a string
        )