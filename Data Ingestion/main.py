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

df = pd.read_csv('../datasets/Monday_head200.csv', comment='#', delimiter=",",index_col=False)
print(df['Nominal value'].head())
#df = pd.read_csv('../datasets/demo.csv', comment='#')
# print(df.head())
# print(df.columns.tolist())
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
            "Date": row['Date'],
            "Time": row['Time'],
            "Ask": row['Ask'],
            "Ask volume": row['Ask volume'],
            "Bid": row['Bid'],
            "Bid volume": row['Bid volume'],
            "Ask time": row['Ask time'],
            "Day's high ask": row['Day\'s high ask'],
            "Close": row['Close'],
            "Currency": row['Currency'],
            "Day's high ask time": row['Day\'s high ask time'],
            "Day's high": row['Day\'s high'],
            "ISIN": row['ISIN'],
            "Auction price": row['Auction price'],
            "Day's low ask": row['Day\'s low ask'],
            "Day's low": row['Day\'s low'],
            "Day's low ask time": row['Day\'s low ask time'],
            "Open": row['Open'],
            "Nominal value": row['Nominal value'],
            "Last": row['Last'],
            "Last volume": row['Last volume'],
            "Trading time": row['Trading time'],
            "Total volume": row['Total volume'],
            "Mid price": row['Mid price'],
            "Trading date": row['Trading date'],
            "Profit": row['Profit'],
            "Current price": row['Current price'],
            "Related indices": row['Related indices'],
            "Day high bid time": row['Day high bid time'],
            "Day low bid time": row['Day low bid time'],
            "Open Time": row['Open Time'],
            "Last trade time": row['Last trade time'],
            "Close Time": row['Close Time'],
            "Day high Time": row['Day high Time'],
            "Day low Time": row['Day low Time'],
            "Bid time": row['Bid time'],
            "Auction Time": row['Auction Time']
        }
        if row['Open'] is not None:
            print(f"Value of nominal value for {index} is {row['Open']}")
        #print(f"Producing value: {value}")
        
        producer.produce(
            topic=outputtopicname,
            headers=[("uuid", doc_uuid)],  # a dict is also allowed here
            key=doc_key,
            value=json.dumps(value),  # needs to be a string
        )