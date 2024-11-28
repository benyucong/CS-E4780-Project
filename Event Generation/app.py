from flask import Flask, render_template
from datetime import datetime
from kafka import KafkaConsumer
import json

app = Flask(__name__)

#consumer=KafkaConsumer('financial_topic',bootstrap_servers='localhost:9092',auto_offset_reset='earliest',group_id='consumer-group-a')

# Mock data for financial information
financial_data = [
    {"stock_id": "IPRUS.FR", "timestamp": "07:30:45.111", "advisory": "Buy"},
    {"stock_id": "IEWLD.FR", "timestamp": "07:30:45.111", "advisory": "Sell"},
    {"stock_id": "IPUST.FR", "timestamp": "07:30:45.111", "advisory": "Hold"},
]
'Unique ID', 'Trading time', 'Advisory'

"""financial_data = []
for message in consumer:
    financial_data.append(json.loads(message.value))
    if len(financial_data)>=3:
        break
"""

@app.route("/")
def home():
    return render_template("index.html", data=financial_data)

if __name__ == "__main__":
    app.run(debug=True)
