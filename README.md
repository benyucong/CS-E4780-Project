# CS-E4780-Project

## Virtual Enviroment

### Install Kafka locally on Ubuntu

```bash
#Install java first
sudo apt update
sudo apt install default-jdk

curl -sSOL https://dlcdn.apache.org/kafka/3.6.1/kafka_2.13-3.6.1.tgz
tar -xzf kafka_2.13-3.6.1.tgz

./kafka_2.13-3.6.1/bin/zookeeper-server-start.sh -daemon ./kafka_2.13-3.6.1/config/zookeeper.properties
./kafka_2.13-3.6.1/bin/kafka-server-start.sh -daemon ./kafka_2.13-3.6.1/config/server.properties
```

Create `python=3.11` virtual environment, then install dependencies

```bash
python3.11 -m venv myenv
source myenv/bin/activate
python -m pip install quixstreams
pip install -r requirements.txt
```

## Tools

### Kafka

### [Quix](https://github.com/quixio/quix-streams)

Quix Streams is an end-to-end framework for real-time Python data engineering, operational analytics and machine learning on Apache Kafka data streams. Extract, transform and load data reliably in fewer lines of code using your favourite Python libraries.

Build data pipelines and event-driven microservice architectures leveraging Kafka's low-level scalability, resiliency and durability features in a lightweight library without server-side clusters to manage.

## Useful References
[Continuously updating a vector store](https://quix.io/templates/continuously-update-a-vector-store-with-new-embeddings)