import os
import json
import pandas as pd
from confluent_kafka import Consumer
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")  # Load from current src directory

conf = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': os.getenv("KAFKA_USERNAME"),
    'sasl.password': os.getenv("KAFKA_PASSWORD"),
    'group.id': 'dataframe-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
topic = os.getenv("KAFKA_TOPIC", "transactions")
consumer.subscribe([topic])

records = []
print(f"Reading messages from topic: {topic}")
try:
    for _ in range(100):  # Read 100 messages, adjust as needed
        msg = consumer.poll(2.0)
        if msg is None or msg.error():
            continue
        value = msg.value().decode('utf-8')
        try:
            record = json.loads(value)
            records.append(record)
        except Exception as e:
            print("Error parsing message:", e)
finally:
    consumer.close()

ggp = pd.DataFrame(records)
print(ggp.head())
ggp.to_csv('kafka_transactions.csv', index=False)
print('Saved DataFrame to kafka_transactions.csv')
