import os
import json
import time
from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv(dotenv_path=".env")

bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
username = os.getenv("KAFKA_USERNAME")
password = os.getenv("KAFKA_PASSWORD")
topic = os.getenv("KAFKA_TOPIC", "transactions")

conf = {
    'bootstrap.servers': bootstrap,
    'client.id': 'test-producer',
    'linger.ms': 5,
    'debug': 'security,broker,protocol',  # Enable detailed debug logs
}
if username and password:
    conf.update({
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': username,
        'sasl.password': password,
    })

print('Using bootstrap:', bootstrap)
print('Using topic:', topic)

p = Producer(conf)

msg = {'test': 'hello', 'ts': time.time()}
try:
    p.produce(topic, key='test-key', value=json.dumps(msg))
    p.flush(10)
    print('Message produced and flushed successfully')
except Exception as e:
    print('Produce failed:', repr(e))
    raise
