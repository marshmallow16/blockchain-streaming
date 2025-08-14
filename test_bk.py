import json
from confluent_kafka import Producer

BOOTSTRAP = "127.0.0.1:19092"
TOPIC = "eth.transfers.raw"

p = Producer({"bootstrap.servers": BOOTSTRAP})
p.produce(TOPIC, json.dumps({"startup":"ok-ko"}).encode("utf-8"))
p.flush(10)
print("confluent send OK")
