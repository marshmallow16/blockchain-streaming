from kafka import KafkaProducer
import json
p = KafkaProducer(bootstrap_servers="127.0.0.1:19092",
                  value_serializer=lambda v: json.dumps(v).encode(),
                  request_timeout_ms=30000, max_block_ms=60000)
f = p.send("eth.transfers.raw", {"sanity":"ok"})
print(f.get(timeout=30))
p.flush()
print("sent")
