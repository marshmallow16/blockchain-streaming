import os, json, time
from dotenv import load_dotenv
from confluent_kafka import Consumer, Producer

load_dotenv()
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:19092")
SRC = os.getenv("TOPIC_ETH", "eth.transfers.raw")
DST = os.getenv("TOPIC_ALERTS", "alerts.suspicious")

HIGH_VALUE_WEI = int(os.getenv("HIGH_VALUE_WEI", str(10**22)))  # adjust
MIXERS = set(a.strip().lower() for a in os.getenv("MIXER_ADDRESSES","").split(",") if a.strip())

c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "alerts_engine", "auto.offset.reset": "latest"})
p = Producer({"bootstrap.servers": BOOTSTRAP, "compression.type": "zstd"})

c.subscribe([SRC])
print(f"Alerts: {SRC} -> {DST} | threshold={HIGH_VALUE_WEI} | mixers={len(MIXERS)}")

def handle(rec):
    v = int(rec.get("value","0"))
    f = (rec.get("from") or "").lower()
    t = (rec.get("to") or "").lower()
    reasons = []
    if v >= HIGH_VALUE_WEI: reasons.append("high_value")
    if f in MIXERS or t in MIXERS: reasons.append("mixer_interaction")
    if reasons:
        alert = {
            "reasons": reasons,
            "txHash": rec.get("txHash"),
            "blockNumber": rec.get("blockNumber"),
            "from": rec.get("from"), "to": rec.get("to"),
            "token": rec.get("token"),
            "value": rec.get("value"),
            "ts": rec.get("ts"),
        }
        p.produce(DST, json.dumps(alert).encode("utf-8"))
        p.poll(0)

try:
    while True:
        m = c.poll(0.5)
        if not m: continue
        if m.error(): print("consumer error:", m.error()); continue
        try:
            handle(json.loads(m.value()))
        except Exception as e:
            print("alert parse err:", e)
except KeyboardInterrupt:
    pass
finally:
    c.close(); p.flush(5)
