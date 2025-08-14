from confluent_kafka import Producer
import json

BOOTSTRAP = "127.0.0.1:19092"
TOPIC = "eth.transfers.raw"

msg = {
    "chain": "ethereum",
    "kind": "erc20_transfer",
    "blockNumber": 23123602,
    "txHash": "0xcdb75d29b26fdf2a4fdd9650d45cc10473e39910f1566676cd32e03a8f68f376",
    "logIndex": 7,
    "token": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
    "from": "0x8681592aFEC55D8E06b155e208b0B512135A39d1",
    "to":   "0xDf31A70a21A1931e02033dBBa7DEaCe6c45cfd0f",  # mixer -> will trigger "mixer_interaction"
    "value": "22689983668",
    "ts": 1755145700  # +2 hours
}

p = Producer({"bootstrap.servers": BOOTSTRAP, "compression.type": "zstd"})
key = f"{msg['txHash']}:{msg['logIndex']}".encode()
p.produce(TOPIC, json.dumps(msg).encode("utf-8"), key=key)
p.flush(5)
print("dummy transfer sent")
