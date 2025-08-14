import os, sys, json, time, signal
from typing import List, Dict, Any,Tuple   

from dotenv import load_dotenv
from web3 import Web3
from confluent_kafka import Producer   # <-- use confluent

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:19092")
TOPIC = os.getenv("TOPIC_ETH", "eth.transfers.raw")

print("Kafka bootstrap =", BOOTSTRAP)

p = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "enable.idempotence": True,
    "acks": "all",
    "compression.type": "zstd",
    "linger.ms": 20,
    "batch.num.messages": 10000,
    "queue.buffering.max.messages": 500000,   # ↑ from ~100k default
    "queue.buffering.max.kbytes": 524288,     # 512MB client buffer
    "message.timeout.ms": 600000,
    "retries": 1000000,
    "max.in.flight.requests.per.connection": 5
})

# sanity send (will fail fast if broker unreachable)
p.produce(TOPIC, json.dumps({"startup": "ok"}).encode("utf-8"))
p.poll(0)
p.flush(10)
print("Kafka sanity send OK")


RPC = os.getenv("WEB3_RPC")
BLOCK_BATCH = int(os.getenv("BLOCK_BATCH", "200"))
CONFIRMATIONS = int(os.getenv("CONFIRMATIONS", "2"))
SLEEP_SEC = float(os.getenv("POLL_SLEEP_SEC", "1.5"))
CKPT_PATH = os.getenv("CHECKPOINT_FILE", ".eth_checkpoint")
TOKENS_ENV = os.getenv("TOKEN_ADDRESSES", "").strip()

if not RPC:
    print("❌ Set WEB3_RPC in .env (Infura HTTPS endpoint).")
    sys.exit(1)

# Web3 over HTTP (stable for get_logs)
w3 = Web3(Web3.HTTPProvider(RPC, request_kwargs={"timeout": 30}))

# ERC-20 Transfer signature
TRANSFER_TOPIC = Web3.keccak(text="Transfer(address,address,uint256)").hex()

# Optional address filtering
ADDRESS_FILTER = None
if TOKENS_ENV:
    ADDRESS_FILTER = [Web3.to_checksum_address(a.strip()) for a in TOKENS_ENV.split(",") if a.strip()]

_running = True


def stop(sig, frame):
    global _running
    print("\n🛑 stopping…")
    _running = False
    try:
        deadline = time.time() + 60
        while len(p)> 0 and time.time() < deadline:
            p.poll(0.1)
        p.flush(10)
    finally:
        os._exit(0)

signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)

def read_ckpt() -> int:
    if os.path.exists(CKPT_PATH):
        try:
            return int(open(CKPT_PATH).read().strip())
        except Exception:
            pass
    return -1

def write_ckpt(h: int):
    with open(CKPT_PATH, "w") as f:
        f.write(str(h))

def checksum_addr(topic_bytes) -> str:
    hb = bytes(topic_bytes)
    return Web3.to_checksum_address("0x" + hb[-20:].hex())

def to_int(data_field) -> int:
    # robust HexBytes/bytes/0x-string -> int
    try:
        return Web3.to_int(data_field)
    except Exception:
        if isinstance(data_field, (bytes, bytearray)):
            return int.from_bytes(bytes(data_field), "big")
        if isinstance(data_field, str):
            s = data_field[2:] if data_field.startswith("0x") else data_field
            return int(s, 16)
        raise

def get_logs(from_b: int, to_b: int):
    params = {
        "fromBlock": from_b,
        "toBlock": to_b,
        "topics": [TRANSFER_TOPIC],
    }
    if ADDRESS_FILTER:
        params["address"] = ADDRESS_FILTER
    return w3.eth.get_logs(params)

def fetch_range_adaptive(from_b: int, to_b: int) -> List[Dict[str, Any]]:
    """
    Split the range until each request returns <= 10k logs (Infura cap).
    """
    stack: List[Tuple[int, int]] = [(from_b, to_b)]
    out: List[Dict[str, Any]] = []
    while stack and _running:
        fb, tb = stack.pop()
        try:
            out.extend(get_logs(fb, tb))
        except Exception as ex:
            s = str(ex)
            # Infura over-limit error
            if ("-32005" in s and "10000" in s) or ("more than 10000 results" in s):
                if fb == tb:
                    print(f"⚠️ skip noisy block {fb} (10k cap)")
                    continue
                mid = (fb + tb) // 2
                stack.append((mid + 1, tb))
                stack.append((fb, mid))
            else:
                # transient or other error: split once and continue
                if fb == tb:
                    print(f"WARN get_logs [{fb}] : {ex}")
                    time.sleep(0.3)
                    continue
                mid = (fb + tb) // 2
                stack.append((mid + 1, tb))
                stack.append((fb, mid))
    return out

def main():
    head = w3.eth.block_number
    ckpt = read_ckpt()

    if ckpt >= 0:
        start_from = ckpt + 1
    else:
        safe_head = max(0, head - CONFIRMATIONS)
        start_from = max(0, safe_head - BLOCK_BATCH)

    print(f"✅ Producer → {TOPIC}")
    print(f"Bootstrap={BOOTSTRAP} | Head={head} | Start={start_from} | Batch={BLOCK_BATCH} | Conf={CONFIRMATIONS}")
    # sanity send
    p.produce(TOPIC, json.dumps({"startup":"ok"}).encode("utf-8"))
    p.flush(5)

    next_from = start_from
    while _running:
        try:
            head = w3.eth.block_number
            safe_head = max(0, head - CONFIRMATIONS)

            if next_from > safe_head:
                time.sleep(SLEEP_SEC)
                continue

            to_block = min(next_from + BLOCK_BATCH - 1, safe_head)
            if to_block < next_from:
                time.sleep(SLEEP_SEC)
                continue

            logs = fetch_range_adaptive(next_from, to_block)

            for e in logs:
                topics = e.get("topics") or []
                if len(topics) < 3:
                    continue
                try:
                    msg = {
                        "chain": "ethereum",
                        "kind": "erc20_transfer",
                        "blockNumber": e["blockNumber"],
                        "txHash": e["transactionHash"].hex(),
                        "logIndex": e["logIndex"],
                        "token": e["address"],
                        "from": checksum_addr(topics[1]),
                        "to": checksum_addr(topics[2]),
                        "value": str(to_int(e.get("data", b""))),
                        "ts": int(time.time()),
                    }
                    p.produce(TOPIC, json.dumps(msg).encode("utf-8"))
                    p.poll(0)

                    if len(p) > 400_000:   # number of messages still queued locally
                        time.sleep(0.05)

                except Exception as parse_err:
                    print("⚠️ parse:", parse_err)

            p.flush(2)                  # push batch out
            write_ckpt(to_block)        # persist progress
            next_from = to_block + 1

            if next_from > safe_head:
                time.sleep(SLEEP_SEC)

        except Exception as ex:
            print("WARN loop:", ex)
            time.sleep(1.0)

if __name__ == "__main__":
    main()
