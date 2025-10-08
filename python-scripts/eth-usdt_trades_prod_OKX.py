import asyncio
import websockets
import json
import time
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "BTC-USDT-trades"
url_wsp = "wss://ws.okx.com:8443/ws/v5/public"

# Kafka config
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def send_to_kafka(topic, message):
    producer.send(topic, value=message)
    producer.flush()

async def main():
    async with websockets.connect(url_wsp) as ws:
        subscribe_msg = {
            "id": "54410001",
            "op": "subscribe",
            "args": [
                {
                    "channel": "trades",
                    "instId": "BTC-USDT"
                }
            ]
        }
        await ws.send(json.dumps(subscribe_msg))

        while True:
            msg = await ws.recv()
            try:
                data = json.loads(msg)
            except json.JSONDecodeError:
                print("Ошибка парсинга:", msg)
                continue

            if "data" in data:
                for trade in data["data"]:
                    formatted = {
                        "ts": int(trade.get("ts", time.time() * 1000)),
                        "exchange": "OKX",
                        "instId": trade.get("instId", "BTC-USDT"),
                        "px": float(trade.get("px", 0)),
                        "sz": float(trade.get("sz", 0)),
                        "side": trade.get("side", "buy"),
                        "type": "trade"
                    }
                    send_to_kafka(TOPIC, formatted)
                    #print(formatted)

if __name__ == "__main__":
    asyncio.run(main())
