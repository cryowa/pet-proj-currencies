import asyncio
import websockets
import json
import datetime
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
url_wsp = "wss://ws.okx.com:8443/ws/v5/public"
# url_wsb = "wss://ws.okx.com:8443/ws/v5/business"


# Kafka config
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


# send to Kafka
def send_to_kafka(topic, message):
    producer.send(topic, value=message)
    producer.flush()


# 30 WebSocket connections per specific WebSocket channel per sub-account
# first one
async def main():
    url = url_wsp  # websocket route
    TOPIC = f"ws_trades_BTC-USDT"
    async with websockets.connect(url) as ws:
        subscribe_msg = {
            "id": "54410001",
            "op": "subscribe",
            "args": [
                {
                    "channel": "books",
                    "instId": "BTC-USDT"
                }
            ]
        }
        await ws.send(json.dumps(subscribe_msg))

        # read messages
        while True:
            msg = await ws.recv()
            try:
                data = json.loads(msg)  # making JSON
            except json.JSONDecodeError:
                print("Error:", msg)
                continue
            send_to_kafka(TOPIC, data)


if __name__ == "__main__":
    asyncio.run(main())
