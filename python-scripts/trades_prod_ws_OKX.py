import asyncio
import websockets
import json
from datetime import datetime
from kafka import KafkaProducer
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "trades"
URL_WSP = "wss://ws.okx.com:8443/ws/v5/public"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def send_to_kafka(topic, message):
    try:
        producer.send(topic, value=message)
    except Exception as e:
        print(f"Kafka send error: {e}")
    producer.flush()

def safe_ts_to_date(ts):
    try:
        ts_int = int(ts)
        if ts_int > 1e12:
            ts_int = ts_int / 1000
        return datetime.fromtimestamp(ts_int).strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return None

def safe_tradeId_to_int(id):
    try:
        return int(id)
    except Exception:
        return None

subscribe_msg = {
    "id": "54410001",
    "op": "subscribe",
    "args": [
        {"channel": "trades", "instId": s}
        for s in [
            "BTC-USDT", "BTC-USDC", "BTC-USD",
            "XRP-USDT", "XRP-USDC",
            "ETH-USDT", "ETH-USDC", "ETH-USD",
            "SOL-USDT", "SOL-USDC", "SOL-USD",
            "LTC-USDT", "LTC-USDC",
            "USDT-USD", "USDC-USDT",
            "BNB-USD", "BNB-USDT"
        ]
    ]
}

async def consume_trades():
    while True:
        try:
            async def on_pong(message: bytes):
                print(f"← Pong message: {message!r}")

            print("Connecting to OKX WebSocket...")
            async with websockets.connect(
                URL_WSP,
                ping_interval=20,      # отправляем пинг каждые 20 секунд
                ping_timeout=10,       # ждем pong не дольше 10 секунд
                close_timeout=1,       # если не закрывается — разрываем
                max_queue=None         
            ) as ws:
                ws.pong_handler = on_pong
                print("Connected")
                await ws.send(json.dumps(subscribe_msg))
                print("Subscribed to trades channels.")

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
                                "date": safe_ts_to_date(trade.get("ts")),
                                "exchange": "OKX",
                                "tradeId": safe_tradeId_to_int(trade.get("tradeId")),
                                "instance": trade.get("instId"),
                                "price": trade.get("px"),
                                "size": trade.get("sz"),
                                "side": trade.get("side", "buy"),
                                "type": "trade"
                            }
                            send_to_kafka(TOPIC, formatted)
                            print(formatted)

        except (ConnectionClosedError, ConnectionClosedOK) as e:
            print(f"⚠️ WebSocket connection closed: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"❌ Unexpected error: {e}. Reconnecting in 10s...")
            await asyncio.sleep(10)

async def main():
    await consume_trades()

if __name__ == "__main__":
    asyncio.run(main())
