import asyncio
import websockets
import json
from datetime import datetime
from kafka import KafkaProducer

# BTC-USDT BTC-USDC BTC-USD XRP-USDT XRP-USDC
# ETH-USDT ETH-USDC ETH-USD SOL-USDT SOL-USDC
# SOL-USD LTC-USDT LTC-USDC USDT-USD USDC-USDT
# BNB-USD BNB-USDT

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "trades"
url_wsp = "wss://ws.okx.com:8443/ws/v5/public"

# Kafka config
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def send_to_kafka(topic, message):
    producer.send(topic, value=message)
    producer.flush()

def safe_ts_to_date(ts):
    try:
        ts_int = int(ts)
        # Проверяем — миллисекунды или секунды
        if ts_int > 1e12:
            ts_int = ts_int / 1000
        return datetime.fromtimestamp(ts_int).strftime('%Y-%m-%d %H:%M:%S')
    except (TypeError, ValueError, OSError):
        return None

def safe_tradeId_to_int(id):
    try:
        id_int = int(id)
        return id_int
    except (TypeError, ValueError, OSError):
        return None

# 30 websocket cons
async def main():
    async with websockets.connect(url_wsp) as ws:
        subscribe_msg = {
            "id": "54410001",
            "op": "subscribe",
            "args": [
                {
                    "channel": "trades",    #1
                    "instId": "BTC-USDT"
                }
                ,{
                    "channel": "trades",    #2
                    "instId": "BTC-USDC"
                }
                ,{
                    "channel": "trades",  # 3
                    "instId": "BTC-USD"
                }
                ,{
                    "channel": "trades",    #4
                    "instId": "XRP-USDT"
                }
                ,{
                    "channel": "trades",    #5
                    "instId": "XRP-USDC"
                }
                ,{
                    "channel": "trades",    #6
                    "instId": "ETH-USDT"
                }
                ,{
                    "channel": "trades",    #7
                    "instId": "ETH-USDC"
                }
                , {
                    "channel": "trades",  # 8
                    "instId": "ETH-USD"
                }
                , {
                    "channel": "trades",    #9
                    "instId": "SOL-USDT"
                }
                , {
                    "channel": "trades",    #10
                    "instId": "SOL-USDC"
                }
                , {
                    "channel": "trades",  # 11
                    "instId": "SOL-USD"
                }
                , {
                    "channel": "trades",  # 12
                    "instId": "LTC-USDT"
                }
                , {
                    "channel": "trades",  # 13
                    "instId": "LTC-USDC"
                }
                , {
                    "channel": "trades",  # 14
                    "instId": "USDT-USD"
                }
                , {
                    "channel": "trades",  # 15
                    "instId": "USDC-USDT"
                }
                , {
                    "channel": "trades",  # 16
                    "instId": "BNB-USD"
                }
                , {
                    "channel": "trades",  # 17
                    "instId": "BNB-USDT"
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
                #print(data)
                for trade in data["data"]:

                    formatted = {
                        "date": safe_ts_to_date(trade.get("ts", None)),
                        "exchange": "OKX",
                        "tradeId" : safe_tradeId_to_int(trade.get("tradeId", None)),
                        "instance": trade.get("instId", None),
                        "price": trade.get("px", 0),
                        "size": trade.get("sz", 0),
                        "side": trade.get("side", "buy"),
                        "type": "trade"
                    }
                    send_to_kafka(TOPIC, formatted)
                    print(formatted)

if __name__ == "__main__":
    asyncio.run(main())
