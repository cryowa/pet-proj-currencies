import asyncio
import websockets
import json
import datetime
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
url_ws = "wss://ws.okx.com:8443/ws/v5/public"


# Конфиг Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Отправка в Kafka
def send_to_kafka(topic, message):
    producer.send(topic, value=message)
    producer.flush()

async def main():
    url = url_ws   # Адрес вебсокета
    TOPIC = "test_topic_orderbook"
    async with websockets.connect(url) as ws:
        
        # Пример подписки (формат зависит от API)
        subscribe_msg = {
            "id": "1512",
            "op": "subscribe",
            "args": [
                {
                "channel": "books",
                "instId": "BTC-USDT"
                }
            ]
        }
        await ws.send(json.dumps(subscribe_msg))
        
        # Чтение сообщений
        while True:
            msg = await ws.recv()
            try:
                data = json.loads(msg)  # Парсим JSON
            except json.JSONDecodeError:
                print("Ошибка парсинга:", msg)
                continue
            
            print("Recieved massage from WSocket:", datetime.datetime.now())
            send_to_kafka(TOPIC, data)

if __name__ == "__main__":
    asyncio.run(main())
