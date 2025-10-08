from fastapi import FastAPI
from pydantic import BaseModel
from kafka import KafkaConsumer
import json
import time
from threading import Thread
from collections import defaultdict

app = FastAPI()

# --- Настройки Kafka ---
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "BTC-USDT-trades"

# --- Структуры данных ---
class Candle(BaseModel):
    ws: int      # window start timestamp (ms)
    open: float
    high: float
    low: float
    close: float
    volume: float
    instId: str

# Хранилище свечей в памяти
candles_store = defaultdict(dict)

CANDLE_INTERVAL = 300_000  # 5 минут в миллисекундах
PRICE_STEP = 0.1

# --- Функция агрегации ---
def aggregate_trade(trade):
    inst = trade["instId"]
    ts = int(trade.get("ts", time.time() * 1000))
    if ts < 1e12:  # переводим в миллисекунды
        ts *= 1000

    px = round(float(trade.get("px", 0)) / PRICE_STEP) * PRICE_STEP
    sz = float(trade.get("sz", 0))
    
    ws = ts - (ts % CANDLE_INTERVAL)

    if ws not in candles_store[inst]:
        candles_store[inst][ws] = Candle(
            ws=ws,
            open=px,
            high=px,
            low=px,
            close=px,
            volume=sz,
            instId=inst
        )
    else:
        candle = candles_store[inst][ws]
        candle.high = max(candle.high, px)
        candle.low = min(candle.low, px)
        candle.close = px
        candle.volume += sz

# --- Kafka Consumer в отдельном потоке ---
def consume_kafka():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    for message in consumer:
        trade = message.value
        aggregate_trade(trade)

Thread(target=consume_kafka, daemon=True).start()

# --- Эндпоинты для Grafana JSON API ---

@app.get("/")
def root():
    return {"status": "ok"}

@app.post("/metrics")
def metrics():
    """
    Возвращаем метрики как поля свечи, которые можно сопоставить в Grafana.
    """
    return [
        {"text": "time", "value": "time"},
        {"text": "open", "value": "open"},
        {"text": "high", "value": "high"},
        {"text": "low", "value": "low"},
        {"text": "close", "value": "close"},
        {"text": "volume", "value": "volume"},
    ]

@app.post("/metric-payload-options")
def metric_payload_options():
    return [
        {"text": "Инструмент", "type": "string", "name": "instId",
         "options": ["BTC-USDT"], "defaultValue": "BTC-USDT"}
    ]

@app.post("/query")
def query(payload: dict):
    """
    Возвращаем реальные данные свечей из Kafka.
    Формат для Grafana: datapoints = [{timestamp: [open, high, low, close, volume]}]
    """
    results = []
    targets = payload.get("targets", [])

    for target in targets:
        inst = target.get("target", "BTC-USDT")
        candles = sorted(candles_store[inst].values(), key=lambda x: x.ws)

        datapoints = [
            {int(c.ws if c.ws > 1e12 else c.ws * 1000): [c.open, c.high, c.low, c.close, c.volume]}
            for c in candles
        ]

        results.append({
            "target": inst,
            "datapoints": datapoints
        })

    return results
