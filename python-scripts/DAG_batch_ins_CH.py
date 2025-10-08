from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from clickhouse_driver import Client
from decimal import Decimal
import json

# Конфиги
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
KAFKA_TOPIC = "trades"
CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_DB = "ccys"
CLICKHOUSE_TABLE = "trades"


def extract_from_kafka(**context):
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        value_deserializer = lambda m: json.loads(m.decode('utf-8')),
        enable_auto_commit=True,
        group_id = "airflow-batch-loader",
        auto_offset_reset='earliest',
    )

    messages = []

    for message in consumer:
        data = message.value

        messages.append(json.loads(data.decode("utf-8")))

    consumer.close()
    return messages


def load_to_clickhouse(**context):
    messages = context['ti'].xcom_pull(task_ids='extract')
    if not messages:
        return

    client = Client(
        host=CLICKHOUSE_HOST,
        user='user',
        password='password',
        database=CLICKHOUSE_DB
    )
    data = [(m["date"], m["exchange"], m["tradeId"], m["instance"], Decimal(m["price"]), Decimal(m["size"]), m["side"], m["type"]) for m in messages]

    client.execute(
        f"INSERT INTO {CLICKHOUSE_TABLE} (date, exchange, tradeId, instance, price, size, side, type) VALUES",
        data
    )


with DAG(
        dag_id="kafka_to_clickhouse_batch",
        default_args={"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)},
        schedule_interval="0 * * * *",
        start_date=datetime(2025, 1, 1),
        catchup=False
) as dag:
    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_from_kafka,
        provide_context=True
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_to_clickhouse,
        provide_context=True
    )

    extract >> load
