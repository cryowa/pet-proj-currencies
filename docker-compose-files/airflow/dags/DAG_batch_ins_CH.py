from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import TaskInstance

from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata

from clickhouse_driver import Client

from datetime import datetime, timedelta
import time
from decimal import Decimal, ROUND_DOWN
import json


KAFKA_BOOTSTRAP_SERVER = "kafka:29092"
KAFKA_TOPIC = "trades"
CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_DB = "ccys"
CLICKHOUSE_TABLE = "trades"

def extract_from_kafka(**context):
    ti = context['ti']
    ti.log.info("Starting Kafka batch extraction (time-limited)...")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        enable_auto_commit=False,          # вручную коммитим после чтения
        group_id="airflow-loader-timebatch-1",
        auto_offset_reset='earliest',        # начинаем с последнего непрочитанного
        max_poll_records=5000,
        fetch_max_wait_ms=1000,
    )

    messages = []
    total_read = 0

    READ_DURATION_SEC = 15
    start_time = time.time()

    try:
        while (time.time() - start_time) < READ_DURATION_SEC:
            records = consumer.poll(timeout_ms=1000, max_records=5000)
            if not records:
                continue
            for tp, recs in records.items():
                for r in recs:
                    messages.append(r.value)
                # Коммитим оффсет последнего сообщения
                last_offset = recs[-1].offset + 1
                
                consumer.commit({tp: OffsetAndMetadata(last_offset, None, leader_epoch=-1)})

            total_read += sum(len(v) for v in records.values())

        ti.log.info(f"Read window closed. Total {total_read} messages read.")

    except Exception as e:
        ti.log.error(f"Kafka read error: {e}")
        raise
    finally:
        consumer.close()
        ti.log.info("Kafka consumer closed.")

    return messages


def load_to_clickhouse(**context):
    messages = context['ti'].xcom_pull(task_ids='extract_from_kafka')
    if not messages:
        return

    client = Client(
        host=CLICKHOUSE_HOST,
        user='user',
        password='password',
        database=CLICKHOUSE_DB
    )
    data = [(datetime.fromisoformat(m["date"].replace("Z", "+00:00")), m["exchange"], m["tradeId"], m["instance"], Decimal(m["price"]).quantize(Decimal('0.0001'), rounding=ROUND_DOWN), Decimal(m["size"]).quantize(Decimal('0.0000000000000001'), rounding=ROUND_DOWN), m["side"], m["type"]) for m in messages]

    client.execute(
        f"INSERT INTO {CLICKHOUSE_TABLE} (date, exchange, tradeId, instance, price, size, side, type) VALUES",
        data
    )


with DAG(
        dag_id="kafka_to_clickhouse_batch",
        default_args={"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)},
        schedule_interval="*/10 * * * *",
        start_date=datetime(2025, 1, 1),
        catchup=False
) as dag:
    extract = PythonOperator(
        task_id="extract_from_kafka",
        python_callable=extract_from_kafka,
        provide_context=True
    )

    load = PythonOperator(
        task_id="load_to_clickhouse",
        python_callable=load_to_clickhouse,
        provide_context=True
    )

    extract >> load
