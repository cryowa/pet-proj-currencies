from kafka import KafkaConsumer
import boto3
import json
from io import BytesIO
from datetime import datetime

# Конфигурация Kafka
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'test-topic'
GROUP_ID = 'minio-consumer-group'  # Можно любое, но лучше стабильное для offset'ов

# Конфигурация MinIO (s3-compatible)
S3_ENDPOINT_URL = 'http://localhost:9000'
S3_ACCESS_KEY = 'minio'
S3_SECRET_KEY = 'minio123'
S3_BUCKET = 'raw-crypto-data'
S3_PREFIX = 'prices/'

# Создаем Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=GROUP_ID
)

# Подключение к MinIO
s3 = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT_URL,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY
)

# Убедимся, что бакет существует
existing_buckets = s3.list_buckets()
if not any(b['Name'] == S3_BUCKET for b in existing_buckets.get('Buckets', [])):
    s3.create_bucket(Bucket=S3_BUCKET)

print("Consumer запущен. Ожидаем сообщения...")

for message in consumer:
    data = message.value
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')
    filename = f"{S3_PREFIX}api_data_{timestamp}.json"

    # Сериализуем данные в JSON и кладем в BytesIO
    buffer = BytesIO()
    buffer.write(json.dumps(data).encode('utf-8'))
    buffer.seek(0)

    # Загружаем напрямую в MinIO
    s3.upload_fileobj(buffer, S3_BUCKET, filename)
    print(f"Загружено сообщение в MinIO как {filename}")
