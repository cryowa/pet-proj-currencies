set -e

echo "Docker shutting down Airflow..."
docker compose -f airflow.yml down

echo "Docker shutting down Spark and Jupyter..."
docker compose -f spark-jupyter.yml down

echo "Docker shutting down Kafka..."
docker compose -f kafka.yml down

echo "Docker shutting down Minio..."
docker compose -f minio.yml down

echo "Docker shutting down Postgres..."
docker compose -f postgres.yml down

echo "All containers are down:"

docker ps
