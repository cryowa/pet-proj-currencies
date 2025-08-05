set -e

echo "Docker building Minio, Kafka and Postgres..."
docker compose -f minio-kafka-postgres.yml up -d
echo "DONE!"

echo "Docker building Spark and Jupyter..."
docker compose -f spark-jupyter.yml up -d
echo "DONE!"

echo "Docker building Airflow..."
docker compose -f airflow.yml up -d
echo "DONE!"
echo "All containers are running:"

docker ps
