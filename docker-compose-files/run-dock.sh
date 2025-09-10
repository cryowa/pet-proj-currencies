set -e

echo "Docker building Kafka..."
docker compose -f kafka.yml up -d
echo "DONE!"

echo "Docker building Minio..."
docker compose -f minio.yml up -d
echo "DONE!"

echo "Docker building Minio, Kafka and Postgres..."
docker compose -f postgres.yml up -d
echo "DONE!"

echo "Docker building Spark and Jupyter..."
docker compose -f spark-jupyter.yml up -d
echo "DONE!"

echo "Docker building Airflow..."
docker compose -f airflow.yml up -d
echo "DONE!"
echo "All containers are running:"

docker ps
