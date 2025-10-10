#!/bin/bash

# -------------------------------
# Running ymls and checking ststus
# -------------------------------

# healthcheck
check_service() {
    local url=$1
    local name=$2
    echo "Waiting for run $name, $url..."
    for i in {1..60}; do
        if curl -s "$url" >/dev/null; then
            echo "$name is ready!"
            return 0
        else
            sleep 5
        fi
    done
    echo "ERROR: $name not running in 300 sec!"
    exit 1
}

# Running Kafka + Zookeeper
#echo "running Kafka + Zookeeper..."
#docker compose -f /home/user/pet/pet-proj-currencies/docker-compose-files/kafka.yml up -d

# Checking Kafka UI
check_service "http://localhost:8080" "Kafka UI"

# Running ClickHouse
#echo "running ClickHouse..."
#docker compose -f /home/user/pet/pet-proj-currencies/docker-compose-files/click.yml up -d

# Checking ClickHouse
check_service "http://localhost:8123" "ClickHouse"

# Running Airflow
#echo "running Airflow..."
#docker compose -f /home/user/pet/pet-proj-currencies/docker-compose-files/airflow.yml up -d

# Checking Airflow
check_service "http://localhost:8081" "Airflow"

# Running producer
echo "Running WebSocket producer..."
python3 /home/user/pet/pet-proj-currencies/python-scripts/trades_prod_ws_OKX.py &

echo "Pipeline is running!"
echo "Kafka UI: http://localhost:8080"
echo "Airflow: http://localhost:8081"
