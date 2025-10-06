#!/bin/bash

# -------------------------------
# Running ymls and checking ststus
# -------------------------------

# healthcheck
check_service() {
    local url=$1
    local name=$2
    echo "Ожидание запуска $name на $url..."
    for i in {1..30}; do
        if curl -s "$url" >/dev/null; then
            echo "$name готов!"
            return 0
        else
            sleep 5
        fi
    done
    echo "ERROR: $name not running in 150 sec!"
    exit 1
}

# Running Kafka + Zookeeper
echo "running Kafka + Zookeeper..."
docker-compose -f /home/user/pet-project/docker-compose-files/kafka.yml up -d

# Checking Kafka UI
check_service "http://localhost:8080" "Kafka UI"

# Checking ksqlDB REST API
check_service "http://localhost:8088" "ksqlDB"

# Running Grafana
echo "running Grafana..."
docker-compose -f /home/user/pet-project/docker-compose-files/grafana.yml up -d

# Checking Grafana
check_service "http://localhost:3000" "Grafana"

# Running producer
echo "Running WebSocket producer..."
python3 /home/user/pet-project/python-scripts/btc_usdt_trades_prod_ws_OKX.py &

echo "Pipeline is running!"
echo "Kafka UI: http://localhost:8080"
echo "ksqlDB REST: http://localhost:8088"
echo "Grafana: http://localhost:3000"