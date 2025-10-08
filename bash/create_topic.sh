TOPIC_NAME=${1:-"trades"}  # topic name
PARTITIONS=${2:-1}                   # partitions
REPLICATION_FACTOR=${3:-1}           # replicas


KAFKA_CONTAINER="kafka" 

echo "creating topic: $TOPIC_NAME (partitions=$PARTITIONS, replication=$REPLICATION_FACTOR)"

docker exec -it $KAFKA_CONTAINER \
kafka-topics --create \
--bootstrap-server kafka:9092 \
--replication-factor $REPLICATION_FACTOR \
--partitions $PARTITIONS \
--topic $TOPIC_NAME

echo "Ready!"
