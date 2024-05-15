#!/bin/bash
# Create Kafka topics

KAFKA_BOOTSTRAP=localhost:9092

echo "Creating topics..."

kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --if-not-exists \
    --topic raw-events --partitions 3 --replication-factor 1

kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --if-not-exists \
    --topic processed-events --partitions 3 --replication-factor 1

kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --if-not-exists \
    --topic alerts --partitions 1 --replication-factor 1

echo "Topics created:"
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --list
