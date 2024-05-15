# Kafka Streaming Pipeline

A real-time data streaming application using Apache Kafka with Python producers, consumers, and stream processing.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌──────────────────┐     ┌─────────────┐
│   Producer  │────▶│    Kafka    │────▶│ Stream Processor │────▶│   Consumer  │
│  (Events)   │     │   Broker    │     │   (Transform)    │     │  (Sink)     │
└─────────────┘     └─────────────┘     └──────────────────┘     └─────────────┘
```

## Features

- **Producer**: Generates real-time events (user activity, transactions, IoT data)
- **Consumer**: Consumes and processes messages from topics
- **Stream Processor**: Real-time transformations, aggregations, and filtering
- **Docker Compose**: Full Kafka ecosystem (Kafka, Zookeeper, Schema Registry)

## Prerequisites

- Docker & Docker Compose
- Python 3.9+
- pip

## Quick Start

### 1. Start Kafka Infrastructure

```bash
docker-compose up -d
```

This starts:
- Zookeeper (port 2181)
- Kafka Broker (port 9092)
- Schema Registry (port 8081)
- Kafka UI (port 8080)

### 2. Install Dependencies

```bash
# Producer
cd producer && pip install -r requirements.txt

# Consumer
cd ../consumer && pip install -r requirements.txt

# Stream Processor
cd ../stream-processor && pip install -r requirements.txt
```

### 3. Run the Pipeline

```bash
# Terminal 1: Start Consumer
cd consumer && python consumer.py

# Terminal 2: Start Stream Processor
cd stream-processor && python stream_app.py

# Terminal 3: Start Producer
cd producer && python producer.py
```

## Project Structure

```
kafka-streaming-pipeline/
├── docker-compose.yml          # Kafka infrastructure
├── producer/
│   ├── producer.py             # Event producer
│   ├── event_generator.py      # Sample data generator
│   └── requirements.txt
├── consumer/
│   ├── consumer.py             # Event consumer
│   └── requirements.txt
├── stream-processor/
│   ├── stream_app.py           # Stream processing logic
│   └── requirements.txt
├── config/
│   └── kafka-config.properties
└── scripts/
    ├── create-topics.sh
    └── cleanup.sh
```

## Configuration

Edit `config/kafka-config.properties`:

```properties
bootstrap.servers=localhost:9092
schema.registry.url=http://localhost:8081
```

## Topics

| Topic | Description |
|-------|-------------|
| `raw-events` | Raw incoming events from producer |
| `processed-events` | Transformed events from stream processor |
| `alerts` | Filtered high-priority events |

## Monitoring

Access Kafka UI at: http://localhost:8080

## Use Cases

1. **Real-time Analytics**: Process clickstream data
2. **IoT Pipelines**: Sensor data ingestion and processing
3. **Fraud Detection**: Real-time transaction analysis
4. **Log Aggregation**: Centralized log processing

## Author

Ramchandra Chintala

## License

MIT License
