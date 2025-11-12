# 🎡 Kafka Streaming Pipeline

[![Kafka: 3.5](https://img.shields.io/badge/Kafka-3.5-blue.svg)](https://kafka.apache.org/)
[![Confluent: 7.5](https://img.shields.io/badge/Confluent-7.5-orange.svg)](https://www.confluent.io/)

> High-throughput real-time telemetry processing engine using Kafka Streams and Schema Registry.

## 🏗️ Architecture

```mermaid
graph LR
    P[Python Producers] -->|Avro| K(Kafka Cluster)
    K -->|Stream| S[Kafka Streams Processor]
    S -->|Enriched| T(Elasticsearch/InfluxDB)
    SR[Schema Registry] -.- K
```

This pipeline demonstrates production-grade stream processing with Avro serialization and schema evolution.

## 🚀 Development Stack

Use the `Makefile` to orchestra the local Confluent environment:

```bash
make up
make topic
make ps
```

Access the **Confluent Control Center** at `http://localhost:9021` to monitor streams visually.

## 🛠️ Components

- **Consumer/**: Intelligent consumer group logic with rebalance listeners.
- **Producer/**: Async producers with retry policies and dead-letter queue (DLQ) support.
- **Stream-Processor/**: Complex transformations including windowed aggregations.

---
maintained by **Ramchandra Chintala**
