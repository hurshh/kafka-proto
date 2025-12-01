# Kafka-like Distributed System

A distributed message broker system inspired by Apache Kafka, implemented in Java.

## Features

- **Basic Operations**: Topic creation, produce, and fetch
- **Multiple Log Segments**: Per partition with size and time-based rolling
- **Protocol Layer**: Custom binary protocol for client communication
- **Compression**: Support for Gzip and Snappy compression
- **Cluster Mode**: Multiple brokers with replication
- **Consumer Offsets**: Offset management and tracking
- **SSL/ACLs**: Security features for encryption and access control
- **Configurability**: Extensive configuration options
- **Concurrency**: High-performance concurrent operations
- **Error Handling**: Comprehensive error handling and recovery
- **Transactions**: Transactional producer and consumer support

## Architecture

- **Broker**: Manages topics, partitions, local storage, replication, and client requests
- **Controller**: Manages cluster metadata, partition assignments, and broker states
- **Producer**: Sends messages to topic partitions via broker's TCP interface
- **Consumer**: Fetches messages from brokers based on offset
- **Replication Module**: Handles synchronization between leader and follower brokers
- **Consensus Module**: Implements leader election and term management using Raft principles

## Building

```bash
./gradlew build
```

## Running

### Start a Broker

```bash
./gradlew run
```

Or with custom configuration:

```bash
BROKER_PORT=9092 BROKER_DATA_DIR=./data/broker ./gradlew run
```

## Configuration

Configuration is loaded from `src/main/resources/application.properties` and can be overridden with environment variables.

Key configuration options:
- `broker.id`: Unique broker identifier
- `broker.port`: Port for client connections
- `broker.data.dir`: Directory for log storage
- `log.segment.size.bytes`: Maximum size of a log segment
- `replication.factor`: Number of replicas per partition

## Development

This project uses:
- Java 11+
- Gradle for build management
- Netty for networking
- SLF4J + Logback for logging

## License

MIT

