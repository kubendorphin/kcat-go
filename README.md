# kcat-go: Apache Kafka consumer and producer in Go

**kcat-go** is a Go rewrite of the [kcat](https://github.com/edenhill/kcat) project (formerly kafkacat).
It is a generic non-JVM producer and consumer for Apache Kafka, think of it as a netcat for Kafka.
Current version: https://github.com/edenhill/kcat:1.7.0

## Building

```bash
make build
```

Requires a C toolchain (CGO) for `confluent-kafka-go`.

## Usage

```bash
# Consumer mode
./kcat -b mybroker -t mytopic

# Producer mode
echo "hello world" | ./kcat -b mybroker -t mytopic

# High-level consumer group
./kcat -b mybroker -G mygroup topic1 topic2

# Metadata listing
./kcat -b mybroker -L

# JSON output
./kcat -b mybroker -t mytopic -J

# Format string output
./kcat -b mybroker -t mytopic -f 'Topic %t [%p]: %s\n'
```

See `./kcat -h` for all options.

## Requirements

- Go 1.23+
- C toolchain (for CGO / confluent-kafka-go)
- librdkafka (provided via confluent-kafka-go)
