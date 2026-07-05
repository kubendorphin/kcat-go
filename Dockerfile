FROM golang:1.23-bookworm

# Install C toolchain (required by confluent-kafka-go)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential pkg-config librdkafka-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=1 go build -o /usr/local/bin/kcat .

ENTRYPOINT ["kcat"]
