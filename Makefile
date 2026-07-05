.PHONY: all build test clean

KCAT_VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

all: build

build:
	CGO_ENABLED=1 go build -ldflags "-X main.version=$(KCAT_VERSION)" -o kcat .

test:
	CGO_ENABLED=1 go test -v ./...

clean:
	rm -f kcat
	go clean
