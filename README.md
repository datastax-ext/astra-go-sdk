# Datastax Astra Go SDK

[![License Apache2](https://img.shields.io/hexpm/l/plug.svg)](http://www.apache.org/licenses/LICENSE-2.0)

Software Development Kit wrapping Astra APIs and drivers.

## Overview

TODO

## Development 

### Testing

To run fast unit tests, run :
```go
go test ./... -run ^Test -test.short
```

To run all unit tests, integration tests, and examples, run:
```go
go test ./...
```

These tests rely on [test containers](https://golang.testcontainers.org/), and require a running Docker daemon to work properly.
