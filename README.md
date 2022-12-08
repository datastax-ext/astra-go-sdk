# Datastax Astra Go SDK

[![License Apache2](https://img.shields.io/hexpm/l/plug.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![Go Report Card](https://goreportcard.com/badge/github.com/datastax-ext/astra-go-sdk)](https://goreportcard.com/report/github.com/datastax-ext/astra-go-sdk)
[![Go Reference](https://pkg.go.dev/badge/github.com/datastax-ext/astra-go-sdk.svg)](https://pkg.go.dev/github.com/datastax-ext/astra-go-sdk)

Software Development Kit wrapping Astra APIs and drivers.

## Overview

TODO

## Development 

### Testing

To run fast unit tests:
```shell
go test ./... -run ^Test -test.short
```

To run all unit tests, integration tests, and examples:

```shell
go test ./...
```

These tests rely on [test containers](https://golang.testcontainers.org/), and
require a running Docker daemon to work properly.

To run all tests online:

```shell
go test ./... \
  -test_scb_path=<path/to/secure-connect-bundle.zip> \
  -test_token=<AstraCS:...>
```
