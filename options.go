package astra

import (
	"crypto/tls"
	"time"

	"google.golang.org/grpc"
)

// ClientOption is an option for a Client.
type ClientOption func(*Client)

// WithDeadline sets the deadline for the initial connection.
func WithDeadline(deadline time.Duration) ClientOption {
	return func(c *Client) {
		c.deadline = deadline
	}
}

// WithTimeout sets the timeout for queries.
func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.timeout = timeout
	}
}

// WithDefaultKeyspace specifies the default keyspace for client queries.
func WithDefaultKeyspace(keyspace string) ClientOption {
	return func(c *Client) {
		c.defaultQueryParams.keyspace(keyspace)
	}
}

// WithGRPCConnParams specifies other connection parameters to use for the gRPC
// connection.
func WithGRPCConnParams(params *grpc.ConnectParams) ClientOption {
	return func(c *Client) {
		c.grpcConnParams = params
	}
}

// WithTLSConfig specifies the TLS configuration to use for the gRPC connection.
func WithTLSConfig(config *tls.Config) ClientOption {
	return func(c *Client) {
		c.tlsConfig = config
	}
}

// WithInsecure specifies whether to use an insecure connection. Intended
// localhost testing only.
func WithInsecure(insecure bool) ClientOption {
	return func(c *Client) {
		c.insecure = insecure
	}
}

// StaticTokenConnectConfig describes a connection method to use in a call to
// NewStaticTokenClient.
type StaticTokenConnectConfig func(*Client)

// WithAstraURI specifies the Astra URI to use for the gRPC connection.
func WithAstraURI(uri string) StaticTokenConnectConfig {
	return func(c *Client) {
		c.astraURI = uri
	}
}

// WithSecureConnectBundle specifies the secure connect bundle to use for the
// gRPC connection.
func WithSecureConnectBundle(path string) StaticTokenConnectConfig {
	return func(c *Client) {
		c.scbPath = path
	}
}
