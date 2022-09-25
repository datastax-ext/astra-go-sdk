package astra

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/stargate/stargate-grpc-go-client/stargate/pkg/auth"
	"github.com/stargate/stargate-grpc-go-client/stargate/pkg/client"
	pb "github.com/stargate/stargate-grpc-go-client/stargate/pkg/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	defaultDeadline = time.Second * 10
	defaultTimeout  = time.Second * 10
)

// Client is a client for Stargate.
type Client struct {
	astraURI string

	token string

	authServiceURL string
	authUsername   string
	authPassword   string

	deadline       time.Duration
	timeout        time.Duration
	tlsConfig      *tls.Config
	grpcConnParams *grpc.ConnectParams

	defaultQueryParams queryParams

	sgClient *client.StargateClient
}

// NewStaticTokenClient creates a new Client which uses the specified static
// auth token for requests.
func NewStaticTokenClient(astraURI, token string, opts ...ClientOption) (*Client, error) {
	c := &Client{
		astraURI: astraURI,
		token:    token,
		deadline: defaultDeadline,
		timeout:  defaultTimeout,
	}
	if err := c.init(opts); err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}
	return c, nil
}

// NewTableBasedTokenClient creates a new Client which uses the specified
// Stargate table auth API service URL, username, and password to obtain an auth
// token for requests.
func NewTableBasedTokenClient(astraURI, authServiceURI, username, password string, opts ...ClientOption) (*Client, error) {
	c := &Client{
		astraURI:       astraURI,
		authServiceURL: authServiceURI,
		authUsername:   username,
		authPassword:   password,
		deadline:       defaultDeadline,
		timeout:        defaultTimeout,
	}
	if err := c.init(opts); err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}
	return c, nil
}

func (c *Client) init(opts []ClientOption) error {
	for _, opt := range opts {
		opt(c)
	}

	dialOpts := []grpc.DialOption{
		grpc.WithBlock(),
	}

	useTLS := c.tlsConfig != nil
	if useTLS {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(c.tlsConfig)))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	var creds credentials.PerRPCCredentials
	if c.authServiceURL != "" {
		if useTLS {
			creds = auth.NewTableBasedTokenProvider(c.authServiceURL, c.authUsername, c.authPassword)
		} else {
			creds = auth.NewTableBasedTokenProviderUnsafe(c.authServiceURL, c.authUsername, c.authPassword)
		}
	} else {
		if useTLS {
			creds = auth.NewStaticTokenProvider(c.token)
		} else {
			creds = auth.NewStaticTokenProviderUnsafe(c.token)
		}
	}
	dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(creds))

	if c.grpcConnParams != nil {
		dialOpts = append(dialOpts, grpc.WithConnectParams(*c.grpcConnParams))
	}

	conn, err := grpc.Dial(c.astraURI, dialOpts...)
	if err != nil {
		return fmt.Errorf("failed to dial %q: %v", c.astraURI, err)
	}

	c.sgClient, err = client.NewStargateClientWithConn(conn, client.WithTimeout(c.deadline))
	if err != nil {
		return fmt.Errorf("failed to create stargate client: %v", err)
	}

	return nil
}

// Query creates a new Astra query.
func (c *Client) Query(cql string, values ...any) *Query {
	return &Query{
		client: c,
		cql:    cql,
		values: values,
	}
}

// Batch creates a new Astra batch query.
func (c *Client) Batch(queries ...*Query) *BatchQuery {
	return &BatchQuery{
		client:    c,
		batchType: BatchLogged,
		queries:   queries,
	}
}

func (c *Client) execQuery(query *Query) (Rows, error) {
	q, err := query.toQueryProto()
	if err != nil {
		return nil, err
	}

	dps := c.defaultQueryParams.params
	if dps != nil {
		if query.params == nil {
			q.Parameters = dps.toQueryParamsProto()
		} else {
			if query.params.keyspace != "" {
				q.Parameters.Keyspace = &wrapperspb.StringValue{Value: dps.keyspace}
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	qr, err := c.sgClient.ExecuteQueryWithContext(q, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}

	switch r := qr.Result.(type) {
	case *pb.Response_ResultSet:
		res, err := newRowsFromResultSet(r.ResultSet)
		if err != nil {
			return nil, fmt.Errorf("failed to create rows from result set: %v", err)
		}
		return res, nil
	case nil, *pb.Response_SchemaChange:
		return nil, nil
	}

	return nil, fmt.Errorf("unexpected response type: %T, %v", qr.Result, qr.Result)
}

func (c *Client) execBatch(bq *BatchQuery) error {
	b, err := bq.toProto()
	if err != nil {
		return fmt.Errorf("failed to create batch query proto: %w", err)
	}

	dps := c.defaultQueryParams.params
	if dps != nil {
		if bq.params == nil {
			b.Parameters = dps.toBatchParamsProto()
		} else {
			if bq.params.keyspace != "" {
				b.Parameters.Keyspace = &wrapperspb.StringValue{Value: dps.keyspace}
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	_, err = c.sgClient.ExecuteBatchWithContext(b, ctx)
	if err != nil {
		return fmt.Errorf("failed to execute batch query: %w", err)
	}

	return nil
}
