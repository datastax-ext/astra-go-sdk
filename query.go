package astra

import (
	"fmt"

	pb "github.com/stargate/stargate-grpc-go-client/stargate/pkg/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type BatchType uint8

// Batch types for BatchQuery.
// See https://docs.datastax.com/en/cql-oss/3.x/cql/cql_reference/cqlBatch.html
const (
	BatchLogged BatchType = iota
	BatchUnlogged
	BatchCounter
)

type params struct {
	keyspace string
}

func (p *params) toQueryParamsProto() *pb.QueryParameters {
	if p == nil {
		return nil
	}
	res := &pb.QueryParameters{}
	if p.keyspace != "" {
		res.Keyspace = &wrapperspb.StringValue{Value: p.keyspace}
	}
	return res
}

func (p *params) toBatchParamsProto() *pb.BatchParameters {
	if p == nil {
		return nil
	}
	res := &pb.BatchParameters{}
	if p.keyspace != "" {
		res.Keyspace = &wrapperspb.StringValue{Value: p.keyspace}
	}
	return res
}

type queryParams struct {
	params *params
}

func (p *queryParams) createIfEmpty() {
	if p.params == nil {
		p.params = &params{}
	}
}

func (p *queryParams) keyspace(value string) {
	p.createIfEmpty()
	p.params.keyspace = value
}

// Query is a configurable and executable Stargate query. Use Client.Query to
// create a Query.
type Query struct {
	client *Client
	cql    string
	values []any
	queryParams
}

// Keyspace sets the keyspace to use for the query.
func (q *Query) Keyspace(value string) *Query {
	q.queryParams.keyspace(value)
	return q
}

// Exec executes the Query using the client that created it and returns the
// resultant rows.
func (q *Query) Exec() (Rows, error) {
	return q.client.execQuery(q)
}

func (q *Query) toQueryProto() (*pb.Query, error) {
	vs, err := valuesToProto(q.values)
	if err != nil {
		return nil, fmt.Errorf("failed to convert values to proto: %v", err)
	}
	return &pb.Query{
		Cql:        q.cql,
		Values:     &pb.Values{Values: vs},
		Parameters: q.queryParams.params.toQueryParamsProto(),
	}, nil
}

func (q *Query) toBatchQueryProto() (*pb.BatchQuery, error) {
	vs, err := valuesToProto(q.values)
	if err != nil {
		return nil, fmt.Errorf("failed to convert values to proto: %v", err)
	}
	return &pb.BatchQuery{
		Cql:    q.cql,
		Values: &pb.Values{Values: vs},
	}, nil
}

// BatchQuery is a configurable and executable Stargate batch query. Use
// Client.Batch to create a BatchQuery.
type BatchQuery struct {
	client    *Client
	batchType BatchType
	queries   []*Query
	queryParams
}

// BatchType sets the batch type to use for the batch query.
func (b *BatchQuery) BatchType(batchType BatchType) *BatchQuery {
	b.batchType = batchType
	return b
}

// Keyspace sets the keyspace to use for the batch query.
func (b *BatchQuery) Keyspace(value string) *BatchQuery {
	b.queryParams.keyspace(value)
	return b
}

func (b *BatchQuery) toProto() (*pb.Batch, error) {
	qs := make([]*pb.BatchQuery, len(b.queries))
	for i, q := range b.queries {
		bq, err := q.toBatchQueryProto()
		if err != nil {
			return nil, err
		}
		qs[i] = bq
	}

	var t pb.Batch_Type
	switch b.batchType {
	case BatchLogged:
		t = pb.Batch_LOGGED
	case BatchUnlogged:
		t = pb.Batch_UNLOGGED
	case BatchCounter:
		t = pb.Batch_COUNTER
	default:
		return nil, fmt.Errorf("unknown batch type: %v", b.batchType)
	}

	res := &pb.Batch{
		Queries:    qs,
		Parameters: b.params.toBatchParamsProto(),
	}
	if t != pb.Batch_LOGGED {
		res.Type = t
	}

	return res, nil
}

// Exec executes the BatchQuery using the client that created it.
func (b *BatchQuery) Exec() error {
	return b.client.execBatch(b)
}
