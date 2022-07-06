package astra

import (
	"fmt"

	pb "github.com/stargate/stargate-grpc-go-client/stargate/pkg/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
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

type queryParams struct {
	params *params
}

func (p *queryParams) createIfEmpty() {
	if p.params == nil {
		p.params = &params{}
	}
}

// Keyspace sets the keyspace to use for the query.
func (p *queryParams) Keyspace(value string) *queryParams {
	p.createIfEmpty()
	p.params.keyspace = value
	return p
}

// Query is a configurable and executable Stargate query. Use Client.Query to
// create a Query.
type Query struct {
	client *Client
	cql    string
	values []any
	queryParams
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
	return &pb.BatchQuery{}, nil
}

// Exec executes the Query using the client that created it and returns the
// resultant rows.
func (q *Query) Exec() (Rows, error) {
	return q.client.execQuery(q)
}
