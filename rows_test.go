package astra

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	pb "github.com/stargate/stargate-grpc-go-client/stargate/pkg/proto"
)

func TestNewRowsFromResultSet(t *testing.T) {
	in := &pb.ResultSet{
		Columns: []*pb.ColumnSpec{
			{
				Type: &pb.TypeSpec{Spec: &pb.TypeSpec_Basic_{Basic: pb.TypeSpec_TEXT}},
				Name: "name",
			},
			{
				Type: &pb.TypeSpec{Spec: &pb.TypeSpec_Basic_{Basic: pb.TypeSpec_INT}},
				Name: "age",
			},
		},
		Rows: []*pb.Row{{
			Values: []*pb.Value{
				{Inner: &pb.Value_String_{String_: "Alice"}}, // text_col
				{Inner: &pb.Value_Int{Int: 30}},              // int_col
			}},
		},
	}

	got, err := newRowsFromResultSet(in)
	if err != nil {
		t.Errorf("newRowsFromResultSet(%q) failed to convert result set to rows: %v", in, err)
	}

	want := Rows{
		{
			spec: &colSpec{
				names: []string{"name", "age"},
				idxs:  map[string]int{"name": 0, "age": 1},
			},
			values: []interface{}{"Alice", int64(30)},
		},
	}

	if diff := cmp.Diff(want, got, cmp.AllowUnexported(Row{}, colSpec{})); diff != "" {
		t.Fatalf("newRowsFromResultSet(%q) unexpected difference (-want +got):\n%s", in, diff)
	}
}
