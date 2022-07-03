package astra

import (
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	pb "github.com/stargate/stargate-grpc-go-client/stargate/pkg/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestValuesToProto(t *testing.T) {
	id := uuid.MustParse("12345678-1234-5678-1234-567812345678")
	ip := net.IPv4(1, 2, 3, 4).To4()
	dec, err := decimal.NewFromString("1.23456789")
	if err != nil {
		t.Fatalf("unexpected error while creating decimal: %v", err)
	}

	in := []interface{}{
		nil,
		int64(-64),
		int32(-32),
		int16(-16),
		int8(-8),
		-1,
		uint64(64), uint32(32),
		uint16(16),
		uint8(8),
		uint(1),
		big.NewInt(16),
		dec,
		float32(1.23456789),
		1.23456789,
		true,
		"foo",
		[]byte("bar"),
		ip,
		&id,
		id,
		[]int64{1, 2, 3},
		[][]int64{{1, 2}, {3, 4}},
		map[string]int{"one": 1},
		map[string][]int{"one": {1, 2}},
	}

	got, err := valuesToProto(in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []*pb.Value{
		{Inner: &pb.Value_Null_{Null: &pb.Value_Null{}}},
		{Inner: &pb.Value_Int{Int: -64}},
		{Inner: &pb.Value_Int{Int: -32}},
		{Inner: &pb.Value_Int{Int: -16}},
		{Inner: &pb.Value_Int{Int: -8}},
		{Inner: &pb.Value_Int{Int: -1}},
		{Inner: &pb.Value_Int{Int: 64}},
		{Inner: &pb.Value_Int{Int: 32}},
		{Inner: &pb.Value_Int{Int: 16}},
		{Inner: &pb.Value_Int{Int: 8}},
		{Inner: &pb.Value_Int{Int: 1}},
		{Inner: &pb.Value_Varint{Varint: &pb.Varint{Value: []byte{0x10}}}},
		{Inner: &pb.Value_Decimal{Decimal: &pb.Decimal{
			Scale: 8, Value: []byte{0x07, 0x5b, 0xcd, 0x15},
		}}},
		{Inner: &pb.Value_Float{Float: 1.23456789}},
		{Inner: &pb.Value_Double{Double: 1.23456789}},
		{Inner: &pb.Value_Boolean{Boolean: true}},
		{Inner: &pb.Value_String_{String_: "foo"}},
		{Inner: &pb.Value_Bytes{Bytes: []byte("bar")}},
		{Inner: &pb.Value_Inet{Inet: &pb.Inet{Value: ip[:]}}},
		{Inner: &pb.Value_Uuid{Uuid: &pb.Uuid{Value: id[:]}}},
		{Inner: &pb.Value_Uuid{Uuid: &pb.Uuid{Value: id[:]}}},
		{Inner: &pb.Value_Collection{Collection: &pb.Collection{Elements: []*pb.Value{
			{Inner: &pb.Value_Int{Int: 1}},
			{Inner: &pb.Value_Int{Int: 2}},
			{Inner: &pb.Value_Int{Int: 3}},
		}}}},
		{Inner: &pb.Value_Collection{Collection: &pb.Collection{Elements: []*pb.Value{
			{Inner: &pb.Value_Collection{Collection: &pb.Collection{Elements: []*pb.Value{
				{Inner: &pb.Value_Int{Int: 1}},
				{Inner: &pb.Value_Int{Int: 2}},
			}}}},
			{Inner: &pb.Value_Collection{Collection: &pb.Collection{Elements: []*pb.Value{
				{Inner: &pb.Value_Int{Int: 3}},
				{Inner: &pb.Value_Int{Int: 4}},
			}}}},
		}}}},
		{Inner: &pb.Value_Collection{Collection: &pb.Collection{Elements: []*pb.Value{
			{Inner: &pb.Value_String_{String_: "one"}},
			{Inner: &pb.Value_Int{Int: 1}},
		}}}},
		{Inner: &pb.Value_Collection{Collection: &pb.Collection{Elements: []*pb.Value{
			{Inner: &pb.Value_String_{String_: "one"}},
			{Inner: &pb.Value_Collection{Collection: &pb.Collection{Elements: []*pb.Value{
				{Inner: &pb.Value_Int{Int: 1}},
				{Inner: &pb.Value_Int{Int: 2}},
			}}}},
		}}}},
	}

	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Fatalf("valuesToProto(%v) unexpected difference (-want +got):\n%s", in, diff)
	}
}

func TestProtosToValue(t *testing.T) {
	id := uuid.MustParse("12345678-1234-5678-1234-567812345678")
	ip := net.IPv4(1, 2, 3, 4).To4()
	dt := time.Date(2019, 4, 24, 12, 23, 34, 123456789, time.UTC) // 2019-04-24 12:23:34.987654321 UTC

	inSpec := []*pb.ColumnSpec{
		{Name: "null_col", Type: &pb.TypeSpec{Spec: &pb.TypeSpec_Basic_{Basic: pb.TypeSpec_INT}}},
		{Name: "int_col", Type: &pb.TypeSpec{Spec: &pb.TypeSpec_Basic_{Basic: pb.TypeSpec_INT}}},
		{Name: "float_col", Type: &pb.TypeSpec{Spec: &pb.TypeSpec_Basic_{Basic: pb.TypeSpec_FLOAT}}},
		{Name: "double_col", Type: &pb.TypeSpec{Spec: &pb.TypeSpec_Basic_{Basic: pb.TypeSpec_DOUBLE}}},
		{Name: "double_col", Type: &pb.TypeSpec{Spec: &pb.TypeSpec_Basic_{Basic: pb.TypeSpec_DECIMAL}}},
		{Name: "bool_col", Type: &pb.TypeSpec{Spec: &pb.TypeSpec_Basic_{Basic: pb.TypeSpec_BOOLEAN}}},
		{Name: "text_col", Type: &pb.TypeSpec{Spec: &pb.TypeSpec_Basic_{Basic: pb.TypeSpec_TEXT}}},
		{Name: "blob_col", Type: &pb.TypeSpec{Spec: &pb.TypeSpec_Basic_{Basic: pb.TypeSpec_BLOB}}},
		{Name: "inet_col", Type: &pb.TypeSpec{Spec: &pb.TypeSpec_Basic_{Basic: pb.TypeSpec_INET}}},
		{Name: "uuid_col", Type: &pb.TypeSpec{Spec: &pb.TypeSpec_Basic_{Basic: pb.TypeSpec_UUID}}},
		{Name: "date_col", Type: &pb.TypeSpec{Spec: &pb.TypeSpec_Basic_{Basic: pb.TypeSpec_DATE}}},
		{Name: "time_col", Type: &pb.TypeSpec{Spec: &pb.TypeSpec_Basic_{Basic: pb.TypeSpec_TIME}}},
		{Name: "list_col", Type: &pb.TypeSpec{Spec: &pb.TypeSpec_List_{
			List: &pb.TypeSpec_List{Element: &pb.TypeSpec{
				Spec: &pb.TypeSpec_Basic_{Basic: pb.TypeSpec_INT},
			}},
		}}},
		{Name: "list_list_col", Type: &pb.TypeSpec{Spec: &pb.TypeSpec_List_{
			List: &pb.TypeSpec_List{Element: &pb.TypeSpec{Spec: &pb.TypeSpec_List_{
				List: &pb.TypeSpec_List{Element: &pb.TypeSpec{
					Spec: &pb.TypeSpec_Basic_{Basic: pb.TypeSpec_INT},
				}},
			}}},
		}}},
		{Name: "map_list_col", Type: &pb.TypeSpec{Spec: &pb.TypeSpec_Map_{
			Map: &pb.TypeSpec_Map{
				Key: &pb.TypeSpec{Spec: &pb.TypeSpec_Basic_{Basic: pb.TypeSpec_TEXT}},
				Value: &pb.TypeSpec{Spec: &pb.TypeSpec_List_{
					List: &pb.TypeSpec_List{Element: &pb.TypeSpec{
						Spec: &pb.TypeSpec_Basic_{Basic: pb.TypeSpec_INT},
					}},
				}},
			},
		}}},
		{Name: "map_col", Type: &pb.TypeSpec{Spec: &pb.TypeSpec_Map_{
			Map: &pb.TypeSpec_Map{
				Key:   &pb.TypeSpec{Spec: &pb.TypeSpec_Basic_{Basic: pb.TypeSpec_TEXT}},
				Value: &pb.TypeSpec{Spec: &pb.TypeSpec_Basic_{Basic: pb.TypeSpec_INT}},
			},
		}}},
	}

	in := []*pb.Value{
		{Inner: &pb.Value_Null_{Null: &pb.Value_Null{}}},
		{Inner: &pb.Value_Int{Int: 1}},
		{Inner: &pb.Value_Varint{Varint: &pb.Varint{Value: []byte{0x10}}}},
		{Inner: &pb.Value_Float{Float: 1.23456789}},
		{Inner: &pb.Value_Double{Double: 1.23456789}},
		{Inner: &pb.Value_Decimal{Decimal: &pb.Decimal{Value: []byte{0x07, 0x5b, 0xcd, 0x15}, Scale: 8}}},
		{Inner: &pb.Value_Boolean{Boolean: true}},
		{Inner: &pb.Value_String_{String_: "foo"}},
		{Inner: &pb.Value_Bytes{Bytes: []byte("bar")}},
		{Inner: &pb.Value_Inet{Inet: &pb.Inet{Value: ip[:]}}},
		{Inner: &pb.Value_Uuid{Uuid: &pb.Uuid{Value: id[:]}}},
		{Inner: &pb.Value_Date{Date: uint32(dt.Unix() / 24 / 60 / 60)}},
		{Inner: &pb.Value_Time{Time: uint64(time.Duration(dt.UnixNano()) % (24 * time.Hour))}},
		{Inner: &pb.Value_Collection{Collection: &pb.Collection{
			Elements: []*pb.Value{
				{Inner: &pb.Value_Int{Int: 1}},
				{Inner: &pb.Value_Int{Int: 2}},
				{Inner: &pb.Value_Int{Int: 3}},
				{Inner: &pb.Value_Int{Int: 4}},
			},
		}}},
		{Inner: &pb.Value_Collection{Collection: &pb.Collection{
			Elements: []*pb.Value{
				{Inner: &pb.Value_Collection{Collection: &pb.Collection{
					Elements: []*pb.Value{
						{Inner: &pb.Value_Int{Int: 1}},
						{Inner: &pb.Value_Int{Int: 2}},
					},
				}}},
				{Inner: &pb.Value_Collection{Collection: &pb.Collection{
					Elements: []*pb.Value{
						{Inner: &pb.Value_Int{Int: 3}},
						{Inner: &pb.Value_Int{Int: 4}},
					},
				}}},
			},
		}}},
		{Inner: &pb.Value_Collection{Collection: &pb.Collection{
			Elements: []*pb.Value{
				{Inner: &pb.Value_String_{String_: "one"}},
				{Inner: &pb.Value_Collection{Collection: &pb.Collection{
					Elements: []*pb.Value{
						{Inner: &pb.Value_Int{Int: 1}},
						{Inner: &pb.Value_Int{Int: 2}},
					},
				}}},
			},
		}}},
		{Inner: &pb.Value_Collection{Collection: &pb.Collection{Elements: []*pb.Value{
			{Inner: &pb.Value_String_{String_: "one"}},
			{Inner: &pb.Value_Int{Int: 1}},
		}}}},
	}

	got, err := protosToValue(in, inSpec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wd := time.Date(2019, 4, 24, 0, 0, 0, 0, time.UTC)
	wt := 12*time.Hour + 23*time.Minute + 34*time.Second + 123456789
	wdec, err := decimal.NewFromString("1.23456789")
	if err != nil {
		t.Fatalf("failed to create decimal: %v", wdec)
	}
	want := []interface{}{
		nil,
		int64(1),
		big.NewInt(16),
		float32(1.23456789),
		1.23456789,
		wdec,
		true,
		"foo",
		[]byte("bar"),
		net.IPv4(1, 2, 3, 4).To4(),
		id,
		&wd,
		wt,
		[]int64{1, 2, 3, 4},
		[][]int64{{1, 2}, {3, 4}},
		map[string][]int64{"one": {1, 2}},
		map[string]int64{"one": 1},
	}

	if diff := cmp.Diff(want, got, cmp.AllowUnexported(big.Int{})); diff != "" {
		t.Fatalf("protosToValue(%v) unexpected difference (-want +got):\n%s", in, diff)
	}
}
