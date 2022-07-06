package astra

import (
	"net"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	pb "github.com/stargate/stargate-grpc-go-client/stargate/pkg/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestValuesToProto(t *testing.T) {
	id := uuid.MustParse("12345678-1234-5678-1234-567812345678")
	ip := net.IPv4(1, 2, 3, 4).To4()

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
		float32(1.23456789),
		1.23456789,
		true,
		"foo",
		[]byte("bar"),
		ip,
		&id,
		id,
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
		{Inner: &pb.Value_Float{Float: 1.23456789}},
		{Inner: &pb.Value_Double{Double: 1.23456789}},
		{Inner: &pb.Value_Boolean{Boolean: true}},
		{Inner: &pb.Value_String_{String_: "foo"}},
		{Inner: &pb.Value_Bytes{Bytes: []byte("bar")}},
		{Inner: &pb.Value_Inet{Inet: &pb.Inet{Value: ip[:]}}},
		{Inner: &pb.Value_Uuid{Uuid: &pb.Uuid{Value: id[:]}}},
		{Inner: &pb.Value_Uuid{Uuid: &pb.Uuid{Value: id[:]}}},
	}

	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Fatalf("valuesToProto(%v) unexpected difference (-want +got):\n%s", in, diff)
	}
}

func TestProtosToValue(t *testing.T) {
	id := uuid.MustParse("12345678-1234-5678-1234-567812345678")
	ip := net.IPv4(1, 2, 3, 4).To4()
	dt := time.Date(2019, 4, 24, 12, 23, 34, 123456789, time.UTC) // 2019-04-24 12:23:34.987654321 UTC

	in := []*pb.Value{
		{Inner: &pb.Value_Null_{Null: &pb.Value_Null{}}},
		{Inner: &pb.Value_Int{Int: 1}},
		{Inner: &pb.Value_Float{Float: 1.23456789}},
		{Inner: &pb.Value_Double{Double: 1.23456789}},
		{Inner: &pb.Value_Boolean{Boolean: true}},
		{Inner: &pb.Value_String_{String_: "foo"}},
		{Inner: &pb.Value_Bytes{Bytes: []byte("bar")}},
		{Inner: &pb.Value_Inet{Inet: &pb.Inet{Value: ip[:]}}},
		{Inner: &pb.Value_Uuid{Uuid: &pb.Uuid{Value: id[:]}}},
		{Inner: &pb.Value_Date{Date: uint32(dt.Unix() / 24 / 60 / 60)}},
		{Inner: &pb.Value_Time{Time: uint64(time.Duration(dt.UnixNano()) % (24 * time.Hour))}},
	}

	got, err := protosToValue(in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wd := time.Date(2019, 4, 24, 0, 0, 0, 0, time.UTC)
	wt := 12*time.Hour + 23*time.Minute + 34*time.Second + 123456789
	want := []interface{}{
		nil,
		int64(1),
		float32(1.23456789),
		1.23456789,
		true,
		"foo",
		[]byte("bar"),
		net.IPv4(1, 2, 3, 4).To4(),
		&id,
		&wd,
		&wt,
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("protosToValue(%v) unexpected difference (-want +got):\n%s", in, diff)
	}
}
