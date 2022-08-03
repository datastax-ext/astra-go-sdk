package astra

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	pb "github.com/stargate/stargate-grpc-go-client/stargate/pkg/proto"
)

func valuesToProto(values []any) ([]*pb.Value, error) {
	res := make([]*pb.Value, len(values))
	var errs []error
	for i, v := range values {
		r, err := valueToProto(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to convert value %q at index %d: %w\n", v, i, err))
		}
		res[i] = r
	}
	if len(errs) > 0 {
		return nil, fmt.Errorf("\n%v", errs)
	}
	return res, nil
}

func valueToProto(value any) (*pb.Value, error) {
	switch v := value.(type) {
	case nil:
		return &pb.Value{Inner: &pb.Value_Null_{Null: &pb.Value_Null{}}}, nil
	case int64:
		return &pb.Value{Inner: &pb.Value_Int{Int: v}}, nil
	case int32:
		return &pb.Value{Inner: &pb.Value_Int{Int: int64(v)}}, nil
	case int16:
		return &pb.Value{Inner: &pb.Value_Int{Int: int64(v)}}, nil
	case int8:
		return &pb.Value{Inner: &pb.Value_Int{Int: int64(v)}}, nil
	case int:
		return &pb.Value{Inner: &pb.Value_Int{Int: int64(v)}}, nil
	case uint64:
		return &pb.Value{Inner: &pb.Value_Int{Int: int64(v)}}, nil
	case uint32:
		return &pb.Value{Inner: &pb.Value_Int{Int: int64(v)}}, nil
	case uint16:
		return &pb.Value{Inner: &pb.Value_Int{Int: int64(v)}}, nil
	case uint8:
		return &pb.Value{Inner: &pb.Value_Int{Int: int64(v)}}, nil
	case uint:
		return &pb.Value{Inner: &pb.Value_Int{Int: int64(v)}}, nil
	case float32:
		return &pb.Value{Inner: &pb.Value_Float{Float: v}}, nil
	case float64:
		return &pb.Value{Inner: &pb.Value_Double{Double: v}}, nil
	case bool:
		return &pb.Value{Inner: &pb.Value_Boolean{Boolean: v}}, nil
	case string:
		return &pb.Value{Inner: &pb.Value_String_{String_: v}}, nil
	case []byte:
		return &pb.Value{Inner: &pb.Value_Bytes{Bytes: v}}, nil
	case net.IP:
		return &pb.Value{Inner: &pb.Value_Inet{Inet: &pb.Inet{Value: v[:]}}}, nil
	case *uuid.UUID:
		return &pb.Value{Inner: &pb.Value_Uuid{Uuid: &pb.Uuid{Value: v[:]}}}, nil
	case uuid.UUID:
		return &pb.Value{Inner: &pb.Value_Uuid{Uuid: &pb.Uuid{Value: v[:]}}}, nil
	case *time.Time:
		return &pb.Value{Inner: &pb.Value_Int{Int: v.UnixMilli()}}, nil
	case time.Time:
		return &pb.Value{Inner: &pb.Value_Int{Int: v.UnixMilli()}}, nil
	case *decimal.Decimal:
		return encodeDecimal(v)
	case decimal.Decimal:
		return encodeDecimal(&v)
		// TODO: add UDT support
		// TODO: add varint support
		// TODO: add map support
		// TODO: add list support
		// TODO: add set support
		// TODO: add tuple support
	}
	return nil, fmt.Errorf("unsupported type: %T", value)
}

func protosToValue(values []*pb.Value) ([]any, error) {
	res := make([]any, len(values))
	var errs []error
	for i, v := range values {
		r, err := protoToValue(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to convert value %q at index %d: %w", v, i, err))
		}
		res[i] = r
	}
	if len(errs) > 0 {
		return nil, fmt.Errorf("%v", errs)
	}
	return res, nil
}

func protoToValue(value *pb.Value) (any, error) {
	switch v := value.GetInner().(type) {
	case *pb.Value_Null_:
		return nil, nil
	case *pb.Value_Int:
		return v.Int, nil
	case *pb.Value_Float:
		return v.Float, nil
	case *pb.Value_Double:
		return v.Double, nil
	case *pb.Value_Boolean:
		return v.Boolean, nil
	case *pb.Value_String_:
		return v.String_, nil
	case *pb.Value_Bytes:
		return v.Bytes, nil
	case *pb.Value_Inet:
		return net.IP(v.Inet.GetValue()), nil
	case *pb.Value_Uuid:
		b := v.Uuid.Value
		if len(b) == 0 {
			return nil, nil
		}
		id, err := uuid.FromBytes(b)
		if err != nil {
			return nil, fmt.Errorf("failed to parse UUID: %v", err)
		}
		return id, nil
	case *pb.Value_Date:
		d := time.Unix(int64((time.Duration(v.Date)*24*time.Hour)/time.Second), 0).UTC()
		return &d, nil
	case *pb.Value_Time:
		return time.Duration(v.Time), nil
	case *pb.Value_Decimal:
		d := make([]byte, 4)
		binary.BigEndian.PutUint32(d, -v.Decimal.Scale)
		d = append(d, v.Decimal.Value...)

		dec := decimal.New(0, 0)
		if err := dec.UnmarshalBinary(d); err != nil {
			return nil, fmt.Errorf("failed to parse decimal: %w", err)
		}
		return dec, nil
		// TODO: add UDT support
		// TODO: add varint support
		// TODO: add map support
		// TODO: add list support
		// TODO: add set support
		// TODO: add tuple support
	}
	return nil, fmt.Errorf("unsupported value type: %T, value: %+v", value.GetInner(), value.GetInner())
}

func encodeDecimal(d *decimal.Decimal) (*pb.Value, error) {
	b, err := d.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal decimal binary: %w", err)
	}
	return &pb.Value{Inner: &pb.Value_Decimal{Decimal: &pb.Decimal{
		Scale: uint32(-d.Exponent()),
		Value: b[4:]},
	}}, nil
}
