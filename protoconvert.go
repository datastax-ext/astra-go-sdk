package astra

import (
	"fmt"
	"math/big"
	"net"
	"reflect"
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
	default:
		res, err := collectionToProto(v)
		if err != nil {
			return nil, fmt.Errorf("failed to convert collection: %w", err)
		}
		if res != nil {
			return res, nil
		}
		// TODO: add decimal support
		// TODO: add UDT support
		// TODO: add varint support
	}
	return nil, fmt.Errorf("unsupported basic type: %T", value)
}

func collectionToProto(value interface{}) (*pb.Value, error) {
	var els []*pb.Value

	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Map:
		ks := v.MapKeys()
		l := len(ks) * 2
		els = make([]*pb.Value, l)
		for i := 0; i < l; i += 2 {
			k := ks[i/2]
			mk, err := valueToProto(k.Interface())
			if err != nil {
				return nil, fmt.Errorf("error resolving map key: %w", err)
			}
			els[i] = mk
			mv, err := valueToProto(v.MapIndex(k).Interface())
			if err != nil {
				return nil, fmt.Errorf("error resolving map value: %w", err)
			}
			els[i+1] = mv
		}
	case reflect.Slice, reflect.Array:
		l := v.Len()
		els = make([]*pb.Value, l)
		for i := 0; i < l; i++ {
			el, err := valueToProto(v.Index(i).Interface())
			if err != nil {
				return nil, fmt.Errorf("error resolving slice element: %w", err)
			}
			els[i] = el
		}
	default:
		return nil, nil
	}
	return &pb.Value{Inner: &pb.Value_Collection{Collection: &pb.Collection{Elements: els}}}, nil
}

func protosToValue(values []*pb.Value, spec []*pb.ColumnSpec) ([]any, error) {
	res := make([]any, len(values))
	var errs []error
	for i, v := range values {
		r, err := protoToValue(v, spec[i].GetType())
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

func protoToValue(value *pb.Value, spec *pb.TypeSpec) (any, error) {
	var r any
	var err error
	switch ts := spec.GetSpec().(type) {
	case *pb.TypeSpec_Basic_:
		r, err = basicProtoToValue(value)
	case *pb.TypeSpec_Map_:
		r, err = protosToMap(value.GetCollection().GetElements(), ts.Map)
	case *pb.TypeSpec_List_, *pb.TypeSpec_Set_:
		r, err = protoToSlice(value.GetCollection().GetElements(), spec)
	case *pb.TypeSpec_Tuple_:
		r, err = tupleProtoToSlice(value.GetCollection().GetElements(), ts.Tuple)
	default:
		err = fmt.Errorf("unsupported type: %s", ts)
	}
	return r, err
}

func basicProtoToValue(value *pb.Value) (any, error) {
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
		dec := decimal.NewFromBigInt(decodeBigInt(v.Decimal.Value), int32(-v.Decimal.Scale))
		return dec, nil
		// TODO: add UDT support
		// TODO: add varint support
	}
	return nil, fmt.Errorf("unsupported value type: %T, value: %+v", value.GetInner(), value.GetInner())
}

func protosToMap(values []*pb.Value, spec *pb.TypeSpec_Map) (any, error) {
	l := len(values)
	if l == 0 {
		return nil, nil
	}

	kts := spec.GetKey()
	vts := spec.GetValue()

	k0, err := protoToValue(values[0], kts)
	if err != nil {
		return nil, fmt.Errorf("failed to convert initial map key: %w", err)
	}
	v0, err := protoToValue(values[1], vts)
	if err != nil {
		return nil, fmt.Errorf("failed to convert initial map value: %w", err)
	}

	kt := reflect.TypeOf(k0)
	vt := reflect.TypeOf(v0)
	mt := reflect.MapOf(kt, vt)
	m := reflect.MakeMapWithSize(mt, l/2)
	m.SetMapIndex(reflect.ValueOf(k0), reflect.ValueOf(v0))

	for i := 2; i < l; i += 2 {
		k, err := protoToValue(values[i], kts)
		if err != nil {
			return nil, fmt.Errorf("failed to convert map key: %w", err)
		}
		v, err := protoToValue(values[i+1], vts)
		if err != nil {
			return nil, fmt.Errorf("failed to convert map value: %w", err)
		}
		m.SetMapIndex(reflect.ValueOf(k), reflect.ValueOf(v))
	}
	return m.Interface(), nil
}

func protoToSlice(values []*pb.Value, spec *pb.TypeSpec) (any, error) {
	l := len(values)
	if l == 0 {
		return nil, nil
	}

	var elSpec *pb.TypeSpec
	switch ts := spec.GetSpec().(type) {
	case *pb.TypeSpec_List_:
		elSpec = ts.List.GetElement()
	case *pb.TypeSpec_Set_:
		elSpec = ts.Set.GetElement()
	default:
		return nil, fmt.Errorf("unsupported slice type: %s", ts)
	}

	v0, err := protoToValue(values[0], elSpec)
	if err != nil {
		return nil, fmt.Errorf("failed to convert initial slice element: %w", err)
	}
	t := reflect.TypeOf(v0)
	s := reflect.MakeSlice(reflect.SliceOf(t), l, l)
	s.Index(0).Set(reflect.ValueOf(v0))

	for i := 1; i < l; i++ {
		v, err := protoToValue(values[i], elSpec)
		if err != nil {
			return nil, fmt.Errorf("failed to convert slice element: %w", err)
		}
		s.Index(i).Set(reflect.ValueOf(v))
	}
	return s.Interface(), nil
}

func tupleProtoToSlice(values []*pb.Value, spec *pb.TypeSpec_Tuple) (any, error) {
	l := len(values)
	if l == 0 {
		return nil, nil
	}

	specs := spec.GetElements()
	if len(specs) != l {
		return nil, fmt.Errorf("expected %d elements in tuple, got %d", len(specs), l)
	}

	s := make([]any, l)

	for i, v := range values {
		vv, err := protoToValue(v, specs[i])
		if err != nil {
			return nil, fmt.Errorf("failed to convert tuple element: %w", err)
		}
		s[i] = vv
	}

	return s, nil
}

func encodeDecimal(d *decimal.Decimal) (*pb.Value, error) {
	return &pb.Value{Inner: &pb.Value_Decimal{Decimal: &pb.Decimal{
		Scale: uint32(-d.Exponent()),
		Value: encodeBigInt(d.Coefficient())},
	}}, nil
}

func decodeBigInt(data []byte) *big.Int {
	l := len(data)
	i := big.NewInt(0).SetBytes(data)
	if l > 0 && data[0]&0x80 > 0 {
		i.Sub(i, big.NewInt(0).Lsh(big.NewInt(1), uint(l)*8))
	}
	return i
}

func encodeBigInt(i *big.Int) []byte {
	switch i.Sign() {
	case 0:
		return []byte{0}
	case 1:
		b := i.Bytes()
		if b[0]&0x80 > 0 {
			b = append([]byte{0}, b...)
		}
		return b
	case -1:
		l := uint(i.BitLen()/8+1) * 8
		ii := big.NewInt(0).Add(i, big.NewInt(0).Lsh(big.NewInt(1), l))
		b := ii.Bytes()
		if len(b) >= 2 && b[0] == 0xff && b[1]&0x80 != 0 {
			b = b[1:]
		}
		return b
	}
	return nil
}
