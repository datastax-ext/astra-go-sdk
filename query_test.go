package astra

import (
	"net"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

func TestClient_Query_Exec_allTypes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	c, err := stc.CreateClientWithStaticToken()
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	id := uuid.MustParse("f066f76d-5e96-4b52-8d8a-0f51387df76b")
	timeUUID := uuid.MustParse("30821634-13ad-11eb-adc1-0242ac120002")
	dec, err := decimal.NewFromString("-1.23456789")
	if err != nil {
		t.Fatalf("unexpected error while creating decimal: %v", err)
	}

	vals := []interface{}{
		id,                                     // id
		"alpha",                                // ascii_col
		"bravo",                                // text_col
		"charlie",                              // varchar_col
		[]byte("foo"),                          // blob_col
		true,                                   // boolean_col
		dec,                      // decimalvalue
		2.2,                                    // double_col
		float32(3.3),                           // float_col
		net.ParseIP("127.0.0.1"),               // inet_col
		1,                                      // bigint_col
		2,                                      // int_col
		3,                                      // smallint_col
		5,                                      // tinyint_col
		&timeUUID,                              // timeuuid_col
		map[int]string{1: "a", 2: "b", 3: "c"}, // map_col
		map[string][]int{"a": {1, 2}, "b": {3, 4}}, // map_list_col
		[]string{"a", "b", "c"},                    // list_col
		[][]string{{"a", "b"}, {"c", "d"}},         // list_list_col
		[]string{"a", "b", "c"},                    // set_col
		[]interface{}{3, "bar", float32(2.1)},      // tuple_col
	}

	_, err = c.Query(`INSERT INTO test.all_types (
		id,
		ascii_col,
		text_col,
		varchar_col,
		blob_col,
		boolean_col,
		decimal_col,
		double_col,
		float_col,
		inet_col,
		bigint_col,
		int_col,
		smallint_col,
		tinyint_col,
		timeuuid_col,
		map_col,
		map_list_col,
		list_col,
		list_list_col,
		set_col,
		tuple_col
	) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		vals...,
	).Exec()
	if err != nil {
		t.Fatalf("failed to insert values into table: %v", err)
	}

	got, err := c.Query(`
		SELECT
			id,
			ascii_col,
			text_col,
			varchar_col,
			blob_col,
			boolean_col,
			decimal_col,
			double_col,
			float_col,
			inet_col,
			bigint_col,
			int_col,
			smallint_col,
			tinyint_col,
			timeuuid_col,
			map_col,
			map_list_col,
			list_col,
			list_list_col,
			set_col,
			tuple_col
		FROM test.all_types WHERE id = ?
	`, id).Exec()
	if err != nil {
		t.Fatalf("failed to select values: %v", err)
	}

	wantVals := []interface{}{
		id,                                       // id
		"alpha",                                  // ascii_col
		"bravo",                                  // text_col
		"charlie",                                // varchar_col
		[]byte("foo"),                            // blob_col
		true,                                     // boolean_col
		dec,                      // decimalvalue
		2.2,                                      // double_col
		float32(3.3),                             // float_col
		net.ParseIP("127.0.0.1"),                 // inet_col
		int64(1),                                 // bigint_col
		int64(2),                                 // int_col
		int64(3),                                 // smallint_col
		int64(5),                                 // tinyint_col
		timeUUID,                                 // timeuuid_col
		map[int64]string{1: "a", 2: "b", 3: "c"}, // map_col
		map[string][]int64{"a": {1, 2}, "b": {3, 4}}, // map_list_col
		[]string{"a", "b", "c"},                      // list_col
		[][]string{{"a", "b"}, {"c", "d"}},           // list_list_col
		[]string{"a", "b", "c"},                      // set_col
		[]interface{}{int64(3), "bar", float32(2.1)}, // tuple_col
	}

	if diff := cmp.Diff(wantVals, got[0].Values()); diff != "" {
		t.Fatalf("got[0].Values() unexpected difference (-want +got):\n%s", diff)
	}
}
