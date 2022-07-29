package astra

import (
	"net"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
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
	vals := []interface{}{
		id,                       // id
		"alpha",                  // asciivalue
		"bravo",                  // textvalue
		"charlie",                // varcharvalue
		[]byte("foo"),            // blobvalue
		true,                     // booleanvalue
		2.2,                      // doublevalue
		float32(3.3),             // floatvalue
		net.ParseIP("127.0.0.1"), // inetvalue
		1,                        // bigintvalue
		2,                        // intvalue
		3,                        // smallintvalue
		5,                        // tinyintvalue
		&timeUUID,                // timeuuidvalue
	}

	_, err = c.Query(`INSERT INTO test.all_types (
		id,
		ascii_col,
		text_col,
		varchar_col,
		blob_col,
		boolean_col,
		double_col,
		float_col,
		inet_col,
		bigint_col,
		int_col,
		smallint_col,
		tinyint_col,
		timeuuid_col
	) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
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
			double_col,
			float_col,
			inet_col,
			bigint_col,
			int_col,
			smallint_col,
			tinyint_col,
			timeuuid_col
		FROM test.all_types WHERE id = ?
	`, id).Exec()
	if err != nil {
		t.Fatalf("failed to select values: %v", err)
	}

	wantVals := []interface{}{
		id,                       // id
		"alpha",                  // asciivalue
		"bravo",                  // textvalue
		"charlie",                // varcharvalue
		[]byte("foo"),            // blobvalue
		true,                     // booleanvalue
		2.2,                      // doublevalue
		float32(3.3),             // floatvalue
		net.ParseIP("127.0.0.1"), // inetvalue
		int64(1),                 // bigintvalue
		int64(2),                 // intvalue
		int64(3),                 // smallintvalue
		int64(5),                 // tinyintvalue
		timeUUID,                 // timeuuidvalue
	}

	if diff := cmp.Diff(wantVals, got[0].Values()); diff != "" {
		t.Fatalf("got[0].Values() unexpected difference (-want +got):\n%s", diff)
	}
}
