package astra

import (
	"flag"
	"log"
	"os"
	"testing"
)

var (
	stc             *TestContainer
	endpoint, token string
)

func TestMain(m *testing.M) {
	flag.Parse()

	if testing.Short() {
		log.Println("skipping integration tests")
		os.Exit(m.Run())
	}

	var err error
	stc, err = NewStargateTestContainer()
	if err != nil {
		log.Fatalf("failed to start Stargate container: %v", err)
	}

	endpoint = stc.grpcEndpoint
	token, err = stc.getAuthToken()
	if err != nil {
		log.Fatalf("failed to get auth token: %v", err)
	}

	c, err := stc.CreateClientWithStaticToken()
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}

	_, err = c.Query(`CREATE KEYSPACE IF NOT EXISTS example WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}`).Exec()
	if err != nil {
		log.Fatalf("failed to create example keyspace: %v", err)
	}

	_, err = c.Query(`CREATE TABLE IF NOT EXISTS example.users (
		id uuid PRIMARY KEY,
		name text,
		age int
	)`).Exec()
	if err != nil {
		log.Fatalf("failed to create example users table: %v", err)
	}

	_, err = c.Query(
		`INSERT INTO example.users (id, name, age) VALUES (12345678-1234-5678-1234-567812345678,'Alice',30)`).
		Exec()
	if err != nil {
		log.Fatalf("failed to insert example user Alice: %v", err)
	}

	_, err = c.Query(`CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}`).Exec()
	if err != nil {
		log.Fatalf("failed to create test keyspace: %v", err)
	}

	_, err = c.Query(`CREATE TABLE IF NOT EXISTS test.all_types (
		id uuid PRIMARY KEY,
		ascii_col ascii,
		text_col text,
		varchar_col varchar,
		blob_col blob,
		boolean_col boolean,
		decimal_col decimal,
		double_col double,
		float_col float,
		inet_col inet,
		bigint_col bigint,
		int_col int,
		smallint_col smallint,
		tinyint_col tinyint,
		varint_col varint,
		timeuuid_col timeuuid,
		map_col map<int, text>,
		map_list_col map<text, frozen<list<int>>>,
		list_col list<text>,
		list_list_col list<frozen<list<text>>>,
		set_col set<text>,
		tuple_col tuple<int, text, float>
	)`).Exec()
	if err != nil {
		log.Fatalf("failed to create test table: %v", err)
	}

	os.Exit(m.Run())
}
