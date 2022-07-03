package astra

import (
	"fmt"
	"log"

	"github.com/google/uuid"
)

func ExampleNewStaticTokenClient() {
	astraURI := "<ASTRA_CLUSTER_ID>-<ASTRA_REGION>.apps.astra.datastax.com:443"
	token := "AstraCS:<...>"
	c, err := NewStaticTokenClient(
		astraURI, token,
		WithDefaultKeyspace("example"),
		// other options
	)
	if err != nil {
		log.Fatalf("failed to initialize client: %v", err)
	}

	_, err = c.Query(`<some query>`).Exec()
	if err != nil {
		log.Fatalf("failed to execute query: %v", err)
	}
}

func ExampleNewTableBasedTokenClient() {
	astraURI := "<ASTRA_CLUSTER_ID>-<ASTRA_REGION>.apps.astra.datastax.com:443"
	authServiceURI := fmt.Sprintf("http://%s/v1/auth", astraURI)
	c, err := NewTableBasedTokenClient(
		astraURI, authServiceURI,
		"username", "password",
		WithDefaultKeyspace("example"),
		// other options
	)
	if err != nil {
		log.Fatalf("failed to initialize client: %v", err)
	}

	_, err = c.Query(`<some query>`).Exec()
	if err != nil {
		log.Fatalf("failed to execute query: %v", err)
	}
}

func ExampleClient_Query() {
	c, err := NewStaticTokenClient(
		endpoint, token,
		WithDefaultKeyspace("example"),
	)
	if err != nil {
		log.Fatalf("failed to initialize client: %v", err)
	}

	rows, err := c.Query(
		`SELECT id, name, age 
		 FROM users 
		 WHERE id = ?`,
		uuid.MustParse("12345678-1234-5678-1234-567812345678"),
	).Exec()
	if err != nil {
		log.Fatalf("failed to execute query: %v", err)
	}

	for _, row := range rows {
		vals := row.Values()
		id := vals[0].(uuid.UUID)
		name := vals[1].(string)
		age := vals[2].(int64)
		fmt.Printf("id: %s, name: %s, age: %d\n", id, name, age)
	}

	// Output:
	// id: 12345678-1234-5678-1234-567812345678, name: Alice, age: 30
}
