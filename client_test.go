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

func ExampleClient_Query_withOptions() {
	c, err := NewStaticTokenClient(endpoint, token)
	if err != nil {
		log.Fatalf("failed to initialize client: %v", err)
	}

	rows, err := c.Query(
		`SELECT * FROM users WHERE id = ?`,
		uuid.MustParse("12345678-1234-5678-1234-567812345678"),
	).
		Keyspace("example").
		Exec()
	if err != nil {
		log.Fatalf("failed to execute query: %v", err)
	}

	fmt.Printf("rows returned: %v", len(rows))

	// Output:
	// rows returned: 1
}

func ExampleClient_Query_cast() {
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

func ExampleClient_Query_scan() {
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

	type User struct {
		ID   uuid.UUID
		Name string
		Age  int16
	}

	for _, row := range rows {
		u := &User{}
		err := row.Scan(&u.ID, &u.Name, &u.Age)
		if err != nil {
			log.Fatalf("failed to scan row: %v", err)
		}
		fmt.Printf("%+v\n", u)
	}

	// Output:
	// &{ID:12345678-1234-5678-1234-567812345678 Name:Alice Age:30}
}

func ExampleClient_Batch() {
	c, err := NewStaticTokenClient(
		endpoint, token,
		WithDefaultKeyspace("example"),
	)
	if err != nil {
		log.Fatalf("failed to initialize client: %v", err)
	}

	err = c.Batch(
		// Table already contains a user named 'Alice'.
		c.Query(
			`INSERT INTO users (id, name, age) VALUES (12345678-1234-5678-1234-56781234567B,'Bob',31)`),
		c.Query(
			`INSERT INTO users (id, name, age) VALUES (12345678-1234-5678-1234-56781234567C,'Charles',32)`),
	).
		BatchType(BatchUnlogged).
		Exec()
	if err != nil {
		log.Fatalf("failed to insert new example users: %v", err)
	}

	rows, err := c.Query(`SELECT * FROM users`).Exec()
	if err != nil {
		log.Fatalf("failed to execute query: %v", err)
	}

	fmt.Printf("rows returned: %v", len(rows))

	// Output:
	// rows returned: 3
}

func ExampleClient_Batch_withOptions() {
	c, err := NewStaticTokenClient(endpoint, token)
	if err != nil {
		log.Fatalf("failed to initialize client: %v", err)
	}

	err = c.Batch(
		// Table already contains a user named 'Alice'.
		c.Query(
			`INSERT INTO users (id, name, age) VALUES (12345678-1234-5678-1234-56781234567B,'Bob',31)`),
		c.Query(
			`INSERT INTO users (id, name, age) VALUES (12345678-1234-5678-1234-56781234567C,'Charles',32)`),
	).
		Keyspace("example").
		BatchType(BatchUnlogged).
		Exec()
	if err != nil {
		log.Fatalf("failed to insert new example users: %v", err)
	}

	rows, err := c.Query(`SELECT * FROM users`).Keyspace("example").Exec()
	if err != nil {
		log.Fatalf("failed to execute query: %v", err)
	}

	fmt.Printf("rows returned: %v", len(rows))

	// Output:
	// rows returned: 3
}
