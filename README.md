# astra-go-sdk
Software Development Kit wrapping Astra APIs and drivers.

## Overview

TODO

## Connecting

Use the following options to connect to the Stargate service:

```go
c := sg.NewClient(,
	// Token provider options. One must be specified. For more information, see:
	// https://stargate.io/docs/stargate/1.0/developers-guide/authnz.html
	sg.WithStaticToken(astraToken),
	sg.WithStaticTokenUnsafe(astraToken),
	sg.WithTableBasedToken(serviceURL, username, password),
	sg.WithTableBasedTokenUnsafe(serviceURL, username, password),
	
	// Optional timeout for initial connection.
	sg.WithDeadline(time.Second * 10),
	
	// Optional TLS config. Assumes insecure if not specified.
	sg.WithTLS(tlsConfig)
	
	// Optional default keyspace in which to run queries that do not specify a
	// keyspace.
	sg.WithDefaultKeyspace(keyspace),
	
	// Optional connection parameters to pass along to the GRPC client.
	sg.WithGRPCConnParams(connParams),
)
```

## Querying

Mirroring: https://github.com/stargate/stargate/tree/master/grpc-examples

```go
c := sg.NewClient(..., sg.WithDefaultKeyspace("ks"))
_, err := c.Query(ctx, `
CREATE TABLE IF NOT EXISTS test (k text, v int, PRIMARY KEY(k, v));
`).Exec()
if err != nil {
	log.Fatalf("error creating table: %v", err)
}

// Plain CQL query with placeholder parameters.
q := c.Query("INSERT INTO test (k, v) VALUES (?, ?)", 'a', 1)

// CQL builder query.
q := c.QueryBuilder().InsertInto("test").Columns("k", "v").Values('b', 2)

// Prepared statement.
ps := c.QueryBuilder().InsertInto("test").
	Columns("k", "v").
	Values(sg.Placeholder(), sg.Placeholder()).
	Prepare()
q := ps.Build('c', 3)

// Batch query.
q := c.Batch(
	c.Query("INSERT INTO test (k, v) VALUES (?, ?)", 'a', 1)
	c.QueryBuilder().InsertInto("test").Columns("k", "v").Values('b', 2),
	ps.Build('c', 3),
).WithOptions(&sg.BatchOptions{
	Logged: true,
})

res, err := q.Exec()
if err != nil {
	log.Fatalf("error executing query: %v", err)
}
```
## Reading Results

Based on https://gist.github.com/mpenick/8b95bd6326d375de46e4fb6981dad066

```go
c := sg.NewClient(..., sg.WithDefaultKeyspace("ks"))
_, err := c.Query(ctx, `
CREATE TABLE IF NOT EXISTS test (k text, v int, name text PRIMARY KEY(k, v));
`).Exec()
if err != nil {
	log.Fatalf("error creating table: %v", err)
}

// Populate table with the data:
// k | v | name
// ---------------
// a | 1 | Alice
// b | 2 | Bob
// b | 3 | Charles
...

// Plain CQL select.
res, err := c.Query("SELECT v, name FROM test WHERE k = ?", 'b').Exec()
if err != nil {
	log.Fatalf("error executing query: %v", err)
}
// res:
// v | name
// -----------
// 2 | Bob
// 3 | Charles

// Read a single row.
row, err := res.ReadOne()
if err != nil {
	log.Fatalf("error reading row: %v", err)
}
fmt.Printf("%d, %s\n", row[0], row[1])
// Output:
// 2, Bob

// Read multiple rows.
rows, err := res.Read()
if err != nil {
	log.Fatalf("error reading row: %v", err)
}
fmt.Printf("%v\n", rows)
// Output:
// [[2 Bob] [3 Charles]]

// Scan row values into variables.
s := res.Scanner()
var v int
var name string
for s.Scan(&v, &name) {
	fmt.Printf("v: %d, name: %s\n", v, name)
	// Output:
	// v: 2, name: Bob
	// v: 3, name: Charles
}
if err := s.Err(); err != nil {
	log.Fatalf("error scanning results: %v", err)
}

// TODO: ORM
// TODO: Streaming

```

## Complete Example

TODO
