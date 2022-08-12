// Package astra provides the Go SDK for developing applications on the Datastax
// Astra platform.
//
// # Connecting
//
// The Astra Go SDK provides two methods for connecting to Astra:
//
// Use the NewStaticTokenClient method to connect to Astra using a static auth
// token. See [Astra DB Manage application tokens].
//
//     c, err := astra.NewStaticTokenClient(
//         // URL of the Stargate service to use.
//         // Example: "localhost:8090"
//         // Example: "<cluster ID>-<cluster region>.apps.astra.datastax.com:443"
//         astraURI,
//         // Static auth token to use.
//         token,
//         // Optional deadline for initial connection.
//         astra.WithDeadline(time.Second * 10),
//         // Optional per-query timeout.
//         astra.WithTimeout(time.Second * 5),
//         // Optional TLS config. Assumes insecure if not specified.
//         astra.WithTLSConfig(tlsConfig)
//         // Optional default keyspace in which to run queries that do not specify
//         // keyspace.
//         astra.WithDefaultKeyspace(keyspace),
//     )
//
// Use the NewTableBasedTokenClient method to connect to Astra using a Stargate
// table auth API service URL, username, and password. See
// [Astra DB Table-based authentication/authorization].
//
//     c, err := astra.NewTableBasedTokenClient(
//         // URL of the Stargate service to use.
//         astraURI,
//         // Stargate auth endpoint URL.
//         authServiceURL,
//         // Username and password with which to authenticate.
//         username, password,
//         ...
//     )
//
// [Astra DB Manage application tokens]: https://docs.datastax.com/en/astra/docs/manage/org/managing-org.html#_manage_application_tokens
// [Astra DB Table-based authentication/authorization]: https://stargate.io/docs/stargate/1.0/developers-guide/authnz.html#_table_based_authenticationauthorization
//
// # Querying
//
// Create new queries by calling Client.Query to return a new Query, then
// execute it with Query.Exec.
//
//     rows, err := c.Query("SELECT * FROM table").Exec()
//     if err != nil {
//         // Handle error.
//     }
//     for _, r := range rows {
//         // Do something with row.
//     }
//
// Iterate over the returned Rows using a standard for loop. Call
// Row.Values to inspect the values.
//
//     for _, r := range rows {
//         vals := r.Values()
//         someText := vals[0].(string)
//         someNumber := vals[1].(int64)
//     }
//
package astra
