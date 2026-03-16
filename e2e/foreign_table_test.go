// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package e2e

import (
	"fmt"
	"testing"
	"time"

	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/antflydb/antfly/pkg/client/query"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// ---------- Antfly-to-Antfly Join Tests ----------

// TestE2E_AntflyJoin tests joining two Antfly tables (no foreign tables / PG needed).
func TestE2E_AntflyJoin(t *testing.T) {
	skipInShortMode(t)

	ctx := testContext(t, 5*time.Minute)

	swarm := startAntflySwarmWithOptions(t, ctx, SwarmOptions{DisableTermite: true})
	t.Cleanup(swarm.Cleanup)

	// ---- Create tables ----
	t.Log("Creating customers and orders tables...")
	err := swarm.Client.CreateTable(ctx, "customers", antfly.CreateTableRequest{NumShards: 1})
	require.NoError(t, err, "creating customers table")

	err = swarm.Client.CreateTable(ctx, "orders", antfly.CreateTableRequest{NumShards: 1})
	require.NoError(t, err, "creating orders table")

	waitForShardsReady(t, ctx, swarm.Client, "customers", 30*time.Second)
	waitForShardsReady(t, ctx, swarm.Client, "orders", 30*time.Second)

	// ---- Insert customer data ----
	t.Log("Inserting customer data...")
	_, err = swarm.Client.Batch(ctx, "customers", antfly.BatchRequest{
		Inserts: map[string]any{
			"cust-1": map[string]any{"customer_id": "cust-1", "name": "Alice", "email": "alice@example.com", "tier": "gold"},
			"cust-2": map[string]any{"customer_id": "cust-2", "name": "Bob", "email": "bob@example.com", "tier": "silver"},
			"cust-3": map[string]any{"customer_id": "cust-3", "name": "Charlie", "email": "charlie@example.com", "tier": "gold"},
			"cust-4": map[string]any{"customer_id": "cust-4", "name": "Diana", "email": "diana@example.com", "tier": "bronze"},
			"cust-5": map[string]any{"customer_id": "cust-5", "name": "Eve", "email": "eve@example.com", "tier": "silver"},
		},
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "inserting customers")

	// ---- Insert order data ----
	t.Log("Inserting order data...")
	_, err = swarm.Client.Batch(ctx, "orders", antfly.BatchRequest{
		Inserts: map[string]any{
			"order-001": map[string]any{"customer_id": "cust-1", "product": "Widget A", "amount": 29.99},
			"order-002": map[string]any{"customer_id": "cust-3", "product": "Widget B", "amount": 49.99},
			"order-003": map[string]any{"customer_id": "cust-1", "product": "Widget C", "amount": 19.99},
		},
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "inserting orders")

	// ---- Wait for keys ----
	for _, key := range []string{"cust-1", "cust-2", "cust-3", "cust-4", "cust-5"} {
		require.NoError(t,
			waitForKeyAvailable(t, ctx, swarm.Client, "customers", key, 15*time.Second),
			"customer key %s not available", key)
	}
	for _, key := range []string{"order-001", "order-002", "order-003"} {
		require.NoError(t,
			waitForKeyAvailable(t, ctx, swarm.Client, "orders", key, 15*time.Second),
			"order key %s not available", key)
	}

	// ---- INNER JOIN ----
	t.Log("Executing INNER JOIN: orders JOIN customers ON customer_id = customer_id...")
	resp, err := swarm.Client.Query(ctx, antfly.QueryRequest{
		Table: "orders",
		Limit: 10,
		Join: antfly.JoinClause{
			RightTable: "customers",
			JoinType:   antfly.JoinTypeInner,
			On: antfly.JoinCondition{
				LeftField:  "customer_id",
				RightField: "customer_id",
				Operator:   antfly.JoinOperatorEq,
			},
			RightFields: []string{"name", "email", "tier"},
		},
	})
	require.NoError(t, err, "inner join query failed")
	require.Len(t, resp.Responses, 1)

	result := resp.Responses[0]
	require.Equal(t, int32(200), result.Status,
		"inner join returned %d: %s", result.Status, result.Error)

	// All 3 orders should match a customer
	require.GreaterOrEqual(t, len(result.Hits.Hits), 3,
		"expected at least 3 joined rows, got %d", len(result.Hits.Hits))

	// Verify joined fields are present (prefixed with right table name)
	for _, hit := range result.Hits.Hits {
		custID, _ := hit.Source["customer_id"].(string)
		switch custID {
		case "cust-1":
			require.Equal(t, "Alice", hit.Source["customers.name"],
				"expected Alice for cust-1")
			require.Equal(t, "gold", hit.Source["customers.tier"])
		case "cust-3":
			require.Equal(t, "Charlie", hit.Source["customers.name"],
				"expected Charlie for cust-3")
		}
	}

	// ---- LEFT JOIN with unmatched row ----
	// Insert an order referencing a non-existent customer
	t.Log("Inserting order with non-existent customer for LEFT JOIN test...")
	_, err = swarm.Client.Batch(ctx, "orders", antfly.BatchRequest{
		Inserts: map[string]any{
			"order-004": map[string]any{"customer_id": "cust-999", "product": "Widget D", "amount": 9.99},
		},
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "inserting unmatched order")
	require.NoError(t,
		waitForKeyAvailable(t, ctx, swarm.Client, "orders", "order-004", 15*time.Second),
		"order-004 not available")

	t.Log("Executing LEFT JOIN: orders LEFT JOIN customers ON customer_id...")
	resp, err = swarm.Client.Query(ctx, antfly.QueryRequest{
		Table: "orders",
		Limit: 20,
		Join: antfly.JoinClause{
			RightTable: "customers",
			JoinType:   antfly.JoinTypeLeft,
			On: antfly.JoinCondition{
				LeftField:  "customer_id",
				RightField: "customer_id",
				Operator:   antfly.JoinOperatorEq,
			},
			RightFields: []string{"name", "email", "tier"},
		},
	})
	require.NoError(t, err, "left join query failed")
	require.Len(t, resp.Responses, 1)

	result = resp.Responses[0]
	require.Equal(t, int32(200), result.Status,
		"left join returned %d: %s", result.Status, result.Error)

	// Should get all 4 orders (3 matched + 1 unmatched)
	require.GreaterOrEqual(t, len(result.Hits.Hits), 4,
		"expected at least 4 rows in left join, got %d", len(result.Hits.Hits))

	// Find the unmatched order and verify right-side fields are nil
	foundUnmatched := false
	for _, hit := range result.Hits.Hits {
		custID, _ := hit.Source["customer_id"].(string)
		if custID == "cust-999" {
			foundUnmatched = true
			// Right-side fields should be nil/missing for unmatched row
			require.Nil(t, hit.Source["customers.name"],
				"expected nil name for unmatched customer")
		}
	}
	require.True(t, foundUnmatched, "expected to find unmatched order (cust-999) in LEFT JOIN results")

	t.Log("Antfly-to-Antfly join test passed")
}

// ---------- Foreign Table Tests ----------

// TestE2E_ForeignTable_BasicQuery queries a PostgreSQL table through the
// Antfly foreign table API without any join.
func TestE2E_ForeignTable_BasicQuery(t *testing.T) {
	skipUnlessPG(t)
	skipIfPostgresUnavailable(t)

	ctx := testContext(t, 3*time.Minute)

	// Set up PG test data
	cleanup := setupPGTestData(t, ctx)
	t.Cleanup(cleanup)

	// Start Antfly (no Termite needed)
	swarm := startAntflySwarmWithOptions(t, ctx, SwarmOptions{DisableTermite: true})
	t.Cleanup(swarm.Cleanup)

	// Query the foreign table — no Antfly table creation needed.
	// The foreign_sources map tells Antfly to route the query to PG.
	t.Log("Querying foreign PostgreSQL table through Antfly...")
	resp, err := swarm.Client.Query(ctx, antfly.QueryRequest{
		Table: "pg_customers",
		Limit: 10,
		ForeignSources: map[string]antfly.ForeignSource{
			"pg_customers": foreignSource(),
		},
	})
	require.NoError(t, err, "foreign table query failed")
	require.Len(t, resp.Responses, 1, "expected 1 query result set")

	result := resp.Responses[0]
	require.Equal(t, int32(200), result.Status, "expected HTTP 200, got %d: %s", result.Status, result.Error)
	require.Equal(t, uint64(5), result.Hits.Total, "expected 5 rows from PG")
	require.Len(t, result.Hits.Hits, 5)

	// Verify fields came through
	found := map[string]bool{}
	for _, hit := range result.Hits.Hits {
		name, _ := hit.Source["name"].(string)
		found[name] = true
	}
	require.True(t, found["Alice"], "expected Alice in results")
	require.True(t, found["Eve"], "expected Eve in results")
}

// TestE2E_ForeignTable_FilteredQuery queries a foreign PG table with a filter.
func TestE2E_ForeignTable_FilteredQuery(t *testing.T) {
	skipUnlessPG(t)
	skipIfPostgresUnavailable(t)

	ctx := testContext(t, 3*time.Minute)

	cleanup := setupPGTestData(t, ctx)
	t.Cleanup(cleanup)

	swarm := startAntflySwarmWithOptions(t, ctx, SwarmOptions{DisableTermite: true})
	t.Cleanup(swarm.Cleanup)

	// Filter for gold tier customers
	t.Log("Querying foreign table with filter_query for tier=gold...")
	filterQ := query.NewTerm("gold", "tier")
	resp, err := swarm.Client.Query(ctx, antfly.QueryRequest{
		Table:       "pg_customers",
		Limit:       10,
		FilterQuery: &filterQ,
		ForeignSources: map[string]antfly.ForeignSource{
			"pg_customers": foreignSource(),
		},
	})
	require.NoError(t, err, "filtered foreign query failed")
	require.Len(t, resp.Responses, 1)

	result := resp.Responses[0]
	require.Equal(t, int32(200), result.Status, "expected 200, got %d: %s", result.Status, result.Error)

	// Should get Alice and Charlie (both gold)
	require.Equal(t, uint64(2), result.Hits.Total, "expected 2 gold-tier customers")
	names := map[string]bool{}
	for _, hit := range result.Hits.Hits {
		name, _ := hit.Source["name"].(string)
		names[name] = true
	}
	require.True(t, names["Alice"], "expected Alice (gold)")
	require.True(t, names["Charlie"], "expected Charlie (gold)")
}

// TestE2E_ForeignTable_UnsupportedOps verifies that unsupported operations
// on foreign tables are rejected with 400.
func TestE2E_ForeignTable_UnsupportedOps(t *testing.T) {
	skipUnlessPG(t)
	skipIfPostgresUnavailable(t)

	ctx := testContext(t, 3*time.Minute)

	cleanup := setupPGTestData(t, ctx)
	t.Cleanup(cleanup)

	swarm := startAntflySwarmWithOptions(t, ctx, SwarmOptions{DisableTermite: true})
	t.Cleanup(swarm.Cleanup)

	// Unsupported aggregation types on foreign tables should fail with 400
	t.Log("Verifying unsupported aggregation types are rejected on foreign tables...")
	resp, err := swarm.Client.Query(ctx, antfly.QueryRequest{
		Table: "pg_customers",
		Limit: 10,
		Aggregations: map[string]antfly.AggregationRequest{
			"geo": {Field: "tier", Type: antfly.AggregationTypeGeohashGrid},
		},
		ForeignSources: map[string]antfly.ForeignSource{
			"pg_customers": foreignSource(),
		},
	})
	require.NoError(t, err, "request should succeed at HTTP level but return error in result status")
	require.Len(t, resp.Responses, 1)
	require.Equal(t, int32(400), resp.Responses[0].Status, "geohash_grid aggregation should be rejected with 400")
}

// ---------- Foreign Table Join Tests ----------

// TestE2E_ForeignTable_JoinWithAntfly tests joining an Antfly table with a
// foreign PostgreSQL table.
func TestE2E_ForeignTable_JoinWithAntfly(t *testing.T) {
	skipUnlessPG(t)
	skipIfPostgresUnavailable(t)

	ctx := testContext(t, 5*time.Minute)

	cleanup := setupPGTestData(t, ctx)
	t.Cleanup(cleanup)

	swarm := startAntflySwarmWithOptions(t, ctx, SwarmOptions{DisableTermite: true})
	t.Cleanup(swarm.Cleanup)

	// Create an Antfly table with order data that references customer IDs
	t.Log("Creating Antfly orders table...")
	err := swarm.Client.CreateTable(ctx, "orders", antfly.CreateTableRequest{
		NumShards: 1,
	})
	require.NoError(t, err, "creating orders table")
	waitForShardsReady(t, ctx, swarm.Client, "orders", 30*time.Second)

	// Insert order documents into Antfly.
	// customer_id is a string to match the PG column type for join compatibility.
	t.Log("Inserting order data into Antfly...")
	_, err = swarm.Client.Batch(ctx, "orders", antfly.BatchRequest{
		Inserts: map[string]any{
			"order-001": map[string]any{
				"customer_id": "cust-1",
				"product":     "Widget A",
				"amount":      29.99,
			},
			"order-002": map[string]any{
				"customer_id": "cust-3",
				"product":     "Widget B",
				"amount":      49.99,
			},
			"order-003": map[string]any{
				"customer_id": "cust-1",
				"product":     "Widget C",
				"amount":      19.99,
			},
		},
		SyncLevel: antfly.SyncLevelWrite,
	})
	require.NoError(t, err, "inserting orders")

	// Wait for all keys to be available
	for _, key := range []string{"order-001", "order-002", "order-003"} {
		require.NoError(t,
			waitForKeyAvailable(t, ctx, swarm.Client, "orders", key, 15*time.Second),
			"key %s not available", key)
	}

	// Join orders (Antfly) with customers (PostgreSQL)
	t.Log("Executing join: orders LEFT JOIN pg_customers ON customer_id = customer_id...")
	resp, err := swarm.Client.Query(ctx, antfly.QueryRequest{
		Table: "orders",
		Limit: 10,
		Join: antfly.JoinClause{
			RightTable: "pg_customers",
			JoinType:   antfly.JoinTypeLeft,
			On: antfly.JoinCondition{
				LeftField:  "customer_id",
				RightField: "customer_id",
				Operator:   antfly.JoinOperatorEq,
			},
			RightFields: []string{"name", "email", "tier"},
		},
		ForeignSources: map[string]antfly.ForeignSource{
			"pg_customers": foreignSource(),
		},
	})
	require.NoError(t, err, "join query failed")
	require.Len(t, resp.Responses, 1)

	result := resp.Responses[0]
	require.Equal(t, int32(200), result.Status,
		"join query returned %d: %s", result.Status, result.Error)

	// We should get 3 joined rows (one per order)
	require.GreaterOrEqual(t, len(result.Hits.Hits), 3,
		"expected at least 3 joined rows, got %d", len(result.Hits.Hits))

	// Verify that customer fields were joined in.
	// The join executor prefixes right-side fields with "{rightTable}.{field}".
	for _, hit := range result.Hits.Hits {
		custID, _ := hit.Source["customer_id"].(string)
		switch custID {
		case "cust-1":
			require.Equal(t, "Alice", hit.Source["pg_customers.name"],
				"expected Alice for customer_id=cust-1, got %v", hit.Source["pg_customers.name"])
			require.Equal(t, "gold", hit.Source["pg_customers.tier"])
		case "cust-3":
			require.Equal(t, "Charlie", hit.Source["pg_customers.name"],
				"expected Charlie for customer_id=cust-3")
		}
	}

	t.Log("Join test passed: Antfly orders successfully joined with PostgreSQL customers")
}

// TestE2E_ForeignTable_CDCJoin tests a CDC-replicated Antfly table joined with
// a foreign PostgreSQL table. This validates the full path: PG → CDC → Antfly → JOIN → PG.
func TestE2E_ForeignTable_CDCJoin(t *testing.T) {
	skipUnlessPG(t)
	skipIfPostgresUnavailable(t)
	skipIfWalLevelNotLogical(t)

	ctx := testContext(t, 5*time.Minute)

	// ---- Set up PG customers (foreign table) ----
	cleanupCustomers := setupPGTestData(t, ctx)
	t.Cleanup(cleanupCustomers)

	// ---- Set up PG orders (CDC source) ----
	const pgOrdersTable = "antfly_e2e_cdc_orders"
	const antflyOrdersTable = "cdc_join_orders"
	slotName := "antfly_cdc_join_orders_" + pgOrdersTable
	pubName := "antfly_pub_cdc_join_orders_" + pgOrdersTable

	t.Cleanup(func() { cdcCleanupPG(t, pgOrdersTable, slotName, pubName) })

	conn, err := pgx.Connect(ctx, pgDSN())
	require.NoError(t, err)
	_, _ = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", pgOrdersTable))
	_, err = conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (
		id          TEXT PRIMARY KEY,
		customer_id TEXT NOT NULL,
		product     TEXT NOT NULL,
		amount      NUMERIC(10,2) NOT NULL
	)`, pgOrdersTable))
	require.NoError(t, err, "creating PG orders table")
	conn.Close(ctx)

	// ---- Start Antfly ----
	swarm := startAntflySwarmWithOptions(t, ctx, SwarmOptions{DisableTermite: true})
	t.Cleanup(swarm.Cleanup)

	// Create Antfly table with CDC replication from PG orders
	t.Log("Creating Antfly table with CDC replication source for orders...")
	err = swarm.Client.CreateTable(ctx, antflyOrdersTable, antfly.CreateTableRequest{
		NumShards: 1,
		ReplicationSources: []antfly.ReplicationSource{
			{
				Type:          antfly.ReplicationSourceTypePostgres,
				Dsn:           pgDSN(),
				PostgresTable: pgOrdersTable,
				KeyTemplate:   "id",
			},
		},
	})
	require.NoError(t, err, "creating Antfly CDC orders table")
	waitForShardsReady(t, ctx, swarm.Client, antflyOrdersTable, 30*time.Second)

	// ---- INSERT orders into PG ----
	t.Log("Inserting orders into PostgreSQL...")
	conn, err = pgx.Connect(ctx, pgDSN())
	require.NoError(t, err)
	_, err = conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, customer_id, product, amount) VALUES
		('order-001', 'cust-1', 'Widget A', 29.99),
		('order-002', 'cust-3', 'Widget B', 49.99),
		('order-003', 'cust-1', 'Widget C', 19.99)
	`, pgOrdersTable))
	require.NoError(t, err, "inserting PG orders")

	// Wait for CDC replication
	t.Log("Waiting for CDC replication of orders...")
	for _, key := range []string{"order-001", "order-002", "order-003"} {
		require.NoError(t,
			waitForKeyAvailable(t, ctx, swarm.Client, antflyOrdersTable, key, 30*time.Second),
			"order %s not replicated", key)
	}

	// Verify order content replicated correctly
	doc, err := swarm.Client.LookupKey(ctx, antflyOrdersTable, "order-001")
	require.NoError(t, err)
	require.Equal(t, "cust-1", doc["customer_id"], "expected customer_id=cust-1")
	require.Equal(t, "Widget A", doc["product"])

	// ---- JOIN: CDC orders (Antfly) LEFT JOIN customers (PG foreign) ----
	t.Log("Executing LEFT JOIN: cdc_orders LEFT JOIN pg_customers ON customer_id...")
	resp, err := swarm.Client.Query(ctx, antfly.QueryRequest{
		Table: antflyOrdersTable,
		Limit: 10,
		Join: antfly.JoinClause{
			RightTable: "pg_customers",
			JoinType:   antfly.JoinTypeLeft,
			On: antfly.JoinCondition{
				LeftField:  "customer_id",
				RightField: "customer_id",
				Operator:   antfly.JoinOperatorEq,
			},
			RightFields: []string{"name", "email", "tier"},
		},
		ForeignSources: map[string]antfly.ForeignSource{
			"pg_customers": foreignSource(),
		},
	})
	require.NoError(t, err, "CDC join query failed")
	require.Len(t, resp.Responses, 1)

	result := resp.Responses[0]
	require.Equal(t, int32(200), result.Status,
		"CDC join returned %d: %s", result.Status, result.Error)
	require.GreaterOrEqual(t, len(result.Hits.Hits), 3,
		"expected at least 3 joined rows, got %d", len(result.Hits.Hits))

	// Verify joined customer fields
	for _, hit := range result.Hits.Hits {
		custID, _ := hit.Source["customer_id"].(string)
		switch custID {
		case "cust-1":
			require.Equal(t, "Alice", hit.Source["pg_customers.name"],
				"expected Alice for customer_id=cust-1")
			require.Equal(t, "gold", hit.Source["pg_customers.tier"])
		case "cust-3":
			require.Equal(t, "Charlie", hit.Source["pg_customers.name"],
				"expected Charlie for customer_id=cust-3")
		}
	}

	// ---- UPDATE: change an order's customer_id via CDC ----
	t.Log("Updating order-003 customer_id from cust-1 to cust-2 in PG...")
	_, err = conn.Exec(ctx, fmt.Sprintf(
		`UPDATE %s SET customer_id = 'cust-2' WHERE id = 'order-003'`, pgOrdersTable))
	require.NoError(t, err, "updating PG order")
	conn.Close(ctx)

	// Wait for CDC to propagate the update
	require.NoError(t,
		waitForFieldValue(t, ctx, swarm.Client, antflyOrdersTable, "order-003", "customer_id", "cust-2", 30*time.Second),
		"CDC update not propagated for order-003")

	// Re-run the join and verify the updated mapping
	t.Log("Re-executing join after CDC update...")
	resp, err = swarm.Client.Query(ctx, antfly.QueryRequest{
		Table: antflyOrdersTable,
		Limit: 10,
		Join: antfly.JoinClause{
			RightTable: "pg_customers",
			JoinType:   antfly.JoinTypeLeft,
			On: antfly.JoinCondition{
				LeftField:  "customer_id",
				RightField: "customer_id",
				Operator:   antfly.JoinOperatorEq,
			},
			RightFields: []string{"name", "email", "tier"},
		},
		ForeignSources: map[string]antfly.ForeignSource{
			"pg_customers": foreignSource(),
		},
	})
	require.NoError(t, err, "post-update join query failed")
	require.Len(t, resp.Responses, 1)

	result = resp.Responses[0]
	require.Equal(t, int32(200), result.Status,
		"post-update join returned %d: %s", result.Status, result.Error)

	// Find order-003 and verify it now maps to Bob (cust-2)
	for _, hit := range result.Hits.Hits {
		id, _ := hit.Source["id"].(string)
		if id == "order-003" {
			require.Equal(t, "cust-2", hit.Source["customer_id"],
				"order-003 should now have customer_id=cust-2 after CDC update")
			require.Equal(t, "Bob", hit.Source["pg_customers.name"],
				"order-003 should now join to Bob after customer_id update")
			break
		}
	}

	t.Log("CDC + Foreign Table Join test passed")
}
