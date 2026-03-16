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
	"encoding/json"
	"fmt"
	"testing"
	"time"

	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestCDCReplication verifies end-to-end CDC from PostgreSQL into Antfly tables.
// Requires: RUN_PG_TESTS=true, a local PostgreSQL with wal_level=logical.
func TestCDCReplication(t *testing.T) {
	skipUnlessPG(t)
	skipIfPostgresUnavailable(t)
	skipIfWalLevelNotLogical(t)

	ctx := testContext(t, 5*time.Minute)

	// Start a single Antfly swarm for all sub-tests
	swarm := startAntflySwarmWithOptions(t, ctx, SwarmOptions{DisableTermite: true})
	t.Cleanup(swarm.Cleanup)

	t.Run("Passthrough", func(t *testing.T) {
		pgTable := "antfly_cdc_e2e_passthrough"
		antflyTable := "cdc_passthrough"
		slotName := "antfly_cdc_passthrough_antfly_cdc_e2e_passthrough"
		pubName := "antfly_pub_cdc_passthrough_antfly_cdc_e2e_passthrough"

		// Cleanup PG artifacts on exit
		t.Cleanup(func() { cdcCleanupPG(t, pgTable, slotName, pubName) })

		// Create PG table (empty — rows inserted after Antfly table setup)
		conn, err := pgx.Connect(ctx, pgDSN())
		require.NoError(t, err)
		_, _ = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", pgTable))
		_, err = conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (
			id    TEXT PRIMARY KEY,
			name  TEXT NOT NULL,
			email TEXT NOT NULL,
			score INTEGER NOT NULL
		)`, pgTable))
		require.NoError(t, err, "creating PG table")
		conn.Close(ctx)

		// Create Antfly table with replication source (passthrough mode — no on_update/on_delete)
		t.Log("Creating Antfly table with CDC replication source (passthrough)...")
		err = swarm.Client.CreateTable(ctx, antflyTable, antfly.CreateTableRequest{
			NumShards: 1,
			ReplicationSources: []antfly.ReplicationSource{
				{
					Type:          antfly.ReplicationSourceTypePostgres,
					Dsn:           pgDSN(),
					PostgresTable: pgTable,
					KeyTemplate:   "id",
				},
			},
		})
		require.NoError(t, err, "creating Antfly table with CDC source")
		waitForShardsReady(t, ctx, swarm.Client, antflyTable, 30*time.Second)

		// INSERT rows into PG
		t.Log("Inserting rows into PostgreSQL...")
		conn, err = pgx.Connect(ctx, pgDSN())
		require.NoError(t, err)
		_, err = conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, name, email, score) VALUES
			('user-1', 'Alice', 'alice@example.com', 100),
			('user-2', 'Bob', 'bob@example.com', 200),
			('user-3', 'Charlie', 'charlie@example.com', 300)
		`, pgTable))
		require.NoError(t, err, "inserting PG rows")

		// Verify documents appear in Antfly
		t.Log("Waiting for CDC replication...")
		for _, key := range []string{"user-1", "user-2", "user-3"} {
			require.NoError(t,
				waitForKeyAvailable(t, ctx, swarm.Client, antflyTable, key, 30*time.Second),
				"key %s not replicated", key)
		}

		// Verify document content
		doc, err := swarm.Client.LookupKey(ctx, antflyTable, "user-1")
		require.NoError(t, err)
		require.Equal(t, "Alice", doc["name"], "expected name=Alice")
		require.Equal(t, "alice@example.com", doc["email"], "expected email")
		t.Logf("user-1 document: %v", doc)

		// UPDATE a row in PG
		t.Log("Updating user-1 email in PostgreSQL...")
		_, err = conn.Exec(ctx, fmt.Sprintf(`UPDATE %s SET email = 'alice-new@example.com' WHERE id = 'user-1'`, pgTable))
		require.NoError(t, err, "updating PG row")

		// Wait for update to propagate
		require.NoError(t,
			waitForFieldValue(t, ctx, swarm.Client, antflyTable, "user-1", "email", "alice-new@example.com", 30*time.Second),
			"UPDATE not replicated")
		t.Log("UPDATE replicated successfully")

		// DELETE a row in PG (passthrough auto on_delete: $unset all fields)
		t.Log("Deleting user-3 from PostgreSQL...")
		_, err = conn.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE id = 'user-3'`, pgTable))
		require.NoError(t, err, "deleting PG row")

		// In passthrough mode, auto on_delete unsets all $set fields.
		// The document still exists but fields should be removed.
		// Wait for a field to disappear.
		require.NoError(t,
			waitForFieldGone(t, ctx, swarm.Client, antflyTable, "user-3", "name", 30*time.Second),
			"DELETE (auto $unset) not replicated")
		t.Log("DELETE (auto $unset) replicated successfully")

		conn.Close(ctx)
	})

	t.Run("CustomTransforms", func(t *testing.T) {
		pgTable := "antfly_cdc_e2e_transforms"
		antflyTable := "cdc_transforms"
		slotName := "antfly_cdc_transforms_antfly_cdc_e2e_transforms"
		pubName := "antfly_pub_cdc_transforms_antfly_cdc_e2e_transforms"

		t.Cleanup(func() { cdcCleanupPG(t, pgTable, slotName, pubName) })

		// Create PG table
		conn, err := pgx.Connect(ctx, pgDSN())
		require.NoError(t, err)
		_, _ = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", pgTable))
		_, err = conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (
			id         TEXT PRIMARY KEY,
			user_name  TEXT NOT NULL,
			user_email TEXT NOT NULL
		)`, pgTable))
		require.NoError(t, err, "creating PG table")
		conn.Close(ctx)

		// Create Antfly table with custom on_update and on_delete transforms
		t.Log("Creating Antfly table with custom CDC transforms...")
		err = swarm.Client.CreateTable(ctx, antflyTable, antfly.CreateTableRequest{
			NumShards: 1,
			ReplicationSources: []antfly.ReplicationSource{
				{
					Type:          antfly.ReplicationSourceTypePostgres,
					Dsn:           pgDSN(),
					PostgresTable: pgTable,
					KeyTemplate:   "id",
					OnUpdate: []antfly.ReplicationTransformOp{
						{Op: "$set", Path: "name", Value: "{{user_name}}"},
						{Op: "$set", Path: "email", Value: "{{user_email}}"},
						{Op: "$set", Path: "active", Value: true},
					},
					OnDelete: []antfly.ReplicationTransformOp{
						{Op: "$set", Path: "active", Value: false},
					},
				},
			},
		})
		require.NoError(t, err, "creating Antfly table with custom transforms")
		waitForShardsReady(t, ctx, swarm.Client, antflyTable, 30*time.Second)

		time.Sleep(3 * time.Second)

		// INSERT a row
		t.Log("Inserting row into PostgreSQL...")
		conn, err = pgx.Connect(ctx, pgDSN())
		require.NoError(t, err)
		_, err = conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, user_name, user_email) VALUES ('t-1', 'Alice', 'alice@example.com')`, pgTable))
		require.NoError(t, err)

		// Wait for document
		require.NoError(t,
			waitForKeyAvailable(t, ctx, swarm.Client, antflyTable, "t-1", 30*time.Second),
			"key t-1 not replicated")

		// Verify custom transforms applied
		doc, err := swarm.Client.LookupKey(ctx, antflyTable, "t-1")
		require.NoError(t, err)
		require.Equal(t, "Alice", doc["name"], "on_update should set name from {{user_name}}")
		require.Equal(t, "alice@example.com", doc["email"], "on_update should set email from {{user_email}}")
		require.Equal(t, true, doc["active"], "on_update should set active=true (literal)")

		// Verify raw PG columns are NOT present (transforms are selective)
		require.Nil(t, doc["user_name"], "raw PG column user_name should not be in doc")
		require.Nil(t, doc["user_email"], "raw PG column user_email should not be in doc")
		t.Logf("t-1 document: %v", doc)

		// DELETE the row (should soft-delete via on_delete: $set active=false)
		t.Log("Deleting t-1 from PostgreSQL (soft-delete via on_delete)...")
		_, err = conn.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE id = 't-1'`, pgTable))
		require.NoError(t, err)

		// Wait for active=false
		require.NoError(t,
			waitForFieldValue(t, ctx, swarm.Client, antflyTable, "t-1", "active", false, 30*time.Second),
			"on_delete soft-delete not replicated")

		// Document should still exist with name intact
		doc, err = swarm.Client.LookupKey(ctx, antflyTable, "t-1")
		require.NoError(t, err)
		require.Equal(t, "Alice", doc["name"], "soft-delete should preserve other fields")
		t.Log("Custom transform test passed: soft-delete working")

		conn.Close(ctx)
	})

	t.Run("DeleteDocument", func(t *testing.T) {
		pgTable := "antfly_cdc_e2e_delete"
		antflyTable := "cdc_delete"
		slotName := "antfly_cdc_delete_antfly_cdc_e2e_delete"
		pubName := "antfly_pub_cdc_delete_antfly_cdc_e2e_delete"

		t.Cleanup(func() { cdcCleanupPG(t, pgTable, slotName, pubName) })

		// Create PG table
		conn, err := pgx.Connect(ctx, pgDSN())
		require.NoError(t, err)
		_, _ = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", pgTable))
		_, err = conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (
			id    TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`, pgTable))
		require.NoError(t, err, "creating PG table")
		conn.Close(ctx)

		// Create Antfly table with $delete_document on_delete
		t.Log("Creating Antfly table with $delete_document on_delete...")
		err = swarm.Client.CreateTable(ctx, antflyTable, antfly.CreateTableRequest{
			NumShards: 1,
			ReplicationSources: []antfly.ReplicationSource{
				{
					Type:          antfly.ReplicationSourceTypePostgres,
					Dsn:           pgDSN(),
					PostgresTable: pgTable,
					KeyTemplate:   "id",
					OnDelete: []antfly.ReplicationTransformOp{
						{Op: "$delete_document"},
					},
				},
			},
		})
		require.NoError(t, err, "creating Antfly table")
		waitForShardsReady(t, ctx, swarm.Client, antflyTable, 30*time.Second)

		time.Sleep(3 * time.Second)

		// INSERT a row
		t.Log("Inserting row into PostgreSQL...")
		conn, err = pgx.Connect(ctx, pgDSN())
		require.NoError(t, err)
		_, err = conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, value) VALUES ('d-1', 'hello')`, pgTable))
		require.NoError(t, err)

		// Wait for document to appear
		require.NoError(t,
			waitForKeyAvailable(t, ctx, swarm.Client, antflyTable, "d-1", 30*time.Second),
			"key d-1 not replicated")

		doc, err := swarm.Client.LookupKey(ctx, antflyTable, "d-1")
		require.NoError(t, err)
		require.Equal(t, "hello", doc["value"])
		t.Log("Document d-1 replicated successfully")

		// DELETE the row — should fully delete the Antfly document
		t.Log("Deleting d-1 from PostgreSQL ($delete_document)...")
		_, err = conn.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE id = 'd-1'`, pgTable))
		require.NoError(t, err)

		// Wait for document to be gone
		require.NoError(t,
			waitForKeyGone(t, ctx, swarm.Client, antflyTable, "d-1", 30*time.Second),
			"$delete_document did not remove Antfly document")
		t.Log("$delete_document test passed: document fully removed")

		conn.Close(ctx)
	})

	t.Run("RouteFanOut", func(t *testing.T) {
		pgTable := "antfly_cdc_e2e_routes"
		// Source table feeds into two Antfly tables via route filters
		antflyPremium := "cdc_routes_premium"
		antflyFree := "cdc_routes_free"
		slotName := "antfly_cdc_routes_premium_antfly_cdc_e2e_routes"
		pubName := "antfly_pub_cdc_routes_premium_antfly_cdc_e2e_routes"

		t.Cleanup(func() { cdcCleanupPG(t, pgTable, slotName, pubName) })

		// Create PG table with a tier column for routing
		conn, err := pgx.Connect(ctx, pgDSN())
		require.NoError(t, err)
		_, _ = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", pgTable))
		_, err = conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (
			id   TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			tier TEXT NOT NULL
		)`, pgTable))
		require.NoError(t, err, "creating PG table")
		conn.Close(ctx)

		// Create the two target Antfly tables first (routes target existing tables)
		t.Log("Creating target Antfly tables for route fan-out...")
		err = swarm.Client.CreateTable(ctx, antflyPremium, antfly.CreateTableRequest{NumShards: 1})
		require.NoError(t, err, "creating premium table")
		waitForShardsReady(t, ctx, swarm.Client, antflyPremium, 30*time.Second)

		err = swarm.Client.CreateTable(ctx, antflyFree, antfly.CreateTableRequest{NumShards: 1})
		require.NoError(t, err, "creating free table")
		waitForShardsReady(t, ctx, swarm.Client, antflyFree, 30*time.Second)

		// Create a "router" table that owns the replication source with routes
		// The router table itself won't receive data — the routes fan out to the targets.
		antflyRouter := "cdc_routes_router"
		t.Log("Creating router Antfly table with CDC routes...")
		err = swarm.Client.CreateTable(ctx, antflyRouter, antfly.CreateTableRequest{
			NumShards: 1,
			ReplicationSources: []antfly.ReplicationSource{
				{
					Type:          antfly.ReplicationSourceTypePostgres,
					Dsn:           pgDSN(),
					PostgresTable: pgTable,
					KeyTemplate:   "id",
					Routes: []antfly.ReplicationRoute{
						{
							TargetTable: antflyPremium,
							Where:       json.RawMessage(`{"term": "premium", "field": "tier"}`),
						},
						{
							TargetTable: antflyFree,
							Where:       json.RawMessage(`{"term": "free", "field": "tier"}`),
						},
					},
				},
			},
		})
		require.NoError(t, err, "creating router table with CDC routes")
		waitForShardsReady(t, ctx, swarm.Client, antflyRouter, 30*time.Second)

		time.Sleep(3 * time.Second)

		// INSERT rows with different tiers
		t.Log("Inserting rows into PostgreSQL...")
		conn, err = pgx.Connect(ctx, pgDSN())
		require.NoError(t, err)
		_, err = conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, name, tier) VALUES
			('r-1', 'Alice',   'premium'),
			('r-2', 'Bob',     'free'),
			('r-3', 'Charlie', 'premium'),
			('r-4', 'Dave',    'free')
		`, pgTable))
		require.NoError(t, err, "inserting PG rows")

		// Verify premium users land in the premium table
		t.Log("Waiting for route fan-out replication...")
		for _, key := range []string{"r-1", "r-3"} {
			require.NoError(t,
				waitForKeyAvailable(t, ctx, swarm.Client, antflyPremium, key, 30*time.Second),
				"premium key %s not replicated", key)
		}

		// Verify free users land in the free table
		for _, key := range []string{"r-2", "r-4"} {
			require.NoError(t,
				waitForKeyAvailable(t, ctx, swarm.Client, antflyFree, key, 30*time.Second),
				"free key %s not replicated", key)
		}

		// Verify premium users are NOT in the free table
		doc, err := swarm.Client.LookupKey(ctx, antflyFree, "r-1")
		require.Error(t, err, "premium user r-1 should not be in free table")
		_ = doc

		// Verify free users are NOT in the premium table
		doc, err = swarm.Client.LookupKey(ctx, antflyPremium, "r-2")
		require.Error(t, err, "free user r-2 should not be in premium table")
		_ = doc

		// Verify document content
		doc, err = swarm.Client.LookupKey(ctx, antflyPremium, "r-1")
		require.NoError(t, err)
		require.Equal(t, "Alice", doc["name"])
		require.Equal(t, "premium", doc["tier"])
		t.Log("Route fan-out test passed: rows correctly routed by tier")

		// Test UPDATE that changes tier (row moves between routes)
		t.Log("Updating r-2 tier from free to premium...")
		_, err = conn.Exec(ctx, fmt.Sprintf(`UPDATE %s SET tier = 'premium' WHERE id = 'r-2'`, pgTable))
		require.NoError(t, err)

		// After the UPDATE, r-2 should appear in premium table
		require.NoError(t,
			waitForKeyAvailable(t, ctx, swarm.Client, antflyPremium, "r-2", 30*time.Second),
			"r-2 should appear in premium table after tier change")
		t.Log("Route fan-out UPDATE test passed")

		conn.Close(ctx)
	})

	t.Run("PublicationFilter", func(t *testing.T) {
		pgTable := "antfly_cdc_e2e_pubfilter"
		antflyTable := "cdc_pubfilter"
		slotName := "antfly_cdc_pubfilter_antfly_cdc_e2e_pubfilter"
		pubName := "antfly_pub_cdc_pubfilter_antfly_cdc_e2e_pubfilter"

		t.Cleanup(func() { cdcCleanupPG(t, pgTable, slotName, pubName) })

		// Create PG table with a status column for filtering
		conn, err := pgx.Connect(ctx, pgDSN())
		require.NoError(t, err)
		_, _ = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", pgTable))
		_, err = conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (
			id     TEXT PRIMARY KEY,
			name   TEXT NOT NULL,
			status TEXT NOT NULL
		)`, pgTable))
		require.NoError(t, err, "creating PG table")
		conn.Close(ctx)

		// Create Antfly table with publication_filter: only status='active' rows
		t.Log("Creating Antfly table with publication_filter...")
		err = swarm.Client.CreateTable(ctx, antflyTable, antfly.CreateTableRequest{
			NumShards: 1,
			ReplicationSources: []antfly.ReplicationSource{
				{
					Type:              antfly.ReplicationSourceTypePostgres,
					Dsn:               pgDSN(),
					PostgresTable:     pgTable,
					KeyTemplate:       "id",
					PublicationFilter: json.RawMessage(`{"term": "active", "field": "status"}`),
				},
			},
		})
		require.NoError(t, err, "creating Antfly table with publication_filter")
		waitForShardsReady(t, ctx, swarm.Client, antflyTable, 30*time.Second)

		time.Sleep(3 * time.Second)

		// INSERT rows — some active, some inactive
		t.Log("Inserting rows into PostgreSQL...")
		conn, err = pgx.Connect(ctx, pgDSN())
		require.NoError(t, err)
		_, err = conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, name, status) VALUES
			('pf-1', 'Alice',   'active'),
			('pf-2', 'Bob',     'inactive'),
			('pf-3', 'Charlie', 'active')
		`, pgTable))
		require.NoError(t, err, "inserting PG rows")

		// Verify active rows appear
		t.Log("Waiting for filtered replication...")
		for _, key := range []string{"pf-1", "pf-3"} {
			require.NoError(t,
				waitForKeyAvailable(t, ctx, swarm.Client, antflyTable, key, 30*time.Second),
				"active key %s not replicated", key)
		}

		// Verify content
		doc, err := swarm.Client.LookupKey(ctx, antflyTable, "pf-1")
		require.NoError(t, err)
		require.Equal(t, "Alice", doc["name"])
		require.Equal(t, "active", doc["status"])

		// Wait a bit and verify inactive row did NOT appear
		// (PG publication WHERE clause should have excluded it)
		time.Sleep(5 * time.Second)
		_, err = swarm.Client.LookupKey(ctx, antflyTable, "pf-2")
		require.Error(t, err, "inactive row pf-2 should not be replicated (publication_filter)")
		t.Log("Publication filter test passed: only active rows replicated")

		conn.Close(ctx)
	})
}
