// Copyright 2026 Antfly, Inc.
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

const http_server = @import("api/http_server.zig");
const public_sql_endpoint_parity = @import("api/public_sql_endpoint_parity.zig");
const api_distributed_txn = @import("api/distributed_txn.zig");
const pgwire = @import("pgwire/mod.zig");
const sql_adapter_integration = @import("api/sql_adapter_integration.zig");
const relational_rows = @import("sql/relational_rows.zig");
const sql_lower_dml = @import("sql/lower_dml.zig");
const sql_lower_expr = @import("sql/lower_expr.zig");
const table_reads = @import("api/table_reads.zig");
const table_reads_relational_rows = @import("api/table_reads/relational_rows.zig");
const db_relational_integrity = @import("storage/db/relational_integrity.zig");
const db_relational_store = @import("storage/db/relational_store.zig");
const db_transactions = @import("storage/db/transactions.zig");
const db_types = @import("storage/db/types.zig");
const metadata_http_server = @import("metadata/http_server.zig");
const metadata_reconciler = @import("metadata/reconciler.zig");
const metadata_placement_planner = @import("metadata/placement_planner.zig");
const metadata_table_manager = @import("metadata/table_manager.zig");
const metadata_catalog_jobs = @import("metadata/catalog/jobs.zig");
const metadata_catalog_routing = @import("metadata/catalog/routing.zig");
const table_writes_integrity = @import("api/table_writes/integrity.zig");
const table_writes_schema_jobs = @import("api/table_writes/schema_jobs.zig");

test {
    _ = http_server;
    _ = public_sql_endpoint_parity;
    _ = api_distributed_txn;
    _ = pgwire;
    _ = sql_adapter_integration;
    _ = relational_rows;
    _ = sql_lower_dml;
    _ = sql_lower_expr;
    _ = table_reads;
    _ = table_reads_relational_rows;
    _ = db_relational_integrity;
    _ = db_relational_store;
    _ = db_transactions;
    _ = db_types;
    _ = metadata_http_server;
    _ = metadata_reconciler;
    _ = metadata_placement_planner;
    _ = metadata_table_manager;
    _ = metadata_catalog_jobs;
    _ = metadata_catalog_routing;
    _ = table_writes_integrity;
    _ = table_writes_schema_jobs;
}
