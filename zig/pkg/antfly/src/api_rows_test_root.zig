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

const std = @import("std");
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
const metadata_api = @import("metadata/api.zig");
const runtime_schema = @import("storage/schema.zig");
const db_mod = @import("storage/db/mod.zig");
const raft_mod = @import("raft/mod.zig");
const table_reads_core = @import("api/table_reads/core.zig");
const table_writes_core = @import("api/table_writes/core.zig");
const distributed_txn = @import("api/distributed_txn.zig");
const query_api = @import("api/query.zig");

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

test "api rows insert-source upsert aborts before commit when unique owner topology moves" {
    const alloc = std.testing.allocator;

    var owner_movement = struct {
        table: metadata_table_manager.TableRecord = .{
            .table_id = 41,
            .name = "usage_records",
            .schema_json =
            \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"source_id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id","source_id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"rows_source_id_key","columns":["source_id"]}]}
            ,
        },
        rows_query_calls: usize = 0,
        unique_owner_calls: usize = 0,
        commit_calls: usize = 0,
        batch_calls: usize = 0,

        fn status(_: *@This()) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @as([*]metadata_table_manager.TableRecord, @ptrCast(&self.table))[0..1],
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn statusSource(self: *@This()) http_server.StatusSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = statusErased,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn statusErased(ptr: *anyopaque) !metadata_api.MetadataStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.status();
        }

        fn readSource(self: *@This()) table_reads_core.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .rows_query_plan = rowsQueryPlan,
                    .relational_unique_owner_lookup = relationalUniqueOwnerLookup,
                },
            };
        }

        fn lookup(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?table_reads_core.LookupResponse {
            return error.TestUnexpectedResult;
        }

        fn scan(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.ScanOptions,
            _: raft_mod.ReadConsistency,
        ) !?table_reads_core.ScanResponse {
            return error.TestUnexpectedResult;
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.SearchRequest,
            _: raft_mod.ReadConsistency,
        ) !?query_api.QueryResponse {
            return error.TestUnexpectedResult;
        }

        fn rowsQueryPlan(
            ptr: *anyopaque,
            alloc_arg: std.mem.Allocator,
            table_name: []const u8,
            _: runtime_schema.TableSchema,
            _: db_mod.types.RelationalRowsQueryPlan,
            _: raft_mod.ReadConsistency,
        ) !?db_mod.types.RelationalRowsQueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("usage_records", table_name);
            self.rows_query_calls += 1;
            const rows = try alloc_arg.alloc([]const u8, 1);
            errdefer alloc_arg.free(rows);
            rows[0] = try alloc_arg.dupe(u8, "{\"id\":\"new-row\",\"source_id\":\"source-1\",\"status\":\"ready\"}");
            return .{ .rows = rows, .total = 1 };
        }

        fn relationalUniqueOwnerLookup(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            table_name: []const u8,
            constraint_name: []const u8,
            encoded_value: []const u8,
            _: raft_mod.ReadConsistency,
        ) !?[]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("usage_records", table_name);
            try std.testing.expectEqualStrings("rows_source_id_key", constraint_name);
            try std.testing.expect(encoded_value.len > 0);
            self.unique_owner_calls += 1;
            return error.UniqueOwnerTopologyUnavailable;
        }

        fn writeSource(self: *@This()) table_writes_core.TableWriteSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .batch = batch,
                    .commit_transaction = commitTransaction,
                },
            };
        }

        fn batch(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.BatchRequest,
        ) !?void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.batch_calls += 1;
            return error.TestUnexpectedResult;
        }

        fn commitTransaction(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: []const distributed_txn.TableCommitRequest,
            _: db_mod.types.SyncLevel,
        ) !?distributed_txn.CommitOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.commit_calls += 1;
            return error.TestUnexpectedResult;
        }
    }{};

    var server = http_server.ApiHttpServer.init(alloc, .{}, owner_movement.statusSource(), owner_movement.readSource(), owner_movement.writeSource());
    defer server.deinit();

    var resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/usage_records/rows/mutation-source",
        .content_type = "application/json",
        .body = "{\"op\":\"insert\",\"source\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"}},\"assignments\":[{\"target_field\":\"id\",\"expr\":{\"field\":\"id\"}},{\"target_field\":\"source_id\",\"expr\":{\"field\":\"source_id\"}},{\"target_field\":\"status\",\"expr\":{\"field\":\"status\"}}],\"on_conflict\":{\"target\":{\"unique\":{\"name\":\"rows_source_id_key\"}},\"action\":\"update\",\"patch\":{\"status\":\"conflicted\"}},\"returning\":[\"id\",\"status\"]}",
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 503), resp.status);
    try std.testing.expectEqualStrings("unique owner unavailable", resp.body);
    try std.testing.expectEqual(@as(usize, 1), owner_movement.rows_query_calls);
    try std.testing.expectEqual(@as(usize, 1), owner_movement.unique_owner_calls);
    try std.testing.expectEqual(@as(usize, 0), owner_movement.commit_calls);
    try std.testing.expectEqual(@as(usize, 0), owner_movement.batch_calls);
}
