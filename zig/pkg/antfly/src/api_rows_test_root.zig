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
const table_writes_sources = @import("api/table_writes/sources.zig");
const metadata_api = @import("metadata/api.zig");
const metadata_transition_state = @import("metadata/transition_state.zig");
const runtime_schema = @import("storage/schema.zig");
const db_mod = @import("storage/db/mod.zig");
const raft_mod = @import("raft/mod.zig");
const raft_reconciler = @import("raft/reconciler.zig");
const table_reads_core = @import("api/table_reads/core.zig");
const table_writes_core = @import("api/table_writes/core.zig");
const table_router = @import("api/table_router.zig");
const distributed_txn = @import("api/distributed_txn.zig");
const http_common = @import("raft/transport/http_common.zig");
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
    _ = table_writes_sources;
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

test "api rows insert-source upsert fails closed after conflict owner moves at commit" {
    const alloc = std.testing.allocator;
    const conflict_key = "\x00antfly-rel-pk:existing-row";

    var owner_movement = struct {
        table: metadata_table_manager.TableRecord = .{
            .table_id = 41,
            .name = "usage_records",
            .schema_json =
            \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"source_id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id","source_id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"rows_source_id_key","columns":["source_id"]}]}
            ,
        },
        rows_query_calls: usize = 0,
        lookup_calls: usize = 0,
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
            ptr: *anyopaque,
            lookup_alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?table_reads_core.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("usage_records", table_name);
            try std.testing.expectEqualStrings(conflict_key, key);
            self.lookup_calls += 1;
            return .{
                .json = try lookup_alloc.dupe(u8, "{\"id\":\"existing-row\",\"source_id\":\"source-1\",\"status\":\"old\"}"),
                .version = 44,
            };
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
            lookup_alloc: std.mem.Allocator,
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
            return try lookup_alloc.dupe(u8, conflict_key);
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
            tables: []const distributed_txn.TableCommitRequest,
            _: db_mod.types.SyncLevel,
        ) !?distributed_txn.CommitOutcome {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.commit_calls += 1;
            try std.testing.expectEqual(@as(usize, 1), tables.len);
            try std.testing.expectEqualStrings("usage_records", tables[0].table_name);
            try std.testing.expectEqual(@as(usize, 0), tables[0].writes.len);
            try std.testing.expectEqual(@as(usize, 0), tables[0].deletes.len);
            try std.testing.expectEqual(@as(usize, 1), tables[0].transforms.len);
            try std.testing.expectEqualStrings(conflict_key, tables[0].transforms[0].key);
            try std.testing.expectEqual(@as(usize, 1), tables[0].transforms[0].operations.len);
            try std.testing.expectEqual(db_mod.types.TransformOpType.set, tables[0].transforms[0].operations[0].op);
            try std.testing.expectEqualStrings("status", tables[0].transforms[0].operations[0].path);
            try std.testing.expectEqualStrings("\"conflicted\"", tables[0].transforms[0].operations[0].value_json.?);
            try std.testing.expectEqual(@as(usize, 1), tables[0].predicates.len);
            try std.testing.expectEqualStrings(conflict_key, tables[0].predicates[0].key);
            try std.testing.expectEqual(@as(u64, 44), tables[0].predicates[0].expected_version);
            return error.TopologyChanged;
        }
    }{};

    var server = http_server.ApiHttpServer.init(alloc, .{}, owner_movement.statusSource(), owner_movement.readSource(), owner_movement.writeSource());
    defer server.deinit();

    var resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/usage_records/rows/mutation-source",
        .content_type = "application/json",
        .body = "{\"op\":\"insert\",\"source\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"}},\"assignments\":[{\"target_field\":\"id\",\"expr\":{\"field\":\"id\"}},{\"target_field\":\"source_id\",\"expr\":{\"field\":\"source_id\"}},{\"target_field\":\"status\",\"expr\":{\"field\":\"status\"}}],\"on_conflict\":{\"target\":{\"unique\":{\"name\":\"rows_source_id_key\"}},\"action\":\"update\",\"patch\":{\"status\":\"conflicted\"},\"where_expressions\":[{\"lhs\":{\"field\":\"status\",\"source\":\"existing\"},\"op\":\"eq\",\"rhs\":{\"value\":\"old\"}},{\"lhs\":{\"field\":\"status\",\"source\":\"proposed\"},\"op\":\"eq\",\"rhs\":{\"value\":\"ready\"}}]},\"returning\":[\"id\",\"status\"]}",
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 503), resp.status);
    try std.testing.expectEqualStrings("topology changed", resp.body);
    try std.testing.expectEqual(@as(usize, 1), owner_movement.rows_query_calls);
    try std.testing.expectEqual(@as(usize, 1), owner_movement.unique_owner_calls);
    try std.testing.expectEqual(@as(usize, 1), owner_movement.lookup_calls);
    try std.testing.expectEqual(@as(usize, 1), owner_movement.commit_calls);
    try std.testing.expectEqual(@as(usize, 0), owner_movement.batch_calls);
}

test "api rows hosted insert-source upsert fails closed when conflict owner moves during prepare" {
    const alloc = std.testing.allocator;
    const conflict_key = "\x00antfly-rel-pk:existing-row";
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"source_id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id","source_id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"rows_source_id_key","columns":["source_id"]}]}
    ;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/api-hosted-insert-source-owner-prepare", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    const Catalog = struct {
        table: metadata_table_manager.TableRecord = .{
            .table_id = 41,
            .name = "usage_records",
            .schema_json = schema_json,
        },

        fn status(_: *@This()) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @as([*]metadata_table_manager.TableRecord, @ptrCast(&self.table))[0..1],
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 41, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
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

        fn catalogSource(self: *@This()) metadata_catalog_routing.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }
    };

    const ReadSource = struct {
        rows_query_calls: usize = 0,
        lookup_calls: usize = 0,
        unique_owner_calls: usize = 0,

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
            ptr: *anyopaque,
            lookup_alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?table_reads_core.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("usage_records", table_name);
            try std.testing.expectEqualStrings(conflict_key, key);
            self.lookup_calls += 1;
            return .{
                .json = try lookup_alloc.dupe(u8, "{\"id\":\"existing-row\",\"source_id\":\"source-1\",\"status\":\"old\"}"),
                .version = 44,
            };
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
            lookup_alloc: std.mem.Allocator,
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
            return try lookup_alloc.dupe(u8, conflict_key);
        }
    };

    const RemoteRouter = struct {
        fn iface() table_router.HostedGroupRouter {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .local_node_id = localNodeId,
                    .local_status = localStatus,
                    .group_leader_node_id = groupLeaderNodeId,
                    .node_status = nodeStatus,
                    .node_base_uri = nodeBaseUri,
                },
            };
        }

        fn localNodeId(_: *anyopaque) u64 {
            return 1;
        }

        fn localStatus(_: *anyopaque, _: u64) raft_mod.HostedReplicaStatus {
            return .absent;
        }

        fn groupLeaderNodeId(_: *anyopaque, group_id: u64) ?u64 {
            return if (group_id == 7001) 2 else null;
        }

        fn nodeStatus(_: *anyopaque, node_id: u64, group_id: u64) raft_mod.HostedReplicaStatus {
            return if (node_id == 2 and group_id == 7001) .active else .absent;
        }

        fn nodeBaseUri(_: *anyopaque, alloc_inner: std.mem.Allocator, node_id: u64) !?[]u8 {
            if (node_id != 2) return null;
            return try alloc_inner.dupe(u8, "http://node2.test:1");
        }
    };

    const RemoteExecutor = struct {
        txn_begin_calls: usize = 0,
        txn_prepare_calls: usize = 0,
        txn_resolve_calls: usize = 0,

        fn iface(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, alloc_inner: std.mem.Allocator, request: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(http_common.Method.POST, request.method);
            try std.testing.expectEqualStrings("application/json", request.content_type.?);
            try std.testing.expect(std.mem.startsWith(u8, request.uri, "http://node2.test:1"));
            try std.testing.expect(std.mem.indexOf(u8, request.uri, "/groups/7001/") != null);
            if (std.mem.endsWith(u8, request.uri, "/txn-begin")) {
                self.txn_begin_calls += 1;
                var parsed = try distributed_txn.parseTxnBeginRequest(alloc_inner, request.body);
                defer distributed_txn.freeTxnBeginRequest(alloc_inner, &parsed);
                try std.testing.expect(parsed.participants.len >= 1);
                return .{ .status = 200, .body = try alloc_inner.dupe(u8, "") };
            }
            if (std.mem.endsWith(u8, request.uri, "/txn-prepare")) {
                self.txn_prepare_calls += 1;
                var parsed = try distributed_txn.parseTxnPrepareRequest(alloc_inner, request.body);
                defer distributed_txn.freeTxnPrepareRequest(alloc_inner, &parsed);
                try std.testing.expectEqual(@as(usize, 1), parsed.req.transforms.len);
                try std.testing.expectEqualStrings(conflict_key, parsed.req.transforms[0].key);
                try std.testing.expectEqual(@as(usize, 1), parsed.req.predicates.len);
                try std.testing.expectEqualStrings(conflict_key, parsed.req.predicates[0].key);
                try std.testing.expectEqual(@as(u64, 44), parsed.req.predicates[0].expected_version);
                return .{ .status = 409, .body = try alloc_inner.dupe(u8, "TopologyChanged") };
            }
            if (std.mem.endsWith(u8, request.uri, "/txn-resolve")) {
                self.txn_resolve_calls += 1;
                const parsed = try distributed_txn.parseTxnResolveRequest(alloc_inner, request.body);
                try std.testing.expectEqual(db_mod.types.TxnStatus.aborted, parsed.status);
                return .{ .status = 200, .body = try alloc_inner.dupe(u8, "") };
            }
            return error.TestUnexpectedResult;
        }
    };

    var catalog = Catalog{};
    var reads = ReadSource{};
    var remote = RemoteExecutor{};
    var hosted_writes = table_writes_sources.HostedProvisionedTableWriteSource.init(replica_root_dir, catalog.catalogSource(), RemoteRouter.iface(), remote.iface());
    defer hosted_writes.invalidateManagedCache("usage_records");

    var server = http_server.ApiHttpServer.init(alloc, .{}, catalog.statusSource(), reads.readSource(), hosted_writes.source());
    defer server.deinit();

    var resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/usage_records/rows/mutation-source",
        .content_type = "application/json",
        .body = "{\"op\":\"insert\",\"source\":{\"where\":{\"field\":\"status\",\"op\":\"eq\",\"value\":\"ready\"}},\"assignments\":[{\"target_field\":\"id\",\"expr\":{\"field\":\"id\"}},{\"target_field\":\"source_id\",\"expr\":{\"field\":\"source_id\"}},{\"target_field\":\"status\",\"expr\":{\"field\":\"status\"}}],\"on_conflict\":{\"target\":{\"unique\":{\"name\":\"rows_source_id_key\"}},\"action\":\"update\",\"patch\":{\"status\":\"conflicted\"},\"where_expressions\":[{\"lhs\":{\"field\":\"status\",\"source\":\"existing\"},\"op\":\"eq\",\"rhs\":{\"value\":\"old\"}},{\"lhs\":{\"field\":\"status\",\"source\":\"proposed\"},\"op\":\"eq\",\"rhs\":{\"value\":\"ready\"}}]},\"returning\":[\"id\",\"status\"]}",
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 503), resp.status);
    try std.testing.expectEqualStrings("topology changed", resp.body);
    try std.testing.expectEqual(@as(usize, 1), reads.rows_query_calls);
    try std.testing.expectEqual(@as(usize, 1), reads.unique_owner_calls);
    try std.testing.expectEqual(@as(usize, 1), reads.lookup_calls);
    try std.testing.expect(remote.txn_begin_calls >= 1);
    try std.testing.expect(remote.txn_prepare_calls >= 1);
}
