// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Strict single-owner SQL catalog evidence. Commands use production admission
//! and committed Raft apply; this fixture does not simulate quorum/readiness.
const std = @import("std");
const local = @import("antfly_local_sources");
const domain = local.system_catalog_domain;
const operation = local.api_operation;
const storage = @import("../metadata/storage/raft_apply_store.zig");
const tables = @import("../metadata/table_manager.zig");
const server_mod = @import("http_server.zig");
const handler_mod = @import("httpx_handler.zig");
const httpx = @import("httpx");
const wire = @import("antfly_metadata_openapi").types;
const A = std.mem.Allocator;

const Case = struct {
    id: []const u8,
    setup: []const []const u8,
    kind: domain.Kind,
    name: []const u8,
    prior_name: ?[]const u8 = null,
    present: bool,
    command_tag: []const u8,
    unchanged: bool = false,
    repeat_noop: bool = false,
};

const Owner = struct {
    alloc: A,
    root: []const u8,
    store: storage.RaftApplyStore,
    open: bool = true,
    metadata_group_id: u64 = 21,
    next_index: u64 = 1,
    calls: usize = 0,
    locked: bool = false,
    lose_reply_once: bool = false,

    fn init(a: A, root: []const u8) !Owner {
        return .{ .alloc = a, .root = root, .store = try storage.RaftApplyStore.init(a, .{ .root_dir = root }) };
    }
    fn deinit(self: *Owner) void {
        if (self.open) self.store.deinit();
    }
    fn reopen(self: *Owner) !void {
        self.store.deinit();
        self.open = false;
        self.store = try storage.RaftApplyStore.init(self.alloc, .{ .root_dir = self.root });
        self.open = true;
    }
    fn source(self: *Owner) server_mod.StatusSource {
        return .{ .ptr = self, .vtable = &.{ .status = status, .system_catalog = run, .supports_query_definitions = true } };
    }
    fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
        return .{ .metadata_group_id = 21, .metrics = .{} };
    }
    pub fn ensureTableTopologyProtocolReadyWithContext(_: *Owner, _: operation.RequestContext, _: u16) !void {}
    pub fn validateTableTopologyProtocolReadinessWithContext(_: *Owner, _: operation.RequestContext, _: void) !void {}
    pub fn ensureLinearizableReadWithContext(_: *Owner, context: operation.RequestContext) !void {
        try context.ensureActive();
    }
    pub fn lockCatalogMutation(self: *Owner) void {
        std.debug.assert(!self.locked);
        self.locked = true;
    }
    pub fn unlockCatalogMutation(self: *Owner) void {
        std.debug.assert(self.locked);
        self.locked = false;
    }
    pub fn projectedStore(self: *Owner) ?*storage.RaftApplyStore {
        return &self.store;
    }
    pub fn captureTableCreateGeneration(self: *Owner, a: A, id: u64) !u64 {
        try self.store.ensureDerivedCatalogIndexes(self.metadata_group_id);
        return self.store.captureTableCreateGeneration(a, self.metadata_group_id, id);
    }
    pub fn captureTableDropAdmission(self: *Owner, a: A, name: []const u8) !@import("../metadata/service.zig").TableDropAdmission {
        try self.store.ensureDerivedCatalogIndexes(self.metadata_group_id);
        var projection = (try self.store.captureTableDropProjection(a, self.metadata_group_id, name)) orelse return error.TableNotFound;
        defer projection.deinit(a);
        if (projection.fence.active()) return error.TableTransitionActive;
        const expected_name = try a.dupe(u8, projection.table.name);
        const ids = projection.range_group_ids;
        projection.range_group_ids = &.{};
        return .{ .table_id = projection.table.table_id, .expected_name = expected_name, .expected_transition_generation = projection.fence.generation, .range_membership = projection.fence.membership(projection.table.table_id), .range_group_ids = ids };
    }
    pub fn proposeTransitionCommandWithReceipt(self: *Owner, command: storage.TransitionCommand) !u64 {
        std.debug.assert(self.locked);
        const bytes = try storage.encodeTransitionCommand(self.alloc, command);
        defer self.alloc.free(bytes);
        const entries = try @import("../raft/state_machine/mod.zig").encodeCommittedEntries(self.alloc, &.{.{ .term = 1, .index = self.next_index, .entry_type = .normal, .data = bytes }});
        defer self.alloc.free(entries);
        try self.store.snapshotBuilder().applyBatch(.{ .group_id = self.metadata_group_id, .commit_index = self.next_index, .entries_bytes = entries });
        const receipt = self.next_index;
        self.next_index += 1;
        return receipt;
    }
    pub fn waitForTransitionAppliedWithContext(_: *Owner, _: u64, context: operation.RequestContext) !void {
        try context.ensureActive();
    }
    pub fn verifyTableCreateProjection(self: *Owner, a: A, table: tables.TableRecord, ranges: []const tables.RangeRecord) !void {
        try self.store.verifyTableCreateProjectionExact(a, self.metadata_group_id, table, ranges);
    }
    pub fn verifyTableDropProjection(self: *Owner, a: A, id: u64) !void {
        if (try self.store.getTable(a, self.metadata_group_id, id)) |table| {
            tables.freeTable(a, table);
            return error.TableTransitionActive;
        }
    }
    fn run(raw: *anyopaque, a: A, context: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
        const self: *Owner = @ptrCast(@alignCast(raw));
        self.calls += 1;
        const result = switch (call) {
            .mutate => |mutation| try @import("../system_catalog/operations.zig").mutate(self, a, context, mutation),
            .resolve_many => |resolution| blk: {
                const found = try self.store.resolveSystemCatalogIdentities(a, self.metadata_group_id, resolution);
                defer found.deinit(a);
                break :blk try std.json.Stringify.valueAlloc(a, found, .{});
            },
            .snapshot => try @import("../system_catalog/operations.zig").snapshotJson(self, a, context),
            else => return error.UnexpectedCatalogCall,
        };
        if (call == .mutate and self.lose_reply_once) {
            self.lose_reply_once = false;
            a.free(result);
            return error.MetadataMutationOutcomeUnknown;
        }
        return result;
    }
};

fn request(a: A, handler: *handler_mod.AntflyApiHandler, sql: []const u8) !httpx.Response {
    var input = try httpx.Request.init(a, .POST, "http://127.0.0.1/db/v1/sql");
    defer input.deinit();
    const body = try std.json.Stringify.valueAlloc(a, .{ .statement = sql }, .{});
    defer a.free(body);
    input.body = body;
    var context = httpx.Context.init(a, std.testing.io, &input);
    defer context.deinit();
    return handler.executeSQL(&context);
}

fn execute(a: A, handler: *handler_mod.AntflyApiHandler, sql: []const u8, tag: ?[]const u8) !void {
    var response = try request(a, handler, sql);
    defer response.deinit();
    if (response.status.code != 200) std.debug.print("SQL catalog campaign failure: {s}: {s}\n", .{ sql, response.body orelse "" });
    try std.testing.expectEqual(@as(u16, 200), response.status.code);
    const result = try std.json.parseFromSlice(wire.SQLResponse, a, response.body.?, .{});
    defer result.deinit();
    if (tag) |expected| try std.testing.expectEqualStrings(expected, result.value.command_tag);
    try std.testing.expectEqual(wire.SQLMutationOutcome.committed, result.value.mutation_outcome.?);
    try std.testing.expectEqual(@as(i64, 0), result.value.rows_affected);
    try std.testing.expectEqual(@as(usize, 0), result.value.rows.len);
    try std.testing.expectEqual(@as(usize, 0), result.value.columns.len);
}

fn parent(kind: domain.Kind) u64 {
    return switch (kind) {
        .database, .tablespace => 0,
        .namespace => domain.default_database_id,
        .table => domain.default_namespace_id,
    };
}

fn verify(a: A, owner: *Owner, case: Case, prior_id: ?u64) !void {
    var snapshot = try owner.store.systemCatalogSnapshot(a, owner.metadata_group_id);
    defer snapshot.deinit();
    const resource = snapshot.value.find(case.kind, parent(case.kind), case.name);
    try std.testing.expectEqual(case.present, resource != null);
    if (case.prior_name) |old| {
        try std.testing.expect(snapshot.value.find(case.kind, parent(case.kind), old) == null);
        try std.testing.expectEqual(prior_id.?, resource.?.id);
    }
    if (case.kind == .database and case.present) {
        try std.testing.expect(snapshot.value.find(.namespace, resource.?.id, "public") != null);
    }
    const physical = try owner.store.listTables(a, owner.metadata_group_id);
    defer owner.store.freeTables(a, physical);
    const ranges = try owner.store.listRanges(a, owner.metadata_group_id);
    defer owner.store.freeRanges(a, ranges);
    const count: usize = @intFromBool(case.kind == .table and case.present);
    try std.testing.expectEqual(count, physical.len);
    try std.testing.expectEqual(count, ranges.len);
    if (count != 0) {
        try std.testing.expectEqual(resource.?.id, physical[0].table_id);
        try std.testing.expectEqualStrings(resource.?.storage_name, physical[0].name);
        try std.testing.expectEqual(physical[0].table_id, ranges[0].table_id);
        var parsed = try local.schema_mod.parseValidatedTableSchema(a, physical[0].schema_json);
        defer parsed.deinit(a);
        const native = try local.schema_mod.deriveRuntimeTableSchema(a, parsed);
        defer local.storage_schema.freeSchema(a, native);
        try std.testing.expectEqual(local.storage_schema.StorageMode.relational, native.storage_mode);
        try std.testing.expectEqual(@as(usize, 3), native.relational_columns.len);
        for ([_][]const u8{ "tenant_id", "id", "status" }) |name| {
            var found = false;
            for (native.relational_columns) |column| {
                if (!std.mem.eql(u8, column.name, name)) continue;
                found = true;
                try std.testing.expectEqual(local.storage_schema.RelationalColumnType.string, column.column_type);
                try std.testing.expectEqual(std.mem.eql(u8, name, "status"), column.allows_null);
                try std.testing.expectEqual(!std.mem.eql(u8, name, "status"), column.required);
            }
            try std.testing.expect(found);
        }
    }
}

fn verifyDataOwner(a: A, owner: *Owner, root: []const u8) !void {
    const published_tables = try owner.store.listTables(a, owner.metadata_group_id);
    defer owner.store.freeTables(a, published_tables);
    const published_ranges = try owner.store.listRanges(a, owner.metadata_group_id);
    defer owner.store.freeRanges(a, published_ranges);
    const provisioner = @import("../metadata/table_provisioner.zig");
    const summary = try provisioner.reconcileReplicaRoot(a, root, owner.metadata_group_id, &.{ owner.metadata_group_id, published_ranges[0].group_id }, published_tables, published_ranges);
    try std.testing.expectEqual(@as(usize, 1), summary.dbs_opened);
    const path = try provisioner.groupDbPathFromReplicaRoot(a, root, published_ranges[0].group_id);
    defer a.free(path);
    {
        var db = try local.storage_db_selected_root.db.DB.open(a, path, .{});
        defer db.close();
        try db.batch(.{ .writes = &.{.{ .key = "doc:valid", .value = "{\"tenant_id\":\"acme\",\"id\":\"550E8400E29B41D4A716446655440000\",\"status\":null}" }}, .sync_level = .write });
        // Admission must enforce the original NOT NULL after restoration.
        try std.testing.expectError(error.InvalidBatchRequest, db.batch(.{ .writes = &.{.{ .key = "doc:invalid", .value = "{\"tenant_id\":null,\"id\":\"550e8400-e29b-41d4-a716-446655440000\"}" }}, .sync_level = .write }));
    }
    {
        var db = try local.storage_db_selected_root.db.DB.open(a, path, .{});
        defer db.close();
        const row = (try db.get(a, "doc:valid")) orelse return error.MissingSqlRow;
        defer a.free(row);
        const value = try std.json.parseFromSlice(std.json.Value, a, row, .{});
        defer value.deinit();
        try std.testing.expectEqualStrings("acme", value.value.object.get("tenant_id").?.string);
        try std.testing.expectEqualStrings("550e8400-e29b-41d4-a716-446655440000", value.value.object.get("id").?.string);
        try std.testing.expect(value.value.object.get("status").? == .null);
        const invalid = try db.get(a, "doc:invalid");
        defer if (invalid) |bytes| a.free(bytes);
        try std.testing.expect(invalid == null);
    }
}

test "httpx SQL catalog originals commit through native authority and recover topology" {
    // Strict source IDs; a case cannot vanish silently from the campaign.
    const ids = [_][]const u8{ "sql-0095", "sql-0101", "sql-0102", "sql-0103", "sql-0104", "sql-0106", "sql-0108", "sql-0157", "sql-0159", "sql-0676" };
    const a = std.testing.allocator;
    const profile = try std.json.parseFromSlice(struct { format: u8, entries: []const Case }, a, local.sql_parity_fixtures.catalog_campaign, .{});
    defer profile.deinit();
    try std.testing.expectEqual(@as(u8, 1), profile.value.format);
    try std.testing.expectEqual(ids.len, profile.value.entries.len);
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded });
    defer runtime.deinit();
    for (profile.value.entries, ids) |case, id| {
        try std.testing.expectEqualStrings(id, case.id);
        const original = try corpus.get(id);
        try std.testing.expectEqual(@as(usize, 0), original.params.len);
        var directory = try local.common_test_directory.TestDirectory.init("sql-catalog-campaign");
        defer directory.cleanup();
        var owner = try Owner.init(a, directory.path());
        defer owner.deinit();
        var server = server_mod.ApiHttpServer.init(a, .{ .backend_runtime = runtime.ptr() }, owner.source(), null, null);
        defer server.deinit();
        var handler: handler_mod.AntflyApiHandler = .{ .api_server = &server };
        for (case.setup) |sql| try execute(a, &handler, sql, null);
        var before = try owner.store.systemCatalogSnapshot(a, owner.metadata_group_id);
        defer before.deinit();
        const prior_id = if (case.prior_name) |old| before.value.find(case.kind, parent(case.kind), old).?.id else null;
        const before_index = owner.next_index;
        try execute(a, &handler, original.sql, case.command_tag);
        try std.testing.expectEqual(before_index + @intFromBool(!case.unchanged), owner.next_index);
        try verify(a, &owner, case, prior_id);
        if (case.repeat_noop) {
            const index = owner.next_index;
            try execute(a, &handler, original.sql, case.command_tag);
            try std.testing.expectEqual(index, owner.next_index);
        }
        var after = try owner.store.systemCatalogSnapshot(a, owner.metadata_group_id);
        defer after.deinit();
        const committed = try std.json.Stringify.valueAlloc(a, after.value, .{});
        defer a.free(committed);
        if (case.unchanged) {
            const unchanged = try std.json.Stringify.valueAlloc(a, before.value, .{});
            defer a.free(unchanged);
            try std.testing.expectEqualStrings(unchanged, committed);
        }
        try owner.reopen();
        try verify(a, &owner, case, prior_id);
        var recovered = try owner.store.systemCatalogSnapshot(a, owner.metadata_group_id);
        defer recovered.deinit();
        const recovered_bytes = try std.json.Stringify.valueAlloc(a, recovered.value, .{});
        defer a.free(recovered_bytes);
        try std.testing.expectEqualStrings(committed, recovered_bytes);
        if (owner.next_index > 1) {
            const bytes = try owner.store.snapshotBuilder().buildSnapshot(a, owner.metadata_group_id);
            defer a.free(bytes);
            const root = try std.fmt.allocPrint(a, "{s}/snapshot", .{directory.path()});
            defer a.free(root);
            var restored = try Owner.init(a, root);
            defer restored.deinit();
            try std.testing.expect(try restored.store.snapshotBuilder().installSnapshot(a, owner.metadata_group_id, owner.next_index - 1, bytes));
            try verify(a, &restored, case, prior_id);
            if (case.kind == .table and case.present) {
                const replica_root = try std.fmt.allocPrint(a, "{s}/data-owner", .{directory.path()});
                defer a.free(replica_root);
                try verifyDataOwner(a, &restored, replica_root);
            }
        }
    }
}

test "httpx SQL catalog diagnostics preserve authority and uncertain commits are not replayed" {
    const a = std.testing.allocator;
    var runtime = try local.storage_background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded });
    defer runtime.deinit();
    const Failure = struct { setup: []const []const u8, sql: []const u8, code: []const u8 };
    const failures = [_]Failure{
        .{ .setup = &.{"CREATE SCHEMA tenant_ops"}, .sql = "CREATE SCHEMA tenant_ops", .code = "42P06" },
        .{ .setup = &.{"CREATE DATABASE tenant_ops"}, .sql = "CREATE DATABASE tenant_ops", .code = "42P04" },
        .{ .setup = &.{"CREATE TABLE usage_records (id uuid, status text)"}, .sql = "CREATE TABLE usage_records (id uuid, status text)", .code = "42P07" },
        .{ .setup = &.{}, .sql = "DROP SCHEMA tenant_ops", .code = "3F000" },
        .{ .setup = &.{}, .sql = "DROP DATABASE tenant_ops", .code = "3D000" },
        .{ .setup = &.{}, .sql = "DROP TABLE usage_records", .code = "42P01" },
        .{ .setup = &.{ "CREATE SCHEMA tenant_ops", "CREATE TABLE tenant_ops.child (id bigint)" }, .sql = "DROP SCHEMA tenant_ops", .code = "2BP01" },
    };
    for (failures) |failure| {
        var directory = try local.common_test_directory.TestDirectory.init("sql-catalog-diagnostic");
        defer directory.cleanup();
        var owner = try Owner.init(a, directory.path());
        defer owner.deinit();
        var server = server_mod.ApiHttpServer.init(a, .{ .backend_runtime = runtime.ptr() }, owner.source(), null, null);
        defer server.deinit();
        var handler: handler_mod.AntflyApiHandler = .{ .api_server = &server };
        for (failure.setup) |sql| try execute(a, &handler, sql, null);
        var before = try owner.store.systemCatalogSnapshot(a, owner.metadata_group_id);
        defer before.deinit();
        const before_bytes = try std.json.Stringify.valueAlloc(a, before.value, .{});
        defer a.free(before_bytes);
        const index = owner.next_index;
        var response = try request(a, &handler, failure.sql);
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 400), response.status.code);
        const diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, a, response.body.?, .{});
        defer diagnostic.deinit();
        if (!std.mem.eql(u8, failure.code, diagnostic.value.code) or diagnostic.value.retryable != false)
            std.debug.print("SQL catalog diagnostic mismatch: {s}: {s}\n", .{ failure.sql, response.body.? });
        try std.testing.expectEqualStrings(failure.code, diagnostic.value.code);
        try std.testing.expectEqual(@as(?bool, false), diagnostic.value.retryable);
        try std.testing.expect(!owner.locked);
        try std.testing.expectEqual(index, owner.next_index);
        try owner.reopen();
        var after = try owner.store.systemCatalogSnapshot(a, owner.metadata_group_id);
        defer after.deinit();
        const after_bytes = try std.json.Stringify.valueAlloc(a, after.value, .{});
        defer a.free(after_bytes);
        try std.testing.expectEqualStrings(before_bytes, after_bytes);
    }
    var directory = try local.common_test_directory.TestDirectory.init("sql-catalog-uncertain");
    defer directory.cleanup();
    var owner = try Owner.init(a, directory.path());
    defer owner.deinit();
    var server = server_mod.ApiHttpServer.init(a, .{ .backend_runtime = runtime.ptr() }, owner.source(), null, null);
    defer server.deinit();
    var handler: handler_mod.AntflyApiHandler = .{ .api_server = &server };
    owner.lose_reply_once = true;
    var response = try request(a, &handler, "CREATE SCHEMA tenant_ops");
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 409), response.status.code);
    const diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, a, response.body.?, .{});
    defer diagnostic.deinit();
    try std.testing.expectEqualStrings("40003", diagnostic.value.code);
    try std.testing.expectEqual(@as(?bool, false), diagnostic.value.retryable);
    try std.testing.expectEqual(@as(u64, 2), owner.next_index);
    try std.testing.expect(!owner.locked);
    try owner.reopen();
    var committed = try owner.store.systemCatalogSnapshot(a, owner.metadata_group_id);
    defer committed.deinit();
    try std.testing.expect(committed.value.find(.namespace, domain.default_database_id, "tenant_ops") != null);
    try std.testing.expectEqual(@as(u64, 2), owner.next_index);
}
