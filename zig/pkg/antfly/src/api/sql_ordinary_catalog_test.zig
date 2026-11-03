// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const local = @import("antfly_local_sources");
const http = @import("http_server.zig");
const native = @import("../system_catalog/catalog_mutations.zig");
const ordinary = @import("sql_ordinary_catalog.zig");
const catalog = local.sql_catalog;
const domain = local.system_catalog_domain;
const storage = @import("../metadata/storage/raft_apply_store.zig");
const L = @import("../system_catalog/lock_authority.zig");
const Context = local.api_operation.RequestContext;
const A = std.mem.Allocator;
const group = local.common_group_ids.main_metadata_group_id;
const a = std.testing.allocator;
fn id(value: u64) [16]u8 {
    var bytes: [16]u8 = @splat(0);
    std.mem.writeInt(u64, bytes[8..16], value, .big);
    return bytes;
}
const Source = struct {
    alloc: A = a,
    store: *storage.RaftApplyStore,
    metadata_group_id: u64 = group,
    replicated: bool = false,
    index: u64 = 100,
    attempts: usize = 0,
    applied: usize = 0,
    prepares: usize = 0,
    observations: usize = 0,
    locked: bool = false,
    lose: enum { none, before, after } = .none,
    hide: bool = false,
    fail_result_after_apply: ?*std.testing.FailingAllocator = null,
    forge_relation_placement: bool = false,
    fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
        return .{ .metadata_group_id = group, .metrics = .{} };
    }
    pub fn ensureTableTopologyProtocolReadyWithContext(self: *@This(), _: Context, required: u16) !@import("../metadata/service.zig").TableTopologyProtocolReadiness {
        const active = (try self.store.topologyActivation(group)).?;
        try std.testing.expect(active.version >= required);
        return .{ .term = 1, .required_version = required, .metadata_incarnation = active.incarnation, .protected_member_count = active.member_count, .protected_membership_fingerprint = active.membership_fingerprint };
    }
    pub fn validateTableTopologyProtocolReadinessWithContext(_: *@This(), _: Context, _: @import("../metadata/service.zig").TableTopologyProtocolReadiness) !void {}
    pub fn proposeTopologyActivationWithReceipt(self: *@This(), bytes: []const u8, _: @import("../metadata/service.zig").TableTopologyProtocolReadiness) !u64 {
        try self.apply(a, .{ .activate_topology_protocol = bytes });
        return self.index;
    }
    pub fn ensureLinearizableReadWithContext(_: *@This(), _: Context) !void {}
    pub fn lockCatalogMutation(self: *@This()) void {
        std.debug.assert(!self.locked);
        self.locked = true;
    }
    pub fn unlockCatalogMutation(self: *@This()) void {
        std.debug.assert(self.locked);
        self.locked = false;
    }
    pub fn projectedStore(self: *@This()) ?*storage.RaftApplyStore {
        return self.store;
    }
    pub fn captureTableCreateGeneration(self: *@This(), alloc: A, table_id: u64) !u64 {
        try self.store.ensureDerivedCatalogIndexes(group);
        return self.store.captureTableCreateGeneration(alloc, group, table_id);
    }
    pub fn captureTableDropAdmission(self: *@This(), alloc: A, name: []const u8) !@import("../metadata/service.zig").TableDropAdmission {
        var value = (try self.store.captureTableDropProjection(alloc, group, name)) orelse return error.TableNotFound;
        defer value.deinit(alloc);
        if (value.fence.active()) return error.TableTransitionActive;
        const owned = try alloc.dupe(u8, value.table.name);
        const groups = value.range_group_ids;
        value.range_group_ids = &.{};
        return .{ .table_id = value.table.table_id, .expected_name = owned, .expected_transition_generation = value.fence.generation, .range_membership = value.fence.membership(value.table.table_id), .range_group_ids = groups };
    }
    pub fn verifyTableCreateProjection(self: *@This(), alloc: A, table: @import("../metadata/table_manager.zig").TableRecord, ranges: []const @import("../metadata/table_manager.zig").RangeRecord) !void {
        try self.store.verifyTableCreateProjectionExact(alloc, group, table, ranges);
    }
    pub fn verifyTableDropProjection(self: *@This(), alloc: A, table_id: u64) !void {
        const table = try self.store.getTable(alloc, group, table_id);
        if (table) |record| {
            @import("../metadata/table_manager.zig").freeTable(alloc, record);
            return error.TableTransitionActive;
        }
    }
    fn apply(self: *@This(), alloc: A, command: storage.TransitionCommand) !void {
        if (!self.replicated) return self.store.applyStandaloneCommand(group, command);
        const wire = try storage.encodeTransitionCommand(alloc, command);
        defer alloc.free(wire);
        self.index += 1;
        const entries = try @import("../raft/state_machine/mod.zig").encodeCommittedEntries(alloc, &.{.{ .term = 1, .index = self.index, .entry_type = .normal, .data = wire }});
        defer alloc.free(entries);
        try self.store.snapshotBuilder().applyBatch(.{ .group_id = group, .commit_index = self.index, .entries_bytes = entries });
    }
    pub fn proposeTransitionCommandWithReceipt(self: *@This(), command: storage.TransitionCommand) !u64 {
        try std.testing.expect(self.locked);
        self.attempts += 1;
        const loss = self.lose;
        self.lose = .none;
        if (loss == .before) return error.MetadataMutationOutcomeUnknown;
        try self.apply(a, command);
        self.applied += 1;
        if (self.fail_result_after_apply) |allocator| {
            allocator.fail_index = allocator.alloc_index;
            allocator.resize_fail_index = allocator.resize_index;
        }
        if (loss == .after) return error.MetadataMutationOutcomeUnknown;
        return self.index;
    }
    pub fn waitForTransitionAppliedWithContext(self: *@This(), _: u64, _: Context) !void {
        try std.testing.expect(!self.locked);
    }
    fn call(raw: *anyopaque, alloc: A, context: Context, input: @import("../system_catalog/server_call.zig").Call) ![]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return switch (input) {
            .sql_catalog_prepare => |request| blk: {
                self.prepares += 1;
                try std.testing.expect(!self.locked);
                const bytes = try @import("../system_catalog/operations.zig").prepareSqlCatalog(self, alloc, context, request);
                if (!self.forge_relation_placement or request.relation == null) break :blk bytes;
                defer alloc.free(bytes);
                var parsed = try std.json.parseFromSlice(@import("../metadata/storage/raft_apply_contract.zig").SystemCatalogCommand, alloc, bytes, .{});
                defer parsed.deinit();
                parsed.value.sql_relation.?.change.definition.table.placement_role = "forged-role";
                break :blk std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
            },
            .sql_catalog_mutate => |command| @import("../system_catalog/operations.zig").mutateSqlCatalog(self, alloc, context, command),
            .sql_catalog_outcome => |request| blk: {
                self.observations += 1;
                if (self.hide) return error.MetadataMutationOutcomeUnknown;
                break :blk self.store.sqlCatalogOutcome(alloc, group, request);
            },
            .resolve => |target| std.json.Stringify.valueAlloc(alloc, try self.store.resolveSystemCatalogIdentity(alloc, group, target), .{}),
            .resolve_many => |request| blk: {
                const result = try self.store.resolveSystemCatalogIdentitiesWithContext(alloc, group, request, context);
                defer result.deinit(alloc);
                break :blk std.json.Stringify.valueAlloc(alloc, result, .{});
            },
            .read => |request| self.store.systemCatalogRead(alloc, group, request),
            .sql_type_bindings => |request| self.store.sqlTypeBindings(alloc, group, request, context),
            .constraint_name_prepare => |request| self.store.prepareConstraintNamesJsonWithContext(alloc, group, .{ .addition = request }, context),
            .constraint_name_check => |request| blk: {
                try self.store.checkConstraintNamesWithContext(alloc, group, request, context);
                break :blk alloc.dupe(u8, "true");
            },
            else => error.UnexpectedOrdinaryCatalogCall,
        };
    }
    fn server(self: *@This()) http.ApiHttpServer {
        return http.ApiHttpServer.init(a, .{ .imported_runtime_io = .{ .api = std.testing.io } }, .{ .ptr = self, .vtable = &.{ .status = status, .system_catalog = call, .supports_query_definitions = true, .supports_sql_views = true } }, null, null);
    }
    fn scope(self: *@This(), alloc: A, fence: L.scopes.CommandFence, nonce: u64, operation: @FieldType(L.Request, "operation")) !void {
        const request: L.Request = .{ .admission = .{ .nonce = id(nonce), .head = (try self.store.systemCatalogMeta(alloc, group)).sequence_outcome_head orelse 0 }, .incarnation = fence.backend.incarnation, .standalone = false, .database = 1, .actor = .{ .session_user = .bootstrap, .credential = "" }, .backend = fence.backend, .operation = operation };
        try self.apply(alloc, .{ .apply_sql_lock_scope = try std.json.Stringify.valueAlloc(alloc, request, .{}) });
        _ = try (try std.json.parseFromSliceLeaky(?L.outcomes.Outcome, alloc, try self.store.sqlLockOutcome(alloc, group, request), .{})).?.result();
    }
    fn setup(self: *@This(), alloc: A) !L.scopes.CommandFence {
        try self.store.applyStandaloneCommand(group, .{ .initialize_metadata_incarnation = @splat('1') });
        var coordinator: @import("../metadata/standalone_relation_reconciliation.zig").Coordinator = .{};
        var ready = false;
        for (0..200) |_| {
            coordinator.next_round_at_ns = 0;
            _ = try coordinator.step(self.store, alloc, group, @splat('1'), 9, @import("antfly_platform").time.monotonicNs());
            const work = try self.store.relationReconciliationWork(group);
            if (work.current) |state| if (state.phase == .ready and state.coverage == .constraint_names and work.root != null and work.root.?.eql(local.system_catalog_relation_reconciliation.Generation.of(&state))) {
                ready = true;
                break;
            };
        }
        try std.testing.expect(ready);
        const fence: L.scopes.CommandFence = .{ .backend = .{ .incarnation = @splat(0x11), .id = id(2), .fence = 1 }, .command = .{ .sequence = 1, .nonce = id(3), .executor = id(4) }, .database = 1, .principal = try L.principalDigest(alloc, .bootstrap), .standalone = false };
        try self.scope(alloc, fence, 5, .{ .register = .{ .lease = .{ .backend = fence.backend, .database = 1, .node = 9, .principal = fence.principal, .expires_at_ms = 100000 }, .connection = @splat('a'), .nonce = id(6), .standalone = false } });
        try self.scope(alloc, fence, 7, .{ .begin = fence.command });
        return fence;
    }
};
fn executeSql(server: *http.ApiHttpServer, alloc: A, context: Context, sql: []const u8) !catalog.DdlOutcome {
    var compiled = try local.sql_compiler.compile(alloc, sql, .{});
    defer compiled.deinit();
    return @import("sql_catalog.zig").execute(server, null, context, "default", "public", alloc, .{ .catalog_ddl = compiled.statement.catalog_ddl });
}

test "SQL catalog ordinary scoped evolution observes lost replies without replaying mutations" {
    for ([_]bool{ false, true }) |replicated| {
        var arena: std.heap.ArenaAllocator = .init(a);
        defer arena.deinit();
        const alloc = arena.allocator();
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var store = try storage.RaftApplyStore.init(a, .{ .root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/evolution", .{tmp.sub_path}) });
        defer store.deinit();
        var source: Source = .{ .store = &store, .replicated = replicated };
        const fence = try source.setup(alloc);
        var server = source.server();
        defer server.deinit();
        for ([_][]const u8{ "CREATE DATABASE receipt_db", "COMMENT ON DATABASE receipt_db IS 'retained'", "ALTER DATABASE receipt_db RENAME TO renamed_db", "DROP DATABASE renamed_db", "CREATE SCHEMA receipt_ns", "COMMENT ON SCHEMA receipt_ns IS 'retained'", "ALTER SCHEMA receipt_ns RENAME TO renamed_ns", "DROP SCHEMA renamed_ns", "CREATE TABLESPACE receipt_space LOCATION '/tmp/antfly-receipt-space'", "ALTER TABLESPACE receipt_space RENAME TO renamed_space", "DROP TABLESPACE renamed_space" }, 0..) |sql, index| {
            source.lose = .after;
            const outcome = try executeSql(&server, alloc, .{ .sql_command_fence = &fence }, sql);
            const receipt = outcome.catalog_receipt orelse return error.MissingOrdinaryReceipt;
            try std.testing.expectEqual(.committed, receipt.state);
            try std.testing.expectEqual(@as(usize, index + 1), source.attempts);
            try std.testing.expectEqual(source.attempts, source.applied);
            const before = try store.systemCatalogMeta(alloc, group);
            const observed = try @import("sql_catalog_recovery.zig").recover(&server, null, .{}, alloc, receipt.recovery_token);
            try std.testing.expectEqualDeep(receipt.catalog_revision, observed.catalog_revision);
            try std.testing.expectEqual(.committed, observed.state);
            try std.testing.expectEqualDeep(before, try store.systemCatalogMeta(alloc, group));
            try std.testing.expectEqual(@as(usize, index + 1), source.attempts);
        }
    }
}

test "SQL catalog ordinary typed table unknown admission recovers after cold reopen and executor fencing" {
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/typed", .{tmp.sub_path});
    var store = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
    var open = true;
    defer if (open) store.deinit();
    var source: Source = .{ .store = &store, .replicated = true };
    const fence = try source.setup(alloc);
    var server = source.server();
    defer server.deinit();
    source.lose = .after;
    source.hide = true;
    const input: catalog.Ddl = .{ .create_table = .{ .name = .{ .table = "typed_receipt" }, .schema_json = "{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}},\"additionalProperties\":false}}}}", .if_not_exists = false } };
    const outcome = try @import("sql_catalog.zig").execute(&server, null, .{ .sql_command_fence = &fence }, "default", "public", alloc, input);
    const receipt = outcome.catalog_receipt orelse return error.MissingOrdinaryReceipt;
    try std.testing.expectEqual(@as(?catalog.MutationOutcome, null), outcome.mutation_outcome);
    try std.testing.expectEqual(.unknown, receipt.state);
    try std.testing.expectEqual(@as(usize, 1), source.attempts);
    store.deinit();
    open = false;
    store = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
    open = true;
    source.hide = false;
    try source.scope(alloc, fence, 5000, .{ .fence = .close });
    const before = try store.systemCatalogMeta(alloc, group);
    const observed = try @import("sql_catalog_recovery.zig").recover(&server, null, .{}, alloc, receipt.recovery_token);
    try std.testing.expectEqual(.committed, observed.state);
    const table = (try store.resolveSystemCatalogTable(alloc, group, .{ .table = "typed_receipt" })).?;
    try std.testing.expectEqual(@as(?u64, table.table_id), observed.object_id);
    try std.testing.expectEqualDeep(before, try store.systemCatalogMeta(alloc, group));
    try std.testing.expectEqual(@as(usize, 1), source.attempts);
}

test "SQL catalog ordinary recovery rejects label substitution and current authorization revocation" {
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try storage.RaftApplyStore.init(a, .{ .root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/authorization", .{tmp.sub_path}) });
    defer store.deinit();
    var source: Source = .{ .store = &store };
    const fence = try source.setup(alloc);
    var server = source.server();
    defer server.deinit();
    const committed = (try executeSql(&server, alloc, .{ .sql_command_fence = &fence }, "CREATE DATABASE private_db")).catalog_receipt.?;
    const size = try std.base64.url_safe_no_pad.Decoder.calcSizeForSlice(committed.recovery_token);
    const bytes = try alloc.alloc(u8, size);
    try std.base64.url_safe_no_pad.Decoder.decode(bytes, committed.recovery_token);
    const token = try std.json.parseFromSliceLeaky(ordinary.Token, alloc, bytes, .{});
    var forged = token.request;
    forged.target.kind = .namespace;
    var permission = try @import("../usermgr/mod.zig").Permission.initOwned(a, .namespace, "default.private_db", .admin);
    defer permission.deinit(a);
    var permissions = [_]@import("../usermgr/mod.zig").Permission{permission};
    const identity: http.AuthenticatedIdentity = .{ .username = @constCast("reader"), .permissions = &permissions };
    const forged_receipt = try ordinary.newReceipt(alloc, alloc, forged);
    const unknown = (try ordinary.recoverIfOrdinary(&server, identity, .{}, alloc, forged_receipt.recovery_token)).?;
    try std.testing.expectEqual(.unknown, unknown.state);
    try std.testing.expectEqual(@as(?u64, null), unknown.object_id);
    const reads = source.observations;
    try std.testing.expectError(error.Forbidden, ordinary.recoverIfOrdinary(&server, identity, .{}, alloc, committed.recovery_token));
    try std.testing.expectEqual(reads, source.observations);
    const users = @import("../usermgr/mod.zig");
    var user_store = users.MemoryStore.init(a);
    defer user_store.deinit();
    var policy_store = @import("antfly_casbin").MemoryAdapter.init(a);
    defer policy_store.deinit();
    var manager = try users.UserManager.init(a, user_store.iface(), try users.initDefaultEnforcer(a, policy_store.iface()));
    defer manager.deinit();
    var user = try manager.createUser("reader", "test-password", &.{permission});
    defer user.deinit(a);
    var live = identity;
    live.live_user_manager = &manager;
    const permitted = (try ordinary.recoverIfOrdinary(&server, live, .{}, alloc, forged_receipt.recovery_token)).?;
    try std.testing.expectEqual(.unknown, permitted.state);
    try manager.removePermissionFromUser("reader", "default.private_db", .namespace);
    const prior = source.observations;
    try std.testing.expectError(error.Forbidden, ordinary.recoverIfOrdinary(&server, live, .{}, alloc, forged_receipt.recovery_token));
    try std.testing.expectEqual(prior, source.observations);
}

test "SQL catalog ordinary constrained create preserves activation receipts without result allocation after admission" {
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try storage.RaftApplyStore.init(a, .{ .root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/activation", .{tmp.sub_path}) });
    defer store.deinit();
    var source: Source = .{ .store = &store };
    const fence = try source.setup(alloc);
    var server = source.server();
    defer server.deinit();
    const schema = "{\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}},\"additionalProperties\":false}}},\"checks\":[{\"name\":\"items_id_check\",\"expression\":{\"op\":\"literal\",\"type\":\"boolean\",\"value\":true}}]}";
    for ([_]bool{ false, true }, 0..) |unknown, ordinal| {
        var result_allocator = std.testing.FailingAllocator.init(alloc, .{});
        source.fail_result_after_apply = &result_allocator;
        defer source.fail_result_after_apply = null;
        source.lose = if (unknown) .after else .none;
        source.hide = unknown;
        const outcome = try @import("sql_catalog.zig").executeWithAllocators(&server, null, .{ .sql_command_fence = &fence }, "default", "public", .{ .result = result_allocator.allocator(), .scratch = a }, .{ .create_table = .{ .name = .{ .table = if (unknown) "uncertain_checked_rows" else "checked_rows" }, .schema_json = schema, .if_not_exists = false } }, null);
        try std.testing.expectEqual(@as(usize, ordinal + 1), source.applied);
        try std.testing.expect(!result_allocator.has_induced_failure);
        const table_receipt = outcome.receipt orelse return error.MissingActivationReceipt;
        try std.testing.expect(!std.mem.eql(u8, table_receipt.table_id, "0"));
        if (unknown) {
            try std.testing.expectEqual(.admission_unknown, table_receipt.state);
            try std.testing.expectEqual(.unknown, outcome.catalog_receipt.?.state);
            try std.testing.expectEqual(@as(?catalog.MutationOutcome, null), outcome.mutation_outcome);
        } else {
            try std.testing.expectEqual(.pending, table_receipt.state);
            try std.testing.expectEqual(.committed, outcome.catalog_receipt.?.state);
            try std.testing.expectEqual(catalog.MutationOutcome.committed_pending, outcome.mutation_outcome.?);
        }
    }
}

test "SQL catalog ordinary uncertain nonsend becomes definite rejection only after native executor fencing" {
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try storage.RaftApplyStore.init(a, .{ .root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/nonsend", .{tmp.sub_path}) });
    defer store.deinit();
    var source: Source = .{ .store = &store };
    const fence = try source.setup(alloc);
    var server = source.server();
    defer server.deinit();
    source.lose = .before;
    const receipt = (try executeSql(&server, alloc, .{ .sql_command_fence = &fence }, "CREATE SCHEMA never_admitted")).catalog_receipt.?;
    try std.testing.expectEqual(.unknown, receipt.state);
    const before_fence = try @import("sql_catalog_recovery.zig").recover(&server, null, .{}, alloc, receipt.recovery_token);
    try std.testing.expectEqual(.unknown, before_fence.state);
    try source.scope(alloc, fence, 5000, .{ .fence = .close });
    const after_fence = try @import("sql_catalog_recovery.zig").recover(&server, null, .{}, alloc, receipt.recovery_token);
    try std.testing.expectEqual(.rejected, after_fence.state);
    try std.testing.expectEqual(@as(usize, 1), source.attempts);
    try std.testing.expectEqual(@as(usize, 0), source.applied);
}

test "SQL catalog ordinary view lifecycle retains exact lost replies dependency closure and cold recovery" {
    const Lost = struct {
        fn run(source: *Source, server: *http.ApiHttpServer, alloc: A, context: Context, sql: []const u8) !catalog.DdlOutcome {
            source.lose = .after;
            return executeSql(server, alloc, context, sql);
        }
    };
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_]bool{ false, true }) |replicated| {
        const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/views-{s}", .{ tmp.sub_path, if (replicated) "raft" else "local" });
        var store = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
        defer store.deinit();
        var source: Source = .{ .store = &store, .replicated = replicated };
        const fence = try source.setup(alloc);
        var server = source.server();
        defer server.deinit();
        source.lose = .after;
        // Ordinary public connections retain outcomes before scope enrollment.
        const unscoped = try Lost.run(&source, &server, alloc, .{}, "CREATE VIEW unscoped_view AS SELECT 0::BIGINT AS id");
        try std.testing.expectEqual(.committed, unscoped.catalog_receipt.?.state);
        _ = try Lost.run(&source, &server, alloc, .{}, "DROP VIEW unscoped_view");
        const context: Context = .{ .sql_command_fence = &fence };
        const first = try Lost.run(&source, &server, alloc, context, "CREATE VIEW retained_view AS SELECT 1::BIGINT AS id");
        const original = first.catalog_receipt.?;
        try std.testing.expectEqual(.view, original.kind);
        try std.testing.expectEqual(.committed, original.state);
        try std.testing.expectEqual(@as(?u64, 1), original.generation);
        _ = try Lost.run(&source, &server, alloc, context, "CREATE VIEW retained_child AS SELECT id FROM retained_view");
        const replacement = try Lost.run(&source, &server, alloc, context, "CREATE OR REPLACE VIEW retained_view AS SELECT 2::BIGINT AS id");
        try std.testing.expectEqual(original.object_id, replacement.catalog_receipt.?.object_id);
        try std.testing.expectEqual(@as(?u64, 2), replacement.catalog_receipt.?.generation);
        const renamed = try Lost.run(&source, &server, alloc, context, "ALTER VIEW retained_view RENAME TO renamed_view");
        try std.testing.expectEqual(original.object_id, renamed.catalog_receipt.?.object_id);
        try std.testing.expectEqual(@as(?u64, 2), renamed.catalog_receipt.?.generation);
        const before_restrict = try store.systemCatalogMeta(alloc, group);
        try std.testing.expectError(error.SqlDependentObjectsExist, Lost.run(&source, &server, alloc, context, "DROP VIEW renamed_view"));
        const after_restrict = try store.systemCatalogMeta(alloc, group);
        try std.testing.expectEqual(before_restrict.revision, after_restrict.revision);
        const dropped = try Lost.run(&source, &server, alloc, context, "DROP VIEW renamed_view CASCADE");
        try std.testing.expectEqual(.committed, dropped.catalog_receipt.?.state);
        try std.testing.expectEqual(@as(?u64, null), dropped.catalog_receipt.?.object_id);
        const selected = try store.resolveSystemCatalogIdentitiesWithContext(alloc, group, .{ .sql_sources = &.{ .{ .name = .{ .table = "renamed_view" } }, .{ .name = .{ .table = "retained_child" } } }, .include_query_definitions = true }, .{});
        defer selected.deinit(alloc);
        try std.testing.expect(selected.sql_sources[0] == null and selected.sql_sources[1] == null);
        source.hide = true;
        const uncertain = try Lost.run(&source, &server, alloc, context, "CREATE VIEW retained_view AS SELECT 3::BIGINT AS id");
        try std.testing.expectEqual(.unknown, uncertain.catalog_receipt.?.state);
        const attempts = source.attempts;
        store.deinit();
        store = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
        source.hide = false;
        try source.scope(alloc, fence, 90, .{ .fence = .close });
        const recovered = (try ordinary.recoverIfOrdinary(&server, null, .{}, alloc, uncertain.catalog_receipt.?.recovery_token)).?;
        try std.testing.expectEqual(.committed, recovered.state);
        try std.testing.expect(recovered.object_id != original.object_id);
        try std.testing.expectEqual(.committed, (try ordinary.recoverIfOrdinary(&server, null, .{}, alloc, original.recovery_token)).?.state);
        try std.testing.expectEqual(original.object_id, (try ordinary.recoverIfOrdinary(&server, null, .{}, alloc, original.recovery_token)).?.object_id);
        try std.testing.expectEqual(attempts, source.attempts);
    }
}

test "SQL catalog ordinary view post effect failures abort publication and journal together" {
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try storage.RaftApplyStore.init(a, .{ .root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/view-abort", .{tmp.sub_path}) });
    defer store.deinit();
    var source: Source = .{ .store = &store };
    const fence = try source.setup(alloc);
    var server = source.server();
    defer server.deinit();
    const before = try store.systemCatalogMeta(alloc, group);
    for ([_]anyerror{ error.CatalogGenerationChanged, error.InjectedCatalogWriteFailure }) |fault| {
        store.sql_catalog_post_effect_error = fault;
        const outcome = try executeSql(&server, alloc, .{ .sql_command_fence = &fence }, "CREATE VIEW aborted_view AS SELECT 1::BIGINT AS id");
        try std.testing.expectEqual(.unknown, outcome.catalog_receipt.?.state);
        try std.testing.expectEqualDeep(before, try store.systemCatalogMeta(alloc, group));
        const selected = try store.resolveSystemCatalogIdentitiesWithContext(alloc, group, .{ .sql_sources = &.{.{ .name = .{ .table = "aborted_view" } }}, .include_query_definitions = true }, .{});
        defer selected.deinit(alloc);
        try std.testing.expect(selected.sql_sources[0] == null);
        try std.testing.expectEqual(.unknown, (try ordinary.recoverIfOrdinary(&server, null, .{}, alloc, outcome.catalog_receipt.?.recovery_token)).?.state);
    }
}

test "SQL catalog ordinary view outcome decoder preserves object generations and rejects impossible completions" {
    var request: native.Observation = .{ .admission = .{ .request = .{ .nonce = @splat(1), .head = 0 }, .incarnation = @splat(2) }, .target = .{ .kind = .view, .action = .create, .database = "default", .namespace = "public", .name = "v", .table_id = 0 }, .expected_revision = 7, .digest = @splat(3) };
    const T = struct {
        fn decoded(observation: native.Observation, result: native.outcomes.Result) !?native.Outcome {
            const bytes = try std.json.Stringify.valueAlloc(a, native.Outcome{ .success = result }, .{});
            defer a.free(bytes);
            return ordinary.decodeOutcome(a, bytes, observation);
        }
    };
    var result: native.outcomes.Result = .{ .revision = 8, .identity = .{ .id = 4, .generation = 3 }, .removed = 0 };
    try std.testing.expect((try T.decoded(request, result)) != null);
    result.revision = 9;
    try std.testing.expect((try T.decoded(request, result)) == null);
    result = .{ .revision = 7, .identity = null, .removed = 0, .no_op = true };
    try std.testing.expect((try T.decoded(request, result)) == null);
    request.target.action = .drop;
    try std.testing.expect((try T.decoded(request, result)) != null);
    result.identity = .{ .id = 4, .generation = 3 };
    try std.testing.expect((try T.decoded(request, result)) == null);
    result = .{ .revision = 8, .identity = null, .removed = 2 };
    try std.testing.expect((try T.decoded(request, result)) == null);
    result.removed = 1;
    try std.testing.expect((try T.decoded(request, result)) != null);
}

fn createRows(server: *http.ApiHttpServer, alloc: A, fence: L.scopes.CommandFence, declaration_sql: []const u8) !u64 {
    return createRowsWithIndexes(server, alloc, fence, declaration_sql, false);
}
fn createRowsWithIndexes(server: *http.ApiHttpServer, alloc: A, fence: L.scopes.CommandFence, declaration_sql: []const u8, indexed: bool) !u64 {
    var declaration = try local.sql_compiler.compile(alloc, declaration_sql, .{});
    defer declaration.deinit();
    var schema = try local.sql_ddl_runtime.createSchemaAlloc(alloc, declaration.statement.create_table);
    if (indexed) {
        // The public typed CREATE contract accepts an initial relational index.
        // This avoids manufacturing a data-owner ACK merely to retire an old
        // layout between two mutations in this metadata recovery campaign.
        var value = try std.json.parseFromSlice(std.json.Value, alloc, schema, .{});
        defer value.deinit();
        const indexes = try std.json.parseFromSliceLeaky(std.json.Value, alloc, "[{\"name\":\"rows_note_idx\",\"keys\":[{\"column\":\"note\"}]}]", .{});
        try value.value.object.put(alloc, "relational_indexes", indexes);
        schema = try std.json.Stringify.valueAlloc(alloc, value.value, .{});
    }
    const outcome = try @import("sql_catalog.zig").executeWithAllocators(server, null, .{ .sql_command_fence = &fence }, "default", "public", .{ .result = alloc, .scratch = alloc }, .{ .create_table = .{ .name = .{ .table = "annotated_rows" }, .schema_json = schema, .if_not_exists = false } }, null);
    return outcome.catalog_receipt.?.object_id.?;
}

test "SQL catalog ordinary schema and annotation commands retain exact outcomes across lost replies and cold reopen" {
    const Case = struct { declaration: []const u8, statements: []const []const u8, indexed: bool = false };
    const cases = [_]Case{
        .{ .declaration = "CREATE TABLE annotated_rows(id BIGINT,note TEXT,CONSTRAINT rows_id_check CHECK(id > 0))", .statements = &.{
            "COMMENT ON TABLE annotated_rows IS 'stable presentation'",
            "COMMENT ON COLUMN annotated_rows.id IS 'stable attribute'",
            "COMMENT ON COLUMN annotated_rows.note IS 'new attribute'",
            "COMMENT ON COLUMN annotated_rows.note IS NULL",
            "CREATE INDEX rows_note_idx ON annotated_rows(note)",
            "COMMENT ON INDEX rows_note_idx IS 'stable index'",
            "COMMENT ON INDEX rows_note_idx IS NULL",
            "COMMENT ON CONSTRAINT rows_id_check ON annotated_rows IS 'stable constraint'",
            "COMMENT ON CONSTRAINT rows_id_check ON annotated_rows IS NULL",
        } },
        .{ .declaration = "CREATE TABLE annotated_rows(id BIGINT,note TEXT)", .statements = &.{ "ALTER TABLE annotated_rows ADD COLUMN extra TEXT", "COMMENT ON COLUMN annotated_rows.extra IS 'added column'" } },
        .{ .declaration = "CREATE TABLE annotated_rows(id BIGINT,note TEXT)", .indexed = true, .statements = &.{ "DROP INDEX public.rows_note_idx", "COMMENT ON TABLE annotated_rows IS 'after qualified index drop'" } },
        .{ .declaration = "CREATE TABLE annotated_rows(id BIGINT,note TEXT)", .statements = &.{"ALTER TABLE annotated_rows ALTER COLUMN note SET DEFAULT 'future'"} },
        .{ .declaration = "CREATE TABLE annotated_rows(id BIGINT,note TEXT DEFAULT 'future')", .statements = &.{"ALTER TABLE annotated_rows ALTER COLUMN note DROP DEFAULT"} },
        .{ .declaration = "CREATE TABLE annotated_rows(id BIGINT,note TEXT)", .statements = &.{ "ALTER TABLE annotated_rows ADD CONSTRAINT rows_id_check CHECK(id > 0) NOT VALID", "COMMENT ON CONSTRAINT rows_id_check ON annotated_rows IS 'deferred constraint'" } },
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for (0..cases.len * 2) |ordinal| {
        const case = cases[ordinal % cases.len];
        const enrolled = ordinal < cases.len;
        var arena: std.heap.ArenaAllocator = .init(a);
        defer arena.deinit();
        const alloc = arena.allocator();
        const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/schema-notes-{d}", .{ tmp.sub_path, ordinal });
        var store = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
        defer store.deinit();
        var source: Source = .{ .store = &store };
        const fence = try source.setup(alloc);
        var server = source.server();
        defer server.deinit();
        const table_id = try createRowsWithIndexes(&server, alloc, fence, case.declaration, case.indexed);
        const initial = (try store.getTable(alloc, group, table_id)).?;
        defer @import("../metadata/table_manager.zig").freeTable(alloc, initial);
        var receipts: std.ArrayList(catalog.CatalogOperationReceipt) = .empty;
        var prior_version = try @import("tables.zig").schemaVersion(initial.schema_json);
        for (case.statements) |sql| {
            source.lose = .after;
            const before = source.attempts;
            const prior_catalog = try store.systemCatalogMeta(alloc, group);
            const prior_reconciliation = try store.relationReconciliationWork(group);
            const outcome = try executeSql(&server, alloc, .{ .sql_command_fence = if (enrolled) &fence else null }, sql);
            const receipt = outcome.catalog_receipt orelse return error.MissingRelationReceipt;
            try std.testing.expectEqual(.committed, receipt.state);
            try std.testing.expectEqual(.table, receipt.kind);
            try std.testing.expectEqual(before + 1, source.attempts);
            try std.testing.expectEqual(@as(?u64, table_id), receipt.object_id);
            const table = (try store.getTable(alloc, group, table_id)).?;
            defer @import("../metadata/table_manager.zig").freeTable(alloc, table);
            const version = try @import("tables.zig").schemaVersion(table.schema_json);
            if (std.mem.startsWith(u8, sql, "COMMENT")) {
                try std.testing.expectEqual(@as(?u64, prior_catalog.revision), receipt.catalog_revision);
                try std.testing.expectEqual(prior_version, version);
                try std.testing.expectEqualDeep(prior_reconciliation, try store.relationReconciliationWork(group));
            } else {
                try std.testing.expectEqual(prior_version + 1, version);
                prior_version = version;
            }
            try receipts.append(alloc, receipt);
        }
        const before = try store.systemCatalogMeta(alloc, group);
        const attempts = source.attempts;
        store.deinit();
        store = try storage.RaftApplyStore.init(a, .{ .root_dir = path });
        try source.scope(alloc, fence, 5000, .{ .fence = .close });
        for (receipts.items) |receipt| {
            const recovered = (try ordinary.recoverIfOrdinary(&server, null, .{}, alloc, receipt.recovery_token)).?;
            try std.testing.expectEqual(.committed, recovered.state);
            try std.testing.expectEqual(receipt.catalog_revision, recovered.catalog_revision);
            try std.testing.expectEqual(receipt.object_id, recovered.object_id);
        }
        try std.testing.expectEqual(attempts, source.attempts);
        try std.testing.expectEqual(before.revision, (try store.systemCatalogMeta(alloc, group)).revision);
    }
}

fn relationImage(store: *storage.RaftApplyStore, alloc: A, guard: domain.RelationMutationGuard) ![]const u8 {
    const table = (try store.getTable(alloc, group, guard.owner.table_id)).?;
    var columns = try store.getSqlColumnIdentities(alloc, group, guard);
    defer columns.deinit();
    const object = try store.getSqlObjectAnnotation(alloc, group, .{ .guard = guard, .key = .{ .kind = .constraint, .name = "rows_id_check" } });
    return std.json.Stringify.valueAlloc(alloc, .{ .table = table, .columns = columns.value, .object = object }, .{});
}

test "SQL catalog ordinary relation faults roll back all definition and annotation effects and retire definite refusals" {
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try storage.RaftApplyStore.init(a, .{ .root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/relation-faults", .{tmp.sub_path}) });
    defer store.deinit();
    var source: Source = .{ .store = &store };
    const fence = try source.setup(alloc);
    var server = source.server();
    defer server.deinit();
    const table_id = try createRows(&server, alloc, fence, "CREATE TABLE annotated_rows(id BIGINT,note TEXT,CONSTRAINT rows_id_check CHECK(id > 0))");
    const captured = (try @import("sql_catalog.zig").captureTableAdmission(&server, null, .{}, alloc, .{ .table = "annotated_rows" }, true, null)).?;
    for ([_][]const u8{
        "COMMENT ON TABLE annotated_rows IS 'rollback table'",
        "COMMENT ON COLUMN annotated_rows.id IS 'rollback column'",
        "COMMENT ON CONSTRAINT rows_id_check ON annotated_rows IS 'rollback object'",
        "ALTER TABLE annotated_rows ADD COLUMN rolled_back TEXT",
    }) |sql| {
        const before = try relationImage(&store, alloc, captured.guard);
        const meta = try store.systemCatalogMeta(alloc, group);
        store.sql_catalog_post_effect_error = error.CatalogGenerationChanged;
        const outcome = try executeSql(&server, alloc, .{ .sql_command_fence = &fence }, sql);
        try std.testing.expectEqual(.unknown, outcome.catalog_receipt.?.state);
        try std.testing.expectEqualStrings(before, try relationImage(&store, alloc, captured.guard));
        try std.testing.expectEqualDeep(meta, try store.systemCatalogMeta(alloc, group));
    }
    const Completion = struct {
        id: u64,
        version: u32,
        prepared: usize = 0,
        rejected: usize = 0,
        fn txn(_: *anyopaque, _: [16]u8) !void {
            return error.UnexpectedTransactionPreparation;
        }
        fn prepare(raw: *anyopaque, owner: local.api_operation.SQLCompletionOwner) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(self.id, owner.id);
            try std.testing.expectEqual(self.version, owner.schema_version);
            self.prepared += 1;
        }
        fn reject(raw: *anyopaque, owner: local.api_operation.SQLCompletionOwner) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(self.id, owner.id);
            try std.testing.expectEqual(self.version, owner.schema_version);
            self.rejected += 1;
        }
    };
    var completion: Completion = .{ .id = table_id, .version = captured.guard.owner.schema_version + 1 };
    source.forge_relation_placement = true;
    const before = try relationImage(&store, alloc, captured.guard);
    const meta = try store.systemCatalogMeta(alloc, group);
    try std.testing.expectError(error.InvalidCatalogMutation, executeSql(&server, alloc, .{ .sql_command_fence = &fence, .sql_completion = .{ .ptr = &completion, .prepare_transaction = Completion.txn, .prepare_metadata = Completion.prepare, .reject_metadata = Completion.reject } }, "ALTER TABLE annotated_rows ADD COLUMN forged TEXT"));
    try std.testing.expectEqual(@as(usize, 1), completion.prepared);
    try std.testing.expect(completion.rejected != 0);
    try std.testing.expectEqualStrings(before, try relationImage(&store, alloc, captured.guard));
    try std.testing.expectEqual(meta.revision, (try store.systemCatalogMeta(alloc, group)).revision);
}

test "SQL catalog ordinary relation completion owns response identities before admission and preserves unknown recovery" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_]bool{ false, true }) |unknown| {
        var arena: std.heap.ArenaAllocator = .init(a);
        defer arena.deinit();
        const alloc = arena.allocator();
        var store = try storage.RaftApplyStore.init(a, .{ .root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/relation-allocation-{s}", .{ tmp.sub_path, if (unknown) "unknown" else "known" }) });
        defer store.deinit();
        var source: Source = .{ .store = &store };
        const fence = try source.setup(alloc);
        var server = source.server();
        defer server.deinit();
        _ = try createRows(&server, alloc, fence, "CREATE TABLE annotated_rows(id BIGINT,note TEXT)");
        var failed = std.testing.FailingAllocator.init(alloc, .{});
        source.fail_result_after_apply = &failed;
        defer source.fail_result_after_apply = null;
        source.lose = if (unknown) .after else .none;
        source.hide = unknown;
        var compiled = try local.sql_compiler.compile(alloc, "ALTER TABLE annotated_rows ADD COLUMN retained TEXT", .{});
        defer compiled.deinit();
        const outcome = try @import("sql_catalog.zig").executeWithAllocators(&server, null, .{ .sql_command_fence = &fence }, "default", "public", .{ .result = failed.allocator(), .scratch = a }, .{ .catalog_ddl = compiled.statement.catalog_ddl }, null);
        try std.testing.expect(!failed.has_induced_failure);
        try std.testing.expectEqual(if (unknown) catalog.CatalogOperationReceipt.State.unknown else .committed, outcome.catalog_receipt.?.state);
        source.hide = false;
        const attempts = source.attempts;
        const recovered = (try ordinary.recoverIfOrdinary(&server, null, .{}, alloc, outcome.catalog_receipt.?.recovery_token)).?;
        try std.testing.expectEqual(.committed, recovered.state);
        try std.testing.expectEqual(attempts, source.attempts);
    }
}

test "SQL catalog ordinary schema preparation waits for the complete route cut before any admission" {
    const router = @import("table_router.zig");
    const State = struct {
        calls: usize = 0,
        missing: bool = true,
        fn node(_: *anyopaque) u64 {
            return 1;
        }
        fn status(_: *anyopaque, _: u64) @import("../raft/host.zig").HostedReplicaStatus {
            return .absent;
        }
        fn endpoint(_: *anyopaque, _: A, _: u64) !?[]u8 {
            return error.UnexpectedScalarRoute;
        }
        fn resolve(raw: *anyopaque, alloc: A, groups: []const u64, _: router.RoutePolicy, budget: router.RouteBudget) !?[]router.GroupRoute {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try budget.check();
            try std.testing.expectEqualSlices(u64, &.{ 11, 12 }, groups);
            self.calls += 1;
            if (self.missing and self.calls == 1) return null;
            const routes = try alloc.alloc(router.GroupRoute, groups.len);
            @memset(routes, .local);
            return routes;
        }
    };
    var state: State = .{};
    var source: Source = .{ .store = undefined };
    var server = source.server();
    defer server.deinit();
    server.cfg.deployment_mode = .distributed;
    server.cfg.session_router = .{ .ptr = &state, .vtable = &.{ .local_node_id = State.node, .local_status = State.status, .node_base_uri = State.endpoint, .resolve_group_routes = State.resolve } };
    var transport: @import("schema_validation_owner_transport.zig").Transport = .{ .server = &server, .alloc = a };
    defer transport.deinit();
    try transport.prepareCohort(&.{ .{ .group_id = 11, .table_id = 1, .start_key = "" }, .{ .group_id = 12, .table_id = 1, .start_key = "x" } }, .{});
    try std.testing.expectEqual(@as(usize, 2), state.calls);
    try std.testing.expectEqual(@as(usize, 2), transport.routes.?.len);
    try std.testing.expectEqual(@as(usize, 0), source.prepares);
    try std.testing.expectEqual(@as(usize, 0), source.attempts);
}
