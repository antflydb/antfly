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

//! Transaction-owned table declarations for generation publication.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.sql_catalog;
const domain = local.system_catalog_domain;
const http = @import("../../http_server.zig");
const generations = @import("../../../metadata/sql/relation_lifecycle/generation_intents.zig");
const creation = @import("../../../metadata/relation_create.zig");
const Root = @import("execution.zig").Adapter;
const A = std.mem.Allocator;

pub fn supportsCreate(root: *const Root, input: catalog.CreateTableDefinition) bool {
    return root.server.cfg.sql_catalog_session_authority and root.active_transaction != null and root.context.sql_command_fence != null and
        input.persistence == .permanent and input.on_commit == null and
        input.partition_by == null and input.partition_of == null and input.creation_sources.len == 0 and
        input.creation_revision == null and input.creation_comment_sources.len == 0 and input.creation_object_comments.len == 0 and
        input.column_sequences.len == 0 and input.generated_constraint_names.len == 0;
}

fn namespace(root: *Root, a: A, target: *domain.Target, use_path: bool) !u64 {
    if (use_path) if (root.ddl_search_path) |path| {
        for (path.entries[0..path.len]) |*part| {
            const name = part.slice();
            if (std.mem.eql(u8, name, "pg_catalog")) continue;
            var candidate = target.*;
            candidate.namespace = name;
            const id = root.server.catalogNamespaceId(a, root.context, candidate) catch |err| switch (err) {
                error.NamespaceNotFound, error.CatalogNotFound => continue,
                else => return err,
            };
            target.* = candidate;
            return id;
        }
        return error.NamespaceNotFound;
    };
    return root.server.catalogNamespaceId(a, root.context, target.*);
}

fn alreadyExists(root: *Root, a: A, conditional: bool, name: []const u8) !catalog.DdlOutcome {
    if (!conditional) return error.CatalogAlreadyExists;
    if (root.notice_sink) |sink| {
        const message = try std.fmt.allocPrint(a, "relation \"{s}\" already exists, skipping", .{name});
        defer a.free(message);
        try sink.emit(sink.ptr, .{ .code = "42P07", .message = message });
    }
    return .{ .mutation_outcome = null };
}

/// Returns null for table shapes that use the existing catalog producer.
pub fn create(root: *Root, allocators: catalog.DdlAllocators, input: catalog.CreateTableDefinition) !?catalog.DdlOutcome {
    if (!supportsCreate(root, input)) return null;
    const server = root.server;
    const id = root.active_transaction.?;
    const fence = root.context.sql_command_fence.?;
    try root.context.ensureActive();
    var arena = std.heap.ArenaAllocator.init(allocators.scratch);
    defer arena.deinit();
    const a = arena.allocator();
    if (try @import("../../../metadata/fk_generation_publication.zig").schemaHasForeignKeys(a, input.schema_json)) return null;
    const principal = http.transactionPrincipal(root.identity.*);
    var target: domain.Target = .{
        .database = input.name.database orelse root.database,
        .namespace = input.name.namespace orelse root.namespace,
        .table = input.name.table,
    };
    try target.validate();
    const namespace_id = try namespace(root, a, &target, input.name.database == null and input.name.namespace == null);
    try @import("../catalog/catalog_auth.zig").authorizeTarget(server, root.identity.*, a, target);
    const binding = try server.txn_sessions.sqlGenerationBinding(a, id, principal, target, true);
    if (binding.entry != null)
        return try alreadyExists(root, a, input.if_not_exists, target.table);
    try bindTransaction(root, a, principal, binding.connection_id, id);

    const physical = try server.catalogStorageNameAlloc(a);
    const schema = try server.bindForeignKeySchema(a, target, physical, input.schema_json, "", root.identity.*, root.context);
    var candidate = @import("../../tables.zig").deriveTableRecord("", .{ .schema_json = @constCast(schema), .indexes_json = @constCast("{}") });
    candidate.table_id = 0;
    var trusted = root.context;
    trusted.setting_admin = true;
    const request: creation.Request = .{
        .sql_generation = .{ .transaction = id, .subtransaction = binding.subtransaction, .fence = fence.* },
        .sql_owner = try @import("../recovery/authorization_recovery.zig").creator(server, root.identity.*),
        .physical_name = physical,
        .namespace_id = namespace_id,
        .logical_name = target.table,
        .tablespace_name = input.tablespace,
        .candidate = candidate,
    };
    const prepared = retry: while (true) {
        try trusted.ensureActive();
        var attempt = std.heap.ArenaAllocator.init(allocators.scratch);
        defer attempt.deinit();
        const bytes = server.source.systemCatalog(attempt.allocator(), trusted, .{ .query_population_prepare = request }) catch |err| switch (err) {
            error.CatalogAlreadyExists, error.TableAlreadyExists => return try alreadyExists(root, a, input.if_not_exists, target.table),
            error.CatalogOccupancyNotReady, error.CatalogPublicationProofPending => {
                try server.embedding_provider_runtime.io.sleep(.fromMilliseconds(10), .awake);
                continue;
            },
            else => return err,
        };
        break :retry try std.json.parseFromSliceLeaky(creation.Prepared, a, bytes, .{ .allocate = .alloc_always });
    };
    if (!std.mem.eql(u8, prepared.child.name, physical)) return error.InvalidSqlBackendResponse;
    var nonce: [16]u8 = undefined;
    try server.embedding_provider_runtime.io.randomSecure(&nonce);
    var physical_id: [16]u8 = undefined;
    _ = try std.fmt.hexToBytes(&physical_id, physical[6..]);
    const entry: generations.Entry = .{
        .transaction = id,
        .nonce = nonce,
        .generation = prepared.child.table_id,
        .logical_relation_id = prepared.catalog_id,
        .physical = physical_id,
        .birth = fence.*,
        .birth_subtransaction = binding.subtransaction,
        .subtransaction = binding.subtransaction,
        .operation = .create,
        .target = target,
        .preparation = try std.json.Stringify.valueAlloc(a, prepared, .{}),
    };
    try server.txn_sessions.stageSqlGeneration(server.alloc, id, principal, entry);
    return .{ .mutation_outcome = null };
}

fn bindTransaction(root: *Root, a: A, principal: ?[]const u8, connection: ?[32]u8, id: [16]u8) !void {
    const server = root.server;
    const fence = root.context.sql_command_fence orelse return error.InvalidLockScope;
    const producer = @import("../session/command_producer.zig");
    const io = server.embedding_provider_runtime.io;
    const frame: @import("../session/advisory_execution.zig").Frame = .{
        .source = server.txn_sessions.durable orelse return error.SqlConnectionUnavailable,
        .connection = connection orelse return error.SqlConnectionNotFound,
        .principal = principal orelse "",
        .instance = try server.sql_connection_admission.executionInstance(io),
        .fence = fence.*,
        .actor = try @import("../recovery/authorization_recovery.zig").actor(server, root.identity.*),
    };
    var native: producer.NativePort = .{ .server = server, .context = root.context };
    var bind_nonce: [16]u8 = undefined;
    try io.randomSecure(&bind_nonce);
    try frame.bindTransaction(a, native.port(), id, bind_nonce, @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms);
}

pub fn truncate(root: *Root, alloc: A, ddl: local.sql_ast.CatalogDdl) !catalog.DdlOutcome {
    if (ddl.truncate_tables.len != 1 or ddl.cascade or ddl.restart_identity) return error.UnsupportedSqlExecution;
    try root.context.ensureActive();
    const fence = root.context.sql_command_fence orelse return error.InvalidLockScope;
    const transaction = root.active_transaction orelse return error.SqlTransactionNotActive;
    const principal = http.transactionPrincipal(root.identity.*);
    var scratch = std.heap.ArenaAllocator.init(alloc);
    defer scratch.deinit();
    const a = scratch.allocator();
    const backend = root.backend();
    const table = try backend.vtable.resolve(backend.ptr, a, ddl.truncate_tables[0], .read);
    const scope = table.scope orelse return error.InvalidSqlBackendResponse;
    if (root.ddl_guard) |guard| {
        const expected = (try guard.objects(a)).tables;
        if (expected.len != 1 or expected[0].id != table.id or !std.mem.eql(u8, expected[0].namespace, scope.namespace) or
            (expected[0].schema_version != null and expected[0].schema_version.? != table.schema_version)) return error.CatalogGenerationChanged;
    }
    const target: domain.Target = .{ .database = scope.database, .namespace = scope.namespace, .table = scope.name };
    try @import("../catalog/catalog_auth.zig").authorizeTarget(root.server, root.identity.*, a, target);
    if (try http.resolveEffectiveRowFilterJson(a, root.identity.*, try target.resourceNameAlloc(a)) != null) return error.Forbidden;
    const binding = try root.server.txn_sessions.sqlGenerationBinding(a, transaction, principal, target, true);
    const prepared = if (binding.entry) |entry| prior: {
        const token = table.private_generation orelse return error.CatalogGenerationChanged;
        if (!std.mem.eql(u8, &entry.nonce, &token.nonce) or entry.generation != table.id) return error.CatalogGenerationChanged;
        break :prior try entry.prepared(a);
    } else prior: {
        try @import("../session/table_lock_execution.zig").table(root, a, table, .access_exclusive, .block);
        break :prior (try publishedTruncateSource(root, a, target, table)).*;
    };
    if (prepared.child.sql_persistence != null or prepared.partition_birth != null or
        prepared.copy_effects != null or prepared.counter_effects != null) return error.UnsupportedSqlExecution;
    var schema = try local.schema_mod.parseValidatedTableSchema(a, prepared.child.schema_json);
    defer schema.deinit(a);
    if (schema.storage_mode != .relational or schema.foreign_keys != null or schema.sql_triggers != null or schema.sql_partition != null or
        try @import("../../../metadata/restore_staging.zig").hasTruncateAttachments(a, prepared.child.schema_json))
        return error.UnsupportedSqlExecution;

    var next = prepared;
    next.child.name = try root.server.catalogStorageNameAlloc(a);
    next.child.table_id = try creation.privateTableId(next.child.name);
    next.transition_generation = 0;
    const old_ranges = if (prepared.previous) |previous| previous.ranges else prepared.child_ranges;
    const ranges = try @import("../../tables.zig").deriveInitialRangesForGeneration(a, next.child, next.transition_generation);
    if (ranges.len != old_ranges.len) return error.CatalogGenerationChanged;
    for (ranges, old_ranges) |*range, old| {
        range.doc_identity_shard_id = range.group_id;
        range.doc_identity_range_id = range.group_id;
        range.start_key = old.start_key;
        range.end_key = old.end_key;
    }
    next.child_ranges = ranges;

    var nonce: [16]u8 = undefined;
    try root.server.embedding_provider_runtime.io.randomSecure(&nonce);
    var physical: [16]u8 = undefined;
    _ = try std.fmt.hexToBytes(&physical, next.child.name[6..]);
    const entry: generations.Entry = .{
        .transaction = transaction,
        .nonce = nonce,
        .generation = next.child.table_id,
        .logical_relation_id = prepared.catalog_id,
        .physical = physical,
        .birth = fence.*,
        .birth_subtransaction = binding.subtransaction,
        .subtransaction = binding.subtransaction,
        .operation = .truncate,
        .target = target,
        .preparation = try std.json.Stringify.valueAlloc(a, next, .{}),
    };
    try bindTransaction(root, a, principal, binding.connection_id, transaction);
    try root.server.txn_sessions.stageSqlGeneration(root.server.alloc, transaction, principal, entry);
    return .{ .mutation_outcome = null };
}

fn publishedTruncateSource(root: *Root, a: A, target: domain.Target, expected: catalog.Table) !*const creation.Prepared {
    const server = root.server;
    const context = root.context;
    const resolved = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, try server.source.systemCatalog(a, context, .{ .resolve_many = .{ .targets = &.{target} } }), .{});
    if (resolved.tables.len != 1) return error.InvalidSqlBackendResponse;
    const table = resolved.tables[0] orelse return error.TableNotFound;
    if (table.table_id != expected.id or !std.mem.eql(u8, table.name, expected.physical_name)) return error.CatalogGenerationChanged;
    try table.requireRequestAccess(context.native_service, context.sql_relation_owner);
    if (table.partition_family != null or table.native_service != null or table.logical_relation_id == null) return error.UnsupportedSqlExecution;
    const state = try std.json.parseFromSliceLeaky(domain.State, a, try server.source.systemCatalog(a, context, .snapshot), .{});
    var index = try domain.StateIndex.init(a, state);
    defer index.deinit(a);
    const namespace_record = try index.namespaceFor(target.database, target.namespace);
    const binding = index.find(.table, namespace_record.id, target.table) orelse return error.CatalogGenerationChanged;
    const database = index.find(.database, 0, target.database) orelse return error.CatalogGenerationChanged;
    if (binding.sql_publications != null or database.sql_all_publications != null) return error.UnsupportedSqlExecution;
    if (binding.id != table.table_id or binding.effectiveLogicalRelationId() != table.effectiveLogicalRelationId() or !std.mem.eql(u8, binding.storage_name, table.name))
        return error.CatalogGenerationChanged;
    for (state.policies) |policy| if (policy.table_id == table.table_id) return error.UnsupportedSqlExecution;
    for (state.policy_publications) |policy| if (policy.table_id == table.table_id) return error.UnsupportedSqlExecution;
    if (root.staged) |staged| {
        for (staged.tables) |item| if (std.mem.eql(u8, staged.physicalName(item.table_name), table.name)) return error.UnsupportedSqlExecution;
        for (staged.read_set) |item| if (std.mem.eql(u8, staged.physicalName(item.table_name), table.name)) return error.UnsupportedSqlExecution;
    }

    var snapshot = (try server.source.linearizableSnapshot(context)) orelse return error.MetadataCapabilityUnavailable;
    defer server.source.freeAdminSnapshot(&snapshot);
    const selected = try @import("../catalog/truncate.zig").select(a, snapshot.tables, &.{table.name}, false);
    if (selected.len != 1 or selected[0].table_id != table.table_id) return error.CatalogGenerationChanged;
    const before = try @import("../../../metadata/table_manager.zig").cloneTable(a, selected[0]);
    if (before.storage_migration != null or before.restore_backup_id.len != 0 or before.relational_retirement_json.len != 0) return error.TableTransitionActive;
    if (try local.storage_db_graph_retirement_config.fromMetadata(a, before.indexes_json) != null) return error.UnsupportedSqlExecution;
    var old_ranges: std.ArrayList(@TypeOf(snapshot.ranges[0])) = .empty;
    for (snapshot.ranges) |range| if (range.table_id == table.table_id) {
        try old_ranges.append(a, try @import("../../../metadata/table_manager.zig").cloneRange(a, range));
    };
    if (old_ranges.items.len == 0 or old_ranges.items.len > 4096) return error.SqlProgramLimitExceeded;
    @import("../../../metadata/table_manager.zig").sortKeyspaceRanges(@TypeOf(snapshot.ranges[0]), old_ranges.items);
    const prepared = try a.create(creation.Prepared);
    prepared.* = .{
        .previous = .{ .resource = binding, .table = before, .ranges = old_ranges.items },
        .catalog_id = table.effectiveLogicalRelationId(),
        .namespace_id = namespace_record.id,
        .expected_catalog_revision = state.revision,
        .tablespace_id = binding.tablespace_id,
        .min_ranges_explicit = true,
        .child = before,
        .child_ranges = &.{},
        .transition_generation = 0,
        .metadata_incarnation = snapshot.status.metadata_incarnation orelse return error.CatalogGenerationChanged,
        .metadata_epoch = snapshot.status.metadata_epoch,
    };
    prepared.child.min_ranges = @intCast(old_ranges.items.len);
    return prepared;
}

pub fn resolve(root: *Root, a: A, target: domain.Target, action: catalog.Action) !?catalog.Table {
    if (!root.server.cfg.sql_catalog_session_authority or (root.context.sql_command_fence == null and !root.copy_freeze_description) or !std.mem.eql(u8, target.database, root.database)) return null;
    const transaction = root.active_transaction orelse return null;
    // The session execution lease makes the local declaration view stable
    // for this command. A miss needs no additional durable read.
    if (root.active_catalog_generations) |pending| {
        if (pending.latest(target) == null) return null;
    }
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const s = scratch.allocator();
    const entry = (try lookup(root, s, transaction, target)) orelse return null;
    if (action != .read and !root.active_private_mutation) return error.UnsupportedSqlExecution;
    try @import("../catalog/catalog_auth.zig").authorizeTarget(root.server, root.identity.*, s, target);
    const prepared = try entry.prepared(s);
    const physical = try a.dupe(u8, prepared.child.name);
    const schema = try a.dupe(u8, prepared.child.schema_json);
    var table = try root.server.sql_schema_cache.resolve(root.server.sqlPlanCacheIo(), a, schema, entry.generation, physical);
    table.private_generation = .{ .transaction = transaction, .nonce = entry.nonce };
    table.logical_relation_id = entry.logical_relation_id;
    table.schema_json = schema;
    table.sql_persistence = prepared.child.sql_persistence;
    table.scope = .{
        .database = try a.dupe(u8, target.database),
        .namespace = try a.dupe(u8, target.namespace),
        .name = try a.dupe(u8, target.table),
        .revision = root.revision orelse prepared.expected_catalog_revision,
    };
    root.target = target;
    table.indexes = &.{};
    table.index_definitions = &.{};
    if (root.identity.*) |*identity| try http.projectCatalogIdentity(root.server.alloc, identity, try target.resourceNameAlloc(s), physical);
    try root.recordPreparedBinding(a, table);
    return table;
}

fn lookup(root: *Root, a: A, transaction: [16]u8, target: domain.Target) !?generations.Entry {
    const principal = http.transactionPrincipal(root.identity.*);
    const binding = try root.server.txn_sessions.sqlGenerationBinding(a, transaction, principal, target, false);
    return binding.entry;
}

pub fn freezeOrigin(root: *Root, a: A, table: catalog.Table, transaction: [16]u8) !@import("../copy/frozen_births.zig").Origin {
    if (root.copy_has_active_cursor) return error.SqlCopyFreezeActiveCursor;
    const private = table.private_generation orelse return error.SqlCopyFreezeGenerationRequired;
    if (!std.mem.eql(u8, &private.transaction, &transaction)) return error.CatalogGenerationChanged;
    const scope = table.scope orelse return error.InvalidSqlBackendResponse;
    const target: domain.Target = .{ .database = scope.database, .namespace = scope.namespace, .table = scope.name };
    const binding = try root.server.txn_sessions.sqlGenerationBinding(a, transaction, http.transactionPrincipal(root.identity.*), target, true);
    const entry = binding.entry orelse return error.SqlCopyFreezeGenerationRequired;
    if (!binding.freeze_eligible) return error.SqlCopyFreezeGenerationRequired;
    if (table.id != entry.generation or !std.mem.eql(u8, &private.nonce, &entry.nonce)) return error.CatalogGenerationChanged;
    return .{ .transaction = transaction, .nonce = entry.nonce, .physical = entry.physical, .schema_version = table.schema_version };
}

pub fn verifyRead(root: *Root, a: A, table: catalog.Table) !void {
    try root.context.ensureActive();
    const token = table.private_generation orelse return error.InvalidSqlBackendResponse;
    const transaction = root.active_transaction orelse return error.SqlTransactionNotActive;
    if (!std.mem.eql(u8, &token.transaction, &transaction)) return error.CatalogGenerationChanged;
    const scope = table.scope orelse return error.InvalidSqlBackendResponse;
    const target: domain.Target = .{ .database = scope.database, .namespace = scope.namespace, .table = scope.name };
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const s = scratch.allocator();
    const entry = (try lookup(root, s, transaction, target)) orelse return error.CatalogGenerationChanged;
    if (entry.generation != table.id or entry.logical_relation_id != table.effectiveLogicalRelationId() or
        !std.mem.eql(u8, &entry.nonce, &token.nonce)) return error.CatalogGenerationChanged;
    const prepared = try entry.prepared(s);
    if (!std.mem.eql(u8, prepared.child.name, table.physical_name) or
        !std.mem.eql(u8, prepared.child.schema_json, table.schema_json) or
        !std.meta.eql(prepared.child.sql_persistence, table.sql_persistence)) return error.CatalogGenerationChanged;
    try @import("../catalog/catalog_auth.zig").authorizeTarget(root.server, root.identity.*, s, target);
}

const Empty = struct {
    var marker: u8 = 0;
    fn next(_: *anyopaque, _: A, _: u32) !catalog.Page {
        return .{ .rows = &.{} };
    }
    fn close(_: *anyopaque) void {}
    fn cursor() catalog.Cursor {
        return .{ .ptr = &marker, .next = next, .close = close, .order_satisfied = true };
    }
};

pub fn openRead(root: *Root, a: A, table: catalog.Table, request: catalog.Scan) !catalog.Cursor {
    try verifyRead(root, a, table);
    if (request.index_equality != null or request.text_predicate != null or request.search != null or request.graph != null) return error.UnsupportedSqlShape;
    const filter = try @import("read_authority.zig").filter(a, table, root.identity.*);
    defer if (filter) |bytes| a.free(bytes);
    const empty: @import("../../transactions.zig").OwnedTransactionCommitRequest = .{};
    return @import("../session/session_overlay.zig").openAtVersion(a, Empty.cursor(), root.staged orelse &empty, table, request, filter, null, root.active_write_version);
}

const Read = struct {
    allocator: A,
    native: ?catalog.StatementRead = null,
    cursors: []catalog.Cursor,
    private: []bool,
    initialized: usize = 0,
    fn close(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        for (self.cursors[0..self.initialized], self.private[0..self.initialized]) |cursor, owned| if (owned) cursor.close(cursor.ptr);
        if (self.native) |native| native.close(native.ptr);
        const a = self.allocator;
        a.free(self.cursors);
        a.free(self.private);
        a.destroy(self);
    }
};

/// Keep all published sources in one normal statement snapshot. Private
/// sources observe only their transaction's immutable staged postimages.
pub fn openStatement(root: *Root, a: A, requests: []const catalog.StatementScan) anyerror!?catalog.StatementRead {
    const has_private = for (requests) |request| {
        if (request.table.private_generation != null) break true;
    } else false;
    if (!has_private) return null;
    if (requests.len > 64) return error.SqlProgramLimitExceeded;
    var published: std.ArrayList(catalog.StatementScan) = .empty;
    defer published.deinit(a);
    for (requests) |request| {
        if (request.table.private_generation != null) try verifyRead(root, a, request.table) else try published.append(a, request);
    }
    const cursors = try a.alloc(catalog.Cursor, requests.len);
    errdefer a.free(cursors);
    const private = try a.alloc(bool, requests.len);
    errdefer a.free(private);
    const read = try a.create(Read);
    read.* = .{ .allocator = a, .cursors = cursors, .private = private };
    // Read.close owns the allocations after initialization, including errors.
    errdefer {
        for (read.cursors[0..read.initialized], read.private[0..read.initialized]) |cursor, owned| if (owned) cursor.close(cursor.ptr);
        if (read.native) |native| native.close(native.ptr);
        a.destroy(read);
    }
    if (published.items.len != 0) read.native = try Root.openStatement(root, a, published.items);
    var index: usize = 0;
    for (requests, cursors, private) |request, *cursor, *owned| {
        owned.* = request.table.private_generation != null;
        cursor.* = if (owned.*) try openRead(root, a, request.table, request.request) else blk: {
            const value = read.native.?.cursors[index];
            index += 1;
            break :blk value;
        };
        read.initialized += 1;
    }
    return .{ .ptr = read, .cursors = cursors, .close = Read.close };
}
