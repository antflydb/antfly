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

//! SQL admission to the existing catalog transaction/outcome lane. No table
//! records are manufactured and an uncertain mutation is never re-proposed.
const std = @import("std");
const sources = @import("antfly_local_sources");
const ast = sources.sql_ast;
const catalog = sources.sql_catalog;
const domain = sources.system_catalog_domain;
const seq = @import("../system_catalog/sequence_catalog.zig");
const http = @import("http_server.zig");
const Path = @import("../pgwire/search_path.zig").Path;
const A = std.mem.Allocator;
const Context = sources.api_operation.RequestContext;
const recovery = @import("sql_catalog_recovery.zig");
const authorize = @import("sql_catalog_auth.zig").authorizeTarget;

fn snapshot(server: *http.ApiHttpServer, a: A, context: Context, request: seq.SnapshotRequest) !seq.Snapshot {
    try context.ensureActive();
    const bytes = try server.source.systemCatalog(a, context, .{ .sql_sequence_snapshot = request });
    const result = std.json.parseFromSliceLeaky(seq.Snapshot, a, bytes, .{ .allocate = .alloc_always }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidSqlBackendResponse,
    };
    try result.validate(request);
    return result;
}

fn integerType(intent: ast.SequenceOptions) !?seq.sequence.Integer {
    const name = intent.data_type orelse return null;
    // Named/custom-type resolution must use the future type catalog, never
    // guess an integer layout from a user schema's spelling.
    if (name.database != null or (name.namespace != null and !std.mem.eql(u8, name.namespace.?, "pg_catalog"))) return error.UnsupportedSqlExecution;
    if (std.mem.eql(u8, name.table, "int2")) return .int16;
    if (std.mem.eql(u8, name.table, "int4")) return .int32;
    if (std.mem.eql(u8, name.table, "int8")) return .int64;
    if (name.namespace == null and !intent.data_type_quoted) {
        if (std.mem.eql(u8, name.table, "smallint")) return .int16;
        if (std.mem.eql(u8, name.table, "integer") or std.mem.eql(u8, name.table, "int")) return .int32;
        if (std.mem.eql(u8, name.table, "bigint")) return .int64;
    }
    for ([_][]const u8{ "smallint", "integer", "int", "bigint" }) |alias|
        if (std.mem.eql(u8, name.table, alias)) return error.SqlTypeNotFound;
    for ([_][]const u8{ "text", "bool", "numeric", "uuid", "jsonb", "float4", "float8" }) |known|
        if (std.mem.eql(u8, name.table, known)) return error.InvalidSequenceDefinition;
    return error.UnsupportedSqlExecution;
}

fn validateOptions(creating: bool, entry: seq.Snapshot.Entry, options: seq.sequence.Options) !void {
    if (creating) {
        _ = try seq.sequence.Record.create(options);
    } else {
        const prior: seq.sequence.Record = .{ .definition = try entry.value.sequence.definition(), .state = entry.counter orelse return error.InvalidSqlBackendResponse };
        _ = try prior.alter(options);
    }
}

fn selected(cut: seq.Snapshot, creating: bool, path_lookup: bool, conditional: bool) !?seq.Snapshot.Entry {
    for (cut.entries) |entry| switch (entry.value) {
        .missing_database => return error.DatabaseNotFound,
        .missing_namespace => if (!path_lookup) {
            if (!creating and conditional) return null;
            return error.NamespaceNotFound;
        },
        .absent => if (creating) return entry,
        .sequence, .other => return entry,
    };
    if (creating) return error.NamespaceNotFound;
    return null;
}

fn knownObject(entry: seq.Snapshot.Entry, creating: bool, conditional: bool) !bool {
    if (creating) {
        if (entry.value != .absent) {
            if (conditional) return true;
            return error.CatalogAlreadyExists;
        }
    } else if (entry.value == .other) return error.SqlWrongObjectType;
    return false;
}

pub fn execute(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, context: Context, database: []const u8, namespace: []const u8, allocators: catalog.DdlAllocators, ddl: ast.CatalogDdl, path: ?*const Path) !catalog.DdlOutcome {
    try context.ensureActive();
    // Only native metadata owners with portable allocator-state restoration
    // and exact public outcome recovery advertise this capability.
    if (!server.source.vtable.supports_sql_sequences or !server.source.vtable.supports_query_definitions) return error.UnsupportedSqlExecution;
    if (ddl.kind != .sequence or (ddl.action != .create and ddl.action != .drop and ddl.action != .alter_sequence and ddl.action != .rename) or ddl.sequence_targets.len > 1) return error.UnsupportedSqlExecution;
    if (server.cfg.auth_enabled and identity == null) return error.Forbidden;
    var arena = std.heap.ArenaAllocator.init(allocators.scratch);
    defer arena.deinit();
    const a = arena.allocator();
    const creating = ddl.action == .create;
    const path_lookup = ddl.name.database == null and ddl.name.namespace == null and path != null;
    const count: usize = if (path_lookup) path.?.len else 1;
    if (count > Path.max_namespaces) return error.InvalidParameter;
    var targets: [Path.max_namespaces]domain.Target = undefined;
    var used: usize = 0;
    for (0..count) |n| {
        const target: domain.Target = .{ .database = ddl.name.database orelse database, .namespace = if (path_lookup) path.?.entries[n].slice() else ddl.name.namespace orelse namespace, .table = ddl.name.table };
        try target.validate();
        if (!path_lookup) try authorize(server, identity, a, target) else if (!try @import("sql_catalog_auth.zig").namespaceUsage(identity, a, target)) continue;
        targets[used] = target;
        used += 1;
    }
    if (used == 0) {
        if (!creating and ddl.conditional) return .{};
        return if (creating) error.NamespaceNotFound else error.SequenceNotFound;
    }
    var private = context;
    private.setting_admin = true;
    var cut = try snapshot(server, a, private, .{ .targets = targets[0..used], .include_counters = ddl.action == .alter_sequence });
    var entry = (try selected(cut, creating, path_lookup, ddl.conditional)) orelse {
        if (ddl.conditional) return .{};
        return error.SequenceNotFound;
    };
    try authorize(server, identity, a, entry.target);
    if (try knownObject(entry, creating, ddl.conditional)) return .{};
    const expected: ?seq.Identity = if (entry.value == .sequence) .{ .id = entry.value.sequence.id, .generation = entry.value.sequence.generation } else null;
    var options: seq.sequence.Options = .{};
    var guard: ?domain.RelationMutationGuard = null;
    if (creating or ddl.action == .alter_sequence) {
        const intent = ddl.sequence_options orelse return error.InvalidSqlSyntax;
        var unowned = intent;
        unowned.owned_by = .unchanged;
        options = try unowned.bind(.{ .integer = try integerType(intent) });
        // Parameter errors precede owner-name resolution, with the complete
        // counter/definition cut pinned for ALTER; commit still revalidates it.
        try validateOptions(creating, entry, options);
        if (intent.owned_by == .column) {
            const owner = intent.owned_by.column;
            const target: domain.Target = .{ .database = owner.table.database orelse database, .namespace = owner.table.namespace orelse namespace, .table = owner.table.table };
            try target.validate();
            if (owner.table.database != null or owner.table.namespace != null or path == null) try authorize(server, identity, a, target);
            const captured = (try @import("sql_catalog.zig").captureTableAdmission(server, identity, context, a, target, owner.table.database != null or owner.table.namespace != null, path)) orelse return error.TableNotFound;
            try authorize(server, identity, a, .{ .database = captured.guard.target.database, .namespace = captured.guard.target.namespace, .table = captured.guard.logical_table });
            const fresh = try snapshot(server, a, private, .{ .targets = &.{entry.target}, .include_counters = ddl.action == .alter_sequence, .owner = .{ .guard = captured.guard, .column = owner.column } });
            const next = fresh.entries[0];
            if (!std.meta.eql(cut.incarnation, fresh.incarnation) or entry.namespace_id != next.namespace_id) return error.CatalogGenerationChanged;
            if (!creating and (next.value != .sequence or next.value.sequence.id != expected.?.id or next.value.sequence.generation != expected.?.generation)) return error.CatalogGenerationChanged;
            if (try knownObject(next, creating, ddl.conditional)) return .{};
            const bound = fresh.owner orelse return error.UndefinedColumn;
            if (bound.namespace_id != next.namespace_id or !std.mem.eql(u8, captured.guard.target.database, entry.target.database) or !std.mem.eql(u8, captured.guard.target.namespace, entry.target.namespace)) return error.SqlSequenceOwnershipScope;
            options = try intent.bind(.{ .integer = try integerType(intent), .owner = bound.column });
            guard = captured.guard;
            cut = fresh;
            entry = next;
            try validateOptions(creating, entry, options);
        } else options.owned_by = if (intent.owned_by == .none) .none else .unchanged;
    }
    if (ddl.new_name) |name| {
        var destination = entry.target;
        destination.table = name;
        try destination.validate();
        try authorize(server, identity, a, destination);
    }
    try context.ensureActive();
    try authorize(server, identity, a, entry.target);
    if (guard) |owner| try authorize(server, identity, a, .{ .database = owner.target.database, .namespace = owner.target.namespace, .table = owner.logical_table });
    const io = server.sharedApiIo() orelse return error.UnsupportedSqlExecution;
    // Only the fully validated, authorized mutating request may activate the
    // decoder. Read-only name binding and conditional no-ops stay read-only.
    const prepared = try server.source.systemCatalog(a, private, .sql_sequence_prepare);
    a.free(prepared);
    try context.ensureActive();
    var nonce: [16]u8 = undefined;
    try io.randomSecure(&nonce);
    const request: seq.Mutation = .{
        .admission = .{ .nonce = nonce, .head = cut.head },
        .target = entry.target,
        .namespace_id = entry.namespace_id orelse return error.InvalidSqlBackendResponse,
        .incarnation = cut.incarnation,
        .owner_guard = guard,
        .operation = switch (ddl.action) {
            .create => .{ .create = .{ .options = options, .if_not_exists = ddl.conditional } },
            .alter_sequence => .{ .alter = .{ .expected = expected.?, .options = options } },
            .rename => .{ .rename = .{ .expected = expected.?, .name = ddl.new_name orelse return error.InvalidSqlSyntax } },
            .drop => .{ .drop = .{ .expected = expected, .if_exists = ddl.conditional, .cascade = ddl.cascade } },
            else => unreachable,
        },
    };
    try request.validate();
    try context.ensureActive();
    // Own every recovery string before entering a possibly committed call.
    // Neither completing the receipt nor returning unknown allocates afterward.
    var receipt = try recovery.newReceipt(allocators.result, a, request, cut.revision);
    errdefer receipt.deinit(allocators.result);
    try context.ensureActive();
    const bytes = server.source.systemCatalog(a, private, .{ .sql_sequence_mutate = request }) catch null;
    // A successful transport may still carry an exact known rejection.
    // Every transport/decode/allocation/cancellation failure after entering
    // mutation is ambiguous. Observe the SAME bound nonce once, within the
    // original request budget; never prepare/re-propose or infer from names.
    var outcome = if (bytes) |value| recovery.decodeOutcome(a, value, request, cut.revision) else null;
    if (outcome == null) outcome = recovery.observe(server, identity, context, a, request, cut.revision);
    if (outcome) |known| {
        _ = try known.result();
        recovery.complete(&receipt, known);
        return .{ .catalog_receipt = receipt };
    }
    return .{ .mutation_outcome = null, .catalog_receipt = receipt };
}
