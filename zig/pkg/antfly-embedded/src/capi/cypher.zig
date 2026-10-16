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

//! Public embedded graph transport over the shared compiler, catalog, values,
//! and bounded immutable graph materialization.
const std = @import("std");
const h = @import("handles.zig");
const api = @import("db.zig");
const cypher = @import("cypher");
const catalog = @import("graph_catalog.zig");
const DB = h.db_mod.DB;
const A = std.mem.Allocator;

fn available(handle: *h.Handle) bool {
    return handle.parent_id == null and handle.storage_owner_context == null and
        handle.storage_owner_path == null and handle.storage_owner_group_id == 0 and
        handle.readable_lease_hook == null;
}
fn failure(out: *h.capi.Buffer, err: anyerror, diagnostic: cypher.compiler.Diagnostic) h.capi.ErrorCode {
    return failureScope(out, err, diagnostic, .{});
}
fn failureScope(out: *h.capi.Buffer, err: anyerror, diagnostic: cypher.compiler.Diagnostic, scope: @import("cypher_write.zig").Scope) h.capi.ErrorCode {
    out.* = api.stringifyJson(.{ .code = @errorName(err), .message = if (diagnostic.message.len != 0) diagnostic.message else @errorName(err), .span = diagnostic.span, .write_outcome = scope.outcome, .transaction_id = if (scope.transaction_id) |id| @as(?[32]u8, std.fmt.bytesToHex(id, .lower)) else null }) catch return .internal;
    return switch (err) {
        error.RowPolicyAuthenticationRequired, error.CypherMutationsUnavailable, error.CypherSearchUnavailable => .unsupported,
        error.CatalogNotFound, error.IndexNotFound, error.UndefinedTable => .not_found,
        error.CatalogGenerationChanged, error.CypherTransactionConflict, error.CypherWriteRequestAlreadyUsed, error.VersionConflict, error.SqlWriteConflict => .version_conflict,
        error.SqlMutationOutcomeUnknown => .outcome_unknown,
        error.OutOfMemory => .internal,
        error.CypherMemoryBudgetExceeded, error.CypherSnapshotNotReady => .busy,
        error.ReadOnly => .unsupported,
        else => .invalid_argument,
    };
}
comptime {
    if (!@import("../storage/engine_selection.zig").enabled) {
        @export(&antfly_db_graph_catalog_json, .{ .name = "antfly_db_graph_catalog_json", .linkage = .strong });
        @export(&antfly_db_cypher_json, .{ .name = "antfly_db_cypher_json", .linkage = .strong });
        @export(&antfly_db_graph_write_status_json, .{ .name = "antfly_db_graph_write_status_json", .linkage = .strong });
    }
}
pub fn antfly_db_graph_catalog_json(raw: ?*anyopaque, request: h.capi.Slice, out: *h.capi.Buffer) callconv(.c) h.capi.ErrorCode {
    out.* = .{};
    const guard = api.enterHandle(raw, .exclusive) orelse return .invalid_argument;
    defer guard.leave();
    if (guard.entry_error) |code| return code;
    if (!available(guard.handle)) return .unsupported;
    var lease = guard.handle.db.local_execution.row_policy_gate.enterRaw() catch |err| return failure(out, err, .{});
    defer lease.release();
    catalog.execute(guard.handle, request.bytes(), out) catch |err| return failure(out, err, .{});
    return .ok;
}
pub fn antfly_db_cypher_json(raw: ?*anyopaque, name: h.capi.Slice, request: h.capi.Slice, out: *h.capi.Buffer) callconv(.c) h.capi.ErrorCode {
    out.* = .{};
    const guard = api.enterHandle(raw, .exclusive) orelse return .invalid_argument;
    defer guard.leave();
    if (guard.entry_error) |code| return code;
    if (!available(guard.handle)) return .unsupported;
    var diagnostic: cypher.compiler.Diagnostic = .{};
    var scope: @import("cypher_write.zig").Scope = .{};
    execute(guard.handle, name.bytes(), request.bytes(), out, &diagnostic, &scope) catch |err| return failureScope(out, err, diagnostic, scope);
    return .ok;
}

pub fn antfly_db_graph_write_status_json(raw: ?*anyopaque, transaction_id: h.capi.Slice, out: *h.capi.Buffer) callconv(.c) h.capi.ErrorCode {
    out.* = .{};
    const guard = api.enterHandle(raw, .read) orelse return .invalid_argument;
    defer guard.leave();
    if (guard.entry_error) |code| if (code != .outcome_unknown) return code;
    if (!available(guard.handle)) return .unsupported;
    const id = @import("cypher_write.zig").parseId(transaction_id.bytes()) catch |err| return failure(out, err, .{});
    const status: ?h.transactions_mod.TxnStatus = guard.handle.db.getTransactionStatus(id) catch |err| switch (err) {
        error.TxnNotFound => null,
        else => return failure(out, err, .{}),
    };
    if (status == null and !(@import("sql_commit.zig").hasAdmission(guard.handle, id) catch |err| return failure(out, err, .{}))) return .not_found;
    const disposition: enum { committed, aborted, outcome_unknown } = if (status) |value| switch (value) {
        .committed => .committed,
        .aborted => .aborted,
        .pending => .outcome_unknown,
    } else .outcome_unknown;
    out.* = api.stringifyJson(.{ .transaction_id = std.fmt.bytesToHex(id, .lower), .disposition = disposition, .durable = disposition != .outcome_unknown, .visibility_pending = disposition == .committed and (@import("sql_commit.zig").hasAdmission(guard.handle, id) catch true), .visibility_repair_required = false }) catch return .internal;
    return .ok;
}

pub const Read = struct {
    handle: *h.Handle,
    graph: cypher.catalog.Graph,
    owners: []const *DB,
    leases: []DB.CypherReadLease,
    tables: []DB.CypherBackend.Table,
    root_lease: *DB.CypherReadLease,
    schemas: []const u32,
    pinned: []PinnedDB = &.{},
    pin_arena: ?std.heap.ArenaAllocator = null,
    pinned_adapter: PinnedAdapter = undefined,
    const PinnedDB = DB.CypherPinnedDatabase;
    const PinnedAdapter = cypher.native.Adapter(PinnedDB, void, DB.CypherEdgeScanCursor);
    pub fn pin(self: *Read, a: A, owners: []const *DB, fences: []const DB.StatementReadFence) !void {
        self.pin_arena = std.heap.ArenaAllocator.init(a);
        errdefer {
            self.pin_arena.?.deinit();
            self.pin_arena = null;
        }
        const owned = self.pin_arena.?.allocator();
        const now = @import("antfly_platform").time.realtimeNs();
        const pinned = try owned.alloc(PinnedDB, owners.len);
        var initialized: usize = 0;
        errdefer for (pinned[0..initialized]) |*entry| entry.cut.deinit();
        for (owners, fences) |owner, *fence| {
            const table = for (self.graph.labels) |label| {
                if ((try @import("tables.zig").get(self.handle, label.table)) == owner) break label.table;
            } else continue;
            const entry = &pinned[initialized];
            entry.* = .{ .source = owner, .core = owner.core, .cut = try owner.captureCypherPinnedSnapshot(owned, fence, self.graph, table) };
            entry.cut.now = now;
            initialized += 1;
        }
        for (self.graph.labels, self.tables) |label, *table| table.* = .{ .name = label.table, .db = try @import("tables.zig").get(self.handle, label.table) };
        const tables = try owned.alloc(PinnedAdapter.Table, self.graph.labels.len);
        for (self.graph.labels, tables) |label, *table| {
            const source = try @import("tables.zig").get(self.handle, label.table);
            const owner = for (pinned[0..initialized]) |*entry| {
                if (entry.source == source) break entry;
            } else return error.CatalogGenerationChanged;
            table.* = .{ .name = label.table, .storage_name = try @import("tables.zig").storageName(self.handle, label.table), .db = owner };
        }
        self.pinned_adapter = .{ .tables = tables, .ttl_now_ns = now, .authority = .{ .ptr = self, .bind = bind, .checkpoint = checkpoint, .project = project, .project_relationship = projectRelationship } };
        self.pinned = pinned[0..initialized];
    }
    pub fn unpin(self: *Read) void {
        for (self.pinned) |*entry| entry.cut.deinit();
        self.pinned = &.{};
        if (self.pin_arena) |*arena| arena.deinit();
        self.pin_arena = null;
    }
    pub fn pinnedBackend(self: *Read) cypher.backend.Backend {
        return .{ .ptr = self, .vtable = &.{ .bind = pinnedBind, .checkpoint = checkpoint, .scan = pinnedScan, .scan_where = pinnedScanWhere, .get = pinnedGet, .expand = pinnedExpand, .set_work = pinnedWork, .set_read_fields = pinnedReadFields, .normalize_node = normalize, .document_relationships = documentRelationships } };
    }
    fn pinnedBind(ptr: *anyopaque, a: A, graph: cypher.catalog.Graph) !void {
        const backend = cast(ptr).pinned_adapter.backend();
        try backend.vtable.bind(backend.ptr, a, graph);
    }
    fn pinnedReadFields(ptr: *anyopaque, fields: ?[]const []const u8) void {
        const backend = cast(ptr).pinned_adapter.backend();
        backend.vtable.set_read_fields.?(backend.ptr, fields);
    }
    fn pinnedWork(ptr: *anyopaque, work: ?cypher.backend.Work) void {
        const backend = cast(ptr).pinned_adapter.backend();
        backend.vtable.set_work.?(backend.ptr, work);
    }
    fn pinnedScan(ptr: *anyopaque, a: A, table: []const u8, after: ?[]const u8, limit: usize) !cypher.backend.Page(*const cypher.value.Node) {
        const backend = cast(ptr).pinned_adapter.backend();
        return backend.vtable.scan(backend.ptr, a, table, after, limit);
    }
    fn pinnedScanWhere(ptr: *anyopaque, a: A, table: []const u8, filters: []const cypher.backend.PropertyFilter, after: ?[]const u8, limit: usize) !cypher.backend.Page(*const cypher.value.Node) {
        const backend = cast(ptr).pinned_adapter.backend();
        return backend.vtable.scan_where.?(backend.ptr, a, table, filters, after, limit);
    }
    fn pinnedGet(ptr: *anyopaque, a: A, identity: cypher.value.Identity) !?*const cypher.value.Node {
        const backend = cast(ptr).pinned_adapter.backend();
        return backend.vtable.get(backend.ptr, a, identity);
    }
    fn pinnedExpand(ptr: *anyopaque, a: A, mapping: cypher.catalog.Relationship, identity: cypher.value.Identity, direction: cypher.ast.Direction, after: ?[]const u8, limit: usize) !cypher.backend.Page(*const cypher.value.Relationship) {
        const backend = cast(ptr).pinned_adapter.backend();
        return backend.vtable.expand(backend.ptr, a, mapping, identity, direction, after, limit);
    }
    fn cast(ptr: *anyopaque) *Read {
        return @ptrCast(@alignCast(ptr));
    }
    fn bind(ptr: *anyopaque, graph: cypher.catalog.Graph) !void {
        const self = cast(ptr);
        if (graph.incarnation != self.graph.incarnation or graph.revision != self.graph.revision) return error.CatalogGenerationChanged;
        try catalog.validateOwners(self.handle.alloc, self.handle, graph);
        try checkpoint(ptr);
    }
    fn checkpoint(ptr: *anyopaque) !void {
        const self = cast(ptr);
        const now = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_s;
        for (self.leases) |*lease| try lease.checkAt(@intCast(now));
        try self.root_lease.checkAt(@intCast(now));
        for (self.owners, self.schemas) |owner, version| {
            var view = owner.core.acquireSchemaView();
            defer if (view) |*value| value.release();
            if ((if (view) |value| value.tableSchema().version else 0) != version) return error.CatalogGenerationChanged;
        }
    }
    pub fn validate(self: *Read) !void {
        try catalog.validateOwners(self.handle.alloc, self.handle, self.graph);
        try checkpoint(self);
    }
    fn normalize(ptr: *anyopaque, a: A, node: *const cypher.value.Node, defaults: bool) !*const cypher.value.Node {
        const self = cast(ptr);
        try checkpoint(ptr);
        return (try @import("tables.zig").get(self.handle, node.identity.table)).normalizeCypherNode(a, self.graph, node, defaults);
    }
    fn documentRelationships(ptr: *anyopaque, a: A, node: *const cypher.value.Node) ![]const *const cypher.value.Relationship {
        const self = cast(ptr);
        try checkpoint(ptr);
        return (try @import("tables.zig").get(self.handle, node.identity.table)).cypherDocumentRelationships(a, self.graph, node);
    }
    fn project(_: *anyopaque, _: A, node: *const cypher.value.Node) !?*const cypher.value.Node {
        return node;
    }
    fn projectRelationship(_: *anyopaque, _: A, edge: *const cypher.value.Relationship) !?*const cypher.value.Relationship {
        return edge;
    }
};

pub fn execute(handle: *h.Handle, name: []const u8, json: []const u8, out: *h.capi.Buffer, diagnostic: *cypher.compiler.Diagnostic, scope: *@import("cypher_write.zig").Scope) !void {
    if (handle.sql_decision_uncertain) return error.SqlMutationOutcomeUnknown;
    var arena = std.heap.ArenaAllocator.init(handle.alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var request = try cypher.request.Request.parse(a, json, .{});
    defer request.deinit();
    var compiled = try cypher.compiler.compile(a, request.query, .{}, diagnostic);
    defer compiled.deinit();
    const graph = try catalog.graph(a, try catalog.get(a, handle, name));
    try catalog.validateOwners(a, handle, graph);
    try cypher.binder.validate(a, graph, compiled.query);
    const writes = cypher.runtime.queryHasWrites(compiled.query) and !compiled.query.explain;
    if (writes and !h.liteOpenModeCanWrite(handle.open_mode)) return error.ReadOnly;
    var root_lease = try handle.db.acquireCypherReadAuthority(a, "", "default");
    defer root_lease.release();
    var owners: std.ArrayList(*DB) = .empty;
    for (graph.labels) |label| {
        const db = try @import("tables.zig").get(handle, label.table);
        const exists = for (owners.items) |owner| {
            if (owner == db) break true;
        } else false;
        if (!exists) try owners.append(a, db);
    }
    std.mem.sort(*DB, owners.items, {}, struct {
        fn less(_: void, left: *DB, right: *DB) bool {
            return left.core.identity_namespace.table_id < right.core.identity_namespace.table_id;
        }
    }.less);
    const leases = try a.alloc(DB.CypherReadLease, owners.items.len);
    var admitted: usize = 0;
    defer for (leases[0..admitted]) |*lease| lease.release();
    for (owners.items, leases) |db, *lease| {
        lease.* = try db.acquireCypherReadAuthority(a, "", "default");
        admitted += 1;
        if (db == &handle.db and db.local_execution.row_policy_table_name == null) db.local_execution.row_policy_table_name = "default";
        if (writes) try db.batch(.{ .activate_range_tracking = true });
    }
    const schemas = try a.alloc(u32, owners.items.len);
    for (owners.items, schemas) |db, *version| version.* = if (db.core.schema) |schema| schema.version else 0;
    var read: Read = .{ .handle = handle, .graph = graph, .owners = owners.items, .leases = leases, .tables = try a.alloc(DB.CypherBackend.Table, graph.labels.len), .root_lease = &root_lease, .schemas = schemas };
    if (compiled.query.explain) {
        const metadata: cypher.backend.Backend = .{ .ptr = &read, .vtable = &.{ .bind = metadataBind, .checkpoint = Read.checkpoint, .scan = noScan, .get = noGet, .expand = noExpand } };
        var result = try cypher.runtime.execute(handle.alloc, metadata, graph, &compiled, request.parameters, request.limits);
        defer result.deinit();
        const bytes = try cypher.bounded_json.encode(std.heap.c_allocator, result, cypher.runtime.max_response_bytes, null);
        out.* = .{ .ptr = bytes.ptr, .len = bytes.len };
        return;
    }
    const io = handle.db.backend_runtime.io() orelse return error.BackendRuntimeIoUnavailable;
    // Drain derived publications before freezing replay. Capturing the primary
    // beside an older graph index would otherwise silently omit relationships.
    for (owners.items) |db| {
        if (h.liteOpenModeCanWrite(handle.open_mode)) {
            _ = try db.lookup(a, "", .{ .cypher_prepare = true });
        } else try db.waitForCurrentSyncLevel(.full_index);
    }
    const fences = try a.alloc(DB.StatementReadFence, owners.items.len);
    var held: usize = 0;
    defer for (fences[0..held]) |*fence| fence.release();
    const deadline = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(5));
    while (held != owners.items.len) {
        if (try owners.items[held].tryStatementReadFence()) |fence| {
            fences[held] = fence;
            held += 1;
            continue;
        }
        for (fences[0..held]) |*fence| fence.release();
        held = 0;
        if (std.Io.Clock.awake.now(io).nanoseconds >= deadline.nanoseconds) return error.CypherSnapshotNotReady;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try read.pin(a, owners.items, fences);
    defer read.unpin();
    const guard_type = @import("sql_commit.zig").GraphGuard;
    const guards = try a.alloc(guard_type, if (writes) owners.items.len else 0);
    for (owners.items[0..guards.len], guards, schemas[0..guards.len]) |db, *guard, version| {
        const table = for (read.tables) |entry| {
            if (entry.db == db) break entry.name;
        } else unreachable;
        var txn = try db.core.store.beginReadTxn();
        defer txn.abort();
        guard.* = .{ .table_name = table, .table_id = db.core.identity_namespace.table_id, .schema_version = version, .proofs = try @import("../storage/range_protection.zig").capture(a, &txn, "", "") };
    }
    for (fences[0..held]) |*fence| fence.release();
    held = 0;
    var writer: @import("cypher_write.zig").Writer = .{ .read = &read, .guards = guards, .scope = scope, .request_id = request.write_request_id };
    var overlay: cypher.transaction.Overlay = .{ .lazy = true, .retain_scan_preimages = true, .base = read.pinnedBackend(), .host = writer.host(), .max_entities = request.limits.intermediate_states, .max_anchors = request.limits.scanned_anchors, .max_edges = request.limits.explored_edges };
    writer.overlay = &overlay;
    var result = try cypher.runtime.execute(handle.alloc, if (writes) overlay.backend() else read.pinnedBackend(), graph, &compiled, request.parameters, request.limits);
    defer result.deinit();
    if (writes) {
        var response = try std.json.parseFromSliceLeaky(std.json.Value, a, try std.json.Stringify.valueAlloc(a, result, .{}), .{});
        const receipt = try std.json.parseFromSliceLeaky(std.json.Value, a, try std.json.Stringify.valueAlloc(a, .{ .transaction_id = std.fmt.bytesToHex(scope.transaction_id.?, .lower), .status = writer.outcome, .outcome = writer.outcome }, .{}), .{});
        try response.object.put(a, "commit", receipt);
        const bytes = try cypher.bounded_json.encode(std.heap.c_allocator, response, cypher.runtime.max_response_bytes, null);
        out.* = .{ .ptr = bytes.ptr, .len = bytes.len };
        return;
    }
    const bytes = try cypher.bounded_json.encode(std.heap.c_allocator, result, cypher.runtime.max_response_bytes, .{ .ptr = &read, .call = Read.checkpoint });
    errdefer std.heap.c_allocator.free(bytes);
    try Read.checkpoint(&read);
    out.* = .{ .ptr = bytes.ptr, .len = bytes.len };
}
fn metadataBind(ptr: *anyopaque, _: A, graph: cypher.catalog.Graph) !void {
    try Read.bind(ptr, graph);
}
fn noScan(_: *anyopaque, _: A, _: []const u8, _: ?[]const u8, _: usize) !cypher.backend.Page(*const cypher.value.Node) {
    return error.CypherSnapshotRequired;
}
fn noGet(_: *anyopaque, _: A, _: cypher.value.Identity) !?*const cypher.value.Node {
    return error.CypherSnapshotRequired;
}
fn noExpand(_: *anyopaque, _: A, _: cypher.catalog.Relationship, _: cypher.value.Identity, _: cypher.ast.Direction, _: ?[]const u8, _: usize) !cypher.backend.Page(*const cypher.value.Relationship) {
    return error.CypherSnapshotRequired;
}

test "capi embedded graph catalog and query retain table identities across reopen" {
    const a = std.testing.allocator;
    var directory = try api.TestDirectory.init("embedded-cypher");
    defer directory.cleanup();
    try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
    const path = try std.fmt.allocPrintSentinel(a, "{s}/graph.aflite", .{directory.path()}, 0);
    defer a.free(path);
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_lite_create_hosted(path, &handle));
    defer if (handle != null) api.antfly_db_close(handle);
    for ([_][]const u8{
        "{\"statement\":\"CREATE TABLE docs (id TEXT, title TEXT)\"}",
        "{\"statement\":\"INSERT INTO docs (_id,id,title) VALUES ('a','a','Alpha'),('b','b','Beta')\"}",
    }) |request| {
        var output: h.capi.Buffer = .{};
        defer api.antfly_buffer_free(&output);
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_sql_json(handle, .fromSlice(request), &output));
    }
    {
        var table: ?*anyopaque = null;
        try std.testing.expectEqual(h.capi.ErrorCode.ok, @import("tables.zig").antfly_db_open_table(handle, .fromSlice("docs"), &table));
        defer api.antfly_db_close(table);
        const index = "{\"name\":\"links\",\"kind\":\"graph\",\"config_json\":\"{}\"}";
        const index_code = api.antfly_db_add_index_json(table, .fromSlice(index));
        if (index_code != .ok) std.debug.print("embedded graph index setup failed: {s}\n", .{@tagName(index_code)});
        try std.testing.expectEqual(h.capi.ErrorCode.ok, index_code);
    }
    const definition = "{\"action\":\"put\",\"name\":\"knowledge\",\"definition\":{\"labels\":[{\"name\":\"Document\",\"table\":\"docs\"}],\"relationships\":[{\"name\":\"KNOWS\",\"index\":\"links\",\"source_label\":\"Document\",\"target_label\":\"Document\",\"stored_type\":\"knows\"}]}}";
    {
        var output: h.capi.Buffer = .{};
        defer api.antfly_buffer_free(&output);
        const catalog_code = antfly_db_graph_catalog_json(handle, .fromSlice(definition), &output);
        if (catalog_code != .ok) std.debug.print("embedded graph catalog: {s}\n", .{if (output.ptr) |ptr| ptr[0..output.len] else "no diagnostic"});
        try std.testing.expectEqual(h.capi.ErrorCode.ok, catalog_code);
        try std.testing.expect(std.mem.indexOf(u8, output.ptr.?[0..output.len], "\"revision\":\"1\"") != null);
    }
    const query = "{\"query\":\"MATCH (n:Document) RETURN n.id ORDER BY n.id\"}";
    {
        var output: h.capi.Buffer = .{};
        defer api.antfly_buffer_free(&output);
        const read_code = antfly_db_cypher_json(handle, .fromSlice("knowledge"), .fromSlice(query), &output);
        if (read_code != .ok) std.debug.print("embedded graph read: {s}\n", .{if (output.ptr) |ptr| ptr[0..output.len] else "no diagnostic"});
        try std.testing.expectEqual(h.capi.ErrorCode.ok, read_code);
        try std.testing.expect(std.mem.indexOf(u8, output.ptr.?[0..output.len], "\"rows\":[[{\"type\":\"string\",\"value\":\"a\"}],[{\"type\":\"string\",\"value\":\"b\"}]]") != null);
    }
    const write_id = "00112233445566778899aabbccddeeff";
    const write = "{\"query\":\"MATCH (a:Document {id:'a'}),(b:Document {id:'b'}) CREATE (a)-[r:KNOWS {since:2020}]->(b) RETURN r.since\",\"write_request_id\":\"00112233445566778899aabbccddeeff\"}";
    {
        var output: h.capi.Buffer = .{};
        defer api.antfly_buffer_free(&output);
        const code = antfly_db_cypher_json(handle, .fromSlice("knowledge"), .fromSlice(write), &output);
        if (code != .ok) std.debug.print("embedded graph write: {s}\n", .{if (output.ptr) |ptr| ptr[0..output.len] else "no diagnostic"});
        try std.testing.expectEqual(h.capi.ErrorCode.ok, code);
        try std.testing.expect(std.mem.indexOf(u8, output.ptr.?[0..output.len], "\"status\":\"committed\"") != null);
        api.antfly_buffer_free(&output);
        try std.testing.expectEqual(h.capi.ErrorCode.version_conflict, antfly_db_cypher_json(handle, .fromSlice("knowledge"), .fromSlice(write), &output));
        api.antfly_buffer_free(&output);
        const rollback = "{\"query\":\"CREATE (n:Document {id:'aborted'}) RETURN 1/0\"}";
        try std.testing.expectEqual(h.capi.ErrorCode.invalid_argument, antfly_db_cypher_json(handle, .fromSlice("knowledge"), .fromSlice(rollback), &output));
    }
    api.antfly_db_close(handle);
    handle = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_lite_open(path, &handle));
    {
        var output: h.capi.Buffer = .{};
        defer api.antfly_buffer_free(&output);
        try std.testing.expectEqual(h.capi.ErrorCode.ok, antfly_db_cypher_json(handle, .fromSlice("knowledge"), .fromSlice(query), &output));
        api.antfly_buffer_free(&output);
        const edges = "{\"query\":\"MATCH (a:Document)-[r:KNOWS]->(b) RETURN a.id,r.since,b.id\"}";
        try std.testing.expectEqual(h.capi.ErrorCode.ok, antfly_db_cypher_json(handle, .fromSlice("knowledge"), .fromSlice(edges), &output));
        try std.testing.expect(std.mem.indexOf(u8, output.ptr.?[0..output.len], "\"type\":\"integer\",\"value\":\"2020\"") != null);
        api.antfly_buffer_free(&output);
        try std.testing.expectEqual(h.capi.ErrorCode.ok, antfly_db_graph_write_status_json(handle, .fromSlice(write_id), &output));
        try std.testing.expect(std.mem.indexOf(u8, output.ptr.?[0..output.len], "\"disposition\":\"committed\"") != null);
        api.antfly_buffer_free(&output);
        try std.testing.expectEqual(h.capi.ErrorCode.version_conflict, antfly_db_graph_catalog_json(handle, .fromSlice(definition), &output));
    }
    try std.testing.expectEqual(h.capi.ErrorCode.ok, @import("tables.zig").antfly_db_drop_table(handle, .fromSlice("docs")));
    {
        var output: h.capi.Buffer = .{};
        defer api.antfly_buffer_free(&output);
        const recreate = "{\"statement\":\"CREATE TABLE docs (id TEXT, title TEXT)\"}";
        try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_sql_json(handle, .fromSlice(recreate), &output));
        api.antfly_buffer_free(&output);
        try std.testing.expectEqual(h.capi.ErrorCode.version_conflict, antfly_db_cypher_json(handle, .fromSlice("knowledge"), .fromSlice(query), &output));
        api.antfly_buffer_free(&output);
        try std.testing.expectEqual(h.capi.ErrorCode.ok, antfly_db_graph_write_status_json(handle, .fromSlice(write_id), &output));
    }
}
