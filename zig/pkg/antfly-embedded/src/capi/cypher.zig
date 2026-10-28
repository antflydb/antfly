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
        error.CypherMemoryBudgetExceeded, error.CypherSnapshotNotReady, error.WouldBlock, error.FileBusy => .busy,
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
    var guard = api.enterHandle(raw, .exclusive) orelse return .invalid_argument;
    defer guard.leave();
    if (guard.entry_error) |code| return code;
    if (!available(guard.handle)) return .unsupported;
    var diagnostic: cypher.compiler.Diagnostic = .{};
    var scope: @import("cypher_write.zig").Scope = .{};
    var finish_arena = std.heap.ArenaAllocator.init(std.heap.c_allocator);
    defer finish_arena.deinit();
    var execution: DetachedRead = .{ .guard = &guard, .allocator = finish_arena.allocator() };
    executeInternal(guard.handle, name.bytes(), request.bytes(), out, &diagnostic, &scope, &execution) catch |err| return failureScope(out, err, diagnostic, scope);
    if (execution.graph != null) {
        // All old-root leases, watches and pinned cuts have been released by
        // executeInternal before resume can collect the retired root.
        if (!guard.resumeAccess(raw, .exclusive)) {
            api.antfly_buffer_free(out);
            return .invalid_argument;
        }
        if (guard.entry_error) |code| {
            api.antfly_buffer_free(out);
            return code;
        }
        execution.validate(guard.handle) catch |err| {
            api.antfly_buffer_free(out);
            return failureScope(out, err, diagnostic, scope);
        };
    }
    return .ok;
}

const DetachedRead = struct {
    guard: *api.HandleGuard,
    allocator: A,
    graph: ?cypher.catalog.Graph = null,
    owners: []const struct { table_id: u64, schema_version: u32 } = &.{},

    fn detach(self: *@This(), handle: *h.Handle, graph: cypher.catalog.Graph) !void {
        if (self.guard.connection == null) return;
        self.graph = try graph.clone(self.allocator);
        const owners = try self.allocator.alloc(@typeInfo(@TypeOf(self.owners)).pointer.child, graph.labels.len);
        for (graph.labels, owners) |label, *owner| {
            const db = try @import("tables.zig").get(handle, label.table);
            owner.* = .{ .table_id = db.core.identity_namespace.table_id, .schema_version = if (db.core.schema) |schema| schema.version else 0 };
        }
        self.owners = owners;
        self.guard.suspendImmutableQuery();
    }

    fn validate(self: *@This(), handle: *h.Handle) !void {
        const graph = self.graph.?;
        const current = catalog.get(self.allocator, handle, graph.name) catch |err| switch (err) {
            error.CatalogNotFound => return error.CatalogGenerationChanged,
            else => return err,
        };
        if (current.incarnation != graph.incarnation or current.revision != graph.revision) return error.CatalogGenerationChanged;
        try catalog.validateOwners(self.allocator, handle, graph);
        var root = try handle.db.acquireCypherReadAuthority(self.allocator, "", "default");
        defer root.release();
        for (graph.labels, self.owners) |label, owner| {
            const db = try @import("tables.zig").get(handle, label.table);
            if (db.core.identity_namespace.table_id != owner.table_id or (if (db.core.schema) |schema| schema.version else 0) != owner.schema_version) return error.CatalogGenerationChanged;
            var lease = try db.acquireCypherReadAuthority(self.allocator, "", "default");
            defer lease.release();
        }
    }
};

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
    const Invalidation = DB.CypherAuthorityInvalidation;
    cohort: Invalidation.Cohort = .{},
    checked_authority: ?u64 = null,
    watches: []Invalidation.Watch = &.{},
    watch_arena: ?std.heap.ArenaAllocator = null,
    expiry_seconds: ?i64 = null,
    deadline_ns: ?u64 = null,
    detached: ?*DetachedRead = null,
    detach_after_ns: ?u64 = null,
    partial_graph: bool = false,
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
    pub fn watch(self: *Read, a: A) !void {
        self.watch_arena = .init(a);
        errdefer {
            self.watch_arena.?.deinit();
            self.watch_arena = null;
        }
        const watches = try self.watch_arena.?.allocator().alloc(Invalidation.Watch, self.owners.len * 2 + 1);
        @memset(watches, .{});
        self.watches = watches;
        watches[0].attach(&self.root_lease.gate.authority_changes, &self.cohort);
        self.expiry_seconds = self.root_lease.expires_at_seconds;
        self.deadline_ns = self.root_lease.expires_at_monotonic_ns;
        for (self.owners, self.leases, 0..) |owner, *lease, index| {
            watches[index * 2 + 1].attach(&lease.gate.authority_changes, &self.cohort);
            watches[index * 2 + 2].attach(&owner.core.schema_registry.authority_changes, &self.cohort);
            if (lease.expires_at_seconds) |expiry| self.expiry_seconds = @min(self.expiry_seconds orelse expiry, expiry);
            if (lease.expires_at_monotonic_ns) |deadline| self.deadline_ns = @min(self.deadline_ns orelse deadline, deadline);
        }
    }
    pub fn unwatch(self: *Read) void {
        for (self.watches) |*entry| entry.detach();
        self.watches = &.{};
        if (self.watch_arena) |*arena| arena.deinit();
        self.watch_arena = null;
        self.checked_authority = null;
        self.detached = null;
        self.detach_after_ns = null;
    }
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
        var table_count: usize = 0;
        for (self.graph.labels) |label| {
            const source = try @import("tables.zig").get(self.handle, label.table);
            const owner = for (pinned[0..initialized]) |*entry| {
                if (entry.source == source) break entry;
            } else {
                if (!self.partial_graph) return error.CatalogGenerationChanged;
                continue;
            };
            const table = &tables[table_count];
            table_count += 1;
            table.* = .{ .name = label.table, .storage_name = try @import("tables.zig").storageName(self.handle, label.table), .db = owner };
        }
        self.pinned_adapter = .{ .partitioned = self.partial_graph, .tables = tables[0..table_count], .ttl_now_ns = now, .authority = .{ .ptr = self, .bind = bind, .checkpoint = checkpoint, .project = project, .project_relationship = projectRelationship, .ordered_projection = true } };
        self.pinned = pinned[0..initialized];
    }
    pub fn unpin(self: *Read) void {
        for (self.pinned) |*entry| entry.cut.deinit();
        self.pinned = &.{};
        if (self.pin_arena) |*arena| arena.deinit();
        self.pin_arena = null;
    }
    pub fn pinnedBackend(self: *Read) cypher.backend.Backend {
        return .{ .ptr = self, .vtable = &.{ .bind = pinnedBind, .checkpoint = checkpoint, .scan = pinnedScan, .scan_where = pinnedScanWhere, .scan_ordered = pinnedScanOrdered, .estimate_where = pinnedEstimateWhere, .get = pinnedGet, .expand = pinnedExpand, .expand_between = pinnedExpandBetween, .set_work = pinnedWork, .set_read_fields = pinnedReadFields, .normalize_node = normalize, .document_relationships = documentRelationships } };
    }
    fn readCallResult(comptime Callback: type) type {
        const Function = switch (@typeInfo(Callback)) {
            .pointer => |pointer| pointer.child,
            .@"fn" => Callback,
            else => @compileError("expected a read callback"),
        };
        return @typeInfo(Function).@"fn".return_type.?;
    }
    /// A detached reader can meet a short native publication/rewrite window.
    /// Retry only that operation on the same pinned cut, retaining its work and
    /// memory charges. Mutation statements keep their original error behavior.
    fn retryRead(self: *Read, callback: anytype, args: anytype) readCallResult(@TypeOf(callback)) {
        var expires: ?u64 = null;
        while (true) {
            return @call(.auto, callback, args) catch |err| {
                if (self.detached == null or (err != error.WouldBlock and err != error.FileBusy)) return err;
                const now = @import("antfly_platform").time.monotonicNs();
                if (expires == null) expires = now +| 100 * std.time.ns_per_ms;
                if (now >= expires.?) return error.CypherSnapshotNotReady;
                try checkpoint(self);
                const io = self.handle.db.backend_runtime.io() orelse return err;
                try io.sleep(.fromMilliseconds(1), .awake);
                continue;
            };
        }
    }
    fn pinnedBind(ptr: *anyopaque, a: A, graph: cypher.catalog.Graph) !void {
        const backend = cast(ptr).pinned_adapter.backend();
        try cast(ptr).retryRead(backend.vtable.bind, .{ backend.ptr, a, graph });
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
        return cast(ptr).retryRead(backend.vtable.scan, .{ backend.ptr, a, table, after, limit });
    }
    fn pinnedScanOrdered(ptr: *anyopaque, a: A, table: []const u8, filters: []const cypher.backend.PropertyFilter, order: []const cypher.backend.PropertyOrder, after: ?[]const u8, limit: usize, max_examined: usize) !?cypher.backend.Page(*const cypher.value.Node) {
        const backend = cast(ptr).pinned_adapter.backend();
        return cast(ptr).retryRead(backend.vtable.scan_ordered.?, .{ backend.ptr, a, table, filters, order, after, limit, max_examined });
    }
    fn pinnedScanWhere(ptr: *anyopaque, a: A, table: []const u8, filters: []const cypher.backend.PropertyFilter, after: ?[]const u8, limit: usize) !cypher.backend.Page(*const cypher.value.Node) {
        const backend = cast(ptr).pinned_adapter.backend();
        return cast(ptr).retryRead(backend.vtable.scan_where.?, .{ backend.ptr, a, table, filters, after, limit });
    }
    fn pinnedEstimateWhere(ptr: *anyopaque, a: A, table: []const u8, filters: []const cypher.backend.PropertyFilter, limit: usize) !?usize {
        const backend = cast(ptr).pinned_adapter.backend();
        return if (backend.vtable.estimate_where) |estimate| try cast(ptr).retryRead(estimate, .{ backend.ptr, a, table, filters, limit }) else null;
    }
    fn pinnedGet(ptr: *anyopaque, a: A, identity: cypher.value.Identity) !?*const cypher.value.Node {
        const backend = cast(ptr).pinned_adapter.backend();
        return cast(ptr).retryRead(backend.vtable.get, .{ backend.ptr, a, identity });
    }
    fn pinnedExpandBetween(ptr: *anyopaque, a: A, mapping: cypher.catalog.Relationship, identity: cypher.value.Identity, peer: cypher.value.Identity, direction: cypher.ast.Direction, filters: []const cypher.backend.PropertyFilter, after: ?[]const u8, limit: usize, max_examined: usize) !cypher.backend.Page(*const cypher.value.Relationship) {
        const backend = cast(ptr).pinned_adapter.backend();
        return cast(ptr).retryRead(backend.vtable.expand_between.?, .{ backend.ptr, a, mapping, identity, peer, direction, filters, after, limit, max_examined });
    }
    fn pinnedExpand(ptr: *anyopaque, a: A, mapping: cypher.catalog.Relationship, identity: cypher.value.Identity, direction: cypher.ast.Direction, after: ?[]const u8, limit: usize) !cypher.backend.Page(*const cypher.value.Relationship) {
        const backend = cast(ptr).pinned_adapter.backend();
        return cast(ptr).retryRead(backend.vtable.expand, .{ backend.ptr, a, mapping, identity, direction, after, limit });
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
        if (self.expiry_seconds) |expiry| if (now > expiry) return error.RowPolicyAuthenticationRequired;
        if (self.deadline_ns) |deadline| if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.RowPolicyAuthenticationRequired;
        // Keep inexpensive reads on the admission fast path. Longer reads
        // yield at an ordinary runtime/encoding checkpoint, after their data
        // cuts are ready. This also runs before the cached authority return.
        if (self.detach_after_ns) |deadline| if (@import("antfly_platform").time.monotonicNs() >= deadline) {
            self.detach_after_ns = null;
            try self.detached.?.detach(self.handle, self.graph);
        };
        const stamp = if (self.watch_arena != null) self.cohort.stable() else null;
        if (stamp != null and self.checked_authority == stamp) return;
        for (self.leases) |*lease| try lease.checkAt(@intCast(now));
        try self.root_lease.checkAt(@intCast(now));
        for (self.owners, self.schemas) |owner, version| {
            var view = owner.core.acquireSchemaView();
            defer if (view) |*value| value.release();
            if ((if (view) |value| value.tableSchema().version else 0) != version) return error.CatalogGenerationChanged;
        }
        // Never certify a validation which straddled a publication. The next
        // checkpoint then rechecks the cohort, including uncaptured mappings.
        self.checked_authority = if (stamp != null and self.cohort.stable() == stamp) stamp else null;
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
    return executeInternal(handle, name, json, out, diagnostic, scope, null);
}

fn executeInternal(handle: *h.Handle, name: []const u8, json: []const u8, out: *h.capi.Buffer, diagnostic: *cypher.compiler.Diagnostic, scope: *@import("cypher_write.zig").Scope, detached: ?*DetachedRead) !void {
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
    var data: std.AutoHashMapUnmanaged(*DB, void) = .empty;
    const needed = try cypher.dependencies.required(a, compiled.query, graph);
    for (graph.labels) |label| {
        const db = try @import("tables.zig").get(handle, label.table);
        if (cypher.dependencies.contains(needed, label.table)) try data.put(a, db, {});
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
    // Authorization leases and schema validation still cover every mapping.
    // Only immutable data cuts and index drains use the smaller dependency set.
    var data_owners: std.ArrayList(*DB) = .empty;
    for (owners.items) |owner| if (data.contains(owner)) try data_owners.append(a, owner);
    const leases = try a.alloc(DB.CypherReadLease, owners.items.len);
    var admitted: usize = 0;
    defer for (leases[0..admitted]) |*lease| lease.release();
    for (owners.items, leases) |db, *lease| {
        lease.* = try db.acquireCypherReadAuthority(a, "", "default");
        admitted += 1;
        if (db == &handle.db and db.local_execution.row_policy_table_name == null) db.local_execution.row_policy_table_name = "default";
        if (writes and data.contains(db)) try db.batch(.{ .activate_range_tracking = true });
    }
    const schemas = try a.alloc(u32, owners.items.len);
    for (owners.items, schemas) |db, *version| version.* = if (db.core.schema) |schema| schema.version else 0;
    var read: Read = .{ .partial_graph = needed != null, .handle = handle, .graph = graph, .owners = owners.items, .leases = leases, .tables = try a.alloc(DB.CypherBackend.Table, graph.labels.len), .root_lease = &root_lease, .schemas = schemas };
    try read.watch(a);
    defer read.unwatch();
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
    for (data_owners.items) |db| {
        if (h.liteOpenModeCanWrite(handle.open_mode)) {
            _ = try db.lookup(a, "", .{ .cypher_prepare = true });
        } else try db.waitForCurrentSyncLevel(.full_index);
    }
    const fences = try a.alloc(DB.StatementReadFence, data_owners.items.len);
    var held: usize = 0;
    defer for (fences[0..held]) |*fence| fence.release();
    const deadline = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(5));
    while (held != data_owners.items.len) {
        if (try data_owners.items[held].tryStatementReadFence()) |fence| {
            fences[held] = fence;
            held += 1;
            continue;
        }
        for (fences[0..held]) |*fence| fence.release();
        held = 0;
        if (std.Io.Clock.awake.now(io).nanoseconds >= deadline.nanoseconds) return error.CypherSnapshotNotReady;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try read.pin(a, data_owners.items, fences);
    defer read.unpin();
    const guard_type = @import("sql_commit.zig").GraphGuard;
    const guards = try a.alloc(guard_type, if (writes) data_owners.items.len else 0);
    for (data_owners.items[0..guards.len], guards) |db, *guard| {
        const version = if (db.core.schema) |schema| schema.version else 0;
        const table = for (read.tables) |entry| {
            if (entry.db == db) break entry.name;
        } else unreachable;
        var txn = try db.core.store.beginReadTxn();
        defer txn.abort();
        guard.* = .{ .table_name = table, .table_id = db.core.identity_namespace.table_id, .schema_version = version, .proofs = try @import("../storage/range_protection.zig").capture(a, &txn, "", "") };
    }
    for (fences[0..held]) |*fence| fence.release();
    held = 0;
    if (!writes) if (detached) |execution| if (execution.guard.connection != null) {
        read.detached = execution;
        read.detach_after_ns = @import("antfly_platform").time.monotonicNs() +| std.time.ns_per_ms;
    };
    var writer: @import("cypher_write.zig").Writer = .{ .read = &read, .guards = guards, .scope = scope, .request_id = request.write_request_id };
    var overlay: cypher.transaction.Overlay = .{ .lazy = true, .retain_scan_preimages = true, .base = read.pinnedBackend(), .host = writer.host(), .max_entities = request.limits.intermediate_states, .max_anchors = request.limits.scanned_anchors, .max_edges = request.limits.explored_edges };
    writer.overlay = &overlay;
    var result = try cypher.runtime.execute(handle.alloc, if (writes) overlay.backend() else read.pinnedBackend(), graph, &compiled, request.parameters, request.limits);
    defer result.deinit();
    if (writes) {
        const bytes = try cypher.bounded_json.encode(std.heap.c_allocator, result.withCommit(.{ .transaction_id = std.fmt.bytesToHex(scope.transaction_id.?, .lower), .status = writer.outcome, .outcome = writer.outcome }), cypher.runtime.max_response_bytes, null);
        out.* = .{ .ptr = bytes.ptr, .len = bytes.len };
        return;
    }
    const bytes = try cypher.bounded_json.encode(std.heap.c_allocator, result, cypher.runtime.max_response_bytes, .{ .ptr = &read, .call = Read.checkpoint });
    errdefer std.heap.c_allocator.free(bytes);
    try Read.checkpoint(&read);
    out.* = .{ .ptr = bytes.ptr, .len = bytes.len };
}
test "capi Cypher authority fast checkpoints observe schema policy and expiry changes" {
    const a = std.testing.allocator;
    // The checkpoint touches only the registry in this otherwise inert owner.
    var owner: DB = undefined;
    var core: @TypeOf(owner.core.*) = undefined;
    var registry = try @TypeOf(core.schema_registry.*).initCloned(a, std.testing.io, .{ .version = 1 });
    defer registry.deinit();
    core.schema_registry = &registry;
    owner.core = &core;
    const Gate = @typeInfo(@FieldType(DB.CypherReadLease, "gate")).pointer.child;
    var root_gate = Gate.init(.{});
    var gate = Gate.init(.{});
    var root_lease = try root_gate.enterRawRead();
    defer root_lease.release();
    var leases = [_]DB.CypherReadLease{try gate.enterRawRead()};
    defer leases[0].release();
    var read: Read = .{ .handle = undefined, .graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{}, .relationships = &.{} }, .owners = &.{&owner}, .schemas = &.{1}, .tables = &.{}, .leases = &leases, .root_lease = &root_lease };
    try read.watch(a);
    defer read.unwatch();
    try Read.checkpoint(&read);
    const certified = read.checked_authority.?;
    for (0..10) |_| try Read.checkpoint(&read);
    try std.testing.expectEqual(certified, read.checked_authority.?);
    try registry.publishPrepared(try @import("../storage/db/schema_registry.zig").Epoch.createCloned(a, .{ .version = 2 }));
    try std.testing.expectError(error.CatalogGenerationChanged, Read.checkpoint(&read));
    try registry.publishPrepared(try @import("../storage/db/schema_registry.zig").Epoch.createCloned(a, .{ .version = 1 }));
    try Read.checkpoint(&read);
    read.expiry_seconds = 0;
    try std.testing.expectError(error.RowPolicyAuthenticationRequired, Read.checkpoint(&read));
    read.expiry_seconds = null;
    read.deadline_ns = 0;
    try std.testing.expectError(error.RowPolicyAuthenticationRequired, Read.checkpoint(&read));
    read.deadline_ns = null;
    try gate.beginPreparing(.disabled);
    try std.testing.expectError(error.RowPolicyCatalogChanged, Read.checkpoint(&read));
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

test "capi embedded graph producer cardinality proves emptiness at the pinned cut" {
    const a = std.testing.allocator;
    var directory = try api.TestDirectory.init("empty-source-cut");
    defer directory.cleanup();
    try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
    const path = try std.fmt.allocPrintSentinel(a, "{s}/empty.aflite", .{directory.path()}, 0);
    defer a.free(path);
    var raw: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_lite_create_hosted(path, &raw));
    defer api.antfly_db_close(raw);
    var guard = api.enterHandle(raw, .exclusive).?;
    defer guard.leave();
    const store = guard.handle.db.core.store;
    const counts = @import("../storage/db/range_cardinality.zig");
    var empty = try store.beginReadTxn();
    defer empty.abort();
    try std.testing.expectEqual(@as(?u64, 0), try counts.loadOrProveEmptyFromTxn(&empty));
    // A legacy user row without a counter is unknown, never an empty proof.
    try store.put("\x01legacy", "{}");
    var legacy = try store.beginReadTxn();
    defer legacy.abort();
    try std.testing.expectEqual(@as(?u64, null), try counts.loadOrProveEmptyFromTxn(&legacy));
    try std.testing.expectEqual(@as(?u64, 0), try counts.loadOrProveEmptyFromTxn(&empty));
    var one: [8]u8 = undefined;
    std.mem.writeInt(u64, &one, 1, .little);
    try store.put(&@import("../storage/internal_keys.zig").range_document_count_key, &one);
    var current = try store.beginReadTxn();
    defer current.abort();
    try std.testing.expectEqual(@as(?u64, 1), try counts.loadOrProveEmptyFromTxn(&current));
    try std.testing.expectEqual(@as(?u64, null), try counts.loadOrProveEmptyFromTxn(&legacy));
}

test "capi embedded graph detached reads retain roots and revalidate replacement tables" {
    const a = std.testing.allocator;
    var directory = try api.TestDirectory.init("detached-graph");
    defer directory.cleanup();
    try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
    const path = try std.fmt.allocPrintSentinel(a, "{s}/graph.aflite", .{directory.path()}, 0);
    defer a.free(path);
    var raw: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_lite_create_hosted(path, &raw));
    defer api.antfly_db_close(raw);
    var output: h.capi.Buffer = .{};
    defer api.antfly_buffer_free(&output);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_sql_json(raw, .fromSlice("{\"statement\":\"CREATE TABLE docs (id TEXT)\"}"), &output));
    api.antfly_buffer_free(&output);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, antfly_db_graph_catalog_json(raw, .fromSlice("{\"action\":\"put\",\"name\":\"g\",\"definition\":{\"labels\":[{\"name\":\"D\",\"table\":\"docs\"}],\"relationships\":[]}}"), &output));
    api.antfly_buffer_free(&output);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var guard = api.enterHandle(raw, .exclusive).?;
    defer guard.leave();
    const graph = try catalog.graph(arena.allocator(), try catalog.get(arena.allocator(), guard.handle, "g"));
    const original = guard.handle;
    var read: DetachedRead = .{ .guard = &guard, .allocator = arena.allocator() };
    {
        var root_lease = try guard.handle.db.acquireCypherReadAuthority(arena.allocator(), "", "default");
        defer root_lease.release();
        var checkpoints: Read = .{ .handle = guard.handle, .graph = graph, .owners = &.{}, .leases = &.{}, .tables = &.{}, .schemas = &.{}, .root_lease = &root_lease, .detached = &read, .detach_after_ns = std.math.maxInt(u64) };
        try checkpoints.watch(arena.allocator());
        defer checkpoints.unwatch();
        try Read.checkpoint(&checkpoints);
        try std.testing.expect(read.graph == null and guard.suspended_query_root == null);
        const Contention = struct {
            calls: usize = 0,
            fn attempt(ptr: *@This()) anyerror!u32 {
                ptr.calls += 1;
                if (ptr.calls < 3) return error.WouldBlock;
                return 42;
            }
        };
        var contention: Contention = .{};
        checkpoints.detached = null;
        try std.testing.expectError(error.WouldBlock, checkpoints.retryRead(Contention.attempt, .{&contention}));
        try std.testing.expectEqual(@as(usize, 1), contention.calls);
        checkpoints.detached = &read;
        contention.calls = 0;
        try std.testing.expectEqual(@as(u32, 42), try checkpoints.retryRead(Contention.attempt, .{&contention}));
        try std.testing.expectEqual(@as(usize, 3), contention.calls);
        // Expiry is deterministic and must yield even with a certified cohort.
        checkpoints.detach_after_ns = 0;
        try Read.checkpoint(&checkpoints);
        try std.testing.expect(read.graph != null and guard.suspended_query_root == original);
    }
    try std.testing.expect(original.owned_lite_backend.?.native_docstore.?.read_only);
    try std.testing.expect(original.owned_lite_backend.?.native_docstore.?.file.read_only);
    try std.testing.expect(guard.resumeAccess(raw, .exclusive));
    try std.testing.expect(guard.entry_error == null);
    // Sequential queries reuse the serving root rather than reopening the file.
    try std.testing.expectEqual(original, guard.handle);
    try std.testing.expect(!guard.handle.owned_lite_backend.?.native_docstore.?.read_only);
    try std.testing.expect(!guard.handle.owned_lite_backend.?.native_docstore.?.file.read_only);
    try read.validate(guard.handle);
    try read.detach(guard.handle, graph);
    {
        var concurrent = api.enterHandle(raw, .exclusive).?;
        defer concurrent.leave();
        try std.testing.expect(concurrent.entry_error == null);
        try std.testing.expect(concurrent.handle != original);
        try std.testing.expectEqual(@as(usize, 1), original.immutable_query_borrows);
        try std.testing.expectEqual(graph.labels.len, original.embedded_tables.count());
    }
    // DDL through the same public connection must not alter the borrowed root.
    try std.testing.expectEqual(h.capi.ErrorCode.ok, @import("tables.zig").antfly_db_drop_table(raw, .fromSlice("docs")));
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_sql_json(raw, .fromSlice("{\"statement\":\"CREATE TABLE docs (id TEXT)\"}"), &output));
    const old_table = try @import("tables.zig").get(original, "docs");
    try std.testing.expectEqual(read.owners[0].table_id, old_table.core.identity_namespace.table_id);
    try std.testing.expect(guard.resumeAccess(raw, .exclusive));
    try std.testing.expect(guard.entry_error == null);
    try std.testing.expectError(error.CatalogGenerationChanged, read.validate(guard.handle));
}
