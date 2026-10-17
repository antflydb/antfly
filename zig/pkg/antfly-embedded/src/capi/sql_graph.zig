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

//! Embedded GRAPH_TABLE shares ordinary SQL's owner capture interval and
//! applies connection-staged SQL postimages before running the shared graph AST.
const std = @import("std");
const h = @import("handles.zig");
const d = h.antfly.capi_dependencies;
const catalog = d.sql_catalog;
const ast = d.sql_ast;
const cypher = @import("cypher");
const graph_catalog = @import("graph_catalog.zig");
const Read = @import("cypher.zig").Read;
const DB = h.db_mod.DB;
const A = std.mem.Allocator;
pub fn resolve(adapter: anytype, a: A, definition: ast.GraphTable) !catalog.Table {
    const handle = adapter.handle orelse return error.UnsupportedSqlExecution;
    if ((definition.name.database != null and !std.mem.eql(u8, definition.name.database.?, "default")) or
        (definition.name.namespace != null and !std.mem.eql(u8, definition.name.namespace.?, "public"))) return error.CatalogNotFound;
    const record = try graph_catalog.get(a, handle, definition.name.table);
    const graph = try graph_catalog.graph(a, record);
    try graph_catalog.validateOwners(a, handle, graph);
    var dependencies: std.ArrayList(catalog.Table) = .empty;
    const backend = adapter.backend();
    for (graph.labels) |label| {
        const table = try backend.vtable.resolve(backend.ptr, a, .{ .table = label.table }, .read);
        const exists = for (dependencies.items) |entry| {
            if (entry.id == table.id) break true;
        } else false;
        if (!exists) try dependencies.append(a, table);
    }
    try cypher.binder.validate(a, graph, definition.query);
    const binding = try a.create(catalog.graph_relation.Binding);
    binding.* = .{ .definition = definition, .graph = graph, .catalog_json = try std.json.Stringify.valueAlloc(a, record, .{}), .dependencies = dependencies.items };
    return .{ .id = record.incarnation | (@as(u64, 1) << 63), .physical_name = record.name, .schema_version = 1, .storage_mode = .document, .columns = try catalog.graph_relation.columns(a, binding.*), .graph = binding, .scope = .{ .database = "default", .namespace = "public", .name = record.name, .revision = record.revision } };
}
const Cursor = struct {
    a: A,
    read: Read,
    root_lease: DB.CypherReadLease,
    child: catalog.Cursor = undefined,
    overlay: cypher.transaction.Overlay = undefined,
    stage_arena: std.heap.ArenaAllocator,
    fn next(ptr: *anyopaque, a: A, limit: u32) !catalog.Page {
        const self: *Cursor = @ptrCast(@alignCast(ptr));
        try self.read.validate();
        var page = try self.child.next(self.child.ptr, a, limit);
        errdefer page.deinit();
        try self.read.validate();
        return page;
    }
    fn enableWeights(ptr: *anyopaque, multiplicity: catalog.Multiplicity) !bool {
        const self: *Cursor = @ptrCast(@alignCast(ptr));
        try self.read.validate();
        const enabled = if (self.child.enable_weights) |enable| try enable(self.child.ptr, multiplicity) else false;
        try self.read.validate();
        return enabled;
    }
    fn enableAggregateWeights(ptr: *anyopaque, requests: []const catalog.AggregateWeight) !bool {
        const self: *Cursor = @ptrCast(@alignCast(ptr));
        try self.read.validate();
        const enabled = if (self.child.enable_aggregate_weights) |enable| try enable(self.child.ptr, requests) else false;
        try self.read.validate();
        return enabled;
    }
    fn countRows(ptr: *anyopaque) !?u64 {
        const self: *Cursor = @ptrCast(@alignCast(ptr));
        try self.read.validate();
        const count = if (self.child.count_rows) |count_rows| try count_rows(self.child.ptr) else null;
        try self.read.validate();
        return count;
    }
    fn close(ptr: *anyopaque) void {
        const self: *Cursor = @ptrCast(@alignCast(ptr));
        self.child.close(self.child.ptr);
        self.stage_arena.deinit();
        self.read.unpin();
        for (self.read.leases) |*lease| lease.release();
        self.root_lease.release();
        self.a.free(self.read.owners);
        self.a.free(self.read.leases);
        self.a.free(self.read.schemas);
        self.a.free(self.read.tables);
        self.a.destroy(self);
    }
};
fn unavailableIdentity(_: *anyopaque, _: A, _: []const u8, _: bool) !cypher.value.Identity {
    return error.CypherMutationsUnavailable;
}
fn unavailableCommit(_: *anyopaque, _: A, _: []const cypher.backend.Mutation) !void {
    return error.CypherMutationsUnavailable;
}
pub fn open(adapter: anytype, a: A, binding: catalog.graph_relation.Binding, request: catalog.Scan, owners: []const *DB, fences: []const DB.StatementReadFence) !catalog.Cursor {
    const handle = adapter.handle orelse return error.UnsupportedSqlExecution;
    const self = try a.create(Cursor);
    errdefer a.destroy(self);
    const saved_owners = try a.dupe(*DB, owners);
    errdefer a.free(saved_owners);
    const leases = try a.alloc(DB.CypherReadLease, owners.len);
    errdefer a.free(leases);
    var admitted: usize = 0;
    errdefer for (leases[0..admitted]) |*lease| lease.release();
    const schemas = try a.alloc(u32, owners.len);
    errdefer a.free(schemas);
    for (owners, leases, schemas) |db, *lease, *version| {
        lease.* = try db.acquireCypherReadAuthority(a, "", "default");
        admitted += 1;
        version.* = if (db.core.schema) |schema| schema.version else 0;
    }
    const tables = try a.alloc(DB.CypherBackend.Table, binding.graph.labels.len);
    errdefer a.free(tables);
    self.* = .{ .a = a, .root_lease = try handle.db.acquireCypherReadAuthority(a, "", "default"), .read = undefined, .stage_arena = std.heap.ArenaAllocator.init(a) };
    errdefer self.root_lease.release();
    errdefer self.stage_arena.deinit();
    self.read = .{ .handle = handle, .graph = binding.graph, .owners = saved_owners, .leases = leases, .tables = tables, .schemas = schemas, .root_lease = &self.root_lease };
    try self.read.pin(a, owners, fences);
    errdefer self.read.unpin();
    var provider = self.read.pinnedBackend();
    if (adapter.transaction) |session| if (session.active) {
        const stage_allocator = self.stage_arena.allocator();
        self.overlay = .{ .lazy = true, .base = provider, .host = .{ .ptr = self, .new_identity = unavailableIdentity, .commit = unavailableCommit } };
        const staged = self.overlay.backend();
        try staged.vtable.bind(staged.ptr, stage_allocator, binding.graph);
        try staged.vtable.begin.?(staged.ptr);
        const entries = try session.merged(stage_allocator);
        for (entries) |entry| {
            const dependency: ?catalog.Table = for (binding.dependencies) |table| {
                if (std.mem.eql(u8, table.physical_name, entry.table.physical_name)) break table;
            } else null;
            const table = dependency orelse continue;
            if (entry.mutation.predicate_only) continue;
            if (table.id != entry.table.id or table.schema_version != entry.table.schema_version or table.storage_mode != entry.table.storage_mode) return error.PreparedGenerationChanged;
            const identity: cypher.value.Identity = .{ .table = entry.table.physical_name, .key = entry.mutation.key };
            const row: ?std.json.Value = if (entry.encoded_row) |bytes|
                try std.json.parseFromSliceLeaky(std.json.Value, stage_allocator, bytes, .{ .parse_numbers = false, .allocate = .alloc_always })
            else
                entry.mutation.row;
            const node: ?*const cypher.value.Node = if (row) |document| blk: {
                const value = try stage_allocator.create(cypher.value.Node);
                value.* = .{ .identity = identity, .properties = try cypher.value.entityProperties(stage_allocator, (try cypher.value.Value.fromJson(stage_allocator, document)).map) };
                break :blk value;
            } else null;
            try self.overlay.stageReadDocument(stage_allocator, identity, node);
        }
        provider = staged;
    };
    var temporary = std.heap.ArenaAllocator.init(a);
    defer temporary.deinit();
    const parameters = try temporary.allocator().alloc(cypher.value.Entry, binding.definition.parameters.len);
    for (binding.definition.parameters, parameters) |name, *parameter| {
        const index = std.fmt.parseUnsigned(usize, name, 10) catch return error.InvalidSqlParameters;
        if (index == 0 or index > adapter.graph_parameters.len) return error.InvalidSqlParameters;
        parameter.* = .{ .name = name, .value = try cypher.value.Value.fromJson(temporary.allocator(), adapter.graph_parameters[index - 1]) };
    }
    self.child = try catalog.graph_relation.Cursor.open(a, binding, provider, parameters, request, .{});
    return .{ .ptr = self, .next = Cursor.next, .close = Cursor.close, .count_rows = Cursor.countRows, .enable_weights = Cursor.enableWeights, .enable_aggregate_weights = Cursor.enableAggregateWeights, .estimated_rows = self.child.estimated_rows };
}
