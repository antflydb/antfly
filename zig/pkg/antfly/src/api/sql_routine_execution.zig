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

//! Statement-owned native SQL callables. Metadata and body preparation are
//! bounded admission work; row execution retains only typed immutable programs.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.sql_catalog;
const native = catalog.routines;
const routines = @import("../system_catalog/routines.zig");
const execution = @import("sql_execution.zig");
const http = @import("http_server.zig");
const domain = local.system_catalog_domain;
const A = std.mem.Allocator;

pub fn snapshot(root: *execution.Adapter, a: A, request: routines.SnapshotRequest) !routines.Snapshot {
    try root.context.ensureActive();
    if (root.server.cfg.auth_enabled and root.identity.* == null) return error.Forbidden;
    if (!root.server.source.vtable.supports_sql_routines) return error.UnsupportedSqlExecution;
    var context = root.context;
    context.setting_admin = true;
    const bytes = try root.server.source.systemCatalog(a, context, .{ .sql_routine_snapshot = request });
    defer a.free(bytes);
    const result = try std.json.parseFromSliceLeaky(routines.Snapshot, a, bytes, .{ .allocate = .alloc_always });
    try result.validate(request);
    return result;
}

pub fn targets(root: *execution.Adapter, a: A, input: local.sql_ast.Name) ![]const domain.Target {
    const path = root.ddl_search_path;
    const unqualified = input.database == null and input.namespace == null and path != null;
    const capacity: usize = if (unqualified) path.?.len else 1;
    var result: std.ArrayList(domain.Target) = .empty;
    for (0..capacity) |i| {
        const target: domain.Target = .{ .database = input.database orelse root.database, .namespace = if (unqualified) path.?.entries[i].slice() else input.namespace orelse root.namespace, .table = input.table };
        try target.validate();
        if (!try @import("sql_catalog_auth.zig").namespaceUsage(root.identity.*, a, target)) {
            if (!unqualified) return error.Forbidden;
            continue;
        }
        try result.append(a, target);
    }
    if (result.items.len == 0) return error.SqlUndefinedFunction;
    return result.toOwnedSlice(a);
}

pub const Owner = struct {
    const Identity = struct { id: u64, generation: u32 };
    root: *execution.Adapter,
    memory: local.sql_runtime.MemoryOwner,
    budget: local.sql_memory_budget,
    arena: std.heap.ArenaAllocator,
    names: std.StringHashMapUnmanaged(*const native.bindings.Resolved) = .empty,
    bodies: std.AutoHashMapUnmanaged(Identity, *const native.bindings.Resolved) = .empty,
    depth: usize = 0,

    pub fn create(root: *execution.Adapter) !*Owner {
        const control = root.server.alloc;
        const self = try control.create(Owner);
        errdefer control.destroy(self);
        const memory = try @import("sql_memory_owner.zig").create(control, control, root.server.shared_resource_manager orelse &root.server.local_resource_manager);
        errdefer memory.deinit();
        self.* = .{ .root = root, .memory = memory, .budget = .{ .backing = memory.allocator, .limit = native.preparation_bytes, .pressure = memory.pressure }, .arena = undefined };
        self.arena = .init(self.budget.allocator());
        return self;
    }
    pub fn deinit(self: *Owner) void {
        const control = self.root.server.alloc;
        self.arena.deinit();
        self.memory.deinit();
        control.destroy(self);
    }
    fn resolver(self: *Owner) native.bindings.Resolver {
        return .{ .ptr = self, .resolve = resolve };
    }
    fn resolve(raw: *anyopaque, _: A, name: []const u8, arguments: []const native.bindings.Argument) anyerror!*const native.bindings.Resolved {
        const self: *Owner = @ptrCast(@alignCast(raw));
        return self.lookup(name, arguments) catch |err| {
            if (err == error.OutOfMemory and self.budget.exhausted) return error.SqlWorkingMemoryLimitExceeded;
            return self.memory.mapError(err);
        };
    }
    fn lookup(self: *Owner, name: []const u8, arguments: []const native.bindings.Argument) !*const native.bindings.Resolved {
        const a = self.arena.allocator();
        try self.root.context.ensureActive();
        const key = try std.json.Stringify.valueAlloc(a, .{ .name = name, .arguments = arguments }, .{});
        if (self.names.get(key)) |ready| return ready;
        if (self.depth >= 32 or self.names.count() >= 512) return error.SqlProgramLimitExceeded;
        self.depth += 1;
        defer self.depth -= 1;
        var parts = std.mem.splitScalar(u8, name, 0);
        const first = parts.next() orelse return error.SqlUndefinedFunction;
        const second = parts.next();
        const third = parts.next();
        if (parts.next() != null) return error.SqlUndefinedFunction;
        const reference: local.sql_ast.Name = if (third) |last| .{ .database = first, .namespace = second.?, .table = last } else if (second) |last| .{ .namespace = first, .table = last } else .{ .table = first };
        const inputs = try a.alloc(routines.records.Input, arguments.len);
        for (arguments, inputs) |argument, *input| {
            input.* = .{ .unknown_literal = argument.unknown_literal };
            if (argument.type.kind) |kind| input.type = try native.storageType(.{ .kind = kind, .element_type = argument.type.element_type, .temporal_kind = argument.type.temporal_kind });
        }
        var pinned: ?@import("sql_prepared.zig").Binding = null;
        if (self.root.prepared_bindings) |manifests| {
            var candidates: std.ArrayList(routines.records.Header) = .empty;
            for (manifests) |manifest| {
                if (manifest.kind != .function or !std.mem.eql(u8, manifest.name, reference.table) or !std.mem.eql(u8, manifest.database, reference.database orelse self.root.database)) continue;
                if (reference.namespace) |namespace| if (!std.mem.eql(u8, manifest.namespace, namespace)) continue;
                try candidates.append(a, .{ .id = manifest.id, .signature = manifest.routine_signature orelse return error.CatalogGenerationChanged });
            }
            const selected = (try routines.records.choose(a, candidates.items, inputs)) orelse return error.CatalogGenerationChanged;
            for (manifests) |manifest| if (manifest.kind == .function and manifest.id == selected.id) {
                pinned = manifest;
                break;
            };
        }
        const scope = try targets(self.root, a, if (pinned) |manifest| .{ .database = manifest.database, .namespace = manifest.namespace, .table = manifest.name } else reference);
        const cut = try snapshot(self.root, a, .{ .targets = scope, .signature = if (pinned) |manifest| manifest.routine_signature else null, .arguments = if (pinned != null) null else inputs, .expected_revision = self.root.revision });
        const found = cut.found orelse return error.SqlUndefinedFunction;
        self.root.revision = cut.revision;
        if (self.root.server.cfg.auth_enabled and found.record.definition.owner == .auth_disabled) return error.Forbidden;
        const Binding = @import("sql_prepared.zig").Binding;
        const manifest = try Binding.fromRoutine(a, cut.scopes[found.scope].target, found.record, cut.incarnation, cut.revision);
        if (self.root.prepared_bindings) |bindings| try Binding.verifyRoutine(bindings, manifest);
        if (self.root.collect_prepared_bindings) |bindings| {
            const exists = for (bindings.items) |entry| {
                if (entry.kind != .function or entry.id != manifest.id) continue;
                try Binding.verifyRoutine(&.{entry}, manifest);
                break true;
            } else false;
            if (!exists) {
                if (bindings.items.len >= 64) return error.SqlProgramLimitExceeded;
                try bindings.append(a, manifest);
            }
        }
        const identity: Identity = .{ .id = found.record.id, .generation = found.record.generation };
        if (self.bodies.get(identity)) |ready| {
            try self.names.put(a, key, ready);
            return ready;
        }
        const ready = try native.prepare(a, found.record, self.resolver(), null);
        try self.bodies.put(a, identity, ready);
        try self.names.put(a, key, ready);
        return ready;
    }
};

pub fn bind(raw: *anyopaque, _: A, name: []const u8, arguments: []const native.bindings.Argument) !*const native.bindings.Resolved {
    const root: *execution.Adapter = @ptrCast(@alignCast(raw));
    if (root.routine_owner == null) root.routine_owner = try Owner.create(root);
    const owner = root.routine_owner.?;
    return Owner.resolve(owner, owner.arena.allocator(), name, arguments);
}
