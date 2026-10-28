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
    control: A,
    lease: local.sql_runtime.Result.ExecutionLease,
    root: *execution.Adapter,
    memory: local.sql_runtime.MemoryOwner,
    budget: local.sql_memory_budget,
    arena: std.heap.ArenaAllocator,
    names: std.StringHashMapUnmanaged(*const native.bindings.Resolved) = .empty,
    bodies: std.AutoHashMapUnmanaged(Identity, *const native.bindings.Resolved) = .empty,
    procedure_names: std.StringHashMapUnmanaged(*native.procedures.Resolved) = .empty,
    aggregate_names: std.StringHashMapUnmanaged(?*const native.bindings.Aggregate) = .empty,
    declaration_cuts: std.StringHashMapUnmanaged(routines.Snapshot) = .empty,
    settings: ?local.sql_setting_catalog.View = null,
    depth: usize = 0,

    pub fn create(root: *execution.Adapter) !*Owner {
        const control = root.server.alloc;
        const self = try control.create(Owner);
        errdefer control.destroy(self);
        const memory = try @import("sql_memory_owner.zig").create(control, control, root.server.shared_resource_manager orelse &root.server.local_resource_manager);
        errdefer memory.deinit();
        self.* = .{ .control = control, .lease = .{ .ptr = self, .release = release }, .root = root, .memory = memory, .budget = .{ .backing = memory.allocator, .limit = native.preparation_bytes, .pressure = memory.pressure }, .arena = undefined };
        self.arena = .init(self.budget.allocator());
        return self;
    }
    pub fn release(raw: *anyopaque) void {
        (@as(*Owner, @ptrCast(@alignCast(raw)))).deinit();
    }
    pub fn deinit(self: *Owner) void {
        const control = self.control;
        if (self.settings) |*settings| settings.deinit();
        self.arena.deinit();
        self.memory.deinit();
        control.destroy(self);
    }
    fn captureSettings(self: *Owner, a: A) !void {
        if (self.settings == null) if (self.root.settingCapture()) |settings_capture| {
            self.settings = try local.sql_setting_catalog.View.capture(a, settings_capture.owner, settings_capture.scope, settings_capture.overlay);
            try self.settings.?.pinSqlUser(settings_capture.sql_user);
        };
    }
    fn resolver(self: *Owner) native.bindings.Resolver {
        return self.resolverFor(false);
    }
    fn resolverFor(self: *Owner, dynamic: bool) native.bindings.Resolver {
        return .{ .ptr = self, .resolve = if (dynamic) dynamicResolve else resolve, .resolve_operator = if (dynamic) resolveDynamicOperator else resolveOperator, .resolve_aggregate = resolveAggregate, .resolve_reference = if (dynamic) resolveDynamicReference else resolveReference };
    }
    fn resolveAggregate(raw: *anyopaque, _: A, name: []const u8, arguments: []const native.bindings.Argument) anyerror!?*const native.bindings.Aggregate {
        const self: *Owner = @ptrCast(@alignCast(raw));
        try self.root.context.ensureActive();
        const a = self.arena.allocator();
        const key = try std.json.Stringify.valueAlloc(a, .{ .name = name, .arguments = arguments }, .{});
        if (self.aggregate_names.get(key)) |ready| return ready;
        if (self.aggregate_names.count() >= 128) return error.SqlProgramLimitExceeded;
        // Prepared scalar functions already have a different captured kind.
        // Classification must not turn their valid manifest into an absent
        // aggregate witness or re-resolve their function identity by name.
        if (self.root.prepared_bindings) |manifests| {
            const plain = if (std.mem.lastIndexOfScalar(u8, name, 0)) |separator| name[separator + 1 ..] else name;
            var aggregate = false;
            var function = false;
            for (manifests) |manifest| if (std.mem.eql(u8, manifest.name, plain)) {
                aggregate = aggregate or manifest.kind == .aggregate;
                function = function or manifest.kind == .function;
            };
            if (function and !aggregate) {
                try self.aggregate_names.put(a, key, null);
                return null;
            }
        }
        const record = self.capture(name, arguments, .aggregate, false) catch |err| {
            if (err == error.SqlUndefinedFunction or err == error.SqlWrongObjectType) {
                try self.aggregate_names.put(a, key, null);
                return null;
            }
            return err;
        };
        const ready = try native.prepareAggregate(a, record, self.resolver());
        try self.aggregate_names.put(a, key, ready);
        return ready;
    }
    fn resolveOperator(raw: *anyopaque, _: A, name: []const u8, arguments: []const native.bindings.Argument) anyerror!*const native.bindings.Resolved {
        const self: *Owner = @ptrCast(@alignCast(raw));
        return self.lookupKind(name, arguments, .operator, false);
    }
    fn resolveDynamicOperator(raw: *anyopaque, _: A, name: []const u8, arguments: []const native.bindings.Argument) anyerror!*const native.bindings.Resolved {
        const self: *Owner = @ptrCast(@alignCast(raw));
        return self.lookupKind(name, arguments, .operator, true);
    }
    fn resolveReference(raw: *anyopaque, _: A, reference: routines.records.FunctionReference, arguments: []const native.bindings.Argument) anyerror!*const native.bindings.Resolved {
        return (@as(*Owner, @ptrCast(@alignCast(raw)))).lookupReference(reference, arguments, false);
    }
    fn resolveDynamicReference(raw: *anyopaque, _: A, reference: routines.records.FunctionReference, arguments: []const native.bindings.Argument) anyerror!*const native.bindings.Resolved {
        return (@as(*Owner, @ptrCast(@alignCast(raw)))).lookupReference(reference, arguments, true);
    }
    fn retainManifest(self: *Owner, a: A, cut: routines.Snapshot, found: routines.Snapshot.Found, dynamic: bool, lookup_name: ?[]const u8) !void {
        const Binding = @import("sql_prepared.zig").Binding;
        var manifest = try Binding.fromRoutine(a, cut.scopes[found.scope].target, found.record, cut.incarnation, cut.revision);
        manifest.routine_namespace_id = cut.scopes[found.scope].namespace_id;
        manifest.routine_lookup = lookup_name;
        if (!dynamic) if (self.root.prepared_bindings) |bindings| try Binding.verifyRoutine(bindings, manifest);
        if (!dynamic) if (self.root.collect_prepared_bindings) |bindings| {
            for (bindings.items) |entry| if (entry.kind == manifest.kind and entry.id == manifest.id and (lookup_name == null or (entry.routine_lookup != null and std.mem.eql(u8, entry.routine_lookup.?, lookup_name.?)))) {
                try Binding.verifyRoutine(&.{entry}, manifest);
                return;
            };
            if (bindings.items.len >= 64) return error.SqlProgramLimitExceeded;
            try bindings.append(a, manifest);
        };
    }
    fn lookupReference(self: *Owner, reference: routines.records.FunctionReference, arguments: []const native.bindings.Argument, dynamic: bool) !*const native.bindings.Resolved {
        const a = self.arena.allocator();
        const cut = try snapshot(self.root, a, .{ .targets = &.{}, .reference = reference, .expected_revision = self.root.revision });
        const found = cut.found orelse return error.CatalogGenerationChanged;
        const target = cut.scopes[found.scope].target;
        if (!try @import("sql_catalog_auth.zig").namespaceUsage(self.root.identity.*, a, target)) return error.Forbidden;
        if (found.record.kind != .function or found.record.id != reference.id or found.record.definition.arguments.len != arguments.len) return error.CatalogGenerationChanged;
        if (self.root.server.cfg.auth_enabled and found.record.definition.owner == .auth_disabled) return error.Forbidden;
        self.root.revision = cut.revision;
        try self.retainManifest(a, cut, found, dynamic, null);
        const identity: Identity = .{ .id = found.record.id, .generation = found.record.generation };
        if (self.bodies.get(identity)) |ready| return ready;
        if (self.depth >= 32) return error.SqlProgramLimitExceeded;
        self.depth += 1;
        defer self.depth -= 1;
        try self.captureSettings(a);
        const ready = try native.prepare(a, found.record, self.resolverFor(dynamic), if (self.settings) |*settings| settings else null);
        try self.bodies.put(a, identity, ready);
        return ready;
    }
    fn resolve(raw: *anyopaque, _: A, name: []const u8, arguments: []const native.bindings.Argument) anyerror!*const native.bindings.Resolved {
        const self: *Owner = @ptrCast(@alignCast(raw));
        return self.lookup(name, arguments, false) catch |err| {
            if (err == error.OutOfMemory and self.budget.exhausted) return error.SqlWorkingMemoryLimitExceeded;
            return self.memory.mapError(err);
        };
    }
    fn capture(self: *Owner, name: []const u8, arguments: []const native.bindings.Argument, routine_kind: routines.records.Kind, dynamic: bool) !routines.records.Record {
        const a = self.arena.allocator();
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
        if (!dynamic) if (self.root.prepared_bindings) |manifests| {
            var candidates: std.ArrayList(routines.records.Header) = .empty;
            for (manifests) |manifest| {
                const family = if (routine_kind == .operator) manifest.kind == .operator else manifest.kind == .function or manifest.kind == .procedure or manifest.kind == .aggregate;
                if (!family or manifest.routine_lookup == null or !std.mem.eql(u8, manifest.routine_lookup.?, name)) continue;
                try candidates.append(a, .{ .id = manifest.id, .signature = manifest.routine_signature orelse return error.CatalogGenerationChanged });
            }
            const selected = (try routines.records.choose(a, candidates.items, inputs)) orelse return error.CatalogGenerationChanged;
            for (manifests) |manifest| if (manifest.id == selected.id and manifest.routine_lookup != null and std.mem.eql(u8, manifest.routine_lookup.?, name)) {
                pinned = manifest;
                break;
            };
        };
        const scope: []const domain.Target = if (pinned != null) &.{} else try targets(self.root, a, reference);
        const request: routines.SnapshotRequest = if (pinned) |manifest| .{ .targets = &.{}, .reference = .{ .database = manifest.database, .namespace = manifest.namespace, .name = manifest.name, .namespace_id = manifest.routine_namespace_id orelse return error.CatalogGenerationChanged, .id = manifest.id }, .kind = std.meta.stringToEnum(routines.records.Kind, @tagName(manifest.kind)).?, .expected_revision = self.root.revision } else .{ .targets = scope, .arguments = inputs, .expected_revision = self.root.revision, .kind = if (routine_kind == .operator) .operator else null };
        // Function/aggregate classification and body binding share one pinned
        // declaration. Do not transfer/parse a large body twice or select the
        // callable kind at one revision and execute it at another.
        const declaration_key = try std.json.Stringify.valueAlloc(a, .{ .targets = scope, .reference = request.reference, .signature = request.signature, .arguments = request.arguments, .operator = routine_kind == .operator, .dynamic = dynamic }, .{});
        const cut = self.declaration_cuts.get(declaration_key) orelse fresh: {
            if (self.declaration_cuts.count() >= 512) return error.SqlProgramLimitExceeded;
            const captured = try snapshot(self.root, a, request);
            try self.declaration_cuts.put(a, declaration_key, captured);
            break :fresh captured;
        };
        self.root.revision = cut.revision;
        const found = cut.found orelse return if (routine_kind == .procedure) error.SqlUndefinedProcedure else if (routine_kind == .operator) error.SqlUndefinedOperator else error.SqlUndefinedFunction;
        if (pinned != null and !try @import("sql_catalog_auth.zig").namespaceUsage(self.root.identity.*, a, cut.scopes[found.scope].target)) return error.Forbidden;
        if (found.record.kind != routine_kind) return error.SqlWrongObjectType;
        if (self.root.server.cfg.auth_enabled and found.record.definition.owner == .auth_disabled) return error.Forbidden;
        try self.retainManifest(a, cut, found, dynamic, name);
        return found.record;
    }
    fn dynamicResolve(raw: *anyopaque, _: A, name: []const u8, arguments: []const native.bindings.Argument) anyerror!*const native.bindings.Resolved {
        const self: *Owner = @ptrCast(@alignCast(raw));
        return self.lookup(name, arguments, true) catch |err| {
            if (err == error.OutOfMemory and self.budget.exhausted) return error.SqlWorkingMemoryLimitExceeded;
            return self.memory.mapError(err);
        };
    }
    fn resolveProcedure(raw: *anyopaque, _: A, name: []const u8, arguments: []const native.bindings.Argument) anyerror!*native.procedures.Resolved {
        const self: *Owner = @ptrCast(@alignCast(raw));
        return self.lookupProcedure(name, arguments) catch |err| {
            if (err == error.OutOfMemory and self.budget.exhausted) return error.SqlWorkingMemoryLimitExceeded;
            return self.memory.mapError(err);
        };
    }
    fn lookupProcedure(self: *Owner, name: []const u8, arguments: []const native.bindings.Argument) !*native.procedures.Resolved {
        const a = self.arena.allocator();
        try self.root.context.ensureActive();
        const key = try std.json.Stringify.valueAlloc(a, .{ .name = name, .arguments = arguments }, .{});
        if (self.procedure_names.get(key)) |ready| return ready;
        if (self.procedure_names.count() >= 128) return error.SqlProgramLimitExceeded;
        const record = try self.capture(name, arguments, .procedure, false);
        try self.captureSettings(a);
        const backend = self.root.backend();
        const ready = try native.procedures.prepare(a, record, self.resolverFor(true), backend.sequence_resolver, if (self.settings) |*settings| settings else null);
        try self.procedure_names.put(a, key, ready);
        return ready;
    }
    fn lookup(self: *Owner, name: []const u8, arguments: []const native.bindings.Argument, dynamic: bool) !*const native.bindings.Resolved {
        return self.lookupKind(name, arguments, .function, dynamic);
    }
    fn lookupKind(self: *Owner, name: []const u8, arguments: []const native.bindings.Argument, kind: routines.records.Kind, dynamic: bool) !*const native.bindings.Resolved {
        const a = self.arena.allocator();
        try self.root.context.ensureActive();
        const key = try std.json.Stringify.valueAlloc(a, .{ .name = name, .arguments = arguments, .kind = kind, .dynamic = dynamic }, .{});
        if (self.names.get(key)) |ready| return ready;
        if (self.depth >= 32 or self.names.count() >= 512) return error.SqlProgramLimitExceeded;
        self.depth += 1;
        defer self.depth -= 1;
        const record = try self.capture(name, arguments, kind, dynamic);
        const identity: Identity = .{ .id = record.id, .generation = record.generation };
        if (self.bodies.get(identity)) |ready| {
            try self.names.put(a, key, ready);
            return ready;
        }
        try self.captureSettings(a);
        const ready = try native.prepare(a, record, self.resolverFor(dynamic), if (self.settings) |*settings| settings else null);
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

pub fn bindProcedure(raw: *anyopaque, _: A, name: []const u8, arguments: []const native.bindings.Argument) !*native.procedures.Resolved {
    const root: *execution.Adapter = @ptrCast(@alignCast(raw));
    if (root.routine_owner == null) root.routine_owner = try Owner.create(root);
    return Owner.resolveProcedure(root.routine_owner.?, undefined, name, arguments);
}

pub fn bindOperator(raw: *anyopaque, _: A, name: []const u8, arguments: []const native.bindings.Argument) !*const native.bindings.Resolved {
    const root: *execution.Adapter = @ptrCast(@alignCast(raw));
    if (root.routine_owner == null) root.routine_owner = try Owner.create(root);
    return Owner.resolveOperator(root.routine_owner.?, undefined, name, arguments);
}

pub fn bindAggregate(raw: *anyopaque, _: A, name: []const u8, arguments: []const native.bindings.Argument) !?*const native.bindings.Aggregate {
    const root: *execution.Adapter = @ptrCast(@alignCast(raw));
    if (root.routine_owner == null) root.routine_owner = try Owner.create(root);
    return Owner.resolveAggregate(root.routine_owner.?, undefined, name, arguments);
}
