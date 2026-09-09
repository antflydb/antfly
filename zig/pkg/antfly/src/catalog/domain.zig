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

//! Native catalog contracts. Logical names are mutable; physical table routing
//! names and IDs are immutable. No SQL, HTTP, storage engine, or control loop
//! dependencies belong here.
const std = @import("std");

pub const default_database_name = "default";
pub const default_namespace_name = "public";
pub const default_database_id: u64 = 1;
pub const default_namespace_id: u64 = 2;
pub const max_name_bytes = 128;
pub const max_command_bytes = 3 * 1024 * 1024;
pub const Kind = enum { database, namespace, tablespace, table };

/// Routing response deliberately excludes schema, index definitions, and
/// credentials. A document operation needs only this stable identity.
pub const ResolvedTable = struct {
    table_id: u64,
    name: []const u8,

    pub fn fromTable(table: anytype) @This() {
        return .{ .table_id = table.table_id, .name = table.name };
    }

    pub fn clone(self: @This(), alloc: std.mem.Allocator) !@This() {
        return .{ .table_id = self.table_id, .name = try alloc.dupe(u8, self.name) };
    }

    pub fn deinit(self: @This(), alloc: std.mem.Allocator) void {
        alloc.free(self.name);
    }
};

pub const PlacementPolicy = struct {
    placement_role: ?[]const u8 = null,
    desired_replica_count: ?u16 = null,
    min_ranges: ?u32 = null,

    pub fn validate(self: @This()) !void {
        if (self.placement_role) |role| {
            if (role.len == 0 or role.len > max_name_bytes) return error.InvalidTablespacePlacementPolicy;
            for (role) |c| if (!std.ascii.isAlphanumeric(c) and c != '_' and c != '-') return error.InvalidTablespacePlacementPolicy;
        }
        if (self.desired_replica_count) |count| if (count == 0 or count > 255) return error.InvalidTablespacePlacementPolicy;
        if (self.min_ranges) |count| if (count == 0 or count > 1024) return error.InvalidTablespacePlacementPolicy;
    }
};

pub const Resource = struct {
    kind: Kind,
    id: u64,
    parent_id: u64 = 0,
    name: []const u8,
    /// Zero means inherit the enclosing namespace/database binding.
    tablespace_id: u64 = 0,
    /// Only table resources have a storage name. Renames never change it.
    storage_name: []const u8 = "",
    placement_policy: PlacementPolicy = .{},
    /// Compatibility metadata only, never a filesystem path override.
    location_json: []const u8 = "null",
};

pub const default_database: Resource = .{ .kind = .database, .id = default_database_id, .name = default_database_name };
pub const default_namespace: Resource = .{ .kind = .namespace, .id = default_namespace_id, .parent_id = default_database_id, .name = default_namespace_name };

pub const Target = struct {
    database: []const u8 = default_database_name,
    namespace: []const u8 = default_namespace_name,
    table: []const u8,

    pub fn parse(name: []const u8) !@This() {
        var parts = std.mem.splitScalar(u8, name, '.');
        const first = parts.next() orelse return error.InvalidCatalogName;
        const second = parts.next();
        const third = parts.next();
        if (parts.next() != null) return error.InvalidCatalogName;
        const result: @This() = if (third) |table| .{ .database = first, .namespace = second.?, .table = table } else if (second) |table| .{ .namespace = first, .table = table } else .{ .table = first };
        try result.validate();
        return result;
    }

    pub fn validate(self: @This()) !void {
        try validateName(self.database);
        try validateName(self.namespace);
        try validateName(self.table);
    }

    pub fn resourceNameAlloc(self: @This(), alloc: std.mem.Allocator) ![]u8 {
        try self.validate();
        return std.fmt.allocPrint(alloc, "{s}.{s}.{s}", .{ self.database, self.namespace, self.table });
    }
};

pub const Action = enum { create, drop, rename, set_tablespace };
pub const Mutation = struct {
    action: Action,
    kind: Kind,
    name: []const u8,
    database: []const u8 = default_database_name,
    namespace: []const u8 = default_namespace_name,
    new_name: ?[]const u8 = null,
    tablespace: ?[]const u8 = null,
    placement_policy: PlacementPolicy = .{},
    location_json: []const u8 = "null",
    /// Populated only by native table admission, never accepted from public JSON.
    table_id: u64 = 0,
    storage_name: []const u8 = "",
};

pub const State = struct {
    revision: u64 = 0,
    next_id: u64 = 3,
    resources: []const Resource = &.{},

    pub fn find(self: @This(), kind: Kind, parent_id: u64, name: []const u8) ?Resource {
        for (self.resources) |r| if (r.kind == kind and r.parent_id == parent_id and std.mem.eql(u8, r.name, name)) return r;
        if (kind == .database and parent_id == 0 and std.mem.eql(u8, name, default_database_name)) return default_database;
        if (kind == .namespace and parent_id == default_database_id and std.mem.eql(u8, name, default_namespace_name)) return default_namespace;
        return null;
    }

    pub fn byId(self: @This(), kind: Kind, id: u64) ?Resource {
        for (self.resources) |r| if (r.kind == kind and r.id == id) return r;
        if (kind == .database and id == default_database_id) return default_database;
        if (kind == .namespace and id == default_namespace_id) return default_namespace;
        return null;
    }

    pub fn namespaceFor(self: @This(), database: []const u8, namespace: []const u8) !Resource {
        const db = self.find(.database, 0, database) orelse return error.DatabaseNotFound;
        return self.find(.namespace, db.id, namespace) orelse error.NamespaceNotFound;
    }

    pub fn effectiveTablespace(self: @This(), namespace: Resource, explicit: u64) !?Resource {
        const database = self.byId(.database, namespace.parent_id) orelse return error.DatabaseNotFound;
        const id = if (explicit != 0) explicit else if (namespace.tablespace_id != 0) namespace.tablespace_id else database.tablespace_id;
        if (id == 0) return null;
        return self.byId(.tablespace, id) orelse error.TablespaceNotFound;
    }
};

/// A deterministic single-command delta. All data is borrowed from the request
/// and snapshot; the allocator owns only the delta arrays.
pub const Delta = struct {
    upserts: []Resource,
    removes: []Resource,
    next_id: u64,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.upserts);
        alloc.free(self.removes);
        self.* = undefined;
    }
};

/// Table IDs visible in the same committed snapshot, including legacy tables
/// whose names are implicitly in default.public. The apply layer supplies these
/// to prevent deleting a namespace/database that still owns physical tables.
pub const PhysicalTable = struct { id: u64, name: []const u8 };

pub fn plan(alloc: std.mem.Allocator, state: State, request: Mutation, tables: []const PhysicalTable) !Delta {
    try validateName(request.name);
    try validateName(request.database);
    try validateName(request.namespace);
    if (request.new_name) |name| try validateName(name);
    if (request.tablespace) |name| try validateName(name);
    try request.placement_policy.validate();
    if (request.location_json.len > 64 * 1024) return error.InvalidTablespaceLocation;
    var location = std.json.parseFromSlice(std.json.Value, alloc, request.location_json, .{}) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidTablespaceLocation,
    };
    defer location.deinit();
    if (request.kind != .tablespace and (!std.mem.eql(u8, request.location_json, "null") or
        request.placement_policy.placement_role != null or request.placement_policy.desired_replica_count != null or request.placement_policy.min_ranges != null))
        return error.InvalidCatalogMutation;
    const parent_id: u64 = switch (request.kind) {
        .database, .tablespace => 0,
        .namespace => (state.find(.database, 0, request.database) orelse return error.DatabaseNotFound).id,
        .table => (try state.namespaceFor(request.database, request.namespace)).id,
    };
    var found = state.find(request.kind, parent_id, request.name);
    if (found == null and request.kind == .table and parent_id == default_namespace_id and request.action != .create) {
        for (tables) |table| if (std.mem.eql(u8, table.name, request.name) and !hasBinding(state, table.id)) {
            found = .{ .kind = .table, .id = table.id, .parent_id = default_namespace_id, .name = table.name, .storage_name = table.name };
            break;
        };
    }
    var upserts = std.ArrayListUnmanaged(Resource).empty;
    defer upserts.deinit(alloc);
    var removes = std.ArrayListUnmanaged(Resource).empty;
    defer removes.deinit(alloc);
    var next_id = state.next_id;
    switch (request.action) {
        .create => {
            if (found != null) return error.CatalogAlreadyExists;
            if (request.new_name != null) return error.InvalidCatalogMutation;
            const binding = if (request.tablespace) |name| (state.find(.tablespace, 0, name) orelse return error.TablespaceNotFound).id else 0;
            if (request.kind == .tablespace and binding != 0) return error.InvalidCatalogMutation;
            var resource: Resource = .{ .kind = request.kind, .id = next_id, .parent_id = parent_id, .name = request.name, .tablespace_id = binding, .placement_policy = request.placement_policy, .location_json = request.location_json };
            if (request.kind == .table) {
                if (request.table_id == 0 or request.storage_name.len == 0) return error.InvalidCatalogMutation;
                for (state.resources) |r| if (r.kind == .table and (r.id == request.table_id or std.mem.eql(u8, r.storage_name, request.storage_name))) return error.CatalogAlreadyExists;
                for (tables) |t| {
                    if (t.id == request.table_id or std.mem.eql(u8, t.name, request.storage_name)) return error.CatalogAlreadyExists;
                    if (parent_id == default_namespace_id and std.mem.eql(u8, t.name, request.name) and !hasBinding(state, t.id)) return error.CatalogAlreadyExists;
                }
                resource.id = request.table_id;
                resource.storage_name = request.storage_name;
            }
            next_id = std.math.add(u64, next_id, 1) catch return error.CatalogIdExhausted;
            try upserts.append(alloc, resource);
            if (request.kind == .database) {
                try upserts.append(alloc, .{ .kind = .namespace, .id = next_id, .parent_id = resource.id, .name = default_namespace_name });
                next_id = std.math.add(u64, next_id, 1) catch return error.CatalogIdExhausted;
            }
        },
        .drop, .rename, .set_tablespace => {
            const existing = found orelse return error.CatalogNotFound;
            if (request.action == .set_tablespace) {
                if (existing.kind == .tablespace or request.new_name != null) return error.InvalidCatalogMutation;
                var updated = existing;
                updated.tablespace_id = if (request.tablespace) |name| (state.find(.tablespace, 0, name) orelse return error.TablespaceNotFound).id else 0;
                try upserts.append(alloc, updated);
            } else {
                if ((existing.kind == .database and existing.id == default_database_id) or (existing.kind == .namespace and existing.id == default_namespace_id)) return error.ProtectedCatalogResource;
                if (request.action == .rename) {
                    const name = request.new_name orelse return error.InvalidCatalogMutation;
                    if (state.find(existing.kind, parent_id, name)) |other| if (other.id != existing.id) return error.CatalogAlreadyExists;
                    if (existing.kind == .table and parent_id == default_namespace_id) for (tables) |t| {
                        if (!hasBinding(state, t.id) and std.mem.eql(u8, t.name, name)) return error.CatalogAlreadyExists;
                    };
                    var updated = existing;
                    updated.name = name;
                    try upserts.append(alloc, updated);
                } else {
                    for (state.resources) |r| {
                        if (existing.kind == .tablespace and r.tablespace_id == existing.id) return error.TablespaceInUse;
                        if (existing.kind == .namespace and r.kind == .table and r.parent_id == existing.id) return error.NamespaceNotEmpty;
                        if (existing.kind == .database and r.kind == .namespace and r.parent_id == existing.id) {
                            for (state.resources) |child| if (child.kind == .table and child.parent_id == r.id) return error.DatabaseNotEmpty;
                            try removes.append(alloc, r);
                        }
                    }
                    // Native table deletion is completed with a topology command,
                    // never by deleting its binding while data is still reachable.
                    if (existing.kind == .table) return error.CatalogTableTopologyRequired;
                    try removes.append(alloc, existing);
                }
            }
        },
    }
    const owned_upserts = try upserts.toOwnedSlice(alloc);
    errdefer alloc.free(owned_upserts);
    return .{ .upserts = owned_upserts, .removes = try removes.toOwnedSlice(alloc), .next_id = next_id };
}

pub fn hasBinding(state: State, id: u64) bool {
    return state.byId(.table, id) != null;
}

pub fn validateName(name: []const u8) !void {
    if (name.len == 0 or name.len > max_name_bytes) return error.InvalidCatalogName;
    // One unambiguous spelling across HTTP, CLI, authorization and future SQL.
    // Dots delimit catalog components; ':' is reserved for physical resources.
    if (!std.ascii.isAlphabetic(name[0]) and name[0] != '_') return error.InvalidCatalogName;
    for (name[1..]) |c| if (!std.ascii.isAlphanumeric(c) and c != '_' and c != '-') return error.InvalidCatalogName;
}

test "catalog rename preserves IDs and tablespace binding inheritance" {
    const alloc = std.testing.allocator;
    const resources = [_]Resource{
        .{ .kind = .database, .id = 3, .name = "analytics", .tablespace_id = 8 },
        .{ .kind = .namespace, .id = 4, .parent_id = 3, .name = "public" },
        .{ .kind = .tablespace, .id = 8, .name = "hot", .placement_policy = .{ .placement_role = "hot", .desired_replica_count = 3 } },
        .{ .kind = .table, .id = 42, .parent_id = 4, .name = "events", .storage_name = "table:42" },
    };
    const state: State = .{ .resources = &resources, .next_id = 9 };
    const cases = [_]struct { kind: Kind, name: []const u8, new_name: []const u8, id: u64 }{
        .{ .kind = .database, .name = "analytics", .new_name = "reports", .id = 3 },
        .{ .kind = .namespace, .name = "public", .new_name = "serving", .id = 4 },
        .{ .kind = .tablespace, .name = "hot", .new_name = "fast", .id = 8 },
        .{ .kind = .table, .name = "events", .new_name = "logs", .id = 42 },
    };
    for (cases) |case| {
        var delta = try plan(alloc, state, .{ .action = .rename, .kind = case.kind, .name = case.name, .new_name = case.new_name, .database = "analytics" }, &.{});
        defer delta.deinit(alloc);
        try std.testing.expectEqual(case.id, delta.upserts[0].id);
        try std.testing.expectEqualStrings(case.new_name, delta.upserts[0].name);
        if (case.kind == .table) try std.testing.expectEqualStrings("table:42", delta.upserts[0].storage_name);
    }
    try std.testing.expectEqual(@as(u64, 8), (try state.effectiveTablespace(resources[1], 0)).?.id);
    try std.testing.expectError(error.TablespaceInUse, plan(alloc, state, .{ .action = .drop, .kind = .tablespace, .name = "hot" }, &.{}));
    try std.testing.expectError(error.DatabaseNotEmpty, plan(alloc, state, .{ .action = .drop, .kind = .database, .name = "analytics" }, &.{}));
    try std.testing.expectError(error.NamespaceNotEmpty, plan(alloc, state, .{ .action = .drop, .kind = .namespace, .database = "analytics", .name = "public" }, &.{}));
}

test "catalog names and placement policies reject ambiguous or invalid input" {
    for ([_][]const u8{ "", ".", "a.b", "a/b", "a%2fb", "a:b", "../x", "1bad" }) |name| try std.testing.expectError(error.InvalidCatalogName, validateName(name));
    try validateName("tenant-west");
    try std.testing.expectError(error.InvalidTablespacePlacementPolicy, (PlacementPolicy{ .desired_replica_count = 0 }).validate());
    try std.testing.expectError(error.ProtectedCatalogResource, plan(std.testing.allocator, .{}, .{ .action = .rename, .kind = .database, .name = "default", .new_name = "other" }, &.{}));
}

pub const Request = struct {
    mutation: Mutation,
    create_table_json: ?[]const u8 = null,
    /// Trusted ingress allocates identity before sealing durable destinations.
    physical_name: ?[]const u8 = null,
};

pub const Call = union(enum) {
    snapshot: void,
    resolve: Target,
    mutate: Request,
};

pub fn httpStatus(err: anyerror) u16 {
    return switch (err) {
        error.DatabaseNotFound, error.NamespaceNotFound, error.TablespaceNotFound, error.CatalogNotFound, error.TableNotFound => 404,
        error.CatalogAlreadyExists, error.CatalogGenerationChanged, error.TablespaceInUse, error.NamespaceNotEmpty, error.DatabaseNotEmpty, error.ProtectedCatalogResource, error.TableAlreadyExists => 409,
        error.InvalidCatalogName, error.InvalidCatalogMutation, error.InvalidTablespaceLocation, error.InvalidTablespacePlacementPolicy, error.InvalidCreateTableRequest => 400,
        error.CatalogCommandTooLarge, error.CreateTableRequestTooLarge => 413,
        error.TableTopologyProtocolUpgradeRequired => 426,
        error.Forbidden => 403,
        error.UnsupportedOperation => 503,
        error.MetadataMutationOutcomeUnknown, error.NotLeader, error.Timeout, error.Cancelled, error.Canceled, error.DeadlineExceeded => 503,
        else => 500,
    };
}

/// Only trusted native ingress constructs these immutable routing identities.
pub fn validateStorageName(name: []const u8) !void {
    if (name.len < 38 or !std.mem.startsWith(u8, name, "table:")) return error.InvalidCatalogMutation;
    for (name[6..38]) |c| if (!std.ascii.isHex(c)) return error.InvalidCatalogMutation;
    if (name.len == 38) return;
    if (name[38] != ':') return error.InvalidCatalogMutation;
    _ = try Target.parse(name[39..]);
}

/// Restore jobs persist their immutable destination identity. The qualified
/// suffix carries admission intent until table topology and the binding commit
/// together; normal reads always use the durable catalog indexes.
pub fn restoreTarget(physical_name: []const u8) !?Target {
    if (!std.mem.startsWith(u8, physical_name, "table:")) return null;
    const suffix = std.mem.indexOfScalarPos(u8, physical_name, 6, ':') orelse return null;
    return try Target.parse(physical_name[suffix + 1 ..]);
}

test "native catalog legacy identity can bind and rename without moving storage" {
    const alloc = std.testing.allocator;
    const physical = [_]PhysicalTable{.{ .id = 41, .name = "docs" }};
    var renamed = try plan(alloc, .{}, .{ .action = .rename, .kind = .table, .name = "docs", .new_name = "articles" }, &physical);
    defer renamed.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 41), renamed.upserts[0].id);
    try std.testing.expectEqualStrings("docs", renamed.upserts[0].storage_name);
    try std.testing.expectEqualStrings("articles", renamed.upserts[0].name);
    try std.testing.expectError(error.CatalogAlreadyExists, plan(alloc, .{}, .{ .action = .create, .kind = .table, .name = "docs", .table_id = 42, .storage_name = "table:new" }, &physical));
}

test "native catalog policy precedence and table overrides are explicit" {
    const resources = [_]Resource{
        .{ .kind = .database, .id = 3, .name = "analytics", .tablespace_id = 10 },
        .{ .kind = .namespace, .id = 4, .parent_id = 3, .name = "public", .tablespace_id = 11 },
        .{ .kind = .tablespace, .id = 10, .name = "database_policy" },
        .{ .kind = .tablespace, .id = 11, .name = "namespace_policy" },
        .{ .kind = .tablespace, .id = 12, .name = "table_policy" },
    };
    const state: State = .{ .resources = &resources };
    try std.testing.expectEqual(@as(u64, 12), (try state.effectiveTablespace(resources[1], 12)).?.id);
    try std.testing.expectEqual(@as(u64, 11), (try state.effectiveTablespace(resources[1], 0)).?.id);
    var unbound = resources[1];
    unbound.tablespace_id = 0;
    try std.testing.expectEqual(@as(u64, 10), (try state.effectiveTablespace(unbound, 0)).?.id);
}

test "native catalog mutation planning releases allocations on every failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(alloc: std.mem.Allocator) !void {
            var delta = try plan(alloc, .{}, .{ .action = .create, .kind = .database, .name = "analytics" }, &.{});
            defer delta.deinit(alloc);
        }
    }.run, .{});
}

pub fn cloneStateAlloc(alloc: std.mem.Allocator, state: State) !std.json.Parsed(State) {
    const bytes = try std.json.Stringify.valueAlloc(alloc, state, .{});
    defer alloc.free(bytes);
    return std.json.parseFromSlice(State, alloc, bytes, .{ .allocate = .alloc_always });
}

pub fn applyDeltaStateAlloc(alloc: std.mem.Allocator, state: State, delta: Delta) !std.json.Parsed(State) {
    var resources = std.ArrayListUnmanaged(Resource).empty;
    defer resources.deinit(alloc);
    const existing = if (state.revision == 0) &[_]Resource{ default_database, default_namespace } else state.resources;
    for (existing) |resource| {
        var replaced = false;
        for (delta.removes) |item| if (item.kind == resource.kind and item.id == resource.id) {
            replaced = true;
            break;
        };
        for (delta.upserts) |item| if (item.kind == resource.kind and item.id == resource.id) {
            replaced = true;
            break;
        };
        if (!replaced) try resources.append(alloc, resource);
    }
    try resources.appendSlice(alloc, delta.upserts);
    return cloneStateAlloc(alloc, .{ .revision = try std.math.add(u64, state.revision, 1), .next_id = delta.next_id, .resources = resources.items });
}

pub fn tableResourceMatches(grant: []const u8, target: []const u8) bool {
    if (std.mem.eql(u8, grant, "*") or std.mem.eql(u8, grant, target)) return true;
    const right = Target.parse(target) catch return false;
    var buf: [512]u8 = undefined;
    const canonical = std.fmt.bufPrint(&buf, "{s}.{s}.{s}", .{ right.database, right.namespace, right.table }) catch return false;
    if (std.mem.endsWith(u8, grant, ".*")) return std.mem.startsWith(u8, canonical, grant[0 .. grant.len - 1]);
    const left = Target.parse(grant) catch return false;
    return std.mem.eql(u8, left.database, right.database) and std.mem.eql(u8, left.namespace, right.namespace) and std.mem.eql(u8, left.table, right.table);
}
