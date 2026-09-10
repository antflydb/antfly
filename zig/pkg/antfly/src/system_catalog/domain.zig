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

//! System catalog contracts. Logical names are mutable; physical table routing
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

/// Request-owned query preparation data, excluding topology and unrelated
/// tables. Read schema and index generations are captured together.
pub const QueryDefinition = struct {
    schema_json: []const u8,
    read_schema_json: []const u8,
    indexes_json: []const u8,

    pub fn fromTable(table: anytype) @This() {
        return .{ .schema_json = table.schema_json, .read_schema_json = table.read_schema_json, .indexes_json = table.indexes_json };
    }
    pub fn clone(self: @This(), alloc: std.mem.Allocator) !@This() {
        const schema = try alloc.dupe(u8, self.schema_json);
        errdefer alloc.free(schema);
        const read_schema = try alloc.dupe(u8, self.read_schema_json);
        errdefer alloc.free(read_schema);
        return .{ .schema_json = schema, .read_schema_json = read_schema, .indexes_json = try alloc.dupe(u8, self.indexes_json) };
    }
    pub fn deinit(self: @This(), alloc: std.mem.Allocator) void {
        alloc.free(self.schema_json);
        alloc.free(self.read_schema_json);
        alloc.free(self.indexes_json);
    }
};

/// Document operations request only identity; query admission can include a
/// narrow definition captured in the same catalog read transaction.
pub const ResolvedTable = struct {
    table_id: u64,
    name: []const u8,
    query_definition: ?QueryDefinition = null,

    pub fn fromTable(table: anytype) @This() {
        return .{ .table_id = table.table_id, .name = table.name };
    }

    pub fn jsonStringify(self: @This(), jw: anytype) !void {
        if (self.query_definition) |definition| {
            try jw.write(.{ .table_id = self.table_id, .name = self.name, .query_definition = definition });
        } else {
            try jw.write(.{ .table_id = self.table_id, .name = self.name });
        }
    }

    pub fn clone(self: @This(), alloc: std.mem.Allocator) !@This() {
        const name = try alloc.dupe(u8, self.name);
        errdefer alloc.free(name);
        return .{ .table_id = self.table_id, .name = name, .query_definition = if (self.query_definition) |definition| try definition.clone(alloc) else null };
    }

    pub fn deinit(self: @This(), alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        if (self.query_definition) |definition| definition.deinit(alloc);
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

/// Public targets are objects. Internal policy keys are length-framed and
/// start with NUL, which is forbidden in legacy literal table names. They are
/// never accepted through a public string field or returned as display names.
pub const target_key_prefix = "\x00catalog:";
pub const Target = struct {
    database: []const u8 = default_database_name,
    namespace: []const u8 = default_namespace_name,
    table: []const u8,

    pub fn literal(name: []const u8) !@This() {
        const target: @This() = .{ .table = name };
        try target.validate();
        return target;
    }

    /// Internal adapters may receive a policy key; every other string is a
    /// literal name in default.public. Qualification is never guessed.
    pub fn parse(name: []const u8) !@This() {
        if (!std.mem.startsWith(u8, name, target_key_prefix)) return literal(name);
        const scope = try TableScope.fromKey(name);
        const target: @This() = .{ .database = scope.database, .namespace = scope.namespace, .table = scope.table orelse return error.InvalidCatalogName };
        try target.validate();
        return target;
    }

    pub fn validate(self: @This()) !void {
        try validateName(self.database);
        try validateName(self.namespace);
        try validateTableName(self.table);
    }

    pub fn resourceNameAlloc(self: @This(), alloc: std.mem.Allocator) ![]u8 {
        try self.validate();
        return (TableScope{ .database = self.database, .namespace = self.namespace, .table = self.table }).keyAlloc(alloc);
    }

    pub fn displayNameAlloc(self: @This(), alloc: std.mem.Allocator) ![]u8 {
        try self.validate();
        if (self.isDefault()) return alloc.dupe(u8, self.table);
        return std.fmt.allocPrint(alloc, "{s}.{s}.{s}", .{ self.database, self.namespace, self.table });
    }

    pub fn isDefault(self: @This()) bool {
        return std.mem.eql(u8, self.database, default_database_name) and std.mem.eql(u8, self.namespace, default_namespace_name);
    }
};

/// A missing table selects the namespace; a literal "*" selects a table named
/// "*". This distinction survives policy persistence and API round trips.
pub const TableScope = struct {
    database: []const u8 = default_database_name,
    namespace: []const u8 = default_namespace_name,
    table: ?[]const u8 = null,

    pub fn keyAlloc(self: @This(), alloc: std.mem.Allocator) ![]u8 {
        try validateName(self.database);
        try validateName(self.namespace);
        if (self.table) |table| {
            try validateTableName(table);
            return std.fmt.allocPrint(alloc, target_key_prefix ++ "{d}:{s}{d}:{s}{d}:{s}", .{ self.database.len, self.database, self.namespace.len, self.namespace, table.len, table });
        }
        return std.fmt.allocPrint(alloc, target_key_prefix ++ "{d}:{s}{d}:{s}*", .{ self.database.len, self.database, self.namespace.len, self.namespace });
    }

    pub fn fromKey(key: []const u8) !@This() {
        if (!std.mem.startsWith(u8, key, target_key_prefix)) return error.InvalidCatalogName;
        var rest = key[target_key_prefix.len..];
        const database = try takeComponent(&rest);
        const namespace = try takeComponent(&rest);
        const table: ?[]const u8 = if (std.mem.eql(u8, rest, "*")) blk: {
            rest = "";
            break :blk null;
        } else try takeComponent(&rest);
        if (rest.len != 0) return error.InvalidCatalogName;
        try validateName(database);
        try validateName(namespace);
        if (table) |name| try validateTableName(name);
        return .{ .database = database, .namespace = namespace, .table = table };
    }

    fn takeComponent(rest: *[]const u8) ![]const u8 {
        const colon = std.mem.indexOfScalar(u8, rest.*, ':') orelse return error.InvalidCatalogName;
        const length = std.fmt.parseInt(usize, rest.*[0..colon], 10) catch return error.InvalidCatalogName;
        const start = colon + 1;
        if (length > rest.len - start) return error.InvalidCatalogName;
        const value = rest.*[start..][0..length];
        rest.* = rest.*[start + length ..];
        return value;
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

/// Borrowed indexes over one immutable state. Rebuild before publication; keys
/// borrow state strings and must be destroyed before their owner.
pub const StateIndex = struct {
    const Name = struct { kind: Kind, parent: u64, name: []const u8 };
    const NameContext = struct {
        pub fn hash(_: @This(), key: Name) u64 {
            var h = std.hash.Wyhash.init(@intFromEnum(key.kind));
            h.update(std.mem.asBytes(&key.parent));
            h.update(key.name);
            return h.final();
        }
        pub fn eql(_: @This(), a: Name, b: Name) bool {
            return a.kind == b.kind and a.parent == b.parent and std.mem.eql(u8, a.name, b.name);
        }
    };
    const Id = struct { kind: Kind, id: u64 };
    names: std.HashMapUnmanaged(Name, Resource, NameContext, 80) = .empty,
    ids: std.AutoHashMapUnmanaged(Id, Resource) = .empty,

    pub fn init(alloc: std.mem.Allocator, state: State) !StateIndex {
        var self: StateIndex = .{};
        errdefer self.deinit(alloc);
        try self.names.ensureTotalCapacity(alloc, @intCast(state.resources.len));
        try self.ids.ensureTotalCapacity(alloc, @intCast(state.resources.len));
        for (state.resources) |r| {
            const name = try self.names.getOrPut(alloc, .{ .kind = r.kind, .parent = r.parent_id, .name = r.name });
            if (name.found_existing) return error.InvalidCatalogRecord;
            name.value_ptr.* = r;
            const id = try self.ids.getOrPut(alloc, .{ .kind = r.kind, .id = r.id });
            if (id.found_existing) return error.InvalidCatalogRecord;
            id.value_ptr.* = r;
        }
        return self;
    }
    pub fn deinit(self: *StateIndex, alloc: std.mem.Allocator) void {
        self.names.deinit(alloc);
        self.ids.deinit(alloc);
        self.* = undefined;
    }
    pub fn find(self: *const StateIndex, kind: Kind, parent: u64, name: []const u8) ?Resource {
        return self.names.get(.{ .kind = kind, .parent = parent, .name = name }) orelse (State{}).find(kind, parent, name);
    }
    pub fn byId(self: *const StateIndex, kind: Kind, id: u64) ?Resource {
        return self.ids.get(.{ .kind = kind, .id = id }) orelse (State{}).byId(kind, id);
    }
    pub fn namespaceFor(self: *const StateIndex, database: []const u8, namespace: []const u8) !Resource {
        const db = self.find(.database, 0, database) orelse return error.DatabaseNotFound;
        return self.find(.namespace, db.id, namespace) orelse error.NamespaceNotFound;
    }
};

pub const IndexedState = struct {
    alloc: std.mem.Allocator,
    owned: std.json.Parsed(State),
    value: State,
    index: StateIndex,

    /// Consumes owned on success; callers retain it on failure.
    pub fn init(alloc: std.mem.Allocator, owned: std.json.Parsed(State)) !IndexedState {
        return .{ .alloc = alloc, .owned = owned, .value = owned.value, .index = try StateIndex.init(alloc, owned.value) };
    }
    pub fn clone(alloc: std.mem.Allocator, state: State) !IndexedState {
        var owned = try cloneStateAlloc(alloc, state);
        errdefer owned.deinit();
        return init(alloc, owned);
    }
    pub fn deinit(self: *IndexedState) void {
        self.index.deinit(self.alloc);
        self.owned.deinit();
        self.* = undefined;
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
    var index = try StateIndex.init(alloc, state);
    defer index.deinit(alloc);
    try validateResourceName(request.kind, request.name);
    try validateName(request.database);
    try validateName(request.namespace);
    if (request.new_name) |name| try validateResourceName(request.kind, name);
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
        .namespace => (index.find(.database, 0, request.database) orelse return error.DatabaseNotFound).id,
        .table => (try index.namespaceFor(request.database, request.namespace)).id,
    };
    var found = index.find(request.kind, parent_id, request.name);
    if (found == null and request.kind == .table and parent_id == default_namespace_id and request.action != .create) {
        for (tables) |table| if (std.mem.eql(u8, table.name, request.name) and index.byId(.table, table.id) == null) {
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
            const binding = if (request.tablespace) |name| (index.find(.tablespace, 0, name) orelse return error.TablespaceNotFound).id else 0;
            if (request.kind == .tablespace and binding != 0) return error.InvalidCatalogMutation;
            var resource: Resource = .{ .kind = request.kind, .id = next_id, .parent_id = parent_id, .name = request.name, .tablespace_id = binding, .placement_policy = request.placement_policy, .location_json = request.location_json };
            if (request.kind == .table) {
                if (request.table_id == 0 or request.storage_name.len == 0) return error.InvalidCatalogMutation;
                for (state.resources) |r| if (r.kind == .table and (r.id == request.table_id or std.mem.eql(u8, r.storage_name, request.storage_name))) return error.CatalogAlreadyExists;
                for (tables) |t| {
                    if (t.id == request.table_id or std.mem.eql(u8, t.name, request.storage_name)) return error.CatalogAlreadyExists;
                    if (parent_id == default_namespace_id and std.mem.eql(u8, t.name, request.name) and index.byId(.table, t.id) == null) return error.CatalogAlreadyExists;
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
                updated.tablespace_id = if (request.tablespace) |name| (index.find(.tablespace, 0, name) orelse return error.TablespaceNotFound).id else 0;
                try upserts.append(alloc, updated);
            } else {
                if ((existing.kind == .database and existing.id == default_database_id) or (existing.kind == .namespace and existing.id == default_namespace_id)) return error.ProtectedCatalogResource;
                if (request.action == .rename) {
                    const name = request.new_name orelse return error.InvalidCatalogMutation;
                    if (index.find(existing.kind, parent_id, name)) |other| if (other.id != existing.id) return error.CatalogAlreadyExists;
                    if (existing.kind == .table and parent_id == default_namespace_id) for (tables) |t| {
                        if (std.mem.eql(u8, t.name, name) and index.byId(.table, t.id) == null) return error.CatalogAlreadyExists;
                    };
                    var updated = existing;
                    updated.name = name;
                    try upserts.append(alloc, updated);
                } else {
                    for (state.resources) |r| {
                        if (existing.kind == .database and r.kind == .table) {
                            const parent = index.byId(.namespace, r.parent_id) orelse return error.InvalidCatalogRecord;
                            if (parent.parent_id == existing.id) return error.DatabaseNotEmpty;
                        }
                        if (existing.kind == .tablespace and r.tablespace_id == existing.id) return error.TablespaceInUse;
                        if (existing.kind == .namespace and r.kind == .table and r.parent_id == existing.id) return error.NamespaceNotEmpty;
                        if (existing.kind == .database and r.kind == .namespace and r.parent_id == existing.id) {
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

pub const ResolveMany = struct {
    targets: []const Target,
    include_query_definitions: bool = false,
    expected_revision: ?u64 = null,
};

pub const ResolvedMany = struct {
    revision: u64,
    tables: []const ?ResolvedTable,

    pub fn deinit(self: @This(), alloc: std.mem.Allocator) void {
        for (self.tables) |table| if (table) |value| value.deinit(alloc);
        alloc.free(self.tables);
    }
};

pub const Call = union(enum) {
    snapshot: void,
    resolve: Target,
    resolve_many: ResolveMany,
    query_definition: []const u8,
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
    if (!try isRestoreTarget(name)) return error.InvalidCatalogMutation;
}

/// Restore jobs persist their immutable destination identity. The qualified
/// suffix carries admission intent until table topology and the binding commit
/// together; normal reads always use the durable catalog indexes.
pub const OwnedRestoreTarget = struct {
    buffer: []u8,
    value: Target,
    pub fn deinit(self: @This(), alloc: std.mem.Allocator) void {
        alloc.free(self.buffer);
    }
};

pub fn restoreStorageNameAlloc(alloc: std.mem.Allocator, physical: []const u8, target: Target) ![]u8 {
    const key = try target.resourceNameAlloc(alloc);
    defer alloc.free(key);
    const codec = std.base64.url_safe_no_pad.Encoder;
    const out = try alloc.alloc(u8, physical.len + 1 + codec.calcSize(key.len));
    @memcpy(out[0..physical.len], physical);
    out[physical.len] = ':';
    _ = codec.encode(out[physical.len + 1 ..], key);
    return out;
}

fn restoreTargetInto(buffer: []u8, physical_name: []const u8) !?Target {
    if (!std.mem.startsWith(u8, physical_name, "table:") or physical_name.len <= 38) return null;
    if (physical_name[38] != ':') return error.InvalidCatalogName;
    for (physical_name[6..38]) |c| if (!std.ascii.isHex(c)) return error.InvalidCatalogName;
    const encoded = physical_name[39..];
    const codec = std.base64.url_safe_no_pad.Decoder;
    const size = codec.calcSizeForSlice(encoded) catch return error.InvalidCatalogName;
    if (size > buffer.len) return error.InvalidCatalogName;
    codec.decode(buffer[0..size], encoded) catch return error.InvalidCatalogName;
    if (!std.mem.startsWith(u8, buffer[0..size], target_key_prefix)) return error.InvalidCatalogName;
    return try Target.parse(buffer[0..size]);
}

pub fn isRestoreTarget(physical_name: []const u8) !bool {
    var buffer: [1024]u8 = undefined;
    return (try restoreTargetInto(&buffer, physical_name)) != null;
}

pub fn restoreTarget(alloc: std.mem.Allocator, physical_name: []const u8) !?OwnedRestoreTarget {
    if (!std.mem.startsWith(u8, physical_name, "table:") or physical_name.len <= 38) return null;
    const buffer = try alloc.alloc(u8, 1024);
    errdefer alloc.free(buffer);
    const value = (try restoreTargetInto(buffer, physical_name)) orelse {
        alloc.free(buffer);
        return null;
    };
    return .{ .buffer = buffer, .value = value };
}

test "system catalog legacy identity can bind and rename without moving storage" {
    const alloc = std.testing.allocator;
    const physical = [_]PhysicalTable{.{ .id = 41, .name = "docs" }};
    var renamed = try plan(alloc, .{}, .{ .action = .rename, .kind = .table, .name = "docs", .new_name = "articles" }, &physical);
    defer renamed.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 41), renamed.upserts[0].id);
    try std.testing.expectEqualStrings("docs", renamed.upserts[0].storage_name);
    try std.testing.expectEqualStrings("articles", renamed.upserts[0].name);
    try std.testing.expectError(error.CatalogAlreadyExists, plan(alloc, .{}, .{ .action = .create, .kind = .table, .name = "docs", .table_id = 42, .storage_name = "table:new" }, &physical));
}

test "system catalog policy precedence and table overrides are explicit" {
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

test "system catalog mutation planning releases allocations on every failure" {
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
    var replaced: std.AutoHashMapUnmanaged(StateIndex.Id, void) = .empty;
    defer replaced.deinit(alloc);
    try replaced.ensureTotalCapacity(alloc, @intCast(delta.removes.len + delta.upserts.len));
    for (delta.removes) |item| replaced.putAssumeCapacity(.{ .kind = item.kind, .id = item.id }, {});
    for (delta.upserts) |item| replaced.putAssumeCapacity(.{ .kind = item.kind, .id = item.id }, {});
    for (existing) |resource| {
        if (!replaced.contains(.{ .kind = resource.kind, .id = resource.id })) try resources.append(alloc, resource);
    }
    try resources.appendSlice(alloc, delta.upserts);
    return cloneStateAlloc(alloc, .{ .revision = try std.math.add(u64, state.revision, 1), .next_id = delta.next_id, .resources = resources.items });
}

pub fn tableResourceMatches(grant: []const u8, target: []const u8) bool {
    if (std.mem.eql(u8, grant, "*")) return true;
    const right = Target.parse(target) catch return false;
    if (std.mem.startsWith(u8, grant, target_key_prefix)) {
        const scope = TableScope.fromKey(grant) catch return false;
        return std.mem.eql(u8, scope.database, right.database) and
            std.mem.eql(u8, scope.namespace, right.namespace) and
            (scope.table == null or std.mem.eql(u8, scope.table.?, right.table));
    }
    return right.isDefault() and std.mem.eql(u8, grant, right.table);
}

/// Whether every resource in `narrow` is covered by `wide`.
pub fn tableScopeContains(wide: []const u8, narrow: []const u8) bool {
    if (std.mem.eql(u8, wide, "*")) return true;
    if (std.mem.eql(u8, narrow, "*")) return false;
    if (std.mem.startsWith(u8, narrow, target_key_prefix)) {
        const scope = TableScope.fromKey(narrow) catch return false;
        if (scope.table == null) {
            if (!std.mem.startsWith(u8, wide, target_key_prefix)) return false;
            const parent = TableScope.fromKey(wide) catch return false;
            return parent.table == null and std.mem.eql(u8, parent.database, scope.database) and std.mem.eql(u8, parent.namespace, scope.namespace);
        }
    }
    return tableResourceMatches(wide, narrow);
}

pub fn validateTableName(name: []const u8) !void {
    if (name.len == 0 or name.len > 255) return error.InvalidCatalogName;
    for (name) |c| if (c < 0x20 or c == 0x7f) return error.InvalidCatalogName;
}

pub fn validateResourceName(kind: Kind, name: []const u8) !void {
    if (kind == .table) return validateTableName(name);
    return validateName(name);
}

test "catalog targets preserve literal names and distinguish exact from scoped grants" {
    const alloc = std.testing.allocator;
    for ([_][]const u8{ "sales.events", "docs table", "path/table", "*", "table:legacy", "1table" }) |name| {
        const target = try Target.literal(name);
        const key = try target.resourceNameAlloc(alloc);
        defer alloc.free(key);
        try std.testing.expectEqualStrings(name, (try Target.parse(key)).table);
        try std.testing.expect(tableResourceMatches(name, key));
    }
    const scoped = try (Target{ .database = "sales", .table = "events" }).resourceNameAlloc(alloc);
    defer alloc.free(scoped);
    try std.testing.expect(!tableResourceMatches("sales.public.events", scoped));
    const wildcard = try (TableScope{ .database = "sales" }).keyAlloc(alloc);
    defer alloc.free(wildcard);
    try std.testing.expect(tableResourceMatches(wildcard, scoped));
    const literal_star = try (Target{ .database = "sales", .table = "*" }).resourceNameAlloc(alloc);
    defer alloc.free(literal_star);
    try std.testing.expect(!tableResourceMatches(literal_star, scoped));
    try std.testing.expectError(error.InvalidCatalogName, Target.literal(scoped));
}

/// Borrowed catalog adapter for background operations. Bind all table names in
/// one read before handing immutable identities to a retrying storage operation.
pub const BindingSource = struct {
    ptr: *anyopaque,
    bind_fn: *const fn (*anyopaque, std.mem.Allocator, []const []const u8) anyerror![][]u8,

    pub fn bind(self: @This(), alloc: std.mem.Allocator, names: []const []const u8) ![][]u8 {
        return self.bind_fn(self.ptr, alloc, names);
    }

    pub fn bindOne(self: @This(), alloc: std.mem.Allocator, name: []const u8) ![]u8 {
        const names = try self.bind(alloc, &.{name});
        defer alloc.free(names);
        std.debug.assert(names.len == 1);
        return names[0];
    }
};

test "system catalog database drop removes all empty namespaces without losing another database" {
    const alloc = std.testing.allocator;
    const state: State = .{ .revision = 1, .next_id = 20, .resources = &.{
        default_database,                                                    default_namespace,
        .{ .kind = .database, .id = 10, .name = "retired" },                 .{ .kind = .namespace, .id = 11, .parent_id = 10, .name = "public" },
        .{ .kind = .namespace, .id = 12, .parent_id = 10, .name = "extra" }, .{ .kind = .table, .id = 13, .parent_id = default_namespace_id, .name = "retained", .storage_name = "table:13" },
    } };
    var delta = try plan(alloc, state, .{ .action = .drop, .kind = .database, .name = "retired" }, &.{});
    defer delta.deinit(alloc);
    var next = try applyDeltaStateAlloc(alloc, state, delta);
    defer next.deinit();
    try std.testing.expectEqual(@as(usize, 3), next.value.resources.len);
    try std.testing.expect(next.value.byId(.table, 13) != null);
    try std.testing.expect(next.value.byId(.namespace, 12) == null);
}
