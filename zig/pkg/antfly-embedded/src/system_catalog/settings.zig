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

//! Durable SQL setting records in the metadata catalog. Public SQL SET never
//! writes these records; publication is a separate administrator operation.
const std = @import("std");

pub const Kind = enum { boolean, integer, string };
pub const Value = union(Kind) { boolean: bool, integer: i64, string: []const u8 };
pub const Identity = struct { id: u64, generation: u64 };
pub const Scope = struct {
    principal: []const u8,
    database: []const u8,
    /// Filled only by the authoritative catalog visibility cut.
    database_id: ?u64 = null,

    pub fn jsonStringify(self: Scope, writer: anytype) !void {
        if (self.database_id) |id| {
            try writer.write(.{ .principal = self.principal, .database = self.database, .database_id = id });
        } else try writer.write(.{ .principal = self.principal, .database = self.database });
    }
};
pub const Definition = struct {
    identity: Identity,
    name: []const u8,
    kind: Kind,
    policy_sensitive: bool = false,
    session_writable: bool = false,
    default: Value,
    database_default: ?Value = null,
    role_default: ?Value = null,
    /// Cold role-binding candidates, emitted only for incarnation-bound SQL
    /// defaults. The authenticated owner must resolve these before evaluation.
    role_overrides: []const RoleDefault = &.{},

    pub fn jsonStringify(self: Definition, writer: anytype) !void {
        if (self.role_overrides.len != 0) {
            try writer.write(.{ .identity = self.identity, .name = self.name, .kind = self.kind, .policy_sensitive = self.policy_sensitive, .session_writable = self.session_writable, .default = self.default, .database_default = self.database_default, .role_default = self.role_default, .role_overrides = self.role_overrides });
        } else {
            try writer.write(.{ .identity = self.identity, .name = self.name, .kind = self.kind, .policy_sensitive = self.policy_sensitive, .session_writable = self.session_writable, .default = self.default, .database_default = self.database_default, .role_default = self.role_default });
        }
    }
};
pub const DatabaseDefault = struct { database: []const u8, value: Value };
/// An empty database denotes ALTER ROLE's cluster-wide default. A nonempty
/// database is an exact database identity/name binding, never a wildcard.
pub const RoleDefault = struct {
    principal: []const u8,
    database: []const u8,
    value: Value,
    principal_instance: ?[16]u8 = null,
    database_id: ?u64 = null,

    pub fn jsonStringify(self: RoleDefault, writer: anytype) !void {
        if (self.database_id) |id| {
            try writer.write(.{ .principal = self.principal, .database = self.database, .value = self.value, .principal_instance = self.principal_instance, .database_id = id });
        } else if (self.principal_instance) |instance| {
            try writer.write(.{ .principal = self.principal, .database = self.database, .value = self.value, .principal_instance = instance });
        } else {
            try writer.write(.{ .principal = self.principal, .database = self.database, .value = self.value });
        }
    }
};
pub const Record = struct {
    identity: Identity,
    name: []const u8,
    kind: Kind,
    policy_sensitive: bool = false,
    session_writable: bool = false,
    default: Value,
    database_defaults: []const DatabaseDefault = &.{},
    role_defaults: []const RoleDefault = &.{},

    pub fn validate(self: Record) !void {
        if (self.identity.id == 0 or self.identity.generation == 0 or
            (self.policy_sensitive and self.session_writable) or self.database_defaults.len > 256 or self.role_defaults.len > 1024)
            return error.InvalidSettingRecord;
        try validateName(self.name);
        try validateValue(self.kind, self.default);
        const builtin = builtinInput(self.name) != null;
        if (builtin) {
            if (self.kind != .integer or self.policy_sensitive or !self.session_writable) return error.InvalidSettingRecord;
            try validateTimeout(self.default);
        }
        for (self.database_defaults, 0..) |entry, i| {
            try validateScopeName(entry.database);
            try validateValue(self.kind, entry.value);
            if (builtin) try validateTimeout(entry.value);
            for (self.database_defaults[0..i]) |prior| if (std.mem.eql(u8, prior.database, entry.database)) return error.InvalidSettingRecord;
        }
        var role_order: [1024]u16 = undefined;
        var bound_database = false;
        var named_database = false;
        for (self.role_defaults, 0..) |entry, i| {
            if (entry.database.len != 0) try validateScopeName(entry.database);
            if (entry.database_id) |id| if (id == 0 or entry.database.len == 0 or self.policy_sensitive) return error.InvalidSettingRecord;
            try validateScopeName(entry.principal);
            try validateValue(self.kind, entry.value);
            if (builtin) try validateTimeout(entry.value);
            if (entry.principal_instance) |instance| {
                if (std.mem.allEqual(u8, &instance, 0) or self.policy_sensitive) return error.InvalidSettingRecord;
            }
            role_order[i] = @intCast(i);
            if (entry.database_id != null) bound_database = true else if (entry.database.len != 0) named_database = true;
        }
        if (self.role_defaults.len > 1) {
            const sorted = role_order[0..self.role_defaults.len];
            std.mem.sortUnstable(u16, sorted, self.role_defaults, roleScopeLess);
            for (sorted[1..], sorted[0 .. sorted.len - 1]) |right, left| {
                if (sameRoleScope(self.role_defaults[left], self.role_defaults[right])) return error.InvalidSettingRecord;
            }
            // An old name-bound slot and a new ID-bound slot for the same
            // scope cannot both be admitted. That would produce three login
            // candidates (including the global default), or order-dependent
            // precedence. A named mutation replaces an old slot atomically;
            // generic definition replacement must not retain both aliases.
            if (bound_database and named_database) {
                std.mem.sortUnstable(u16, sorted, self.role_defaults, roleNameLess);
                for (sorted[1..], sorted[0 .. sorted.len - 1]) |right, left| {
                    const first = self.role_defaults[left];
                    const second = self.role_defaults[right];
                    if ((first.database_id == null) != (second.database_id == null) and
                        std.mem.eql(u8, first.principal, second.principal) and
                        std.mem.eql(u8, first.database, second.database)) return error.InvalidSettingRecord;
                }
            }
        }
    }

    pub fn effective(self: Record, principal: []const u8, database: []const u8) Definition {
        var definition: Definition = .{ .identity = self.identity, .name = self.name, .kind = self.kind, .policy_sensitive = self.policy_sensitive, .session_writable = self.session_writable, .default = self.default };
        for (self.database_defaults) |entry| if (std.mem.eql(u8, entry.database, database)) {
            definition.database_default = entry.value;
            break;
        };
        // PostgreSQL precedence is role-in-database > role > database >
        // definition. Record order must not influence the selected default.
        var global_role: ?Value = null;
        for (self.role_defaults) |entry| {
            if (!std.mem.eql(u8, entry.principal, principal)) continue;
            if (entry.principal_instance != null or entry.database_id != null) continue;
            if (std.mem.eql(u8, entry.database, database)) {
                definition.role_default = entry.value;
                return definition;
            }
            if (entry.database.len == 0) global_role = entry.value;
        }
        definition.role_default = global_role;
        return definition;
    }

    pub fn effectiveAlloc(self: Record, a: std.mem.Allocator, scope: Scope) !Definition {
        var definition = self.effective(scope.principal, scope.database);
        var count: usize = 0;
        var bound = false;
        for (self.role_defaults) |entry| {
            if (!std.mem.eql(u8, entry.principal, scope.principal) or !roleDatabaseMatches(entry, scope)) continue;
            count += 1;
            bound = bound or entry.principal_instance != null or entry.database_id != null;
        }
        if (!bound) return definition;
        if (count > 2) return error.InvalidSettingRecord;
        const candidates = try a.alloc(RoleDefault, count);
        var index: usize = 0;
        for (self.role_defaults) |entry| {
            if (!std.mem.eql(u8, entry.principal, scope.principal) or !roleDatabaseMatches(entry, scope)) continue;
            candidates[index] = entry;
            index += 1;
        }
        definition.role_overrides = candidates;
        return definition;
    }
};

pub const Input = struct {
    name: []const u8,
    kind: Kind,
    policy_sensitive: bool = false,
    session_writable: bool = false,
    default: Value,
    database_defaults: []const DatabaseDefault = &.{},
    role_defaults: []const RoleDefault = &.{},

    pub fn record(self: Input, identity: Identity) Record {
        return .{ .identity = identity, .name = self.name, .kind = self.kind, .policy_sensitive = self.policy_sensitive, .session_writable = self.session_writable, .default = self.default, .database_defaults = self.database_defaults, .role_defaults = self.role_defaults };
    }

    /// A retried publication of the same definition is a no-op: it must not
    /// advance the identity generation and invalidate prepared SQL plans.
    pub fn matches(self: Input, prior: Record) bool {
        if (self.role_defaults.len > 1024) return false;
        if (!std.mem.eql(u8, self.name, prior.name) or self.kind != prior.kind or self.policy_sensitive != prior.policy_sensitive or self.session_writable != prior.session_writable or !valueEqual(self.default, prior.default) or self.database_defaults.len != prior.database_defaults.len or self.role_defaults.len != prior.role_defaults.len) return false;
        for (self.database_defaults) |left| {
            const right = for (prior.database_defaults) |candidate| {
                if (std.mem.eql(u8, left.database, candidate.database)) break candidate;
            } else return false;
            if (!valueEqual(left.value, right.value)) return false;
        }
        if (self.role_defaults.ptr == prior.role_defaults.ptr) return true;
        var left_order: [1024]u16 = undefined;
        var right_order: [1024]u16 = undefined;
        const count = self.role_defaults.len;
        for (0..count) |i| {
            left_order[i] = @intCast(i);
            right_order[i] = @intCast(i);
        }
        std.mem.sortUnstable(u16, left_order[0..count], self.role_defaults, roleScopeLess);
        std.mem.sortUnstable(u16, right_order[0..count], prior.role_defaults, roleScopeLess);
        for (left_order[0..count], right_order[0..count]) |li, ri| {
            const left = self.role_defaults[li];
            const right = prior.role_defaults[ri];
            if (!sameRoleScope(left, right)) return false;
            if (!valueEqual(left.value, right.value) or !std.meta.eql(left.principal_instance, right.principal_instance)) return false;
        }
        return true;
    }
};

fn sameRoleScope(left: RoleDefault, right: RoleDefault) bool {
    if (!std.mem.eql(u8, left.principal, right.principal)) return false;
    if (left.database_id != null or right.database_id != null) return std.meta.eql(left.database_id, right.database_id);
    return std.mem.eql(u8, left.database, right.database);
}

fn roleDatabaseMatches(entry: RoleDefault, scope: Scope) bool {
    if (entry.database.len == 0) return entry.database_id == null;
    if (entry.database_id) |id| return scope.database_id == id;
    return std.mem.eql(u8, entry.database, scope.database);
}

fn roleScopeLess(entries: []const RoleDefault, left: u16, right: u16) bool {
    const principal = std.mem.order(u8, entries[left].principal, entries[right].principal);
    if (principal != .eq) return principal == .lt;
    if (entries[left].database_id) |id| return if (entries[right].database_id) |other| id < other else false;
    if (entries[right].database_id != null) return true;
    return std.mem.lessThan(u8, entries[left].database, entries[right].database);
}

fn roleNameLess(entries: []const RoleDefault, left: u16, right: u16) bool {
    const principal = std.mem.order(u8, entries[left].principal, entries[right].principal);
    if (principal != .eq) return principal == .lt;
    const database = std.mem.order(u8, entries[left].database, entries[right].database);
    if (database != .eq) return database == .lt;
    return entries[left].database_id == null and entries[right].database_id != null;
}

fn valueEqual(left: Value, right: Value) bool {
    if (std.meta.activeTag(left) != std.meta.activeTag(right)) return false;
    return switch (left) {
        .boolean => |value| value == right.boolean,
        .integer => |value| value == right.integer,
        .string => |value| std.mem.eql(u8, value, right.string),
    };
}

/// The first role SET may install a native control definition and its value
/// atomically. Custom settings still require explicit typed registration.
pub fn builtinInput(name: []const u8) ?Input {
    if (std.ascii.eqlIgnoreCase(name, "statement_timeout")) return .{
        .name = "statement_timeout",
        .kind = .integer,
        .session_writable = true,
        .default = .{ .integer = 0 },
    };
    return null;
}

fn validateTimeout(value: Value) !void {
    if (value != .integer or value.integer < 0 or value.integer > std.math.maxInt(i32)) return error.InvalidSettingValue;
}

test "role defaults bootstrap only known typed controls and reject invalid native budgets" {
    const builtin = builtinInput("STATEMENT_TIMEOUT").?;
    try std.testing.expectEqualStrings("statement_timeout", builtin.name);
    const record = builtin.record(.{ .id = 23, .generation = 1 });
    try record.validate();
    try std.testing.expect(builtinInput("app.unknown") == null);
    const patch: RoleDefaultChange = .{ .name = builtin.name, .principal = "alice", .database = "", .value = .{ .integer = 1 } };
    const roles = try patch.applyAlloc(std.testing.allocator, record);
    defer std.testing.allocator.free(roles);
    try std.testing.expectEqual(@as(usize, 1), roles.len);
    for ([_]Value{ .{ .integer = -1 }, .{ .integer = @as(i64, std.math.maxInt(i32)) + 1 }, .{ .string = "1ms" } }) |invalid| {
        var request = patch;
        request.value = invalid;
        try std.testing.expectError(error.InvalidSettingValue, request.validate());
    }
    var invalid_definition = record;
    invalid_definition.kind = .string;
    invalid_definition.default = .{ .string = "0" };
    try std.testing.expectError(error.InvalidSettingRecord, invalid_definition.validate());
}

pub const RoleDefaultChange = struct {
    name: []const u8,
    principal: []const u8,
    database: []const u8,
    principal_instance: ?[16]u8 = null,
    database_id: ?u64 = null,
    /// null means RESET, not a SQL NULL setting value.
    value: ?Value,

    pub fn validate(self: RoleDefaultChange) !void {
        try validateName(self.name);
        try validateScopeName(self.principal);
        if (self.database.len != 0) try validateScopeName(self.database);
        if (self.database_id) |id| if (id == 0 or self.database.len == 0) return error.InvalidSettingRecord;
        if (builtinInput(self.name) != null) if (self.value) |value| try validateTimeout(value);
    }

    /// Run in the catalog mutation lane, not after an ingress-side snapshot.
    /// Nested slices borrow prior/change until the command is serialized.
    pub fn applyAlloc(self: RoleDefaultChange, alloc: std.mem.Allocator, prior: Record) ![]RoleDefault {
        try self.validate();
        try prior.validate();
        if (prior.policy_sensitive and (self.principal_instance != null or self.database_id != null)) return error.SettingWriteForbidden;
        if (self.principal_instance) |instance| {
            if (std.mem.allEqual(u8, &instance, 0)) return error.InvalidSettingRecord;
            // Policy-sensitive defaults need a policy-publication transition,
            // not a connection-setting mutation or an unsigned row override.
            if (prior.policy_sensitive) return error.SettingWriteForbidden;
        }
        if (!std.ascii.eqlIgnoreCase(self.name, prior.name)) return error.InvalidSettingRecord;
        if (self.value) |value| try validateValue(prior.kind, value);
        var matched: usize = 0;
        for (prior.role_defaults) |entry| if (self.matches(entry)) {
            matched += 1;
        };
        const len = prior.role_defaults.len - matched + @as(usize, @intFromBool(self.value != null));
        if (len > 1024) return error.SettingLimitExceeded;
        const output = try alloc.alloc(RoleDefault, len);
        var index: usize = 0;
        for (prior.role_defaults) |entry| {
            if (self.matches(entry)) continue;
            output[index] = entry;
            index += 1;
        }
        if (self.value) |value| {
            output[index] = .{ .principal = self.principal, .database = self.database, .value = value, .principal_instance = self.principal_instance, .database_id = self.database_id };
            index += 1;
        }
        std.debug.assert(index == output.len);
        return output;
    }

    fn matches(self: RoleDefaultChange, entry: RoleDefault) bool {
        if (!std.mem.eql(u8, self.principal, entry.principal)) return false;
        if (self.database_id != null and entry.database_id != null) return self.database_id == entry.database_id;
        return std.mem.eql(u8, self.database, entry.database);
    }
};

pub const Request = union(enum) { put: Input, drop: []const u8, role_default: RoleDefaultChange };
pub const Command = struct {
    version: u16 = 1,
    expected_revision: u64,
    /// Defaults change only future connections. Their publication must not
    /// invalidate an existing connection's pinned type/authorization identity.
    role_defaults_only: bool = false,
    change: union(enum) { put: Record, drop: Identity },

    pub fn requiresRoleDefaultReaders(self: Command) bool {
        return self.role_defaults_only or switch (self.change) {
            .put => |record| roleDefaultsRequireReaders(record.role_defaults),
            .drop => false,
        };
    }

    pub fn jsonStringify(self: Command, writer: anytype) !void {
        // Preserve the established command shape for ordinary publications.
        // The additional intent is emitted only under the role-default floor.
        if (self.role_defaults_only) {
            try writer.write(.{ .version = self.version, .expected_revision = self.expected_revision, .change = self.change, .role_defaults_only = true });
        } else {
            try writer.write(.{ .version = self.version, .expected_revision = self.expected_revision, .change = self.change });
        }
    }
};

pub fn roleDefaultsRequireReaders(roles: []const RoleDefault) bool {
    for (roles) |role| if (role.database.len == 0 or role.principal_instance != null or role.database_id != null) return true;
    return false;
}

/// Only the named, catalog-cut scope mutation mints database bindings. Generic
/// definition replacement can preserve or remove them, never bind a guessed
/// future ID. Sorted scratch keeps large role sets out of a quadratic loop.
pub fn databaseBindingsRetained(roles: []const RoleDefault, prior: ?Record) bool {
    const needed = for (roles) |role| {
        if (role.database_id != null) break true;
    } else false;
    if (!needed) return true;
    const existing = prior orelse return false;
    if (existing.role_defaults.len > 1024) return false;
    var known: [1024]u64 = undefined;
    var count: usize = 0;
    for (existing.role_defaults) |role| if (role.database_id) |id| {
        known[count] = id;
        count += 1;
    };
    std.mem.sortUnstable(u64, known[0..count], {}, std.sort.asc(u64));
    for (roles) |role| if (role.database_id) |id| {
        var lower: usize = 0;
        var upper = count;
        while (lower < upper) {
            const middle = lower + (upper - lower) / 2;
            if (known[middle] < id) lower = middle + 1 else upper = middle;
        }
        if (lower == count or known[lower] != id) return false;
    };
    return true;
}

/// Existing references were proved by the predecessor catalog. Validate only
/// newly introduced IDs, once each, through the caller's exact transaction or
/// unpublished restore cut. Scratch is bounded by this setting's role limit.
pub fn requireDatabaseBindings(a: std.mem.Allocator, record: Record, prior: ?Record, reader: anytype) !void {
    var known: std.AutoHashMapUnmanaged(u64, void) = .empty;
    defer known.deinit(a);
    if (prior) |existing| for (existing.role_defaults) |role| {
        if (role.database_id) |id| try known.put(a, id, {});
    };
    for (record.role_defaults) |role| if (role.database_id) |id| {
        if (known.contains(id)) continue;
        try reader.requireDatabase(id);
        try known.put(a, id, {});
    };
}

pub fn sameDefinition(left: Record, right: Record) bool {
    const input: Input = .{ .name = left.name, .kind = left.kind, .policy_sensitive = left.policy_sensitive, .session_writable = left.session_writable, .default = left.default, .database_defaults = left.database_defaults, .role_defaults = right.role_defaults };
    return input.matches(right);
}

/// Request-owned projection of an existing catalog transaction. Fresh IDs
/// cannot own defaults yet; normal database creation therefore does not scan
/// settings. Rename retains the binding and DROP removes unreachable slots.
pub const DatabaseChanges = struct {
    names: std.AutoHashMapUnmanaged(u64, []const u8) = .empty,
    removed: std.AutoHashMapUnmanaged(u64, void) = .empty,
    removed_names: std.StringHashMapUnmanaged(void) = .empty,

    pub fn init(a: std.mem.Allocator, removals: anytype, upserts: anytype, next_id: u64) !DatabaseChanges {
        var self: DatabaseChanges = .{};
        errdefer self.deinit(a);
        for (upserts) |resource| if (resource.kind == .database and resource.id < next_id) {
            try self.names.put(a, resource.id, resource.name);
        };
        for (removals) |resource| if (resource.kind == .database and !self.names.contains(resource.id)) {
            try self.removed.put(a, resource.id, {});
            try self.removed_names.put(a, resource.name, {});
        };
        return self;
    }

    pub fn deinit(self: *DatabaseChanges, a: std.mem.Allocator) void {
        self.names.deinit(a);
        self.removed.deinit(a);
        self.removed_names.deinit(a);
    }

    pub fn empty(self: DatabaseChanges) bool {
        return self.names.count() == 0 and self.removed.count() == 0;
    }

    fn role(self: DatabaseChanges, entry: RoleDefault) ?RoleDefault {
        if (entry.database_id) |id| {
            if (self.removed.contains(id)) return null;
            if (self.names.get(id)) |name| {
                var renamed = entry;
                renamed.database = name;
                return renamed;
            }
        } else if (self.removed_names.contains(entry.database)) return null;
        return entry;
    }

    /// Nested strings/values borrow the source or catalog delta. The two
    /// returned arrays belong to a and may be serialized or deep-cloned before
    /// their source owner retires. Definition generation is deliberately stable.
    pub fn applyAlloc(self: DatabaseChanges, a: std.mem.Allocator, prior: Record) !?Record {
        var changed = false;
        var roles: usize = 0;
        for (prior.role_defaults) |entry| {
            if (self.role(entry)) |next| {
                roles += 1;
                changed = changed or !std.mem.eql(u8, entry.database, next.database);
            } else changed = true;
        }
        var databases: usize = 0;
        for (prior.database_defaults) |entry| {
            if (self.removed_names.contains(entry.database)) changed = true else databases += 1;
        }
        if (!changed) return null;
        const role_defaults = try a.alloc(RoleDefault, roles);
        errdefer a.free(role_defaults);
        const database_defaults = try a.alloc(DatabaseDefault, databases);
        var index: usize = 0;
        for (prior.role_defaults) |entry| if (self.role(entry)) |next| {
            role_defaults[index] = next;
            index += 1;
        };
        index = 0;
        for (prior.database_defaults) |entry| if (!self.removed_names.contains(entry.database)) {
            database_defaults[index] = entry;
            index += 1;
        };
        var replacement = prior;
        replacement.role_defaults = role_defaults;
        replacement.database_defaults = database_defaults;
        return replacement;
    }
};

pub const Snapshot = struct {
    scope: Scope,
    epoch: u64,
    definitions: []const Definition,

    pub fn init(scope: Scope, epoch: u64, definitions: []const Definition) Snapshot {
        var bound = scope;
        const needed = blk: {
            for (definitions) |definition| for (definition.role_overrides) |entry| {
                if (entry.database_id != null) break :blk true;
            };
            break :blk false;
        };
        if (!needed) bound.database_id = null;
        return .{ .scope = bound, .epoch = epoch, .definitions = definitions };
    }

    pub fn needsRoleAuthorization(self: Snapshot) bool {
        for (self.definitions) |definition| if (definition.role_overrides.len != 0) return true;
        return false;
    }

    /// A recreated role cannot adopt defaults belonging to its predecessor.
    /// Evaluate both scoped and global candidates after checking incarnation,
    /// so a stale scoped entry cannot mask a valid current global default.
    pub fn authorizeRoleDefaultsLeaky(self: Snapshot, a: std.mem.Allocator, instance: ?[16]u8) !Snapshot {
        if (!self.needsRoleAuthorization()) return self;
        const definitions = try a.dupe(Definition, self.definitions);
        for (definitions) |*definition| {
            if (definition.role_overrides.len == 0) continue;
            if (definition.role_overrides.len > 2 or definition.policy_sensitive) return error.InvalidSettingCatalogSnapshot;
            var global: ?Value = null;
            var scoped: ?Value = null;
            var seen_global = false;
            var seen_scoped = false;
            for (definition.role_overrides) |entry| {
                if (entry.database_id) |id| if (id == 0 or entry.database.len == 0) return error.InvalidSettingCatalogSnapshot;
                if (!std.mem.eql(u8, entry.principal, self.scope.principal) or !roleDatabaseMatches(entry, self.scope)) return error.InvalidSettingCatalogSnapshot;
                try validateValue(definition.kind, entry.value);
                if (entry.database.len == 0) {
                    if (seen_global) return error.InvalidSettingCatalogSnapshot;
                    seen_global = true;
                } else {
                    if (seen_scoped) return error.InvalidSettingCatalogSnapshot;
                    seen_scoped = true;
                }
                if (entry.principal_instance) |required| {
                    if (std.mem.allEqual(u8, &required, 0)) return error.InvalidSettingCatalogSnapshot;
                    if (instance == null or !std.mem.eql(u8, &required, &instance.?)) continue;
                }
                if (entry.database.len == 0) global = entry.value else scoped = entry.value;
            }
            definition.role_default = scoped orelse global;
            definition.role_overrides = &.{};
        }
        return .{ .scope = self.scope, .epoch = self.epoch, .definitions = definitions };
    }
};

pub fn validateValue(kind: Kind, value: Value) !void {
    if (std.meta.activeTag(value) != kind) return error.InvalidSettingValue;
    if (value == .string and (value.string.len > 4096 or !std.unicode.utf8ValidateSlice(value.string))) return error.InvalidSettingValue;
}

pub fn validateName(name: []const u8) !void {
    if (name.len == 0 or name.len > 128) return error.InvalidSettingRecord;
    for (name) |ch| if (!std.ascii.isAlphanumeric(ch) and ch != '_' and ch != '.') return error.InvalidSettingRecord;
}

fn validateScopeName(value: []const u8) !void {
    if (value.len == 0 or value.len > 128 or !std.unicode.utf8ValidateSlice(value)) return error.InvalidSettingRecord;
    for (value) |ch| if (std.ascii.isControl(ch)) return error.InvalidSettingRecord;
}

test "role defaults bind database identities across rename reset drop and name reuse with allocation atomic cleanup" {
    const Check = struct {
        fn run(backing: std.mem.Allocator) !void {
            var arena = std.heap.ArenaAllocator.init(backing);
            defer arena.deinit();
            const a = arena.allocator();
            const Resource = struct { kind: enum { database }, id: u64, name: []const u8 };
            const before: Resource = .{ .kind = .database, .id = 17, .name = "old" };
            const after: Resource = .{ .kind = .database, .id = 17, .name = "renamed" };
            const instance: [16]u8 = @splat(3);
            const record: Record = .{
                .identity = .{ .id = 7, .generation = 2 },
                .name = "app.mode",
                .kind = .string,
                .default = .{ .string = "base" },
                .role_defaults = &.{
                    .{ .principal = "alice", .principal_instance = instance, .database = "", .value = .{ .string = "global" } },
                    .{ .principal = "alice", .principal_instance = instance, .database = "old", .database_id = 17, .value = .{ .string = "scoped" } },
                },
            };
            try record.validate();
            try std.testing.expect(databaseBindingsRetained(record.role_defaults, record));
            try std.testing.expect(!databaseBindingsRetained(record.role_defaults, null));
            const Reader = struct {
                calls: *usize,
                pub fn requireDatabase(reader: @This(), id: u64) !void {
                    reader.calls.* += 1;
                    if (id != 17) return error.DatabaseNotFound;
                }
            };
            var calls: usize = 0;
            try requireDatabaseBindings(a, record, record, Reader{ .calls = &calls });
            try std.testing.expectEqual(@as(usize, 0), calls);
            try requireDatabaseBindings(a, record, null, Reader{ .calls = &calls });
            try std.testing.expectEqual(@as(usize, 1), calls);
            var dangling = record;
            var orphan = record.role_defaults[1];
            orphan.database_id = 18;
            dangling.role_defaults = &.{orphan};
            try std.testing.expectError(error.DatabaseNotFound, requireDatabaseBindings(a, dangling, null, Reader{ .calls = &calls }));
            var alias = record.role_defaults[1];
            alias.database_id = null;
            var ambiguous = record;
            ambiguous.role_defaults = &.{ record.role_defaults[0], alias, record.role_defaults[1] };
            try std.testing.expectError(error.InvalidSettingRecord, ambiguous.validate());
            const coalesce: RoleDefaultChange = .{ .name = record.name, .principal = "alice", .database = "old", .database_id = 17, .principal_instance = instance, .value = .{ .string = "coalesced" } };
            ambiguous.role_defaults = &.{ record.role_defaults[0], alias };
            ambiguous.role_defaults = try coalesce.applyAlloc(a, ambiguous);
            try ambiguous.validate();
            try std.testing.expectEqual(@as(usize, 2), ambiguous.role_defaults.len);
            try std.testing.expectEqualStrings("coalesced", ambiguous.role_defaults[1].value.string);
            var sensitive = record;
            sensitive.role_defaults = &.{};
            sensitive.policy_sensitive = true;
            const forbidden: RoleDefaultChange = .{ .name = record.name, .principal = "alice", .database = "old", .database_id = 17, .value = .{ .string = "unsigned" } };
            try std.testing.expectError(error.SettingWriteForbidden, forbidden.applyAlloc(a, sensitive));
            var changes = try DatabaseChanges.init(a, &[_]Resource{before}, &[_]Resource{after}, 20);
            defer changes.deinit(a);
            const renamed = (try changes.applyAlloc(a, record)).?;
            try std.testing.expectEqualDeep(record.identity, renamed.identity);
            try std.testing.expectEqualStrings("renamed", renamed.role_defaults[1].database);
            for ([_]struct { id: u64, expected: []const u8 }{ .{ .id = 17, .expected = "scoped" }, .{ .id = 18, .expected = "global" } }) |test_case| {
                const scope: Scope = .{ .principal = "alice", .database = "renamed", .database_id = test_case.id };
                const definition = try renamed.effectiveAlloc(a, scope);
                const view = try Snapshot.init(scope, 1, &.{definition}).authorizeRoleDefaultsLeaky(a, instance);
                try std.testing.expectEqualStrings(test_case.expected, view.definitions[0].role_default.?.string);
            }
            // RESET matches the immutable scope even before its cached display
            // name has been refreshed by the rename transaction.
            const reset: RoleDefaultChange = .{ .name = record.name, .principal = "alice", .database = "renamed", .database_id = 17, .principal_instance = instance, .value = null };
            try std.testing.expectEqual(@as(usize, 1), (try reset.applyAlloc(a, record)).len);
            var dropped = try DatabaseChanges.init(a, &[_]Resource{after}, &[_]Resource{}, 20);
            defer dropped.deinit(a);
            var at_drop = renamed;
            at_drop.database_defaults = &.{.{ .database = "renamed", .value = .{ .string = "database" } }};
            const pruned = (try dropped.applyAlloc(a, at_drop)).?;
            try std.testing.expectEqual(@as(usize, 1), pruned.role_defaults.len);
            try std.testing.expectEqual(@as(usize, 0), pruned.database_defaults.len);
            try std.testing.expectEqualDeep(record.identity, pruned.identity);
            var created = try DatabaseChanges.init(a, &[_]Resource{}, &[_]Resource{.{ .kind = .database, .id = 20, .name = "new" }}, 20);
            defer created.deinit(a);
            try std.testing.expect(created.empty());
        }
    };
    try Check.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "role defaults select database override before global role and database defaults" {
    const global: RoleDefault = .{ .principal = "alice", .database = "", .value = .{ .string = "global-role" } };
    const scoped: RoleDefault = .{ .principal = "alice", .database = "main", .value = .{ .string = "scoped-role" } };
    var record: Record = .{ .identity = .{ .id = 7, .generation = 3 }, .name = "app.tenant", .kind = .string, .default = .{ .string = "base" }, .database_defaults = &.{.{ .database = "main", .value = .{ .string = "database" } }} };
    for ([_][]const RoleDefault{ &.{ global, scoped }, &.{ scoped, global } }) |defaults| {
        record.role_defaults = defaults;
        try record.validate();
        try std.testing.expectEqualStrings("scoped-role", record.effective("alice", "main").role_default.?.string);
        try std.testing.expectEqualStrings("global-role", record.effective("alice", "other").role_default.?.string);
        try std.testing.expect(record.effective("bob", "main").role_default == null);
        try std.testing.expectEqualStrings("database", record.effective("bob", "main").database_default.?.string);
    }
}

test "role defaults bind principal incarnations and stale scoped entries cannot mask a current global default" {
    const Check = struct {
        fn run(a: std.mem.Allocator) !void {
            const old: [16]u8 = @splat(1);
            const current: [16]u8 = @splat(2);
            const record: Record = .{ .identity = .{ .id = 7, .generation = 3 }, .name = "app.mode", .kind = .string, .default = .{ .string = "base" }, .role_defaults = &.{
                .{ .principal = "alice", .database = "main", .principal_instance = old, .value = .{ .string = "old-scoped" } },
                .{ .principal = "alice", .database = "", .principal_instance = current, .value = .{ .string = "current-global" } },
            } };
            try record.validate();
            const definition = try record.effectiveAlloc(a, .{ .principal = "alice", .database = "main" });
            defer a.free(definition.role_overrides);
            const raw: Snapshot = .{ .scope = .{ .principal = "alice", .database = "main" }, .epoch = 1, .definitions = &.{definition} };
            const selected = try raw.authorizeRoleDefaultsLeaky(a, current);
            defer a.free(selected.definitions);
            try std.testing.expectEqualStrings("current-global", selected.definitions[0].role_default.?.string);
            const missing = try raw.authorizeRoleDefaultsLeaky(a, null);
            defer a.free(missing.definitions);
            try std.testing.expect(missing.definitions[0].role_default == null);
            const predecessor = try raw.authorizeRoleDefaultsLeaky(a, old);
            defer a.free(predecessor.definitions);
            try std.testing.expectEqualStrings("old-scoped", predecessor.definitions[0].role_default.?.string);
            var next = record;
            var roles = try a.dupe(RoleDefault, record.role_defaults);
            defer a.free(roles);
            roles[0].principal_instance = current;
            next.role_defaults = roles;
            const input: Input = .{ .name = next.name, .kind = next.kind, .default = next.default, .role_defaults = next.role_defaults };
            try std.testing.expect(!input.matches(record));
        }
    };
    try Check.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "role defaults reject nonadjacent duplicate scopes independently of principal incarnation" {
    var record: Record = .{ .identity = .{ .id = 7, .generation = 1 }, .name = "app.mode", .kind = .string, .default = .{ .string = "base" }, .role_defaults = &.{
        .{ .principal = "alice", .database = "main", .value = .{ .string = "a" }, .principal_instance = @splat(1) },
        .{ .principal = "bob", .database = "main", .value = .{ .string = "b" } },
        .{ .principal = "alice", .database = "main", .value = .{ .string = "c" }, .principal_instance = @splat(2) },
    } };
    try std.testing.expectError(error.InvalidSettingRecord, record.validate());
    record.role_defaults = record.role_defaults[0..2];
    try record.validate();
}

test "role default patch preserves unrelated authority and reset only removes its exact slot" {
    const Check = struct {
        fn run(a: std.mem.Allocator) !void {
            var prior: Record = .{ .identity = .{ .id = 7, .generation = 3 }, .name = "app.tenant", .kind = .string, .policy_sensitive = true, .default = .{ .string = "base" }, .role_defaults = &.{
                .{ .principal = "alice", .database = "", .value = .{ .string = "global" } },
                .{ .principal = "alice", .database = "main", .value = .{ .string = "old" } },
                .{ .principal = "bob", .database = "main", .value = .{ .string = "bob" } },
            } };
            var patch: RoleDefaultChange = .{ .name = "APP.TENANT", .principal = "alice", .database = "main", .value = .{ .string = "next" } };
            const replaced = try patch.applyAlloc(a, prior);
            defer a.free(replaced);
            try std.testing.expectEqual(@as(usize, 3), replaced.len);
            prior.role_defaults = replaced;
            try prior.validate();
            try std.testing.expectEqualStrings("next", prior.effective("alice", "main").role_default.?.string);
            try std.testing.expectEqualStrings("global", prior.effective("alice", "other").role_default.?.string);
            try std.testing.expectEqualStrings("bob", prior.effective("bob", "main").role_default.?.string);
            patch.value = null;
            const reset = try patch.applyAlloc(a, prior);
            defer a.free(reset);
            prior.role_defaults = reset;
            try prior.validate();
            try std.testing.expectEqual(@as(usize, 2), reset.len);
            try std.testing.expectEqualStrings("global", prior.effective("alice", "main").role_default.?.string);
            const repeated = try patch.applyAlloc(a, prior);
            defer a.free(repeated);
            try std.testing.expectEqual(@as(usize, 2), repeated.len);
            patch.database = "other";
            patch.value = .{ .string = "added" };
            const added = try patch.applyAlloc(a, prior);
            defer a.free(added);
            try std.testing.expectEqual(@as(usize, 3), added.len);
            patch.value = .{ .integer = 1 };
            try std.testing.expectError(error.InvalidSettingValue, patch.applyAlloc(a, prior));
            patch.principal = "";
            try std.testing.expectError(error.InvalidSettingRecord, patch.applyAlloc(a, prior));
        }
    };
    try Check.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "durable setting records validate typed and scoped defaults" {
    const record: Record = .{ .identity = .{ .id = 12, .generation = 2 }, .name = "app.tenant", .kind = .string, .policy_sensitive = true, .default = .{ .string = "none" }, .database_defaults = &.{.{ .database = "main", .value = .{ .string = "database" } }}, .role_defaults = &.{.{ .principal = "alice", .database = "main", .value = .{ .string = "role" } }} };
    try record.validate();
    try std.testing.expectEqualStrings("role", record.effective("alice", "main").role_default.?.string);
    try std.testing.expectEqualStrings("database", record.effective("bob", "main").database_default.?.string);
    var unsafe = record;
    unsafe.session_writable = true;
    try std.testing.expectError(error.InvalidSettingRecord, unsafe.validate());
    unsafe = record;
    unsafe.role_defaults = &.{.{ .principal = "alice", .database = "main", .value = .{ .integer = 1 } }};
    try std.testing.expectError(error.InvalidSettingValue, unsafe.validate());
    const identical: Input = .{ .name = record.name, .kind = record.kind, .policy_sensitive = record.policy_sensitive, .default = record.default, .database_defaults = record.database_defaults, .role_defaults = record.role_defaults };
    try std.testing.expect(identical.matches(record));
    var reordered = identical;
    reordered.database_defaults = &.{
        .{ .database = "other", .value = .{ .string = "other" } },
        .{ .database = "main", .value = .{ .string = "database" } },
    };
    var matching = record;
    matching.database_defaults = &.{
        .{ .database = "main", .value = .{ .string = "database" } },
        .{ .database = "other", .value = .{ .string = "other" } },
    };
    reordered.role_defaults = &.{
        .{ .principal = "bob", .database = "main", .value = .{ .string = "bob" } },
        .{ .principal = "alice", .database = "main", .value = .{ .string = "role" } },
    };
    matching.role_defaults = &.{
        .{ .principal = "alice", .database = "main", .value = .{ .string = "role" } },
        .{ .principal = "bob", .database = "main", .value = .{ .string = "bob" } },
    };
    try std.testing.expect(reordered.matches(matching));
}
