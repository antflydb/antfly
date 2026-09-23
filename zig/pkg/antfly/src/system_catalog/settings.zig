// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Durable SQL setting records in the metadata catalog. Public SQL SET never
//! writes these records; publication is a separate administrator operation.
const std = @import("std");

pub const Kind = enum { boolean, integer, string };
pub const Value = union(Kind) { boolean: bool, integer: i64, string: []const u8 };
pub const Identity = struct { id: u64, generation: u64 };
pub const Scope = struct { principal: []const u8, database: []const u8 };
pub const Definition = struct {
    identity: Identity,
    name: []const u8,
    kind: Kind,
    policy_sensitive: bool = false,
    session_writable: bool = false,
    default: Value,
    database_default: ?Value = null,
    role_default: ?Value = null,
};
pub const DatabaseDefault = struct { database: []const u8, value: Value };
pub const RoleDefault = struct { principal: []const u8, database: []const u8, value: Value };
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
        for (self.database_defaults, 0..) |entry, i| {
            try validateScopeName(entry.database);
            try validateValue(self.kind, entry.value);
            for (self.database_defaults[0..i]) |prior| if (std.mem.eql(u8, prior.database, entry.database)) return error.InvalidSettingRecord;
        }
        for (self.role_defaults, 0..) |entry, i| {
            try validateScopeName(entry.database);
            try validateScopeName(entry.principal);
            try validateValue(self.kind, entry.value);
            for (self.role_defaults[0..i]) |prior| if (std.mem.eql(u8, prior.database, entry.database) and std.mem.eql(u8, prior.principal, entry.principal)) return error.InvalidSettingRecord;
        }
    }

    pub fn effective(self: Record, principal: []const u8, database: []const u8) Definition {
        var definition: Definition = .{ .identity = self.identity, .name = self.name, .kind = self.kind, .policy_sensitive = self.policy_sensitive, .session_writable = self.session_writable, .default = self.default };
        for (self.database_defaults) |entry| if (std.mem.eql(u8, entry.database, database)) {
            definition.database_default = entry.value;
            break;
        };
        for (self.role_defaults) |entry| if (std.mem.eql(u8, entry.database, database) and std.mem.eql(u8, entry.principal, principal)) {
            definition.role_default = entry.value;
            break;
        };
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
        if (!std.mem.eql(u8, self.name, prior.name) or self.kind != prior.kind or self.policy_sensitive != prior.policy_sensitive or self.session_writable != prior.session_writable or !valueEqual(self.default, prior.default) or self.database_defaults.len != prior.database_defaults.len or self.role_defaults.len != prior.role_defaults.len) return false;
        for (self.database_defaults) |left| {
            const right = for (prior.database_defaults) |candidate| {
                if (std.mem.eql(u8, left.database, candidate.database)) break candidate;
            } else return false;
            if (!valueEqual(left.value, right.value)) return false;
        }
        for (self.role_defaults) |left| {
            const right = for (prior.role_defaults) |candidate| {
                if (std.mem.eql(u8, left.principal, candidate.principal) and std.mem.eql(u8, left.database, candidate.database)) break candidate;
            } else return false;
            if (!valueEqual(left.value, right.value)) return false;
        }
        return true;
    }
};

fn valueEqual(left: Value, right: Value) bool {
    if (std.meta.activeTag(left) != std.meta.activeTag(right)) return false;
    return switch (left) {
        .boolean => |value| value == right.boolean,
        .integer => |value| value == right.integer,
        .string => |value| std.mem.eql(u8, value, right.string),
    };
}

pub const Request = union(enum) { put: Input, drop: []const u8 };
pub const Command = struct {
    version: u16 = 1,
    expected_revision: u64,
    change: union(enum) { put: Record, drop: Identity },
};
pub const Snapshot = struct { scope: Scope, epoch: u64, definitions: []const Definition };

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
