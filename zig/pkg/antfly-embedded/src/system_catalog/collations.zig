// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Namespace-owned collation contracts. Names live in the catalog directory;
//! bound expressions retain immutable identities through rename. This module
//! does not load providers or claim that a definition is installed durably.
const std = @import("std");
const owners = @import("owners.zig");

pub const max_record_bytes = 16 * 1024;
pub const Provider = enum { icu };
pub const Version = struct {
    collation: [4]u8,
    uca: [4]u8,

    pub fn validate(self: Version) !void {
        if (std.mem.allEqual(u8, &self.collation, 0) or std.mem.allEqual(u8, &self.uca, 0)) return error.InvalidSqlCollationRecord;
    }
};

pub const Reference = struct {
    database_id: u64,
    namespace_id: u64,
    id: u64,
    origin_id: [16]u8,
    generation: u32,

    pub fn validate(self: Reference) !void {
        if (self.database_id == 0 or self.namespace_id == 0 or self.id == 0 or self.generation == 0 or std.mem.allEqual(u8, &self.origin_id, 0))
            return error.InvalidSqlCollationRecord;
    }
    pub fn eql(self: Reference, other: Reference) bool {
        return std.meta.eql(self, other);
    }
};

pub const Definition = struct {
    provider: Provider = .icu,
    locale: []const u8,
    deterministic: bool = true,
    version: Version,
    owner: owners.Owner,

    pub fn validate(self: Definition) !void {
        if (self.locale.len == 0 or self.locale.len > 1024 or !std.unicode.utf8ValidateSlice(self.locale) or std.mem.indexOfScalar(u8, self.locale, 0) != null)
            return error.InvalidSqlCollationRecord;
        try self.version.validate();
        switch (self.owner) {
            .auth_disabled => {},
            .user, .role => |principal| {
                if (principal.name.len == 0 or principal.name.len > 255 or !std.unicode.utf8ValidateSlice(principal.name) or std.mem.indexOfScalar(u8, principal.name, 0) != null or std.mem.allEqual(u8, &principal.instance_id, 0))
                    return error.InvalidSqlCollationRecord;
            },
        }
    }
};

/// Full comparison-domain identity. Persist this on columns/expressions/indexes
/// and policy plans. An execution owner must verify provider versions before
/// loading data, not silently regenerate old index keys with new ICU weights.
pub const Bound = struct {
    reference: Reference,
    provider: Provider,
    deterministic: bool,
    version: Version,

    pub fn validate(self: Bound) !void {
        try self.reference.validate();
        try self.version.validate();
    }
    pub fn eql(self: Bound, other: Bound) bool {
        return std.meta.eql(self, other);
    }
    pub fn requireRecord(self: Bound, record: Record) !void {
        try self.validate();
        try record.validate();
        if (!self.reference.eql(record.reference)) return error.SqlStaleCollation;
        if (!self.eql(record.bound())) return error.SqlCollationVersionMismatch;
    }
};

/// Trusted producer intent; the transaction authority assigns all catalog IDs.
pub const Create = struct {
    expected_database_id: u64,
    expected_namespace_id: u64,
    origin_id: [16]u8,
    definition: Definition,
    pub fn decode(a: std.mem.Allocator, bytes: []const u8) !std.json.Parsed(Create) {
        if (bytes.len > max_record_bytes) return error.InvalidSqlCollationRecord;
        var parsed = std.json.parseFromSlice(Create, a, bytes, .{ .allocate = .alloc_always }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSqlCollationRecord;
        errdefer parsed.deinit();
        try parsed.value.validate();
        return parsed;
    }
    pub fn validate(self: Create) !void {
        if (self.expected_database_id == 0 or self.expected_namespace_id == 0) return error.InvalidSqlCollationRecord;
        if (std.mem.allEqual(u8, &self.origin_id, 0)) return error.InvalidSqlCollationRecord;
        try self.definition.validate();
        if (!self.definition.deterministic) return error.UnsupportedSqlCollation;
    }
};

/// Original-cut declaration without catalog ownership or privilege grants.
/// A destination may have different local IDs/origin, but comparison semantics
/// must match exactly; provider upgrades are never implicit restore migrations.
pub const Contract = struct {
    bound: Bound,
    name: @import("domain.zig").Target,
    locale: []const u8,

    pub fn validate(self: Contract) !void {
        try self.bound.validate();
        try self.name.validate();
        if (self.locale.len == 0 or self.locale.len > 1024 or !std.unicode.utf8ValidateSlice(self.locale) or std.mem.indexOfScalar(u8, self.locale, 0) != null) return error.InvalidSqlCollationRecord;
    }
    pub fn compatible(self: Contract, other: Contract) bool {
        return self.bound.provider == other.bound.provider and self.bound.deterministic == other.bound.deterministic and
            std.meta.eql(self.bound.version, other.bound.version) and std.mem.eql(u8, self.locale, other.locale);
    }
    pub fn sameName(self: Contract, other: Contract) bool {
        return std.mem.eql(u8, self.name.database, other.name.database) and std.mem.eql(u8, self.name.namespace, other.name.namespace) and std.mem.eql(u8, self.name.table, other.name.table);
    }
    pub fn lessName(self: Contract, other: Contract) bool {
        inline for (.{ "database", "namespace", "table" }) |field| switch (std.mem.order(u8, @field(self.name, field), @field(other.name, field))) {
            .lt => return true,
            .gt => return false,
            .eq => {},
        };
        return false;
    }
    pub fn own(self: Contract, a: std.mem.Allocator) !Contract {
        var result = self;
        const database = try a.dupe(u8, self.name.database);
        errdefer a.free(database);
        const namespace = try a.dupe(u8, self.name.namespace);
        errdefer a.free(namespace);
        const table = try a.dupe(u8, self.name.table);
        errdefer a.free(table);
        result.name = .{ .database = database, .namespace = namespace, .table = table };
        result.locale = try a.dupe(u8, self.locale);
        return result;
    }
};

pub const Record = struct {
    format: u8 = 1,
    reference: Reference,
    definition: Definition,

    pub fn validate(self: Record) !void {
        if (self.format != 1) return error.InvalidSqlCollationRecord;
        try self.reference.validate();
        try self.definition.validate();
    }
    pub fn bound(self: Record) Bound {
        return .{ .reference = self.reference, .provider = self.definition.provider, .deterministic = self.definition.deterministic, .version = self.definition.version };
    }
    pub fn encode(self: Record, a: std.mem.Allocator) ![]u8 {
        try self.validate();
        const bytes = try std.json.Stringify.valueAlloc(a, self, .{});
        errdefer a.free(bytes);
        if (bytes.len > max_record_bytes) return error.InvalidSqlCollationRecord;
        return bytes;
    }
    pub fn decode(a: std.mem.Allocator, bytes: []const u8) !std.json.Parsed(Record) {
        if (bytes.len > max_record_bytes) return error.InvalidSqlCollationRecord;
        var parsed = std.json.parseFromSlice(Record, a, bytes, .{ .allocate = .alloc_always }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSqlCollationRecord;
        errdefer parsed.deinit();
        try parsed.value.validate();
        return parsed;
    }
};

test "SQL collation bindings retain identity and reject drop reuse and stale provider versions" {
    const a = std.testing.allocator;
    const record: Record = .{
        .reference = .{ .database_id = 1, .namespace_id = 2, .id = 19, .origin_id = @splat(5), .generation = 1 },
        .definition = .{ .locale = "und-u-ks-level2", .version = .{ .collation = .{ 153, 120, 0, 0 }, .uca = .{ 15, 1, 0, 0 } }, .owner = .auth_disabled },
    };
    const bytes = try record.encode(a);
    defer a.free(bytes);
    var restored = try Record.decode(a, bytes);
    defer restored.deinit();
    try std.testing.expectEqualDeep(record, restored.value);
    const bound = record.bound();
    try bound.requireRecord(restored.value);
    // Display names are deliberately absent: catalog rename preserves bindings.
    var replacement = record;
    replacement.reference.id += 1;
    try std.testing.expectError(error.SqlStaleCollation, bound.requireRecord(replacement));
    replacement = record;
    replacement.reference.origin_id[0] ^= 1;
    try std.testing.expectError(error.SqlStaleCollation, bound.requireRecord(replacement));
    replacement = record;
    replacement.definition.version.uca[0] += 1;
    try std.testing.expectError(error.SqlCollationVersionMismatch, bound.requireRecord(replacement));
    replacement = record;
    replacement.definition.deterministic = false;
    try std.testing.expectError(error.SqlCollationVersionMismatch, bound.requireRecord(replacement));
    replacement = record;
    replacement.reference.namespace_id = 0;
    try std.testing.expectError(error.InvalidSqlCollationRecord, replacement.validate());
    try std.testing.expectError(error.InvalidSqlCollationRecord, Record.decode(a, "{}"));
    try std.testing.expectError(error.InvalidSqlCollationRecord, Record.decode(a, "{\"format\":2}"));
}

const domain = @import("domain.zig");

pub const SnapshotRequest = struct {
    database: []const u8,
    targets: []const domain.Target = &.{},
    bound: ?Bound = null,
    expected_revision: ?u64 = null,
    pub fn validate(self: @This()) !void {
        try domain.validateName(self.database);
        if ((self.targets.len == 0) != (self.bound != null) or self.targets.len > 128) return error.InvalidCatalogMutation;
        for (self.targets) |target| {
            try target.validate();
            if (!std.mem.eql(u8, target.database, self.database)) return error.UnsupportedSqlShape;
        }
        if (self.bound) |bound| try bound.validate();
    }
};
pub const Scope = struct { target: domain.Target, database_id: ?u64, namespace_id: ?u64 };
pub const Found = struct { scope: ?usize = null, target: domain.Target, record: Record };
pub const Snapshot = struct {
    revision: u64,
    scopes: []const Scope,
    found: ?Found,
    pub fn validate(self: @This(), request: SnapshotRequest) !void {
        try request.validate();
        if (request.expected_revision) |revision| if (self.revision != revision) return error.CatalogGenerationChanged;
        if (self.scopes.len != request.targets.len) return error.InvalidSqlBackendResponse;
        for (self.scopes, request.targets) |scope, target| {
            if (!sameTarget(scope.target, target)) return error.InvalidSqlBackendResponse;
            if (scope.database_id == 0 or scope.namespace_id == 0 or (scope.namespace_id != null and scope.database_id == null)) return error.InvalidSqlBackendResponse;
        }
        if (self.found) |found| {
            try found.target.validate();
            try found.record.validate();
            if (!std.mem.eql(u8, found.target.database, request.database)) return error.InvalidSqlBackendResponse;
            if (request.bound) |bound| {
                if (found.scope != null) return error.InvalidSqlBackendResponse;
                try bound.requireRecord(found.record);
            } else {
                const ordinal = found.scope orelse return error.InvalidSqlBackendResponse;
                if (ordinal >= self.scopes.len) return error.InvalidSqlBackendResponse;
                const scope = self.scopes[ordinal];
                if (scope.namespace_id != found.record.reference.namespace_id or scope.database_id != found.record.reference.database_id or !sameTarget(scope.target, found.target)) return error.InvalidSqlBackendResponse;
            }
        } else if (request.bound != null) return error.SqlStaleCollation;
    }
};

fn sameTarget(left: domain.Target, right: domain.Target) bool {
    return std.mem.eql(u8, left.database, right.database) and
        std.mem.eql(u8, left.namespace, right.namespace) and
        std.mem.eql(u8, left.table, right.table);
}

test "SQL collation snapshots bind scope names, database ownership and revision" {
    const record: Record = .{
        .reference = .{ .database_id = 1, .namespace_id = 2, .id = 19, .origin_id = @splat(5), .generation = 1 },
        .definition = .{ .locale = "und", .version = .{ .collation = .{ 153, 120, 0, 0 }, .uca = .{ 15, 1, 0, 0 } }, .owner = .auth_disabled },
    };
    const target: domain.Target = .{ .database = "app", .namespace = "public", .table = "ordering" };
    const request: SnapshotRequest = .{ .database = "app", .targets = &.{target}, .expected_revision = 5 };
    const snapshot: Snapshot = .{
        .revision = 5,
        .scopes = &.{.{ .target = target, .database_id = 1, .namespace_id = 2 }},
        .found = .{ .scope = 0, .target = target, .record = record },
    };
    try snapshot.validate(request);
    var substituted = snapshot;
    substituted.found.?.target.namespace = "other";
    try std.testing.expectError(error.InvalidSqlBackendResponse, substituted.validate(request));
    substituted = snapshot;
    substituted.revision = 6;
    try std.testing.expectError(error.CatalogGenerationChanged, substituted.validate(request));
    const bound_request: SnapshotRequest = .{ .database = "app", .bound = record.bound() };
    substituted = .{ .revision = 5, .scopes = &.{}, .found = null };
    try std.testing.expectError(error.SqlStaleCollation, substituted.validate(bound_request));
    substituted.found = .{ .target = target, .record = record };
    try substituted.validate(bound_request);
    substituted.found.?.record.reference.origin_id[0] ^= 1;
    try std.testing.expectError(error.SqlStaleCollation, substituted.validate(bound_request));
}
