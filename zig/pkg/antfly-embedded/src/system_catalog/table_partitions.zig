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

//! Declarative RANGE partition membership. Catalog admission supplies identities;
//! SQL names and arbitrary schema JSON never confer routing authority. Values
//! are native timestamptz microseconds, including native timestamp infinities.
//! MINVALUE/MAXVALUE are bound sentinels distinct from timestamp infinities.
//! This module grants no publication or physical owner authority.
const std = @import("std");
const A = std.mem.Allocator;
/// Ordinary SQL timestamptz storage currently uses unsigned Unix nanoseconds.
/// Read that native value exactly; no float, milliseconds, epoch guessing or
/// lexicographic timestamp comparison participates in partition routing.
pub fn timestampKey(key: std.json.Value) !i64 {
    const temporal = @import("../sql/temporal/value.zig");
    return switch (key) {
        .string => |text| (try @import("../sql/temporal/text.zig").parseInput(.timestamp_tz, text)).timestamp_tz.micros,
        .integer => |nanos| (try temporal.Timestamp.fromUnixNanoseconds(nanos)).micros,
        .number_string => |text| (try temporal.Timestamp.fromUnixNanoseconds(try std.fmt.parseInt(u64, text, 10))).micros,
        else => error.SqlPartitionBoundViolation,
    };
}

pub const max_members = 64;
pub const Digest = [32]u8;
pub const Create = struct {
    key_name: []const u8,
    pub fn validate(self: Create) !void {
        if (self.key_name.len == 0 or self.key_name.len > 1024 or std.mem.indexOfScalar(u8, self.key_name, 0) != null or !std.unicode.utf8ValidateSlice(self.key_name)) return error.InvalidPartitionDescriptor;
    }
};
/// Stable content address shared by catalog, schemas and owner delivery.
pub fn programDigest(bytes: []const u8) Digest {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/partition-policy-program/v1\x00");
    hash.update(bytes);
    var result: Digest = undefined;
    hash.final(&result);
    return result;
}

/// Immutable schema identity, never executable program storage. Materializing
/// a reference requires its exact shared-registry bytes; owner authority still
/// comes from the committed parent epoch, independently of membership schemas.
pub const PolicyReference = struct {
    version: u8 = 1,
    generation: u64 = 0,
    catalog_epoch: u64 = 0,
    enabled: bool = false,
    program_digest: Digest = @splat(0),
    pub fn validate(self: @This()) !void {
        if (self.version != 1 or (self.enabled and (self.generation == 0 or self.catalog_epoch == 0 or std.mem.allEqual(u8, &self.program_digest, 0))) or
            (!self.enabled and (self.generation != 0 or self.catalog_epoch != 0 or !std.mem.allEqual(u8, &self.program_digest, 0)))) return error.InvalidPartitionDescriptor;
    }
    pub fn digest(self: @This()) !Digest {
        try self.validate();
        var bytes: [50]u8 = undefined;
        bytes[0] = self.version;
        bytes[1] = @intFromBool(self.enabled);
        std.mem.writeInt(u64, bytes[2..10], self.generation, .little);
        std.mem.writeInt(u64, bytes[10..18], self.catalog_epoch, .little);
        @memcpy(bytes[18..50], &self.program_digest);
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/partition-parent-policy/v2\x00");
        hash.update(&bytes);
        var result: Digest = undefined;
        hash.final(&result);
        return result;
    }
    /// Borrowed payload; callers retain its registry/packet lifetime separately.
    pub fn materialize(self: @This(), bytes: []const u8) !PolicySnapshot {
        try self.validate();
        const snapshot: PolicySnapshot = .{ .generation = self.generation, .catalog_epoch = self.catalog_epoch, .enabled = self.enabled, .snapshot_json = bytes };
        if (!std.meta.eql(self, try snapshot.reference())) return error.PartitionCatalogChanged;
        return snapshot;
    }
};

/// Owned/leased transport program. Schemas store PolicyReference instead.
pub const PolicySnapshot = struct {
    generation: u64 = 0,
    catalog_epoch: u64 = 0,
    enabled: bool = false,
    snapshot_json: []const u8 = "",
    pub fn validate(self: @This()) !void {
        if (self.snapshot_json.len > 4 * 1024 * 1024 or self.enabled != (self.snapshot_json.len != 0) or
            (self.enabled and (self.generation == 0 or self.catalog_epoch == 0)) or
            (!self.enabled and (self.generation != 0 or self.catalog_epoch != 0))) return error.InvalidPartitionDescriptor;
    }
    pub fn reference(self: @This()) !PolicyReference {
        try self.validate();
        return .{ .generation = self.generation, .catalog_epoch = self.catalog_epoch, .enabled = self.enabled, .program_digest = if (self.enabled) programDigest(self.snapshot_json) else @splat(0) };
    }
    pub fn digest(self: @This()) !Digest {
        return (try self.reference()).digest();
    }
};
/// Native, signed logical-to-physical access cut. No SQL name or caller JSON
/// can establish membership; the receiver rejoins its committed inverse owner.
pub const Route = struct {
    parent_id: u64,
    parent_revision: u64,
    parent_descriptor: Digest,
    child_id: u64,
    child_schema_version: u32,
    membership_generation: u64,
    policy: Digest,
    inherited: ?Digest = null,
    pub fn validate(self: Route) !void {
        if (self.inherited) |digest| if (std.mem.allEqual(u8, &digest, 0)) return error.InvalidPartitionDescriptor;
        if (self.parent_id == 0 or self.child_id == 0 or self.parent_id == self.child_id or
            self.parent_revision == 0 or self.membership_generation == 0 or self.membership_generation > self.parent_revision or
            std.mem.allEqual(u8, &self.parent_descriptor, 0) or std.mem.allEqual(u8, &self.policy, 0)) return error.InvalidPartitionDescriptor;
    }
    pub fn require(self: Route, member: Membership) !void {
        try self.requireMembership(member);
        if (!std.mem.eql(u8, &self.policy, &try member.parent_policy.digest())) return error.PartitionCatalogChanged;
    }
    /// A current owner policy sidecar supplies its own exact policy/descriptor
    /// cut. Membership identity remains bound to this immutable schema.
    pub fn requireMembership(self: Route, member: Membership) !void {
        try self.validate();
        if (!std.meta.eql(self.inherited, try inheritanceDigest(member.inherited))) return error.PartitionCatalogChanged;
        if (self.parent_id != member.parent_id or self.child_id != member.child.table_id or
            self.child_schema_version != member.child.schema_version or self.membership_generation != member.membership_generation) return error.PartitionCatalogChanged;
    }
};

pub const Relation = struct {
    table_id: u64,
    schema_version: u32,
    /// Canonical row layout + constraints, independently verified at admission.
    definition: Digest,

    pub fn validate(self: Relation) !void {
        if (self.table_id == 0 or std.mem.allEqual(u8, &self.definition, 0)) return error.InvalidPartitionDescriptor;
    }
};
pub const Bound = union(enum) {
    minvalue,
    value: i64,
    maxvalue,

    pub fn order(left: Bound, right: Bound) std.math.Order {
        return switch (left) {
            .minvalue => if (right == .minvalue) .eq else .lt,
            .maxvalue => if (right == .maxvalue) .eq else .gt,
            .value => |value| switch (right) {
                .minvalue => .gt,
                .maxvalue => .lt,
                .value => |other| std.math.order(value, other),
            },
        };
    }
};
pub const Range = struct {
    lower: Bound,
    upper: Bound,

    pub fn validate(self: Range) !void {
        if (Bound.order(self.lower, self.upper) != .lt) return error.SqlEmptyPartitionBound;
    }
    pub fn contains(self: Range, key: i64) bool {
        return Bound.order(self.lower, .{ .value = key }) != .gt and Bound.order(.{ .value = key }, self.upper) == .lt;
    }
    pub fn overlaps(self: Range, other: Range) bool {
        return Bound.order(self.lower, other.upper) == .lt and Bound.order(other.lower, self.upper) == .lt;
    }
};
pub const ConstraintBinding = struct {
    kind: enum { check, unique, foreign_key, index },
    parent_name: []const u8,
    child_name: []const u8,
};
pub fn validateInheritance(bindings: []const ConstraintBinding) !void {
    if (bindings.len > 1024) return error.InvalidPartitionDescriptor;
    for (bindings, 0..) |binding, i| {
        for ([_][]const u8{ binding.parent_name, binding.child_name }) |name| if (name.len == 0 or name.len > 256 or std.mem.indexOfScalar(u8, name, 0) != null or !std.unicode.utf8ValidateSlice(name)) return error.InvalidPartitionDescriptor;
        for (bindings[0..i]) |prior| if (prior.kind == binding.kind and (std.mem.eql(u8, prior.parent_name, binding.parent_name) or std.mem.eql(u8, prior.child_name, binding.child_name))) return error.InvalidPartitionDescriptor;
    }
}
pub fn inheritanceDigest(bindings: []const ConstraintBinding) !?Digest {
    try validateInheritance(bindings);
    if (bindings.len == 0) return null;
    var writer = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(std.crypto.hash.Blake3.init(.{}), &.{});
    try writer.writer.writeAll("antfly/sql-partition-inheritance/v1\x00");
    try std.json.Stringify.value(bindings, .{}, &writer.writer);
    var digest: Digest = undefined;
    writer.hasher.final(&digest);
    return digest;
}
pub fn cloneInheritance(a: std.mem.Allocator, bindings: []const ConstraintBinding) ![]const ConstraintBinding {
    const result = try a.alloc(ConstraintBinding, bindings.len);
    var n: usize = 0;
    errdefer {
        deinitInheritance(a, result[0..n]);
        a.free(result);
    }
    for (bindings) |binding| {
        const parent = try a.dupe(u8, binding.parent_name);
        errdefer a.free(parent);
        result[n] = .{ .kind = binding.kind, .parent_name = parent, .child_name = try a.dupe(u8, binding.child_name) };
        n += 1;
    }
    return result;
}
fn deinitInheritance(a: std.mem.Allocator, bindings: []const ConstraintBinding) void {
    for (bindings) |binding| {
        a.free(binding.parent_name);
        a.free(binding.child_name);
    }
}
pub const Member = struct { relation: Relation, bounds: Range, membership_generation: u64 = 0, inherited: []const ConstraintBinding = &.{} };
pub const Descriptor = struct {
    version: u8 = 1,
    parent: Relation,
    /// Monotone membership revision; included in every statement's read/write cut.
    revision: u64,
    /// Stable native attribute identity, independent of SQL column ordinals.
    key_attribute: u64,
    key_name: []const u8 = "created_at",
    key_type_oid: u32 = 1184,
    policy: PolicyReference = .{},
    /// Sorted by lower bound. No SQL name or mutable schema cache is retained.
    members: []const Member = &.{},

    pub fn validate(self: Descriptor) !void {
        try self.parent.validate();
        try self.policy.validate();
        if (self.version != 1 or self.revision == 0 or self.key_attribute == 0 or self.key_name.len == 0 or self.key_name.len > 1024 or std.mem.indexOfScalar(u8, self.key_name, 0) != null or self.key_type_oid != 1184 or self.members.len > max_members) return error.InvalidPartitionDescriptor;
        for (self.members, 0..) |member, index| {
            try member.relation.validate();
            try member.bounds.validate();
            try validateInheritance(member.inherited);
            if (member.relation.table_id == self.parent.table_id or member.membership_generation == 0 or member.membership_generation > self.revision) return error.InvalidPartitionDescriptor;
            for (self.members[0..index]) |prior| if (prior.relation.table_id == member.relation.table_id) return error.InvalidPartitionDescriptor;
            if (index != 0 and Bound.order(self.members[index - 1].bounds.upper, member.bounds.lower) == .gt) return error.SqlPartitionOverlap;
        }
    }
    /// Own every nested declaration used after the source catalog cut closes.
    pub fn clone(self: Descriptor, a: std.mem.Allocator) !Descriptor {
        try self.validate();
        var result = self;
        result.key_name = try a.dupe(u8, self.key_name);
        errdefer a.free(result.key_name);
        const members = try a.alloc(Member, self.members.len);
        errdefer a.free(members);
        var n: usize = 0;
        errdefer for (members[0..n]) |member| {
            deinitInheritance(a, member.inherited);
            a.free(member.inherited);
        };
        for (self.members) |member| {
            members[n] = member;
            members[n].inherited = try cloneInheritance(a, member.inherited);
            n += 1;
        }
        result.members = members;
        return result;
    }
    pub fn deinit(self: *Descriptor, a: std.mem.Allocator) void {
        a.free(self.key_name);
        for (self.members) |member| {
            deinitInheritance(a, member.inherited);
            a.free(member.inherited);
        }
        a.free(self.members);
        self.* = undefined;
    }
    pub fn digest(self: Descriptor) !Digest {
        try self.validate();
        var writer = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(std.crypto.hash.Blake3.init(.{}), &.{});
        try writer.writer.writeAll("antfly/sql-range-partition-descriptor/v1\x00");
        try std.json.Stringify.value(self, .{}, &writer.writer);
        var result: Digest = undefined;
        writer.hasher.final(&result);
        return result;
    }
    /// Callers retain the descriptor's catalog snapshot and acquire each leaf's
    /// ordinary physical read authority; a descriptor never replaces that proof.
    pub fn route(self: Descriptor, key: ?i64) !Relation {
        try self.validate();
        return self.members[try self.routeIndexValidated(key)].relation;
    }

    /// Binary lookup in a descriptor already validated by its retained family
    /// owner. Batch routing validates the immutable directory once, rather
    /// than scanning every member for every input row.
    pub fn routeIndexValidated(self: Descriptor, key: ?i64) !usize {
        const value = key orelse return error.SqlNoPartitionForRow;
        var low: usize = 0;
        var high = self.members.len;
        while (low < high) {
            const middle = low + (high - low) / 2;
            const member = self.members[middle];
            if (member.bounds.contains(value)) return middle;
            if (Bound.order(.{ .value = value }, member.bounds.lower) == .lt) high = middle else low = middle + 1;
        }
        return error.SqlNoPartitionForRow;
    }
    /// Candidate only. Native coordinator commits this and the child's inverse
    /// membership together after all physical owners installed the same decision.
    pub fn attach(self: Descriptor, a: A, child: Member) !Descriptor {
        try self.validate();
        try child.relation.validate();
        try child.bounds.validate();
        try validateInheritance(child.inherited);
        if (self.members.len == max_members) return error.SqlLimitExceeded;
        if (child.relation.table_id == self.parent.table_id) return error.SqlPartitionCycle;
        var position: usize = self.members.len;
        for (self.members, 0..) |member, index| {
            if (member.relation.table_id == child.relation.table_id) return error.SqlAlreadyPartition;
            if (child.bounds.overlaps(member.bounds)) return error.SqlPartitionOverlap;
            if (position == self.members.len and Bound.order(child.bounds.lower, member.bounds.lower) == .lt) position = index;
        }
        const revision = std.math.add(u64, self.revision, 1) catch return error.InvalidPartitionDescriptor;
        const members = try a.alloc(Member, self.members.len + 1);
        @memcpy(members[0..position], self.members[0..position]);
        members[position] = child;
        members[position].membership_generation = revision;
        @memcpy(members[position + 1 ..], self.members[position..]);
        var result = self;
        result.revision = revision;
        result.members = members;
        return result;
    }
    pub fn detach(self: Descriptor, a: A, child: u64) !Descriptor {
        try self.validate();
        const position = for (self.members, 0..) |member, index| {
            if (member.relation.table_id == child) break index;
        } else return error.SqlNotPartition;
        const revision = std.math.add(u64, self.revision, 1) catch return error.InvalidPartitionDescriptor;
        const members = try a.alloc(Member, self.members.len - 1);
        @memcpy(members[0..position], self.members[0..position]);
        @memcpy(members[position..], self.members[position + 1 ..]);
        var result = self;
        result.revision = revision;
        result.members = members;
        return result;
    }
};
/// Inverse membership lives on the physical child. A parent descriptor alone
/// cannot attach a child already owned by another parent or resurrect a stale
/// child generation. New writes need BOTH sides at the same published decision.
pub const Membership = struct {
    parent_id: u64,
    membership_generation: u64,
    key_attribute: u64,
    key_name: []const u8,
    child: Relation,
    bounds: Range,
    parent_policy: PolicyReference = .{},
    inherited: []const ConstraintBinding = &.{},
    pub fn clone(self: Membership, a: std.mem.Allocator) !Membership {
        var result = self;
        result.key_name = try a.dupe(u8, self.key_name);
        errdefer a.free(result.key_name);
        result.inherited = try cloneInheritance(a, self.inherited);
        return result;
    }
    pub fn deinit(self: *Membership, a: std.mem.Allocator) void {
        a.free(self.key_name);
        deinitInheritance(a, self.inherited);
        a.free(self.inherited);
        self.* = undefined;
    }

    pub fn require(self: Membership, parent: Descriptor) !void {
        try self.requireRouting(parent);
        if (!std.mem.eql(u8, &try self.parent_policy.digest(), &try parent.policy.digest())) return error.PartitionCatalogChanged;
    }
    /// Immutable physical membership outlives policy-only epochs. Callers must
    /// authenticate the current parent policy independently; birth still uses
    /// require() to bind its original policy in the accepting CREATE decision.
    pub fn requireRouting(self: Membership, parent: Descriptor) !void {
        try parent.validate();
        if (self.parent_id != parent.parent.table_id or self.key_attribute != parent.key_attribute or !std.mem.eql(u8, self.key_name, parent.key_name) or self.membership_generation == 0 or self.membership_generation > parent.revision) return error.PartitionCatalogChanged;
        const matching = for (parent.members) |member| {
            if (member.relation.table_id == self.child.table_id) break member;
        } else return error.PartitionCatalogChanged;
        if (!std.meta.eql(try inheritanceDigest(self.inherited), try inheritanceDigest(matching.inherited))) return error.PartitionCatalogChanged;
        if (matching.membership_generation != self.membership_generation or !std.meta.eql(matching.relation, self.child) or !std.meta.eql(matching.bounds, self.bounds)) return error.PartitionCatalogChanged;
    }
};
/// Persisted by trusted native SQL admission only. Public schema ingress may
/// never introduce or alter either variant. Both travel with the ordinary
/// versioned public schema and are interpreted by the epoch-owned validator.
pub const State = union(enum) {
    parent: Descriptor,
    member: Membership,

    pub fn validate(self: State) !void {
        switch (self) {
            .parent => |parent| try parent.validate(),
            .member => |membership| {
                try membership.child.validate();
                try membership.bounds.validate();
                try membership.parent_policy.validate();
                try validateInheritance(membership.inherited);
                if (membership.parent_id == 0 or membership.parent_id == membership.child.table_id or
                    membership.membership_generation == 0 or membership.key_attribute == 0 or
                    membership.key_name.len == 0 or membership.key_name.len > 1024 or
                    std.mem.indexOfScalar(u8, membership.key_name, 0) != null) return error.InvalidPartitionDescriptor;
            },
        }
    }
};

/// This check is shared by CREATE, ADD UNIQUE/PRIMARY KEY and unique-index
/// admission. Parent uniqueness is leaf-local only when every partition key
/// attribute participates in the equality key; INCLUDE attributes do not count.
pub fn requireUniqueKey(partition_keys: []const []const u8, unique_keys: []const []const u8) !void {
    for (partition_keys) |partition_key| {
        const found = for (unique_keys) |unique_key| {
            if (std.mem.eql(u8, partition_key, unique_key)) break true;
        } else false;
        if (!found) return error.SqlPartitionUniqueKeyMissing;
    }
}
fn relation(id: u64) Relation {
    return .{ .table_id = id, .schema_version = 1, .definition = @splat(1) };
}
fn initial() Descriptor {
    return .{ .parent = relation(1), .revision = 1, .key_attribute = 7 };
}
fn testMember(id: u64, lower: i64, upper: i64) Member {
    return .{ .relation = relation(id), .bounds = .{ .lower = .{ .value = lower }, .upper = .{ .value = upper } } };
}
test "partition immutable attach detach and half-open routing" {
    const a = std.testing.allocator;
    const first = try initial().attach(a, testMember(3, 100, 200));
    defer a.free(first.members);
    const next = try first.attach(a, testMember(2, 0, 100));
    defer a.free(next.members);
    try std.testing.expectEqual(@as(u64, 1), initial().revision);
    try std.testing.expectEqual(@as(u64, 2), (try next.route(0)).table_id);
    try std.testing.expectEqual(@as(u64, 3), (try next.route(100)).table_id);
    try std.testing.expectError(error.SqlNoPartitionForRow, next.route(200));
    try std.testing.expectError(error.SqlNoPartitionForRow, next.route(null));
    try std.testing.expectError(error.SqlNoPartitionForRow, next.route(-1));
    try std.testing.expectError(error.SqlPartitionOverlap, next.attach(a, testMember(4, 99, 101)));
    try std.testing.expectError(error.SqlAlreadyPartition, next.attach(a, testMember(2, 300, 400)));
    try std.testing.expectError(error.SqlPartitionCycle, next.attach(a, testMember(1, 300, 400)));
    var invalid_child = testMember(4, 300, 400);
    invalid_child.inherited = &.{.{ .kind = .check, .parent_name = "", .child_name = "local_check" }};
    try std.testing.expectError(error.InvalidPartitionDescriptor, next.attach(a, invalid_child));
    const detached = try next.detach(a, 2);
    defer a.free(detached.members);
    try std.testing.expectError(error.SqlNoPartitionForRow, detached.route(99));
    try std.testing.expectEqual(@as(u64, 2), (try next.route(99)).table_id);
    try std.testing.expectError(error.SqlNotPartition, detached.detach(a, 2));
}
test "partition cold descriptor preserves routing and stale membership is fenced" {
    const a = std.testing.allocator;
    const descriptor = try initial().attach(a, testMember(2, 0, 100));
    defer a.free(descriptor.members);
    const bytes = try std.json.Stringify.valueAlloc(a, descriptor, .{});
    defer a.free(bytes);
    var cold = try std.json.parseFromSlice(Descriptor, a, bytes, .{});
    defer cold.deinit();
    try cold.value.validate();
    try std.testing.expectEqual(try descriptor.digest(), try cold.value.digest());
    try std.testing.expectEqual(@as(u64, 2), (try cold.value.route(42)).table_id);
    const inverse: Membership = .{ .parent_id = 1, .membership_generation = descriptor.members[0].membership_generation, .key_attribute = descriptor.key_attribute, .key_name = descriptor.key_name, .child = relation(2), .bounds = descriptor.members[0].bounds };
    try inverse.require(cold.value);
    const detached = try cold.value.detach(a, 2);
    defer a.free(detached.members);
    try std.testing.expectError(error.PartitionCatalogChanged, inverse.require(detached));
    const reattached = try detached.attach(a, testMember(2, 0, 100));
    defer a.free(reattached.members);
    try std.testing.expectError(error.PartitionCatalogChanged, inverse.require(reattached));
}
test "partition unique constraints include every key and infinity differs from MAXVALUE" {
    try requireUniqueKey(&.{"created_at"}, &.{ "tenant_id", "id", "created_at" });
    try std.testing.expectError(error.SqlPartitionUniqueKeyMissing, requireUniqueKey(&.{"created_at"}, &.{ "tenant_id", "id" }));
    const upper_timestamp = Range{ .lower = .minvalue, .upper = .{ .value = std.math.maxInt(i64) } };
    const upper_sentinel = Range{ .lower = .minvalue, .upper = .maxvalue };
    try std.testing.expect(!upper_timestamp.contains(std.math.maxInt(i64)));
    try std.testing.expect(upper_sentinel.contains(std.math.maxInt(i64)));
    try std.testing.expectError(error.SqlEmptyPartitionBound, (Range{ .lower = .{ .value = 1 }, .upper = .{ .value = 1 } }).validate());
}

/// Owner-local historical validation. The caller supplies its ordinary bounded
/// primary image iterator under schema-validation fencing. The callback returns
/// native typed timestamps; it must not read an index that can omit old rows.
/// Passing this function is not an authorization or publication attestation.
pub fn validateRows(bounds: Range, iterator: anytype) !u64 {
    try bounds.validate();
    var count: u64 = 0;
    while (try iterator.next()) |row| {
        try iterator.checkpoint();
        const key = row.key orelse return error.SqlPartitionBoundViolation;
        if (!bounds.contains(key)) return error.SqlPartitionBoundViolation;
        count = std.math.add(u64, count, 1) catch return error.SqlLimitExceeded;
    }
    return count;
}

test "partition historical attach rejects out-of-range and NULL primary rows" {
    const Iterator = struct {
        keys: []const ?i64,
        position: usize = 0,
        calls: usize = 0,
        const Row = struct { key: ?i64 };
        fn next(self: *@This()) !?Row {
            if (self.position == self.keys.len) return null;
            defer self.position += 1;
            return .{ .key = self.keys[self.position] };
        }
        fn checkpoint(self: *@This()) !void {
            self.calls += 1;
        }
    };
    const bounds = testMember(2, 0, 100).bounds;
    var accepted: Iterator = .{ .keys = &.{ 0, 99 } };
    try std.testing.expectEqual(@as(u64, 2), try validateRows(bounds, &accepted));
    try std.testing.expectEqual(@as(usize, 2), accepted.calls);
    var upper: Iterator = .{ .keys = &.{ 0, 100 } };
    try std.testing.expectError(error.SqlPartitionBoundViolation, validateRows(bounds, &upper));
    var lower: Iterator = .{ .keys = &.{-1} };
    try std.testing.expectError(error.SqlPartitionBoundViolation, validateRows(bounds, &lower));
    var missing: Iterator = .{ .keys = &.{null} };
    try std.testing.expectError(error.SqlPartitionBoundViolation, validateRows(bounds, &missing));
}

fn allocationProbe(a: A) !void {
    const before = initial();
    const original_digest = try before.digest();
    const first = try before.attach(a, testMember(2, 0, 100));
    defer a.free(first.members);
    const second = try first.attach(a, testMember(3, 100, 200));
    defer a.free(second.members);
    const detached = try second.detach(a, 2);
    defer a.free(detached.members);
    try std.testing.expectEqual(original_digest, try before.digest());
    try std.testing.expectEqual(@as(u64, 2), (try first.route(42)).table_id);
    try std.testing.expectEqual(@as(u64, 3), (try detached.route(142)).table_id);
}

test "partition candidate allocation failure never mutates published membership" {
    try allocationProbe(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationProbe, .{});
}

pub fn routeFor(parent: Descriptor, child: u64) !Route {
    try parent.validate();
    const member = for (parent.members) |entry| {
        if (entry.relation.table_id == child) break entry;
    } else return error.SqlNotPartition;
    return .{ .parent_id = parent.parent.table_id, .parent_revision = parent.revision, .parent_descriptor = try parent.digest(), .child_id = child, .child_schema_version = member.relation.schema_version, .membership_generation = member.membership_generation, .policy = try parent.policy.digest(), .inherited = try inheritanceDigest(member.inherited) };
}

test "row-policy routing retains physical membership across policy epochs without relaxing original birth authority" {
    const parent: Descriptor = .{ .parent = .{ .table_id = 1, .schema_version = 0, .definition = @splat(1) }, .revision = 1, .key_attribute = 1, .members = &.{.{ .relation = .{ .table_id = 2, .schema_version = 0, .definition = @splat(2) }, .bounds = .{ .lower = .minvalue, .upper = .maxvalue }, .membership_generation = 1 }} };
    const member: Membership = .{ .parent_id = 1, .membership_generation = 1, .key_attribute = 1, .key_name = "created_at", .child = parent.members[0].relation, .bounds = parent.members[0].bounds };
    try member.require(parent);
    var current = parent;
    current.revision += 1;
    current.policy = .{ .enabled = true, .generation = 2, .catalog_epoch = 3, .program_digest = @splat(4) };
    try member.requireRouting(current);
    try std.testing.expectError(error.PartitionCatalogChanged, member.require(current));
    const route = try routeFor(current, 2);
    try route.requireMembership(member);
    try std.testing.expectError(error.PartitionCatalogChanged, route.require(member));
    var rebound = member;
    rebound.key_attribute += 1;
    try std.testing.expectError(error.PartitionCatalogChanged, rebound.requireRouting(current));
    rebound = member;
    rebound.bounds.lower = .{ .value = 1 };
    try std.testing.expectError(error.PartitionCatalogChanged, rebound.requireRouting(current));
    rebound = member;
    rebound.child.definition[0] ^= 1;
    try std.testing.expectError(error.PartitionCatalogChanged, rebound.requireRouting(current));
    rebound = member;
    rebound.membership_generation += 1;
    try std.testing.expectError(error.PartitionCatalogChanged, rebound.requireRouting(current));
}

test "partition descriptor deep clones inherited owners and route rejects substitution" {
    const Runner = struct {
        fn run(a: std.mem.Allocator) !void {
            var parent_name = [_]u8{ 'p', 'k' };
            var child_name = [_]u8{ 'c', 'k' };
            const bindings = [_]ConstraintBinding{.{ .kind = .unique, .parent_name = &parent_name, .child_name = &child_name }};
            const descriptor: Descriptor = .{ .parent = .{ .table_id = 1, .schema_version = 2, .definition = @splat(1) }, .revision = 2, .key_attribute = 1, .key_name = "at", .members = &.{.{ .relation = .{ .table_id = 2, .schema_version = 1, .definition = @splat(2) }, .bounds = .{ .lower = .minvalue, .upper = .maxvalue }, .membership_generation = 2, .inherited = &bindings }} };
            var owned = try descriptor.clone(a);
            defer owned.deinit(a);
            const digest = try owned.digest();
            const route = try routeFor(owned, 2);
            const inverse: Membership = .{ .parent_id = 1, .membership_generation = 2, .key_attribute = 1, .key_name = "at", .child = owned.members[0].relation, .bounds = owned.members[0].bounds, .inherited = owned.members[0].inherited };
            var inverse_owned = try inverse.clone(a);
            defer inverse_owned.deinit(a);
            parent_name[0] = 'x';
            child_name[0] = 'x';
            try std.testing.expectEqualStrings("pk", owned.members[0].inherited[0].parent_name);
            try std.testing.expectEqualStrings("ck", inverse_owned.inherited[0].child_name);
            try std.testing.expectEqual(digest, try owned.digest());
            try route.require(inverse_owned);
            var substituted = inverse_owned;
            substituted.inherited = &.{.{ .kind = .unique, .parent_name = "pk", .child_name = "other" }};
            try std.testing.expectError(error.PartitionCatalogChanged, route.require(substituted));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "row-policy compact references keep64 schema members bounded independently of4MiB executable programs" {
    const a = std.testing.allocator;
    const body = try a.alloc(u8, 4 * 1024 * 1024);
    defer a.free(body);
    @memset(body, ' ');
    const prefix = "{\"version\":1,\"columns\":[],\"records\":[],\"settings\":[]}";
    @memcpy(body[0..prefix.len], prefix);
    const program: PolicySnapshot = .{ .enabled = true, .generation = 5, .catalog_epoch = 6, .snapshot_json = body };
    const reference = try program.reference();
    const encoded = try std.json.Stringify.valueAlloc(a, reference, .{});
    defer a.free(encoded);
    try std.testing.expect(encoded.len < 512);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "snapshot_json") == null);
    try std.testing.expectEqualDeep(reference, try (try reference.materialize(body)).reference());
    try std.testing.expectEqualDeep(try program.digest(), try reference.digest());
    body[body.len - 1] = '\n';
    try std.testing.expectError(error.PartitionCatalogChanged, reference.materialize(body));
    body[body.len - 1] = ' ';
    var members: [max_members]Member = undefined;
    for (&members, 0..) |*entry, index| entry.* = .{ .relation = .{ .table_id = index + 1, .schema_version = 0, .definition = @splat(1) }, .membership_generation = 1, .bounds = .{ .lower = .{ .value = @intCast(index) }, .upper = .{ .value = @intCast(index + 1) } } };
    const descriptor: Descriptor = .{ .parent = .{ .table_id = 1000, .schema_version = 0, .definition = @splat(1) }, .revision = 1, .key_attribute = 1, .policy = reference, .members = &members };
    var owned = try descriptor.clone(a);
    defer owned.deinit(a);
    const bytes = try std.json.Stringify.valueAlloc(a, owned, .{});
    defer a.free(bytes);
    try std.testing.expect(bytes.len < 64 * 1024);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "snapshot_json") == null);
    var invalid = reference;
    invalid.program_digest = @splat(0);
    try std.testing.expectError(error.InvalidPartitionDescriptor, invalid.validate());
    try std.testing.expectError(error.InvalidPartitionDescriptor, (PolicyReference{ .generation = 1 }).validate());
    try std.testing.expectEqualDeep(try (PolicySnapshot{}).digest(), try (PolicyReference{}).digest());
}
