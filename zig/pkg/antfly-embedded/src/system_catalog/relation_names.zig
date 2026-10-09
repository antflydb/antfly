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

//! Namespace relation ownership for atomic catalog/schema publication.
//! This planner deliberately does not discover owners by scanning tables, nor
//! does it commit independently of the caller's metadata write transaction.
const std = @import("std");
const A = std.mem.Allocator;
pub const max_claims = 8192;
pub const max_name_bytes = 256;

pub const Kind = enum(u8) { table = 1, index = 2, constraint_index = 3 };
pub const Phase = enum(u8) { reserved = 1, active = 2, retiring = 3 };
pub const Owner = struct {
    table_id: u64,
    schema_version: u32,
    schema_digest: [32]u8,
    publication_id: [16]u8 = @splat(0),
    kind: Kind,
    phase: Phase = .active,

    pub fn eql(self: Owner, other: Owner) bool {
        return self.table_id == other.table_id and self.schema_version == other.schema_version and
            std.mem.eql(u8, &self.schema_digest, &other.schema_digest) and
            std.mem.eql(u8, &self.publication_id, &other.publication_id) and
            self.kind == other.kind and self.phase == other.phase;
    }
    pub fn validate(self: Owner) !void {
        if (self.table_id == 0) return error.InvalidCatalogRecord;
        if (self.phase != .active and std.mem.allEqual(u8, &self.publication_id, 0)) return error.InvalidCatalogRecord;
    }

    const magic = "AFRN01";
    pub const encoded_len = magic.len + 8 + 4 + 32 + 16 + 2;
    pub fn encode(self: Owner) ![encoded_len]u8 {
        try self.validate();
        var out: [encoded_len]u8 = undefined;
        @memcpy(out[0..magic.len], magic);
        var offset: usize = magic.len;
        std.mem.writeInt(u64, out[offset..][0..8], self.table_id, .big);
        offset += 8;
        std.mem.writeInt(u32, out[offset..][0..4], self.schema_version, .big);
        offset += 4;
        @memcpy(out[offset..][0..32], &self.schema_digest);
        offset += 32;
        @memcpy(out[offset..][0..16], &self.publication_id);
        offset += 16;
        out[offset] = @backingInt(self.kind);
        out[offset + 1] = @backingInt(self.phase);
        return out;
    }
    pub fn decode(bytes: []const u8) !Owner {
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..magic.len], magic)) return error.InvalidCatalogRecord;
        var offset: usize = magic.len;
        const table_id = std.mem.readInt(u64, bytes[offset..][0..8], .big);
        offset += 8;
        const version = std.mem.readInt(u32, bytes[offset..][0..4], .big);
        offset += 4;
        const digest: [32]u8 = bytes[offset..][0..32].*;
        offset += 32;
        const publication: [16]u8 = bytes[offset..][0..16].*;
        offset += 16;
        const result: Owner = .{
            .table_id = table_id,
            .schema_version = version,
            .schema_digest = digest,
            .publication_id = publication,
            .kind = std.enums.fromInt(Kind, bytes[offset]) orelse return error.InvalidCatalogRecord,
            .phase = std.enums.fromInt(Phase, bytes[offset + 1]) orelse return error.InvalidCatalogRecord,
        };
        try result.validate();
        return result;
    }
};

pub const Key = struct {
    namespace_id: u64,
    name: []const u8,
    pub fn validate(self: Key) !void {
        if (self.namespace_id == 0 or self.name.len == 0 or self.name.len > max_name_bytes or
            std.mem.indexOfScalar(u8, self.name, 0) != null or !std.unicode.utf8ValidateSlice(self.name)) return error.InvalidCatalogName;
    }
    /// Length-delimited UTF-8 preserves quoted SQL names, including dots and
    /// colons. Namespace IDs already identify their parent database uniquely.
    pub fn storageKeyAlloc(self: Key, a: A, group_id: u64) ![]u8 {
        try self.validate();
        if (group_id == 0) return error.InvalidCatalogRecord;
        const prefix = "\x00\x00__metadata_derived__:sql_relation_names:v1:";
        const bytes = try a.alloc(u8, prefix.len + 18 + self.name.len);
        @memcpy(bytes[0..prefix.len], prefix);
        std.mem.writeInt(u64, bytes[prefix.len..][0..8], group_id, .big);
        std.mem.writeInt(u64, bytes[prefix.len + 8 ..][0..8], self.namespace_id, .big);
        std.mem.writeInt(u16, bytes[prefix.len + 16 ..][0..2], @intCast(self.name.len), .big);
        @memcpy(bytes[prefix.len + 18 ..], self.name);
        return bytes;
    }
};
pub const Claim = struct { key: Key, owner: Owner };
const Context = struct {
    pub fn hash(_: Context, key: Key) u64 {
        var h = std.hash.Wyhash.init(key.namespace_id);
        h.update(key.name);
        return h.final();
    }
    pub fn eql(_: Context, left: Key, right: Key) bool {
        return left.namespace_id == right.namespace_id and std.mem.eql(u8, left.name, right.name);
    }
};
const Map = std.HashMapUnmanaged(Key, Owner, Context, 80);

/// Point adapter over the metadata owner's existing read/write transaction.
/// No second transaction or independent namespace commit can be opened here.
pub fn Store(comptime Txn: type) type {
    return struct {
        txn: *Txn,
        alloc: A,
        group_id: u64,
        pub fn getClaim(self: *@This(), key: Key) !?Owner {
            const bytes = try key.storageKeyAlloc(self.alloc, self.group_id);
            defer self.alloc.free(bytes);
            const value = self.txn.get(bytes) catch |err| {
                if (err == error.NotFound) return null;
                return err;
            };
            return try Owner.decode(value);
        }
        pub fn putClaim(self: *@This(), key: Key, owner: Owner) !void {
            const bytes = try key.storageKeyAlloc(self.alloc, self.group_id);
            defer self.alloc.free(bytes);
            const value = try owner.encode();
            try self.txn.put(bytes, &value);
        }
        pub fn deleteClaim(self: *@This(), key: Key) !void {
            const bytes = try key.storageKeyAlloc(self.alloc, self.group_id);
            defer self.alloc.free(bytes);
            try self.txn.delete(bytes);
        }
    };
}

/// An immutable before/after cut, owned independently of request JSON and
/// schema-cache lifetimes. All claims, including retained names, carry their
/// exact table epoch, publication identity, origin and lifecycle phase.
pub const Plan = struct {
    arena: std.heap.ArenaAllocator,
    before: []const Claim,
    after: []const Claim,
    before_by_name: Map,
    after_by_name: Map,

    pub fn init(a: A, before: []const Claim, after: []const Claim) !Plan {
        if (before.len > max_claims or after.len > max_claims) return error.CatalogCommandTooLarge;
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const owned = arena.allocator();
        var old_names: Map = .empty;
        var new_names: Map = .empty;
        const old = try copyCut(owned, before, &old_names, false);
        const new = try copyCut(owned, after, &new_names, true);
        return .{ .arena = arena, .before = old, .after = new, .before_by_name = old_names, .after_by_name = new_names };
    }
    pub fn deinit(self: *Plan) void {
        self.arena.deinit();
        self.* = undefined;
    }
    fn copyCut(a: A, claims: []const Claim, map: *Map, proposed: bool) ![]const Claim {
        const result = try a.alloc(Claim, claims.len);
        try map.ensureTotalCapacity(a, @intCast(claims.len));
        for (claims, result) |claim, *copy| {
            try claim.key.validate();
            try claim.owner.validate();
            const key: Key = .{ .namespace_id = claim.key.namespace_id, .name = try a.dupe(u8, claim.key.name) };
            const found = map.getOrPutAssumeCapacity(key);
            if (found.found_existing) return if (proposed) error.CatalogAlreadyExists else error.InvalidCatalogRecord;
            found.value_ptr.* = claim.owner;
            copy.* = .{ .key = key, .owner = claim.owner };
        }
        return result;
    }

    /// Reader must pin one catalog transaction for this complete validation.
    /// Work is one point lookup per distinct name in the before/after cut;
    /// neither the number of unrelated tables nor their schemas is involved.
    pub fn validate(self: *const Plan, reader: anytype) !void {
        for (self.before) |claim| {
            const current = (try reader.getClaim(claim.key)) orelse return error.CatalogGenerationChanged;
            if (!current.eql(claim.owner)) return error.CatalogGenerationChanged;
        }
        for (self.after) |claim| {
            // Retained names were already generation-fenced in this same
            // pinned transaction. Do not reread every unchanged index owner.
            if (self.before_by_name.contains(claim.key)) continue;
            if (try reader.getClaim(claim.key)) |_| return error.CatalogAlreadyExists;
        }
    }

    /// Apply inside the SAME write transaction as schema/catalog metadata and
    /// publication/outbox state. On any error the caller MUST abort that
    /// transaction. This method never commits, retries, or assumes ownership
    /// merely because a conflicting claim has the same table ID/name.
    pub fn apply(self: *const Plan, txn: anytype) !void {
        try self.validate(txn);
        for (self.before) |claim| if (!self.after_by_name.contains(claim.key)) try txn.deleteClaim(claim.key);
        for (self.after) |claim| {
            if (self.before_by_name.get(claim.key)) |prior| if (prior.eql(claim.owner)) continue;
            try txn.putClaim(claim.key, claim.owner);
        }
    }
};

const TestStore = struct {
    rows: std.ArrayList(Claim) = .empty,
    calls: usize = 0,
    writes: usize = 0,
    fn getClaim(self: *TestStore, key: Key) !?Owner {
        self.calls += 1;
        for (self.rows.items) |claim| if (Context.eql(.{}, key, claim.key)) return claim.owner;
        return null;
    }
    fn putClaim(self: *TestStore, key: Key, owner: Owner) !void {
        self.writes += 1;
        for (self.rows.items) |*claim| if (Context.eql(.{}, key, claim.key)) {
            claim.owner = owner;
            return;
        };
        try self.rows.append(std.testing.allocator, .{ .key = key, .owner = owner });
    }
    fn deleteClaim(self: *TestStore, key: Key) !void {
        self.writes += 1;
        for (self.rows.items, 0..) |claim, i| if (Context.eql(.{}, key, claim.key)) {
            _ = self.rows.orderedRemove(i);
            return;
        };
        return error.InvalidCatalogRecord;
    }
};
fn testClaim(namespace: u64, name: []const u8, table: u64, version: u32, kind: Kind) Claim {
    return .{ .key = .{ .namespace_id = namespace, .name = name }, .owner = .{ .table_id = table, .schema_version = version, .schema_digest = @splat(@intCast(version)), .kind = kind } };
}

test "catalog relation ownership is namespace scoped and generation fenced" {
    const a = std.testing.allocator;
    const old = testClaim(2, "email_key", 7, 1, .index);
    const other = testClaim(2, "other_key", 8, 1, .constraint_index);
    var store: TestStore = .{};
    defer store.rows.deinit(a);
    try store.rows.appendSlice(a, &.{ old, other });
    var collision = try Plan.init(a, &.{}, &.{testClaim(2, "email_key", 9, 1, .table)});
    defer collision.deinit();
    try std.testing.expectError(error.CatalogAlreadyExists, collision.apply(&store));
    try std.testing.expectEqual(@as(usize, 0), store.writes);
    var scoped = try Plan.init(a, &.{}, &.{testClaim(3, "email_key", 9, 1, .index)});
    defer scoped.deinit();
    try scoped.apply(&store);
    const next = testClaim(2, "email_key", 7, 2, .index);
    var replace = try Plan.init(a, &.{old}, &.{next});
    defer replace.deinit();
    store.calls = 0;
    try replace.apply(&store);
    try std.testing.expectEqual(@as(usize, 1), store.calls);
    const writes = store.writes;
    try std.testing.expectError(error.CatalogGenerationChanged, replace.apply(&store));
    try std.testing.expectEqual(writes, store.writes);
    var retire = try Plan.init(a, &.{next}, &.{});
    defer retire.deinit();
    try retire.apply(&store);
    try std.testing.expect((try store.getClaim(other.key)).?.eql(other.owner));
    try std.testing.expect((try store.getClaim(.{ .namespace_id = 3, .name = "email_key" })) != null);
}

test "catalog relation ownership rejects duplicate cuts and preserves pending publication authority" {
    const a = std.testing.allocator;
    const claim = testClaim(2, "items", 7, 1, .table);
    try std.testing.expectError(error.CatalogAlreadyExists, Plan.init(a, &.{}, &.{ claim, claim }));
    try std.testing.expectError(error.InvalidCatalogRecord, Plan.init(a, &.{ claim, claim }, &.{}));
    var pending = claim;
    pending.owner.kind = .index;
    pending.owner.phase = .reserved;
    try std.testing.expectError(error.InvalidCatalogRecord, pending.owner.encode());
    pending.owner.publication_id = @splat(3);
    var store: TestStore = .{};
    defer store.rows.deinit(a);
    try store.rows.append(a, pending);
    var stolen = try Plan.init(a, &.{claim}, &.{claim});
    defer stolen.deinit();
    try std.testing.expectError(error.CatalogGenerationChanged, stolen.apply(&store));
    var active = pending;
    active.owner.phase = .active;
    var publish = try Plan.init(a, &.{pending}, &.{active});
    defer publish.deinit();
    try publish.apply(&store);
    try std.testing.expect((try store.getClaim(active.key)).?.eql(active.owner));
    try std.testing.expectError(error.CatalogGenerationChanged, publish.apply(&store));
}

test "catalog relation ownership durable encoding rejects ambiguity corruption and unknown versions" {
    const a = std.testing.allocator;
    const claim = testClaim(2, "quoted.index:名", 7, 3, .constraint_index);
    const encoded = try claim.owner.encode();
    try std.testing.expect(claim.owner.eql(try Owner.decode(&encoded)));
    var corrupt = encoded;
    corrupt[0] = 'X';
    try std.testing.expectError(error.InvalidCatalogRecord, Owner.decode(&corrupt));
    corrupt = encoded;
    corrupt[corrupt.len - 1] = 255;
    try std.testing.expectError(error.InvalidCatalogRecord, Owner.decode(&corrupt));
    try std.testing.expectError(error.InvalidCatalogRecord, Owner.decode(encoded[0 .. encoded.len - 1]));
    const first = try claim.key.storageKeyAlloc(a, 1);
    defer a.free(first);
    const group = try claim.key.storageKeyAlloc(a, 2);
    defer a.free(group);
    const namespace = try (Key{ .namespace_id = 3, .name = claim.key.name }).storageKeyAlloc(a, 1);
    defer a.free(namespace);
    try std.testing.expect(!std.mem.eql(u8, first, group));
    try std.testing.expect(!std.mem.eql(u8, first, namespace));
    try std.testing.expectError(error.InvalidCatalogName, (Key{ .namespace_id = 2, .name = "bad\x00name" }).validate());
}

test "catalog relation ownership owns names and unwinds allocation failures" {
    const Probe = struct {
        fn run(a: A) !void {
            var plan = blk: {
                const name = try a.dupe(u8, "owned_name");
                defer a.free(name);
                break :blk try Plan.init(a, &.{testClaim(2, name, 7, 1, .index)}, &.{testClaim(2, name, 7, 2, .index)});
            };
            defer plan.deinit();
            try std.testing.expectEqualStrings("owned_name", plan.before[0].key.name);
            try std.testing.expectEqualStrings("owned_name", plan.after[0].key.name);
            try std.testing.expectEqual(@as(u32, 2), plan.after_by_name.get(.{ .namespace_id = 2, .name = "owned_name" }).?.schema_version);
        }
    };
    try Probe.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}

test "catalog relation ownership transaction adapter persists exact fenced records and isolates groups" {
    const Txn = struct {
        rows: std.StringHashMapUnmanaged([]u8) = .empty,
        fn get(self: *@This(), key: []const u8) ![]const u8 {
            return self.rows.get(key) orelse error.NotFound;
        }
        fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            const a = std.testing.allocator;
            const bytes = try a.dupe(u8, value);
            errdefer a.free(bytes);
            const name = try a.dupe(u8, key);
            errdefer a.free(name);
            const found = try self.rows.getOrPut(a, name);
            if (found.found_existing) {
                a.free(name);
                a.free(found.value_ptr.*);
            }
            found.value_ptr.* = bytes;
        }
        fn delete(self: *@This(), key: []const u8) !void {
            const entry = self.rows.fetchRemove(key) orelse return error.NotFound;
            std.testing.allocator.free(entry.key);
            std.testing.allocator.free(entry.value);
        }
        fn deinit(self: *@This()) void {
            var it = self.rows.iterator();
            while (it.next()) |entry| {
                std.testing.allocator.free(entry.key_ptr.*);
                std.testing.allocator.free(entry.value_ptr.*);
            }
            self.rows.deinit(std.testing.allocator);
        }
    };
    var txn: Txn = .{};
    defer txn.deinit();
    var store: Store(Txn) = .{ .txn = &txn, .alloc = std.testing.allocator, .group_id = 7 };
    const claim = testClaim(2, "quoted.index:名", 11, 9, .constraint_index);
    var plan = try Plan.init(std.testing.allocator, &.{}, &.{claim});
    defer plan.deinit();
    try plan.apply(&store);
    try std.testing.expect((try store.getClaim(claim.key)).?.eql(claim.owner));
    var other = store;
    other.group_id = 8;
    try std.testing.expect((try other.getClaim(claim.key)) == null);
    const key = try claim.key.storageKeyAlloc(std.testing.allocator, 7);
    defer std.testing.allocator.free(key);
    try txn.put(key, "corrupt");
    try std.testing.expectError(error.InvalidCatalogRecord, store.getClaim(claim.key));
    try store.deleteClaim(claim.key);
    try std.testing.expect((try store.getClaim(claim.key)) == null);
}

test "catalog relation ownership unchanged cuts issue no writes and rename preserves unrelated owners" {
    const a = std.testing.allocator;
    const old = testClaim(2, "old", 7, 1, .table);
    const other = testClaim(2, "unrelated", 99, 1, .index);
    var txn: TestStore = .{};
    defer txn.rows.deinit(a);
    try txn.rows.appendSlice(a, &.{ old, other });
    var noop = try Plan.init(a, &.{old}, &.{old});
    defer noop.deinit();
    try noop.apply(&txn);
    try std.testing.expectEqual(@as(usize, 0), txn.writes);
    try std.testing.expectEqual(@as(usize, 1), txn.calls);
    const new = testClaim(3, "new", 7, 2, .table);
    var rename = try Plan.init(a, &.{old}, &.{new});
    defer rename.deinit();
    try rename.apply(&txn);
    try std.testing.expectEqual(@as(usize, 2), txn.writes);
    try std.testing.expect((try txn.getClaim(old.key)) == null);
    try std.testing.expect((try txn.getClaim(new.key)).?.eql(new.owner));
    try std.testing.expect((try txn.getClaim(other.key)).?.eql(other.owner));
}

test "catalog relation ownership validates the complete cut before any mutation" {
    const a = std.testing.allocator;
    const Reader = struct {
        old: Claim,
        calls: usize = 0,
        pub fn getClaim(self: *@This(), key: Key) !?Owner {
            self.calls += 1;
            if (self.calls == 2) return error.InjectedReadFailure;
            return if (Context.eql(.{}, key, self.old.key)) self.old.owner else null;
        }
        pub fn putClaim(_: *@This(), _: Key, _: Owner) !void {
            return error.UnexpectedMutation;
        }
        pub fn deleteClaim(_: *@This(), _: Key) !void {
            return error.UnexpectedMutation;
        }
    };
    const old = testClaim(2, "old", 7, 1, .index);
    var plan = try Plan.init(a, &.{old}, &.{testClaim(2, "new", 7, 2, .index)});
    defer plan.deinit();
    var reader: Reader = .{ .old = old };
    try std.testing.expectError(error.InjectedReadFailure, plan.apply(&reader));
    try std.testing.expectEqual(@as(usize, 2), reader.calls);
}

test "catalog relation ownership bounds claims and accepts only valid unambiguous names" {
    const a = std.testing.allocator;
    const oversized = try a.alloc(Claim, max_claims + 1);
    defer a.free(oversized);
    try std.testing.expectError(error.CatalogCommandTooLarge, Plan.init(a, oversized, &.{}));
    try std.testing.expectError(error.CatalogCommandTooLarge, Plan.init(a, &.{}, oversized));
    for ([_][]const u8{ "", "bad\x00name", "\xff" }) |name| {
        try std.testing.expectError(error.InvalidCatalogName, Plan.init(a, &.{}, &.{testClaim(2, name, 7, 1, .index)}));
    }
    try std.testing.expectError(error.InvalidCatalogName, Plan.init(a, &.{}, &.{testClaim(0, "name", 7, 1, .index)}));
    try std.testing.expectError(error.InvalidCatalogRecord, Plan.init(a, &.{}, &.{testClaim(2, "name", 0, 1, .index)}));
}
