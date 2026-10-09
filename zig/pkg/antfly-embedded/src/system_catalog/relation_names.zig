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
const key_prefix = "\x00\x00__metadata_derived__:sql_relation_names:v1:";

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

/// A namespace name can remain readable while a restore/FK successor is
/// reserved. The reservation is never an active lookup result. These cuts
/// must be published through the caller's metadata transaction, not a second
/// reservation service or independently committed registry.
pub const Entry = struct {
    active: ?Owner = null,
    pending: ?Owner = null,

    pub const Reservation = struct {
        predecessor: ?Owner = null,
        successor: Owner,
        pub fn validate(self: @This()) !void {
            if (self.predecessor) |owner| {
                try owner.validate();
                if (owner.phase != .active) return error.InvalidCatalogRecord;
            }
            try self.successor.validate();
            if (self.successor.phase != .reserved) return error.InvalidCatalogRecord;
        }
    };
    pub fn validate(self: Entry) !void {
        if (self.active) |owner| {
            try owner.validate();
            if (owner.phase != .active) return error.InvalidCatalogRecord;
        }
        if (self.pending) |owner| {
            try owner.validate();
            if (owner.phase != .reserved) return error.InvalidCatalogRecord;
        }
    }
    fn same(left: ?Owner, right: ?Owner) bool {
        return if (left) |l| if (right) |r| l.eql(r) else false else right == null;
    }
    pub fn eql(self: Entry, other: Entry) bool {
        return same(self.active, other.active) and same(self.pending, other.pending);
    }
    pub fn empty(self: Entry) bool {
        return self.active == null and self.pending == null;
    }
    pub fn reserve(self: Entry, request: Reservation) !Entry {
        try self.validate();
        try request.validate();
        if (!same(self.active, request.predecessor)) return error.CatalogGenerationChanged;
        if (self.pending) |owner| {
            if (!owner.eql(request.successor)) return error.CatalogAlreadyExists;
            return self; // Exact in-flight retry, not same-name/table inference.
        }
        return .{ .active = self.active, .pending = request.successor };
    }
    fn checkReservation(self: Entry, request: Reservation) !void {
        try self.validate();
        try request.validate();
        if (!same(self.active, request.predecessor) or !same(self.pending, request.successor))
            return error.CatalogGenerationChanged;
    }
    pub fn publish(self: Entry, request: Reservation) !Entry {
        try self.checkReservation(request);
        var owner = request.successor;
        owner.phase = .active;
        return .{ .active = owner };
    }
    pub fn cancel(self: Entry, request: Reservation) !Entry {
        try self.checkReservation(request);
        return .{ .active = self.active };
    }
    /// Ordinary DDL cannot overwrite or retire a publication's reservation.
    /// Even a same-table successor is protected by its exact schema/plan cut.
    pub fn replaceActive(self: Entry, before: ?Owner, after: ?Owner) !Entry {
        try self.validate();
        if (!same(self.active, before)) return error.CatalogGenerationChanged;
        if (self.pending != null) return error.CatalogAlreadyExists;
        const next: Entry = .{ .active = after };
        try next.validate();
        return next;
    }

    const magic = "AFRE01";
    pub const encoded_len = magic.len + 1 + 2 * Owner.encoded_len;
    pub fn encode(self: Entry) ![encoded_len]u8 {
        try self.validate();
        if (self.empty()) return error.InvalidCatalogRecord;
        var bytes: [encoded_len]u8 = @splat(0);
        @memcpy(bytes[0..magic.len], magic);
        bytes[magic.len] = @as(u8, @intFromBool(self.active != null)) | (@as(u8, @intFromBool(self.pending != null)) << 1);
        const start = magic.len + 1;
        if (self.active) |owner| @memcpy(bytes[start..][0..Owner.encoded_len], &(try owner.encode()));
        if (self.pending) |owner| @memcpy(bytes[start + Owner.encoded_len ..][0..Owner.encoded_len], &(try owner.encode()));
        return bytes;
    }
    pub fn decode(bytes: []const u8) !Entry {
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..magic.len], magic)) return error.InvalidCatalogRecord;
        const flags = bytes[magic.len];
        if (flags == 0 or flags > 3) return error.InvalidCatalogRecord;
        const start = magic.len + 1;
        const old = bytes[start..][0..Owner.encoded_len];
        const next = bytes[start + Owner.encoded_len ..][0..Owner.encoded_len];
        if ((flags & 1 == 0 and !std.mem.allEqual(u8, old, 0)) or (flags & 2 == 0 and !std.mem.allEqual(u8, next, 0))) return error.InvalidCatalogRecord;
        const entry: Entry = .{
            .active = if (flags & 1 != 0) try Owner.decode(old) else null,
            .pending = if (flags & 2 != 0) try Owner.decode(next) else null,
        };
        try entry.validate();
        return entry;
    }
};

pub const Key = struct {
    namespace_id: u64,
    name: []const u8,
    pub fn validate(self: Key) !void {
        if (self.namespace_id == 0 or self.name.len == 0 or self.name.len > max_name_bytes or
            std.mem.indexOfScalar(u8, self.name, 0) != null or !std.unicode.utf8ValidateSlice(self.name)) return error.InvalidCatalogName;
    }
    pub fn prefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
        if (group_id == 0) return error.InvalidCatalogRecord;
        if (buf.len < key_prefix.len + 8) return error.NoSpaceLeft;
        @memcpy(buf[0..key_prefix.len], key_prefix);
        std.mem.writeInt(u64, buf[key_prefix.len..][0..8], group_id, .big);
        return buf[0 .. key_prefix.len + 8];
    }
    pub fn allGroupsPrefix() []const u8 {
        return key_prefix;
    }
    pub fn groupFromStorageKey(bytes: []const u8) !?u64 {
        if (!std.mem.startsWith(u8, bytes, key_prefix)) return null;
        if (bytes.len < key_prefix.len + 8) return error.InvalidCatalogRecord;
        const group_id = std.mem.readInt(u64, bytes[key_prefix.len..][0..8], .big);
        _ = try fromStorageKey(bytes, group_id);
        return group_id;
    }
    /// Length-delimited UTF-8 preserves quoted SQL names, including dots and
    /// colons. Namespace IDs already identify their parent database uniquely.
    pub fn storageKeyAlloc(self: Key, a: A, group_id: u64) ![]u8 {
        try self.validate();
        if (group_id == 0) return error.InvalidCatalogRecord;
        const prefix = key_prefix;
        const bytes = try a.alloc(u8, prefix.len + 18 + self.name.len);
        @memcpy(bytes[0..prefix.len], prefix);
        std.mem.writeInt(u64, bytes[prefix.len..][0..8], group_id, .big);
        std.mem.writeInt(u64, bytes[prefix.len + 8 ..][0..8], self.namespace_id, .big);
        std.mem.writeInt(u16, bytes[prefix.len + 16 ..][0..2], @intCast(self.name.len), .big);
        @memcpy(bytes[prefix.len + 18 ..], self.name);
        return bytes;
    }
    /// Borrow the logical name from an authenticated effect key. A relation
    /// record in another group is not an unrelated key: reject cross-group
    /// ownership effects rather than omitting them from replay validation.
    pub fn fromStorageKey(bytes: []const u8, group_id: u64) !?Key {
        if (!std.mem.startsWith(u8, bytes, key_prefix)) return null;
        const tail = bytes[key_prefix.len..];
        if (tail.len < 18 or group_id == 0 or std.mem.readInt(u64, tail[0..8], .big) != group_id) return error.InvalidCatalogRecord;
        const length = std.mem.readInt(u16, tail[16..18], .big);
        if (tail.len != 18 + @as(usize, length)) return error.InvalidCatalogRecord;
        const key: Key = .{ .namespace_id = std.mem.readInt(u64, tail[8..16], .big), .name = tail[18..] };
        try key.validate();
        return key;
    }
};
pub const Claim = struct { key: Key, owner: Owner };
/// Name claims derived from one authoritative table definition and namespace
/// binding. Own only the resulting names, not a second copy of the schema DOM.
/// The digest fences the exact public schema bytes, independently of its
/// numeric layout version. CHECK and FK names are not namespace relations.
pub const TableCut = struct {
    arena: std.heap.ArenaAllocator,
    claims: []const Claim,

    pub const Definition = struct {
        namespace_id: u64,
        table_id: u64,
        name: []const u8,
        schema_json: []const u8,
        phase: Phase = .active,
        publication_id: [16]u8 = @splat(0),
    };
    // A names-only projection skips column definitions, expressions, display
    // metadata and other unrelated schema payloads without building their DOM.
    const SchemaNames = struct {
        version: u32 = 0,
        relational_indexes: ?[]const struct { name: []const u8 } = null,
        unique_constraints: ?[]const struct { name: []const u8, origin: ?[]const u8 = null } = null,
    };
    pub fn init(a: A, definition: Definition) !TableCut {
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const owned = arena.allocator();
        // Temporary parsing storage is reclaimed before returning the cut.
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const schema: SchemaNames = if (definition.schema_json.len == 0)
            .{}
        else
            std.json.parseFromSliceLeaky(SchemaNames, scratch.allocator(), definition.schema_json, .{ .ignore_unknown_fields = true }) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidCatalogRecord,
            };
        const indexes = schema.relational_indexes orelse &.{};
        const uniques = schema.unique_constraints orelse &.{};
        if (indexes.len > max_claims or uniques.len > max_claims) return error.CatalogCommandTooLarge;
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(definition.schema_json, &digest, .{});
        var owner: Owner = .{ .table_id = definition.table_id, .schema_version = schema.version, .schema_digest = digest, .kind = .table, .phase = definition.phase, .publication_id = definition.publication_id };
        try owner.validate();
        var claims: std.ArrayList(Claim) = .empty;
        try append(owned, &claims, definition.namespace_id, definition.name, owner);
        var names: std.StringHashMapUnmanaged(bool) = .empty;
        try names.put(scratch.allocator(), definition.name, true);
        for (indexes) |index| {
            const name = index.name;
            const found = try names.getOrPut(scratch.allocator(), name);
            if (found.found_existing) return error.CatalogAlreadyExists;
            found.value_ptr.* = false;
            owner.kind = .index;
            try append(owned, &claims, definition.namespace_id, name, owner);
        }
        for (uniques) |unique| {
            const name = unique.name;
            const index_owned = if (unique.origin) |origin| std.mem.eql(u8, origin, "index") else false;
            if (unique.origin) |origin| if (!index_owned and !std.mem.eql(u8, origin, "constraint")) return error.InvalidCatalogRecord;
            if (index_owned) {
                // CREATE UNIQUE INDEX has an access declaration and a rule
                // with explicit index provenance: one relation, not two.
                // Never infer provenance from its editable description.
                const mirrored = names.getPtr(name) orelse return error.InvalidCatalogRecord;
                if (mirrored.*) return error.CatalogAlreadyExists;
                mirrored.* = true;
            } else {
                const found = try names.getOrPut(scratch.allocator(), name);
                if (found.found_existing) return error.CatalogAlreadyExists;
                found.value_ptr.* = true;
                owner.kind = .constraint_index;
                try append(owned, &claims, definition.namespace_id, name, owner);
            }
        }
        return .{ .arena = arena, .claims = try claims.toOwnedSlice(owned) };
    }
    pub fn deinit(self: *TableCut) void {
        self.arena.deinit();
        self.* = undefined;
    }
    fn append(a: A, claims: *std.ArrayList(Claim), namespace: u64, name: []const u8, owner: Owner) !void {
        if (claims.items.len == max_claims) return error.CatalogCommandTooLarge;
        const key: Key = .{ .namespace_id = namespace, .name = name };
        try key.validate();
        try claims.append(a, .{ .key = .{ .namespace_id = namespace, .name = try a.dupe(u8, name) }, .owner = owner });
    }
};

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

/// Owned CAS over the whole active/reserved cut. Validate every affected name
/// before the first write. The caller supplies ONE pinned metadata transaction
/// and must abort it on any error; this planner never commits or retries.
/// CAS is not publication authority: the producer must validate its immutable
/// plan, membership capability and lifecycle phase before constructing a cut.
pub const EntryPlan = struct {
    pub const Change = struct { key: Key, before: Entry, after: Entry };
    arena: std.heap.ArenaAllocator,
    changes: []const Change,
    pub fn init(a: A, changes: []const Change) !EntryPlan {
        if (changes.len > max_claims) return error.CatalogCommandTooLarge;
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const owned = arena.allocator();
        var seen: std.HashMapUnmanaged(Key, void, Context, 80) = .empty;
        try seen.ensureTotalCapacity(owned, @intCast(changes.len));
        const copy = try owned.alloc(Change, changes.len);
        for (changes, copy) |change, *next| {
            try change.key.validate();
            try change.before.validate();
            try change.after.validate();
            const key: Key = .{ .namespace_id = change.key.namespace_id, .name = try owned.dupe(u8, change.key.name) };
            if (seen.getOrPutAssumeCapacity(key).found_existing) return error.InvalidCatalogRecord;
            next.* = .{ .key = key, .before = change.before, .after = change.after };
        }
        return .{ .arena = arena, .changes = copy };
    }
    pub fn deinit(self: *EntryPlan) void {
        self.arena.deinit();
        self.* = undefined;
    }
    fn observed(reader: anytype, key: Key) !Entry {
        const current = (try reader.getEntry(key)) orelse return .{};
        try current.validate();
        if (current.empty()) return error.InvalidCatalogRecord;
        return current;
    }
    pub fn validate(self: *const EntryPlan, reader: anytype) !void {
        for (self.changes) |change| if (!(try observed(reader, change.key)).eql(change.before)) return error.CatalogGenerationChanged;
    }
    pub fn apply(self: *const EntryPlan, txn: anytype) !void {
        try self.validate(txn);
        for (self.changes) |change| {
            if (change.before.eql(change.after)) continue;
            if (change.after.empty()) try txn.deleteEntry(change.key) else try txn.putEntry(change.key, change.after);
        }
    }
    /// Authenticated replay verifies sender effects, never fills missing rows
    /// or repairs a mismatched generation under a new producer identity.
    pub fn verifyPublished(self: *const EntryPlan, reader: anytype) !void {
        for (self.changes) |change| if (!(try observed(reader, change.key)).eql(change.after)) return error.CatalogGenerationChanged;
    }
};

/// Point adapter over the metadata owner's existing read/write transaction.
/// No second transaction or independent namespace commit can be opened here.
/// Keyspace binds entries to the caller's immutable generation/root. It owns
/// no transaction and returns an owned physical key from keyAlloc(alloc, key).
pub fn EntryStore(comptime Txn: type, comptime Keyspace: type) type {
    return struct {
        txn: *Txn,
        alloc: A,
        keys: Keyspace,
        pub fn getEntry(self: *@This(), key: Key) !?Entry {
            try key.validate();
            const encoded_key = try self.keys.keyAlloc(self.alloc, key);
            defer self.alloc.free(encoded_key);
            const bytes = self.txn.get(encoded_key) catch |err| {
                if (err == error.NotFound) return null;
                return err;
            };
            return try Entry.decode(bytes);
        }
        pub fn putEntry(self: *@This(), key: Key, entry: Entry) !void {
            try key.validate();
            const encoded = try entry.encode();
            const encoded_key = try self.keys.keyAlloc(self.alloc, key);
            defer self.alloc.free(encoded_key);
            try self.txn.put(encoded_key, &encoded);
        }
        pub fn deleteEntry(self: *@This(), key: Key) !void {
            try key.validate();
            const encoded_key = try self.keys.keyAlloc(self.alloc, key);
            defer self.alloc.free(encoded_key);
            try self.txn.delete(encoded_key);
        }
    };
}

/// Current single-owner registry; activation of compound entries is a separate
/// capability/source-completeness decision, not a fallback decoder here.
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
    /// Verify a received final cut without synthesizing missing effects. The
    /// sender's authenticated schema and ownership rows must agree exactly;
    /// silently repairing replay would hide incompatible producer behavior.
    pub fn verifyPublished(self: *const Plan, reader: anytype) !void {
        for (self.after) |claim| {
            const current = (try reader.getClaim(claim.key)) orelse return error.CatalogGenerationChanged;
            if (!current.eql(claim.owner)) return error.CatalogGenerationChanged;
        }
        for (self.before) |claim| if (!self.after_by_name.contains(claim.key)) {
            if (try reader.getClaim(claim.key)) |_| return error.CatalogGenerationChanged;
        };
    }
};

/// One metadata transaction may change a table's schema, binding and
/// publication phase through separate producers. Keep its original cut
/// immutable and replace only its proposed cut, independently of the store's
/// read-your-writes behavior. Compile all tables before the first registry
/// mutation so swaps and cross-table collisions use the final atomic cut.
pub const Publication = struct {
    alloc: A,
    changes: std.AutoHashMapUnmanaged(u64, Plan) = .empty,
    before_count: usize = 0,
    after_count: usize = 0,

    pub fn init(a: A) Publication {
        return .{ .alloc = a };
    }
    pub fn deinit(self: *Publication) void {
        var it = self.changes.valueIterator();
        while (it.next()) |plan| plan.deinit();
        self.changes.deinit(self.alloc);
        self.* = undefined;
    }
    /// Borrowed until the next stage for this table. A missing entry differs
    /// from a staged deletion, whose proposed cut is present but empty.
    pub fn pending(self: *const Publication, table_id: u64) ?[]const Claim {
        const plan = self.changes.getPtr(table_id) orelse return null;
        return plan.after;
    }
    pub fn stage(self: *Publication, table_id: u64, before: []const Claim, after: []const Claim) !void {
        if (table_id == 0) return error.InvalidCatalogRecord;
        if (before.len > max_claims or after.len > max_claims) return error.CatalogCommandTooLarge;
        for (before) |claim| if (claim.owner.table_id != table_id) return error.InvalidCatalogRecord;
        for (after) |claim| if (claim.owner.table_id != table_id) return error.InvalidCatalogRecord;
        const prior = self.changes.getPtr(table_id);
        const before_count = self.before_count - (if (prior) |p| p.before.len else @as(usize, 0));
        const after_count = self.after_count - (if (prior) |p| p.after.len else @as(usize, 0));
        if (before.len > max_claims - before_count or after.len > max_claims - after_count or
            (prior == null and self.changes.count() >= max_claims)) return error.CatalogCommandTooLarge;
        var next = try Plan.init(self.alloc, before, after);
        errdefer next.deinit();
        if (prior) |p| {
            if (p.before.len != next.before.len) return error.CatalogGenerationChanged;
            for (p.before) |claim| {
                const observed = next.before_by_name.get(claim.key) orelse return error.CatalogGenerationChanged;
                if (!observed.eql(claim.owner)) return error.CatalogGenerationChanged;
            }
            // A failed replacement never destroys the preceding pending cut.
            // Its independent arena also bounds memory across many updates.
            p.deinit();
            p.* = next;
        } else try self.changes.put(self.alloc, table_id, next);
        self.before_count = before_count + before.len;
        self.after_count = after_count + after.len;
    }
    pub fn compile(self: *const Publication) !Plan {
        const before = try self.alloc.alloc(Claim, self.before_count);
        defer self.alloc.free(before);
        const after = try self.alloc.alloc(Claim, self.after_count);
        defer self.alloc.free(after);
        var bi: usize = 0;
        var ai: usize = 0;
        var it = self.changes.valueIterator();
        while (it.next()) |plan| {
            @memcpy(before[bi..][0..plan.before.len], plan.before);
            @memcpy(after[ai..][0..plan.after.len], plan.after);
            bi += plan.before.len;
            ai += plan.after.len;
        }
        return Plan.init(self.alloc, before, after);
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

test "catalog relation received cuts verify without repairing stale or omitted effects" {
    const a = std.testing.allocator;
    const old = testClaim(2, "old", 7, 1, .index);
    const next = testClaim(2, "next", 7, 2, .index);
    var plan = try Plan.init(a, &.{old}, &.{next});
    defer plan.deinit();
    var store: TestStore = .{};
    defer store.rows.deinit(a);
    try store.putClaim(old.key, old.owner);
    const writes = store.writes;
    try std.testing.expectError(error.CatalogGenerationChanged, plan.verifyPublished(&store));
    try std.testing.expectEqual(writes, store.writes);
    try store.putClaim(next.key, next.owner);
    try std.testing.expectError(error.CatalogGenerationChanged, plan.verifyPublished(&store));
    try store.deleteClaim(old.key);
    try plan.verifyPublished(&store);
    var stale = next.owner;
    stale.schema_digest[0] ^= 1;
    try store.putClaim(next.key, stale);
    try std.testing.expectError(error.CatalogGenerationChanged, plan.verifyPublished(&store));
}

test "catalog relation effect keys decode canonically and reject cross-group records" {
    const a = std.testing.allocator;
    const key: Key = .{ .namespace_id = 2, .name = "quoted.index:name" };
    const encoded = try key.storageKeyAlloc(a, 41);
    defer a.free(encoded);
    const decoded = (try Key.fromStorageKey(encoded, 41)).?;
    try std.testing.expect(Context.eql(.{}, key, decoded));
    try std.testing.expectEqual(@as(?u64, 41), try Key.groupFromStorageKey(encoded));
    var prefix_buf: [160]u8 = undefined;
    try std.testing.expect(std.mem.startsWith(u8, encoded, try Key.prefixForGroup(&prefix_buf, 41)));
    try std.testing.expectError(error.NoSpaceLeft, Key.prefixForGroup(prefix_buf[0..1], 41));
    try std.testing.expectError(error.InvalidCatalogRecord, Key.groupFromStorageKey(Key.allGroupsPrefix()));
    try std.testing.expect((try Key.groupFromStorageKey("unrelated")) == null);
    try std.testing.expectError(error.InvalidCatalogRecord, Key.fromStorageKey(encoded, 42));
    try std.testing.expectError(error.InvalidCatalogRecord, Key.fromStorageKey(encoded[0 .. encoded.len - 1], 41));
    try std.testing.expectError(error.InvalidCatalogRecord, Key.fromStorageKey(key_prefix, 41));
    try std.testing.expect((try Key.fromStorageKey("unrelated", 41)) == null);
    encoded[key_prefix.len + 17] += 1;
    try std.testing.expectError(error.InvalidCatalogRecord, Key.fromStorageKey(encoded, 41));
}
fn testClaim(namespace: u64, name: []const u8, table: u64, version: u32, kind: Kind) Claim {
    return .{ .key = .{ .namespace_id = namespace, .name = name }, .owner = .{ .table_id = table, .schema_version = version, .schema_digest = @splat(@intCast(version)), .kind = kind } };
}

test "catalog table cuts preserve constraint and index provenance without duplicating unique index relations" {
    const a = std.testing.allocator;
    const schema =
        \\{"version":9,"relational_indexes":[{"name":"ordinary","description":"SQL UNIQUE INDEX"},{"name":"unique","description":"operator edited"}],"unique_constraints":[{"name":"pk","primary":true},{"name":"named","origin":"constraint"},{"name":"unique","origin":"index"}],"checks":[{"name":"ordinary"}],"foreign_keys":[{"name":"unique"}]}
    ;
    var cut = try TableCut.init(a, .{ .namespace_id = 2, .table_id = 7, .name = "quoted.table:名", .schema_json = schema, .phase = .reserved, .publication_id = @splat(3) });
    defer cut.deinit();
    try std.testing.expectEqual(@as(usize, 5), cut.claims.len);
    const kinds = [_]Kind{ .table, .index, .index, .constraint_index, .constraint_index };
    const names = [_][]const u8{ "quoted.table:名", "ordinary", "unique", "pk", "named" };
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(schema, &digest, .{});
    for (cut.claims, kinds, names) |claim, kind, name| {
        try std.testing.expectEqualStrings(name, claim.key.name);
        try std.testing.expect(claim.owner.eql(.{ .table_id = 7, .schema_version = 9, .schema_digest = digest, .kind = kind, .phase = .reserved, .publication_id = @splat(3) }));
        try std.testing.expectEqual(@as(u64, 2), claim.key.namespace_id);
    }
    var document = try TableCut.init(a, .{ .namespace_id = 3, .table_id = 8, .name = "document", .schema_json = "" });
    defer document.deinit();
    try std.testing.expectEqual(@as(usize, 1), document.claims.len);
    try std.testing.expectEqual(@as(u32, 0), document.claims[0].owner.schema_version);
}

test "catalog table cuts skip unrelated schema payloads under bounded allocator headroom" {
    const a = std.testing.allocator;
    const payload = try a.alloc(u8, 128 * 1024);
    defer a.free(payload);
    @memset(payload, 'x');
    const schema = try std.fmt.allocPrint(a, "{{\"version\":1,\"document_schemas\":{{\"row\":{{\"schema\":{{\"description\":\"{s}\"}}}}}},\"relational_indexes\":[{{\"name\":\"idx\",\"description\":\"{s}\"}}]}}", .{ payload, payload });
    defer a.free(schema);
    var storage: [16 * 1024]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&storage);
    var cut = try TableCut.init(fixed.allocator(), .{ .namespace_id = 2, .table_id = 7, .name = "items", .schema_json = schema });
    defer cut.deinit();
    try std.testing.expectEqual(@as(usize, 2), cut.claims.len);
    try std.testing.expectEqualStrings("idx", cut.claims[1].key.name);
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(schema, &digest, .{});
    try std.testing.expectEqualSlices(u8, &digest, &cut.claims[1].owner.schema_digest);
}

test "catalog table cuts reject ambiguous ownership and malformed declarations" {
    const a = std.testing.allocator;
    const Case = struct { schema: []const u8, expected: anyerror };
    for ([_]Case{
        .{ .schema = "[]", .expected = error.InvalidCatalogRecord },
        .{ .schema = "{\"version\":null}", .expected = error.InvalidCatalogRecord },
        .{ .schema = "{\"version\":-1}", .expected = error.InvalidCatalogRecord },
        .{ .schema = "{\"version\":4294967296}", .expected = error.InvalidCatalogRecord },
        .{ .schema = "{\"relational_indexes\":{}}", .expected = error.InvalidCatalogRecord },
        .{ .schema = "{\"unique_constraints\":[null]}", .expected = error.InvalidCatalogRecord },
        .{ .schema = "{\"relational_indexes\":[{\"name\":\"items\"}]}", .expected = error.CatalogAlreadyExists },
        .{ .schema = "{\"relational_indexes\":[{\"name\":\"idx\"},{\"name\":\"idx\"}]}", .expected = error.CatalogAlreadyExists },
        .{ .schema = "{\"unique_constraints\":[{\"name\":\"idx\",\"origin\":\"index\"}]}", .expected = error.InvalidCatalogRecord },
        .{ .schema = "{\"unique_constraints\":[{\"name\":\"idx\",\"origin\":false}]}", .expected = error.InvalidCatalogRecord },
        .{ .schema = "{\"unique_constraints\":[{\"name\":\"idx\",\"origin\":\"future\"}]}", .expected = error.InvalidCatalogRecord },
        .{ .schema = "{\"relational_indexes\":[{\"name\":\"idx\"}],\"unique_constraints\":[{\"name\":\"idx\"}]}", .expected = error.CatalogAlreadyExists },
        .{ .schema = "{\"relational_indexes\":[{\"name\":\"idx\"}],\"unique_constraints\":[{\"name\":\"idx\",\"origin\":\"index\"},{\"name\":\"idx\",\"origin\":\"index\"}]}", .expected = error.CatalogAlreadyExists },
    }) |case| try std.testing.expectError(case.expected, TableCut.init(a, .{ .namespace_id = 2, .table_id = 7, .name = "items", .schema_json = case.schema }));
    try std.testing.expectError(error.InvalidCatalogName, TableCut.init(a, .{ .namespace_id = 2, .table_id = 7, .name = "items", .schema_json = "{\"unique_constraints\":[{\"name\":\"\"}]}" }));
}

test "catalog table cuts own source names after schema retirement and unwind allocation failures" {
    const Probe = struct {
        fn run(a: A) !void {
            var cut = blk: {
                const schema = try a.dupe(u8, "{\"version\":4,\"relational_indexes\":[{\"name\":\"idx\"}],\"unique_constraints\":[{\"name\":\"idx\",\"origin\":\"index\"}]}");
                defer a.free(schema);
                const name = try a.dupe(u8, "owned_table");
                defer a.free(name);
                break :blk try TableCut.init(a, .{ .namespace_id = 2, .table_id = 7, .name = name, .schema_json = schema });
            };
            defer cut.deinit();
            try std.testing.expectEqualStrings("owned_table", cut.claims[0].key.name);
            try std.testing.expectEqualStrings("idx", cut.claims[1].key.name);
            var publication = Publication.init(a);
            defer publication.deinit();
            try publication.stage(7, &.{}, cut.claims);
            var plan = try publication.compile();
            defer plan.deinit();
            try std.testing.expectEqual(@as(usize, 2), plan.after.len);
        }
    };
    try Probe.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}

test "catalog relation publication coalesces schemas and bindings before writing the final atomic cut" {
    const a = std.testing.allocator;
    const first = testClaim(2, "first", 7, 1, .table);
    const second = testClaim(2, "second", 8, 1, .table);
    const interim = testClaim(2, "temporary", 7, 2, .table);
    const next_first = testClaim(2, "second", 7, 3, .table);
    const next_second = testClaim(2, "first", 8, 2, .table);
    var publication = Publication.init(a);
    defer publication.deinit();
    try std.testing.expect(publication.pending(7) == null);
    try publication.stage(7, &.{first}, &.{interim});
    try publication.stage(7, &.{first}, &.{next_first});
    try publication.stage(8, &.{second}, &.{next_second});
    try std.testing.expectEqual(@as(usize, 2), publication.before_count);
    try std.testing.expectEqual(@as(usize, 2), publication.after_count);
    var plan = try publication.compile();
    defer plan.deinit();
    var store: TestStore = .{};
    defer store.rows.deinit(a);
    try store.rows.appendSlice(a, &.{ first, second });
    try std.testing.expectEqual(@as(usize, 0), store.writes);
    try plan.apply(&store);
    try std.testing.expectEqual(@as(usize, 2), store.calls);
    try std.testing.expectEqual(@as(usize, 2), store.writes);
    try std.testing.expect((try store.getClaim(first.key)).?.eql(next_second.owner));
    try std.testing.expect((try store.getClaim(second.key)).?.eql(next_first.owner));
    try std.testing.expect((try store.getClaim(interim.key)) == null);
    // Repeated producers must retain the original read cut, not pretend the
    // transaction's pending writes have become a new committed generation.
    try std.testing.expectError(error.CatalogGenerationChanged, publication.stage(7, &.{interim}, &.{}));
    try std.testing.expect(publication.pending(7).?[0].owner.eql(next_first.owner));
    try std.testing.expectError(error.InvalidCatalogRecord, publication.stage(7, &.{first}, &.{next_second}));
    try publication.stage(7, &.{first}, &.{});
    try std.testing.expectEqual(@as(usize, 0), publication.pending(7).?.len);
}

test "catalog relation publication detects cross-table collisions before registry mutations" {
    const a = std.testing.allocator;
    var publication = Publication.init(a);
    defer publication.deinit();
    try publication.stage(7, &.{}, &.{testClaim(2, "shared", 7, 1, .index)});
    try publication.stage(8, &.{}, &.{testClaim(2, "shared", 8, 1, .constraint_index)});
    try std.testing.expectError(error.CatalogAlreadyExists, publication.compile());
    try publication.stage(8, &.{}, &.{testClaim(3, "shared", 8, 1, .constraint_index)});
    var plan = try publication.compile();
    defer plan.deinit();
    try std.testing.expectEqual(@as(usize, 2), plan.after.len);
}

test "catalog relation publication bounds the aggregate cut and retains publication fences" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const claims = try arena.allocator().alloc(Claim, max_claims);
    for (claims, 0..) |*claim, i| claim.* = testClaim(2, try std.fmt.allocPrint(arena.allocator(), "index_{d}", .{i}), 7, 1, .index);
    var publication = Publication.init(a);
    defer publication.deinit();
    try publication.stage(7, claims, claims);
    const other = testClaim(2, "other", 8, 1, .table);
    try std.testing.expectError(error.CatalogCommandTooLarge, publication.stage(8, &.{}, &.{other}));
    try std.testing.expectError(error.CatalogCommandTooLarge, publication.stage(8, &.{other}, &.{}));
    try std.testing.expect(publication.pending(8) == null);
    try std.testing.expectEqual(max_claims, publication.after_count);
    try publication.stage(7, claims, &.{});
    try publication.stage(8, &.{}, &.{other});
    try std.testing.expectEqual(@as(usize, 1), publication.after_count);
    const reserved = Claim{
        .key = .{ .namespace_id = 3, .name = "reserved" },
        .owner = .{ .table_id = 9, .schema_version = 1, .schema_digest = @splat(1), .kind = .constraint_index, .phase = .reserved, .publication_id = @splat(2) },
    };
    var fences = Publication.init(a);
    defer fences.deinit();
    try fences.stage(9, &.{reserved}, &.{reserved});
    var stale = reserved;
    stale.owner.publication_id = @splat(3);
    try std.testing.expectError(error.CatalogGenerationChanged, fences.stage(9, &.{stale}, &.{}));
    stale = reserved;
    stale.owner.phase = .active;
    try std.testing.expectError(error.CatalogGenerationChanged, fences.stage(9, &.{stale}, &.{}));
    stale = reserved;
    stale.owner.schema_digest = @splat(4);
    try std.testing.expectError(error.CatalogGenerationChanged, fences.stage(9, &.{stale}, &.{}));
    try std.testing.expect(fences.pending(9).?[0].owner.eql(reserved.owner));
}

test "catalog relation publication owns repeated cuts and unwinds allocation failures" {
    const Probe = struct {
        fn run(a: A) !void {
            var publication = Publication.init(a);
            defer publication.deinit();
            const old = testClaim(2, "old", 7, 1, .table);
            const next = testClaim(3, "renamed", 7, 2, .table);
            try publication.stage(7, &.{old}, &.{old});
            publication.stage(7, &.{old}, &.{next}) catch |err| {
                try std.testing.expect(publication.pending(7).?[0].owner.eql(old.owner));
                try std.testing.expectEqual(@as(usize, 1), publication.after_count);
                return err;
            };
            var plan = try publication.compile();
            defer plan.deinit();
            // A compiled cut must outlive subsequent producer replacements.
            try publication.stage(7, &.{old}, &.{});
            try std.testing.expectEqualStrings("renamed", plan.after[0].key.name);
            try std.testing.expect(plan.after[0].owner.eql(next.owner));
            try std.testing.expectEqual(@as(usize, 0), publication.after_count);
        }
    };
    try Probe.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
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

test "catalog relation entry reserves successors without exposing them and fences stale publication" {
    const old = testClaim(2, "rows", 7, 1, .table).owner;
    var successor = testClaim(2, "rows", 8, 2, .table).owner;
    successor.phase = .reserved;
    successor.publication_id = @splat(1);
    const request: Entry.Reservation = .{ .predecessor = old, .successor = successor };
    const active: Entry = .{ .active = old };
    const reserved = try active.reserve(request);
    try std.testing.expect(reserved.active.?.eql(old));
    try std.testing.expect(reserved.pending.?.eql(successor));
    try std.testing.expect(reserved.eql(try reserved.reserve(request)));
    var competing = request;
    competing.successor.schema_digest[0] ^= 1;
    try std.testing.expectError(error.CatalogAlreadyExists, reserved.reserve(competing));
    try std.testing.expectError(error.CatalogGenerationChanged, reserved.publish(competing));
    try std.testing.expectError(error.CatalogGenerationChanged, reserved.cancel(competing));
    try std.testing.expectError(error.CatalogAlreadyExists, reserved.replaceActive(old, null));
    try std.testing.expect(active.eql(try reserved.cancel(request)));
    try std.testing.expectError(error.CatalogGenerationChanged, active.publish(request));
    const published = try reserved.publish(request);
    try std.testing.expect(published.pending == null);
    try std.testing.expectEqual(@as(u64, 8), published.active.?.table_id);
    try std.testing.expectEqual(Phase.active, published.active.?.phase);
    try std.testing.expectEqualSlices(u8, &successor.publication_id, &published.active.?.publication_id);
    try std.testing.expectError(error.CatalogGenerationChanged, published.reserve(request));
    try std.testing.expectError(error.CatalogGenerationChanged, published.cancel(request));
    var wrong_predecessor = request;
    wrong_predecessor.predecessor.?.schema_digest[0] ^= 1;
    try std.testing.expectError(error.CatalogGenerationChanged, active.reserve(wrong_predecessor));
    const fresh: Entry.Reservation = .{ .successor = successor };
    const pending_only = try (Entry{}).reserve(fresh);
    try std.testing.expect(pending_only.active == null);
    try std.testing.expect((try pending_only.cancel(fresh)).empty());
    try std.testing.expect((try (Entry{}).replaceActive(null, old)).eql(active));
}

test "catalog relation entry codec is canonical and rejects incomplete or impossible owner cuts" {
    const old = testClaim(2, "rows", 7, 1, .table).owner;
    var successor = old;
    successor.phase = .reserved;
    successor.publication_id = @splat(1);
    for ([_]Entry{ .{ .active = old }, .{ .pending = successor }, .{ .active = old, .pending = successor } }) |entry| {
        const encoded = try entry.encode();
        try std.testing.expect(entry.eql(try Entry.decode(&encoded)));
        for (0..encoded.len) |length| try std.testing.expectError(error.InvalidCatalogRecord, Entry.decode(encoded[0..length]));
    }
    try std.testing.expectError(error.InvalidCatalogRecord, (Entry{}).encode());
    try std.testing.expectError(error.InvalidCatalogRecord, (Entry{ .active = successor }).encode());
    try std.testing.expectError(error.InvalidCatalogRecord, (Entry{ .pending = old }).encode());
    var encoded = try (Entry{ .active = old }).encode();
    encoded[Entry.magic.len + 1 + Owner.encoded_len] = 1;
    try std.testing.expectError(error.InvalidCatalogRecord, Entry.decode(&encoded));
    encoded = try (Entry{ .pending = successor }).encode();
    encoded[Entry.magic.len] = 0xff;
    try std.testing.expectError(error.InvalidCatalogRecord, Entry.decode(&encoded));
    encoded = try (Entry{ .pending = successor }).encode();
    encoded[encoded.len - 1] = @backingInt(Phase.active);
    try std.testing.expectError(error.InvalidCatalogRecord, Entry.decode(&encoded));
    successor.publication_id = @splat(0);
    try std.testing.expectError(error.InvalidCatalogRecord, (Entry{}).reserve(.{ .successor = successor }));
}

test "catalog relation entry plans validate the complete cut before writes and never repair replay" {
    const a = std.testing.allocator;
    const Fake = struct {
        rows: [2]?Entry,
        reads: usize = 0,
        writes: usize = 0,
        fail_read: ?usize = null,
        fn index(key: Key) usize {
            return if (std.mem.eql(u8, key.name, "rows")) 0 else 1;
        }
        pub fn getEntry(self: *@This(), key: Key) !?Entry {
            self.reads += 1;
            if (self.fail_read == self.reads) return error.InjectedReadFailure;
            return self.rows[index(key)];
        }
        pub fn putEntry(self: *@This(), key: Key, entry: Entry) !void {
            self.writes += 1;
            self.rows[index(key)] = entry;
        }
        pub fn deleteEntry(self: *@This(), key: Key) !void {
            self.writes += 1;
            self.rows[index(key)] = null;
        }
    };
    const old = testClaim(2, "rows", 7, 1, .table).owner;
    var successor = testClaim(2, "rows", 8, 2, .table).owner;
    successor.phase = .reserved;
    successor.publication_id = @splat(1);
    const prior: Entry = .{ .active = old };
    const reserved = try prior.reserve(.{ .predecessor = old, .successor = successor });
    const index_reserved = try (Entry{}).reserve(.{ .successor = successor });
    const changes = [_]EntryPlan.Change{
        .{ .key = .{ .namespace_id = 2, .name = "rows" }, .before = prior, .after = reserved },
        .{ .key = .{ .namespace_id = 2, .name = "idx" }, .before = .{}, .after = index_reserved },
    };
    var plan = try EntryPlan.init(a, &changes);
    defer plan.deinit();
    var store: Fake = .{ .rows = .{ prior, null }, .fail_read = 2 };
    try std.testing.expectError(error.InjectedReadFailure, plan.apply(&store));
    try std.testing.expectEqual(@as(usize, 0), store.writes);
    store.fail_read = null;
    store.reads = 0;
    try plan.apply(&store);
    try std.testing.expectEqual(@as(usize, 2), store.reads);
    try std.testing.expectEqual(@as(usize, 2), store.writes);
    try plan.verifyPublished(&store);
    try std.testing.expectError(error.CatalogGenerationChanged, plan.apply(&store));
    store.rows[1] = null;
    try std.testing.expectError(error.CatalogGenerationChanged, plan.verifyPublished(&store));
    try std.testing.expectEqual(@as(usize, 2), store.writes);
    store.rows[1] = index_reserved;
    var cancel = try EntryPlan.init(a, &.{
        .{ .key = changes[0].key, .before = reserved, .after = prior },
        .{ .key = changes[1].key, .before = index_reserved, .after = .{} },
    });
    defer cancel.deinit();
    try cancel.apply(&store);
    try cancel.verifyPublished(&store);
    try std.testing.expect(store.rows[0].?.eql(prior));
    try std.testing.expect(store.rows[1] == null);
}

test "catalog relation entry plan ownership survives input mutation and allocation faults" {
    const a = std.testing.allocator;
    var name = [_]u8{ 'r', 'o', 'w', 's' };
    const before: Entry = .{ .active = testClaim(2, "rows", 7, 1, .table).owner };
    const changes = [_]EntryPlan.Change{.{ .key = .{ .namespace_id = 2, .name = &name }, .before = before, .after = .{} }};
    const T = struct {
        fn prepare(alloc: A, input: []const EntryPlan.Change) !void {
            var plan = try EntryPlan.init(alloc, input);
            defer plan.deinit();
        }
    };
    var failing = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(failing.allocator(), T.prepare, .{&changes});
    var plan = try EntryPlan.init(a, &changes);
    defer plan.deinit();
    name[0] = 'x';
    try std.testing.expectEqualStrings("rows", plan.changes[0].key.name);
    try std.testing.expectError(error.InvalidCatalogRecord, EntryPlan.init(a, &.{ changes[0], changes[0] }));
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
