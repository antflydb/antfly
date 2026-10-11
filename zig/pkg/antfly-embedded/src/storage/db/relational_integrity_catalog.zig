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

//! Durable constraint identities, separate from mutable activation coverage.
//! The table incarnation is replicated metadata, never locally random. A
//! monotonically increasing allocation counter prevents drop/readd from
//! reviving old globally routed claims or child references.
const std = @import("std");
const integrity = @import("relational_integrity_contract.zig");
const Allocator = std.mem.Allocator;
pub const key = "\x00\x00__metadata__:relational_integrity_catalog";
pub const max_definitions = 1024;
pub const max_catalog_bytes = 1024 * 1024;
pub const Kind = enum(u8) { unique = 1, foreign_key = 2 };
pub const Definition = struct { kind: Kind, name: []const u8, fingerprint: integrity.Digest, payload: []const u8 = "" };
pub const Binding = struct {
    definition: Definition,
    generation: integrity.Generation,
    allocation: u64,
    retired: bool = false,
};

pub fn incarnationFromTableId(table_id: u64) !integrity.Generation {
    if (table_id == 0) return error.CoordinatedConstraintsRequireTableIdentity;
    var result: integrity.Generation = undefined;
    @memcpy(result[0..8], "AFTABLE1");
    std.mem.writeInt(u64, result[8..16], table_id, .little);
    return result;
}

pub const Catalog = struct {
    arena: std.heap.ArenaAllocator,
    incarnation: integrity.Generation,
    schema_version: u32,
    schema_digest: integrity.Digest,
    /// CHECKs have no routed claims/references or retirement records. Their
    /// logical identity still participates in the shared activation fence.
    checks_digest: integrity.Digest = @splat(0),
    next_generation: u64,
    bindings: []const Binding,
    pub fn deinit(self: *Catalog) void {
        self.arena.deinit();
        self.* = undefined;
    }
    pub fn find(self: Catalog, kind: Kind, name: []const u8) ?Binding {
        var lo: usize = 0;
        var hi = self.bindings.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const entry = self.bindings[mid];
            const order = if (entry.definition.kind == kind) std.mem.order(u8, entry.definition.name, name) else std.math.order(@backingInt(entry.definition.kind), @backingInt(kind));
            switch (order) {
                .lt => lo = mid + 1,
                .gt => hi = mid,
                .eq => hi = mid,
            }
        }
        if (lo == self.bindings.len) return null;
        const entry = self.bindings[lo];
        return if (!entry.retired and entry.definition.kind == kind and std.mem.eql(u8, entry.definition.name, name)) entry else null;
    }
    /// Historical descriptors stay pinned by generation until distributed
    /// retirement proves that no references, claims or action jobs need them.
    pub fn findGeneration(self: Catalog, id: integrity.Generation) ?Binding {
        for (self.bindings) |entry| if (std.mem.eql(u8, &entry.generation, &id)) return entry;
        return null;
    }
};

fn digest(bytes: []const u8) integrity.Digest {
    var result: integrity.Digest = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}

fn generation(incarnation: integrity.Generation, allocation: u64) integrity.Generation {
    var state = std.crypto.hash.Blake3.init(.{});
    state.update("antfly constraint generation v1");
    state.update(&incarnation);
    var count: [8]u8 = undefined;
    std.mem.writeInt(u64, &count, allocation, .little);
    state.update(&count);
    var result: integrity.Digest = undefined;
    state.final(&result);
    return result[0..16].*;
}

const header_len = 100;
const entry_header_len = 64;

pub const PrivateIndexAllocation = struct { allocation: u64, generation: integrity.Generation };

/// Extract the one allocation burned by an exact private index reservation.
/// This verifies bounded catalog metadata without allocating or decoding its
/// definitions. It is not publication or
/// retirement authority. Caller must independently require the durable owner
/// job, reserved catalogs and source fence in the committing snapshot.
pub fn privateIndexAllocationFromReservation(bytes: []const u8, expected: integrity.Digest, table_id: u64) !PrivateIndexAllocation {
    if (bytes.len < header_len + 32 or bytes.len > max_catalog_bytes or
        !std.mem.eql(u8, bytes[0..4], "AIC2") or !std.mem.eql(u8, &digest(bytes), &expected)) return error.IntegrityCatalogChanged;
    const incarnation = try incarnationFromTableId(table_id);
    if (!std.mem.eql(u8, bytes[4..20], &incarnation) or
        !std.mem.eql(u8, bytes[bytes.len - 32 ..], &digest(bytes[0 .. bytes.len - 32])) or
        std.mem.readInt(u32, bytes[64..68], .little) > max_definitions) return error.InvalidIntegrityCatalog;
    const next = std.mem.readInt(u64, bytes[56..64], .little);
    if (next <= 1) return error.InvalidIntegrityCatalog;
    return .{ .allocation = next - 1, .generation = generation(incarnation, next - 1) };
}

pub fn privateIndexGeneration(table_id: u64, allocation: u64) !integrity.Generation {
    if (allocation == 0) return error.InvalidIntegrityCatalog;
    return generation(try incarnationFromTableId(table_id), allocation);
}

pub fn decode(alloc: Allocator, bytes: []const u8) !Catalog {
    if (bytes.len < header_len + 32 or bytes.len > max_catalog_bytes or !std.mem.eql(u8, bytes[0..4], "AIC2") or
        !std.mem.eql(u8, bytes[bytes.len - 32 ..], &digest(bytes[0 .. bytes.len - 32]))) return error.InvalidIntegrityCatalog;
    const count = std.mem.readInt(u32, bytes[64..68], .little);
    const next_generation = std.mem.readInt(u64, bytes[56..64], .little);
    const incarnation = bytes[4..20].*;
    if (count > max_definitions or next_generation == 0 or std.mem.allEqual(u8, &incarnation, 0)) return error.InvalidIntegrityCatalog;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const bindings = try owned.alloc(Binding, count);
    var offset: usize = header_len;
    const end = bytes.len - 32;
    var allocations = std.AutoHashMapUnmanaged(u64, void).empty;
    defer allocations.deinit(alloc);
    for (bindings, 0..) |*binding, index| {
        if (end - offset < entry_header_len) return error.InvalidIntegrityCatalog;
        const body = bytes[offset..][0..entry_header_len];
        const kind: Kind = switch (body[0]) {
            1 => .unique,
            2 => .foreign_key,
            else => return error.InvalidIntegrityCatalog,
        };
        const name_len = std.mem.readInt(u16, body[1..3], .little);
        const allocation = std.mem.readInt(u64, body[3..11], .little);
        const payload_len = std.mem.readInt(u32, body[60..64], .little);
        if (name_len == 0 or name_len > 256 or allocation == 0 or allocation >= next_generation) return error.InvalidIntegrityCatalog;
        if (count <= 64) {
            for (bindings[0..index]) |previous| if (previous.allocation == allocation) return error.InvalidIntegrityCatalog;
        } else if ((try allocations.getOrPut(alloc, allocation)).found_existing) return error.InvalidIntegrityCatalog;
        offset += entry_header_len;
        if (name_len > end - offset or payload_len > end - offset - name_len or body[59] > 1) return error.InvalidIntegrityCatalog;
        const name = bytes[offset..][0..name_len];
        if (!std.unicode.utf8ValidateSlice(name)) return error.InvalidIntegrityCatalog;
        binding.* = .{ .definition = .{ .kind = kind, .name = try owned.dupe(u8, name), .fingerprint = body[27..59].*, .payload = try owned.dupe(u8, bytes[offset + name_len ..][0..payload_len]) }, .allocation = allocation, .generation = body[11..27].*, .retired = body[59] != 0 };
        if (!std.mem.eql(u8, &binding.generation, &generation(incarnation, allocation))) return error.InvalidIntegrityCatalog;
        if (index != 0 and !lessThan({}, bindings[index - 1], binding.*)) return error.InvalidIntegrityCatalog;
        if (index != 0 and !binding.retired and !bindings[index - 1].retired and binding.definition.kind == bindings[index - 1].definition.kind and std.mem.eql(u8, binding.definition.name, bindings[index - 1].definition.name)) return error.InvalidIntegrityCatalog;
        offset += name_len + payload_len;
    }
    if (offset != end) return error.InvalidIntegrityCatalog;
    return .{ .arena = arena, .incarnation = incarnation, .schema_version = std.mem.readInt(u32, bytes[20..24], .little), .schema_digest = bytes[24..56].*, .checks_digest = bytes[68..100].*, .next_generation = next_generation, .bindings = bindings };
}

fn lessThan(_: void, left: Binding, right: Binding) bool {
    if (left.definition.kind != right.definition.kind) return @backingInt(left.definition.kind) < @backingInt(right.definition.kind);
    switch (std.mem.order(u8, left.definition.name, right.definition.name)) {
        .lt => return true,
        .gt => return false,
        .eq => {},
    }
    if (left.retired != right.retired) return !left.retired;
    return left.allocation < right.allocation;
}

/// An index-only candidate may add exactly one UNIQUE generation, preserving
/// every active and retired predecessor binding. The returned binding borrows
/// the successor. Inputs must already have passed catalog decode/prepare.
/// This linear, allocation-free check is independent of the candidate builder.
pub fn indexAddition(before: Catalog, after: Catalog, name: []const u8) !Binding {
    if (!std.mem.eql(u8, &before.incarnation, &after.incarnation) or
        after.schema_version != (std.math.add(u32, before.schema_version, 1) catch return error.InvalidIntegrityDefinition) or
        after.next_generation != (std.math.add(u64, before.next_generation, 1) catch return error.InvalidIntegrityDefinition) or
        !std.mem.eql(u8, &before.checks_digest, &after.checks_digest) or
        after.bindings.len != before.bindings.len + 1 or
        before.find(.unique, name) != null) return error.InvalidIntegrityDefinition;
    const added = after.find(.unique, name) orelse return error.InvalidIntegrityDefinition;
    if (added.allocation != before.next_generation or
        !std.mem.eql(u8, &added.generation, &generation(before.incarnation, added.allocation))) return error.InvalidIntegrityDefinition;
    var old_index: usize = 0;
    for (after.bindings) |entry| {
        if (std.mem.eql(u8, &entry.generation, &added.generation)) continue;
        if (old_index == before.bindings.len) return error.InvalidIntegrityDefinition;
        const old = before.bindings[old_index];
        if (entry.retired != old.retired or entry.allocation != old.allocation or entry.definition.kind != old.definition.kind or
            !std.mem.eql(u8, &entry.generation, &old.generation) or
            !std.mem.eql(u8, entry.definition.name, old.definition.name) or
            !std.mem.eql(u8, &entry.definition.fingerprint, &old.definition.fingerprint) or
            !std.mem.eql(u8, entry.definition.payload, old.definition.payload)) return error.InvalidIntegrityDefinition;
        old_index += 1;
    }
    if (old_index != before.bindings.len) return error.InvalidIntegrityDefinition;
    return added;
}

fn accountDefinition(length: usize, definition: Definition) !usize {
    var next = std.math.add(usize, length, entry_header_len) catch return error.IntegrityCatalogTooLarge;
    next = std.math.add(usize, next, definition.name.len) catch return error.IntegrityCatalogTooLarge;
    next = std.math.add(usize, next, definition.payload.len) catch return error.IntegrityCatalogTooLarge;
    if (next > max_catalog_bytes) return error.IntegrityCatalogTooLarge;
    return next;
}

fn encode(alloc: Allocator, catalog: Catalog) ![]u8 {
    var length: usize = header_len + 32;
    for (catalog.bindings) |binding| length = try accountDefinition(length, binding.definition);
    const bytes = try alloc.alloc(u8, length);
    @memcpy(bytes[0..4], "AIC2");
    @memcpy(bytes[4..20], &catalog.incarnation);
    std.mem.writeInt(u32, bytes[20..24], catalog.schema_version, .little);
    @memcpy(bytes[24..56], &catalog.schema_digest);
    std.mem.writeInt(u64, bytes[56..64], catalog.next_generation, .little);
    std.mem.writeInt(u32, bytes[64..68], @intCast(catalog.bindings.len), .little);
    @memcpy(bytes[68..100], &catalog.checks_digest);
    var offset: usize = header_len;
    for (catalog.bindings) |binding| {
        bytes[offset] = @backingInt(binding.definition.kind);
        std.mem.writeInt(u16, bytes[offset + 1 ..][0..2], @intCast(binding.definition.name.len), .little);
        std.mem.writeInt(u64, bytes[offset + 3 ..][0..8], binding.allocation, .little);
        @memcpy(bytes[offset + 11 ..][0..16], &binding.generation);
        @memcpy(bytes[offset + 27 ..][0..32], &binding.definition.fingerprint);
        bytes[offset + 59] = @intFromBool(binding.retired);
        std.mem.writeInt(u32, bytes[offset + 60 ..][0..4], @intCast(binding.definition.payload.len), .little);
        @memcpy(bytes[offset + entry_header_len ..][0..binding.definition.name.len], binding.definition.name);
        @memcpy(bytes[offset + entry_header_len + binding.definition.name.len ..][0..binding.definition.payload.len], binding.definition.payload);
        offset += entry_header_len + binding.definition.name.len + binding.definition.payload.len;
    }
    @memcpy(bytes[offset..], &digest(bytes[0..offset]));
    return bytes;
}

pub const Update = struct {
    catalog: Catalog,
    expected: ?[]const u8,
    value: []const u8,
    changed: bool,
    pub fn deinit(self: *Update) void {
        self.catalog.deinit();
        self.* = undefined;
    }
    /// Caller stages schema, outbox and this CAS in ONE transaction, then
    /// publishes the new immutable schema view only after durable commit.
    pub fn stage(self: Update, txn: anytype) !void {
        const current = txn.get(key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (self.expected) |old| {
            if (!std.mem.eql(u8, current orelse return error.IntegrityCatalogChanged, old)) return error.IntegrityCatalogChanged;
        } else if (current != null) return error.IntegrityCatalogChanged;
        if (self.changed) try txn.put(key, self.value);
    }
};

/// Burn a private candidate's allocation without publishing its schema or
/// binding. The owner must first fence ordinary writes and drain predecessor
/// participants, then stage this with its job binding and outbox in one
/// transaction. Cancellation retains the burn, including on owners which
/// did not receive begin; it must never restore the old allocation counter.
pub fn prepareIndexReservation(alloc: Allocator, previous: []const u8, candidate: Catalog, name: []const u8) !Update {
    var before = try decode(alloc, previous);
    errdefer before.deinit();
    _ = try indexAddition(before, candidate, name);
    before.next_generation = candidate.next_generation;
    const owned = before.arena.allocator();
    const value = try encode(owned, before);
    const expected = try owned.dupe(u8, previous);
    return .{ .catalog = before, .expected = expected, .value = value, .changed = true };
}

/// Prepare installation only against the exact reserved predecessor. This is
/// not publication authority: the receiving owner must independently verify
/// the metadata decision, complete validation receipts and its live job fence
/// before staging this with schema, activation and outbox in one transaction.
pub fn prepareIndexPublication(alloc: Allocator, predecessor: []const u8, reserved: []const u8, candidate: []const u8, name: []const u8) !Update {
    var before = try decode(alloc, predecessor);
    defer before.deinit();
    var after = try decode(alloc, candidate);
    errdefer after.deinit();
    _ = try indexAddition(before, after, name);
    before.next_generation = after.next_generation;
    const owned = after.arena.allocator();
    const expected = try encode(owned, before);
    if (!std.mem.eql(u8, expected, reserved)) return error.IntegrityCatalogChanged;
    const value = try owned.dupe(u8, candidate);
    return .{ .catalog = after, .expected = expected, .value = value, .changed = true };
}

pub fn prepare(alloc: Allocator, previous: ?[]const u8, incarnation: integrity.Generation, schema_version: u32, schema_digest: integrity.Digest, definitions: []const Definition) !Update {
    return prepareWithChecks(alloc, previous, incarnation, schema_version, schema_digest, definitions, @splat(0));
}

pub fn prepareWithChecks(alloc: Allocator, previous: ?[]const u8, incarnation: integrity.Generation, schema_version: u32, schema_digest: integrity.Digest, definitions: []const Definition, checks_digest: integrity.Digest) !Update {
    if (definitions.len > max_definitions or std.mem.allEqual(u8, &incarnation, 0)) return error.InvalidIntegrityCatalog;
    // Reject an oversized candidate before decoding its predecessor or cloning
    // payloads. Per-definition limits alone allow allocation amplification.
    var encoded_length: usize = header_len + 32;
    for (definitions) |definition| {
        if (definition.name.len == 0 or definition.name.len > 256 or definition.payload.len > max_catalog_bytes or !std.unicode.utf8ValidateSlice(definition.name)) return error.InvalidIntegrityDefinition;
        encoded_length = try accountDefinition(encoded_length, definition);
    }
    var prior: ?Catalog = if (previous) |bytes| try decode(alloc, bytes) else null;
    defer if (prior) |*catalog| catalog.deinit();
    if (prior) |catalog| if (!std.mem.eql(u8, &catalog.incarnation, &incarnation)) return error.IntegrityCatalogIncarnationMismatch;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    var bindings: std.ArrayList(Binding) = .empty;
    // Temporary membership indexes never become part of the immutable result.
    var names: [2]std.StringHashMapUnmanaged(void) = .{ .{}, .{} };
    defer for (&names) |*set| set.deinit(alloc);
    var retained_allocations: std.AutoHashMapUnmanaged(u64, void) = .empty;
    defer retained_allocations.deinit(alloc);
    const linear_names = definitions.len <= 64;
    const linear_retirement = linear_names and (if (prior) |catalog| catalog.bindings.len <= 64 else true);
    var next = if (prior) |catalog| catalog.next_generation else 1;
    for (definitions) |definition| {
        if (linear_names) {
            for (bindings.items) |binding| if (binding.definition.kind == definition.kind and std.mem.eql(u8, binding.definition.name, definition.name)) return error.InvalidIntegrityDefinition;
        } else if ((try names[@backingInt(definition.kind) - 1].getOrPut(alloc, definition.name)).found_existing) return error.InvalidIntegrityDefinition;
        const old: ?Binding = if (prior) |catalog| catalog.find(definition.kind, definition.name) else null;
        const allocation = if (old != null and std.mem.eql(u8, &old.?.definition.fingerprint, &definition.fingerprint)) old.?.allocation else blk: {
            const allocated = next;
            next = std.math.add(u64, next, 1) catch return error.IntegrityGenerationExhausted;
            break :blk allocated;
        };
        if (prior != null and !linear_retirement) try retained_allocations.put(alloc, allocation, {});
        try bindings.append(owned, .{ .definition = .{ .kind = definition.kind, .name = try owned.dupe(u8, definition.name), .fingerprint = definition.fingerprint, .payload = try owned.dupe(u8, definition.payload) }, .allocation = allocation, .generation = generation(incarnation, allocation) });
    }
    if (prior) |catalog| for (catalog.bindings) |old| {
        if (linear_retirement) {
            var retained = false;
            for (bindings.items) |binding| if (binding.allocation == old.allocation) {
                retained = true;
                break;
            };
            if (retained) continue;
        } else if (retained_allocations.contains(old.allocation)) continue;
        if (bindings.items.len == max_definitions) return error.IntegrityRetirementBacklogFull;
        encoded_length = try accountDefinition(encoded_length, old.definition);
        try bindings.append(owned, .{ .definition = .{ .kind = old.definition.kind, .name = try owned.dupe(u8, old.definition.name), .fingerprint = old.definition.fingerprint, .payload = try owned.dupe(u8, old.definition.payload) }, .allocation = old.allocation, .generation = old.generation, .retired = true });
    };
    std.mem.sort(Binding, bindings.items, {}, lessThan);
    const binding_items = try bindings.toOwnedSlice(owned);
    var catalog: Catalog = .{ .arena = undefined, .incarnation = incarnation, .schema_version = schema_version, .schema_digest = schema_digest, .checks_digest = checks_digest, .next_generation = next, .bindings = binding_items };
    const value = try encode(owned, catalog);
    const expected = if (previous) |bytes| try owned.dupe(u8, bytes) else null;
    catalog.arena = arena;
    return .{ .catalog = catalog, .expected = expected, .value = value, .changed = if (previous) |bytes| !std.mem.eql(u8, bytes, value) else true };
}

test "relational integrity catalog retains unchanged identities and never revives dropped generations" {
    const alloc = std.testing.allocator;
    const definition: Definition = .{ .kind = .unique, .name = "parent_id", .fingerprint = @splat(4) };
    var first = try prepare(alloc, null, @splat(1), 1, @splat(2), &.{definition});
    defer first.deinit();
    const first_id = first.catalog.bindings[0].generation;
    var unchanged = try prepare(alloc, first.value, @splat(1), 2, @splat(3), &.{definition});
    defer unchanged.deinit();
    try std.testing.expectEqual(first_id, unchanged.catalog.bindings[0].generation);
    var dropped = try prepare(alloc, unchanged.value, @splat(1), 3, @splat(4), &.{});
    defer dropped.deinit();
    var added = try prepare(alloc, dropped.value, @splat(1), 4, @splat(5), &.{definition});
    defer added.deinit();
    try std.testing.expect(!std.mem.eql(u8, &first_id, &added.catalog.bindings[0].generation));
    var decoded = try decode(alloc, added.value);
    defer decoded.deinit();
    try std.testing.expectEqual(added.catalog.bindings[0].generation, decoded.find(.unique, "parent_id").?.generation);
    try std.testing.expectError(error.IntegrityCatalogIncarnationMismatch, prepare(alloc, added.value, @splat(9), 5, @splat(6), &.{definition}));
    const retired = decoded.findGeneration(first_id).?;
    try std.testing.expect(retired.retired);
    try std.testing.expectEqual(@as(usize, 2), decoded.bindings.len);
}

test "relational index catalog private claim allocation verifies exact bounded reservation" {
    const alloc = std.testing.allocator;
    var value = try prepare(alloc, null, try incarnationFromTableId(101), 1, @splat(2), &.{});
    defer value.deinit();
    try std.testing.expectError(error.InvalidIntegrityCatalog, privateIndexAllocationFromReservation(value.value, digest(value.value), 101));
    // The extractor is allocation-free, but is not authority: these header
    // fixtures deliberately do not constitute a real reserved owner job.
    value.catalog.next_generation = 2;
    const reserved = try encode(alloc, value.catalog);
    defer alloc.free(reserved);
    const scope = try privateIndexAllocationFromReservation(reserved, digest(reserved), 101);
    try std.testing.expectEqual(@as(u64, 1), scope.allocation);
    try std.testing.expectEqual(try privateIndexGeneration(101, 1), scope.generation);
    try std.testing.expectError(error.InvalidIntegrityCatalog, privateIndexGeneration(101, 0));
    try std.testing.expectError(error.InvalidIntegrityCatalog, privateIndexAllocationFromReservation(reserved, digest(reserved), 102));
    try std.testing.expectError(error.IntegrityCatalogChanged, privateIndexAllocationFromReservation(reserved, @splat(0), 101));
    for (0..reserved.len) |length| try std.testing.expectError(error.IntegrityCatalogChanged, privateIndexAllocationFromReservation(reserved[0..length], digest(reserved), 101));
    const corrupt = try alloc.dupe(u8, reserved);
    defer alloc.free(corrupt);
    corrupt[corrupt.len - 1] ^= 1;
    try std.testing.expectError(error.InvalidIntegrityCatalog, privateIndexAllocationFromReservation(corrupt, digest(corrupt), 101));
    @memcpy(corrupt, reserved);
    std.mem.writeInt(u32, corrupt[64..68], max_definitions + 1, .little);
    @memcpy(corrupt[corrupt.len - 32 ..], &digest(corrupt[0 .. corrupt.len - 32]));
    try std.testing.expectError(error.InvalidIntegrityCatalog, privateIndexAllocationFromReservation(corrupt, digest(corrupt), 101));
    value.catalog.next_generation = std.math.maxInt(u64);
    const last = try encode(alloc, value.catalog);
    defer alloc.free(last);
    try std.testing.expectEqual(std.math.maxInt(u64) - 1, (try privateIndexAllocationFromReservation(last, digest(last), 101)).allocation);
}

test "index-only publication preserves active and retired bindings and rejects unrelated changes" {
    const alloc = std.testing.allocator;
    const pk: Definition = .{ .kind = .unique, .name = "id", .fingerprint = @splat(4), .payload = "id-key" };
    const retired: Definition = .{ .kind = .unique, .name = "old", .fingerprint = @splat(5) };
    const fk: Definition = .{ .kind = .foreign_key, .name = "parent", .fingerprint = @splat(6), .payload = "parent-binding" };
    const email: Definition = .{ .kind = .unique, .name = "email", .fingerprint = @splat(7), .payload = "email-key" };
    var original = try prepareWithChecks(alloc, null, @splat(1), 1, @splat(2), &.{ pk, retired, fk }, @splat(8));
    defer original.deinit();
    var before = try prepareWithChecks(alloc, original.value, @splat(1), 2, @splat(3), &.{ pk, fk }, @splat(8));
    defer before.deinit();
    var after = try prepareWithChecks(alloc, before.value, @splat(1), 3, @splat(9), &.{ pk, email, fk }, @splat(8));
    defer after.deinit();
    const added = try indexAddition(before.catalog, after.catalog, "email");
    try std.testing.expectEqualStrings("email-key", added.definition.payload);
    try std.testing.expectEqual(before.catalog.next_generation, added.allocation);
    try std.testing.expect(after.catalog.findGeneration(original.catalog.find(.unique, "old").?.generation).?.retired);
    try std.testing.expectError(error.InvalidIntegrityDefinition, indexAddition(before.catalog, after.catalog, "id"));
    try std.testing.expectError(error.InvalidIntegrityDefinition, indexAddition(before.catalog, after.catalog, "missing"));
    var changed = after.catalog;
    changed.schema_version += 1;
    try std.testing.expectError(error.InvalidIntegrityDefinition, indexAddition(before.catalog, changed, "email"));
    changed = after.catalog;
    changed.next_generation += 1;
    try std.testing.expectError(error.InvalidIntegrityDefinition, indexAddition(before.catalog, changed, "email"));
    changed = after.catalog;
    changed.incarnation[0] ^= 1;
    try std.testing.expectError(error.InvalidIntegrityDefinition, indexAddition(before.catalog, changed, "email"));
    changed = after.catalog;
    changed.checks_digest[0] ^= 1;
    try std.testing.expectError(error.InvalidIntegrityDefinition, indexAddition(before.catalog, changed, "email"));
    // Do not rely on a caller's fingerprint to conceal a changed FK payload.
    var changed_fk = fk;
    changed_fk.payload = "different-parent-binding";
    var altered = try prepareWithChecks(alloc, before.value, @splat(1), 3, @splat(9), &.{ pk, email, changed_fk }, @splat(8));
    defer altered.deinit();
    try std.testing.expectError(error.InvalidIntegrityDefinition, indexAddition(before.catalog, altered.catalog, "email"));
    var dropped = try prepareWithChecks(alloc, before.value, @splat(1), 3, @splat(9), &.{ pk, email }, @splat(8));
    defer dropped.deinit();
    try std.testing.expectError(error.InvalidIntegrityDefinition, indexAddition(before.catalog, dropped.catalog, "email"));
    const copies = try alloc.dupe(Binding, after.catalog.bindings);
    defer alloc.free(copies);
    for (copies) |*binding| if (binding.retired) {
        binding.definition.payload = "changed-retired-payload";
    };
    changed = after.catalog;
    changed.bindings = copies;
    try std.testing.expectError(error.InvalidIntegrityDefinition, indexAddition(before.catalog, changed, "email"));
}

test "index-only publication reuses a retired name without reviving its generation" {
    const alloc = std.testing.allocator;
    const rule: Definition = .{ .kind = .unique, .name = "email", .fingerprint = @splat(4) };
    var original = try prepare(alloc, null, @splat(1), 1, @splat(2), &.{rule});
    defer original.deinit();
    var before = try prepare(alloc, original.value, @splat(1), 2, @splat(3), &.{});
    defer before.deinit();
    var after = try prepare(alloc, before.value, @splat(1), 3, @splat(5), &.{rule});
    defer after.deinit();
    const addition = try indexAddition(before.catalog, after.catalog, "email");
    try std.testing.expect(!std.mem.eql(u8, &addition.generation, &original.catalog.bindings[0].generation));
}

test "index-only reservation leaves active catalog unchanged and prevents canceled generation reuse" {
    const alloc = std.testing.allocator;
    const pk: Definition = .{ .kind = .unique, .name = "id", .fingerprint = @splat(4) };
    const email: Definition = .{ .kind = .unique, .name = "email", .fingerprint = @splat(5) };
    var before = try prepare(alloc, null, @splat(1), 1, @splat(2), &.{pk});
    defer before.deinit();
    var candidate = try prepare(alloc, before.value, @splat(1), 2, @splat(3), &.{ pk, email });
    defer candidate.deinit();
    var reserved = try prepareIndexReservation(alloc, before.value, candidate.catalog, "email");
    defer reserved.deinit();
    try std.testing.expectEqual(before.catalog.schema_version, reserved.catalog.schema_version);
    try std.testing.expectEqual(before.catalog.schema_digest, reserved.catalog.schema_digest);
    try std.testing.expectEqual(before.catalog.checks_digest, reserved.catalog.checks_digest);
    try std.testing.expectEqual(before.catalog.find(.unique, "id").?.generation, reserved.catalog.find(.unique, "id").?.generation);
    try std.testing.expect(reserved.catalog.find(.unique, "email") == null);
    try std.testing.expectEqual(candidate.catalog.next_generation, reserved.catalog.next_generation);
    // A canceled build leaves this reservation in place. An explicit repaired
    // submission gets a different generation, even with the same index name.
    var repaired = try prepare(alloc, reserved.value, @splat(1), 2, @splat(3), &.{ pk, email });
    defer repaired.deinit();
    try std.testing.expect(!std.mem.eql(u8, &candidate.catalog.find(.unique, "email").?.generation, &repaired.catalog.find(.unique, "email").?.generation));
    var publication = try prepareIndexPublication(alloc, before.value, reserved.value, candidate.value, "email");
    defer publication.deinit();
    try std.testing.expectEqualStrings(candidate.value, publication.value);
    try std.testing.expectEqualStrings(reserved.value, publication.expected.?);
    try std.testing.expectError(error.IntegrityCatalogChanged, prepareIndexPublication(alloc, before.value, before.value, candidate.value, "email"));
}

test "index-only reservation and installation CAS cannot overwrite a concurrent publication" {
    const alloc = std.testing.allocator;
    const pk: Definition = .{ .kind = .unique, .name = "id", .fingerprint = @splat(4) };
    const email: Definition = .{ .kind = .unique, .name = "email", .fingerprint = @splat(5) };
    var before = try prepare(alloc, null, @splat(1), 1, @splat(2), &.{pk});
    defer before.deinit();
    var candidate = try prepare(alloc, before.value, @splat(1), 2, @splat(3), &.{ pk, email });
    defer candidate.deinit();
    var reserved = try prepareIndexReservation(alloc, before.value, candidate.catalog, "email");
    defer reserved.deinit();
    var publication = try prepareIndexPublication(alloc, before.value, reserved.value, candidate.value, "email");
    defer publication.deinit();
    const Txn = struct {
        value: []const u8,
        puts: usize = 0,
        pub fn get(self: *@This(), physical_key: []const u8) anyerror![]const u8 {
            try std.testing.expectEqualStrings(key, physical_key);
            return self.value;
        }
        pub fn put(self: *@This(), physical_key: []const u8, value: []const u8) !void {
            try std.testing.expectEqualStrings(key, physical_key);
            self.value = value;
            self.puts += 1;
        }
    };
    var txn: Txn = .{ .value = before.value };
    try std.testing.expectError(error.IntegrityCatalogChanged, publication.stage(&txn));
    try std.testing.expectEqual(@as(usize, 0), txn.puts);
    try reserved.stage(&txn);
    try std.testing.expectError(error.IntegrityCatalogChanged, reserved.stage(&txn));
    try publication.stage(&txn);
    try std.testing.expectEqualStrings(candidate.value, txn.value);
    try std.testing.expectEqual(@as(usize, 2), txn.puts);
    try std.testing.expectError(error.IntegrityCatalogChanged, reserved.stage(&txn));
    try std.testing.expectError(error.IntegrityCatalogChanged, publication.stage(&txn));
    try std.testing.expectEqualStrings(candidate.value, txn.value);
}

test "relational catalog membership indexes preserve kind-scoped duplicate checks at both size boundaries" {
    const alloc = std.testing.allocator;
    for ([_]usize{ 2, 65 }) |count| {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const temporary = arena.allocator();
        const definitions = try temporary.alloc(Definition, count);
        for (definitions, 0..) |*definition, i| definition.* = .{ .kind = .unique, .name = try std.fmt.allocPrint(temporary, "key-{d}", .{i}), .fingerprint = @splat(4) };
        definitions[count - 1].name = definitions[0].name;
        definitions[count - 1].kind = .foreign_key;
        var accepted = try prepare(alloc, null, @splat(1), 1, @splat(2), definitions);
        defer accepted.deinit();
        try std.testing.expectEqual(count, accepted.catalog.bindings.len);
        definitions[count - 1].kind = .unique;
        try std.testing.expectError(error.InvalidIntegrityDefinition, prepare(alloc, null, @splat(1), 1, @splat(2), definitions));
    }
}

test "relational catalog decoder rejects duplicated generation allocations on both membership paths" {
    const alloc = std.testing.allocator;
    for ([_]usize{ 2, 65 }) |count| {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const temporary = arena.allocator();
        const definitions = try temporary.alloc(Definition, count);
        for (definitions, 0..) |*definition, i| definition.* = .{ .kind = .unique, .name = try std.fmt.allocPrint(temporary, "key-{d:0>3}", .{i}), .fingerprint = @splat(4) };
        var valid = try prepare(alloc, null, @splat(1), 1, @splat(2), definitions);
        defer valid.deinit();
        const copies = try temporary.dupe(Binding, valid.catalog.bindings);
        copies[1].allocation = copies[0].allocation;
        copies[1].generation = copies[0].generation;
        var invalid = valid.catalog;
        invalid.bindings = copies;
        const encoded = try encode(alloc, invalid);
        defer alloc.free(encoded);
        // encode recomputes a valid physical checksum; structural generation
        // uniqueness must still fail, not be delegated to checksum checking.
        try std.testing.expectError(error.InvalidIntegrityCatalog, decode(alloc, encoded));
    }
}

test "index-only publication scratch indexes release ownership on every allocation failure" {
    const Harness = struct {
        fn run(alloc: Allocator) !void {
            var names_arena = std.heap.ArenaAllocator.init(alloc);
            defer names_arena.deinit();
            const temporary = names_arena.allocator();
            const definitions = try temporary.alloc(Definition, 66);
            for (definitions, 0..) |*definition, i| definition.* = .{ .kind = .unique, .name = try std.fmt.allocPrint(temporary, "key-{d:0>3}", .{i}), .fingerprint = @splat(4) };
            var before = try prepare(alloc, null, @splat(1), 1, @splat(2), definitions[0..65]);
            defer before.deinit();
            var after = try prepare(alloc, before.value, @splat(1), 2, @splat(3), definitions);
            defer after.deinit();
            _ = try indexAddition(before.catalog, after.catalog, definitions[65].name);
            var reserved = try prepareIndexReservation(alloc, before.value, after.catalog, definitions[65].name);
            defer reserved.deinit();
            var publication = try prepareIndexPublication(alloc, before.value, reserved.value, after.value, definitions[65].name);
            defer publication.deinit();
            var removed = try prepare(alloc, after.value, @splat(1), 3, @splat(5), definitions[0..33]);
            defer removed.deinit();
            try std.testing.expectEqual(@as(usize, 66), removed.catalog.bindings.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "catalog preparation rejects oversized candidates before any owned allocation and accounts for retired payloads" {
    const alloc = std.testing.allocator;
    const payload = try alloc.alloc(u8, max_catalog_bytes);
    defer alloc.free(payload);
    @memset(payload, 0);
    const rule: Definition = .{ .kind = .unique, .name = "key", .fingerprint = @splat(4), .payload = payload };
    var predecessor = try prepare(alloc, null, @splat(1), 1, @splat(2), &.{});
    defer predecessor.deinit();
    for ([_]?[]const u8{ null, predecessor.value }) |previous| {
        var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
        try std.testing.expectError(error.IntegrityCatalogTooLarge, prepare(failing.allocator(), previous, @splat(1), 2, @splat(3), &.{rule}));
        try std.testing.expect(!failing.has_induced_failure);
    }
    var exact_rule = rule;
    exact_rule.payload = payload[0 .. max_catalog_bytes - header_len - 32 - entry_header_len - rule.name.len];
    var exact = try prepare(alloc, null, @splat(1), 1, @splat(2), &.{exact_rule});
    defer exact.deinit();
    try std.testing.expectEqual(@as(usize, max_catalog_bytes), exact.value.len);
    var half = rule;
    half.payload = payload[0 .. max_catalog_bytes / 2];
    var original = try prepare(alloc, null, @splat(1), 1, @splat(2), &.{half});
    defer original.deinit();
    half.name = "new";
    // Each payload fits alone, but the retired predecessor also occupies the
    // immutable catalog. Reject before cloning that extra retired descriptor.
    try std.testing.expectError(error.IntegrityCatalogTooLarge, prepare(alloc, original.value, @splat(1), 2, @splat(3), &.{half}));
}

test "relational integrity catalog releases ownership on every allocation failure" {
    const Harness = struct {
        fn run(alloc: Allocator) !void {
            const definition: Definition = .{ .kind = .foreign_key, .name = "parent", .fingerprint = @splat(4), .payload = "immutable descriptor" };
            var original = try prepare(alloc, null, @splat(1), 1, @splat(2), &.{definition});
            defer original.deinit();
            var removed = try prepare(alloc, original.value, @splat(1), 2, @splat(3), &.{});
            defer removed.deinit();
            var decoded = try decode(alloc, removed.value);
            defer decoded.deinit();
            try std.testing.expectEqualStrings("immutable descriptor", decoded.bindings[0].definition.payload);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}
