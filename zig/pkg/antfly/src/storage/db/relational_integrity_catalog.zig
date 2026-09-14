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
            const order = if (entry.definition.kind == kind) std.mem.order(u8, entry.definition.name, name) else std.math.order(@intFromEnum(entry.definition.kind), @intFromEnum(kind));
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

const header_len = 68;
const entry_header_len = 64;

pub fn decode(alloc: Allocator, bytes: []const u8) !Catalog {
    if (bytes.len < header_len + 32 or bytes.len > max_catalog_bytes or !std.mem.eql(u8, bytes[0..4], "AIC1") or
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
        if ((try allocations.getOrPut(owned, allocation)).found_existing) return error.InvalidIntegrityCatalog;
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
    return .{ .arena = arena, .incarnation = incarnation, .schema_version = std.mem.readInt(u32, bytes[20..24], .little), .schema_digest = bytes[24..56].*, .next_generation = next_generation, .bindings = bindings };
}

fn lessThan(_: void, left: Binding, right: Binding) bool {
    if (left.definition.kind != right.definition.kind) return @intFromEnum(left.definition.kind) < @intFromEnum(right.definition.kind);
    switch (std.mem.order(u8, left.definition.name, right.definition.name)) {
        .lt => return true,
        .gt => return false,
        .eq => {},
    }
    if (left.retired != right.retired) return !left.retired;
    return left.allocation < right.allocation;
}

fn encode(alloc: Allocator, catalog: Catalog) ![]u8 {
    var length: usize = header_len + 32;
    for (catalog.bindings) |binding| length = std.math.add(usize, length, entry_header_len + binding.definition.name.len + binding.definition.payload.len) catch return error.IntegrityCatalogTooLarge;
    if (length > max_catalog_bytes) return error.IntegrityCatalogTooLarge;
    const bytes = try alloc.alloc(u8, length);
    @memcpy(bytes[0..4], "AIC1");
    @memcpy(bytes[4..20], &catalog.incarnation);
    std.mem.writeInt(u32, bytes[20..24], catalog.schema_version, .little);
    @memcpy(bytes[24..56], &catalog.schema_digest);
    std.mem.writeInt(u64, bytes[56..64], catalog.next_generation, .little);
    std.mem.writeInt(u32, bytes[64..68], @intCast(catalog.bindings.len), .little);
    var offset: usize = header_len;
    for (catalog.bindings) |binding| {
        bytes[offset] = @intFromEnum(binding.definition.kind);
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

pub fn prepare(alloc: Allocator, previous: ?[]const u8, incarnation: integrity.Generation, schema_version: u32, schema_digest: integrity.Digest, definitions: []const Definition) !Update {
    if (definitions.len > max_definitions or std.mem.allEqual(u8, &incarnation, 0)) return error.InvalidIntegrityCatalog;
    var prior: ?Catalog = if (previous) |bytes| try decode(alloc, bytes) else null;
    defer if (prior) |*catalog| catalog.deinit();
    if (prior) |catalog| if (!std.mem.eql(u8, &catalog.incarnation, &incarnation)) return error.IntegrityCatalogIncarnationMismatch;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    var bindings: std.ArrayList(Binding) = .empty;
    var next = if (prior) |catalog| catalog.next_generation else 1;
    for (definitions) |definition| {
        if (definition.name.len == 0 or definition.name.len > 256 or definition.payload.len > max_catalog_bytes or !std.unicode.utf8ValidateSlice(definition.name)) return error.InvalidIntegrityDefinition;
        for (bindings.items) |binding| if (binding.definition.kind == definition.kind and std.mem.eql(u8, binding.definition.name, definition.name)) return error.InvalidIntegrityDefinition;
        const old: ?Binding = if (prior) |catalog| catalog.find(definition.kind, definition.name) else null;
        const allocation = if (old != null and std.mem.eql(u8, &old.?.definition.fingerprint, &definition.fingerprint)) old.?.allocation else blk: {
            const allocated = next;
            next = std.math.add(u64, next, 1) catch return error.IntegrityGenerationExhausted;
            break :blk allocated;
        };
        try bindings.append(owned, .{ .definition = .{ .kind = definition.kind, .name = try owned.dupe(u8, definition.name), .fingerprint = definition.fingerprint, .payload = try owned.dupe(u8, definition.payload) }, .allocation = allocation, .generation = generation(incarnation, allocation) });
    }
    if (prior) |catalog| for (catalog.bindings) |old| {
        var retained = false;
        for (bindings.items) |binding| if (binding.allocation == old.allocation) {
            retained = true;
            break;
        };
        if (retained) continue;
        if (bindings.items.len == max_definitions) return error.IntegrityRetirementBacklogFull;
        try bindings.append(owned, .{ .definition = .{ .kind = old.definition.kind, .name = try owned.dupe(u8, old.definition.name), .fingerprint = old.definition.fingerprint, .payload = try owned.dupe(u8, old.definition.payload) }, .allocation = old.allocation, .generation = old.generation, .retired = true });
    };
    std.mem.sort(Binding, bindings.items, {}, lessThan);
    const binding_items = try bindings.toOwnedSlice(owned);
    var catalog: Catalog = .{ .arena = undefined, .incarnation = incarnation, .schema_version = schema_version, .schema_digest = schema_digest, .next_generation = next, .bindings = binding_items };
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
