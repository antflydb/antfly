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

//! Transactional incoming-FK directory. Active and retained read layouts share
//! one parent/table edge; its lanes distinguish their independent lifetimes.
//! A populated directory is not proof of complete catalog coverage.
const std = @import("std");
const A = std.mem.Allocator;
pub const max_parents = 512;
pub const max_name_bytes = 256;
pub const key_bytes = 768;
const magic = "AFKD01";
const invalid = error.InvalidForeignKeyDependencies;

pub const Lanes = enum(u8) { active = 1, retained = 2, both = 3 };
pub const Reference = struct {
    parent: []const u8,
    lanes: Lanes,
    pub fn fingerprint(self: Reference, table: u64) [32]u8 {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly-incoming-fk-v1");
        var number: [8]u8 = undefined;
        std.mem.writeInt(u64, &number, table, .big);
        hash.update(&number);
        std.mem.writeInt(u64, &number, self.parent.len, .big);
        hash.update(&number);
        hash.update(self.parent);
        hash.update(&.{@backingInt(self.lanes)});
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }
};
fn nameValid(name: []const u8) bool {
    return name.len != 0 and name.len <= max_name_bytes and std.mem.indexOfScalar(u8, name, 0) == null and std.unicode.utf8ValidateSlice(name);
}
fn validate(items: []const Reference) !void {
    if (items.len > max_parents) return invalid;
    for (items, 0..) |item, i| {
        if (!nameValid(item.parent) or (i != 0 and std.mem.order(u8, items[i - 1].parent, item.parent) != .lt)) return invalid;
    }
}
fn less(_: void, left: Reference, right: Reference) bool {
    return std.mem.lessThan(u8, left.parent, right.parent);
}
/// Returned names belong to a. The caller normally uses one scratch arena for
/// both source layouts; arbitrary JSON numbers are never converted to floats.
fn fromSchemas(a: A, active: []const u8, retained: []const u8) ![]Reference {
    var parents: std.StringArrayHashMapUnmanaged(Lanes) = .empty;
    defer parents.deinit(a);
    inline for (.{ active, retained }, .{ Lanes.active, Lanes.retained }) |schema, lane| {
        if (schema.len != 0) {
            const value = std.json.parseFromSliceLeaky(struct {
                foreign_keys: ?[]const struct { parent_table: []const u8 } = null,
            }, a, schema, .{ .ignore_unknown_fields = true, .allocate = .alloc_always }) catch |err| return if (err == error.OutOfMemory) err else invalid;
            const foreign_keys = value.foreign_keys orelse &.{};
            if (foreign_keys.len > max_parents / 2) return invalid;
            for (foreign_keys) |fk| {
                if (!nameValid(fk.parent_table)) return invalid;
                const entry = try parents.getOrPut(a, fk.parent_table);
                entry.value_ptr.* = if (entry.found_existing) @fromBackingInt(@intCast(@backingInt(entry.value_ptr.*) | @backingInt(lane))) else lane;
            }
        }
    }
    if (parents.count() > max_parents) return invalid;
    const result = try a.alloc(Reference, parents.count());
    for (result, parents.keys(), parents.values()) |*item, parent, lanes| item.* = .{ .parent = parent, .lanes = lanes };
    std.mem.sort(Reference, result, {}, less);
    return result;
}
pub fn tableKey(buffer: []u8, group: u64, table: u64) ![]const u8 {
    if (table == 0) return invalid;
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:fk-table:{d}", .{ group, table });
}
pub fn dependentPrefix(buffer: []u8, group: u64, parent: []const u8) ![]const u8 {
    if (!nameValid(parent)) return invalid;
    // Hex framing is prefix-safe even when names contain colons. Physical
    // names are used so staged/self parents need no premature name lookup.
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:fk-dependent:{x}:", .{ group, parent });
}
// Active/both edges precede retained-only edges. Each dependency is stored
// once, while active lookup never visits obsolete-layout-only dependencies.
fn laneKey(lanes: Lanes) u8 {
    return if (lanes == .retained) 2 else 1;
}
pub fn edgeKey(buffer: []u8, group: u64, parent: []const u8, table: u64) ![]const u8 {
    return edgeKeyForLanes(buffer, group, parent, table, .active);
}
pub fn edgeKeyForLanes(buffer: []u8, group: u64, parent: []const u8, table: u64, lanes: Lanes) ![]const u8 {
    if (table == 0) return invalid;
    const prefix = try dependentPrefix(buffer, group, parent);
    if (buffer.len - prefix.len < 9) return error.NoSpaceLeft;
    buffer[prefix.len] = laneKey(lanes);
    std.mem.writeInt(u64, buffer[prefix.len + 1 ..][0..8], table, .big);
    return buffer[0 .. prefix.len + 9];
}
fn selectedPrefix(buffer: []u8, group: u64, parent: []const u8, lanes: Lanes) ![]const u8 {
    const prefix = try dependentPrefix(buffer, group, parent);
    if (lanes != .active) return prefix;
    if (buffer.len == prefix.len) return error.NoSpaceLeft;
    buffer[prefix.len] = 1;
    return buffer[0 .. prefix.len + 1];
}
fn edgeTable(key: []const u8, prefix: []const u8, active_only: bool) !u64 {
    const suffix: usize = if (active_only) 8 else 9;
    if (key.len != prefix.len + suffix) return invalid;
    if (!active_only and key[prefix.len] != 1 and key[prefix.len] != 2) return invalid;
    const table = std.mem.readInt(u64, key[key.len - 8 ..][0..8], .big);
    if (table == 0) return invalid;
    return table;
}
pub fn summaryKey(buffer: []u8, group: u64, parent: []const u8) ![]const u8 {
    if (!nameValid(parent)) return invalid;
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:fk-summary:{x}", .{ group, parent });
}
pub const Stamp = struct {
    revision: u64 = 0,
    count: u64 = 0,
    active_count: u64 = 0,
    pub const encoded_len = 30;
    pub fn encode(self: Stamp) ![encoded_len]u8 {
        if (self.revision == 0 or self.active_count > self.count) return invalid;
        var result: [encoded_len]u8 = undefined;
        result[0..6].* = "AFKS02".*;
        std.mem.writeInt(u64, result[6..14], self.revision, .little);
        std.mem.writeInt(u64, result[14..22], self.count, .little);
        std.mem.writeInt(u64, result[22..30], self.active_count, .little);
        return result;
    }
    pub fn decode(bytes: []const u8) !Stamp {
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..6], "AFKS02")) return invalid;
        const result: Stamp = .{ .revision = std.mem.readInt(u64, bytes[6..14], .little), .count = std.mem.readInt(u64, bytes[14..22], .little), .active_count = std.mem.readInt(u64, bytes[22..30], .little) };
        if (result.revision == 0 or result.active_count > result.count) return invalid;
        return result;
    }
    fn changed(self: Stamp, before: ?Reference, after: ?Reference) !Stamp {
        const was_active = before != null and before.?.lanes != .retained;
        const now_active = after != null and after.?.lanes != .retained;
        if ((before != null and self.count == 0) or (was_active and self.active_count == 0) or self.active_count > self.count) return invalid;
        return .{
            .revision = std.math.add(u64, self.revision, 1) catch return error.CatalogRevisionExhausted,
            .count = std.math.add(u64, self.count - @intFromBool(before != null), @intFromBool(after != null)) catch return invalid,
            .active_count = std.math.add(u64, self.active_count - @intFromBool(was_active), @intFromBool(now_active)) catch return invalid,
        };
    }
};
fn optionalGet(reader: anytype, key: []const u8) !?[]const u8 {
    return reader.get(key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}
pub fn stamp(reader: anytype, group: u64, parent: []const u8) !Stamp {
    var buffer: [key_bytes]u8 = undefined;
    return if (try optionalGet(reader, try summaryKey(&buffer, group, parent))) |bytes| try Stamp.decode(bytes) else .{};
}
pub fn encode(a: A, items: []const Reference) ![]u8 {
    try validate(items);
    if (items.len == 0) return invalid;
    var length: usize = magic.len + 2;
    for (items) |item| length += 3 + item.parent.len;
    const bytes = try a.alloc(u8, length);
    bytes[0..magic.len].* = magic.*;
    std.mem.writeInt(u16, bytes[magic.len..][0..2], @intCast(items.len), .little);
    var offset: usize = magic.len + 2;
    for (items) |item| {
        std.mem.writeInt(u16, bytes[offset..][0..2], @intCast(item.parent.len), .little);
        bytes[offset + 2] = @backingInt(item.lanes);
        @memcpy(bytes[offset + 3 ..][0..item.parent.len], item.parent);
        offset += 3 + item.parent.len;
    }
    return bytes;
}
/// Names borrow the owned frame. Full canonical validation precedes lookup.
pub fn decode(a: A, bytes: []const u8) ![]Reference {
    if (bytes.len < magic.len + 2 or !std.mem.eql(u8, bytes[0..magic.len], magic)) return invalid;
    const count = std.mem.readInt(u16, bytes[magic.len..][0..2], .little);
    if (count == 0 or count > max_parents) return invalid;
    const result = try a.alloc(Reference, count);
    errdefer a.free(result);
    var offset: usize = magic.len + 2;
    for (result) |*item| {
        if (bytes.len - offset < 3) return invalid;
        const len = std.mem.readInt(u16, bytes[offset..][0..2], .little);
        const lanes: Lanes = switch (bytes[offset + 2]) {
            1 => .active,
            2 => .retained,
            3 => .both,
            else => return invalid,
        };
        offset += 3;
        if (len > bytes.len - offset) return invalid;
        item.* = .{ .parent = bytes[offset..][0..len], .lanes = lanes };
        offset += len;
    }
    if (offset != bytes.len) return invalid;
    try validate(result);
    return result;
}
fn find(items: []const Reference, parent: []const u8) ?Reference {
    var low: usize = 0;
    var high = items.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        switch (std.mem.order(u8, items[mid].parent, parent)) {
            .lt => low = mid + 1,
            .gt => high = mid,
            .eq => return items[mid],
        }
    }
    return null;
}
const Change = struct { parent: []const u8, before: ?Reference, after: ?Reference, bytes: [Stamp.encoded_len]u8 = undefined };
pub const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    table: u64,
    before_bytes: ?[]const u8,
    after_bytes: ?[]const u8,
    changes: []Change,
    pub fn deinit(self: *Prepared) void {
        self.arena.deinit();
        self.* = undefined;
    }
    /// Complete validation/allocation precedes writes. A stale prepared forward
    /// image fails rather than applying a delta twice; summaries rebase on prior
    /// participants in the same transaction. Any storage failure aborts it.
    pub fn matchesTxn(self: Prepared, txn: anytype, group: u64) !bool {
        var buffer: [key_bytes]u8 = undefined;
        return optionalEqual(try optionalGet(txn, try tableKey(&buffer, group, self.table)), self.before_bytes);
    }
    pub fn write(self: Prepared, txn: anytype, group: u64) !void {
        var buffer: [key_bytes]u8 = undefined;
        const current = try optionalGet(txn, try tableKey(&buffer, group, self.table));
        if (!optionalEqual(current, self.before_bytes)) return error.CatalogGenerationChanged;
        for (self.changes) |*change| {
            inline for (.{ Lanes.active, Lanes.retained }) |lane| {
                const old = try optionalGet(txn, try edgeKeyForLanes(&buffer, group, change.parent, self.table, lane));
                if (change.before != null and laneKey(change.before.?.lanes) == laneKey(lane)) {
                    if (old == null or !std.mem.eql(u8, old.?, &change.before.?.fingerprint(self.table))) return invalid;
                } else if (old != null) return invalid;
            }
            change.bytes = try (try (try stamp(txn, group, change.parent)).changed(change.before, change.after)).encode();
        }
        for (self.changes) |change| {
            if (change.before) |before| {
                if (change.after == null or laneKey(before.lanes) != laneKey(change.after.?.lanes))
                    try txn.delete(try edgeKeyForLanes(&buffer, group, change.parent, self.table, before.lanes));
            }
            if (change.after) |item| try txn.put(try edgeKeyForLanes(&buffer, group, change.parent, self.table, item.lanes), &item.fingerprint(self.table));
            try txn.put(try summaryKey(&buffer, group, change.parent), &change.bytes);
        }
        const key = try tableKey(&buffer, group, self.table);
        if (self.after_bytes) |bytes| try txn.put(key, bytes) else try txn.delete(key);
    }
};
fn optionalEqual(left: ?[]const u8, right: ?[]const u8) bool {
    return if (left) |bytes| right != null and std.mem.eql(u8, bytes, right.?) else right == null;
}
pub fn prepare(backing: A, txn: anytype, group: u64, table: u64, active: []const u8, retained: []const u8) !?Prepared {
    var arena = std.heap.ArenaAllocator.init(backing);
    errdefer arena.deinit();
    const a = arena.allocator();
    const after = fromSchemas(a, active, retained) catch |err| return if (err == error.OutOfMemory) err else error.InvalidForeignKeyDependencyDefinition;
    const after_bytes = if (after.len == 0) null else try encode(a, after);
    var buffer: [key_bytes]u8 = undefined;
    const stored = try optionalGet(txn, try tableKey(&buffer, group, table));
    if (optionalEqual(stored, after_bytes)) {
        arena.deinit();
        return null;
    }
    const before_bytes = if (stored) |bytes| try a.dupe(u8, bytes) else null;
    const before = if (before_bytes) |bytes| try decode(a, bytes) else &.{};
    var changes: std.ArrayList(Change) = .empty;
    for (before) |old| {
        const next = find(after, old.parent);
        if (next) |value| if (value.lanes == old.lanes) continue;
        try changes.append(a, .{ .parent = old.parent, .before = old, .after = next });
    }
    for (after) |next| {
        if (find(before, next.parent) != null) continue;
        try changes.append(a, .{ .parent = next.parent, .before = null, .after = next });
    }
    for (changes.items) |change| _ = try (try stamp(txn, group, change.parent)).changed(change.before, change.after);
    return .{ .arena = arena, .table = table, .before_bytes = before_bytes, .after_bytes = after_bytes, .changes = changes.items };
}
pub fn synchronize(a: A, txn: anytype, group: u64, table: u64, active: []const u8, retained: []const u8) !void {
    var prepared = (try prepare(a, txn, group, table, active, retained)) orelse return;
    defer prepared.deinit();
    try prepared.write(txn, group);
}

pub const Captured = struct {
    tables: []u64,
    stamp: Stamp,
    matching: u64 = 0,
    pub fn deinit(self: Captured, a: A) void {
        a.free(self.tables);
    }
};
/// Discovery under a pinned snapshot. A caller must independently establish
/// complete catalog coverage before treating a missing edge as nonexistence.
/// Stamps commit the edge set, not the mutable table schemas or privileges.
pub fn capture(a: A, txn: anytype, group: u64, parent: []const u8, limit: usize) !Captured {
    return captureSelected(a, txn, group, parent, limit, null, .both);
}
/// A retirement cohort counts surviving active dependents, not its own source
/// table or obsolete read-layout edges. The stamp still covers EVERY edge.
pub fn captureSelected(a: A, txn: anytype, group: u64, parent: []const u8, limit: usize, excluded: ?u64, lanes: Lanes) !Captured {
    return captureSelectedWithContext(a, txn, group, parent, limit, excluded, lanes, .{});
}
pub fn captureSelectedWithContext(a: A, txn: anytype, group: u64, parent: []const u8, limit: usize, excluded: ?u64, lanes: Lanes, context: @import("antfly_local_sources").api_operation.RequestContext) !Captured {
    try context.ensureActive();
    if (limit == 0 or limit > 128 or (excluded != null and excluded.? == 0)) return error.SqlProgramLimitExceeded;
    const captured = try stamp(txn, group, parent);
    var buffer: [key_bytes]u8 = undefined;
    const prefix = try selectedPrefix(&buffer, group, parent, lanes);
    var cursor = try txn.openCursor();
    defer cursor.close();
    var tables: std.ArrayList(u64) = .empty;
    errdefer tables.deinit(a);
    var previous: ?[]const u8 = null;
    var previous_key: [key_bytes]u8 = undefined;
    var count: u64 = 0;
    var matching: u64 = 0;
    var row = try cursor.seekAtOrAfter(prefix);
    while (row) |entry| : (row = try cursor.next()) {
        if (!std.mem.startsWith(u8, entry.key, prefix)) break;
        try context.ensureActive();
        if (entry.value.len != 32) return invalid;
        const table = try edgeTable(entry.key, prefix, lanes == .active);
        if (previous) |key| if (std.mem.order(u8, key, entry.key) != .lt) return invalid;
        @memcpy(previous_key[0..entry.key.len], entry.key);
        previous = previous_key[0..entry.key.len];
        count = std.math.add(u64, count, 1) catch return invalid;
        var forward_buffer: [key_bytes]u8 = undefined;
        const bytes = (try optionalGet(txn, try tableKey(&forward_buffer, group, table))) orelse return invalid;
        const forward = try decode(a, bytes);
        defer a.free(forward);
        const reference = find(forward, parent) orelse return invalid;
        if (laneKey(reference.lanes) != entry.key[entry.key.len - 9] or !std.mem.eql(u8, entry.value, &reference.fingerprint(table))) return invalid;
        if (@backingInt(reference.lanes) & @backingInt(lanes) == 0) continue;
        matching += 1;
        if (excluded != null and table == excluded.?) continue;
        if (tables.items.len == limit) return error.SqlProgramLimitExceeded;
        try tables.append(a, table);
    }
    if (count != (if (lanes == .active) captured.active_count else captured.count)) return invalid;
    std.mem.sort(u64, tables.items, {}, std.sort.asc(u64));
    return .{ .tables = try tables.toOwnedSlice(a), .stamp = captured, .matching = matching };
}

fn rootPrefix(buffer: []u8, group: u64, lane: []const u8) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:fk-{s}:", .{ group, lane });
}
fn tableFromKey(bytes: []const u8, group: u64) !?u64 {
    var buffer: [192]u8 = undefined;
    const prefix = try rootPrefix(&buffer, group, "table");
    if (!std.mem.startsWith(u8, bytes, prefix)) return null;
    const tail = bytes[prefix.len..];
    if (tail.len == 0 or tail[0] == '0') return invalid;
    for (tail) |byte| if (byte < '0' or byte > '9') return invalid;
    return std.fmt.parseInt(u64, tail, 10) catch invalid;
}
fn decodeParent(buffer: []u8, bytes: []const u8) ![]const u8 {
    if (bytes.len == 0 or bytes.len > 2 * max_name_bytes or bytes.len % 2 != 0) return invalid;
    // Require the canonical lowercase spelling used by the writer.
    for (bytes) |byte| if (!((byte >= '0' and byte <= '9') or (byte >= 'a' and byte <= 'f'))) return invalid;
    const result = std.fmt.hexToBytes(buffer, bytes) catch return invalid;
    if (!nameValid(result)) return invalid;
    return result;
}
/// Partial directories are permitted during catalog adoption, but every stored
/// row must already be exact. This proves both directions and each summary,
/// using bounded per-table memory. Completeness is a separate source-epoch seal;
/// an absent forward row must never imply that a legacy table has no FKs.
pub fn verifyDirectory(backing: A, reader: anytype, group: u64, rows: anytype, comptime table_key: anytype, comptime table_prefix: anytype, comptime decode_table: anytype, cancelled: ?*const std.atomic.Value(bool)) !void {
    const coverage = try progress(reader, group);
    var source_buffer: [192]u8 = undefined;
    const source_prefix = try table_prefix(&source_buffer, group);
    var covered_buffer: [192]u8 = undefined;
    const covered = if (coverage.after_table == 0) source_prefix else try table_key(&covered_buffer, group, coverage.after_table);
    // A durable cursor is evidence about a contiguous source prefix. Recovery
    // verifies it even before completion; no empty index can fabricate coverage.
    if (coverage.revision != 0) {
        try rows.reset();
        while (try rows.next()) |row| {
            if (cancelled) |flag| if (flag.load(.acquire)) return error.SnapshotBuildCancelled;
            if (!std.mem.startsWith(u8, row.key, source_prefix)) continue;
            if (!coverage.complete and std.mem.order(u8, row.key, covered) == .gt) continue;
            var arena = std.heap.ArenaAllocator.init(backing);
            defer arena.deinit();
            const a = arena.allocator();
            const source = try decode_table(a, row.value);
            const expected = try fromSchemas(a, source.schema_json, source.read_schema_json);
            var buffer: [key_bytes]u8 = undefined;
            const actual = try optionalGet(reader, try tableKey(&buffer, group, source.table_id));
            if (expected.len == 0) {
                if (actual != null) return invalid;
            } else if (actual == null or !std.mem.eql(u8, try encode(a, expected), actual.?)) return invalid;
        }
    }
    var expected_edges: u64 = 0;
    var expected_active: u64 = 0;
    try rows.reset();
    while (try rows.next()) |row| {
        if (cancelled) |flag| if (flag.load(.acquire)) return error.SnapshotBuildCancelled;
        const table = (try tableFromKey(row.key, group)) orelse continue;
        var arena = std.heap.ArenaAllocator.init(backing);
        defer arena.deinit();
        const a = arena.allocator();
        const actual = try decode(a, row.value);
        var buffer: [key_bytes]u8 = undefined;
        const raw = (try optionalGet(reader, try table_key(&buffer, group, table))) orelse return invalid;
        const source = try decode_table(a, raw);
        if (source.table_id != table) return invalid;
        const expected = try fromSchemas(a, source.schema_json, source.read_schema_json);
        if (expected.len == 0 or !std.mem.eql(u8, try encode(a, expected), row.value)) return invalid;
        for (actual) |item| {
            if (item.lanes != .retained) expected_active = std.math.add(u64, expected_active, 1) catch return invalid;
            const reverse = (try optionalGet(reader, try edgeKeyForLanes(&buffer, group, item.parent, table, item.lanes))) orelse return invalid;
            if (!std.mem.eql(u8, reverse, &item.fingerprint(table))) return invalid;
        }
        expected_edges = std.math.add(u64, expected_edges, actual.len) catch return invalid;
    }
    var prefix_buffer: [192]u8 = undefined;
    const reverse_prefix = try rootPrefix(&prefix_buffer, group, "dependent");
    var actual_edges: u64 = 0;
    var actual_active: u64 = 0;
    var parent_buffer: [max_name_bytes]u8 = undefined;
    var previous: [max_name_bytes]u8 = undefined;
    var previous_len: usize = 0;
    var parent_count: u64 = 0;
    var parent_active: u64 = 0;
    try rows.reset();
    while (try rows.next()) |row| {
        if (cancelled) |flag| if (flag.load(.acquire)) return error.SnapshotBuildCancelled;
        if (!std.mem.startsWith(u8, row.key, reverse_prefix)) continue;
        const tail = row.key[reverse_prefix.len..];
        const separator = std.mem.indexOfScalar(u8, tail, ':') orelse return invalid;
        if (tail.len != separator + 10 or row.value.len != 32 or (tail[separator + 1] != 1 and tail[separator + 1] != 2) or std.mem.readInt(u64, tail[separator + 2 ..][0..8], .big) == 0) return invalid;
        const parent = try decodeParent(&parent_buffer, tail[0..separator]);
        if (!std.mem.eql(u8, previous[0..previous_len], parent)) {
            if (previous_len != 0) {
                const observed = try stamp(reader, group, previous[0..previous_len]);
                if (observed.count != parent_count or observed.active_count != parent_active) return invalid;
            }
            @memcpy(previous[0..parent.len], parent);
            previous_len = parent.len;
            parent_count = 0;
            parent_active = 0;
        }
        parent_count += 1;
        if (tail[separator + 1] == 1) {
            parent_active += 1;
            actual_active = std.math.add(u64, actual_active, 1) catch return invalid;
        }
        actual_edges = std.math.add(u64, actual_edges, 1) catch return invalid;
    }
    if (previous_len != 0) {
        const observed = try stamp(reader, group, previous[0..previous_len]);
        if (observed.count != parent_count or observed.active_count != parent_active) return invalid;
    }
    if (expected_edges != actual_edges or expected_active != actual_active) return invalid;
    var summary_buffer: [192]u8 = undefined;
    const summary_prefix = try rootPrefix(&summary_buffer, group, "summary");
    var summary_edges: u64 = 0;
    var summary_active: u64 = 0;
    try rows.reset();
    while (try rows.next()) |row| {
        if (cancelled) |flag| if (flag.load(.acquire)) return error.SnapshotBuildCancelled;
        if (!std.mem.startsWith(u8, row.key, summary_prefix)) continue;
        _ = try decodeParent(&parent_buffer, row.key[summary_prefix.len..]);
        const value = try Stamp.decode(row.value);
        summary_edges = std.math.add(u64, summary_edges, value.count) catch return invalid;
        summary_active = std.math.add(u64, summary_active, value.active_count) catch return invalid;
    }
    if (summary_edges != expected_edges or summary_active != expected_active) return invalid;
}

test "incoming FK directory canonical frames retain both layout lifetimes and physical names" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const source = try fromSchemas(arena.allocator(),
        \\{"foreign_keys":[{"parent_table":"z:parent"},{"parent_table":"a"},{"parent_table":"a"}],"unrelated":0.12345678901234567890123456789}
    ,
        \\{"foreign_keys":[{"parent_table":"old"},{"parent_table":"a"}]}
    );
    try std.testing.expectEqual(@as(usize, 3), source.len);
    try std.testing.expectEqualStrings("a", source[0].parent);
    try std.testing.expectEqual(Lanes.both, source[0].lanes);
    try std.testing.expectEqual(Lanes.retained, source[1].lanes);
    const bytes = try encode(a, source);
    defer a.free(bytes);
    const restored = try decode(a, bytes);
    defer a.free(restored);
    for (source, restored) |left, right| {
        try std.testing.expectEqualStrings(left.parent, right.parent);
        try std.testing.expectEqual(left.lanes, right.lanes);
        try std.testing.expectEqual(left.fingerprint(7), right.fingerprint(7));
        try std.testing.expect(!std.mem.eql(u8, &left.fingerprint(7), &left.fingerprint(8)));
    }
    for (0..bytes.len) |end| try std.testing.expectError(error.InvalidForeignKeyDependencies, decode(a, bytes[0..end]));
    var reversed = [_]Reference{ source[1], source[0] };
    try std.testing.expectError(error.InvalidForeignKeyDependencies, encode(a, &reversed));
    var buffer: [key_bytes]u8 = undefined;
    const key = try edgeKey(&buffer, std.math.maxInt(u64), "z:parent", std.math.maxInt(u64));
    var other: [key_bytes]u8 = undefined;
    try std.testing.expect(!std.mem.startsWith(u8, key, try dependentPrefix(&other, std.math.maxInt(u64), "z")));
    const max_name: [max_name_bytes]u8 = @splat('a');
    try std.testing.expect((try edgeKey(&buffer, std.math.maxInt(u64), &max_name, std.math.maxInt(u64))).len <= key_bytes);
    try std.testing.expectError(error.InvalidForeignKeyDependencies, dependentPrefix(&buffer, 1, "a\x00b"));
    try std.testing.expectError(error.InvalidForeignKeyDependencies, dependentPrefix(&buffer, 1, "\xff"));
}

fn preparedAllocationTest(a: A) !void {
    const Empty = struct {
        pub fn get(_: *@This(), _: []const u8) anyerror![]const u8 {
            return error.NotFound;
        }
    };
    var reader: Empty = .{};
    var prepared = (try prepare(a, &reader, 1, 7,
        \\{"foreign_keys":[{"parent_table":"parent"},{"parent_table":"other"}]}
    ,
        \\{"foreign_keys":[{"parent_table":"parent"}]}
    )).?;
    defer prepared.deinit();
    try std.testing.expectEqual(@as(usize, 2), prepared.changes.len);
    try std.testing.expectEqual(Lanes.both, prepared.changes[1].after.?.lanes);
}
test "incoming FK directory preparation owns all frames and cleans up every allocation failure" {
    try preparedAllocationTest(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, preparedAllocationTest, .{});
}

pub const BackfillCut = @import("antfly_local_sources").system_catalog_relation_reconciliation.ForeignKeyBackfill;
pub fn progressKey(buffer: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:fk-coverage", .{group});
}
pub const Progress = struct {
    revision: u64 = 0,
    after_table: u64 = 0,
    complete: bool = false,
    pub const encoded_len = 23;
    pub fn cut(self: Progress) BackfillCut {
        return .{ .revision = self.revision, .after_table = self.after_table };
    }
    pub fn encode(self: Progress) ![encoded_len]u8 {
        if (self.revision == 0) return invalid;
        try self.cut().validate();
        var bytes: [encoded_len]u8 = undefined;
        bytes[0..6].* = "AFKP01".*;
        std.mem.writeInt(u64, bytes[6..14], self.revision, .big);
        std.mem.writeInt(u64, bytes[14..22], self.after_table, .big);
        bytes[22] = @intFromBool(self.complete);
        return bytes;
    }
    pub fn decode(bytes: []const u8) !Progress {
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..6], "AFKP01") or bytes[22] > 1) return invalid;
        const result: Progress = .{
            .revision = std.mem.readInt(u64, bytes[6..14], .big),
            .after_table = std.mem.readInt(u64, bytes[14..22], .big),
            .complete = bytes[22] == 1,
        };
        _ = try result.encode();
        return result;
    }
};
pub fn progress(reader: anytype, group: u64) !Progress {
    var buffer: [192]u8 = undefined;
    return if (try optionalGet(reader, try progressKey(&buffer, group))) |bytes| try Progress.decode(bytes) else .{};
}
/// Exact native references are checked before selecting a lane. A read layout
/// alone must not resurrect a logically retired FK in SQL DROP admission.
pub fn firstDependent(backing: A, txn: anytype, group: u64, parent: []const u8, excluded: ?u64, lanes: Lanes) !?u64 {
    const First = struct {
        table: ?u64 = null,
        pub fn inspect(self: *@This(), table: u64) !bool {
            self.table = table;
            return false;
        }
    };
    var first: First = .{};
    _ = try visitDependents(backing, txn, group, parent, excluded, lanes, &first);
    return first.table;
}
/// Streaming read-only visitor: no result-array limit and one retained scratch
/// frame. Returning false stops at the first incompatible dependent. Visitors
/// must not mutate the transaction while its source cursor is open.
pub fn visitDependents(backing: A, txn: anytype, group: u64, parent: []const u8, excluded: ?u64, lanes: Lanes, visitor: anytype) !bool {
    if (!(try progress(txn, group)).complete) return error.CatalogDependencyCoverageRequired;
    var buffer: [key_bytes]u8 = undefined;
    const prefix = try selectedPrefix(&buffer, group, parent, lanes);
    var cursor = try txn.openCursor();
    defer cursor.close();
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    var row = try cursor.seekAtOrAfter(prefix);
    while (row) |entry| : (row = try cursor.next()) {
        if (!std.mem.startsWith(u8, entry.key, prefix)) break;
        if (entry.value.len != 32) return invalid;
        const table = try edgeTable(entry.key, prefix, lanes == .active);
        _ = arena.reset(.retain_capacity);
        var forward_buffer: [key_bytes]u8 = undefined;
        const frame = (try optionalGet(txn, try tableKey(&forward_buffer, group, table))) orelse return invalid;
        const reference = find(try decode(arena.allocator(), frame), parent) orelse return invalid;
        if (laneKey(reference.lanes) != entry.key[entry.key.len - 9] or !std.mem.eql(u8, entry.value, &reference.fingerprint(table))) return invalid;
        if (excluded != null and excluded.? == table) continue;
        if (@backingInt(reference.lanes) & @backingInt(lanes) != 0 and !try visitor.inspect(table)) return false;
    }
    return true;
}
