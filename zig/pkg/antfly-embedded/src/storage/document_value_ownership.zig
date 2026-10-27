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

//! Row-local immutable-value ownership. The current row and retained/history
//! leases share values without copying them; no global digest counter exists.
//! All operations must run in the same KV transaction as their publication.
const std = @import("std");
const A = std.mem.Allocator;
pub const Digest = [32]u8;
pub const value_prefix = "\x00\x00__metadata__:document_value_v1:";
pub const reference_prefix = "\x00\x00__metadata__:document_value_refs_v1:";
pub const current_prefix = "\x00\x00__metadata__:document_current_values_v1:";
pub const ValueKey = [value_prefix.len + 64]u8;
const ReferenceKey = [reference_prefix.len + 64]u8;
const CurrentKey = [current_prefix.len + 32]u8;
pub const Value = struct { digest: Digest, length: usize, bytes: ?[]const u8 = null };
pub const Delta = struct { written: usize = 0, reused: usize = 0, retired: usize = 0, removed_current: usize = 0 };
pub fn valueKey(physical: []const u8, digest: Digest) ValueKey {
    var out: ValueKey = undefined;
    ownerKey(value_prefix, physical, out[0 .. value_prefix.len + 32]);
    @memcpy(out[value_prefix.len + 32 ..], &digest);
    return out;
}
fn referenceKey(physical: []const u8, digest: Digest) ReferenceKey {
    var out: ReferenceKey = undefined;
    ownerKey(reference_prefix, physical, out[0 .. reference_prefix.len + 32]);
    @memcpy(out[reference_prefix.len + 32 ..], &digest);
    return out;
}
fn currentKey(physical: []const u8) CurrentKey {
    var out: CurrentKey = undefined;
    ownerKey(current_prefix, physical, &out);
    return out;
}
fn ownerKey(comptime prefix: []const u8, physical: []const u8, out: []u8) void {
    @memcpy(out[0..prefix.len], prefix);
    std.crypto.hash.sha2.Sha256.hash(physical, out[prefix.len..][0..32], .{});
}
fn checksum(bytes: []const u8) Digest {
    var out: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &out, .{});
    return out;
}
fn get(reader: anytype, name: []const u8) ![]const u8 {
    const T = switch (@typeInfo(@TypeOf(reader))) {
        .pointer => |p| p.child,
        else => @TypeOf(reader),
    };
    if (comptime @hasDecl(T, "getPhysical")) return reader.getPhysical(name);
    return reader.get(name);
}
const Reference = struct {
    current: bool,
    pins: u64,
    indexed_pins: u64 = 0,
    length: usize,
    fn encode(self: Reference) [53]u8 {
        std.debug.assert(self.indexed_pins == 0);
        var out: [53]u8 = undefined;
        @memcpy(out[0..4], "DVR1");
        out[4] = @intFromBool(self.current);
        std.mem.writeInt(u64, out[5..13], self.pins, .little);
        std.mem.writeInt(u64, out[13..21], self.length, .little);
        @memcpy(out[21..53], &checksum(out[0..21]));
        return out;
    }
    fn encodeIndexed(self: Reference) [61]u8 {
        std.debug.assert(self.indexed_pins != 0);
        var out: [61]u8 = undefined;
        @memcpy(out[0..4], "DVR2");
        out[4] = @intFromBool(self.current);
        std.mem.writeInt(u64, out[5..13], self.pins, .little);
        std.mem.writeInt(u64, out[13..21], self.indexed_pins, .little);
        std.mem.writeInt(u64, out[21..29], self.length, .little);
        @memcpy(out[29..61], &checksum(out[0..29]));
        return out;
    }
    fn decode(raw: []const u8) !Reference {
        const indexed = raw.len == 61 and std.mem.eql(u8, raw[0..4], "DVR2");
        if ((!indexed and (raw.len != 53 or !std.mem.eql(u8, raw[0..4], "DVR1"))) or raw[4] > 1) return error.InvalidDocumentOwnership;
        const footer: usize = if (indexed) 29 else 21;
        if (!std.mem.eql(u8, raw[footer..], &checksum(raw[0..footer]))) return error.InvalidDocumentOwnership;
        const pins = std.mem.readInt(u64, raw[5..13], .little);
        const indexed_pins = if (indexed) std.mem.readInt(u64, raw[13..21], .little) else 0;
        const length = std.math.cast(usize, std.mem.readInt(u64, raw[footer - 8 ..][0..8], .little)) orelse return error.InvalidDocumentOwnership;
        if (length == 0 or (indexed and indexed_pins == 0) or (raw[4] == 0 and pins == 0 and indexed_pins == 0)) return error.InvalidDocumentOwnership;
        return .{ .current = raw[4] == 1, .pins = pins, .indexed_pins = indexed_pins, .length = length };
    }
};
fn load(writer: anytype, physical: []const u8, digest: Digest) !?Reference {
    const raw = get(writer, &referenceKey(physical, digest)) catch |err| return if (err == error.NotFound) null else err;
    return try Reference.decode(raw);
}
fn save(writer: anytype, physical: []const u8, digest: Digest, reference: Reference) !void {
    if (reference.pins == 0 and reference.indexed_pins == 0) {
        if (!reference.current) try writer.delete(&valueKey(physical, digest));
        // Current membership already owns the value. A separate record exists
        // only while history pins it (legacy zero-pin records are retired here).
        try writer.delete(&referenceKey(physical, digest));
    } else if (reference.indexed_pins != 0) {
        try writer.put(&referenceKey(physical, digest), &reference.encodeIndexed());
    } else try writer.put(&referenceKey(physical, digest), &reference.encode());
}
fn order(_: void, l: Value, r: Value) bool {
    return std.mem.order(u8, &l.digest, &r.digest) == .lt;
}
fn unique(a: A, input: []const Value) ![]Value {
    if (input.len > 512) return error.InvalidDocumentOwnership;
    const copy = try a.dupe(Value, input);
    errdefer a.free(copy);
    std.mem.sort(Value, copy, {}, order);
    var count: usize = 0;
    for (copy) |value| {
        if (value.length == 0 or (value.bytes != null and value.bytes.?.len != value.length)) return error.InvalidDocumentOwnership;
        if (count != 0 and std.mem.eql(u8, &copy[count - 1].digest, &value.digest)) {
            if (copy[count - 1].length != value.length) return error.InvalidDocumentOwnership;
            continue;
        }
        copy[count] = value;
        count += 1;
    }
    const out = try a.dupe(Value, copy[0..count]);
    a.free(copy);
    return out;
}
fn find(values: []const Value, digest: Digest) ?Value {
    var low: usize = 0;
    var high = values.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        switch (std.mem.order(u8, &digest, &values[mid].digest)) {
            .lt => high = mid,
            .gt => low = mid + 1,
            .eq => return values[mid],
        }
    }
    return null;
}
/// Current membership is atomic ownership metadata, unlike the disposable
/// physical-layout bit in a logical-authority cache.
pub fn hasCurrent(writer: anytype, physical: []const u8) !bool {
    const raw = get(writer, &currentKey(physical)) catch |err| return if (err == error.NotFound) false else err;
    if (raw.len < 38 or !std.mem.eql(u8, raw[0..4], "DVC1") or !std.mem.eql(u8, raw[raw.len - 32 ..], &checksum(raw[0 .. raw.len - 32]))) return error.InvalidDocumentOwnership;
    const count = std.mem.readInt(u16, raw[4..6], .little);
    if (count == 0 or count > 512 or raw.len != 38 + @as(usize, count) * 40) return error.InvalidDocumentOwnership;
    return true;
}
fn current(a: A, writer: anytype, physical: []const u8) ![]Value {
    const raw = get(writer, &currentKey(physical)) catch |err| return if (err == error.NotFound) try a.alloc(Value, 0) else err;
    if (raw.len < 38 or !std.mem.eql(u8, raw[0..4], "DVC1") or !std.mem.eql(u8, raw[raw.len - 32 ..], &checksum(raw[0 .. raw.len - 32]))) return error.InvalidDocumentOwnership;
    const count = std.mem.readInt(u16, raw[4..6], .little);
    if (count == 0 or count > 512 or raw.len != 38 + @as(usize, count) * 40) return error.InvalidDocumentOwnership;
    const out = try a.alloc(Value, count);
    errdefer a.free(out);
    for (out, 0..) |*value, index| {
        const offset = 6 + index * 40;
        value.* = .{ .digest = raw[offset..][0..32].*, .length = std.math.cast(usize, std.mem.readInt(u64, raw[offset + 32 ..][0..8], .little)) orelse return error.InvalidDocumentOwnership };
        if (value.length == 0 or (index != 0 and !order({}, out[index - 1], value.*))) return error.InvalidDocumentOwnership;
    }
    return out;
}
fn saveCurrent(a: A, writer: anytype, physical: []const u8, values: []const Value) !void {
    if (values.len == 0) {
        writer.delete(&currentKey(physical)) catch |err| if (err != error.NotFound) return err;
        return;
    }
    const raw = try a.alloc(u8, 38 + values.len * 40);
    defer a.free(raw);
    @memcpy(raw[0..4], "DVC1");
    std.mem.writeInt(u16, raw[4..6], @intCast(values.len), .little);
    for (values, 0..) |value, index| {
        const offset = 6 + index * 40;
        @memcpy(raw[offset..][0..32], &value.digest);
        std.mem.writeInt(u64, raw[offset + 32 ..][0..8], value.length, .little);
    }
    @memcpy(raw[raw.len - 32 ..], &checksum(raw[0 .. raw.len - 32]));
    try writer.put(&currentKey(physical), raw);
}
/// No payload reads. The caller supplies new validated bytes only when a value
/// has never existed in this row's current or pinned history.
pub fn publishCurrent(a: A, writer: anytype, physical: []const u8, input: []const Value) !Delta {
    const next = try unique(a, input);
    defer a.free(next);
    const before = try current(a, writer, physical);
    defer a.free(before);
    var delta: Delta = .{};
    var changed = before.len != next.len;
    for (next) |value| {
        if (find(before, value.digest)) |previous| {
            if (previous.length != value.length) return error.InvalidDocumentOwnership;
            delta.reused += value.length;
            continue;
        }
        changed = true;
        var reference = (try load(writer, physical, value.digest)) orelse Reference{ .current = false, .pins = 0, .length = value.length };
        if (reference.current or reference.length != value.length) return error.InvalidDocumentOwnership;
        if (reference.pins == 0 and reference.indexed_pins == 0) {
            try writer.put(&valueKey(physical, value.digest), value.bytes orelse return error.DocumentValueUnavailable);
            delta.written += value.length;
        } else delta.reused += value.length;
        reference.current = true;
        if (reference.pins != 0 or reference.indexed_pins != 0) try save(writer, physical, value.digest, reference);
    }
    for (before) |value| if (find(next, value.digest) == null) {
        changed = true;
        delta.removed_current += 1;
        if (try load(writer, physical, value.digest)) |loaded| {
            var reference = loaded;
            if (!reference.current or reference.length != value.length) return error.InvalidDocumentOwnership;
            reference.current = false;
            try save(writer, physical, value.digest, reference);
            delta.retired += @intFromBool(reference.pins == 0 and reference.indexed_pins == 0);
        } else {
            try writer.delete(&valueKey(physical, value.digest));
            delta.retired += 1;
        }
    };
    if (changed) try saveCurrent(a, writer, physical, next);
    return delta;
}
/// Returns bytes newly charged to the unique pinned-value budget. Repeated
/// references from later afterimages retain only another small local lease.
pub fn acquirePin(a: A, writer: anytype, physical: []const u8, input: []const Value) !u64 {
    const values = try unique(a, input);
    defer a.free(values);
    const members = try current(a, writer, physical);
    defer a.free(members);
    var charged: u64 = 0;
    for (values) |value| {
        const member = find(members, value.digest) orelse return error.DocumentValueUnavailable;
        if (member.length != value.length) return error.InvalidDocumentOwnership;
        var reference = (try load(writer, physical, value.digest)) orelse Reference{ .current = true, .pins = 0, .length = value.length };
        if (!reference.current) return error.InvalidDocumentOwnership;
        if (reference.length != value.length) return error.InvalidDocumentOwnership;
        if (reference.pins == 0) charged = try std.math.add(u64, charged, value.length);
        reference.pins = try std.math.add(u64, reference.pins, 1);
        try save(writer, physical, value.digest, reference);
    }
    return charged;
}
/// Returns bytes no longer charged to the pinned-value budget. Current values
/// stay alive; replaced/deleted values are collected only after the final pin.
pub fn releasePin(a: A, writer: anytype, physical: []const u8, input: []const Value) !u64 {
    const values = try unique(a, input);
    defer a.free(values);
    var released: u64 = 0;
    for (values) |value| {
        var reference = (try load(writer, physical, value.digest)) orelse return error.InvalidDocumentOwnership;
        if (reference.pins == 0 or reference.length != value.length) return error.InvalidDocumentOwnership;
        reference.pins -= 1;
        if (reference.pins == 0) released = try std.math.add(u64, released, value.length);
        try save(writer, physical, value.digest, reference);
    }
    return released;
}
/// Index and history retention have independent accounting domains. A history
/// frame can release its complete charge while an index still owns the bytes.
/// This avoids leaking the history budget after a source-claim handoff. These
/// counts never authorize a source revision: callers first verify a current
/// manifest or a live named history/index lease. Requires reader format 27.
pub fn acquireIndexedPin(a: A, writer: anytype, physical: []const u8, input: []const Value) !u64 {
    const values = try unique(a, input);
    defer a.free(values);
    const members = try current(a, writer, physical);
    defer a.free(members);
    var charged: u64 = 0;
    for (values) |value| {
        const loaded = try load(writer, physical, value.digest);
        var reference = loaded orelse blk: {
            const member = find(members, value.digest) orelse return error.DocumentValueUnavailable;
            if (member.length != value.length) return error.InvalidDocumentOwnership;
            break :blk Reference{ .current = true, .pins = 0, .length = value.length };
        };
        if (reference.length != value.length) return error.InvalidDocumentOwnership;
        if (reference.indexed_pins == 0) charged = try std.math.add(u64, charged, value.length);
        reference.indexed_pins = try std.math.add(u64, reference.indexed_pins, 1);
        try save(writer, physical, value.digest, reference);
    }
    return charged;
}

/// Install ownership for immutable values imported from a verified source
/// recipe. The caller retains that source's native reader lease. Existing
/// destination values are shared; missing values are authenticated individually
/// and copied without flattening the document or changing current membership.
pub fn importIndexedPin(a: A, writer: anytype, source: anytype, physical: []const u8, input: []const Value) !u64 {
    errdefer writer.poison();
    const values = try unique(a, input);
    defer a.free(values);
    const members = try current(a, writer, physical);
    defer a.free(members);
    var charged: u64 = 0;
    for (values) |value| {
        var reference = (try load(writer, physical, value.digest)) orelse blk: {
            if (find(members, value.digest)) |member| {
                if (member.length != value.length) return error.InvalidDocumentOwnership;
                break :blk Reference{ .current = true, .pins = 0, .length = value.length };
            }
            const bytes = try get(source, &valueKey(physical, value.digest));
            if (bytes.len != value.length or !std.mem.eql(u8, &checksum(bytes), &value.digest)) return error.InvalidDocumentOwnership;
            try writer.put(&valueKey(physical, value.digest), bytes);
            break :blk Reference{ .current = false, .pins = 0, .length = value.length };
        };
        if (reference.length != value.length) return error.InvalidDocumentOwnership;
        if (reference.indexed_pins == 0) charged = try std.math.add(u64, charged, value.length);
        reference.indexed_pins = try std.math.add(u64, reference.indexed_pins, 1);
        try save(writer, physical, value.digest, reference);
    }
    return charged;
}

pub fn releaseIndexedPin(a: A, writer: anytype, physical: []const u8, input: []const Value) !u64 {
    const values = try unique(a, input);
    defer a.free(values);
    var released: u64 = 0;
    for (values) |value| {
        var reference = (try load(writer, physical, value.digest)) orelse return error.InvalidDocumentOwnership;
        if (reference.indexed_pins == 0 or reference.length != value.length) return error.InvalidDocumentOwnership;
        reference.indexed_pins -= 1;
        if (reference.indexed_pins == 0) released = try std.math.add(u64, released, value.length);
        try save(writer, physical, value.digest, reference);
    }
    return released;
}

pub fn privatePrefix(candidate: []const u8) ?[]const u8 {
    for ([_][]const u8{ value_prefix, reference_prefix, current_prefix }) |prefix| if (std.mem.startsWith(u8, candidate, prefix)) return prefix;
    return null;
}

test "document payload ownership shares pins and preserves rollback cleanup under allocation failures" {
    const Fixture = struct {
        const Store = struct {
            a: A,
            map: std.StringHashMap([]u8),
            payload_reads: usize = 0,
            payload_writes: usize = 0,
            pub fn get(self: *@This(), name: []const u8) ![]const u8 {
                if (std.mem.startsWith(u8, name, value_prefix)) self.payload_reads += 1;
                return self.map.get(name) orelse error.NotFound;
            }
            pub fn put(self: *@This(), name: []const u8, bytes: []const u8) !void {
                const value = try self.a.dupe(u8, bytes);
                errdefer self.a.free(value);
                const key = try self.a.dupe(u8, name);
                errdefer self.a.free(key);
                const entry = try self.map.getOrPut(key);
                if (entry.found_existing) {
                    self.a.free(key);
                    self.a.free(entry.value_ptr.*);
                }
                entry.value_ptr.* = value;
                if (std.mem.startsWith(u8, name, value_prefix)) self.payload_writes += 1;
            }
            pub fn delete(self: *@This(), name: []const u8) !void {
                if (self.map.fetchRemove(name)) |entry| {
                    self.a.free(entry.key);
                    self.a.free(entry.value);
                }
            }
            fn deinit(self: *@This()) void {
                var iter = self.map.iterator();
                while (iter.next()) |entry| {
                    self.a.free(entry.key_ptr.*);
                    self.a.free(entry.value_ptr.*);
                }
                self.map.deinit();
            }
        };
        fn run(a: A) !void {
            var store: Store = .{ .a = a, .map = .init(a) };
            defer store.deinit();
            const bytes = "immutable";
            const digest = checksum(bytes);
            const input: []const Value = &.{.{ .digest = digest, .length = bytes.len, .bytes = bytes }};
            const row = "physical-row";
            const delta = try publishCurrent(a, &store, row, input);
            try std.testing.expectEqual(bytes.len, delta.written);
            try std.testing.expectEqual(@as(usize, 2), store.map.count());
            try std.testing.expectEqual(@as(u64, bytes.len), try acquirePin(a, &store, row, input));
            try std.testing.expectEqual(@as(u64, 0), try acquirePin(a, &store, row, input));
            _ = try publishCurrent(a, &store, row, &.{});
            try std.testing.expectEqual(@as(u64, 0), try releasePin(a, &store, row, input));
            // Reattach from pinned history without supplying or fetching bytes.
            _ = try publishCurrent(a, &store, row, &.{.{ .digest = digest, .length = bytes.len }});
            try std.testing.expectEqual(@as(u64, bytes.len), try releasePin(a, &store, row, input));
            try std.testing.expectEqual(@as(usize, 2), store.map.count());
            try std.testing.expectEqual(@as(usize, 0), store.payload_reads);
            try std.testing.expectEqual(@as(usize, 1), store.payload_writes);
            _ = try publishCurrent(a, &store, row, &.{});
            try std.testing.expectEqual(@as(usize, 0), store.map.count());
            // History and index quotas have separate charges while sharing
            // exactly one stored value. Releasing one domain cannot collect
            // the other domain's bytes or strand its quota.
            _ = try publishCurrent(a, &store, row, input);
            try std.testing.expectEqual(@as(u64, bytes.len), try acquirePin(a, &store, row, input));
            try std.testing.expectEqual(@as(u64, bytes.len), try acquireIndexedPin(a, &store, row, input));
            try std.testing.expectEqual(@as(u64, 0), try acquireIndexedPin(a, &store, row, input));
            _ = try publishCurrent(a, &store, row, &.{});
            try std.testing.expectEqual(@as(u64, bytes.len), try releasePin(a, &store, row, input));
            try std.testing.expectEqualStrings(bytes, store.map.get(&valueKey(row, digest)).?);
            try std.testing.expectEqual(@as(u64, 0), try releaseIndexedPin(a, &store, row, input));
            try std.testing.expectEqual(@as(u64, bytes.len), try releaseIndexedPin(a, &store, row, input));
            try std.testing.expectEqual(@as(usize, 0), store.map.count());
            _ = try publishCurrent(a, &store, row, input);
            _ = try acquirePin(a, &store, row, input);
            _ = try acquireIndexedPin(a, &store, row, input);
            _ = try publishCurrent(a, &store, row, &.{});
            try std.testing.expectEqual(@as(u64, bytes.len), try releaseIndexedPin(a, &store, row, input));
            try std.testing.expectEqualStrings(bytes, store.map.get(&valueKey(row, digest)).?);
            try std.testing.expectEqual(@as(u64, bytes.len), try releasePin(a, &store, row, input));
            try std.testing.expectEqual(@as(usize, 0), store.map.count());
            const encoded = (Reference{ .current = true, .pins = 1, .length = bytes.len }).encode();
            try std.testing.expectEqual(@as(u64, 1), (try Reference.decode(&encoded)).pins);
            var corrupt = encoded;
            corrupt[20] ^= 1;
            try std.testing.expectError(error.InvalidDocumentOwnership, Reference.decode(&corrupt));
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}
