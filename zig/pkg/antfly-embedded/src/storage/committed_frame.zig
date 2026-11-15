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

//! Versioned, bounded committed tuple images. Storage writes descriptors and
//! chunks in the same transaction as base rows. Consumers fetch only chunks
//! needed by their current page and verify each before exposing its bytes.
const std = @import("std");
const A = std.mem.Allocator;
pub const Digest = [32]u8;
pub const Namespace = [24]u8;
/// Private whole-transaction evidence. Frames alone prove only local effects.
pub const completion = @import("transaction_completion.zig");
test "committed frame completion contract discovery" {
    _ = completion;
}
pub const chunk_bytes = 1024 * 1024;
pub const max_bytes = 256 * 1024 * 1024;
pub const max_events = 65536;
pub const max_schema_bytes = 1024 * 1024;
const header_bytes = 112;
const absent = std.math.maxInt(u32);

pub fn digest(bytes: []const u8) Digest {
    var result: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
    return result;
}

pub const Event = struct {
    key: []const u8,
    before: ?[]const u8,
    after: ?[]const u8,
    pub fn operation(self: Event) enum { insert, update, delete } {
        return if (self.before == null) .insert else if (self.after == null) .delete else .update;
    }
};
/// Unknown preserves legacy frames without manufacturing a completion proof.
/// Coordinated frames require an independently retained participant outcome.
pub const TransactionScope = enum(u8) { unknown = 0, root_local = 1, coordinated = 2 };
pub const Header = struct {
    namespace: Namespace,
    sequence: u64,
    transaction_id: [16]u8,
    transaction_scope: TransactionScope = .unknown,
    /// Opaque authoritative source position, never a PostgreSQL LSN.
    source_position: [32]u8 = @splat(0),
    committed_at_ns: i64 = 0,
    schema_before: ?[]const u8 = null,
    schema_after: ?[]const u8 = null,
};
fn length(value: ?[]const u8) !u32 {
    return if (value) |bytes| std.math.cast(u32, bytes.len) orelse error.HistoryTooLarge else absent;
}
pub fn encodedLength(header: Header, events: []const Event) !usize {
    if (header.sequence == 0 or std.mem.allEqual(u8, &header.namespace, 0) or events.len == 0 or events.len > max_events) return error.InvalidCommittedFrame;
    if ((if (header.schema_before) |bytes| bytes.len else 0) > max_schema_bytes or
        (if (header.schema_after) |bytes| bytes.len else 0) > max_schema_bytes) return error.HistorySchemaTooLarge;
    var total: usize = header_bytes + (if (header.schema_before) |bytes| bytes.len else 0) + (if (header.schema_after) |bytes| bytes.len else 0);
    var previous: ?[]const u8 = null;
    for (events) |event| {
        if (event.key.len == 0 or (event.before == null and event.after == null)) return error.InvalidCommittedFrame;
        if (event.key.len > max_bytes or (if (event.before) |v| v.len else 0) > max_bytes or (if (event.after) |v| v.len else 0) > max_bytes) return error.HistoryTooLarge;
        if (previous) |key| if (std.mem.order(u8, key, event.key) != .lt) return error.InvalidCommittedFrame;
        previous = event.key;
        const size = std.math.add(usize, event.key.len, if (event.before) |v| v.len else 0) catch return error.HistoryTooLarge;
        total = std.math.add(usize, total, std.math.add(usize, size, 12 + (if (event.after) |v| v.len else 0)) catch return error.HistoryTooLarge) catch return error.HistoryTooLarge;
        if (total > max_bytes) return error.HistoryTooLarge;
    }
    return total;
}
pub fn encode(a: A, header: Header, events: []const Event) ![]u8 {
    const out = try a.alloc(u8, try encodedLength(header, events));
    errdefer a.free(out);
    @memset(out[0..header_bytes], 0);
    @memcpy(out[0..4], "CHF1");
    out[4] = @backingInt(header.transaction_scope);
    @memcpy(out[8..32], &header.namespace);
    std.mem.writeInt(u64, out[32..40], header.sequence, .little);
    @memcpy(out[40..56], &header.transaction_id);
    @memcpy(out[56..88], &header.source_position);
    std.mem.writeInt(i64, out[88..96], header.committed_at_ns, .little);
    std.mem.writeInt(u32, out[96..100], @intCast(events.len), .little);
    std.mem.writeInt(u32, out[100..104], try length(header.schema_before), .little);
    std.mem.writeInt(u32, out[104..108], try length(header.schema_after), .little);
    var offset: usize = header_bytes;
    for ([_]?[]const u8{ header.schema_before, header.schema_after }) |value| if (value) |raw| {
        @memcpy(out[offset..][0..raw.len], raw);
        offset += raw.len;
    };
    for (events) |event| {
        std.mem.writeInt(u32, out[offset..][0..4], @intCast(event.key.len), .little);
        std.mem.writeInt(u32, out[offset + 4 ..][0..4], try length(event.before), .little);
        std.mem.writeInt(u32, out[offset + 8 ..][0..4], try length(event.after), .little);
        offset += 12;
        for ([_]?[]const u8{ event.key, event.before, event.after }) |value| if (value) |raw| {
            @memcpy(out[offset..][0..raw.len], raw);
            offset += raw.len;
        };
    }
    std.debug.assert(offset == out.len);
    return out;
}

pub const Source = struct {
    context: *anyopaque,
    read: *const fn (*anyopaque, u32) anyerror![]const u8,
};
/// The checksummed descriptor is a small durable root. All slices borrow it.
pub const Descriptor = struct {
    raw: []const u8,
    namespace: Namespace,
    sequence: u64,
    total: u32,
    count: u32,
    events: u32,
    pub fn encode(a: A, namespace: Namespace, sequence: u64, bytes: []const u8) ![]u8 {
        if (bytes.len < header_bytes or bytes.len > max_bytes) return error.InvalidCommittedFrame;
        const chunks = std.math.divCeil(usize, bytes.len, chunk_bytes) catch unreachable;
        const events = std.mem.readInt(u32, bytes[96..100], .little);
        if (events == 0 or events > max_events) return error.InvalidCommittedFrame;
        const out = try a.alloc(u8, 48 + chunks * 32 + @as(usize, events) * 4 + 32);
        errdefer a.free(out);
        @memcpy(out[0..4], "CHD1");
        @memcpy(out[4..28], &namespace);
        std.mem.writeInt(u64, out[28..36], sequence, .little);
        std.mem.writeInt(u32, out[36..40], @intCast(bytes.len), .little);
        std.mem.writeInt(u32, out[40..44], @intCast(chunks), .little);
        std.mem.writeInt(u32, out[44..48], events, .little);
        for (0..chunks) |i| @memcpy(out[48 + i * 32 ..][0..32], &digest(bytes[i * chunk_bytes .. @min(bytes.len, (i + 1) * chunk_bytes)]));
        const before = std.mem.readInt(u32, bytes[100..104], .little);
        const after = std.mem.readInt(u32, bytes[104..108], .little);
        if ((before != absent and before > max_schema_bytes) or (after != absent and after > max_schema_bytes)) return error.InvalidCommittedFrame;
        var offset: usize = header_bytes + @as(usize, if (before == absent) 0 else before) + @as(usize, if (after == absent) 0 else after);
        for (0..events) |ordinal| {
            if (offset > bytes.len or bytes.len - offset < 12) return error.InvalidCommittedFrame;
            std.mem.writeInt(u32, out[48 + chunks * 32 + ordinal * 4 ..][0..4], @intCast(offset), .little);
            const key_size = std.mem.readInt(u32, bytes[offset..][0..4], .little);
            const before_size = std.mem.readInt(u32, bytes[offset + 4 ..][0..4], .little);
            const after_size = std.mem.readInt(u32, bytes[offset + 8 ..][0..4], .little);
            const size = 12 + @as(u64, key_size) + @as(u64, if (before_size == absent) 0 else before_size) + @as(u64, if (after_size == absent) 0 else after_size);
            if (size > bytes.len - offset) return error.InvalidCommittedFrame;
            offset += @intCast(size);
        }
        if (offset != bytes.len) return error.InvalidCommittedFrame;
        @memcpy(out[out.len - 32 ..], &digest(out[0 .. out.len - 32]));
        return out;
    }
    pub fn decode(raw: []const u8, namespace: Namespace, sequence: u64) !Descriptor {
        if (raw.len < 112 or !std.mem.eql(u8, raw[0..4], "CHD1") or
            !std.mem.eql(u8, raw[4..28], &namespace) or std.mem.readInt(u64, raw[28..36], .little) != sequence or
            !std.mem.eql(u8, raw[raw.len - 32 ..], &digest(raw[0 .. raw.len - 32]))) return error.InvalidCommittedFrame;
        const total = std.mem.readInt(u32, raw[36..40], .little);
        const count = std.mem.readInt(u32, raw[40..44], .little);
        const events = std.mem.readInt(u32, raw[44..48], .little);
        if (total < header_bytes or total > max_bytes or count != std.math.divCeil(u32, total, chunk_bytes) catch unreachable or events == 0 or events > max_events or raw.len != 80 + @as(usize, count) * 32 + @as(usize, events) * 4 or sequence == 0) return error.InvalidCommittedFrame;
        const result: Descriptor = .{ .raw = raw, .namespace = namespace, .sequence = sequence, .total = total, .count = count, .events = events };
        var previous: u32 = header_bytes - 1;
        for (0..events) |ordinal| {
            const offset = result.eventOffset(@intCast(ordinal));
            if (offset <= previous or offset > total or total - offset < 12) return error.InvalidCommittedFrame;
            previous = offset;
        }
        return result;
    }
    pub fn eventOffset(self: Descriptor, ordinal: u32) u32 {
        std.debug.assert(ordinal <= self.events);
        if (ordinal == self.events) return self.total;
        return std.mem.readInt(u32, self.raw[48 + @as(usize, self.count) * 32 + @as(usize, ordinal) * 4 ..][0..4], .little);
    }
    pub fn storedBytes(self: Descriptor) u64 {
        return @as(u64, self.total) + self.raw.len;
    }
};

pub const Reader = struct {
    descriptor: Descriptor,
    source: Source,
    offset: u32 = 0,
    remaining: ?u32 = null,
    cached_ordinal: ?u32 = null,
    cached_chunk: []const u8 = "",
    /// Every returned field is copied into the caller's bounded page arena.
    /// Chunk buffers belong to Source and may be reused on its next read.
    fn take(self: *Reader, a: A, size: usize) ![]const u8 {
        if (self.offset > self.descriptor.total) return error.InvalidCommittedFrame;
        if (size > self.descriptor.total - self.offset) return error.InvalidCommittedFrame;
        const out = try a.alloc(u8, size);
        errdefer a.free(out);
        var written: usize = 0;
        while (written != size) {
            const ordinal = self.offset / chunk_bytes;
            if (self.cached_ordinal == null or self.cached_ordinal.? != ordinal) {
                const chunk = try self.source.read(self.source.context, ordinal);
                const expected = @min(chunk_bytes, self.descriptor.total - ordinal * chunk_bytes);
                if (chunk.len != expected or !std.mem.eql(u8, self.descriptor.raw[48 + @as(usize, ordinal) * 32 ..][0..32], &digest(chunk))) return error.InvalidCommittedFrame;
                self.cached_ordinal = ordinal;
                self.cached_chunk = chunk;
            }
            const chunk = self.cached_chunk;
            const start = self.offset % chunk_bytes;
            const copied = @min(size - written, chunk.len - start);
            @memcpy(out[written..][0..copied], chunk[start..][0..copied]);
            written += copied;
            self.offset += @intCast(copied);
        }
        return out;
    }
    pub fn header(self: *Reader, a: A) !struct { value: Header, count: u32 } {
        if (self.offset != 0) return error.InvalidCommittedFrame;
        const raw = try self.take(a, header_bytes);
        if (!std.mem.eql(u8, raw[0..4], "CHF1") or !std.mem.allEqual(u8, raw[5..8], 0) or !std.mem.allEqual(u8, raw[108..112], 0) or
            !std.mem.eql(u8, raw[8..32], &self.descriptor.namespace) or std.mem.readInt(u64, raw[32..40], .little) != self.descriptor.sequence) return error.InvalidCommittedFrame;
        const scope = std.enums.fromInt(TransactionScope, raw[4]) orelse return error.InvalidCommittedFrame;
        const count = std.mem.readInt(u32, raw[96..100], .little);
        if (count != self.descriptor.events) return error.InvalidCommittedFrame;
        var result: Header = .{ .namespace = raw[8..32].*, .sequence = self.descriptor.sequence, .transaction_id = raw[40..56].*, .transaction_scope = scope, .source_position = raw[56..88].*, .committed_at_ns = std.mem.readInt(i64, raw[88..96], .little) };
        const before = std.mem.readInt(u32, raw[100..104], .little);
        const after = std.mem.readInt(u32, raw[104..108], .little);
        if ((before != absent and before > max_schema_bytes) or (after != absent and after > max_schema_bytes)) return error.InvalidCommittedFrame;
        if (before != absent) result.schema_before = try self.take(a, before);
        if (after != absent) result.schema_after = try self.take(a, after);
        if (self.offset != self.descriptor.eventOffset(0)) return error.InvalidCommittedFrame;
        self.remaining = count;
        return .{ .value = result, .count = count };
    }
    /// Resume directly from an authenticated ordinal. No row prefix is read.
    /// The header/schema remains available to interpret this page's tuples.
    pub fn seek(self: *Reader, a: A, ordinal: u32) !Header {
        if (ordinal > self.descriptor.events) return error.InvalidCommittedFrame;
        const result = try self.header(a);
        self.offset = self.descriptor.eventOffset(ordinal);
        self.remaining = self.descriptor.events - ordinal;
        return result.value;
    }
    pub fn next(self: *Reader, a: A) !?Event {
        const remaining = self.remaining orelse return error.InvalidCommittedFrame;
        if (remaining == 0) {
            if (self.offset != self.descriptor.total) return error.InvalidCommittedFrame;
            return null;
        }
        const sizes = try self.take(a, 12);
        const key_len = std.mem.readInt(u32, sizes[0..4], .little);
        const before = std.mem.readInt(u32, sizes[4..8], .little);
        const after = std.mem.readInt(u32, sizes[8..12], .little);
        if (key_len == 0 or (before == absent and after == absent)) return error.InvalidCommittedFrame;
        const key = try self.take(a, key_len);
        const result: Event = .{ .key = key, .before = if (before != absent) try self.take(a, before) else null, .after = if (after != absent) try self.take(a, after) else null };
        if (self.offset != self.descriptor.eventOffset(self.descriptor.events - remaining + 1)) return error.InvalidCommittedFrame;
        self.remaining = remaining - 1;
        return result;
    }
};

test "committed frame pages preserve binary tuple images and reject damaged chunks" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const large = try scratch.alloc(u8, 3 * chunk_bytes + 17);
    @memset(large, 255);
    const header_value: Header = .{ .namespace = @splat(1), .sequence = 9, .transaction_id = @splat(2), .schema_before = "schema-1", .schema_after = "schema-2" };
    const encoded = try encode(scratch, header_value, &.{ .{ .key = "a\x00", .before = "old", .after = large }, .{ .key = "b", .before = "deleted", .after = null } });
    const descriptor_raw = try Descriptor.encode(scratch, header_value.namespace, 9, encoded);
    const descriptor = try Descriptor.decode(descriptor_raw, header_value.namespace, 9);
    const Store = struct {
        bytes: []u8,
        reads: u32 = 0,
        fn read(ptr: *anyopaque, ordinal: u32) ![]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.reads |= @as(u32, 1) << @intCast(ordinal);
            const start = @as(usize, ordinal) * chunk_bytes;
            return self.bytes[start..@min(self.bytes.len, start + chunk_bytes)];
        }
    };
    var store: Store = .{ .bytes = encoded };
    var reader: Reader = .{ .descriptor = descriptor, .source = .{ .context = &store, .read = Store.read } };
    const decoded_header = try reader.header(scratch);
    try std.testing.expectEqual(@as(u32, 2), decoded_header.count);
    try std.testing.expectEqualStrings("schema-1", decoded_header.value.schema_before.?);
    const first = (try reader.next(scratch)).?;
    try std.testing.expectEqualStrings("old", first.before.?);
    try std.testing.expectEqualSlices(u8, large, first.after.?);
    try std.testing.expect((try reader.next(scratch)).?.operation() == .delete);
    try std.testing.expect((try reader.next(scratch)) == null);
    store.reads = 0;
    reader = .{ .descriptor = descriptor, .source = .{ .context = &store, .read = Store.read } };
    _ = try reader.seek(scratch, 1);
    try std.testing.expect((try reader.next(scratch)).?.operation() == .delete);
    try std.testing.expect((try reader.next(scratch)) == null);
    try std.testing.expectEqual(@as(u32, 0b1001), store.reads);
    encoded[chunk_bytes + 1] ^= 1;
    reader = .{ .descriptor = descriptor, .source = .{ .context = &store, .read = Store.read } };
    _ = try reader.header(scratch);
    try std.testing.expectError(error.InvalidCommittedFrame, reader.next(scratch));
    try std.testing.expectError(error.InvalidCommittedFrame, Descriptor.decode(descriptor_raw, @splat(3), 9));
}

test "committed frame scopes distinguish native root effects from coordinated and legacy transactions" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Store = struct {
        bytes: []u8,
        fn read(ptr: *anyopaque, ordinal: u32) ![]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (ordinal != 0) return error.InvalidCommittedFrame;
            return self.bytes;
        }
    };
    // Identical caller transaction IDs cannot change the captured scope fact.
    for ([_]TransactionScope{ .unknown, .root_local, .coordinated }) |scope| {
        const header: Header = .{ .namespace = @splat(1), .sequence = 1, .transaction_id = @splat(2), .transaction_scope = scope };
        const bytes = try encode(a, header, &.{.{ .key = "doc", .after = "{}", .before = null }});
        var store: Store = .{ .bytes = bytes };
        var descriptor = try Descriptor.decode(try Descriptor.encode(a, header.namespace, 1, bytes), header.namespace, 1);
        var reader: Reader = .{ .descriptor = descriptor, .source = .{ .context = &store, .read = Store.read } };
        try std.testing.expectEqual(scope, (try reader.header(a)).value.transaction_scope);
        bytes[4] = 255;
        descriptor = try Descriptor.decode(try Descriptor.encode(a, header.namespace, 1, bytes), header.namespace, 1);
        reader = .{ .descriptor = descriptor, .source = .{ .context = &store, .read = Store.read } };
        try std.testing.expectError(error.InvalidCommittedFrame, reader.header(a));
    }
}
