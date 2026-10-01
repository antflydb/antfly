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

//! Authenticated bounded HA transport for one atomic metadata effect.
const std = @import("std");
const effects = @import("metadata_effects.zig");
// Reserve the replication-record envelope as well as the chunk envelope.
pub const max_frame_bytes: usize = 1024 * 1024 - @as(usize, @import("replication_record.zig").header_size);
pub const frame_overhead = 120;
pub const max_payload_bytes = max_frame_bytes - frame_overhead;
pub const max_chunk_payload_bytes = max_payload_bytes;

pub const Descriptor = struct {
    source: [16]u8,
    sequence: u64,
    group_id: u64,
    total_bytes: u64,
    digest: [32]u8,
    chunk_count: u32,

    pub fn eql(a: Descriptor, b: Descriptor) bool {
        return std.meta.eql(a, b);
    }

    pub fn validate(self: Descriptor) !void {
        if (self.sequence == 0 or self.group_id == 0 or std.mem.allEqual(u8, &self.source, 0) or
            self.total_bytes < 72 or self.total_bytes > effects.max_effect_bytes or
            self.chunk_count != (self.total_bytes + max_payload_bytes - 1) / max_payload_bytes)
            return error.InvalidMetadataHAEffectChunk;
    }

    pub fn fromEffect(bytes: []const u8) !Descriptor {
        const decoder = try effects.Decoder.init(bytes);
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(bytes, &digest, .{});
        return .{ .source = decoder.header.source, .sequence = decoder.header.sequence, .group_id = decoder.header.group_id, .total_bytes = bytes.len, .digest = digest, .chunk_count = @intCast((bytes.len + max_payload_bytes - 1) / max_payload_bytes) };
    }

    pub fn payloadLength(self: Descriptor, index: u32) !usize {
        try self.validate();
        if (index >= self.chunk_count) return error.InvalidMetadataHAEffectChunk;
        return @intCast(@min(max_payload_bytes, self.total_bytes - @as(u64, index) * max_payload_bytes));
    }
};

fn testEffect(alloc: std.mem.Allocator, rows: []const effects.Row) ![]u8 {
    var size: usize = 72;
    for (rows) |row| size += 12 + row.key.len + if (row.value) |value| value.len else @as(usize, 0);
    const out = try alloc.alloc(u8, size);
    @memcpy(out[0..4], "AFMH");
    @memset(out[4..20], 7);
    std.mem.writeInt(u64, out[20..28], 11, .little);
    std.mem.writeInt(u64, out[28..36], 1, .little);
    std.mem.writeInt(u32, out[36..40], @intCast(rows.len), .little);
    var offset: usize = 40;
    for (rows) |row| {
        std.mem.writeInt(u32, out[offset..][0..4], @intCast(row.key.len), .little);
        std.mem.writeInt(u64, out[offset + 4 ..][0..8], if (row.value) |value| value.len else std.math.maxInt(u64), .little);
        offset += 12;
        @memcpy(out[offset..][0..row.key.len], row.key);
        offset += row.key.len;
        if (row.value) |value| {
            @memcpy(out[offset..][0..value.len], value);
            offset += value.len;
        }
    }
    std.crypto.hash.Blake3.hash(out[0..offset], out[offset..][0..32], .{});
    return out;
}

const TestSource = struct {
    effect: []const u8,
    descriptor: Descriptor,
    reads: u32 = 0,
    fn source(self: *@This()) Source {
        return .{ .ptr = self, .read_frame = readFrame };
    }
    fn readFrame(ptr: *anyopaque, index: u32, buffer: []u8) !usize {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const offset = @as(usize, index) * max_payload_bytes;
        const bytes = try encodeFrame(std.testing.allocator, self.descriptor, index, self.effect[offset..][0..try self.descriptor.payloadLength(index)]);
        defer std.testing.allocator.free(bytes);
        @memcpy(buffer[0..bytes.len], bytes);
        self.reads += 1;
        return bytes.len;
    }
};

test "metadata chunks stream cross-frame rows and distinguish empty from deletion" {
    const alloc = std.testing.allocator;
    const value = try alloc.alloc(u8, max_payload_bytes * 2 + 13);
    defer alloc.free(value);
    @memset(value, 42);
    const bytes = try testEffect(alloc, &.{ .{ .key = "a", .value = value }, .{ .key = "b", .value = "" }, .{ .key = "c", .value = null } });
    defer alloc.free(bytes);
    const descriptor = try Descriptor.fromEffect(bytes);
    try std.testing.expectEqual(@as(u32, 3), descriptor.chunk_count);
    var source: TestSource = .{ .effect = bytes, .descriptor = descriptor };
    var decoder = try StreamingDecoder.init(alloc, descriptor, source.source());
    defer decoder.deinit();
    const first = (try decoder.next()).?;
    try std.testing.expectEqualStrings("a", first.key);
    try std.testing.expectEqualSlices(u8, value, first.value.?);
    try std.testing.expectEqual(@as(usize, 0), (try decoder.next()).?.value.?.len);
    try std.testing.expect((try decoder.next()).?.value == null);
    try std.testing.expect((try decoder.next()) == null);
    try std.testing.expect((try decoder.next()) == null);
    try std.testing.expectEqual(descriptor.chunk_count, source.reads);
    try std.testing.expect(decoder.verified);
}

test "metadata chunks reject corrupt envelopes and noncanonical partitions" {
    const alloc = std.testing.allocator;
    const bytes = try testEffect(alloc, &.{});
    defer alloc.free(bytes);
    const descriptor = try Descriptor.fromEffect(bytes);
    const frame = try encodeFrame(alloc, descriptor, 0, bytes);
    defer alloc.free(frame);
    try std.testing.expect((try decodeFrame(frame)).descriptor.eql(descriptor));
    try std.testing.expect(frame.len + @import("replication_record.zig").header_size <= 1024 * 1024);
    frame[88] ^= 1;
    try std.testing.expectError(error.InvalidMetadataHAEffectChunk, decodeFrame(frame));
    var invalid = descriptor;
    invalid.chunk_count += 1;
    try std.testing.expectError(error.InvalidMetadataHAEffectChunk, invalid.validate());
    try std.testing.expectError(error.InvalidMetadataHAEffectChunk, encodeFrame(alloc, descriptor, 1, bytes));
    try std.testing.expectError(error.InvalidMetadataHAEffectChunk, encodeFrame(alloc, descriptor, 0, bytes[0 .. bytes.len - 1]));
}

test "metadata chunks require terminal effect integrity before publication" {
    const alloc = std.testing.allocator;
    const bytes = try testEffect(alloc, &.{.{ .key = "a", .value = "data" }});
    defer alloc.free(bytes);
    var descriptor = try Descriptor.fromEffect(bytes);
    bytes[bytes.len - 1] ^= 1;
    std.crypto.hash.Blake3.hash(bytes, &descriptor.digest, .{});
    var source: TestSource = .{ .effect = bytes, .descriptor = descriptor };
    var decoder = try StreamingDecoder.init(alloc, descriptor, source.source());
    defer decoder.deinit();
    try std.testing.expectEqualStrings("data", (try decoder.next()).?.value.?);
    try std.testing.expectError(error.InvalidMetadataHAEffect, decoder.next());
    try std.testing.expect(!decoder.verified);
}

test "metadata chunks enforce sorted unique keys and private-key exclusion" {
    const alloc = std.testing.allocator;
    for ([_][]const effects.Row{
        &.{ .{ .key = "b", .value = null }, .{ .key = "a", .value = "bad" } },
        &.{ .{ .key = "a", .value = null }, .{ .key = "a", .value = "bad" } },
        &.{.{ .key = effects.prefix, .value = "bad" }},
    }) |rows| {
        const bytes = try testEffect(alloc, rows);
        defer alloc.free(bytes);
        const descriptor = try Descriptor.fromEffect(bytes);
        var source: TestSource = .{ .effect = bytes, .descriptor = descriptor };
        var decoder = try StreamingDecoder.init(alloc, descriptor, source.source());
        defer decoder.deinit();
        if (rows.len == 2) _ = try decoder.next();
        try std.testing.expectError(error.InvalidMetadataHAEffect, decoder.next());
    }
}

pub const Frame = struct { descriptor: Descriptor, index: u32, payload: []const u8 };

pub fn encodeFrame(alloc: std.mem.Allocator, descriptor: Descriptor, index: u32, payload: []const u8) ![]u8 {
    if (payload.len != try descriptor.payloadLength(index)) return error.InvalidMetadataHAEffectChunk;
    const out = try alloc.alloc(u8, frame_overhead + payload.len);
    @memcpy(out[0..4], "AFMC");
    @memcpy(out[4..20], &descriptor.source);
    std.mem.writeInt(u64, out[20..28], descriptor.sequence, .little);
    std.mem.writeInt(u64, out[28..36], descriptor.group_id, .little);
    std.mem.writeInt(u64, out[36..44], descriptor.total_bytes, .little);
    @memcpy(out[44..76], &descriptor.digest);
    std.mem.writeInt(u32, out[76..80], descriptor.chunk_count, .little);
    std.mem.writeInt(u32, out[80..84], index, .little);
    std.mem.writeInt(u32, out[84..88], @intCast(payload.len), .little);
    @memcpy(out[88..][0..payload.len], payload);
    std.crypto.hash.Blake3.hash(out[0 .. out.len - 32], out[out.len - 32 ..][0..32], .{});
    return out;
}

pub fn decodeFrame(bytes: []const u8) !Frame {
    if (bytes.len < frame_overhead or bytes.len > max_frame_bytes or !std.mem.eql(u8, bytes[0..4], "AFMC")) return error.InvalidMetadataHAEffectChunk;
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(bytes[0 .. bytes.len - 32], &digest, .{});
    if (!std.mem.eql(u8, &digest, bytes[bytes.len - 32 ..])) return error.InvalidMetadataHAEffectChunk;
    const descriptor: Descriptor = .{ .source = bytes[4..20].*, .sequence = std.mem.readInt(u64, bytes[20..28], .little), .group_id = std.mem.readInt(u64, bytes[28..36], .little), .total_bytes = std.mem.readInt(u64, bytes[36..44], .little), .digest = bytes[44..76].*, .chunk_count = std.mem.readInt(u32, bytes[76..80], .little) };
    const index = std.mem.readInt(u32, bytes[80..84], .little);
    const size = std.mem.readInt(u32, bytes[84..88], .little);
    if (size != try descriptor.payloadLength(index) or size != bytes.len - frame_overhead) return error.InvalidMetadataHAEffectChunk;
    return .{ .descriptor = descriptor, .index = index, .payload = bytes[88 .. bytes.len - 32] };
}

pub const Source = struct {
    ptr: *anyopaque,
    /// Load exactly one durably staged AFMC frame into the supplied 1 MiB buffer.
    read_frame: *const fn (*anyopaque, u32, []u8) anyerror!usize,
};

/// Rows are borrowed until next/deinit. The caller MUST consume through null
/// before committing: terminal validation authenticates the complete effect.
pub const StreamingDecoder = struct {
    alloc: std.mem.Allocator,
    descriptor: Descriptor,
    source: Source,
    header: effects.Header = undefined,
    buffer: []u8,
    payload: []const u8 = "",
    payload_offset: usize = 0,
    chunk_index: u32 = 0,
    offset: u64 = 0,
    remaining: u32 = 0,
    previous_key: ?[]u8 = null,
    current_value: ?[]u8 = null,
    body_hash: std.crypto.hash.Blake3 = .init(.{}),
    full_hash: std.crypto.hash.Blake3 = .init(.{}),
    verified: bool = false,

    pub fn init(alloc: std.mem.Allocator, descriptor: Descriptor, source: Source) !StreamingDecoder {
        try descriptor.validate();
        var self: StreamingDecoder = .{ .alloc = alloc, .descriptor = descriptor, .source = source, .buffer = try alloc.alloc(u8, max_frame_bytes) };
        errdefer self.deinit();
        var header: [40]u8 = undefined;
        try self.readExact(&header);
        if (!std.mem.eql(u8, header[0..4], "AFMH")) return error.InvalidMetadataHAEffect;
        self.header = .{ .source = header[4..20].*, .sequence = std.mem.readInt(u64, header[20..28], .little), .group_id = std.mem.readInt(u64, header[28..36], .little), .count = std.mem.readInt(u32, header[36..40], .little) };
        if (!std.mem.eql(u8, &descriptor.source, &self.header.source) or descriptor.sequence != self.header.sequence or descriptor.group_id != self.header.group_id) return error.InvalidMetadataHAEffect;
        self.remaining = self.header.count;
        return self;
    }

    pub fn deinit(self: *StreamingDecoder) void {
        if (self.previous_key) |key| self.alloc.free(key);
        if (self.current_value) |value| self.alloc.free(value);
        self.alloc.free(self.buffer);
        self.* = undefined;
    }

    fn readExact(self: *StreamingDecoder, out: []u8) !void {
        if (out.len > self.descriptor.total_bytes - self.offset) return error.InvalidMetadataHAEffect;
        var rest = out;
        while (rest.len != 0) {
            if (self.payload_offset == self.payload.len) {
                if (self.chunk_index >= self.descriptor.chunk_count) return error.InvalidMetadataHAEffectChunk;
                const size = try self.source.read_frame(self.source.ptr, self.chunk_index, self.buffer);
                if (size > self.buffer.len) return error.InvalidMetadataHAEffectChunk;
                const frame = try decodeFrame(self.buffer[0..size]);
                if (frame.index != self.chunk_index or !std.meta.eql(frame.descriptor, self.descriptor)) return error.InvalidMetadataHAEffectChunk;
                self.chunk_index += 1;
                self.payload = frame.payload;
                self.payload_offset = 0;
            }
            const count = @min(rest.len, self.payload.len - self.payload_offset);
            const bytes = self.payload[self.payload_offset..][0..count];
            @memcpy(rest[0..count], bytes);
            self.full_hash.update(bytes);
            const body_end = self.descriptor.total_bytes - 32;
            if (self.offset < body_end) self.body_hash.update(bytes[0..@intCast(@min(count, body_end - self.offset))]);
            self.offset += count;
            self.payload_offset += count;
            rest = rest[count..];
        }
    }

    pub fn next(self: *StreamingDecoder) !?effects.Row {
        if (self.current_value) |value| self.alloc.free(value);
        self.current_value = null;
        if (self.remaining == 0) {
            if (self.verified) return null;
            if (self.offset != self.descriptor.total_bytes - 32) return error.InvalidMetadataHAEffect;
            var expected: [32]u8 = undefined;
            try self.readExact(&expected);
            var digest: [32]u8 = undefined;
            self.body_hash.final(&digest);
            if (!std.mem.eql(u8, &expected, &digest)) return error.InvalidMetadataHAEffect;
            self.full_hash.final(&digest);
            if (!std.mem.eql(u8, &self.descriptor.digest, &digest)) return error.InvalidMetadataHAEffect;
            self.verified = true;
            return null;
        }
        const body_remaining = self.descriptor.total_bytes - 32 - self.offset;
        if (body_remaining < 12) return error.InvalidMetadataHAEffect;
        var header: [12]u8 = undefined;
        try self.readExact(&header);
        const key_len = std.mem.readInt(u32, header[0..4], .little);
        const raw_value_len = std.mem.readInt(u64, header[4..12], .little);
        const deleted = raw_value_len == std.math.maxInt(u64);
        const value_len = if (deleted) 0 else raw_value_len;
        if (key_len == 0 or key_len +| value_len > effects.max_row_bytes or key_len +| value_len > body_remaining - 12) return error.InvalidMetadataHAEffect;
        const key = try self.alloc.alloc(u8, key_len);
        errdefer self.alloc.free(key);
        try self.readExact(key);
        if (std.mem.startsWith(u8, key, effects.prefix)) return error.InvalidMetadataHAEffect;
        if (self.previous_key) |previous| if (!std.mem.lessThan(u8, previous, key)) return error.InvalidMetadataHAEffect;
        const value: ?[]u8 = if (deleted) null else try self.alloc.alloc(u8, @intCast(value_len));
        errdefer if (value) |bytes| self.alloc.free(bytes);
        if (value) |bytes| try self.readExact(bytes);
        if (self.previous_key) |previous| self.alloc.free(previous);
        self.previous_key = key;
        self.current_value = value;
        self.remaining -= 1;
        return .{ .key = key, .value = value };
    }
};
