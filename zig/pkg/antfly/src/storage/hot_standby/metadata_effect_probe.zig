// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Fixed-memory preflight of selected metadata effect keys. Each feed consumes
//! exactly one authenticated transport frame, allowing the receiver to yield
//! between frames without retaining a transaction, cursor, or borrowed bytes.
//! This is not publication authority: the caller must still independently
//! verify local candidates and run the canonical effect decoder before commit.
const std = @import("std");
const chunks = @import("metadata_effect_chunks.zig");
const effects = @import("metadata_effects.zig");

pub fn PointProbe(comptime key_count: usize, comptime max_key_bytes: usize, comptime max_value_bytes: usize) type {
    if (key_count == 0 or key_count > 64) @compileError("point probe requires 1..64 keys");
    return struct {
        const Self = @This();
        pub const Capture = struct {
            kind: enum { missing, deleted, value } = .missing,
            len: usize = 0,
            bytes: [max_value_bytes]u8 = undefined,

            pub fn value(self: *const Capture) ?[]const u8 {
                return if (self.kind == .value) self.bytes[0..self.len] else null;
            }
        };
        descriptor: chunks.Descriptor,
        keys: [key_count][max_key_bytes]u8 = undefined,
        key_lengths: [key_count]usize = undefined,
        captures: [key_count]Capture = @splat(.{}),
        next_frame: u32 = 0,
        offset: u64 = 0,
        phase: enum { header, row_header, key, value, footer, done } = .header,
        scratch: [40]u8 = undefined,
        scratch_len: usize = 0,
        remaining: u32 = 0,
        key_len: usize = 0,
        key_offset: usize = 0,
        value_len: usize = 0,
        value_offset: usize = 0,
        deleted: bool = false,
        matches: u64 = 0,
        selected: ?usize = null,
        private_prefix: bool = false,
        body_hash: std.crypto.hash.Blake3 = .init(.{}),
        full_hash: std.crypto.hash.Blake3 = .init(.{}),
        failed: bool = false,

        pub fn init(descriptor: chunks.Descriptor, keys: [key_count][]const u8) !Self {
            try descriptor.validate();
            var self: Self = .{ .descriptor = descriptor };
            for (keys, 0..) |key, i| {
                if (key.len == 0 or key.len > max_key_bytes) return error.InvalidMetadataHAEffect;
                for (keys[0..i]) |previous| if (std.mem.eql(u8, previous, key)) return error.InvalidMetadataHAEffect;
                @memcpy(self.keys[i][0..key.len], key);
                self.key_lengths[i] = key.len;
            }
            return self;
        }

        pub fn verified(self: *const Self) bool {
            return !self.failed and self.phase == .done and self.next_frame == self.descriptor.chunk_count;
        }

        /// No tentative capture may be used for admission before both the
        /// effect footer and descriptor digest have been authenticated.
        pub fn get(self: *const Self, index: usize) !*const Capture {
            if (self.failed or index >= key_count) return error.InvalidMetadataHAEffect;
            if (!self.verified()) return error.CatalogPublicationProofPending;
            return &self.captures[index];
        }

        pub fn feed(self: *Self, frame: chunks.Frame) !void {
            self.feedChecked(frame) catch |err| {
                self.failed = true;
                return err;
            };
        }

        fn consume(self: *Self, bytes: []const u8) !void {
            if (bytes.len > self.descriptor.total_bytes - self.offset) return error.InvalidMetadataHAEffect;
            self.full_hash.update(bytes);
            const body_end = self.descriptor.total_bytes - 32;
            if (self.offset < body_end) self.body_hash.update(bytes[0..@intCast(@min(bytes.len, body_end - self.offset))]);
            self.offset += bytes.len;
        }

        fn finishRow(self: *Self) void {
            self.remaining -= 1;
            self.phase = if (self.remaining == 0) .footer else .row_header;
            self.scratch_len = 0;
        }

        fn feedChecked(self: *Self, frame: chunks.Frame) !void {
            if (self.failed or self.phase == .done or !frame.descriptor.eql(self.descriptor) or
                frame.index != self.next_frame or frame.payload.len != try self.descriptor.payloadLength(frame.index))
                return error.InvalidMetadataHAEffectChunk;
            var bytes = frame.payload;
            while (bytes.len != 0) {
                switch (self.phase) {
                    .header, .row_header, .footer => {
                        const size: usize = switch (self.phase) {
                            .header => 40,
                            .row_header => 12,
                            .footer => 32,
                            else => unreachable,
                        };
                        if (self.phase == .footer and self.scratch_len == 0 and self.offset != self.descriptor.total_bytes - 32)
                            return error.InvalidMetadataHAEffect;
                        const count = @min(bytes.len, size - self.scratch_len);
                        @memcpy(self.scratch[self.scratch_len..][0..count], bytes[0..count]);
                        self.scratch_len += count;
                        try self.consume(bytes[0..count]);
                        bytes = bytes[count..];
                        if (self.scratch_len != size) continue;
                        switch (self.phase) {
                            .header => {
                                if (!std.mem.eql(u8, self.scratch[0..4], "AFMH") or
                                    !std.mem.eql(u8, self.scratch[4..20], &self.descriptor.source) or
                                    std.mem.readInt(u64, self.scratch[20..28], .little) != self.descriptor.sequence or
                                    std.mem.readInt(u64, self.scratch[28..36], .little) != self.descriptor.group_id)
                                    return error.InvalidMetadataHAEffect;
                                self.remaining = std.mem.readInt(u32, self.scratch[36..40], .little);
                                if (self.remaining > (self.descriptor.total_bytes - 72) / 13) return error.InvalidMetadataHAEffect;
                                self.phase = if (self.remaining == 0) .footer else .row_header;
                            },
                            .row_header => {
                                const raw_len = std.mem.readInt(u64, self.scratch[4..12], .little);
                                self.key_len = std.mem.readInt(u32, self.scratch[0..4], .little);
                                self.deleted = raw_len == std.math.maxInt(u64);
                                const value_len = if (self.deleted) 0 else raw_len;
                                if (self.offset > self.descriptor.total_bytes - 32 or self.key_len == 0 or
                                    self.key_len +| value_len > effects.max_row_bytes or
                                    self.key_len +| value_len > self.descriptor.total_bytes - 32 - self.offset)
                                    return error.InvalidMetadataHAEffect;
                                self.value_len = @intCast(value_len);
                                self.key_offset = 0;
                                self.value_offset = 0;
                                self.matches = 0;
                                self.selected = null;
                                self.private_prefix = self.key_len >= effects.prefix.len;
                                for (self.key_lengths, 0..) |len, i| if (len == self.key_len) {
                                    self.matches |= @as(u64, 1) << @intCast(i);
                                };
                                self.phase = .key;
                            },
                            .footer => {
                                var digest: [32]u8 = undefined;
                                self.body_hash.final(&digest);
                                if (!std.mem.eql(u8, &digest, self.scratch[0..32])) return error.InvalidMetadataHAEffect;
                                self.full_hash.final(&digest);
                                if (self.offset != self.descriptor.total_bytes or !std.mem.eql(u8, &digest, &self.descriptor.digest))
                                    return error.InvalidMetadataHAEffect;
                                self.phase = .done;
                            },
                            else => unreachable,
                        }
                        self.scratch_len = 0;
                    },
                    .key => {
                        const count = @min(bytes.len, self.key_len - self.key_offset);
                        for (0..key_count) |i| {
                            const mask = @as(u64, 1) << @intCast(i);
                            if (self.matches & mask != 0 and !std.mem.eql(u8, bytes[0..count], self.keys[i][self.key_offset..][0..count])) self.matches &= ~mask;
                        }
                        if (self.private_prefix and self.key_offset < effects.prefix.len) {
                            const size = @min(count, effects.prefix.len - self.key_offset);
                            if (!std.mem.eql(u8, bytes[0..size], effects.prefix[self.key_offset..][0..size])) self.private_prefix = false;
                        }
                        try self.consume(bytes[0..count]);
                        bytes = bytes[count..];
                        self.key_offset += count;
                        if (self.key_offset != self.key_len) continue;
                        if (self.private_prefix) return error.InvalidMetadataHAEffect;
                        if (self.matches != 0) {
                            const i: usize = @intCast(@ctz(self.matches));
                            if (self.captures[i].kind != .missing or self.value_len > max_value_bytes) return error.InvalidMetadataHAEffect;
                            self.captures[i].kind = if (self.deleted) .deleted else .value;
                            self.captures[i].len = self.value_len;
                            self.selected = i;
                        }
                        if (self.value_len == 0) self.finishRow() else self.phase = .value;
                    },
                    .value => {
                        const count = @min(bytes.len, self.value_len - self.value_offset);
                        if (self.selected) |i| @memcpy(self.captures[i].bytes[self.value_offset..][0..count], bytes[0..count]);
                        try self.consume(bytes[0..count]);
                        bytes = bytes[count..];
                        self.value_offset += count;
                        if (self.value_offset == self.value_len) self.finishRow();
                    },
                    .done => return error.InvalidMetadataHAEffect,
                }
            }
            self.next_frame += 1;
            if (self.next_frame == self.descriptor.chunk_count and self.phase != .done) return error.InvalidMetadataHAEffect;
        }
    };
}

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

fn testFeed(probe: anytype, bytes: []const u8, descriptor: chunks.Descriptor, index: u32) !void {
    const offset = @as(usize, index) * chunks.max_payload_bytes;
    const encoded = try chunks.encodeFrame(std.testing.allocator, descriptor, index, bytes[offset..][0..try descriptor.payloadLength(index)]);
    defer std.testing.allocator.free(encoded);
    try probe.feed(try chunks.decodeFrame(encoded));
    // The probe owns every capture and key; it must not borrow transport bytes.
    @memset(encoded, 0xa5);
}

test "storage.hot_standby metadata point probe bounds memory and authenticates missing empty and deleted captures" {
    const alloc = std.testing.allocator;
    const ignored = try alloc.alloc(u8, chunks.max_payload_bytes * 2);
    defer alloc.free(ignored);
    @memset(ignored, 42);
    const bytes = try testEffect(alloc, &.{
        .{ .key = "a", .value = ignored },
        .{ .key = "b", .value = "" },
        .{ .key = "c", .value = null },
        .{ .key = "d", .value = "manifest" },
    });
    defer alloc.free(bytes);
    const descriptor = try chunks.Descriptor.fromEffect(bytes);
    const Probe = PointProbe(4, 8, 32);
    try std.testing.expect(@sizeOf(Probe) < 8192);
    var mutable_key = [_]u8{'d'};
    var probe = try Probe.init(descriptor, .{ "b", "c", &mutable_key, "missing" });
    mutable_key[0] = 'z';
    for (0..descriptor.chunk_count) |i| {
        try std.testing.expectError(error.CatalogPublicationProofPending, probe.get(0));
        try testFeed(&probe, bytes, descriptor, @intCast(i));
        try std.testing.expectEqual(@as(u32, @intCast(i + 1)), probe.next_frame);
    }
    try std.testing.expect(probe.verified());
    try std.testing.expectEqualStrings("", (try probe.get(0)).value().?);
    try std.testing.expectEqual(.deleted, (try probe.get(1)).kind);
    try std.testing.expectEqualStrings("manifest", (try probe.get(2)).value().?);
    try std.testing.expectEqual(.missing, (try probe.get(3)).kind);
    try std.testing.expectError(error.InvalidMetadataHAEffect, probe.get(4));
}

test "storage.hot_standby metadata point probe handles split row header key value and footer" {
    const alloc = std.testing.allocator;
    const key = "b123456789";
    const value = "0123456789abcdef0123456789abcdef";
    for ([_]usize{ 6, 12 + 5, 12 + key.len + 5, 12 + key.len + value.len + 16 }) |before_boundary| {
        const padding = try alloc.alloc(u8, chunks.max_payload_bytes - before_boundary - 53);
        defer alloc.free(padding);
        @memset(padding, 'x');
        const bytes = try testEffect(alloc, &.{ .{ .key = "a", .value = padding }, .{ .key = key, .value = value } });
        defer alloc.free(bytes);
        const descriptor = try chunks.Descriptor.fromEffect(bytes);
        try std.testing.expectEqual(@as(u32, 2), descriptor.chunk_count);
        var probe = try PointProbe(1, 16, 32).init(descriptor, .{key});
        try testFeed(&probe, bytes, descriptor, 0);
        try std.testing.expect(!probe.verified());
        try testFeed(&probe, bytes, descriptor, 1);
        try std.testing.expectEqualStrings(value, (try probe.get(0)).value().?);
    }
}

test "storage.hot_standby metadata point probe rejects forged envelopes captures and terminal integrity" {
    const alloc = std.testing.allocator;
    const Probe = PointProbe(1, 8, 8);
    for ([_][]const effects.Row{
        &.{ .{ .key = "a", .value = "one" }, .{ .key = "a", .value = "two" } },
        &.{.{ .key = "a", .value = "oversized" }},
        &.{.{ .key = effects.prefix, .value = null }},
    }) |rows| {
        const bytes = try testEffect(alloc, rows);
        defer alloc.free(bytes);
        const descriptor = try chunks.Descriptor.fromEffect(bytes);
        var probe = try Probe.init(descriptor, .{"a"});
        try std.testing.expectError(error.InvalidMetadataHAEffect, testFeed(&probe, bytes, descriptor, 0));
        try std.testing.expect(!probe.verified());
        try std.testing.expectError(error.InvalidMetadataHAEffect, probe.get(0));
    }
    const bytes = try testEffect(alloc, &.{.{ .key = "a", .value = "valid" }});
    defer alloc.free(bytes);
    const descriptor = try chunks.Descriptor.fromEffect(bytes);
    try std.testing.expectError(error.InvalidMetadataHAEffect, PointProbe(2, 8, 8).init(descriptor, .{ "a", "a" }));
    try std.testing.expectError(error.InvalidMetadataHAEffect, Probe.init(descriptor, .{"too-long-key"}));
    {
        var probe = try Probe.init(descriptor, .{"a"});
        const frame = chunks.Frame{ .descriptor = descriptor, .index = 1, .payload = bytes };
        try std.testing.expectError(error.InvalidMetadataHAEffectChunk, probe.feed(frame));
        try std.testing.expect(!probe.verified());
    }
    for ([_]usize{ 0, 4, 20, 28, 36, 40, bytes.len - 1 }) |fault| {
        bytes[fault] ^= 1;
        defer bytes[fault] ^= 1;
        // The sender can authenticate its corrupt frame and descriptor. The
        // probe must also validate the inner effect identity and body footer.
        var forged = descriptor;
        std.crypto.hash.Blake3.hash(bytes, &forged.digest, .{});
        var probe = try Probe.init(forged, .{"a"});
        try std.testing.expectError(error.InvalidMetadataHAEffect, testFeed(&probe, bytes, forged, 0));
        try std.testing.expect(!probe.verified());
    }
    {
        var forged = descriptor;
        forged.digest[0] ^= 1;
        var probe = try Probe.init(forged, .{"a"});
        try std.testing.expectError(error.InvalidMetadataHAEffect, testFeed(&probe, bytes, forged, 0));
    }
}
