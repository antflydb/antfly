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

//! Zstandard frame boundaries and checksum verification around the native decoder.
const std = @import("std");
const A = std.mem.Allocator;
pub const Options = struct { dictionary: []const u8 = &.{}, max_window_bytes: usize = 64 * 1024 * 1024 };
pub fn decode(a: A, data: []const u8, limit: usize) ![]u8 {
    return decodeOptions(a, data, limit, .{});
}
pub fn decodeOptions(a: A, data: []const u8, limit: usize, options: Options) ![]u8 {
    return decodeInternal(a, data, limit, options, null);
}

/// Decode into caller-owned output without a second decoded-page allocation.
/// Frame windows remain bounded independently of the exact output size.
pub fn decodeExact(a: A, data: []const u8, output: []u8, options: Options) !void {
    _ = try decodeInternal(a, data, output.len, options, output);
}

fn decodeInternal(a: A, data: []const u8, limit: usize, options: Options, output: ?[]u8) ![]u8 {
    const Native = @import("zstd_native.zig");
    const dictionary = if (options.dictionary.len > 0) try Native.Dictionary.init(a, options.dictionary) else null;
    defer if (dictionary) |value| value.deinit(a);
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(a);
    var written: usize = 0;
    var pos: usize = 0;
    while (pos < data.len) {
        const start = pos;
        const magic = try takeInt(u32, data, &pos);
        if (magic & 0xfffffff0 == 0x184d2a50) {
            const n = try takeInt(u32, data, &pos);
            try skip(data, &pos, n);
            continue;
        }
        if (magic != 0xfd2fb528) return error.InvalidZstd;
        if (pos == data.len) return error.InvalidZstd;
        const flags = data[pos];
        pos += 1;
        if (flags & 0x18 != 0) return error.InvalidZstd;
        const single = flags & 32 != 0;
        var window: u64 = 0;
        if (!single) {
            if (pos == data.len) return error.InvalidZstd;
            const descriptor = data[pos];
            pos += 1;
            const base: u64 = @as(u64, 1) << @intCast(10 + (descriptor >> 3));
            window = base + (base / 8) * (descriptor & 7);
        }
        const dict_size: usize = switch (flags & 3) {
            0 => 0,
            1 => 1,
            2 => 2,
            3 => 4,
            else => unreachable,
        };
        try skip(data, &pos, dict_size);
        const fcs_size: usize = switch (flags >> 6) {
            0 => if (single) 1 else 0,
            1 => 2,
            2 => 4,
            3 => 8,
            else => unreachable,
        };
        var content: u64 = 0;
        for (0..fcs_size) |i| {
            if (pos == data.len) return error.InvalidZstd;
            content |= @as(u64, data[pos]) << @intCast(i * 8);
            pos += 1;
        }
        if (fcs_size == 2) content += 256;
        if (single) window = content;
        if (window > options.max_window_bytes) return error.WindowOversize;
        window = @max(window, if (dictionary) |value| value.content.len else 0);
        while (true) {
            if (data.len - pos < 3) return error.InvalidZstd;
            const header = @as(u32, data[pos]) | (@as(u32, data[pos + 1]) << 8) | (@as(u32, data[pos + 2]) << 16);
            pos += 3;
            const kind = (header >> 1) & 3;
            if (kind == 3) return error.InvalidZstd;
            try skip(data, &pos, if (kind == 1) 1 else header >> 3);
            if (header & 1 != 0) break;
        }
        const checksum: ?u32 = if (flags & 4 != 0) try takeInt(u32, data, &pos) else null;
        var reader = std.Io.Reader.fixed(data[start..pos]);
        const window_len: u32 = @intCast(@max(1024, window));
        const buffer = try a.alloc(u8, @as(usize, window_len) + std.compress.zstd.block_size_max);
        defer a.free(buffer);
        var d = Native.init(&reader, buffer, .{ .window_len = window_len, .dictionary = dictionary, .verify_checksum = true });
        if (output) |dest| {
            const n = d.reader.readSliceShort(dest[written..]) catch |err| return d.err orelse err;
            // Consume the frame terminator/checksum even when output filled
            // exactly, and reject output beyond the caller's exact budget.
            if (d.reader.takeByte()) |_| return error.StreamTooLong else |err| {
                if (err != error.EndOfStream) return d.err orelse err;
            }
            written += n;
            continue;
        }
        const part = d.reader.allocRemaining(a, .limited(limit - out.items.len)) catch |err| return d.err orelse err;
        defer a.free(part);
        if (checksum) |expected| if (expected != @as(u32, @truncate(std.hash.XxHash64.hash(0, part)))) return error.ChecksumFailure;
        try out.appendSlice(a, part);
    }
    if (output) |dest| {
        if (written != dest.len) return error.EndOfStream;
        return dest;
    }
    return out.toOwnedSlice(a);
}
fn skip(data: []const u8, pos: *usize, n: usize) !void {
    if (n > data.len - pos.*) return error.InvalidZstd;
    pos.* += n;
}
fn takeInt(comptime T: type, data: []const u8, pos: *usize) !T {
    if (@sizeOf(T) > data.len - pos.*) return error.InvalidZstd;
    defer pos.* += @sizeOf(T);
    return std.mem.readInt(T, data[pos.*..][0..@sizeOf(T)], .little);
}

test "Zstandard prefix decoding matches standard decoding across literal streams and blocks" {
    const a = std.testing.allocator;
    const cases = [_]struct { frame: []const u8, len: usize, sha256: []const u8 }{
        .{ .frame = @embedFile("testdata/cohere_literals.zst"), .len = 24576, .sha256 = "c4d4f0bb547163a34653b2b74769aadd76c7ec89c5cf004b440c8a4da0ca0f03" },
        .{ .frame = @embedFile("testdata/cohere_multiblock.zst"), .len = 294912, .sha256 = "608a7180b1a6b04784c430b108b4caea87387c5398830111a019f5d18be1346e" },
    };
    for (cases) |case| {
        const actual = try decode(a, case.frame, case.len);
        defer a.free(actual);
        var input: std.Io.Reader = .fixed(case.frame);
        const window = try a.alloc(u8, std.compress.zstd.default_window_len + std.compress.zstd.block_size_max);
        defer a.free(window);
        var reference: std.compress.zstd.Decompress = .init(&input, window, .{});
        const expected = try reference.reader.allocRemaining(a, .limited(case.len + 1));
        defer a.free(expected);
        try std.testing.expectEqualSlices(u8, expected, actual);
        const exact = try a.alloc(u8, case.len);
        defer a.free(exact);
        try decodeExact(a, case.frame, exact, .{});
        try std.testing.expectEqualSlices(u8, expected, exact);
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(actual, &digest, .{});
        try std.testing.expectEqualStrings(case.sha256, &std.fmt.bytesToHex(digest, .lower));
        for ([_]usize{ 0, 1, 4, 5, 10, case.frame.len / 2, case.frame.len - 1 }) |cut| {
            if (decode(a, case.frame[0..cut], case.len)) |value| {
                defer a.free(value);
                // Empty input is an empty frame sequence; every nonempty
                // prefix must fail before yielding partial vector payloads.
                try std.testing.expect(cut == 0 and value.len == 0);
            } else |_| {}
        }
    }
}

test "exact Zstandard pages use frame-sized scratch and enforce output bounds" {
    // Single-segment raw frame, three decoded bytes. A query-sized allocator
    // must not need the decoder's default eight-megabyte history window.
    const frame = [_]u8{ 0x28, 0xb5, 0x2f, 0xfd, 0x20, 3, 0x19, 0, 0, 'a', 'b', 'c' };
    var scratch: [256 * 1024]u8 = undefined;
    var bounded = std.heap.FixedBufferAllocator.init(&scratch);
    var output: [3]u8 = undefined;
    try decodeExact(bounded.allocator(), &frame, &output, .{});
    try std.testing.expectEqualStrings("abc", &output);
    var short: [2]u8 = undefined;
    try std.testing.expectError(error.StreamTooLong, decodeExact(std.testing.allocator, &frame, &short, .{}));
    var long: [4]u8 = undefined;
    try std.testing.expectError(error.EndOfStream, decodeExact(std.testing.allocator, &frame, &long, .{}));
    try std.testing.expectError(error.InvalidZstd, decodeExact(std.testing.allocator, frame[0 .. frame.len - 1], &output, .{}));
    try std.testing.expectError(error.WindowOversize, decodeExact(std.testing.allocator, &frame, &output, .{ .max_window_bytes = 2 }));
    // Preserve concatenated and skippable frame semantics without allocating
    // an intermediate decoded page per frame.
    const skipped = [_]u8{ 0x50, 0x2a, 0x4d, 0x18, 1, 0, 0, 0, 99 };
    var joined: [6]u8 = undefined;
    try decodeExact(std.testing.allocator, &(frame ++ skipped ++ frame), &joined, .{});
    try std.testing.expectEqualStrings("abcabc", &joined);
}

test "exact Zstandard decoding verifies checksums" {
    const frame = [_]u8{
        0x28, 0xb5, 0x2f, 0xfd, 0x04, 0x58, 0x81, 0x00, 0x00, 0x0a, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0xd7, 0x96, 0xbf, 0xba,
    };
    var output: [16]u8 = undefined;
    try decodeExact(std.testing.allocator, &frame, &output, .{});
    var corrupt = frame;
    corrupt[corrupt.len - 1] ^= 1;
    try std.testing.expectError(error.ChecksumFailure, decodeExact(std.testing.allocator, &corrupt, &output, .{}));
}
