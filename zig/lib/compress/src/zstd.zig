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
    const Native = @import("zstd_native.zig");
    const dictionary = if (options.dictionary.len > 0) try Native.Dictionary.init(a, options.dictionary) else null;
    defer if (dictionary) |value| value.deinit(a);
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(a);
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
        const part = d.reader.allocRemaining(a, .limited(limit - out.items.len)) catch |err| return d.err orelse err;
        defer a.free(part);
        if (checksum) |expected| if (expected != @as(u32, @truncate(std.hash.XxHash64.hash(0, part)))) return error.ChecksumFailure;
        try out.appendSlice(a, part);
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
