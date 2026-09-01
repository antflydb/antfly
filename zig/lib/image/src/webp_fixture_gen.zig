// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const webp = @import("webp.zig");

const Allocator = std.mem.Allocator;
const chunk_header_len = 8;
const vp8x_flag_alpha = 0x10;
const vp8x_flag_animation = 0x02;

pub fn main(init: std.process.Init) !void {
    const alloc = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, alloc);
    defer args.deinit();

    _ = args.next();
    const out_root = args.next() orelse "testdata/image/webp";

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    try createFixtureDir(alloc, io, out_root, "lossless");
    try createFixtureDir(alloc, io, out_root, "lossy");
    try createFixtureDir(alloc, io, out_root, "unsupported");
    try createFixtureDir(alloc, io, out_root, "invalid");

    const vp8l = try buildLiteralVp8lWebp(alloc, 2, 3, .{ 0x44, 0x22, 0x11, 0xff });
    defer alloc.free(vp8l);
    try writeAndDescribe(alloc, io, out_root, "lossless/literal-rgba-2x3.webp", vp8l);

    const first_partition = @as([200]u8, @splat(0));
    const token_partition = @as([64]u8, @splat(0));
    const vp8 = try buildVp8Webp(alloc, 1, 1, &first_partition, &.{&token_partition});
    defer alloc.free(vp8);
    try writeAndDescribe(alloc, io, out_root, "lossy/minimal-vp8-1x1.webp", vp8);

    const alpha_payload = [_]u8{ 0, 0x7d };
    const alpha_vp8 = try buildVp8xAlphVp8Webp(alloc, &alpha_payload, 1, 1, &first_partition, &.{&token_partition});
    defer alloc.free(alpha_vp8);
    try writeAndDescribe(alloc, io, out_root, "lossy/alpha-vp8-1x1.webp", alpha_vp8);

    const animated = [_]u8{
        'R', 'I', 'F', 'F', 30,                  0,   0,   0,
        'W', 'E', 'B', 'P', 'V',                 'P', '8', 'X',
        10,  0,   0,   0,   vp8x_flag_animation, 0,   0,   0,
        0,   0,   0,   0,   0,                   0,   'A', 'N',
        'I', 'M', 0,   0,   0,                   0,
    };
    try writeFixture(alloc, io, out_root, "unsupported/animated-1x1.webp", &animated);

    const truncated = [_]u8{ 'R', 'I', 'F', 'F', 12, 0, 0, 0, 'W', 'E', 'B', 'P', 'V', 'P' };
    try writeFixture(alloc, io, out_root, "invalid/truncated-riff.webp", &truncated);
}

fn join(alloc: Allocator, base: []const u8, rel: []const u8) ![]u8 {
    return try std.fs.path.join(alloc, &.{ base, rel });
}

fn createFixtureDir(alloc: Allocator, io: anytype, out_root: []const u8, rel: []const u8) !void {
    const path = try join(alloc, out_root, rel);
    defer alloc.free(path);
    try std.Io.Dir.cwd().createDirPath(io, path);
}

fn writeFixture(alloc: Allocator, io: anytype, out_root: []const u8, rel: []const u8, bytes: []const u8) !void {
    const path = try join(alloc, out_root, rel);
    defer alloc.free(path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = bytes });
}

fn writeAndDescribe(alloc: Allocator, io: anytype, out_root: []const u8, rel: []const u8, bytes: []const u8) !void {
    try writeFixture(alloc, io, out_root, rel, bytes);

    const decoded = try webp.decodeRgba(alloc, bytes);
    defer alloc.free(decoded.rgba);

    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(decoded.rgba, &hash, .{});
    std.debug.print("{s}\t{d}x{d}\t{s}\n", .{ rel, decoded.width, decoded.height, std.fmt.bytesToHex(hash, .lower) });
}

fn appendU32Le(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u32) !void {
    try out.append(alloc, @intCast(value & 0xff));
    try out.append(alloc, @intCast((value >> 8) & 0xff));
    try out.append(alloc, @intCast((value >> 16) & 0xff));
    try out.append(alloc, @intCast((value >> 24) & 0xff));
}

fn buildVp8Payload(alloc: Allocator, width: u16, height: u16, first_partition: []const u8, token_partitions: []const []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    const frame_tag = (@as(u32, @intCast(first_partition.len)) << 5) | 0x10;
    try out.append(alloc, @intCast(frame_tag & 0xff));
    try out.append(alloc, @intCast((frame_tag >> 8) & 0xff));
    try out.append(alloc, @intCast((frame_tag >> 16) & 0xff));
    try out.appendSlice(alloc, &.{ 0x9d, 0x01, 0x2a });
    try out.append(alloc, @intCast(width & 0xff));
    try out.append(alloc, @intCast(width >> 8));
    try out.append(alloc, @intCast(height & 0xff));
    try out.append(alloc, @intCast(height >> 8));
    try out.appendSlice(alloc, first_partition);
    for (token_partitions[0 .. token_partitions.len - 1]) |partition| {
        const len = partition.len;
        try out.append(alloc, @intCast(len & 0xff));
        try out.append(alloc, @intCast((len >> 8) & 0xff));
        try out.append(alloc, @intCast((len >> 16) & 0xff));
    }
    for (token_partitions) |partition| try out.appendSlice(alloc, partition);
    return try out.toOwnedSlice(alloc);
}

fn buildVp8Webp(alloc: Allocator, width: u16, height: u16, first_partition: []const u8, token_partitions: []const []const u8) ![]u8 {
    const payload = try buildVp8Payload(alloc, width, height, first_partition, token_partitions);
    defer alloc.free(payload);

    const padded_payload_len = payload.len + (payload.len & 1);
    const riff_size: u32 = @intCast(4 + chunk_header_len + padded_payload_len);

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "RIFF");
    try appendU32Le(alloc, &out, riff_size);
    try out.appendSlice(alloc, "WEBP");
    try out.appendSlice(alloc, "VP8 ");
    try appendU32Le(alloc, &out, @intCast(payload.len));
    try out.appendSlice(alloc, payload);
    if ((payload.len & 1) != 0) try out.append(alloc, 0);
    return try out.toOwnedSlice(alloc);
}

fn buildVp8xAlphVp8Webp(alloc: Allocator, alpha_payload: []const u8, width: u16, height: u16, first_partition: []const u8, token_partitions: []const []const u8) ![]u8 {
    const vp8_payload = try buildVp8Payload(alloc, width, height, first_partition, token_partitions);
    defer alloc.free(vp8_payload);

    const alpha_padded_len = alpha_payload.len + (alpha_payload.len & 1);
    const vp8_padded_len = vp8_payload.len + (vp8_payload.len & 1);
    const riff_size: u32 = @intCast(4 + chunk_header_len + 10 + chunk_header_len + alpha_padded_len + chunk_header_len + vp8_padded_len);

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "RIFF");
    try appendU32Le(alloc, &out, riff_size);
    try out.appendSlice(alloc, "WEBP");
    try out.appendSlice(alloc, "VP8X");
    try appendU32Le(alloc, &out, 10);
    try out.append(alloc, vp8x_flag_alpha);
    try out.appendSlice(alloc, &.{ 0, 0, 0 });
    try out.append(alloc, @intCast((width - 1) & 0xff));
    try out.append(alloc, @intCast((width - 1) >> 8));
    try out.append(alloc, 0);
    try out.append(alloc, @intCast((height - 1) & 0xff));
    try out.append(alloc, @intCast((height - 1) >> 8));
    try out.append(alloc, 0);
    try out.appendSlice(alloc, "ALPH");
    try appendU32Le(alloc, &out, @intCast(alpha_payload.len));
    try out.appendSlice(alloc, alpha_payload);
    if ((alpha_payload.len & 1) != 0) try out.append(alloc, 0);
    try out.appendSlice(alloc, "VP8 ");
    try appendU32Le(alloc, &out, @intCast(vp8_payload.len));
    try out.appendSlice(alloc, vp8_payload);
    if ((vp8_payload.len & 1) != 0) try out.append(alloc, 0);
    return try out.toOwnedSlice(alloc);
}

const BitWriter = struct {
    bytes: std.ArrayListUnmanaged(u8) = .empty,
    bit_pos: u3 = 0,

    fn deinit(self: *BitWriter, alloc: Allocator) void {
        self.bytes.deinit(alloc);
        self.* = undefined;
    }

    fn writeBits(self: *BitWriter, alloc: Allocator, value: u32, count: u5) !void {
        var i: u5 = 0;
        while (i < count) : (i += 1) {
            try self.writeBit(alloc, @intCast((value >> i) & 1));
        }
    }

    fn writeBit(self: *BitWriter, alloc: Allocator, bit: u1) !void {
        if (self.bit_pos == 0) try self.bytes.append(alloc, 0);
        const last = self.bytes.items.len - 1;
        self.bytes.items[last] |= @as(u8, bit) << self.bit_pos;
        self.bit_pos +%= 1;
    }
};

fn writeSimplePrefixSymbol(writer: *BitWriter, alloc: Allocator, symbol: u8) !void {
    try writer.writeBits(alloc, 1, 1);
    try writer.writeBits(alloc, 0, 1);
    try writer.writeBits(alloc, 1, 1);
    try writer.writeBits(alloc, symbol, 8);
}

fn buildLiteralVp8lWebp(alloc: Allocator, width: u32, height: u32, rgba: [4]u8) ![]u8 {
    var payload = std.ArrayListUnmanaged(u8).empty;
    defer payload.deinit(alloc);
    try payload.append(alloc, 0x2f);

    var bits = BitWriter{};
    defer bits.deinit(alloc);
    try bits.writeBits(alloc, width - 1, 14);
    try bits.writeBits(alloc, height - 1, 14);
    try bits.writeBits(alloc, if (rgba[3] == 255) 0 else 1, 1);
    try bits.writeBits(alloc, 0, 3);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 0, 1);
    try writeSimplePrefixSymbol(&bits, alloc, rgba[1]);
    try writeSimplePrefixSymbol(&bits, alloc, rgba[0]);
    try writeSimplePrefixSymbol(&bits, alloc, rgba[2]);
    try writeSimplePrefixSymbol(&bits, alloc, rgba[3]);
    try writeSimplePrefixSymbol(&bits, alloc, 0);
    try payload.appendSlice(alloc, bits.bytes.items);

    const padded_payload_len = payload.items.len + (payload.items.len & 1);
    const riff_size: u32 = @intCast(4 + chunk_header_len + padded_payload_len);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "RIFF");
    try appendU32Le(alloc, &out, riff_size);
    try out.appendSlice(alloc, "WEBP");
    try out.appendSlice(alloc, "VP8L");
    try appendU32Le(alloc, &out, @intCast(payload.items.len));
    try out.appendSlice(alloc, payload.items);
    if ((payload.items.len & 1) != 0) try out.append(alloc, 0);
    return try out.toOwnedSlice(alloc);
}
