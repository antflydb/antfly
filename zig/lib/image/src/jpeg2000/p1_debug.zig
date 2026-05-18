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
const decode = @import("decode.zig");
const compat = @import("compat.zig");
const codeblock = @import("codeblock.zig");
const codestream = @import("codestream.zig");
const packet = @import("packet.zig");
const reconstruct = @import("reconstruct.zig");

fn readPgxU16(allocator: std.mem.Allocator, path: []const u8) !struct {
    width: usize,
    height: usize,
    samples: []u16,
} {
    const bytes = try compat.cwd().readFileAlloc(compat.io(), path, allocator, .limited(1 << 30));
    defer allocator.free(bytes);

    const nl = std.mem.indexOfScalar(u8, bytes, '\n') orelse return error.InvalidPgx;
    const header = bytes[0..nl];
    var it = std.mem.tokenizeScalar(u8, header, ' ');
    if (!std.mem.eql(u8, it.next() orelse return error.InvalidPgx, "PG")) return error.InvalidPgx;
    if (!std.mem.eql(u8, it.next() orelse return error.InvalidPgx, "ML")) return error.InvalidPgx;
    var token = it.next() orelse return error.InvalidPgx;
    if (std.mem.eql(u8, token, "+") or std.mem.eql(u8, token, "-")) token = it.next() orelse return error.InvalidPgx;
    const depth = try std.fmt.parseInt(u8, token, 10);
    if (depth > 16) return error.InvalidPgx;
    const width = try std.fmt.parseInt(usize, it.next() orelse return error.InvalidPgx, 10);
    const height = try std.fmt.parseInt(usize, it.next() orelse return error.InvalidPgx, 10);
    const payload = bytes[nl + 1 ..];
    if (payload.len < width * height * 2) return error.InvalidPgx;
    const samples = try allocator.alloc(u16, width * height);
    errdefer allocator.free(samples);
    for (samples, 0..) |*sample, i| {
        sample.* = std.mem.readInt(u16, @ptrCast(payload[i * 2 .. i * 2 + 2].ptr), .big);
    }
    return .{ .width = width, .height = height, .samples = samples };
}

fn writePgxU16(path: []const u8, width: usize, height: usize, samples: []const u16) !void {
    const allocator = std.heap.page_allocator;
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);
    const header = try std.fmt.allocPrint(allocator, "PG ML 12 {d} {d}\n", .{ width, height });
    defer allocator.free(header);
    try out.appendSlice(allocator, header);
    var pair: [2]u8 = undefined;
    for (samples) |sample| {
        std.mem.writeInt(u16, &pair, sample, .big);
        try out.appendSlice(allocator, &pair);
    }
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = out.items });
}

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const input = try compat.cwd().readFileAlloc(compat.io(), "/tmp/openjpeg-data/input/conformance/p1_04.j2k", allocator, .limited(1 << 30));
    defer allocator.free(input);

    var state = try codestream.parseState(allocator, input);
    defer state.deinit(allocator);
    const ranges = try codestream.parseTilePartRanges(allocator, input);
    defer allocator.free(ranges);
    std.debug.print("tiles={d} cb_exp={d},{d} qsteps={d}\n", .{
        ranges.len,
        state.coding_style.?.code_block_width_exponent,
        state.coding_style.?.code_block_height_exponent,
        state.quantization_style.?.step_values.len,
    });
    for (state.quantization_style.?.step_values, 0..) |step, i| {
        std.debug.print(" q[{d}]=0x{x:0>4} exp={d} mant={d}\n", .{ i, step, step >> 11, step & 0x7ff });
    }

    const r0 = ranges[0];
    var tile_state = codestream.State{
        .header = .{
            .width = 128,
            .height = 128,
            .components = state.header.components,
            .tile_width = 128,
            .tile_height = 128,
            .uses_multiple_tiles = false,
        },
        .coding_style = state.coding_style,
        .quantization_style = if (state.tile_parts[0].quantization_style) |q| q else state.quantization_style,
        .comments = state.comments,
        .tile_parts = &.{},
        .has_start_of_data = true,
        .has_end_of_codestream = true,
    };
    var model = try packet.buildPacketModelFromPayload(allocator, &tile_state, input[r0.data_offset..r0.next_offset], 0, .packet_present_tagtree_first_inclusion);
    defer model.deinit(allocator);
    std.debug.print("tile0 payload={d} entries={d}\n", .{ r0.next_offset - r0.data_offset, model.entries.len });
    for (model.entries, 0..) |entry, i| {
        std.debug.print(" e[{d}] r={d} sb={s} zbp={d} passes={d} off={d} len={d} rect={d}x{d}\n", .{
            i,
            entry.coordinate.resolution_index,
            @tagName(entry.subband),
            entry.zero_bit_planes,
            entry.num_coding_passes,
            entry.body_offset,
            entry.body_length,
            entry.rect.width(),
            entry.rect.height(),
        });
    }
    var execution = try packet.executeTier1SegmentsForState(
        allocator,
        &model,
        input[r0.data_offset..r0.next_offset],
        &tile_state,
        .standard,
        codeblock.RefinementPolicy.midpoint_signed,
        codeblock.MagnitudePolicy.midpoint,
        0,
        .standard,
    );
    defer execution.deinit(allocator);
    for (execution.codeblocks, 0..) |cb, i| {
        var max_mag: i32 = 0;
        var nonzero: usize = 0;
        for (cb.grid.cells) |cell| {
            if (cell.magnitude != 0) nonzero += 1;
            if (cell.magnitude > max_mag) max_mag = cell.magnitude;
        }
        std.debug.print(" cb[{d}] r={d} sb={s} passes={d} nonzero={d} maxmag={d}\n", .{
            i,
            cb.coordinate.resolution_index,
            @tagName(cb.subband),
            cb.executed_passes,
            nonzero,
            max_mag,
        });
    }
    const planes0 = try reconstruct.assemblePlanesFromTier1Irreversible(allocator, &tile_state, &execution);
    defer {
        for (planes0) |plane| allocator.free(plane);
        allocator.free(planes0);
    }
    var raw_min: i32 = std.math.maxInt(i32);
    var raw_max: i32 = std.math.minInt(i32);
    for (planes0[0]) |v| {
        raw_min = @min(raw_min, v);
        raw_max = @max(raw_max, v);
    }
    std.debug.print("tile0 raw min={d} max={d} first16:", .{ raw_min, raw_max });
    for (planes0[0][0..16]) |v| std.debug.print(" {d}", .{v});
    std.debug.print("\n", .{});

    var decoded = try decode.decodeU16Bytes(allocator, input);
    defer decoded.deinit();
    try writePgxU16("/tmp/p1_04_ours.pgx", decoded.width, decoded.height, decoded.pixels);

    const ref = try readPgxU16(allocator, "/tmp/openjpeg-data/baseline/conformance/c1p1_04_0.pgx");
    defer allocator.free(ref.samples);

    var max_abs: i32 = 0;
    var max_idx: usize = 0;
    var sum: i64 = 0;
    var clipped_hi: usize = 0;
    var clipped_lo: usize = 0;
    var tile_max = [_]i32{0} ** 64;
    var tile_max_idx = [_]usize{0} ** 64;
    for (decoded.pixels, 0..) |sample, i| {
        if (sample == 4095) clipped_hi += 1;
        if (sample == 0) clipped_lo += 1;
        const diff = @as(i32, @intCast(sample)) - @as(i32, @intCast(ref.samples[i]));
        sum += diff;
        const abs = if (diff < 0) -diff else diff;
        const x = i % decoded.width;
        const y = i / decoded.width;
        const tile_idx = (y / 128) * 8 + (x / 128);
        if (tile_idx < tile_max.len and abs > tile_max[tile_idx]) {
            tile_max[tile_idx] = abs;
            tile_max_idx[tile_idx] = i;
        }
        if (abs > max_abs) {
            max_abs = abs;
            max_idx = i;
        }
    }
    std.debug.print("decoded={d}x{d} max_abs={d} idx={d} diff_sum={d} clipped_hi={d} clipped_lo={d}\n", .{
        decoded.width,
        decoded.height,
        max_abs,
        max_idx,
        sum,
        clipped_hi,
        clipped_lo,
    });
    std.debug.print("first16 ours/ref:", .{});
    for (decoded.pixels[0..16], 0..) |sample, i| {
        std.debug.print(" {d}/{d}", .{ sample, ref.samples[i] });
    }
    std.debug.print("\n", .{});
    for (tile_max, 0..) |value, i| {
        if (value > 20) std.debug.print(" tile {d} max={d} idx={d}\n", .{ i, value, tile_max_idx[i] });
    }
}
