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

//! Tile kernel tests: every lowering against the scalar reference.
//! Run with `zig build test-tile-kernels`.

const std = @import("std");
const tile = @import("tile.zig");
const cpu = @import("cpu.zig");
const gpu = @import("gpu.zig");
const whisper = @import("whisper_logits.zig");

const k = whisper.kernel;

const Params = struct {
    out_dim: u32,
    suppress_count: u32 = 0,
    ts_begin: u32,
    text_allowed: u32 = 1,
    ts_min: u32,
    ts_max: u32,
    eot: u32,
    probe_id: u32 = 0,
    mode: u32 = 0,
    token_slot: u32 = 0,
    stats_slot: u32 = 0,
    reserved: u32 = 0,
};

fn expectStates(expected: k.Result(), actual: k.Result()) !void {
    inline for (k.channels, 0..) |c, ci| {
        const e = expected[ci];
        const a = actual[ci];
        switch (c.reduce) {
            .argmax => {
                if (e.id() != a.id() or !(e.a == a.a or (std.math.isNan(e.a) and std.math.isNan(a.a)))) {
                    std.debug.print("channel {s}: expected ({d}, {d}) got ({d}, {d})\n", .{ c.name, e.a, e.id(), a.a, a.id() });
                    return error.TestExpectedEqual;
                }
            },
            .logsumexp => {
                try std.testing.expectEqual(e.a, a.a);
                try std.testing.expectApproxEqRel(e.b, a.b, 1e-4);
            },
        }
    }
}

const Case = struct {
    values: []f32,
    params: Params,
    suppress: []i32,
    grammar: [8]u32,
};

fn randomCase(allocator: std.mem.Allocator, rng: std.Random) !Case {
    const sizes = [_]usize{ 0, 1, 7, 33, 1024, 1500, 4099, 51_865 };
    const n = sizes[rng.uintLessThan(usize, sizes.len)];
    const values = try allocator.alloc(f32, n + rng.uintLessThan(usize, 3));
    const coarse = rng.boolean(); // coarse values produce exact ties
    for (values) |*v| {
        v.* = if (coarse) @floatFromInt(rng.intRangeAtMost(i32, -4, 4)) else (rng.float(f32) - 0.5) * 40;
        if (rng.uintLessThan(u32, 50) == 0) v.* = tile.neg_inf;
    }
    const dim: u32 = @intCast(n);
    const ts_begin: u32 = if (dim == 0) 0 else rng.uintAtMost(u32, dim);
    const ts_min = ts_begin + (if (dim > ts_begin) rng.uintAtMost(u32, dim - ts_begin) else 0);
    const ts_max = ts_min + (if (dim > ts_min) rng.uintAtMost(u32, dim - ts_min) else 0);
    const suppress = try allocator.alloc(i32, rng.uintLessThan(usize, 100));
    for (suppress) |*s| s.* = rng.intRangeAtMost(i32, -2, @intCast(dim + 2));
    var grammar: [8]u32 = @splat(0);
    grammar[0] = rng.uintLessThan(u32, 2);
    grammar[1] = ts_begin;
    grammar[2] = ts_max;
    return .{
        .values = values,
        .params = .{
            .out_dim = dim,
            .suppress_count = @intCast(suppress.len),
            .ts_begin = ts_begin,
            .text_allowed = rng.uintLessThan(u32, 2),
            .ts_min = ts_min,
            .ts_max = ts_max,
            .eot = if (dim == 0) 0 else rng.uintLessThan(u32, dim + 1),
            .mode = rng.uintLessThan(u32, 16),
        },
        .suppress = suppress,
        .grammar = grammar,
    };
}

test "tile whisper logits: CPU vector lowering matches the scalar reference" {
    var prng = std.Random.DefaultPrng.init(0x7117_e5ee);
    const rng = prng.random();
    for (0..200) |_| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const case = try randomCase(arena.allocator(), rng);
        const args = whisper.args(case.values, case.params, case.suppress, &case.grammar);
        const expected = tile.reference(k, args);
        try expectStates(expected, cpu.RowReduce(k, 8).run(&args));
        try expectStates(expected, cpu.RowReduce(k, 16).run(&args));
    }
}

test "tile whisper logits: block partials combine to the full reduction" {
    var prng = std.Random.DefaultPrng.init(0x9a27_1a15);
    const rng = prng.random();
    for (0..100) |_| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const case = try randomCase(arena.allocator(), rng);
        const args = whisper.args(case.values, case.params, case.suppress, &case.grammar);
        const blocks = (case.values.len + k.block - 1) / k.block;
        const records = try arena.allocator().alloc(f32, @max(blocks, 1) * k.partial_stride);
        const written = cpu.RowReduce(k, 8).partials(&args, records);
        try expectStates(tile.reference(k, args), tile.combineRecords(k, records[0 .. written * k.partial_stride]));
    }
}

test "tile whisper logits: grammar window overrides params in device-window mode" {
    var values: [64]f32 = undefined;
    for (&values, 0..) |*v, i| v.* = @floatFromInt(i);
    const base: Params = .{ .out_dim = 64, .ts_begin = 32, .text_allowed = 1, .ts_min = 32, .ts_max = 64, .eot = 31 };
    const grammar = [8]u32{ 0, 40, 48, 0, 0, 0, 0, 0 };

    const plain = tile.reference(k, whisper.args(&values, base, &.{}, &grammar));
    try std.testing.expectEqual(@as(u32, 63), plain[0].id()); // best
    try std.testing.expectEqual(@as(u32, 31), plain[1].id()); // text

    var windowed = base;
    windowed.mode = 1;
    const out = tile.reference(k, whisper.args(&values, windowed, &.{}, &grammar));
    try std.testing.expectEqual(@as(u32, 47), out[0].id()); // best: ts window [40, 48)
    try std.testing.expectEqual(@as(u32, 31), out[1].id()); // text: only eot survives
    try std.testing.expectEqual(@as(u32, 47), out[2].id()); // ts
    try std.testing.expectEqual(@as(u32, 63), out[3].id()); // raw ignores masks
    try expectStates(out, cpu.RowReduce(k, 8).run(&whisper.args(&values, windowed, &.{}, &grammar)));
}

test "tile whisper logits: suppressed eot and -inf logits" {
    var values = [_]f32{ tile.neg_inf, 1, 5, tile.neg_inf, 2 };
    const params: Params = .{ .out_dim = 5, .ts_begin = 3, .ts_min = 3, .ts_max = 5, .eot = 2 };
    const out = tile.reference(k, whisper.args(&values, params, &.{ 2, -1 }, &@as([8]u32, @splat(0))));
    try std.testing.expectEqual(@as(u32, 4), out[0].id()); // eot (index 2) suppressed
    try std.testing.expectEqual(@as(u32, 1), out[1].id());
    // -inf contributes nothing; the hand-written kernel's push would turn a
    // leading -inf into NaN.
    try std.testing.expect(!std.math.isNan(out[4].b));
    try std.testing.expectApproxEqRel(@exp(@as(f32, -1)) + 1, out[4].b, 1e-6);
}

test "tile whisper logits: rendered sources keep the hand-written ABI" {
    const msl = comptime gpu.renderMetal(k);
    try std.testing.expect(std.mem.indexOf(u8, msl, "kernel void antfly_tile_whisper_logits_partials(device const float *logits [[buffer(0)]], device const int *suppress_ids [[buffer(1)]], device float *partials [[buffer(2)]], constant antfly_tile_whisper_logits_params &p [[buffer(3)]], device const uint *grammar [[buffer(4)]], threadgroup float *sh [[threadgroup(0)]]") != null);
    try std.testing.expect(std.mem.indexOf(u8, msl, "struct antfly_tile_whisper_logits_params { uint out_dim; uint suppress_count; uint ts_begin; uint text_allowed; uint ts_min; uint ts_max; uint eot; uint probe_id; uint mode; uint token_slot; uint stats_slot; uint reserved; };") != null);
    const cu = comptime gpu.renderCuda(k);
    try std.testing.expect(std.mem.indexOf(u8, cu, "extern \"C\" __global__ void __launch_bounds__(256) antfly_tile_whisper_logits_partials(") != null);
    try std.testing.expect(std.mem.indexOf(u8, cu, "antfly_tile_whisper_logits_partials_combine(") != null);
}

test "tile cpu vexp matches @exp" {
    var x: f32 = -90;
    while (x < 88) : (x += 0.0137) {
        const v: @Vector(8, f32) = @splat(x);
        const got = cpu.vexp(8, v)[0];
        const want = @exp(x);
        if (x < -87.3) {
            try std.testing.expectEqual(@as(f32, 0), got);
        } else {
            try std.testing.expectApproxEqRel(want, got, 4e-7);
        }
    }
    try std.testing.expectEqual(@as(f32, 0), cpu.vexp(8, @splat(tile.neg_inf))[0]);
    try std.testing.expectEqual(@as(f32, 1), cpu.vexp(8, @splat(0))[0]);
}
