//! Offline experiment only. No serving defaults or durable formats change.
//! Reads SGRP fixtures emitted by scripts/probe_dense_global_subgroups.py.
//! Score timings include one query quantization for i8; sorting is separate.
const std = @import("std");
const Mode = enum { scalar_f64, simd_f32, simd_f16, simd_i8 };
const lanes = 16;

pub fn scalarDot(q: []const f32, c: []const f32) f64 {
    var dot: f64 = 0;
    for (q, c) |a, b| dot += @as(f64, a) * b;
    return dot;
}

fn simdDot(comptime T: type, q: []const f32, c: []const T) f32 {
    var sum: @Vector(lanes, f32) = @splat(0);
    var i: usize = 0;
    while (i + lanes <= q.len) : (i += lanes) {
        const a: @Vector(lanes, f32) = q[i..][0..lanes].*;
        const b: @Vector(lanes, T) = c[i..][0..lanes].*;
        sum += a * @as(@Vector(lanes, f32), @floatCast(b));
    }
    var dot = @reduce(.Add, sum);
    while (i < q.len) : (i += 1) dot += q[i] * @as(f32, c[i]);
    return dot;
}

pub fn intDot(q: []const i8, c: []const i8) i32 {
    var sum: @Vector(lanes, i32) = @splat(0);
    var i: usize = 0;
    while (i + lanes <= q.len) : (i += lanes) {
        const a: @Vector(lanes, i8) = q[i..][0..lanes].*;
        const b: @Vector(lanes, i8) = c[i..][0..lanes].*;
        sum += @as(@Vector(lanes, i32), @intCast(a)) * @as(@Vector(lanes, i32), @intCast(b));
    }
    var dot = @reduce(.Add, sum);
    while (i < q.len) : (i += 1) dot += @as(i32, q[i]) * c[i];
    return dot;
}

pub fn quantize(source: []const f32, dest: []i8) f32 {
    var largest: f32 = 0;
    for (source) |x| largest = @max(largest, @abs(x));
    const scale = if (largest == 0) 1 else largest / 127;
    for (source, dest) |x, *out| out.* = @intFromFloat(std.math.clamp(@round(x / scale), -127, 127));
    return scale;
}

const Ranked = struct {
    score: f64,
    id: u32,
    fn less(_: void, a: Ranked, b: Ranked) bool {
        return a.score > b.score or (a.score == b.score and a.id < b.id);
    }
};

fn u32At(bytes: []const u8, offset: usize) u32 {
    return std.mem.readInt(u32, bytes[offset..][0..4], .little);
}

pub fn main(init: std.process.Init) !void {
    const alloc = init.gpa;
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    if (args.len != 2) return error.ExpectedFixturePath;
    const bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, args[1], alloc, .limited(160 * 1024 * 1024));
    defer alloc.free(bytes);
    if (bytes.len < 16 or !std.mem.eql(u8, bytes[0..4], "SGRP")) return error.InvalidFixture;
    const count: usize = u32At(bytes, 4);
    const dims: usize = u32At(bytes, 8);
    const query_count: usize = u32At(bytes, 12);
    if (count == 0 or count > 8192 or dims == 0 or dims > 4096 or query_count == 0 or query_count > 256 or
        bytes.len != 16 + (count + query_count) * dims * 4) return error.InvalidFixture;
    const values = try alloc.alloc(f32, (count + query_count) * dims);
    defer alloc.free(values);
    for (values, 0..) |*out, i| {
        out.* = @bitCast(u32At(bytes, 16 + i * 4));
        if (!std.math.isFinite(out.*) or @abs(out.*) > 1.001) return error.InvalidFixture;
    }
    const centers = values[0 .. count * dims];
    const queries = values[count * dims ..];
    const half = try alloc.alloc(f16, centers.len);
    defer alloc.free(half);
    for (half, centers) |*out, x| out.* = @floatCast(x);
    const codes = try alloc.alloc(i8, centers.len);
    defer alloc.free(codes);
    const scales = try alloc.alloc(f32, count);
    defer alloc.free(scales);
    for (scales, 0..) |*out, i| out.* = quantize(centers[i * dims ..][0..dims], codes[i * dims ..][0..dims]);
    const qcodes = try alloc.alloc(i8, dims);
    defer alloc.free(qcodes);
    const ranked = try alloc.alloc(Ranked, count);
    defer alloc.free(ranked);
    const modes = [_]Mode{ .scalar_f64, .simd_f32, .simd_f16, .simd_i8 };
    // First round warms pages; five measured rounds reverse order on odd rounds.
    for (0..6) |round| {
        for (0..modes.len) |position| {
            const mode = modes[if (round % 2 == 0) position else modes.len - 1 - position];
            var score_ns: i96 = 0;
            var sort_ns: i96 = 0;
            var checksum: f64 = 0;
            for (0..query_count) |qi| {
                const query = queries[qi * dims ..][0..dims];
                const started = std.Io.Clock.awake.now(init.io).nanoseconds;
                const query_scale = if (mode == .simd_i8) quantize(query, qcodes) else 1;
                for (ranked, 0..) |*out, i| {
                    const offset = i * dims;
                    const score: f64 = switch (mode) {
                        .scalar_f64 => scalarDot(query, centers[offset..][0..dims]),
                        .simd_f32 => simdDot(f32, query, centers[offset..][0..dims]),
                        .simd_f16 => simdDot(f16, query, half[offset..][0..dims]),
                        .simd_i8 => @as(f64, @floatFromInt(intDot(qcodes, codes[offset..][0..dims]))) * query_scale * scales[i],
                    };
                    out.* = .{ .score = score, .id = @intCast(i) };
                }
                const scored = std.Io.Clock.awake.now(init.io).nanoseconds;
                std.mem.sort(Ranked, ranked, {}, Ranked.less);
                const sorted = std.Io.Clock.awake.now(init.io).nanoseconds;
                score_ns += scored - started;
                sort_ns += sorted - scored;
                // Consume every result, preventing dead-code elimination.
                for (ranked, 0..) |value, i| checksum += value.score * @as(f64, @floatFromInt(i + value.id + 1));
            }
            std.debug.print("{{\"round\":{},\"warmup\":{},\"mode\":\"{s}\",\"groups\":{},\"dims\":{},\"queries\":{},\"score_ns_per_query\":{d},\"sort_ns_per_query\":{d},\"checksum\":{d}}}\n", .{
                round,                                                                    round == 0,                                                              @tagName(mode), count, dims, query_count,
                @as(f64, @floatFromInt(score_ns)) / @as(f64, @floatFromInt(query_count)), @as(f64, @floatFromInt(sort_ns)) / @as(f64, @floatFromInt(query_count)), checksum,
            });
        }
    }
}

test "portable SIMD representative scoring tails and quantization" {
    var random = std.Random.DefaultPrng.init(593);
    var a: [1537]f32 = undefined;
    var b: [1537]f32 = undefined;
    var half: [1537]f16 = undefined;
    var qa: [1537]i8 = undefined;
    var qb: [1537]i8 = undefined;
    for (&a, &b, &half) |*x, *y, *h| {
        x.* = random.random().float(f32) * 2 - 1;
        y.* = random.random().float(f32) * 2 - 1;
        h.* = @floatCast(y.*);
    }
    for ([_]usize{ 1, 15, 16, 17, 768, 1536, 1537 }) |dims| {
        try std.testing.expectApproxEqAbs(scalarDot(a[0..dims], b[0..dims]), @as(f64, simdDot(f32, a[0..dims], b[0..dims])), 0.0001);
        var expected_half: f64 = 0;
        for (a[0..dims], half[0..dims]) |x, y| expected_half += @as(f64, x) * @as(f32, y);
        try std.testing.expectApproxEqAbs(expected_half, @as(f64, simdDot(f16, a[0..dims], half[0..dims])), 0.0001);
        _ = quantize(a[0..dims], qa[0..dims]);
        _ = quantize(b[0..dims], qb[0..dims]);
        var expected_int: i32 = 0;
        for (qa[0..dims], qb[0..dims]) |x, y| expected_int += @as(i32, x) * y;
        try std.testing.expectEqual(expected_int, intDot(qa[0..dims], qb[0..dims]));
    }
    @memset(&qa, 127);
    @memset(&qb, -127);
    try std.testing.expectEqual(@as(i32, -127 * 127 * 1537), intDot(&qa, &qb));
    var ties: [5]i8 = undefined;
    try std.testing.expectEqual(@as(f32, 1), quantize(&.{ 127, 0.5, -0.5, 1.5, -1.5 }, &ties));
    try std.testing.expectEqualSlices(i8, &.{ 127, 1, -1, 2, -2 }, &ties);
}
