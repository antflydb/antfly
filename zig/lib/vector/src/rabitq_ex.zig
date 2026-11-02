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

//! Extended (multi-bit) RaBitQ: 1 to 8 bits per dimension.
//!
//! Reference: "Practical and Asymptotically Optimal Quantization of
//! High-Dimensional Vectors in Euclidean Space for Approximate Nearest
//! Neighbor Search" by Jianyang Gao, Yutong Gou, Yuexuan Xu, Yongyi Yang,
//! Cheng Long and Raymond Chi-Wing Wong (SIGMOD 2025).
//!
//! A vector x is coded relative to a centroid c, in an already rotated
//! space (callers apply the random rotation). With residual r = x - c and
//! o = r / |r|, the code is the point y of the grid
//! {±0.5, ±1.5, ..., ±(2^(B-1) - 0.5)}^D whose direction is closest to o,
//! found exactly by sweeping the scale at which each coordinate's level
//! steps up. Distances use the unbiased estimator
//! <o, q> ≈ <ŷ, q> / <ŷ, o> with ŷ = y / |y|, so the only error comes from
//! the code (the query stays full precision). With B = 1 the grid is the
//! sign vector and this is the original RaBitQ estimator.
//!
//! Each row is self-contained bytes: the packed code (B bits per dimension,
//! little-endian bit order), then f32 |r|, f32 1 / (|y| <ŷ, o>), f32
//! <ŷ, o>, and for inner product and cosine f32 <x, c>.

const std = @import("std");
const Allocator = std.mem.Allocator;
const vec = @import("vector.zig");

pub const max_bits: u8 = 8;

pub const Metric = vec.DistanceMetric;

/// Scale applied to the estimator's standard deviation to form the error
/// bound. 4/3 makes 1-bit bounds match the 1-bit kernel's
/// `|r| |q - c| / sqrt(D)` for a typical <ŷ, o> of 0.8, so a given
/// `bound_scale` means the same confidence for both kernels.
pub const bound_sigmas: f32 = 4.0 / 3.0;

pub const Estimate = struct { distance: f32, bound: f32 };

pub const Codec = struct {
    dims: usize,
    bits: u8,
    metric: Metric,

    pub fn init(dims: usize, bits: u8, metric: Metric) !Codec {
        if (dims < 2 or bits == 0 or bits > max_bits) return error.InvalidConfig;
        return .{ .dims = dims, .bits = bits, .metric = metric };
    }

    pub fn codeBytes(self: Codec) usize {
        return (self.dims * self.bits + 7) / 8;
    }

    pub fn rowBytes(self: Codec) usize {
        return self.codeBytes() + 12 + (if (self.metric != .l2_squared) @as(usize, 4) else 0);
    }

    fn levels(self: Codec) u32 {
        return @as(u32, 1) << @intCast(self.bits - 1);
    }

    /// Encodes `x` (rotated space) into `row` (`rowBytes` long). `scratch`
    /// holds at least `2 * dims` floats plus `dims * (levels - 1)` events;
    /// use `encodeAlloc` when allocation is fine.
    pub fn encodeAlloc(self: Codec, alloc: Allocator, centroid: []const f32, x: []const f32, row: []u8) !void {
        std.debug.assert(x.len == self.dims and centroid.len == self.dims and row.len == self.rowBytes());
        const d = self.dims;
        const r = try alloc.alloc(f32, d);
        defer alloc.free(r);
        vec.subTo(r, x, centroid);
        const r_norm = vec.norm(r);
        if (r_norm > 0) {
            for (r) |*v| v.* /= r_norm;
        } else @memset(r, 0);
        const f = try self.encodeUnit(alloc, r, row[0..self.codeBytes()]);
        var pos = self.codeBytes();
        writeF32(row[pos..], r_norm);
        writeF32(row[pos + 4 ..], f.inv);
        writeF32(row[pos + 8 ..], f.cos);
        pos += 12;
        if (self.metric != .l2_squared) writeF32(row[pos..], vec.dot(x, centroid));
    }

    /// Per-vector factors stored next to a code (column layouts keep |r|
    /// and <x, c> in their own columns).
    pub const Factors = struct {
        /// 1 / (|y| <ŷ, o>); zero for a vector equal to the centroid.
        inv: f32,
        /// <ŷ, o>.
        cos: f32,
    };

    /// Encodes a unit residual `o` = (x - c) / |x - c| (all zeros when x is
    /// the centroid) into `code` (`codeBytes` long).
    pub fn encodeUnit(self: Codec, alloc: Allocator, o: []const f32, code: []u8) !Factors {
        std.debug.assert(o.len == self.dims and code.len == self.codeBytes());
        const d = self.dims;
        const mags = try alloc.alloc(u8, d);
        defer alloc.free(mags);
        @memset(mags, 0);
        var zero = true;
        for (o) |v| {
            if (v != 0) zero = false;
        }
        var a: f32 = 1;
        var y_norm: f32 = @sqrt(@as(f32, @floatFromInt(d))) * 0.5;
        if (!zero) {
            const best = try self.bestLevels(alloc, o, mags);
            a = best.cos;
            y_norm = best.y_norm;
        }
        @memset(code, 0);
        const lv = self.levels();
        for (0..d) |i| {
            const m: u32 = mags[i];
            // u in [0, 2L): negative coordinates map below L, positive at or above.
            const u: u32 = if (o[i] < 0) lv - 1 - m else lv + m;
            putBits(code, i * self.bits, self.bits, u);
        }
        return .{ .inv = if (zero or a <= 0) 0 else 1.0 / (y_norm * a), .cos = a };
    }

    const Best = struct { cos: f32, y_norm: f32 };

    /// Chooses per-dimension magnitude levels m (|y_i| = m_i + 0.5)
    /// maximizing <y, |o|> / |y|, by sweeping the critical scales in order.
    /// Chooses per-dimension magnitude levels m (|y_i| = m_i + 0.5)
    /// maximizing <y, |o|> / |y|. The levels at scale t are
    /// m_i = min(floor(t |o_i|), L - 1); a golden-section search over log t
    /// finds the best scale with O(D) work per probe and no allocation. On
    /// rotated (near-Gaussian) residuals it matches the exact sweep
    /// (`bestLevelsExact`) to about five digits of <ŷ, o> at a fraction of
    /// the cost, which matters for bulk builds.
    fn bestLevels(self: Codec, alloc: Allocator, o: []const f32, mags: []u8) !Best {
        _ = alloc;
        const lv = self.levels();
        var max_abs: f32 = 0;
        var sum_abs: f64 = 0;
        for (o) |v| {
            max_abs = @max(max_abs, @abs(v));
            sum_abs += @abs(v);
        }
        const zero = Probe{ .ip = 0.5 * sum_abs, .ny2 = 0.25 * @as(f64, @floatFromInt(o.len)) };
        if (lv == 1 or max_abs == 0) {
            @memset(mags, 0);
            return zero.best();
        }
        const mean_abs: f64 = sum_abs / @as(f64, @floatFromInt(o.len));
        const levels_f: f64 = @floatFromInt(lv);
        // Below 1 / max|o| every level is 0 (the 1-bit code); well above
        // L / mean|o| most dimensions saturate. The optimum lies between.
        var lo: f64 = @log(0.5 / @as(f64, max_abs));
        var hi: f64 = @log(4 * levels_f / mean_abs);
        if (hi <= lo) hi = lo + 1;
        const g: f64 = 0.6180339887498949;
        var x1 = hi - g * (hi - lo);
        var x2 = lo + g * (hi - lo);
        var f1 = probeAt(o, @floatCast(@exp(x1)), lv);
        var f2 = probeAt(o, @floatCast(@exp(x2)), lv);
        var best_t: f64 = 0;
        var best = zero;
        if (f1.ratio() > best.ratio()) {
            best = f1;
            best_t = @exp(x1);
        }
        if (f2.ratio() > best.ratio()) {
            best = f2;
            best_t = @exp(x2);
        }
        for (0..golden_iterations) |_| {
            if (f1.ratio() < f2.ratio()) {
                lo = x1;
                x1 = x2;
                f1 = f2;
                x2 = lo + g * (hi - lo);
                f2 = probeAt(o, @floatCast(@exp(x2)), lv);
                if (f2.ratio() > best.ratio()) {
                    best = f2;
                    best_t = @exp(x2);
                }
            } else {
                hi = x2;
                x2 = x1;
                f2 = f1;
                x1 = hi - g * (hi - lo);
                f1 = probeAt(o, @floatCast(@exp(x1)), lv);
                if (f1.ratio() > best.ratio()) {
                    best = f1;
                    best_t = @exp(x1);
                }
            }
        }
        const t: f32 = @floatCast(best_t);
        const top: f32 = @floatFromInt(lv - 1);
        for (o, mags) |v, *m| m.* = if (best_t == 0) 0 else @intFromFloat(@min(@floor(t * @abs(v)), top));
        return best.best();
    }

    const golden_iterations = 24;

    const Probe = struct {
        ip: f64,
        ny2: f64,
        fn ratio(self: Probe) f64 {
            return self.ip / @sqrt(self.ny2);
        }
        fn best(self: Probe) Best {
            return .{ .cos = @floatCast(self.ratio()), .y_norm = @floatCast(@sqrt(self.ny2)) };
        }
    };

    /// <y, |o|> and |y|^2 for the levels at scale t.
    fn probeAt(o: []const f32, t: f32, lv: u32) Probe {
        const lanes = 8;
        const V = @Vector(lanes, f32);
        const top: V = @splat(@floatFromInt(lv - 1));
        const half: V = @splat(0.5);
        const tv: V = @splat(t);
        var ip: V = @splat(0);
        var ny: V = @splat(0);
        var i: usize = 0;
        while (i + lanes <= o.len) : (i += lanes) {
            const a = @abs(@as(V, o[i..][0..lanes].*));
            const y = @min(@floor(tv * a), top) + half;
            ip += a * y;
            ny += y * y;
        }
        var ip_s: f64 = @reduce(.Add, ip);
        var ny_s: f64 = @reduce(.Add, ny);
        const top_s: f32 = @floatFromInt(lv - 1);
        while (i < o.len) : (i += 1) {
            const a = @abs(o[i]);
            const y = @min(@floor(t * a), top_s) + 0.5;
            ip_s += a * y;
            ny_s += y * y;
        }
        return .{ .ip = ip_s, .ny2 = ny_s };
    }

    /// Exact optimum by sweeping every critical scale in order
    /// (O(D L log(D L))). Kept as the reference for tests.
    fn bestLevelsExact(self: Codec, alloc: Allocator, o: []const f32, mags: []u8) !Best {
        const d = self.dims;
        const lv = self.levels();
        var ip: f64 = 0;
        for (o) |v| ip += 0.5 * @abs(v);
        var ny2: f64 = 0.25 * @as(f64, @floatFromInt(d));
        var best_ratio = ip / @sqrt(ny2);
        var best_ip = ip;
        var best_ny2 = ny2;
        var best_events: usize = 0;
        if (lv > 1) {
            const Event = struct { t: f32, dim: u32 };
            const events = try alloc.alloc(Event, d * (lv - 1));
            defer alloc.free(events);
            var n: usize = 0;
            for (o, 0..) |v, i| {
                const mag = @abs(v);
                if (mag == 0) continue;
                var k: u32 = 1;
                while (k < lv) : (k += 1) {
                    events[n] = .{ .t = @as(f32, @floatFromInt(k)) / mag, .dim = @intCast(i) };
                    n += 1;
                }
            }
            const ByT = struct {
                fn lessThan(_: void, x: Event, y: Event) bool {
                    return x.t < y.t;
                }
            };
            std.mem.sortUnstable(Event, events[0..n], {}, ByT.lessThan);
            for (events[0..n], 1..) |e, count| {
                const m: f64 = @floatFromInt(mags[e.dim]);
                ip += @abs(o[e.dim]);
                ny2 += 2 * m + 2; // (m + 1.5)^2 - (m + 0.5)^2
                mags[e.dim] += 1;
                const ratio = ip / @sqrt(ny2);
                if (ratio > best_ratio) {
                    best_ratio = ratio;
                    best_ip = ip;
                    best_ny2 = ny2;
                    best_events = count;
                }
            }
            // Replay up to the best scale.
            @memset(mags, 0);
            for (events[0..best_events]) |e| mags[e.dim] += 1;
        }
        return .{ .cos = @floatCast(best_ip / @sqrt(best_ny2)), .y_norm = @floatCast(@sqrt(best_ny2)) };
    }

    /// Writes the code's reconstruction c + |r| ŷ (rotated space) to `out`.
    pub fn decode(self: Codec, centroid: []const f32, row: []const u8, out: []f32) void {
        const code = row[0..self.codeBytes()];
        const pos = self.codeBytes();
        const r_norm = readF32(row[pos..]);
        const inv = readF32(row[pos + 4 ..]);
        const a = readF32(row[pos + 8 ..]);
        // 1 / |y| = inv * a.
        const scale = r_norm * inv * a;
        const half: f32 = @as(f32, @floatFromInt(self.levels())) - 0.5;
        for (out, 0..) |*o, i| {
            const u: f32 = @floatFromInt(getBits(code, i * self.bits, self.bits));
            o.* = centroid[i] + scale * (u - half);
        }
    }

    pub const Query = struct {
        /// (q - c) / |q - c|, rotated space.
        unit: []f32,
        sum_unit: f32,
        q_dist: f32,
        q_dot_c: f32,
        c_norm_sq: f32,
        /// For 2, 4 and 8 bits: the query split by position within a code
        /// byte, `phase[t * code_bytes + j] = unit[j * (8 / bits) + t]`
        /// (zero past the last dimension), so scoring is a byte-parallel
        /// SIMD dot product. Empty for other widths.
        phase: []f32 = &.{},

        pub fn deinit(self: *Query, alloc: Allocator) void {
            alloc.free(self.unit);
            if (self.phase.len > 0) alloc.free(self.phase);
            self.* = undefined;
        }
    };

    /// Prepares a rotated query against `centroid`.
    pub fn prepare(self: Codec, alloc: Allocator, centroid: []const f32, q: []const f32) !Query {
        const unit = try alloc.alloc(f32, self.dims);
        vec.subTo(unit, q, centroid);
        const qd = vec.norm(unit);
        if (qd > 0) {
            for (unit) |*v| v.* /= qd;
        }
        errdefer alloc.free(unit);
        var sum: f32 = 0;
        for (unit) |v| sum += v;
        var phase: []f32 = &.{};
        if (self.bits == 2 or self.bits == 4 or self.bits == 8) {
            const per_byte: usize = 8 / self.bits;
            const nb = self.codeBytes();
            phase = try alloc.alloc(f32, per_byte * nb);
            @memset(phase, 0);
            for (unit, 0..) |v, i| phase[(i % per_byte) * nb + i / per_byte] = v;
        }
        return .{
            .phase = phase,
            .unit = unit,
            .sum_unit = sum,
            .q_dist = qd,
            .q_dot_c = if (self.metric != .l2_squared) vec.dot(q, centroid) else 0,
            .c_norm_sq = if (self.metric != .l2_squared) vec.dot(centroid, centroid) else 0,
        };
    }

    /// Estimated distance (Antfly's metric conventions: l2 squared,
    /// negative inner product, 1 - cosine) and its error bound.
    pub fn estimate(self: Codec, q: *const Query, row: []const u8) Estimate {
        const pos = self.codeBytes();
        const x_dot_c: f32 = if (self.metric != .l2_squared) readF32(row[pos + 12 ..]) else 0;
        return self.estimateParts(q, row[0..pos], readF32(row[pos..]), .{ .inv = readF32(row[pos + 4 ..]), .cos = readF32(row[pos + 8 ..]) }, x_dot_c);
    }

    /// `estimate` over column storage: the code, |x - c|, the factors, and
    /// <x, c> (ignored for l2).
    pub fn estimateParts(self: Codec, q: *const Query, code: []const u8, r_norm: f32, f: Factors, x_dot_c: f32) Estimate {
        const inv = f.inv;
        const a = f.cos;
        const half: f32 = @as(f32, @floatFromInt(self.levels())) - 0.5;
        const acc: f32 = if (q.phase.len > 0) phasedDot(code, q.phase, self.bits) else blk: {
            var sum: f32 = 0;
            for (q.unit, 0..) |v, i| sum += @as(f32, @floatFromInt(getBits(code, i * self.bits, self.bits))) * v;
            break :blk sum;
        };
        // <y, unit> = sum(u_i * unit_i) - half * sum(unit); <o, unit> ≈ that * inv.
        const est_cos = (acc - half * q.sum_unit) * inv;
        const sigma = if (a > 0 and a < 1) @sqrt((1 - a * a) / (a * a)) / @sqrt(@as(f32, @floatFromInt(self.dims - 1))) else 0;
        const multiplier = r_norm * q.q_dist;
        switch (self.metric) {
            .l2_squared => {
                var dist = r_norm * r_norm + q.q_dist * q.q_dist - 2 * multiplier * est_cos;
                var bound = 2 * multiplier * sigma * bound_sigmas;
                if (dist < 0) {
                    bound = @max(bound + dist, 0);
                    dist = 0;
                }
                return .{ .distance = dist, .bound = bound };
            },
            .inner_product => {
                const ip = multiplier * est_cos + x_dot_c + q.q_dot_c - q.c_norm_sq;
                return .{ .distance = -ip, .bound = multiplier * sigma * bound_sigmas };
            },
            .cosine => {
                const ip = multiplier * est_cos + x_dot_c + q.q_dot_c - q.c_norm_sq;
                var dist = 1 - ip;
                var bound = multiplier * sigma * bound_sigmas;
                if (dist < 0) {
                    bound = @max(bound + dist, 0);
                    dist = 0;
                } else if (dist > 2) {
                    bound = @max(@min(bound - (dist - 2), 2), 0);
                    dist = 2;
                }
                return .{ .distance = dist, .bound = bound };
            },
        }
    }
};

/// sum over dimensions of code level * query, for byte-aligned widths
/// (2, 4, 8 bits), 16 code bytes per step.
fn phasedDot(code: []const u8, phase: []const f32, bits: u8) f32 {
    const lanes = 16;
    const V8 = @Vector(lanes, u8);
    const VF = @Vector(lanes, f32);
    const nb = code.len;
    const per_byte: usize = 8 / @as(usize, bits);
    const mask: u8 = @intCast((@as(u16, 1) << @intCast(bits)) - 1);
    var acc: VF = @splat(0);
    var j: usize = 0;
    while (j + lanes <= nb) : (j += lanes) {
        const bytes: V8 = code[j..][0..lanes].*;
        var t: usize = 0;
        while (t < per_byte) : (t += 1) {
            const shift: @Vector(lanes, u3) = @splat(@intCast(t * bits));
            const levels = (bytes >> shift) & @as(V8, @splat(mask));
            const qv: VF = phase[t * nb + j ..][0..lanes].*;
            acc += @as(VF, @floatFromInt(levels)) * qv;
        }
    }
    var total = @reduce(.Add, acc);
    while (j < nb) : (j += 1) {
        var t: usize = 0;
        while (t < per_byte) : (t += 1) {
            const level: u8 = (code[j] >> @intCast(t * bits)) & mask;
            total += @as(f32, @floatFromInt(level)) * phase[t * nb + j];
        }
    }
    return total;
}

fn putBits(buf: []u8, bit_offset: usize, width: u8, value: u32) void {
    var v = value;
    var off = bit_offset;
    var left: u32 = width;
    while (left > 0) {
        const byte = off / 8;
        const shift: u3 = @intCast(off % 8);
        const take: u32 = @min(left, 8 - @as(u32, shift));
        const mask: u32 = (@as(u32, 1) << @intCast(take)) - 1;
        buf[byte] |= @intCast((v & mask) << shift);
        v >>= @intCast(take);
        off += take;
        left -= take;
    }
}

fn getBits(buf: []const u8, bit_offset: usize, width: u8) u32 {
    var out: u32 = 0;
    var off = bit_offset;
    var got: u32 = 0;
    while (got < width) {
        const byte = off / 8;
        const shift: u3 = @intCast(off % 8);
        const take: u32 = @min(width - got, 8 - @as(u32, shift));
        const mask: u32 = (@as(u32, 1) << @intCast(take)) - 1;
        out |= ((@as(u32, buf[byte]) >> shift) & mask) << @intCast(got);
        off += take;
        got += take;
    }
    return out;
}

fn writeF32(buf: []u8, v: f32) void {
    std.mem.writeInt(u32, buf[0..4], @bitCast(v), .little);
}

fn readF32(buf: []const u8) f32 {
    return @bitCast(std.mem.readInt(u32, buf[0..4], .little));
}

const testing = std.testing;

fn randomUnitish(rand: std.Random, out: []f32) void {
    for (out) |*v| v.* = rand.floatNorm(f32);
}

test "rabitq_ex: bits pack and unpack" {
    var buf: [16]u8 = @splat(0);
    const widths = [_]u8{ 1, 3, 5, 8, 2, 7 };
    var off: usize = 0;
    for (widths, 0..) |w, i| {
        putBits(&buf, off, w, @intCast((i * 37 + 5) & ((@as(usize, 1) << @intCast(w)) - 1)));
        off += w;
    }
    off = 0;
    for (widths, 0..) |w, i| {
        try testing.expectEqual(@as(u32, @intCast((i * 37 + 5) & ((@as(usize, 1) << @intCast(w)) - 1))), getBits(&buf, off, w));
        off += w;
    }
}

test "rabitq_ex: more bits estimate distances more accurately, bounds mostly hold" {
    const alloc = testing.allocator;
    const d = 128;
    var prng = std.Random.DefaultPrng.init(3);
    const rand = prng.random();
    const centroid = try alloc.alloc(f32, d);
    defer alloc.free(centroid);
    for (centroid) |*c| c.* = rand.floatNorm(f32) * 0.1;
    const n = 300;
    const xs = try alloc.alloc(f32, n * d);
    defer alloc.free(xs);
    randomUnitish(rand, xs);
    const q = try alloc.alloc(f32, d);
    defer alloc.free(q);
    randomUnitish(rand, q);

    var prev_err: f64 = std.math.inf(f64);
    for ([_]u8{ 1, 2, 3, 4, 8 }) |bits| {
        const codec = try Codec.init(d, bits, .l2_squared);
        const row = try alloc.alloc(u8, codec.rowBytes());
        defer alloc.free(row);
        var query = try codec.prepare(alloc, centroid, q);
        defer query.deinit(alloc);
        var err_sum: f64 = 0;
        var within: usize = 0;
        const decoded = try alloc.alloc(f32, d);
        defer alloc.free(decoded);
        var recon_err: f64 = 0;
        for (0..n) |i| {
            const x = xs[i * d ..][0..d];
            try codec.encodeAlloc(alloc, centroid, x, row);
            const est = codec.estimate(&query, row);
            var exact: f32 = 0;
            for (x, q) |a, b| exact += (a - b) * (a - b);
            err_sum += @abs(est.distance - exact) / exact;
            if (@abs(est.distance - exact) <= est.bound) within += 1;
            codec.decode(centroid, row, decoded);
            var e: f32 = 0;
            var nx: f32 = 0;
            for (x, decoded, centroid) |a, b, c| {
                e += (a - b) * (a - b);
                nx += (a - c) * (a - c);
            }
            recon_err += e / nx;
        }
        const mean_err = err_sum / n;
        // Relative error falls as bits grow, and the bound covers most rows.
        try testing.expect(mean_err < prev_err);
        prev_err = mean_err;
        try testing.expect(within * 100 >= n * 70);
        if (bits >= 4) try testing.expect(recon_err / n < 0.05);
    }
}

test "rabitq_ex: SIMD scoring matches the scalar path" {
    const alloc = testing.allocator;
    var prng = std.Random.DefaultPrng.init(21);
    const rand = prng.random();
    for ([_]usize{ 64, 100, 257 }) |d| {
        const centroid = try alloc.alloc(f32, d);
        defer alloc.free(centroid);
        @memset(centroid, 0);
        const x = try alloc.alloc(f32, d);
        defer alloc.free(x);
        const q = try alloc.alloc(f32, d);
        defer alloc.free(q);
        randomUnitish(rand, x);
        randomUnitish(rand, q);
        for ([_]u8{ 2, 4, 8 }) |bits| {
            const codec = try Codec.init(d, bits, .l2_squared);
            const row = try alloc.alloc(u8, codec.rowBytes());
            defer alloc.free(row);
            try codec.encodeAlloc(alloc, centroid, x, row);
            var query = try codec.prepare(alloc, centroid, q);
            defer query.deinit(alloc);
            const fast = codec.estimate(&query, row);
            const saved = query.phase;
            query.phase = &.{};
            const slow = codec.estimate(&query, row);
            query.phase = saved;
            try testing.expectApproxEqRel(slow.distance, fast.distance, 1e-4);
        }
    }
}

test "rabitq_ex: inner product and cosine conventions" {
    const alloc = testing.allocator;
    const d = 64;
    var prng = std.Random.DefaultPrng.init(9);
    const rand = prng.random();
    const centroid = try alloc.alloc(f32, d);
    defer alloc.free(centroid);
    @memset(centroid, 0.01);
    const x = try alloc.alloc(f32, d);
    defer alloc.free(x);
    const q = try alloc.alloc(f32, d);
    defer alloc.free(q);
    randomUnitish(rand, x);
    randomUnitish(rand, q);
    const xn = vec.norm(x);
    const qn = vec.norm(q);
    for (x) |*v| v.* /= xn;
    for (q) |*v| v.* /= qn;
    const ip = vec.dot(x, q);
    for ([_]Metric{ .inner_product, .cosine }) |metric| {
        const codec = try Codec.init(d, 8, metric);
        const row = try alloc.alloc(u8, codec.rowBytes());
        defer alloc.free(row);
        try codec.encodeAlloc(alloc, centroid, x, row);
        var query = try codec.prepare(alloc, centroid, q);
        defer query.deinit(alloc);
        const est = codec.estimate(&query, row);
        const want: f32 = if (metric == .inner_product) -ip else 1 - ip;
        try testing.expectApproxEqAbs(want, est.distance, 0.02);
    }
    try testing.expectError(error.InvalidConfig, Codec.init(d, 9, .l2_squared));
}

test "rabitq_ex: golden-section levels match the exact sweep" {
    const alloc = testing.allocator;
    var prng = std.Random.DefaultPrng.init(31);
    const rand = prng.random();
    for ([_]usize{ 64, 257, 768 }) |d| {
        const o = try alloc.alloc(f32, d);
        defer alloc.free(o);
        const m1 = try alloc.alloc(u8, d);
        defer alloc.free(m1);
        const m2 = try alloc.alloc(u8, d);
        defer alloc.free(m2);
        for ([_]u8{ 2, 4, 8 }) |bits| {
            const codec = try Codec.init(d, bits, .l2_squared);
            var worst: f64 = 0;
            for (0..40) |_| {
                randomUnitish(rand, o);
                const n = vec.norm(o);
                for (o) |*v| v.* /= n;
                @memset(m1, 0);
                @memset(m2, 0);
                const fast = try codec.bestLevels(alloc, o, m1);
                const exact = try codec.bestLevelsExact(alloc, o, m2);
                try testing.expect(fast.cos <= exact.cos + 1e-5);
                worst = @max(worst, exact.cos - fast.cos);
            }
            try testing.expect(worst < 1e-3);
        }
    }
}
