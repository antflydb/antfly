// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const bloom = @import("bloom");
const baseline = @import("baseline.zig");
const Timespec = extern struct { sec: i64, ns: i64 };
extern "c" fn clock_gettime(c_int, *Timespec) c_int;
fn now() u64 {
    var t: Timespec = undefined;
    _ = clock_gettime(if (@import("builtin").os.tag == .macos) 6 else 1, &t);
    return @as(u64, @intCast(t.sec)) * 1_000_000_000 + @as(u64, @intCast(t.ns));
}
fn mix(x: u64) u64 {
    var h = x;
    h = (h ^ (h >> 30)) *% 0xbf58476d1ce4e5b9;
    h = (h ^ (h >> 27)) *% 0x94d049bb133111eb;
    return h ^ (h >> 31);
}
const Pair = struct { a: u64, b: u64 };
fn pair(i: u64) Pair {
    return .{ .a = mix(i +% 0x9e3779b97f4a7c15), .b = mix(i +% 0x123456789abcdef) };
}
const primes = [_]u64{ 67, 71, 73, 79, 83, 89, 50 };
const salts = [_]u32{ 0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31 };
const Kind = enum { baseline, optimized, masked, ohbf, ohbf_full, split, split64 };
const Filter = struct {
    kind: Kind,
    bytes: []align(64) u8,
    bits: usize,
    k: u8,
    fn init(kind: Kind, n: usize, bpk: usize, percent: usize) !Filter {
        const original_bits = try std.math.ceilPowerOfTwo(usize, n * bpk);
        const bits = if (kind == .baseline or kind == .optimized or kind == .masked) original_bits else std.mem.alignForward(usize, original_bits * percent / 100, 512);
        const bytes = try std.heap.page_allocator.alignedAlloc(u8, .@"64", bits / 8);
        @memset(bytes, 0);
        return .{ .kind = kind, .bytes = bytes, .bits = bits, .k = bloom.optimalHashCount(bpk, 16) };
    }
    fn access(self: *Filter, p: Pair, comptime add: bool) bool {
        switch (self.kind) {
            .baseline => {
                if (!add) return (baseline.BorrowedFilter{ .bytes = self.bytes, .bit_count = @intCast(self.bits), .hash_count = self.k }).maybeContainsHashes(p.a, p.b);
                var builder = baseline.Builder{ .allocator = std.heap.page_allocator, .bytes = self.bytes, .bit_count = @intCast(self.bits), .hash_count = self.k };
                builder.addHashes(p.a, p.b);
            },
            .optimized => {
                if (!add) return (bloom.BorrowedFilter{ .bytes = self.bytes, .bit_count = @intCast(self.bits), .hash_count = self.k }).maybeContainsHashes(p.a, p.b);
                var builder = bloom.Builder{ .allocator = std.heap.page_allocator, .bytes = self.bytes, .bit_count = @intCast(self.bits), .hash_count = self.k };
                builder.addHashes(p.a, p.b);
            },
            .masked => {
                const h2 = if (p.b == 0) 0x9e3779b97f4a7c15 else p.b;
                var h = p.a;
                var i: u8 = 0;
                while (i < self.k) : (i += 1) {
                    const bit = h & (self.bits - 1);
                    const mask = @as(u8, 1) << @intCast(bit % 8);
                    if (add) {
                        self.bytes[bit / 8] |= mask;
                    } else if (self.bytes[bit / 8] & mask == 0) return false;
                    h +%= h2;
                }
            },
            .ohbf, .ohbf_full => {
                const block: usize = @intCast((@as(u128, p.a) * (self.bits / 512)) >> 64);
                var offset: usize = block * 512;
                var missing: u8 = 0;
                inline for (primes) |prime| {
                    const bit = offset + p.b % prime;
                    const mask = @as(u8, 1) << @intCast(bit % 8);
                    if (add) {
                        self.bytes[bit / 8] |= mask;
                    } else if (self.kind == .ohbf_full) {
                        missing |= (self.bytes[bit / 8] & mask) ^ mask;
                    } else if (self.bytes[bit / 8] & mask == 0) return false;
                    offset += prime;
                }
                if (!add and self.kind == .ohbf_full) return missing == 0;
            },
            // Eight 64-bit lanes reduce load variance at the same memory
            // budget, trading wider shifts for fewer false positives.
            .split64 => {
                const block: usize = @intCast((@as(u128, p.a) * (self.bits / 512)) >> 64);
                const words: *[8]u64 = @ptrCast(@alignCast(self.bytes.ptr + block * 64));
                const h: u32 = @truncate(p.b);
                var missing: u64 = 0;
                inline for (salts, 0..) |salt, i| {
                    const mask = @as(u64, 1) << @intCast((h *% salt) >> 26);
                    if (add) {
                        words[i] |= mask;
                    } else {
                        missing |= (words[i] & mask) ^ mask;
                    }
                }
                if (!add) return missing == 0;
            },
            .split => {
                const block: usize = @intCast((@as(u128, p.a) * (self.bits / 256)) >> 64);
                const words: *[8]u32 = @ptrCast(@alignCast(self.bytes.ptr + block * 32));
                const h: u32 = @truncate(p.b);
                var missing: u32 = 0;
                inline for (salts, 0..) |salt, i| {
                    const mask = @as(u32, 1) << @intCast((h *% salt) >> 27);
                    if (add) {
                        words[i] |= mask;
                    } else {
                        missing |= (words[i] & mask) ^ mask;
                    }
                }
                if (!add) return missing == 0;
            },
        }
        return true;
    }
};

const Query = struct { hashes: Pair, key: [64]u8 };
fn makeQuery(i: u64) Query {
    var q: Query = undefined;
    for (0..8) |j| std.mem.writeInt(u64, q.key[j * 8 ..][0..8], mix(i +% (j *% 0x9e3779b97f4a7c15)), .little);
    const h1 = std.hash.Wyhash.hash(0, &q.key);
    q.hashes = .{ .a = h1, .b = mix(h1) };
    return q;
}
fn queryPair(q: *const Query, comptime hash_keys: bool) Pair {
    if (!hash_keys) return q.hashes;
    const h1 = std.hash.Wyhash.hash(0, &q.key);
    return .{ .a = h1, .b = mix(h1) };
}
pub fn main() !void {
    const qn = 1_000_000;
    const queries = try std.heap.page_allocator.alloc(Query, qn);
    defer std.heap.page_allocator.free(queries);
    std.debug.print("mode,kind,n,requested_bpk,budget_pct,bytes,build_with_keygen_ns_key,miss_ns,hit_ns,fpr,miss_min_ns,miss_max_ns,hit_min_ns,hit_max_ns\n", .{});
    inline for (.{ false, true }) |hash_keys| {
        for ([_]usize{ 10_000, 1_000_000, 8_000_000 }) |n| {
            for ([_]Kind{ .baseline, .optimized, .ohbf, .ohbf_full, .split, .split64 }) |kind| {
                for ([_]usize{ 100, 110, 125, 150, 200 }) |percent| {
                    if ((kind == .baseline or kind == .optimized) and percent != 100) continue;
                    const bpk = 14;
                    var f = try Filter.init(kind, n, bpk, percent);
                    defer std.heap.page_allocator.free(f.bytes);
                    const start = now();
                    for (0..n) |i| {
                        const q = makeQuery(i);
                        _ = f.access(queryPair(&q, hash_keys), true);
                    }
                    const build = now() - start;
                    // Check every inserted key independently of timed hit sampling.
                    for (0..n) |i| {
                        const q = makeQuery(i);
                        if (!f.access(queryPair(&q, hash_keys), false)) return error.FalseNegative;
                    }
                    var times: [2][7]u64 = undefined;
                    var fp: usize = 0;
                    for (0..2) |hit| {
                        for (queries, 0..) |*q, i| q.* = makeQuery(if (hit == 1) mix(i) % n else n + i);
                        // One warmup, then median and range of seven measurements.
                        var warm: usize = 0;
                        for (queries) |*q| warm += @intFromBool(f.access(queryPair(q, hash_keys), false));
                        std.mem.doNotOptimizeAway(warm);
                        for (0..7) |rep| {
                            var count: usize = 0;
                            const t = now();
                            for (queries) |*q| count += @intFromBool(f.access(queryPair(q, hash_keys), false));
                            times[hit][rep] = now() - t;
                            std.mem.doNotOptimizeAway(count);
                            if (hit == 0) fp = count else if (count != qn) return error.FalseNegative;
                        }
                        std.mem.sort(u64, &times[hit], {}, std.sort.asc(u64));
                    }
                    std.debug.print("{s},{s},{d},{d},{d},{d},{d:.3},{d:.3},{d:.3},{d:.6},{d:.3},{d:.3},{d:.3},{d:.3}\n", .{ if (hash_keys) "term64" else "prehashed", @tagName(kind), n, bpk, percent, f.bytes.len, @as(f64, @floatFromInt(build)) / @as(f64, @floatFromInt(n)), @as(f64, @floatFromInt(times[0][3])) / qn, @as(f64, @floatFromInt(times[1][3])) / qn, @as(f64, @floatFromInt(fp)) / qn, @as(f64, @floatFromInt(times[0][0])) / qn, @as(f64, @floatFromInt(times[0][6])) / qn, @as(f64, @floatFromInt(times[1][0])) / qn, @as(f64, @floatFromInt(times[1][6])) / qn });
                }
            }
        }
    }
}

test "blocked layouts retain inserted keys for multiple independent datasets" {
    for ([_]Kind{ .ohbf, .ohbf_full, .split, .split64 }) |kind| {
        for ([_]usize{ 1, 64, 1000, 10000 }) |n| {
            for (0..3) |seed| {
                var f = try Filter.init(kind, n, 14, 125);
                defer std.heap.page_allocator.free(f.bytes);
                for (0..n) |i| {
                    const q = makeQuery(i + seed * 1000000);
                    _ = f.access(q.hashes, true);
                }
                for (0..n) |i| {
                    const q = makeQuery(i + seed * 1000000);
                    try std.testing.expect(f.access(q.hashes, false));
                }
                var fp: usize = 0;
                for (0..100000) |i| {
                    const q = makeQuery(n + i + seed * 1000000);
                    fp += @intFromBool(f.access(q.hashes, false));
                }
                try std.testing.expect(fp < 1000);
            }
        }
    }
}
