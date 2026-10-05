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

//! Streaming aggregation collectors.
//!
//! Aggregations are computed in a single pass during query execution,
//! not as post-processing. Each aggregation maintains streaming state
//! updated via collect().
//!
//! Supported aggregations:
//!   - StatsAgg: min/max/sum/count/avg for numeric columns
//!   - HistogramAgg: fixed-width buckets for numeric columns
//!   - TermsFacet: top-k term counts from doc values
//!   - DateHistogramAgg: calendar-aligned buckets for nanosecond timestamps
//!   - RangeAgg: user-defined numeric ranges
//!   - GeoDistanceAgg: distance bands from a center point
//!   - GeohashGridAgg: geohash-based spatial bucketing

const std = @import("std");
const Allocator = std.mem.Allocator;
const typed_dv = @import("../section/typed_doc_values.zig");
const geo = @import("geo.zig");
const epoch = std.time.epoch;

// ============================================================================
// Stats aggregation
// ============================================================================

/// Computes min, max, sum, count, avg over a numeric column.
pub const StatsAgg = struct {
    min: f64,
    max: f64,
    sum: f64,
    count: u64,

    pub fn init() StatsAgg {
        return .{
            .min = std.math.inf(f64),
            .max = -std.math.inf(f64),
            .sum = 0,
            .count = 0,
        };
    }

    /// Collect a single value.
    pub fn collect(self: *StatsAgg, value: f64) void {
        if (value < self.min) self.min = value;
        if (value > self.max) self.max = value;
        self.sum += value;
        self.count += 1;
    }

    /// Collect a bulk chunk of f64 values with SIMD acceleration.
    pub fn collectChunk(self: *StatsAgg, values: []const f64) void {
        const V = @Vector(4, f64);
        const len = values.len;
        const simd_len = len - (len % 4);

        if (simd_len >= 4) {
            var v_min: V = @splat(std.math.inf(f64));
            var v_max: V = @splat(-std.math.inf(f64));
            var v_sum: V = @splat(@as(f64, 0));

            var i: usize = 0;
            while (i < simd_len) : (i += 4) {
                const v: V = values[i..][0..4].*;
                v_min = @min(v_min, v);
                v_max = @max(v_max, v);
                v_sum += v;
            }

            // Horizontal reduce
            self.min = @min(self.min, @reduce(.Min, v_min));
            self.max = @max(self.max, @reduce(.Max, v_max));
            self.sum += @reduce(.Add, v_sum);
            self.count += simd_len;
        }

        // Handle remainder
        for (values[simd_len..]) |v| self.collect(v);
    }

    /// Collect a bulk chunk of u64 values (cast to f64).
    pub fn collectU64Chunk(self: *StatsAgg, values: []const u64) void {
        for (values) |v| self.collect(@floatFromInt(v));
    }

    pub fn avg(self: *const StatsAgg) f64 {
        if (self.count == 0) return 0;
        return self.sum / @as(f64, @floatFromInt(self.count));
    }
};

// ============================================================================
// Histogram aggregation
// ============================================================================

/// Fixed-width bucket histogram over a numeric column.
pub const HistogramAgg = struct {
    alloc: Allocator,
    interval: f64,
    offset: f64,
    buckets: std.ArrayHashMapUnmanaged(i64, u64, HashI64, true),

    const HashI64 = struct {
        pub fn hash(_: @This(), key: i64) u32 {
            const k: u64 = @bitCast(key);
            return @truncate(k ^ (k >> 32));
        }
        pub fn eql(_: @This(), a: i64, b: i64, _: usize) bool {
            return a == b;
        }
    };

    pub fn init(alloc: Allocator, interval: f64, offset: f64) HistogramAgg {
        return .{
            .alloc = alloc,
            .interval = interval,
            .offset = offset,
            .buckets = .empty,
        };
    }

    pub fn deinit(self: *HistogramAgg) void {
        self.buckets.deinit(self.alloc);
    }

    pub fn collect(self: *HistogramAgg, value: f64) !void {
        const bucket_key = self.bucketKey(value) orelse return;
        const gop = try self.buckets.getOrPut(self.alloc, bucket_key);
        if (!gop.found_existing) gop.value_ptr.* = 0;
        gop.value_ptr.* += 1;
    }

    fn bucketKey(self: *const HistogramAgg, value: f64) ?i64 {
        if (std.math.isNan(value) or std.math.isInf(value) or self.interval == 0) return null;
        return @intFromFloat(@floor((value - self.offset) / self.interval));
    }

    /// Get the lower bound of a bucket.
    pub fn bucketLowerBound(self: *const HistogramAgg, key: i64) f64 {
        return @as(f64, @floatFromInt(key)) * self.interval + self.offset;
    }

    /// Get count for a specific bucket key.
    pub fn getCount(self: *const HistogramAgg, key: i64) u64 {
        return self.buckets.get(key) orelse 0;
    }

    /// Return all bucket keys sorted.
    pub fn sortedKeys(self: *const HistogramAgg, alloc: Allocator) ![]i64 {
        const keys = try alloc.dupe(i64, self.buckets.keys());
        std.mem.sort(i64, keys, {}, struct {
            fn cmp(_: void, a: i64, b: i64) bool {
                return a < b;
            }
        }.cmp);
        return keys;
    }
};

// ============================================================================
// Terms facet aggregation
// ============================================================================

/// Counts occurrences of each distinct byte-string value.
pub const TermsFacet = struct {
    alloc: Allocator,
    counts: std.StringHashMapUnmanaged(u64),

    pub fn init(alloc: Allocator) TermsFacet {
        return .{
            .alloc = alloc,
            .counts = .empty,
        };
    }

    pub fn deinit(self: *TermsFacet) void {
        var it = self.counts.keyIterator();
        while (it.next()) |k| self.alloc.free(k.*);
        self.counts.deinit(self.alloc);
    }

    pub fn collect(self: *TermsFacet, value: []const u8) !void {
        const gop = try self.counts.getOrPut(self.alloc, value);
        if (!gop.found_existing) {
            gop.key_ptr.* = try self.alloc.dupe(u8, value);
            gop.value_ptr.* = 0;
        }
        gop.value_ptr.* += 1;
    }

    /// Get count for a specific term.
    pub fn getCount(self: *const TermsFacet, term: []const u8) u64 {
        return self.counts.get(term) orelse 0;
    }

    pub const FacetEntry = struct { term: []const u8, count: u64 };

    /// Return top-k terms by count. Caller owns the returned slice (not the strings).
    pub fn topK(self: *const TermsFacet, alloc: Allocator, k: u32) ![]FacetEntry {
        var entries = std.ArrayListUnmanaged(FacetEntry).empty;
        defer entries.deinit(alloc);

        var it = self.counts.iterator();
        while (it.next()) |e| {
            try entries.append(alloc, .{ .term = e.key_ptr.*, .count = e.value_ptr.* });
        }

        std.mem.sort(FacetEntry, entries.items, {}, struct {
            fn cmp(_: void, a: FacetEntry, b: FacetEntry) bool {
                return a.count > b.count;
            }
        }.cmp);

        const n = @min(k, @as(u32, @intCast(entries.items.len)));
        return try alloc.dupe(FacetEntry, entries.items[0..n]);
    }
};

// ============================================================================
// Date histogram aggregation
// ============================================================================

pub const DateInterval = enum {
    minute,
    hour,
    day,
    week,
    month,
    year,
};

/// Calendar-aligned histogram over u64 nanosecond timestamps.
pub const DateHistogramAgg = struct {
    alloc: Allocator,
    interval: DateInterval,
    buckets: std.ArrayHashMapUnmanaged(u64, u64, HashU64, true),

    const HashU64 = struct {
        pub fn hash(_: @This(), key: u64) u32 {
            return @truncate(key ^ (key >> 32));
        }
        pub fn eql(_: @This(), a: u64, b: u64, _: usize) bool {
            return a == b;
        }
    };

    pub fn init(alloc: Allocator, interval: DateInterval) DateHistogramAgg {
        return .{
            .alloc = alloc,
            .interval = interval,
            .buckets = .empty,
        };
    }

    pub fn deinit(self: *DateHistogramAgg) void {
        self.buckets.deinit(self.alloc);
    }

    pub fn collect(self: *DateHistogramAgg, ns_timestamp: u64) !void {
        const bucket_key = truncateToInterval(ns_timestamp, self.interval);
        const gop = try self.buckets.getOrPut(self.alloc, bucket_key);
        if (!gop.found_existing) gop.value_ptr.* = 0;
        gop.value_ptr.* += 1;
    }

    pub fn getCount(self: *const DateHistogramAgg, key: u64) u64 {
        return self.buckets.get(key) orelse 0;
    }

    pub fn sortedKeys(self: *const DateHistogramAgg, alloc: Allocator) ![]u64 {
        const keys = try alloc.dupe(u64, self.buckets.keys());
        std.mem.sort(u64, keys, {}, struct {
            fn cmp(_: void, a: u64, b: u64) bool {
                return a < b;
            }
        }.cmp);
        return keys;
    }
};

const ns_per_sec: u64 = 1_000_000_000;

/// Truncate a nanosecond timestamp to the start of the given calendar interval.
pub fn truncateToInterval(ns: u64, interval: DateInterval) u64 {
    const secs = ns / ns_per_sec;
    switch (interval) {
        .minute => return (secs / 60) * 60 * ns_per_sec,
        .hour => return (secs / 3600) * 3600 * ns_per_sec,
        .day => return (secs / 86400) * 86400 * ns_per_sec,
        .week => {
            // Epoch (Jan 1 1970) was a Thursday. Align to Monday.
            // (days + 3) % 7 gives Monday=0.
            const days = secs / 86400;
            const dow = (days + 3) % 7;
            const monday = days - dow;
            return monday * 86400 * ns_per_sec;
        },
        .month => {
            const epoch_secs = epoch.EpochSeconds{ .secs = secs };
            const epoch_day = epoch_secs.getEpochDay();
            const year_day = epoch_day.calculateYearDay();
            const month_day = year_day.calculateMonthDay();
            const start_day = epoch_day.day - month_day.day_index;
            return @as(u64, start_day) * 86400 * ns_per_sec;
        },
        .year => {
            const epoch_secs = epoch.EpochSeconds{ .secs = secs };
            const epoch_day = epoch_secs.getEpochDay();
            const year_day = epoch_day.calculateYearDay();
            const start_day = epoch_day.day - year_day.day;
            return @as(u64, start_day) * 86400 * ns_per_sec;
        },
    }
}

// ============================================================================
// Range aggregation
// ============================================================================

pub const RangeSpec = struct {
    from: ?f64 = null, // inclusive, null = unbounded
    to: ?f64 = null, // exclusive, null = unbounded
};

pub const RangeBucket = struct {
    from: ?f64 = null,
    to: ?f64 = null,
    count: u64 = 0,
};

/// User-defined numeric range aggregation. A doc can match multiple overlapping ranges.
pub const RangeAgg = struct {
    alloc: Allocator,
    buckets: []RangeBucket,

    pub fn init(alloc: Allocator, ranges: []const RangeSpec) !RangeAgg {
        const buckets = try alloc.alloc(RangeBucket, ranges.len);
        for (ranges, 0..) |r, i| {
            buckets[i] = .{ .from = r.from, .to = r.to, .count = 0 };
        }
        return .{ .alloc = alloc, .buckets = buckets };
    }

    pub fn deinit(self: *RangeAgg) void {
        self.alloc.free(self.buckets);
    }

    pub fn collect(self: *RangeAgg, value: f64) void {
        for (self.buckets) |*b| {
            const above_from = if (b.from) |f| value >= f else true;
            const below_to = if (b.to) |t| value < t else true;
            if (above_from and below_to) b.count += 1;
        }
    }
};

// ============================================================================
// Geo distance aggregation
// ============================================================================

pub const GeoDistanceRange = struct {
    from: ?f64 = null, // meters, inclusive
    to: ?f64 = null, // meters, exclusive
};

pub const GeoDistanceBand = struct {
    from_meters: ?f64 = null,
    to_meters: ?f64 = null,
    count: u64 = 0,
};

/// Distance-band aggregation from a center GeoPoint.
pub const GeoDistanceAgg = struct {
    alloc: Allocator,
    center: geo.GeoPoint,
    bands: []GeoDistanceBand,

    pub fn init(alloc: Allocator, center: geo.GeoPoint, ranges: []const GeoDistanceRange) !GeoDistanceAgg {
        const bands = try alloc.alloc(GeoDistanceBand, ranges.len);
        for (ranges, 0..) |r, i| {
            bands[i] = .{ .from_meters = r.from, .to_meters = r.to, .count = 0 };
        }
        return .{ .alloc = alloc, .center = center, .bands = bands };
    }

    pub fn deinit(self: *GeoDistanceAgg) void {
        self.alloc.free(self.bands);
    }

    pub fn collect(self: *GeoDistanceAgg, point: geo.GeoPoint) void {
        const dist = geo.haversineDistance(self.center, point);
        for (self.bands) |*b| {
            const above = if (b.from_meters) |f| dist >= f else true;
            const below = if (b.to_meters) |t| dist < t else true;
            if (above and below) b.count += 1;
        }
    }
};

// ============================================================================
// Geohash grid aggregation
// ============================================================================

/// Geohash-based spatial bucketing aggregation.
pub const GeohashGridAgg = struct {
    alloc: Allocator,
    precision: u8,
    counts: std.StringHashMapUnmanaged(u64),

    pub const GridEntry = struct { geohash: []const u8, count: u64 };

    pub fn init(alloc: Allocator, precision: u8) GeohashGridAgg {
        return .{
            .alloc = alloc,
            .precision = @min(precision, 12),
            .counts = .empty,
        };
    }

    pub fn deinit(self: *GeohashGridAgg) void {
        var it = self.counts.keyIterator();
        while (it.next()) |k| self.alloc.free(k.*);
        self.counts.deinit(self.alloc);
    }

    pub fn collect(self: *GeohashGridAgg, point: geo.GeoPoint) !void {
        const hash = geo.encode(point, self.precision);
        const key = hash[0..self.precision];
        const gop = try self.counts.getOrPut(self.alloc, key);
        if (!gop.found_existing) {
            gop.key_ptr.* = try self.alloc.dupe(u8, key);
            gop.value_ptr.* = 0;
        }
        gop.value_ptr.* += 1;
    }

    pub fn getCount(self: *const GeohashGridAgg, geohash: []const u8) u64 {
        return self.counts.get(geohash) orelse 0;
    }

    /// Return top-k cells by count. Caller owns returned slice (not strings).
    pub fn topK(self: *const GeohashGridAgg, alloc: Allocator, k: u32) ![]GridEntry {
        var entries = std.ArrayListUnmanaged(GridEntry).empty;
        defer entries.deinit(alloc);

        var it = self.counts.iterator();
        while (it.next()) |e| {
            try entries.append(alloc, .{ .geohash = e.key_ptr.*, .count = e.value_ptr.* });
        }

        std.mem.sort(GridEntry, entries.items, {}, struct {
            fn cmp(_: void, a: GridEntry, b: GridEntry) bool {
                return a.count > b.count;
            }
        }.cmp);

        const n = @min(k, @as(u32, @intCast(entries.items.len)));
        return try alloc.dupe(GridEntry, entries.items[0..n]);
    }
};

// ============================================================================
// Tests
// ============================================================================

test "stats aggregation" {
    var stats = StatsAgg.init();

    stats.collect(10.0);
    stats.collect(20.0);
    stats.collect(30.0);
    stats.collect(5.0);

    try std.testing.expectApproxEqAbs(@as(f64, 5.0), stats.min, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 30.0), stats.max, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 65.0), stats.sum, 0.001);
    try std.testing.expectEqual(@as(u64, 4), stats.count);
    try std.testing.expectApproxEqAbs(@as(f64, 16.25), stats.avg(), 0.001);
}

test "stats aggregation bulk chunk" {
    var stats = StatsAgg.init();

    const values = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    stats.collectChunk(&values);

    try std.testing.expectApproxEqAbs(@as(f64, 1.0), stats.min, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 5.0), stats.max, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 15.0), stats.sum, 0.001);
    try std.testing.expectEqual(@as(u64, 5), stats.count);
    try std.testing.expectApproxEqAbs(@as(f64, 3.0), stats.avg(), 0.001);
}

test "histogram aggregation" {
    const alloc = std.testing.allocator;

    var hist = HistogramAgg.init(alloc, 10.0, 0.0);
    defer hist.deinit();

    try hist.collect(5.0); // bucket 0
    try hist.collect(15.0); // bucket 1
    try hist.collect(25.0); // bucket 2
    try hist.collect(12.0); // bucket 1
    try hist.collect(7.0); // bucket 0

    try std.testing.expectEqual(@as(u64, 2), hist.getCount(0)); // [0, 10)
    try std.testing.expectEqual(@as(u64, 2), hist.getCount(1)); // [10, 20)
    try std.testing.expectEqual(@as(u64, 1), hist.getCount(2)); // [20, 30)
    try std.testing.expectEqual(@as(u64, 0), hist.getCount(3)); // [30, 40)

    try std.testing.expectApproxEqAbs(@as(f64, 0.0), hist.bucketLowerBound(0), 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 10.0), hist.bucketLowerBound(1), 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 20.0), hist.bucketLowerBound(2), 0.001);

    const keys = try hist.sortedKeys(alloc);
    defer alloc.free(keys);
    try std.testing.expectEqual(@as(usize, 3), keys.len);
    try std.testing.expectEqual(@as(i64, 0), keys[0]);
    try std.testing.expectEqual(@as(i64, 1), keys[1]);
    try std.testing.expectEqual(@as(i64, 2), keys[2]);
}

test "terms facet" {
    const alloc = std.testing.allocator;

    var facet = TermsFacet.init(alloc);
    defer facet.deinit();

    try facet.collect("red");
    try facet.collect("blue");
    try facet.collect("red");
    try facet.collect("green");
    try facet.collect("red");
    try facet.collect("blue");

    try std.testing.expectEqual(@as(u64, 3), facet.getCount("red"));
    try std.testing.expectEqual(@as(u64, 2), facet.getCount("blue"));
    try std.testing.expectEqual(@as(u64, 1), facet.getCount("green"));
    try std.testing.expectEqual(@as(u64, 0), facet.getCount("yellow"));

    const top = try facet.topK(alloc, 2);
    defer alloc.free(top);
    try std.testing.expectEqual(@as(usize, 2), top.len);
    try std.testing.expectEqualStrings("red", top[0].term);
    try std.testing.expectEqual(@as(u64, 3), top[0].count);
    try std.testing.expectEqualStrings("blue", top[1].term);
    try std.testing.expectEqual(@as(u64, 2), top[1].count);
}

test "date histogram hour" {
    const alloc = std.testing.allocator;

    var agg = DateHistogramAgg.init(alloc, .hour);
    defer agg.deinit();

    // Three timestamps in different hours on 2024-01-15
    // Hour 0: 2024-01-15 00:30:00 UTC
    const base_day: u64 = 19737 * 86400; // days since epoch for ~2024-01-15
    const t1 = (base_day + 1800) * ns_per_sec; // 00:30
    const t2 = (base_day + 5400) * ns_per_sec; // 01:30
    const t3 = (base_day + 1200) * ns_per_sec; // 00:20 (same hour as t1)

    try agg.collect(t1);
    try agg.collect(t2);
    try agg.collect(t3);

    const keys = try agg.sortedKeys(alloc);
    defer alloc.free(keys);

    try std.testing.expectEqual(@as(usize, 2), keys.len);
    // Hour 0 bucket should have 2 hits
    try std.testing.expectEqual(@as(u64, 2), agg.getCount(keys[0]));
    // Hour 1 bucket should have 1 hit
    try std.testing.expectEqual(@as(u64, 1), agg.getCount(keys[1]));
    // Keys should differ by exactly 1 hour in nanoseconds
    try std.testing.expectEqual(3600 * ns_per_sec, keys[1] - keys[0]);
}

test "date histogram month" {
    const alloc = std.testing.allocator;

    var agg = DateHistogramAgg.init(alloc, .month);
    defer agg.deinit();

    // Jan 15 2024 and Feb 10 2024
    // Jan 15 = day 19737 from epoch (approximate)
    const jan15: u64 = 19737 * 86400 * ns_per_sec;
    const feb10: u64 = (19737 + 26) * 86400 * ns_per_sec;

    try agg.collect(jan15);
    try agg.collect(feb10);

    const keys = try agg.sortedKeys(alloc);
    defer alloc.free(keys);

    try std.testing.expectEqual(@as(usize, 2), keys.len);
    try std.testing.expectEqual(@as(u64, 1), agg.getCount(keys[0]));
    try std.testing.expectEqual(@as(u64, 1), agg.getCount(keys[1]));
}

test "date histogram week monday alignment" {
    const alloc = std.testing.allocator;

    var agg = DateHistogramAgg.init(alloc, .week);
    defer agg.deinit();

    // 2024-01-01 was a Monday. Days since epoch = 19723.
    // Verify: (19723 + 3) % 7 = 19726 % 7 = 0 → Monday
    const monday_jan1: u64 = 19723 * 86400 * ns_per_sec;
    const tuesday_jan2: u64 = 19724 * 86400 * ns_per_sec;
    const monday_jan8: u64 = 19730 * 86400 * ns_per_sec;

    try agg.collect(monday_jan1);
    try agg.collect(tuesday_jan2);
    try agg.collect(monday_jan8);

    const keys = try agg.sortedKeys(alloc);
    defer alloc.free(keys);

    // Should be 2 buckets: week of Jan 1 (2 hits), week of Jan 8 (1 hit)
    try std.testing.expectEqual(@as(usize, 2), keys.len);
    try std.testing.expectEqual(@as(u64, 2), agg.getCount(keys[0]));
    try std.testing.expectEqual(@as(u64, 1), agg.getCount(keys[1]));
    // Both bucket keys should be Mondays (keys diff = 7 days)
    try std.testing.expectEqual(7 * 86400 * ns_per_sec, keys[1] - keys[0]);
}

test "range aggregation non-overlapping" {
    const alloc = std.testing.allocator;

    var agg = try RangeAgg.init(alloc, &.{
        .{ .from = 0, .to = 100 },
        .{ .from = 100, .to = 200 },
        .{ .from = 200, .to = 300 },
    });
    defer agg.deinit();

    agg.collect(50); // bucket 0
    agg.collect(150); // bucket 1
    agg.collect(250); // bucket 2
    agg.collect(99); // bucket 0
    agg.collect(100); // bucket 1 (from inclusive)

    try std.testing.expectEqual(@as(u64, 2), agg.buckets[0].count);
    try std.testing.expectEqual(@as(u64, 2), agg.buckets[1].count);
    try std.testing.expectEqual(@as(u64, 1), agg.buckets[2].count);
}

test "range aggregation overlapping" {
    const alloc = std.testing.allocator;

    var agg = try RangeAgg.init(alloc, &.{
        .{ .from = 0, .to = 50 },
        .{ .from = 25, .to = 75 },
    });
    defer agg.deinit();

    agg.collect(30); // matches both
    agg.collect(10); // matches first only
    agg.collect(60); // matches second only

    try std.testing.expectEqual(@as(u64, 2), agg.buckets[0].count);
    try std.testing.expectEqual(@as(u64, 2), agg.buckets[1].count);
}

test "range aggregation unbounded" {
    const alloc = std.testing.allocator;

    var agg = try RangeAgg.init(alloc, &.{
        .{ .from = null, .to = 100 }, // everything < 100
        .{ .from = 100, .to = null }, // everything >= 100
    });
    defer agg.deinit();

    agg.collect(-500);
    agg.collect(50);
    agg.collect(100);
    agg.collect(9999);

    try std.testing.expectEqual(@as(u64, 2), agg.buckets[0].count);
    try std.testing.expectEqual(@as(u64, 2), agg.buckets[1].count);
}

test "geo distance aggregation" {
    const alloc = std.testing.allocator;

    // Center: San Francisco
    const sf = geo.GeoPoint{ .lat = 37.7749, .lon = -122.4194 };

    var agg = try GeoDistanceAgg.init(alloc, sf, &.{
        .{ .from = null, .to = 2000 }, // < 2km
        .{ .from = 2000, .to = 10000 }, // 2-10km
        .{ .from = 10000, .to = 200000 }, // 10-200km
    });
    defer agg.deinit();

    // ~1km away (slight offset)
    agg.collect(.{ .lat = 37.7839, .lon = -122.4194 });
    // ~5km away
    agg.collect(.{ .lat = 37.8199, .lon = -122.4194 });
    // ~100km away (Sacramento area)
    agg.collect(.{ .lat = 38.5816, .lon = -121.4944 });

    try std.testing.expectEqual(@as(u64, 1), agg.bands[0].count); // < 2km
    try std.testing.expectEqual(@as(u64, 1), agg.bands[1].count); // 2-10km
    try std.testing.expectEqual(@as(u64, 1), agg.bands[2].count); // 10-200km
}

test "geohash grid aggregation" {
    const alloc = std.testing.allocator;

    var agg = GeohashGridAgg.init(alloc, 5);
    defer agg.deinit();

    // Two points very close together (same geohash cell at precision 5)
    try agg.collect(.{ .lat = 37.7749, .lon = -122.4194 });
    try agg.collect(.{ .lat = 37.7750, .lon = -122.4195 });
    // One point far away (different cell)
    try agg.collect(.{ .lat = 40.7128, .lon = -74.0060 });

    const top = try agg.topK(alloc, 10);
    defer alloc.free(top);

    try std.testing.expectEqual(@as(usize, 2), top.len);
    // Top cell should have count 2 (the two SF points)
    try std.testing.expectEqual(@as(u64, 2), top[0].count);
    // Second cell should have count 1 (NYC)
    try std.testing.expectEqual(@as(u64, 1), top[1].count);
}
