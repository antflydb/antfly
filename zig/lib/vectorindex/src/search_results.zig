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
const Allocator = std.mem.Allocator;

pub const SearchResult = struct {
    vector_id: u64,
    distance: f32,
    error_bound: f32 = 0,
    metadata: ?[]u8 = null,

    pub fn maybeCloser(self: SearchResult, other: SearchResult) bool {
        return self.distance - self.error_bound <= other.distance + other.error_bound;
    }

    pub fn definitelyCloser(self: SearchResult, other: SearchResult) bool {
        return self.distance + self.error_bound < other.distance - other.error_bound;
    }
};

pub const ApproxSearchResult = struct {
    vector_id: u64,
    distance: f32,
    error_bound: f32 = 0,

    pub fn maybeCloser(self: ApproxSearchResult, other: ApproxSearchResult) bool {
        return self.distance - self.error_bound <= other.distance + other.error_bound;
    }

    pub fn definitelyCloser(self: ApproxSearchResult, other: ApproxSearchResult) bool {
        return self.distance + self.error_bound < other.distance - other.error_bound;
    }
};

fn resultWorseThan(_: void, a: SearchResult, b: SearchResult) std.math.Order {
    const primary = std.math.order(b.distance, a.distance);
    if (primary != .eq) return primary;
    return std.math.order(b.vector_id, a.vector_id);
}

fn approxResultWorseThan(_: void, a: ApproxSearchResult, b: ApproxSearchResult) std.math.Order {
    return std.math.order(b.distance, a.distance);
}

pub const SearchResults = struct {
    alloc: Allocator,
    items: std.PriorityQueue(SearchResult, void, resultWorseThan),
    k: usize,
    max_items: usize,

    pub fn init(alloc: Allocator, k: usize) SearchResults {
        return .{ .alloc = alloc, .items = .initContext({}), .k = k, .max_items = k };
    }

    pub fn initCapacity(alloc: Allocator, k: usize, max_items: usize, capacity: usize) !SearchResults {
        var out = SearchResults.init(alloc, k);
        out.max_items = max_items;
        try out.items.ensureTotalCapacity(alloc, capacity);
        return out;
    }

    pub fn deinit(self: *SearchResults) void {
        for (self.items.items) |item| {
            if (item.metadata) |metadata| self.alloc.free(metadata);
        }
        self.items.deinit(self.alloc);
    }

    pub fn addResult(self: *SearchResults, vector_id: u64, dist: f32, error_bound: f32) void {
        self.addResultWithOwnedMetadata(vector_id, dist, error_bound, null);
    }

    pub fn addResultWithOwnedMetadata(self: *SearchResults, vector_id: u64, dist: f32, error_bound: f32, metadata: ?[]u8) void {
        if (self.k == 0) {
            if (metadata) |owned| self.alloc.free(owned);
            return;
        }
        const candidate = SearchResult{
            .vector_id = vector_id,
            .distance = dist,
            .error_bound = error_bound,
            .metadata = metadata,
        };
        if (self.items.items.len < self.k) {
            self.items.push(self.alloc, candidate) catch {
                if (metadata) |owned| self.alloc.free(owned);
                return;
            };
        } else {
            const worst = self.items.peek() orelse return;
            if (candidate.distance < worst.distance) {
                if (self.items.pop()) |removed| {
                    if (removed.metadata) |owned| self.alloc.free(owned);
                }
                self.items.push(self.alloc, candidate) catch {
                    if (metadata) |owned| self.alloc.free(owned);
                    return;
                };
            } else if (metadata) |owned| {
                self.alloc.free(owned);
            }
        }
    }

    pub fn addApproxResult(self: *SearchResults, vector_id: u64, dist: f32, error_bound: f32) void {
        const candidate = SearchResult{
            .vector_id = vector_id,
            .distance = dist,
            .error_bound = error_bound,
        };
        if (self.items.items.len < self.k) {
            self.items.push(self.alloc, candidate) catch return;
            return;
        }

        const worst = self.items.peek() orelse return;
        if (self.items.items.len >= self.max_items and !candidate.maybeCloser(worst)) {
            return;
        }

        if (candidate.distance < worst.distance) {
            if (candidate.definitelyCloser(worst)) {
                _ = self.items.pop();
            }
            self.items.push(self.alloc, candidate) catch return;
        } else if (candidate.maybeCloser(worst)) {
            self.items.push(self.alloc, candidate) catch return;
        }

        if (self.items.items.len > self.max_items) {
            _ = self.items.pop();
        }
    }

    pub fn isFull(self: *const SearchResults) bool {
        return self.items.items.len >= self.k;
    }

    pub fn worstDistance(self: *const SearchResults) f32 {
        return if (self.items.items.len > 0) self.items.items[0].distance else -std.math.inf(f32);
    }

    pub fn sort(self: *SearchResults) void {
        std.mem.sort(SearchResult, self.items.items, {}, struct {
            fn lessThan(_: void, a: SearchResult, b: SearchResult) bool {
                return a.distance < b.distance;
            }
        }.lessThan);
    }

    pub fn getHits(self: *const SearchResults) []const SearchResult {
        return self.items.items;
    }

    pub fn takeMetadata(self: *SearchResults, index: usize) ?[]u8 {
        if (index >= self.items.items.len) return null;
        const metadata = self.items.items[index].metadata;
        self.items.items[index].metadata = null;
        return metadata;
    }

    pub fn fromSortedSlice(alloc: Allocator, k: usize, items: []const SearchResult) !SearchResults {
        const keep = @min(k, items.len);
        var out = try SearchResults.initCapacity(alloc, k, k, keep);
        errdefer out.deinit();
        out.items.items.len = keep;
        @memcpy(out.items.items[0..keep], items[0..keep]);
        return out;
    }

    pub fn fromSortedApproxSlice(alloc: Allocator, k: usize, items: []const ApproxSearchResult) !SearchResults {
        const keep = @min(k, items.len);
        var out = try SearchResults.initCapacity(alloc, k, k, keep);
        errdefer out.deinit();
        out.items.items.len = keep;
        for (items[0..keep], 0..) |item, i| {
            out.items.items[i] = .{
                .vector_id = item.vector_id,
                .distance = item.distance,
                .error_bound = item.error_bound,
            };
        }
        return out;
    }
};

pub const ApproxSearchResults = struct {
    alloc: Allocator,
    items: std.PriorityQueue(ApproxSearchResult, void, approxResultWorseThan),
    k: usize,
    max_items: usize,

    pub fn init(alloc: Allocator, k: usize) ApproxSearchResults {
        return .{ .alloc = alloc, .items = .initContext({}), .k = k, .max_items = k };
    }

    pub fn initCapacity(alloc: Allocator, k: usize, max_items: usize, capacity: usize) !ApproxSearchResults {
        var out = ApproxSearchResults.init(alloc, k);
        out.max_items = max_items;
        try out.items.ensureTotalCapacity(alloc, capacity);
        return out;
    }

    pub fn deinit(self: *ApproxSearchResults) void {
        self.items.deinit(self.alloc);
    }

    pub fn addResult(self: *ApproxSearchResults, vector_id: u64, dist: f32, error_bound: f32) void {
        const candidate = ApproxSearchResult{
            .vector_id = vector_id,
            .distance = dist,
            .error_bound = error_bound,
        };
        if (self.items.items.len < self.k) {
            self.items.push(self.alloc, candidate) catch return;
        } else {
            const worst = self.items.peek() orelse return;
            if (candidate.distance < worst.distance) {
                _ = self.items.pop();
                self.items.push(self.alloc, candidate) catch return;
            }
        }
    }

    pub fn addApproxResult(self: *ApproxSearchResults, vector_id: u64, dist: f32, error_bound: f32) void {
        if (self.items.items.len < self.k) {
            self.items.push(self.alloc, .{
                .vector_id = vector_id,
                .distance = dist,
                .error_bound = error_bound,
            }) catch return;
            return;
        }

        const worst = self.items.peek() orelse return;
        const candidate_maybe_closer = dist - error_bound <= worst.distance + worst.error_bound;
        if (self.items.items.len >= self.max_items and !candidate_maybe_closer) return;

        if (dist < worst.distance) {
            if (dist + error_bound < worst.distance - worst.error_bound) {
                _ = self.items.pop();
            }
            self.items.push(self.alloc, .{
                .vector_id = vector_id,
                .distance = dist,
                .error_bound = error_bound,
            }) catch return;
        } else if (candidate_maybe_closer) {
            self.items.push(self.alloc, .{
                .vector_id = vector_id,
                .distance = dist,
                .error_bound = error_bound,
            }) catch return;
        }

        if (self.items.items.len > self.max_items) {
            _ = self.items.pop();
        }
    }

    /// Admit an ordered block of approximate results with the same semantics
    /// as repeated addApproxResult calls. Once the heap is at its hard limit,
    /// groups whose lower bounds all exceed the current worst upper bound
    /// cannot mutate the heap and can be rejected with one SIMD comparison.
    /// A group containing even one possible admission is replayed in original
    /// order so error-bound overlap and tie behavior remain unchanged.
    pub fn addApproxResults(
        self: *ApproxSearchResults,
        vector_ids: []const u64,
        distances: []const f32,
        error_bounds: []const f32,
    ) void {
        std.debug.assert(vector_ids.len == distances.len);
        std.debug.assert(vector_ids.len == error_bounds.len);

        const F32x8 = @Vector(8, f32);
        var i: usize = 0;
        while (i < vector_ids.len) {
            if (i + 8 <= vector_ids.len and self.items.items.len >= self.max_items) {
                const worst = self.items.peek() orelse break;
                const worst_upper: F32x8 = @splat(worst.distance + worst.error_bound);
                const dist: F32x8 = distances[i..][0..8].*;
                const error_bound: F32x8 = error_bounds[i..][0..8].*;
                const maybe_closer = dist - error_bound <= worst_upper;
                if (!@reduce(.Or, maybe_closer)) {
                    i += 8;
                    continue;
                }
            }

            self.addApproxResult(vector_ids[i], distances[i], error_bounds[i]);
            i += 1;
        }
    }

    pub fn sort(self: *ApproxSearchResults) void {
        std.mem.sort(ApproxSearchResult, self.items.items, {}, struct {
            fn lessThan(_: void, a: ApproxSearchResult, b: ApproxSearchResult) bool {
                if (a.distance != b.distance) return a.distance < b.distance;
                return a.vector_id < b.vector_id;
            }
        }.lessThan);
    }

    pub fn isFull(self: *const ApproxSearchResults) bool {
        return self.items.items.len >= self.k;
    }

    pub fn worstDistance(self: *const ApproxSearchResults) f32 {
        return if (self.items.items.len > 0) self.items.items[0].distance else -std.math.inf(f32);
    }

    pub fn toFinalResults(self: *ApproxSearchResults) !SearchResults {
        var out = try SearchResults.initCapacity(self.alloc, self.k, self.max_items, self.items.items.len);
        errdefer out.deinit();
        out.items.items.len = self.items.items.len;
        for (self.items.items, 0..) |item, i| {
            out.items.items[i] = .{
                .vector_id = item.vector_id,
                .distance = item.distance,
                .error_bound = item.error_bound,
            };
        }
        return out;
    }
};

test "approx result heap comparator ignores vector id ties to match Go" {
    try std.testing.expectEqual(
        std.math.Order.eq,
        approxResultWorseThan(
            {},
            .{ .vector_id = 1, .distance = 1, .error_bound = 0 },
            .{ .vector_id = 2, .distance = 1, .error_bound = 0 },
        ),
    );
}

test "batched approximate admission is identical to scalar admission" {
    const vector_ids = [_]u64{
        11, 12, 13, 14, 15, 16, 17, 18,
        21, 22, 23, 24, 25, 26, 27, 28,
        31, 32, 33, 34, 35, 36, 37, 38,
    };
    const distances = [_]f32{
        4.0,  1.0,  8.0,  2.0,  7.0,  3.0,  6.0,  5.0,
        20.0, 21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0,
        2.5,  9.0,  1.5,  30.0, 0.5,  4.5,  2.25, 12.0,
    };
    const error_bounds = [_]f32{
        0.1, 0.2, 0.1, 0.3, 0.2, 0.1, 0.2, 0.1,
        0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
        0.4, 0.1, 0.2, 0.1, 0.1, 0.3, 0.2, 0.1,
    };

    var scalar = try ApproxSearchResults.initCapacity(std.testing.allocator, 4, 8, 8);
    defer scalar.deinit();
    for (vector_ids, 0..) |vector_id, i| {
        scalar.addApproxResult(vector_id, distances[i], error_bounds[i]);
    }

    var batched = try ApproxSearchResults.initCapacity(std.testing.allocator, 4, 8, 8);
    defer batched.deinit();
    batched.addApproxResults(&vector_ids, &distances, &error_bounds);

    scalar.sort();
    batched.sort();
    try std.testing.expectEqualSlices(ApproxSearchResult, scalar.items.items, batched.items.items);
}

test "batched approximate admission preserves scalar thresholds across random blocks" {
    const count = 513;
    var vector_ids: [count]u64 = undefined;
    var distances: [count]f32 = undefined;
    var error_bounds: [count]f32 = undefined;
    var prng = std.Random.DefaultPrng.init(0x4150_5052_4f58);
    const random = prng.random();
    for (0..count) |i| {
        vector_ids[i] = @intCast(i + 1);
        distances[i] = random.float(f32) * 100.0;
        error_bounds[i] = random.float(f32) * 3.0;
    }

    for ([_]struct { k: usize, max_items: usize }{
        .{ .k = 1, .max_items = 1 },
        .{ .k = 10, .max_items = 80 },
        .{ .k = 100, .max_items = 800 },
    }) |config| {
        var scalar = try ApproxSearchResults.initCapacity(std.testing.allocator, config.k, config.max_items, config.max_items);
        defer scalar.deinit();
        for (vector_ids, 0..) |vector_id, i| {
            scalar.addApproxResult(vector_id, distances[i], error_bounds[i]);
        }

        var batched = try ApproxSearchResults.initCapacity(std.testing.allocator, config.k, config.max_items, config.max_items);
        defer batched.deinit();
        batched.addApproxResults(&vector_ids, &distances, &error_bounds);

        scalar.sort();
        batched.sort();
        try std.testing.expectEqualSlices(ApproxSearchResult, scalar.items.items, batched.items.items);
    }
}

test "fromSortedApproxSlice preserves sorted order without heap pushes" {
    const items = [_]ApproxSearchResult{
        .{ .vector_id = 10, .distance = 1.0, .error_bound = 0.1 },
        .{ .vector_id = 20, .distance = 2.0, .error_bound = 0.2 },
        .{ .vector_id = 30, .distance = 3.0, .error_bound = 0.3 },
    };

    var results = try SearchResults.fromSortedApproxSlice(std.testing.allocator, 2, items[0..]);
    defer results.deinit();

    const hits = results.getHits();
    try std.testing.expectEqual(@as(usize, 2), hits.len);
    try std.testing.expectEqual(@as(u64, 10), hits[0].vector_id);
    try std.testing.expectEqual(@as(u64, 20), hits[1].vector_id);
    try std.testing.expectEqual(@as(f32, 1.0), hits[0].distance);
    try std.testing.expectEqual(@as(f32, 2.0), hits[1].distance);
}

test "owned metadata is released when result is not retained" {
    var zero = SearchResults.init(std.testing.allocator, 0);
    zero.addResultWithOwnedMetadata(1, 1.0, 0, try std.testing.allocator.dupe(u8, "doc:a"));
    try std.testing.expectEqual(@as(usize, 0), zero.getHits().len);
    zero.deinit();

    var top_one = SearchResults.init(std.testing.allocator, 1);
    defer top_one.deinit();
    top_one.addResultWithOwnedMetadata(1, 1.0, 0, try std.testing.allocator.dupe(u8, "doc:a"));
    top_one.addResultWithOwnedMetadata(2, 2.0, 0, try std.testing.allocator.dupe(u8, "doc:b"));
    try std.testing.expectEqual(@as(usize, 1), top_one.getHits().len);
    try std.testing.expectEqual(@as(u64, 1), top_one.getHits()[0].vector_id);
}
