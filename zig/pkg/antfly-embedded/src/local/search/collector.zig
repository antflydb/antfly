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

//! Result collectors for query execution.
//!
//! Collectors accumulate results during query execution:
//!   - TopKCollector: min-heap for top-k scored results
//!   - FilterCollector: collects all matching doc IDs (no scoring)

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const ScoredHit = struct {
    doc_id: u32,
    score: f32,
};

/// Min-heap based top-k collector.
/// Maintains the k highest-scoring documents seen so far.
pub const TopKCollector = struct {
    alloc: Allocator,
    k: u32,
    heap: std.ArrayListUnmanaged(ScoredHit),
    threshold: f32,

    pub fn init(alloc: Allocator, k: u32) TopKCollector {
        return .{
            .alloc = alloc,
            .k = k,
            .heap = .empty,
            .threshold = 0,
        };
    }

    pub fn deinit(self: *TopKCollector) void {
        self.heap.deinit(self.alloc);
    }

    /// Submit a scored document. Only retained if it beats the current threshold.
    pub fn collect(self: *TopKCollector, doc_id: u32, score: f32) !void {
        if (self.heap.items.len < self.k) {
            try self.heap.append(self.alloc, .{ .doc_id = doc_id, .score = score });
            if (self.heap.items.len == self.k) {
                self.recomputeThreshold();
            }
        } else if (score > self.threshold) {
            // Replace the lowest-scoring hit
            var min_idx: usize = 0;
            for (self.heap.items[1..], 1..) |h, i| {
                if (h.score < self.heap.items[min_idx].score) min_idx = i;
            }
            self.heap.items[min_idx] = .{ .doc_id = doc_id, .score = score };
            self.recomputeThreshold();
        }
    }

    fn recomputeThreshold(self: *TopKCollector) void {
        self.threshold = self.heap.items[0].score;
        for (self.heap.items[1..]) |h| {
            if (h.score < self.threshold) self.threshold = h.score;
        }
    }

    /// Return results sorted by score descending. Caller owns the slice.
    pub fn results(self: *TopKCollector) ![]ScoredHit {
        const owned = try self.alloc.dupe(ScoredHit, self.heap.items);
        std.mem.sort(ScoredHit, owned, {}, struct {
            fn cmp(_: void, a: ScoredHit, b: ScoredHit) bool {
                return a.score > b.score;
            }
        }.cmp);
        return owned;
    }
};

/// Collects all matching document IDs without scoring.
pub const FilterCollector = struct {
    alloc: Allocator,
    doc_ids: std.ArrayListUnmanaged(u32),

    pub fn init(alloc: Allocator) FilterCollector {
        return .{
            .alloc = alloc,
            .doc_ids = .empty,
        };
    }

    pub fn deinit(self: *FilterCollector) void {
        self.doc_ids.deinit(self.alloc);
    }

    pub fn collect(self: *FilterCollector, doc_id: u32) !void {
        try self.doc_ids.append(self.alloc, doc_id);
    }

    /// Return collected doc IDs. Caller owns the slice.
    pub fn results(self: *FilterCollector) ![]u32 {
        return try self.alloc.dupe(u32, self.doc_ids.items);
    }
};

// ============================================================================
// Tests
// ============================================================================

test "top-k collector returns highest scores" {
    const alloc = std.testing.allocator;

    var collector = TopKCollector.init(alloc, 3);
    defer collector.deinit();

    try collector.collect(0, 1.0);
    try collector.collect(1, 5.0);
    try collector.collect(2, 3.0);
    try collector.collect(3, 2.0);
    try collector.collect(4, 7.0);
    try collector.collect(5, 4.0);

    const res = try collector.results();
    defer alloc.free(res);

    try std.testing.expectEqual(@as(usize, 3), res.len);
    // Sorted descending
    try std.testing.expectEqual(@as(u32, 4), res[0].doc_id); // score 7.0
    try std.testing.expectEqual(@as(u32, 1), res[1].doc_id); // score 5.0
    try std.testing.expectEqual(@as(u32, 5), res[2].doc_id); // score 4.0
}

test "top-k collector with fewer than k results" {
    const alloc = std.testing.allocator;

    var collector = TopKCollector.init(alloc, 10);
    defer collector.deinit();

    try collector.collect(0, 2.0);
    try collector.collect(1, 1.0);

    const res = try collector.results();
    defer alloc.free(res);

    try std.testing.expectEqual(@as(usize, 2), res.len);
    try std.testing.expectEqual(@as(u32, 0), res[0].doc_id);
}

test "filter collector collects all docs" {
    const alloc = std.testing.allocator;

    var collector = FilterCollector.init(alloc);
    defer collector.deinit();

    try collector.collect(5);
    try collector.collect(10);
    try collector.collect(15);

    const res = try collector.results();
    defer alloc.free(res);

    try std.testing.expectEqual(@as(usize, 3), res.len);
    try std.testing.expectEqual(@as(u32, 5), res[0]);
    try std.testing.expectEqual(@as(u32, 10), res[1]);
    try std.testing.expectEqual(@as(u32, 15), res[2]);
}
