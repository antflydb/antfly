// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License for the specific language governing permissions and
// limitations.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Config = struct {
    base_weight: f64,
    metric_weight: f64,
    missing_score: f64,
};

/// A selected candidate borrows its id from the caller's hit array. The
/// caller uses original_index to move only the requested page into its result
/// and allocates explanation strings only for those surviving hits.
pub const Selection = struct {
    original_index: usize,
    id: []const u8,
    base_score: f64,
    metric_score: ?f64,
    metric_score_used: f64,
    final_score: f32,
};

fn comesBefore(left: Selection, right: Selection) bool {
    if (left.final_score == right.final_score)
        return std.mem.lessThan(u8, left.id, right.id);
    return left.final_score > right.final_score;
}

fn worstFirst(_: void, left: Selection, right: Selection) std.math.Order {
    if (comesBefore(left, right)) return .gt;
    if (comesBefore(right, left)) return .lt;
    return .eq;
}

fn lessThan(_: void, left: Selection, right: Selection) bool {
    return comesBefore(left, right);
}

pub fn clampF64ToF32(value: f64) f32 {
    const max = std.math.floatMax(f32);
    if (value > max) return max;
    if (value < -max) return -max;
    return @floatCast(value);
}

/// Select one reranked page in O(N log(offset + limit)) time and
/// O(offset + limit) memory. `hits` may be any SearchHit-compatible slice with
/// `id` and optional `score` fields, keeping this policy independent of the
/// native and serverless storage implementations.
pub fn selectPageAlloc(
    alloc: Allocator,
    hits: anytype,
    metric_scores: []const ?f64,
    config: Config,
    offset: u32,
    limit: u32,
) ![]Selection {
    if (hits.len != metric_scores.len) return error.InvalidGraphMetricScores;
    if (!std.math.isFinite(config.base_weight) or
        !std.math.isFinite(config.metric_weight) or
        !std.math.isFinite(config.missing_score))
        return error.InvalidGraphMetricScores;
    if (limit == 0 or hits.len == 0) return try alloc.alloc(Selection, 0);
    if (@as(usize, offset) >= hits.len) return try alloc.alloc(Selection, 0);
    const requested = std.math.add(usize, offset, limit) catch
        return error.InvalidGraphMetricRerankWindow;
    const capacity = @min(requested, hits.len);
    var selected = std.PriorityQueue(Selection, void, worstFirst).initContext({});
    defer selected.deinit(alloc);
    try selected.ensureTotalCapacity(alloc, capacity);

    for (hits, metric_scores, 0..) |hit, metric_score, index| {
        const base_score: f64 = if (hit.score) |score| @floatCast(score) else 0;
        const metric_score_used = metric_score orelse config.missing_score;
        if (!std.math.isFinite(base_score) or !std.math.isFinite(metric_score_used))
            return error.InvalidGraphMetricScores;
        const final_score = config.base_weight * base_score + config.metric_weight * metric_score_used;
        if (!std.math.isFinite(final_score)) return error.InvalidGraphMetricScores;
        const candidate = Selection{
            .original_index = index,
            .id = hit.id,
            .base_score = base_score,
            .metric_score = metric_score,
            .metric_score_used = metric_score_used,
            .final_score = clampF64ToF32(final_score),
        };
        if (selected.count() < capacity) {
            try selected.push(alloc, candidate);
        } else if (comesBefore(candidate, selected.peek().?)) {
            _ = selected.pop();
            try selected.push(alloc, candidate);
        }
    }

    std.mem.sort(Selection, selected.items, {}, lessThan);
    const start = @min(@as(usize, offset), selected.items.len);
    const count = @min(@as(usize, limit), selected.items.len - start);
    return try alloc.dupe(Selection, selected.items[start .. start + count]);
}

test "bounded rerank rejects non-finite inputs and skips impossible offsets" {
    const Hit = struct { id: []const u8, score: ?f32 };
    const hits = [_]Hit{.{ .id = "a", .score = 1 }};
    const scores = [_]?f64{0.5};
    const empty = try selectPageAlloc(std.testing.allocator, &hits, &scores, .{
        .base_weight = 1,
        .metric_weight = 1,
        .missing_score = 0,
    }, 2, 1);
    defer std.testing.allocator.free(empty);
    try std.testing.expectEqual(@as(usize, 0), empty.len);

    try std.testing.expectError(error.InvalidGraphMetricScores, selectPageAlloc(
        std.testing.allocator,
        &hits,
        &scores,
        .{ .base_weight = std.math.nan(f64), .metric_weight = 1, .missing_score = 0 },
        0,
        1,
    ));
}

test "bounded rerank selects the requested stable page" {
    const Hit = struct { id: []const u8, score: ?f32 };
    const hits = [_]Hit{
        .{ .id = "c", .score = 1 },
        .{ .id = "a", .score = 1 },
        .{ .id = "b", .score = 1 },
        .{ .id = "d", .score = 1 },
    };
    const scores = [_]?f64{ 0.2, 0.5, 0.5, null };
    const selected = try selectPageAlloc(std.testing.allocator, &hits, &scores, .{
        .base_weight = 1,
        .metric_weight = 1,
        .missing_score = 0,
    }, 1, 2);
    defer std.testing.allocator.free(selected);
    try std.testing.expectEqual(@as(usize, 2), selected.len);
    try std.testing.expectEqualStrings("b", selected[0].id);
    try std.testing.expectEqualStrings("c", selected[1].id);
}
