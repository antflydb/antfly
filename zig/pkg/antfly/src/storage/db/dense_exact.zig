// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

const std = @import("std");
const Allocator = std.mem.Allocator;
const vectorindex = @import("antfly_vectorindex");

/// The result of exact dense scoring together with work measured at the point
/// where it happened. `vectors_scored` counts distance evaluations, not input
/// IDs: duplicates, exclusions, and missing vectors are not counted.
pub const SearchOutcome = struct {
    results: vectorindex.SearchResults,
    vectors_scored: u64,
};

/// An owned, sorted, unique include set with a sorted exclusion set removed.
///
/// Both scorers use this type so fallback and database-backed execution have
/// identical candidate semantics. The merge subtraction is linear once an
/// unsorted input has been normalized, avoiding O(includes * excludes) scans.
pub const CandidateDifference = struct {
    alloc: Allocator,
    candidate_storage: []u64,
    owned_exclude_storage: ?[]u64,
    values: []u64,

    pub fn init(
        alloc: Allocator,
        include_ids: []const u64,
        exclude_ids: []const u64,
    ) !CandidateDifference {
        const candidate_storage = try alloc.dupe(u64, include_ids);
        errdefer alloc.free(candidate_storage);
        std.mem.sort(u64, candidate_storage, {}, std.sort.asc(u64));
        const sorted_unique_candidates = candidate_storage[0..uniqueSortedU64(candidate_storage)];

        var owned_exclude_storage: ?[]u64 = null;
        errdefer if (owned_exclude_storage) |storage| alloc.free(storage);
        const sorted_unique_excludes: []const u64 = if (isSortedUniqueU64(exclude_ids))
            exclude_ids
        else blk: {
            const storage = try alloc.dupe(u64, exclude_ids);
            owned_exclude_storage = storage;
            std.mem.sort(u64, storage, {}, std.sort.asc(u64));
            break :blk storage[0..uniqueSortedU64(storage)];
        };

        const output_len = subtractSortedUniqueU64InPlace(
            sorted_unique_candidates,
            sorted_unique_excludes,
        );
        return .{
            .alloc = alloc,
            .candidate_storage = candidate_storage,
            .owned_exclude_storage = owned_exclude_storage,
            .values = sorted_unique_candidates[0..output_len],
        };
    }

    pub fn deinit(self: *CandidateDifference) void {
        if (self.owned_exclude_storage) |storage| self.alloc.free(storage);
        self.alloc.free(self.candidate_storage);
        self.* = undefined;
    }
};

/// Return the exact cardinality of `include_ids \\ exclude_ids` without
/// allocating when both inputs already satisfy the scorer's normalized set
/// contract. Null means a caller must normalize first or use a conservative
/// upper bound.
pub fn sortedUniqueDifferenceCount(
    include_ids: []const u64,
    exclude_ids: []const u64,
) ?usize {
    if (!isSortedUniqueU64(include_ids) or !isSortedUniqueU64(exclude_ids)) return null;

    return sortedUniqueDifferenceCountAssumeNormalized(include_ids, exclude_ids);
}

fn sortedUniqueDifferenceCountAssumeNormalized(
    include_ids: []const u64,
    exclude_ids: []const u64,
) usize {
    var count: usize = 0;
    var excluded_index: usize = 0;
    for (include_ids) |value| {
        while (excluded_index < exclude_ids.len and exclude_ids[excluded_index] < value) {
            excluded_index += 1;
        }
        if (excluded_index < exclude_ids.len and exclude_ids[excluded_index] == value) continue;
        count += 1;
    }
    return count;
}

/// Return a conservative cardinality for `include_ids \\ exclude_ids` without
/// allocating when the exclusion set is sorted and unique.
///
/// Index-derived include IDs are unique but intentionally retain document
/// order, so requiring them to be sorted would discard the useful fast path.
/// An unordered or duplicate include can only make this value too large: each
/// occurrence is counted independently. That is safe for routing because it
/// can keep a query on ANN, but can never route too many unique candidates to
/// exact scoring. Sorted unique inputs use the linear exact-count path.
pub fn differenceCandidateUpperBound(
    include_ids: []const u64,
    exclude_ids: []const u64,
) ?usize {
    // The overwhelmingly common positive-filter query has no exclusions.
    // Its input length is already a conservative unique-candidate bound, so
    // avoid walking a potentially large include list merely to rediscover it.
    if (include_ids.len == 0 or exclude_ids.len == 0) return include_ids.len;
    if (!isSortedUniqueU64(exclude_ids)) return null;
    return differenceCandidateUpperBoundNormalizedExclusions(include_ids, exclude_ids);
}

/// Same bound as `differenceCandidateUpperBound`, for callers that already
/// own a sorted, unique exclusion set. This avoids validating large native
/// exclusion lists a second time after their merge/normalization step.
pub fn differenceCandidateUpperBoundNormalizedExclusions(
    include_ids: []const u64,
    exclude_ids: []const u64,
) usize {
    if (include_ids.len == 0 or exclude_ids.len == 0) return include_ids.len;
    if (isSortedUniqueU64(include_ids)) {
        return sortedUniqueDifferenceCountAssumeNormalized(include_ids, exclude_ids);
    }

    var count: usize = 0;
    for (include_ids) |value| {
        if (!containsSortedU64(exclude_ids, value)) count += 1;
    }
    return count;
}

fn containsSortedU64(values: []const u64, needle: u64) bool {
    var low: usize = 0;
    var high: usize = values.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        if (values[mid] < needle) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    return low < values.len and values[low] == needle;
}

fn uniqueSortedU64(values: []u64) usize {
    if (values.len == 0) return 0;
    var out: usize = 1;
    for (values[1..]) |value| {
        if (value == values[out - 1]) continue;
        values[out] = value;
        out += 1;
    }
    return out;
}

fn isSortedUniqueU64(values: []const u64) bool {
    if (values.len < 2) return true;
    for (values[1..], values[0 .. values.len - 1]) |value, previous| {
        if (value <= previous) return false;
    }
    return true;
}

fn subtractSortedUniqueU64InPlace(values: []u64, excluded: []const u64) usize {
    var out: usize = 0;
    var excluded_index: usize = 0;
    for (values) |value| {
        while (excluded_index < excluded.len and excluded[excluded_index] < value) {
            excluded_index += 1;
        }
        if (excluded_index < excluded.len and excluded[excluded_index] == value) continue;
        values[out] = value;
        out += 1;
    }
    return out;
}

test "sorted unique vector id subtraction handles sparse and dense exclusions" {
    var normalized = try CandidateDifference.init(
        std.testing.allocator,
        &.{ 12, 1, 4, 2, 9, 7, 4, 2 },
        &.{ 14, 4, 0, 12, 3, 2, 8, 4 },
    );
    defer normalized.deinit();
    try std.testing.expectEqualSlices(u64, &.{ 1, 7, 9 }, normalized.values);

    var dense = try CandidateDifference.init(
        std.testing.allocator,
        &.{ 5, 4, 3, 2, 1, 0, 3 },
        &.{ 0, 1, 2, 3, 4 },
    );
    defer dense.deinit();
    try std.testing.expectEqualSlices(u64, &.{5}, dense.values);

    var empty = try CandidateDifference.init(std.testing.allocator, &.{ 2, 2 }, &.{ 2, 2 });
    defer empty.deinit();
    try std.testing.expectEqual(@as(usize, 0), empty.values.len);

    try std.testing.expectEqual(
        @as(?usize, 3),
        sortedUniqueDifferenceCount(&.{ 1, 2, 4, 7, 9, 12 }, &.{ 0, 2, 3, 4, 8, 12, 14 }),
    );
    try std.testing.expectEqual(@as(?usize, null), sortedUniqueDifferenceCount(&.{ 2, 1 }, &.{}));
    try std.testing.expectEqual(@as(?usize, null), sortedUniqueDifferenceCount(&.{ 1, 2 }, &.{ 1, 1 }));

    try std.testing.expectEqual(
        @as(?usize, 3),
        differenceCandidateUpperBound(&.{ 12, 1, 4, 2, 9, 7 }, &.{ 0, 2, 3, 4, 8, 12, 14 }),
    );
    // Duplicate survivors deliberately overestimate the unique difference.
    try std.testing.expectEqual(@as(?usize, 2), differenceCandidateUpperBound(&.{ 7, 7 }, &.{ 1, 2 }));
    try std.testing.expectEqual(@as(?usize, null), differenceCandidateUpperBound(&.{ 1, 2 }, &.{ 2, 1 }));
    try std.testing.expectEqual(@as(?usize, 3), differenceCandidateUpperBound(&.{ 3, 1, 3 }, &.{}));
    try std.testing.expectEqual(
        @as(usize, 2),
        differenceCandidateUpperBoundNormalizedExclusions(&.{ 9, 2, 7, 2 }, &.{ 1, 2, 3 }),
    );
}
