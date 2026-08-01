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
}
