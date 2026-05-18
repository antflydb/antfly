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
const types = @import("types.zig");
const search_results = @import("search_results.zig");
const search_types = @import("search_types.zig");

pub const BeamSearchState = struct {
    leaves_explored: u32 = 0,
    dynamic_pruning_min: f32 = std.math.inf(f32),
};

pub fn rerankFactor(epsilon: f32) usize {
    return @max(1, @as(usize, @intFromFloat(@ceil(epsilon + 1.0))));
}

pub fn candidateCapacity(search_width: u32, branching_factor: u32) usize {
    return @max(
        @as(usize, 1),
        @min(
            @as(usize, 1024),
            @max(
                @as(usize, @intCast(search_width * 4)),
                @as(usize, @intCast(branching_factor * 2)),
            ),
        ),
    );
}

pub fn shouldStopBeamSearch(state: *const BeamSearchState, search_width: u32) bool {
    return state.leaves_explored >= search_width;
}

pub fn shouldBreakOnInternalCandidate(
    candidate: types.PriorityItem,
    approx_results: *const search_results.ApproxSearchResults,
) bool {
    _ = candidate;
    _ = approx_results;
    return false;
}

pub fn shouldSkipInternalCandidate(
    candidate: types.PriorityItem,
    approx_results: *const search_results.ApproxSearchResults,
    state: *const BeamSearchState,
    epsilon: f32,
) bool {
    if (!approx_results.isFull()) return false;
    if (state.dynamic_pruning_min >= std.math.inf(f32)) return false;
    return candidate.distance > state.dynamic_pruning_min * (epsilon + 1.0);
}

pub fn shouldSkipLeafCandidate(
    candidate: types.PriorityItem,
    approx_results: *const search_results.ApproxSearchResults,
    state: *BeamSearchState,
    epsilon: f32,
) bool {
    if (!approx_results.isFull()) return false;
    if (candidate.distance < state.dynamic_pruning_min) {
        state.dynamic_pruning_min = candidate.distance;
        return false;
    }
    return @abs(candidate.distance) > @abs(state.dynamic_pruning_min * (epsilon + 1.0));
}

pub fn noteLeafExplored(state: *BeamSearchState) void {
    state.leaves_explored += 1;
}

pub fn collectCompetitiveCandidatesAlloc(
    alloc: Allocator,
    child_ids: []const u64,
    distances: []const f32,
    error_bounds: []const f32,
) ![]types.PriorityItem {
    var competitive = std.ArrayListUnmanaged(types.PriorityItem).empty;
    errdefer competitive.deinit(alloc);
    try competitive.ensureTotalCapacity(alloc, child_ids.len);

    outer: for (child_ids, 0..) |child_id, i| {
        const candidate = types.PriorityItem{
            .id = child_id,
            .distance = distances[i],
            .error_bound = error_bounds[i],
        };
        if (competitive.items.len == 0) {
            competitive.appendAssumeCapacity(candidate);
            continue;
        }

        while (true) {
            const worst_idx = worstCompetitiveIndex(competitive.items);
            const worst = competitive.items[worst_idx];
            if (!candidate.definitelyCloser(worst)) break;
            _ = competitive.swapRemove(worst_idx);
            if (competitive.items.len == 0) {
                competitive.appendAssumeCapacity(candidate);
                continue :outer;
            }
        }

        const worst_idx = worstCompetitiveIndex(competitive.items);
        if (candidate.maybeCloser(competitive.items[worst_idx])) {
            competitive.appendAssumeCapacity(candidate);
        }
    }

    return competitive.toOwnedSlice(alloc);
}

fn worstCompetitiveIndex(items: []const types.PriorityItem) usize {
    var worst_idx: usize = 0;
    for (items[1..], 1..) |item, idx| {
        if (item.distance > items[worst_idx].distance) worst_idx = idx;
    }
    return worst_idx;
}

pub fn sortApproxResultsByVectorId(items: []search_results.ApproxSearchResult) void {
    std.mem.sort(search_results.ApproxSearchResult, items, {}, struct {
        fn lessThan(_: void, a: search_results.ApproxSearchResult, b: search_results.ApproxSearchResult) bool {
            return a.vector_id < b.vector_id;
        }
    }.lessThan);
}

pub fn sortApproxResultsByDistance(items: []search_results.ApproxSearchResult) void {
    std.mem.sort(search_results.ApproxSearchResult, items, {}, struct {
        fn lessThan(_: void, a: search_results.ApproxSearchResult, b: search_results.ApproxSearchResult) bool {
            if (a.distance != b.distance) return a.distance < b.distance;
            return a.vector_id < b.vector_id;
        }
    }.lessThan);
}

pub fn sortSearchResultsByDistance(items: []search_results.SearchResult) void {
    std.mem.sort(search_results.SearchResult, items, {}, struct {
        fn lessThan(_: void, a: search_results.SearchResult, b: search_results.SearchResult) bool {
            if (a.distance != b.distance) return a.distance < b.distance;
            return a.vector_id < b.vector_id;
        }
    }.lessThan);
}

pub fn sortDebugLeafScores(items: []search_types.DebugLeafScore) void {
    std.mem.sort(search_types.DebugLeafScore, items, {}, struct {
        fn lessThan(_: void, a: search_types.DebugLeafScore, b: search_types.DebugLeafScore) bool {
            if (a.approx_distance != b.approx_distance) return a.approx_distance < b.approx_distance;
            return a.vector_id < b.vector_id;
        }
    }.lessThan);
}

test "internal candidate pruning matches Go inner-product semantics" {
    var approx_results = try search_results.ApproxSearchResults.initCapacity(std.testing.allocator, 1, 1, 1);
    defer approx_results.deinit();
    approx_results.addResult(1, -10.0, 0);

    const state = BeamSearchState{
        .leaves_explored = 1,
        .dynamic_pruning_min = -10.0,
    };

    try std.testing.expect(!shouldBreakOnInternalCandidate(.{
        .id = 2,
        .distance = -8.0,
        .error_bound = 0.5,
    }, &approx_results));

    try std.testing.expect(shouldSkipInternalCandidate(.{
        .id = 2,
        .distance = -8.0,
        .error_bound = 0,
    }, &approx_results, &state, 0.1));

    try std.testing.expect(!shouldSkipInternalCandidate(.{
        .id = 3,
        .distance = -11.5,
        .error_bound = 0,
    }, &approx_results, &state, 0.1));
}
