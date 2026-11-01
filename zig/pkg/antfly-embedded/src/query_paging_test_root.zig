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

//! Pure value-plan tests; the production control adapter calls this same code.
const std = @import("std");
const plan = @import("storage/db/query/paging_plan.zig");

test "query paging plan preserves uncomposed offsets and saturates windows" {
    const values = [_]u32{ 0, 1, 10, 100, std.math.maxInt(u32) - 1, std.math.maxInt(u32) };
    for (values) |offset| for (values) |limit| {
        const input: plan.Input = .{ .offset = offset, .limit = limit };
        const result = plan.componentPaging(input);
        try std.testing.expectEqual(offset, result.offset);
        try std.testing.expectEqual(limit, result.limit);
        try std.testing.expectEqual(offset +| limit, plan.candidateWindow(input));
    };
}

test "query paging plan resets offsets for fusion and pruning and retains the original window" {
    const values = [_]u32{ 0, 1, 10, 100, std.math.maxInt(u32) };
    for (values) |offset| for (values) |limit| for (values) |window| {
        const fused = plan.componentPaging(.{ .offset = offset, .limit = limit, .merge_window = window });
        try std.testing.expectEqual(@as(u32, 0), fused.offset);
        try std.testing.expectEqual(@max(offset +| limit, window), fused.limit);
        const pruned = plan.componentPaging(.{ .offset = offset, .limit = limit, .has_pruner = true });
        try std.testing.expectEqual(@as(u32, 0), pruned.offset);
        try std.testing.expectEqual(offset +| limit, pruned.limit);
    };
}

test "query paging plan distinguishes explicit candidate ceilings from top-n plus offset" {
    try std.testing.expectEqual(@as(u32, 40), plan.candidateWindow(.{ .offset = 20, .limit = 10, .reranking = .{ .candidate_count = 40, .top_n = 80 } }));
    try std.testing.expectEqual(@as(u32, 100), plan.candidateWindow(.{ .offset = 20, .limit = 10, .reranking = .{ .top_n = 80 } }));
    try std.testing.expectEqual(@as(u32, 30), plan.candidateWindow(.{ .offset = 20, .limit = 10, .reranking = .{} }));
    try std.testing.expectEqual(@as(u32, 200), plan.candidateWindow(.{ .offset = 20, .limit = 10, .merge_window = 200, .reranking = .{ .candidate_count = 40 } }));
    try std.testing.expectEqual(std.math.maxInt(u32), plan.candidateWindow(.{ .offset = 20, .limit = 10, .reranking = .{ .top_n = std.math.maxInt(u32) } }));
}

test "query paging plan declines unsupported decomposition before checking budgets" {
    const blocked = [_]plan.DomainInput{
        .{ .paging = .{ .limit = 100 }, .vector_components = 0 },
        .{ .paging = .{ .limit = 100 }, .vector_components = 2, .legacy_knn = true },
        .{ .paging = .{ .limit = 100 }, .vector_components = 2, .expands = true },
        .{ .paging = .{ .limit = 100 }, .vector_components = 2, .unit_hierarchy = true },
    };
    for (blocked) |input| try std.testing.expect((try plan.aggregationDomain(input, 1)) == .unchanged);
}

test "query paging plan enforces exact domain budget and keeps zero windows" {
    try std.testing.expectEqual(@as(u32, 30), (try plan.aggregationDomain(.{ .paging = .{ .limit = 20, .offset = 10 }, .vector_components = 2 }, 30)).decomposed);
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, plan.aggregationDomain(.{ .paging = .{ .limit = 20, .offset = 10 }, .vector_components = 2 }, 29));
    try std.testing.expectEqual(@as(u32, 0), (try plan.aggregationDomain(.{ .paging = .{ .limit = 0 }, .vector_components = 2 }, 0)).decomposed);
    try std.testing.expect((try plan.aggregationDomain(.{ .paging = .{}, .vector_components = 1 }, plan.default_aggregation_full_result_budget)) == .decomposed);
}
