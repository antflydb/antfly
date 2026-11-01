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

//! Concrete query-window and aggregation-domain plans. This module has no DB,
//! schema, model, wire parser, source catalog or execution-provider imports.
pub const default_aggregation_full_result_budget: u32 = 100_000;
pub const Paging = struct { offset: u32, limit: u32 };
pub const RerankingWindow = struct { candidate_count: ?u32 = null, top_n: ?u32 = null };
pub const Input = struct {
    offset: u32 = 0,
    limit: u32 = 10,
    merge_window: ?u32 = null,
    reranking: ?RerankingWindow = null,
    has_pruner: bool = false,
};

pub fn componentPaging(input: Input) Paging {
    var limit = if (input.reranking) |reranker|
        reranker.candidate_count orelse (reranker.top_n orelse input.limit) +| input.offset
    else
        input.limit +| input.offset;
    if (input.merge_window == null and input.reranking == null and !input.has_pruner)
        return .{ .offset = input.offset, .limit = input.limit };
    if (input.merge_window) |window| limit = @max(limit, window);
    if (input.reranking) |reranker| {
        const window = reranker.candidate_count orelse (reranker.top_n orelse input.limit) +| input.offset;
        limit = @max(limit, window);
    }
    return .{ .offset = 0, .limit = limit };
}

pub fn candidateWindow(input: Input) u32 {
    const page = componentPaging(input);
    return page.offset +| page.limit;
}

pub const DomainInput = struct {
    paging: Input,
    vector_components: usize,
    legacy_knn: bool = false,
    expands: bool = false,
    unit_hierarchy: bool = false,
};
pub const Domain = union(enum) { unchanged, decomposed: u32 };
pub fn aggregationDomain(input: DomainInput, budget: u32) !Domain {
    if (input.vector_components == 0 or input.legacy_knn or input.expands or input.unit_hierarchy)
        return .unchanged;
    const window = candidateWindow(input.paging);
    if (window > budget) return error.QueryCandidateBudgetExceeded;
    return .{ .decomposed = window };
}
