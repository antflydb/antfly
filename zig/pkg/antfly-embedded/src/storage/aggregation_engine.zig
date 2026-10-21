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

//! Aggregation-only native contract. No physical DB source capability is needed
//! by a coordinator folding already selected rows or distributed statistics.
const std = @import("std");
const aggregates = @import("db/aggregations.zig");
const types = @import("db/types.zig");
const runtime = @import("antfly_runtime_abi");
const native = runtime.native_abi;
const outcome = @import("native_outcome.zig");
pub const Method = *const fn (std.mem.Allocator, []const aggregates.SearchAggregationRequest, types.SearchResult, aggregates.Context) outcome.Outcome([]aggregates.SearchAggregationResult);
const VTable = struct { computeSearchAggregations: Method };
pub const Boundary = runtime.callback_abi.Boundary(VTable);
pub const Descriptor = extern struct {
    version: u32 = 1,
    method_layout: native.TypeContract,
    context_layout: native.TypeContract,
    index_manager_layout: native.TypeContract,
    docstore_layout: native.TypeContract,
    dispatch: runtime.callback_abi.CallbackDispatch,
};
pub const method_layout = native.TypeContract.of(Method);
pub const context_layout = native.TypeContract.of(aggregates.Context);
pub const index_manager_layout = blk: {
    @setEvalBranchQuota(1_000_000);
    break :blk native.TypeContract.of(@import("db/catalog/index_manager.zig").IndexManager);
};
pub const docstore_layout = blk: {
    @setEvalBranchQuota(1_000_000);
    break :blk native.TypeContract.of(@import("docstore.zig").DocStore);
};
extern fn antfly_storage_engine_get_aggregation_descriptor_v1() callconv(.c) *const Descriptor;
extern fn antfly_storage_engine_computeSearchAggregations_v29() callconv(.c) *const anyopaque;
pub fn compute(alloc: std.mem.Allocator, requests: []const aggregates.SearchAggregationRequest, result: types.SearchResult, context: aggregates.Context) ![]aggregates.SearchAggregationResult {
    const descriptor = antfly_storage_engine_get_aggregation_descriptor_v1();
    if (descriptor.version != 1 or !descriptor.method_layout.matches(method_layout) or
        !descriptor.context_layout.matches(context_layout) or
        (context.index_manager != null and !descriptor.index_manager_layout.matches(index_manager_layout)) or
        (context.doc_store != null and !descriptor.docstore_layout.matches(docstore_layout))) return error.UnsupportedVersion;
    const method: Method = @ptrCast(@alignCast(antfly_storage_engine_computeSearchAggregations_v29()));
    const value = try Boundary.call("computeSearchAggregations", descriptor.dispatch, method, .{ alloc, requests, result, context });
    if (value.failure.ptr) |ptr| return @import("storage_engine_errors").fromName(ptr[0..value.failure.len]);
    return value.value;
}
