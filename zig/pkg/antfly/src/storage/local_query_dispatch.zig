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

//! Storage-free dispatch of complete owner-issued query operations. No private
//! DB, index, model, parser implementation or storage descriptor imports.
const std = @import("std");
const abi = @import("local_query_abi");
const identity = @import("kernel_error_identity");

pub fn execute(request: *const abi.LocalQueryRequest, out_response: *abi.QueryOwnedResponse, out_failure: *abi.FailureIdentity) callconv(.c) abi.Status {
    out_response.* = .{};
    out_failure.* = .{};
    if (request.version != abi.abi_version) return fail(error.InvalidAbiVersion, out_failure);
    request.execution.validate() catch |err| return fail(err, out_failure);
    const callback = request.execution.execute_fn.?;
    return callback(request.execution.context, request, out_response, out_failure);
}

fn fail(err: anyerror, out_failure: *abi.FailureIdentity) abi.Status {
    out_failure.* = identity.failureFromError(err, .local_query, abi.abi_version, @backingInt(abi.LocalQueryOperation.validate_request));
    return out_failure.status;
}

pub fn bufferDestroy(buffer: *abi.OwnedBytes) callconv(.c) void {
    if (buffer.ptr) |ptr| std.heap.c_allocator.free(ptr[0..@intCast(buffer.len)]);
    buffer.* = .{};
}
