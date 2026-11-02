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

const abi = @import("kernel_owner_abi");
const types = @import("db/types.zig");

pub fn executionOptions(req: types.SearchRequest) abi.LocalQueryExecutionOptions {
    return .{
        .enabled = 1,
        .include_stored = @intFromBool(req.include_stored),
        .return_mode = switch (req.return_mode) {
            .parent => .parent,
            .chunk => .chunk,
            .parent_with_chunks => .parent_with_chunks,
            .unit => .unit,
            .unit_with_chunks => .unit_with_chunks,
            .member => .member,
        },
        .max_chunks_per_parent = req.max_chunks_per_parent,
        .response_table_name = if (req.response_table_name) |name| .fromSlice(name) else .{},
    };
}

pub fn applyExecutionOptions(req: *types.SearchRequest, options: abi.LocalQueryExecutionOptions) void {
    if (options.enabled != 0) {
        req.include_stored = options.include_stored != 0;
        req.return_mode = switch (options.return_mode) {
            .parent => .parent,
            .chunk => .chunk,
            .parent_with_chunks => .parent_with_chunks,
            .unit => .unit,
            .unit_with_chunks => .unit_with_chunks,
            .member => .member,
        };
        req.max_chunks_per_parent = options.max_chunks_per_parent;
        req.response_table_name = if (options.response_table_name.ptr != null) options.response_table_name.slice() else null;
    }
}

/// Apply trusted invocation controls without changing the parsed query. A
/// serialized deadline may tighten the caller's deadline but never extend it.
/// Authority and cancellation are accepted only from the synchronous caller.
pub fn applyTrustedControls(req: *types.SearchRequest, controls: types.SearchRequest) void {
    if (controls.execution_deadline_ns) |deadline| {
        req.execution_deadline_ns = if (req.execution_deadline_ns) |parsed| @min(parsed, deadline) else deadline;
    }
    req.execution_runnable = controls.execution_runnable;
    req.cancellation = controls.cancellation;
}

/// Native scoring holds index read locks. This borrowed callback must not
/// suspend or acquire CPU, and retains the owner's original clock authority.
pub fn nativeTextAuthority(req: *const types.SearchRequest) @import("../search/work_authority.zig").Authority {
    return .{ .context = req, .check_fn = checkNativeTextLifetime };
}

fn checkNativeTextLifetime(raw: *const anyopaque) !void {
    const req: *const types.SearchRequest = @ptrCast(@alignCast(raw));
    if (req.execution_runnable) |grant| {
        try grant.checkLifetime();
    } else if (req.execution_deadline_ns) |deadline| {
        if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.Timeout;
    }
    if (req.cancellation) |cancellation| {
        if (cancellation.isCancelled()) return error.Cancelled;
    }
}
