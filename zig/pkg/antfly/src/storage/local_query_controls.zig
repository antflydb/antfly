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
