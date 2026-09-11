// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

const abi = @import("kernel_owner_abi");
const types = @import("db/types.zig");

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
    }
}
