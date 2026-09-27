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

//! Owned JSON response passed across table runtime callback boundaries.

const std = @import("std");
const graph_wire_envelope = @import("graph_wire_envelope.zig");

pub const QueryResponse = struct {
    /// Internal group-query response header used to carry the storage snapshot
    /// selected by the shard. This stays outside the public query JSON contract.
    pub const identity_read_generation_header = "X-Antfly-Identity-Read-Generation";

    json: []u8,
    identity_read_generation: ?u64 = null,
    /// Dialect admitted at the public graph boundary. This is transport
    /// metadata only: graph execution and storage always use the canonical IR.
    graph_dialect: ?graph_wire_envelope.Dialect = null,

    pub fn deinit(self: *QueryResponse, alloc: std.mem.Allocator) void {
        alloc.free(self.json);
        self.* = undefined;
    }
};
