// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

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
