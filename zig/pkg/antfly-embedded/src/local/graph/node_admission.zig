// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");

pub const NodeRef = struct {
    key: []const u8,
    /// Target document table when the edge crosses a table boundary. The
    /// slice is borrowed for the duration of `filter_many`.
    table: ?[]const u8 = null,
    /// External nodes are authorized and hydrated by their target-table read
    /// path; they must not be looked up in the source table's identity store.
    external: bool = false,
};

/// Query-scoped admission policy for graph nodes. Implementations should batch
/// backing-store work and may cache decisions for the lifetime of one query.
pub const NodeAdmission = struct {
    ctx: ?*anyopaque,
    /// Artifact mappings with external targets do not have source-table
    /// identity rows for edge targets.
    external_targets: bool = false,
    /// Returns a mask allocated with `alloc`, including for an empty input.
    filter_many: *const fn (
        ctx: ?*anyopaque,
        alloc: std.mem.Allocator,
        nodes: []const NodeRef,
    ) anyerror![]bool,

    pub fn filterAlloc(
        self: NodeAdmission,
        alloc: std.mem.Allocator,
        nodes: []const NodeRef,
    ) ![]bool {
        const mask = try self.filter_many(self.ctx, alloc, nodes);
        if (mask.len != nodes.len) {
            alloc.free(mask);
            return error.InvalidGraphNodeAdmissionResult;
        }
        return mask;
    }

    pub fn filterLocalKeysAlloc(
        self: NodeAdmission,
        alloc: std.mem.Allocator,
        keys: []const []const u8,
    ) ![]bool {
        return try self.filterKeysAlloc(alloc, keys, false);
    }

    pub fn filterKeysAlloc(
        self: NodeAdmission,
        alloc: std.mem.Allocator,
        keys: []const []const u8,
        external: bool,
    ) ![]bool {
        const nodes = try alloc.alloc(NodeRef, keys.len);
        defer alloc.free(nodes);
        for (keys, 0..) |key, i| nodes[i] = .{
            .key = key,
            .table = null,
            .external = external,
        };
        return try self.filterAlloc(alloc, nodes);
    }
};
