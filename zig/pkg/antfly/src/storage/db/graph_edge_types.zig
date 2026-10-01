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

const std = @import("std");

pub const GraphEdgeWrite = struct {
    edge_id: []const u8 = "",
    owner_document: []const u8 = "",
    index_name: []const u8,
    source: []const u8,
    target: []const u8,
    edge_type: []const u8,
    weight: f64 = 1.0,
    created_at: u64 = 0,
    updated_at: u64 = 0,
    metadata_json: []const u8 = "",

    /// Owned fields retained by projection pages, including the complete
    /// relationship identity. Saturation makes overflow exceed any page budget.
    pub fn retainedBytes(self: @This()) usize {
        var bytes: usize = @sizeOf(@This());
        inline for (identity_fields) |field| bytes +|= @field(self, field).len;
        return bytes +| self.metadata_json.len;
    }

    pub fn cloneAlloc(self: @This(), alloc: std.mem.Allocator) !@This() {
        return cloneMutationAlloc(alloc, self);
    }

    /// Release an owned mutation returned by cloneAlloc.
    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeOwnedMutation(alloc, self.*);
        self.* = undefined;
    }
};

pub const GraphEdgeDelete = struct {
    edge_id: []const u8 = "",
    owner_document: []const u8 = "",
    index_name: []const u8,
    source: []const u8,
    target: []const u8,
    edge_type: []const u8,
    pub fn cloneAlloc(self: @This(), alloc: std.mem.Allocator) !@This() {
        return cloneMutationAlloc(alloc, self);
    }

    /// Release an owned mutation returned by cloneAlloc.
    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        freeOwnedMutation(alloc, self.*);
        self.* = undefined;
    }
};

const identity_fields = [_][]const u8{ "index_name", "source", "target", "edge_type", "edge_id", "owner_document" };

fn freeOwnedMutation(alloc: std.mem.Allocator, mutation: anytype) void {
    inline for (identity_fields) |field| alloc.free(@field(mutation, field));
    if (@hasField(@TypeOf(mutation), "metadata_json")) alloc.free(mutation.metadata_json);
}

fn cloneMutationAlloc(alloc: std.mem.Allocator, mutation: anytype) !@TypeOf(mutation) {
    var result = mutation;
    inline for (identity_fields) |field| @field(result, field) = "";
    if (@hasField(@TypeOf(mutation), "metadata_json")) result.metadata_json = "";
    errdefer freeOwnedMutation(alloc, result);
    inline for (identity_fields) |field| @field(result, field) = try alloc.dupe(u8, @field(mutation, field));
    if (@hasField(@TypeOf(mutation), "metadata_json")) result.metadata_json = try alloc.dupe(u8, mutation.metadata_json);
    return result;
}

test "graph mutation clones release partial relationship allocations" {
    const Case = struct {
        fn run(alloc: std.mem.Allocator) !void {
            const write = GraphEdgeWrite{
                .index_name = "facts",
                .source = "a",
                .target = "b",
                .edge_type = "R",
                .edge_id = "fact:1",
                .owner_document = "fact:1",
                .metadata_json = "{}",
            };
            var copy = try write.cloneAlloc(alloc);
            defer copy.deinit(alloc);
            try std.testing.expectEqualStrings(write.edge_id, copy.edge_id);
            var deletion = try (GraphEdgeDelete{
                .index_name = write.index_name,
                .source = write.source,
                .target = write.target,
                .edge_type = write.edge_type,
                .edge_id = write.edge_id,
                .owner_document = write.owner_document,
            }).cloneAlloc(alloc);
            defer deletion.deinit(alloc);
            try std.testing.expectEqualStrings(write.owner_document, deletion.owner_document);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}
