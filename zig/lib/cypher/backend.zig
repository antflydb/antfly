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

const std = @import("std");
const catalog = @import("catalog.zig");
const v = @import("value.zig");
const ast = @import("ast.zig");
pub fn Page(comptime T: type) type {
    return struct { items: []const T, next: ?[]const u8 = null, examined: ?usize = null };
}
pub const Seed = struct { node: *const v.Node, score: f64 };
pub const SearchInput = struct { parameters: v.Value, prepared_json: []const u8 = "", max_examined: usize = 1_000_000 };
pub const SearchPage = struct { seeds: []const Seed, examined: usize = 0, approximate: bool = false };
pub const StartupWork = struct { scanned_anchors: usize = 0, explored_edges: usize = 0 };
pub const Mutation = union(enum) {
    create_node: *const v.Node,
    create_relationship: *const v.Relationship,
    /// Null-valued properties remove the property, as in Cypher SET.
    set_properties: struct { target: v.EntityIdentity, properties: []const v.Entry, replace: bool },
    remove_properties: struct { target: v.EntityIdentity, names: []const []const u8 },
    delete_node: struct { identity: v.Identity, detach: bool },
    delete_relationship: v.RelationshipIdentity,
    pub fn validateProperties(self: Mutation) !void {
        const entries = switch (self) {
            .create_node => |node| node.properties,
            .create_relationship => |relationship| relationship.properties,
            .set_properties => |update| update.properties,
            else => return,
        };
        try v.validatePropertyValue(.{ .map = entries });
    }
};
/// All returned values are allocated in the supplied execution allocator and
/// remain valid through execution. Read callbacks enforce row/field policies
/// and share the statement's pinned visibility cut. A missing authorized node
/// is null; hidden nodes are not legal intermediate traversal endpoints.
pub const Backend = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    pub const VTable = struct {
        bind: *const fn (*anyopaque, std.mem.Allocator, catalog.Graph) anyerror!void,
        checkpoint: *const fn (*anyopaque) anyerror!void,
        scan: *const fn (*anyopaque, std.mem.Allocator, []const u8, ?[]const u8, usize) anyerror!Page(*const v.Node),
        get: *const fn (*anyopaque, std.mem.Allocator, v.Identity) anyerror!?*const v.Node,
        /// Resolve the current statement postimage of a retained relationship
        /// alias. Mutable adapters provide this even without read projection.
        get_relationship: ?*const fn (*anyopaque, std.mem.Allocator, v.RelationshipIdentity) anyerror!?*const v.Relationship = null,
        expand: *const fn (*anyopaque, std.mem.Allocator, catalog.Relationship, v.Identity, ast.Direction, ?[]const u8, usize) anyerror!Page(*const v.Relationship),
        search: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, SearchInput) anyerror!SearchPage = null,
        normalize_node: ?*const fn (*anyopaque, std.mem.Allocator, *const v.Node, bool) anyerror!*const v.Node = null,
        /// Mutable adapters retain full postimages for commit. Reads project
        /// them separately so WITH CHECK cannot grant USING visibility.
        project_postimage: ?*const fn (*anyopaque, std.mem.Allocator, *const v.Node) anyerror!?*const v.Node = null,
        project_node: ?*const fn (*anyopaque, std.mem.Allocator, *const v.Node) anyerror!?*const v.Node = null,
        /// Rebuild source-document contributions against a staged postimage.
        document_relationships: ?*const fn (*anyopaque, std.mem.Allocator, *const v.Node) anyerror![]const *const v.Relationship = null,
        /// Mutation adapters must support read-your-writes, transactional MERGE
        /// uniqueness, and atomic rollback of every staged mutation on failure.
        begin: ?*const fn (*anyopaque) anyerror!void = null,
        startup_work: ?*const fn (*anyopaque) StartupWork = null,
        mutate: ?*const fn (*anyopaque, std.mem.Allocator, Mutation) anyerror!void = null,
        commit: ?*const fn (*anyopaque) anyerror!void = null,
        rollback: ?*const fn (*anyopaque) void = null,
        new_identity: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, bool) anyerror!v.Identity = null,
    };
};
