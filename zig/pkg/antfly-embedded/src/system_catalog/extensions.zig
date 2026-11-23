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

//! Durable database-scoped extension identity and typed callable membership.
const std = @import("std");
const domain = @import("domain.zig");
const records = @import("routines.zig");
const A = std.mem.Allocator;
pub const max_members = 256;
pub const max_installed = 128;

pub const Member = struct {
    id: u64,
    generation: u32,
    name: []const u8,
    signature: []const u8,
};
pub const Dependency = struct { id: u64, name: []const u8 };
pub const Record = struct {
    version: u16 = 1,
    id: u64,
    generation: u64,
    database_id: u64,
    namespace_id: u64,
    package_name: []const u8,
    package_version: []const u8,
    package_digest: [32]u8,
    relocatable: bool,
    owner: @FieldType(records.Definition, "owner"),
    members: []const Member,
    dependencies: []const Dependency,

    pub fn validate(self: @This()) !void {
        if (self.version != 1 or self.id == 0 or self.generation == 0 or self.database_id == 0 or self.namespace_id == 0 or std.mem.allEqual(u8, &self.package_digest, 0) or self.members.len == 0 or self.members.len > max_members or self.dependencies.len > max_installed) return error.InvalidSqlExtensionRecord;
        try domain.validateName(self.package_name);
        if (self.package_version.len == 0 or self.package_version.len > 128 or std.mem.indexOfScalar(u8, self.package_version, 0) != null) return error.InvalidSqlExtensionRecord;
        // Reuse the callable principal validator; no mutable display name can
        // stand in for the owner's immutable user/role instance.
        try (records.Definition{ .arguments = &.{}, .result = .{ .scalar = .int64 }, .body = "SELECT 1", .owner = self.owner }).validate();
        for (self.members, 0..) |member, i| {
            if (member.id == 0 or member.generation == 0 or member.signature.len > 4096 or std.mem.indexOfScalar(u8, member.signature, 0) != null) return error.InvalidSqlExtensionRecord;
            try domain.validateTableName(member.name);
            for (self.members[0..i]) |prior| if (prior.id == member.id or (std.mem.eql(u8, prior.name, member.name) and std.mem.eql(u8, prior.signature, member.signature))) return error.InvalidSqlExtensionRecord;
        }
        for (self.dependencies, 0..) |dependency, i| {
            if (dependency.id == 0 or dependency.id == self.id) return error.InvalidSqlExtensionRecord;
            try domain.validateName(dependency.name);
            for (self.dependencies[0..i]) |prior| if (prior.id == dependency.id or std.mem.eql(u8, prior.name, dependency.name)) return error.InvalidSqlExtensionRecord;
        }
    }
    pub fn encode(self: @This(), a: A) ![]const u8 {
        try self.validate();
        return std.json.Stringify.valueAlloc(a, self, .{});
    }
    pub fn decode(a: A, resource: domain.Resource) !Record {
        if (resource.kind != .extension) return error.InvalidSqlExtensionRecord;
        const result = try std.json.parseFromSliceLeaky(Record, a, resource.source_json, .{ .allocate = .alloc_always });
        try result.validate();
        if (result.id != resource.id or result.database_id != resource.parent_id or !std.mem.eql(u8, result.package_name, resource.name)) return error.InvalidSqlExtensionRecord;
        return result;
    }
};

pub const Visit = enum { visiting, done };
/// Shared whole-state restore validation. Memoized traversal is linear in
/// installed extensions and edges; a diamond cannot cause exponential work.
pub fn requireAcyclic(a: A, index: anytype, id: u64, visited: *std.AutoHashMapUnmanaged(u64, Visit), depth: usize) anyerror!void {
    const entry = try visited.getOrPut(a, id);
    if (entry.found_existing) {
        if (entry.value_ptr.* == .visiting) return error.InvalidSqlExtensionRecord;
        return;
    }
    if (depth >= max_installed) return error.InvalidSqlExtensionRecord;
    entry.value_ptr.* = .visiting;
    var scratch: std.heap.ArenaAllocator = .init(a);
    defer scratch.deinit();
    const resource = index.byId(.extension, id) orelse return error.InvalidSqlExtensionRecord;
    const record = try Record.decode(scratch.allocator(), resource);
    for (record.dependencies) |dependency| try requireAcyclic(a, index, dependency.id, visited, depth + 1);
    visited.getPtr(id).?.* = .done;
}
pub fn sameOwner(left: @FieldType(records.Definition, "owner"), right: @FieldType(records.Definition, "owner")) bool {
    if (std.meta.activeTag(left) != std.meta.activeTag(right)) return false;
    return switch (left) {
        .auth_disabled => true,
        .user => |owner| std.mem.eql(u8, &owner.instance_id, &right.user.instance_id),
        .role => |owner| std.mem.eql(u8, &owner.instance_id, &right.role.instance_id),
    };
}
