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

//! Self-contained LIKE inputs and annotation postimages for hidden FK CREATE.
//! Native BEGIN revalidates sources and independently derives the entire image.
const std = @import("std");
const local = @import("antfly_local_sources");
const domain = local.system_catalog_domain;
const objects = domain.object_annotations;
const object_store = domain.object_annotation_store;
const A = std.mem.Allocator;

pub const Input = struct {
    revision: u64,
    sources: []const domain.RelationMutationGuard,
    column_comments: []const u64 = &.{},
    object_comments: []const objects.Copy = &.{},

    pub fn validate(self: Input) !void {
        if (self.sources.len == 0) return error.InvalidGenerationPublication;
        try domain.validateCreationSources(self.sources);
        try domain.validateCreationCommentSources(self.sources, self.column_comments);
        try domain.validateCreationObjectComments(self.sources, self.object_comments);
    }
};

pub const Note = struct { object: u64, value: []const u8 };
pub const Effects = struct {
    columns: ?[]const u8 = null,
    directory: ?[]const u8 = null,
    notes: []const Note = &.{},

    /// Check standalone recovery bytes against fresh destination identities.
    /// Source notes may have changed or disappeared after accepted admission.
    pub fn validate(self: Effects, backing: A, input: Input, child: local.common_topology_records.TableRecord) !void {
        try input.validate();
        if ((input.column_comments.len != 0) != (self.columns != null) or
            (input.object_comments.len != 0) != (self.directory != null) or
            self.notes.len > input.object_comments.len) return error.InvalidGenerationPublication;
        var region = std.heap.ArenaAllocator.init(backing);
        defer region.deinit();
        const a = region.allocator();
        if (self.columns) |raw| {
            var actual = try local.system_catalog_column_identities.State.decode(a, raw);
            defer actual.deinit();
            var expected = try @import("column_identity_definition.zig").initial(a, child);
            defer expected.deinit();
            if (actual.value.columns.len != expected.value.columns.len) return error.InvalidGenerationPublication;
            const columns = try a.dupe(local.system_catalog_column_identities.Column, actual.value.columns);
            for (columns) |*column| column.description = null;
            var normalized = actual.value;
            normalized.columns = columns;
            if (!std.mem.eql(u8, try normalized.encode(a), try expected.value.encode(a))) return error.InvalidGenerationPublication;
        }
        if (self.directory) |raw| {
            const Empty = struct {
                pub fn get(_: *@This(), _: []const u8) anyerror![]const u8 {
                    return error.NotFound;
                }
            };
            var empty: Empty = .{};
            var expected = (try object_store.prepareTableChange(a, &empty, 1, child.table_id, null, .{ .schema_json = child.schema_json, .indexes_json = child.indexes_json }, &.{})) orelse return error.InvalidGenerationPublication;
            defer expected.deinit();
            if (!std.mem.eql(u8, raw, expected.directory)) return error.InvalidGenerationPublication;
            var directory = try objects.State.decode(a, raw);
            defer directory.deinit();
            var seen: std.AutoHashMapUnmanaged(u64, void) = .empty;
            for (self.notes) |note| {
                var annotation = try objects.Annotation.decode(a, note.value);
                defer annotation.deinit();
                if (annotation.value.table_id != child.table_id or annotation.value.object_id != note.object or
                    (try seen.getOrPut(a, note.object)).found_existing) return error.InvalidGenerationPublication;
                const exists = for (directory.value.objects) |object| {
                    if (object.id == note.object) break true;
                } else false;
                if (!exists) return error.InvalidGenerationPublication;
            }
        } else if (self.notes.len != 0) return error.InvalidGenerationPublication;
    }

    pub fn write(self: Effects, txn: anytype, group: u64, table: u64) !void {
        if (self.columns) |raw| try txn.put(&try local.system_catalog_column_identities.key(.{ .group = group, .table = table }), raw);
        if (self.directory) |raw| try txn.put(&try objects.directoryKey(group, table), raw);
        for (self.notes) |note| try txn.put(&try objects.annotationKey(group, table, note.object), note.value);
    }
};

/// Bounded value comparison; every allocation is reclaimed on both outcomes.
pub fn equal(a: A, left: anytype, right: @TypeOf(left)) !bool {
    const lhs = try std.json.Stringify.valueAlloc(a, left, .{});
    defer a.free(lhs);
    const rhs = try std.json.Stringify.valueAlloc(a, right, .{});
    defer a.free(rhs);
    return std.mem.eql(u8, lhs, rhs);
}
