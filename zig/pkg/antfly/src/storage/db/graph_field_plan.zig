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

//! Field-derived graph writes. Callers hold the catalog/schema fence;
//! this owner stages complete results before changing the extracted write.
const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const mapper = @import("document_mapper.zig");
const appendUniqueOwnedKey = @import("owned_keys.zig").appendUniqueOwnedKey;

const Builder = struct {
    alloc: Allocator,
    writes: std.ArrayListUnmanaged(types.GraphEdgeWrite) = .empty,
    indexes: std.ArrayListUnmanaged([]u8) = .empty,
    adopted: bool = false,

    fn deinit(self: *Builder) void {
        if (!self.adopted) {
            for (self.writes.items) |*write| write.deinit(self.alloc);
            for (self.indexes.items) |name| self.alloc.free(name);
        }
        self.writes.deinit(self.alloc);
        self.indexes.deinit(self.alloc);
    }
    fn field(self: *Builder, root: std.json.ObjectMap, key: []const u8, index: []const u8, edge: []const u8, field_name: []const u8) !void {
        var targets = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (targets.items) |target| self.alloc.free(target);
            targets.deinit(self.alloc);
        }
        try appendGraphFieldTargets(self.alloc, &targets, root, field_name);
        for (targets.items) |target| try appendOwnedGraphFieldWrite(self.alloc, &self.writes, index, key, target, edge);
    }
    fn mention(self: *Builder, extracted: *const mapper.ExtractedWrite, index: []const u8) !void {
        for (extracted.mentioned_graph_indexes) |name| if (std.mem.eql(u8, name, index)) return;
        try appendUniqueOwnedKey(self.alloc, &self.indexes, index);
    }
    fn publish(self: *Builder, extracted: *mapper.ExtractedWrite) !void {
        const writes = if (self.writes.items.len > 0)
            try self.alloc.alloc(types.GraphEdgeWrite, extracted.graph_writes.len + self.writes.items.len)
        else
            null;
        errdefer if (writes) |items| self.alloc.free(items);
        const indexes = if (self.indexes.items.len > 0)
            try self.alloc.alloc([]u8, extracted.mentioned_graph_indexes.len + self.indexes.items.len)
        else
            null;
        // All fallible work is complete. Only the outer old arrays retire;
        // their elements and the builder's elements move into the new arrays.
        if (writes) |items| {
            @memcpy(items[0..extracted.graph_writes.len], extracted.graph_writes);
            @memcpy(items[extracted.graph_writes.len..], self.writes.items);
            if (extracted.graph_writes.len > 0) self.alloc.free(extracted.graph_writes);
            extracted.graph_writes = items;
        }
        if (indexes) |items| {
            @memcpy(items[0..extracted.mentioned_graph_indexes.len], extracted.mentioned_graph_indexes);
            @memcpy(items[extracted.mentioned_graph_indexes.len..], self.indexes.items);
            if (extracted.mentioned_graph_indexes.len > 0) self.alloc.free(extracted.mentioned_graph_indexes);
            extracted.mentioned_graph_indexes = items;
        }
        self.adopted = true;
    }
};

pub fn fromCatalog(core: anytype, alloc: Allocator, key: []const u8, root: std.json.Value, extracted: *mapper.ExtractedWrite) !void {
    if (!core.hasGraphIndexes() or !extracted.hasDocument() or root != .object) return;
    var builder: Builder = .{ .alloc = alloc };
    defer builder.deinit();
    for (core.graphIndexes()) |entry| {
        var mentioned = false;
        for (entry.edge_type_configs) |edge| {
            const field_name = edge.field_name orelse continue;
            mentioned = true;
            try builder.field(root.object, key, entry.config.name, edge.name, field_name);
        }
        if (mentioned) try builder.mention(extracted, entry.config.name);
    }
    try builder.publish(extracted);
}

pub fn fromSnapshot(fields: anytype, alloc: Allocator, key: []const u8, root: std.json.Value, extracted: *mapper.ExtractedWrite) !void {
    if (fields.len == 0 or !extracted.hasDocument() or root != .object) return;
    var builder: Builder = .{ .alloc = alloc };
    defer builder.deinit();
    for (fields) |graph| {
        if (graph.edges.len == 0) continue;
        for (graph.edges) |edge| try builder.field(root.object, key, graph.index_name, edge.edge_type, edge.field_name);
        try builder.mention(extracted, graph.index_name);
    }
    try builder.publish(extracted);
}

fn appendGraphFieldTargets(
    alloc: Allocator,
    targets: *std.ArrayListUnmanaged([]u8),
    root: std.json.ObjectMap,
    field_name: []const u8,
) !void {
    const field = root.get(field_name) orelse return;
    switch (field) {
        .string => try appendUniqueOwnedKey(alloc, targets, field.string),
        .array => {
            for (field.array.items) |item| {
                if (item != .string) return error.InvalidGraphEdges;
                try appendUniqueOwnedKey(alloc, targets, item.string);
            }
        },
        else => return error.InvalidGraphEdges,
    }
}

fn appendOwnedGraphFieldWrite(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(types.GraphEdgeWrite),
    index_name: []const u8,
    source: []const u8,
    target: []const u8,
    edge_type: []const u8,
) !void {
    try writes.ensureUnusedCapacity(alloc, 1);
    const owned_index_name = try alloc.dupe(u8, index_name);
    errdefer alloc.free(owned_index_name);
    const owned_source = try alloc.dupe(u8, source);
    errdefer alloc.free(owned_source);
    const owned_target = try alloc.dupe(u8, target);
    errdefer alloc.free(owned_target);
    const owned_edge_type = try alloc.dupe(u8, edge_type);
    errdefer alloc.free(owned_edge_type);
    writes.appendAssumeCapacity(.{
        .index_name = owned_index_name,
        .source = owned_source,
        .target = owned_target,
        .edge_type = owned_edge_type,
        .weight = 1.0,
        .created_at = 0,
        .updated_at = 0,
        .metadata_json = "",
    });
}

const FailureFixture = struct {
    const Edge = struct { field_name: ?[]const u8, name: []const u8 };
    const Entry = struct { config: struct { name: []const u8 }, edge_type_configs: []const Edge };
    const Catalog = struct {
        pub fn hasGraphIndexes(_: *@This()) bool {
            return true;
        }
        pub fn graphIndexes(_: *@This()) []const Entry {
            return &.{.{ .config = .{ .name = "graph" }, .edge_type_configs = &.{.{ .field_name = "links", .name = "rel" }} }};
        }
    };
    fn run(alloc: Allocator, root: std.json.Value, snapshot: bool) !void {
        var extracted: mapper.ExtractedWrite = .{
            .cleaned_value = @constCast("{}"),
            .cleaned_value_owned = false,
            .graph_writes = &.{},
            .mentioned_graph_indexes = &.{},
            .dense_embeddings = &.{},
            .sparse_embeddings = &.{},
        };
        defer extracted.deinit(alloc);
        // Start with an existing contribution and mentioned index to verify
        // rollback preserves both original arrays and their element ownership.
        extracted.graph_writes = try alloc.alloc(types.GraphEdgeWrite, 1);
        extracted.graph_writes[0] = (types.GraphEdgeWrite{
            .index_name = "original",
            .source = "doc",
            .target = "existing",
            .edge_type = "old",
        }).cloneAlloc(alloc) catch |err| {
            alloc.free(extracted.graph_writes);
            extracted.graph_writes = &.{};
            return err;
        };
        const before = extracted.graph_writes.ptr;
        if (snapshot) {
            const Fields = struct {
                index_name: []const u8,
                edges: []const struct { edge_type: []const u8, field_name: []const u8 },
            };
            const fields = [_]Fields{.{ .index_name = "graph", .edges = &.{.{ .edge_type = "rel", .field_name = "links" }} }};
            fromSnapshot(&fields, alloc, "doc", root, &extracted) catch |err| {
                try std.testing.expect(extracted.graph_writes.ptr == before);
                try std.testing.expectEqual(@as(usize, 1), extracted.graph_writes.len);
                try std.testing.expectEqual(@as(usize, 0), extracted.mentioned_graph_indexes.len);
                return err;
            };
        } else {
            var catalog: Catalog = .{};
            fromCatalog(&catalog, alloc, "doc", root, &extracted) catch |err| {
                try std.testing.expect(extracted.graph_writes.ptr == before);
                try std.testing.expectEqual(@as(usize, 1), extracted.graph_writes.len);
                try std.testing.expectEqual(@as(usize, 0), extracted.mentioned_graph_indexes.len);
                return err;
            };
        }
        try std.testing.expectEqualStrings("existing", extracted.graph_writes[0].target);
        try std.testing.expectEqual(@as(usize, 1), extracted.mentioned_graph_indexes.len);
        try std.testing.expectEqualStrings("graph", extracted.mentioned_graph_indexes[0]);
        const expected: usize = if (root.object.get("links").? == .string) 2 else if (root.object.get("links").?.array.items.len == 0) 1 else 3;
        try std.testing.expectEqual(expected, extracted.graph_writes.len);
    }
};

test "graph field plans publish atomically across every allocation failure for live and snapshot catalogs" {
    for ([_][]const u8{ "{\"links\":[]}", "{\"links\":\"target\"}", "{\"links\":[\"one\",\"two\",\"one\"]}" }) |json| {
        var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, json, .{});
        defer parsed.deinit();
        for ([_]bool{ false, true }) |snapshot| try std.testing.checkAllAllocationFailures(std.testing.allocator, FailureFixture.run, .{ parsed.value, snapshot });
    }
}
