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

//! Concrete request-only mutation planning. Durable reads are supplied as an
//! explicit snapshot; this module cannot open, mutate, or schedule a DB.
const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("mutation_values.zig");
const transform_mod = @import("transform.zig");

test "mutation plan preserves delete and transform ordering without a database" {
    const alloc = std.testing.allocator;
    var plan = try prepare(alloc, &.{.{ .key = "a", .value = "{\"count\":1}" }}, &.{"a"}, &.{
        .{ .key = "a", .operations = &.{.{ .op = .inc, .path = "count", .value_json = "1" }} },
        .{ .key = "b", .upsert = true, .operations = &.{.{ .op = .set, .path = "count", .value_json = "2" }} },
    }, &.{});
    defer plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), plan.writes.len);
    try std.testing.expectEqualStrings("b", plan.writes[0].key);
    try std.testing.expectEqualStrings("{\"count\":2}", plan.writes[0].value);
    try std.testing.expectEqualStrings("a", plan.deletes[0]);
}

test "mutation plan resolves repeated transforms from one immutable durable snapshot" {
    const alloc = std.testing.allocator;
    const original = "{\"count\":10}";
    var entries = [_]ReadSnapshot.Entry{.{ .key = "a", .value = @constCast(original), .expected_version = 73 }};
    var snapshot: ReadSnapshot = .{ .entries = &entries };
    defer snapshot.positions.deinit(alloc);
    try snapshot.positions.put(alloc, "a", 0);
    var plan = try prepare(alloc, &.{}, &.{}, &.{
        .{ .key = "a", .operations = &.{.{ .op = .inc, .path = "count", .value_json = "1" }} },
        .{ .key = "a", .operations = &.{.{ .op = .inc, .path = "count", .value_json = "1" }} },
    }, &snapshot);
    defer plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), plan.writes.len);
    try std.testing.expectEqualStrings("{\"count\":12}", plan.writes[0].value);
    try std.testing.expectEqualStrings(original, snapshot.valueFor("a").?);
    try std.testing.expectEqual(@as(u64, 73), snapshot.entries[0].expected_version);
}

test "mutation plan retains JSON null provenance for batch and transaction writes" {
    try std.testing.expect(types.BatchWrite == types.TransactionWrite);
    const alloc = std.testing.allocator;
    const writes = [_]types.TransactionWrite{.{ .key = "row", .value = "{\"payload\":null}", .json_null_fields = &.{"payload"} }};
    var plan = try prepare(alloc, &writes, &.{}, &.{}, &.{});
    defer plan.deinit(alloc);
    try std.testing.expectEqualStrings("payload", plan.writes[0].json_null_fields[0]);
    try std.testing.expectError(error.UnsupportedTransformOperation, prepare(alloc, &writes, &.{}, &.{.{ .key = "row", .operations = &.{} }}, &.{}));
}

test "mutation plan frees graph and document effects at every allocation failure" {
    const Check = struct {
        fn run(alloc: Allocator) !void {
            var plan = try prepare(alloc, &.{.{ .key = "a", .value = "{\"count\":1}" }}, &.{"deleted"}, &.{.{ .key = "a", .operations = &.{
                .{ .op = .inc, .path = "count", .value_json = "1" },
                .{ .op = .push, .path = "$._edges.graph.FRIEND", .value_json = "{\"target\":\"b\",\"metadata\":{\"since\":2026}}" },
                .{ .op = .pull, .path = "$._edges.graph.FRIEND", .value_json = "{\"target\":\"b\"}" },
            } }}, &.{});
            defer plan.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 0), plan.graph_writes.items.len);
            try std.testing.expectEqual(@as(usize, 1), plan.graph_deletes.items.len);
        }
    };
    // A backing allocator may remap in place depending on the address it
    // returned. Deny resizing here so every fault sweep visits the same
    // allocation sites in both Debug and ReleaseFast.
    const Stable = struct {
        fn allocate(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
            const backing: *Allocator = @ptrCast(@alignCast(ctx));
            return backing.rawAlloc(len, alignment, ret);
        }
        fn free(ctx: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ret: usize) void {
            const backing: *Allocator = @ptrCast(@alignCast(ctx));
            backing.rawFree(bytes, alignment, ret);
        }
    };
    var backing = std.testing.allocator;
    const stable: Allocator = .{ .ptr = &backing, .vtable = &.{ .alloc = Stable.allocate, .resize = Allocator.noResize, .remap = Allocator.noRemap, .free = Stable.free } };
    try std.testing.checkAllAllocationFailures(stable, Check.run, .{});
}

pub const PreparedMutation = struct {
    pub const Entry = struct {
        json_null_fields: []const []const u8 = &.{},
        key: []const u8,
        value: ?[]const u8 = null,
        kind: enum { write, delete },
        owned_key: bool = false,
        owned_value: bool = false,
    };

    entries: []Entry = &.{},
    writes: []types.BatchWrite = &.{},
    deletes: [][]const u8 = &.{},
    graph_writes: std.ArrayListUnmanaged(types.GraphEdgeWrite) = .empty,
    graph_deletes: std.ArrayListUnmanaged(types.GraphEdgeDelete) = .empty,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.entries) |entry| {
            if (entry.owned_key) alloc.free(@constCast(entry.key));
            if (entry.owned_value) alloc.free(@constCast(entry.value.?));
        }
        if (self.entries.len > 0) alloc.free(self.entries);
        if (self.writes.len > 0) alloc.free(self.writes);
        if (self.deletes.len > 0) alloc.free(self.deletes);
        for (self.graph_writes.items) |*write| deinitOwnedGraphEdgeWrite(alloc, write);
        self.graph_writes.deinit(alloc);
        for (self.graph_deletes.items) |*delete| deinitOwnedGraphEdgeDelete(alloc, delete);
        self.graph_deletes.deinit(alloc);
        self.* = .{};
    }
};

pub const ReadSnapshot = struct {
    pub const Entry = struct {
        key: []const u8,
        value: ?[]u8,
        expected_version: u64,
    };

    entries: []Entry = &.{},
    positions: std.StringHashMapUnmanaged(usize) = .empty,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.entries) |entry| if (entry.value) |value| alloc.free(value);
        if (self.entries.len > 0) alloc.free(self.entries);
        self.positions.deinit(alloc);
        self.* = .{};
    }

    pub fn valueFor(self: *const @This(), key: []const u8) ?[]const u8 {
        const index = self.positions.get(key) orelse return null;
        return self.entries[index].value;
    }
};

pub noinline fn prepare(alloc: Allocator, writes: []const types.BatchWrite, deletes: []const []const u8, transforms: []const types.DocumentTransform, read_snapshot: *const ReadSnapshot) !PreparedMutation {
    const T = types.BatchWrite;
    // SQL mutations lower to complete replacements. A document transform
    // cannot retain the provenance of JSON null after reconstructing JSON.
    if (transforms.len != 0) for (writes) |write| if (write.json_null_fields.len != 0) return error.UnsupportedTransformOperation;
    var result = PreparedMutation{};
    var order = std.ArrayListUnmanaged(PreparedMutation.Entry).empty;
    defer order.deinit(alloc);
    errdefer {
        // Before toOwnedSlice, the list still owns its full capacity.
        // Release fields here and let order.deinit free that exact backing
        // allocation; never pass the shorter items slice to alloc.free.
        for (order.items) |entry| {
            if (entry.owned_key) alloc.free(@constCast(entry.key));
            if (entry.owned_value) alloc.free(@constCast(entry.value.?));
        }
        result.deinit(alloc);
    }

    var positions = std.StringHashMapUnmanaged(usize){};
    defer positions.deinit(alloc);

    for (writes) |write| {
        const gop = try positions.getOrPut(alloc, write.key);
        if (!gop.found_existing) {
            gop.value_ptr.* = order.items.len;
            try order.append(alloc, .{
                .key = write.key,
                .value = write.value,
                .json_null_fields = write.json_null_fields,
                .kind = .write,
            });
            continue;
        }
        const entry = &order.items[gop.value_ptr.*];
        if (entry.owned_value) alloc.free(@constCast(entry.value.?));
        if (entry.owned_key) alloc.free(@constCast(entry.key));
        setCoalescedEntryToBorrowedWrite(entry, write);
    }

    for (deletes) |key| {
        const gop = try positions.getOrPut(alloc, key);
        if (!gop.found_existing) {
            gop.value_ptr.* = order.items.len;
            try order.append(alloc, .{
                .key = key,
                .kind = .delete,
            });
            continue;
        }
        const entry = &order.items[gop.value_ptr.*];
        if (entry.owned_value) alloc.free(@constCast(entry.value.?));
        entry.owned_value = false;
        resetCoalescedEntryToDelete(entry);
    }

    for (transforms) |transform| {
        const maybe_index = positions.get(transform.key);
        const base_json = blk: {
            if (maybe_index) |entry_index| {
                const entry = order.items[entry_index];
                break :blk if (entry.kind == .write) entry.value.? else null;
            }
            break :blk read_snapshot.valueFor(transform.key);
        };

        var document_operations = std.ArrayListUnmanaged(types.TransformOp).empty;
        defer document_operations.deinit(alloc);
        var graph_operations = std.ArrayListUnmanaged(struct {
            op: types.TransformOpType,
            path: transform_mod.GraphProjectionPath,
            value_json: []const u8,
        }).empty;
        defer graph_operations.deinit(alloc);
        for (transform.operations) |operation| {
            const graph_path = try transform_mod.graphProjectionPath(operation.path);
            if (graph_path) |path| {
                switch (operation.op) {
                    .push, .pull, .add_to_set => {},
                    else => return error.UnsupportedTransformOperation,
                }
                try graph_operations.append(alloc, .{
                    .op = operation.op,
                    .path = path,
                    .value_json = operation.value_json orelse return error.InvalidArgument,
                });
            } else {
                try document_operations.append(alloc, operation);
            }
        }

        const graph_writes_start = result.graph_writes.items.len;
        const graph_deletes_start = result.graph_deletes.items.len;
        for (graph_operations.items) |operation| {
            switch (operation.op) {
                .push, .add_to_set => try appendGraphTransformWrite(
                    alloc,
                    &result.graph_writes,
                    &result.graph_deletes,
                    transform.key,
                    operation.path,
                    operation.value_json,
                ),
                .pull => try appendGraphTransformDelete(
                    alloc,
                    &result.graph_writes,
                    &result.graph_deletes,
                    transform.key,
                    operation.path,
                    operation.value_json,
                ),
                else => unreachable,
            }
        }

        const document_transform: types.DocumentTransform = .{
            .key = transform.key,
            .operations = document_operations.items,
            .upsert = transform.upsert,
        };

        // A non-upsert transform against an absent (or same-batch deleted)
        // document is a no-op, but malformed document operations remain
        // errors independent of state. Graph operands were validated while
        // constructing their pending deltas above.
        if (base_json == null and !transform.upsert) {
            try transform_mod.validateDocumentTransform(alloc, document_transform);
            for (result.graph_writes.items[graph_writes_start..]) |*write| {
                deinitOwnedGraphEdgeWrite(alloc, write);
            }
            result.graph_writes.shrinkRetainingCapacity(graph_writes_start);
            for (result.graph_deletes.items[graph_deletes_start..]) |*delete| {
                deinitOwnedGraphEdgeDelete(alloc, delete);
            }
            result.graph_deletes.shrinkRetainingCapacity(graph_deletes_start);
            continue;
        }

        // Even a graph-only transform remains a logical document write:
        // preserve source version/timestamp and change-journal semantics
        // while applying the projected edge as a delta. The mapper sees
        // the unchanged stripped document and therefore does not clear
        // the graph generation.
        const resolved = try transform_mod.resolveDocumentTransform(alloc, base_json, document_transform);

        const resolved_document = resolved orelse continue;
        var resolved_document_owned = true;
        defer if (resolved_document_owned) alloc.free(resolved_document);

        const gop = try positions.getOrPut(alloc, transform.key);
        if (!gop.found_existing) {
            gop.value_ptr.* = order.items.len;
            const owned_key = try alloc.dupe(u8, transform.key);
            errdefer alloc.free(owned_key);
            try order.append(alloc, .{
                .key = owned_key,
                .value = resolved_document,
                .kind = .write,
                .owned_key = true,
                .owned_value = true,
            });
            resolved_document_owned = false;
            continue;
        }

        const entry = &order.items[gop.value_ptr.*];
        try setCoalescedEntryToOwnedWrite(alloc, entry, transform.key, resolved_document);
        resolved_document_owned = false;
    }

    var write_count: usize = 0;
    var delete_count: usize = 0;
    for (order.items) |entry| {
        switch (entry.kind) {
            .write => write_count += 1,
            .delete => delete_count += 1,
        }
    }

    const final_entries = try order.toOwnedSlice(alloc);
    result.entries = final_entries;
    if (write_count > 0) result.writes = try alloc.alloc(T, write_count);
    if (delete_count > 0) result.deletes = try alloc.alloc([]const u8, delete_count);

    var write_index: usize = 0;
    var delete_index: usize = 0;
    for (final_entries) |entry| {
        switch (entry.kind) {
            .write => {
                result.writes[write_index] = .{
                    .key = entry.key,
                    .value = entry.value.?,
                    .json_null_fields = entry.json_null_fields,
                };
                write_index += 1;
            },
            .delete => {
                result.deletes[delete_index] = entry.key;
                delete_index += 1;
            },
        }
    }
    return result;
}

pub fn resetCoalescedEntryToDelete(entry: *PreparedMutation.Entry) void {
    if (entry.owned_value) {
        // Caller frees previous owned value before switching the entry.
        entry.owned_value = false;
    }
    entry.value = null;
    entry.kind = .delete;
}

pub fn setCoalescedEntryToBorrowedWrite(entry: *PreparedMutation.Entry, write: types.BatchWrite) void {
    entry.key = write.key;
    entry.value = write.value;
    entry.json_null_fields = write.json_null_fields;
    entry.kind = .write;
    entry.owned_key = false;
    entry.owned_value = false;
}

pub fn setCoalescedEntryToOwnedWrite(
    alloc: Allocator,
    entry: *PreparedMutation.Entry,
    key: []const u8,
    value: []u8,
) !void {
    if (!entry.owned_key) {
        entry.key = try alloc.dupe(u8, key);
        entry.owned_key = true;
    }
    if (entry.owned_value) alloc.free(@constCast(entry.value.?));
    entry.value = value;
    entry.kind = .write;
    entry.owned_value = true;
}

pub fn appendGraphTransformDelete(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(types.GraphEdgeWrite),
    deletes: *std.ArrayListUnmanaged(types.GraphEdgeDelete),
    source: []const u8,
    path: transform_mod.GraphProjectionPath,
    value_json: []const u8,
) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, value_json, .{ .parse_numbers = false });
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidGraphEdges;
    const target_value = parsed.value.object.get("target") orelse return error.InvalidGraphEdges;
    if (target_value != .string or target_value.string.len == 0) return error.InvalidGraphEdges;

    const index_name = try alloc.dupe(u8, path.index_name);
    errdefer alloc.free(index_name);
    const owned_source = try alloc.dupe(u8, source);
    errdefer alloc.free(owned_source);
    const target = try alloc.dupe(u8, target_value.string);
    errdefer alloc.free(target);
    const edge_type = try alloc.dupe(u8, path.edge_type);
    errdefer alloc.free(edge_type);
    const edge_id = if (parsed.value.object.get("edge_id")) |value| blk: {
        if (value != .string or value.string.len == 0) return error.InvalidGraphEdges;
        break :blk try alloc.dupe(u8, value.string);
    } else "";
    errdefer if (edge_id.len > 0) alloc.free(edge_id);

    // A later pull overrides every earlier projected write for the same
    // physical relationship before the split graph mutation batch is built.
    var write_index = writes.items.len;
    while (write_index > 0) {
        write_index -= 1;
        const prior = writes.items[write_index];
        if (!graphMutationIdentityEql(prior, .{
            .index_name = index_name,
            .source = owned_source,
            .target = target,
            .edge_type = edge_type,
            .edge_id = edge_id,
        })) continue;
        var removed = writes.orderedRemove(write_index);
        deinitOwnedGraphEdgeWrite(alloc, &removed);
    }
    try deletes.append(alloc, .{
        .index_name = index_name,
        .source = owned_source,
        .target = target,
        .edge_type = edge_type,
        .edge_id = edge_id,
    });
}

pub fn appendGraphTransformWrite(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(types.GraphEdgeWrite),
    deletes: *std.ArrayListUnmanaged(types.GraphEdgeDelete),
    source: []const u8,
    path: transform_mod.GraphProjectionPath,
    value_json: []const u8,
) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, value_json, .{ .parse_numbers = false });
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidGraphEdges;

    const target_value = parsed.value.object.get("target") orelse return error.InvalidGraphEdges;
    if (target_value != .string or target_value.string.len == 0) return error.InvalidGraphEdges;

    const weight: f64 = if (parsed.value.object.get("weight")) |value| switch (value) {
        .integer => |integer| @floatFromInt(integer),
        .float => |float| float,
        .number_string => |number| try std.fmt.parseFloat(f64, number),
        else => return error.InvalidGraphEdges,
    } else 1.0;
    if (!std.math.isFinite(weight)) return error.InvalidGraphEdges;

    const metadata_json: []const u8 = if (parsed.value.object.get("metadata")) |metadata| blk: {
        // Edge metadata has one stable public shape across ingestion,
        // storage, graph queries, and generated SDKs. Reject scalar or
        // array values before they become durable artifacts rather than
        // allowing a later response to violate the object contract.
        if (metadata != .object) return error.InvalidGraphEdges;
        break :blk try std.json.Stringify.valueAlloc(alloc, metadata, .{});
    } else "";
    errdefer if (metadata_json.len > 0) alloc.free(@constCast(metadata_json));

    const index_name = try alloc.dupe(u8, path.index_name);
    errdefer alloc.free(index_name);
    const owned_source = try alloc.dupe(u8, source);
    errdefer alloc.free(owned_source);
    const target = try alloc.dupe(u8, target_value.string);
    errdefer alloc.free(target);
    const edge_type = try alloc.dupe(u8, path.edge_type);
    errdefer alloc.free(edge_type);
    const edge_id = if (parsed.value.object.get("edge_id")) |value| blk: {
        if (value != .string or value.string.len == 0) return error.InvalidGraphEdges;
        break :blk try alloc.dupe(u8, value.string);
    } else "";
    errdefer if (edge_id.len > 0) alloc.free(edge_id);

    // Transform operations are ordered. Since the storage batch carries
    // graph writes and deletes in separate slices (deletes execute first),
    // discard any earlier projected delete for this identity so a later
    // push/addToSet remains the final operation.
    var delete_index = deletes.items.len;
    while (delete_index > 0) {
        delete_index -= 1;
        const prior = deletes.items[delete_index];
        if (!graphMutationIdentityEql(prior, .{
            .index_name = index_name,
            .source = owned_source,
            .target = target,
            .edge_type = edge_type,
            .edge_id = edge_id,
        })) continue;
        var removed = deletes.orderedRemove(delete_index);
        deinitOwnedGraphEdgeDelete(alloc, &removed);
    }

    try writes.append(alloc, .{
        .index_name = index_name,
        .source = owned_source,
        .target = target,
        .edge_type = edge_type,
        .edge_id = edge_id,
        .weight = weight,
        .metadata_json = metadata_json,
    });
}

pub fn deinitOwnedGraphEdgeDelete(alloc: Allocator, delete: *types.GraphEdgeDelete) void {
    alloc.free(@constCast(delete.index_name));
    alloc.free(@constCast(delete.source));
    alloc.free(@constCast(delete.target));
    alloc.free(@constCast(delete.edge_type));
    if (delete.edge_id.len > 0) alloc.free(@constCast(delete.edge_id));
    if (delete.owner_document.len > 0) alloc.free(@constCast(delete.owner_document));
    if (delete.owner.len > 0) alloc.free(@constCast(delete.owner));
    delete.* = undefined;
}

pub fn deinitOwnedGraphEdgeWrite(alloc: Allocator, write: *types.GraphEdgeWrite) void {
    alloc.free(@constCast(write.index_name));
    alloc.free(@constCast(write.source));
    alloc.free(@constCast(write.target));
    alloc.free(@constCast(write.edge_type));
    if (write.edge_id.len > 0) alloc.free(@constCast(write.edge_id));
    if (write.owner_document.len > 0) alloc.free(@constCast(write.owner_document));
    if (write.metadata_json.len > 0) alloc.free(@constCast(write.metadata_json));
    if (write.owner.len > 0) alloc.free(@constCast(write.owner));
    write.* = undefined;
}

pub fn graphMutationIdentityEql(left: anytype, right: anytype) bool {
    return std.mem.eql(u8, left.index_name, right.index_name) and
        std.mem.eql(u8, left.source, right.source) and
        std.mem.eql(u8, left.target, right.target) and
        std.mem.eql(u8, left.edge_type, right.edge_type) and
        std.mem.eql(u8, left.edge_id, if (@hasField(@TypeOf(right), "edge_id")) right.edge_id else "") and
        std.mem.eql(u8, left.owner_document, if (@hasField(@TypeOf(right), "owner_document")) right.owner_document else "");
}
