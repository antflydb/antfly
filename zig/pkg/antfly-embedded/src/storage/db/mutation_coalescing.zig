// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Shared document/graph mutation preparation over one captured primary read set.
//! This leaf has no storage, VM, lock, RPC, or publication capability.
const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const transform_mod = @import("transform.zig");

pub fn CoalescedKeyValueRequest(comptime T: type) type {
    return struct {
        pub const Entry = struct {
            json_null_fields: []const []const u8 = &.{},
            prepared_domain: ?*const @import("../../schema/domain_handoff.zig").Contract = null,
            key: []const u8,
            value: ?[]const u8 = null,
            kind: enum { write, delete },
            owned_key: bool = false,
            owned_value: bool = false,
            owned_null_fields: bool = false,
        };

        entries: []Entry = &.{},
        writes: []T = &.{},
        deletes: [][]const u8 = &.{},
        graph_writes: std.ArrayListUnmanaged(types.GraphEdgeWrite) = .empty,
        graph_deletes: std.ArrayListUnmanaged(types.GraphEdgeDelete) = .empty,

        pub fn deinit(self: *@This(), alloc: Allocator) void {
            for (self.entries) |entry| {
                if (entry.owned_key) alloc.free(@constCast(entry.key));
                if (entry.owned_value) alloc.free(@constCast(entry.value.?));
                if (entry.owned_null_fields) deinitNullFields(alloc, entry.json_null_fields);
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
}

pub const TransformReadSnapshot = struct {
    pub const Entry = struct {
        key: []const u8,
        value: ?[]u8,
        expected_version: u64,
        json_null_fields: []const []const u8 = &.{},
    };

    json_columns: []const []const u8 = &.{},
    entries: []Entry = &.{},
    positions: std.StringHashMapUnmanaged(usize) = .empty,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.entries) |entry| {
            if (entry.value) |value| alloc.free(value);
            deinitNullFields(alloc, entry.json_null_fields);
        }
        deinitNullFields(alloc, self.json_columns);
        if (self.entries.len > 0) alloc.free(self.entries);
        self.positions.deinit(alloc);
        self.* = .{};
    }

    pub fn nullFieldsFor(self: *const @This(), key: []const u8) []const []const u8 {
        const index = self.positions.get(key) orelse return &.{};
        return self.entries[index].json_null_fields;
    }
    pub fn valueFor(self: *const @This(), key: []const u8) ?[]const u8 {
        const index = self.positions.get(key) orelse return null;
        return self.entries[index].value;
    }
};

pub fn coalesceKeyValueRequest(
    self: anytype,
    alloc: Allocator,
    comptime T: type,
    writes: []const T,
    deletes: []const []const u8,
    transforms: []const types.DocumentTransform,
    transform_snapshot: ?*const TransformReadSnapshot,
) !CoalescedKeyValueRequest(T) {
    if (transform_snapshot == null and transforms.len != 0) for (writes) |write| {
        if (write.json_null_fields.len != 0 or write.prepared_domain != null) return error.UnsupportedTransformOperation;
    };
    var result = CoalescedKeyValueRequest(T){};
    var order = std.ArrayListUnmanaged(CoalescedKeyValueRequest(T).Entry).empty;
    defer order.deinit(alloc);
    errdefer {
        // Before toOwnedSlice, the list still owns its full capacity.
        // Release fields here and let order.deinit free that exact backing
        // allocation; never pass the shorter items slice to alloc.free.
        for (order.items) |entry| {
            if (entry.owned_key) alloc.free(@constCast(entry.key));
            if (entry.owned_value) alloc.free(@constCast(entry.value.?));
            if (entry.owned_null_fields) deinitNullFields(alloc, entry.json_null_fields);
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
                .prepared_domain = write.prepared_domain,
                .kind = .write,
            });
            continue;
        }
        const entry = &order.items[gop.value_ptr.*];
        if (entry.owned_value) alloc.free(@constCast(entry.value.?));
        if (entry.owned_key) alloc.free(@constCast(entry.key));
        if (entry.owned_null_fields) deinitNullFields(alloc, entry.json_null_fields);
        setCoalescedEntryToBorrowedWrite(T, entry, write);
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
        if (entry.owned_null_fields) deinitNullFields(alloc, entry.json_null_fields);
        resetCoalescedEntryToDelete(T, entry);
    }

    for (transforms) |transform| {
        const maybe_index = positions.get(transform.key);
        const base_json = blk: {
            if (maybe_index) |entry_index| {
                const entry = order.items[entry_index];
                break :blk if (entry.kind == .write) entry.value.? else null;
            }
            if (transform_snapshot) |read_snapshot| break :blk read_snapshot.valueFor(transform.key);
            // Borrowed mutation execution always supplies a pinned read
            // set; it has no foreground lookup capability to fall back to.
            if (self.isRecoveryExecution()) return error.PreparedReadSetChanged;
            break :blk try self.get(alloc, transform.key);
        };
        defer if (maybe_index == null and transform_snapshot == null) {
            if (base_json) |body| alloc.free(body);
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
        const before_flags: []const []const u8 = if (maybe_index) |index| order.items[index].json_null_fields else if (transform_snapshot) |snapshot| snapshot.nullFieldsFor(transform.key) else &.{};
        const json_columns: []const []const u8 = if (transform_snapshot) |snapshot| snapshot.json_columns else &.{};
        const null_fields = transform_mod.resolveJsonNullFields(alloc, before_flags, json_columns, document_transform, base_json == null) catch |err| {
            alloc.free(resolved_document);
            return err;
        };
        var null_fields_owned = true;
        defer if (null_fields_owned) deinitNullFields(alloc, null_fields);
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
                .json_null_fields = null_fields,
                .owned_null_fields = true,
            });
            null_fields_owned = false;
            resolved_document_owned = false;
            continue;
        }

        const entry = &order.items[gop.value_ptr.*];
        if (entry.owned_null_fields) deinitNullFields(alloc, entry.json_null_fields);
        entry.owned_null_fields = false;
        entry.json_null_fields = &.{};
        try setCoalescedEntryToOwnedWrite(T, alloc, entry, transform.key, resolved_document);
        entry.prepared_domain = null;
        entry.json_null_fields = null_fields;
        entry.owned_null_fields = true;
        null_fields_owned = false;
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
                    .prepared_domain = entry.prepared_domain,
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

pub fn resetCoalescedEntryToDelete(comptime T: type, entry: *CoalescedKeyValueRequest(T).Entry) void {
    if (entry.owned_value) {
        // Caller frees previous owned value before switching the entry.
        entry.owned_value = false;
    }
    entry.value = null;
    entry.prepared_domain = null;
    entry.json_null_fields = &.{};
    entry.owned_null_fields = false;
    entry.kind = .delete;
}

pub fn setCoalescedEntryToBorrowedWrite(comptime T: type, entry: *CoalescedKeyValueRequest(T).Entry, write: T) void {
    entry.key = write.key;
    entry.value = write.value;
    entry.json_null_fields = write.json_null_fields;
    entry.prepared_domain = write.prepared_domain;
    entry.kind = .write;
    entry.owned_key = false;
    entry.owned_value = false;
    entry.owned_null_fields = false;
}

pub fn setCoalescedEntryToOwnedWrite(
    comptime T: type,
    alloc: Allocator,
    entry: *CoalescedKeyValueRequest(T).Entry,
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

const CapturedOnlyReader = struct {
    pub fn isRecoveryExecution(_: @This()) bool {
        return true;
    }
    pub fn get(_: @This(), _: Allocator, _: []const u8) !?[]u8 {
        return error.PreparedReadSetChanged;
    }
};

test "shared mutation coalescing preserves ordered graph deltas outside the document and never fetches uncaptured state" {
    const a = std.testing.allocator;
    const snapshot: TransformReadSnapshot = .{};
    var result = try coalesceKeyValueRequest(CapturedOnlyReader{}, a, types.TransactionWrite, &.{.{ .key = "a", .value = "{\"count\":1}" }}, &.{}, &.{
        .{ .key = "a", .operations = &.{
            .{ .op = .inc, .path = "count", .value_json = "1" },
            .{ .op = .push, .path = "_edges.g.R", .value_json = "{\"target\":\"b\",\"metadata\":{\"n\":1.5000000000000001}}" },
            .{ .op = .pull, .path = "_edges.g.R", .value_json = "{\"target\":\"b\"}" },
            .{ .op = .add_to_set, .path = "_edges.g.R", .value_json = "{\"target\":\"c\",\"edge_id\":\"edge\"}" },
        } },
    }, &snapshot);
    defer result.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), result.writes.len);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, result.writes[0].value, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(i64, 2), parsed.value.object.get("count").?.integer);
    try std.testing.expect(parsed.value.object.get("_edges") == null);
    try std.testing.expectEqual(@as(usize, 1), result.graph_writes.items.len);
    try std.testing.expectEqualStrings("c", result.graph_writes.items[0].target);
    try std.testing.expectEqualStrings("edge", result.graph_writes.items[0].edge_id);
    try std.testing.expectEqual(@as(usize, 1), result.graph_deletes.items.len);
    try std.testing.expectEqualStrings("b", result.graph_deletes.items[0].target);
    try std.testing.expectError(error.PreparedReadSetChanged, coalesceKeyValueRequest(CapturedOnlyReader{}, a, types.TransactionWrite, &.{}, &.{}, &.{.{ .key = "absent", .operations = &.{.{ .op = .inc, .path = "count", .value_json = "1" }} }}, null));
}

test "shared mutation coalescing absent nonupsert keeps graph deltas and primary rows empty" {
    const a = std.testing.allocator;
    const snapshot: TransformReadSnapshot = .{};
    var result = try coalesceKeyValueRequest(CapturedOnlyReader{}, a, types.TransactionWrite, &.{}, &.{}, &.{.{ .key = "absent", .operations = &.{.{ .op = .push, .path = "_edges.g.R", .value_json = "{\"target\":\"b\"}" }} }}, &snapshot);
    defer result.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), result.writes.len);
    try std.testing.expectEqual(@as(usize, 0), result.graph_writes.items.len);
    try std.testing.expectEqual(@as(usize, 0), result.graph_deletes.items.len);
}

fn deinitNullFields(a: Allocator, fields: []const []const u8) void {
    for (fields) |field| a.free(field);
    if (fields.len != 0) a.free(fields);
}

test "shared mutation coalescing retains typed JSON nulls and invalidates transformed prepared domains" {
    const Check = struct {
        fn run(a: Allocator) !void {
            var domain: @import("../../schema/domain_handoff.zig").Contract = undefined;
            const snapshot: TransformReadSnapshot = .{ .json_columns = &.{"payload"} };
            var result = try coalesceKeyValueRequest(CapturedOnlyReader{}, a, types.TransactionWrite, &.{.{ .key = "row", .value = "{\"payload\":null,\"count\":1}", .json_null_fields = &.{"payload"}, .prepared_domain = &domain }}, &.{}, &.{.{ .key = "row", .operations = &.{.{ .op = .inc, .path = "count", .value_json = "1" }} }}, &snapshot);
            defer result.deinit(a);
            try std.testing.expectEqual(@as(usize, 1), result.writes.len);
            try std.testing.expectEqual(@as(usize, 1), result.writes[0].json_null_fields.len);
            try std.testing.expectEqualStrings("payload", result.writes[0].json_null_fields[0]);
            try std.testing.expect(result.writes[0].prepared_domain == null);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}
