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
const db_mod = @import("../storage/db/mod.zig");
const document_mapper = @import("../storage/db/document_mapper.zig");
const public_limits = @import("public_limits.zig");

pub const BatchResult = struct {
    inserted: u32,
    deleted: u32,
    transformed: u32 = 0,
};

pub const OwnedBatchRequest = struct {
    writes: []db_mod.types.BatchWrite = &.{},
    deletes: [][]const u8 = &.{},
    transforms: []db_mod.types.DocumentTransform = &.{},
    split_checkpoint_range_start: ?[]u8 = null,
    split_checkpoint_range_end: ?[]u8 = null,
    split_transition_key: ?[]u8 = null,
    req: db_mod.types.BatchRequest = .{},

    pub fn deinit(self: *OwnedBatchRequest, alloc: std.mem.Allocator) void {
        for (self.writes) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        if (self.writes.len > 0) alloc.free(self.writes);
        for (self.deletes) |key| alloc.free(key);
        if (self.deletes.len > 0) alloc.free(self.deletes);
        for (self.transforms) |transform| {
            alloc.free(@constCast(transform.key));
            for (transform.operations) |op| {
                alloc.free(@constCast(op.path));
                if (op.value_json) |value_json| alloc.free(@constCast(value_json));
            }
            if (transform.operations.len > 0) alloc.free(transform.operations);
        }
        if (self.transforms.len > 0) alloc.free(self.transforms);
        if (self.split_checkpoint_range_start) |value| alloc.free(value);
        if (self.split_checkpoint_range_end) |value| alloc.free(value);
        if (self.split_transition_key) |value| alloc.free(value);
        self.* = undefined;
    }

    pub fn result(self: OwnedBatchRequest) BatchResult {
        return .{
            .inserted = @intCast(self.writes.len),
            .deleted = @intCast(self.deletes.len),
            .transformed = @intCast(self.transforms.len),
        };
    }
};

pub fn parseBatchRequest(alloc: std.mem.Allocator, body: []const u8) !OwnedBatchRequest {
    return try parseBatchRequestWithOptions(alloc, body, .{
        .allocate = .alloc_always,
        .max_value_len = public_limits.max_json_value_len,
    }, false);
}

pub fn parseInternalBatchRequest(alloc: std.mem.Allocator, body: []const u8) !OwnedBatchRequest {
    return try parseBatchRequestWithOptions(alloc, body, .{
        .allocate = .alloc_always,
        .max_value_len = public_limits.max_json_value_len,
    }, true);
}

fn parseBatchRequestWithOptions(
    alloc: std.mem.Allocator,
    body: []const u8,
    options: std.json.ParseOptions,
    allow_internal: bool,
) !OwnedBatchRequest {
    if (body.len == 0) return .{};

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, options) catch |err| switch (err) {
        error.ValueTooLong => return error.ValueTooLong,
        else => return error.InvalidBatchRequest,
    };
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidBatchRequest;
    const root = parsed.value.object;

    const writes: []db_mod.types.BatchWrite = writes: {
        if (root.get("inserts")) |inserts| {
            if (inserts == .null) break :writes &.{};
            const parsed_writes = try parseInserts(alloc, inserts);
            errdefer freeWrites(alloc, parsed_writes);
            break :writes parsed_writes;
        }
        break :writes &.{};
    };
    errdefer freeWrites(alloc, writes);

    const deletes: [][]const u8 = deletes: {
        if (root.get("deletes")) |deletes_value| {
            if (deletes_value == .null) break :deletes &.{};
            const parsed_deletes = try parseDeletes(alloc, deletes_value);
            errdefer freeDeletes(alloc, parsed_deletes);
            break :deletes parsed_deletes;
        }
        break :deletes &.{};
    };
    errdefer freeDeletes(alloc, deletes);

    const transforms: []db_mod.types.DocumentTransform = transforms: {
        if (root.get("transforms")) |transforms_value| {
            if (transforms_value == .null) break :transforms &.{};
            const parsed_transforms = try parseTransforms(alloc, transforms_value);
            errdefer freeTransforms(alloc, parsed_transforms);
            break :transforms parsed_transforms;
        }
        break :transforms &.{};
    };
    errdefer freeTransforms(alloc, transforms);

    const sync_level = sync_level: {
        if (root.get("sync_level")) |sync_level_value| {
            if (sync_level_value == .null) break :sync_level db_mod.types.SyncLevel.propose;
            break :sync_level try syncLevelFromValue(sync_level_value);
        }
        break :sync_level db_mod.types.SyncLevel.propose;
    };

    var checkpoint_start: ?[]u8 = null;
    errdefer if (checkpoint_start) |value| alloc.free(value);
    var checkpoint_end: ?[]u8 = null;
    errdefer if (checkpoint_end) |value| alloc.free(value);
    const split_checkpoint: ?db_mod.types.SplitReplicationCheckpoint = checkpoint: {
        const value = root.get("_split_checkpoint") orelse break :checkpoint null;
        if (!allow_internal or value != .object) return error.InvalidBatchRequest;
        const object = value.object;
        const kind_value = object.get("kind") orelse return error.InvalidBatchRequest;
        const transition_value = object.get("transition_id") orelse return error.InvalidBatchRequest;
        const attempt_value = object.get("attempt_epoch") orelse return error.InvalidBatchRequest;
        const source_value = object.get("source_group_id") orelse return error.InvalidBatchRequest;
        const destination_value = object.get("destination_group_id") orelse return error.InvalidBatchRequest;
        const sequence_value = object.get("delta_sequence") orelse return error.InvalidBatchRequest;
        if (kind_value != .string) return error.InvalidBatchRequest;
        const transition_id = try parseInternalU64(transition_value);
        const attempt_epoch = try parseInternalU64(attempt_value);
        const source_group_id = try parseInternalU64(source_value);
        const destination_group_id = try parseInternalU64(destination_value);
        const delta_sequence = try parseInternalU64(sequence_value);
        const kind: db_mod.types.SplitReplicationCheckpoint.Kind = std.meta.stringToEnum(
            db_mod.types.SplitReplicationCheckpoint.Kind,
            kind_value.string,
        ) orelse return error.InvalidBatchRequest;
        const range_start = if (object.get("range_start")) |item| start: {
            if (item != .string) return error.InvalidBatchRequest;
            break :start item.string;
        } else "";
        const range_end = if (object.get("range_end")) |item| end: {
            if (item != .string) return error.InvalidBatchRequest;
            break :end item.string;
        } else "";
        checkpoint_start = try alloc.dupe(u8, range_start);
        checkpoint_end = try alloc.dupe(u8, range_end);
        if (transition_id == 0 or attempt_epoch == 0) return error.InvalidBatchRequest;
        break :checkpoint .{
            .kind = kind,
            .transition_id = transition_id,
            .attempt_epoch = attempt_epoch,
            .source_group_id = source_group_id,
            .destination_group_id = destination_group_id,
            .range_start = checkpoint_start.?,
            .range_end = checkpoint_end.?,
            .delta_sequence = delta_sequence,
        };
    };

    const split_replication: ?db_mod.types.SplitReplicationContext = replication: {
        const value = root.get("_split_replication") orelse break :replication null;
        if (!allow_internal or value != .object) return error.InvalidBatchRequest;
        const object = value.object;
        const transition_value = object.get("transition_id") orelse return error.InvalidBatchRequest;
        const attempt_value = object.get("attempt_epoch") orelse return error.InvalidBatchRequest;
        const source_value = object.get("source_group_id") orelse return error.InvalidBatchRequest;
        const destination_value = object.get("destination_group_id") orelse return error.InvalidBatchRequest;
        const table_value = object.get("namespace_table_id") orelse return error.InvalidBatchRequest;
        const shard_value = object.get("namespace_shard_id") orelse return error.InvalidBatchRequest;
        const range_value = object.get("namespace_range_id") orelse return error.InvalidBatchRequest;
        const transition_id = try parseInternalU64(transition_value);
        const attempt_epoch = try parseInternalU64(attempt_value);
        const source_group_id = try parseInternalU64(source_value);
        const destination_group_id = try parseInternalU64(destination_value);
        const table_id = try parseInternalU64(table_value);
        const shard_id = try parseInternalU64(shard_value);
        const range_id = try parseInternalU64(range_value);
        if (transition_id == 0 or attempt_epoch == 0 or source_group_id == 0 or destination_group_id == 0 or table_id == 0 or shard_id == 0 or range_id == 0)
            return error.InvalidBatchRequest;
        break :replication .{
            .transition_id = transition_id,
            .attempt_epoch = attempt_epoch,
            .source_group_id = source_group_id,
            .destination_group_id = destination_group_id,
            .identity_namespace = .{
                .table_id = table_id,
                .shard_id = shard_id,
                .range_id = range_id,
            },
        };
    };

    var transition_key: ?[]u8 = null;
    errdefer if (transition_key) |value| alloc.free(value);
    const split_transition: ?db_mod.types.SplitTransitionMutation = transition: {
        const value = root.get("_split_transition") orelse break :transition null;
        if (!allow_internal or value != .object) return error.InvalidBatchRequest;
        const object = value.object;
        const kind_value = object.get("kind") orelse return error.InvalidBatchRequest;
        const transition_value = object.get("transition_id") orelse return error.InvalidBatchRequest;
        const attempt_value = object.get("attempt_epoch") orelse return error.InvalidBatchRequest;
        const destination_value = object.get("destination_group_id") orelse return error.InvalidBatchRequest;
        if (kind_value != .string) return error.InvalidBatchRequest;
        const transition_id = try parseInternalU64(transition_value);
        const attempt_epoch = try parseInternalU64(attempt_value);
        const destination_group_id = try parseInternalU64(destination_value);
        if (transition_id == 0 or attempt_epoch == 0 or destination_group_id == 0) return error.InvalidBatchRequest;
        const kind = std.meta.stringToEnum(db_mod.types.SplitTransitionMutation.Kind, kind_value.string) orelse
            return error.InvalidBatchRequest;
        const split_key = if (object.get("split_key")) |item| key: {
            if (item != .string) return error.InvalidBatchRequest;
            break :key item.string;
        } else "";
        if ((kind == .prepare or kind == .start) and split_key.len == 0) return error.InvalidBatchRequest;
        transition_key = try alloc.dupe(u8, split_key);
        break :transition .{
            .kind = kind,
            .transition_id = transition_id,
            .attempt_epoch = attempt_epoch,
            .destination_group_id = destination_group_id,
            .split_key = transition_key.?,
        };
    };

    if (split_transition != null and
        (writes.len != 0 or deletes.len != 0 or transforms.len != 0 or
            split_checkpoint != null or split_replication != null))
    {
        return error.InvalidBatchRequest;
    }
    if (split_checkpoint != null and split_checkpoint.?.kind == .source_ack and
        (writes.len != 0 or deletes.len != 0 or transforms.len != 0 or
            split_replication != null))
    {
        return error.InvalidBatchRequest;
    }

    return .{
        .writes = writes,
        .deletes = deletes,
        .transforms = transforms,
        .split_checkpoint_range_start = checkpoint_start,
        .split_checkpoint_range_end = checkpoint_end,
        .split_transition_key = transition_key,
        .req = .{
            .writes = writes,
            .deletes = deletes,
            .transforms = transforms,
            .sync_level = sync_level,
            .split_checkpoint = split_checkpoint,
            .split_replication = split_replication,
            .split_transition = split_transition,
        },
    };
}

pub fn encodeBatchResponse(alloc: std.mem.Allocator, result: BatchResult) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{{\"inserted\":{d},\"deleted\":{d},\"transformed\":{d}}}", .{
        result.inserted,
        result.deleted,
        result.transformed,
    });
}

pub fn encodeBatchRequest(alloc: std.mem.Allocator, req: db_mod.types.BatchRequest) ![]u8 {
    if (req.graph_writes.len > 0 or req.graph_deletes.len > 0 or req.predicates.len > 0) {
        return error.UnsupportedBatchRequestEncoding;
    }
    if (req.split_transition != null and
        (req.writes.len != 0 or req.deletes.len != 0 or req.transforms.len != 0 or
            req.split_checkpoint != null or req.split_replication != null))
    {
        return error.InvalidBatchRequest;
    }
    if (req.split_checkpoint != null and req.split_checkpoint.?.kind == .source_ack and
        (req.writes.len != 0 or req.deletes.len != 0 or req.transforms.len != 0 or
            req.graph_writes.len != 0 or req.graph_deletes.len != 0 or req.predicates.len != 0 or
            req.split_replication != null))
    {
        return error.InvalidBatchRequest;
    }

    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;

    try writer.writeAll("{\"inserts\":{");
    for (req.writes, 0..) |write, i| {
        if (i != 0) try writer.writeByte(',');
        try writer.print("{f}:", .{std.json.fmt(write.key, .{})});
        try writer.writeAll(write.value);
    }
    try writer.writeAll("},\"deletes\":[");
    for (req.deletes, 0..) |key, i| {
        if (i != 0) try writer.writeByte(',');
        try writer.print("{f}", .{std.json.fmt(key, .{})});
    }
    try writer.writeAll("]");
    if (req.transforms.len > 0) {
        try writer.writeAll(",\"transforms\":[");
        for (req.transforms, 0..) |transform, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{{\"key\":{f},\"operations\":[", .{std.json.fmt(transform.key, .{})});
            for (transform.operations, 0..) |op, op_index| {
                if (op_index != 0) try writer.writeByte(',');
                try writer.print("{{\"op\":{f},\"path\":{f}", .{
                    std.json.fmt(db_mod.transform.transformOpText(op.op), .{}),
                    std.json.fmt(op.path, .{}),
                });
                if (op.value_json) |value_json| {
                    try writer.writeAll(",\"value\":");
                    try writer.writeAll(value_json);
                }
                try writer.writeByte('}');
            }
            try writer.writeByte(']');
            if (transform.upsert) try writer.writeAll(",\"upsert\":true");
            try writer.writeByte('}');
        }
        try writer.writeAll("]");
    }
    if (req.split_checkpoint) |checkpoint| {
        try writer.print(",\"_split_checkpoint\":{{\"kind\":{f},\"transition_id\":\"{d}\",\"attempt_epoch\":\"{d}\",\"source_group_id\":\"{d}\",\"destination_group_id\":\"{d}\",\"range_start\":{f},\"range_end\":{f},\"delta_sequence\":\"{d}\"}}", .{
            std.json.fmt(@tagName(checkpoint.kind), .{}),
            checkpoint.transition_id,
            checkpoint.attempt_epoch,
            checkpoint.source_group_id,
            checkpoint.destination_group_id,
            std.json.fmt(checkpoint.range_start, .{}),
            std.json.fmt(checkpoint.range_end, .{}),
            checkpoint.delta_sequence,
        });
    }
    if (req.split_replication) |replication| {
        try writer.print(",\"_split_replication\":{{\"transition_id\":\"{d}\",\"attempt_epoch\":\"{d}\",\"source_group_id\":\"{d}\",\"destination_group_id\":\"{d}\",\"namespace_table_id\":\"{d}\",\"namespace_shard_id\":\"{d}\",\"namespace_range_id\":\"{d}\"}}", .{
            replication.transition_id,
            replication.attempt_epoch,
            replication.source_group_id,
            replication.destination_group_id,
            replication.identity_namespace.table_id,
            replication.identity_namespace.shard_id,
            replication.identity_namespace.range_id,
        });
    }
    if (req.split_transition) |transition| {
        try writer.print(",\"_split_transition\":{{\"kind\":{f},\"transition_id\":\"{d}\",\"attempt_epoch\":\"{d}\",\"destination_group_id\":\"{d}\",\"split_key\":{f}}}", .{
            std.json.fmt(@tagName(transition.kind), .{}),
            transition.transition_id,
            transition.attempt_epoch,
            transition.destination_group_id,
            std.json.fmt(transition.split_key, .{}),
        });
    }
    try writer.print(",\"sync_level\":\"{s}\"}}", .{syncLevelName(req.sync_level)});
    return try out.toOwnedSlice();
}

fn syncLevelName(sync_level: db_mod.types.SyncLevel) []const u8 {
    return switch (sync_level) {
        .propose => "propose",
        .write => "write",
        .full_text => "full_text",
        .enrichments => "enrichments",
        .full_index => "full_index",
    };
}

fn parseInternalU64(value: std.json.Value) !u64 {
    return switch (value) {
        .integer => |number| if (number >= 0) @intCast(number) else error.InvalidBatchRequest,
        .number_string, .string => |text| blk: {
            if (text.len == 0) break :blk error.InvalidBatchRequest;
            for (text) |byte| if (byte < '0' or byte > '9') break :blk error.InvalidBatchRequest;
            break :blk std.fmt.parseUnsigned(u64, text, 10) catch error.InvalidBatchRequest;
        },
        else => error.InvalidBatchRequest,
    };
}

fn parseInserts(alloc: std.mem.Allocator, value: std.json.Value) ![]db_mod.types.BatchWrite {
    if (value != .object) return error.InvalidBatchRequest;
    const inserts = value.object;
    const writes = try alloc.alloc(db_mod.types.BatchWrite, inserts.count());
    var initialized: usize = 0;
    errdefer {
        for (writes[0..initialized]) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        alloc.free(writes);
    }

    var it = inserts.iterator();
    while (it.next()) |entry| {
        writes[initialized] = .{
            .key = try alloc.dupe(u8, entry.key_ptr.*),
            .value = try std.json.Stringify.valueAlloc(alloc, entry.value_ptr.*, .{}),
        };
        initialized += 1;
    }
    return writes;
}

fn parseDeletes(alloc: std.mem.Allocator, value: std.json.Value) ![][]const u8 {
    if (value != .array) return error.InvalidBatchRequest;
    const values = value.array.items;
    const deletes = try alloc.alloc([]const u8, values.len);
    var initialized: usize = 0;
    errdefer {
        for (deletes[0..initialized]) |key| alloc.free(key);
        alloc.free(deletes);
    }
    for (values) |item| {
        if (item != .string) return error.InvalidBatchRequest;
        deletes[initialized] = try alloc.dupe(u8, item.string);
        initialized += 1;
    }
    return deletes;
}

fn parseTransforms(alloc: std.mem.Allocator, value: std.json.Value) ![]db_mod.types.DocumentTransform {
    if (value != .array) return error.InvalidBatchRequest;
    const values = value.array.items;
    const transforms = try alloc.alloc(db_mod.types.DocumentTransform, values.len);
    var initialized: usize = 0;
    errdefer {
        freeTransforms(alloc, transforms[0..initialized]);
        alloc.free(transforms);
    }

    for (values) |item| {
        if (item != .object) return error.InvalidBatchRequest;
        const key_value = item.object.get("key") orelse return error.InvalidBatchRequest;
        if (key_value != .string) return error.InvalidBatchRequest;
        const operations_value = item.object.get("operations") orelse return error.InvalidBatchRequest;
        const operations = try parseTransformOps(alloc, operations_value);
        errdefer freeTransformOps(alloc, operations);
        const key = try alloc.dupe(u8, key_value.string);
        errdefer alloc.free(key);
        const upsert = if (item.object.get("upsert")) |upsert_value| blk: {
            if (upsert_value == .null) break :blk false;
            if (upsert_value != .bool) return error.InvalidBatchRequest;
            break :blk upsert_value.bool;
        } else false;

        transforms[initialized] = .{
            .key = key,
            .operations = operations,
            .upsert = upsert,
        };
        initialized += 1;
    }
    return transforms;
}

fn parseTransformOps(alloc: std.mem.Allocator, value: std.json.Value) ![]db_mod.types.TransformOp {
    if (value != .array) return error.InvalidBatchRequest;
    const values = value.array.items;
    const ops = try alloc.alloc(db_mod.types.TransformOp, values.len);
    var initialized: usize = 0;
    errdefer {
        freeTransformOps(alloc, ops[0..initialized]);
        alloc.free(ops);
    }

    for (values) |item| {
        if (item != .object) return error.InvalidBatchRequest;
        const op_value = item.object.get("op") orelse return error.InvalidBatchRequest;
        if (op_value != .string) return error.InvalidBatchRequest;
        const path_value = item.object.get("path") orelse return error.InvalidBatchRequest;
        if (path_value != .string) return error.InvalidBatchRequest;
        const value_json = if (item.object.get("value")) |raw| try std.json.Stringify.valueAlloc(alloc, raw, .{}) else null;
        errdefer if (value_json) |json| alloc.free(json);
        const op = try transformOpTypeFromString(op_value.string);
        const path = try alloc.dupe(u8, path_value.string);
        errdefer alloc.free(path);
        ops[initialized] = .{
            .op = op,
            .path = path,
            .value_json = value_json,
        };
        initialized += 1;
    }
    return ops;
}

fn transformOpTypeFromString(op: []const u8) !db_mod.types.TransformOpType {
    if (std.mem.eql(u8, op, "$set")) return .set;
    if (std.mem.eql(u8, op, "$setOnInsert")) return .set_on_insert;
    if (std.mem.eql(u8, op, "$set_on_insert")) return .set_on_insert;
    if (std.mem.eql(u8, op, "$unset")) return .unset;
    if (std.mem.eql(u8, op, "$inc")) return .inc;
    if (std.mem.eql(u8, op, "$push")) return .push;
    if (std.mem.eql(u8, op, "$pull")) return .pull;
    if (std.mem.eql(u8, op, "$addToSet")) return .add_to_set;
    if (std.mem.eql(u8, op, "$add_to_set")) return .add_to_set;
    if (std.mem.eql(u8, op, "$pop")) return .pop;
    if (std.mem.eql(u8, op, "$mul")) return .mul;
    if (std.mem.eql(u8, op, "$min")) return .min;
    if (std.mem.eql(u8, op, "$max")) return .max;
    if (std.mem.eql(u8, op, "$currentDate")) return .current_date;
    if (std.mem.eql(u8, op, "$current_date")) return .current_date;
    if (std.mem.eql(u8, op, "$rename")) return .rename;
    return error.InvalidBatchRequest;
}

fn syncLevelFromValue(value: std.json.Value) !db_mod.types.SyncLevel {
    if (value != .string) return error.InvalidBatchRequest;
    const level = value.string;
    if (std.mem.eql(u8, level, "propose")) return .propose;
    if (std.mem.eql(u8, level, "write")) return .write;
    if (std.mem.eql(u8, level, "full_text")) return .full_text;
    if (std.mem.eql(u8, level, "enrichments")) return .enrichments;
    if (std.mem.eql(u8, level, "full_index")) return .full_index;
    return error.InvalidBatchRequest;
}

fn freeWrites(alloc: std.mem.Allocator, writes: []db_mod.types.BatchWrite) void {
    for (writes) |write| {
        alloc.free(@constCast(write.key));
        alloc.free(@constCast(write.value));
    }
    if (writes.len > 0) alloc.free(writes);
}

fn freeDeletes(alloc: std.mem.Allocator, deletes: [][]const u8) void {
    for (deletes) |key| alloc.free(key);
    if (deletes.len > 0) alloc.free(deletes);
}

fn freeTransforms(alloc: std.mem.Allocator, transforms: []db_mod.types.DocumentTransform) void {
    for (transforms) |transform| {
        alloc.free(@constCast(transform.key));
        freeTransformOps(alloc, transform.operations);
    }
}

fn freeTransformOps(alloc: std.mem.Allocator, ops: []const db_mod.types.TransformOp) void {
    for (ops) |op| {
        alloc.free(@constCast(op.path));
        if (op.value_json) |value_json| alloc.free(@constCast(value_json));
    }
    if (ops.len > 0) alloc.free(@constCast(ops));
}

test "batch parser accepts inserts and deletes" {
    var owned = try parseBatchRequest(std.testing.allocator,
        \\{"inserts":{"doc:a":{"title":"alpha"}},"deletes":["doc:b"]}
    );
    defer owned.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), owned.writes.len);
    try std.testing.expectEqual(@as(usize, 1), owned.deletes.len);
}

test "batch parser preserves oversized value errors" {
    const body =
        \\{"inserts":{"doc:a":{"raw_payload":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}}
    ;
    try std.testing.expectError(error.ValueTooLong, parseBatchRequestWithOptions(std.testing.allocator, body, .{ .allocate = .alloc_always, .max_value_len = 64 }, false));
}

test "internal batch parser owns and round trips split checkpoint" {
    const body =
        \\{"inserts":{},"deletes":[],"_split_checkpoint":{"kind":"destination","transition_id":40,"source_group_id":41,"destination_group_id":42,"range_start":"doc:m","range_end":"doc:z","delta_sequence":7}}
    ;
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(std.testing.allocator, body));

    var owned = try parseInternalBatchRequest(std.testing.allocator, body);
    defer owned.deinit(std.testing.allocator);
    const checkpoint = owned.req.split_checkpoint orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(db_mod.types.SplitReplicationCheckpoint.Kind.destination, checkpoint.kind);
    try std.testing.expectEqual(@as(u64, 40), checkpoint.transition_id);
    try std.testing.expectEqual(@as(u64, 41), checkpoint.source_group_id);
    try std.testing.expectEqual(@as(u64, 42), checkpoint.destination_group_id);
    try std.testing.expectEqualStrings("doc:m", checkpoint.range_start);
    try std.testing.expectEqualStrings("doc:z", checkpoint.range_end);
    try std.testing.expectEqual(@as(u64, 7), checkpoint.delta_sequence);

    const encoded = try encodeBatchRequest(std.testing.allocator, owned.req);
    defer std.testing.allocator.free(encoded);
    var reparsed = try parseInternalBatchRequest(std.testing.allocator, encoded);
    defer reparsed.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 7), reparsed.req.split_checkpoint.?.delta_sequence);
}

test "internal batch parser requires source acknowledgements to be metadata-only" {
    const mixed =
        \\{"inserts":{"doc:m":{}},"_split_checkpoint":{"kind":"source_ack","transition_id":40,"source_group_id":41,"destination_group_id":42,"delta_sequence":7}}
    ;
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(std.testing.allocator, mixed));
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(std.testing.allocator, .{
        .writes = &.{.{ .key = "doc:m", .value = "{}" }},
        .split_checkpoint = .{
            .kind = .source_ack,
            .transition_id = 40,
            .source_group_id = 41,
            .destination_group_id = 42,
            .delta_sequence = 7,
        },
    }));
}

test "internal batch parser rejects public split replication identity" {
    const body =
        \\{"inserts":{"doc:m":{}},"_split_replication":{"transition_id":40,"source_group_id":41,"destination_group_id":42,"namespace_table_id":7,"namespace_shard_id":41,"namespace_range_id":4100}}
    ;
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(std.testing.allocator, body));

    var owned = try parseInternalBatchRequest(std.testing.allocator, body);
    defer owned.deinit(std.testing.allocator);
    const replication = owned.req.split_replication orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 40), replication.transition_id);
    try std.testing.expectEqual(@as(u64, 41), replication.source_group_id);
    try std.testing.expectEqual(@as(u64, 42), replication.destination_group_id);
    try std.testing.expectEqual(@as(u64, 4100), replication.identity_namespace.range_id);
}

test "internal batch split identity round trips the full u64 id space" {
    const max = std.math.maxInt(u64);
    const encoded = try encodeBatchRequest(std.testing.allocator, .{
        .writes = &.{.{ .key = "doc:m", .value = "{}" }},
        .split_replication = .{
            .transition_id = max - 5,
            .source_group_id = max - 4,
            .destination_group_id = max - 3,
            .identity_namespace = .{
                .table_id = max,
                .shard_id = max - 1,
                .range_id = max - 2,
            },
        },
    });
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"namespace_table_id\":\"18446744073709551615\"") != null);

    var parsed = try parseInternalBatchRequest(std.testing.allocator, encoded);
    defer parsed.deinit(std.testing.allocator);
    const replication = parsed.req.split_replication orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(max - 5, replication.transition_id);
    try std.testing.expectEqual(max - 4, replication.source_group_id);
    try std.testing.expectEqual(max - 3, replication.destination_group_id);
    try std.testing.expectEqual(max, replication.identity_namespace.table_id);
    try std.testing.expectEqual(max - 1, replication.identity_namespace.shard_id);
    try std.testing.expectEqual(max - 2, replication.identity_namespace.range_id);
}

test "internal batch parser owns and round trips split transition" {
    const body =
        \\{"_split_transition":{"kind":"start","transition_id":40,"destination_group_id":42,"split_key":"doc:m"}}
    ;
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(std.testing.allocator, body));

    var owned = try parseInternalBatchRequest(std.testing.allocator, body);
    defer owned.deinit(std.testing.allocator);
    const transition = owned.req.split_transition orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(db_mod.types.SplitTransitionMutation.Kind.start, transition.kind);
    try std.testing.expectEqual(@as(u64, 40), transition.transition_id);
    try std.testing.expectEqual(@as(u64, 42), transition.destination_group_id);
    try std.testing.expectEqualStrings("doc:m", transition.split_key);

    const encoded = try encodeBatchRequest(std.testing.allocator, owned.req);
    defer std.testing.allocator.free(encoded);
    var reparsed = try parseInternalBatchRequest(std.testing.allocator, encoded);
    defer reparsed.deinit(std.testing.allocator);
    try std.testing.expectEqual(db_mod.types.SplitTransitionMutation.Kind.start, reparsed.req.split_transition.?.kind);
}

test "internal batch parser rejects mixed split transition commands" {
    const body =
        \\{"inserts":{"doc:m":{}},"_split_transition":{"kind":"prepare","transition_id":40,"destination_group_id":42,"split_key":"doc:m"}}
    ;
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(std.testing.allocator, body));
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(std.testing.allocator, .{
        .writes = &.{.{ .key = "doc:m", .value = "{}" }},
        .split_transition = .{
            .kind = .prepare,
            .transition_id = 40,
            .destination_group_id = 42,
            .split_key = "doc:m",
        },
    }));
}

test "batch parser accepts raw payload value under public request cap" {
    const alloc = std.testing.allocator;
    const payload = try alloc.alloc(u8, 6 * 1024 * 1024);
    defer alloc.free(payload);
    @memset(payload, 'x');

    var out: std.Io.Writer.Allocating = .init(alloc);
    defer out.deinit();
    const writer = &out.writer;

    try writer.writeAll("{\"inserts\":{\"doc:a\":{\"raw_payload\":\"");
    try writer.writeAll(payload);
    try writer.writeAll("\"}}}");

    var owned = try parseBatchRequest(alloc, out.written());
    defer owned.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), owned.writes.len);
    try std.testing.expect(std.mem.indexOf(u8, owned.writes[0].value, "\"raw_payload\"") != null);
}

test "batch parser rejects removed aknn sync level" {
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(std.testing.allocator,
        \\{"inserts":{"doc:a":{"title":"alpha"}},"sync_level":"aknn"}
    ));
}

test "batch parser accepts transforms" {
    var owned = try parseBatchRequest(std.testing.allocator,
        \\{"transforms":[{"key":"doc:a","operations":[{"op":"$max","path":"version","value":3},{"op":"$set","path":"status","value":"updated"}],"upsert":true}]}
    );
    defer owned.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), owned.transforms.len);
    try std.testing.expect(owned.transforms[0].upsert);
    try std.testing.expectEqual(db_mod.types.TransformOpType.max, owned.transforms[0].operations[0].op);
    try std.testing.expectEqualStrings("version", owned.transforms[0].operations[0].path);
}

test "batch parser accepts Go transform op spelling" {
    var owned = try parseBatchRequest(std.testing.allocator,
        \\{"transforms":[{"key":"doc:a","operations":[{"op":"$addToSet","path":"tags","value":"zig"},{"op":"$currentDate","path":"updated_at"}]}]}
    );
    defer owned.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), owned.transforms.len);
    try std.testing.expectEqual(@as(usize, 2), owned.transforms[0].operations.len);
    try std.testing.expectEqual(db_mod.types.TransformOpType.add_to_set, owned.transforms[0].operations[0].op);
    try std.testing.expectEqual(db_mod.types.TransformOpType.current_date, owned.transforms[0].operations[1].op);
}

test "batch parser preserves packed embeddings for mapper extraction" {
    var owned = try parseBatchRequest(std.testing.allocator,
        \\{"inserts":{"doc:a":{"title":"alpha","_embeddings":{"dense_idx":"AACAPwAAAEAAAEBA","sparse_idx":{"packed_indices":"AQAAAAUAAAA=","packed_values":"AAAAPwAAQD8="}}}}}
    );
    defer owned.deinit(std.testing.allocator);

    var extracted = try document_mapper.extractWrite(std.testing.allocator, owned.writes[0].key, owned.writes[0].value);
    defer extracted.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), extracted.dense_embeddings.len);
    try std.testing.expectEqual(@as(usize, 1), extracted.sparse_embeddings.len);
    try std.testing.expect(extracted.cleaned_value != null);
    try std.testing.expect(std.mem.indexOf(u8, extracted.cleaned_value.?, "\"title\":\"alpha\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, extracted.cleaned_value.?, "_embeddings") == null);
}

test "batch parser accepts compact vdbbench-shaped embeddings batch" {
    const alloc = std.testing.allocator;
    var out: std.Io.Writer.Allocating = .init(alloc);
    defer out.deinit();
    const writer = &out.writer;

    try writer.writeAll("{\"inserts\":{");
    for (0..500) |i| {
        if (i != 0) try writer.writeByte(',');
        try writer.print(
            "\"key:{d}\":{{\"id\":{d},\"metadata\":{{\"source\":\"vdbbench\",\"ordinal\":{d}}},\"vec_data\":[0.1,0.2,0.3],\"_embeddings\":{{\"vec\":[0.1,0.2,0.3]}}}}",
            .{ i, i, i },
        );
    }
    try writer.writeAll("},\"sync_level\":\"write\"}");

    var owned = try parseBatchRequest(alloc, out.written());
    defer owned.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 500), owned.writes.len);
    try std.testing.expectEqual(db_mod.types.SyncLevel.write, owned.req.sync_level);

    var extracted = try document_mapper.extractWrite(alloc, owned.writes[0].key, owned.writes[0].value);
    defer extracted.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), extracted.dense_embeddings.len);
    try std.testing.expectEqualStrings("vec", extracted.dense_embeddings[0].index_name);
    try std.testing.expectEqual(@as(usize, 3), extracted.dense_embeddings[0].vector.len);
    try std.testing.expect(extracted.cleaned_value != null);
    try std.testing.expect(std.mem.indexOf(u8, extracted.cleaned_value.?, "\"vec_data\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, extracted.cleaned_value.?, "_embeddings") == null);
}
