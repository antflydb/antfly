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
    });
}

fn parseBatchRequestWithOptions(alloc: std.mem.Allocator, body: []const u8, options: std.json.ParseOptions) !OwnedBatchRequest {
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

    return .{
        .writes = writes,
        .deletes = deletes,
        .transforms = transforms,
        .req = .{
            .writes = writes,
            .deletes = deletes,
            .transforms = transforms,
            .sync_level = sync_level,
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
    try writer.print(",\"sync_level\":\"{s}\"}}", .{syncLevelName(req.sync_level)});
    return try out.toOwnedSlice();
}

fn syncLevelName(sync_level: db_mod.types.SyncLevel) []const u8 {
    return switch (sync_level) {
        .propose => "propose",
        .write => "write",
        .full_text => "full_text",
        .enrichments => "enrichments",
        .aknn => "aknn",
        .full_index => "full_index",
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
    if (std.mem.eql(u8, level, "aknn")) return .full_index;
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
    try std.testing.expectError(error.ValueTooLong, parseBatchRequestWithOptions(std.testing.allocator, body, .{ .allocate = .alloc_always, .max_value_len = 64 }));
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

test "batch parser accepts go sync levels" {
    var owned = try parseBatchRequest(std.testing.allocator,
        \\{"inserts":{"doc:a":{"title":"alpha"}},"sync_level":"aknn"}
    );
    defer owned.deinit(std.testing.allocator);
    try std.testing.expectEqual(db_mod.types.SyncLevel.full_index, owned.req.sync_level);
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
