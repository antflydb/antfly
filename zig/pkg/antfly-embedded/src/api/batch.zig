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

//! Public mutation decoding and ownership. Private Raft/owner commands live in
//! batch_internal.zig and are never reachable from this decoder's import graph.
const std = @import("std");
const ant_json = @import("antfly-json");
const public_limits = @import("antfly_public_limits");
const mutation = @import("../storage/db/mutation_values.zig");
const validation = @import("../storage/db/mutation_validation.zig");
pub const Request = mutation.PublicBatchRequest;
pub const BatchFailure = struct { code: []const u8, message: []const u8, reason: ?[]const u8 = null, retryable: bool };
pub const BatchResult = struct {
    status: []const u8 = "committed",
    inserted: u32,
    deleted: u32,
    transformed: u32 = 0,
    failure: ?BatchFailure = null,
};
pub const OwnedBatchRequest = struct {
    writes: []mutation.BatchWrite = &.{},
    deletes: [][]const u8 = &.{},
    transforms: []mutation.DocumentTransform = &.{},
    graph_writes: []mutation.GraphEdgeWrite = &.{},
    graph_deletes: []mutation.GraphEdgeDelete = &.{},
    req: Request = .{},
    pub fn deinit(self: *OwnedBatchRequest, alloc: std.mem.Allocator) void {
        freeWrites(alloc, self.writes);
        freeDeletes(alloc, self.deletes);
        freeTransforms(alloc, self.transforms);
        freeGraphWrites(alloc, self.graph_writes);
        freeGraphDeletes(alloc, self.graph_deletes);
        self.* = undefined;
    }
    pub fn result(self: OwnedBatchRequest) BatchResult {
        return .{ .inserted = @intCast(self.writes.len), .deleted = @intCast(self.deletes.len), .transformed = @intCast(self.transforms.len) };
    }
};
pub fn parseBatchRequest(alloc: std.mem.Allocator, body: []const u8) !OwnedBatchRequest {
    if (body.len == 0) return .{};
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{ .parse_numbers = false, .allocate = .alloc_always, .max_value_len = public_limits.max_json_value_len }) catch |err| switch (err) {
        error.OutOfMemory, error.ValueTooLong => return err,
        else => return error.InvalidBatchRequest,
    };
    defer parsed.deinit();
    return parseBatchValue(alloc, parsed.value);
}
/// Decode an existing lossless JSON tree directly. Every retained field is
/// copied, so neither the tree nor its allocator must survive this call.
pub fn parseBatchValue(alloc: std.mem.Allocator, value: std.json.Value) !OwnedBatchRequest {
    if (value != .object) return error.InvalidBatchRequest;
    const root = value.object;
    for (private_fields) |field| if (root.get(field) != null) return error.InvalidBatchRequest;
    var owned: OwnedBatchRequest = .{};
    errdefer owned.deinit(alloc);
    if (root.get("inserts")) |item| if (item != .null) {
        owned.writes = try parseInserts(alloc, item, true);
    };
    if (root.get("deletes")) |item| if (item != .null) {
        owned.deletes = try parseDeletes(alloc, item);
    };
    if (root.get("transforms")) |item| if (item != .null) {
        owned.transforms = try parseTransforms(alloc, item);
    };
    if (root.get("graph_writes")) |item| if (item != .null) {
        owned.graph_writes = try parseGraphWrites(alloc, item);
    };
    if (root.get("graph_deletes")) |item| if (item != .null) {
        owned.graph_deletes = try parseGraphDeletes(alloc, item);
    };
    const sync_level = if (root.get("sync_level")) |item| if (item == .null) mutation.SyncLevel.propose else try syncLevelFromValue(item) else mutation.SyncLevel.propose;
    owned.req = .{ .writes = owned.writes, .deletes = owned.deletes, .transforms = owned.transforms, .graph_writes = owned.graph_writes, .graph_deletes = owned.graph_deletes, .sync_level = sync_level };
    for (owned.writes) |write| if (isReservedControlKey(write.key)) return error.InvalidBatchRequest;
    for (owned.deletes) |key| if (isReservedControlKey(key)) return error.InvalidBatchRequest;
    for (owned.transforms) |item| if (isReservedControlKey(item.key)) return error.InvalidBatchRequest;
    return owned;
}
/// Reserved graph-owner maintenance namespaces are part of admission, not an
/// import of the storage key encoder or cleanup executor.
fn isReservedControlKey(key: []const u8) bool {
    for ([_][]const u8{
        "\x00\x00__metadata__:graph_owner_replay:v1:",
        "\x00\x00__metadata__:graph_endpoint_cleanup:v1:",
        "\x00\x00__metadata__:graph_endpoint_cleanup_active:v1:",
    }) |prefix| if (std.mem.startsWith(u8, key, prefix)) return true;
    return std.mem.eql(u8, key, "\x00\x00__metadata__:graph_endpoint_cleanup_active_count:v1") or std.mem.eql(u8, key, "\x00\x00__metadata__:graph_endpoint_cleanup_generation:v2");
}
pub fn encodeBatchResponse(alloc: std.mem.Allocator, result: BatchResult) ![]u8 {
    return ant_json.valueAlloc(alloc, result, .{ .emit_null_optional_fields = false });
}
const private_fields = [_][]const u8{
    "_activate_range_tracking",
    "_artifact_catalog",
    "_artifact_publication",
    "_artifact_publication_transport",
    "_document_version_scheme",
    "_graph_deletes",
    "_graph_endpoint_cleanup",
    "_graph_endpoint_cleanup_guards",
    "_graph_endpoint_cleanup_planned",
    "_graph_writes",
    "_group_source",
    "_integrity",
    "_integrity_commands",
    "_json_null_fields",
    "_leann_record",
    "_merge_artifacts",
    "_merge_checkpoint",
    "_merge_page",
    "_merge_page_effects",
    "_merge_proof_adoption",
    "_merge_replication",
    "_merge_source_transition",
    "_normalized_graph_rows",
    "_online_source",
    "_predicates",
    "_range_guards",
    "_reject_graph_transform_projections",
    "_relational_activation",
    "_relational_generation_gc",
    "_relational_index_maintenance",
    "_relational_integrity_generation_set",
    "_relational_repair",
    "_relational_retirement",
    "_relational_schema_version",
    "_relational_topology",
    "_resolver_review",
    "_restore_staging",
    "_restore_staging_plan_id",
    "_restore_staging_scope",
    "_row_policy_admitted_at_seconds",
    "_row_policy_database",
    "_row_policy_install_bundle",
    "_row_policy_principal_proof",
    "_row_policy_publication",
    "_schema_version",
    "_split_checkpoint",
    "_split_replication",
    "_split_transition",
    "_timestamp_ns",
    "_transaction",
};
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

fn parseInserts(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    require_document_objects: bool,
) ![]mutation.BatchWrite {
    if (value != .object) return error.InvalidBatchRequest;
    const inserts = value.object;
    const writes = try alloc.alloc(mutation.BatchWrite, inserts.count());
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
        // Public BatchRequest.inserts is map[string]object in OpenAPI. Enforce
        // that contract before storage so every admitted document can be
        // returned through QueryHit._source. Internal decoding stays opaque to
        // preserve replay and movement of durable data written by older nodes.
        if (require_document_objects and entry.value_ptr.* != .object)
            return error.InvalidBatchRequest;
        const key = try alloc.dupe(u8, entry.key_ptr.*);
        errdefer alloc.free(key);
        writes[initialized] = .{
            .key = key,
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

fn requiredObjectString(object: std.json.ObjectMap, name: []const u8) ![]const u8 {
    const value = object.get(name) orelse return error.InvalidBatchRequest;
    if (value != .string) return error.InvalidBatchRequest;
    return value.string;
}

fn optionalObjectU64(object: std.json.ObjectMap, name: []const u8) !u64 {
    const value = object.get(name) orelse return 0;
    return try parseInternalU64(value);
}

fn optionalObjectF64(object: std.json.ObjectMap, name: []const u8, default: f64) !f64 {
    const value = object.get(name) orelse return default;
    return switch (value) {
        .integer => |number| @floatFromInt(number),
        .float => |number| number,
        .number_string, .string => |text| std.fmt.parseFloat(f64, text) catch error.InvalidBatchRequest,
        else => error.InvalidBatchRequest,
    };
}

fn optionalGraphIdentityString(alloc: std.mem.Allocator, object: std.json.ObjectMap, name: []const u8) ![]const u8 {
    const value = object.get(name) orelse return "";
    if (value != .string or value.string.len == 0) return error.InvalidBatchRequest;
    return alloc.dupe(u8, value.string);
}

fn parseGraphWrites(alloc: std.mem.Allocator, value: std.json.Value) ![]mutation.GraphEdgeWrite {
    if (value != .array) return error.InvalidBatchRequest;
    const writes = try alloc.alloc(mutation.GraphEdgeWrite, value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        freeGraphWriteElements(alloc, writes[0..initialized]);
        alloc.free(writes);
    }
    for (value.array.items, 0..) |item, i| {
        if (item != .object) return error.InvalidBatchRequest;
        const metadata_json = if (item.object.get("metadata_json")) |metadata| blk: {
            if (metadata != .string) return error.InvalidBatchRequest;
            break :blk metadata.string;
        } else "";
        const index_name = try alloc.dupe(u8, try requiredObjectString(item.object, "index_name"));
        errdefer alloc.free(index_name);
        const source = try alloc.dupe(u8, try requiredObjectString(item.object, "source"));
        errdefer alloc.free(source);
        const target = try alloc.dupe(u8, try requiredObjectString(item.object, "target"));
        errdefer alloc.free(target);
        const edge_type = try alloc.dupe(u8, try requiredObjectString(item.object, "edge_type"));
        errdefer alloc.free(edge_type);
        const owned_metadata_json = try alloc.dupe(u8, metadata_json);
        errdefer alloc.free(owned_metadata_json);
        const edge_id = try optionalGraphIdentityString(alloc, item.object, "edge_id");
        errdefer if (edge_id.len > 0) alloc.free(edge_id);
        const owner_document = try optionalGraphIdentityString(alloc, item.object, "owner_document");
        errdefer if (owner_document.len > 0) alloc.free(owner_document);
        if (owner_document.len > 0 and edge_id.len == 0) return error.InvalidBatchRequest;
        writes[i] = .{
            .edge_id = edge_id,
            .owner_document = owner_document,
            .index_name = index_name,
            .source = source,
            .target = target,
            .edge_type = edge_type,
            .weight = try optionalObjectF64(item.object, "weight", 1.0),
            .created_at = try optionalObjectU64(item.object, "created_at"),
            .updated_at = try optionalObjectU64(item.object, "updated_at"),
            .metadata_json = owned_metadata_json,
        };
        initialized += 1;
    }
    return writes;
}

fn parseGraphDeletes(alloc: std.mem.Allocator, value: std.json.Value) ![]mutation.GraphEdgeDelete {
    if (value != .array) return error.InvalidBatchRequest;
    const deletes = try alloc.alloc(mutation.GraphEdgeDelete, value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        freeGraphDeleteElements(alloc, deletes[0..initialized]);
        alloc.free(deletes);
    }
    for (value.array.items, 0..) |item, i| {
        if (item != .object) return error.InvalidBatchRequest;
        const index_name = try alloc.dupe(u8, try requiredObjectString(item.object, "index_name"));
        errdefer alloc.free(index_name);
        const source = try alloc.dupe(u8, try requiredObjectString(item.object, "source"));
        errdefer alloc.free(source);
        const target = try alloc.dupe(u8, try requiredObjectString(item.object, "target"));
        errdefer alloc.free(target);
        const edge_type = try alloc.dupe(u8, try requiredObjectString(item.object, "edge_type"));
        errdefer alloc.free(edge_type);
        const edge_id = try optionalGraphIdentityString(alloc, item.object, "edge_id");
        errdefer if (edge_id.len > 0) alloc.free(edge_id);
        const owner_document = try optionalGraphIdentityString(alloc, item.object, "owner_document");
        errdefer if (owner_document.len > 0) alloc.free(owner_document);
        if (owner_document.len > 0 and edge_id.len == 0) return error.InvalidBatchRequest;
        const owner = try optionalGraphIdentityString(alloc, item.object, "owner");
        errdefer if (owner.len > 0) alloc.free(owner);
        if (owner.len > 0 and (owner_document.len > 0 or edge_id.len > 0)) return error.InvalidBatchRequest;
        deletes[i] = .{
            .owner = owner,
            .edge_id = edge_id,
            .owner_document = owner_document,
            .index_name = index_name,
            .source = source,
            .target = target,
            .edge_type = edge_type,
        };
        initialized += 1;
    }
    return deletes;
}

fn parseTransforms(alloc: std.mem.Allocator, value: std.json.Value) ![]mutation.DocumentTransform {
    if (value != .array) return error.InvalidBatchRequest;
    const values = value.array.items;
    const transforms = try alloc.alloc(mutation.DocumentTransform, values.len);
    var initialized: usize = 0;
    errdefer {
        freeTransformElements(alloc, transforms[0..initialized]);
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

fn parseTransformOps(alloc: std.mem.Allocator, value: std.json.Value) ![]mutation.TransformOp {
    if (value != .array) return error.InvalidBatchRequest;
    const values = value.array.items;
    const ops = try alloc.alloc(mutation.TransformOp, values.len);
    var initialized: usize = 0;
    errdefer {
        freeTransformOpElements(alloc, ops[0..initialized]);
        alloc.free(ops);
    }

    for (values) |item| {
        if (item != .object) return error.InvalidBatchRequest;
        const op_value = item.object.get("op") orelse return error.InvalidBatchRequest;
        if (op_value != .string) return error.InvalidBatchRequest;
        const path_value = item.object.get("path") orelse return error.InvalidBatchRequest;
        if (path_value != .string) return error.InvalidBatchRequest;
        const op = try transformOpTypeFromString(op_value.string);
        const value_json = if (item.object.get("value")) |raw| try std.json.Stringify.valueAlloc(alloc, raw, .{}) else null;
        errdefer if (value_json) |json| alloc.free(json);
        const path = try alloc.dupe(u8, path_value.string);
        errdefer alloc.free(path);
        const parsed_op: mutation.TransformOp = .{
            .op = op,
            .path = path,
            .value_json = value_json,
        };
        validation.validateDocumentTransform(alloc, .{
            .key = "",
            .operations = @constCast((&[_]mutation.TransformOp{parsed_op})[0..]),
        }) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidBatchRequest,
        };
        ops[initialized] = parsed_op;
        initialized += 1;
    }
    return ops;
}

fn transformOpTypeFromString(op: []const u8) !mutation.TransformOpType {
    if (std.mem.eql(u8, op, "$set")) return .set;
    if (std.mem.eql(u8, op, "$setOnInsert")) return .set_on_insert;
    if (std.mem.eql(u8, op, "$unset")) return .unset;
    if (std.mem.eql(u8, op, "$inc")) return .inc;
    if (std.mem.eql(u8, op, "$push")) return .push;
    if (std.mem.eql(u8, op, "$pull")) return .pull;
    if (std.mem.eql(u8, op, "$addToSet")) return .add_to_set;
    if (std.mem.eql(u8, op, "$min")) return .min;
    if (std.mem.eql(u8, op, "$max")) return .max;
    return error.InvalidBatchRequest;
}

fn syncLevelFromValue(value: std.json.Value) !mutation.SyncLevel {
    if (value != .string) return error.InvalidBatchRequest;
    const level = value.string;
    if (std.mem.eql(u8, level, "propose")) return .propose;
    if (std.mem.eql(u8, level, "write")) return .write;
    if (std.mem.eql(u8, level, "full_text")) return .full_text;
    if (std.mem.eql(u8, level, "enrichments")) return .enrichments;
    if (std.mem.eql(u8, level, "full_index")) return .full_index;
    return error.InvalidBatchRequest;
}

fn freeWrites(alloc: std.mem.Allocator, writes: []mutation.BatchWrite) void {
    for (writes) |write| {
        alloc.free(@constCast(write.key));
        alloc.free(@constCast(write.value));
        for (write.json_null_fields) |name| alloc.free(name);
        if (write.json_null_fields.len != 0) alloc.free(write.json_null_fields);
    }
    if (writes.len > 0) alloc.free(writes);
}

fn freeDeletes(alloc: std.mem.Allocator, deletes: [][]const u8) void {
    for (deletes) |key| alloc.free(key);
    if (deletes.len > 0) alloc.free(deletes);
}

fn freeTransforms(alloc: std.mem.Allocator, transforms: []mutation.DocumentTransform) void {
    freeTransformElements(alloc, transforms);
    if (transforms.len > 0) alloc.free(transforms);
}

fn freeTransformElements(alloc: std.mem.Allocator, transforms: []mutation.DocumentTransform) void {
    for (transforms) |transform| {
        alloc.free(@constCast(transform.key));
        freeTransformOps(alloc, transform.operations);
    }
}

fn freeGraphWrites(alloc: std.mem.Allocator, writes: []mutation.GraphEdgeWrite) void {
    freeGraphWriteElements(alloc, writes);
    if (writes.len > 0) alloc.free(writes);
}

fn freeGraphWriteElements(alloc: std.mem.Allocator, writes: []mutation.GraphEdgeWrite) void {
    for (writes) |write| {
        alloc.free(@constCast(write.index_name));
        alloc.free(@constCast(write.source));
        alloc.free(@constCast(write.target));
        alloc.free(@constCast(write.edge_type));
        if (write.edge_id.len > 0) alloc.free(@constCast(write.edge_id));
        if (write.owner_document.len > 0) alloc.free(@constCast(write.owner_document));
        alloc.free(@constCast(write.metadata_json));
    }
}

fn freeGraphDeletes(alloc: std.mem.Allocator, deletes: []mutation.GraphEdgeDelete) void {
    freeGraphDeleteElements(alloc, deletes);
    if (deletes.len > 0) alloc.free(deletes);
}

fn freeGraphDeleteElements(alloc: std.mem.Allocator, deletes: []mutation.GraphEdgeDelete) void {
    for (deletes) |delete| {
        alloc.free(@constCast(delete.index_name));
        alloc.free(@constCast(delete.source));
        alloc.free(@constCast(delete.target));
        alloc.free(@constCast(delete.edge_type));
        if (delete.edge_id.len > 0) alloc.free(@constCast(delete.edge_id));
        if (delete.owner_document.len > 0) alloc.free(@constCast(delete.owner_document));
        if (delete.owner.len > 0) alloc.free(@constCast(delete.owner));
    }
}

fn freeTransformOps(alloc: std.mem.Allocator, ops: []const mutation.TransformOp) void {
    freeTransformOpElements(alloc, ops);
    if (ops.len > 0) alloc.free(@constCast(ops));
}

fn freeTransformOpElements(alloc: std.mem.Allocator, ops: []const mutation.TransformOp) void {
    for (ops) |op| {
        alloc.free(@constCast(op.path));
        if (op.value_json) |value_json| alloc.free(@constCast(value_json));
    }
}

test "public batch owns exact number spellings without private authority" {
    const a = std.testing.allocator;
    const input = try a.dupe(u8,
        \\{"inserts":{"row":{"n":9007199254740993,"d":9.007199254740993e15,"z":-0}},"transforms":[{"key":"row","operations":[{"op":"$set","path":"n","value":9007199254740993}]}]}
    );
    defer a.free(input);
    var owned = try parseBatchRequest(a, input);
    defer owned.deinit(a);
    @memset(input, '?');
    try std.testing.expectEqualStrings("row", owned.req.writes[0].key);
    try std.testing.expectEqualStrings("{\"n\":9007199254740993,\"d\":9.007199254740993e15,\"z\":-0}", owned.req.writes[0].value);
    try std.testing.expectEqualStrings("9007199254740993", owned.req.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(mutation.SyncLevel.propose, owned.req.sync_level);
    try std.testing.expect(!@hasField(Request, "transaction"));
    try std.testing.expect(!@hasField(Request, "row_policy_principal_proof"));
    try std.testing.expect(!@hasField(OwnedBatchRequest, "artifact_catalog"));
}

test "public batch rejects every privileged command field and preserves null defaults" {
    const a = std.testing.allocator;
    for (private_fields) |field| {
        const body = try std.fmt.allocPrint(a, "{{\"{s}\":null}}", .{field});
        defer a.free(body);
        try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(a, body));
    }
    var empty = try parseBatchRequest(a, "{\"inserts\":null,\"deletes\":null,\"transforms\":null,\"sync_level\":null}");
    defer empty.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), empty.req.writes.len);
    try std.testing.expectEqual(mutation.SyncLevel.propose, empty.req.sync_level);
    var missing_body = try parseBatchRequest(a, "");
    defer missing_body.deinit(a);
    try std.testing.expectEqual(mutation.SyncLevel.write, missing_body.req.sync_level);
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(a, "{\"inserts\":{\"row\":1}}"));
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(a,
        \\{"graph_deletes":[{"index_name":"g","source":"a","target":"b","edge_type":"R","edge_id":"fact","owner":"fact"}]}
    ));
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(a,
        \\{"transforms":[{"key":"row","operations":[{"op":"$inc","path":"x","value":"bad"}]}]}
    ));
}

test "public batch allocation failures release every public value" {
    const Exercise = struct {
        fn run(a: std.mem.Allocator) !void {
            var owned = try parseBatchRequest(a,
                \\{"inserts":{"row":{"n":9007199254740993}},"deletes":["old"],"transforms":[{"key":"row","operations":[{"op":"$set","path":"x","value":{"y":1}}]}],"graph_writes":[{"index_name":"graph","source":"row","target":"other","edge_type":"link"}],"graph_deletes":[{"index_name":"graph","source":"row","target":"gone","edge_type":"link"}]}
            );
            defer owned.deinit(a);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Exercise.run, .{});
}
