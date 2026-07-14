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
const batch_api = @import("../api/batch.zig");
const db_mod = @import("../storage/db/mod.zig");

pub const OwnedReplicatedBatch = struct {
    table_name: []u8,
    batch: batch_api.OwnedBatchRequest,

    pub fn deinit(self: *OwnedReplicatedBatch, alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.batch.deinit(alloc);
        self.* = undefined;
    }
};

pub fn encode(alloc: std.mem.Allocator, table_name: []const u8, req: db_mod.types.BatchRequest) ![]u8 {
    const batch_json = try batch_api.encodeBatchRequest(alloc, req);
    defer alloc.free(batch_json);

    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.print("{{\"table\":{f},\"batch\":", .{std.json.fmt(table_name, .{})});
    try writer.writeAll(batch_json);
    try writer.writeByte('}');
    return try out.toOwnedSlice();
}

pub fn looksLikeEnvelope(payload: []const u8) bool {
    const trimmed = std.mem.trim(u8, payload, " \t\r\n");
    if (!std.mem.startsWith(u8, trimmed, "{")) return false;
    return std.mem.indexOf(u8, trimmed, "\"table\"") != null and
        std.mem.indexOf(u8, trimmed, "\"batch\"") != null;
}

pub fn decode(alloc: std.mem.Allocator, payload: []const u8) !OwnedReplicatedBatch {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, payload, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidReplicatedBatch;
    const root = parsed.value.object;
    const table_value = root.get("table") orelse return error.InvalidReplicatedBatch;
    if (table_value != .string) return error.InvalidReplicatedBatch;
    const batch_value = root.get("batch") orelse return error.InvalidReplicatedBatch;

    const table_name = try alloc.dupe(u8, table_value.string);
    errdefer alloc.free(table_name);
    const batch_json = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(batch_value, .{})});
    defer alloc.free(batch_json);
    var batch = try batch_api.parseInternalBatchRequest(alloc, batch_json);
    errdefer batch.deinit(alloc);

    return .{
        .table_name = table_name,
        .batch = batch,
    };
}

test "raft batch envelope detector tolerates whitespace and object field order" {
    try std.testing.expect(looksLikeEnvelope(" \n {\"batch\":{},\"table\":\"docs\"}"));
    try std.testing.expect(looksLikeEnvelope("{\"table\":\"docs\",\"batch\":{}}"));
    try std.testing.expect(!looksLikeEnvelope(""));
    try std.testing.expect(!looksLikeEnvelope("{\"kind\":\"metadata\",\"value\":1}"));
}

test "raft batch round trips table batch payload" {
    const encoded = try encode(std.testing.allocator, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .deletes = &.{"doc:b"},
        .sync_level = .write,
    });
    defer std.testing.allocator.free(encoded);

    var decoded = try decode(std.testing.allocator, encoded);
    defer decoded.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("docs", decoded.table_name);
    try std.testing.expectEqual(@as(usize, 1), decoded.batch.req.writes.len);
    try std.testing.expectEqualStrings("doc:a", decoded.batch.req.writes[0].key);
    try std.testing.expectEqualStrings("{\"title\":\"alpha\"}", decoded.batch.req.writes[0].value);
    try std.testing.expectEqual(@as(usize, 1), decoded.batch.req.deletes.len);
    try std.testing.expectEqualStrings("doc:b", decoded.batch.req.deletes[0]);
    try std.testing.expectEqual(db_mod.types.SyncLevel.write, decoded.batch.req.sync_level);
}

test "raft batch round trips internal split checkpoint" {
    const encoded = try encode(std.testing.allocator, "docs", .{
        .split_checkpoint = .{
            .kind = .destination,
            .source_group_id = 41,
            .destination_group_id = 42,
            .range_start = "doc:m",
            .range_end = "doc:z",
            .delta_sequence = 7,
        },
    });
    defer std.testing.allocator.free(encoded);

    var decoded = try decode(std.testing.allocator, encoded);
    defer decoded.deinit(std.testing.allocator);
    const checkpoint = decoded.batch.req.split_checkpoint orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(db_mod.types.SplitReplicationCheckpoint.Kind.destination, checkpoint.kind);
    try std.testing.expectEqual(@as(u64, 41), checkpoint.source_group_id);
    try std.testing.expectEqual(@as(u64, 42), checkpoint.destination_group_id);
    try std.testing.expectEqualStrings("doc:m", checkpoint.range_start);
    try std.testing.expectEqualStrings("doc:z", checkpoint.range_end);
    try std.testing.expectEqual(@as(u64, 7), checkpoint.delta_sequence);
}
