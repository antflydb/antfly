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
const db_mod = @import("../storage/db/selected_root.zig").db;
const descriptor_contract = @import("../storage/kernel_owner_descriptor.zig");
const internal_batch_forwarding = @import("../api/internal_batch_forwarding.zig");

pub const protocol_version = internal_batch_forwarding.raft_batch_protocol_version;
pub const timestamp_protocol_version = internal_batch_forwarding.raft_batch_timestamp_protocol_version;
pub const activation_barrier_protocol_version = internal_batch_forwarding.raft_batch_activation_barrier_protocol_version;

pub const OwnedStorageOwnerDescriptor = struct {
    descriptor: descriptor_contract.Descriptor,

    pub fn view(self: *const OwnedStorageOwnerDescriptor) descriptor_contract.Descriptor {
        return self.descriptor;
    }

    pub fn deinit(self: *OwnedStorageOwnerDescriptor, alloc: std.mem.Allocator) void {
        alloc.free(self.descriptor.schema_json);
        alloc.free(self.descriptor.indexes_json);
        self.* = undefined;
    }
};

pub const OwnedReplicatedBatch = struct {
    table_name: []u8,
    batch: batch_api.OwnedBatchRequest,
    storage_owner_descriptor: ?OwnedStorageOwnerDescriptor = null,
    protocol_barrier_version: ?u16 = null,

    pub fn deinit(self: *OwnedReplicatedBatch, alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.batch.deinit(alloc);
        if (self.storage_owner_descriptor) |*descriptor| descriptor.deinit(alloc);
        self.* = undefined;
    }
};

/// Encodes an irreversible Raft log-format activation point. `batch` is
/// intentionally null: binaries that predate protocol barriers reject the
/// entry and stop applying instead of silently continuing past a format they
/// cannot interpret. Binaries that understand the barrier handle it before
/// parsing the batch payload.
pub fn encodeProtocolBarrier(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    version: u16,
) ![]u8 {
    if (version == 0 or version > timestamp_protocol_version) return error.UnsupportedRaftBatchProtocolVersion;
    return try std.fmt.allocPrint(
        alloc,
        "{{\"table\":{f},\"protocol_barrier\":{d},\"batch\":null}}",
        .{ std.json.fmt(table_name, .{}), version },
    );
}

pub fn encode(alloc: std.mem.Allocator, table_name: []const u8, req: db_mod.types.BatchRequest) ![]u8 {
    return try encodeWithStorageOwnerDescriptor(alloc, table_name, req, null);
}

pub fn encodeWithStorageOwnerDescriptor(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    req: db_mod.types.BatchRequest,
    descriptor: ?descriptor_contract.Descriptor,
) ![]u8 {
    const batch_json = try batch_api.encodeBatchRequest(alloc, req);
    defer alloc.free(batch_json);

    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.print("{{\"table\":{f}", .{std.json.fmt(table_name, .{})});
    if (descriptor) |value| {
        try writer.print(",\"storage_owner\":{f}", .{std.json.fmt(value, .{})});
    }
    try writer.writeAll(",\"batch\":");
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

fn cloneStorageOwnerDescriptor(
    alloc: std.mem.Allocator,
    descriptor: descriptor_contract.Descriptor,
) !OwnedStorageOwnerDescriptor {
    const schema_json = try alloc.dupe(u8, descriptor.schema_json);
    errdefer alloc.free(schema_json);
    return .{ .descriptor = .{
        .lsm_root_generation = descriptor.lsm_root_generation,
        .identity = descriptor.identity,
        .schema_json = schema_json,
        .indexes_json = try alloc.dupe(u8, descriptor.indexes_json),
    } };
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
    if (root.get("protocol_barrier")) |barrier_value| {
        if (barrier_value != .integer or barrier_value.integer <= 0 or
            barrier_value.integer > std.math.maxInt(u16))
        {
            return error.InvalidReplicatedBatch;
        }
        const version: u16 = @intCast(barrier_value.integer);
        if (version > timestamp_protocol_version) return error.UnsupportedRaftBatchProtocolVersion;
        if (batch_value != .null) return error.InvalidReplicatedBatch;
        return .{
            .table_name = table_name,
            .batch = .{},
            .protocol_barrier_version = version,
        };
    }
    const batch_json = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(batch_value, .{})});
    defer alloc.free(batch_json);
    var batch = try batch_api.parseInternalBatchRequest(alloc, batch_json);
    errdefer batch.deinit(alloc);

    var storage_owner_descriptor: ?OwnedStorageOwnerDescriptor = null;
    errdefer if (storage_owner_descriptor) |*descriptor| descriptor.deinit(alloc);
    if (root.get("storage_owner")) |descriptor_value| {
        var parsed_descriptor = try std.json.parseFromValue(
            descriptor_contract.Descriptor,
            alloc,
            descriptor_value,
            .{},
        );
        defer parsed_descriptor.deinit();
        storage_owner_descriptor = try cloneStorageOwnerDescriptor(alloc, parsed_descriptor.value);
    }

    return .{
        .table_name = table_name,
        .batch = batch,
        .storage_owner_descriptor = storage_owner_descriptor,
    };
}

test "raft protocol barrier is fail closed for legacy batch parsers" {
    try std.testing.expect(activation_barrier_protocol_version > timestamp_protocol_version);
    const encoded = try encodeProtocolBarrier(std.testing.allocator, "docs", timestamp_protocol_version);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    const batch_value = parsed.value.object.get("batch") orelse return error.TestExpectedEqual;
    const batch_json = try std.fmt.allocPrint(std.testing.allocator, "{f}", .{std.json.fmt(batch_value, .{})});
    defer std.testing.allocator.free(batch_json);
    try std.testing.expectError(
        error.InvalidBatchRequest,
        batch_api.parseInternalBatchRequest(std.testing.allocator, batch_json),
    );

    var decoded = try decode(std.testing.allocator, encoded);
    defer decoded.deinit(std.testing.allocator);
    try std.testing.expectEqual(timestamp_protocol_version, decoded.protocol_barrier_version.?);
    try std.testing.expectEqual(@as(usize, 0), decoded.batch.req.writes.len);
}

test "raft protocol barrier rejects unsupported future versions" {
    const encoded = try std.fmt.allocPrint(
        std.testing.allocator,
        "{{\"table\":\"docs\",\"protocol_barrier\":{d},\"batch\":null}}",
        .{timestamp_protocol_version + 1},
    );
    defer std.testing.allocator.free(encoded);
    try std.testing.expectError(
        error.UnsupportedRaftBatchProtocolVersion,
        decode(std.testing.allocator, encoded),
    );
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
        .timestamp_ns = 123,
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
    try std.testing.expectEqual(@as(u64, 123), decoded.batch.req.timestamp_ns);
    try std.testing.expectEqual(db_mod.types.SyncLevel.write, decoded.batch.req.sync_level);
}

test "raft batch round trips internal split checkpoint" {
    const encoded = try encode(std.testing.allocator, "docs", .{
        .split_checkpoint = .{
            .kind = .destination_complete,
            .transition_id = 40,
            .attempt_epoch = 1,
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
    try std.testing.expectEqual(db_mod.types.SplitReplicationCheckpoint.Kind.destination_complete, checkpoint.kind);
    try std.testing.expectEqual(@as(u64, 40), checkpoint.transition_id);
    try std.testing.expectEqual(@as(u64, 41), checkpoint.source_group_id);
    try std.testing.expectEqual(@as(u64, 42), checkpoint.destination_group_id);
    try std.testing.expectEqualStrings("doc:m", checkpoint.range_start);
    try std.testing.expectEqualStrings("doc:z", checkpoint.range_end);
    try std.testing.expectEqual(@as(u64, 7), checkpoint.delta_sequence);
}

test "raft batch round trips internal split replication identity" {
    const namespace = db_mod.DocIdentityNamespace{ .table_id = 7, .shard_id = 41, .range_id = 4100 };
    const encoded = try encode(std.testing.allocator, "docs", .{
        .writes = &.{.{ .key = "doc:m", .value = "{}" }},
        .split_replication = .{
            .transition_id = 40,
            .attempt_epoch = 1,
            .source_group_id = 41,
            .destination_group_id = 42,
            .identity_namespace = namespace,
        },
    });
    defer std.testing.allocator.free(encoded);

    var decoded = try decode(std.testing.allocator, encoded);
    defer decoded.deinit(std.testing.allocator);
    const replication = decoded.batch.req.split_replication orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 40), replication.transition_id);
    try std.testing.expectEqual(@as(u64, 41), replication.source_group_id);
    try std.testing.expectEqual(@as(u64, 42), replication.destination_group_id);
    try std.testing.expect(replication.identity_namespace.eql(namespace));
}

test "raft batch round trips deterministic storage owner descriptor" {
    const descriptor = descriptor_contract.Descriptor{
        .lsm_root_generation = 9,
        .identity = .{ .table_id = 7, .shard_id = 42, .range_id = 4200 },
        .schema_json = "{\"fields\":{\"title\":{\"type\":\"string\"}}}",
        .indexes_json = "{\"title\":{\"type\":\"full_text\"}}",
    };
    const encoded = try encodeWithStorageOwnerDescriptor(
        std.testing.allocator,
        "docs",
        .{ .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }} },
        descriptor,
    );
    defer std.testing.allocator.free(encoded);

    var decoded = try decode(std.testing.allocator, encoded);
    defer decoded.deinit(std.testing.allocator);
    const actual = decoded.storage_owner_descriptor orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(descriptor.lsm_root_generation, actual.descriptor.lsm_root_generation);
    try std.testing.expect(actual.descriptor.identity.eql(descriptor.identity));
    try std.testing.expectEqualStrings(descriptor.schema_json, actual.descriptor.schema_json);
    try std.testing.expectEqualStrings(descriptor.indexes_json, actual.descriptor.indexes_json);
}

test "raft batch round trips deterministic transaction begin" {
    const txn_id: db_mod.types.TxnId = .{1} ** 16;
    const encoded = try encode(std.testing.allocator, "docs", .{
        .transaction = .{ .begin = .{
            .txn_id = txn_id,
            .begin_timestamp = 100,
            .created_at_ns = 200,
            .topology_epoch = 3,
            .participants = &.{ "table2:4:docs:group:7", "table2:5:other:group:8" },
        } },
    });
    defer std.testing.allocator.free(encoded);

    var decoded = try decode(std.testing.allocator, encoded);
    defer decoded.deinit(std.testing.allocator);
    const begin = switch (decoded.batch.req.transaction orelse return error.TestExpectedEqual) {
        .begin => |value| value,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(txn_id, begin.txn_id);
    try std.testing.expectEqual(@as(u64, 200), begin.created_at_ns);
    try std.testing.expectEqualStrings("table2:4:docs:group:7", begin.participants[0]);
}
