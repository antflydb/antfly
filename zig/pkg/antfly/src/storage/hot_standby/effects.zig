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

//! Adapters from committed DB effects into the HA replication stream.
//!
//! The HA wire format is a stable replication envelope. Existing DB-specific
//! effect encodings, such as the derived/change journal payload, are nested as
//! payloads instead of becoming the HA record header itself.

const std = @import("std");
const Allocator = std.mem.Allocator;
const change_journal = @import("../db/derived/change_journal.zig");
const db_types = @import("../db/types.zig");
const primary_mod = @import("primary.zig");
const replication_record = @import("replication_record.zig");
const schema_mod = @import("../schema.zig");

var test_path_counter: u64 = 0;

pub const AppendDerivedEffectOptions = struct {
    shard_id: ?u64 = null,
    table_id: ?u64 = null,
    commit_timestamp_ns: i64 = 0,
};

pub const AppendBatchMutationOptions = struct {
    shard_id: ?u64 = null,
    table_id: ?u64 = null,
    commit_timestamp_ns: i64 = 0,
};

pub const AppendMetadataMutationOptions = struct {
    shard_id: ?u64 = null,
    table_id: ?u64 = null,
    commit_timestamp_ns: i64 = 0,
};

pub const BatchMutationPayload = struct {
    /// Original source cut identity, assigned by native apply, never by a caller.
    online_source_applied_index: ?u64 = null,
    restore_staging_bootstrap: ?@import("../db/restore_staging_contract.zig").OwnerBootstrap = null,
    schema_version: u32 = 1,
    request: db_types.BatchRequest,
};

/// Page semantics cannot be silently ignored by older standbys. Their V1
/// decoder rejects V2 before applying rows, independently of Raft negotiation.
fn batchMutationVersion(request: db_types.BatchRequest) u32 {
    if (request.restore_staging != null or request.online_source != null or
        (if (request.merge_page) |page| page.source.retention != null else false) or
        (if (request.merge_checkpoint) |checkpoint| if (checkpoint.page_source) |source| source.retention != null else false else false)) return 7;
    if (request.restore_staging != null or request.online_source != null or
        (if (request.merge_page) |page| page.source.integrity != null else false) or
        (if (request.merge_checkpoint) |checkpoint| if (checkpoint.page_source) |source| source.integrity != null else false else false)) return 6;
    if (if (request.merge_page) |page| page.next_snapshot_position != null else false) return 5;
    if (request.online_source != null or (if (request.merge_page) |page| page.chunk != null else false)) return 4;
    if ((if (request.merge_page) |page| page.source.retention != null else false) or
        (if (request.merge_checkpoint) |checkpoint| if (checkpoint.page_source) |source| source.retention != null else false else false)) return 3;
    if (request.merge_page != null) return 2;
    if (request.merge_checkpoint) |checkpoint|
        if (checkpoint.page_source != null or checkpoint.page_receiver_namespace != null) return 2;
    return 1;
}

fn validatePageFields(request: db_types.BatchRequest) !void {
    try @import("../db/online_source_contract.zig").validateRequest(request);
    try @import("../db/merge_page_contract.zig").validateRequest(request);
    if (request.merge_checkpoint) |checkpoint| {
        if ((checkpoint.page_source != null) != (checkpoint.page_receiver_namespace != null)) return error.InvalidMergePage;
        if (checkpoint.page_source) |source| {
            if (checkpoint.kind != .begin_copy and !(checkpoint.kind == .accept and source.integrity != null)) return error.InvalidMergePage;
            try source.validate();
            const receiver = checkpoint.page_receiver_namespace.?;
            if (receiver.table_id == 0 or receiver.shard_id == 0 or receiver.range_id == 0) return error.InvalidMergePage;
        }
    }
}

test "HA integrity mutations preserve binary keys and absent versus empty guards" {
    const alloc = std.testing.allocator;
    const binary = &[_]u8{ 0, 255, 192, 128, 34 };
    const encoded = try encodeBatchMutationRequestAlloc(alloc, .{
        .relational_schema_version = 7,
        .relational_integrity_generation_set = @splat(255),
        .integrity = &.{
            .{ .routing_key = binary, .key = binary, .kind = .guard },
            .{ .routing_key = binary, .key = binary, .kind = .put, .value = binary, .expected_value = "" },
        },
    });
    defer alloc.free(encoded);
    var parsed = try std.json.parseFromSlice(BatchMutationPayload, alloc, encoded, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(?u32, 7), parsed.value.request.relational_schema_version);
    try std.testing.expectEqual([_]u8{255} ** 32, parsed.value.request.relational_integrity_generation_set.?);
    const operations = parsed.value.request.integrity;
    try std.testing.expectEqualSlices(u8, binary, operations[0].key);
    try std.testing.expect(operations[0].expected_value == null);
    try std.testing.expectEqualSlices(u8, "", operations[1].expected_value.?);
    try std.testing.expectEqualSlices(u8, binary, operations[1].value.?);
}

test "storage.hot_standby merge pages require version two and reject silent downgrade" {
    const alloc = std.testing.allocator;
    const pages = @import("../db/merge_page_contract.zig");
    const namespace = @import("../db/doc_identity_namespace.zig").Namespace{ .table_id = 1, .shard_id = 2, .range_id = 3 };
    var request: db_types.BatchRequest = .{
        .merge_replication = .{
            .transition_id = 4,
            .donor_group_id = 5,
            .receiver_group_id = 6,
            .identity_namespace = namespace,
            .copy_attempt = .{ .donor_term = 1, .sequence = 1 },
        },
        .merge_page = .{
            .source = .{ .namespace = namespace, .pin_digest = @splat(255), .applied_index = 7 },
            .sequence = 1,
            .phase = .cleanup,
            .exhausted = true,
            .digest = @splat(0),
        },
    };
    request.merge_page.?.digest = pages.commandDigest(request);
    const bytes = try encodeBatchMutationRequestAlloc(alloc, request);
    defer alloc.free(bytes);
    var record: replication_record.RecordView = .{
        .kind = .batch_mutation,
        .payload_codec = .json,
        .cluster_id = 1,
        .timeline_id = 1,
        .epoch = 1,
        .lsn = 1,
        .previous_lsn = 0,
        .payload = bytes,
    };
    var decoded = try decodeBatchMutationRequest(alloc, record);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u32, 2), decoded.value.schema_version);
    try std.testing.expectEqualDeep(request.merge_page.?, decoded.value.request.merge_page.?);
    try std.testing.expect(try decodeRestoreFinishForReplay(alloc, record) == null);
    // This is the same version check made by the previous standby decoder.
    const Legacy = struct { schema_version: u32 = 1 };
    var legacy = try std.json.parseFromSlice(Legacy, alloc, bytes, .{ .ignore_unknown_fields = true });
    defer legacy.deinit();
    try std.testing.expect(legacy.value.schema_version != 1);
    const downgraded = try std.json.Stringify.valueAlloc(alloc, BatchMutationPayload{ .request = request }, .{});
    defer alloc.free(downgraded);
    record.payload = downgraded;
    try std.testing.expectError(error.UnsupportedBatchMutationPayloadVersion, decodeBatchMutationRequest(alloc, record));
    try std.testing.expectError(error.UnsupportedBatchMutationPayloadVersion, decodeRestoreFinishForReplay(alloc, record));
    const checkpoint = db_types.BatchRequest{ .merge_checkpoint = .{
        .kind = .begin_copy,
        .transition_id = 4,
        .donor_group_id = 5,
        .receiver_group_id = 6,
        .receiver_base_start = "a",
        .receiver_base_end = "m",
        .merged_start = "a",
        .merged_end = "z",
        .copy_attempt = .{ .donor_term = 1, .sequence = 1 },
        .page_source = request.merge_page.?.source,
        .page_receiver_namespace = namespace,
    } };
    const begin = try encodeBatchMutationRequestAlloc(alloc, checkpoint);
    defer alloc.free(begin);
    record.payload = begin;
    var decoded_begin = try decodeBatchMutationRequest(alloc, record);
    defer decoded_begin.deinit();
    try std.testing.expectEqual(@as(u32, 2), decoded_begin.value.schema_version);
    try std.testing.expectEqualDeep(checkpoint.merge_checkpoint.?, decoded_begin.value.request.merge_checkpoint.?);
    request.merge_page.?.source.retention = .{ .epoch = 1, .after_sequence = 0 };
    request.merge_page.?.digest = pages.commandDigest(request);
    const tail_bound = try encodeBatchMutationRequestAlloc(alloc, request);
    defer alloc.free(tail_bound);
    record.payload = tail_bound;
    var decoded_tail = try decodeBatchMutationRequest(alloc, record);
    defer decoded_tail.deinit();
    try std.testing.expectEqual(@as(u32, 7), decoded_tail.value.schema_version);
    try std.testing.expect(try decodeRestoreFinishForReplay(alloc, record) == null);
    const downgraded_tail = try std.json.Stringify.valueAlloc(alloc, BatchMutationPayload{ .schema_version = 2, .request = request }, .{});
    defer alloc.free(downgraded_tail);
    record.payload = downgraded_tail;
    try std.testing.expectError(error.UnsupportedBatchMutationPayloadVersion, decodeBatchMutationRequest(alloc, record));
    try std.testing.expectError(error.UnsupportedBatchMutationPayloadVersion, decodeRestoreFinishForReplay(alloc, record));
    var bound_checkpoint = checkpoint;
    bound_checkpoint.merge_checkpoint.?.page_source = request.merge_page.?.source;
    const bound = try encodeBatchMutationRequestAlloc(alloc, bound_checkpoint);
    defer alloc.free(bound);
    record.payload = bound;
    var decoded_bound = try decodeBatchMutationRequest(alloc, record);
    defer decoded_bound.deinit();
    try std.testing.expectEqual(@as(u32, 7), decoded_bound.value.schema_version);
    try std.testing.expect(try decodeRestoreFinishForReplay(alloc, record) == null);
}

test "storage.hot_standby merge pages chunk receipts require version four" {
    const alloc = std.testing.allocator;
    const pages = @import("../db/merge_page_contract.zig");
    var request: db_types.BatchRequest = .{
        .writes = &.{.{ .key = "row", .value = "{\"x\":1}" }},
        .merge_replication = .{
            .transition_id = 1,
            .donor_group_id = 2,
            .receiver_group_id = 3,
            .identity_namespace = .{ .table_id = 4, .shard_id = 3, .range_id = 5 },
            .copy_attempt = .{ .donor_term = 1, .sequence = 1 },
        },
        .merge_page = .{
            .source = .{ .namespace = .{ .table_id = 4, .shard_id = 2, .range_id = 6 }, .pin_digest = @splat(1), .applied_index = 9 },
            .sequence = 2,
            .phase = .rows,
            .next = "row",
            .exhausted = true,
            .timestamps = &.{123},
            .digest = @splat(0),
        },
    };
    request.merge_page.?.digest = pages.commandDigest(request);
    const chunker = try pages.RowChunks(db_types.BatchRequest).init(request);
    const chunk = try chunker.requestAt(0);
    const bytes = try encodeBatchMutationRequestAlloc(alloc, chunk);
    defer alloc.free(bytes);
    var record: replication_record.RecordView = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 1, .previous_lsn = 0, .payload = bytes };
    var decoded = try decodeBatchMutationRequest(alloc, record);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u32, 4), decoded.value.schema_version);
    try std.testing.expectEqualDeep(chunk.merge_page.?, decoded.value.request.merge_page.?);
    try std.testing.expect(try decodeRestoreFinishForReplay(alloc, record) == null);
    const downgraded = try std.json.Stringify.valueAlloc(alloc, BatchMutationPayload{ .schema_version = 3, .request = chunk }, .{});
    defer alloc.free(downgraded);
    record.payload = downgraded;
    try std.testing.expectError(error.UnsupportedBatchMutationPayloadVersion, decodeBatchMutationRequest(alloc, record));
    try std.testing.expectError(error.UnsupportedBatchMutationPayloadVersion, decodeRestoreFinishForReplay(alloc, record));

    // Archive locations are durable progress too: an older standby must not
    // acknowledge a page while silently discarding its physical resume point.
    request.merge_page.?.source.retention = .{ .epoch = 1, .after_sequence = 5 };
    request.merge_page.?.next_snapshot_position = .{ .object = 3, .offset = 9007199254740993, .remaining = 1 };
    request.merge_page.?.digest = pages.commandDigest(request);
    const located = try encodeBatchMutationRequestAlloc(alloc, request);
    defer alloc.free(located);
    record.payload = located;
    var parsed_location = try decodeBatchMutationRequest(alloc, record);
    defer parsed_location.deinit();
    try std.testing.expectEqual(@as(u32, 7), parsed_location.value.schema_version);
    try std.testing.expectEqualDeep(request.merge_page, parsed_location.value.request.merge_page);
    try std.testing.expect(try decodeRestoreFinishForReplay(alloc, record) == null);
    for (1..7) |version| {
        const downgrade = try std.json.Stringify.valueAlloc(alloc, BatchMutationPayload{ .schema_version = @intCast(version), .request = request }, .{});
        defer alloc.free(downgrade);
        record.payload = downgrade;
        try std.testing.expectError(error.UnsupportedBatchMutationPayloadVersion, decodeBatchMutationRequest(alloc, record));
        try std.testing.expectError(error.UnsupportedBatchMutationPayloadVersion, decodeRestoreFinishForReplay(alloc, record));
    }
}

test "storage.hot_standby source controls preserve original cut and require version seven" {
    const alloc = std.testing.allocator;
    const request: db_types.BatchRequest = .{ .online_source = .{ .admit = .{ .scope = .{
        .fence = .{ .transition_id = 1, .attempt = 1, .owner_group_id = 2, .peer_group_id = 3, .role = .merge_source, .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 4 }, .catalog_digest = @splat(255) },
        .receiver_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 5 },
        .consumer_epoch = 1,
        .copy_attempt = .{ .donor_term = 1, .sequence = 1 },
    } } } };
    try std.testing.expectError(error.MissingOnlineSourceAppliedIndex, encodeBatchMutationRequestAlloc(alloc, request));
    const bytes = try encodeOnlineSourceMutationRequestAlloc(alloc, request, 9007199254740993);
    defer alloc.free(bytes);
    var record: replication_record.RecordView = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 1, .previous_lsn = 0, .payload = bytes };
    var decoded = try decodeBatchMutationRequest(alloc, record);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u32, 7), decoded.value.schema_version);
    try std.testing.expectEqual(@as(?u64, 9007199254740993), decoded.value.online_source_applied_index);
    try std.testing.expectEqualDeep(request.online_source, decoded.value.request.online_source);
    try std.testing.expect(try decodeRestoreFinishForReplay(alloc, record) == null);
    const missing = try std.json.Stringify.valueAlloc(alloc, BatchMutationPayload{ .schema_version = 7, .request = request }, .{});
    defer alloc.free(missing);
    record.payload = missing;
    try std.testing.expectError(error.MissingOnlineSourceAppliedIndex, decodeBatchMutationRequest(alloc, record));
    try std.testing.expectError(error.MissingOnlineSourceAppliedIndex, decodeRestoreFinishForReplay(alloc, record));
    const downgraded = try std.json.Stringify.valueAlloc(alloc, BatchMutationPayload{ .schema_version = 6, .request = request, .online_source_applied_index = 1 }, .{});
    defer alloc.free(downgraded);
    record.payload = downgraded;
    try std.testing.expectError(error.UnsupportedBatchMutationPayloadVersion, decodeBatchMutationRequest(alloc, record));
    try std.testing.expectError(error.UnsupportedBatchMutationPayloadVersion, decodeRestoreFinishForReplay(alloc, record));
    const finish: db_types.BatchRequest = .{ .restore_staging = .{ .finish = .{ .scope = @splat(3), .phase = .validated } } };
    const restore = try encodeBatchMutationRequestAlloc(alloc, finish);
    defer alloc.free(restore);
    record.payload = restore;
    var restore_decoded = try decodeBatchMutationRequest(alloc, record);
    defer restore_decoded.deinit();
    try std.testing.expectEqual(@as(u32, 7), restore_decoded.value.schema_version);
    try std.testing.expectEqualDeep(finish.restore_staging.?.finish, (try decodeRestoreFinishForReplay(alloc, record)).?);
    const restore_v6 = try std.json.Stringify.valueAlloc(alloc, BatchMutationPayload{ .schema_version = 6, .request = finish }, .{});
    defer alloc.free(restore_v6);
    record.payload = restore_v6;
    try std.testing.expectError(error.UnsupportedBatchMutationPayloadVersion, decodeBatchMutationRequest(alloc, record));
    try std.testing.expectError(error.UnsupportedBatchMutationPayloadVersion, decodeRestoreFinishForReplay(alloc, record));
}

pub const MetadataMutationKind = enum {
    schema,
};

pub const MetadataMutationPayload = struct {
    schema_version: u32 = 2,
    kind: MetadataMutationKind,
    schema_bytes: []const u8,
    public_schema_json: ?[]const u8 = null,
};

pub fn encodeBatchMutationRequestAlloc(
    alloc: Allocator,
    request: db_types.BatchRequest,
) ![]u8 {
    if (request.online_source != null) return error.MissingOnlineSourceAppliedIndex;
    try validatePageFields(request);
    return try std.json.Stringify.valueAlloc(alloc, BatchMutationPayload{
        .schema_version = batchMutationVersion(request),
        .request = request,
    }, .{});
}

pub fn encodeOnlineSourceMutationRequestAlloc(alloc: Allocator, request: db_types.BatchRequest, applied_index: u64) ![]u8 {
    if (request.online_source == null) return error.InvalidOnlineSourceCommand;
    try validatePageFields(request);
    return std.json.Stringify.valueAlloc(alloc, BatchMutationPayload{
        .schema_version = batchMutationVersion(request),
        .request = request,
        .online_source_applied_index = applied_index,
    }, .{});
}

pub fn encodeBatchMutationWithRestoreBootstrapAlloc(alloc: Allocator, request: db_types.BatchRequest, bootstrap: @import("../db/restore_staging_contract.zig").OwnerBootstrap) ![]u8 {
    try validatePageFields(request);
    if (request.restore_staging == null or request.restore_staging.? != .begin or !std.mem.eql(u8, &request.restore_staging.?.begin.digest(), &bootstrap.scope.digest())) return error.InvalidRestoreStagingCommand;
    try bootstrap.validate();
    return std.json.Stringify.valueAlloc(alloc, BatchMutationPayload{ .schema_version = batchMutationVersion(request), .request = request, .restore_staging_bootstrap = bootstrap }, .{});
}

pub fn appendBatchMutationRequest(
    alloc: Allocator,
    primary: *primary_mod.Primary,
    request: db_types.BatchRequest,
    options: AppendBatchMutationOptions,
) !u64 {
    const payload = try encodeBatchMutationRequestAlloc(alloc, request);
    defer alloc.free(payload);

    return try appendEncodedBatchMutationRequest(primary, payload, options);
}

pub fn appendEncodedBatchMutationRequest(
    primary: *primary_mod.Primary,
    payload: []const u8,
    options: AppendBatchMutationOptions,
) !u64 {
    return try primary.append(.{
        .kind = .batch_mutation,
        .payload_codec = .json,
        .shard_id = options.shard_id,
        .table_id = options.table_id,
        .commit_timestamp_ns = options.commit_timestamp_ns,
        .payload = payload,
    });
}

pub fn decodeBatchMutationRequest(
    alloc: Allocator,
    record: replication_record.RecordView,
) !std.json.Parsed(BatchMutationPayload) {
    if (record.kind != .batch_mutation) return error.NotBatchMutationRecord;
    if (record.payload_codec != .json) return error.UnsupportedBatchMutationCodec;
    var parsed = try std.json.parseFromSlice(BatchMutationPayload, alloc, record.payload, .{
        .ignore_unknown_fields = true,
        .allocate = .alloc_always,
    });
    errdefer parsed.deinit();
    if (parsed.value.schema_version != batchMutationVersion(parsed.value.request)) return error.UnsupportedBatchMutationPayloadVersion;
    if ((parsed.value.request.online_source != null) != (parsed.value.online_source_applied_index != null)) return error.MissingOnlineSourceAppliedIndex;
    try validatePageFields(parsed.value.request);
    return parsed;
}

/// Inspect only the fixed-size restore completion proof on duplicate replay.
/// Unknown JSON values (notably ordinary row payloads) are scanned without
/// materializing them, so published restore tombstones do not make later
/// large-batch replays allocate a second copy of every row.
pub fn decodeRestoreFinishForReplay(
    alloc: Allocator,
    record: replication_record.RecordView,
) !?@FieldType(@import("../db/restore_staging_contract.zig").Control, "finish") {
    if (record.kind != .batch_mutation) return error.NotBatchMutationRecord;
    if (record.payload_codec != .json) return error.UnsupportedBatchMutationCodec;
    const Finish = @FieldType(@import("../db/restore_staging_contract.zig").Control, "finish");
    const Projection = struct {
        schema_version: u32 = 1,
        online_source_applied_index: ?u64 = null,
        request: struct {
            restore_staging: ?struct { finish: ?Finish = null } = null,
            online_source: ?struct {} = null,
            merge_page: ?struct { source: struct { retention: ?struct {} = null, integrity: ?struct {} = null }, chunk: ?struct {} = null, next_snapshot_position: ?struct {} = null } = null,
            merge_checkpoint: ?struct { page_source: ?struct { retention: ?struct {} = null, integrity: ?struct {} = null } = null, page_receiver_namespace: ?struct {} = null } = null,
        },
    };
    var parsed = try std.json.parseFromSlice(Projection, alloc, record.payload, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const has_page = parsed.value.request.merge_page != null or if (parsed.value.request.merge_checkpoint) |checkpoint|
        checkpoint.page_source != null or checkpoint.page_receiver_namespace != null
    else
        false;
    const has_source = parsed.value.request.online_source != null;
    if (has_source != (parsed.value.online_source_applied_index != null)) return error.MissingOnlineSourceAppliedIndex;
    const has_tail = (if (parsed.value.request.merge_page) |page| page.source.retention != null else false) or
        (if (parsed.value.request.merge_checkpoint) |checkpoint| if (checkpoint.page_source) |source| source.retention != null else false else false);
    const has_chunk = if (parsed.value.request.merge_page) |page| page.chunk != null else false;
    const has_locator = if (parsed.value.request.merge_page) |page| page.next_snapshot_position != null else false;
    const has_integrity = (if (parsed.value.request.merge_page) |page| page.source.integrity != null else false) or
        (if (parsed.value.request.merge_checkpoint) |checkpoint| if (checkpoint.page_source) |source| source.integrity != null else false else false);
    const has_staging = parsed.value.request.restore_staging != null;
    if (parsed.value.schema_version != @as(u32, if (has_staging or has_source or has_tail) 7 else if (has_integrity) 6 else if (has_locator) 5 else if (has_chunk) 4 else if (has_page) 2 else 1)) return error.UnsupportedBatchMutationPayloadVersion;
    if (has_page or has_source) {
        if (parsed.value.request.restore_staging != null) return error.InvalidMergePage;
        return null;
    }
    return if (parsed.value.request.restore_staging) |command| command.finish else null;
}

pub fn encodeSchemaMetadataMutationAlloc(
    alloc: Allocator,
    schema: schema_mod.TableSchema,
    public_schema_json: ?[]const u8,
) ![]u8 {
    const schema_bytes = try schema_mod.serializeSchema(alloc, schema);
    defer alloc.free(schema_bytes);
    return try std.json.Stringify.valueAlloc(alloc, MetadataMutationPayload{
        .kind = .schema,
        .schema_bytes = schema_bytes,
        .public_schema_json = public_schema_json,
    }, .{});
}

pub fn appendSchemaMetadataMutation(
    alloc: Allocator,
    primary: *primary_mod.Primary,
    schema: schema_mod.TableSchema,
    public_schema_json: ?[]const u8,
    options: AppendMetadataMutationOptions,
) !u64 {
    const payload = try encodeSchemaMetadataMutationAlloc(alloc, schema, public_schema_json);
    defer alloc.free(payload);

    return try appendEncodedSchemaMetadataMutation(primary, payload, options);
}

pub fn appendEncodedSchemaMetadataMutation(
    primary: *primary_mod.Primary,
    payload: []const u8,
    options: AppendMetadataMutationOptions,
) !u64 {
    return try primary.append(.{
        .kind = .metadata_mutation,
        .payload_codec = .json,
        .shard_id = options.shard_id,
        .table_id = options.table_id,
        .commit_timestamp_ns = options.commit_timestamp_ns,
        .payload = payload,
    });
}

pub fn decodeMetadataMutation(
    alloc: Allocator,
    record: replication_record.RecordView,
) !std.json.Parsed(MetadataMutationPayload) {
    if (record.kind != .metadata_mutation) return error.NotMetadataMutationRecord;
    if (record.payload_codec != .json) return error.UnsupportedMetadataMutationCodec;
    var parsed = try std.json.parseFromSlice(MetadataMutationPayload, alloc, record.payload, .{
        .ignore_unknown_fields = true,
        .allocate = .alloc_always,
    });
    errdefer parsed.deinit();
    if (parsed.value.schema_version != 1 and parsed.value.schema_version != 2) return error.UnsupportedMetadataMutationPayloadVersion;
    return parsed;
}

pub const DecodedSchemaMetadataMutation = struct {
    alloc: Allocator,
    schema: schema_mod.TableSchema,
    public_schema_json: ?[]u8,

    pub fn deinit(self: *DecodedSchemaMetadataMutation) void {
        schema_mod.freeSchema(self.alloc, self.schema);
        if (self.public_schema_json) |value| self.alloc.free(value);
        self.* = undefined;
    }
};

pub fn decodeSchemaMetadataMutation(
    alloc: Allocator,
    record: replication_record.RecordView,
) !DecodedSchemaMetadataMutation {
    var parsed = try decodeMetadataMutation(alloc, record);
    defer parsed.deinit();
    if (parsed.value.kind != .schema) return error.UnsupportedMetadataMutationKind;
    const schema = try schema_mod.deserializeSchema(alloc, parsed.value.schema_bytes);
    errdefer schema_mod.freeSchema(alloc, schema);
    const public_schema_json = if (parsed.value.schema_version >= 2)
        if (parsed.value.public_schema_json) |value| try alloc.dupe(u8, value) else null
    else
        null;
    return .{
        .alloc = alloc,
        .schema = schema,
        .public_schema_json = public_schema_json,
    };
}

pub fn appendDerivedChangeRecord(
    alloc: Allocator,
    primary: *primary_mod.Primary,
    record: change_journal.Record,
    options: AppendDerivedEffectOptions,
) !u64 {
    const payload = try change_journal.encodeRecord(alloc, record);
    defer alloc.free(payload);

    return try appendEncodedDerivedChangeRecord(primary, payload, options);
}

pub fn appendEncodedDerivedChangeRecord(
    primary: *primary_mod.Primary,
    encoded_change_record: []const u8,
    options: AppendDerivedEffectOptions,
) !u64 {
    if (!change_journal.looksLikeBinaryRecord(encoded_change_record)) {
        return error.UnsupportedDerivedEffectPayload;
    }

    return try primary.append(.{
        .kind = .derived_effect,
        .payload_codec = .binary,
        .shard_id = options.shard_id,
        .table_id = options.table_id,
        .commit_timestamp_ns = options.commit_timestamp_ns,
        .payload = encoded_change_record,
    });
}

pub fn decodeDerivedChangeRecord(
    alloc: Allocator,
    record: replication_record.RecordView,
) !change_journal.DecodedRecord {
    if (record.kind != .derived_effect) return error.NotDerivedEffectRecord;
    if (record.payload_codec != .binary) return error.UnsupportedDerivedEffectCodec;
    return try change_journal.decodeRecord(alloc, record.payload);
}

fn testPath(alloc: Allocator, comptime name: []const u8) ![:0]u8 {
    const nonce = @atomicRmw(u64, &test_path_counter, .Add, 1, .seq_cst);
    const raw = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-effects-" ++ name ++ "-{d}-{d}",
        .{ std.testing.random_seed, nonce },
    );
    defer alloc.free(raw);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), raw) catch {};
    return try alloc.dupeZ(u8, raw);
}

test "storage.hot_standby effects appends derived change journal payload as HA derived effect" {
    const alloc = std.testing.allocator;
    const log_path = try testPath(alloc, "log");
    defer alloc.free(log_path);
    const slots_path = try testPath(alloc, "slots");
    defer alloc.free(slots_path);

    var primary = try primary_mod.Primary.open(alloc, log_path.ptr, slots_path.ptr, .{
        .cluster_id = 100,
        .shard_id = 7,
        .table_id = 11,
        .timeline_id = 3,
        .epoch = 4,
    }, .{});
    defer primary.close();

    const lsn = try appendDerivedChangeRecord(alloc, &primary, .{
        .sequence = 42,
        .changed_doc_keys = &.{"doc-a"},
        .changed_artifact_keys = &.{"artifact-a"},
        .target_hints = &.{ .dense_vector, .graph },
    }, .{ .commit_timestamp_ns = 1234 });
    try std.testing.expectEqual(@as(u64, 1), lsn);

    var entry = (try primary.log.entryAt(alloc, lsn)) orelse return error.TestExpectedEqual;
    defer entry.deinit(alloc);
    try std.testing.expectEqual(replication_record.RecordKind.derived_effect, entry.record.kind);
    try std.testing.expectEqual(replication_record.PayloadCodec.binary, entry.record.payload_codec);
    try std.testing.expectEqual(@as(u64, 100), entry.record.cluster_id);
    try std.testing.expectEqual(@as(u64, 7), entry.record.shard_id);
    try std.testing.expectEqual(@as(u64, 11), entry.record.table_id);
    try std.testing.expectEqual(@as(i64, 1234), entry.record.commit_timestamp_ns);

    var decoded = try decodeDerivedChangeRecord(alloc, entry.record);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u64, 42), decoded.record.sequence);
    try std.testing.expectEqualStrings("doc-a", decoded.record.changed_doc_keys[0]);
    try std.testing.expectEqualStrings("artifact-a", decoded.record.changed_artifact_keys[0]);
    try std.testing.expectEqual(@as(usize, 2), decoded.record.target_hints.len);
    try std.testing.expectEqual(change_journal.TargetHint.dense_vector, decoded.record.target_hints[0]);
    try std.testing.expectEqual(change_journal.TargetHint.graph, decoded.record.target_hints[1]);
}

test "storage.hot_standby effects appends db batch mutation payload as HA batch mutation" {
    const alloc = std.testing.allocator;
    const log_path = try testPath(alloc, "batch-log");
    defer alloc.free(log_path);
    const slots_path = try testPath(alloc, "batch-slots");
    defer alloc.free(slots_path);

    var primary = try primary_mod.Primary.open(alloc, log_path.ptr, slots_path.ptr, .{
        .cluster_id = 101,
        .shard_id = 8,
        .table_id = 12,
        .timeline_id = 3,
        .epoch = 4,
    }, .{});
    defer primary.close();

    const lsn = try appendBatchMutationRequest(alloc, &primary, .{
        .writes = &.{.{ .key = "doc-a", .value = "{\"title\":\"alpha\"}" }},
        .deletes = &.{"doc-old"},
        .timestamp_ns = 55,
        .sync_level = .write,
    }, .{ .commit_timestamp_ns = 5678 });
    try std.testing.expectEqual(@as(u64, 1), lsn);

    var entry = (try primary.log.entryAt(alloc, lsn)) orelse return error.TestExpectedEqual;
    defer entry.deinit(alloc);
    try std.testing.expectEqual(replication_record.RecordKind.batch_mutation, entry.record.kind);
    try std.testing.expectEqual(replication_record.PayloadCodec.json, entry.record.payload_codec);
    try std.testing.expectEqual(@as(u64, 101), entry.record.cluster_id);
    try std.testing.expectEqual(@as(u64, 8), entry.record.shard_id);
    try std.testing.expectEqual(@as(u64, 12), entry.record.table_id);
    try std.testing.expectEqual(@as(i64, 5678), entry.record.commit_timestamp_ns);

    var decoded = try decodeBatchMutationRequest(alloc, entry.record);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u32, 1), decoded.value.schema_version);
    try std.testing.expectEqual(@as(usize, 1), decoded.value.request.writes.len);
    try std.testing.expectEqualStrings("doc-a", decoded.value.request.writes[0].key);
    try std.testing.expectEqualStrings("{\"title\":\"alpha\"}", decoded.value.request.writes[0].value);
    try std.testing.expectEqual(@as(usize, 1), decoded.value.request.deletes.len);
    try std.testing.expectEqualStrings("doc-old", decoded.value.request.deletes[0]);
    try std.testing.expectEqual(@as(u64, 55), decoded.value.request.timestamp_ns);
    try std.testing.expectEqual(db_types.SyncLevel.write, decoded.value.request.sync_level);
}

test "storage.hot_standby effects appends schema metadata payload as HA metadata mutation" {
    const alloc = std.testing.allocator;
    const log_path = try testPath(alloc, "metadata-log");
    defer alloc.free(log_path);
    const slots_path = try testPath(alloc, "metadata-slots");
    defer alloc.free(slots_path);

    var primary = try primary_mod.Primary.open(alloc, log_path.ptr, slots_path.ptr, .{
        .cluster_id = 102,
        .shard_id = 9,
        .table_id = 13,
        .timeline_id = 3,
        .epoch = 4,
    }, .{});
    defer primary.close();

    const lsn = try appendSchemaMetadataMutation(alloc, &primary, .{
        .version = 7,
        .default_type = "doc",
        .ttl_duration_ns = 123,
        .ttl_field = "expires_at",
    }, "{\"version\":7}", .{ .commit_timestamp_ns = 9012 });
    try std.testing.expectEqual(@as(u64, 1), lsn);

    var entry = (try primary.log.entryAt(alloc, lsn)) orelse return error.TestExpectedEqual;
    defer entry.deinit(alloc);
    try std.testing.expectEqual(replication_record.RecordKind.metadata_mutation, entry.record.kind);
    try std.testing.expectEqual(replication_record.PayloadCodec.json, entry.record.payload_codec);
    try std.testing.expectEqual(@as(u64, 102), entry.record.cluster_id);
    try std.testing.expectEqual(@as(u64, 9), entry.record.shard_id);
    try std.testing.expectEqual(@as(u64, 13), entry.record.table_id);
    try std.testing.expectEqual(@as(i64, 9012), entry.record.commit_timestamp_ns);

    var decoded = try decodeSchemaMetadataMutation(alloc, entry.record);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u32, 7), decoded.schema.version);
    try std.testing.expectEqualStrings("doc", decoded.schema.default_type);
    try std.testing.expectEqual(@as(u64, 123), decoded.schema.ttl_duration_ns);
    try std.testing.expectEqualStrings("expires_at", decoded.schema.ttl_field);
    try std.testing.expectEqualStrings("{\"version\":7}", decoded.public_schema_json.?);

    const legacy_schema_bytes = try schema_mod.serializeSchema(alloc, .{
        .version = 6,
        .default_type = "legacy",
    });
    defer alloc.free(legacy_schema_bytes);
    const LegacyPayload = struct {
        schema_version: u32 = 1,
        kind: MetadataMutationKind = .schema,
        schema_bytes: []const u8,
    };
    const legacy_payload = try std.json.Stringify.valueAlloc(alloc, LegacyPayload{
        .schema_bytes = legacy_schema_bytes,
    }, .{});
    defer alloc.free(legacy_payload);
    const legacy_lsn = try primary.append(.{
        .kind = .metadata_mutation,
        .payload_codec = .json,
        .payload = legacy_payload,
    });
    var legacy_entry = (try primary.log.entryAt(alloc, legacy_lsn)) orelse return error.TestExpectedEqual;
    defer legacy_entry.deinit(alloc);
    var legacy_decoded = try decodeSchemaMetadataMutation(alloc, legacy_entry.record);
    defer legacy_decoded.deinit();
    try std.testing.expectEqual(@as(u32, 6), legacy_decoded.schema.version);
    try std.testing.expectEqual(@as(?[]u8, null), legacy_decoded.public_schema_json);
}

test "storage.hot_standby effects rejects non-derived HA records when decoding derived payloads" {
    const record = replication_record.Record{
        .kind = .batch_mutation,
        .payload_codec = .binary,
        .cluster_id = 1,
        .timeline_id = 1,
        .epoch = 1,
        .lsn = 1,
        .previous_lsn = 0,
        .payload = "",
    };
    try std.testing.expectError(
        error.NotDerivedEffectRecord,
        decodeDerivedChangeRecord(std.testing.allocator, record),
    );
}

test "storage.hot_standby effects rejects unsupported batch mutation payloads" {
    const derived = replication_record.Record{
        .kind = .derived_effect,
        .payload_codec = .json,
        .cluster_id = 1,
        .timeline_id = 1,
        .epoch = 1,
        .lsn = 1,
        .previous_lsn = 0,
        .payload = "{}",
    };
    try std.testing.expectError(
        error.NotBatchMutationRecord,
        decodeBatchMutationRequest(std.testing.allocator, derived),
    );

    const binary = replication_record.Record{
        .kind = .batch_mutation,
        .payload_codec = .binary,
        .cluster_id = 1,
        .timeline_id = 1,
        .epoch = 1,
        .lsn = 1,
        .previous_lsn = 0,
        .payload = "",
    };
    try std.testing.expectError(
        error.UnsupportedBatchMutationCodec,
        decodeBatchMutationRequest(std.testing.allocator, binary),
    );

    const bad_version = replication_record.Record{
        .kind = .batch_mutation,
        .payload_codec = .json,
        .cluster_id = 1,
        .timeline_id = 1,
        .epoch = 1,
        .lsn = 1,
        .previous_lsn = 0,
        .payload = "{\"schema_version\":2,\"request\":{}}",
    };
    try std.testing.expectError(
        error.UnsupportedBatchMutationPayloadVersion,
        decodeBatchMutationRequest(std.testing.allocator, bad_version),
    );
}

test "storage.hot_standby effects rejects unsupported metadata mutation payloads" {
    const batch = replication_record.Record{
        .kind = .batch_mutation,
        .payload_codec = .json,
        .cluster_id = 1,
        .timeline_id = 1,
        .epoch = 1,
        .lsn = 1,
        .previous_lsn = 0,
        .payload = "{}",
    };
    try std.testing.expectError(
        error.NotMetadataMutationRecord,
        decodeMetadataMutation(std.testing.allocator, batch),
    );

    const binary = replication_record.Record{
        .kind = .metadata_mutation,
        .payload_codec = .binary,
        .cluster_id = 1,
        .timeline_id = 1,
        .epoch = 1,
        .lsn = 1,
        .previous_lsn = 0,
        .payload = "",
    };
    try std.testing.expectError(
        error.UnsupportedMetadataMutationCodec,
        decodeMetadataMutation(std.testing.allocator, binary),
    );

    const bad_version = replication_record.Record{
        .kind = .metadata_mutation,
        .payload_codec = .json,
        .cluster_id = 1,
        .timeline_id = 1,
        .epoch = 1,
        .lsn = 1,
        .previous_lsn = 0,
        .payload = "{\"schema_version\":3,\"kind\":\"schema\",\"schema_bytes\":\"\"}",
    };
    try std.testing.expectError(
        error.UnsupportedMetadataMutationPayloadVersion,
        decodeMetadataMutation(std.testing.allocator, bad_version),
    );
}

test "storage.hot_standby effects rejects non-binary encoded change records before append" {
    var primary: primary_mod.Primary = undefined;
    try std.testing.expectError(
        error.UnsupportedDerivedEffectPayload,
        appendEncodedDerivedChangeRecord(&primary, "{}", .{}),
    );
}

test "storage.hot_standby effects rejects unsupported derived effect payload codecs" {
    const record = replication_record.Record{
        .kind = .derived_effect,
        .payload_codec = .json,
        .cluster_id = 1,
        .timeline_id = 1,
        .epoch = 1,
        .lsn = 1,
        .previous_lsn = 0,
        .payload = "{}",
    };
    try std.testing.expectError(
        error.UnsupportedDerivedEffectCodec,
        decodeDerivedChangeRecord(std.testing.allocator, record),
    );
}
