// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
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
    schema_version: u32 = 1,
    request: db_types.BatchRequest,
};

pub const MetadataMutationKind = enum {
    schema,
};

pub const MetadataMutationPayload = struct {
    schema_version: u32 = 1,
    kind: MetadataMutationKind,
    schema_bytes: []const u8,
    local_schema_json: ?[]const u8 = null,
    lite_sql_table_record_json: ?[]const u8 = null,
};

pub const SchemaMetadataMutationOptions = struct {
    local_schema_json: ?[]const u8 = null,
    lite_sql_table_record_json: ?[]const u8 = null,
};

pub fn encodeBatchMutationRequestAlloc(
    alloc: Allocator,
    request: db_types.BatchRequest,
) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, BatchMutationPayload{
        .request = request,
    }, .{});
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
    if (parsed.value.schema_version != 1) return error.UnsupportedBatchMutationPayloadVersion;
    return parsed;
}

pub fn encodeSchemaMetadataMutationAlloc(
    alloc: Allocator,
    schema: schema_mod.TableSchema,
) ![]u8 {
    return try encodeSchemaMetadataMutationWithOptionsAlloc(alloc, schema, .{});
}

pub fn encodeSchemaMetadataMutationWithOptionsAlloc(
    alloc: Allocator,
    schema: schema_mod.TableSchema,
    options: SchemaMetadataMutationOptions,
) ![]u8 {
    const schema_bytes = try schema_mod.serializeSchema(alloc, schema);
    defer alloc.free(schema_bytes);
    return try std.json.Stringify.valueAlloc(alloc, MetadataMutationPayload{
        .kind = .schema,
        .schema_bytes = schema_bytes,
        .local_schema_json = options.local_schema_json,
        .lite_sql_table_record_json = options.lite_sql_table_record_json,
    }, .{});
}

pub fn appendSchemaMetadataMutation(
    alloc: Allocator,
    primary: *primary_mod.Primary,
    schema: schema_mod.TableSchema,
    options: AppendMetadataMutationOptions,
) !u64 {
    return try appendSchemaMetadataMutationWithPayloadOptions(alloc, primary, schema, .{}, options);
}

pub fn appendSchemaMetadataMutationWithPayloadOptions(
    alloc: Allocator,
    primary: *primary_mod.Primary,
    schema: schema_mod.TableSchema,
    payload_options: SchemaMetadataMutationOptions,
    options: AppendMetadataMutationOptions,
) !u64 {
    const payload = try encodeSchemaMetadataMutationWithOptionsAlloc(alloc, schema, payload_options);
    defer alloc.free(payload);

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
    if (parsed.value.schema_version != 1) return error.UnsupportedMetadataMutationPayloadVersion;
    return parsed;
}

pub fn decodeSchemaMetadataMutation(
    alloc: Allocator,
    record: replication_record.RecordView,
) !schema_mod.TableSchema {
    var parsed = try decodeMetadataMutation(alloc, record);
    defer parsed.deinit();
    if (parsed.value.kind != .schema) return error.UnsupportedMetadataMutationKind;
    return try schema_mod.deserializeSchema(alloc, parsed.value.schema_bytes);
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

test "storage.ha effects appends derived change journal payload as HA derived effect" {
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

test "storage.ha effects appends db batch mutation payload as HA batch mutation" {
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

test "storage.ha effects appends schema metadata payload as HA metadata mutation" {
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
    }, .{ .commit_timestamp_ns = 9012 });
    try std.testing.expectEqual(@as(u64, 1), lsn);

    var entry = (try primary.log.entryAt(alloc, lsn)) orelse return error.TestExpectedEqual;
    defer entry.deinit(alloc);
    try std.testing.expectEqual(replication_record.RecordKind.metadata_mutation, entry.record.kind);
    try std.testing.expectEqual(replication_record.PayloadCodec.json, entry.record.payload_codec);
    try std.testing.expectEqual(@as(u64, 102), entry.record.cluster_id);
    try std.testing.expectEqual(@as(u64, 9), entry.record.shard_id);
    try std.testing.expectEqual(@as(u64, 13), entry.record.table_id);
    try std.testing.expectEqual(@as(i64, 9012), entry.record.commit_timestamp_ns);

    const decoded = try decodeSchemaMetadataMutation(alloc, entry.record);
    defer schema_mod.freeSchema(alloc, decoded);
    try std.testing.expectEqual(@as(u32, 7), decoded.version);
    try std.testing.expectEqualStrings("doc", decoded.default_type);
    try std.testing.expectEqual(@as(u64, 123), decoded.ttl_duration_ns);
    try std.testing.expectEqualStrings("expires_at", decoded.ttl_field);
}

test "storage.ha effects appends lite sql table metadata in schema metadata payload" {
    const alloc = std.testing.allocator;
    const log_path = try testPath(alloc, "lite-metadata-log");
    defer alloc.free(log_path);
    const slots_path = try testPath(alloc, "lite-metadata-slots");
    defer alloc.free(slots_path);

    var primary = try primary_mod.Primary.open(alloc, log_path.ptr, slots_path.ptr, .{
        .cluster_id = 103,
        .shard_id = 9,
        .table_id = 13,
        .timeline_id = 3,
        .epoch = 4,
    }, .{});
    defer primary.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true}
    ;
    const table_record_json =
        \\{"table_id":42,"name":"usage_records","database_name":"tenant_db","namespace_name":"billing","placement_role":"data","desired_replica_count":1,"schema_json":"{}","indexes_json":"{}"}
    ;
    const lsn = try appendSchemaMetadataMutationWithPayloadOptions(alloc, &primary, .{
        .version = 8,
        .default_type = "row",
    }, .{
        .local_schema_json = schema_json,
        .lite_sql_table_record_json = table_record_json,
    }, .{});
    try std.testing.expectEqual(@as(u64, 1), lsn);

    var entry = (try primary.log.entryAt(alloc, lsn)) orelse return error.TestExpectedEqual;
    defer entry.deinit(alloc);
    var decoded = try decodeMetadataMutation(alloc, entry.record);
    defer decoded.deinit();
    try std.testing.expectEqual(MetadataMutationKind.schema, decoded.value.kind);
    try std.testing.expectEqualStrings(schema_json, decoded.value.local_schema_json orelse return error.TestUnexpectedResult);
    try std.testing.expectEqualStrings(table_record_json, decoded.value.lite_sql_table_record_json orelse return error.TestUnexpectedResult);

    const decoded_schema = try schema_mod.deserializeSchema(alloc, decoded.value.schema_bytes);
    defer schema_mod.freeSchema(alloc, decoded_schema);
    try std.testing.expectEqual(@as(u32, 8), decoded_schema.version);
    try std.testing.expectEqualStrings("row", decoded_schema.default_type);
}

test "storage.ha effects rejects non-derived HA records when decoding derived payloads" {
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

test "storage.ha effects rejects unsupported batch mutation payloads" {
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

test "storage.ha effects rejects unsupported metadata mutation payloads" {
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
        .payload = "{\"schema_version\":2,\"kind\":\"schema\",\"schema_bytes\":\"\"}",
    };
    try std.testing.expectError(
        error.UnsupportedMetadataMutationPayloadVersion,
        decodeMetadataMutation(std.testing.allocator, bad_version),
    );
}

test "storage.ha effects rejects non-binary encoded change records before append" {
    var primary: primary_mod.Primary = undefined;
    try std.testing.expectError(
        error.UnsupportedDerivedEffectPayload,
        appendEncodedDerivedChangeRecord(&primary, "{}", .{}),
    );
}

test "storage.ha effects rejects unsupported derived effect payload codecs" {
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
