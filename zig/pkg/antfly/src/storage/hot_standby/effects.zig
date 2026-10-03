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
const codecs = @import("antfly_local_sources").storage_db_replication_effects;
const Allocator = std.mem.Allocator;
const change_journal = @import("antfly_local_sources").storage_db_derived_change_journal;
pub const primary_effect = codecs.primary_effect;
const db_types = @import("antfly_local_sources").storage_db_types;
const primary_mod = @import("primary.zig");
const replication_record = @import("antfly_local_sources").storage_db_replication_record;
const schema_mod = @import("antfly_local_sources").storage_schema;

var test_path_counter: u64 = 0;

pub const AppendDerivedEffectOptions = codecs.AppendDerivedEffectOptions;

pub const AppendBatchMutationOptions = codecs.AppendBatchMutationOptions;

pub const AppendMetadataMutationOptions = codecs.AppendMetadataMutationOptions;

pub const BatchMutationPayload = codecs.BatchMutationPayload;

/// Page semantics cannot be silently ignored by older standbys. Their V1
/// decoder rejects V2 before applying rows, independently of Raft negotiation.
pub const encodeInitialChildMutationRequestAlloc = codecs.encodeInitialChildMutationRequestAlloc;

pub const MetadataMutationKind = codecs.MetadataMutationKind;

pub const MetadataMutationPayload = codecs.MetadataMutationPayload;

/// Policy installation is an owner Raft decision, not a schema update. The
/// exact signed snapshot and Raft identity travel in one ordered HA record.
pub const encodeRowPolicyMetadataMutationAlloc = codecs.encodeRowPolicyMetadataMutationAlloc;

/// An HA standby must replay the same source-fence cut as the primary, not a
/// generic schema update that would bypass (or fail) FK generation admission.
pub const PublishedChildSchema = codecs.PublishedChildSchema;

pub const encodeBatchMutationRequestAlloc = codecs.encodeBatchMutationRequestAlloc;

pub const encodeGraphRetirementSealMutationRequestAlloc = codecs.encodeGraphRetirementSealMutationRequestAlloc;

pub const encodeRestoreGenerationAdmissionMutationRequestAlloc = codecs.encodeRestoreGenerationAdmissionMutationRequestAlloc;

pub const encodeOnlineSourceMutationRequestAlloc = codecs.encodeOnlineSourceMutationRequestAlloc;

pub const encodeArtifactCatalogMutationRequestAlloc = codecs.encodeArtifactCatalogMutationRequestAlloc;

pub const encodeArtifactPublicationMutationRequestAlloc = codecs.encodeArtifactPublicationMutationRequestAlloc;

pub const encodeArtifactPublicationTransportMutationRequestAlloc = codecs.encodeArtifactPublicationTransportMutationRequestAlloc;

pub const encodeMergeProofAdoptionMutationRequestAlloc = codecs.encodeMergeProofAdoptionMutationRequestAlloc;

pub const encodeRaftBatchMutationRequestAlloc = codecs.encodeRaftBatchMutationRequestAlloc;

pub const encodeBatchMutationWithRestoreBootstrapAlloc = codecs.encodeBatchMutationWithRestoreBootstrapAlloc;

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

pub const decodeBatchMutationRequest = codecs.decodeBatchMutationRequest;

pub const encodeNativeTopologyMutationRequestAlloc = codecs.encodeNativeTopologyMutationRequestAlloc;

/// Inspect only the fixed-size restore completion proof on duplicate replay.
/// Unknown JSON values (notably ordinary row payloads) are scanned without
/// materializing them, so published restore tombstones do not make later
/// large-batch replays allocate a second copy of every row.
pub const decodeRestoreFinishForReplay = codecs.decodeRestoreFinishForReplay;

pub const encodeSchemaMetadataMutationAlloc = codecs.encodeSchemaMetadataMutationAlloc;

pub const encodePublishedChildSchemaMetadataMutationAlloc = codecs.encodePublishedChildSchemaMetadataMutationAlloc;

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

pub const decodeMetadataMutation = codecs.decodeMetadataMutation;

pub const DecodedSchemaMetadataMutation = codecs.DecodedSchemaMetadataMutation;

pub const decodeSchemaMetadataMutation = codecs.decodeSchemaMetadataMutation;

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
    if (!change_journal.looksLikeBinaryRecord(encoded_change_record) and !primary_effect.isPrimaryEffect(encoded_change_record)) {
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

pub const decodeDerivedChangeRecord = codecs.decodeDerivedChangeRecord;

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
    try std.testing.expect(decoded.published_child == null);

    const published_fence: @import("antfly_local_sources").storage_db_relational_integrity_topology_contract.Fence = .{
        .role = .child_generation_source,
        .transition_id = 11,
        .attempt = 1,
        .peer_group_id = 31,
        .owner_group_id = 21,
        .namespace = .{ .table_id = 41, .shard_id = 21, .range_id = 21 },
        .catalog_digest = @splat(1),
    };
    const published = PublishedChildSchema{
        .fence = published_fence,
        .before_schema_json_digest = @splat(2),
        .schema_json_digest = @splat(3),
        .before_catalog_digest = @splat(4),
        .after_catalog_digest = @splat(5),
        .applied_term = 7,
        .applied_index = 11,
    };
    const payload = try encodePublishedChildSchemaMetadataMutationAlloc(alloc, .{ .version = 8, .default_type = "row" }, "{\"version\":8}", published);
    defer alloc.free(payload);
    const published_lsn = try appendEncodedSchemaMetadataMutation(&primary, payload, .{});
    var published_entry = (try primary.log.entryAt(alloc, published_lsn)) orelse return error.TestExpectedEqual;
    defer published_entry.deinit(alloc);
    var decoded_published = try decodeSchemaMetadataMutation(alloc, published_entry.record);
    defer decoded_published.deinit();
    try std.testing.expectEqual(@as(u32, 8), decoded_published.schema.version);
    try std.testing.expect(decoded_published.published_child.?.fence.eql(published_fence));
    try std.testing.expectEqual(@as(u64, 11), decoded_published.published_child.?.applied_index);

    var dual = published;
    dual.fence.role = .child_generation_dual;
    dual.fence.peer_group_id = dual.fence.owner_group_id;
    const dual_payload = try encodePublishedChildSchemaMetadataMutationAlloc(alloc, .{ .version = 8, .default_type = "row" }, "{\"version\":8}", dual);
    defer alloc.free(dual_payload);
    const dual_lsn = try appendEncodedSchemaMetadataMutation(&primary, dual_payload, .{});
    var dual_entry = (try primary.log.entryAt(alloc, dual_lsn)) orelse return error.TestExpectedEqual;
    defer dual_entry.deinit(alloc);
    var decoded_dual = try decodeSchemaMetadataMutation(alloc, dual_entry.record);
    defer decoded_dual.deinit();
    try std.testing.expect(decoded_dual.published_child.?.fence.eql(dual.fence));
    var invalid_dual = dual;
    invalid_dual.fence.role = .child_generation_parent;
    try std.testing.expectError(error.InvalidGenerationPublication, encodePublishedChildSchemaMetadataMutationAlloc(alloc, .{ .version = 8, .default_type = "row" }, "{\"version\":8}", invalid_dual));
    invalid_dual = dual;
    invalid_dual.applied_index = 0;
    try std.testing.expectError(error.InvalidGenerationPublication, encodePublishedChildSchemaMetadataMutationAlloc(alloc, .{ .version = 8, .default_type = "row" }, "{\"version\":8}", invalid_dual));

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

test "storage.hot_standby effects rejects non-binary encoded change records before append" {
    var primary: primary_mod.Primary = undefined;
    try std.testing.expectError(
        error.UnsupportedDerivedEffectPayload,
        appendEncodedDerivedChangeRecord(&primary, "{}", .{}),
    );
}

test {
    _ = codecs;
}
