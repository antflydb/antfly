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

//! Portable committed-mutation codecs used by storage and replication adapters.
//! This owner encodes and decodes payloads; it never appends to a runtime log.
//!
//! The HA wire format is a stable replication envelope. Existing DB-specific
//! effect encodings, such as the derived/change journal payload, are nested as
//! payloads instead of becoming the HA record header itself.

const std = @import("std");
const Allocator = std.mem.Allocator;
const change_journal = @import("derived/change_journal.zig");
pub const primary_effect = @import("../hot_standby/primary_effect.zig");
const db_types = @import("types.zig");

const replication_record = @import("../hot_standby/replication_record.zig");
const schema_mod = @import("../schema.zig");

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
    /// Original authority for ordinary rows and coordinated resolutions. Input
    /// provenance cannot substitute a standby-local replay sequence.
    ordinary_raft_entry: ?db_types.RaftAppliedEntryIdentity = null,
    artifact_publication_raft_entry: ?db_types.RaftAppliedEntryIdentity = null,
    artifact_publication_transport_raft_entry: ?db_types.RaftAppliedEntryIdentity = null,
    merge_proof_adoption_raft_entry: ?db_types.RaftAppliedEntryIdentity = null,
    native_topology_position: ?@import("receipt_position.zig").Native = null,
    artifact_catalog_raft_entry: ?db_types.RaftAppliedEntryIdentity = null,
    /// Initial hidden FK owner controls are Raft decisions. Standby replay
    /// must preserve that exact entry identity for owner receipts and reject
    /// an ordinary batch carrying the same private command.
    initial_child_raft_entry: ?db_types.RaftAppliedEntryIdentity = null,
    /// Graph owner seals bind a durable receipt to the primary's exact Raft
    /// apply entry; a standby must replay the same term/index.
    graph_retirement_raft_entry: ?db_types.RaftAppliedEntryIdentity = null,
    /// Imported FK proof becomes target admission only at this exact primary
    /// Raft entry. A standby may not invent a local term/index for its receipt.
    restore_generation_admission_raft_entry: ?db_types.RaftAppliedEntryIdentity = null,
    /// Original source cut identity, assigned by native apply, never by a caller.
    online_source_applied_index: ?u64 = null,
    restore_staging_bootstrap: ?@import("restore_staging_contract.zig").OwnerBootstrap = null,
    schema_version: u32 = 1,
    request: db_types.BatchRequest,
};

/// Page semantics cannot be silently ignored by older standbys. Their V1
/// decoder rejects V2 before applying rows, independently of Raft negotiation.
fn batchMutationVersion(request: db_types.BatchRequest) u32 {
    if (request.merge_proof_adoption != null) return 17;
    if (request.artifact_publication_transport != null) return 16;
    if (request.artifact_publication != null) return 14;
    if (request.artifact_catalog != null) return 12;
    if (request.restore_staging) |command| if (command == .import_page and command.import_page.source_generation_proof_page) return 11;
    if (request.restore_staging) |command| if (command == .install_generation_admissions) return 10;
    if (request.relational_topology) |command| switch (command.action) {
        .provision_initial_child, .release_initial_child, .cancel_initial_child => return 8,
        .seal_graph_retirement => return 9,
        else => {},
    };
    if (request.relational_topology) |command| if (command.action == .begin and command.graph_retirement != null) return 9;
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

test "storage.hot_standby graph retirement begin and seal require versioned standby decoder" {
    const alloc = std.testing.allocator;
    const scope: @import("graph_retirement_seal.zig").Scope = .{
        .fence = .{ .role = .rewrite_source, .transition_id = 7, .attempt = 1, .admission_epoch = 1, .peer_group_id = 401, .owner_group_id = 301, .namespace = .{ .table_id = 9, .shard_id = 301, .range_id = 301 }, .catalog_digest = @splat(4) },
        .plan_id = @splat(1),
        .plan_digest = @splat(2),
        .target_table_id = 10,
        .graph_config_digest = @splat(3),
    };
    inline for (.{ .begin, .seal_graph_retirement }) |action| {
        const request: db_types.BatchRequest = .{ .relational_topology = .{ .action = action, .fence = scope.fence, .graph_retirement = scope } };
        const encoded = if (action == .seal_graph_retirement)
            try encodeGraphRetirementSealMutationRequestAlloc(alloc, request, .{ .term = 2, .index = 3 })
        else
            try encodeBatchMutationRequestAlloc(alloc, request);
        defer alloc.free(encoded);
        var record: replication_record.RecordView = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 1, .previous_lsn = 0, .payload = encoded };
        var decoded = try decodeBatchMutationRequest(alloc, record);
        defer decoded.deinit();
        try std.testing.expectEqual(@as(u32, 9), decoded.value.schema_version);
        try std.testing.expect(decoded.value.request.relational_topology.?.graph_retirement.?.eql(scope));
        if (action == .seal_graph_retirement) {
            try std.testing.expectEqual(@as(u64, 3), decoded.value.graph_retirement_raft_entry.?.index);
            try std.testing.expectError(error.InvalidGraphRetirementSeal, encodeBatchMutationRequestAlloc(alloc, request));
        } else try std.testing.expect(decoded.value.graph_retirement_raft_entry == null);
        const downgraded = try std.json.Stringify.valueAlloc(alloc, BatchMutationPayload{ .request = request }, .{});
        defer alloc.free(downgraded);
        record.payload = downgraded;
        try std.testing.expectError(error.UnsupportedBatchMutationPayloadVersion, decodeBatchMutationRequest(alloc, record));
    }
}

test "storage.hot_standby mapped restore admissions require original Raft receipt identity" {
    const alloc = std.testing.allocator;
    const contract = @import("restore_staging_contract.zig");
    const mappings = [_]contract.GenerationAdmissionMapping{.{
        .source_child_table_id = 11,
        .source_child_table_name = "children",
        .target_child_table_id = 21,
        .target_child_table_name = "children",
        .constraint_name = "parent_fk",
        .source_generation = @splat(1),
        .target_generation = @splat(2),
        .source_scope_digest = @splat(3),
    }};
    const request: db_types.BatchRequest = .{ .restore_staging = .{ .install_generation_admissions = .{
        .scope = @splat(4),
        .source_summary_digest = @splat(5),
        .mappings = &mappings,
    } } };
    try std.testing.expectError(error.InvalidRestoreStagingCommand, encodeBatchMutationRequestAlloc(alloc, request));
    const encoded = try encodeRestoreGenerationAdmissionMutationRequestAlloc(alloc, request, .{ .term = 6, .index = 7 });
    defer alloc.free(encoded);
    var record: replication_record.RecordView = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 1, .previous_lsn = 0, .payload = encoded };
    var decoded = try decodeBatchMutationRequest(alloc, record);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u32, 10), decoded.value.schema_version);
    try std.testing.expectEqual(@as(u64, 6), decoded.value.restore_generation_admission_raft_entry.?.term);
    const missing = try std.json.Stringify.valueAlloc(alloc, BatchMutationPayload{ .schema_version = 10, .request = request }, .{});
    defer alloc.free(missing);
    record.payload = missing;
    try std.testing.expectError(error.InvalidRestoreStagingCommand, decodeBatchMutationRequest(alloc, record));
    const downgraded = try std.json.Stringify.valueAlloc(alloc, BatchMutationPayload{ .schema_version = 7, .request = request }, .{});
    defer alloc.free(downgraded);
    record.payload = downgraded;
    try std.testing.expectError(error.UnsupportedBatchMutationPayloadVersion, decodeBatchMutationRequest(alloc, record));
}

test "storage.hot_standby source generation proof page requires versioned standby decoder" {
    const alloc = std.testing.allocator;
    const request: db_types.BatchRequest = .{ .restore_staging = .{ .import_page = .{
        .expected = @splat(1),
        .next = "proof progress",
        .scope = @splat(2),
        .timestamps = &.{},
        .source_generation_proof_page = true,
        .artifacts = &.{},
    } } };
    const encoded = try encodeBatchMutationRequestAlloc(alloc, request);
    defer alloc.free(encoded);
    var record: replication_record.RecordView = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 1, .previous_lsn = 0, .payload = encoded };
    var decoded = try decodeBatchMutationRequest(alloc, record);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u32, 11), decoded.value.schema_version);
    try std.testing.expect(decoded.value.request.restore_staging.?.import_page.source_generation_proof_page);
    const downgraded = try std.json.Stringify.valueAlloc(alloc, BatchMutationPayload{ .schema_version = 7, .request = request }, .{});
    defer alloc.free(downgraded);
    record.payload = downgraded;
    try std.testing.expectError(error.UnsupportedBatchMutationPayloadVersion, decodeBatchMutationRequest(alloc, record));
}

pub fn encodeInitialChildMutationRequestAlloc(alloc: Allocator, request: db_types.BatchRequest, entry: db_types.RaftAppliedEntryIdentity) ![]u8 {
    if (batchMutationVersion(request) != 8 or entry.term == 0 or entry.index == 0) return error.InvalidInitialChildPublication;
    try validatePageFields(request);
    return std.json.Stringify.valueAlloc(alloc, BatchMutationPayload{
        .schema_version = 8,
        .request = request,
        .initial_child_raft_entry = entry,
    }, .{});
}

test "storage.hot_standby initial hidden FK HA payload preserves exact Raft receipt identity" {
    const alloc = std.testing.allocator;
    const fence: @import("relational_integrity_topology_contract.zig").Fence = .{
        .role = .child_generation_source,
        .transition_id = 7,
        .attempt = 1,
        .admission_epoch = 1,
        .peer_group_id = 9,
        .owner_group_id = 9,
        .namespace = .{ .table_id = 7, .shard_id = 9, .range_id = 9 },
        .catalog_digest = @splat(3),
    };
    const req: db_types.BatchRequest = .{ .relational_topology = .{ .action = .cancel_initial_child, .fence = fence, .initial_child_control = .{
        .plan_id = @splat(1),
        .plan_digest = @splat(2),
        .schema_version = 0,
        .schema_digest = @splat(4),
        .public_schema_json_digest = @splat(5),
        .catalog_digest = @splat(3),
    } } };
    try std.testing.expectError(error.InvalidInitialChildPublication, encodeBatchMutationRequestAlloc(alloc, req));
    const encoded = try encodeInitialChildMutationRequestAlloc(alloc, req, .{ .term = 2, .index = 5 });
    defer alloc.free(encoded);
    var parsed = try std.json.parseFromSlice(BatchMutationPayload, alloc, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u32, 8), parsed.value.schema_version);
    try std.testing.expectEqual(@as(u64, 5), parsed.value.initial_child_raft_entry.?.index);
}

fn validatePageFields(request: db_types.BatchRequest) !void {
    try @import("merge_proof_adoption.zig").validateRequest(request);
    try @import("artifact_inventory.zig").validateRequest(request);
    try @import("online_source_contract.zig").validateRequest(request);
    try @import("merge_page_contract.zig").validateRequest(request);
    if (request.merge_checkpoint) |checkpoint| {
        if ((checkpoint.page_source != null) != (checkpoint.page_receiver_namespace != null)) return error.InvalidMergePage;
        if (checkpoint.page_source) |source| {
            if (checkpoint.kind != .begin_copy and !(checkpoint.kind == .accept and (source.integrity != null or source.artifact_catalog != null))) return error.InvalidMergePage;
            try source.validate();
            const receiver = checkpoint.page_receiver_namespace.?;
            if (receiver.table_id == 0 or receiver.shard_id == 0 or receiver.range_id == 0) return error.InvalidMergePage;
        }
    }
}

test "storage.hot_standby HA integrity mutations preserve binary keys and absent versus empty guards" {
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
    const pages = @import("merge_page_contract.zig");
    const namespace = @import("doc_identity_namespace.zig").Namespace{ .table_id = 1, .shard_id = 2, .range_id = 3 };
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
    const pages = @import("merge_page_contract.zig");
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
    row_policy,
};

pub const MetadataMutationPayload = struct {
    schema_version: u32 = 2,
    kind: MetadataMutationKind,
    schema_bytes: []const u8 = "",
    public_schema_json: ?[]const u8 = null,
    published_child: ?PublishedChildSchema = null,
    row_policy_bundle: ?[]const u8 = null,
    row_policy_request: ?@import("../../system_catalog/policies.zig").InstallRequest = null,
    row_policy_raft_entry: ?db_types.RaftAppliedEntryIdentity = null,
};

/// Policy installation is an owner Raft decision, not a schema update. The
/// exact signed snapshot and Raft identity travel in one ordered HA record.
pub fn encodeRowPolicyMetadataMutationAlloc(
    alloc: Allocator,
    bundle: []const u8,
    request: @import("../../system_catalog/policies.zig").InstallRequest,
    entry: db_types.RaftAppliedEntryIdentity,
) ![]u8 {
    if (bundle.len == 0 or entry.term == 0 or entry.index == 0) return error.InvalidMetadataMutationPayload;
    return std.json.Stringify.valueAlloc(alloc, MetadataMutationPayload{
        .schema_version = 4,
        .kind = .row_policy,
        .row_policy_bundle = bundle,
        .row_policy_request = request,
        .row_policy_raft_entry = entry,
    }, .{});
}

/// An HA standby must replay the same source-fence cut as the primary, not a
/// generic schema update that would bypass (or fail) FK generation admission.
pub const PublishedChildSchema = struct {
    fence: @import("relational_integrity_topology_contract.zig").Fence,
    before_schema_json_digest: [32]u8,
    schema_json_digest: [32]u8,
    before_catalog_digest: [32]u8,
    after_catalog_digest: [32]u8,
    applied_term: u64,
    applied_index: u64,
};

pub fn encodeBatchMutationRequestAlloc(
    alloc: Allocator,
    request: db_types.BatchRequest,
) ![]u8 {
    if (request.artifact_catalog != null) return error.InvalidArtifactCatalogCommand;
    if (request.artifact_publication != null or request.artifact_publication_transport != null or request.merge_proof_adoption != null) return error.InvalidBatchRequest;
    if (batchMutationVersion(request) == 8) return error.InvalidInitialChildPublication;
    if (batchMutationVersion(request) == 10) return error.InvalidRestoreStagingCommand;
    if (request.relational_topology) |command| if (command.action == .seal_graph_retirement) return error.InvalidGraphRetirementSeal;
    if (request.online_source != null) return error.MissingOnlineSourceAppliedIndex;
    try validatePageFields(request);
    return try std.json.Stringify.valueAlloc(alloc, BatchMutationPayload{
        .schema_version = batchMutationVersion(request),
        .request = request,
    }, .{});
}

pub fn encodeGraphRetirementSealMutationRequestAlloc(
    alloc: Allocator,
    request: db_types.BatchRequest,
    entry: db_types.RaftAppliedEntryIdentity,
) ![]u8 {
    const command = request.relational_topology orelse return error.InvalidGraphRetirementSeal;
    if (command.action != .seal_graph_retirement or command.graph_retirement == null or
        entry.term == 0 or entry.index == 0) return error.InvalidGraphRetirementSeal;
    try validatePageFields(request);
    return std.json.Stringify.valueAlloc(alloc, BatchMutationPayload{
        .schema_version = 9,
        .request = request,
        .graph_retirement_raft_entry = entry,
    }, .{});
}

pub fn encodeRestoreGenerationAdmissionMutationRequestAlloc(
    alloc: Allocator,
    request: db_types.BatchRequest,
    entry: db_types.RaftAppliedEntryIdentity,
) ![]u8 {
    const command = request.restore_staging orelse return error.InvalidRestoreStagingCommand;
    if (command != .install_generation_admissions or entry.term == 0 or entry.index == 0)
        return error.InvalidRestoreStagingCommand;
    try command.install_generation_admissions.validate();
    try validatePageFields(request);
    return std.json.Stringify.valueAlloc(alloc, BatchMutationPayload{
        .schema_version = 10,
        .request = request,
        .restore_generation_admission_raft_entry = entry,
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

pub fn encodeArtifactCatalogMutationRequestAlloc(alloc: Allocator, request: db_types.BatchRequest, entry: db_types.RaftAppliedEntryIdentity) ![]u8 {
    if (request.artifact_catalog == null or entry.term == 0 or entry.index == 0) return error.InvalidArtifactCatalogCommand;
    try validatePageFields(request);
    return std.json.Stringify.valueAlloc(alloc, BatchMutationPayload{ .schema_version = 12, .request = request, .artifact_catalog_raft_entry = entry, .online_source_applied_index = if (request.online_source != null) entry.index else null }, .{});
}

pub fn encodeArtifactPublicationMutationRequestAlloc(alloc: Allocator, request: db_types.BatchRequest, entry: db_types.RaftAppliedEntryIdentity) ![]u8 {
    if (request.artifact_publication == null or entry.term == 0 or entry.index == 0) return error.InvalidBatchRequest;
    try @import("artifact_publication.zig").validateRequest(alloc, request);
    return std.json.Stringify.valueAlloc(alloc, BatchMutationPayload{ .schema_version = 14, .request = request, .artifact_publication_raft_entry = entry }, .{});
}

pub fn encodeArtifactPublicationTransportMutationRequestAlloc(alloc: Allocator, request: db_types.BatchRequest, entry: db_types.RaftAppliedEntryIdentity) ![]u8 {
    if (request.artifact_publication_transport == null or entry.term == 0 or entry.index == 0) return error.InvalidBatchRequest;
    try @import("artifact_publication_transport.zig").validateBatchRequest(request);
    return std.json.Stringify.valueAlloc(alloc, BatchMutationPayload{ .schema_version = 16, .request = request, .artifact_publication_transport_raft_entry = entry }, .{});
}

pub fn encodeMergeProofAdoptionMutationRequestAlloc(alloc: Allocator, request: db_types.BatchRequest, entry: db_types.RaftAppliedEntryIdentity) ![]u8 {
    if (request.merge_proof_adoption == null or entry.term == 0 or entry.index == 0) return error.InvalidBatchRequest;
    try @import("merge_proof_adoption.zig").validateRequest(request);
    return std.json.Stringify.valueAlloc(alloc, BatchMutationPayload{ .schema_version = 17, .request = request, .merge_proof_adoption_raft_entry = entry }, .{});
}

test "storage.hot_standby ordered artifact inventory merge adoption requires a versioned standby entry" {
    const alloc = std.testing.allocator;
    const request: db_types.BatchRequest = .{ .merge_proof_adoption = .{
        .transition_id = 7,
        .attempt = .{ .donor_term = 3, .sequence = 4 },
        .source_pin = @splat(1),
        .proof_digest = @splat(2),
        .record_digest = @splat(3),
    } };
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchMutationRequestAlloc(alloc, request));
    try std.testing.expectError(error.InvalidBatchRequest, encodeRaftBatchMutationRequestAlloc(alloc, request, .{ .term = 2, .index = 5 }));
    const encoded = try encodeMergeProofAdoptionMutationRequestAlloc(alloc, request, .{ .term = 2, .index = 5 });
    defer alloc.free(encoded);
    const record: replication_record.RecordView = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 1, .previous_lsn = 0, .payload = encoded };
    var decoded = try decodeBatchMutationRequest(alloc, record);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u32, 17), decoded.value.schema_version);
    try std.testing.expectEqualDeep(request.merge_proof_adoption.?, decoded.value.request.merge_proof_adoption.?);
    try std.testing.expectEqual(@as(u64, 5), decoded.value.merge_proof_adoption_raft_entry.?.index);
    try std.testing.expect((try decodeRestoreFinishForReplay(alloc, record)) == null);
    const downgraded = try std.json.Stringify.valueAlloc(alloc, BatchMutationPayload{ .schema_version = 16, .request = request }, .{});
    defer alloc.free(downgraded);
    const bad: replication_record.RecordView = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 1, .previous_lsn = 0, .payload = downgraded };
    try std.testing.expectError(error.UnsupportedBatchMutationPayloadVersion, decodeBatchMutationRequest(alloc, bad));
}

pub fn encodeRaftBatchMutationRequestAlloc(alloc: Allocator, request: db_types.BatchRequest, entry: db_types.RaftAppliedEntryIdentity) ![]u8 {
    const payload: BatchMutationPayload = .{ .schema_version = 15, .request = request, .ordinary_raft_entry = entry };
    try validateOrdinaryRaftPayload(payload);
    return std.json.Stringify.valueAlloc(alloc, payload, .{});
}

test "storage.hot_standby ordered artifact inventory standby finalization carries only the upload control and exact Raft identity" {
    const alloc = std.testing.allocator;
    const request: db_types.BatchRequest = .{ .artifact_publication_transport = .{ .action = .finalize, .namespace = @splat(1), .publication_digest = @splat(2), .manifest_root = @splat(3) } };
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchMutationRequestAlloc(alloc, request));
    try std.testing.expectError(error.InvalidBatchRequest, encodeRaftBatchMutationRequestAlloc(alloc, request, .{ .term = 4, .index = 5 }));
    const bytes = try encodeArtifactPublicationTransportMutationRequestAlloc(alloc, request, .{ .term = 4, .index = 5 });
    defer alloc.free(bytes);
    try std.testing.expect(bytes.len < 4096);
    const record: replication_record.RecordView = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 1, .previous_lsn = 0, .payload = bytes };
    var decoded = try decodeBatchMutationRequest(alloc, record);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u32, 16), decoded.value.schema_version);
    try std.testing.expectEqualDeep(request, decoded.value.request);
    try std.testing.expectEqual(@as(u64, 5), decoded.value.artifact_publication_transport_raft_entry.?.index);
    try std.testing.expect((try decodeRestoreFinishForReplay(alloc, record)) == null);
}

test "storage.hot_standby ordered artifact inventory standby baseline keeps discovery and apply cuts distinct" {
    const alloc = std.testing.allocator;
    const publication = @import("artifact_publication.zig");
    const row = try @import("../internal_keys.zig").relationalRowKeyAlloc(alloc, "row\x00\xff");
    defer alloc.free(row);
    var command: publication.Command = .{
        .mode = .baseline,
        .namespace = @splat(1),
        .authority_epoch = 2,
        .catalog_digest = @splat(3),
        .producer_name = "",
        .producer_generation = 0,
        .sources = &.{},
        .mutations = &.{},
        .publication_digest = @splat(0),
        .baseline = .{ .observed_term = 4, .observed_index = 5, .expected_cursor = "", .next_cursor = row, .upper_bound = row, .row_keys = &.{row}, .at_end = true },
    };
    command.publication_digest = command.digest();
    const request: db_types.BatchRequest = .{ .artifact_publication = command };
    const applied: db_types.RaftAppliedEntryIdentity = .{ .term = 6, .index = 9 };
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchMutationRequestAlloc(alloc, request));
    try std.testing.expectError(error.InvalidBatchRequest, encodeRaftBatchMutationRequestAlloc(alloc, request, applied));
    const bytes = try encodeArtifactPublicationMutationRequestAlloc(alloc, request, applied);
    defer alloc.free(bytes);
    const record: replication_record.RecordView = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 1, .previous_lsn = 0, .payload = bytes };
    var decoded = try decodeBatchMutationRequest(alloc, record);
    defer decoded.deinit();
    try std.testing.expectEqualDeep(request, decoded.value.request);
    try std.testing.expectEqualDeep(applied, decoded.value.artifact_publication_raft_entry.?);
    try std.testing.expectEqual(@as(u64, 5), decoded.value.request.artifact_publication.?.baseline.?.observed_index);
}

test "storage.hot_standby ordered artifact inventory standby validation retains epoch repairs and exact apply identity" {
    const alloc = std.testing.allocator;
    const publication = @import("artifact_publication.zig");
    var command: publication.Command = .{
        .mode = .validate_inputs,
        .namespace = @splat(1),
        .authority_epoch = 2,
        .catalog_digest = @splat(3),
        .producer_name = "",
        .producer_generation = 0,
        .sources = &.{},
        .mutations = &.{},
        .publication_digest = @splat(0),
        .validation = .{ .mutation_epoch = 11, .expected_cursor = "", .next_cursor = "\xff\x00proof", .at_end = true, .repair_documents = &.{"doc\xff"} },
    };
    command.publication_digest = command.digest();
    const request: db_types.BatchRequest = .{ .artifact_publication = command };
    const applied: db_types.RaftAppliedEntryIdentity = .{ .term = 6, .index = 9 };
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchMutationRequestAlloc(alloc, request));
    try std.testing.expectError(error.InvalidBatchRequest, encodeRaftBatchMutationRequestAlloc(alloc, request, applied));
    const bytes = try encodeArtifactPublicationMutationRequestAlloc(alloc, request, applied);
    defer alloc.free(bytes);
    const record: replication_record.RecordView = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 1, .previous_lsn = 0, .payload = bytes };
    var decoded = try decodeBatchMutationRequest(alloc, record);
    defer decoded.deinit();
    try std.testing.expectEqualDeep(request, decoded.value.request);
    try std.testing.expectEqualDeep(applied, decoded.value.artifact_publication_raft_entry.?);
}

test "storage.hot_standby ordered artifact inventory standby census preserves page claims and exact apply identity" {
    const alloc = std.testing.allocator;
    const publication = @import("artifact_publication.zig");
    var command: publication.Command = .{
        .mode = .census,
        .namespace = @splat(1),
        .authority_epoch = 2,
        .catalog_digest = @splat(3),
        .producer_name = "index\xff",
        .producer_generation = 4,
        .producer_artifact_name = "model\x00",
        .sources = &.{},
        .mutations = &.{},
        .publication_digest = @splat(0),
        .census = .{ .document_key = "doc\x80", .chunk_name = "chunk\x00", .before = @splat(5), .after = @splat(6) },
    };
    command.publication_digest = command.digest();
    const request: db_types.BatchRequest = .{ .artifact_publication = command };
    const applied: db_types.RaftAppliedEntryIdentity = .{ .term = 6, .index = 9 };
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchMutationRequestAlloc(alloc, request));
    try std.testing.expectError(error.InvalidBatchRequest, encodeRaftBatchMutationRequestAlloc(alloc, request, applied));
    const bytes = try encodeArtifactPublicationMutationRequestAlloc(alloc, request, applied);
    defer alloc.free(bytes);
    const record: replication_record.RecordView = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 1, .previous_lsn = 0, .payload = bytes };
    var decoded = try decodeBatchMutationRequest(alloc, record);
    defer decoded.deinit();
    try std.testing.expectEqualDeep(request, decoded.value.request);
    try std.testing.expectEqualDeep(applied, decoded.value.artifact_publication_raft_entry.?);
}

test "storage.hot_standby ordered artifact inventory standby transport marker cannot masquerade as ordinary replay" {
    const alloc = std.testing.allocator;
    const raw = try std.json.Stringify.valueAlloc(alloc, BatchMutationPayload{
        .schema_version = 15,
        .request = .{},
        .ordinary_raft_entry = .{ .term = 1, .index = 2 },
        .artifact_publication_transport_raft_entry = .{ .term = 1, .index = 2 },
    }, .{});
    defer alloc.free(raw);
    const record: replication_record.RecordView = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 1, .previous_lsn = 0, .payload = raw };
    try std.testing.expectError(error.InvalidBatchRequest, decodeBatchMutationRequest(alloc, record));
    try std.testing.expectError(error.InvalidBatchRequest, decodeRestoreFinishForReplay(alloc, record));
}

fn validateOrdinaryRaftPayload(payload: BatchMutationPayload) !void {
    const entry = payload.ordinary_raft_entry orelse return error.InvalidBatchRequest;
    if (payload.schema_version != 15 or entry.term == 0 or entry.index == 0 or
        payload.native_topology_position != null or payload.artifact_publication_raft_entry != null or payload.artifact_publication_transport_raft_entry != null or payload.merge_proof_adoption_raft_entry != null or payload.artifact_catalog_raft_entry != null or
        payload.initial_child_raft_entry != null or payload.graph_retirement_raft_entry != null or payload.restore_generation_admission_raft_entry != null or
        payload.online_source_applied_index != null or payload.restore_staging_bootstrap != null or payload.request.online_source != null)
        return error.InvalidBatchRequest;
    const base = batchMutationVersion(payload.request);
    if (base > 7 and base != 11) return error.InvalidBatchRequest;
    try validatePageFields(payload.request);
}

pub fn encodeBatchMutationWithRestoreBootstrapAlloc(alloc: Allocator, request: db_types.BatchRequest, bootstrap: @import("restore_staging_contract.zig").OwnerBootstrap) ![]u8 {
    try validatePageFields(request);
    if (request.restore_staging == null or request.restore_staging.? != .begin or !std.mem.eql(u8, &request.restore_staging.?.begin.digest(), &bootstrap.scope.digest())) return error.InvalidRestoreStagingCommand;
    try bootstrap.validate();
    return std.json.Stringify.valueAlloc(alloc, BatchMutationPayload{ .schema_version = batchMutationVersion(request), .request = request, .restore_staging_bootstrap = bootstrap }, .{});
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
    if (parsed.value.schema_version == 15) {
        try validateOrdinaryRaftPayload(parsed.value);
        return parsed;
    }
    if (parsed.value.ordinary_raft_entry != null) return error.InvalidBatchRequest;
    if (parsed.value.schema_version == 13) {
        try validateNativeTopologyPayload(alloc, parsed.value);
        return parsed;
    }
    if (parsed.value.native_topology_position != null) return error.InvalidControlReceiptPosition;
    if (parsed.value.schema_version != batchMutationVersion(parsed.value.request)) return error.UnsupportedBatchMutationPayloadVersion;
    if ((parsed.value.schema_version == 17) != (parsed.value.merge_proof_adoption_raft_entry != null)) return error.InvalidBatchRequest;
    if (parsed.value.merge_proof_adoption_raft_entry) |entry| {
        if (entry.term == 0 or entry.index == 0 or parsed.value.restore_staging_bootstrap != null) return error.InvalidBatchRequest;
        try @import("merge_proof_adoption.zig").validateRequest(parsed.value.request);
    }
    if ((parsed.value.schema_version == 16) != (parsed.value.artifact_publication_transport_raft_entry != null)) return error.InvalidBatchRequest;
    if (parsed.value.artifact_publication_transport_raft_entry) |entry| {
        if (entry.term == 0 or entry.index == 0 or parsed.value.restore_staging_bootstrap != null) return error.InvalidBatchRequest;
        try @import("artifact_publication_transport.zig").validateBatchRequest(parsed.value.request);
    }
    if ((parsed.value.schema_version == 14) != (parsed.value.artifact_publication_raft_entry != null)) return error.InvalidBatchRequest;
    if (parsed.value.artifact_publication_raft_entry) |entry| {
        if (entry.term == 0 or entry.index == 0 or parsed.value.restore_staging_bootstrap != null) return error.InvalidBatchRequest;
        try @import("artifact_publication.zig").validateRequest(alloc, parsed.value.request);
    }
    if ((parsed.value.schema_version == 12) != (parsed.value.artifact_catalog_raft_entry != null)) return error.InvalidArtifactCatalogCommand;
    if (parsed.value.artifact_catalog_raft_entry) |entry| if (entry.term == 0 or entry.index == 0) return error.InvalidArtifactCatalogCommand;
    if ((parsed.value.schema_version == 8) != (parsed.value.initial_child_raft_entry != null)) return error.InvalidInitialChildPublication;
    if (parsed.value.initial_child_raft_entry) |entry| if (entry.term == 0 or entry.index == 0) return error.InvalidInitialChildPublication;
    const graph_seal = if (parsed.value.request.relational_topology) |command| command.action == .seal_graph_retirement else false;
    if (graph_seal != (parsed.value.graph_retirement_raft_entry != null)) return error.InvalidGraphRetirementSeal;
    if (parsed.value.graph_retirement_raft_entry) |entry| if (entry.term == 0 or entry.index == 0) return error.InvalidGraphRetirementSeal;
    if ((parsed.value.schema_version == 10) != (parsed.value.restore_generation_admission_raft_entry != null))
        return error.InvalidRestoreStagingCommand;
    if (parsed.value.restore_generation_admission_raft_entry) |entry| if (entry.term == 0 or entry.index == 0)
        return error.InvalidRestoreStagingCommand;
    if ((parsed.value.request.online_source != null) != (parsed.value.online_source_applied_index != null)) return error.MissingOnlineSourceAppliedIndex;
    try validatePageFields(parsed.value.request);
    return parsed;
}

fn validateNativeTopologyPayload(alloc: Allocator, payload: BatchMutationPayload) !void {
    const stamp = payload.native_topology_position orelse return error.InvalidControlReceiptPosition;
    try stamp.validate();
    if (payload.schema_version != 13 or payload.ordinary_raft_entry != null or payload.artifact_catalog_raft_entry != null or payload.artifact_publication_raft_entry != null or
        payload.initial_child_raft_entry != null or payload.graph_retirement_raft_entry != null or
        payload.restore_generation_admission_raft_entry != null or payload.online_source_applied_index != null or
        payload.restore_staging_bootstrap != null) return error.InvalidControlReceiptPosition;
    try @import("native_topology_receipt.zig").validateRequest(alloc, payload.request);
    if (!stamp.namespace.eql(payload.request.relational_topology.?.fence.namespace)) return error.IdentityNamespaceMismatch;
}

pub fn encodeNativeTopologyMutationRequestAlloc(alloc: Allocator, request: db_types.BatchRequest, stamp: @import("receipt_position.zig").Native) ![]u8 {
    const payload: BatchMutationPayload = .{ .schema_version = 13, .request = request, .native_topology_position = stamp };
    try validateNativeTopologyPayload(alloc, payload);
    return std.json.Stringify.valueAlloc(alloc, payload, .{});
}

/// Inspect only the fixed-size restore completion proof on duplicate replay.
/// Unknown JSON values (notably ordinary row payloads) are scanned without
/// materializing them, so published restore tombstones do not make later
/// large-batch replays allocate a second copy of every row.
pub fn decodeRestoreFinishForReplay(
    alloc: Allocator,
    record: replication_record.RecordView,
) !?@FieldType(@import("restore_staging_contract.zig").Control, "finish") {
    if (record.kind != .batch_mutation) return error.NotBatchMutationRecord;
    if (record.payload_codec != .json) return error.UnsupportedBatchMutationCodec;
    const Finish = @FieldType(@import("restore_staging_contract.zig").Control, "finish");
    const Projection = struct {
        schema_version: u32 = 1,
        ordinary_raft_entry: ?db_types.RaftAppliedEntryIdentity = null,
        artifact_publication_raft_entry: ?db_types.RaftAppliedEntryIdentity = null,
        artifact_publication_transport_raft_entry: ?db_types.RaftAppliedEntryIdentity = null,
        merge_proof_adoption_raft_entry: ?db_types.RaftAppliedEntryIdentity = null,
        online_source_applied_index: ?u64 = null,
        request: struct {
            artifact_publication: ?struct {} = null,
            artifact_publication_transport: ?struct {} = null,
            merge_proof_adoption: ?struct {} = null,
            restore_staging: ?struct { finish: ?Finish = null } = null,
            online_source: ?struct {} = null,
            merge_page: ?struct { source: struct { retention: ?struct {} = null, integrity: ?struct {} = null }, chunk: ?struct {} = null, next_snapshot_position: ?struct {} = null } = null,
            merge_checkpoint: ?struct { page_source: ?struct { retention: ?struct {} = null, integrity: ?struct {} = null } = null, page_receiver_namespace: ?struct {} = null } = null,
        },
    };
    var parsed = try std.json.parseFromSlice(Projection, alloc, record.payload, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    if (parsed.value.schema_version == 15) {
        const entry = parsed.value.ordinary_raft_entry orelse return error.InvalidBatchRequest;
        if (entry.term == 0 or entry.index == 0 or parsed.value.artifact_publication_raft_entry != null or
            parsed.value.artifact_publication_transport_raft_entry != null or parsed.value.merge_proof_adoption_raft_entry != null or parsed.value.request.merge_proof_adoption != null or parsed.value.request.artifact_publication_transport != null or
            parsed.value.request.artifact_publication != null or parsed.value.request.online_source != null or parsed.value.online_source_applied_index != null)
            return error.InvalidBatchRequest;
        return if (parsed.value.request.restore_staging) |staging| staging.finish else null;
    }
    if (parsed.value.ordinary_raft_entry != null) return error.InvalidBatchRequest;
    if (parsed.value.schema_version == 17) {
        var control = try decodeBatchMutationRequest(alloc, record);
        defer control.deinit();
        return null;
    }
    if (parsed.value.schema_version == 16) {
        const entry = parsed.value.artifact_publication_transport_raft_entry orelse return error.InvalidBatchRequest;
        if (entry.term == 0 or entry.index == 0 or parsed.value.request.artifact_publication_transport == null or
            parsed.value.artifact_publication_raft_entry != null or parsed.value.request.artifact_publication != null or
            parsed.value.request.restore_staging != null) return error.InvalidBatchRequest;
        var control = try decodeBatchMutationRequest(alloc, record);
        defer control.deinit();
        return null;
    }
    if (parsed.value.schema_version == 14) {
        const entry = parsed.value.artifact_publication_raft_entry orelse return error.InvalidBatchRequest;
        if (entry.term == 0 or entry.index == 0 or parsed.value.request.artifact_publication == null or
            parsed.value.request.restore_staging != null or parsed.value.request.online_source != null or
            parsed.value.request.merge_page != null or parsed.value.request.merge_checkpoint != null or
            parsed.value.online_source_applied_index != null) return error.InvalidBatchRequest;
        // This projection is only used after the full envelope was applied.
        // Scan immutable payload bytes without cloning large artifact values.
        return null;
    }
    if (parsed.value.merge_proof_adoption_raft_entry != null or parsed.value.request.merge_proof_adoption != null or
        parsed.value.artifact_publication_raft_entry != null or parsed.value.request.artifact_publication != null) return error.UnsupportedBatchMutationPayloadVersion;
    if (parsed.value.schema_version == 13) {
        // Native control envelopes are bounded control records, not row
        // batches. Validate their full authority before accepting a duplicate.
        var control = try decodeBatchMutationRequest(alloc, record);
        defer control.deinit();
        return null;
    }
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

pub fn encodePublishedChildSchemaMetadataMutationAlloc(alloc: Allocator, schema: schema_mod.TableSchema, public_schema_json: []const u8, published: PublishedChildSchema) ![]u8 {
    if ((published.fence.role != .child_generation_source and published.fence.role != .child_generation_dual) or
        published.applied_term == 0 or published.applied_index == 0) return error.InvalidGenerationPublication;
    const schema_bytes = try schema_mod.serializeSchema(alloc, schema);
    defer alloc.free(schema_bytes);
    return try std.json.Stringify.valueAlloc(alloc, MetadataMutationPayload{
        .schema_version = 3,
        .kind = .schema,
        .schema_bytes = schema_bytes,
        .public_schema_json = public_schema_json,
        .published_child = published,
    }, .{});
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
    if (parsed.value.schema_version < 1 or parsed.value.schema_version > 4) return error.UnsupportedMetadataMutationPayloadVersion;
    switch (parsed.value.kind) {
        .schema => {
            if (parsed.value.schema_version == 4 or parsed.value.schema_bytes.len == 0 or
                (parsed.value.schema_version == 3) != (parsed.value.published_child != null) or
                parsed.value.row_policy_bundle != null or parsed.value.row_policy_request != null or
                parsed.value.row_policy_raft_entry != null) return error.InvalidMetadataMutationPayload;
        },
        .row_policy => {
            if (parsed.value.schema_version != 4 or parsed.value.schema_bytes.len != 0 or
                parsed.value.public_schema_json != null or parsed.value.published_child != null or
                parsed.value.row_policy_bundle == null or parsed.value.row_policy_bundle.?.len == 0 or
                parsed.value.row_policy_request == null or parsed.value.row_policy_raft_entry == null or
                parsed.value.row_policy_raft_entry.?.term == 0 or parsed.value.row_policy_raft_entry.?.index == 0)
                return error.InvalidMetadataMutationPayload;
        },
    }
    return parsed;
}

pub const DecodedSchemaMetadataMutation = struct {
    alloc: Allocator,
    schema: schema_mod.TableSchema,
    public_schema_json: ?[]u8,
    published_child: ?PublishedChildSchema,

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
        .published_child = parsed.value.published_child,
    };
}

pub fn decodeDerivedChangeRecord(
    alloc: Allocator,
    record: replication_record.RecordView,
) !change_journal.DecodedRecord {
    if (record.kind != .derived_effect) return error.NotDerivedEffectRecord;
    if (record.payload_codec != .binary) return error.UnsupportedDerivedEffectCodec;
    if (primary_effect.isPrimaryEffect(record.payload)) {
        var decoded = try primary_effect.decode(alloc, record.payload);
        defer decoded.deinit();
        return try change_journal.decodeRecord(alloc, decoded.replay);
    }
    return try change_journal.decodeRecord(alloc, record.payload);
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
        .payload = "{\"schema_version\":5,\"kind\":\"schema\",\"schema_bytes\":\"\"}",
    };
    try std.testing.expectError(
        error.UnsupportedMetadataMutationPayloadVersion,
        decodeMetadataMutation(std.testing.allocator, bad_version),
    );

    var malformed_v3 = bad_version;
    malformed_v3.payload = "{\"schema_version\":3,\"kind\":\"schema\",\"schema_bytes\":\"\"}";
    try std.testing.expectError(
        error.InvalidMetadataMutationPayload,
        decodeMetadataMutation(std.testing.allocator, malformed_v3),
    );
}

test "storage.hot_standby row policy publication carries exact owner Raft identity" {
    const alloc = std.testing.allocator;
    const request: @import("../../system_catalog/policies.zig").InstallRequest = .{
        .table_id = 7,
        .expected_generation = 3,
        .expected_catalog_epoch = 9,
        .expected_phase = .pending_install,
        .owner_group_id = 11,
        .expected_descriptor_digest = @splat(4),
    };
    const encoded = try encodeRowPolicyMetadataMutationAlloc(alloc, "signed-publication", request, .{ .term = 5, .index = 13 });
    defer alloc.free(encoded);
    var record = replication_record.Record{
        .kind = .metadata_mutation,
        .payload_codec = .json,
        .cluster_id = 1,
        .timeline_id = 1,
        .epoch = 1,
        .lsn = 1,
        .previous_lsn = 0,
        .payload = encoded,
    };
    var decoded = try decodeMetadataMutation(alloc, record);
    defer decoded.deinit();
    try std.testing.expectEqual(MetadataMutationKind.row_policy, decoded.value.kind);
    try std.testing.expectEqualDeep(request, decoded.value.row_policy_request.?);
    try std.testing.expectEqual(@as(u64, 13), decoded.value.row_policy_raft_entry.?.index);
    try std.testing.expectEqualSlices(u8, "signed-publication", decoded.value.row_policy_bundle.?);
    try std.testing.expectError(error.UnsupportedMetadataMutationKind, decodeSchemaMetadataMutation(alloc, record));

    const missing_identity = try std.json.Stringify.valueAlloc(alloc, MetadataMutationPayload{
        .schema_version = 4,
        .kind = .row_policy,
        .row_policy_bundle = "signed-publication",
        .row_policy_request = request,
    }, .{});
    defer alloc.free(missing_identity);
    record.payload = missing_identity;
    try std.testing.expectError(error.InvalidMetadataMutationPayload, decodeMetadataMutation(alloc, record));
    record.payload = "{\"schema_version\":3,\"kind\":\"row_policy\",\"schema_bytes\":\"\"}";
    try std.testing.expectError(error.InvalidMetadataMutationPayload, decodeMetadataMutation(alloc, record));
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
