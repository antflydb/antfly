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

//! Decodes committed mutation envelopes for an engine owner. This ingress
//! owns temporary payload allocations; DB methods only execute typed mutations.
const DB = @import("db.zig").DB;
const record_mod = @import("replication_record.zig");
const effects = @import("replication_effects.zig");

pub fn applyRecord(db: *DB, record: record_mod.RecordView) !void {
    if (try db.replicationMutationAlreadyApplied(record.lsn)) {
        if (record.kind == .batch_mutation and try db.recoverReplicatedBatchCut(record.lsn)) {
            if (try effects.decodeRestoreFinishForReplay(db.alloc, record)) |finish|
                try db.recoverReplicatedRestoreFinish(finish);
        }
        return;
    }
    switch (record.kind) {
        .batch_mutation => {
            var decoded = try effects.decodeBatchMutationRequest(db.alloc, record);
            defer decoded.deinit();
            const mutation = try normalizeBatch(decoded.value);
            if (mutation.receipt == .ordered and (mutation.request.online_source != null or mutation.request.merge_checkpoint != null))
                try db.reconcileReplicatedArtifactAdmission(mutation.request, mutation.receipt.ordered);
            try db.applyReplicatedBatch(mutation, record.lsn);
        },
        .metadata_mutation => {
            var metadata = try effects.decodeMetadataMutation(db.alloc, record);
            defer metadata.deinit();
            switch (metadata.value.kind) {
                .schema => {
                    var schema = try effects.decodeSchemaMetadataMutation(db.alloc, record);
                    defer schema.deinit();
                    try db.applyReplicatedSchema(schema.view(), record.lsn);
                },
                .row_policy => try db.applyReplicatedRowPolicy(.{
                    .bundle = metadata.value.row_policy_bundle orelse return error.InvalidMetadataMutationPayload,
                    .request = metadata.value.row_policy_request orelse return error.InvalidMetadataMutationPayload,
                    .entry = metadata.value.row_policy_raft_entry orelse return error.InvalidMetadataMutationPayload,
                }, record.lsn),
            }
        },
        .derived_effect => {
            _ = try applyDerivedRecord(db, record);
            try db.recordReplicationApplied(record.lsn);
        },
        .backup_start, .backup_end, .checkpoint, .manifest, .truncate, .timeline_switch => try db.recordReplicationApplied(record.lsn),
        _ => return error.HAReplicationRecordApplyUnsupported,
    }
}

pub fn applyDerivedRecord(db: *DB, record: record_mod.RecordView) !u64 {
    // Keep duplicate delivery allocation-free, including direct derived replay.
    if (try db.replicationMutationAlreadyApplied(record.lsn)) return 0;
    var primary = if (effects.primary_effect.isPrimaryEffect(record.payload)) (effects.primary_effect.decode(db.alloc, record.payload) catch |err| {
        if (try db.replicationMutationAlreadyApplied(record.lsn)) return 0;
        return err;
    }) else null;
    defer if (primary) |*effect| effect.deinit();
    var decoded = effects.decodeDerivedChangeRecord(db.alloc, record) catch |err| {
        // Another delivery can commit while this envelope is being decoded.
        if (try db.replicationMutationAlreadyApplied(record.lsn)) return 0;
        return err;
    };
    defer decoded.deinit();
    return try db.applyReplicatedDerivedEffect(decoded.record, if (primary) |*effect| effect.view() else null, record.lsn);
}

pub fn applyCallback(ctx: *anyopaque, record: record_mod.RecordView) !void {
    const db: *DB = @ptrCast(@alignCast(ctx));
    try applyRecord(db, record);
}

/// Preserve envelope provenance precedence after version-specific validation.
/// DB receives one receipt rather than interpreting the wire's optional fields.
pub fn normalizeBatch(payload: effects.BatchMutationPayload) !@import("replicated_mutation.zig").Batch {
    return .{ .request = payload.request, .receipt = if (payload.native_topology_position) |position|
        .{ .native = position }
    else if (payload.ordinary_raft_entry orelse payload.artifact_publication_raft_entry orelse payload.artifact_publication_transport_raft_entry orelse payload.merge_proof_adoption_raft_entry orelse payload.artifact_catalog_raft_entry orelse payload.initial_child_raft_entry orelse payload.graph_retirement_raft_entry orelse payload.restore_generation_admission_raft_entry) |entry|
        .{ .ordered = entry }
    else if (payload.request.online_source != null)
        .{ .online_source = payload.online_source_applied_index orelse return error.InvalidOnlineSourceCommand }
    else
        .none };
}

test "storage.db replication ingress normalizes every ordered provenance field" {
    const std = @import("std");
    inline for (.{ "ordinary_raft_entry", "artifact_publication_raft_entry", "artifact_publication_transport_raft_entry", "merge_proof_adoption_raft_entry", "artifact_catalog_raft_entry", "initial_child_raft_entry", "graph_retirement_raft_entry", "restore_generation_admission_raft_entry" }) |field| {
        var payload: effects.BatchMutationPayload = .{ .request = .{} };
        @field(payload, field) = .{ .term = 7, .index = 19 };
        const normalized = try normalizeBatch(payload);
        try std.testing.expect(normalized.receipt == .ordered);
        try std.testing.expectEqual(@as(u64, 7), normalized.receipt.ordered.term);
        try std.testing.expectEqual(@as(u64, 19), normalized.receipt.ordered.index);
    }
    const ordinary = try normalizeBatch(.{ .request = .{} });
    try std.testing.expect(ordinary.receipt == .none);
    const native = try normalizeBatch(.{
        .request = .{},
        .native_topology_position = .{ .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .sequence = 23 },
        .ordinary_raft_entry = .{ .term = 7, .index = 19 },
    });
    try std.testing.expect(native.receipt == .native);
    try std.testing.expectEqual(@as(u64, 23), native.receipt.native.sequence);
}
