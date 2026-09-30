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
            try db.applyReplicatedBatch(decoded.value, record.lsn);
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
