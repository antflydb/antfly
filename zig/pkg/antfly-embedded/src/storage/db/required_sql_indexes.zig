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

//! Physical full-text lifetime pins captured alongside a primary read cut.
//! This is dependency authority, never a replacement query/postings predicate.
const std = @import("std");
const requirements = @import("../../common/sql_view_index_requirement.zig");
const index_mod = @import("../../index.zig");
const apply_state = @import("derived/apply_state.zig");
const types = @import("types.zig");
const A = std.mem.Allocator;
pub const Lease = struct {
    alloc: A,
    snapshots: []*index_mod.IndexSnapshot,
    digest: [32]u8,
    pub fn deinit(self: *Lease) void {
        for (self.snapshots) |snapshot| snapshot.release();
        self.alloc.free(self.snapshots);
        self.* = undefined;
    }
};
/// Caller holds apply shared and has already checked quarantine. Readiness is
/// relative to this exact primary transaction, not a mutable metadata report.
pub fn capture(core: anytype, a: A, txn: anytype, expected: []const requirements.Requirement) !?Lease {
    const digest = (try requirements.digest(expected)) orelse return null;
    const snapshots = try a.alloc(*index_mod.IndexSnapshot, expected.len);
    var initialized: usize = 0;
    errdefer {
        for (snapshots[0..initialized]) |snapshot| snapshot.release();
        a.free(snapshots);
    }
    const source_sequence = try core.store.lastReplaySequenceFromTxn(txn, 0);
    const manager = core.index_manager;
    if (!manager.catalog_mutex.tryLockShared()) return error.StorageReadTemporarilyUnavailable;
    defer manager.catalog_mutex.unlockShared();
    for (expected, snapshots) |guard, *out| {
        const entry = manager.textIndexEntry(guard.name) orelse return error.IndexNotFound;
        if (!entry.tryLockAnalysisShared()) return error.StorageReadTemporarilyUnavailable;
        defer entry.unlockAnalysisShared();
        if (!entry.coversPrimaryDocuments()) return error.UnsupportedSqlExecution;
        const config_fingerprint = entry.config.coverage_config_fingerprint orelse return error.PreparedGenerationChanged;
        if (entry.config.coverage_generation != guard.incarnation or config_fingerprint != guard.config_hash) return error.PreparedGenerationChanged;
        const checkpoint = try apply_state.loadProjectionCheckpointTxn(txn, guard.name);
        if (checkpoint.status != .clean or checkpoint.config_hash != types.indexConfigHash(entry.config) or checkpoint.applied_sequence != source_sequence) return error.SqlStatementReadUnavailable;
        // The immutable postings generation keeps the actual index lifetime
        // alive after catalog locks release, including drop/rebuild and close.
        out.* = entry.persistent.acquireSnapshot();
        initialized += 1;
    }
    return .{ .alloc = a, .snapshots = snapshots, .digest = digest };
}
