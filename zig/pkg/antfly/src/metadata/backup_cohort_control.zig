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

//! Shared durable backup cohort mutation used by local and hosted coordinators.
const std = @import("std");
const metadata_service = @import("service.zig");
const operation = @import("antfly_local_sources").api_operation;
const store_mod = @import("storage/raft_apply_store.zig");

pub fn compareAndSet(svc: anytype, alloc: std.mem.Allocator, write: store_mod.BackupCohortWrite, request: operation.RequestContext) !void {
    try request.ensureActive();
    const cohort = @import("antfly_local_sources").metadata_backup_cohort;
    if (write.job_id == 0 or write.value.len == 0 or write.value.len >
        @as(usize, if (write.expected_revision == 0) cohort.Job.max_encoded_bytes else 4096)) return error.InvalidBackupCohort;
    // Canonicalize before proposal: semantically valid input with
    // whitespace must not commit and then report a false CAS miss
    // when the getter merges immutable plan and progress records.
    const canonical = if (write.expected_revision == 0) blk: {
        var parsed = try std.json.parseFromSlice(cohort.Job, alloc, write.value, .{});
        defer parsed.deinit();
        if (parsed.value.id != write.job_id) return error.InvalidBackupCohort;
        try cohort.validateReplacement(alloc, null, parsed.value);
        break :blk try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
    } else blk: {
        var parsed = try std.json.parseFromSlice(cohort.Progress, alloc, write.value, .{});
        defer parsed.deinit();
        break :blk try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
    };
    defer alloc.free(canonical);
    var canonical_write = write;
    canonical_write.value = canonical;
    try metadata_service.ensureCoordinatedDecoderWithContext(svc, .{ .compare_and_set_backup_cohort = canonical_write }, request);
    request.markSQLSubmission();
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .compare_and_set_backup_cohort = canonical_write });
    try svc.waitForTransitionAppliedWithContext(receipt, request);
    try svc.ensureLinearizableReadWithContext(request);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const actual = (if (write.expected_revision == 0)
        try store.getBackupCohort(alloc, svc.metadata_group_id, write.job_id)
    else
        try store.getBackupCohortProgress(alloc, svc.metadata_group_id, write.job_id)) orelse return error.BackupCohortChanged;
    defer alloc.free(actual);
    if (!std.mem.eql(u8, actual, canonical)) return error.BackupCohortChanged;
}
