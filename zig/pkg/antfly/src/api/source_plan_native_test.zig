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

//! Owned input for the completion-aware coordinator. Freeze before the first
//! BEGIN. Retry from these bytes, never from current routing, schema or rows.
//! Qualification across the actual separately compiled native engine facade.
const std = @import("std");
const local = @import("antfly_local_sources");
const db_mod = local.storage_db_selected_root.db;
const control = local.storage_source_plan_control;
const storage = control.storage;

pub fn run(comptime raft: bool) !void {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/source-plan-control", .{tmp.sub_path});
    defer a.free(path);
    const options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 }, .online_source_authority = if (raft) .raft else .native };
    var db = try db_mod.DB.open(a, path, options);
    var open = true;
    defer if (open) db.close();
    var namespace: [24]u8 = undefined;
    std.mem.writeInt(u64, namespace[0..8], 7, .big);
    std.mem.writeInt(u64, namespace[8..16], 1, .big);
    std.mem.writeInt(u64, namespace[16..24], 1, .big);
    if (!raft) try db.provisionNativeHistoryAuthority(namespace);
    const root: storage.Root = .{ .authority = if (raft) .raft else .native, .namespace = namespace, .generation = 0, .fence = @splat(3) };
    var frame: [128]u8 = @splat(0);
    @memcpy(frame[0..4], "AFP1");
    std.mem.writeInt(u32, frame[4..8], 1, .little);
    std.mem.writeInt(u64, frame[8..16], 10, .little);
    std.mem.writeInt(u64, frame[16..24], 11, .little);
    std.mem.writeInt(u64, frame[24..32], 1, .little);
    const digest = try storage.frozenDigest(&frame);
    @memcpy(frame[32..64], &digest);
    var checksum: storage.Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(&frame, &checksum, .{});
    const hashes = [_]storage.Digest{storage.chunkDigest(0, &frame)};
    var attempt: [16]u8 = @splat(1);
    std.mem.writeInt(u64, attempt[0..8], 1, .big);
    const spec: storage.Spec = .{ .namespace = storage.namespaceFor(root, 1), .storage_epoch = 1, .binding = @splat(2), .domain = storage.domainFor(@splat(2)), .attempt = attempt, .bytes = frame.len, .checksum = checksum, .plan_digest = digest, .chunk_hashes = &hashes };
    var request: control.Request = .{ .root = root, .spec = spec, .action = .begin };
    if (raft) {
        try std.testing.expectError(error.HistoryAuthorityRequired, db.nativeSourcePlanControl(request));
        const first = try db.applyOrderedSourcePlanControl(request, .{ .term = 1, .index = 1 }, false);
        try std.testing.expect(first == .applied and first.applied.present);
        try std.testing.expectEqualDeep(first, try db.applyOrderedSourcePlanControl(request, .{ .term = 1, .index = 1 }, false));
        request.action = .seal;
        const rejected = try db.applyOrderedSourcePlanControl(request, .{ .term = 1, .index = 2 }, false);
        try std.testing.expect(rejected == .rejected and rejected.rejected == .pending);
        try std.testing.expectEqualDeep(rejected, try db.orderedSourcePlanOutcome(.{ .request = request, .entry = .{ .term = 1, .index = 2 }, .minimum_applied_index = 2 }));
        var changed = request;
        changed.action = .begin;
        try std.testing.expectError(error.ConflictingOrderedApplyReceipt, db.orderedSourcePlanOutcome(.{ .request = changed, .entry = .{ .term = 1, .index = 2 }, .minimum_applied_index = 2 }));
        request.action = .chunk;
        request.chunk = &frame;
        _ = try db.applyOrderedSourcePlanControl(request, .{ .term = 1, .index = 3 }, false);
        request.action = .seal;
        request.chunk = "";
    } else {
        try std.testing.expectError(error.HistoryAuthorityRequired, db.applyOrderedSourcePlanControl(request, .{ .term = 1, .index = 1 }, false));
        const first = try db.nativeSourcePlanControl(request);
        try std.testing.expect(first.present);
        request.action = .seal;
        try std.testing.expectError(error.SourcePendingWork, db.nativeSourcePlanControl(request));
        request.action = .chunk;
        request.chunk = &frame;
        _ = try db.nativeSourcePlanControl(request);
        request.action = .seal;
        request.chunk = "";
    }
    try std.testing.expect(!(try db.relationalTopologyStatus()).drained);
    const sealed = if (raft) (try db.applyOrderedSourcePlanControl(request, .{ .term = 1, .index = 4 }, false)).applied else try db.nativeSourcePlanControl(request);
    const seal = sealed.seal.?;
    const original_position = sealed.sealed_position.?;
    try std.testing.expectEqual(@as(u8, if (raft) 1 else 2), original_position[31]);
    try std.testing.expectEqual(@as(u64, if (raft) 1 else 3), std.mem.readInt(u64, original_position[0..8], .little));
    if (raft) try std.testing.expectEqual(@as(u64, 4), std.mem.readInt(u64, original_position[8..16], .little));
    db.close();
    open = false;
    db = try db_mod.DB.open(a, path, options);
    open = true;
    const query: control.Query = .{ .root = root, .spec = spec, .minimum_applied_index = if (raft) 4 else 0 };
    const observed = try db.sourcePlanObservation(query);
    try std.testing.expectEqual(seal, observed.status.?.seal.?);
    const read = try db.sourcePlanChunk(a, .{ .query = query, .seal = seal, .ordinal = 0 });
    defer a.free(read.bytes);
    try std.testing.expectEqualSlices(u8, &frame, read.bytes);
    try std.testing.expectEqual(observed.cut, read.observation.cut);
    if (!raft) {
        const topology = try db.relationalTopologyIdentity();
        const fence: local.storage_db_relational_integrity_topology.Fence = .{ .role = .backup_snapshot, .transition_id = 900, .attempt = 1, .peer_group_id = 1, .owner_group_id = 1, .admission_epoch = topology.next_epoch, .namespace = topology.namespace, .catalog_digest = topology.catalog_digest };
        try db.applyRelationalTopologyControl(.{ .fence = fence, .action = .begin }, null);
        try std.testing.expect(!(try db.relationalTopologyStatus()).drained);
        var next = request;
        next.action = .begin;
        std.mem.writeInt(u64, next.spec.attempt[0..8], 2, .big);
        try std.testing.expectError(error.IntegrityTopologyBusy, db.nativeSourcePlanControl(next));
        // Existing artifacts can finish while topology waits for their debt.
        const repeated = try db.nativeSourcePlanControl(request);
        try std.testing.expectEqual(original_position, repeated.sealed_position.?);
        try db.applyRelationalTopologyControl(.{ .fence = fence, .action = .cancel }, null);
    }
    var ahead = query;
    if (raft) ahead.minimum_applied_index = 100 else ahead.minimum_native_sequence = 100;
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, db.sourcePlanObservation(ahead));
    var wrong = query;
    wrong.root.generation += 1;
    wrong.spec = null;
    try std.testing.expectError(error.HistoryResynchronizationRequired, db.sourcePlanObservation(wrong));
    request.action = .fence;
    if (raft) {
        _ = try db.applyOrderedSourcePlanControl(request, .{ .term = 2, .index = 5 }, false);
        try std.testing.expect((try db.orderedSourcePlanOutcome(.{ .request = .{ .root = root, .spec = spec, .action = .seal }, .entry = .{ .term = 1, .index = 4 }, .minimum_applied_index = 5 })) == .superseded);
    } else _ = try db.nativeSourcePlanControl(request);
    try std.testing.expectError(error.SourceReceiptExpired, db.sourcePlanChunk(a, .{ .query = query, .seal = seal, .ordinal = 0 }));
    request.action = .reclaim;
    const reclaimed = if (raft) (try db.applyOrderedSourcePlanControl(request, .{ .term = 2, .index = 6 }, false)).applied else try db.nativeSourcePlanControl(request);
    try std.testing.expect(reclaimed.reclamation != null and !reclaimed.present);
    db.close();
    open = false;
    db = try db_mod.DB.open(a, path, options);
    open = true;
    const final = try db.sourcePlanObservation(query);
    try std.testing.expectEqual(@as(u16, 0), final.quota.active);
    try std.testing.expect((try db.relationalTopologyStatus()).drained);
    try std.testing.expectEqual(reclaimed.reclamation.?, final.status.?.reclamation.?);
    request.action = .begin;
    if (raft) {
        const expired = try db.applyOrderedSourcePlanControl(request, .{ .term = 2, .index = 7 }, false);
        try std.testing.expect(expired == .rejected and expired.rejected == .expired);
    } else try std.testing.expectError(error.SourceReceiptExpired, db.nativeSourcePlanControl(request));
    try std.testing.expectEqualSlices(u8, &frame, read.bytes);
}
