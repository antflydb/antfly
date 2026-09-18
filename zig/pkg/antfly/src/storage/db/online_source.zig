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

//! Transactional source retention lifecycle. Exactly sixteen reusable catalog
//! slots bound metadata growth; the retained journal's epoch high-watermark
//! prevents old admissions from resurrecting after a terminal slot is reused.
const std = @import("std");
const retained = @import("../retained_effects.zig");
const topology = @import("relational_integrity_topology.zig");
const contract = @import("online_source_contract.zig");
pub const Scope = contract.Scope;
pub const Command = contract.Command;
const prefix = "\x00\x00__metadata__:online_source:";
const snapshot_contract = @import("../source_snapshot.zig");
const certificate_offset = 207;
const checksum_offset = certificate_offset + snapshot_contract.encoded_size;
const encoded_size = checksum_offset + 32;
pub const Phase = enum(u8) { retaining = 1, fenced = 2, released = 3 };
pub const SnapshotPhase = enum(u8) { prepared = 1, pinned = 2, published = 3 };
pub const Progress = struct {
    namespace: [24]u8,
    consumer_epoch: u64,
    pin: [32]u8,
    start: u64,
    acknowledged: u64,
    phase: Phase = .retaining,
    through_sequence: u64 = 0,
    applied_index: u64 = 0,
    cut_digest: [32]u8 = @splat(0),
    admitted_applied_index: u64 = 0,
    snapshot_certificate: [32]u8 = @splat(0),
    /// Publication authority must survive loss of the local sidecars and a
    /// metadata CAS response. The complete fixed-size certificate is committed
    /// with its digest, not reconstructed from the current live primary.
    published_certificate: ?snapshot_contract.Certificate = null,
    snapshot_phase: SnapshotPhase = .prepared,
    local_seal_digest: [32]u8 = @splat(0),
    local_cleanup_complete: bool = false,
};

fn key(slot: usize) [prefix.len + 1]u8 {
    var result: [prefix.len + 1]u8 = undefined;
    @memcpy(result[0..prefix.len], prefix);
    result[prefix.len] = @intCast(slot);
    return result;
}
fn digest(bytes: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
    return result;
}
fn encode(value: Progress) [encoded_size]u8 {
    var bytes: [encoded_size]u8 = undefined;
    @memcpy(bytes[0..4], "AOS4");
    @memcpy(bytes[4..28], &value.namespace);
    std.mem.writeInt(u64, bytes[28..36], value.consumer_epoch, .little);
    @memcpy(bytes[36..68], &value.pin);
    bytes[68] = @intFromEnum(value.phase);
    std.mem.writeInt(u64, bytes[69..77], value.start, .little);
    std.mem.writeInt(u64, bytes[77..85], value.acknowledged, .little);
    std.mem.writeInt(u64, bytes[85..93], value.through_sequence, .little);
    std.mem.writeInt(u64, bytes[93..101], value.applied_index, .little);
    @memcpy(bytes[101..133], &value.cut_digest);
    std.mem.writeInt(u64, bytes[133..141], value.admitted_applied_index, .little);
    @memcpy(bytes[141..173], &value.snapshot_certificate);
    bytes[173] = @intFromEnum(value.snapshot_phase);
    @memcpy(bytes[174..206], &value.local_seal_digest);
    bytes[206] = @intFromBool(value.local_cleanup_complete);
    @memset(bytes[certificate_offset..checksum_offset], 0);
    if (value.published_certificate) |certificate| {
        const encoded = certificate.encode() catch unreachable; // Validated before atomic publication.
        @memcpy(bytes[certificate_offset..checksum_offset], &encoded);
    }
    @memcpy(bytes[checksum_offset..encoded_size], &digest(bytes[0..checksum_offset]));
    return bytes;
}
fn decode(bytes: []const u8) !Progress {
    if (bytes.len != encoded_size or !std.mem.eql(u8, bytes[0..4], "AOS4") or bytes[206] > 1 or
        !std.mem.eql(u8, bytes[checksum_offset..encoded_size], &digest(bytes[0..checksum_offset]))) return error.OnlineSourceCorrupt;
    const result: Progress = .{
        .namespace = bytes[4..28].*,
        .consumer_epoch = std.mem.readInt(u64, bytes[28..36], .little),
        .pin = bytes[36..68].*,
        .phase = std.enums.fromInt(Phase, bytes[68]) orelse return error.OnlineSourceCorrupt,
        .start = std.mem.readInt(u64, bytes[69..77], .little),
        .acknowledged = std.mem.readInt(u64, bytes[77..85], .little),
        .through_sequence = std.mem.readInt(u64, bytes[85..93], .little),
        .applied_index = std.mem.readInt(u64, bytes[93..101], .little),
        .cut_digest = bytes[101..133].*,
        .admitted_applied_index = std.mem.readInt(u64, bytes[133..141], .little),
        .snapshot_certificate = bytes[141..173].*,
        .published_certificate = if (std.mem.allEqual(u8, bytes[certificate_offset..checksum_offset], 0)) null else snapshot_contract.Certificate.decode(bytes[certificate_offset..checksum_offset]) catch return error.OnlineSourceCorrupt,
        .snapshot_phase = std.enums.fromInt(SnapshotPhase, bytes[173]) orelse return error.OnlineSourceCorrupt,
        .local_seal_digest = bytes[174..206].*,
        .local_cleanup_complete = bytes[206] == 1,
    };
    if (result.consumer_epoch == 0 or result.admitted_applied_index == 0 or result.start > result.acknowledged or (result.local_cleanup_complete and result.phase != .released) or
        (result.phase == .fenced and result.acknowledged > result.through_sequence) or
        ((result.snapshot_phase == .prepared) != std.mem.allEqual(u8, &result.local_seal_digest, 0)) or
        ((result.snapshot_phase == .published) != !std.mem.allEqual(u8, &result.snapshot_certificate, 0)) or
        ((result.snapshot_phase == .published) != (result.published_certificate != null))) return error.OnlineSourceCorrupt;
    if (result.published_certificate) |certificate| {
        if (!std.mem.eql(u8, &contract.namespaceBytes(certificate.cut.namespace), &result.namespace) or
            certificate.cut.applied_index != result.admitted_applied_index or certificate.cut.retained_start != result.start or
            !std.mem.eql(u8, &try certificate.digest(), &result.snapshot_certificate)) return error.OnlineSourceCorrupt;
    }
    return result;
}
fn load(txn: anytype, slot: usize) !?Progress {
    const bytes = txn.get(&key(slot)) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    return try decode(bytes);
}

test "relational index system source publication retains full certificate and rejects inconsistent authority" {
    const certificate: snapshot_contract.Certificate = .{
        .cut = .{ .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .applied_index = 7, .retained_start = 3 },
        .objects = 1,
        .content_bytes = 19,
        .schema_manifest_digest = @splat(1),
        .ordered_content_digest = @splat(2),
    };
    var progress: Progress = .{
        .namespace = contract.namespaceBytes(certificate.cut.namespace),
        .consumer_epoch = 1,
        .pin = @splat(3),
        .start = 3,
        .acknowledged = 3,
        .admitted_applied_index = 7,
        .snapshot_certificate = try certificate.digest(),
        .published_certificate = certificate,
        .snapshot_phase = .published,
        .local_seal_digest = @splat(4),
    };
    var bytes = encode(progress);
    const recovered = try decode(&bytes);
    try std.testing.expect(certificate.eql(recovered.published_certificate.?));
    // A valid outer checksum must not hide a mismatched certificate digest.
    bytes[141] ^= 1;
    @memcpy(bytes[checksum_offset..], &digest(bytes[0..checksum_offset]));
    try std.testing.expectError(error.OnlineSourceCorrupt, decode(&bytes));
    progress.published_certificate.?.cut.applied_index += 1;
    progress.snapshot_certificate = try progress.published_certificate.?.digest();
    bytes = encode(progress);
    try std.testing.expectError(error.OnlineSourceCorrupt, decode(&bytes));
    progress.published_certificate = null;
    bytes = encode(progress);
    try std.testing.expectError(error.OnlineSourceCorrupt, decode(&bytes));

    const Txn = struct {
        bytes: []const u8,
        fn get(self: *@This(), target: []const u8) anyerror![]const u8 {
            if (!std.mem.eql(u8, target, &key(0))) return error.NotFound;
            return self.bytes;
        }
    };
    progress.snapshot_phase = .pinned;
    progress.snapshot_certificate = @splat(0);
    bytes = encode(progress);
    var txn = Txn{ .bytes = &bytes };
    try std.testing.expectError(error.OnlineSourcePinPending, requireTransferableSnapshots(&txn, progress.namespace));
    // A retired pin no longer needs to be replayed/materialized on a new peer.
    progress.phase = .released;
    bytes = encode(progress);
    try requireTransferableSnapshots(&txn, progress.namespace);
    bytes = encode(recovered);
    try requireTransferableSnapshots(&txn, recovered.namespace);
}

/// Native Raft checkpoints carry the primary ledger, not an unpublished
/// source cut's local checkpoint sidecars. Retain the admission WAL until the
/// cut has a replicated transferable certificate. This bounded eligibility
/// check does not fence ordinary source writes or require a table scan.
pub fn requireTransferableSnapshots(txn: anytype, namespace: [24]u8) !void {
    for (0..retained.max_consumers) |slot| if (try load(txn, slot)) |value| {
        if (std.mem.eql(u8, &value.namespace, &namespace) and value.phase != .released and
            value.snapshot_phase != .published) return error.OnlineSourcePinPending;
    };
}
fn requireNamespace(txn: anytype, scope: Scope) !void {
    const raw = txn.get(&@import("../internal_keys.zig").identity_namespace_key) catch |err| switch (err) {
        error.NotFound => return error.RetainedEffectsIdentityRequired,
        else => return err,
    };
    if (!std.mem.eql(u8, raw, &scope.namespace())) return error.RetainedEffectsNamespaceMismatch;
}
pub const Located = struct { slot: usize, progress: Progress };
pub fn locate(txn: anytype, scope: Scope) !Located {
    return try find(txn, scope) orelse error.OnlineSourceScopeChanged;
}
pub fn pendingCleanupAt(txn: anytype, slot: usize) !?Located {
    if (slot >= retained.max_consumers) return error.OnlineSourceCorrupt;
    if (try load(txn, slot)) |value| {
        if (value.phase == .released and !value.local_cleanup_complete) return .{ .slot = slot, .progress = value };
    }
    return null;
}
pub fn stageCleanupComplete(txn: anytype, expected: Located) !void {
    var current = try load(txn, expected.slot) orelse return error.OnlineSourceScopeChanged;
    if (current.phase != .released or !std.mem.eql(u8, &current.pin, &expected.progress.pin) or !std.mem.eql(u8, &current.namespace, &expected.progress.namespace)) return error.OnlineSourceScopeChanged;
    if (current.local_cleanup_complete) return;
    current.local_cleanup_complete = true;
    try txn.put(&key(expected.slot), &encode(current));
}
fn find(txn: anytype, scope: Scope) !?Located {
    try scope.validate();
    try requireNamespace(txn, scope);
    for (0..retained.max_consumers) |slot| if (try load(txn, slot)) |value| {
        if (!std.mem.eql(u8, &value.namespace, &scope.namespace()) or value.consumer_epoch != scope.consumer_epoch) continue;
        if (!std.mem.eql(u8, &value.pin, &scope.pin())) return error.OnlineSourceScopeChanged;
        return .{ .slot = slot, .progress = value };
    };
    return null;
}
pub fn status(txn: anytype, scope: Scope) !Progress {
    const progress = (try find(txn, scope) orelse return error.OnlineSourceScopeChanged).progress;
    if (progress.phase == .released) return progress;
    const journal = try retained.load(txn) orelse return error.OnlineSourceCorrupt;
    if (!std.mem.eql(u8, &journal.namespace, &progress.namespace) or
        (progress.phase == .fenced and journal.latest != progress.through_sequence)) return error.OnlineSourceCorrupt;
    for (journal.consumers) |consumer| if (consumer.epoch == progress.consumer_epoch) {
        if (!std.mem.eql(u8, &consumer.pin, &progress.pin) or consumer.start != progress.start or
            consumer.acknowledged != progress.acknowledged) return error.OnlineSourceCorrupt;
        return progress;
    };
    return error.OnlineSourceCorrupt;
}

/// Called only by the native immutable snapshot publication path, never from
/// a user digest or a recaptured live owner. The full validated certificate
/// must describe this admission's exact source cut; retries are exact CAS.
pub fn stageCertificate(txn: anytype, scope: Scope, certificate: @import("../source_snapshot.zig").Certificate) !void {
    if (certificate.integrity) |binding| if (!std.mem.eql(u8, &binding.catalog_digest, &scope.fence.catalog_digest)) return error.SourceSnapshotCutMismatch;
    const certificate_digest = try certificate.digest();
    const found = try find(txn, scope) orelse return error.OnlineSourceScopeChanged;
    var progress = found.progress;
    if (progress.snapshot_phase == .prepared) return error.OnlineSourcePinPending;
    if (progress.phase == .released or !certificate.cut.namespace.eql(scope.fence.namespace) or
        certificate.cut.retained_start != progress.start or certificate.cut.applied_index != progress.admitted_applied_index)
        return error.SourceSnapshotCutMismatch;
    if (!std.mem.allEqual(u8, &progress.snapshot_certificate, 0)) {
        if (!std.mem.eql(u8, &progress.snapshot_certificate, &certificate_digest)) return error.SourceSnapshotCutMismatch;
        return;
    }
    progress.snapshot_certificate = certificate_digest;
    progress.published_certificate = certificate;
    progress.snapshot_phase = .published;
    try txn.put(&key(found.slot), &encode(progress));
}

/// Replica-local physical receipt. It is never copied into the replicated
/// command: inode-bound seal hashes differ across independently pinned owners.
pub fn stagePinned(txn: anytype, scope: Scope, seal_digest: [32]u8) !void {
    const found = try find(txn, scope) orelse return error.OnlineSourceScopeChanged;
    var progress = found.progress;
    if (progress.phase == .released or std.mem.allEqual(u8, &seal_digest, 0)) return error.OnlineSourceScopeChanged;
    if (progress.snapshot_phase != .prepared) {
        if (!std.mem.eql(u8, &progress.local_seal_digest, &seal_digest)) return error.SourceSnapshotCutMismatch;
        return;
    }
    const pending = try @import("../source_pin_state.zig").load(txn) orelse return error.OnlineSourceCorrupt;
    if (!std.mem.eql(u8, &pending.namespace, &progress.namespace) or !std.mem.eql(u8, &pending.pin, &progress.pin) or
        pending.applied_index != progress.admitted_applied_index or pending.retained_start != progress.start) return error.OnlineSourceCorrupt;
    progress.local_seal_digest = seal_digest;
    progress.snapshot_phase = .pinned;
    try txn.delete(@import("../source_pin_state.zig").key);
    try txn.put(&key(found.slot), &encode(progress));
}

/// The caller owns the native apply fence and commits this with its Raft/standby
/// marker and outbox. For final_fence it must additionally check transaction
/// drainage under that same apply fence before calling stage.
pub fn stage(txn: anytype, command: Command, applied_index: u64) !void {
    try command.validate();
    // Snapshot certificates bind the source's explicit authority clock: Raft
    // index or atomically allocated native sequence, never a caller's guess.
    if ((command == .admit or command == .final_fence) and applied_index == 0) return error.InvalidOnlineSourceCommand;
    const scope = command.scope();
    const existing = try find(txn, scope);
    if (command == .admit) {
        if (existing) |found| {
            if (found.progress.phase == .released) return error.OnlineSourceScopeChanged;
            _ = try retained.admit(txn, scope.namespace(), scope.consumer_epoch, scope.pin(), command.admit.limit);
            return;
        }
        try @import("../source_pin_state.zig").requireNoPrepared(txn, scope.namespace());
        if (try topology.current(txn) != null) return error.IntegrityTopologyBusy;
        if (try topology.nextEpoch(txn) != scope.fence.admission_epoch) return error.IntegrityTopologyChanged;
        const catalog = try topology.catalogForFence(txn, scope.fence);
        var catalog_digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(catalog, &catalog_digest, .{});
        if (!std.mem.eql(u8, &catalog_digest, &scope.fence.catalog_digest)) return error.IntegrityCatalogChanged;
        var cleanup_pending = false;
        for (0..retained.max_consumers) |slot| {
            if (try load(txn, slot)) |old| if (!old.local_cleanup_complete) {
                cleanup_pending = cleanup_pending or old.phase == .released;
                continue;
            };
            const start = try retained.admit(txn, scope.namespace(), scope.consumer_epoch, scope.pin(), command.admit.limit);
            try txn.put(&key(slot), &encode(.{ .namespace = scope.namespace(), .consumer_epoch = scope.consumer_epoch, .pin = scope.pin(), .start = start, .acknowledged = start, .admitted_applied_index = applied_index }));
            try txn.put(@import("../source_pin_state.zig").key, &(@import("../source_pin_state.zig").Prepared{ .namespace = scope.namespace(), .pin = scope.pin(), .applied_index = applied_index, .retained_start = start, .scope_bytes = try scope.encode() }).encode());
            return;
        }
        if (cleanup_pending) return error.OnlineSourcePinPending;
        return error.RetainedEffectsConsumerLimit;
    }
    const found = existing orelse return error.OnlineSourceScopeChanged;
    var progress = found.progress;
    if (progress.phase == .released and command != .release and command != .reclaim) return error.OnlineSourceScopeChanged;
    switch (command) {
        .admit => unreachable,
        .publish_certificate => |value| return stageCertificate(txn, scope, value.certificate),
        .acknowledge => |value| {
            if (progress.phase == .fenced and value.next > progress.through_sequence) return error.RetainedEffectsCursorMismatch;
            try retained.acknowledge(txn, scope.namespace(), scope.consumer_epoch, scope.pin(), value.previous, value.next);
            progress.acknowledged = value.next;
        },
        .release => {
            if (progress.phase == .released) return;
            try retained.release(txn, scope.namespace(), scope.consumer_epoch, scope.pin());
            progress.phase = .released;
            if (try @import("../source_pin_state.zig").load(txn)) |pending| if (std.mem.eql(u8, &pending.namespace, &progress.namespace) and std.mem.eql(u8, &pending.pin, &progress.pin)) try txn.delete(@import("../source_pin_state.zig").key);
        },
        .final_fence => |value| {
            const active = try topology.current(txn) orelse return error.IntegrityTopologyChanged;
            if (!active.eql(scope.fence)) return error.IntegrityTopologyChanged;
            const state = try retained.load(txn) orelse return error.OnlineSourceScopeChanged;
            if (!std.mem.eql(u8, &state.namespace, &scope.namespace()) or state.latest != value.expected_sequence)
                return error.RetainedEffectsCursorMismatch;
            if (progress.phase == .fenced) {
                if (progress.through_sequence != value.expected_sequence) return error.RetainedEffectsCursorMismatch;
                return;
            }
            progress.phase = .fenced;
            progress.through_sequence = state.latest;
            progress.applied_index = applied_index;
            progress.cut_digest = @import("online_source_contract.zig").finalCutDigest(scope, progress.through_sequence, applied_index);
        },
        .reclaim => |value| {
            _ = try retained.reclaim(txn, scope.namespace(), value.frame_limit, value.byte_limit);
            return;
        },
    }
    try txn.put(&key(found.slot), &encode(progress));
}

test "relational index system online source controls survive LSM reopen with atomic abort and final fences" {
    const db_mod = @import("db.zig");
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/online-source", .{tmp.sub_path});
    defer alloc.free(path);
    const options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var scope: Scope = undefined;
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        try db.setSchemaJson(alloc, "{}");
        const owner = try db.relationalTopologyIdentity();
        scope = .{
            .fence = .{ .admission_epoch = owner.next_epoch, .transition_id = 44, .attempt = 1, .peer_group_id = 3, .owner_group_id = 2, .role = .merge_source, .namespace = owner.namespace, .catalog_digest = owner.catalog_digest },
            .receiver_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 },
            .consumer_epoch = 1,
            .copy_attempt = .{ .donor_term = 1, .sequence = 1 },
        };
        var uncertifiable = scope;
        uncertifiable.fence.namespace.range_id = 0;
        try std.testing.expectError(error.InvalidOnlineSourceCommand, uncertifiable.validate());
        try std.testing.expectError(error.InvalidOnlineSourceCommand, db.batchRaftReplicatedApply(.{ .online_source = .{ .admit = .{ .scope = uncertifiable, .limit = retained.max_frame_bytes } } }, .{ .term = 1, .index = 1 }));
        try std.testing.expectError(error.InvalidOnlineSourceCommand, db.batchRaftReplicatedApply(.{ .online_source = .{ .admit = .{ .scope = scope } }, .restore_staging_plan_id = @splat(1) }, .{ .term = 1, .index = 1 }));
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            try std.testing.expect(try retained.load(&read) == null);
        }
        try db.batchRaftReplicatedApply(.{ .online_source = .{ .admit = .{ .scope = scope, .limit = retained.max_frame_bytes } } }, .{ .term = 1, .index = 1 });
        try std.testing.expectEqual(@as(u64, 0), (try db.onlineSourceStatus(scope)).start);
        try std.testing.expectEqual(@as(u64, 1), (try db.onlineSourceStatus(scope)).admitted_applied_index);
        {
            var txn = try db.core.store.beginWriteTxn();
            defer txn.abort();
            const table = @import("table_catalog.zig");
            var facts = try table.Catalog.decode(try txn.get(table.key));
            facts.storage_mode = .relational;
            try txn.put(table.key, &facts.encode());
            try txn.delete(@import("relational_integrity_catalog.zig").key);
            var missing_catalog = scope;
            missing_catalog.consumer_epoch = 2;
            try std.testing.expectError(error.IntegrityCatalogChanged, stage(&txn, .{ .admit = .{ .scope = missing_catalog, .limit = retained.max_frame_bytes } }, 2));
        }
        // Admission itself leaves source writes online, with a single retained
        // after-image frame committed alongside the two rows and apply marker.
        try db.batchRaftReplicatedApply(.{ .timestamp_ns = 111, .writes = &.{ .{ .key = "a", .value = "{\"v\":1}" }, .{ .key = "b", .value = "{\"v\":2}" } } }, .{ .term = 1, .index = 2 });
        try std.testing.expectEqual(@as(u64, 1), db.core.table_catalog.row_count);
        var second = scope;
        second.consumer_epoch = 2;
        // Reserve a complete maximum frame before admitting another consumer:
        // a lagging receiver cannot reduce write headroom to a zero-progress trap.
        try std.testing.expectError(error.RetainedEffectsFull, db.batchRaftReplicatedApply(.{ .online_source = .{ .admit = .{ .scope = second, .limit = retained.max_frame_bytes } } }, .{ .term = 1, .index = 3 }));
        try std.testing.expectError(error.OnlineSourceScopeChanged, db.onlineSourceStatus(second));
        {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            var frame = (try retained.read(&read, scope.namespace(), scope.consumer_epoch, scope.pin(), 0)).?;
            try std.testing.expectEqual(@as(u32, 2), frame.remaining);
            try std.testing.expect((try frame.next()) != null);
        }
        // A staged ACK that aborts cannot expose either catalog acknowledgement
        // or reclaimed history to the next transaction.
        {
            var txn = try db.core.store.beginWriteTxn();
            defer txn.abort();
            try stage(&txn, .{ .acknowledge = .{ .scope = scope, .previous = 0, .next = 1 } }, 0);
        }
        try std.testing.expectEqual(@as(u64, 0), (try db.onlineSourceStatus(scope)).acknowledged);
        try std.testing.expectError(error.IntegrityTopologyFenceMissing, db.batchRaftReplicatedApply(.{ .online_source = .{ .final_fence = .{ .scope = scope, .expected_sequence = 1 } } }, .{ .term = 1, .index = 3 }));
        var wrong = scope;
        wrong.fence.attempt += 1;
        try std.testing.expectError(error.OnlineSourceScopeChanged, db.batchRaftReplicatedApply(.{ .online_source = .{ .acknowledge = .{ .scope = wrong, .previous = 0, .next = 1 } } }, .{ .term = 1, .index = 3 }));
        try db.batchRaftReplicatedApply(.{ .online_source = .{ .acknowledge = .{ .scope = scope, .previous = 0, .next = 1 } } }, .{ .term = 1, .index = 3 });
        try db.batchRaftReplicatedApply(.{ .relational_topology = .{ .fence = scope.fence, .action = .begin } }, .{ .term = 1, .index = 4 });
        try std.testing.expectError(error.RetainedEffectsCursorMismatch, db.batchRaftReplicatedApply(.{ .online_source = .{ .final_fence = .{ .scope = scope, .expected_sequence = 0 } } }, .{ .term = 1, .index = 5 }));
        try db.batchRaftReplicatedApply(.{ .online_source = .{ .final_fence = .{ .scope = scope, .expected_sequence = 1 } } }, .{ .term = 1, .index = 5 });
        const cut = try db.onlineSourceStatus(scope);
        try std.testing.expectEqual(Phase.fenced, cut.phase);
        try std.testing.expectEqual(@as(u64, 5), cut.applied_index);
        try std.testing.expect(!std.mem.allEqual(u8, &cut.cut_digest, 0));
    }
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        const cut = try db.onlineSourceStatus(scope);
        try std.testing.expectEqual(Phase.fenced, cut.phase);
        try db.batchRaftReplicatedApply(.{ .online_source = .{ .final_fence = .{ .scope = scope, .expected_sequence = 1 } } }, .{ .term = 1, .index = 5 });
        try std.testing.expectEqualSlices(u8, &cut.cut_digest, &(try db.onlineSourceStatus(scope)).cut_digest);
        try db.batchRaftReplicatedApply(.{ .online_source = .{ .release = scope } }, .{ .term = 1, .index = 6 });
        try db.batchRaftReplicatedApply(.{ .online_source = .{ .reclaim = .{ .scope = scope, .frame_limit = 1, .byte_limit = 1 } } }, .{ .term = 1, .index = 7 });
        // An old admission receipt is still a no-op after release/reclaim;
        // replay must not resurrect the pin or require its removed files.
        try db.batchRaftReplicatedApply(.{ .online_source = .{ .admit = .{ .scope = scope } } }, .{ .term = 1, .index = 1 });
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        const state = (try retained.load(&read)).?;
        try std.testing.expectEqual(@as(u64, 0), state.retained_bytes);
        try std.testing.expectEqual(@as(u64, 1), state.reclaimed);
        try std.testing.expectError(error.OnlineSourceScopeChanged, db.batchRaftReplicatedApply(.{ .online_source = .{ .admit = .{ .scope = scope } } }, .{ .term = 1, .index = 8 }));
    }
}

test "relational index system online source standby replay preserves admission certificate and final cut clocks" {
    const db_mod = @import("db.zig");
    const effects = @import("../hot_standby/effects.zig");
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const source_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/source", .{tmp.sub_path});
    defer alloc.free(source_path);
    const standby_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/standby", .{tmp.sub_path});
    defer alloc.free(standby_path);
    const options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var source = try db_mod.DB.open(alloc, source_path, options);
    defer source.close();
    var standby = try db_mod.DB.open(alloc, standby_path, options);
    defer standby.close();
    try source.setSchemaJson(alloc, "{}");
    try standby.setSchemaJson(alloc, "{}");
    const owner = try source.relationalTopologyIdentity();
    const scope: Scope = .{
        .fence = .{ .admission_epoch = owner.next_epoch, .transition_id = 44, .attempt = 1, .peer_group_id = 3, .owner_group_id = 2, .role = .merge_source, .namespace = owner.namespace, .catalog_digest = owner.catalog_digest },
        .receiver_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 },
        .consumer_epoch = 1,
        .copy_attempt = .{ .donor_term = 1, .sequence = 1 },
    };
    var certificate: @import("../source_snapshot.zig").Certificate = .{
        .cut = .{ .namespace = scope.fence.namespace, .applied_index = 11, .retained_start = 0 },
        .objects = 1,
        .content_bytes = 100,
        .schema_manifest_digest = @splat(0x80),
        .ordered_content_digest = @splat(0x90),
    };
    var commands = [_]@import("types.zig").BatchRequest{
        .{ .online_source = .{ .admit = .{ .scope = scope } } },
        .{ .online_source = .{ .publish_certificate = .{ .scope = scope, .certificate = certificate } } },
        .{ .relational_topology = .{ .fence = scope.fence, .action = .begin } },
        .{ .online_source = .{ .final_fence = .{ .scope = scope, .expected_sequence = 0 } } },
    };
    for (0..commands.len) |i| {
        const request = commands[i];
        const raft_index: u64 = 11 + @as(u64, @intCast(i));
        const lsn: u64 = 1 + @as(u64, @intCast(i));
        try source.batchRaftReplicatedApply(request, .{ .term = 1, .index = raft_index });
        const payload = if (request.online_source != null)
            try effects.encodeOnlineSourceMutationRequestAlloc(alloc, request, raft_index)
        else
            try effects.encodeBatchMutationRequestAlloc(alloc, request);
        defer alloc.free(payload);
        const record: @import("../hot_standby/replication_record.zig").RecordView = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = lsn, .previous_lsn = lsn - 1, .payload = payload };
        try standby.applyHAReplicationRecord(record);
        try standby.applyHAReplicationRecord(record);
        if (i == 0) {
            certificate = try source.prepareOnlineSourcePublication(scope, .none);
            const replica_certificate = try standby.prepareOnlineSourcePublication(scope, .none);
            try std.testing.expect(certificate.eql(replica_certificate));
            commands[1] = .{ .online_source = .{ .publish_certificate = .{ .scope = scope, .certificate = certificate } } };
        }
        const primary_value = try source.onlineSourceStatus(scope);
        var standby_value = try standby.onlineSourceStatus(scope);
        // Local seals bind replica-local inode inventories; only logical cut,
        // consumer state and the transferable certificate must be identical.
        try std.testing.expect(!std.mem.allEqual(u8, &standby_value.local_seal_digest, 0));
        standby_value.local_seal_digest = primary_value.local_seal_digest;
        const primary_progress = encode(primary_value);
        const standby_progress = encode(standby_value);
        try std.testing.expectEqualSlices(u8, &primary_progress, &standby_progress);
        try std.testing.expectEqual(lsn, try standby.haAppliedReplicationLsn());
    }
    try std.testing.expectEqual(@as(u64, 11), (try standby.onlineSourceStatus(scope)).admitted_applied_index);
    try std.testing.expectEqual(@as(u64, 14), (try standby.onlineSourceStatus(scope)).applied_index);
    const expected_certificate = try certificate.digest();
    try std.testing.expectEqualSlices(u8, &expected_certificate, &(try standby.onlineSourceStatus(scope)).snapshot_certificate);
    var wrong = certificate;
    wrong.cut.retained_start = 1;
    try std.testing.expectError(error.SourceSnapshotCutMismatch, source.batchRaftReplicatedApply(.{ .online_source = .{ .publish_certificate = .{ .scope = scope, .certificate = wrong } } }, .{ .term = 1, .index = 15 }));
    try std.testing.expectEqualSlices(u8, &expected_certificate, &(try source.onlineSourceStatus(scope)).snapshot_certificate);
}

test "relational index system online source durable standby outbox resumes before already applied Raft receipt" {
    try sourceOutboxRecovery(false);
}

test "relational index system native rewrite source clock is forwarded by durable outbox across lost acknowledgement" {
    try sourceOutboxRecovery(true);
}

fn sourceOutboxRecovery(native_authority: bool) !void {
    const db_mod = @import("db.zig");
    const primary_mod = @import("../hot_standby/primary.zig");
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    const path = try std.fmt.allocPrint(owned, ".zig-cache/tmp/{s}/source-outbox", .{tmp.sub_path});
    const log_path = try std.fmt.allocPrintSentinel(owned, ".zig-cache/tmp/{s}/log", .{tmp.sub_path}, 0);
    const slots_path = try std.fmt.allocPrintSentinel(owned, ".zig-cache/tmp/{s}/slots", .{tmp.sub_path}, 0);
    var primary = try primary_mod.Primary.open(alloc, log_path, slots_path, .{ .cluster_id = 1, .timeline_id = 1, .epoch = 1, .table_id = 1, .shard_id = 2 }, .{});
    defer primary.close();
    try primary.createSlot("standby", 0);
    const Ack = struct {
        calls: usize = 0,
        fn wait(ptr: *anyopaque, stream: *primary_mod.Primary, lsn: u64, _: primary_mod.SyncPolicy) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.calls == 1) return error.InjectedSourceMirrorWaitFailure;
            try stream.standbyStatusUpdate("standby", 1, lsn, lsn);
        }
    };
    var ack: Ack = .{};
    const options: db_mod.OpenOptions = .{ .online_source_authority = if (native_authority) .native else null, .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var scope: Scope = undefined;
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        try db.setSchemaJson(alloc, "{}");
        const owner = try db.relationalTopologyIdentity();
        scope = .{
            .authority = if (native_authority) .native else .raft,
            .fence = .{ .admission_epoch = owner.next_epoch, .transition_id = 44, .attempt = 1, .peer_group_id = 3, .owner_group_id = 2, .role = if (native_authority) .rewrite_source else .merge_source, .namespace = owner.namespace, .catalog_digest = owner.catalog_digest },
            .receiver_namespace = .{ .table_id = if (native_authority) 4 else 1, .shard_id = 3, .range_id = 3 },
            .consumer_epoch = 1,
            .copy_attempt = .{ .donor_term = if (native_authority) 0 else 1, .sequence = 1 },
        };
        db.ha_async_batch_mirror = .{ .primary = &primary, .sync_policy = .{ .mode = .remote_write, .standby_names = &.{"standby"}, .failure_policy = .block }, .sync_wait_ctx = &ack, .sync_wait_fn = Ack.wait };
        const request: @import("types.zig").BatchRequest = .{ .online_source = .{ .admit = .{ .scope = scope } } };
        try std.testing.expectError(error.InjectedSourceMirrorWaitFailure, if (native_authority) db.batch(request) else db.batchRaftReplicatedApply(request, .{ .term = 1, .index = 11 }));
        try std.testing.expectEqual(@as(u64, if (native_authority) 1 else 11), (try db.onlineSourceStatus(scope)).admitted_applied_index);
        try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
        // Simulate loss of process-local mirror state before its outstanding
        // acknowledgement can complete. The owner/outbox remain durable.
        db.ha_async_batch_mirror = null;
    }
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        db.ha_async_batch_mirror = .{ .primary = &primary, .sync_policy = .{ .mode = .remote_write, .standby_names = &.{"standby"}, .failure_policy = .block }, .sync_wait_ctx = &ack, .sync_wait_fn = Ack.wait };
        const request: @import("types.zig").BatchRequest = .{ .online_source = .{ .admit = .{ .scope = scope } } };
        if (native_authority) try db.batch(request) else try db.batchRaftReplicatedApply(request, .{ .term = 1, .index = 11 });
        try std.testing.expect(ack.calls >= 2);
        try std.testing.expectEqual(@as(u64, if (native_authority) 2 else 1), primary.lastLsn());
        var entry = (try primary.log.entryAt(alloc, 1)) orelse return error.TestUnexpectedResult;
        defer entry.deinit(alloc);
        var decoded = try @import("../hot_standby/effects.zig").decodeBatchMutationRequest(alloc, entry.record);
        defer decoded.deinit();
        try std.testing.expectEqual(@as(?u64, if (native_authority) 1 else 11), decoded.value.online_source_applied_index);
        try std.testing.expectEqualSlices(u8, &scope.pin(), &decoded.value.request.online_source.?.scope().pin());
        if (native_authority) {
            var retry = (try primary.log.entryAt(alloc, 2)).?;
            defer retry.deinit(alloc);
            var retry_decoded = try @import("../hot_standby/effects.zig").decodeBatchMutationRequest(alloc, retry.record);
            defer retry_decoded.deinit();
            try std.testing.expectEqual(@as(?u64, 2), retry_decoded.value.online_source_applied_index);
            try std.testing.expectEqual(.native, retry_decoded.value.request.online_source.?.scope().authority);
            try std.testing.expectEqual(@as(u64, 1), (try db.onlineSourceStatus(scope)).admitted_applied_index);
            try std.testing.expect((try db.raftAppliedEntry()) == null);
        }
    }
}

test "relational index system native rewrite authority clocks survive pin crash ordinary writes and exact standby replay" {
    const DB = @import("db.zig");
    const clock = @import("../source_authority.zig");
    const effects = @import("../hot_standby/effects.zig");
    const pin = @import("source_pin.zig");
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const source_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/native-source", .{tmp.sub_path});
    const replica_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/native-replica", .{tmp.sub_path});
    const options: DB.OpenOptions = .{ .online_source_authority = .native, .identity_namespace = .{ .table_id = 1, .shard_id = 102, .range_id = 103 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var primary = try DB.DB.open(alloc, source_path, options);
    defer primary.close();
    var replica = try DB.DB.open(alloc, replica_path, options);
    defer replica.close();
    try primary.setSchemaJson(alloc, "{}");
    try replica.setSchemaJson(alloc, "{}");
    const owner = try primary.relationalTopologyIdentity();
    const scope: Scope = .{ .authority = .native, .fence = .{ .role = .rewrite_source, .transition_id = 45, .attempt = 1, .admission_epoch = owner.next_epoch, .owner_group_id = 2, .peer_group_id = 3, .namespace = owner.namespace, .catalog_digest = owner.catalog_digest }, .receiver_namespace = .{ .table_id = 4, .shard_id = 3, .range_id = 3 }, .consumer_epoch = 1, .copy_attempt = .{ .donor_term = 0, .sequence = 1 } };
    const Helper = struct {
        fn sequence(db: *DB.DB) !u64 {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            return (try clock.require(&read, .native, @import("online_source_contract.zig").namespaceBytes(db.core.identity_namespace))).sequence;
        }
        fn replay(db: *DB.DB, request: @import("types.zig").BatchRequest, sequence_value: u64, lsn: u64) !void {
            const payload = if (request.online_source != null) try effects.encodeOnlineSourceMutationRequestAlloc(alloc, request, sequence_value) else try effects.encodeBatchMutationRequestAlloc(alloc, request);
            defer alloc.free(payload);
            const record: @import("../hot_standby/replication_record.zig").RecordView = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = lsn, .previous_lsn = lsn - 1, .payload = payload };
            try db.applyHAReplicationRecord(record);
            try db.applyHAReplicationRecord(record);
        }
    };
    const admit: @import("types.zig").BatchRequest = .{ .online_source = .{ .admit = .{ .scope = scope } } };
    pin.test_failure = .after_prepare;
    defer pin.test_failure = .none;
    try std.testing.expectError(error.InjectedSourcePinFailure, primary.batch(admit));
    try std.testing.expectError(error.OnlineSourcePinPending, primary.batch(.{ .writes = &.{.{ .key = "blocked", .value = "{}" }} }));
    try std.testing.expectEqual(@as(u64, 1), try Helper.sequence(&primary));
    pin.test_failure = .none;
    primary.close();
    primary = try DB.DB.open(alloc, source_path, options);
    try std.testing.expectEqual(@as(u64, 1), (try primary.onlineSourceStatus(scope)).admitted_applied_index);
    pin.test_failure = .after_prepare;
    try std.testing.expectError(error.InjectedSourcePinFailure, Helper.replay(&replica, admit, 1, 1));
    pin.test_failure = .none;
    // The already-applied LSN must repair its pending pin before ACK, too.
    try Helper.replay(&replica, admit, 1, 1);
    try std.testing.expectEqual(.pinned, (try replica.onlineSourceStatus(scope)).snapshot_phase);
    const certificate = try primary.prepareOnlineSourcePublication(scope, .none);
    try std.testing.expect(certificate.eql(try replica.prepareOnlineSourcePublication(scope, .none)));
    const publish: @import("types.zig").BatchRequest = .{ .online_source = .{ .publish_certificate = .{ .scope = scope, .certificate = certificate } } };
    try primary.batch(publish);
    try Helper.replay(&replica, publish, 2, 2);
    const mutation: @import("types.zig").BatchRequest = .{ .writes = &.{.{ .key = "after", .value = "{\"v\":7}" }}, .timestamp_ns = 71 };
    try primary.batch(mutation);
    try Helper.replay(&replica, mutation, 0, 3);
    try std.testing.expectEqual(@as(u64, 3), try Helper.sequence(&primary));
    try std.testing.expectEqual(@as(u64, 3), try Helper.sequence(&replica));
    const begin: @import("types.zig").BatchRequest = .{ .relational_topology = .{ .fence = scope.fence, .action = .begin } };
    try primary.batch(begin);
    try Helper.replay(&replica, begin, 0, 4);
    const finish: @import("types.zig").BatchRequest = .{ .online_source = .{ .final_fence = .{ .scope = scope, .expected_sequence = 1 } } };
    try std.testing.expectError(error.OnlineSourceScopeChanged, Helper.replay(&replica, finish, 9, 5));
    try std.testing.expectEqual(@as(u64, 4), try replica.haAppliedReplicationLsn());
    try primary.batch(finish);
    try Helper.replay(&replica, finish, 4, 5);
    const final = try primary.onlineSourceStatus(scope);
    try std.testing.expectEqual(@as(u64, 4), final.applied_index);
    try std.testing.expectEqualSlices(u8, &final.cut_digest, &(try replica.onlineSourceStatus(scope)).cut_digest);
    try std.testing.expect((try primary.raftAppliedEntry()) == null);
    try std.testing.expect((try replica.raftAppliedEntry()) == null);
    var forged = scope;
    forged.authority = .raft;
    forged.copy_attempt.donor_term = 1;
    try std.testing.expectError(error.OnlineSourceScopeChanged, primary.batchRaftReplicatedApply(.{ .online_source = .{ .admit = .{ .scope = forged } } }, .{ .term = 1, .index = 1 }));
    try std.testing.expectError(error.OnlineSourceScopeChanged, primary.batchRaftReplicatedApply(.{}, .{ .term = 1, .index = 1 }));
    try std.testing.expectEqual(@as(u64, 4), try Helper.sequence(&primary));
    replica.close();
    replica = try DB.DB.open(alloc, replica_path, options);
    try std.testing.expectEqual(@as(u64, 4), try Helper.sequence(&replica));
    try std.testing.expectEqualSlices(u8, &final.cut_digest, &(try replica.onlineSourceStatus(scope)).cut_digest);
}

test "relational index system unknown identity summary never preserves false empty admission after mutation" {
    const db_mod = @import("db.zig");
    const identity = @import("doc_identity.zig");
    const internal = @import("../internal_keys.zig");
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_]bool{ false, true }, 0..) |delete_only, trial| {
        const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/unknown-summary-{d}", .{ tmp.sub_path, trial });
        defer alloc.free(path);
        var db = try db_mod.DB.open(alloc, path, .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
        defer db.close();
        try db.setSchemaJson(alloc, "{}");
        // A no-op delete on a genuinely empty owner retains its empty proof.
        try db.batch(.{ .deletes = &.{"absent"} });
        try std.testing.expectEqual(@as(u64, 0), db.core.table_catalog.row_count);
        const old_key = try internal.documentKeyAlloc(alloc, "old");
        defer alloc.free(old_key);
        var namespace: [24]u8 = undefined;
        identity.encodeNamespace(&namespace, db.core.identity_namespace);
        // Reproduce an uncounted older primary row, without fabricating a
        // visibility summary that would falsely make the root countable.
        try db.core.store.putBatch(&.{ .{ .key = &internal.identity_namespace_key, .value = &namespace }, .{ .key = old_key, .value = "{}" } }, &.{&internal.identity_visibility_summary_key});
        try std.testing.expectEqual(@as(u64, 0), db.core.table_catalog.row_count);
        if (delete_only) {
            try db.batch(.{ .deletes = &.{"old"} });
        } else {
            try db.batch(.{ .writes = &.{.{ .key = "new", .value = "{}" }} });
        }
        try std.testing.expectEqual(@as(u64, 1), db.core.table_catalog.row_count);
        try db.batch(.{ .deletes = &.{ "old", "new" } });
        // Unknown history cannot be converted to an empty-table proof by
        // another mutation. Explicit reconciliation owns clearing that bit.
        try std.testing.expectEqual(@as(u64, 1), db.core.table_catalog.row_count);
    }
}

test "relational index system native source authority preserves same namespace and rebinds only durable adopted identity" {
    const DB = @import("db.zig");
    const clock = @import("../source_authority.zig");
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/native-adoption", .{tmp.sub_path});
    defer alloc.free(path);
    const old: @import("doc_identity_namespace.zig").Namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 };
    const adopted: @import("doc_identity_namespace.zig").Namespace = .{ .table_id = 11, .shard_id = 12, .range_id = 12 };
    var options: DB.OpenOptions = .{ .online_source_authority = .native, .identity_namespace = old, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    {
        var db = try DB.DB.open(alloc, path, options);
        defer db.close();
        try db.setSchemaJson(alloc, "{}");
        try db.batch(.{ .writes = &.{.{ .key = "row", .value = "{}" }} });
        const owner = try db.relationalTopologyIdentity();
        const scope: Scope = .{ .authority = .native, .fence = .{ .role = .rewrite_source, .transition_id = 1, .attempt = 1, .admission_epoch = owner.next_epoch, .owner_group_id = 2, .peer_group_id = 3, .namespace = old, .catalog_digest = owner.catalog_digest }, .receiver_namespace = .{ .table_id = 4, .shard_id = 3, .range_id = 3 }, .consumer_epoch = 1, .copy_attempt = .{ .donor_term = 0, .sequence = 1 } };
        try db.batch(.{ .online_source = .{ .admit = .{ .scope = scope } } });
        try db.batch(.{ .online_source = .{ .release = scope } });
        {
            var txn = try db.core.store.beginWriteTxn();
            defer txn.abort();
            try std.testing.expectEqual(@as(u64, 2), (try clock.require(&txn, .native, scope.namespace())).sequence);
            try std.testing.expectError(error.OnlineSourceScopeChanged, clock.bind(&txn, .native, @import("online_source_contract.zig").namespaceBytes(adopted)));
        }
    }
    {
        var db = try DB.DB.open(alloc, path, options);
        defer db.close();
        {
            var txn = try db.core.store.beginReadTxn();
            defer txn.abort();
            try std.testing.expectEqual(@as(u64, 2), (try clock.require(&txn, .native, @import("online_source_contract.zig").namespaceBytes(old))).sequence);
        }
        try db.reassignIdentityNamespaceForInternalTransition(adopted);
    }
    options.identity_namespace = adopted;
    {
        var db = try DB.DB.open(alloc, path, options);
        defer db.close();
        var txn = try db.core.store.beginReadTxn();
        defer txn.abort();
        try std.testing.expectEqual(@as(u64, 0), (try clock.require(&txn, .native, @import("online_source_contract.zig").namespaceBytes(adopted))).sequence);
        const row = (try db.lookup(alloc, "row", .{})).?;
        defer alloc.free(row.json);
    }
    options.online_source_authority = .raft;
    try std.testing.expectError(error.OnlineSourceScopeChanged, DB.DB.open(alloc, path, options));
}
