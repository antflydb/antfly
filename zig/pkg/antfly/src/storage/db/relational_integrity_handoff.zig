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

//! Bounded routed integrity metadata handoff between quiesced owners. Primary
//! rows travel through the ordinary split bootstrap; these records must never
//! be interpreted as primary document keys by its projection/delta codec.
const std = @import("std");
const topology = @import("relational_integrity_topology.zig");
const integrity = @import("relational_integrity.zig");
const catalog = @import("relational_integrity_catalog.zig");
const activation = @import("relational_integrity_activation.zig");
const docstore = @import("../docstore.zig");
const Allocator = std.mem.Allocator;
pub const manifest_key = @import("relational_integrity_handoff_contract.zig").manifest_key;
pub const progress_key = @import("relational_integrity_handoff_contract.zig").progress_key;
pub const prune_key = @import("relational_integrity_handoff_contract.zig").prune_key;
pub const max_records = @import("relational_integrity_handoff_contract.zig").max_records;
pub const max_bytes = @import("relational_integrity_handoff_contract.zig").max_bytes;

pub const Manifest = @import("relational_integrity_handoff_contract.zig").Manifest;
pub const Record = @import("relational_integrity_handoff_contract.zig").Record;
pub const Page = @import("relational_integrity_handoff_contract.zig").Page;
pub const Command = @import("relational_integrity_handoff_contract.zig").Command;
pub const Progress = @import("relational_integrity_handoff_contract.zig").Progress;

pub const encode = @import("relational_integrity_handoff_contract.zig").encode;

const hash = @import("relational_integrity_handoff_contract.zig").hash;

fn optional(txn: anytype, key: []const u8) !?[]const u8 {
    return txn.get(key) catch |err| {
        if (err == error.NotFound) return null;
        return err;
    };
}

pub fn loadProgress(alloc: Allocator, txn: anytype) !std.json.Parsed(Progress) {
    return std.json.parseFromSlice(Progress, alloc, (try optional(txn, progress_key)) orelse return error.IntegrityHandoffMissing, .{ .allocate = .alloc_always });
}

fn saveProgress(alloc: Allocator, txn: anytype, progress: Progress) !void {
    const bytes = try encode(alloc, progress);
    defer alloc.free(bytes);
    try txn.put(progress_key, bytes);
}

fn requireRange(lower: []const u8, upper: []const u8) !void {
    if (lower.len > 1024 * 1024 or upper.len > 1024 * 1024 or (upper.len != 0 and std.mem.order(u8, lower, upper) != .lt)) return error.InvalidRange;
}

fn contains(manifest: Manifest, key: []const u8) bool {
    return std.mem.order(u8, key, manifest.lower) != .lt and (manifest.upper.len == 0 or std.mem.order(u8, key, manifest.upper) == .lt);
}

/// Source producer runs under the source apply fence and verifies its durable
/// topology fence/drain separately before opening this current cursor. Three
/// bounded prefix seeks visit only the transferred logical range, not every
/// claim/reference in the source table when splitting a small tail range.
pub fn readPage(alloc: Allocator, store: *docstore.DocStore, manifest: Manifest, progress: Progress) !Page {
    try requireRange(manifest.lower, manifest.upper);
    var read = try store.beginCurrentScanTxn();
    defer read.abort();
    var cursor = try read.openCursor();
    defer cursor.close();
    var records: std.ArrayList(Record) = .empty;
    var bytes: usize = 0;
    var next_buf: [integrity.key_len + 32]u8 = undefined;
    var next: []const u8 = progress.cursor;
    var exhausted = true;
    const initial_kind = if (progress.cursor.len != 0) (try integrity.parseKey(progress.cursor)).kind else integrity.Kind.claim;
    outer: for ([_]integrity.Kind{ .claim, .reference, .job }) |kind| {
        if (@intFromEnum(kind) < @intFromEnum(initial_kind)) continue;
        var prefix: [integrity.namespace.len + 1]u8 = undefined;
        @memcpy(prefix[0..integrity.namespace.len], integrity.namespace);
        prefix[integrity.namespace.len] = @intFromEnum(kind);
        const first = try std.mem.concat(alloc, u8, &.{ &prefix, manifest.lower });
        defer alloc.free(first);
        const resuming = kind == initial_kind and progress.cursor.len != 0;
        var item = try cursor.seekAtOrAfter(if (resuming) progress.cursor else first);
        if (item) |entry| if (resuming and std.mem.eql(u8, entry.key, progress.cursor)) {
            item = try cursor.next();
        };
        while (item) |entry| : (item = try cursor.next()) {
            if (!std.mem.startsWith(u8, entry.key, &prefix)) break;
            const address = (try integrity.parseKey(entry.key)).address;
            if (manifest.upper.len != 0 and std.mem.order(u8, &address.routing, manifest.upper) != .lt) break;
            if (!contains(manifest, &address.routing)) return error.KeyOutOfRange;
            const size = std.math.add(usize, entry.key.len, entry.value.len) catch return error.IntegrityRecordTooLarge;
            if (records.items.len == max_records or size > max_bytes - bytes) {
                if (records.items.len == 0) return error.IntegrityRecordTooLarge;
                exhausted = false;
                break :outer;
            }
            bytes += size;
            _ = try integrity.validateTransferRecord(entry.key, entry.value);
            try records.append(alloc, .{ .key = try alloc.dupe(u8, entry.key), .value = try alloc.dupe(u8, entry.value) });
            @memcpy(next_buf[0..entry.key.len], entry.key);
            next = next_buf[0..entry.key.len];
        }
    }
    return .{ .sequence = std.math.add(u64, progress.sequence, 1) catch return error.IntegrityHandoffTooLarge, .previous_digest = progress.digest, .after = try alloc.dupe(u8, progress.cursor), .next_cursor = try alloc.dupe(u8, next), .records = try records.toOwnedSlice(alloc), .exhausted = exhausted };
}

/// Called inside the destination's one Raft apply transaction. The existing
/// topology fence is retained until primary publication and metadata cutover.
pub fn apply(alloc: Allocator, txn: anytype, fence: topology.Fence, command: Command) !void {
    const current = (try topology.current(txn)) orelse return error.IntegrityTopologyFenceMissing;
    if (!current.eql(fence)) return error.IntegrityTopologyChanged;
    if (fence.role != .split_destination and fence.role != .merge_destination) return error.InvalidIntegrityTopologyFence;
    if (fence.role == .merge_destination) {
        var state = try @import("merge_state.zig").decodeAlloc(alloc, (try optional(txn, @import("merge_state.zig").key)) orelse return error.IntegrityHandoffIncomplete);
        defer state.deinit(alloc);
        if (state.phase != .accepting or state.transition_id != fence.transition_id or state.donor_group_id != fence.peer_group_id) return error.IntegrityTopologyChanged;
    }
    switch (command) {
        .begin => |manifest| {
            if (!manifest.destination.eql(fence) or manifest.source.transition_id != fence.transition_id or
                manifest.source.attempt != fence.attempt or manifest.source.owner_group_id != fence.peer_group_id or
                manifest.source.peer_group_id != fence.owner_group_id or manifest.source.namespace.table_id != fence.namespace.table_id or
                manifest.source.role != (if (fence.role == .split_destination) topology.Role.split_source else topology.Role.merge_source)) return error.InvalidIntegrityTopologyFence;
            try requireRange(manifest.lower, manifest.upper);
            try requireRange(manifest.source_range_start, manifest.source_range_end);
            if (std.mem.order(u8, manifest.lower, manifest.source_range_start) == .lt or
                (manifest.source_range_end.len != 0 and (manifest.upper.len == 0 or std.mem.order(u8, manifest.upper, manifest.source_range_end) == .gt))) return error.KeyOutOfRange;
            if (manifest.catalog_bytes.len > max_bytes or manifest.activation_bytes.len > 4096) return error.IntegrityRecordTooLarge;
            if (fence.role == .merge_destination and (manifest.merge_copy_attempt.donor_term == 0 or manifest.merge_copy_attempt.sequence == 0)) return error.InvalidIntegrityTopologyFence;
            if (!std.mem.eql(u8, &hash(manifest.catalog_bytes), &manifest.source.catalog_digest)) return error.IntegrityCatalogChanged;
            var compiled = try catalog.decode(alloc, manifest.catalog_bytes);
            defer compiled.deinit();
            if (!std.mem.eql(u8, &compiled.incarnation, &(try catalog.incarnationFromTableId(fence.namespace.table_id)))) return error.IdentityNamespaceMismatch;
            const coverage = try activation.Progress.decode(manifest.activation_bytes);
            const source_range = try @import("range_state.zig").encodeRangeAlloc(alloc, .{ .start = manifest.source_range_start, .end = manifest.source_range_end });
            defer alloc.free(source_range);
            var source_namespace: [24]u8 = undefined;
            @import("doc_identity.zig").encodeNamespace(&source_namespace, manifest.source.namespace);
            var owner_state = std.crypto.hash.Blake3.init(.{});
            owner_state.update("antfly constraint coverage owner v1");
            owner_state.update(&source_namespace);
            owner_state.update(source_range);
            var owner: integrity.Digest = undefined;
            owner_state.final(&owner);
            if (coverage.state != .enforced or coverage.schema_version != compiled.schema_version or
                !std.mem.eql(u8, &coverage.owner, &owner) or !std.mem.eql(u8, &coverage.generation_set, &activation.generationSet(compiled))) return error.ConstraintActivationInProgress;
            const raw_manifest = try encode(alloc, manifest);
            defer alloc.free(raw_manifest);
            if (try optional(txn, manifest_key)) |existing| {
                if (!std.mem.eql(u8, existing, raw_manifest)) return error.IntegrityTopologyChanged;
                return;
            }
            // A merge receiver retains its own cohort. Its active definitions
            // must already have exactly the same generation identity.
            if (fence.role == .merge_destination) {
                const existing = (try optional(txn, catalog.key)) orelse return error.IntegrityCatalogChanged;
                if (!std.mem.eql(u8, existing, manifest.catalog_bytes)) return error.IntegrityCatalogChanged;
                var receiver = try @import("merge_state.zig").decodeAlloc(alloc, (try optional(txn, @import("merge_state.zig").key)) orelse return error.IntegrityHandoffIncomplete);
                defer receiver.deinit(alloc);
                if (receiver.phase != .accepting or receiver.transition_id != fence.transition_id or receiver.donor_group_id != fence.peer_group_id) return error.IntegrityTopologyChanged;
                const base = receiver.receiver_base_range;
                if (!((manifest.upper.len != 0 and std.mem.eql(u8, manifest.upper, base.start)) or
                    (base.end.len != 0 and std.mem.eql(u8, base.end, manifest.lower)))) return error.KeyOutOfRange;
            }
            try txn.put(catalog.key, manifest.catalog_bytes);
            try txn.put(manifest_key, raw_manifest);
            try saveProgress(alloc, txn, .{ .manifest_digest = hash(raw_manifest) });
        },
        .page => |page| {
            if (page.records.len > max_records or page.after.len > integrity.key_len + 32 or page.next_cursor.len > integrity.key_len + 32) return error.IntegrityRecordTooLarge;
            var admission_bytes: usize = 0;
            for (page.records) |record| {
                const size = std.math.add(usize, record.key.len, record.value.len) catch return error.IntegrityRecordTooLarge;
                admission_bytes = std.math.add(usize, admission_bytes, size) catch return error.IntegrityRecordTooLarge;
                if (admission_bytes > max_bytes) return error.IntegrityRecordTooLarge;
            }
            var state = try loadProgress(alloc, txn);
            defer state.deinit();
            const raw_page = try encode(alloc, page);
            defer alloc.free(raw_page);
            const page_digest = hash(raw_page);
            if (state.value.sequence == page.sequence and std.mem.eql(u8, &state.value.digest, &page_digest)) return;
            if (state.value.ready or state.value.exhausted or page.sequence != (std.math.add(u64, state.value.sequence, 1) catch return error.IntegrityHandoffTooLarge) or
                !std.mem.eql(u8, &state.value.digest, &page.previous_digest) or !std.mem.eql(u8, state.value.cursor, page.after) or
                std.mem.order(u8, page.next_cursor, page.after) == .lt or (!page.exhausted and std.mem.eql(u8, page.next_cursor, page.after))) return error.IntegrityHandoffSequenceChanged;
            var manifest = try std.json.parseFromSlice(Manifest, alloc, (try optional(txn, manifest_key)) orelse return error.IntegrityHandoffMissing, .{});
            defer manifest.deinit();
            var compiled = try catalog.decode(alloc, manifest.value.catalog_bytes);
            defer compiled.deinit();
            var bytes: usize = 0;
            var previous = page.after;
            for (page.records) |record| {
                bytes = std.math.add(usize, bytes, record.key.len + record.value.len) catch return error.IntegrityRecordTooLarge;
                if (bytes > max_bytes or std.mem.order(u8, record.key, previous) != .gt or std.mem.order(u8, record.key, page.next_cursor) == .gt) return error.InvalidIntegrityKey;
                const address = try integrity.validateTransferRecord(record.key, record.value);
                if (compiled.findGeneration(address.generation) == null) return error.IntegrityCatalogChanged;
                if (!contains(manifest.value, &address.routing)) return error.KeyOutOfRange;
                if (try optional(txn, record.key)) |existing| if (!std.mem.eql(u8, existing, record.value)) return error.IntegrityHandoffCollision;
                try txn.put(record.key, record.value);
                previous = record.key;
            }
            state.value.sequence = page.sequence;
            state.value.digest = page_digest;
            state.value.cursor = page.next_cursor;
            state.value.exhausted = page.exhausted;
            try saveProgress(alloc, txn, state.value);
        },
        .finish => |finish| {
            var state = try loadProgress(alloc, txn);
            defer state.deinit();
            if (!state.value.exhausted or state.value.sequence != finish.sequence or !std.mem.eql(u8, &state.value.digest, &finish.digest)) return error.IntegrityHandoffIncomplete;
            // Companion checks follow the complete stream: a draining claim
            // can refer to an action job sorted after all reference records.
            // Each retry verifies one bounded cursor page, never a full scan.
            if (state.value.ready) return;
            var cursor = try txn.openCursor();
            var cursor_closed = false;
            defer if (!cursor_closed) cursor.close();
            var item = try cursor.seekAtOrAfter(if (state.value.verification_cursor.len == 0) integrity.namespace else state.value.verification_cursor);
            if (item) |entry| if (std.mem.eql(u8, entry.key, state.value.verification_cursor)) {
                item = try cursor.next();
            };
            var inspected: usize = 0;
            var verified_bytes: usize = 0;
            var verified_parents: std.AutoHashMapUnmanaged([integrity.key_len]u8, void) = .empty;
            defer verified_parents.deinit(alloc);
            var next_buf: [integrity.key_len + 32]u8 = undefined;
            var next = state.value.verification_cursor;
            state.value.ready = true;
            while (item) |entry| : (item = try cursor.next()) {
                if (!std.mem.startsWith(u8, entry.key, integrity.namespace)) break;
                const size = std.math.add(usize, entry.key.len, entry.value.len) catch return error.IntegrityRecordTooLarge;
                if (inspected == max_records or size > max_bytes - verified_bytes) {
                    if (inspected == 0) return error.IntegrityRecordTooLarge;
                    state.value.ready = false;
                    break;
                }
                verified_bytes += size;
                const parsed_key = try integrity.parseKey(entry.key);
                const parent = try verified_parents.getOrPut(alloc, parsed_key.address.claimKey());
                if (parent.found_existing and parsed_key.kind == .reference) {
                    _ = try integrity.validateTransferRecord(entry.key, entry.value);
                } else try integrity.validateTransferredCompanions(txn, entry.key, entry.value);
                @memcpy(next_buf[0..entry.key.len], entry.key);
                next = next_buf[0..entry.key.len];
                inspected += 1;
            }
            state.value.verification_cursor = next;
            cursor.close();
            cursor_closed = true;
            try saveProgress(alloc, txn, state.value);
        },
    }
}

/// Ownership publication calls this only after its primary bootstrap-complete
/// check; progress by itself is not permission to serve the new row range.
pub fn rebaseCoverage(alloc: Allocator, txn: anytype) !void {
    var progress = try loadProgress(alloc, txn);
    defer progress.deinit();
    if (!progress.value.ready) return error.IntegrityHandoffIncomplete;
    var compiled = try catalog.decode(alloc, (try optional(txn, catalog.key)) orelse return error.IntegrityCatalogChanged);
    defer compiled.deinit();
    const coverage: activation.Progress = .{ .generation_set = activation.generationSet(compiled), .owner = try activation.ownership(txn), .schema_version = compiled.schema_version, .state = .enforced, .phase = if (@import("relational_integrity_activation_contract.zig").hasChecks(compiled)) .check else .foreign_key };
    const bytes = try coverage.encode(alloc);
    defer alloc.free(bytes);
    try txn.put(activation.key, bytes);
}

pub fn coverageForRange(alloc: Allocator, raw_catalog: []const u8, namespace: @import("doc_identity.zig").Namespace, range: docstore.ByteRange) ![]u8 {
    var compiled = try catalog.decode(alloc, raw_catalog);
    defer compiled.deinit();
    const raw_range = try @import("range_state.zig").encodeRangeAlloc(alloc, range);
    defer alloc.free(raw_range);
    var raw_namespace: [24]u8 = undefined;
    @import("doc_identity.zig").encodeNamespace(&raw_namespace, namespace);
    var state = std.crypto.hash.Blake3.init(.{});
    state.update("antfly constraint coverage owner v1");
    state.update(&raw_namespace);
    state.update(raw_range);
    var owner: integrity.Digest = undefined;
    state.final(&owner);
    return (activation.Progress{ .generation_set = activation.generationSet(compiled), .owner = owner, .schema_version = compiled.schema_version, .state = .enforced, .phase = if (@import("relational_integrity_activation_contract.zig").hasChecks(compiled)) .check else .foreign_key }).encode(alloc);
}

/// Receiver-side admission for the existing primary bootstrap. Raw metadata
/// pages are a different command; allowing this context never permits callers
/// to send reserved keys through document encoding.
pub fn admitSplitRequest(alloc: Allocator, txn: anytype, req: @import("types.zig").BatchRequest) !bool {
    const fence = (try topology.current(txn)) orelse return false;
    if (req.split_replication) |replication| {
        if (fence.role != .split_destination or fence.transition_id != replication.transition_id or fence.attempt != replication.attempt_epoch or
            fence.owner_group_id != replication.destination_group_id or fence.peer_group_id != replication.source_group_id or
            !fence.namespace.eql(replication.identity_namespace)) return error.IntegrityTopologyChanged;
        var manifest = try std.json.parseFromSlice(Manifest, alloc, (try optional(txn, manifest_key)) orelse return error.IntegrityHandoffMissing, .{});
        defer manifest.deinit();
        if (!manifest.value.destination.eql(fence) or replication.operation == .delta or replication.bootstrap_sequence == null or replication.bootstrap_sequence.? != manifest.value.primary_sequence) return error.IntegrityHandoffSequenceChanged;
        for (req.writes) |write| if (!contains(manifest.value, write.key)) return error.KeyOutOfRange;
        for (req.deletes) |key| if (!contains(manifest.value, key)) return error.KeyOutOfRange;
        if (req.split_checkpoint) |checkpoint| {
            if (!std.mem.eql(u8, checkpoint.range_start, manifest.value.lower) or !std.mem.eql(u8, checkpoint.range_end, manifest.value.upper)) return error.KeyOutOfRange;
            if (checkpoint.kind == .destination_complete) {
                var progress = try loadProgress(alloc, txn);
                defer progress.deinit();
                if (!progress.value.ready) return error.IntegrityHandoffIncomplete;
            }
        }
        return true;
    }
    if (req.split_checkpoint) |checkpoint| if (checkpoint.kind == .source_ack) {
        if (fence.role != .split_source or fence.transition_id != checkpoint.transition_id or fence.attempt != checkpoint.attempt_epoch or
            fence.owner_group_id != checkpoint.source_group_id or fence.peer_group_id != checkpoint.destination_group_id) return error.IntegrityTopologyChanged;
        return true;
    };
    return false;
}

/// Merge receivers retain a live base range. A frozen donor may change only
/// its transferred slice, under the exact durable handoff/copy identity.
pub fn admitMergeRequest(alloc: Allocator, txn: anytype, req: @import("types.zig").BatchRequest) !bool {
    const fence = (try topology.current(txn)) orelse return false;
    if (fence.role != .merge_destination) return error.IntegrityTopologyChanged;
    if (req.merge_checkpoint) |checkpoint| {
        if (fence.transition_id != checkpoint.transition_id or fence.owner_group_id != checkpoint.receiver_group_id or
            fence.peer_group_id != checkpoint.donor_group_id) return error.IntegrityTopologyChanged;
        if (checkpoint.kind == .accept) return true;
        if (checkpoint.kind == .rollback and try optional(txn, manifest_key) == null) return true;
    }
    var manifest = try std.json.parseFromSlice(Manifest, alloc, (try optional(txn, manifest_key)) orelse return error.IntegrityHandoffMissing, .{});
    defer manifest.deinit();
    const proof = manifest.value;
    if (!proof.destination.eql(fence)) return error.IntegrityTopologyChanged;
    if (req.merge_replication) |replication| {
        if (fence.transition_id != replication.transition_id or fence.owner_group_id != replication.receiver_group_id or
            fence.peer_group_id != replication.donor_group_id or !fence.namespace.eql(replication.identity_namespace) or
            @import("types.zig").MergeCopyAttempt.order(replication.copy_attempt, proof.merge_copy_attempt) == .lt) return error.IntegrityTopologyChanged;
        for (req.writes) |write| if (!contains(proof, write.key)) return error.KeyOutOfRange;
        for (req.deletes) |key| if (!contains(proof, key)) return error.KeyOutOfRange;
        for (req.merge_artifacts) |artifact| {
            const owner = (try @import("../internal_keys.zig").decodeDocumentComponentAlloc(alloc, artifact.key)) orelse return error.InvalidBatchRequest;
            defer alloc.free(owner);
            if (!contains(proof, owner)) return error.KeyOutOfRange;
        }
    }
    if (req.merge_checkpoint) |checkpoint| {
        if (@import("types.zig").MergeCopyAttempt.order(checkpoint.copy_attempt, proof.merge_copy_attempt) == .lt) return error.IntegrityHandoffSequenceChanged;
        const donor = docstore.ByteRange{ .start = proof.lower, .end = proof.upper };
        const base = docstore.ByteRange{ .start = checkpoint.receiver_base_start, .end = checkpoint.receiver_base_end };
        const merged = docstore.ByteRange{ .start = checkpoint.merged_start, .end = checkpoint.merged_end };
        const adjacent = (donor.end.len != 0 and std.mem.eql(u8, donor.end, base.start) and std.mem.eql(u8, merged.start, donor.start) and std.mem.eql(u8, merged.end, base.end)) or
            (base.end.len != 0 and std.mem.eql(u8, base.end, donor.start) and std.mem.eql(u8, merged.start, base.start) and std.mem.eql(u8, merged.end, donor.end));
        if (!adjacent) return error.KeyOutOfRange;
        if (checkpoint.kind == .bootstrap_complete or checkpoint.kind == .finalize) {
            var progress = try loadProgress(alloc, txn);
            defer progress.deinit();
            if (!progress.value.ready) return error.IntegrityHandoffIncomplete;
            if (checkpoint.bootstrap_applied_index < proof.primary_sequence) return error.IntegrityHandoffSequenceChanged;
        }
    }
    return true;
}

pub const PruneProgress = @import("relational_integrity_handoff_contract.zig").PruneProgress;

/// Post-cutover cleanup is an ordinary bounded Raft maintenance command. It
/// deletes only physically retained metadata outside the *current* logical
/// owner range; new writes in the live range remain admitted between pages.
pub fn prune(alloc: Allocator, txn: anytype, fence: topology.Fence, range: docstore.ByteRange) !void {
    if (try topology.current(txn)) |active| {
        // A retained merge receiver must remove imported donor claims before
        // rollback unfreezes its original range. Unlike a split destination,
        // this root cannot simply be discarded and provisioned afresh.
        if (!active.eql(fence) or fence.role != .merge_destination) return error.IntegrityTopologyBusy;
        var state = try @import("merge_state.zig").decodeAlloc(alloc, (try optional(txn, @import("merge_state.zig").key)) orelse return error.IntegrityHandoffIncomplete);
        defer state.deinit(alloc);
        if (state.phase != .rolled_back or state.transition_id != fence.transition_id or state.donor_group_id != fence.peer_group_id or
            !std.mem.eql(u8, range.start, state.receiver_base_range.start) or !std.mem.eql(u8, range.end, state.receiver_base_range.end)) return error.IntegrityHandoffIncomplete;
    } else {
        const receipt = (try optional(txn, topology.receipt_key)) orelse return error.IntegrityTopologyFenceMissing;
        if (!(try topology.Fence.decode(receipt)).eql(fence) or (fence.role != .split_source and fence.role != .merge_source)) return error.IntegrityTopologyChanged;
    }
    var parsed: ?std.json.Parsed(PruneProgress) = null;
    defer if (parsed) |*value| value.deinit();
    var progress: PruneProgress = .{ .fence = fence, .lower = range.start, .upper = range.end };
    if (try optional(txn, prune_key)) |bytes| {
        parsed = try std.json.parseFromSlice(PruneProgress, alloc, bytes, .{ .allocate = .alloc_always });
        if (parsed.?.value.fence.eql(fence)) {
            progress = parsed.?.value;
            if (!std.mem.eql(u8, progress.lower, range.start) or !std.mem.eql(u8, progress.upper, range.end)) return error.IntegrityTopologyChanged;
        }
    }
    if (progress.complete) return;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    var deletions: std.ArrayList([]const u8) = .empty;
    {
        var cursor = try txn.openCursor();
        defer cursor.close();
        var item = try cursor.seekAtOrAfter(if (progress.cursor.len == 0) integrity.namespace else progress.cursor);
        if (item) |entry| if (std.mem.eql(u8, entry.key, progress.cursor)) {
            item = try cursor.next();
        };
        var inspected: usize = 0;
        progress.complete = true;
        while (item) |entry| : (item = try cursor.next()) {
            if (!std.mem.startsWith(u8, entry.key, integrity.namespace)) break;
            if (inspected == max_records) {
                progress.complete = false;
                break;
            }
            const address = (try integrity.parseKey(entry.key)).address;
            const copy = try owned.dupe(u8, entry.key);
            if (!range.contains(&address.routing)) try deletions.append(owned, copy);
            progress.cursor = copy;
            inspected += 1;
        }
    }
    for (deletions.items) |key| try txn.delete(key);
    try txn.put(prune_key, try encode(owned, progress));
}
