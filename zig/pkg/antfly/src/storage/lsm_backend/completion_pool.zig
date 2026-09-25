// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Native replicated completion ownership. All calls run under backend writer
//! serialization. Installing backing is distinct from publishing readiness:
//! accepted guards must first be reconciled against the durable Raft frontier.
//! This module never treats an incoming speculative Append as truncation proof.
const std = @import("std");
const domains = @import("completion_allocator.zig");
const entry_codec = @import("completion_entry.zig");
const slot_codec = @import("completion_slot.zig");
const completion = @import("completion_runtime.zig");
const storage_io = @import("storage_io.zig");
const repository = @import("repository.zig");
const manifest_set = @import("manifest_set.zig");
const manifest = @import("../lsm/manifest.zig");
const table_file = @import("../lsm/table_file.zig");
const resources = @import("../resource_manager.zig");
const abi = @import("kernel_owner_abi").completion_pool;
const wal = @import("wal.zig");
const state = @import("state.zig");
const Allocator = std.mem.Allocator;
pub const maintenance = @import("completion_maintenance.zig");
const capacity = @import("completion_capacity.zig");
const generations_mod = @import("completion_generations.zig");
const control_begin = @import("completion_control_begin.zig");
const control_accepted = @import("completion_control_accepted.zig");
const control_transition = @import("completion_control_transition.zig");
const control_shape = @import("../completion_control_budget.zig");
const control_capacity = @import("completion_control_capacity.zig");
const control_guard = @import("completion_control_guard.zig");
const control_record = @import("completion_control_record.zig");
const control_resources = @import("completion_control_resources.zig");
comptime {
    if (completion.scratch_bytes != maintenance.compiler_workspace_bytes)
        @compileError("completion maintenance certificate must match installed compiler backing");
}

pub const max_slots = completion.max_slots;
pub const accepted_filenames = [_][]const u8{
    "completion-accepted-0.guard", "completion-accepted-1.guard",
    "completion-accepted-2.guard", "completion-accepted-3.guard",
};
pub const publication_per_cell = 8 * 1024 * 1024;
pub const control_bytes = 8 * 1024 * 1024;
pub const ControlCheckpointFault = enum { after_manifest, after_wal_reset };
pub var test_control_checkpoint_fault: ?ControlCheckpointFault = null;
pub var test_control_checkpoint_fault_hit: bool = false;
pub const accepted_header_bytes = 192;
pub const max_accepted_bytes = accepted_header_bytes + entry_codec.max_wire_bytes;

/// Immutable shape limits, not a ceiling on total database bytes. Streaming
/// maintenance may replace arbitrarily many records using the same buffers.
pub const Shape = struct {
    max_runs: usize = 68,
    max_metadata_bytes: usize = 1024 * 1024,
    max_block_bytes: usize = 1024 * 1024,
    max_record_bytes: usize = 256 * 1024,
};
pub const Config = struct {
    identity: abi.Identity,
    schema_catalog_digest: [32]u8,
    namespace: enum(u8) { root, docs } = .docs,
    shape: Shape = .{},
    /// Staging is an internal component path while replicated activation is
    /// disabled. It cannot make a control owner runnable after restart yet.
    control_owner_staging: bool = false,
};

pub fn encodeProgress(identity: abi.Identity, progress: completion.AcceptedIdentity) [112]u8 {
    var bytes: [112]u8 = undefined;
    std.mem.writeInt(u64, bytes[0..8], identity.group_id, .little);
    @memcpy(bytes[8..24], &identity.incarnation);
    @memcpy(bytes[24..56], &identity.policy_digest);
    std.mem.writeInt(u64, bytes[56..64], identity.generation, .little);
    @memcpy(bytes[64..112], &progress.encode());
    return bytes;
}

pub fn decodeProgress(identity: abi.Identity, bytes: []const u8) !completion.AcceptedIdentity {
    if (bytes.len != 112 or std.mem.readInt(u64, bytes[0..8], .little) != identity.group_id or
        !std.mem.eql(u8, bytes[8..24], &identity.incarnation) or !std.mem.eql(u8, bytes[24..56], &identity.policy_digest) or
        std.mem.readInt(u64, bytes[56..64], .little) != identity.generation) return error.InvalidCompletionSlot;
    const result: completion.AcceptedIdentity = .{
        .term = std.mem.readInt(u64, bytes[64..72], .little),
        .index = std.mem.readInt(u64, bytes[72..80], .little),
        .digest = bytes[80..112].*,
    };
    if (result.term == 0 or result.index == 0 or std.mem.allEqual(u8, &result.digest, 0)) return error.InvalidCompletionSlot;
    return result;
}

/// Storage-kernel-local callbacks; never a cross-runtime raw error vtable.
/// Provider takes DB apply serialization BEFORE backend.mu. Callbacks assume
/// both are held and cannot reacquire DB apply or discover owners after ACK.
pub const PublicationOwner = struct {
    context: *anyopaque,
    prepare: *const fn (*anyopaque, Allocator, *const entry_codec.OwnedEntry) anyerror!*anyopaque,
    applied: *const fn (*anyopaque, *anyopaque, u64, u64) void,
    /// Marks terminal only; DB.resolve may still use its backlog after return.
    native_terminal: *const fn (*anyopaque, *anyopaque) void,
    cancel: *const fn (*anyopaque, *anyopaque) void,
    /// Shutdown drops transient ownership without claiming durable cancellation.
    release_after_quiesce: *const fn (*anyopaque, *anyopaque) void,
};

pub const Accepted = struct {
    cell: u8,
    term: u64,
    index: u64,
    identity: abi.Identity,
    cohort: completion.guard.Info,
    envelope: []const u8,
};

/// Sidecar is fsynced before an accepted entry can be acknowledged. It contains
/// sufficient immutable ownership input to restore capacity before admitting
/// another append; its checksum does not authenticate a remote request.
pub fn encodeAccepted(alloc: Allocator, accepted: Accepted) ![]u8 {
    try validateIdentity(accepted.identity);
    if (accepted.cell >= accepted.identity.capacity or accepted.term == 0 or accepted.index == 0 or
        accepted.envelope.len > entry_codec.max_wire_bytes or accepted.cohort.index != accepted.cell or
        accepted.cohort.initial_runs > 64 or accepted.cohort.base_run_id == 0 or accepted.cohort.legacy)
        return error.InvalidCompletionSlot;
    const bytes = try alloc.alloc(u8, accepted_header_bytes + accepted.envelope.len);
    @memset(bytes[0..accepted_header_bytes], 0);
    @memcpy(bytes[0..8], "AFCACPT1");
    bytes[8] = accepted.cell;
    std.mem.writeInt(u32, bytes[12..16], @intCast(bytes.len), .little);
    std.mem.writeInt(u64, bytes[16..24], accepted.term, .little);
    std.mem.writeInt(u64, bytes[24..32], accepted.index, .little);
    std.mem.writeInt(u64, bytes[32..40], accepted.identity.group_id, .little);
    std.mem.writeInt(u64, bytes[40..48], accepted.identity.node_id, .little);
    @memcpy(bytes[48..64], &accepted.identity.incarnation);
    @memcpy(bytes[64..96], &accepted.identity.policy_digest);
    std.mem.writeInt(u64, bytes[96..104], accepted.identity.generation, .little);
    std.mem.writeInt(u32, bytes[104..108], accepted.identity.capacity, .little);
    std.mem.writeInt(u32, bytes[108..112], accepted.cohort.initial_runs, .little);
    std.mem.writeInt(u64, bytes[112..120], accepted.cohort.base_run_id, .little);
    @memcpy(bytes[120..136], &accepted.cohort.cohort_id);
    @memcpy(bytes[accepted_header_bytes..], accepted.envelope);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(bytes[0..160]);
    hash.update(bytes[accepted_header_bytes..]);
    hash.final(bytes[160..192]);
    return bytes;
}

pub fn decodeAccepted(bytes: []const u8) !Accepted {
    if (bytes.len < accepted_header_bytes or bytes.len > max_accepted_bytes or
        !std.mem.eql(u8, bytes[0..8], "AFCACPT1") or
        std.mem.readInt(u32, bytes[12..16], .little) != bytes.len or
        !std.mem.allEqual(u8, bytes[9..12], 0) or !std.mem.allEqual(u8, bytes[136..160], 0))
        return error.InvalidCompletionSlot;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(bytes[0..160]);
    hash.update(bytes[accepted_header_bytes..]);
    if (!std.mem.eql(u8, &hash.finalResult(), bytes[160..192])) return error.CompletionSlotChecksumMismatch;
    const result: Accepted = .{
        .cell = bytes[8],
        .term = std.mem.readInt(u64, bytes[16..24], .little),
        .index = std.mem.readInt(u64, bytes[24..32], .little),
        .identity = .{
            .group_id = std.mem.readInt(u64, bytes[32..40], .little),
            .node_id = std.mem.readInt(u64, bytes[40..48], .little),
            .incarnation = bytes[48..64].*,
            .policy_digest = bytes[64..96].*,
            .generation = std.mem.readInt(u64, bytes[96..104], .little),
            .capacity = std.mem.readInt(u32, bytes[104..108], .little),
        },
        .cohort = .{
            .index = bytes[8],
            .initial_runs = std.mem.readInt(u32, bytes[108..112], .little),
            .base_run_id = std.mem.readInt(u64, bytes[112..120], .little),
            .cohort_id = bytes[120..136].*,
        },
        .envelope = bytes[accepted_header_bytes..],
    };
    try validateIdentity(result.identity);
    if (result.cell >= result.identity.capacity or result.term == 0 or result.index == 0 or
        result.cohort.initial_runs > 64 or result.cohort.base_run_id == 0 or
        result.cohort.base_run_id > std.math.maxInt(u64) - max_slots) return error.InvalidCompletionSlot;
    return result;
}

fn validateIdentity(identity: abi.Identity) !void {
    if (identity.version != abi.pool_abi_version or identity.protocol != 1 or identity.profile != 1 or
        identity.capacity == 0 or identity.capacity > max_slots or identity.group_id == 0 or identity.node_id == 0 or
        identity.generation == 0 or std.mem.allEqual(u8, &identity.incarnation, 0)) return error.InvalidCompletionSlot;
}

pub fn hasAcceptedGuards(storage: storage_io.Storage, alloc: Allocator, root: []const u8) !bool {
    for (accepted_filenames) |filename| {
        const path = try std.fs.path.join(alloc, &.{ root, filename });
        defer alloc.free(path);
        if (storage.fileSize(path)) |_| return true else |err| if (err != error.FileNotFound) return err;
    }
    return false;
}

fn savedCohort(storage: storage_io.Storage, alloc: Allocator, root: []const u8, identity: abi.Identity) !?completion.guard.Info {
    var result: ?completion.guard.Info = null;
    for (accepted_filenames, 0..) |filename, i| {
        const path = try std.fs.path.join(alloc, &.{ root, filename });
        defer alloc.free(path);
        const size = storage.fileSize(path) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => return err,
        };
        if (size > max_accepted_bytes) return error.CompletionSlotTooLarge;
        const bytes = try alloc.alloc(u8, @intCast(size));
        defer alloc.free(bytes);
        try storage.readFileRangeInto(alloc, path, 0, bytes);
        const guard = try decodeAccepted(bytes);
        if (guard.cell != i or !std.meta.eql(guard.identity, identity)) return error.InvalidCompletionSlot;
        if (result) |existing| {
            if (guard.cohort.base_run_id != existing.base_run_id or guard.cohort.initial_runs != existing.initial_runs or
                !std.mem.eql(u8, &guard.cohort.cohort_id, &existing.cohort_id)) return error.InvalidCompletionSlot;
        } else result = guard.cohort;
    }
    return result;
}

/// One independently owned result; null means absence or a newer tombstone.
pub const Point = struct {
    found: bool = false,
    value: ?[]u8 = null,
    pub fn deinit(self: Point, alloc: Allocator) void {
        if (self.value) |value| alloc.free(value);
    }
};

/// Uses caller-owned scratch and the direct prepared IO scope. No backend
/// cache, ordinary allocator, whole-SST hydration, or nested FD admission.
pub fn readRunPoint(io: storage_io.Storage, alloc: Allocator, path: []const u8, shape: Shape, namespace: ?[]const u8, key: []const u8) !Point {
    const size = try io.fileSize(path);
    if (size < table_file.footer_len) return error.InvalidTableFile;
    var footer_bytes: [table_file.footer_len]u8 = undefined;
    try io.readFileRangeInto(alloc, path, size - footer_bytes.len, &footer_bytes);
    const footer = try table_file.decodeFooterBytes(&footer_bytes);
    if (footer.metadata_len > shape.max_metadata_bytes or
        footer.metadata_offset > size - footer_bytes.len or
        footer.metadata_len != size - footer_bytes.len - footer.metadata_offset)
        return error.UnsupportedCompletionProfile;
    const metadata = try alloc.alloc(u8, footer.metadata_len);
    defer alloc.free(metadata);
    try io.readFileRangeInto(alloc, path, footer.metadata_offset, metadata);
    var index = try table_file.decodeIndexFromFooterAlloc(alloc, footer, metadata);
    defer index.deinit(alloc);
    for (index.blocks) |block| if (block.len > shape.max_block_bytes or block.physicalLen() > shape.max_block_bytes)
        return error.UnsupportedCompletionProfile;
    const block_index = index.findBlockIndex(namespace, key) orelse return .{};
    const window = index.blockWindow(block_index);
    const physical = try alloc.alloc(u8, window.physicalLen());
    defer alloc.free(physical);
    try io.readFileRangeInto(alloc, path, index.entry_data_start + window.physicalRelativeOffset(), physical);
    const decoded = try table_file.decodeBlockPayloadAlloc(alloc, window.compression, physical, window.len, window.checksum);
    defer alloc.free(decoded);
    const found = try table_file.findExactEntryInBlock(&index, decoded, block_index, namespace, key) orelse return .{};
    if (found.entry.key.len +| found.entry.value.len > shape.max_record_bytes) return error.UnsupportedCompletionProfile;
    return .{ .found = true, .value = if (found.entry.tombstone) null else try alloc.dupe(u8, found.entry.value) };
}

/// These observations come only from a successfully persisted complete log
/// image, never volatile Raft state or a pre-persistence Ready preview.
pub const DurableCell = struct {
    identity: completion.AcceptedIdentity,
    prepared: bool,
};
pub const DurableControlOwner = struct {
    identity: completion.AcceptedIdentity,
    slot_index: usize,
};
pub const DurableObservation = struct {
    expected: completion.AcceptedIdentity,
    present: bool,
    observed_term: u64 = 0,
    observed_digest: [32]u8 = @splat(0),
    replaced_in_this_persist: bool = false,
};
pub const DurableLog = struct {
    mode: enum { startup_complete, persisted_replacement },
    compacted_index: u64,
    compacted_term: u64,
    last_index: u64,
    commit_index: u64,
    observations: []const DurableObservation,
};

/// Match the SST record framing used by the maintenance reader. Bindings
/// patch fixed eight-byte fields and never change these lengths.
fn validateRecordSize(namespace: ?[]const u8, key: []const u8, value_len: usize, limit: usize) !void {
    var size = std.math.add(usize, 13, if (namespace) |name| name.len else 0) catch return error.UnsupportedCompletionProfile;
    size = std.math.add(usize, size, key.len) catch return error.UnsupportedCompletionProfile;
    size = std.math.add(usize, size, value_len) catch return error.UnsupportedCompletionProfile;
    if (size > limit) return error.UnsupportedCompletionProfile;
}

fn validateEntryRecordSizes(entry: *const entry_codec.OwnedEntry, limit: usize) !void {
    const descriptor = entry.decoded_descriptor.descriptor;
    for ([_][]const slot_codec.Operation{ entry.entry.prepare_operations, descriptor.commit, descriptor.abort }) |operations| {
        for (operations) |op| try validateRecordSize(descriptor.namespace, op.key, op.value.len, limit);
    }
    // Include native-only records which are deliberately absent from the
    // leader's wire operations. Validate every possible cell before selecting
    // one, so a later cohort position cannot have a longer unchecked key.
    for (completion.storage_keys) |key| try validateRecordSize(descriptor.namespace, key, entry.entry.descriptor.len, limit);
    for (completion.applied_keys) |key| try validateRecordSize(descriptor.namespace, key, 16, limit);
    const receipt_key = entry_codec.receiptKey(descriptor.txn_id);
    try validateRecordSize(descriptor.namespace, &receipt_key, 48, limit);
    try validateRecordSize(descriptor.namespace, entry_codec.group_progress_key, 112, limit);
    try validateRecordSize(descriptor.namespace, &@import("../internal_keys.zig").raft_document_applied_entry_key, 16, limit);
}

fn entryCapacity(entry: *const entry_codec.OwnedEntry) !capacity.Cost {
    const descriptor = entry.decoded_descriptor.descriptor;
    const ns = if (descriptor.namespace) |name| name.len else 0;
    var result: capacity.Cost = .{};
    for ([_][]const slot_codec.Operation{ entry.entry.prepare_operations, descriptor.commit, descriptor.abort }) |operations|
        for (operations) |op| {
            result = try result.plus(try capacity.Cost.record(ns, op.key.len, op.value.len));
        };
    for (completion.storage_keys) |key| result = try result.plus(try capacity.Cost.record(ns, key.len, entry.entry.descriptor.len));
    for (completion.applied_keys) |key| result = try result.plus(try capacity.Cost.record(ns, key.len, 16));
    const receipt = entry_codec.receiptKey(descriptor.txn_id);
    result = try result.plus(try (try capacity.Cost.record(ns, receipt.len, 48)).repeated(2));
    result = try result.plus(try (try capacity.Cost.record(ns, entry_codec.group_progress_key.len, 112)).repeated(2));
    result = try result.plus(try (try capacity.Cost.record(ns, @import("../internal_keys.zig").raft_document_applied_entry_key.len, 16)).repeated(2));
    return result;
}

const replay_max_records = completion.recovery_wal_records + 2 * max_slots;
const replay_max_entries = completion.foreground_entries + max_slots * 2 * completion.records_per_phase;
const replay_max_bytes = completion.recovery_wal_bytes + max_slots * (completion.prepare_append_budget.bytes + completion.outcome_append_budget.bytes);
const ReplayWorkspace = struct { tree: usize, pending: usize, paths: usize, total: usize };

/// Replay starts with no readers or shared roots. Payload includes every WAL
/// version, and the fixed pending span can hold the entire retained WAL, so
/// arbitrary valid record/chunk boundaries never require buffer growth.
fn replayWorkspaceRequirement() !ReplayWorkspace {
    const footprint = domains.RecyclingScratch.allocationFootprint;
    // The existing applied-entry hook checks after insertion. Include the one
    // extra decoded entry which a corrupt over-limit input may insert first.
    const tree = try @import("state.zig").ActiveMemTable.uniqueReplayAllocationBound(replay_max_entries + 1, replay_max_bytes);
    const pending = try footprint(replay_max_bytes, 1);
    // Current index+segment, two retention scans (3+segments each), replay
    // (3+segments), and an optional truncated-tail path.
    const paths = try std.math.mul(usize, 12 + 3 * (replay_max_records + 1), try footprint(512 + 32, 1));
    var total = try std.math.add(usize, tree, try std.math.add(usize, pending, paths));
    total = std.mem.alignForward(usize, total, @alignOf(usize));
    if (total > completion.scratch_bytes) return error.UnsupportedCompletionProfile;
    return .{ .tree = tree, .pending = pending, .paths = paths, .total = total };
}

/// A manifest frame retains both physical key bounds. Reserve the configured
/// record ceiling for each bound; an accepted small key must not lend future
/// journal capacity to another member of its already-owned cohort.
fn journalHeadroom(max_record_bytes: usize, remaining_outputs: usize) !usize {
    const frame = try @import("../lsm/manifest.zig").singleRunJournalFrameSize(512 + 32, max_record_bytes, max_record_bytes);
    return std.math.mul(usize, remaining_outputs, frame) catch error.CompletionReservationBusy;
}

fn journalNeedsMaintenance(journal_size: u64, max_record_bytes: usize, remaining_outputs: usize) !bool {
    const limit = repository.maxManifestReadBytes();
    const reserve = try journalHeadroom(max_record_bytes, remaining_outputs);
    if (reserve > limit or journal_size > limit) return error.CompletionReservationBusy;
    return journal_size > limit - reserve;
}

fn metadataCapacity(outputs: u64, key_bytes: u64) !void {
    // Root <=512 bytes, slash + runs/ + maximum 20-digit u64 + .tbl.
    const path_bytes = 512 + 1 + 5 + 20 + 4;
    const count = std.math.cast(usize, outputs) orelse return error.UnsupportedCompletionProfile;
    const bound = std.math.cast(usize, key_bytes) orelse return error.UnsupportedCompletionProfile;
    const writer = try @import("run_store.zig").Store.freshAllocationBound(count, bound, path_bytes);
    const readers = try @import("run_directory.zig").Directory.freshAllocationBound(count, bound, path_bytes);
    const total = std.math.add(usize, writer, readers) catch return error.UnsupportedCompletionProfile;
    if (total > generations_mod.metadata_bytes) return error.UnsupportedCompletionProfile;
}

fn nativePublicationCapacity(entry: *const entry_codec.OwnedEntry, record_limit: usize) !usize {
    const footprint = domains.RecyclingScratch.allocationFootprint;
    const descriptor = entry.decoded_descriptor.descriptor;
    var bindings: usize = 0;
    for ([_][]const slot_codec.Operation{ descriptor.commit, descriptor.abort }) |ops| {
        for (ops) |op| bindings += op.bindings.len;
    }
    const decoded_bytes = entry.entry.descriptor.len + (descriptor.commit.len + descriptor.abort.len) * @sizeOf(slot_codec.Operation) + bindings * @sizeOf(slot_codec.Binding);
    var bytes = try footprint(decoded_bytes, @alignOf(slot_codec.Operation));
    bytes = try std.math.add(usize, bytes, try footprint(entry.entry.descriptor.len, 1));
    bytes = try std.math.add(usize, bytes, try footprint(entry.entry.descriptor.len + completion.guard.header_bytes, 1));
    // Guard, journal and all four reserved run paths copied by createFromPool.
    bytes = try std.math.add(usize, bytes, try std.math.mul(usize, 2 + max_slots, try footprint(512 + 64, 1)));
    if (entry.entry.kind == .prepare) {
        const ns = if (descriptor.namespace) |name| name.len else 0;
        var records: usize = entry.entry.descriptor.len + 48 + 112 + 16;
        for (entry.entry.prepare_operations) |op| records = try std.math.add(usize, records, ns + op.key.len + op.value.len);
        // Four private records: their keys/ns are bounded independently of
        // incoming operations. Using the record cap also covers their framing.
        records = try std.math.add(usize, records, 4 * @as(usize, 256));
        bytes = try std.math.add(usize, bytes, try @import("state.zig").ActiveMemTable.publicationAllocationBound(completion.foreground_entries + max_slots * (256 + 8), entry.entry.prepare_operations.len + 4, records));
    }
    // One protected SST publication can include sibling foreground keys, so
    // reserve the configured maximum bound rather than this entry's keys.
    bytes = try std.math.add(usize, bytes, try @import("run_store.zig").Store.singleInsertAllocationBound(68, record_limit, 512 + 64));
    bytes = try std.math.add(usize, bytes, try @import("run_directory.zig").Directory.singleInsertAllocationBound(68, record_limit, 512 + 64));
    return bytes;
}

pub fn Pool(comptime Backend: type) type {
    return struct {
        const Self = @This();
        const Slot = completion.Slot(Backend);
        pub const Phase = enum { free, accepted, prepared, spent };
        pub const Cell = struct {
            slot: *Slot,
            credit_pin: resources.ObserverMetadataPin,
            publication: ?*domains.PublicationReservation,
            phase: Phase = .free,
            entry: ?entry_codec.OwnedEntry = null,
            accepted_wire: ?[]u8 = null,
            term: u64 = 0,
            index: u64 = 0,
            baseline: completion.PooledBaseline = .{},
            publication_token: ?*anyopaque = null,
            publication_notified: bool = false,
            restored_prepared: bool = false,
            resolution: ?struct { identity: completion.AcceptedIdentity, commit: bool } = null,
        };
        const ControlOwner = struct {
            held: *control_resources.Resources,
            output_pin: Backend.CompletionRunPathPin,
            declaration: control_begin.Declaration,
            begin: completion.AcceptedIdentity,
            pending: ?completion.AcceptedIdentity = null,
            transition: ?control_transition.Transition = null,
        };
        const ControlTarget = struct { slot_index: usize, transition: control_transition.Transition };

        fn controlOwnerRecord(self: *const Self, index: usize) control_record.Record {
            const active = self.control_owners[index].?;
            return .{
                .authority = .{
                    .group_id = self.config.identity.group_id,
                    .incarnation = self.config.identity.incarnation,
                    .policy_digest = self.config.identity.policy_digest,
                    .schema_catalog_digest = self.config.schema_catalog_digest,
                    .generation = self.config.identity.generation,
                },
                .txn_id = active.declaration.txn_id,
                .begin = .{ .term = active.begin.term, .index = active.begin.index, .digest = active.begin.digest },
                .participants = active.declaration.participants,
                .slot_index = @intCast(index),
                .output_run_id = self.control_output_ids[index],
            };
        }

        fn priorControlProgress(self: *Self, backend: *Backend, alloc: Allocator, namespace: ?[]const u8, owner_index: usize, participants: []const u8, old_resolved: ?[]const u8) !control_record.Progress {
            const owner_record = self.controlOwnerRecord(owner_index);
            var prior_point = try self.point(backend, alloc, namespace, &control_record.progressKey(owner_record.txn_id));
            defer prior_point.deinit(alloc);
            const prior = try control_record.Progress.decode(prior_point.value orelse return error.UnsupportedCompletionProfile);
            try prior.verifyOwner(owner_record);
            if (prior.phase == .begin or prior.decision == .none or prior.acknowledged >= owner_record.participants.count or
                (prior.acknowledged != 0 and std.mem.allEqual(u8, &prior.ack_bitmap, 0)))
                return error.UnsupportedCompletionProfile;
            if (prior.acknowledged == 0) {
                if (old_resolved != null or !std.mem.allEqual(u8, &prior.resolved_digest, 0)) return error.UnsupportedCompletionProfile;
            } else {
                if (old_resolved == null or !std.mem.eql(u8, &prior.resolved_digest, &control_transition.resolvedDigest(old_resolved.?)))
                    return error.UnsupportedCompletionProfile;
                const expected = try control_transition.resolvedBitmap(participants, old_resolved);
                if (!std.mem.eql(u8, &expected, &prior.ack_bitmap)) return error.UnsupportedCompletionProfile;
            }
            return prior;
        }
        config: Config,
        /// Local component-test opt-in only; no installation/config ABI can
        /// activate the incomplete decision/ACK lifecycle.
        control_transition_staging: bool = false,
        control_owners: [control_record.max_owners]?ControlOwner = @splat(null),
        control_output_ids: [control_record.max_owners]u64 = @splat(0),
        control: *domains.RecyclingScratch,
        maintenance_active: bool = false,
        maintenance_pending: bool = false,
        capacity_cost: capacity.Cost = .{},
        capacity_growth: capacity.Cost = .{},
        capacity_baseline_frontier: u64 = 0,
        capacity_certified: bool = false,
        capacity_max_credit_bytes: u64 = 0,
        legacy_obsolete_imported: bool = false,
        retired: [2]@import("completion_maintenance_cycle.zig").Retired = .{ .{}, .{} },
        generations: generations_mod.Generations,
        scratch: *domains.RecyclingScratch,
        compiler: domains.CompilerWorkspace,
        io: *storage_io.NativeCompletionIo,
        memory_pin: resources.ObserverMetadataPin,
        wal_pin: resources.ObserverMetadataPin,
        cells: [max_slots]Cell = undefined,
        cell_count: usize = 0,
        cohort: completion.guard.Info,
        guard_paths: [max_slots][]u8,
        accepted_paths: [max_slots][]u8,
        control_accepted_paths: [control_record.max_owners][]u8,
        run_paths: [max_slots]?[]const u8,
        run_path_pins: [max_slots]?Backend.CompletionRunPathPin = @splat(null),
        journal_path: []u8,
        restored: bool = false,
        startup_reconciliation_pending: bool = false,
        ready: bool = false,
        failed: bool = false,
        progress: ?completion.AcceptedIdentity = null,
        publication_owner: ?PublicationOwner = null,
        wal_bytes_start: u64,
        wal_entries_start: u64,
        wal_records_start: u64,
        replayed_wal_bytes: u64 = 0,
        replayed_wal_entries: u64 = 0,
        replayed_wal_records: u64 = 0,

        /// Caller established a durable baseline before installing the pool.
        /// Creation is fallible ordinary startup work and grants no Raft proof.
        pub fn create(backend: *Backend, config: Config) !*Self {
            try validateIdentity(config.identity);
            _ = try replayWorkspaceRequirement();
            const native = backend.storage_owner orelse return error.UnsupportedCompletionBackend;
            const root = backend.root_dir orelse return error.UnsupportedCompletionBackend;
            if (root.len > 512) return error.UnsupportedCompletionProfile;
            const manager = backend.options.resource_manager orelse return error.CompletionResourceManagerRequired;
            if (backend.options.backend.read_only or backend.options.backend.durability != .full or
                !backend.options.wal_enabled or !backend.options.wal_sync_on_commit or
                backend.storage.?.ptr != native.storage().ptr or backend.storage.?.vtable != native.storage().vtable)
                return error.UnsupportedCompletionBackend;
            if (backend.manifest_recovery_required or backend.durable_completion != null or backend.bulkIngestActive() or
                backend.activeImmutableMemtableCount() != 0 or backend.mutable.entryCount() != 0 or
                backend.runs.count() > 68 or config.shape.max_runs > 68 or config.shape.max_runs < 64 or
                backend.manifest_journal.sequence == null or backend.manifest_journal.active_segment == 0)
                return error.CompletionReservationBusy;
            const saved = try savedCohort(backend.storage.?, backend.allocator, root, config.identity);
            const cohort = saved orelse completion.guard.Info{ .base_run_id = backend.next_run_id, .initial_runs = @intCast(@min(backend.runs.count(), 64)), .cohort_id = config.identity.incarnation };
            if (cohort.initial_runs > 64 or backend.runs.count() > cohort.initial_runs + max_slots or
                (saved != null and backend.runs.count() < cohort.initial_runs)) return error.CompletionReservationBusy;
            const next_id = std.math.add(u64, cohort.base_run_id, max_slots) catch return error.CompletionReservationBusy;
            var control_output_ids: [control_record.max_owners]u64 = @splat(0);
            const next_after_control = if (config.control_owner_staging) blk: {
                for (&control_output_ids, 0..) |*id, i| id.* = std.math.add(u64, next_id, @intCast(i)) catch return error.CompletionReservationBusy;
                break :blk std.math.add(u64, next_id, control_record.max_owners) catch return error.CompletionReservationBusy;
            } else next_id;
            const control = try domains.RecyclingScratch.create(backend.allocator, manager, control_bytes);
            errdefer control.retire();
            const alloc = control.allocator();
            const self = try alloc.create(Self);
            errdefer alloc.destroy(self);
            var generations = try generations_mod.Generations.create(backend.allocator, manager, config.identity.capacity, publication_per_cell);
            errdefer generations.retire();
            const scratch = try domains.RecyclingScratch.create(backend.allocator, manager, completion.scratch_bytes);
            errdefer scratch.destroy() catch unreachable;
            var compiler = try domains.CompilerWorkspace.init(backend.allocator, manager, completion.scratch_bytes);
            errdefer compiler.deinit() catch unreachable;
            var memory_pin = try manager.pinObserverMetadata(.lsm_in_memory_state, &backend.tracked_in_memory_state_bytes);
            errdefer memory_pin.release() catch unreachable;
            var wal_pin = try manager.pinObserverMetadata(.lsm_wal_retention, &backend.tracked_wal_retention_bytes);
            errdefer wal_pin.release() catch unreachable;
            var specs: [storage_io.NativeCompletionIo.max_prepared_files]storage_io.NativeCompletionIo.FileSpec = undefined;
            var spec_count: usize = 0;
            var guard_paths: [max_slots][]u8 = undefined;
            var accepted_paths: [max_slots][]u8 = undefined;
            var control_accepted_paths: [control_record.max_owners][]u8 = undefined;
            var control_path_count: usize = 0;
            errdefer for (control_accepted_paths[0..control_path_count]) |path| alloc.free(path);
            for (&control_accepted_paths, 0..) |*path, i| {
                path.* = try std.fs.path.join(alloc, &.{ root, control_accepted.filenames[i] });
                control_path_count += 1;
                specs[spec_count] = .{ .path = path.*, .max_bytes = control_accepted.max_bytes, .allow_delete = true };
                spec_count += 1;
            }
            var run_paths: [max_slots]?[]const u8 = @splat(null);
            var path_count: usize = 0;
            errdefer for (0..path_count) |i| {
                alloc.free(guard_paths[i]);
                alloc.free(accepted_paths[i]);
                alloc.free(run_paths[i].?);
            };
            for (0..max_slots) |i| {
                const gp = try std.fs.path.join(alloc, &.{ root, completion.guard_filenames[i] });
                errdefer alloc.free(gp);
                const ap = try std.fs.path.join(alloc, &.{ root, accepted_filenames[i] });
                errdefer alloc.free(ap);
                const rp = try repository.runPath(alloc, root, cohort.base_run_id + i);
                guard_paths[i] = gp;
                accepted_paths[i] = ap;
                run_paths[i] = rp;
                path_count += 1;
                specs[spec_count] = .{ .path = gp, .max_bytes = completion.limits.max_encoded_bytes + completion.guard.header_bytes, .allow_delete = true };
                spec_count += 1;
                specs[spec_count] = .{ .path = ap, .max_bytes = max_accepted_bytes, .allow_delete = true };
                spec_count += 1;
                specs[spec_count] = .{ .path = rp, .max_bytes = completion.limits.flush_bytes, .allow_delete = true };
                spec_count += 1;
            }
            var run_path_pins: [max_slots]?Backend.CompletionRunPathPin = @splat(null);
            errdefer for (&run_path_pins) |*pin| if (pin.*) |*held| held.release();
            for (0..max_slots) |i| run_path_pins[i] = try Backend.pinCompletionRunPath(alloc, run_paths[i].?);
            const journal_path = try manifest_set.pathAlloc(alloc, root, backend.manifest_journal.active_segment, .journal);
            errdefer alloc.free(journal_path);
            specs[spec_count] = .{ .path = journal_path, .max_bytes = repository.maxManifestReadBytes(), .allow_append = true };
            spec_count += 1;
            const journal_size = try backend.storage.?.fileSize(journal_path);
            // Persisted output runs have already consumed their frame; only
            // the remaining cohort drains need headroom on a guarded reopen.
            const manifested_outputs = if (saved != null) backend.runs.count() - cohort.initial_runs else 0;
            const journal_maintenance = try journalNeedsMaintenance(journal_size, config.shape.max_record_bytes, max_slots - manifested_outputs);
            if (saved != null and journal_maintenance) return error.CompletionRecoveryCapacityRequired;
            // An idle installation can rotate a full journal with its prepaid
            // maintenance scope before issuing a fresh readiness proof.
            var wal_paths: [3][]u8 = undefined;
            var wal_count: usize = 0;
            defer for (wal_paths[0..wal_count]) |path| alloc.free(path);
            for ([_][]const u8{ "wal.log", "wal/replay.index", "wal/replay.segments" }, 0..) |name, i| {
                wal_paths[i] = try std.fs.path.join(alloc, &.{ root, name });
                wal_count += 1;
                specs[spec_count] = .{ .path = wal_paths[i], .max_bytes = completion.limits.wal_bytes, .allow_delete = i == 0 };
                spec_count += 1;
            }
            for (0..backend.runs.count()) |i| {
                const path = backend.runs.at(i).path orelse return error.UnsupportedCompletionProfile;
                var output = false;
                for (run_paths) |planned| if (planned) |p| if (std.mem.eql(u8, p, path)) {
                    output = true;
                    break;
                };
                if (output) continue;
                specs[spec_count] = .{ .path = path, .max_bytes = repository.maxRunFileReadBytes() };
                spec_count += 1;
            }
            const io = try storage_io.NativeCompletionIo.createWithFilesAndHeadroom(alloc, native, root, specs[0..spec_count], 2);
            errdefer io.deinit() catch unreachable;
            io.allow_sequential_input = true;
            io.allow_wal_reset = true;
            self.* = .{
                .config = config,
                .control_output_ids = control_output_ids,
                .maintenance_pending = journal_maintenance,
                .control = control,
                .generations = generations,
                .scratch = scratch,
                .compiler = compiler,
                .io = io,
                .memory_pin = memory_pin,
                .wal_pin = wal_pin,
                .guard_paths = guard_paths,
                .accepted_paths = accepted_paths,
                .control_accepted_paths = control_accepted_paths,
                .run_paths = run_paths,
                .run_path_pins = run_path_pins,
                .journal_path = journal_path,
                .wal_bytes_start = backend.write_stats.wal_append_bytes,
                .wal_entries_start = backend.write_stats.wal_append_entries,
                .wal_records_start = backend.write_stats.wal_append_records,
                .cohort = cohort,
            };
            errdefer self.releaseCells();
            var initial_reservations = try self.generations.reserveCells();
            defer initial_reservations.deinit();
            for (0..config.identity.capacity) |i| {
                const cell_slot = try alloc.create(Slot);
                errdefer alloc.destroy(cell_slot);
                cell_slot.wal_credit = 0;
                var credit_pin = try manager.pinObserverMetadata(.lsm_wal_retention, &cell_slot.wal_credit);
                errdefer credit_pin.release() catch unreachable;
                try manager.adjustUsage(.lsm_wal_retention, &cell_slot.wal_credit, completion.limits.wal_bytes);
                errdefer manager.observeUsage(.lsm_wal_retention, &cell_slot.wal_credit, 0);
                const reservation = initial_reservations.items[i].?;
                self.cells[i] = .{ .slot = cell_slot, .credit_pin = credit_pin, .publication = reservation };
                initial_reservations.items[i] = null;
                self.cell_count += 1;
            }
            backend.next_run_id = @max(backend.next_run_id, next_after_control);
            if (config.control_owner_staging) try self.bindControlOutputPaths(backend);
            return self;
        }

        fn releaseCells(self: *Self) void {
            for (self.cells[0..self.cell_count]) |*cell| {
                std.debug.assert(cell.phase != .prepared);
                if (cell.publication_token) |token| self.publication_owner.?.release_after_quiesce(self.publication_owner.?.context, token);
                if (cell.entry) |*entry| entry.deinit();
                if (cell.accepted_wire) |wire| cell.publication.?.allocator().free(wire);
                if (cell.publication) |publication| publication.finish();
                self.wal_pin.manager.observeUsage(.lsm_wal_retention, &cell.slot.wal_credit, 0);
                cell.credit_pin.release() catch unreachable;
                self.control.allocator().destroy(cell.slot);
            }
            self.cell_count = 0;
        }

        /// Internal opt-in for a freshly installed pool. No DATA service path
        /// calls this while replicated activation remains disabled.
        fn bindControlOutputPaths(self: *Self, backend: *Backend) !void {
            const alloc = self.control.allocator();
            var specs: [storage_io.NativeCompletionIo.max_prepared_files]storage_io.NativeCompletionIo.FileSpec = undefined;
            var count: usize = 0;
            for (self.io.files) |file| {
                specs[count] = .{ .path = file.final, .max_bytes = file.max_bytes, .allow_append = file.allow_append, .allow_delete = file.allow_delete };
                count += 1;
            }
            var paths: [control_record.max_owners * 2][]u8 = undefined;
            var path_count: usize = 0;
            defer for (paths[0..path_count]) |path| alloc.free(path);
            for (0..control_record.max_owners) |i| {
                if (count + 2 > specs.len) return error.CompletionFileCapacityExceeded;
                paths[path_count] = try repository.runPath(alloc, backend.root_dir.?, self.control_output_ids[i]);
                specs[count] = .{ .path = paths[path_count], .max_bytes = completion.limits.flush_bytes, .allow_delete = true };
                count += 1;
                path_count += 1;
                paths[path_count] = try std.fs.path.join(alloc, &.{ backend.root_dir.?, control_guard.filenames[i] });
                specs[count] = .{ .path = paths[path_count], .max_bytes = control_guard.max_bytes, .allow_delete = true };
                count += 1;
                path_count += 1;
            }
            try self.io.replacePreparedFiles(alloc, specs[0..count]);
        }

        pub fn enableControlOwnerStaging(self: *Self, backend: *Backend) !void {
            if (self.config.control_owner_staging) return;
            if (!self.ready or self.failed or !self.restored or self.hasAcceptedDebt()) return error.CompletionReservationBusy;
            for (self.control_owners) |owner| if (owner != null) return error.CompletionReservationBusy;
            for (self.cells[0..self.cell_count]) |cell| if (cell.phase != .free) return error.CompletionReservationBusy;
            if (try control_guard.hasAny(backend.storage.?, backend.allocator, backend.root_dir.?)) return error.CompletionRecoveryCapacityRequired;
            const first = backend.next_run_id;
            const next = std.math.add(u64, first, control_record.max_owners) catch return error.CompletionReservationBusy;
            for (&self.control_output_ids, 0..) |*id, i| id.* = first + @as(u64, @intCast(i));
            try self.bindControlOutputPaths(backend);
            backend.next_run_id = next;
            self.config.control_owner_staging = true;
        }

        pub fn enableControlTransitionStaging(self: *Self) !void {
            if (!self.config.control_owner_staging or !self.ready or self.failed or !self.restored)
                return error.CompletionAdmissionUnavailable;
            for (self.cells[0..self.cell_count]) |cell| if (cell.phase == .accepted or cell.phase == .prepared)
                return error.CompletionReservationBusy;
            // v3's exact bitmap is fixed at 96 participants. Larger legal
            // BEGINs stay fenced until a separately certified bitmap format
            // and capacity profile exist.
            for (self.control_owners) |owner| if (owner) |active| {
                if (active.declaration.participants.count > control_record.progress_bitmap_bits)
                    return error.UnsupportedCompletionProfile;
            };
            self.control_transition_staging = true;
        }

        /// Native owner calls this only after attached runtime leases are gone
        /// and pooled Slots have returned their cell ownership. Published tree
        /// allocations retain the publication domain through old reader release.
        pub fn destroy(self: *Self) void {
            self.releaseCells();
            for (&self.control_owners) |*owner| if (owner.*) |*active| {
                active.output_pin.release();
                active.held.destroy();
                owner.* = null;
            };
            self.compiler.deinit() catch unreachable;
            self.scratch.retire();
            self.io.deinit() catch unreachable;
            self.memory_pin.release() catch unreachable;
            self.wal_pin.release() catch unreachable;
            for (&self.run_path_pins) |*pin| if (pin.*) |*held| held.release();
            for (0..max_slots) |i| {
                self.control.allocator().free(self.guard_paths[i]);
                self.control.allocator().free(self.accepted_paths[i]);
                self.control.allocator().free(self.run_paths[i].?);
            }
            for (self.control_accepted_paths) |path| self.control.allocator().free(path);
            self.control.allocator().free(self.journal_path);
            for (&self.retired) |*generation| generation.deinit(self.control.allocator());
            self.generations.retire();
            const control = self.control;
            control.allocator().destroy(self);
            control.retire();
        }

        fn validateEntry(self: *Self, entry: *const entry_codec.OwnedEntry) !void {
            const e = entry.entry;
            const identity = self.config.identity;
            if (e.group_id != identity.group_id or !std.mem.eql(u8, &e.group_incarnation, &identity.incarnation) or
                !std.mem.eql(u8, &e.policy_digest, &identity.policy_digest) or
                !std.mem.eql(u8, &e.schema_catalog_digest, &self.config.schema_catalog_digest) or
                !std.meta.eql(entry.decoded_descriptor.descriptor.limits, completion.limits)) return error.UnsupportedCompletionProfile;
            const namespace = entry.decoded_descriptor.descriptor.namespace;
            if (self.config.namespace == .root) {
                if (namespace != null) return error.UnsupportedCompletionProfile;
            } else if (namespace == null or !std.mem.eql(u8, namespace.?, "docs")) return error.UnsupportedCompletionProfile;
            try validateEntryRecordSizes(entry, self.config.shape.max_record_bytes);
        }

        fn point(self: *Self, backend: *Backend, alloc: Allocator, namespace: ?[]const u8, key: []const u8) !Point {
            if (backend.activeImmutableMemtableCount() != 0) return error.CompletionReservationBusy;
            if (backend.mutable.findIndex(.{ .name = namespace }, key)) |i| {
                const value = backend.mutable.entryAt(i);
                return .{ .found = true, .value = if (value.tombstone) null else try alloc.dupe(u8, value.value) };
            }
            for (0..backend.runs.count()) |i| {
                const run = backend.runs.at(i);
                const result = try readRunPoint(self.io.storage(), alloc, run.path orelse return error.UnsupportedCompletionProfile, self.config.shape, namespace, key);
                if (result.found) return result;
            }
            return .{};
        }

        /// A new transaction record is a future decision/ACK obligation. Even
        /// while full control capacity is disabled, its accepted physical
        /// mutation must be the exact fresh BEGIN understood by the native
        /// owner codec. Existing records may be rewritten by later controls
        /// only when no staged native owner holds their future control debt.
        fn validateNewTransactionRecord(self: *Self, backend: *Backend, alloc: Allocator, entry: *const entry_codec.OwnedEntry) !?control_begin.Declaration {
            if (entry.entry.kind != .mutation) return null;
            const namespace = entry.decoded_descriptor.descriptor.namespace;
            for (entry.entry.prepare_operations) |op| {
                if (op.kind != .put or !std.mem.startsWith(u8, op.key, control_shape.records_prefix)) continue;
                const existing = try self.point(backend, alloc, namespace, op.key);
                defer existing.deinit(alloc);
                if (existing.value == null) {
                    return try control_begin.inspect(entry.entry.prepare_operations);
                }
            }
            return null;
        }

        /// The generic document cell has no authority to spend a retained
        /// BEGIN owner's decision/ACK budget. Until control transitions and
        /// replay are wired to that owner, reject every exact metadata key for
        /// its transaction before allocating or publishing a document cell.
        /// Inspect prepared outcome templates too, so they cannot later write
        /// one of the retained owner's metadata keys through a different cell.
        fn classifyOwnedControlTransition(self: *Self, backend: *Backend, alloc: Allocator, entry: *const entry_codec.OwnedEntry) !?ControlTarget {
            const descriptor = entry.decoded_descriptor.descriptor;
            for ([_][]const slot_codec.Operation{ entry.entry.prepare_operations, descriptor.commit, descriptor.abort }) |operations| {
                for (operations) |op| {
                    // Native owner/progress rows are generated only by the
                    // owner-backed apply path, never by a replicated payload.
                    if (std.mem.startsWith(u8, op.key, control_record.owner_prefix) or
                        std.mem.startsWith(u8, op.key, control_record.receipt_prefix) or
                        std.mem.startsWith(u8, op.key, control_record.progress_prefix))
                        return error.CompletionAdmissionUnavailable;
                    if (!self.config.control_owner_staging) continue;
                    inline for (.{ control_shape.records_prefix, control_shape.participants_prefix, control_shape.resolved_participants_prefix, control_shape.completion_prefix, control_shape.intent_admission_prefix, control_shape.intent_keys_prefix, control_shape.schema_leases_prefix, "\x00\x00__txn_read_admission__:" }) |prefix| {
                        if (op.key.len == prefix.len + 16 and std.mem.startsWith(u8, op.key, prefix)) {
                            const id = op.key[prefix.len..][0..16];
                            for (self.control_owners, 0..) |owner, owner_index| if (owner) |active| {
                                if (!std.mem.eql(u8, id, &active.declaration.txn_id)) continue;
                                // The exact physical classifier is exercised
                                // at the real preaccept boundary, but it does
                                // not license generic-cell application.
                                if (entry.entry.kind != .mutation or descriptor.commit.len != 0 or descriptor.abort.len != 0)
                                    return error.CompletionAdmissionUnavailable;
                                const record_key = try std.mem.concat(alloc, u8, &.{ control_shape.records_prefix, id });
                                defer alloc.free(record_key);
                                const participants_key = try std.mem.concat(alloc, u8, &.{ control_shape.participants_prefix, id });
                                defer alloc.free(participants_key);
                                const resolved_key = try std.mem.concat(alloc, u8, &.{ control_shape.resolved_participants_prefix, id });
                                defer alloc.free(resolved_key);
                                var before = try self.point(backend, alloc, descriptor.namespace, record_key);
                                defer before.deinit(alloc);
                                var participants = try self.point(backend, alloc, descriptor.namespace, participants_key);
                                defer participants.deinit(alloc);
                                var resolved = try self.point(backend, alloc, descriptor.namespace, resolved_key);
                                defer resolved.deinit(alloc);
                                // The v3 ordinal map is only meaningful for
                                // a unique immutable participant list. Check
                                // it before even the first decision sidecar.
                                if (self.control_transition_staging)
                                    _ = try control_transition.resolvedBitmap(participants.value orelse return error.UnsupportedCompletionProfile, resolved.value);
                                const transition = try control_transition.inspect(entry.entry.prepare_operations, active.declaration, before.value orelse return error.UnsupportedCompletionProfile, participants.value orelse return error.UnsupportedCompletionProfile, resolved.value);
                                switch (transition) {
                                    .decision => {
                                        var progress = try self.point(backend, alloc, descriptor.namespace, &control_record.progressKey(active.declaration.txn_id));
                                        defer progress.deinit(alloc);
                                        if (progress.value != null) return error.UnsupportedCompletionProfile;
                                    },
                                    .acknowledgement => |ack| {
                                        if (ack.participant_index >= control_record.progress_bitmap_bits) return error.UnsupportedCompletionProfile;
                                        const prior = try self.priorControlProgress(backend, alloc, descriptor.namespace, owner_index, participants.value.?, resolved.value);
                                        if (prior.acknowledged + 1 != ack.count or
                                            prior.ack_bitmap[ack.participant_index / 8] & (@as(u8, 1) << @intCast(ack.participant_index % 8)) != 0)
                                            return error.UnsupportedCompletionProfile;
                                    },
                                }
                                return .{ .slot_index = owner_index, .transition = transition };
                            };
                        }
                    }
                }
            }
            return null;
        }

        fn acceptOwnedControlTransition(self: *Self, backend: *Backend, alloc: Allocator, target: ControlTarget, identity: completion.AcceptedIdentity, entry: *const entry_codec.OwnedEntry, envelope: []const u8) !usize {
            if (self.control_owners[target.slot_index] == null) return error.InvalidCompletionSlot;
            const active = &self.control_owners[target.slot_index].?;
            if (active.pending != null or active.held.accepted_len != 0) return error.CompletionReservationBusy;
            for (self.control_owners, 0..) |owner, i| if (i != target.slot_index and owner != null and owner.?.pending != null)
                return error.CompletionReservationBusy;
            for (self.cells[0..self.cell_count]) |cell| {
                if (cell.phase == .accepted or (cell.phase == .prepared and !cell.slot.retired)) return error.CompletionReservationBusy;
            }
            if (!self.control_transition_staging) return error.CompletionAdmissionUnavailable;
            switch (target.transition) {
                .decision => {},
                .acknowledgement => |ack| {
                    if (active.declaration.participants.count > control_record.progress_bitmap_bits or
                        ack.participant_index >= active.declaration.participants.count)
                        return error.UnsupportedCompletionProfile;
                    if (ack.count == active.declaration.participants.count) {
                        // The first terminal checkpoint owns exactly one
                        // output and cannot borrow another owner's guard or
                        // an accepted document's still-unpublished WAL.
                        for (self.control_owners, 0..) |owner, i| if (i != target.slot_index and owner != null)
                            return error.CompletionReservationBusy;
                        for (self.cells[0..self.cell_count]) |cell| if (cell.phase == .accepted or cell.phase == .prepared)
                            return error.CompletionReservationBusy;
                        if (backend.hasDurableCompletions() or backend.activeImmutableMemtableCount() != 0 or backend.runs.count() >= self.config.shape.max_runs or
                            try journalNeedsMaintenance(try self.io.storage().fileSize(self.journal_path), self.config.shape.max_record_bytes, 1))
                            return error.CompletionReservationBusy;
                        _ = try backend.planningDirectory();
                    }
                },
            }
            try self.certifyActiveControls(backend, .{});
            try self.validateBaseline(backend, alloc, entry);
            const accepted: control_accepted.Accepted = .{
                .slot_index = @intCast(target.slot_index),
                .txn_id = active.declaration.txn_id,
                .begin = .{ .term = active.begin.term, .index = active.begin.index, .digest = active.begin.digest },
                .transition = .{ .term = identity.term, .index = identity.index, .digest = identity.digest },
                .envelope = envelope,
            };
            const bytes = try accepted.encodeInto(active.held.accepted);
            active.held.accepted_len = bytes.len;
            active.pending = identity;
            active.transition = target.transition;
            var writer = self.io.storage().beginAtomicWrite(self.scratch.allocator(), self.control_accepted_paths[target.slot_index]) catch |err| {
                self.failed = true;
                backend.fenceFailedBulkWal();
                return err;
            };
            var live = true;
            errdefer if (live) writer.abort();
            writer.appendSlice(bytes) catch |err| {
                self.failed = true;
                backend.fenceFailedBulkWal();
                return err;
            };
            live = false;
            writer.finish() catch |err| {
                self.failed = true;
                backend.fenceFailedBulkWal();
                return err;
            };
            return target.slot_index;
        }

        /// The accepted sidecar survives uncertain append. A terminal ACK is
        /// checkpointed to its retained output before its BEGIN guard retires.
        pub fn applyOwnedControlTransition(self: *Self, backend: *Backend, identity: completion.AcceptedIdentity) !bool {
            if (self.failed or !self.restored or backend.manifest_recovery_required) return error.RecoveryRequired;
            const owner_index: usize = blk: {
                for (self.control_owners, 0..) |owner, i| if (owner) |active| {
                    if (active.pending) |pending| if (std.meta.eql(pending, identity)) break :blk i;
                };
                return false;
            };
            const active = &self.control_owners[owner_index].?;
            const transition = active.transition orelse return error.InvalidCompletionSlot;
            var borrow = try self.compiler.tryBorrow();
            defer borrow.release() catch unreachable;
            const alloc = try borrow.allocator();
            const accepted = try control_accepted.Accepted.decode(active.held.accepted[0..active.held.accepted_len]);
            if (accepted.slot_index != owner_index or !std.mem.eql(u8, &accepted.txn_id, &active.declaration.txn_id) or
                !std.meta.eql(accepted.transition, control_record.Receipt{ .term = identity.term, .index = identity.index, .digest = identity.digest }))
                return error.InvalidCompletionSlot;
            var decoded = try entry_codec.decode(alloc, accepted.envelope);
            defer decoded.deinit();
            if (!std.mem.eql(u8, &decoded.digest, &identity.digest)) return error.InvalidCompletionSlot;
            const ns = @import("../backend_types.zig").Namespace{ .name = decoded.decoded_descriptor.descriptor.namespace };
            var incoming: state.ActiveMemTable = .{};
            defer incoming.deinit(alloc);
            var new_resolved: ?[]const u8 = null;
            for (decoded.entry.prepare_operations) |op| {
                if (op.bindings.len != 0) return error.InvalidCompletionSlot;
                if (op.key.len == control_shape.resolved_participants_prefix.len + 16 and
                    std.mem.startsWith(u8, op.key, control_shape.resolved_participants_prefix) and
                    std.mem.eql(u8, op.key[control_shape.resolved_participants_prefix.len..], &active.declaration.txn_id))
                    new_resolved = op.value;
                try incoming.upsert(alloc, ns, op.key, op.value, op.kind == .delete);
            }
            const owner_record = self.controlOwnerRecord(owner_index);
            const progress: control_record.Progress = switch (transition) {
                .decision => |decision| .{
                    .txn_id = active.declaration.txn_id,
                    .begin = accepted.begin,
                    .latest = accepted.transition,
                    .phase = .decision,
                    .decision = decision,
                    .acknowledged = 0,
                    .resolved_digest = @splat(0),
                },
                .acknowledgement => |ack| blk: {
                    const participants_key = try std.mem.concat(alloc, u8, &.{ control_shape.participants_prefix, &active.declaration.txn_id });
                    defer alloc.free(participants_key);
                    const resolved_key = try std.mem.concat(alloc, u8, &.{ control_shape.resolved_participants_prefix, &active.declaration.txn_id });
                    defer alloc.free(resolved_key);
                    var participants = try self.point(backend, alloc, decoded.decoded_descriptor.descriptor.namespace, participants_key);
                    defer participants.deinit(alloc);
                    var resolved = try self.point(backend, alloc, decoded.decoded_descriptor.descriptor.namespace, resolved_key);
                    defer resolved.deinit(alloc);
                    const prior = try self.priorControlProgress(backend, alloc, decoded.decoded_descriptor.descriptor.namespace, owner_index, participants.value orelse return error.UnsupportedCompletionProfile, resolved.value);
                    if (prior.acknowledged + 1 != ack.count or ack.participant_index >= control_record.progress_bitmap_bits)
                        return error.UnsupportedCompletionProfile;
                    var bitmap = prior.ack_bitmap;
                    const mask = @as(u8, 1) << @intCast(ack.participant_index % 8);
                    if (bitmap[ack.participant_index / 8] & mask != 0) return error.UnsupportedCompletionProfile;
                    bitmap[ack.participant_index / 8] |= mask;
                    const next_resolved = new_resolved orelse return error.UnsupportedCompletionProfile;
                    if (!std.mem.eql(u8, &control_transition.resolvedDigest(next_resolved), &ack.resolved_digest) or
                        !std.mem.eql(u8, &try control_transition.resolvedBitmap(participants.value.?, next_resolved), &bitmap))
                        return error.UnsupportedCompletionProfile;
                    break :blk .{
                        .txn_id = active.declaration.txn_id,
                        .begin = accepted.begin,
                        .latest = accepted.transition,
                        .phase = .acknowledgement,
                        .decision = prior.decision,
                        .acknowledged = ack.count,
                        .resolved_digest = ack.resolved_digest,
                        .ack_bitmap = bitmap,
                    };
                },
            };
            try progress.verifyOwner(owner_record);
            const progress_value = try progress.encode();
            const terminal = progress.phase == .acknowledgement and progress.acknowledged == owner_record.participants.count;
            switch (transition) {
                .decision => {
                    const owner_value = try owner_record.encode();
                    try incoming.upsert(alloc, ns, &control_record.ownerKey(active.declaration.txn_id), &owner_value, false);
                },
                .acknowledgement => if (terminal) try incoming.upsert(alloc, ns, &control_record.ownerKey(active.declaration.txn_id), "", true),
            }
            try incoming.upsert(alloc, ns, &control_record.progressKey(active.declaration.txn_id), &progress_value, false);
            const marker = identity.encode();
            try incoming.upsert(alloc, ns, &@import("../internal_keys.zig").raft_document_applied_entry_key, marker[0..16], false);
            const group_progress = encodeProgress(self.config.identity, identity);
            try incoming.upsert(alloc, ns, entry_codec.group_progress_key, &group_progress, false);
            const pub_alloc = active.held.publication.allocator();
            var candidate = try backend.mutable.preparePublicationOwned(pub_alloc, &incoming);
            defer candidate.deinit(pub_alloc);
            var append = try wal.PreparedAppend.init(alloc, backend.root_dir.?, &incoming, true, .{ .segment_bytes = backend.options.wal_segment_bytes });
            defer append.deinit();
            var wal_lock = try backend.acquireWalOperationLock(.exclusive);
            defer wal_lock.release();
            self.failed = true; // Any subsequent uncertainty must never admit.
            errdefer backend.fenceFailedBulkWal();
            try active.held.chargeWal(&backend.tracked_wal_retention_bytes, @intCast(append.record.len));
            const outcome = try backend.wal_retention.appendPrepared(self.io.storage(), alloc, &append, backend.writeStatsNowNs());
            const result = switch (outcome) {
                .appended => |value| value,
                .uncertain => |err| return err,
            };
            backend.write_stats.wal_append_records += 1;
            backend.write_stats.wal_append_entries += incoming.entryCount();
            backend.write_stats.wal_append_bytes += result.bytes;
            backend.noteMutableWalSegment(result.segment);
            backend.invalidateMutableReadSnapshot();
            backend.mutable.publishPrepared(&candidate);
            backend.syncTrackedInMemoryStateUsageCurrentLocked();
            self.progress = identity;
            try self.io.storage().deleteFileAbsolute(self.control_accepted_paths[owner_index]);
            try self.io.storage().syncParentAbsolute(self.control_accepted_paths[owner_index]);
            active.pending = null;
            active.transition = null;
            active.held.accepted_len = 0;
            if (terminal) try self.checkpointTerminalControlOwner(backend, alloc, owner_index);
            self.failed = false;
            return true;
        }

        /// One retained owner spends its predeclared output after the final
        /// ACK's canonical rows, v3 receipt, and owner tombstone share one WAL
        /// record. Manifest publication precedes WAL reset and guard unlink.
        /// Any failure retains the guard (or fences the process if unlink was
        /// uncertain); restart still refuses an unresolved guarded owner.
        fn checkpointTerminalControlOwner(self: *Self, backend: *Backend, alloc: Allocator, owner_index: usize) !void {
            const active = &self.control_owners[owner_index].?;
            for (self.control_owners, 0..) |owner, i| if (i != owner_index and owner != null)
                return error.CompletionReservationBusy;
            const output_id = self.control_output_ids[owner_index];
            const output_path = active.output_pin.path orelse return error.InvalidCompletionSlot;
            var current = try backend.mutable.snapshot(alloc);
            defer current.deinit(alloc);
            const writer_limits: maintenance.Limits = .{
                .max_metadata_bytes = self.config.shape.max_metadata_bytes,
                .max_output_metadata_bytes = self.config.shape.max_metadata_bytes,
                .max_record_bytes = self.config.shape.max_record_bytes,
                .max_output_file_bytes = completion.limits.flush_bytes,
            };
            var manifest_attempted = false;
            errdefer if (!manifest_attempted) self.io.storage().deleteFileAbsolute(output_path) catch {};
            var built = try maintenance.buildStateDrain(alloc, self.io.storage(), backend.root_dir.?, &.{&current}, output_id, writer_limits);
            defer built.deinit(alloc);
            const pub_alloc = active.held.publication.allocator();
            var run = try repository.cloneRunCompactionSnapshot(pub_alloc, built);
            run.metadata_allocator = pub_alloc;
            var run_owned = true;
            defer if (run_owned) run.deinit(backend.allocator);
            var candidate = backend.runs.fork();
            var candidate_owned = true;
            defer if (candidate_owned) candidate.deinit(backend.allocator);
            try candidate.append(pub_alloc, run);
            run_owned = false;
            const directory = try backend.run_directory.?.fork(pub_alloc);
            var directory_owned = true;
            defer if (directory_owned) directory.destroy(backend.allocator);
            const View = struct {
                allocator: Allocator,
                options: @TypeOf(backend.options),
                pub fn retainRunSnapshotRef(_: *@This(), item: *repository.Run) !void {
                    try Backend.retainRunSnapshotRef(undefined, item);
                }
                pub fn releaseDirectoryRunSnapshotRef(item: *repository.Run) void {
                    Backend.releaseDirectoryRunSnapshotRef(item);
                }
            };
            var view = View{ .allocator = pub_alloc, .options = backend.options };
            try directory.put(&view, candidate.find(&run).?.*);
            const durable_directory = try directory.fork(pub_alloc);
            var durable_owned = true;
            defer if (durable_owned) durable_directory.destroy(backend.allocator);
            var meta = repository.runMeta(run);
            meta.path = repository.manifestRelativePath(backend.root_dir.?, meta.path);
            const sequence = try std.math.add(u64, backend.manifest_journal.sequence.?, 1);
            const frame = try manifest.encodeSingleRunJournalFrameAlloc(alloc, sequence, backend.next_run_id, meta);
            defer alloc.free(frame);
            manifest_attempted = true;
            try self.io.storage().appendFileAbsolute(alloc, self.journal_path, frame, true);
            if (@import("builtin").is_test and test_control_checkpoint_fault == .after_manifest) {
                test_control_checkpoint_fault_hit = true;
                return error.RecoveryRequired;
            }
            backend.invalidateMutableReadSnapshot();
            backend.invalidateReadVersion();
            std.mem.swap(@TypeOf(backend.runs), &backend.runs, &candidate);
            candidate.deinit(backend.allocator);
            candidate_owned = false;
            backend.publishRunDirectory(directory);
            directory_owned = false;
            backend.publishManifestDirectory(durable_directory);
            durable_owned = false;
            backend.mutable.deinit(backend.allocator);
            backend.mutable = .{};
            backend.mutable_wal_range = .{};
            backend.manifest_journal.sequence = sequence;
            backend.manifest_journal.bytes += frame.len;
            backend.manifest_journal.edit_bytes += frame.len;
            backend.manifest_journal.next_run_id = backend.next_run_id;
            backend.manifest_dirty = false;
            backend.manifest_unpublished_wire_bytes = 0;
            backend.manifest_pending_mutation_bytes = 0;
            backend.clearPublishedWalLogicalDebtLocked();
            backend.syncTrackedInMemoryStateUsageCurrentLocked();
            try wal.protectedReset(self.io.storage(), alloc, backend.root_dir.?);
            if (@import("builtin").is_test and test_control_checkpoint_fault == .after_wal_reset) {
                test_control_checkpoint_fault_hit = true;
                return error.RecoveryRequired;
            }
            backend.wal_retention.primary = .{ .oldest_retained_segment = 1, .current_segment = 1 };
            backend.wal_retention.replay = .{ .current_segment = 1 };
            backend.wal_retention.primary_ns = backend.writeStatsNowNs();
            backend.wal_retention.replay_ns = backend.writeStatsNowNs();
            try restoreWalCredits(self, backend);
            self.wal_pin.manager.observeUsage(.lsm_wal_retention, &backend.tracked_wal_retention_bytes, 0);
            const guard_path = try std.fs.path.join(alloc, &.{ backend.root_dir.?, control_guard.filenames[owner_index] });
            defer alloc.free(guard_path);
            try self.io.storage().deleteFileAbsolute(guard_path);
            try self.io.storage().syncParentAbsolute(guard_path);
            active.output_pin.release();
            active.held.destroy();
            self.control_owners[owner_index] = null;
            // The run frontier and mutable baseline changed. Requalification
            // must precede any further admission in this staged process.
            self.ready = false;
            self.capacity_certified = false;
            self.maintenance_pending = true;
        }

        /// Allocate independent control debt before publishing the accepted
        /// document cell. The durable guard makes any interrupted publication a
        /// startup obligation; runnable restore and control transitions remain
        /// disabled until the complete lifecycle is installed.
        fn stageControlBegin(self: *Self, backend: *Backend, declaration: control_begin.Declaration, identity: completion.AcceptedIdentity, envelope: []const u8, growth: capacity.Cost) !void {
            for (self.control_owners) |owner| if (owner) |active| if (active.pending != null)
                return error.CompletionReservationBusy;
            const slot = for (self.control_owners, 0..) |owner, i| {
                if (owner) |active| if (std.mem.eql(u8, &active.declaration.txn_id, &declaration.txn_id)) return error.CompletionReservationBusy;
                if (owner == null) break i;
            } else return error.CompletionPlanCapacityExceeded;
            var rows: [control_record.max_owners][3]control_capacity.NativeRowShape = undefined;
            var inputs: [control_record.max_owners]control_capacity.OwnerInput = undefined;
            var count: usize = 0;
            for (self.control_owners) |owner| if (owner) |active| {
                const budget = control_capacity.NumericBudget.fromMeasured(active.declaration.budget);
                rows[count] = control_capacity.ownershipRows(budget.mutations);
                inputs[count] = .{ .budget = budget, .rows = &rows[count] };
                count += 1;
            };
            const budget = control_capacity.NumericBudget.fromMeasured(declaration.budget);
            rows[count] = control_capacity.ownershipRows(budget.mutations);
            inputs[count] = .{ .budget = budget, .rows = &rows[count] };
            const base_cost = try self.capacity_cost.plus(growth);
            const mutable_growth = try self.capacity_growth.plus(growth);
            const proof = try control_capacity.certifyCohort(.{
                .cost = base_cost,
                .mutable_growth = mutable_growth,
                .baseline_frontier_bytes = self.capacity_baseline_frontier,
                .max_mutable_entries = try std.math.add(usize, completion.foreground_entries, std.math.cast(usize, mutable_growth.records) orelse return error.UnsupportedCompletionProfile),
                .current_runs = backend.runs.count(),
                .future_document_outputs = self.cell_count,
                .append = try (try self.remainingAppendBudget()).plus(completion.foreground_append_budget),
            }, inputs[0 .. count + 1], .{
                .format = .{ .metadata_bytes = self.config.shape.max_metadata_bytes },
                .max_record_bytes = self.config.shape.max_record_bytes,
                // The installed pool has at most shape.max_runs prepaid input
                // paths/cursors. A certificate for 69–72 inputs cannot be
                // spent until those extra native handles are installed.
                .max_inputs = self.config.shape.max_runs,
                .max_path_bytes = 544,
                .single_drain_metadata_bytes = self.config.shape.max_metadata_bytes,
                .max_retained_wal_bytes = completion.limits.wal_bytes,
                .replay_workspace_bytes = completion.scratch_bytes,
                .compiler_workspace_bytes = completion.scratch_bytes,
            });
            try proof.requireHeadroom(backend.write_stats, backend.manifest_journal.sequence orelse return error.RecoveryRequired, backend.next_run_id, if (backend.wal_retention.primary) |primary| primary.current_segment else 1, .{
                .manifest_steps = self.cell_count,
                .run_ids = self.cell_count,
                .wal_segments = self.cell_count,
            });
            // Prepay the run-path registry node from the retained pool domain;
            // later publication must not allocate a new path reference.
            const pin_alloc = self.control.allocator();
            const output_path = try repository.runPath(pin_alloc, backend.root_dir.?, self.control_output_ids[slot]);
            defer pin_alloc.free(output_path);
            var output_pin = try Backend.pinCompletionRunPath(pin_alloc, output_path);
            var pin_transferred = false;
            errdefer if (!pin_transferred) output_pin.release();
            const manager = backend.options.resource_manager orelse return error.CompletionResourceManagerRequired;
            const held = try control_resources.Resources.create(backend.allocator, manager, &self.compiler, declaration.txn_id, .{
                .publication_bytes = proof.owners[count].publication_bytes,
                .wal_bytes = proof.owners[count].append.bytes,
            });
            var transferred = false;
            errdefer if (!transferred) held.destroy();
            const authority: control_record.Authority = .{
                .group_id = self.config.identity.group_id,
                .incarnation = self.config.identity.incarnation,
                .policy_digest = self.config.identity.policy_digest,
                .schema_catalog_digest = self.config.schema_catalog_digest,
                .generation = self.config.identity.generation,
            };
            const owner_record: control_record.Record = .{
                .authority = authority,
                .txn_id = declaration.txn_id,
                .begin = .{ .term = identity.term, .index = identity.index, .digest = identity.digest },
                .participants = declaration.participants,
                .slot_index = @intCast(slot),
                .output_run_id = self.control_output_ids[slot],
            };
            var publication = try control_guard.Publication.prepare(backend.allocator, backend.root_dir.?, .{ .record = owner_record, .envelope = envelope });
            defer publication.deinit();
            self.control_owners[slot] = .{ .held = held, .output_pin = output_pin, .declaration = declaration, .begin = identity };
            transferred = true;
            pin_transferred = true;
            publication.stage(backend.storage.?) catch |err| {
                self.failed = true;
                backend.fenceFailedBulkWal();
                return err;
            };
            publication.publish(backend.storage.?) catch |err| {
                self.failed = true;
                backend.fenceFailedBulkWal();
                return err;
            };
        }

        pub fn validateBaseline(self: *Self, backend: *Backend, alloc: Allocator, entry: *const entry_codec.OwnedEntry) !void {
            var hash = entry_codec.BaselineHasher.init();
            for (entry.entry.baseline_keys) |key| {
                const value = try self.point(backend, alloc, entry.decoded_descriptor.descriptor.namespace, key);
                defer value.deinit(alloc);
                try hash.add(key, value.value);
            }
            if (!std.mem.eql(u8, &hash.finish(), &entry.entry.baseline_digest)) return error.CompletionProfileChanged;
        }

        /// Free cells may still become prepare+outcome obligations. Applied
        /// prepares need only their outcome; durable terminal cells need no WAL.
        /// This budget excludes the future transaction-control owner lifetime.
        fn remainingAppendBudget(self: *const Self) !completion.AppendCounters {
            var result: completion.AppendCounters = .{};
            for (self.cells[0..self.cell_count]) |cell| {
                switch (cell.phase) {
                    .spent => continue,
                    .free => result = try result.plus(try completion.prepare_append_budget.plus(completion.outcome_append_budget)),
                    .accepted, .prepared => {
                        if (cell.phase == .prepared and cell.slot.retired) continue;
                        const is_prepare = cell.entry.?.entry.kind == .prepare;
                        if (cell.phase == .accepted or !cell.slot.durable) result = try result.plus(completion.prepare_append_budget);
                        if (is_prepare) result = try result.plus(completion.outcome_append_budget);
                    },
                }
            }
            return result;
        }

        fn checkFreshAppendHeadroom(self: *const Self, backend: *const Backend) !void {
            const reserve = try (try self.remainingAppendBudget()).plus(completion.foreground_append_budget);
            try reserve.requireHeadroom(backend.write_stats, .{});
        }

        pub fn checkOrdinary(self: *Self, backend: *Backend, incoming: anytype) !void {
            if (self.maintenance_active) return error.CompletionReservationBusy;
            if (self.failed or !self.restored) return error.RecoveryRequired;
            if (self.control_transition_staging and !self.capacity_certified) return error.CompletionReservationBusy;
            for (self.control_owners) |owner| if (owner) |active| {
                // Phase A has no certified pressure-relief output while a
                // decision is runnable. Keep its physical envelope exclusive.
                if (self.control_transition_staging) return error.CompletionReservationBusy;
                if (active.pending != null or !self.capacity_certified) return error.CompletionReservationBusy;
            };
            const reserve = try self.remainingAppendBudget();
            reserve.requireHeadroom(backend.write_stats, completion.incomingAppendCounters(incoming)) catch return error.CompletionForegroundCapacityExceeded;
            if (incoming.estimatedLogicalBytes() > completion.foreground_bytes -| backend.mutable.estimatedLogicalBytes() or
                incoming.entryCount() > completion.foreground_entries -| backend.mutable.entryCount() or
                self.replayed_wal_bytes +| (backend.write_stats.wal_append_bytes -| self.wal_bytes_start) +| @import("wal.zig").encodedStateRecordLen(incoming) > completion.recovery_wal_bytes or
                self.replayed_wal_entries +| (backend.write_stats.wal_append_entries -| self.wal_entries_start) +| incoming.entryCount() > completion.foreground_entries or
                self.replayed_wal_records +| (backend.write_stats.wal_append_records -| self.wal_records_start) >= completion.recovery_wal_records)
                return error.CompletionForegroundCapacityExceeded;
            for (0..incoming.entryCount()) |i| {
                const item = incoming.entryAt(i);
                try validateRecordSize(item.namespace_name, item.key, item.value.len, self.config.shape.max_record_bytes);
                if (!item.tombstone) try self.counterValueHeadroom(item.key, item.value, self.capacity_max_credit_bytes);
                const key = item.key;
                if (std.mem.eql(u8, key, entry_codec.group_progress_key) or
                    std.mem.startsWith(u8, key, entry_codec.receipt_prefix) or
                    std.mem.startsWith(u8, key, control_record.owner_prefix) or
                    std.mem.startsWith(u8, key, control_record.receipt_prefix) or
                    std.mem.startsWith(u8, key, control_record.progress_prefix) or
                    std.mem.startsWith(u8, key, completion.storage_key) or
                    std.mem.startsWith(u8, key, completion.applied_key)) return error.PreparedCompletionActive;
            }
            try self.checkAcceptedFootprint(incoming);
            if (self.capacity_certified) {
                var growth: capacity.Cost = .{};
                for (0..incoming.entryCount()) |i| {
                    const item = incoming.entryAt(i);
                    growth = try growth.plus(try capacity.Cost.record(if (item.namespace_name) |ns| ns.len else 0, item.key.len, item.value.len));
                }
                try self.certifyActiveControls(backend, growth);
                try self.checkCapacity(growth);
                // Preflight can precede a failed write. Retain the conservative
                // charge until maintenance rather than refund uncertain I/O.
                self.chargeCapacity(growth);
            }
        }

        fn checkCapacity(self: *const Self, growth: capacity.Cost) !void {
            if (!self.capacity_certified) return error.CompletionReservationBusy;
            const total = try self.capacity_cost.plus(growth);
            const proof = try capacity.certify(total, .{ .metadata_bytes = self.config.shape.max_metadata_bytes, .additional_runs = self.cell_count });
            try metadataCapacity(proof.outputs, total.max_key_bytes);
            const future = try self.capacity_growth.plus(growth);
            // A protected drain writes one run from mutable+delta. Its metadata
            // must remain readable by the later bounded maintenance cursor.
            _ = try completion.operationWorkspaceRequirement(future, .{
                .max_metadata_bytes = self.config.shape.max_metadata_bytes,
                .max_output_metadata_bytes = self.config.shape.max_metadata_bytes,
                .max_record_bytes = self.config.shape.max_record_bytes,
                .max_output_file_bytes = completion.limits.flush_bytes,
            });
            const added = try capacity.certify(future, .{ .metadata_bytes = self.config.shape.max_metadata_bytes, .additional_runs = self.cell_count });
            // Existing immutable cursors remain live while the newly generated
            // protected runs are merged. Certifying only the replacement run
            // layout would omit this first maintenance frontier.
            if (self.capacity_baseline_frontier +| added.frontier_bytes > 16 * 1024 * 1024)
                return error.UnsupportedCompletionProfile;
            _ = try maintenance.workspaceRequirement(total, @max(proof.frontier_bytes, self.capacity_baseline_frontier +| added.frontier_bytes), proof.outputs, .{
                .max_metadata_bytes = self.config.shape.max_metadata_bytes,
                .max_output_metadata_bytes = self.config.shape.max_metadata_bytes,
                .max_record_bytes = self.config.shape.max_record_bytes,
            });
        }

        /// Reprove all retained control obligations after any competing
        /// foreground or document growth. A BEGIN's one-time certificate is
        /// not permission for later work to consume its unpaid capacity.
        fn certifyActiveControls(self: *const Self, backend: *const Backend, growth: capacity.Cost) !void {
            var rows: [control_record.max_owners][3]control_capacity.NativeRowShape = undefined;
            var inputs: [control_record.max_owners]control_capacity.OwnerInput = undefined;
            var count: usize = 0;
            for (self.control_owners) |owner| if (owner) |active| {
                const budget = control_capacity.NumericBudget.fromMeasured(active.declaration.budget);
                rows[count] = control_capacity.ownershipRows(budget.mutations);
                inputs[count] = .{ .budget = budget, .rows = &rows[count] };
                count += 1;
            };
            if (count == 0) return;
            const projected = try self.capacity_cost.plus(growth);
            const mutable = try self.capacity_growth.plus(growth);
            const proof = try control_capacity.certifyCohort(.{
                .cost = projected,
                .mutable_growth = mutable,
                .baseline_frontier_bytes = self.capacity_baseline_frontier,
                .max_mutable_entries = try std.math.add(usize, completion.foreground_entries, std.math.cast(usize, mutable.records) orelse return error.UnsupportedCompletionProfile),
                .current_runs = backend.runs.count(),
                .future_document_outputs = self.cell_count,
                .append = try (try self.remainingAppendBudget()).plus(completion.foreground_append_budget),
            }, inputs[0..count], .{
                .format = .{ .metadata_bytes = self.config.shape.max_metadata_bytes },
                .max_record_bytes = self.config.shape.max_record_bytes,
                .max_inputs = self.config.shape.max_runs,
                .max_path_bytes = 544,
                .single_drain_metadata_bytes = self.config.shape.max_metadata_bytes,
                .max_retained_wal_bytes = completion.limits.wal_bytes,
                .replay_workspace_bytes = completion.scratch_bytes,
                .compiler_workspace_bytes = completion.scratch_bytes,
            });
            try proof.requireHeadroom(backend.write_stats, backend.manifest_journal.sequence orelse return error.RecoveryRequired, backend.next_run_id, if (backend.wal_retention.primary) |primary| primary.current_segment else 1, .{
                .manifest_steps = self.cell_count,
                .run_ids = self.cell_count,
                .wal_segments = self.cell_count,
            });
        }

        fn chargeCapacity(self: *Self, growth: capacity.Cost) void {
            self.capacity_cost = self.capacity_cost.plus(growth) catch unreachable;
            self.capacity_growth = self.capacity_growth.plus(growth) catch unreachable;
        }

        fn counterValueHeadroom(self: *const Self, key: []const u8, value: []const u8, max_credit: u64) !void {
            if (std.mem.eql(u8, key, &@import("../internal_keys.zig").replay_meta_next_sequence_key)) {
                if (value.len != 8) return error.InvalidCompletionSlot;
                try capacity.sharedCounterHeadroom(std.mem.readInt(u64, value[0..8], .little), 1, self.cell_count);
            } else if (std.mem.eql(u8, key, "\x00\x00__metadata__:txn_completion_v1")) {
                if (value.len != 16) return error.InvalidCompletionSlot;
                try capacity.sharedCounterHeadroom(std.mem.readInt(u64, value[0..8], .little), 1, self.cell_count);
                try capacity.sharedCounterHeadroom(std.mem.readInt(u64, value[8..16], .little), max_credit, self.cell_count);
            }
        }

        fn checkCounterHeadroom(self: *Self, backend: *Backend, alloc: Allocator, entry: *const entry_codec.OwnedEntry) !u64 {
            try self.checkFreshAppendHeadroom(backend);
            try capacity.nativeCounterHeadroom(backend.manifest_journal.sequence orelse return error.RecoveryRequired, backend.next_run_id, if (backend.wal_retention.primary) |primary| primary.current_segment else 1, self.cell_count);
            var maximum_credit = self.capacity_max_credit_bytes;
            const credit_prefix = "\x00\x00__txn_completion_v1__:";
            for (entry.entry.prepare_operations) |op| {
                if (op.kind != .put or op.key.len != credit_prefix.len + 16 or !std.mem.startsWith(u8, op.key, credit_prefix)) continue;
                if (op.value.len != 16) return error.InvalidCompletionSlot;
                const bytes = std.math.add(u64, std.mem.readInt(u64, op.value[0..8], .little), std.mem.readInt(u64, op.value[8..16], .little)) catch
                    return error.UnsupportedCompletionProfile;
                maximum_credit = @max(maximum_credit, bytes);
            }
            const ns = entry.decoded_descriptor.descriptor.namespace;
            for ([_][]const u8{ &@import("../internal_keys.zig").replay_meta_next_sequence_key, "\x00\x00__metadata__:txn_completion_v1" }) |key| {
                const current = try self.point(backend, alloc, ns, key);
                defer current.deinit(alloc);
                if (current.value) |value| try self.counterValueHeadroom(key, value, maximum_credit);
            }
            for (entry.entry.prepare_operations) |op| if (op.kind == .put)
                try self.counterValueHeadroom(op.key, op.value, maximum_credit);
            return maximum_credit;
        }

        pub fn checkAcceptedFootprint(self: *Self, incoming: anytype) !void {
            if (self.failed or !self.restored) return error.RecoveryRequired;
            for (0..incoming.entryCount()) |i| {
                const op = incoming.entryAt(i);
                for (self.cells[0..self.cell_count]) |*cell| {
                    if (cell.phase != .accepted) continue;
                    const owned = &cell.entry.?;
                    const namespace = owned.decoded_descriptor.descriptor.namespace;
                    if (namespace == null and op.namespace_name != null or namespace != null and op.namespace_name == null) continue;
                    if (namespace != null and !std.mem.eql(u8, namespace.?, op.namespace_name.?)) continue;
                    for (owned.entry.baseline_keys) |key| if (std.mem.eql(u8, key, op.key)) return error.PreparedCompletionActive;
                }
            }
        }

        pub fn hasAcceptedDebt(self: *const Self) bool {
            for (self.cells[0..self.cell_count]) |cell| if (cell.phase == .accepted) return true;
            for (self.control_owners) |owner| if (owner) |active| if (active.pending != null) return true;
            return false;
        }

        pub fn clearTerminalAccepted(self: *Self) !void {
            if (self.hasAcceptedDebt() or self.failed) return error.CompletionReservationBusy;
            for (self.cells[0..self.cell_count], 0..) |cell, i| {
                if (cell.phase != .prepared) continue;
                if (!cell.slot.retired) return error.CompletionReservationBusy;
                try self.io.storage().deleteFileAbsolute(self.accepted_paths[i]);
                try self.io.storage().syncParentAbsolute(self.accepted_paths[i]);
            }
        }

        /// The caller's applied frontier equals index-1. This is authoritative
        /// pre-persistence Ready/proposal admission, never speculative inbound
        /// traffic. A failed write leaves the cell owned and fences the pool.
        pub fn accept(self: *Self, backend: *Backend, term: u64, index: u64, previous_term: u64, applied: u64, envelope: []const u8) !usize {
            if (!self.ready or self.failed or !self.restored) return error.RecoveryRequired;
            if (term == 0 or index == 0) return error.InvalidCompletionSlot;
            for (self.control_owners) |owner| if (owner) |active| if (active.pending != null) return error.CompletionReservationBusy;
            var borrow = try self.compiler.tryBorrow();
            defer borrow.release() catch unreachable;
            const scratch = try borrow.allocator();
            var checked = try entry_codec.decode(scratch, envelope);
            defer checked.deinit();
            try self.validateEntry(&checked);
            for (self.cells[0..self.cell_count], 0..) |*cell, i| {
                if (cell.phase == .free or cell.phase == .spent) continue;
                if (cell.index == index) {
                    if (cell.term == term and std.mem.eql(u8, &cell.entry.?.digest, &checked.digest)) return i;
                    return error.CompletionReservationBusy;
                }
                if (std.mem.eql(u8, &cell.entry.?.entry.txn_id, &checked.entry.txn_id)) return error.InvalidCompletionSlot;
                // Earlier accepted work must apply before accepting a baseline
                // dependent successor. Disjoint prepared footprints can coexist.
                if (cell.phase == .accepted) return error.CompletionReservationBusy;
            }
            if (applied != index - 1) return error.CompletionReservationBusy;
            if (checked.entry.previous_index != index - 1 or checked.entry.previous_term != previous_term or
                (index > 1 and previous_term == 0)) return error.CompletionProfileChanged;
            try Slot.validateFootprint(backend, checked.decoded_descriptor.descriptor);
            if (checked.entry.kind == .mutation)
                try Slot.validateCanonicalFootprint(backend, checked.decoded_descriptor.descriptor.namespace, checked.entry.prepare_operations);
            if (try self.classifyOwnedControlTransition(backend, scratch, &checked)) |target|
                return self.acceptOwnedControlTransition(backend, scratch, target, .{ .term = term, .index = index, .digest = checked.digest }, &checked, envelope);
            if (self.control_transition_staging) {
                for (self.control_owners) |owner| if (owner != null) return error.CompletionReservationBusy;
            }
            const fresh_begin = try self.validateNewTransactionRecord(backend, scratch, &checked);
            try self.validateBaseline(backend, scratch, &checked);
            const growth = try entryCapacity(&checked);
            try self.checkCapacity(growth);
            if (fresh_begin == null) try self.certifyActiveControls(backend, growth);
            const maximum_credit = try self.checkCounterHeadroom(backend, scratch, &checked);
            const free_index = for (self.cells[0..self.cell_count], 0..) |cell, i| {
                if (cell.phase == .free) break i;
            } else return error.CompletionReservationBusy;
            const cell = &self.cells[free_index];
            const alloc = cell.publication.?.allocator();
            var transferred = false;
            // This private span is monotonic. An allocation-stage rejection
            // spends it; maintenance replaces it before another admission.
            // Earlier pure validation failures leave the untouched cell free.
            errdefer if (!transferred) {
                cell.publication.?.finish();
                cell.publication = null;
                cell.phase = .spent;
                self.ready = false;
            };
            var owned = try entry_codec.decode(alloc, envelope);
            errdefer if (!transferred) owned.deinit();
            var cohort = self.cohort;
            cohort.index = @intCast(free_index);
            const wire = try encodeAccepted(alloc, .{ .cell = @intCast(free_index), .term = term, .index = index, .identity = self.config.identity, .cohort = cohort, .envelope = envelope });
            errdefer if (!transferred) alloc.free(wire);
            var baseline_cell = cell.*;
            baseline_cell.entry = owned;
            const baseline = try self.captureBaseline(backend, scratch, &baseline_cell, free_index);
            const token = if (self.publication_owner) |owner| try owner.prepare(owner.context, alloc, &owned) else null;
            errdefer if (!transferred) if (token) |value| self.publication_owner.?.cancel(self.publication_owner.?.context, value);
            if (cell.publication.?.remainingBytes() < try nativePublicationCapacity(&owned, self.config.shape.max_record_bytes))
                return error.CompletionPlanCapacityExceeded;
            // Complete all fallible allocation before durable I/O starts.
            if (fresh_begin) |declaration| if (self.config.control_owner_staging) {
                try self.stageControlBegin(backend, declaration, .{ .term = term, .index = index, .digest = checked.digest }, envelope, growth);
            };
            cell.baseline = baseline;
            cell.publication_token = token;
            cell.entry = owned;
            cell.accepted_wire = wire;
            cell.term = term;
            cell.index = index;
            cell.phase = .accepted;
            self.chargeCapacity(growth);
            self.capacity_max_credit_bytes = maximum_credit;
            transferred = true;
            self.writeAccepted(free_index) catch |err| {
                self.failed = true;
                backend.fenceFailedBulkWal();
                // Ownership stays in the cell after uncertain publication.
                return err;
            };
            return free_index;
        }

        fn writeAccepted(self: *Self, index: usize) !void {
            var writer = try self.io.storage().beginAtomicWrite(self.scratch.allocator(), self.accepted_paths[index]);
            var live = true;
            errdefer if (live) writer.abort();
            try writer.appendSlice(self.cells[index].accepted_wire.?);
            live = false;
            try writer.finish();
        }

        fn capture(self: *Self, backend: *Backend, alloc: Allocator, namespace: ?[]const u8, key: []const u8, comptime n: usize) !?[n]u8 {
            const value = try self.point(backend, alloc, namespace, key);
            defer value.deinit(alloc);
            const bytes = value.value orelse return null;
            if (bytes.len != n) return error.InvalidCompletionSlot;
            return bytes[0..n].*;
        }

        fn captureBaseline(self: *Self, backend: *Backend, alloc: Allocator, cell: *Cell, index: usize) !completion.PooledBaseline {
            const id = cell.entry.?.entry.txn_id;
            const ns = cell.entry.?.decoded_descriptor.descriptor.namespace;
            const record_key = "\x00\x00__txn_records__:" ++ "\x00" ** 16;
            const credit_key = "\x00\x00__txn_completion_v1__:" ++ "\x00" ** 16;
            var record = record_key.*;
            var credit = credit_key.*;
            @memcpy(record[record.len - 16 ..], &id);
            @memcpy(credit[credit.len - 16 ..], &id);
            const stored = try self.point(backend, alloc, ns, completion.storage_keys[index]);
            defer stored.deinit(alloc);
            if (stored.value) |value| if (!std.mem.eql(u8, value, cell.entry.?.entry.descriptor)) return error.InvalidCompletionSlot;
            return .{
                .record = try self.capture(backend, alloc, ns, &record, 53),
                .credit = try self.capture(backend, alloc, ns, &credit, 16),
                .summary = try self.capture(backend, alloc, ns, "\x00\x00__metadata__:txn_completion_v1", 16),
                .applied = try self.capture(backend, alloc, ns, completion.applied_keys[index], 16),
                .slot_present = stored.value != null,
            };
        }

        /// Restore all accepted sidecars into preowned cells before publication.
        /// No absent record implies truncation; caller must later reconcile the
        /// complete durable Raft suffix and any already-applied native receipt.
        pub fn restoreAccepted(self: *Self, backend: *Backend) !void {
            if (self.restored) return error.InvalidCompletionSlot;
            var borrow = try self.compiler.tryBorrow();
            defer borrow.release() catch unreachable;
            const scratch = try borrow.allocator();
            var first: ?completion.guard.Info = null;
            for (0..max_slots) |i| {
                const size = self.io.storage().fileSize(self.accepted_paths[i]) catch |err| switch (err) {
                    error.FileNotFound => continue,
                    else => return err,
                };
                if (i >= self.cell_count or size > max_accepted_bytes) return error.InvalidCompletionSlot;
                const cell = &self.cells[i];
                if (cell.phase != .free) return error.InvalidCompletionSlot;
                const alloc = cell.publication.?.allocator();
                const wire = try alloc.alloc(u8, @intCast(size));
                var transferred = false;
                errdefer if (!transferred) alloc.free(wire);
                try self.io.storage().readFileRangeInto(scratch, self.accepted_paths[i], 0, wire);
                const accepted = try decodeAccepted(wire);
                if (accepted.cell != i or !std.meta.eql(accepted.identity, self.config.identity)) return error.InvalidCompletionSlot;
                if (first) |cohort| {
                    if (cohort.base_run_id != accepted.cohort.base_run_id or cohort.initial_runs != accepted.cohort.initial_runs or
                        !std.mem.eql(u8, &cohort.cohort_id, &accepted.cohort.cohort_id)) return error.InvalidCompletionSlot;
                } else first = accepted.cohort;
                var owned = try entry_codec.decode(alloc, accepted.envelope);
                errdefer if (!transferred) owned.deinit();
                try self.validateEntry(&owned);
                for (self.cells[0..self.cell_count]) |other| {
                    if (other.entry) |e| if (other.index == accepted.index or std.mem.eql(u8, &e.entry.txn_id, &owned.entry.txn_id)) return error.InvalidCompletionSlot;
                }
                cell.entry = owned;
                cell.accepted_wire = wire;
                cell.term = accepted.term;
                cell.index = accepted.index;
                cell.phase = .accepted;
                self.startup_reconciliation_pending = true;
                transferred = true;
                cell.baseline = try self.captureBaseline(backend, scratch, cell, i);
            }
            // Paths cannot be silently rebound here. Startup installation must
            // have used the saved cohort's output range when guards exist.
            if (first) |cohort| if (cohort.base_run_id != self.cohort.base_run_id or cohort.initial_runs != self.cohort.initial_runs)
                return error.CompletionRecoveryCapacityRequired;
            self.restored = true;
        }

        pub fn maintenanceRequired(self: *const Self, backend: *const Backend) bool {
            if (self.maintenance_pending or backend.mutable.entryCount() != 0 or backend.runs.count() > 64) return true;
            for (self.cells[0..self.cell_count]) |cell| if (cell.phase == .spent) return true;
            return false;
        }

        /// Baseline shape qualification runs outside consensus locks. It does
        /// no compaction and issues no proof while restoration has pending debt.
        pub fn qualifyFresh(self: *Self, backend: *Backend) !void {
            if (!self.restored or self.failed or backend.manifest_recovery_required) return error.RecoveryRequired;
            if (self.maintenanceRequired(backend)) return error.CompletionReservationBusy;
            try self.checkFreshAppendHeadroom(backend);
            for (self.cells[0..self.cell_count]) |cell| if (cell.phase != .free) return error.CompletionReservationBusy;
            if (!backend.mutable.ordered_enabled) return error.UnsupportedCompletionProfile;
            if (backend.mutable.entryCount() != 0 or backend.activeImmutableMemtableCount() != 0 or
                backend.runs.count() > self.config.shape.max_runs) return error.CompletionReservationBusy;
            var borrow = try self.compiler.tryBorrow();
            defer borrow.release() catch unreachable;
            const alloc = try borrow.allocator();
            var paths: [68][]const u8 = undefined;
            for (0..backend.runs.count()) |i| paths[i] = backend.runs.at(i).path orelse return error.UnsupportedCompletionProfile;
            const measured = try maintenance.measure(alloc, self.io.storage(), paths[0..backend.runs.count()], .{
                .max_inputs = self.config.shape.max_runs,
                .max_metadata_bytes = self.config.shape.max_metadata_bytes,
                .max_block_bytes = self.config.shape.max_block_bytes,
                .max_record_bytes = self.config.shape.max_record_bytes,
            });
            const proof = try capacity.certify(measured.cost, .{ .metadata_bytes = self.config.shape.max_metadata_bytes, .additional_runs = self.cell_count });
            try metadataCapacity(proof.outputs, measured.cost.max_key_bytes);
            _ = try maintenance.workspaceRequirement(measured.cost, @max(measured.frontier_bytes, proof.frontier_bytes), proof.outputs, .{
                .max_metadata_bytes = self.config.shape.max_metadata_bytes,
                .max_output_metadata_bytes = self.config.shape.max_metadata_bytes,
                .max_record_bytes = self.config.shape.max_record_bytes,
            });
            try self.restoreProgress(backend, alloc);
            self.capacity_cost = measured.cost;
            self.capacity_growth = .{};
            self.capacity_max_credit_bytes = 0;
            self.capacity_baseline_frontier = measured.frontier_bytes;
            self.capacity_certified = true;
            self.ready = true;
        }

        /// Caller holds backend serialization and runs outside consensus locks.
        /// This may release the backend lock for streaming I/O; readiness and
        /// every ordinary mutation are fenced until the handoff completes.
        pub fn maintainLocked(self: *Self, backend: *Backend) !void {
            // The bounded document maintenance proof does not include the
            // staged owner's future checkpoint. Defer that merge until the
            // combined control checkpoint contract is implemented.
            if (self.control_transition_staging) {
                for (self.control_owners) |owner| if (owner != null) return error.CompletionReservationBusy;
            }
            try @import("completion_maintenance_cycle.zig").run(Backend, self, backend);
        }

        pub fn checkMaintenanceMetadata(_: *Self, outputs: []const repository.Run) !void {
            var largest: usize = 0;
            for (outputs) |run| {
                largest = @max(largest, run.smallest_key.len + (if (run.smallest_namespace_name) |name| name.len else @as(usize, 0)));
                largest = @max(largest, run.largest_key.len + (if (run.largest_namespace_name) |name| name.len else @as(usize, 0)));
            }
            try metadataCapacity(outputs.len, largest);
        }

        /// Adoption transfers the concrete publication span into the native
        /// Slot. A failure after this point retains ownership for recovery.
        pub fn adopt(self: *Self, backend: *Backend, index: usize) !*Slot {
            if (index >= self.cell_count or self.failed or !self.restored) return error.InvalidCompletionSlot;
            const cell = &self.cells[index];
            if (cell.phase == .prepared) return cell.slot;
            if (cell.phase != .accepted) return error.InvalidCompletionSlot;
            var cohort = self.cohort;
            cohort.index = @intCast(index);
            const slot = try Slot.createFromPool(backend, .{
                .self_storage = cell.slot,
                .publication = cell.publication.?,
                .scratch = self.scratch,
                .drain_workspace = &self.compiler,
                .drain_limits = .{
                    .max_metadata_bytes = self.config.shape.max_metadata_bytes,
                    .max_output_metadata_bytes = self.config.shape.max_metadata_bytes,
                    .max_record_bytes = self.config.shape.max_record_bytes,
                    .max_output_file_bytes = completion.limits.flush_bytes,
                },
                .io = self.io,
                .memory_pin = self.memory_pin,
                .wal_pin = self.wal_pin,
                .owner = .{ .context = self, .release_cell = releaseCell, .restore_wal_credits_after_checkpoint = restoreWalCredits, .prepare_progress = prepareProgress, .publish_progress = publishProgress },
                .baseline = cell.baseline,
                .accepted_identity = .{ .term = cell.term, .index = cell.index, .digest = cell.entry.?.digest },
                .durable_restored = cell.restored_prepared,
                .encoded = cell.entry.?.entry.descriptor,
                .cohort = cohort,
                .guard_path = self.guard_paths[index],
                .journal_path = self.journal_path,
                .run_paths = self.run_paths,
            });
            cell.phase = .prepared;
            return slot;
        }

        /// Pool slabs and all accepted envelopes are restored before parsing
        /// primary WAL. Retained nodes carry allocator provenance past teardown.
        pub fn replayBeforePublication(self: *Self, backend: *Backend) !void {
            if (!self.restored or self.ready or self.failed) return error.RecoveryRequired;
            if (!backend.mutable.ordered_enabled or backend.mutable.entryCount() != 0 or !self.scratch.isEmpty())
                return error.CompletionRecoveryCapacityRequired;
            _ = try replayWorkspaceRequirement();
            const alloc = self.scratch.allocator();
            var lock = try backend.acquireWalOperationLock(.exclusive);
            defer lock.release();
            if (try wal.currentSegment(self.io.storage(), alloc, backend.root_dir.?) > replay_max_records + 1) return error.CompletionRecoveryCapacityRequired;
            const retention = try wal.snapshotRetention(self.io.storage(), alloc, backend.root_dir.?);
            if (retention.bytes > replay_max_bytes or retention.segments > replay_max_records + 1) return error.CompletionRecoveryCapacityRequired;
            const pending = try alloc.alloc(u8, replay_max_bytes);
            defer alloc.free(pending);
            const Hooks = struct {
                pool: *Self,
                entries: usize = 0,
                records: usize = 0,
                fn entryAllocator(raw: *anyopaque, _: Allocator) !Allocator {
                    const hooks: *@This() = @ptrCast(@alignCast(raw));
                    return hooks.pool.scratch.allocator();
                }
                fn onEntry(raw: *anyopaque, _: u64, _: u64) !void {
                    const hooks: *@This() = @ptrCast(@alignCast(raw));
                    hooks.entries += 1;
                    if (hooks.entries > replay_max_entries) return error.CompletionRecoveryCapacityRequired;
                }
                fn onRecord(raw: *anyopaque, _: u64, _: u64) !void {
                    const hooks: *@This() = @ptrCast(@alignCast(raw));
                    hooks.records += 1;
                    if (hooks.records > replay_max_records) return error.CompletionRecoveryCapacityRequired;
                }
            };
            var hooks: Hooks = .{ .pool = self };
            const stats = try wal.replayIntoMutableWithHooksAndOptions(self.io.storage(), alloc, backend.root_dir.?, &backend.mutable, .{
                .ctx = &hooks,
                .entry_allocator = Hooks.entryAllocator,
                .on_applied_entry = Hooks.onEntry,
                .on_applied_record = Hooks.onRecord,
            }, .{ .pending_buffer = pending });
            if (stats.truncated_tail_bytes != 0) {
                const segment = stats.truncated_tail_segment orelse return error.InvalidCompletionSlot;
                if (stats.multiple_truncated_segments or segment != retention.current_segment) return error.InvalidCompletionSlot;
                const path = try std.fmt.allocPrint(alloc, "{s}/wal/{d:0>20}.log", .{ backend.root_dir.?, segment });
                defer alloc.free(path);
                const size = try self.io.storage().fileSize(path);
                if (stats.truncated_tail_bytes > size) return error.InvalidCompletionSlot;
                try self.io.truncateWalTail(segment, size - stats.truncated_tail_bytes);
            }
            backend.wal_retention.primary = try wal.snapshotRetention(self.io.storage(), alloc, backend.root_dir.?);
            backend.wal_retention.primary_ns = backend.writeStatsNowNs();
            // Restore observed WAL by transferring already held cell credit.
            var remaining = backend.wal_retention.primary.?.bytes;
            for (self.cells[0..self.cell_count]) |*cell| {
                const charge = @min(remaining, cell.slot.wal_credit);
                if (charge != 0) try self.wal_pin.manager.transferUsage(.lsm_wal_retention, &cell.slot.wal_credit, cell.slot.wal_credit - charge, &backend.tracked_wal_retention_bytes, backend.tracked_wal_retention_bytes + charge);
                remaining -= charge;
            }
            if (remaining != 0) return error.CompletionRecoveryCapacityRequired;
            // Restarts do not renew the cumulative foreground allowance. Count
            // all retained records conservatively until a protected checkpoint;
            // this also covers duplicate versions absent from the mutable root.
            self.replayed_wal_bytes = backend.wal_retention.primary.?.bytes;
            self.replayed_wal_entries = stats.entries;
            self.replayed_wal_records = stats.records;
            backend.write_stats.wal_replay_records += stats.records;
            backend.write_stats.wal_replay_entries += stats.entries;
            backend.write_stats.wal_replay_bytes += stats.bytes;
            backend.write_stats.wal_replay_truncated_tail_bytes += stats.truncated_tail_bytes;
            backend.mutable_wal_range = if (backend.mutable.entryCount() == 0) .{} else .{ .first = retention.oldest_retained_segment, .last = retention.current_segment };
            backend.syncTrackedInMemoryStateUsageCurrentLocked();
        }

        /// Matches only actual persisted native control records. An accepted
        /// sidecar alone is never a prepared transaction or a decision to abort.
        pub fn restoreMaterialized(self: *Self, backend: *Backend) !void {
            var completed: [max_slots]bool = @splat(false);
            var needs_drain: ?*Slot = null;
            {
                var borrow = try self.compiler.tryBorrow();
                defer borrow.release() catch unreachable;
                const alloc = try borrow.allocator();
                try self.restoreProgress(backend, alloc);
                // Materialize all siblings before any drain refreshes their shared
                // baselines. A manifested sibling is not authority to discard WAL
                // containing later ordinary writes or another accepted prepare.
                for (self.cells[0..self.cell_count], 0..) |*cell, i| {
                    if (cell.phase != .accepted) continue;
                    const ns = cell.entry.?.decoded_descriptor.descriptor.namespace;
                    const id = cell.entry.?.entry.txn_id;
                    const manifested = !cell.baseline.slot_present and if (cell.baseline.applied) |marker| std.mem.eql(u8, &marker, &id) else false;
                    const baseline = try self.captureBaseline(backend, alloc, cell, i);
                    const receipt_key = entry_codec.receiptKey(id);
                    const receipt = try self.capture(backend, alloc, ns, &receipt_key, 48);
                    const expected = completion.AcceptedIdentity{ .term = cell.term, .index = cell.index, .digest = cell.entry.?.digest };
                    completed[i] = !baseline.slot_present and if (baseline.applied) |marker| std.mem.eql(u8, &marker, &id) else false;
                    if (baseline.slot_present) {
                        if (receipt == null or !std.mem.eql(u8, &receipt.?, &expected.encode())) return error.InvalidCompletionSlot;
                        cell.baseline = baseline;
                        cell.restored_prepared = true;
                    } else if (receipt != null) return error.InvalidCompletionSlot else if (!completed[i]) continue;
                    const slot = try self.adopt(backend, i);
                    backend.durable_completion_members[i] = slot;
                    if (backend.durable_completion == null) backend.durable_completion = slot;
                    if (completed[i] and !manifested) {
                        if (needs_drain != null) return error.InvalidCompletionSlot;
                        needs_drain = slot;
                    }
                }
            }
            // The drain independently borrows the now-empty writer workspace.
            // No lookup temporary may survive into output construction.
            if (needs_drain) |slot| try slot.finishReplayed(backend);
            for (self.cells[0..self.cell_count], 0..) |*cell, i| if (completed[i]) {
                cell.slot.retired = true;
            };
            try backend.retireDurableCompletionCohort();
        }

        pub fn notifyApplied(self: *Self, index: usize) void {
            const cell = &self.cells[index];
            std.debug.assert(cell.phase == .prepared and cell.slot.durable);
            if (cell.publication_notified) return;
            if (cell.publication_token) |token| self.publication_owner.?.applied(self.publication_owner.?.context, token, cell.term, cell.index);
            cell.publication_notified = true;
        }

        fn restoreProgress(self: *Self, backend: *Backend, alloc: Allocator) !void {
            const value = try self.point(backend, alloc, if (self.config.namespace == .docs) "docs" else null, entry_codec.group_progress_key);
            defer value.deinit(alloc);
            self.progress = if (value.value) |bytes| try decodeProgress(self.config.identity, bytes) else null;
        }

        /// No allocation or storage reads. Only a successfully published native
        /// operation updates this cache; failed/uncertain pools never issue it.
        pub fn durableProgress(self: *const Self) !abi.Progress {
            if (self.failed or !self.restored) return error.RecoveryRequired;
            const progress = self.progress orelse return error.NotFound;
            return .{ .term = progress.term, .index = progress.index, .payload_digest = progress.digest };
        }

        pub fn durableCells(self: *const Self, out: *[max_slots]DurableCell) ![]const DurableCell {
            if (self.failed or !self.restored) return error.RecoveryRequired;
            var count: usize = 0;
            for (self.cells[0..self.cell_count]) |cell| {
                if (cell.phase != .accepted and cell.phase != .prepared) continue;
                out[count] = .{
                    .identity = .{ .term = cell.term, .index = cell.index, .digest = cell.entry.?.digest },
                    .prepared = cell.phase == .prepared,
                };
                count += 1;
            }
            return out[0..count];
        }

        /// Retained BEGIN ownership is independent of the reusable document
        /// cells. Enumerate the immutable identities without allocating or
        /// deriving authority from a mutable transaction row.
        pub fn durableControlOwners(self: *const Self, out: *[control_record.max_owners]DurableControlOwner) ![]const DurableControlOwner {
            if (self.failed or !self.restored) return error.RecoveryRequired;
            var count: usize = 0;
            for (self.control_owners, 0..) |owner, slot_index| if (owner) |active| {
                out[count] = .{ .identity = active.begin, .slot_index = slot_index };
                count += 1;
            };
            return out[0..count];
        }

        /// A complete persisted suffix can attest an exact live BEGIN. This
        /// stage never retires or restores a control owner: absent, replaced,
        /// or compacted entries still require a separate native receipt and
        /// owner-backed transition path before any capacity can be released.
        pub fn reconcileControlDurableLog(self: *const Self, log: DurableLog) !void {
            if (self.failed or !self.restored) return error.RecoveryRequired;
            if (log.commit_index > log.last_index or log.compacted_index > log.commit_index or
                (log.compacted_index == 0) != (log.compacted_term == 0) or
                log.observations.len > control_record.max_owners) return error.InvalidCompletionSlot;
            var owners: [control_record.max_owners]DurableControlOwner = undefined;
            const active = try self.durableControlOwners(&owners);
            if (active.len != log.observations.len) return error.InvalidCompletionSlot;
            var seen: [control_record.max_owners]bool = @splat(false);
            for (active) |owner| {
                const observation = for (log.observations, 0..) |candidate, i| {
                    if (!std.meta.eql(candidate.expected, owner.identity)) continue;
                    if (seen[i]) return error.InvalidCompletionSlot;
                    seen[i] = true;
                    break candidate;
                } else return error.InvalidCompletionSlot;
                if (!observation.present or owner.identity.index <= log.compacted_index or
                    owner.identity.index > log.last_index or observation.observed_term != owner.identity.term or
                    !std.mem.eql(u8, &observation.observed_digest, &owner.identity.digest))
                    return error.RecoveryRequired;
            }
            for (seen[0..log.observations.len]) |matched| if (!matched) return error.InvalidCompletionSlot;
        }

        /// Only accepted, never-applied ownership can be retired by durable log
        /// replacement. Online absence is not proof: the accepted entry may be
        /// queued beyond the just-persisted prefix. At process startup the full
        /// replayed durable log can additionally prove an absent suffix.
        /// Caller holds the DB publication lock before the backend lock.
        pub fn reconcileDurableLog(self: *Self, backend: *Backend, log: DurableLog) !void {
            if (self.failed or !self.restored) return error.RecoveryRequired;
            if (log.commit_index > log.last_index or log.compacted_index > log.commit_index or
                (log.compacted_index == 0) != (log.compacted_term == 0) or log.observations.len > max_slots)
                return error.InvalidCompletionSlot;
            if (log.mode == .startup_complete and (!self.startup_reconciliation_pending or self.ready))
                return error.InvalidCompletionSlot;
            var retire: [max_slots]bool = @splat(false);
            var observed: [max_slots]bool = @splat(false);
            var live_count: usize = 0;
            for (self.cells[0..self.cell_count], 0..) |cell, i| {
                if (cell.phase != .accepted and cell.phase != .prepared) continue;
                live_count += 1;
                const expected: completion.AcceptedIdentity = .{ .term = cell.term, .index = cell.index, .digest = cell.entry.?.digest };
                const observation = for (log.observations, 0..) |candidate, j| {
                    if (!std.meta.eql(candidate.expected, expected)) continue;
                    if (observed[j]) return error.InvalidCompletionSlot;
                    observed[j] = true;
                    break candidate;
                } else return error.InvalidCompletionSlot;
                if (observation.present) {
                    if (cell.index <= log.compacted_index or cell.index > log.last_index or observation.observed_term == 0 or
                        std.mem.allEqual(u8, &observation.observed_digest, 0)) return error.InvalidCompletionSlot;
                } else if (observation.observed_term != 0 or !std.mem.allEqual(u8, &observation.observed_digest, 0) or observation.replaced_in_this_persist)
                    return error.InvalidCompletionSlot;
                if (cell.index <= log.compacted_index) {
                    if (cell.phase != .prepared or !cell.slot.durable) return error.RecoveryRequired;
                    // The exact native descriptor + receipt were validated when
                    // this prepared cell was restored; absence alone is unused.
                    continue;
                }
                const matches = observation.present and observation.observed_term == cell.term and
                    std.mem.eql(u8, &observation.observed_digest, &cell.entry.?.digest);
                if (matches) continue;
                if (cell.phase == .prepared) return error.RecoveryRequired;
                if (!observation.present and cell.index <= log.last_index) return error.InvalidCompletionSlot;
                retire[i] = log.mode == .startup_complete or (observation.present and observation.replaced_in_this_persist);
            }
            if (live_count != log.observations.len) return error.InvalidCompletionSlot;
            for (observed[0..log.observations.len]) |used| if (!used) return error.InvalidCompletionSlot;
            // Validate the entire bounded proof before any irreversible cleanup.
            for (retire[0..self.cell_count], 0..) |remove, i| {
                if (!remove) continue;
                try self.retireUnappliedCell(backend, i);
            }
            if (log.mode == .startup_complete) self.startup_reconciliation_pending = false;
        }

        /// Explicit local proposal rejection is proof only for this exact
        /// provisional tuple. Clearing a resolution never retires its prepared
        /// transaction or releases the resources backing either outcome.
        pub fn cancelUnacceptedProposal(self: *Self, backend: *Backend, identity: completion.AcceptedIdentity) !void {
            if (self.failed or !self.restored) return error.RecoveryRequired;
            for (&self.control_owners, 0..) |*owner, i| if (owner.*) |*active| {
                if (active.pending) |pending| if (std.meta.eql(pending, identity)) {
                    self.io.storage().deleteFileAbsolute(self.control_accepted_paths[i]) catch |err| {
                        self.failed = true;
                        backend.fenceFailedBulkWal();
                        return err;
                    };
                    self.io.storage().syncParentAbsolute(self.control_accepted_paths[i]) catch |err| {
                        self.failed = true;
                        backend.fenceFailedBulkWal();
                        return err;
                    };
                    active.pending = null;
                    active.transition = null;
                    active.held.accepted_len = 0;
                    return;
                };
            };
            for (self.cells[0..self.cell_count], 0..) |*cell, i| {
                if (cell.phase != .accepted and cell.phase != .prepared) continue;
                if (cell.resolution) |resolution| if (std.meta.eql(identity, resolution.identity)) {
                    cell.resolution = null;
                    return;
                };
                if (cell.term != identity.term or cell.index != identity.index or
                    !std.mem.eql(u8, &cell.entry.?.digest, &identity.digest)) continue;
                if (cell.phase != .accepted) return error.RecoveryRequired;
                try self.retireUnappliedCell(backend, i);
                return self.retireRejectedControlBegin(backend, identity);
            }
            return error.NotFound;
        }

        /// A direct proposal rejection proves that this exact provisional
        /// BEGIN never entered the local log. Delete its document sidecar first;
        /// a failure to remove/sync the independent control guard retains the
        /// owner and fences further admission rather than refunding uncertainty.
        fn retireRejectedControlBegin(self: *Self, backend: *Backend, identity: completion.AcceptedIdentity) !void {
            for (&self.control_owners, 0..) |*owner, i| if (owner.*) |*active| {
                if (!std.meta.eql(active.begin, identity)) continue;
                const path = std.fs.path.join(backend.allocator, &.{ backend.root_dir.?, control_guard.filenames[i] }) catch |err| {
                    self.failed = true;
                    backend.fenceFailedBulkWal();
                    return err;
                };
                defer backend.allocator.free(path);
                backend.storage.?.deleteFileAbsolute(path) catch |err| {
                    self.failed = true;
                    backend.fenceFailedBulkWal();
                    return err;
                };
                backend.storage.?.syncParentAbsolute(path) catch |err| {
                    self.failed = true;
                    backend.fenceFailedBulkWal();
                    return err;
                };
                active.output_pin.release();
                active.held.destroy();
                owner.* = null;
                return;
            };
        }

        fn retireUnappliedCell(self: *Self, backend: *Backend, i: usize) !void {
            const cell = &self.cells[i];
            std.debug.assert(cell.phase == .accepted);
            self.io.storage().deleteFileAbsolute(self.accepted_paths[i]) catch |err| {
                self.failed = true;
                backend.fenceFailedBulkWal();
                return err;
            };
            self.io.storage().syncParentAbsolute(self.accepted_paths[i]) catch |err| {
                self.failed = true;
                backend.fenceFailedBulkWal();
                return err;
            };
            if (cell.publication_token) |token| self.publication_owner.?.cancel(self.publication_owner.?.context, token);
            cell.publication_token = null;
            const reservation = cell.publication.?;
            reservation.allocator().free(cell.accepted_wire.?);
            cell.accepted_wire = null;
            cell.entry.?.deinit();
            cell.entry = null;
            cell.publication = null;
            reservation.finish();
            cell.phase = .spent;
            self.ready = false;
        }

        /// Exact ownership, never an index-only inference. A retired latest
        /// operation remains owned through its durable group receipt. Older
        /// operations must be skipped using the verified applied checkpoint;
        /// falling back to ordinary execution would replay their effects.
        pub fn ownsAccepted(self: *const Self, term: u64, index: u64, payload: []const u8) !bool {
            if (self.failed or !self.restored) return error.RecoveryRequired;
            const digest = entry_codec.protocol.payloadDigest(payload);
            const identity: completion.AcceptedIdentity = .{ .term = term, .index = index, .digest = digest };
            for (self.control_owners) |owner| if (owner) |active| if (active.pending) |pending| {
                if (pending.index != index) continue;
                if (!std.meta.eql(pending, identity)) return error.InvalidCompletionSlot;
                return true;
            };
            for (self.cells[0..self.cell_count]) |cell| {
                if (cell.phase != .accepted and cell.phase != .prepared) continue;
                if (cell.term == term and cell.index == index) {
                    if (!std.mem.eql(u8, &cell.entry.?.digest, &digest)) return error.InvalidCompletionSlot;
                    return true;
                }
                if (cell.resolution) |resolution| if (resolution.identity.index == index) {
                    if (!std.meta.eql(resolution.identity, identity)) return error.InvalidCompletionSlot;
                    return true;
                };
            }
            if (self.progress) |progress| {
                if (progress.index == index) {
                    if (!std.meta.eql(progress, identity)) return error.InvalidCompletionSlot;
                    return true;
                }
                if (index < progress.index) return error.RecoveryRequired;
            }
            return false;
        }

        /// Called by the native consensus classifier for an existing prepared
        /// transaction at its actual applied predecessor. No new memory/FD
        /// admission occurs: either outcome is already owned by this cell.
        pub fn reserveResolution(self: *Self, id: [16]u8, identity: completion.AcceptedIdentity, commit: bool, applied: u64) !void {
            if (self.failed or !self.restored or self.hasAcceptedDebt()) return error.RecoveryRequired;
            if (identity.term == 0 or identity.index == 0 or applied != identity.index - 1 or std.mem.allEqual(u8, &identity.digest, 0)) return error.InvalidCompletionSlot;
            for (self.cells[0..self.cell_count]) |*cell| {
                if (cell.phase != .prepared or cell.slot.retired or !cell.slot.durable or
                    !std.mem.eql(u8, &cell.entry.?.entry.txn_id, &id)) continue;
                if (cell.resolution) |existing| {
                    if (existing.commit == commit and std.meta.eql(existing.identity, identity)) return;
                    return error.CompletionReservationBusy;
                }
                cell.resolution = .{ .identity = identity, .commit = commit };
                return;
            }
            return error.CompletionNotPrepared;
        }

        fn prepareProgress(raw: *anyopaque, slot: *Slot, identity: completion.AcceptedIdentity, commit: ?bool) ![112]u8 {
            const self: *Self = @ptrCast(@alignCast(raw));
            if (self.failed or !self.restored) return error.RecoveryRequired;
            if (identity.term == 0 or identity.index == 0 or std.mem.allEqual(u8, &identity.digest, 0)) return error.InvalidCompletionSlot;
            if (self.progress) |old| if (identity.index <= old.index or identity.term < old.term) return error.InvalidCompletionSlot;
            for (self.cells[0..self.cell_count]) |*cell| {
                if (cell.slot != slot or cell.phase != .prepared) continue;
                if (commit) |outcome| {
                    const accepted = cell.resolution orelse return error.CompletionNotPrepared;
                    if (accepted.commit != outcome or !std.meta.eql(accepted.identity, identity)) return error.InvalidCompletionSlot;
                } else if (cell.term != identity.term or cell.index != identity.index or !std.mem.eql(u8, &cell.entry.?.digest, &identity.digest)) return error.InvalidCompletionSlot;
                return encodeProgress(self.config.identity, identity);
            }
            return error.InvalidCompletionSlot;
        }

        fn publishProgress(raw: *anyopaque, identity: completion.AcceptedIdentity) void {
            const self: *Self = @ptrCast(@alignCast(raw));
            if (self.progress) |old| std.debug.assert(identity.index > old.index);
            self.progress = identity;
        }

        /// DB quiesces its apply/workers before detaching its stable snapshot.
        /// This does not cancel any accepted log entry or remove durable debt.
        pub fn releasePublicationOwnerAfterQuiesce(self: *Self) void {
            self.ready = false;
            const owner = self.publication_owner orelse return;
            for (self.cells[0..self.cell_count]) |*cell| {
                if (cell.publication_token) |token| owner.release_after_quiesce(owner.context, token);
                cell.publication_token = null;
            }
            self.publication_owner = null;
        }

        /// Restored DB backlogs and owner targets are pinned before consensus
        /// admission. Failure keeps readiness false and unwinds newly made tokens.
        pub fn attachPublicationOwner(self: *Self, owner: PublicationOwner) !void {
            if (self.publication_owner != null or self.ready) return error.CompletionReservationBusy;
            self.publication_owner = owner;
            errdefer self.releasePublicationOwnerAfterQuiesce();
            for (self.cells[0..self.cell_count]) |*cell| {
                if (cell.phase != .accepted and cell.phase != .prepared) continue;
                if (cell.phase == .prepared and cell.slot.retired) continue;
                cell.publication_token = try owner.prepare(owner.context, cell.publication.?.allocator(), &cell.entry.?);
                if (cell.phase == .prepared and cell.slot.durable) {
                    owner.applied(owner.context, cell.publication_token.?, cell.term, cell.index);
                    cell.publication_notified = true;
                }
            }
        }

        fn releaseCell(raw: *anyopaque, slot: *Slot) void {
            const self: *Self = @ptrCast(@alignCast(raw));
            for (self.cells[0..self.cell_count]) |*cell| {
                if (cell.slot != slot) continue;
                std.debug.assert(cell.phase == .prepared);
                if (cell.publication_token) |token| {
                    if (slot.retired) self.publication_owner.?.native_terminal(self.publication_owner.?.context, token) else self.publication_owner.?.release_after_quiesce(self.publication_owner.?.context, token);
                    cell.publication_token = null;
                }
                // Slot.finish already released the reservation owner's handle;
                // these children keep its allocator alive through their frees.
                const alloc = cell.publication.?.allocator();
                alloc.free(cell.accepted_wire.?);
                cell.accepted_wire = null;
                cell.entry.?.deinit();
                cell.entry = null;
                cell.publication = null;
                cell.phase = .spent;
                self.ready = false;
                return;
            }
            unreachable;
        }

        pub fn restoreWalCredits(raw: *anyopaque, backend: *Backend) !void {
            const self: *Self = @ptrCast(@alignCast(raw));
            for (self.cells[0..self.cell_count]) |*cell| {
                const needed = completion.limits.wal_bytes -| cell.slot.wal_credit;
                if (needed == 0) continue;
                if (needed > backend.tracked_wal_retention_bytes) return error.ResourceAccountingMismatch;
                try self.wal_pin.manager.transferUsage(.lsm_wal_retention, &backend.tracked_wal_retention_bytes, backend.tracked_wal_retention_bytes - needed, &cell.slot.wal_credit, cell.slot.wal_credit + needed);
            }
            self.wal_bytes_start = backend.write_stats.wal_append_bytes;
            self.wal_entries_start = backend.write_stats.wal_append_entries;
            self.wal_records_start = backend.write_stats.wal_append_records;
            self.replayed_wal_bytes = 0;
            self.replayed_wal_entries = 0;
            self.replayed_wal_records = 0;
        }
    };
}

test "workload admission physical completion control guards fence native restart before ordinary replay" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const alloc = std.testing.allocator;
    const config: Config = .{ .identity = .{ .capacity = 4, .group_id = 23, .node_id = 7, .incarnation = @splat(11), .policy_digest = @splat(12), .generation = 19 }, .schema_catalog_digest = @splat(13), .namespace = .root };
    for ([_][]const u8{ control_guard.filenames[0], control_guard.pending_filenames[3] }) |filename| {
        var fd_pool = storage_io.NativeStoragePool.initWithCapacityForTest(alloc, 32);
        defer fd_pool.deinit();
        var manager = resources.ResourceManager.init(.{ .identity_allocator = alloc, .memory_budget = .{ .hard_limit_bytes = 256 * 1024 * 1024 } });
        defer manager.deinit(alloc);
        var path_buffer: [256]u8 = undefined;
        const root = repository.tmpPath(&path_buffer, "native-control-guard-restart");
        defer repository.cleanupTmp(root);
        const options: @import("../lsm_backend.zig").Options = .{ .resource_manager = &manager, .native_storage_pool = &fd_pool };
        const guard_path = try std.fs.path.join(alloc, &.{ std.mem.span(root), filename });
        defer alloc.free(guard_path);
        {
            var backend: Backend = undefined;
            try backend.openInto(alloc, std.mem.span(root), options);
            defer backend.abandonAfterCrash();
            try backend.persistManifest();
            try std.testing.expect(!try control_guard.hasAny(backend.storage.?, alloc, std.mem.span(root)));
            try manifest_set.replace(alloc, backend.storage.?, guard_path, "interrupted-control-owner");
            try std.testing.expect(try control_guard.hasAny(backend.storage.?, alloc, std.mem.span(root)));
            const authority: @import("completion_control_record.zig").Authority = .{
                .group_id = config.identity.group_id,
                .incarnation = config.identity.incarnation,
                .policy_digest = config.identity.policy_digest,
                .schema_catalog_digest = config.schema_catalog_digest,
                .generation = config.identity.generation,
            };
            if (std.mem.eql(u8, filename, control_guard.pending_filenames[3])) {
                try std.testing.expectError(error.CompletionRecoveryCapacityRequired, control_guard.load(alloc, backend.storage.?, std.mem.span(root), 3, authority));
            } else {
                try std.testing.expectError(error.InvalidCompletionSlot, control_guard.load(alloc, backend.storage.?, std.mem.span(root), 0, authority));
            }
        }
        {
            var missing: Backend = undefined;
            try std.testing.expectError(error.CompletionRecoveryCapacityRequired, missing.openInto(alloc, std.mem.span(root), options));
        }
        {
            var configured: Backend = undefined;
            var restoring = options;
            restoring.completion_pool_config = config;
            try std.testing.expectError(
                if (std.mem.eql(u8, filename, control_guard.pending_filenames[3])) error.CompletionRecoveryCapacityRequired else error.InvalidCompletionSlot,
                configured.openInto(alloc, std.mem.span(root), restoring),
            );
        }
        try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    }
}

test "workload admission physical completion orphaned accepted control transition fences restart" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const alloc = std.testing.allocator;
    var fd_pool = storage_io.NativeStoragePool.initWithCapacityForTest(alloc, 32);
    defer fd_pool.deinit();
    var manager = resources.ResourceManager.init(.{ .identity_allocator = alloc, .memory_budget = .{ .hard_limit_bytes = 256 * 1024 * 1024 } });
    defer manager.deinit(alloc);
    var path_buffer: [256]u8 = undefined;
    const root = repository.tmpPath(&path_buffer, "native-control-accepted-restart");
    defer repository.cleanupTmp(root);
    const options: @import("../lsm_backend.zig").Options = .{ .resource_manager = &manager, .native_storage_pool = &fd_pool };
    const path = try std.fs.path.join(alloc, &.{ std.mem.span(root), control_accepted.filenames[0] });
    defer alloc.free(path);
    {
        var backend: Backend = undefined;
        try backend.openInto(alloc, std.mem.span(root), options);
        defer backend.abandonAfterCrash();
        try backend.persistManifest();
        // Model a crash after the accepted transition is durable but before
        // either WAL publication or restoration of the retained BEGIN guard.
        try manifest_set.replace(alloc, backend.storage.?, path, "interrupted-control-transition");
        try std.testing.expect(try control_accepted.hasAny(backend.storage.?, alloc, std.mem.span(root)));
    }
    var reopened: Backend = undefined;
    try std.testing.expectError(error.CompletionRecoveryCapacityRequired, reopened.openInto(alloc, std.mem.span(root), options));
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
}

test "workload admission completion accepted guard binds durable owner and rejects corrupt framing" {
    const identity: abi.Identity = .{ .capacity = 4, .group_id = 23, .node_id = 7, .incarnation = @splat(11), .policy_digest = @splat(12), .generation = 19 };
    const input: Accepted = .{ .cell = 2, .term = 7, .index = 91, .identity = identity, .cohort = .{ .index = 2, .base_run_id = 99, .initial_runs = 64, .cohort_id = @splat(42) }, .envelope = "binary\x00envelope" };
    const bytes = try encodeAccepted(std.testing.allocator, input);
    defer std.testing.allocator.free(bytes);
    const decoded = try decodeAccepted(bytes);
    try std.testing.expectEqualDeep(input.identity, decoded.identity);
    try std.testing.expectEqualDeep(input.cohort, decoded.cohort);
    try std.testing.expectEqualStrings(input.envelope, decoded.envelope);
    bytes[32] ^= 1;
    try std.testing.expectError(error.CompletionSlotChecksumMismatch, decodeAccepted(bytes));
    bytes[32] ^= 1;
    bytes[bytes.len - 1] ^= 1;
    try std.testing.expectError(error.CompletionSlotChecksumMismatch, decodeAccepted(bytes));
    try std.testing.expectError(error.InvalidCompletionSlot, decodeAccepted(bytes[0 .. bytes.len - 1]));
}

test "workload admission physical completion progress binds incarnation policy and exact payload" {
    const identity: abi.Identity = .{ .capacity = 4, .group_id = 23, .node_id = 7, .incarnation = @splat(11), .policy_digest = @splat(12), .generation = 19 };
    const progress: completion.AcceptedIdentity = .{ .term = 5, .index = 113, .digest = @splat(17) };
    const bytes = encodeProgress(identity, progress);
    try std.testing.expectEqualDeep(progress, try decodeProgress(identity, &bytes));
    // The replicated record is shared by applying members, not node-local.
    var other = identity;
    other.node_id = 21;
    try std.testing.expectEqualDeep(progress, try decodeProgress(other, &bytes));
    other.incarnation[0] ^= 1;
    try std.testing.expectError(error.InvalidCompletionSlot, decodeProgress(other, &bytes));
    other = identity;
    other.policy_digest[0] ^= 1;
    try std.testing.expectError(error.InvalidCompletionSlot, decodeProgress(other, &bytes));
    other = identity;
    other.generation += 1;
    try std.testing.expectError(error.InvalidCompletionSlot, decodeProgress(other, &bytes));
    try std.testing.expectError(error.InvalidCompletionSlot, decodeProgress(identity, bytes[0..48]));
}

test "workload admission physical completion pool accepts through native prepaid baseline IO and fences read dependencies" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const runtime = @import("runtime.zig");
    const alloc = std.testing.allocator;
    var failing = std.testing.FailingAllocator.init(alloc, .{});
    var fd_pool = storage_io.NativeStoragePool.initWithCapacityForTest(alloc, 32);
    defer fd_pool.deinit();
    var manager = resources.ResourceManager.init(.{ .identity_allocator = alloc, .memory_budget = .{ .hard_limit_bytes = 256 * 1024 * 1024 } });
    defer manager.deinit(alloc);
    var path_buffer: [256]u8 = undefined;
    const path = repository.tmpPath(&path_buffer, "native-completion-pool");
    defer repository.cleanupTmp(path);
    var backend: Backend = undefined;
    try backend.openInto(failing.allocator(), std.mem.span(path), .{ .resource_manager = &manager, .native_storage_pool = &fd_pool, .flush_threshold = 10000 });
    defer backend.abandonAfterCrash();
    {
        var batch = try backend.beginWrite();
        errdefer batch.abort();
        try batch.put(.{}, "read-only", "original");
        try batch.commit();
    }
    try backend.checkpointWalAfterDurableBoundary();
    const identity: abi.Identity = .{ .capacity = 4, .group_id = 23, .node_id = 7, .incarnation = @splat(11), .policy_digest = @splat(12), .generation = 19 };
    const locked = runtime.lockBackend(Backend, &backend);
    defer runtime.unlockBackend(Backend, &backend, locked);
    try backend.installCompletionPoolLocked(.{ .identity = identity, .schema_catalog_digest = @splat(13), .namespace = .root });
    const pool = backend.completion_pool.?;
    try pool.qualifyFresh(&backend);
    const id: [16]u8 = @splat(51);
    const descriptor = try slot_codec.encode(alloc, .{ .txn_id = id, .intent_revision = 1, .limits = completion.limits, .profile_fence = "replicated-profile", .commit = &.{.{ .kind = .put, .key = "row", .value = "complete" }}, .abort = &.{} }, .{});
    defer alloc.free(descriptor);
    var hash = entry_codec.BaselineHasher.init();
    try hash.add("intent", null);
    try hash.add("read-only", "original");
    try hash.add("row", null);
    const envelope = try entry_codec.encode(alloc, .{ .group_id = identity.group_id, .group_incarnation = identity.incarnation, .policy_digest = identity.policy_digest, .schema_catalog_digest = @splat(13), .txn_id = id, .original_input_digest = @splat(14), .baseline_digest = hash.finish(), .previous_term = 3, .previous_index = 8, .baseline_keys = &.{ "intent", "read-only", "row" }, .descriptor = descriptor, .prepare_operations = &.{.{ .kind = .put, .key = "intent", .value = "prepared" }} });
    defer alloc.free(envelope);
    pool.config.shape.max_record_bytes = 32;
    try std.testing.expectError(error.UnsupportedCompletionProfile, pool.accept(&backend, 3, 9, 3, 8, envelope));
    try std.testing.expectEqual(Pool(Backend).Phase.free, pool.cells[0].phase);
    try std.testing.expectError(error.FileNotFound, pool.io.storage().fileSize(pool.accepted_paths[0]));
    pool.config.shape.max_record_bytes = (Shape{}).max_record_bytes;
    const required_counters = try (try pool.remainingAppendBudget()).plus(completion.foreground_append_budget);
    inline for (.{ .{ "wal_append_bytes", "bytes" }, .{ "wal_append_entries", "entries" }, .{ "wal_append_records", "records" } }) |fields| {
        const before = @field(backend.write_stats, fields[0]);
        @field(backend.write_stats, fields[0]) = std.math.maxInt(u64) - @field(required_counters, fields[1]) + 1;
        try std.testing.expectError(error.UnsupportedCompletionProfile, pool.accept(&backend, 3, 9, 3, 8, envelope));
        try std.testing.expectEqual(Pool(Backend).Phase.free, pool.cells[0].phase);
        try std.testing.expectError(error.FileNotFound, pool.io.storage().fileSize(pool.accepted_paths[0]));
        @field(backend.write_stats, fields[0]) = before;
    }
    const saved_sequence = backend.manifest_journal.sequence;
    backend.manifest_journal.sequence = std.math.maxInt(u64) - 5;
    try std.testing.expectError(error.UnsupportedCompletionProfile, pool.accept(&backend, 3, 9, 3, 8, envelope));
    backend.manifest_journal.sequence = saved_sequence;
    try std.testing.expectEqual(Pool(Backend).Phase.free, pool.cells[0].phase);
    try std.testing.expectError(error.FileNotFound, pool.io.storage().fileSize(pool.accepted_paths[0]));
    const saved_cost = pool.capacity_cost;
    pool.capacity_cost = try (try capacity.Cost.record(0, 8, 1)).repeated(1_000_000);
    try std.testing.expectError(error.UnsupportedCompletionProfile, pool.accept(&backend, 3, 9, 3, 8, envelope));
    pool.capacity_cost = saved_cost;
    try std.testing.expectEqual(Pool(Backend).Phase.free, pool.cells[0].phase);
    try std.testing.expectError(error.FileNotFound, pool.io.storage().fileSize(pool.accepted_paths[0]));
    Backend.rejectNewRunSnapshotRefsForTest(true);
    defer Backend.rejectNewRunSnapshotRefsForTest(false);
    var unreserved_run: repository.Run = undefined;
    unreserved_run.path = @constCast("unreserved-completion-output");
    try std.testing.expectError(error.OutOfMemory, backend.retainRunSnapshotRef(&unreserved_run));
    failing.fail_index = failing.alloc_index;
    failing.resize_fail_index = failing.resize_index;
    fd_pool.fd_cache.capacity = 1;
    manager.memory.budget.hard_limit_bytes = 1;
    try std.testing.expectError(error.CompletionReservationBusy, pool.accept(&backend, 3, 9, 3, 7, envelope));
    try std.testing.expectEqual(@as(usize, 0), try pool.accept(&backend, 3, 9, 3, 8, envelope));
    try std.testing.expectEqual(@as(usize, 0), try pool.accept(&backend, 3, 9, 3, 8, envelope));
    var incoming: @import("state.zig").ActiveMemTable = .{};
    defer incoming.deinit(alloc);
    try incoming.upsert(alloc, .{}, "read-only", "changed", false);
    try std.testing.expectError(error.PreparedCompletionActive, pool.checkOrdinary(&backend, &incoming));
    try std.testing.expect(!failing.has_induced_failure);
    const disk_size = try pool.io.storage().fileSize(pool.accepted_paths[0]);
    try std.testing.expect(disk_size > accepted_header_bytes);
    const slot = try pool.adopt(&backend, 0);
    backend.durable_completion = slot;
    backend.durable_completion_members[0] = slot;
    try slot.applyCanonicalPrepare(&backend, pool.cells[0].entry.?.entry.prepare_operations);
    pool.notifyApplied(0);
    try std.testing.expectEqual(@as(u64, 9), (try pool.durableProgress()).index);
    const receipt_key = entry_codec.receiptKey(id);
    const prepared_receipt = try pool.point(&backend, alloc, null, &receipt_key);
    defer prepared_receipt.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 48), prepared_receipt.value.?.len);
    try std.testing.expectEqual(@as(u64, 9), std.mem.readInt(u64, prepared_receipt.value.?[8..16], .little));
    try std.testing.expectError(error.InvalidCompletionSlot, slot.complete(&backend, true, .{
        .commit_timestamp = 200,
        .replay_sequence = 0,
        .shared_ledger_count = 0,
        .shared_ledger_bytes = 0,
        .raft_term = 3,
        .raft_index = 10,
    }));
    try std.testing.expect(!slot.attempted and !backend.manifest_recovery_required);
    const resolution_payload = "exact original resolution Raft entry";
    const resolution_digest = entry_codec.protocol.payloadDigest(resolution_payload);
    try std.testing.expect(try pool.ownsAccepted(3, 9, envelope));
    try pool.reserveResolution(id, .{ .term = 3, .index = 10, .digest = resolution_digest }, true, 9);
    try std.testing.expect(try pool.ownsAccepted(3, 10, resolution_payload));
    try std.testing.expectError(error.InvalidCompletionSlot, pool.ownsAccepted(3, 10, "other payload"));
    try pool.cancelUnacceptedProposal(&backend, .{ .term = 3, .index = 10, .digest = resolution_digest });
    try std.testing.expectEqual(Pool(Backend).Phase.prepared, pool.cells[0].phase);
    try std.testing.expect(!(try pool.ownsAccepted(3, 10, resolution_payload)));
    try pool.reserveResolution(id, .{ .term = 3, .index = 10, .digest = resolution_digest }, true, 9);
    try slot.complete(&backend, true, .{ .canonical_payload_digest = resolution_digest, .commit_timestamp = 200, .replay_sequence = 0, .shared_ledger_count = 0, .shared_ledger_bytes = 0, .raft_term = 3, .raft_index = 10 });
    const completed_row = try pool.point(&backend, alloc, null, "row");
    defer completed_row.deinit(alloc);
    try std.testing.expectEqualStrings("complete", completed_row.value.?);
    const completed_receipt = try pool.point(&backend, alloc, null, &receipt_key);
    defer completed_receipt.deinit(alloc);
    try std.testing.expect(completed_receipt.value == null);
    const durable_progress = try pool.durableProgress();
    try std.testing.expectEqual(@as(u64, 10), durable_progress.index);
    try std.testing.expectEqual(resolution_digest, durable_progress.payload_digest);
    slot.retired = true;
    try backend.retireDurableCompletionCohort();
    try std.testing.expect(try pool.ownsAccepted(3, 10, resolution_payload));
    try std.testing.expectError(error.RecoveryRequired, pool.ownsAccepted(3, 9, envelope));
    try std.testing.expectError(error.InvalidCompletionSlot, pool.ownsAccepted(4, 10, resolution_payload));
    try std.testing.expect(!(try pool.ownsAccepted(3, 11, resolution_payload)));
    try std.testing.expectError(error.FileNotFound, pool.io.storage().fileSize(pool.accepted_paths[0]));
    const old_base = pool.cohort.base_run_id;
    try pool.maintainLocked(&backend);
    try std.testing.expect(!pool.ready);
    try std.testing.expect(pool.cohort.base_run_id > old_base);
    try std.testing.expectEqual(Pool(Backend).Phase.free, pool.cells[0].phase);
    try pool.qualifyFresh(&backend);
    try std.testing.expect(pool.ready);
    for (0..3) |_| {
        try pool.maintainLocked(&backend);
        try pool.qualifyFresh(&backend);
    }
    const maintained = try pool.point(&backend, alloc, null, "row");
    defer maintained.deinit(alloc);
    try std.testing.expectEqualStrings("complete", maintained.value.?);
    try std.testing.expectEqual(@as(u64, 10), (try pool.durableProgress()).index);
    try std.testing.expect(!failing.has_induced_failure);
}

test "workload admission completion generations carry four maximum point plans with a retained mutable reader" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const runtime = @import("runtime.zig");
    const alloc = std.testing.allocator;
    var failing = std.testing.FailingAllocator.init(alloc, .{});
    var fd_pool = storage_io.NativeStoragePool.initWithCapacityForTest(alloc, 32);
    defer fd_pool.deinit();
    var manager = resources.ResourceManager.init(.{ .identity_allocator = alloc, .memory_budget = .{ .hard_limit_bytes = 256 * 1024 * 1024 } });
    defer manager.deinit(alloc);
    var path_buffer: [256]u8 = undefined;
    const path = repository.tmpPath(&path_buffer, "completion-four-max-plans");
    defer repository.cleanupTmp(path);
    var backend: Backend = undefined;
    try backend.openInto(failing.allocator(), std.mem.span(path), .{ .resource_manager = &manager, .native_storage_pool = &fd_pool, .flush_threshold = 10000 });
    defer backend.abandonAfterCrash();
    {
        var batch = try backend.beginWrite();
        errdefer batch.abort();
        try batch.put(.{}, "baseline", "preserved");
        try batch.commit();
    }
    try backend.checkpointWalAfterDurableBoundary();
    const identity: abi.Identity = .{ .capacity = 4, .group_id = 23, .node_id = 7, .incarnation = @splat(11), .policy_digest = @splat(12), .generation = 19 };
    const locked = runtime.lockBackend(Backend, &backend);
    defer runtime.unlockBackend(Backend, &backend, locked);
    // Exactly the full cohort plus foreground counter allowance remains near
    // u64 exhaustion. Every accepted prepare and commit/abort must still finish.
    const counter_budget = try (try (try completion.prepare_append_budget.plus(completion.outcome_append_budget)).repeated(max_slots)).plus(completion.foreground_append_budget);
    backend.write_stats.wal_append_bytes = std.math.maxInt(u64) - counter_budget.bytes;
    backend.write_stats.wal_append_entries = std.math.maxInt(u64) - counter_budget.entries;
    backend.write_stats.wal_append_records = std.math.maxInt(u64) - counter_budget.records;
    try backend.installCompletionPoolLocked(.{ .identity = identity, .schema_catalog_digest = @splat(13), .namespace = .root });
    const pool = backend.completion_pool.?;
    try pool.qualifyFresh(&backend);
    var envelopes: [4]?[]u8 = @splat(null);
    defer for (envelopes) |wire| if (wire) |bytes| alloc.free(bytes);
    var ids: [4][16]u8 = undefined;
    var key_storage: [512][8]u8 = undefined;
    var keys: [512][]const u8 = undefined;
    var prepare_ops: [256]slot_codec.Operation = undefined;
    var commit_ops: [256]slot_codec.Operation = undefined;
    const value: [512]u8 = @splat(0x9c);
    for (&envelopes, 0..) |*envelope, i| {
        ids[i] = @splat(@intCast(40 + i));
        var hash = entry_codec.BaselineHasher.init();
        for (&key_storage, &keys, 0..) |*key, *slice, j| {
            std.mem.writeInt(u64, key, i * 1024 + j, .big);
            slice.* = key;
            try hash.add(key, null);
            if (j < 256) prepare_ops[j] = .{ .kind = .put, .key = key, .value = &value } else commit_ops[j - 256] = .{ .kind = .put, .key = key, .value = &value };
        }
        const descriptor = try slot_codec.encode(alloc, .{ .txn_id = ids[i], .intent_revision = 1, .limits = completion.limits, .profile_fence = "replicated-profile", .commit = &commit_ops, .abort = &.{} }, .{});
        defer alloc.free(descriptor);
        envelope.* = try entry_codec.encode(alloc, .{
            .group_id = identity.group_id,
            .group_incarnation = identity.incarnation,
            .policy_digest = identity.policy_digest,
            .schema_catalog_digest = @splat(13),
            .txn_id = ids[i],
            .original_input_digest = @splat(14),
            .baseline_digest = hash.finish(),
            .previous_term = if (i == 0) 0 else 3,
            .previous_index = i,
            .baseline_keys = &keys,
            .descriptor = descriptor,
            .prepare_operations = &prepare_ops,
        });
    }
    var future_cost: capacity.Cost = .{};
    for (envelopes) |wire| {
        var decoded = try entry_codec.decode(alloc, wire.?);
        defer decoded.deinit();
        future_cost = try future_cost.plus(try entryCapacity(&decoded));
    }
    const operation_bound = try completion.operationWorkspaceRequirement(future_cost, .{ .max_output_file_bytes = completion.limits.flush_bytes });
    const exact_workspace = try domains.RecyclingScratch.create(failing.allocator(), &manager, operation_bound.total);
    try pool.compiler.scratch.destroy();
    pool.compiler.scratch = exact_workspace;
    // Existing replay/readers may consume almost the entire independent domain.
    // New accepted operations must use only the separately certified workspace.
    const retained_scratch = try pool.scratch.allocator().alloc(u8, completion.scratch_bytes - 1024);
    defer pool.scratch.allocator().free(retained_scratch);
    failing.fail_index = failing.alloc_index;
    failing.resize_fail_index = failing.resize_index;
    fd_pool.fd_cache.capacity = 1;
    manager.memory.budget.hard_limit_bytes = 1;
    Backend.rejectNewRunSnapshotRefsForTest(true);
    defer Backend.rejectNewRunSnapshotRefsForTest(false);
    var reader: ?@import("state.zig").State = null;
    defer if (reader) |*snapshot| snapshot.deinit(alloc);
    for (envelopes, 0..) |wire, i| {
        try std.testing.expectEqual(i, try pool.accept(&backend, 3, i + 1, if (i == 0) 0 else 3, i, wire.?));
        const slot = try pool.adopt(&backend, i);
        backend.durable_completion_members[i] = slot;
        if (backend.durable_completion == null) backend.durable_completion = slot;
        try slot.applyCanonicalPrepare(&backend, pool.cells[i].entry.?.entry.prepare_operations);
        pool.notifyApplied(i);
        if (i == 1) reader = try backend.mutable.snapshot(alloc);
    }
    // Ordinary WAL admission cannot consume the mandatory counter reserve.
    var ordinary: @import("state.zig").ActiveMemTable = .{};
    defer ordinary.deinit(alloc);
    try ordinary.upsert(alloc, .{}, "ordinary-spare", "value", false);
    const remaining_counters = try pool.remainingAppendBudget();
    inline for (.{ .{ "wal_append_bytes", "bytes" }, .{ "wal_append_entries", "entries" }, .{ "wal_append_records", "records" } }) |fields| {
        const before = @field(backend.write_stats, fields[0]);
        @field(backend.write_stats, fields[0]) = std.math.maxInt(u64) - @field(remaining_counters, fields[1]);
        const records_before = backend.write_stats.wal_append_records;
        try std.testing.expectError(error.CompletionForegroundCapacityExceeded, backend.appendWalForMutable(&ordinary));
        try std.testing.expectEqual(records_before, backend.write_stats.wal_append_records);
        @field(backend.write_stats, fields[0]) = before;
    }
    // Normal admission epochs may be exhausted after acceptance; all owned
    // outcomes and mandatory checkpoint maintenance must still make progress.
    pool.compiler.generation = std.math.maxInt(u64);
    try std.testing.expectError(error.CompletionReservationBusy, pool.compiler.tryBorrow());
    for (ids, 0..) |id, i| {
        const progress = completion.AcceptedIdentity{ .term = 3, .index = 5 + i, .digest = @splat(@intCast(70 + i)) };
        const commit = i % 2 == 0;
        try pool.reserveResolution(id, progress, commit, progress.index - 1);
        const slot = pool.cells[i].slot;
        try slot.complete(&backend, commit, .{ .commit_timestamp = 200, .replay_sequence = 1, .shared_ledger_count = 0, .shared_ledger_bytes = 0, .raft_term = 3, .raft_index = progress.index, .canonical_payload_digest = progress.digest });
        slot.retired = true;
        // Checkpoint renewed foreground baselines, not monotonic counter room.
        try std.testing.expectEqual(backend.write_stats.wal_append_bytes, pool.wal_bytes_start);
        try std.testing.expectEqual(backend.write_stats.wal_append_entries, pool.wal_entries_start);
        try std.testing.expectEqual(backend.write_stats.wal_append_records, pool.wal_records_start);
        const after_checkpoint = try pool.remainingAppendBudget();
        inline for (.{ .{ "wal_append_bytes", "bytes" }, .{ "wal_append_entries", "entries" }, .{ "wal_append_records", "records" } }) |fields| {
            const before = @field(backend.write_stats, fields[0]);
            @field(backend.write_stats, fields[0]) = std.math.maxInt(u64) - @field(after_checkpoint, fields[1]);
            const records_before = backend.write_stats.wal_append_records;
            try std.testing.expectError(error.CompletionForegroundCapacityExceeded, backend.appendWalForMutable(&ordinary));
            try std.testing.expectEqual(records_before, backend.write_stats.wal_append_records);
            @field(backend.write_stats, fields[0]) = before;
        }
    }
    try backend.retireDurableCompletionCohort();
    try pool.maintainLocked(&backend);
    try std.testing.expect(backend.write_stats.wal_append_bytes > std.math.maxInt(u64) - counter_budget.bytes);
    try std.testing.expect(backend.write_stats.wal_append_entries > std.math.maxInt(u64) - counter_budget.entries);
    try std.testing.expect(backend.write_stats.wal_append_records > std.math.maxInt(u64) - counter_budget.records);
    try std.testing.expectError(error.UnsupportedCompletionProfile, pool.qualifyFresh(&backend));
    try std.testing.expectError(error.CompletionReservationBusy, pool.compiler.tryBorrow());
    try std.testing.expect(!pool.ready and !pool.failed);
    try std.testing.expect(!backend.hasDurableCompletions());
    for (0..4) |i| {
        var key: [8]u8 = undefined;
        std.mem.writeInt(u64, &key, i * 1024 + 256, .big);
        const found = try pool.point(&backend, alloc, null, &key);
        defer found.deinit(alloc);
        if (i % 2 == 0) try std.testing.expectEqualSlices(u8, &value, found.value.?) else try std.testing.expect(found.value == null);
    }
    const first = [_]u8{0} ** 8;
    try std.testing.expect(reader.?.findIndex(.{}, &first) != null);
    try std.testing.expect(!failing.has_induced_failure);
}

test "workload admission physical completion pool restores accepted and prepared debt before native admission" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const runtime = @import("runtime.zig");
    const alloc = std.testing.allocator;
    for ([_]bool{ false, true }) |prepared| {
        var fd_pool = storage_io.NativeStoragePool.initWithCapacityForTest(alloc, 32);
        defer fd_pool.deinit();
        var manager = resources.ResourceManager.init(.{ .identity_allocator = alloc, .memory_budget = .{ .hard_limit_bytes = 256 * 1024 * 1024 } });
        defer manager.deinit(alloc);
        var path_buffer: [256]u8 = undefined;
        const path = repository.tmpPath(&path_buffer, "native-pool-restore");
        defer repository.cleanupTmp(path);
        const config: Config = .{ .identity = .{ .capacity = 4, .group_id = 23, .node_id = 7, .incarnation = @splat(11), .policy_digest = @splat(12), .generation = 19 }, .schema_catalog_digest = @splat(13), .namespace = .root };
        const options: @import("../lsm_backend.zig").Options = .{ .resource_manager = &manager, .native_storage_pool = &fd_pool, .flush_threshold = 10000 };
        const id: [16]u8 = @splat(51);
        const descriptor = try slot_codec.encode(alloc, .{ .txn_id = id, .intent_revision = 1, .limits = completion.limits, .profile_fence = "replicated-profile", .commit = &.{.{ .kind = .put, .key = "row", .value = "complete" }}, .abort = &.{} }, .{});
        defer alloc.free(descriptor);
        var hash = entry_codec.BaselineHasher.init();
        try hash.add("intent", null);
        try hash.add("row", null);
        const envelope = try entry_codec.encode(alloc, .{
            .group_id = config.identity.group_id,
            .group_incarnation = config.identity.incarnation,
            .policy_digest = config.identity.policy_digest,
            .schema_catalog_digest = config.schema_catalog_digest,
            .txn_id = id,
            .original_input_digest = @splat(14),
            .baseline_digest = hash.finish(),
            .baseline_keys = &.{ "intent", "row" },
            .descriptor = descriptor,
            .prepare_operations = &.{.{ .kind = .put, .key = "intent", .value = "prepared" }},
        });
        defer alloc.free(envelope);
        {
            var backend: Backend = undefined;
            try backend.openInto(alloc, std.mem.span(path), options);
            defer backend.abandonAfterCrash();
            try backend.persistManifest();
            {
                const locked = runtime.lockBackend(Backend, &backend);
                defer runtime.unlockBackend(Backend, &backend, locked);
                try backend.installCompletionPoolLocked(config);
                const pool = backend.completion_pool.?;
                try pool.qualifyFresh(&backend);
                _ = try pool.accept(&backend, 3, 1, 0, 0, envelope);
                if (prepared) {
                    const slot = try pool.adopt(&backend, 0);
                    backend.durable_completion = slot;
                    backend.durable_completion_members[0] = slot;
                    try slot.applyCanonicalPrepare(&backend, pool.cells[0].entry.?.entry.prepare_operations);
                }
            }
            if (!prepared) for (0..completion.recovery_wal_records) |_| {
                var batch = try backend.beginWrite();
                errdefer batch.abort();
                try batch.put(.{}, "unrelated-overwrite", "retained");
                try batch.commit();
            };
        }
        {
            var missing: Backend = undefined;
            try std.testing.expectError(error.CompletionRecoveryCapacityRequired, missing.openInto(alloc, std.mem.span(path), options));
        }
        {
            var restored: Backend = undefined;
            var restoring_options = options;
            restoring_options.completion_pool_config = config;
            try restored.openInto(alloc, std.mem.span(path), restoring_options);
            defer restored.abandonAfterCrash();
            const pool = restored.completion_pool.?;
            try std.testing.expect(pool.restored and !pool.ready);
            try std.testing.expectEqual(restored.write_stats.wal_append_bytes, pool.wal_bytes_start);
            try std.testing.expectEqual(restored.write_stats.wal_append_entries, pool.wal_entries_start);
            try std.testing.expectEqual(restored.write_stats.wal_append_records, pool.wal_records_start);
            try std.testing.expectEqual(if (prepared) Pool(Backend).Phase.prepared else .accepted, pool.cells[0].phase);
            try std.testing.expectEqual(prepared, restored.findDurableCompletion(id) != null);
            const value = try pool.point(&restored, alloc, null, "intent");
            defer value.deinit(alloc);
            if (prepared) try std.testing.expectEqualStrings("prepared", value.value.?) else try std.testing.expect(value.value == null);
            try std.testing.expect((try pool.io.storage().fileSize(pool.accepted_paths[0])) > accepted_header_bytes);
            if (!prepared) {
                try std.testing.expectEqual(completion.recovery_wal_records, pool.replayed_wal_records);
                var incoming: @import("state.zig").ActiveMemTable = .{};
                defer incoming.deinit(alloc);
                try incoming.upsert(alloc, .{}, "unrelated-overwrite", "new", false);
                try std.testing.expectError(error.CompletionForegroundCapacityExceeded, pool.checkOrdinary(&restored, &incoming));
            } else try std.testing.expectEqual(@as(u64, 1), (try pool.durableProgress()).index);
            var cell_buffer: [max_slots]DurableCell = undefined;
            const cells = try pool.durableCells(&cell_buffer);
            try std.testing.expectEqual(@as(usize, 1), cells.len);
            var observation: DurableObservation = .{ .expected = cells[0].identity, .present = false };
            const missing: DurableLog = .{ .mode = .persisted_replacement, .compacted_index = 0, .compacted_term = 0, .last_index = 0, .commit_index = 0, .observations = (&observation)[0..1] };
            if (prepared) {
                try std.testing.expectError(error.RecoveryRequired, pool.reconcileDurableLog(&restored, missing));
                observation.present = true;
                observation.observed_term = 4;
                observation.observed_digest = @splat(82);
                observation.replaced_in_this_persist = true;
                var replacement = missing;
                replacement.last_index = 1;
                replacement.commit_index = 1;
                try std.testing.expectError(error.RecoveryRequired, pool.reconcileDurableLog(&restored, replacement));
                try std.testing.expectEqual(Pool(Backend).Phase.prepared, pool.cells[0].phase);
                observation = .{ .expected = cells[0].identity, .present = false };
                var compacted = missing;
                compacted.mode = .startup_complete;
                compacted.last_index = 1;
                compacted.commit_index = 1;
                compacted.compacted_index = 1;
                compacted.compacted_term = 3;
                try pool.reconcileDurableLog(&restored, compacted);
                try std.testing.expectEqual(Pool(Backend).Phase.prepared, pool.cells[0].phase);
            } else {
                // Online suffix absence cannot discard a queued accepted entry.
                try pool.reconcileDurableLog(&restored, missing);
                try std.testing.expectEqual(Pool(Backend).Phase.accepted, pool.cells[0].phase);
                var startup = missing;
                startup.mode = .startup_complete;
                try pool.reconcileDurableLog(&restored, startup);
                try std.testing.expectEqual(Pool(Backend).Phase.spent, pool.cells[0].phase);
                try std.testing.expect(!pool.ready);
                try std.testing.expectError(error.FileNotFound, pool.io.storage().fileSize(pool.accepted_paths[0]));
            }
        }
        try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    }
}

test "workload admission physical completion pool idle receipt restoration survives maintenance boundaries" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const Options = @import("../lsm_backend.zig").Options;
    const runtime = @import("runtime.zig");
    const cycle = @import("completion_maintenance_cycle.zig");
    const alloc = std.testing.allocator;
    const config: Config = .{ .identity = .{ .capacity = 4, .group_id = 23, .node_id = 7, .incarnation = @splat(11), .policy_digest = @splat(12), .generation = 19 }, .schema_catalog_digest = @splat(13), .namespace = .root };
    const progress: completion.AcceptedIdentity = .{ .term = 3, .index = 17, .digest = @splat(71) };
    const progress_bytes = encodeProgress(config.identity, progress);
    for ([_]?cycle.Fault{ null, .before_publish, .after_publish, .after_reset }) |failure| {
        var fd_pool = storage_io.NativeStoragePool.initWithCapacityForTest(alloc, 32);
        defer fd_pool.deinit();
        var manager = resources.ResourceManager.init(.{ .identity_allocator = alloc, .memory_budget = .{ .hard_limit_bytes = 256 * 1024 * 1024 } });
        defer manager.deinit(alloc);
        var path_buffer: [256]u8 = undefined;
        const path = repository.tmpPath(&path_buffer, "native-pool-idle-cycle");
        defer repository.cleanupTmp(path);
        const options: Options = .{ .resource_manager = &manager, .native_storage_pool = &fd_pool, .flush_threshold = 10000, .compact_threshold_runs = 1024, .l0_overlap_compact_threshold_runs = 1024 };
        const receipt_path = try std.fs.path.join(alloc, &.{ std.mem.span(path), "completion-installation.guard" });
        defer alloc.free(receipt_path);
        {
            var backend: Backend = undefined;
            try backend.openInto(alloc, std.mem.span(path), options);
            defer backend.abandonAfterCrash();
            {
                var write = try backend.beginWrite();
                errdefer write.abort();
                try write.put(.{}, entry_codec.group_progress_key, &progress_bytes);
                try write.put(.{}, "row", "old");
                try write.commit();
            }
            try backend.checkpointWalAfterDurableBoundary();
            if (failure == null) {
                // The last four-member cohort can leave 65-68 runs without
                // any accepted guard; restoration must permit maintenance.
                for (0..64) |i| {
                    {
                        var write = try backend.beginWrite();
                        errdefer write.abort();
                        var key: [32]u8 = undefined;
                        try write.put(.{}, try std.fmt.bufPrint(&key, "edge-{d}", .{i}), "retained");
                        try write.commit();
                    }
                    try backend.checkpointWalAfterDurableBoundary();
                }
                try std.testing.expectEqual(@as(usize, 65), backend.runs.count());
            }
            {
                const locked = runtime.lockBackend(Backend, &backend);
                defer runtime.unlockBackend(Backend, &backend, locked);
                try backend.installCompletionPoolLocked(config);
                if (failure == null) try std.testing.expectError(error.CompletionReservationBusy, backend.completion_pool.?.qualifyFresh(&backend)) else try backend.completion_pool.?.qualifyFresh(&backend);
            }
            // Native recovery only checks the presence fence; DB separately
            // validates the real authenticated installation receipt bytes.
            try manifest_set.replace(alloc, backend.storage.?, receipt_path, "installed");
            {
                var write = try backend.beginWrite();
                errdefer write.abort();
                try write.put(.{}, "row", "latest");
                try write.commit();
            }
            if (failure) |point| {
                cycle.test_fail_at = point;
                defer cycle.test_fail_at = null;
                const locked = runtime.lockBackend(Backend, &backend);
                defer runtime.unlockBackend(Backend, &backend, locked);
                try std.testing.expectError(error.RecoveryRequired, backend.completion_pool.?.maintainLocked(&backend));
                try std.testing.expect(!backend.completion_pool.?.ready);
                if (point == .before_publish) try std.testing.expectError(error.CompletionReservationBusy, backend.completion_pool.?.qualifyFresh(&backend));
            }
        }
        {
            var missing: Backend = undefined;
            try std.testing.expectError(error.CompletionRecoveryCapacityRequired, missing.openInto(alloc, std.mem.span(path), options));
        }
        {
            var restored: Backend = undefined;
            var restoring_options = options;
            restoring_options.completion_pool_config = config;
            try restored.openInto(alloc, std.mem.span(path), restoring_options);
            defer restored.abandonAfterCrash();
            const pool = restored.completion_pool.?;
            try std.testing.expect(pool.restored and !pool.ready and !pool.startup_reconciliation_pending);
            for (pool.cells[0..pool.cell_count]) |cell| try std.testing.expectEqual(Pool(Backend).Phase.free, cell.phase);
            try std.testing.expectEqual(@as(u64, 17), (try pool.durableProgress()).index);
            const latest = try pool.point(&restored, alloc, null, "row");
            defer latest.deinit(alloc);
            try std.testing.expectEqualStrings("latest", latest.value.?);
            {
                const locked = runtime.lockBackend(Backend, &restored);
                defer runtime.unlockBackend(Backend, &restored, locked);
                if (pool.maintenanceRequired(&restored)) try pool.maintainLocked(&restored);
                try pool.qualifyFresh(&restored);
                try std.testing.expect(pool.ready);
                // Repeated idle checkpoints must not forget permanent progress.
                try pool.maintainLocked(&restored);
                try pool.qualifyFresh(&restored);
                try std.testing.expectEqual(@as(u64, 17), (try pool.durableProgress()).index);
            }
        }
        {
            var native = try storage_io.NativeStorage.initWithPool(alloc, .threaded, &fd_pool);
            defer native.deinit();
            const pointer = try repository.manifestPath(alloc, std.mem.span(path));
            defer alloc.free(pointer);
            try native.storage().deleteFileAbsolute(pointer);
            var missing_manifest: Backend = undefined;
            var restoring_options = options;
            restoring_options.completion_pool_config = config;
            try std.testing.expectError(error.InvalidManifest, missing_manifest.openInto(alloc, std.mem.span(path), restoring_options));
            try std.testing.expectEqual(@as(u64, 9), try native.storage().fileSize(receipt_path));
        }
        try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    }
}

test "workload admission physical completion pool bounds reader-retained maintenance generations" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const runtime = @import("runtime.zig");
    const alloc = std.testing.allocator;
    var fd_pool = storage_io.NativeStoragePool.initWithCapacityForTest(alloc, 32);
    defer fd_pool.deinit();
    var manager = resources.ResourceManager.init(.{ .identity_allocator = alloc, .memory_budget = .{ .hard_limit_bytes = 256 * 1024 * 1024 } });
    defer manager.deinit(alloc);
    var path_buffer: [256]u8 = undefined;
    const path = repository.tmpPath(&path_buffer, "native-pool-readers-cycle");
    defer repository.cleanupTmp(path);
    {
        var backend: Backend = undefined;
        try backend.openInto(alloc, std.mem.span(path), .{ .resource_manager = &manager, .native_storage_pool = &fd_pool, .flush_threshold = 10000 });
        defer backend.abandonAfterCrash();
        {
            var write = try backend.beginWrite();
            errdefer write.abort();
            try write.put(.{}, "row", "version0");
            try write.commit();
        }
        try backend.checkpointWalAfterDurableBoundary();
        {
            const locked = runtime.lockBackend(Backend, &backend);
            defer runtime.unlockBackend(Backend, &backend, locked);
            try backend.installCompletionPoolLocked(.{ .identity = .{ .capacity = 4, .group_id = 23, .node_id = 7, .incarnation = @splat(11), .policy_digest = @splat(12), .generation = 19 }, .schema_catalog_digest = @splat(13), .namespace = .root });
            try backend.completion_pool.?.qualifyFresh(&backend);
        }
        const pool = backend.completion_pool.?;
        var first = try backend.beginRead();
        var first_live = true;
        defer if (first_live) first.abort();
        {
            var write = try backend.beginWrite();
            errdefer write.abort();
            try write.put(.{}, "row", "version1");
            try write.commit();
        }
        {
            const locked = runtime.lockBackend(Backend, &backend);
            defer runtime.unlockBackend(Backend, &backend, locked);
            try pool.maintainLocked(&backend);
            try pool.qualifyFresh(&backend);
        }
        var second = try backend.beginRead();
        var second_live = true;
        defer if (second_live) second.abort();
        {
            var write = try backend.beginWrite();
            errdefer write.abort();
            try write.put(.{}, "row", "version2");
            try write.commit();
        }
        {
            const locked = runtime.lockBackend(Backend, &backend);
            defer runtime.unlockBackend(Backend, &backend, locked);
            try pool.maintainLocked(&backend);
            try pool.qualifyFresh(&backend);
            try std.testing.expectError(error.CompletionReservationBusy, pool.maintainLocked(&backend));
            try std.testing.expect(!pool.ready and !pool.failed);
        }
        try std.testing.expectEqualStrings("version0", try first.get(.{}, "row"));
        try std.testing.expectEqualStrings("version1", try second.get(.{}, "row"));
        first.abort();
        first_live = false;
        {
            const locked = runtime.lockBackend(Backend, &backend);
            defer runtime.unlockBackend(Backend, &backend, locked);
            try pool.maintainLocked(&backend);
            try pool.qualifyFresh(&backend);
        }
        try std.testing.expectEqualStrings("version1", try second.get(.{}, "row"));
        second.abort();
        second_live = false;
        var latest = try backend.beginRead();
        defer latest.abort();
        try std.testing.expectEqualStrings("version2", try latest.get(.{}, "row"));
    }
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
}

test "workload admission physical completion record admission includes canonical outcome and private framing" {
    const alloc = std.testing.allocator;
    const id: [16]u8 = @splat(31);
    const Fixture = struct {
        fn decode(allocator: Allocator, descriptor: []const u8, prepare: []const slot_codec.Operation) !entry_codec.OwnedEntry {
            const wire = try entry_codec.encode(allocator, .{
                .group_id = 7,
                .group_incarnation = @splat(1),
                .policy_digest = @splat(2),
                .schema_catalog_digest = @splat(3),
                .txn_id = @splat(31),
                .original_input_digest = @splat(4),
                .baseline_digest = @splat(5),
                .baseline_keys = &.{ "prep", "row" },
                .descriptor = descriptor,
                .prepare_operations = prepare,
            });
            defer allocator.free(wire);
            return entry_codec.decode(allocator, wire);
        }
    };
    const small_descriptor = try slot_codec.encode(alloc, .{ .txn_id = id, .intent_revision = 1, .namespace = "docs", .limits = completion.limits, .profile_fence = "profile", .commit = &.{.{ .kind = .put, .key = "row", .value = "complete" }}, .abort = &.{.{ .kind = .delete, .key = "row" }} }, .{});
    defer alloc.free(small_descriptor);
    const payload = try alloc.alloc(u8, 1024 - 13 - "docs".len - "prep".len + 1);
    defer alloc.free(payload);
    @memset(payload, 'v');
    {
        var exact = try Fixture.decode(alloc, small_descriptor, &.{.{ .kind = .put, .key = "prep", .value = payload[0 .. payload.len - 1] }});
        defer exact.deinit();
        try validateEntryRecordSizes(&exact, 1024);
    }
    {
        var too_large = try Fixture.decode(alloc, small_descriptor, &.{.{ .kind = .put, .key = "prep", .value = payload }});
        defer too_large.deinit();
        try std.testing.expectError(error.UnsupportedCompletionProfile, validateEntryRecordSizes(&too_large, 1024));
    }
    // Tombstones still serialize namespace and key; fixed-size bindings cannot
    // make an otherwise oversized key acceptable.
    try validateRecordSize("docs", "row", 0, 20);
    try std.testing.expectError(error.UnsupportedCompletionProfile, validateRecordSize("docs", "row", 0, 19));
    const empty_descriptor = try slot_codec.encode(alloc, .{ .txn_id = id, .intent_revision = 1, .namespace = "docs", .limits = completion.limits, .profile_fence = "profile", .commit = &.{.{ .kind = .put, .key = "row", .value = "" }}, .abort = &.{} }, .{});
    defer alloc.free(empty_descriptor);
    const outcome = try alloc.alloc(u8, completion.limits.max_encoded_bytes - empty_descriptor.len);
    defer alloc.free(outcome);
    @memset(outcome, 'x');
    const full_descriptor = try slot_codec.encode(alloc, .{ .txn_id = id, .intent_revision = 1, .namespace = "docs", .limits = completion.limits, .profile_fence = "profile", .commit = &.{.{ .kind = .put, .key = "row", .value = outcome }}, .abort = &.{} }, .{});
    defer alloc.free(full_descriptor);
    try std.testing.expectEqual(@as(usize, completion.limits.max_encoded_bytes), full_descriptor.len);
    var private = try Fixture.decode(alloc, full_descriptor, &.{.{ .kind = .put, .key = "prep", .value = "v" }});
    defer private.deinit();
    // The actual outcome fits; the private descriptor record needs additional
    // SST framing and the longest cohort storage key, and must reject pre-ACK.
    try validateRecordSize("docs", "row", outcome.len, (Shape{}).max_record_bytes);
    try std.testing.expectError(error.UnsupportedCompletionProfile, validateEntryRecordSizes(&private, (Shape{}).max_record_bytes));
    const exact_private_size = 13 + "docs".len + completion.storage_keys[1].len + full_descriptor.len;
    try validateEntryRecordSizes(&private, exact_private_size);
    try std.testing.expectError(error.UnsupportedCompletionProfile, validateEntryRecordSizes(&private, exact_private_size - 1));
}

test "workload admission completion single drain rejects cumulative large keys before accepted ownership" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const runtime = @import("runtime.zig");
    const alloc = std.testing.allocator;
    var fd_pool = storage_io.NativeStoragePool.initWithCapacityForTest(alloc, 32);
    defer fd_pool.deinit();
    var manager = resources.ResourceManager.init(.{ .identity_allocator = alloc, .memory_budget = .{ .hard_limit_bytes = 256 * 1024 * 1024 } });
    defer manager.deinit(alloc);
    var path_buffer: [256]u8 = undefined;
    const path = repository.tmpPath(&path_buffer, "completion-single-drain-metadata");
    defer repository.cleanupTmp(path);
    var backend: Backend = undefined;
    try backend.openInto(alloc, std.mem.span(path), .{ .resource_manager = &manager, .native_storage_pool = &fd_pool, .flush_threshold = 10000 });
    defer backend.abandonAfterCrash();
    {
        var batch = try backend.beginWrite();
        errdefer batch.abort();
        try batch.put(.{}, "baseline", "preserved");
        try batch.commit();
    }
    try backend.checkpointWalAfterDurableBoundary();
    const identity: abi.Identity = .{ .capacity = 4, .group_id = 23, .node_id = 7, .incarnation = @splat(11), .policy_digest = @splat(12), .generation = 19 };
    const locked = runtime.lockBackend(Backend, &backend);
    defer runtime.unlockBackend(Backend, &backend, locked);
    try backend.installCompletionPoolLocked(.{ .identity = identity, .schema_catalog_digest = @splat(13), .namespace = .root });
    const pool = backend.completion_pool.?;
    try pool.qualifyFresh(&backend);
    const storage = try alloc.alloc(u8, 6 * 40000);
    defer alloc.free(storage);
    var keys: [6][]const u8 = undefined;
    var prepare: [3]slot_codec.Operation = undefined;
    var commit: [3]slot_codec.Operation = undefined;
    for (0..3) |i| {
        var hash = entry_codec.BaselineHasher.init();
        for (&keys, 0..) |*key, j| {
            const bytes = storage[j * 40000 ..][0..40000];
            @memset(bytes, 0x91);
            std.mem.writeInt(u64, bytes[0..8], i * 6 + j, .big);
            key.* = bytes;
            try hash.add(bytes, null);
            if (j < 3) prepare[j] = .{ .kind = .put, .key = bytes, .value = "v" } else commit[j - 3] = .{ .kind = .put, .key = bytes, .value = "done" };
        }
        const id: [16]u8 = @splat(@intCast(100 + i));
        const descriptor = try slot_codec.encode(alloc, .{ .txn_id = id, .intent_revision = 1, .limits = completion.limits, .profile_fence = "replicated-profile", .commit = &commit, .abort = &.{} }, .{});
        defer alloc.free(descriptor);
        const wire = try entry_codec.encode(alloc, .{ .group_id = identity.group_id, .group_incarnation = identity.incarnation, .policy_digest = identity.policy_digest, .schema_catalog_digest = @splat(13), .txn_id = id, .original_input_digest = @splat(14), .baseline_digest = hash.finish(), .previous_term = if (i == 0) 0 else 3, .previous_index = i, .baseline_keys = &keys, .descriptor = descriptor, .prepare_operations = &prepare });
        defer alloc.free(wire);
        if (i == 2) {
            const before = pool.capacity_growth;
            try std.testing.expectError(error.UnsupportedCompletionProfile, pool.accept(&backend, 3, i + 1, 3, i, wire));
            try std.testing.expectEqual(Pool(Backend).Phase.free, pool.cells[i].phase);
            try std.testing.expect(std.meta.eql(before, pool.capacity_growth));
            try std.testing.expectError(error.FileNotFound, backend.storage.?.fileSize(pool.accepted_paths[i]));
        } else {
            try std.testing.expectEqual(i, try pool.accept(&backend, 3, i + 1, if (i == 0) 0 else 3, i, wire));
            const slot = try pool.adopt(&backend, i);
            backend.durable_completion_members[i] = slot;
            if (backend.durable_completion == null) backend.durable_completion = slot;
            try slot.applyCanonicalPrepare(&backend, pool.cells[i].entry.?.entry.prepare_operations);
            pool.notifyApplied(i);
        }
    }
}

test "workload admission completion operation workspace reserves exact remaining journal frames" {
    const record_limit = (Shape{}).max_record_bytes;
    const frame = try journalHeadroom(record_limit, 1);
    try std.testing.expect(frame > completion.limits.max_encoded_bytes + 64 * 1024);
    const cap = repository.maxManifestReadBytes();
    const initial = cap - try journalHeadroom(record_limit, max_slots);
    for (0..max_slots + 1) |manifested| {
        const size = initial + manifested * frame;
        try std.testing.expect(!try journalNeedsMaintenance(size, record_limit, max_slots - manifested));
    }
    try std.testing.expect(try journalNeedsMaintenance(initial + 1, record_limit, max_slots));
    // A fully manifested cohort may reopen at the file ceiling, then requires
    // rotation before any new cohort can be admitted.
    try std.testing.expect(!try journalNeedsMaintenance(cap, record_limit, 0));
    try std.testing.expect(try journalNeedsMaintenance(cap, record_limit, max_slots));
    try std.testing.expectError(error.CompletionReservationBusy, journalNeedsMaintenance(cap + 1, record_limit, 0));
}

test "workload admission completion replay workspace covers unique and replaced versions with fixed pending storage" {
    const alloc = std.testing.allocator;
    const bound = try replayWorkspaceRequirement();
    try std.testing.expect(bound.total <= completion.scratch_bytes);
    for ([_]bool{ false, true }) |replace| {
        var memory = storage_io.MemoryStorage.init(alloc);
        defer memory.deinit();
        const root = "/completion-replay-bound";
        try memory.storage().createDirPath(root);
        const distinct = if (replace) replay_max_entries / 2 else replay_max_entries;
        const value: [1792]u8 = @splat(0xa5);
        var position: usize = 0;
        while (position < replay_max_entries) {
            var incoming: state.ActiveMemTable = .{};
            defer incoming.deinit(alloc);
            const end = @min(position + 128, replay_max_entries);
            while (position < end) : (position += 1) {
                var key: [8]u8 = undefined;
                std.mem.writeInt(u64, &key, position % distinct, .big);
                const tombstone = replace and position >= distinct and position % 17 == 0;
                const size: usize = if (tombstone) 0 else if (!replace) 900 else if (position < distinct) 64 else value.len;
                try incoming.upsert(alloc, .{ .name = "docs" }, &key, value[0..size], tombstone);
            }
            _ = try wal.appendState(memory.storage(), alloc, root, &incoming, true);
        }
        const retained = try wal.snapshotRetention(memory.storage(), alloc, root);
        try std.testing.expect(retained.bytes <= replay_max_bytes);
        var failing = std.testing.FailingAllocator.init(alloc, .{});
        var manager = resources.ResourceManager.init(.{ .identity_allocator = alloc, .memory_budget = .{ .hard_limit_bytes = completion.scratch_bytes } });
        defer manager.deinit(alloc);
        const domain = try domains.RecyclingScratch.create(failing.allocator(), &manager, bound.total);
        defer domain.destroy() catch unreachable;
        failing.fail_index = failing.alloc_index;
        failing.resize_fail_index = failing.resize_index;
        const scratch = domain.allocator();
        {
            const pending = try scratch.alloc(u8, replay_max_bytes);
            defer scratch.free(pending);
            var replayed: state.ActiveMemTable = .{};
            defer replayed.deinit(scratch);
            const stats = try wal.replayIntoMutableWithHooksAndOptions(memory.storage(), scratch, root, &replayed, null, .{ .pending_buffer = pending });
            try std.testing.expectEqual(@as(u64, replay_max_entries), stats.entries);
            try std.testing.expectEqual(distinct, replayed.entryCount());
            for (0..distinct) |i| {
                var key: [8]u8 = undefined;
                std.mem.writeInt(u64, &key, i, .big);
                if (replace and (i + distinct) % 17 == 0) {
                    try std.testing.expectError(error.NotFound, replayed.get(.{ .name = "docs" }, &key));
                } else {
                    const found = try replayed.get(.{ .name = "docs" }, &key);
                    try std.testing.expectEqualSlices(u8, value[0..if (replace) value.len else 900], found);
                }
            }
        }
        try std.testing.expect(domain.isEmpty());
        var tiny: [8]u8 = undefined;
        var rejected: state.ActiveMemTable = .{};
        defer rejected.deinit(scratch);
        try std.testing.expectError(error.WalRecordTooLarge, wal.replayIntoMutableWithHooksAndOptions(memory.storage(), scratch, root, &rejected, null, .{ .pending_buffer = &tiny }));
        try std.testing.expectEqual(@as(usize, 0), rejected.entryCount());
    }
}

test {
    _ = @import("completion_control_capacity.zig");
    _ = @import("completion_output_layout.zig");
}
