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
const table_file = @import("../lsm/table_file.zig");
const resources = @import("../resource_manager.zig");
const abi = @import("kernel_owner_abi").completion_pool;
const wal = @import("wal.zig");
const Allocator = std.mem.Allocator;
pub const maintenance = @import("completion_maintenance.zig");
const capacity = @import("completion_capacity.zig");
const generations_mod = @import("completion_generations.zig");
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
        config: Config,
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
            if (cohort.initial_runs > 64 or backend.runs.count() > cohort.initial_runs + max_slots) return error.CompletionReservationBusy;
            const next_id = std.math.add(u64, cohort.base_run_id, max_slots) catch return error.CompletionReservationBusy;
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
            if (journal_size > repository.maxManifestReadBytes() - max_slots * (completion.limits.max_encoded_bytes + 64 * 1024)) return error.CompletionReservationBusy;
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
                .control = control,
                .generations = generations,
                .scratch = scratch,
                .compiler = compiler,
                .io = io,
                .memory_pin = memory_pin,
                .wal_pin = wal_pin,
                .guard_paths = guard_paths,
                .accepted_paths = accepted_paths,
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
            backend.next_run_id = @max(backend.next_run_id, next_id);
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

        /// Native owner calls this only after attached runtime leases are gone
        /// and pooled Slots have returned their cell ownership. Published tree
        /// allocations retain the publication domain through old reader release.
        pub fn destroy(self: *Self) void {
            self.releaseCells();
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

        pub fn validateBaseline(self: *Self, backend: *Backend, alloc: Allocator, entry: *const entry_codec.OwnedEntry) !void {
            var hash = entry_codec.BaselineHasher.init();
            for (entry.entry.baseline_keys) |key| {
                const value = try self.point(backend, alloc, entry.decoded_descriptor.descriptor.namespace, key);
                defer value.deinit(alloc);
                try hash.add(key, value.value);
            }
            if (!std.mem.eql(u8, &hash.finish(), &entry.entry.baseline_digest)) return error.CompletionProfileChanged;
        }

        pub fn checkOrdinary(self: *Self, backend: *Backend, incoming: anytype) !void {
            if (self.maintenance_active) return error.CompletionReservationBusy;
            if (self.failed or !self.restored) return error.RecoveryRequired;
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
            _ = try maintenance.drainWorkspaceRequirement(future, .{
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
            try self.validateBaseline(backend, scratch, &checked);
            const growth = try entryCapacity(&checked);
            try self.checkCapacity(growth);
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
            for (self.cells[0..self.cell_count]) |cell| if (cell.phase != .free) return error.CompletionReservationBusy;
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
            const alloc = self.scratch.allocator();
            var lock = try backend.acquireWalOperationLock(.exclusive);
            defer lock.release();
            const max_records = completion.recovery_wal_records + 2 * max_slots;
            if (try wal.currentSegment(self.io.storage(), alloc, backend.root_dir.?) > max_records + 1) return error.CompletionRecoveryCapacityRequired;
            const retention = try wal.snapshotRetention(self.io.storage(), alloc, backend.root_dir.?);
            const max_replay_bytes = completion.recovery_wal_bytes + max_slots * (entry_codec.max_wire_bytes + completion.limits.max_encoded_bytes + 8192);
            if (retention.bytes > max_replay_bytes or retention.segments > max_records + 1) return error.CompletionRecoveryCapacityRequired;
            const parser_bytes = try alloc.alloc(u8, 4 * 1024 * 1024);
            defer alloc.free(parser_bytes);
            var parser = std.heap.FixedBufferAllocator.init(parser_bytes);
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
                    if (hooks.entries > completion.foreground_entries + max_slots * (2 * 256 + 6)) return error.CompletionRecoveryCapacityRequired;
                }
                fn onRecord(raw: *anyopaque, _: u64, _: u64) !void {
                    const hooks: *@This() = @ptrCast(@alignCast(raw));
                    hooks.records += 1;
                    if (hooks.records > max_records) return error.CompletionRecoveryCapacityRequired;
                }
            };
            var hooks: Hooks = .{ .pool = self };
            const stats = try wal.replayIntoMutableWithHooks(self.io.storage(), parser.allocator(), backend.root_dir.?, &backend.mutable, .{
                .ctx = &hooks,
                .entry_allocator = Hooks.entryAllocator,
                .on_applied_entry = Hooks.onEntry,
                .on_applied_record = Hooks.onRecord,
            });
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
            for (self.cells[0..self.cell_count], 0..) |*cell, i| {
                if (cell.phase != .accepted and cell.phase != .prepared) continue;
                if (cell.resolution) |resolution| if (std.meta.eql(identity, resolution.identity)) {
                    cell.resolution = null;
                    return;
                };
                if (cell.term != identity.term or cell.index != identity.index or
                    !std.mem.eql(u8, &cell.entry.?.digest, &identity.digest)) continue;
                if (cell.phase != .accepted) return error.RecoveryRequired;
                return self.retireUnappliedCell(backend, i);
            }
            return error.NotFound;
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
    }
    try backend.retireDurableCompletionCohort();
    try pool.maintainLocked(&backend);
    try std.testing.expectError(error.CompletionReservationBusy, pool.qualifyFresh(&backend));
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
