// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! One internal native completion slot. The descriptor is published by the
//! transaction prepare batch; opening restores its physical ownership before
//! foreground admission. Unsupported providers and resource shapes fail closed.
const std = @import("std");
const builtin = @import("builtin");
pub const guard = @import("completion_guard.zig");
pub const max_slots = guard.max_slots;
const codec = @import("completion_slot.zig");
const domains = @import("completion_allocator.zig");
const storage_io = @import("storage_io.zig");
const repository = @import("repository.zig");
const manifest_set = @import("manifest_set.zig");
const resources = @import("../resource_manager.zig");
const state = @import("state.zig");
const compaction = @import("compaction.zig");
const manifest = @import("../lsm/manifest.zig");
const wal = @import("wal.zig");
const runtime = @import("runtime.zig");
const Directory = @import("run_directory.zig").Directory;

pub const receipt_prefix = @import("completion_entry.zig").receipt_prefix;
pub const receiptKey = @import("completion_entry.zig").receiptKey;
pub const AcceptedIdentity = struct {
    term: u64,
    index: u64,
    digest: [32]u8,
    pub fn encode(self: AcceptedIdentity) [48]u8 {
        var value: [48]u8 = undefined;
        std.mem.writeInt(u64, value[0..8], self.term, .little);
        std.mem.writeInt(u64, value[8..16], self.index, .little);
        @memcpy(value[16..48], &self.digest);
        return value;
    }
};

pub const applied_key = "\x00\x00__metadata__:completion_applied_v1";
pub const guard_filename = "completion-slot.guard";
pub var test_after_wal: ?*const fn () bool = null;
pub var test_after_manifest: ?*const fn () bool = null;
pub const storage_key = "\x00\x00__metadata__:completion_slot_v1";
pub const guard_filenames = [_][]const u8{ guard_filename, "completion-slot-1.guard", "completion-slot-2.guard", "completion-slot-3.guard" };
pub const storage_keys = [_][]const u8{ storage_key, storage_key ++ "_1", storage_key ++ "_2", storage_key ++ "_3" };
pub const applied_keys = [_][]const u8{ applied_key, applied_key ++ "_1", applied_key ++ "_2", applied_key ++ "_3" };
pub const foreground_bytes = 256 * 1024;
pub const foreground_entries = 2048;
pub const recovery_wal_bytes = 1024 * 1024;
pub const recovery_wal_records = 128;
pub const publication_bytes = 4 * 1024 * 1024;
pub const scratch_bytes = 32 * 1024 * 1024;
pub const limits: codec.Limits = .{
    .memory_bytes = publication_bytes + scratch_bytes,
    .wal_bytes = 8 * 1024 * 1024,
    .flush_bytes = 16 * 1024 * 1024,
    .fd_count = 2,
    .max_operations = 256,
    .max_encoded_bytes = 256 * 1024,
};

pub const AppendCounters = @import("completion_capacity.zig").AppendCounters;
pub const private_records_per_phase = 4;
pub const records_per_phase = limits.max_operations + private_records_per_phase;
pub const max_private_key_bytes = blk: {
    var largest: usize = @max(receiptKey(@splat(0)).len, @import("completion_entry.zig").group_progress_key.len);
    largest = @max(largest, @import("../internal_keys.zig").raft_document_applied_entry_key.len);
    for (storage_keys ++ applied_keys) |key| largest = @max(largest, key.len);
    break :blk largest;
};
pub const wal_phase_overhead = private_records_per_phase * (max_private_key_bytes + 112) + records_per_phase * ("docs".len + 16) + 20;
// One canonical prepare (or one-phase mutation) and one document outcome.
// A retained transaction-control lifetime must add its own separate budget.
pub const prepare_append_budget: AppendCounters = .{ .bytes = @import("completion_entry.zig").max_wire_bytes + wal_phase_overhead, .entries = records_per_phase, .records = 1 };
pub const outcome_append_budget: AppendCounters = .{ .bytes = limits.max_encoded_bytes + wal_phase_overhead, .entries = records_per_phase, .records = 1 };
pub const foreground_append_budget: AppendCounters = .{ .bytes = recovery_wal_bytes, .entries = foreground_entries, .records = recovery_wal_records };

pub fn incomingAppendCounters(incoming: anytype) AppendCounters {
    if (incoming.entryCount() == 0) return .{};
    return .{ .bytes = wal.encodedStateRecordLen(incoming), .entries = incoming.entryCount(), .records = 1 };
}

pub const OperationWorkspace = struct {
    temporary_tree: usize,
    bindings: usize,
    wal_record: usize,
    manifest_frame: usize,
    paths: usize,
    writer: usize,
    largest_span: usize,
    total: usize,
};

/// Cumulative allocation bound, not just simultaneously live logical bytes.
/// Starting with an empty recycling domain, every split consumes no more than
/// its charged footprint; freeing/reusing a block cannot increase high-water
/// consumption. Thus the sum also proves every individual contiguous request.
pub fn operationWorkspaceRequirement(cost: @import("completion_capacity.zig").Cost, writer_limits: @import("completion_maintenance.zig").Limits) !OperationWorkspace {
    const footprint = domains.RecyclingScratch.allocationFootprint;
    const private_records = private_records_per_phase; // prepare: descriptor/marker/receipt/progress; outcome: four control edits.
    const entries = limits.max_operations + private_records;
    const namespace_bytes = "docs".len; // The only non-null native completion namespace.
    const private_key_bytes = max_private_key_bytes;
    // Canonical wire bounds all public keys/values. Descriptor storage is an
    // additional private value; the other private values fit the 112-byte
    // authority-bound group receipt. Repeated namespaces are charged separately.
    const payload_bytes = @import("completion_entry.zig").max_wire_bytes + limits.max_encoded_bytes +
        entries * namespace_bytes + private_records * (private_key_bytes + 112);
    const temporary = try state.ActiveMemTable.publicationAllocationBound(entries, entries, payload_bytes);
    const bindings = try std.math.add(usize, limits.max_encoded_bytes, try std.math.mul(usize, 2 * limits.max_operations, try footprint(0, 1)));
    const wal_record = try footprint(payload_bytes + 16 * entries + 20, 1); // WAL entry/header/count framing.
    const key = std.math.cast(usize, cost.max_key_bytes) orelse return error.UnsupportedCompletionProfile;
    const manifest_frame = try footprint(try manifest.singleRunJournalFrameSize(512 + 32, key, key), 1);
    const max_segments = recovery_wal_records + 2 * max_slots + 1;
    const append_paths = 4;
    const reset_fixed_paths = 11; // directory/read-index/first/legacy, three controls, replay directory/index/segments, final legacy.
    const paths = try std.math.mul(usize, append_paths + reset_fixed_paths + max_segments - 1, try footprint(512 + 32, 1));
    const writer = try @import("completion_maintenance.zig").drainWorkspaceRequirement(cost, writer_limits);
    var total = writer.total;
    for ([_]usize{ temporary, bindings, wal_record, manifest_frame, paths }) |bytes| total = try std.math.add(usize, total, bytes);
    const largest = @max(try footprint(writer.writer, 1), @max(try footprint(writer.compression, 1), @max(wal_record, manifest_frame)));
    if (total > scratch_bytes or largest > scratch_bytes) return error.UnsupportedCompletionProfile;
    total = std.mem.alignForward(usize, total, @alignOf(usize));
    return .{ .temporary_tree = temporary, .bindings = bindings, .wal_record = wal_record, .manifest_frame = manifest_frame, .paths = paths, .writer = writer.total, .largest_span = largest, .total = total };
}

pub const Values = struct {
    commit_timestamp: u64,
    replay_sequence: u64,
    shared_ledger_count: u64,
    shared_ledger_bytes: u64,
    /// Supplied only by the applying Raft runtime, never by transaction JSON.
    raft_term: u64 = 0,
    raft_index: u64 = 0,
    canonical_payload_digest: [32]u8 = @splat(0),
};

pub const PooledBaseline = struct {
    record: ?[53]u8 = null,
    credit: ?[16]u8 = null,
    summary: ?[16]u8 = null,
    applied: ?[16]u8 = null,
    slot_present: bool = false,
};

pub const PublicationDomain = union(enum) {
    arena: *domains.Arena,
    reservation: *domains.PublicationReservation,

    pub fn allocator(self: PublicationDomain) std.mem.Allocator {
        return switch (self) {
            .arena => |value| value.allocator(),
            .reservation => |value| value.allocator(),
        };
    }
    fn release(self: PublicationDomain) void {
        switch (self) {
            .arena => |value| value.release(),
            .reservation => |value| value.finish(),
        }
    }
};

pub const ScratchDomain = union(enum) {
    arena: *domains.Arena,
    borrowed: *domains.RecyclingScratch,

    pub fn allocator(self: ScratchDomain) std.mem.Allocator {
        return switch (self) {
            .arena => |value| value.allocator(),
            .borrowed => |value| value.allocator(),
        };
    }
    fn release(self: ScratchDomain) void {
        switch (self) {
            .arena => |value| value.release(),
            .borrowed => {},
        }
    }
};

pub fn Slot(comptime Backend: type) type {
    return struct {
        const Self = @This();
        allocator: std.mem.Allocator,
        publication: PublicationDomain,
        scratch: ScratchDomain,
        pooled_owner: ?PooledOwner = null,
        drain_workspace: ?*domains.CompilerWorkspace = null,
        drain_limits: @import("completion_maintenance.zig").Limits = .{},
        accepted_identity: ?AcceptedIdentity = null,
        descriptor: codec.OwnedDescriptor,
        encoded: []u8,
        guard_encoded: []u8,
        cohort: guard.Info,
        retired: bool = false,
        run_paths: [max_slots]?[]u8 = @splat(null),
        run_path_pins: [max_slots]?Backend.CompletionRunPathPin = @splat(null),
        io: *storage_io.NativeCompletionIo,
        wal_credit: u64 = 0,
        memory_pin: resources.ObserverMetadataPin,
        wal_pin: resources.ObserverMetadataPin,
        owns_observer_pins: bool,
        guard_path: []u8,
        wal_append_start: u64,
        replayed_wal_bytes: u64 = 0,
        wal_entries_start: u64,
        wal_records_start: u64,
        replayed_wal_entries: u64 = 0,
        replayed_wal_records: u64 = 0,
        run_id: u64,
        journal_path: []u8,
        run_path: []u8,
        durable: bool = false,
        attempted: bool = false,
        baseline_record: ?[53]u8 = null,
        baseline_credit: ?[16]u8 = null,
        baseline_summary: ?[16]u8 = null,
        baseline_applied: ?[16]u8 = null,
        baseline_slot_present: bool = false,

        pub const PooledOwner = struct {
            context: *anyopaque,
            release_cell: *const fn (*anyopaque, *Self) void,
            restore_wal_credits_after_checkpoint: *const fn (*anyopaque, *Backend) anyerror!void,
            prepare_progress: *const fn (*anyopaque, *Self, AcceptedIdentity, ?bool) anyerror![112]u8,
            publish_progress: *const fn (*anyopaque, AcceptedIdentity) void,
        };

        pub const PooledInput = struct {
            self_storage: *Self,
            publication: *domains.PublicationReservation,
            scratch: *domains.RecyclingScratch,
            drain_workspace: *domains.CompilerWorkspace,
            drain_limits: @import("completion_maintenance.zig").Limits,
            io: *storage_io.NativeCompletionIo,
            memory_pin: resources.ObserverMetadataPin,
            wal_pin: resources.ObserverMetadataPin,
            owner: PooledOwner,
            baseline: PooledBaseline,
            /// Set only after the pool has matched the persisted prepare and
            /// receipt to this accepted identity during startup reconciliation.
            durable_restored: bool = false,
            accepted_identity: AcceptedIdentity,
            encoded: []const u8,
            cohort: guard.Info,
            guard_path: []const u8,
            journal_path: []const u8,
            run_paths: [max_slots]?[]const u8,
        };

        /// All backing, WAL credit, observer identities and paths were installed
        /// before consensus acceptance. This constructor uses only the cell's
        /// concrete publication span and performs no baseline storage reads.
        pub fn createFromPool(backend: *Backend, input: PooledInput) !*Self {
            const alloc = input.publication.allocator();
            const self = input.self_storage;
            var descriptor = try codec.decode(alloc, input.encoded, .{});
            errdefer descriptor.deinit();
            if ((if (descriptor.descriptor.namespace) |name| !std.mem.eql(u8, name, "docs") else false) or
                !std.meta.eql(descriptor.descriptor.limits, limits) or input.cohort.capacity() > max_slots or input.cohort.index >= input.cohort.capacity())
                return error.InvalidCompletionSlot;
            if (input.durable_restored and !input.baseline.slot_present) return error.InvalidCompletionSlot;
            const wire = try alloc.dupe(u8, input.encoded);
            errdefer alloc.free(wire);
            const guard_wire = try guard.encode(alloc, input.cohort, input.encoded);
            errdefer alloc.free(guard_wire);
            const guard_path = try alloc.dupe(u8, input.guard_path);
            errdefer alloc.free(guard_path);
            const journal_path = try alloc.dupe(u8, input.journal_path);
            errdefer alloc.free(journal_path);
            var paths: [max_slots]?[]u8 = @splat(null);
            errdefer for (paths) |path| if (path) |owned| alloc.free(owned);
            for (0..input.cohort.capacity()) |i| paths[i] = try alloc.dupe(u8, input.run_paths[i] orelse return error.InvalidCompletionSlot);
            const wal_credit = self.wal_credit;
            self.* = .{
                .allocator = alloc,
                .publication = .{ .reservation = input.publication },
                .scratch = .{ .borrowed = input.scratch },
                .pooled_owner = input.owner,
                .drain_workspace = input.drain_workspace,
                .drain_limits = input.drain_limits,
                .accepted_identity = input.accepted_identity,
                .descriptor = descriptor,
                .encoded = wire,
                .guard_encoded = guard_wire,
                .cohort = input.cohort,
                .run_paths = paths,
                .io = input.io,
                .wal_credit = wal_credit,
                .memory_pin = input.memory_pin,
                .wal_pin = input.wal_pin,
                .owns_observer_pins = false,
                .guard_path = guard_path,
                .journal_path = journal_path,
                .run_path = paths[0].?,
                .run_id = input.cohort.base_run_id,
                .wal_append_start = backend.write_stats.wal_append_bytes,
                .wal_entries_start = backend.write_stats.wal_append_entries,
                .wal_records_start = backend.write_stats.wal_append_records,
                .baseline_record = input.baseline.record,
                .baseline_credit = input.baseline.credit,
                .baseline_summary = input.baseline.summary,
                .baseline_applied = input.baseline.applied,
                .baseline_slot_present = input.baseline.slot_present,
                .durable = input.durable_restored,
            };
            return self;
        }

        fn runOperation(self: *Self, backend: *Backend, args: anytype, comptime execute: anytype) !void {
            if (self.drain_workspace) |workspace| {
                const Context = struct {
                    slot: *Self,
                    backend: *Backend,
                    args: @TypeOf(args),
                    fn run(context: @This(), alloc: std.mem.Allocator) !void {
                        return execute(context.slot, context.backend, context.args, alloc);
                    }
                };
                return workspace.withCompletion(void, Context{ .slot = self, .backend = backend, .args = args }, Context.run);
            }
            return execute(self, backend, args, self.scratch.allocator());
        }

        /// Publish the leader's canonical prepare using held native capacity.
        /// The caller holds backend serialization and has installed this cell
        /// in the pending cohort. No logical planner/provider is invoked here.
        pub fn applyCanonicalPrepare(self: *Self, backend: *Backend, operations: []const codec.Operation) !void {
            return self.runOperation(backend, .{operations}, applyCanonicalPrepareInWorkspace);
        }

        fn applyCanonicalPrepareInWorkspace(self: *Self, backend: *Backend, args: anytype, alloc: std.mem.Allocator) !void {
            const operations: []const codec.Operation = args[0];
            const identity = self.accepted_identity orelse return error.InvalidCompletionSlot;
            if (identity.term == 0 or identity.index == 0 or operations.len == 0 or operations.len > 256)
                return error.InvalidCompletionSlot;
            if (self.pooled_owner == null or self.attempted or backend.manifest_recovery_required)
                return error.RecoveryRequired;
            if (self.durable) return;
            const publication_alloc = self.publication.allocator();
            const namespace = @import("../backend_types.zig").Namespace{ .name = self.descriptor.descriptor.namespace };
            var incoming: state.ActiveMemTable = .{};
            defer incoming.deinit(alloc);
            for (operations) |op| {
                if (op.bindings.len != 0) return error.InvalidCompletionSlot;
                for (storage_keys) |key| if (std.mem.eql(u8, op.key, key)) return error.InvalidCompletionSlot;
                for (applied_keys) |key| if (std.mem.eql(u8, op.key, key)) return error.InvalidCompletionSlot;
                if (std.mem.startsWith(u8, op.key, receipt_prefix)) return error.InvalidCompletionSlot;
                if (std.mem.eql(u8, op.key, &@import("../internal_keys.zig").raft_document_applied_entry_key)) return error.InvalidCompletionSlot;
                try incoming.upsert(alloc, namespace, op.key, op.value, op.kind == .delete);
            }
            const marker_key = @import("../internal_keys.zig").raft_document_applied_entry_key;
            const receipt_key = receiptKey(self.descriptor.descriptor.txn_id);
            const receipt = identity.encode();
            try incoming.upsert(alloc, namespace, self.storageKey(), self.encoded, false);
            try incoming.upsert(alloc, namespace, &marker_key, receipt[0..16], false);
            try incoming.upsert(alloc, namespace, &receipt_key, &receipt, false);
            const owner = self.pooled_owner.?;
            const progress = try owner.prepare_progress(owner.context, self, identity, null);
            try incoming.upsert(alloc, namespace, @import("completion_entry.zig").group_progress_key, &progress, false);
            var candidate = try backend.mutable.preparePublicationOwned(publication_alloc, &incoming);
            defer candidate.deinit(publication_alloc);
            var append = try wal.PreparedAppend.init(alloc, backend.root_dir.?, &incoming, true, .{ .segment_bytes = backend.options.wal_segment_bytes });
            defer append.deinit();
            const charge: u64 = @intCast(append.record.len);
            if (charge > self.wal_credit) return error.CompletionPlanCapacityExceeded;
            var wal_lock = try backend.acquireWalOperationLock(.exclusive);
            defer wal_lock.release();
            self.attempted = true;
            errdefer backend.fenceFailedBulkWal();
            try self.writeGuard();
            try self.wal_pin.manager.transferUsage(.lsm_wal_retention, &self.wal_credit, self.wal_credit - charge, &backend.tracked_wal_retention_bytes, backend.tracked_wal_retention_bytes + charge);
            const outcome = try backend.wal_retention.appendPrepared(self.io.storage(), alloc, &append, backend.writeStatsNowNs());
            const result = switch (outcome) {
                .appended => |result| result,
                .uncertain => |err| return err,
            };
            backend.write_stats.wal_append_records += 1;
            backend.write_stats.wal_append_entries += incoming.entryCount();
            backend.write_stats.wal_append_bytes += result.bytes;
            backend.noteMutableWalSegment(result.segment);
            try refreshCohortBaselines(backend, &incoming);
            backend.invalidateMutableReadSnapshot();
            backend.mutable.publishPrepared(&candidate);
            backend.syncTrackedInMemoryStateUsageCurrentLocked();
            self.baseline_slot_present = true;
            self.durable = true;
            self.attempted = false;
            owner.publish_progress(owner.context, identity);
        }

        /// One atomic physical mutation consumes its accepted reservation and
        /// becomes terminal immediately. No prepared descriptor or transaction
        /// receipt is published, and no later decision is needed to drain it.
        pub fn applyCanonicalMutation(self: *Self, backend: *Backend, operations: []const codec.Operation) !void {
            return self.runOperation(backend, .{operations}, applyCanonicalMutationInWorkspace);
        }

        fn applyCanonicalMutationInWorkspace(self: *Self, backend: *Backend, args: anytype, alloc: std.mem.Allocator) !void {
            const operations: []const codec.Operation = args[0];
            const identity = self.accepted_identity orelse return error.InvalidCompletionSlot;
            const owner = self.pooled_owner orelse return error.InvalidCompletionSlot;
            if (identity.term == 0 or identity.index == 0 or operations.len == 0 or operations.len > 256 or
                self.descriptor.descriptor.commit.len != 0 or self.descriptor.descriptor.abort.len != 0)
                return error.InvalidCompletionSlot;
            if (self.attempted or backend.manifest_recovery_required) return error.RecoveryRequired;
            if (self.durable) return;
            const progress = try owner.prepare_progress(owner.context, self, identity, null);
            self.attempted = true;
            errdefer backend.fenceFailedBulkWal();
            const namespace = @import("../backend_types.zig").Namespace{ .name = self.descriptor.descriptor.namespace };
            var delta: state.ActiveMemTable = .{};
            defer delta.deinit(alloc);
            for (operations) |op| {
                if (op.bindings.len != 0) return error.InvalidCompletionSlot;
                try delta.upsert(alloc, namespace, op.key, op.value, op.kind == .delete);
            }
            const marker = identity.encode();
            try delta.upsert(alloc, namespace, &@import("../internal_keys.zig").raft_document_applied_entry_key, marker[0..16], false);
            try delta.upsert(alloc, namespace, @import("completion_entry.zig").group_progress_key, &progress, false);
            try delta.upsert(alloc, namespace, self.appliedKey(), &self.descriptor.descriptor.txn_id, false);
            try backend.checkCompletionPoolFootprint(&delta);
            try self.writeGuard();
            try self.drain(backend, &delta, true, true, alloc);
            self.durable = true;
            self.attempted = false;
            owner.publish_progress(owner.context, identity);
        }

        fn captureBaseline(backend: *Backend, namespace: ?[]const u8, key: []const u8, comptime size: usize) !?[size]u8 {
            const raw = backend.getMergedWithMutable(&backend.mutable, .{ .name = namespace }, key) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            if (raw.len != size) return error.InvalidTxnRecord;
            return raw[0..size].*;
        }

        /// No SST lookup after prepare. Flush remains excluded while live;
        /// changed metadata is mutable, otherwise the captured base is valid.
        pub fn currentMetadata(self: *Self, backend: *Backend, key: []const u8, baseline: ?[]const u8) ?[]const u8 {
            if (backend.mutable.findIndex(.{ .name = self.descriptor.descriptor.namespace }, key)) |index| {
                const entry = backend.mutable.entryAt(index);
                return if (entry.tombstone) null else entry.value;
            }
            return baseline;
        }

        pub fn destroy(self: *Self) void {
            const alloc = self.allocator;
            const pub_domain = self.publication;
            const scratch_domain = self.scratch;
            const pooled_owner = self.pooled_owner;
            if (pooled_owner == null) self.io.deinit() catch unreachable;
            const manager = self.wal_pin.manager;
            if (pooled_owner == null) manager.observeUsage(.lsm_wal_retention, &self.wal_credit, 0);
            if (self.owns_observer_pins) {
                self.wal_pin.release() catch unreachable;
                self.memory_pin.release() catch unreachable;
            }
            alloc.free(self.guard_path);
            self.descriptor.deinit();
            alloc.free(self.encoded);
            alloc.free(self.guard_encoded);
            alloc.free(self.journal_path);
            for (&self.run_path_pins) |*pin| if (pin.*) |*held| held.release();
            for (self.run_paths) |path| if (path) |owned| alloc.free(owned);
            if (pooled_owner == null) alloc.destroy(self);
            scratch_domain.release();
            pub_domain.release();
            if (pooled_owner) |owner| owner.release_cell(owner.context, self);
        }

        pub fn create(backend: *Backend, encoded: []const u8, restored: bool, cohort: guard.Info) !*Self {
            const native = backend.storage_owner orelse return error.UnsupportedCompletionBackend;
            const root = backend.root_dir orelse return error.UnsupportedCompletionBackend;
            const manager = backend.options.resource_manager orelse return error.CompletionResourceManagerRequired;
            if (backend.options.backend.read_only or backend.options.backend.durability != .full or
                !backend.options.wal_enabled or !backend.options.wal_sync_on_commit or
                backend.storage.?.ptr != native.storage().ptr or backend.storage.?.vtable != native.storage().vtable)
                return error.UnsupportedCompletionBackend;
            // Two descriptors remain reserved through completion. The ordinary
            // prepare WAL append needs an independent parent/file descriptor pair.
            // Account for persistent root locks and native open headroom too.
            if (native.usableTransientDescriptorCapacity() < 4) return error.DescriptorAdmissionExhausted;
            var checked = try codec.decode(backend.allocator, encoded, .{});
            defer checked.deinit();
            if ((if (checked.descriptor.namespace) |name| !std.mem.eql(u8, name, "docs") else false) or !std.meta.eql(checked.descriptor.limits, limits))
                return error.UnsupportedCompletionProfile;
            if (backend.manifest_recovery_required) return error.RecoveryRequired;
            const append_reserve = try (try outcome_append_budget.repeated(cohort.capacity())).plus(foreground_append_budget);
            try append_reserve.requireHeadroom(backend.write_stats, .{});
            if (backend.bulkIngestActive() or backend.activeImmutableMemtableCount() != 0 or
                backend.immutable_flush_build_in_flight or backend.manifest_publish_in_flight or
                backend.manifest_checkpoint_build_in_flight or backend.runs.count() > @as(usize, cohort.initial_runs) + cohort.capacity() or
                backend.manifest_journal.sequence == null or backend.manifest_journal.active_segment == 0)
                return error.CompletionReservationBusy;
            if (backend.mutable.estimatedLogicalBytes() > foreground_bytes or backend.mutable.entryCount() > foreground_entries)
                return error.CompletionForegroundCapacityExceeded;
            const txn_id = checked.descriptor.txn_id;
            const record_prefix = "\x00\x00__txn_records__:";
            const credit_prefix = "\x00\x00__txn_completion_v1__:";
            var record_key: [record_prefix.len + 16]u8 = undefined;
            @memcpy(record_key[0..record_prefix.len], record_prefix);
            @memcpy(record_key[record_prefix.len..], &txn_id);
            var credit_key: [credit_prefix.len + 16]u8 = undefined;
            @memcpy(credit_key[0..credit_prefix.len], credit_prefix);
            @memcpy(credit_key[credit_prefix.len..], &txn_id);
            const sibling_namespace: ?[]const u8 = if (checked.descriptor.namespace == null) "docs" else null;
            if (backend.getMergedWithMutable(&backend.mutable, .{ .name = sibling_namespace }, storage_keys[cohort.index])) |_| return error.InvalidCompletionSlot else |err| {
                if (err != error.NotFound) return err;
            }
            const baseline_slot_present = if (backend.getMergedWithMutable(&backend.mutable, .{ .name = checked.descriptor.namespace }, storage_keys[cohort.index])) |stored| blk: {
                if (!std.mem.eql(u8, stored, encoded)) return error.InvalidCompletionSlot;
                break :blk true;
            } else |err| switch (err) {
                error.NotFound => false,
                else => return err,
            };
            const baseline_applied = try captureBaseline(backend, checked.descriptor.namespace, applied_keys[cohort.index], 16);
            const manifest_completed = !baseline_slot_present and if (baseline_applied) |marker| std.mem.eql(u8, &marker, &txn_id) else false;
            if (cohort.legacy and backend.runs.count() > 64 and !manifest_completed) return error.CompletionReservationBusy;
            if (!restored and manifest_completed) return error.InvalidCompletionSlot;
            const baseline_record = try captureBaseline(backend, checked.descriptor.namespace, &record_key, 53);
            const baseline_credit = try captureBaseline(backend, checked.descriptor.namespace, &credit_key, 16);
            const baseline_summary = try captureBaseline(backend, checked.descriptor.namespace, "\x00\x00__metadata__:txn_completion_v1", 16);
            const publication = try domains.Arena.create(backend.allocator, manager, publication_bytes);
            errdefer publication.release();
            const scratch = try domains.Arena.create(backend.allocator, manager, scratch_bytes);
            errdefer scratch.release();
            const alloc = publication.allocator();
            const self = try alloc.create(Self);
            errdefer alloc.destroy(self);
            var descriptor = try codec.decode(alloc, encoded, .{});
            errdefer descriptor.deinit();
            const wire = try alloc.dupe(u8, encoded);
            errdefer alloc.free(wire);
            const guard_wire = if (cohort.legacy) try alloc.dupe(u8, encoded) else try guard.encode(alloc, cohort, encoded);
            errdefer alloc.free(guard_wire);
            // The cohort anchor owns the stable backend observer pins. Siblings
            // borrow their manager access and are destroyed before the anchor.
            const owns_observer_pins = backend.durable_completion == null;
            var memory_pin = if (backend.durable_completion) |first| first.memory_pin else try manager.pinObserverMetadata(.lsm_in_memory_state, &backend.tracked_in_memory_state_bytes);
            errdefer if (owns_observer_pins) memory_pin.release() catch unreachable;
            var wal_pin = if (backend.durable_completion) |first| first.wal_pin else try manager.pinObserverMetadata(.lsm_wal_retention, &backend.tracked_wal_retention_bytes);
            errdefer if (owns_observer_pins) wal_pin.release() catch unreachable;
            self.wal_credit = 0;
            try manager.adjustUsage(.lsm_wal_retention, &self.wal_credit, limits.wal_bytes);
            errdefer manager.observeUsage(.lsm_wal_retention, &self.wal_credit, 0);
            const guard_path = try std.fs.path.join(alloc, &.{ root, guard_filenames[cohort.index] });
            errdefer alloc.free(guard_path);
            const legacy_path = try std.fs.path.join(alloc, &.{ root, "wal.log" });
            defer alloc.free(legacy_path);
            const replay_index = try std.fs.path.join(alloc, &.{ root, "wal", "replay.index" });
            defer alloc.free(replay_index);
            const replay_segments = try std.fs.path.join(alloc, &.{ root, "wal", "replay.segments" });
            defer alloc.free(replay_segments);
            if (!restored) {
                if (backend.storage.?.fileSize(guard_path)) |_| return error.CompletionReservationBusy else |err| {
                    if (err != error.FileNotFound) return err;
                }
            }
            const run_id = cohort.base_run_id;
            const next_run_id = try std.math.add(u64, run_id, cohort.capacity());
            var run_paths: [max_slots]?[]u8 = @splat(null);
            errdefer for (run_paths) |path| {
                if (path) |owned| alloc.free(owned);
            };
            for (0..cohort.capacity()) |i| run_paths[i] = try repository.runPath(alloc, root, run_id + i);
            var run_path_pins: [max_slots]?Backend.CompletionRunPathPin = @splat(null);
            errdefer for (&run_path_pins) |*pin| if (pin.*) |*held| held.release();
            for (0..cohort.capacity()) |i| run_path_pins[i] = try Backend.pinCompletionRunPath(alloc, run_paths[i].?);
            const run_path = run_paths[0].?;
            const journal_path = try manifest_set.pathAlloc(alloc, root, backend.manifest_journal.active_segment, .journal);
            errdefer alloc.free(journal_path);
            const journal_size = try backend.storage.?.fileSize(journal_path);
            // A single output's metadata and namespace/key bounds must fit
            // without journal rotation after prepare.
            if (journal_size > repository.maxManifestReadBytes() - cohort.capacity() * (limits.max_encoded_bytes + 64 * 1024))
                return error.CompletionReservationBusy;
            var files: [max_slots + 5]storage_io.NativeCompletionIo.FileSpec = undefined;
            files[0] = .{ .path = guard_path, .max_bytes = limits.max_encoded_bytes + guard.header_bytes, .allow_delete = true };
            files[1] = .{ .path = legacy_path, .max_bytes = limits.wal_bytes, .allow_delete = true };
            files[2] = .{ .path = replay_index, .max_bytes = 4096 };
            files[3] = .{ .path = replay_segments, .max_bytes = limits.wal_bytes };
            files[4] = .{ .path = journal_path, .max_bytes = repository.maxManifestReadBytes(), .allow_append = true };
            for (0..cohort.capacity()) |i| files[5 + i] = .{ .path = run_paths[i].?, .max_bytes = limits.flush_bytes, .allow_delete = true };
            const io = try storage_io.NativeCompletionIo.createWithFilesAndHeadroom(alloc, native, root, files[0 .. 5 + cohort.capacity()], 2);
            errdefer io.deinit() catch unreachable;
            io.allow_wal_reset = true;
            self.* = .{ .allocator = alloc, .publication = .{ .arena = publication }, .scratch = .{ .arena = scratch }, .descriptor = descriptor, .encoded = wire, .guard_encoded = guard_wire, .cohort = cohort, .run_paths = run_paths, .run_path_pins = run_path_pins, .io = io, .wal_credit = self.wal_credit, .memory_pin = memory_pin, .wal_pin = wal_pin, .owns_observer_pins = owns_observer_pins, .guard_path = guard_path, .wal_append_start = backend.write_stats.wal_append_bytes, .wal_entries_start = backend.write_stats.wal_append_entries, .wal_records_start = backend.write_stats.wal_append_records, .run_id = run_id, .journal_path = journal_path, .run_path = run_path, .durable = restored, .baseline_record = baseline_record, .baseline_credit = baseline_credit, .baseline_summary = baseline_summary, .baseline_applied = baseline_applied, .baseline_slot_present = baseline_slot_present };
            if (!restored) {
                // Any storage failure may have published this durable anchor.
                // Never let an uncertain create resume ordinary mutation.
                self.writeGuard() catch |err| {
                    backend.fenceFailedBulkWal();
                    return err;
                };
            }
            backend.next_run_id = @max(backend.next_run_id, next_run_id);
            return self;
        }

        pub fn storageKey(self: *const Self) []const u8 {
            return storage_keys[self.cohort.index];
        }
        pub fn appliedKey(self: *const Self) []const u8 {
            return applied_keys[self.cohort.index];
        }

        fn refreshField(self: *Self, backend: *Backend, delta: anytype, key: []const u8, comptime n: usize, baseline: *?[n]u8) !void {
            const value = if (delta.findIndex(.{ .name = self.descriptor.descriptor.namespace }, key)) |index| blk: {
                const entry = delta.entryAt(index);
                break :blk if (entry.tombstone) null else entry.value;
            } else self.currentMetadata(backend, key, if (baseline.*) |*bytes| bytes else null);
            if (value) |bytes| {
                if (bytes.len != n) return error.InvalidTxnRecord;
                baseline.* = bytes[0..n].*;
            } else baseline.* = null;
        }

        fn refreshCohortBaselines(backend: *Backend, delta: anytype) !void {
            for (backend.durable_completion_members) |maybe| if (maybe) |member| {
                const id = member.descriptor.descriptor.txn_id;
                const record = "\x00\x00__txn_records__:".* ++ id;
                const credit = "\x00\x00__txn_completion_v1__:".* ++ id;
                try member.refreshField(backend, delta, &record, 53, &member.baseline_record);
                try member.refreshField(backend, delta, &credit, 16, &member.baseline_credit);
                try member.refreshField(backend, delta, "\x00\x00__metadata__:txn_completion_v1", 16, &member.baseline_summary);
                try member.refreshField(backend, delta, member.appliedKey(), 16, &member.baseline_applied);
                if (delta.findIndex(.{ .name = member.descriptor.descriptor.namespace }, member.storageKey())) |index| {
                    member.baseline_slot_present = !delta.entryAt(index).tombstone;
                } else member.baseline_slot_present = member.currentMetadata(backend, member.storageKey(), if (member.baseline_slot_present) member.encoded else null) != null;
            };
        }

        fn writeGuard(self: *Self) !void {
            var writer = try self.io.storage().beginAtomicWrite(self.allocator, self.guard_path);
            var owned = true;
            errdefer if (owned) writer.abort();
            try writer.appendSlice(self.guard_encoded);
            owned = false;
            try writer.finish();
        }

        pub fn clearGuard(self: *Self) !void {
            try self.io.storage().deleteFileAbsolute(self.guard_path);
            try self.io.storage().syncParentAbsolute(self.guard_path);
        }

        pub fn checkOrdinary(self: *Self, backend: *Backend, incoming: *const state.ActiveMemTable, bulk: *const state.State) !void {
            if (bulk.entryCount() != 0) return error.CompletionForegroundCapacityExceeded;
            try self.checkWalInput(backend, incoming);
        }

        /// Shared by transaction admission and raw native WAL helpers. Direct
        /// callers cannot alter the anchor/marker or evade cumulative bounds.
        pub fn checkWalInput(self: *Self, backend: *Backend, incoming: anytype) !void {
            if (self.attempted and !self.retired) return error.RecoveryRequired;
            if (self.pooled_owner == null) {
                // Ordinary prepare belongs to the foreground allowance. Keep
                // every reserved standalone outcome's increments untouched.
                const reserve = try outcome_append_budget.repeated(self.cohort.capacity());
                reserve.requireHeadroom(backend.write_stats, incomingAppendCounters(incoming)) catch return error.CompletionForegroundCapacityExceeded;
            }
            const append_bytes = backend.write_stats.wal_append_bytes -| self.wal_append_start;
            const incoming_bytes = wal.encodedStateRecordLen(incoming);
            if (self.replayed_wal_bytes +| append_bytes +| incoming_bytes > recovery_wal_bytes or
                self.replayed_wal_entries +| (backend.write_stats.wal_append_entries -| self.wal_entries_start) +| incoming.entryCount() > foreground_entries or
                self.replayed_wal_records +| (backend.write_stats.wal_append_records -| self.wal_records_start) >= recovery_wal_records)
                return error.CompletionForegroundCapacityExceeded;
            if (incoming.estimatedLogicalBytes() > foreground_bytes -| backend.mutable.estimatedLogicalBytes() or
                incoming.entryCount() > foreground_entries -| backend.mutable.entryCount()) return error.CompletionForegroundCapacityExceeded;
            for (0..incoming.entryCount()) |index| {
                const entry = incoming.entryAt(index);
                for (backend.durable_completion_members) |maybe| if (maybe) |member| {
                    if (member.retired) continue;
                    const ns = member.descriptor.descriptor.namespace;
                    if (ns == null and entry.namespace_name != null or ns != null and entry.namespace_name == null) continue;
                    if (ns != null and !std.mem.eql(u8, ns.?, entry.namespace_name.?)) continue;
                    for ([_][]const codec.Operation{ member.descriptor.descriptor.commit, member.descriptor.descriptor.abort }) |ops| for (ops) |op| {
                        if (std.mem.eql(u8, op.key, entry.key) and !codec.isSharedDynamicOperation(op) and !member.permitsFootprintWrite(backend, incoming, entry)) return error.PreparedCompletionActive;
                    };
                };
                for (applied_keys) |key| if (std.mem.eql(u8, entry.key, key)) return error.PreparedCompletionActive;
                for (storage_keys, 0..) |key, member_index| if (std.mem.eql(u8, entry.key, key)) {
                    const member = backend.durable_completion_members[member_index] orelse return error.PreparedCompletionActive;
                    const namespace = member.descriptor.descriptor.namespace;
                    const same_namespace = if (namespace) |name| if (entry.namespace_name) |incoming_name| std.mem.eql(u8, name, incoming_name) else false else entry.namespace_name == null;
                    if (!same_namespace or member.durable or member.retired or entry.tombstone or !std.mem.eql(u8, entry.value, member.encoded)) return error.PreparedCompletionActive;
                };
            }
        }

        pub fn validateCanonicalFootprint(backend: *Backend, namespace: ?[]const u8, operations: []const codec.Operation) !void {
            for (backend.durable_completion_members) |maybe| if (maybe) |member| {
                if (member.retired) continue;
                const other_namespace = member.descriptor.descriptor.namespace;
                if (namespace == null and other_namespace != null or namespace != null and other_namespace == null) continue;
                if (namespace != null and !std.mem.eql(u8, namespace.?, other_namespace.?)) continue;
                for (operations) |op| for ([_][]const codec.Operation{ member.descriptor.descriptor.commit, member.descriptor.descriptor.abort }) |owned| for (owned) |other| {
                    if (std.mem.eql(u8, op.key, other.key) and !codec.isSharedDynamicOperation(other)) return error.PreparedCompletionActive;
                };
            };
        }

        pub fn validateFootprint(backend: *Backend, descriptor: codec.Descriptor) !void {
            for ([_][]const codec.Operation{ descriptor.commit, descriptor.abort }) |ops| for (ops) |op| {
                for (storage_keys) |key| if (std.mem.eql(u8, op.key, key)) return error.UnsupportedCompletionTemplate;
                for (applied_keys) |key| if (std.mem.eql(u8, op.key, key)) return error.UnsupportedCompletionTemplate;
                if (std.mem.eql(u8, op.key, @import("completion_entry.zig").group_progress_key) or std.mem.startsWith(u8, op.key, receipt_prefix)) return error.UnsupportedCompletionTemplate;
                const shared = codec.isSharedDynamicOperation(op);
                for (op.bindings) |binding| if (binding.target == .key and !shared) return error.UnsupportedCompletionTemplate;
                for (backend.durable_completion_members) |maybe| if (maybe) |member| {
                    if (member.retired) continue;
                    const a = descriptor.namespace;
                    const b = member.descriptor.descriptor.namespace;
                    if (a == null and b != null or a != null and b == null) continue;
                    if (a != null and !std.mem.eql(u8, a.?, b.?)) continue;
                    for ([_][]const codec.Operation{ member.descriptor.descriptor.commit, member.descriptor.descriptor.abort }) |other_ops| for (other_ops) |other| {
                        if (std.mem.eql(u8, op.key, other.key) and !(shared and codec.isSharedDynamicOperation(other))) return error.PreparedCompletionActive;
                    };
                };
            };
        }

        fn permitsFootprintWrite(self: *Self, backend: *Backend, incoming: anytype, entry: state.OwnedEntry) bool {
            const txn_id = self.descriptor.descriptor.txn_id;
            if (!self.durable) {
                const descriptor_index = incoming.findIndex(.{ .name = self.descriptor.descriptor.namespace }, self.storageKey()) orelse return false;
                const descriptor_entry = incoming.entryAt(descriptor_index);
                if (descriptor_entry.tombstone or !std.mem.eql(u8, descriptor_entry.value, self.encoded)) return false;
                const record_key = "\x00\x00__txn_records__:".* ++ txn_id;
                const record_index = incoming.findIndex(.{ .name = self.descriptor.descriptor.namespace }, &record_key) orelse return false;
                const record = incoming.entryAt(record_index);
                if (record.tombstone or record.value.len != 53 or record.value[0] != 0 or record.value[49] != 1 or record.value[52] != 0 or
                    std.mem.readInt(u64, record.value[33..41], .little) != self.descriptor.descriptor.intent_revision) return false;
                if (codec.isPreparedTransactionMetadataKey(entry.key, txn_id)) return !entry.tombstone;
                return std.mem.startsWith(u8, entry.key, "\x00\x00__txn_intent_locks__:") and !entry.tombstone and std.mem.eql(u8, entry.value, &txn_id);
            }
            const record_key = "\x00\x00__txn_records__:".* ++ txn_id;
            if (!std.mem.eql(u8, entry.key, &record_key) or entry.tombstone or entry.value.len != 53) return false;
            const before = self.currentMetadata(backend, &record_key, if (self.baseline_record) |*record| record else null) orelse return false;
            if (before.len != 53 or before[0] > 2 or entry.value[0] > 2 or (before[0] != 0 and entry.value[0] != before[0])) return false;
            // Ordinary decision persistence may bind decision timestamps, but
            // cannot change intent revision, participants, profile or resolve phase.
            for (0..53) |i| {
                if (i == 0 or (i >= 9 and i < 17) or (i >= 25 and i < 33)) continue;
                if (entry.value[i] != before[i]) return false;
            }
            return true;
        }

        /// Every heap allocation below partitions slabs physically obtained
        /// before prepare. The existing mutable is bounded while this slot is
        /// live; one merged SST drains both intervening writes and completion.
        pub fn complete(self: *Self, backend: *Backend, commit: bool, values: Values) !void {
            return self.runOperation(backend, .{ commit, values }, completeInWorkspace);
        }

        fn completeInWorkspace(self: *Self, backend: *Backend, args: anytype, alloc: std.mem.Allocator) !void {
            const commit: bool = args[0];
            const values: Values = args[1];
            if (!self.durable) return error.CompletionNotPrepared;
            if (self.attempted or backend.manifest_recovery_required) return error.RecoveryRequired;
            const group_progress = if (self.pooled_owner) |owner| try owner.prepare_progress(owner.context, self, .{
                .term = values.raft_term,
                .index = values.raft_index,
                .digest = values.canonical_payload_digest,
            }, commit) else null;
            // The bump domains are single-attempt. Even a pre-I/O allocation
            // failure must not allow an unbounded sequence of retries.
            self.attempted = true;
            errdefer backend.fenceFailedBulkWal();
            var delta: state.ActiveMemTable = .{};
            defer delta.deinit(alloc);
            const operations = if (commit) self.descriptor.descriptor.commit else self.descriptor.descriptor.abort;
            for (operations) |op| {
                const key = try alloc.dupe(u8, op.key);
                defer alloc.free(key);
                const value = try alloc.dupe(u8, op.value);
                defer alloc.free(value);
                for (op.bindings) |binding| {
                    const number = switch (binding.kind) {
                        .commit_timestamp => values.commit_timestamp,
                        .replay_sequence => values.replay_sequence,
                        .replay_next_sequence => try std.math.add(u64, values.replay_sequence, 1),
                        .shared_ledger_count => values.shared_ledger_count,
                        .shared_ledger_bytes => values.shared_ledger_bytes,
                        .raft_term => if (values.raft_term != 0) values.raft_term else return error.InvalidCompletionSlot,
                        .raft_index => if (values.raft_index != 0) values.raft_index else return error.InvalidCompletionSlot,
                    };
                    const bytes = if (binding.target == .key) key else value;
                    const field: *[8]u8 = bytes[binding.offset..][0..8];
                    switch (binding.byte_order) {
                        .little => std.mem.writeInt(u64, field, number, .little),
                        .big => std.mem.writeInt(u64, field, number, .big),
                    }
                }
                if (op.kind == .put) try codec.finishBoundValue(op, value);
                try delta.upsert(alloc, .{ .name = self.descriptor.descriptor.namespace }, key, value, op.kind == .delete);
            }
            if (self.pooled_owner != null) {
                const receipt_key = receiptKey(self.descriptor.descriptor.txn_id);
                try delta.upsert(alloc, .{ .name = self.descriptor.descriptor.namespace }, &receipt_key, "", true);
                try delta.upsert(alloc, .{ .name = self.descriptor.descriptor.namespace }, @import("completion_entry.zig").group_progress_key, &group_progress.?, false);
            }
            try delta.upsert(alloc, .{ .name = self.descriptor.descriptor.namespace }, self.storageKey(), "", true);
            try delta.upsert(alloc, .{ .name = self.descriptor.descriptor.namespace }, self.appliedKey(), &self.descriptor.descriptor.txn_id, false);
            try backend.checkCompletionPoolFootprint(&delta);
            try self.drain(backend, &delta, true, true, alloc);
            if (self.pooled_owner) |owner| owner.publish_progress(owner.context, .{ .term = values.raft_term, .index = values.raft_index, .digest = values.canonical_payload_digest });
        }

        /// Replay has already applied the whole atomic completion record. Drain
        /// its current result; never bind the transaction templates a second time.
        pub fn finishReplayed(self: *Self, backend: *Backend) !void {
            return self.runOperation(backend, .{}, finishReplayedInWorkspace);
        }

        fn finishReplayedInWorkspace(self: *Self, backend: *Backend, _: anytype, alloc: std.mem.Allocator) !void {
            if (self.attempted) return error.RecoveryRequired;
            self.attempted = true;
            errdefer backend.fenceFailedBulkWal();
            var delta: state.ActiveMemTable = .{};
            defer delta.deinit(alloc);
            // A tombstone keeps the drain nonempty even after a prior manifest
            // survived and WAL reset completed before guard unlink failed.
            try delta.upsert(alloc, .{ .name = self.descriptor.descriptor.namespace }, self.storageKey(), "", true);
            try self.drain(backend, &delta, false, true, alloc);
        }

        /// The manifest already contains the completion marker and no slot
        /// descriptor. No foreground work can run while this guard exists, so
        /// replay contains only the already-manifested prefix. Avoid another
        /// SST on every retry of checkpoint/guard cleanup.
        pub fn finishManifested(self: *Self, backend: *Backend) !void {
            return self.runOperation(backend, .{}, finishManifestedInWorkspace);
        }

        fn finishManifestedInWorkspace(self: *Self, backend: *Backend, _: anytype, alloc: std.mem.Allocator) !void {
            if (self.attempted or self.baseline_slot_present) return error.InvalidCompletionSlot;
            const marker = self.baseline_applied orelse return error.InvalidCompletionSlot;
            if (!std.mem.eql(u8, &marker, &self.descriptor.descriptor.txn_id)) return error.InvalidCompletionSlot;
            self.attempted = true;
            errdefer backend.fenceFailedBulkWal();
            var wal_lock = try backend.acquireWalOperationLock(.exclusive);
            defer wal_lock.release();
            try self.checkpointAndClearGuard(backend, true, alloc);
            backend.invalidateMutableReadSnapshot();
            backend.mutable.deinit(backend.allocator);
            backend.mutable = .{};
            backend.mutable_wal_range = .{};
            backend.clearPublishedWalLogicalDebtLocked();
            backend.syncTrackedInMemoryStateUsageCurrentLocked();
        }

        fn drain(self: *Self, backend: *Backend, delta: *state.ActiveMemTable, append_wal: bool, retire_guard: bool, alloc: std.mem.Allocator) !void {
            const pub_alloc = self.publication.allocator();
            const output_index = if (self.cohort.legacy) @as(usize, 0) else backend.runs.count() -| self.cohort.initial_runs;
            if (output_index >= self.cohort.capacity()) return error.CompletionPlanCapacityExceeded;
            self.run_id = self.cohort.base_run_id + output_index;
            self.run_path = self.run_paths[output_index].?;
            var prepared_append = try wal.PreparedAppend.init(alloc, backend.root_dir.?, delta, true, .{ .segment_bytes = backend.options.wal_segment_bytes });
            defer prepared_append.deinit();
            var completion = try delta.toStateMove(alloc);
            defer completion.deinit(alloc);
            var current = try backend.mutable.snapshot(alloc);
            defer current.deinit(alloc);
            const Build = struct {
                allocator: std.mem.Allocator,
                storage: ?storage_io.Storage,
                root_dir: ?[]u8,
                options: @TypeOf(backend.options),
                next_run_id: u64,
            };
            var build = Build{ .allocator = alloc, .storage = self.io.storage(), .root_dir = backend.root_dir, .options = backend.options, .next_run_id = self.run_id };
            build.options.resource_manager = null; // Already physically charged to the dedicated slab.
            // This slot owns the predeclared output until publication or
            // restart reconciliation; ordinary cleanup admission is forbidden.
            build.options.unpublished_outputs = null;
            var manifest_attempted = false;
            errdefer if (!manifest_attempted) self.io.storage().deleteFileAbsolute(self.run_path) catch {};
            build.options.max_run_file_bytes = limits.flush_bytes;
            build.options.max_run_file_physical_bytes = limits.flush_bytes;
            build.options.max_run_file_entries = 0;
            build.options.run_partition_prefix_bytes = 0;
            build.options.run_partition_key = null;
            var states = [_]*const state.State{ &completion, &current };
            var wal_lock = try backend.acquireWalOperationLock(.exclusive);
            defer wal_lock.release();
            if (append_wal) {
                const charge: u64 = @intCast(prepared_append.record.len);
                if (charge > self.wal_credit) return error.CompletionPlanCapacityExceeded;
                try self.wal_pin.manager.transferUsage(.lsm_wal_retention, &self.wal_credit, self.wal_credit - charge, &backend.tracked_wal_retention_bytes, backend.tracked_wal_retention_bytes + charge);
                const outcome = try backend.wal_retention.appendPrepared(self.io.storage(), alloc, &prepared_append, backend.writeStatsNowNs());
                const result = switch (outcome) {
                    .appended => |result| result,
                    .uncertain => |err| return err,
                };
                backend.write_stats.wal_append_records += 1;
                backend.write_stats.wal_append_entries += completion.entryCount();
                backend.write_stats.wal_append_bytes += result.bytes;
                if (builtin.is_test) if (test_after_wal) |hook| if (hook()) return error.RecoveryRequired;
            }
            var run = if (self.drain_workspace != null) bounded: {
                // The entire operation already owns the empty compiler scope;
                // replay/readers retain their independent scratch allocation.
                var output = try @import("completion_maintenance.zig").buildStateDrain(alloc, self.io.storage(), backend.root_dir.?, &states, self.run_id, self.drain_limits);
                defer output.deinit(alloc);
                break :bounded try repository.cloneRunCompactionSnapshot(pub_alloc, output);
            } else legacy: {
                // Standalone reservations retain their existing generic writer;
                // the fixed pooled-workspace certificate does not cover it.
                var built = try compaction.buildRunsFromStatesBorrowedWithReservedIds(Build, &build, states[0..if (current.entryCount() == 0) @as(usize, 1) else 2], self.run_id, self.run_id + 1);
                defer {
                    for (built.items) |*item| item.deinit(alloc);
                    built.deinit(alloc);
                }
                if (built.items.len != 1) return error.CompletionDrainShapeChanged;
                break :legacy try repository.cloneRunCompactionSnapshot(pub_alloc, built.items[0]);
            };
            run.metadata_allocator = pub_alloc;
            var run_owned = true;
            defer if (run_owned) run.deinit(backend.allocator);
            var candidate = backend.runs.fork();
            var candidate_owned = true;
            errdefer if (candidate_owned) candidate.deinit(backend.allocator);
            try candidate.append(pub_alloc, run);
            run_owned = false;
            const directory = try backend.run_directory.?.fork(pub_alloc);
            var directory_owned = true;
            errdefer if (directory_owned) directory.destroy(backend.allocator);
            const View = struct {
                allocator: std.mem.Allocator,
                options: @TypeOf(backend.options),
                pub fn retainRunSnapshotRef(_: *@This(), entry: *repository.Run) !void {
                    try Backend.retainRunSnapshotRef(undefined, entry);
                }
                pub fn releaseDirectoryRunSnapshotRef(entry: *repository.Run) void {
                    Backend.releaseDirectoryRunSnapshotRef(entry);
                }
            };
            var view = View{ .allocator = pub_alloc, .options = backend.options };
            try directory.put(&view, candidate.find(&run).?.*);
            const durable_directory = try directory.fork(pub_alloc);
            var durable_directory_owned = true;
            errdefer if (durable_directory_owned) durable_directory.destroy(backend.allocator);
            var meta = repository.runMeta(run);
            meta.path = repository.manifestRelativePath(backend.root_dir.?, meta.path);
            const sequence = try std.math.add(u64, backend.manifest_journal.sequence.?, 1);
            const frame = try manifest.encodeSingleRunJournalFrameAlloc(alloc, sequence, backend.next_run_id, meta);
            defer alloc.free(frame);
            // An uncertain manifest append may already reference this SST.
            manifest_attempted = true;
            try self.io.storage().appendFileAbsolute(alloc, self.journal_path, frame, true);
            if (builtin.is_test) if (test_after_manifest) |hook| if (hook()) return error.RecoveryRequired;
            try refreshCohortBaselines(backend, &completion);
            // The full current mutable and completion delta are durable in
            // the same manifest edit. Publish their replacement atomically.
            backend.invalidateMutableReadSnapshot();
            backend.invalidateReadVersion();
            std.mem.swap(@TypeOf(backend.runs), &backend.runs, &candidate);
            candidate.deinit(backend.allocator);
            candidate_owned = false;
            backend.publishRunDirectory(directory);
            directory_owned = false;
            backend.publishManifestDirectory(durable_directory);
            durable_directory_owned = false;
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
            try self.checkpointAndClearGuard(backend, retire_guard, alloc);
        }

        pub fn finishCheckpointCut(self: *Self, backend: *Backend) !void {
            return self.runOperation(backend, .{}, finishCheckpointCutInWorkspace);
        }

        fn finishCheckpointCutInWorkspace(self: *Self, backend: *Backend, _: anytype, alloc: std.mem.Allocator) !void {
            var wal_lock = try backend.acquireWalOperationLock(.exclusive);
            defer wal_lock.release();
            try self.checkpointAndClearGuard(backend, false, alloc);
        }

        fn checkpointAndClearGuard(self: *Self, backend: *Backend, retire_guard: bool, alloc: std.mem.Allocator) !void {
            // Only the durable manifest authorizes retiring WAL recovery data.
            try wal.protectedReset(self.io.storage(), alloc, backend.root_dir.?);
            backend.wal_retention.primary = .{ .oldest_retained_segment = 1, .current_segment = 1 };
            backend.wal_retention.replay = .{ .current_segment = 1 };
            backend.wal_retention.primary_ns = backend.writeStatsNowNs();
            backend.wal_retention.replay_ns = backend.writeStatsNowNs();
            if (self.pooled_owner) |owner| try owner.restore_wal_credits_after_checkpoint(owner.context, backend);
            self.wal_pin.manager.observeUsage(.lsm_wal_retention, &backend.tracked_wal_retention_bytes, 0);
            _ = retire_guard; // Cohort retirement clears all guards only when every member is terminal.
            for (backend.durable_completion_members) |maybe| if (maybe) |member| {
                member.wal_append_start = backend.write_stats.wal_append_bytes;
                member.wal_entries_start = backend.write_stats.wal_append_entries;
                member.wal_records_start = backend.write_stats.wal_append_records;
                member.replayed_wal_bytes = 0;
                member.replayed_wal_entries = 0;
                member.replayed_wal_records = 0;
            };
        }
    };
}

test "workload admission completion operation workspace rejects cumulative costs beyond a valid writer span" {
    const capacity = @import("completion_capacity.zig");
    const maintenance = @import("completion_maintenance.zig");
    const cost = try capacity.Cost.record("docs".len, 128, 1024);
    const normal = try operationWorkspaceRequirement(cost, .{ .max_output_file_bytes = limits.flush_bytes });
    try std.testing.expect(normal.total <= scratch_bytes);
    try std.testing.expect(normal.largest_span <= normal.total);
    var rejected_sum = false;
    for (16..128) |steps| {
        const shape: maintenance.Limits = .{ .max_metadata_bytes = steps * 64 * 1024, .max_output_metadata_bytes = steps * 64 * 1024, .max_output_file_bytes = limits.flush_bytes };
        _ = maintenance.drainWorkspaceRequirement(cost, shape) catch continue;
        _ = operationWorkspaceRequirement(cost, shape) catch |err| {
            try std.testing.expectEqual(error.UnsupportedCompletionProfile, err);
            rejected_sum = true;
            break;
        };
    }
    try std.testing.expect(rejected_sum);
    // Namespace and key may together almost fill the maximum physical record.
    const widest = try capacity.Cost.record("docs".len, limits.max_encoded_bytes - 64, 0);
    const bound = try operationWorkspaceRequirement(widest, .{ .max_output_file_bytes = limits.flush_bytes });
    try std.testing.expect(bound.manifest_frame >= 2 * widest.max_key_bytes);
    try std.testing.expect(bound.total <= scratch_bytes);
}
