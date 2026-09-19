// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! One internal native completion slot. The descriptor is published by the
//! transaction prepare batch; opening restores its physical ownership before
//! foreground admission. Unsupported providers and resource shapes fail closed.
const std = @import("std");
const builtin = @import("builtin");
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

pub const applied_key = "\x00\x00__metadata__:completion_applied_v1";
pub const guard_filename = "completion-slot.guard";
pub var test_after_wal: ?*const fn () bool = null;
pub var test_after_manifest: ?*const fn () bool = null;
pub const storage_key = "\x00\x00__metadata__:completion_slot_v1";
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

pub const Values = struct {
    commit_timestamp: u64,
    replay_sequence: u64,
    shared_ledger_count: u64,
    shared_ledger_bytes: u64,
};

pub fn Slot(comptime Backend: type) type {
    return struct {
        const Self = @This();
        allocator: std.mem.Allocator,
        publication: *domains.Arena,
        scratch: *domains.Arena,
        descriptor: codec.OwnedDescriptor,
        encoded: []u8,
        io: *storage_io.NativeCompletionIo,
        wal_credit: u64 = 0,
        memory_pin: resources.ObserverMetadataPin,
        wal_pin: resources.ObserverMetadataPin,
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
            self.io.deinit() catch unreachable;
            const manager = self.wal_pin.manager;
            manager.observeUsage(.lsm_wal_retention, &self.wal_credit, 0);
            self.wal_pin.release() catch unreachable;
            self.memory_pin.release() catch unreachable;
            alloc.free(self.guard_path);
            self.descriptor.deinit();
            alloc.free(self.encoded);
            alloc.free(self.journal_path);
            alloc.free(self.run_path);
            alloc.destroy(self);
            scratch_domain.release();
            pub_domain.release();
        }

        pub fn create(backend: *Backend, encoded: []const u8, restored: bool) !*Self {
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
            if (backend.bulkIngestActive() or backend.activeImmutableMemtableCount() != 0 or
                backend.immutable_flush_build_in_flight or backend.manifest_publish_in_flight or
                backend.manifest_checkpoint_build_in_flight or backend.runs.count() > (if (restored) @as(usize, 65) else 64) or
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
            if (backend.getMergedWithMutable(&backend.mutable, .{ .name = sibling_namespace }, storage_key)) |_| return error.InvalidCompletionSlot else |err| {
                if (err != error.NotFound) return err;
            }
            const baseline_slot_present = if (backend.getMergedWithMutable(&backend.mutable, .{ .name = checked.descriptor.namespace }, storage_key)) |stored| blk: {
                if (!std.mem.eql(u8, stored, encoded)) return error.InvalidCompletionSlot;
                break :blk true;
            } else |err| switch (err) {
                error.NotFound => false,
                else => return err,
            };
            const baseline_applied = try captureBaseline(backend, checked.descriptor.namespace, applied_key, 16);
            const manifest_completed = !baseline_slot_present and if (baseline_applied) |marker| std.mem.eql(u8, &marker, &txn_id) else false;
            if (backend.runs.count() > 64 and !manifest_completed) return error.CompletionReservationBusy;
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
            var memory_pin = try manager.pinObserverMetadata(.lsm_in_memory_state, &backend.tracked_in_memory_state_bytes);
            errdefer memory_pin.release() catch unreachable;
            var wal_pin = try manager.pinObserverMetadata(.lsm_wal_retention, &backend.tracked_wal_retention_bytes);
            errdefer wal_pin.release() catch unreachable;
            self.wal_credit = 0;
            try manager.adjustUsage(.lsm_wal_retention, &self.wal_credit, limits.wal_bytes);
            errdefer manager.observeUsage(.lsm_wal_retention, &self.wal_credit, 0);
            const guard_path = try std.fs.path.join(alloc, &.{ root, guard_filename });
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
            const run_id = backend.next_run_id;
            const next_run_id = try std.math.add(u64, run_id, 1);
            const run_path = try repository.runPath(alloc, root, run_id);
            errdefer alloc.free(run_path);
            const journal_path = try manifest_set.pathAlloc(alloc, root, backend.manifest_journal.active_segment, .journal);
            errdefer alloc.free(journal_path);
            const journal_size = try backend.storage.?.fileSize(journal_path);
            // A single output's metadata and namespace/key bounds must fit
            // without journal rotation after prepare.
            if (journal_size > repository.maxManifestReadBytes() - limits.max_encoded_bytes - 64 * 1024)
                return error.CompletionReservationBusy;
            const io = try storage_io.NativeCompletionIo.createWithFilesAndHeadroom(alloc, native, root, &.{
                .{ .path = guard_path, .max_bytes = limits.max_encoded_bytes, .allow_delete = true },
                .{ .path = legacy_path, .max_bytes = limits.wal_bytes, .allow_delete = true },
                .{ .path = replay_index, .max_bytes = 4096 },
                .{ .path = replay_segments, .max_bytes = limits.wal_bytes },
                .{ .path = run_path, .max_bytes = limits.flush_bytes, .allow_delete = true },
                .{ .path = journal_path, .max_bytes = repository.maxManifestReadBytes(), .allow_append = true },
            }, 2);
            errdefer io.deinit() catch unreachable;
            io.allow_wal_reset = true;
            self.* = .{ .allocator = alloc, .publication = publication, .scratch = scratch, .descriptor = descriptor, .encoded = wire, .io = io, .wal_credit = self.wal_credit, .memory_pin = memory_pin, .wal_pin = wal_pin, .guard_path = guard_path, .wal_append_start = backend.write_stats.wal_append_bytes, .wal_entries_start = backend.write_stats.wal_append_entries, .wal_records_start = backend.write_stats.wal_append_records, .run_id = run_id, .journal_path = journal_path, .run_path = run_path, .durable = restored, .baseline_record = baseline_record, .baseline_credit = baseline_credit, .baseline_summary = baseline_summary, .baseline_applied = baseline_applied, .baseline_slot_present = baseline_slot_present };
            if (!restored) {
                // Any storage failure may have published this durable anchor.
                // Never let an uncertain create resume ordinary mutation.
                self.writeGuard() catch |err| {
                    backend.fenceFailedBulkWal();
                    return err;
                };
            }
            backend.next_run_id = next_run_id;
            return self;
        }

        fn writeGuard(self: *Self) !void {
            var writer = try self.io.storage().beginAtomicWrite(self.allocator, self.guard_path);
            var owned = true;
            errdefer if (owned) writer.abort();
            try writer.appendSlice(self.encoded);
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
            if (self.attempted) return error.RecoveryRequired;
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
                const reserved_namespace = if (entry.namespace_name) |name| std.mem.eql(u8, name, "docs") else true;
                if (!reserved_namespace) continue;
                if (std.mem.eql(u8, entry.key, applied_key)) return error.PreparedCompletionActive;
                if (!std.mem.eql(u8, entry.key, storage_key)) continue;
                const same_namespace = if (entry.namespace_name) |name|
                    (if (self.descriptor.descriptor.namespace) |expected| std.mem.eql(u8, name, expected) else false)
                else
                    self.descriptor.descriptor.namespace == null;
                // Only the original matching prepare may publish the anchor.
                if (!same_namespace or self.durable or entry.tombstone or !std.mem.eql(u8, entry.value, self.encoded))
                    return error.PreparedCompletionActive;
            }
        }

        /// Every heap allocation below partitions slabs physically obtained
        /// before prepare. The existing mutable is bounded while this slot is
        /// live; one merged SST drains both intervening writes and completion.
        pub fn complete(self: *Self, backend: *Backend, commit: bool, values: Values) !void {
            if (!self.durable) return error.CompletionNotPrepared;
            if (self.attempted or backend.manifest_recovery_required) return error.RecoveryRequired;
            // The bump domains are single-attempt. Even a pre-I/O allocation
            // failure must not allow an unbounded sequence of retries.
            self.attempted = true;
            errdefer backend.fenceFailedBulkWal();
            const alloc = self.scratch.allocator();
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
                    };
                    const bytes = if (binding.target == .key) key else value;
                    const field: *[8]u8 = bytes[binding.offset..][0..8];
                    switch (binding.byte_order) {
                        .little => std.mem.writeInt(u64, field, number, .little),
                        .big => std.mem.writeInt(u64, field, number, .big),
                    }
                }
                try delta.upsert(alloc, .{ .name = self.descriptor.descriptor.namespace }, key, value, op.kind == .delete);
            }
            try delta.upsert(alloc, .{ .name = self.descriptor.descriptor.namespace }, storage_key, "", true);
            try delta.upsert(alloc, .{ .name = self.descriptor.descriptor.namespace }, applied_key, &self.descriptor.descriptor.txn_id, false);
            try self.drain(backend, &delta, true, true);
        }

        /// Replay has already applied the whole atomic completion record. Drain
        /// its current result; never bind the transaction templates a second time.
        pub fn finishReplayed(self: *Self, backend: *Backend) !void {
            if (self.attempted) return error.RecoveryRequired;
            self.attempted = true;
            errdefer backend.fenceFailedBulkWal();
            var delta: state.ActiveMemTable = .{};
            defer delta.deinit(self.scratch.allocator());
            // A tombstone keeps the drain nonempty even after a prior manifest
            // survived and WAL reset completed before guard unlink failed.
            try delta.upsert(self.scratch.allocator(), .{ .name = self.descriptor.descriptor.namespace }, storage_key, "", true);
            try self.drain(backend, &delta, false, true);
        }

        /// The manifest already contains the completion marker and no slot
        /// descriptor. No foreground work can run while this guard exists, so
        /// replay contains only the already-manifested prefix. Avoid another
        /// SST on every retry of checkpoint/guard cleanup.
        pub fn finishManifested(self: *Self, backend: *Backend) !void {
            if (self.attempted or self.baseline_slot_present) return error.InvalidCompletionSlot;
            const marker = self.baseline_applied orelse return error.InvalidCompletionSlot;
            if (!std.mem.eql(u8, &marker, &self.descriptor.descriptor.txn_id)) return error.InvalidCompletionSlot;
            self.attempted = true;
            errdefer backend.fenceFailedBulkWal();
            var wal_lock = try backend.acquireWalOperationLock(.exclusive);
            defer wal_lock.release();
            try self.checkpointAndClearGuard(backend, true);
            backend.invalidateMutableReadSnapshot();
            backend.mutable.deinit(backend.allocator);
            backend.mutable = .{};
            backend.mutable_wal_range = .{};
            backend.clearPublishedWalLogicalDebtLocked();
            backend.syncTrackedInMemoryStateUsageCurrentLocked();
        }

        fn drain(self: *Self, backend: *Backend, delta: *state.ActiveMemTable, append_wal: bool, retire_guard: bool) !void {
            const alloc = self.scratch.allocator();
            const pub_alloc = self.publication.allocator();
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
            var built = try compaction.buildRunsFromStatesBorrowedWithReservedIds(Build, &build, states[0..if (current.entryCount() == 0) @as(usize, 1) else 2], self.run_id, self.run_id + 1);
            defer {
                for (built.items) |*run| run.deinit(alloc);
                built.deinit(alloc);
            }
            if (built.items.len != 1) return error.CompletionDrainShapeChanged;
            var run = try repository.cloneRunCompactionSnapshot(pub_alloc, built.items[0]);
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
            const frame = try manifest.encodeJournalFrameAlloc(alloc, sequence, false, &.{}, &.{}, .{
                .next_run_id = backend.next_run_id,
                .runs = &.{meta},
                .obsolete_paths = &.{},
            });
            defer alloc.free(frame);
            // An uncertain manifest append may already reference this SST.
            manifest_attempted = true;
            try self.io.storage().appendFileAbsolute(alloc, self.journal_path, frame, true);
            if (builtin.is_test) if (test_after_manifest) |hook| if (hook()) return error.RecoveryRequired;
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
            try self.checkpointAndClearGuard(backend, retire_guard);
        }

        fn checkpointAndClearGuard(self: *Self, backend: *Backend, retire_guard: bool) !void {
            const alloc = self.scratch.allocator();
            // Only the durable manifest authorizes retiring WAL recovery data.
            try wal.protectedReset(self.io.storage(), alloc, backend.root_dir.?);
            backend.wal_retention.primary = .{ .oldest_retained_segment = 1, .current_segment = 1 };
            backend.wal_retention.replay = .{ .current_segment = 1 };
            backend.wal_retention.primary_ns = backend.writeStatsNowNs();
            backend.wal_retention.replay_ns = backend.writeStatsNowNs();
            self.wal_pin.manager.observeUsage(.lsm_wal_retention, &backend.tracked_wal_retention_bytes, 0);
            if (retire_guard) try self.clearGuard();
        }
    };
}
