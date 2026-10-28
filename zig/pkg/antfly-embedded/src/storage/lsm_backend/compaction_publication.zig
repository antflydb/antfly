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

//! An admitted compaction installs three immutable roots, not K locked edits.
//! The caller owns inputs/outputs until this registered job finishes. All
//! allocating preparation, rebase, and ledger cleanup is sliced off-lock.
const std = @import("std");
const Directory = @import("run_directory.zig").Directory;
const Store = @import("run_store.zig").Store;
const Ledger = @import("obsolete_ledger.zig").Ledger;
const repository = @import("repository.zig");
const Run = repository.Run;
const Plan = @import("compaction.zig").CompactionPlan;
const Certificate = @import("dependency_job.zig").Job;
const runtime = @import("runtime.zig");
const resources = @import("../resource_manager.zig");
pub const memtable_reclamation = @import("memtable_reclamation.zig");
pub const retirement_execution = @import("retirement_execution.zig");
const quantum_ns = 2 * std.time.ns_per_ms;

/// Deterministic interleaving at the real off-lock preparation boundary.
/// Erased from production; the test must join its backend before clearing it.
pub var test_preparation_hook: ?*const fn (*@import("../lsm_backend.zig").Backend, *const Job) anyerror!void = null;
pub var test_directory_hook: ?*const fn (@import("../../common/read_admission.zig").Port) anyerror!void = null;
pub var test_controlled_hook: ?*const fn (*@import("../lsm_backend.zig").Backend, @import("../../common/read_admission.zig").Port) anyerror!void = null;

pub const Job = struct {
    accounting: Directory.Accounting,
    base: *Directory,
    directory: *Directory,
    source: *Store,
    store: *Store,
    base_obsolete: Ledger,
    obsolete: Ledger,
    plan: Plan,
    inputs: ?[]Directory.Handle = null,
    prepared_inputs: usize = 0,
    certificate: ?Certificate = null,
    index: usize = 0,
    phase: enum { inputs, outputs, deadlines, ready } = .inputs,
    requested_gc: bool = false,
    wire: u64 = 128,
    output_bytes: u64 = 0,
    file_bytes: u64 = 0,
    file_count: u64 = 0,
    compression: @import("../lsm/table_file.zig").CompressionStats = .{},
    delete_after_ns: u64,
    deadline_slack_ns: u64 = quantum_ns,
    created_ns: u64,
    rebases: usize = 0,
    rebase: ?Rebase = null,
    reservation: ?resources.Reservation = null,
    next: ?*Job = null,
    published: bool = false,
    append_only: bool = false,
    controlled_append: bool = false,
    execution_deadline_ns: ?u64 = null,
    append_accounted: bool = false,
    append_lease: ?Directory.EntryLease = null,
    append_candidate: ?*Directory = null,
    append_pools: ?Directory.IndexPreparation = null,
    append_directory: ?Directory.Put = null,
    append_writer: ?Store.OutputInsertion = null,
    append_retired: ?*Directory = null,
    append_ledger_retirement: [2]?*@import("ledger_reclamation.zig").Snapshot = @splat(null),

    const Rebase = struct {
        directory: *Directory,
        store: *Store,
        obsolete: Ledger,
        runs: Directory.ChangeCursor,
        paths: Ledger.ChangeCursor,
    };

    pub fn init(backend: anytype, plan: Plan, output_count: usize) !Job {
        const allocator = backend.allocator;
        const current = try backend.planningDirectory();
        // COW paths are shared; bound distinct copied nodes, not K full roots.
        const edits = plan.source_len + plan.target_len + output_count;
        const height: u64 = @max(if (current.tree.root) |root| root.height else 1, if (backend.runs.tree.root) |root| root.height else 1);
        const nodes = @min(current.count(), edits *| height) +| output_count *| height;
        const ledger_height: u64 = if (backend.obsolete_paths.tree.root) |root| root.height else 1;
        const ledger_nodes = @min(backend.obsolete_paths.count(), edits *| ledger_height) +| edits *| 2 +| 32;
        const bytes = @sizeOf(Job) +| 8192 +| (nodes +| height *| 16 +| 64) *| 1536 +| ledger_nodes *| 256 +| edits *| 512;
        var reservation: ?resources.Reservation = null;
        errdefer if (reservation) |*lease| lease.release();
        if (backend.options.resource_manager) |manager| reservation = try manager.reserve(.lsm_table_builder_working_set, bytes);
        const base = try current.fork(allocator);
        errdefer base.destroy(allocator);
        const directory = try current.fork(allocator);
        errdefer directory.destroy(allocator);
        const source = try allocator.create(Store);
        errdefer allocator.destroy(source);
        source.* = backend.runs.fork();
        errdefer source.deinit(allocator);
        const store = try allocator.create(Store);
        store.* = backend.runs.fork();
        return .{ .accounting = current.pinAccounting(), .base = base, .directory = directory, .source = source, .store = store, .base_obsolete = backend.obsolete_paths.fork(), .obsolete = backend.obsolete_paths.fork(), .plan = plan, .reservation = reservation, .created_ns = backend.nowNs(), .delete_after_ns = if (backend.options.obsolete_retention_ns == 0) 0 else backend.nowNs() +| backend.options.obsolete_retention_ns };
    }

    /// A flush adds outputs without retiring any selected run. Reuse the
    /// existing immutable candidate/rebase protocol rather than rebuilding the
    /// directory and editing the live writer under Backend.mu.
    pub fn initAppend(backend: anytype, output_count: usize) !Job {
        var self = try init(backend, .{ .source_level = 0, .source_start = 0, .source_len = 0, .target_start = 0, .target_len = 0, .output_level = 0 }, output_count);
        self.append_only = true;
        return self;
    }

    pub fn prepareAppendRetirementLocked(self: *Job, backend: anytype) !void {
        for (&self.append_ledger_retirement) |*slot| slot.* = try @import("ledger_reclamation.zig").Snapshot.capture(backend, &Ledger.empty);
    }

    pub fn accountedMemoryBytes(self: *const Job, pass: u64) u64 {
        // Never inspect candidate headers while the off-lock step mutates them.
        var bytes = self.accounting.accountedMemoryBytes(pass) +| self.base.accountedMemoryBytes(pass) +| self.source.memoryBytes(pass) +| self.base_obsolete.memoryBytes(pass);
        if (self.rebase) |*rebase| bytes +|= rebase.directory.accountedMemoryBytes(pass) +| rebase.store.memoryBytes(pass) +| rebase.obsolete.memoryBytes(pass);
        return bytes;
    }

    fn input(self: *Job, i: usize) !Directory.Handle {
        if (self.plan.input_handles) |handles| {
            const offset = if (i < self.plan.source_len) self.plan.source_start + i else self.plan.target_start + i - self.plan.source_len;
            const handle = handles[offset];
            if (self.base.resolve(handle) == null) return error.CompactionPlanningStale;
            return handle;
        }
        return self.base.at(if (i < self.plan.source_len) self.plan.sourceIndex(i) else self.plan.targetIndex(i - self.plan.source_len));
    }

    fn admitNames(self: *Job, bytes: u64) !void {
        if (self.reservation) |*lease| try lease.growBoundedOversized(bytes, 1);
    }

    fn putObsolete(self: *Job, allocator: std.mem.Allocator, path: []const u8, deadline: u64) !void {
        try self.obsolete.ensureUnusedCapacity(allocator, 1);
        if (self.obsolete.get(path)) |old| {
            self.obsolete.setDeadlinePrepared(path, @max(old.delete_after_ns, deadline));
        } else {
            const owned = try allocator.dupe(u8, path);
            self.obsolete.appendAssumeCapacity(.{ .path = owned, .delete_after_ns = deadline });
        }
    }

    fn step(self: *Job, backend: anytype, outputs: []Run, credits_arg: usize, deadline: u64) !void {
        const allocator = backend.allocator;
        const count = self.plan.source_len + self.plan.target_len;
        var credits = credits_arg;
        if (self.inputs == null) {
            self.inputs = try allocator.alloc(Directory.Handle, count);
            if (count != 0) {
                self.inputs.?[0] = try self.input(0);
                var plan = self.plan;
                plan.input_handles = self.inputs;
                self.certificate = .init(self.base, plan);
                self.certificate.?.covered = plan.complete_coverage orelse true;
            }
        }
        while (credits != 0 and runtime.workNowNs(backend) < deadline) {
            credits -= 1;
            switch (self.phase) {
                .inputs => {
                    if (self.index == count) {
                        self.index = 0;
                        self.phase = .outputs;
                        continue;
                    }
                    const handle = try self.input(self.index);
                    self.inputs.?[self.index] = handle.retain();
                    self.prepared_inputs = self.index + 1;
                    const run = handle.run;
                    const cert = &self.certificate.?;
                    cert.noteInputBounds(run, self.index == 0);
                    self.requested_gc = self.requested_gc or run.gc_requested;
                    if (run.path) |path| {
                        try self.admitNames(path.len);
                        try self.putObsolete(allocator, path, self.delete_after_ns);
                        self.wire +|= 20 +| path.len;
                    } else self.wire +|= 20;
                    try self.store.remove(allocator, run);
                    try self.directory.remove(allocator, run);
                    self.index += 1;
                },
                .outputs => {
                    if (self.index == outputs.len) {
                        self.phase = .ready;
                        return;
                    }
                    const output = &outputs[self.index];
                    // A full overlap rewrite discharges both tombstones and
                    // superseded-value requests. Partial outputs keep the intent.
                    output.gc_requested = !self.plan.tombstone_gc and (self.requested_gc or output.gc_requested);
                    const names = (if (output.path) |path| path.len else 0) + output.smallest_key.len + output.largest_key.len +
                        (if (output.smallest_namespace_name) |name| name.len else 0) + (if (output.largest_namespace_name) |name| name.len else 0);
                    try self.admitNames(names + (if (output.state) |*state| state.estimatedMemoryBytes() else 0));
                    self.wire +|= 192 +| names;
                    try self.store.stage(allocator, output.*);
                    self.store.adopt(output);
                    // The caller keeps one owner reference, so failure cleanup
                    // can delete unpublished SSTs after candidate retirement.
                    const retained = self.store.find(output).?.retainOwned();
                    // Registered flush outputs already own a source reference.
                    // The candidate retained that parent during stage; transfer
                    // the caller's reference instead of leaking it on overwrite.
                    if (output.owner) |owner| owner.release(allocator);
                    output.* = retained;
                    try self.directory.put(backend, output.*);
                    self.output_bytes +|= output.size_bytes;
                    if (output.path != null) {
                        self.file_count += 1;
                        self.file_bytes +|= output.size_bytes;
                        self.compression.add(output.compression_stats);
                    }
                    self.index += 1;
                },
                .deadlines => {
                    if (self.index == count) {
                        self.phase = .ready;
                        return;
                    }
                    if (self.inputs.?[self.index].run.path) |path| try self.putObsolete(allocator, path, self.delete_after_ns);
                    self.index += 1;
                },
                .ready => return,
            }
        }
    }

    fn constructionCancelled(raw: ?*const anyopaque) callconv(.c) u8 {
        const backend: *const @import("../lsm_backend.zig").Backend = @ptrCast(@alignCast(raw.?));
        return @intFromBool(backend.closing.load(.acquire));
    }

    /// One output per admission. All partial paths stay rooted in this durable
    /// job; the pass ends before reacquiring Backend.mu or retiring old roots.
    fn stepAppendWithExecution(self: *Job, backend: anytype, outputs: []Run) !void {
        const execution = @import("../../common/background_execution.zig");
        const admission = @import("../../common/read_admission.zig");
        const platform = @import("antfly_platform");
        const allocator = backend.allocator;
        if (self.inputs == null) self.inputs = try allocator.alloc(Directory.Handle, 0);
        if (self.index == outputs.len) {
            self.phase = .ready;
            return;
        }
        const output = &outputs[self.index];
        if (self.append_candidate == null and self.append_directory == null) self.append_candidate = try self.directory.fork(allocator);
        if (self.append_pools == null) self.append_pools = try Directory.IndexPreparation.init(self.append_candidate.?, output.level, 1);
        if (!self.append_accounted) {
            const names = (if (output.path) |path| path.len else 0) + output.smallest_key.len + output.largest_key.len +
                (if (output.smallest_namespace_name) |name| name.len else 0) + (if (output.largest_namespace_name) |name| name.len else 0);
            // Resource-manager admission happens without CPU ownership. The
            // already owned job funds all pools/wrappers before metadata edits.
            try self.admitNames(names + @sizeOf(Directory) + try self.append_pools.?.memoryBound() + try Store.OutputInsertion.memoryBound(self.store));
            self.append_accounted = true;
        }
        const runtime_owner = backend.background_executor.runtime orelse return error.ExecutionWaitUnsupported;
        const io = runtime_owner.io() orelse return error.ExecutionWaitUnsupported;
        if (self.execution_deadline_ns == null) self.execution_deadline_ns = platform.time.monotonicNs() +| 30 * std.time.ns_per_s;
        var pass = try execution.Pass.init(runtime_owner.runnableOwner(), &io, self.execution_deadline_ns.?, .{ .context = backend, .is_cancelled = constructionCancelled });
        defer pass.deinit();
        var scope = try pass.admissionScope(io);
        const payload = scope.payload();
        const original = admission.Port.init(&payload);
        if (self.append_lease == null and self.append_directory == null) self.append_lease = try @import("registered_writer.zig").Record.retainLegacyEntryWithAdmission(output.versionOwner(), original);
        if (self.append_writer == null) self.append_writer = try Store.OutputInsertion.init(self.store, allocator, output.versionOwner(), original);
        if (comptime @import("builtin").is_test) if (test_controlled_hook) |hook| try hook(backend, original);
        try self.append_writer.?.resumeWithAdmission(allocator, original);
        try self.append_pools.?.resumeWithAdmission(allocator, original);
        if (self.append_directory == null) {
            self.append_directory = try Directory.Put.init(self.directory, self.append_candidate.?, self.append_lease.?);
            self.append_candidate = null;
            self.append_lease = null;
        }
        try self.append_directory.?.resumeWithAdmission(allocator, original);
        if (comptime @import("builtin").is_test) if (test_directory_hook) |hook| try hook(original);
        var prepared = try self.append_directory.?.takePreparedWithAdmission(original);
        const old = self.directory;
        self.directory = prepared.directory;
        old.retired_next = self.append_retired;
        self.append_retired = old;
        prepared.lease.deinit(allocator); // Registered anchor makes this fixed.
        self.append_directory = null;
        self.append_pools = null;
        const retained = try self.append_writer.?.takeWithAdmission(original);
        if (output.owner) |owner| owner.release(allocator);
        output.* = retained;
        self.append_writer.?.deinit(allocator);
        self.append_writer = null;
        const names = (if (output.path) |path| path.len else 0) + output.smallest_key.len + output.largest_key.len +
            (if (output.smallest_namespace_name) |name| name.len else 0) + (if (output.largest_namespace_name) |name| name.len else 0);
        self.wire +|= 192 +| names;
        self.output_bytes +|= output.size_bytes;
        if (output.path != null) {
            self.file_count += 1;
            self.file_bytes +|= output.size_bytes;
            self.compression.add(output.compression_stats);
        }
        self.index += 1;
        self.append_accounted = false;
    }

    fn retireAppendHeadersLocked(self: *Job, backend: anytype) void {
        while (self.append_retired) |directory| {
            self.append_retired = directory.retired_next;
            backend.retireCheckpointDirectory(directory);
        }
    }

    /// Append jobs have no selected inputs to reconcile. Capture the newest
    /// roots in constant work, then replay only our registered outputs through
    /// the admitted writer/directory edits. Never import concurrent legacy
    /// records through the unadmitted compaction diff path.
    fn restartAppendLocked(self: *Job, backend: anytype) !void {
        std.debug.assert(self.append_only and self.phase == .ready);
        std.debug.assert(self.append_writer == null and self.append_directory == null and self.append_candidate == null);
        if (self.rebases == 4) return error.CompactionPlanningStale;
        const allocator = backend.allocator;
        const Snapshot = @import("ledger_reclamation.zig").Snapshot;
        try self.admitNames(2 * @sizeOf(Directory) + 2 * @sizeOf(Store) + 2 * @sizeOf(Snapshot));
        const current = try backend.planningDirectory();
        const base = try current.fork(allocator);
        errdefer base.destroy(allocator);
        const directory = try current.fork(allocator);
        errdefer directory.destroy(allocator);
        const source = try allocator.create(Store);
        errdefer allocator.destroy(source);
        source.* = backend.runs.fork();
        errdefer source.deinit(allocator);
        const store = try allocator.create(Store);
        errdefer allocator.destroy(store);
        store.* = backend.runs.fork();
        errdefer store.deinit(allocator);
        // Allocate both retirement headers before changing job ownership.
        // Transfer the entire old ledgers, including their spare pools, so no
        // variable destruction or admission wait occurs under Backend.mu.
        const old_base = try Snapshot.capture(backend, &Ledger.empty);
        errdefer old_base.retire(backend);
        const old_candidate = try Snapshot.capture(backend, &Ledger.empty);
        old_base.value = self.base_obsolete;
        old_candidate.value = self.obsolete;
        self.base_obsolete = backend.obsolete_paths.fork();
        self.obsolete = backend.obsolete_paths.fork();
        old_base.retire(backend);
        old_candidate.retire(backend);
        backend.retireCheckpointDirectory(self.base);
        backend.retireCheckpointDirectory(self.directory);
        backend.retireRunStore(self.source);
        backend.retireRunStore(self.store);
        self.accounting.deinit();
        self.accounting = current.pinAccounting();
        self.base = base;
        self.directory = directory;
        self.source = source;
        self.store = store;
        self.index = 0;
        self.phase = .outputs;
        self.append_accounted = false;
        self.wire = 128;
        self.output_bytes = 0;
        self.file_bytes = 0;
        self.file_count = 0;
        self.compression = .{};
        self.rebases += 1;
        // execution_deadline_ns deliberately survives every replay.
    }

    fn beginRebase(self: *Job, backend: anytype) !void {
        if (self.rebases == 4) return error.CompactionPlanningStale;
        const directory = try (try backend.planningDirectory()).fork(backend.allocator);
        errdefer directory.destroy(backend.allocator);
        const store = try backend.allocator.create(Store);
        store.* = backend.runs.fork();
        const obsolete = backend.obsolete_paths.fork();
        self.rebase = .{ .directory = directory, .store = store, .obsolete = obsolete, .runs = .init(self.base, directory), .paths = .init(&self.base_obsolete, &obsolete) };
        self.rebases += 1;
    }

    fn rebaseStep(self: *Job, backend: anytype, credits_arg: usize, deadline: u64) !bool {
        var credits = credits_arg;
        const rebase = &self.rebase.?;
        while (credits != 0 and runtime.workNowNs(backend) < deadline) {
            if (!rebase.runs.done()) {
                const change = rebase.runs.next(&credits) orelse continue;
                if (!self.append_only and (!self.certificate.?.acceptChange(change) or
                    ((self.plan.complete_coverage orelse true) and !self.certificate.?.covered))) return error.CompactionPlanningStale;
                const run = change.run;
                try self.admitNames(16384 + @as(usize, if (rebase.directory.tree.root) |root| root.height else 1) * 8192 + run.smallest_key.len + run.largest_key.len + (if (run.path) |path| path.len else 0));
                if (change.kind == .remove) {
                    try self.store.remove(backend.allocator, run);
                    try self.directory.remove(backend.allocator, run);
                } else {
                    const source = rebase.store.find(run) orelse return error.CompactionPlanningStale;
                    const revision = Store.revision(source, run.*);
                    try self.store.stageRevision(backend.allocator, revision);
                    self.store.adopt(&revision);
                    try self.directory.put(backend, revision);
                }
            } else if (!rebase.paths.done()) {
                const change = rebase.paths.next(&credits) orelse continue;
                try self.admitNames(16384 + change.path.path.len);
                if (change.kind == .put) {
                    try self.obsolete.ensureUnusedCapacity(backend.allocator, 1);
                    // Concurrent deadline changes are authoritative, including
                    // shorter retry deadlines after successful reclamation.
                    if (self.obsolete.contains(change.path.path)) self.obsolete.setDeadlinePrepared(change.path.path, change.path.delete_after_ns) else try self.putObsolete(backend.allocator, change.path.path, change.path.delete_after_ns);
                } else {
                    try self.obsolete.ensureUnusedCapacity(backend.allocator, 1);
                    self.obsolete.removePrepared(change.path.path);
                }
            } else return true;
        }
        return rebase.runs.done() and rebase.paths.done();
    }

    fn drainLedger(_: *Job, backend: anytype, ledger: Ledger) void {
        var owned = ledger;
        backend.releaseObsoleteLedgerLocked(&owned);
    }

    pub fn advanceLocked(self: *Job, backend: anytype, outputs: []Run) !bool {
        if (backend.manifestCoordinationIo()) |io| try io.checkCancel();
        if (self.rebase != null) {
            runtime.unlockBackend(@TypeOf(backend.*), backend, true);
            const result = self.rebaseStep(backend, 512, runtime.workNowNs(backend) +| quantum_ns);
            _ = runtime.lockBackend(@TypeOf(backend.*), backend);
            if (!try result) return false;
            const rebase = self.rebase.?;
            const obsolete = self.base_obsolete;
            backend.retireCheckpointDirectory(self.base);
            self.base = rebase.directory;
            backend.retireRunStore(rebase.store);
            self.base_obsolete = rebase.obsolete;
            self.rebase = null;
            self.drainLedger(backend, obsolete);
        } else if (self.phase != .ready) {
            runtime.unlockBackend(@TypeOf(backend.*), backend, true);
            const result = if (self.controlled_append)
                self.stepAppendWithExecution(backend, outputs)
            else
                self.step(backend, outputs, 512, runtime.workNowNs(backend) +| quantum_ns);
            const observed: anyerror!void = if (comptime @import("builtin").is_test) blk: {
                if (test_preparation_hook) |hook| break :blk hook(backend, self);
                break :blk {};
            } else {};
            _ = runtime.lockBackend(@TypeOf(backend.*), backend);
            self.retireAppendHeadersLocked(backend);
            try result;
            try observed;
        }
        if (self.phase != .ready) return false;
        if ((try backend.planningDirectory()).tree.root != self.base.tree.root or backend.obsolete_paths.tree.root != self.base_obsolete.tree.root) {
            if (self.controlled_append) try self.restartAppendLocked(backend) else try self.beginRebase(backend);
            return false;
        }
        // Retention starts no earlier than publication. Refresh only new
        // obsolete entries off-lock; increasing slack avoids chasing the clock
        // once per input on large jobs or after a long executor suspension.
        if (!self.append_only and backend.options.obsolete_retention_ns != 0 and self.delete_after_ns < backend.nowNs() +| backend.options.obsolete_retention_ns) {
            self.deadline_slack_ns = @max(self.deadline_slack_ns *| 2, (backend.nowNs() -| self.created_ns) *| 2 +| quantum_ns);
            self.delete_after_ns = backend.nowNs() +| backend.options.obsolete_retention_ns +| self.deadline_slack_ns;
            self.index = 0;
            self.phase = .deadlines;
            return false;
        }
        return true;
    }

    pub fn publishLocked(self: *Job, backend: anytype, input_bytes: u64, start_ns: u64) !void {
        if (backend.manifest_recovery_required) return error.RecoveryRequired;
        if (self.controlled_append) {
            if (backend.closing.load(.acquire)) return error.Canceled;
            if (@import("antfly_platform").time.monotonicNs() >= self.execution_deadline_ns.?) return error.Timeout;
        }
        std.debug.assert(self.phase == .ready and self.rebase == null);
        std.debug.assert(self.base.tree.root == backend.run_directory.?.tree.root);
        std.debug.assert(self.base_obsolete.tree.root == backend.obsolete_paths.tree.root);
        // Flush metadata was prepaid by the accepted writes/WAL window.
        // Charging it again as a compaction can stall an otherwise admitted
        // flush at the manifest frontier. Compaction still admits its new edit.
        var credit: ?@import("../lsm_backend.zig").Backend.MetadataCredit = if (self.append_only) null else try backend.admitCompactionMetadataBytes(self.wire);
        defer if (credit) |*held| held.release();
        std.mem.swap(Store, &backend.runs, self.store);
        std.mem.swap(Ledger, &backend.obsolete_paths, &self.obsolete);
        backend.invalidateReadVersion();
        backend.publishRunDirectory(self.directory);
        self.published = true;
        if (credit) |*held| held.commit();
        if (!self.append_only) {
            backend.obsolete_manifest_dirty = true;
            backend.obsolete_reclaim_retry_at_ns = 0;
        }
        backend.markManifestDirty();
        if (self.append_only) return;
        backend.recordPreparedCompactionWriteStats(input_bytes, self.output_bytes, self.file_count, self.file_bytes, self.compression, backend.writeStatsNowNs() -| start_ns);
        backend.compaction_stats.compactions += 1;
        backend.compaction_stats.input_runs += self.inputs.?.len;
        backend.compaction_stats.input_bytes +|= input_bytes;
        backend.compaction_stats.output_bytes +|= self.output_bytes;
    }

    pub fn finishLocked(self: *Job, backend: anytype, outputs: *std.ArrayListUnmanaged(Run)) void {
        if (self.controlled_append) {
            // Unique run IDs make an append's partial paths wholly rooted in
            // its candidate. Transfer variable retirement to the backend; only
            // fixed registered-anchor/wrapper drops run in this guarded handoff.
            if (self.append_directory) |*edit| {
                var abandoned = edit.abandonAppend();
                self.append_candidate = abandoned.directory;
                abandoned.lease.deinit(backend.allocator);
                self.append_directory = null;
            }
            if (self.append_writer) |*edit| edit.deinit(backend.allocator);
            self.append_writer = null;
            if (self.append_lease) |*lease| lease.deinit(backend.allocator);
            self.append_lease = null;
        } else if (self.append_directory != null or self.append_writer != null or self.append_lease != null) {
            // The failed admission has ended. Cleanup owns its rooted partial
            // trees and never invokes or retains the rejected original scope.
            runtime.unlockBackend(@TypeOf(backend.*), backend, true);
            if (self.append_directory) |*edit| edit.deinit(backend.allocator);
            self.append_directory = null;
            if (self.append_writer) |*edit| edit.deinit(backend.allocator);
            self.append_writer = null;
            if (self.append_lease) |*lease| lease.deinit(backend.allocator);
            self.append_lease = null;
            _ = runtime.lockBackend(@TypeOf(backend.*), backend);
        }
        if (self.append_candidate) |directory| backend.retireCheckpointDirectory(directory);
        self.append_candidate = null;
        self.retireAppendHeadersLocked(backend);
        if (self.published) {
            var i: usize = 0;
            while (i < self.inputs.?.len) {
                const end = @min(self.inputs.?.len, i + 64);
                for (self.inputs.?[i..end]) |handle| if (self.source.find(handle.run)) |run| backend.releaseRunVersionRef(run);
                runtime.unlockBackend(@TypeOf(backend.*), backend, true);
                for (self.inputs.?[i..end]) |handle| if (handle.run.path) |path| if (backend.options.cache) |cache| cache.invalidatePath(path);
                if (backend.manifestCoordinationIo()) |io| io.sleep(.fromNanoseconds(1), .awake) catch {};
                _ = runtime.lockBackend(@TypeOf(backend.*), backend);
                i = end;
            }
            // The normal flush returns its vector and retained credit to the
            // registered batch's prearranged cleanup handoff.
            if (!self.controlled_append) releaseOutputsLocked(backend, outputs, false);
        }
        if (self.rebase) |rebase| {
            backend.retireCheckpointDirectory(rebase.directory);
            backend.retireRunStore(rebase.store);
            self.rebase = null;
            self.drainLedger(backend, rebase.obsolete);
        }
        // Clear inspected headers before reclaiming their account references.
        const base_obsolete = self.base_obsolete;
        self.base_obsolete = .empty;
        const obsolete = self.obsolete;
        self.obsolete = .empty;
        for ([_]Ledger{ base_obsolete, obsolete }, 0..) |ledger, i| {
            if (self.append_ledger_retirement[i]) |snapshot| {
                snapshot.value = ledger;
                snapshot.retire(backend);
                self.append_ledger_retirement[i] = null;
            } else self.drainLedger(backend, ledger);
        }
        if (self.inputs) |inputs| {
            var i: usize = 0;
            while (i < self.prepared_inputs) {
                runtime.unlockBackend(@TypeOf(backend.*), backend, true);
                const deadline = runtime.workNowNs(backend) +| quantum_ns;
                const end = @min(self.prepared_inputs, i + 512);
                while (i < end and runtime.workNowNs(backend) < deadline) : (i += 1) inputs[i].release(backend.allocator);
                if (backend.manifestCoordinationIo()) |io| io.sleep(.fromNanoseconds(1), .awake) catch {};
                _ = runtime.lockBackend(@TypeOf(backend.*), backend);
            }
            backend.allocator.free(inputs);
        }
        if (!self.published) backend.retireCheckpointDirectory(self.directory);
        backend.retireCheckpointDirectory(self.base);
        backend.retireRunStore(self.store);
        backend.retireRunStore(self.source);
        self.accounting.deinit();
        // Adopt the published/queued legacy accounts into backend ownership
        // before releasing the independent construction reservation.
        if (self.controlled_append) backend.syncTrackedInMemoryStateUsageCurrentLocked();
        if (self.reservation) |*lease| lease.release();
    }
};

pub fn releaseOutputsLocked(backend: anytype, outputs: *std.ArrayListUnmanaged(Run), discard: bool) void {
    const Retirement = @import("run_snapshot_clone.zig").Retirement;
    var index: usize = 0;
    var physical: ?Retirement = null;
    var vector_offset: usize = 0;
    var complete = false;
    while (!complete) {
        runtime.unlockBackend(@TypeOf(backend.*), backend, true);
        const deadline = runtime.workNowNs(backend) +| quantum_ns;
        var steps: usize = 0;
        while (steps < 64 and runtime.workNowNs(backend) < deadline) : (steps += 1) {
            if (physical) |*retirement| {
                if (retirement.step(backend.allocator, null) catch unreachable) physical = null;
                continue;
            }
            if (index == outputs.items.len) {
                // ArrayList.deinit would poison a variable-sized allocation in
                // one call in Debug. Retire capacity, not just initialized runs.
                const bytes = std.mem.sliceAsBytes(outputs.allocatedSlice());
                const end = @min(bytes.len, vector_offset + 16 * 1024);
                @memset(bytes[vector_offset..end], undefined);
                vector_offset = end;
                if (end == bytes.len) {
                    if (bytes.len != 0) backend.allocator.rawFree(bytes, .of(Run), @returnAddress());
                    outputs.* = .empty;
                    complete = true;
                    break;
                }
                continue;
            }
            const run = &outputs.items[index];
            if (discard) {
                // Production outputs own preallocated tickets. Destruction
                // queues cleanup even if the request's Io is cancelled.
                if (!run.abandonOutput()) if (backend.storage) |storage| if (run.path) |path| repository.deleteFileAbsoluteWithStorage(storage, path) catch {};
            } else run.commitOutput();
            if (Retirement.supports(run.*)) {
                physical = Retirement.init(run.*) catch unreachable;
                run.* = undefined; // The fixed cursor owns the physical bytes.
            } else {
                // Writer owners and pinned generations still use their own
                // lifetime protocol; never reinterpret them as isolated runs.
                run.deinit(backend.allocator);
            }
            index += 1;
        }
        if (backend.manifestCoordinationIo()) |io| io.sleep(.fromNanoseconds(1), .awake) catch {};
        _ = runtime.lockBackend(@TypeOf(backend.*), backend);
    }
}

const TestFixture = struct {
    /// In-place resize/remap availability can depend on allocator placement.
    /// Make every growth an explicit allocation for exhaustive fault coverage.
    const MovingAllocator = struct {
        backing: std.mem.Allocator,
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.backing.rawAlloc(len, alignment, ra);
        }
        fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.backing.rawFree(bytes, alignment, ra);
        }
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
        }
    };
    fn fixedWorkClock(_: ?*anyopaque, _: std.Io.Clock) std.Io.Timestamp {
        return .{ .nanoseconds = 0 };
    }
    fn run(allocator: std.mem.Allocator, id: u64, level: u32, key: []const u8) !Run {
        return .{ .id = id, .level = level, .size_bytes = 1024, .path = try std.fmt.allocPrint(allocator, "publication-{d}.sst", .{id}), .smallest_namespace_name = null, .smallest_key = @constCast(key), .largest_namespace_name = null, .largest_key = @constCast(key), .entry_count = 1, .tombstone_count = 0, .bloom_filter = null, .state = null, .owns_metadata = false, .owns_path = true };
    }
    fn append(backend: anytype, id: u64, level: u32, key: []const u8) !void {
        var owned = try run(backend.allocator, id, level, key);
        errdefer owned.deinit(backend.allocator);
        try backend.runs.append(backend.allocator, owned);
    }
    fn obsolete(backend: anytype, path: []const u8) !void {
        const owned = try backend.allocator.dupe(u8, path);
        errdefer backend.allocator.free(owned);
        try backend.queueObsoleteFilePath(owned);
    }
    fn check(allocator: std.mem.Allocator, variant: usize) !void {
        const Backend = @import("../lsm_backend.zig").Backend;
        var manager = resources.ResourceManager.init(.{});
        defer std.debug.assert(manager.sliceStats(.lsm_table_builder_working_set).used_bytes == 0);
        // Exhaustive allocation failure needs a deterministic allocation
        // trace. Time-budgeted reclamation otherwise changes pool reuse with
        // machine load; borrowed-clock/native tests cover actual time budgets.
        var callbacks = std.testing.io.vtable.*;
        callbacks.now = fixedWorkClock;
        const io: std.Io = .{ .userdata = std.testing.io.userdata, .vtable = &callbacks };
        var backend = Backend.init(allocator, .{ .wal_enabled = false, .resource_manager = &manager, .read_runtime = @import("storage_io.zig").ReadRuntime.init(io) });
        defer backend.close();
        if (variant == 5) backend.options.obsolete_retention_ns = 0;
        try std.testing.expect(backend.mu.tryLock());
        defer backend.mu.unlock();
        backend.retainReaderKind(.compaction);
        defer backend.releaseReaderKind(.compaction);
        try append(&backend, 1, 0, "a");
        try append(&backend, 2, 0, "a");
        try append(&backend, 3, 1, "z");
        try obsolete(&backend, "prior.sst");
        const directory = try backend.planningDirectory();
        const handles = [_]Directory.Handle{ directory.at(0), directory.at(1) };
        const plan = Plan{ .source_level = 0, .source_start = 0, .source_len = 2, .target_start = 2, .target_len = 0, .output_level = 1, .input_handles = &handles, .complete_coverage = true };
        var outputs: std.ArrayListUnmanaged(Run) = .empty;
        defer {
            for (outputs.items) |*output| output.deinit(allocator);
            outputs.deinit(allocator);
        }
        {
            var output = try run(allocator, 10, 1, "a");
            errdefer output.deinit(allocator);
            try outputs.append(allocator, output);
        }
        var job = try Job.init(&backend, plan, 1);
        job.next = backend.active_compaction_publications;
        backend.active_compaction_publications = &job;
        defer {
            job.finishLocked(&backend, &outputs);
            backend.active_compaction_publications = job.next;
        }
        if (variant == 3) {
            // Abandon a partially emitted preparation just like cancellation.
            backend.mu.unlock();
            const advanced = job.step(&backend, outputs.items, 1, std.math.maxInt(u64));
            try std.testing.expect(backend.mu.tryLock());
            try advanced;
            try std.testing.expectEqual(@as(usize, 1), job.prepared_inputs);
            try std.testing.expectEqual(@as(usize, 3), backend.runs.count());
            return;
        }
        while (!try job.advanceLocked(&backend, outputs.items)) {}
        try std.testing.expectEqual(@as(usize, 3), backend.runs.count());
        try std.testing.expect(!backend.obsolete_paths.contains("publication-1.sst"));
        if (variant == 0) {
            // Force expiration of the preparation deadline: refresh must
            // preserve the full retention interval at actual publication.
            job.delete_after_ns = 0;
            while (!try job.advanceLocked(&backend, outputs.items)) {}
        }
        if (variant == 1) {
            // An unrelated run and simultaneous obsolete-ledger removal/put
            // must both survive the final three-root publication.
            var added = try run(allocator, 4, 0, "z");
            var adopted = false;
            defer if (!adopted) added.deinit(allocator);
            const changed = try backend.prepareRunDirectoryChange(null, &.{added});
            errdefer if (!adopted) if (changed) |root| root.destroy(allocator);
            try backend.runs.append(allocator, added);
            adopted = true;
            backend.invalidateReadVersion();
            backend.publishRunDirectory(changed);
            try backend.obsolete_paths.ensureUnusedCapacity(allocator, 1);
            backend.obsolete_paths.removePrepared("prior.sst");
            try obsolete(&backend, "concurrent.sst");
            while (!try job.advanceLocked(&backend, outputs.items)) {}
        } else if (variant == 2) {
            const source = backend.runs.find(handles[0].run).?;
            var replacement = Store.revision(source, source.*);
            replacement.level = 2;
            const changed = try backend.prepareRunDirectoryMove(source, 2);
            {
                errdefer if (changed) |root| root.destroy(allocator);
                try backend.runs.replace(allocator, source, replacement);
            }
            backend.invalidateReadVersion();
            backend.publishRunDirectory(changed);
            while (true) {
                _ = job.advanceLocked(&backend, outputs.items) catch |err| {
                    if (err != error.CompactionPlanningStale) return err;
                    try std.testing.expectEqual(@as(usize, 3), backend.runs.count());
                    try std.testing.expect(!backend.obsolete_paths.contains("publication-1.sst"));
                    return;
                };
            }
        } else if (variant == 4) {
            backend.manifest_recovery_required = true;
            try std.testing.expectError(error.RecoveryRequired, job.publishLocked(&backend, 2048, 0));
            try std.testing.expectEqual(@as(usize, 3), backend.runs.count());
            return;
        }
        try job.publishLocked(&backend, 2048, 0);
        try std.testing.expectEqual(@as(usize, if (variant == 1) 3 else 2), backend.runs.count());
        try std.testing.expect(backend.run_directory.?.byId(10) != null);
        try std.testing.expect(backend.run_directory.?.byId(1) == null);
        try std.testing.expect(backend.obsolete_paths.contains("publication-1.sst"));
        try std.testing.expect(backend.obsolete_paths.contains("publication-2.sst"));
        if (variant == 5) try std.testing.expectEqual(@as(u64, 0), backend.obsolete_paths.get("publication-1.sst").?.delete_after_ns) else try std.testing.expect(backend.obsolete_paths.get("publication-1.sst").?.delete_after_ns >= backend.nowNs() + backend.options.obsolete_retention_ns);
        if (variant == 1) {
            try std.testing.expect(backend.run_directory.?.byId(4) != null);
            try std.testing.expect(!backend.obsolete_paths.contains("prior.sst"));
            try std.testing.expect(backend.obsolete_paths.contains("concurrent.sst"));
        }
    }
};

fn checkAppendPublication(allocator: std.mem.Allocator, variant: usize) !void {
    const Backend = @import("../lsm_backend.zig").Backend;
    var manager = resources.ResourceManager.init(.{});
    defer std.debug.assert(manager.sliceStats(.lsm_table_builder_working_set).used_bytes == 0);
    var callbacks = std.testing.io.vtable.*;
    callbacks.now = TestFixture.fixedWorkClock;
    const io: std.Io = .{ .userdata = std.testing.io.userdata, .vtable = &callbacks };
    var backend = Backend.init(allocator, .{ .wal_enabled = false, .resource_manager = &manager, .read_runtime = @import("storage_io.zig").ReadRuntime.init(io) });
    defer backend.close();
    try std.testing.expect(backend.mu.tryLock());
    defer backend.mu.unlock();
    backend.retainReaderKind(.compaction);
    defer backend.releaseReaderKind(.compaction);
    try TestFixture.append(&backend, 1, 0, "a");
    try TestFixture.append(&backend, 2, 1, "z");
    try TestFixture.obsolete(&backend, "prior.sst");
    var outputs: std.ArrayListUnmanaged(Run) = .empty;
    defer {
        for (outputs.items) |*output| output.deinit(allocator);
        outputs.deinit(allocator);
    }
    for ([_][]const u8{ "b", "y" }, 0..) |key, index| {
        var output = try TestFixture.run(allocator, 10 + index, 0, key);
        errdefer output.deinit(allocator);
        try outputs.append(allocator, output);
    }
    var job = try Job.initAppend(&backend, outputs.items.len);
    job.next = backend.active_compaction_publications;
    backend.active_compaction_publications = &job;
    defer {
        job.finishLocked(&backend, &outputs);
        backend.active_compaction_publications = job.next;
    }
    if (variant == 2) {
        backend.mu.unlock();
        const result = job.step(&backend, outputs.items, 2, std.math.maxInt(u64));
        try std.testing.expect(backend.mu.tryLock());
        try result;
        try std.testing.expectEqual(@as(usize, 2), backend.runs.count());
        return;
    }
    if (variant >= 5) try job.prepareAppendRetirementLocked(&backend);
    while (!try job.advanceLocked(&backend, outputs.items)) {}
    if (variant == 1 or variant >= 5) {
        var added = try TestFixture.run(allocator, 3, 0, "c");
        var adopted = false;
        defer if (!adopted) added.deinit(allocator);
        const directory = try backend.prepareRunDirectoryChange(null, &.{added});
        errdefer if (!adopted) if (directory) |owned| owned.destroy(allocator);
        try backend.runs.append(allocator, added);
        adopted = true;
        backend.invalidateReadVersion();
        backend.publishRunDirectory(directory);
        try backend.obsolete_paths.ensureUnusedCapacity(allocator, 1);
        backend.obsolete_paths.removePrepared("prior.sst");
        try TestFixture.obsolete(&backend, "concurrent.sst");
        if (variant >= 5) {
            // Exercise the production replay capture/retirement transaction at
            // every allocation failure, independently of the native worker's
            // execution provider. The legacy fixture drives the later edits;
            // actual admitted replay is covered by the normal-worker tests.
            job.execution_deadline_ns = 123456789;
            try job.restartAppendLocked(&backend);
            try std.testing.expect(job.rebase == null);
            try std.testing.expectEqual(@as(?u64, 123456789), job.execution_deadline_ns);
            try std.testing.expectEqual(@as(usize, 3), job.source.count());
            if (variant == 6) return; // Abort after complete ownership transfer.
            if (variant == 7) {
                for (0..3) |_| {
                    while (!try job.advanceLocked(&backend, outputs.items)) {}
                    try job.restartAppendLocked(&backend);
                }
                while (!try job.advanceLocked(&backend, outputs.items)) {}
                try std.testing.expectError(error.CompactionPlanningStale, job.restartAppendLocked(&backend));
                try std.testing.expectEqual(@as(usize, 4), job.rebases);
            }
        }
        while (!try job.advanceLocked(&backend, outputs.items)) {}
    }
    if (variant == 3) {
        backend.manifest_recovery_required = true;
        try std.testing.expectError(error.RecoveryRequired, job.publishLocked(&backend, 0, 0));
        try std.testing.expectEqual(@as(usize, 2), backend.runs.count());
        return;
    }
    const frontier_root = if (variant == 4) try allocator.dupe(u8, "prepaid-flush-frontier") else null;
    defer if (frontier_root) |root| {
        backend.root_dir = null;
        allocator.free(root);
    };
    if (frontier_root) |root| {
        backend.root_dir = root;
        backend.manifest_unpublished_wire_bytes = repository.maxManifestReadBytes();
        try std.testing.expectError(error.ResourceBudgetExceeded, backend.admitCompactionMetadataBytes(1));
    }
    try job.publishLocked(&backend, 0, 0);
    try std.testing.expectEqual(@as(usize, if (variant == 1 or variant >= 5) 5 else 4), backend.runs.count());
    for ([_]u64{ 1, 2, 10, 11 }) |id| try std.testing.expect(backend.run_directory.?.byId(id) != null);
    try std.testing.expectEqual(@as(u64, 0), backend.compaction_stats.compactions);
    if (variant == 1 or variant >= 5) {
        try std.testing.expect(backend.run_directory.?.byId(3) != null);
        try std.testing.expect(!backend.obsolete_paths.contains("prior.sst"));
        try std.testing.expect(backend.obsolete_paths.contains("concurrent.sst"));
    } else try std.testing.expect(backend.obsolete_paths.contains("prior.sst"));
}

test "compaction publication stages append flush roots and rebases concurrent edits through every allocation failure" {
    var backing: TestFixture.MovingAllocator = .{ .backing = std.testing.allocator };
    for (0..8) |variant| try std.testing.checkAllAllocationFailures(backing.allocator(), checkAppendPublication, .{variant});
}

test "compaction publication stages and rebases all roots and cleans every allocation failure" {
    var backing: TestFixture.MovingAllocator = .{ .backing = std.testing.allocator };
    for (0..6) |variant| std.testing.checkAllAllocationFailures(backing.allocator(), TestFixture.check, .{variant}) catch |cause| {
        std.debug.print("publication allocation failure variant={d} cause={s}\n", .{ variant, @errorName(cause) });
        return cause;
    };
}

test "compaction publication uses the borrowed clock for bounded preparation" {
    const allocator = std.testing.allocator;
    const Clock = struct {
        var now_ns: i96 = 0;
        fn now(_: ?*anyopaque, _: std.Io.Clock) std.Io.Timestamp {
            return .{ .nanoseconds = now_ns };
        }
    };
    // This leaf storage test also runs in lean sparse-index roots. Override
    // only the clock, retaining the native I/O context for synchronization.
    // Composed VOPR tests separately cover scheduling and cancellation.
    Clock.now_ns = 0;
    var vtable = std.testing.io.vtable.*;
    vtable.now = Clock.now;
    const io: std.Io = .{ .userdata = std.testing.io.userdata, .vtable = &vtable };
    const Backend = @import("../lsm_backend.zig").Backend;
    var backend = Backend.init(allocator, .{
        .wal_enabled = false,
        .read_runtime = @import("storage_io.zig").ReadRuntime.init(io),
    });
    defer backend.close();
    try std.testing.expect(backend.mu.tryLock());
    defer backend.mu.unlock();
    backend.retainReaderKind(.compaction);
    defer backend.releaseReaderKind(.compaction);
    try TestFixture.append(&backend, 1, 0, "a");
    try TestFixture.append(&backend, 2, 0, "a");
    const directory = try backend.planningDirectory();
    const handles = [_]Directory.Handle{ directory.at(0), directory.at(1) };
    const plan = Plan{
        .source_level = 0,
        .source_start = 0,
        .source_len = 2,
        .target_start = 2,
        .target_len = 0,
        .output_level = 1,
        .input_handles = &handles,
        .complete_coverage = true,
    };
    var outputs: std.ArrayListUnmanaged(Run) = .empty;
    var job = try Job.init(&backend, plan, 0);
    backend.active_compaction_publications = &job;
    defer {
        job.finishLocked(&backend, &outputs);
        backend.active_compaction_publications = null;
    }
    // One credit advances exactly one input while the owning clock is
    // before the deadline, regardless of the host clock's epoch.
    backend.mu.unlock();
    const first = job.step(&backend, &.{}, 1, 10);
    try std.testing.expect(backend.mu.tryLock());
    try first;
    try std.testing.expectEqual(@as(usize, 1), job.prepared_inputs);
    Clock.now_ns = 10;
    backend.mu.unlock();
    const expired = job.step(&backend, &.{}, 1, 10);
    try std.testing.expect(backend.mu.tryLock());
    try expired;
    try std.testing.expectEqual(@as(usize, 1), job.prepared_inputs);
}

test "compaction publication atomic fence scaling benchmark" {
    if (@import("builtin").mode != .fast) return error.SkipZigTest;
    const Backend = @import("../lsm_backend.zig").Backend;
    const allocator = std.heap.smp_allocator;
    const io = std.Io.Threaded.global_single_threaded.io();
    for ([_]usize{ 1000, 10000, 50000 }) |count| {
        var backend = Backend.init(allocator, .{ .wal_enabled = false });
        defer backend.close();
        try std.testing.expect(backend.mu.tryLock());
        defer backend.mu.unlock();
        backend.retainReaderKind(.compaction);
        defer backend.releaseReaderKind(.compaction);
        for (0..count) |i| try TestFixture.append(&backend, i + 1, 0, "a");
        const directory = try backend.planningDirectory();
        const handles = try allocator.alloc(Directory.Handle, count);
        defer allocator.free(handles);
        for (handles, 0..) |*handle, i| handle.* = directory.at(i);
        const plan = Plan{ .source_level = 0, .source_start = 0, .source_len = count, .target_start = count, .target_len = 0, .output_level = 1, .input_handles = handles, .complete_coverage = true };
        const old_started = std.Io.Clock.awake.now(io);
        var old_candidate = backend.runs.fork();
        for (handles) |handle| try old_candidate.remove(allocator, backend.runs.find(handle.run).?);
        const old_directory = (try backend.prepareRunDirectoryChange(plan, &.{})).?;
        const old_ns = old_started.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds();
        backend.mu.unlock();
        old_candidate.deinit(allocator);
        old_directory.destroy(allocator);
        try std.testing.expect(backend.mu.tryLock());
        var outputs: std.ArrayListUnmanaged(Run) = .empty;
        var job = try Job.init(&backend, plan, 0);
        backend.active_compaction_publications = &job;
        defer {
            job.finishLocked(&backend, &outputs);
            backend.active_compaction_publications = null;
        }
        var slices: usize = 0;
        var max_slice_ns: i96 = 0;
        const start = std.Io.Clock.awake.now(io);
        while (true) {
            const slice_start = std.Io.Clock.awake.now(io);
            const ready = try job.advanceLocked(&backend, outputs.items);
            max_slice_ns = @max(max_slice_ns, slice_start.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds());
            slices += 1;
            if (ready) break;
        }
        const prepared_ns = start.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds();
        const publish_start = std.Io.Clock.awake.now(io);
        try job.publishLocked(&backend, count * 1024, 0);
        const publish_ns = publish_start.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds();
        try std.testing.expectEqual(@as(usize, 0), backend.runs.count());
        try std.testing.expectEqual(count, backend.obsolete_paths.count());
        std.debug.print("publication-fence inputs={d} old_locked_removals_ns={d} new_atomic_publish_ns={d} preparation_wall_ns={d} slices={d} max_slice_ns={d}\n", .{ count, old_ns, publish_ns, prepared_ns, slices, max_slice_ns });
    }
}

test "controlled physical sync output retirement normal cleanup yields within a run and vector and hands off tickets off lock" {
    const Queue = @import("output_cleanup.zig").Queue;
    const Fixture = struct {
        mu: std.atomic.Mutex = .unlocked,
        allocator: std.mem.Allocator = undefined,
        storage: ?@import("storage_io.zig").Storage = null,
        unlocks: usize = 0,
        freeing: bool = false,
        frees: usize = 0,
        pub fn manifestCoordinationIo(_: *@This()) ?std.Io {
            return null;
        }
        pub fn unlockWithReclamation(self: *@This()) void {
            self.unlocks += 1;
            self.mu.unlock();
        }
        fn alloc(_: *anyopaque, n: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            return std.testing.allocator.rawAlloc(n, alignment, ra);
        }
        fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.freeing) {
                if (!self.mu.tryLock()) @panic("output freed under backend lock");
                self.mu.unlock();
                self.frees += 1;
            }
            std.testing.allocator.rawFree(bytes, alignment, ra);
        }
    };
    for ([_]bool{ false, true }) |discard| {
        var backend: Fixture = .{};
        backend.allocator = .{ .ptr = &backend, .vtable = &.{ .alloc = Fixture.alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = Fixture.free } };
        const a = backend.allocator;
        var queue: Queue = .{ .allocator = a };
        defer queue.deinit();
        var outputs: std.ArrayListUnmanaged(Run) = .empty;
        defer outputs.deinit(a);
        try outputs.ensureTotalCapacityPrecise(a, 8192);
        var run: Run = .{ .id = 1, .level = 1, .size_bytes = 1, .path = try a.dupe(u8, "unpublished.sst"), .smallest_namespace_name = null, .smallest_key = try a.alloc(u8, 2 << 20), .largest_namespace_name = null, .largest_key = try a.dupe(u8, "z"), .entry_count = 1, .bloom_filter = null, .state = null };
        var transferred = false;
        defer if (!transferred) run.deinit(a);
        run.output_ticket = try queue.create(run.path.?);
        outputs.appendAssumeCapacity(run);
        transferred = true;
        try std.testing.expect(backend.mu.tryLock());
        backend.freeing = true;
        releaseOutputsLocked(&backend, &outputs, discard);
        backend.freeing = false;
        backend.mu.unlock();
        try std.testing.expect(outputs.items.len == 0 and outputs.capacity == 0);
        try std.testing.expect(backend.unlocks > 3 and backend.frees >= 4);
        try std.testing.expectEqual(@as(usize, if (discard) 1 else 0), queue.pending.load(.acquire));
        try std.testing.expectEqual(@as(usize, if (discard) 1 else 0), queue.live.load(.acquire));
    }
}
