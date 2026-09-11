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

//! An admitted compaction installs three immutable roots, not K locked edits.
//! The caller owns inputs/outputs until this registered job finishes. All
//! allocating preparation, rebase, and ledger cleanup is sliced off-lock.
const std = @import("std");
const Directory = @import("run_directory.zig").Directory;
const Store = @import("run_store.zig").Store;
const Ledger = @import("obsolete_ledger.zig").Ledger;
const Account = @import("memory_account.zig").Account;
const repository = @import("repository.zig");
const Run = repository.Run;
const Plan = @import("compaction.zig").CompactionPlan;
const Certificate = @import("dependency_job.zig").Job;
const runtime = @import("runtime.zig");
const resources = @import("../resource_manager.zig");
const clock = @import("antfly_platform").time;
const quantum_ns = 2 * std.time.ns_per_ms;

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
    cleanup_account: ?*Account = null,
    next: ?*Job = null,
    published: bool = false,

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
        return .{ .accounting = current.pinAccounting(), .base = base, .directory = directory, .source = source, .store = store, .base_obsolete = backend.obsolete_paths.fork(), .obsolete = backend.obsolete_paths.fork(), .plan = plan, .reservation = reservation, .created_ns = clock.monotonicNs(), .delete_after_ns = if (backend.options.obsolete_retention_ns == 0) 0 else backend.nowNs() +| backend.options.obsolete_retention_ns };
    }

    pub fn accountedMemoryBytes(self: *const Job, pass: u64) u64 {
        // Never inspect candidate headers while the off-lock step mutates them.
        var bytes = self.accounting.accountedMemoryBytes(pass) +| self.base.accountedMemoryBytes(pass) +| self.source.memoryBytes(pass) +| self.base_obsolete.memoryBytes(pass);
        if (self.rebase) |*rebase| bytes +|= rebase.directory.accountedMemoryBytes(pass) +| rebase.store.memoryBytes(pass) +| rebase.obsolete.memoryBytes(pass);
        if (self.cleanup_account) |account| bytes +|= account.chargeOnce(pass);
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

    fn admitNames(self: *Job, bytes: usize) !void {
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
            self.inputs.?[0] = try self.input(0);
            var plan = self.plan;
            plan.input_handles = self.inputs;
            self.certificate = .init(self.base, plan);
            self.certificate.?.covered = plan.complete_coverage orelse true;
        }
        while (credits != 0 and clock.monotonicNs() < deadline) {
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
                    if (self.requested_gc and (output.tombstone_count orelse 0) != 0) output.gc_requested = true;
                    const names = (if (output.path) |path| path.len else 0) + output.smallest_key.len + output.largest_key.len +
                        (if (output.smallest_namespace_name) |name| name.len else 0) + (if (output.largest_namespace_name) |name| name.len else 0);
                    try self.admitNames(names + (if (output.state) |*state| state.estimatedMemoryBytes() else 0));
                    self.wire +|= 192 +| names;
                    try self.store.stage(allocator, output.*);
                    self.store.adopt(output);
                    // The caller keeps one owner reference, so failure cleanup
                    // can delete unpublished SSTs after candidate retirement.
                    output.* = self.store.find(output).?.retainOwned();
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
        while (credits != 0 and clock.monotonicNs() < deadline) {
            if (!rebase.runs.done()) {
                const change = rebase.runs.next(&credits) orelse continue;
                if (!self.certificate.?.acceptChange(change) or
                    ((self.plan.complete_coverage orelse true) and !self.certificate.?.covered)) return error.CompactionPlanningStale;
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

    fn drainLedger(self: *Job, backend: anytype, ledger: Ledger) void {
        self.cleanup_account = if (ledger.tree.account) |account| account.retain() else null;
        var reclaimer = Ledger.Reclaimer.init(ledger);
        while (true) {
            runtime.unlockBackend(@TypeOf(backend.*), backend, true);
            var credits: usize = 2048;
            const deadline = clock.monotonicNs() +| quantum_ns;
            var done = false;
            while (credits != 0 and clock.monotonicNs() < deadline) {
                var part: usize = @min(credits, 64);
                const before = part;
                done = reclaimer.step(backend.allocator, &part);
                credits -= before - part;
                if (done) break;
            }
            if (!done) if (backend.manifestCoordinationIo()) |io| io.sleep(.fromNanoseconds(1), .awake) catch {};
            _ = runtime.lockBackend(@TypeOf(backend.*), backend);
            if (done) break;
        }
        if (self.cleanup_account) |account| account.release();
        self.cleanup_account = null;
    }

    pub fn advanceLocked(self: *Job, backend: anytype, outputs: []Run) !bool {
        if (backend.manifestCoordinationIo()) |io| try io.checkCancel();
        if (self.rebase != null) {
            runtime.unlockBackend(@TypeOf(backend.*), backend, true);
            const result = self.rebaseStep(backend, 512, clock.monotonicNs() +| quantum_ns);
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
            const result = self.step(backend, outputs, 512, clock.monotonicNs() +| quantum_ns);
            _ = runtime.lockBackend(@TypeOf(backend.*), backend);
            try result;
        }
        if (self.phase != .ready) return false;
        if ((try backend.planningDirectory()).tree.root != self.base.tree.root or backend.obsolete_paths.tree.root != self.base_obsolete.tree.root) {
            try self.beginRebase(backend);
            return false;
        }
        // Retention starts no earlier than publication. Refresh only new
        // obsolete entries off-lock; increasing slack avoids chasing the clock
        // once per input on large jobs or after a long executor suspension.
        if (backend.options.obsolete_retention_ns != 0 and self.delete_after_ns < backend.nowNs() +| backend.options.obsolete_retention_ns) {
            self.deadline_slack_ns = @max(self.deadline_slack_ns *| 2, (clock.monotonicNs() -| self.created_ns) *| 2 +| quantum_ns);
            self.delete_after_ns = backend.nowNs() +| backend.options.obsolete_retention_ns +| self.deadline_slack_ns;
            self.index = 0;
            self.phase = .deadlines;
            return false;
        }
        return true;
    }

    pub fn publishLocked(self: *Job, backend: anytype, input_bytes: u64, start_ns: u64) !void {
        std.debug.assert(self.phase == .ready and self.rebase == null);
        std.debug.assert(self.base.tree.root == backend.run_directory.?.tree.root);
        std.debug.assert(self.base_obsolete.tree.root == backend.obsolete_paths.tree.root);
        var credit = try backend.admitCompactionMetadataBytes(self.wire);
        defer credit.release();
        std.mem.swap(Store, &backend.runs, self.store);
        std.mem.swap(Ledger, &backend.obsolete_paths, &self.obsolete);
        backend.invalidateReadVersion();
        backend.publishRunDirectory(self.directory);
        self.published = true;
        credit.commit();
        backend.obsolete_manifest_dirty = true;
        backend.obsolete_reclaim_retry_at_ns = 0;
        backend.markManifestDirty();
        backend.recordPreparedCompactionWriteStats(input_bytes, self.output_bytes, self.file_count, self.file_bytes, self.compression, backend.writeStatsNowNs() -| start_ns);
        backend.compaction_stats.compactions += 1;
        backend.compaction_stats.input_runs += self.inputs.?.len;
        backend.compaction_stats.input_bytes +|= input_bytes;
        backend.compaction_stats.output_bytes +|= self.output_bytes;
    }

    pub fn finishLocked(self: *Job, backend: anytype, outputs: *std.ArrayListUnmanaged(Run)) void {
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
            releaseOutputsLocked(backend, outputs, false);
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
        self.drainLedger(backend, base_obsolete);
        self.drainLedger(backend, self.obsolete);
        if (self.inputs) |inputs| {
            var i: usize = 0;
            while (i < self.prepared_inputs) {
                runtime.unlockBackend(@TypeOf(backend.*), backend, true);
                const deadline = clock.monotonicNs() +| quantum_ns;
                const end = @min(self.prepared_inputs, i + 512);
                while (i < end and clock.monotonicNs() < deadline) : (i += 1) inputs[i].release(backend.allocator);
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
        if (self.reservation) |*lease| lease.release();
    }
};

pub fn releaseOutputsLocked(backend: anytype, outputs: *std.ArrayListUnmanaged(Run), discard: bool) void {
    var index: usize = 0;
    while (index < outputs.items.len) {
        runtime.unlockBackend(@TypeOf(backend.*), backend, true);
        const deadline = clock.monotonicNs() +| quantum_ns;
        const end = @min(outputs.items.len, index + 512);
        while (index < end and clock.monotonicNs() < deadline) : (index += 1) {
            const run = &outputs.items[index];
            if (discard) if (backend.storage) |storage| if (run.path) |path| repository.deleteFileAbsoluteWithStorage(storage, path) catch {};
            run.deinit(backend.allocator);
        }
        if (backend.manifestCoordinationIo()) |io| io.sleep(.fromNanoseconds(1), .awake) catch {};
        _ = runtime.lockBackend(@TypeOf(backend.*), backend);
    }
    outputs.deinit(backend.allocator);
    outputs.* = .empty;
}

const TestFixture = struct {
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
        var backend = Backend.init(allocator, .{ .wal_enabled = false, .resource_manager = &manager });
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

test "compaction publication stages and rebases all roots and cleans every allocation failure" {
    for (0..6) |variant| try std.testing.checkAllAllocationFailures(std.testing.allocator, TestFixture.check, .{variant});
}

test "compaction publication atomic fence scaling benchmark" {
    if (@import("builtin").mode != .ReleaseFast) return error.SkipZigTest;
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
