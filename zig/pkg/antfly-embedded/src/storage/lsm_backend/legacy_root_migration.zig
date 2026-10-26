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

//! Startup-only copy/install bridge for legacy file roots. The caller prepays
//! this stable frame and excludes ALL Backend access (including accounting)
//! between capture and joined deinit. Legacy physical destruction is explicitly
//! joined, not claimed to be an admitted bounded maintenance operation.
const std = @import("std");
const admission = @import("../../common/read_admission.zig");
const Publication = @import("registered_publication.zig");
const Scope = @import("publication_resources.zig").Owner;
const Directory = @import("run_directory.zig").Directory;
const Store = @import("run_store.zig").Store;
const Ledger = @import("obsolete_ledger.zig").Ledger;
const Run = @import("repository.zig").Run;
const Fence = @import("repository.zig").ManifestJournal.CheckpointFence;

const TreeStamp = struct {
    root: ?*const anyopaque,
    account: ?*const anyopaque,
    spare: ?*const anyopaque,
    len: usize,
    capacity: usize,
    fn of(tree: anytype) TreeStamp {
        return .{ .root = tree.root, .account = tree.account, .spare = if (tree.spare.capacity == 0) null else @ptrCast(tree.spare.items.ptr), .len = tree.spare.items.len, .capacity = tree.spare.capacity };
    }
};
const DirectoryStamp = struct {
    pointer: ?*const Directory,
    trees: [6]TreeStamp = undefined,
    bytes: u64 = 0,
    memory: usize = 0,
    fn of(directory: ?*const Directory) DirectoryStamp {
        var out: DirectoryStamp = .{ .pointer = directory };
        if (directory) |d| {
            inline for (.{ "tree", "ids", "bounds", "ends", "levels", "generations" }, 0..) |field, i| out.trees[i] = .of(@field(d, field));
            out.bytes = d.total_run_bytes;
            out.memory = d.memory_run_count;
        } else out.trees = @splat(.{ .root = null, .account = null, .spare = null, .len = 0, .capacity = 0 });
        return out;
    }
};
const LedgerStamp = struct {
    tree: TreeStamp,
    spare: ?*const anyopaque,
    len: usize,
    capacity: usize,
    fn of(ledger: *const Ledger) LedgerStamp {
        return .{ .tree = .of(ledger.tree), .spare = if (ledger.spare.capacity == 0) null else @ptrCast(ledger.spare.items.ptr), .len = ledger.spare.items.len, .capacity = ledger.spare.capacity };
    }
};

pub fn Migration(comptime Backend: type) type {
    return struct {
        const Self = @This();
        const Stamp = struct {
            directory: DirectoryStamp,
            writer: TreeStamp,
            obsolete: LedgerStamp,
            durable_directory: DirectoryStamp,
            durable_obsolete: LedgerStamp,
            generation: u64,
            next_id: u64,
            fn of(b: *const Backend) Stamp {
                return .{ .directory = .of(b.run_directory), .writer = .of(b.runs.tree), .obsolete = .of(&b.obsolete_paths), .durable_directory = .of(b.manifest_directory), .durable_obsolete = .of(&b.manifest_journal.obsolete), .generation = b.run_directory_generation, .next_id = b.next_run_id };
            }
        };
        backend: ?*Backend = null,
        turn: ?Backend.ManifestTurn = null,
        stamp: ?Stamp = null,
        fence: ?Fence = null,
        scope: ?*Scope = null,
        frame: ?*Publication.Candidate = null,
        cursor: ?Directory.Cursor = null,
        source: ?Run = null,
        snapshot: ?*Scope.FileSnapshot.Owner = null,
        entry: ?Directory.EntryLease = null,
        record: ?*Scope.RegisteredWriter = null,
        run: ?Run = null,
        phase: enum { initialize, snapshot, entry, writer, append, applying, obsolete, obsolete_resume, ready, installed, closed } = .initialize,
        obsolete_rank: usize = 0,
        receipt: ?Publication.Candidate.PreparedReceipt = null,
        old_store: ?Store = null,
        old_directory: ?*Directory = null,
        old_obsolete: ?Ledger = null,
        old_durable_directory: ?*Directory = null,
        old_durable_obsolete: ?Ledger = null,
        directory_cleanup: ?Directory.Reclaimer = null,
        durable_directory_cleanup: ?Directory.Reclaimer = null,
        obsolete_cleanup: ?Ledger.Reclaimer = null,
        durable_obsolete_cleanup: ?Ledger.Reclaimer = null,

        fn validateQuiescence(b: *const Backend, reserved: bool) !void {
            if (b.closing.load(.acquire)) return error.BackendClosing;
            if (b.options.backend.read_only) return error.ReadOnly;
            if (b.manifest_recovery_required) return error.RecoveryRequired;
            if (b.registered_publication.seal != null or b.registered_manifest_checkpoint != null) return error.InvalidArgument;
            if (b.active_readers != @intFromBool(reserved) or b.read_version != null or b.domain_index != null or b.live_read_versions != null) return error.ResourceBudgetExceeded;
            inline for (.{ "immutable_flush_job_in_flight", "immutable_flush_build_in_flight", "maintenance_job_in_flight", "manifest_checkpoint_build_in_flight", "directory_planning_in_flight", "admission_in_flight", "admission_reclaim_in_flight", "bulk_plan_in_flight", "bulk_plan_reclaim_in_flight", "tombstone_reconcile_in_flight", "gc_reclaim_in_flight", "obsolete_reclaim_in_flight", "closure_reclaim_in_flight", "store_reclaim_in_flight", "directory_reclaim_in_flight", "read_metadata_reclaim_in_flight", "ledger_reclaim_in_flight", "memtable_reclaim_in_flight" }) |field| {
                if (@field(b, field)) return error.ResourceBudgetExceeded;
            }
            inline for (.{ "pending_directory_closure", "pending_l0_directory_closure", "pending_bulk_plan", "pending_tombstone_reconcile", "pending_gc", "active_compaction_publications", "active_bulk_plans", "retired_run_stores", "retired_run_directories", "store_reclaimer", "directory_reclaimer", "retired_read_versions", "retired_projection_versions", "retired_domain_indexes", "read_metadata_reclaimer", "manifest_checkpoint_directory", "active_ledger_reclamations", "ledger_snapshots", "retired_ledger_snapshots", "retired_admissions", "retired_bulk_plans", "retired_gc", "retired_closures" }) |field| {
                if (@field(b, field) != null) return error.ResourceBudgetExceeded;
            }
            for (b.pending_admissions) |pending| if (pending != null) return error.ResourceBudgetExceeded;
            const d = b.run_directory orelse return error.ExecutionWaitUnsupported;
            if (b.run_directory_dirty or d.memory_run_count != 0 or d.count() != b.runs.count()) return error.ExecutionWaitUnsupported;
            if (d.registered_owner != null or d.registered_account != null or b.runs.registered_owner != null or b.runs.registered_account != null or b.obsolete_paths.registered_owner != null or b.manifest_journal.obsolete.registered_owner != null) return error.ExecutionWaitUnsupported;
            if (b.manifest_directory) |durable| {
                if (durable == d or durable.registered_owner != null or durable.registered_account != null) return error.ExecutionWaitUnsupported;
            }
        }

        /// Under Backend.mu, before any source borrow. The retained manifest
        /// lane pins Backend lifetime; external startup exclusion remains required.
        pub fn captureWithAdmission(self: *Self, b: *Backend, original: admission.Port) !void {
            try original.checkLifetime();
            if (self.backend != null or self.phase != .initialize) return error.InvalidArgument;
            try validateQuiescence(b, false);
            _ = b.options.resource_manager orelse return error.ExecutionWaitUnsupported;
            if (b.manifest_publish_in_flight) return error.ResourceBudgetExceeded;
            const turn = (try b.tryBeginManifestTurnWithAdmission(original)) orelse return error.ResourceBudgetExceeded;
            self.backend = b;
            self.turn = turn;
            self.stamp = .of(b);
            self.fence = .capture(b);
            self.cursor = b.run_directory.?.readCursor();
        }

        /// Off-lock. Progress is saved before every later refusal. No execution
        /// port escapes; producer file authority remains in the quiesced Backend.
        pub fn resumeWithAdmission(self: *Self, original: admission.Port) !void {
            const b = self.backend orelse return error.InvalidArgument;
            while (true) {
                try original.checkpoint();
                switch (self.phase) {
                    .initialize => {
                        if (self.scope == null) self.scope = try b.beginPublicationResourcesWithAdmission(original);
                        try self.scope.?.resumeWithAdmission(original);
                        if (self.frame == null) {
                            self.frame = try self.scope.?.preparePublicationWithAdmission(original);
                        }
                        if (!self.frame.?.source_captured) try self.frame.?.captureRootsWithAdmission(&Publication.Roots.empty, original);
                        try self.frame.?.prepareUnchangedWithAdmission(original);
                        self.phase = .snapshot;
                    },
                    .snapshot => {
                        if (self.source == null) self.source = if (self.cursor.?.next()) |handle| handle.run.* else {
                            self.phase = .obsolete;
                            continue;
                        };
                        if (self.snapshot == null) self.snapshot = try self.scope.?.beginFileSnapshotWithAdmission(self.source.?, original);
                        try self.snapshot.?.resumeWithAdmission(original);
                        self.phase = .entry;
                    },
                    .entry => {
                        if (self.scope.?.entry == null) {
                            // Preparation borrows only the old directory's real
                            // account. Successful take replaces it with the
                            // entry owner's independently funded account.
                            try self.scope.?.beginEntryFromFileSnapshotWithAdmission(b, b.run_directory.?, self.snapshot.?, original);
                        }
                        self.entry = try self.scope.?.takeEntryWithAdmission(original);
                        self.snapshot.?.requestRetirement();
                        self.snapshot = null;
                        self.phase = .writer;
                    },
                    .writer => {
                        if (self.record == null) self.record = try self.scope.?.beginRegisteredWriterWithAdmission(b, &self.entry.?, .{}, original);
                        self.run = try self.record.?.takeRunWithAdmission(original);
                        self.phase = .append;
                    },
                    .append => {
                        try self.frame.?.appendPutWithAdmission(&self.entry.?, &self.run.?, original);
                        self.phase = .applying;
                    },
                    .applying => {
                        try self.frame.?.resumeWithAdmission(original);
                        self.releaseInput(b);
                        self.phase = .snapshot;
                    },
                    .obsolete => {
                        if (self.obsolete_rank == b.obsolete_paths.count()) {
                            self.receipt = try self.frame.?.preparedReceiptWithAdmission(original);
                            self.phase = .ready;
                            return;
                        }
                        try self.frame.?.appendLegacyObsoleteWithAdmission(&b.obsolete_paths, self.obsolete_rank, original);
                        self.phase = .obsolete_resume;
                    },
                    .obsolete_resume => {
                        try self.frame.?.resumeWithAdmission(original);
                        self.obsolete_rank += 1;
                        self.phase = .obsolete;
                    },
                    .ready => return,
                    .installed, .closed => return error.InvalidArgument,
                }
            }
        }

        fn releaseInput(self: *Self, b: *Backend) void {
            if (self.snapshot) |owner| owner.requestRetirement();
            self.snapshot = null;
            if (self.run) |*run| run.deinit(b.allocator);
            self.run = null;
            if (self.record) |owner| owner.requestRetirement();
            self.record = null;
            if (self.entry) |*entry| entry.deinit(b.allocator);
            self.entry = null;
            self.source = null;
        }

        /// Under Backend.mu. All refusals precede the fixed ownership swap.
        /// Durable files, journal lineage and WAL debt are unchanged. Detaching
        /// legacy durable RAM views forces a registered checkpoint before cleanup.
        pub fn installWithAdmission(self: *Self, original: admission.Port) !void {
            try original.checkLifetime();
            const b = self.backend orelse return error.InvalidArgument;
            if (self.phase != .ready) return error.InvalidArgument;
            try validateQuiescence(b, true);
            if (!b.manifest_publish_in_flight or !std.meta.eql(self.stamp.?, Stamp.of(b)) or !self.fence.?.matches(b)) return error.CompactionPlanningStale;
            try self.frame.?.validatePreparedReceiptFixedWithAdmission(self.receipt.?, original);
            var prepared: Publication.Roots = .{};
            try self.frame.?.publishFixedWithAdmission(&prepared, original);
            // Infallible tail: every displaced owning header has a saved owner.
            self.old_store = b.runs;
            self.old_directory = b.run_directory;
            self.old_obsolete = b.obsolete_paths;
            self.old_durable_directory = b.manifest_directory;
            self.old_durable_obsolete = b.manifest_journal.obsolete;
            b.registered_publication = prepared;
            b.runs = prepared.writer.?.*;
            b.run_directory = prepared.directory;
            b.obsolete_paths = prepared.obsolete;
            b.manifest_directory = null;
            b.manifest_journal.obsolete = .{};
            b.invalidateReadVersion();
            b.run_directory_dirty = false;
            b.manifest_dirty = true;
            b.obsolete_manifest_dirty = true;
            b.obsolete_reclaim_retry_at_ns = 0;
            b.cached_maintenance_hint.store(1, .release);
            self.frame.?.requestRetirement();
            self.frame = null;
            self.cursor = null;
            self.fence = null;
            self.phase = .installed;
        }

        /// Off-lock while startup exclusion remains held. The original port
        /// reaches every detached node and physical metadata quantum. Writer
        /// RunOwner retirement is separate; it still requires joined teardown.
        pub fn retireOldDirectoriesAndLedgersWithAdmission(self: *Self, original: admission.Port) !void {
            const b = self.backend orelse return error.InvalidArgument;
            if (self.phase != .installed) return error.InvalidArgument;
            while (self.old_directory != null) {
                try original.checkpoint();
                if (self.directory_cleanup == null) self.directory_cleanup = .init(self.old_directory.?);
                var credits: usize = 1;
                if (!try self.directory_cleanup.?.stepWithAdmission(b.allocator, &credits, original)) continue;
                self.directory_cleanup.?.finish(b.allocator);
                self.directory_cleanup = null;
                self.old_directory = null;
            }
            while (self.old_durable_directory != null) {
                try original.checkpoint();
                if (self.durable_directory_cleanup == null) self.durable_directory_cleanup = .init(self.old_durable_directory.?);
                var credits: usize = 1;
                if (!try self.durable_directory_cleanup.?.stepWithAdmission(b.allocator, &credits, original)) continue;
                self.durable_directory_cleanup.?.finish(b.allocator);
                self.durable_directory_cleanup = null;
                self.old_durable_directory = null;
            }
            while (self.old_obsolete != null) {
                try original.checkpoint();
                if (self.obsolete_cleanup == null) self.obsolete_cleanup = .init(self.old_obsolete.?);
                var credits: usize = 1;
                if (!try self.obsolete_cleanup.?.stepWithAdmission(b.allocator, &credits, original)) continue;
                self.obsolete_cleanup = null;
                self.old_obsolete = null;
            }
            while (self.old_durable_obsolete != null) {
                try original.checkpoint();
                if (self.durable_obsolete_cleanup == null) self.durable_obsolete_cleanup = .init(self.old_durable_obsolete.?);
                var credits: usize = 1;
                if (!try self.durable_obsolete_cleanup.?.stepWithAdmission(b.allocator, &credits, original)) continue;
                self.durable_obsolete_cleanup = null;
                self.old_durable_obsolete = null;
            }
        }

        /// Joined startup teardown under Backend.mu. Stop all source borrows
        /// before freeing old roots; it is deliberately NOT the production
        /// admitted retirement path. Accounting observers remain excluded until
        /// this returns, because displaced legacy charge still belongs to Backend.
        pub fn deinit(self: *Self) void {
            if (self.phase == .closed) return;
            const b = self.backend orelse {
                self.phase = .closed;
                return;
            };
            self.releaseInput(b);
            if (self.scope) |owner| owner.requestRetirement();
            self.scope = null;
            if (self.frame) |owner| owner.requestRetirement();
            self.frame = null;
            // A rejected physical step may already have consumed some nodes.
            // Join that exact cursor; never run a fresh destructor on its header.
            inline for (.{ .{ "directory_cleanup", "old_directory" }, .{ "durable_directory_cleanup", "old_durable_directory" } }) |fields| {
                if (@field(self, fields[0])) |*job| {
                    var credits: usize = std.math.maxInt(usize);
                    std.debug.assert(job.step(b.allocator, &credits));
                    job.finish(b.allocator);
                    @field(self, fields[0]) = null;
                    @field(self, fields[1]) = null;
                }
            }
            inline for (.{ .{ "obsolete_cleanup", "old_obsolete" }, .{ "durable_obsolete_cleanup", "old_durable_obsolete" } }) |fields| {
                if (@field(self, fields[0])) |*job| {
                    var credits: usize = std.math.maxInt(usize);
                    std.debug.assert(job.step(b.allocator, &credits));
                    @field(self, fields[0]) = null;
                    @field(self, fields[1]) = null;
                }
            }
            if (self.old_store) |*store| store.deinit(b.allocator);
            if (self.old_directory) |directory| directory.destroy(b.allocator);
            if (self.old_obsolete) |*ledger| ledger.deinit(b.allocator);
            if (self.old_durable_directory) |directory| directory.destroy(b.allocator);
            if (self.old_durable_obsolete) |*ledger| ledger.deinit(b.allocator);
            if (self.turn) |*turn| turn.deinit();
            self.* = .{ .phase = .closed };
        }
    };
}
