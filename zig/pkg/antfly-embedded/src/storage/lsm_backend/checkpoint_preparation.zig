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

//! Prepaid checkpoint roots for a quiesced backend. No filesystem work or
//! backend mutex spans CPU admission. Live directory materialization and the
//! existing observer baseline are prerequisites. The caller prepays memoryBound()
//! for the fixed preparation and transferred input headers and retains the backend, manager, allocator and live directory until
//! transfer/retirement. No original port is retained.
const std = @import("std");
const admission = @import("../../common/read_admission.zig");
const resources = @import("../resource_manager.zig");
const repository = @import("repository.zig");
const Ledger = @import("obsolete_ledger.zig").Ledger;
const Snapshot = @import("ledger_reclamation.zig").Snapshot;
const Directory = @import("run_directory.zig").Directory;
const Account = @import("memory_account.zig").Account;

pub fn Preparation(comptime Backend: type) type {
    return struct {
        pub const Inputs = struct {
            snapshot: *Snapshot,
            live_snapshot: *Snapshot,
            directory: *Directory,
            source: *const Directory,
            prior_root: @FieldType(@FieldType(Ledger, "tree"), "root"),
            prepared_root: @FieldType(@FieldType(Ledger, "tree"), "root"),
            fence: repository.ManifestJournal.CheckpointFence,
            metadata: ?Backend.MetadataCredit,
            pub fn matches(self: *const @This(), b: *Backend) bool {
                return self.metadata != null and self.metadata.?.backend == b and self.fence.matches(b) and b.run_directory == self.source and !b.run_directory_dirty and
                    b.obsolete_paths.tree.root == self.prior_root and b.manifest_checkpoint_directory == self.directory and
                    !self.snapshot.retired and !self.live_snapshot.retired and self.snapshot.value.tree.root == self.prepared_root and self.live_snapshot.value.tree.root == self.prepared_root;
            }
            /// Caller still fences the backend; fixed ownership receipts only.
            /// Successful publisher initialization consumes this bundle instead.
            pub fn retire(self: *@This(), b: *Backend) void {
                if (self.metadata == null) return;
                std.debug.assert(self.metadata.?.backend == b);
                self.snapshot.retire(b);
                self.live_snapshot.retire(b);
                if (b.manifest_checkpoint_directory == self.directory) b.manifest_checkpoint_directory = null;
                b.retireCheckpointDirectory(self.directory);
                self.metadata.?.release();
                self.metadata = null;
            }
        };
        backend: *Backend,
        source: *const Directory,
        prior_root: @FieldType(@FieldType(Ledger, "tree"), "root"),
        fence: repository.ManifestJournal.CheckpointFence,
        candidate: ?Ledger = null,
        directory: ?*Directory = null,
        snapshot: ?*Snapshot = null,
        live_snapshot: ?*Snapshot = null,
        published_spares: u64 = 0,
        deadline: u64,
        path_deadline: u64 = 0,
        path: ?[]u8 = null,
        buffer: [std.fs.max_path_bytes]u8 = undefined,
        path_len: usize = 0,
        index: usize = 0,
        edits: usize = 0,
        path_bytes: u64 = 0,
        wire_bytes: u64 = 0,
        metadata: ?Backend.MetadataCredit = null,
        credit: ?resources.ConstructionCredit = null,
        reclaimer: ?Ledger.Reclaimer = null,
        meter: ?*Account,
        baseline_bytes: u64,
        baseline_usage: u64,
        bound: u64 = 0,
        retirement_usage: ?u64 = null,
        phase: enum { plan, reserve, directory, snapshot, live_snapshot, select, prepare, path, copy, edit, adopt, publish, complete } = .plan,
        published: bool = false,
        adopted: bool = false,
        aborted: bool = false,
        transferred: bool = false,
        settled: bool = false,
        retired: bool = false,

        pub fn memoryBound() u64 {
            return @sizeOf(@This()) + @sizeOf(Inputs);
        }
        pub fn init(b: *Backend) !@This() {
            if (comptime @hasField(Backend, "registered_publication")) if (b.registered_publication.seal != null) return error.ExecutionWaitUnsupported;
            if (b.options.resource_manager == null or b.root_dir == null) return error.ExecutionWaitUnsupported;
            if (b.manifest_recovery_required) return error.RecoveryRequired;
            if (b.active_readers != 0 or b.manifest_publish_in_flight or b.manifest_checkpoint_build_in_flight or b.ledger_reclaim_in_flight or b.manifest_checkpoint_directory != null) return error.ResourceBudgetExceeded;
            const source = b.run_directory orelse return error.ExecutionWaitUnsupported;
            if (b.run_directory_dirty or source.memory_run_count != 0) return error.ExecutionWaitUnsupported;
            const meter = b.obsolete_paths.tree.account;
            const bytes = if (meter) |a| a.bytes.load(.acquire) else 0;
            const spares = b.obsolete_paths.spareBytes();
            std.debug.assert(b.tracked_in_memory_state_bytes >= bytes + spares);
            return .{ .backend = b, .source = source, .fence = .capture(b), .meter = if (meter) |a| a.retain() else null, .baseline_bytes = bytes, .prior_root = b.obsolete_paths.tree.root, .deadline = b.nowNs() +| b.options.obsolete_retention_ns, .baseline_usage = b.tracked_in_memory_state_bytes };
        }
        fn matches(self: *const @This()) bool {
            const b = self.backend;
            return self.fence.matches(b) and b.obsolete_paths.tree.root == self.prior_root and b.run_directory == self.source and !b.run_directory_dirty and b.manifest_checkpoint_directory == null and !b.manifest_checkpoint_build_in_flight and !b.manifest_publish_in_flight;
        }
        fn pinMeter(self: *@This()) void {
            if (self.meter == null) if (self.candidate) |*candidate| {
                if (candidate.tree.account) |a| self.meter = a.retain();
            };
        }
        fn headers() u64 {
            return @sizeOf(Directory) + 2 * @sizeOf(Snapshot);
        }
        pub fn resumeWithAdmission(self: *@This(), original: admission.Port) !void {
            if (self.aborted or self.transferred or self.retired) return error.InvalidArgument;
            const b = self.backend;
            const manager = b.options.resource_manager.?;
            if (!self.published and !self.matches()) return error.RecoveryRequired;
            while (self.phase != .complete) {
                try original.checkpoint();
                switch (self.phase) {
                    .plan => {
                        while (self.index < self.fence.retiredPathCount()) {
                            try original.checkpoint();
                            const path = try self.fence.retiredPathAt(b.root_dir.?, self.index, &self.buffer);
                            if (path.len > 4096) return error.NameTooLong;
                            const bytes = try std.math.add(u64, self.path_bytes, path.len);
                            const wire = try std.math.add(u64, self.wire_bytes, 32 + path.len);
                            self.edits += 1;
                            self.path_bytes = bytes;
                            self.wire_bytes = wire;
                            self.index += 1;
                        }
                        self.bound = try std.math.add(u64, headers(), try std.math.add(u64, try b.obsolete_paths.prepareMemoryBound(self.edits), self.path_bytes));
                        self.phase = .reserve;
                    },
                    .reserve => {
                        if (self.metadata == null) self.metadata = try b.admitCompactionMetadataBytes(self.wire_bytes);
                        try manager.prepareUsageWithAdmission(.lsm_in_memory_state, &b.tracked_in_memory_state_bytes, original);
                        if (self.credit == null) self.credit = try manager.reserveConstructionWithAdmission(.lsm_table_builder_working_set, self.bound, original);
                        self.candidate = b.obsolete_paths.fork();
                        self.phase = .directory;
                    },
                    .directory => {
                        self.directory = try self.source.fork(b.allocator);
                        self.phase = .snapshot;
                    },
                    .snapshot => {
                        // Private uninitialized header until the fixed receipt.
                        self.snapshot = try b.allocator.create(Snapshot);
                        self.phase = .live_snapshot;
                    },
                    .live_snapshot => {
                        self.live_snapshot = try b.allocator.create(Snapshot);
                        self.index = 0;
                        self.phase = .select;
                    },
                    .select => {
                        if (self.index == self.fence.retiredPathCount()) {
                            self.phase = .adopt;
                            continue;
                        }
                        const path = try self.fence.retiredPathAt(b.root_dir.?, self.index, &self.buffer);
                        self.path_len = path.len;
                        const old = self.candidate.?.get(path);
                        self.path_deadline = if (old) |value| if (value.delete_after_ns == std.math.maxInt(u64)) self.deadline else value.delete_after_ns else self.deadline;
                        self.phase = .prepare;
                    },
                    .prepare => {
                        self.candidate.?.prepareWithAdmission(b.allocator, original) catch |cause| {
                            self.pinMeter();
                            return cause;
                        };
                        self.pinMeter();
                        self.phase = .path;
                    },
                    .path => {
                        self.path = try b.allocator.alloc(u8, self.path_len);
                        self.phase = .copy;
                    },
                    .copy => {
                        @memcpy(self.path.?, self.buffer[0..self.path_len]);
                        self.phase = .edit;
                    },
                    .edit => {
                        // Finite deadlines remain private until descriptor
                        // durability. An abort cannot expose live-file deletes.
                        self.candidate.?.appendAssumeCapacity(.{ .path = self.path.?, .delete_after_ns = self.path_deadline });
                        self.path = null;
                        self.index += 1;
                        self.phase = .select;
                    },
                    .adopt => {
                        const bytes = if (self.meter) |a| a.bytes.load(.acquire) else 0;
                        const added = bytes - self.baseline_bytes + self.candidate.?.spareBytes() + headers();
                        std.debug.assert(added <= self.bound);
                        try self.credit.?.adoptUsageWithAdmission(.lsm_in_memory_state, &b.tracked_in_memory_state_bytes, added, original);
                        self.adopted = true;
                        self.phase = .publish;
                    },
                    .publish => {
                        if (!self.matches()) return error.RecoveryRequired;
                        self.published_spares = self.candidate.?.spareBytes();
                        const snapshot = self.snapshot.?;
                        snapshot.* = .{ .value = self.candidate.?, .active_next = b.ledger_snapshots };
                        self.candidate = null;
                        if (snapshot.active_next) |next| next.active_previous = snapshot;
                        b.ledger_snapshots = snapshot;
                        const live = self.live_snapshot.?;
                        live.* = .{ .value = snapshot.value.fork(), .active_next = snapshot };
                        snapshot.active_previous = live;
                        b.ledger_snapshots = live;
                        b.manifest_checkpoint_directory = self.directory;
                        self.published = true;
                        self.phase = .complete;
                    },
                    .complete => unreachable,
                }
            }
        }

        fn checkpoint(original: ?admission.Port) !void {
            if (original) |port| try port.checkpoint();
        }
        fn settle(self: *@This(), original: ?admission.Port) !void {
            if (self.settled) return;
            const b = self.backend;
            if (self.path) |path| {
                try checkpoint(original);
                b.allocator.free(path);
                self.path = null;
            }
            self.pinMeter();
            if (self.candidate) |candidate| {
                self.reclaimer = .init(candidate);
                self.candidate = null;
            }
            if (self.reclaimer) |*reclaimer| {
                while (true) {
                    try checkpoint(original);
                    var steps: usize = 1;
                    if (reclaimer.step(b.allocator, &steps)) break;
                }
                self.reclaimer = null;
            }
            if (self.retirement_usage == null) {
                const bytes = if (self.published or self.baseline_bytes != 0) (if (self.meter) |a| a.bytes.load(.acquire) else @as(u64, 0)) else 0;
                // Registered private roots, including their spare vectors, stay
                // observed when abort transfers them to retirement queues.
                self.retirement_usage = self.baseline_usage - self.baseline_bytes + bytes + if (self.published) headers() + self.published_spares else @as(u64, 0);
                if (self.meter) |a| a.release();
                self.meter = null;
            }
            if (self.adopted) {
                const manager = b.options.resource_manager.?;
                if (original) |port| try manager.adjustUsageWithAdmission(.lsm_in_memory_state, &b.tracked_in_memory_state_bytes, self.retirement_usage.?, port) else std.debug.assert(manager.tryObserveUsage(.lsm_in_memory_state, &b.tracked_in_memory_state_bytes, self.retirement_usage.?));
            }
            if (self.credit) |*credit| credit.retire();
            self.credit = null;
            self.settled = true;
        }

        /// Settle private construction before allowing publisher mutation.
        /// Rejected accounting retains all prepared roots and their funding.
        pub fn takeWithAdmission(self: *@This(), original: admission.Port) !Inputs {
            try original.checkpoint();
            if (!self.published or self.aborted or self.transferred or self.retired) return error.InvalidArgument;
            const b = self.backend;
            if (!self.fence.matches(b) or b.run_directory != self.source or b.run_directory_dirty or b.obsolete_paths.tree.root != self.prior_root or b.manifest_checkpoint_directory != self.directory) return error.RecoveryRequired;
            try self.settle(original);
            const inputs: Inputs = .{ .snapshot = self.snapshot.?, .live_snapshot = self.live_snapshot.?, .directory = self.directory.?, .source = self.source, .prior_root = self.prior_root, .prepared_root = self.snapshot.?.value.tree.root, .fence = self.fence, .metadata = self.metadata };
            self.snapshot = null;
            self.live_snapshot = null;
            self.directory = null;
            self.metadata = null;
            self.transferred = true;
            return inputs;
        }
        fn retire(self: *@This(), original: ?admission.Port) !void {
            try checkpoint(original);
            if (self.transferred or self.retired) return;
            self.aborted = true;
            const b = self.backend;
            if (self.metadata) |*metadata| metadata.release();
            self.metadata = null;
            if (self.directory) |directory| {
                try checkpoint(original);
                if (self.published) {
                    if (b.manifest_checkpoint_directory == directory) b.manifest_checkpoint_directory = null;
                    b.retireCheckpointDirectory(directory);
                } else directory.destroy(b.allocator); // All roots share live source.
                self.directory = null;
            }
            if (self.snapshot) |snapshot| {
                try checkpoint(original);
                if (self.published) snapshot.retire(b) else b.allocator.destroy(snapshot);
                self.snapshot = null;
            }
            if (self.live_snapshot) |snapshot| {
                try checkpoint(original);
                if (self.published) snapshot.retire(b) else b.allocator.destroy(snapshot);
                self.live_snapshot = null;
            }
            try self.settle(original);
            self.retired = true;
        }
        pub fn retireWithAdmission(self: *@This(), original: admission.Port) !void {
            try self.retire(original);
        }
        /// Joined, quiesced fallback only. Normal refusal retains this owner.
        pub fn deinit(self: *@This()) void {
            self.retire(null) catch unreachable;
        }
    };
}
