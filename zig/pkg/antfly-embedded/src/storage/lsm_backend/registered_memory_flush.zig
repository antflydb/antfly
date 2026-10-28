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

//! Retained conversion of one registered memory run into native SST outputs.
//! The caller prepays this stable frame, owns Backend lifetime before capture,
//! and serializes its calls. Capture/reserve/install/deinit require Backend.mu;
//! selection, building and publication preparation run off-lock. The actual
//! manifest turn pins Backend through joined deinit. Reader/source leases remain
//! independent; no original execution port is retained.
const std = @import("std");
const admission = @import("../../common/read_admission.zig");
const Scope = @import("publication_resources.zig").Owner;
const Directory = @import("run_directory.zig").Directory;
const Run = @import("repository.zig").Run;
const State = @import("state.zig").State;
const Builder = @import("registered_flush_builder.zig");
const Batch = @import("registered_flush_outputs.zig").Owner;

pub fn Flush(comptime Backend: type) type {
    return struct {
        const Self = @This();
        backend: ?*Backend = null,
        scope: ?*Scope = null,
        frame: ?*Scope.Publication.Candidate = null,
        turn: ?Backend.ManifestTurn = null,
        captured: bool = false,
        selected: bool = false,
        reserved: bool = false,
        live: bool = false,
        closed: bool = false,
        selection: ?Directory.Cursor = null,
        handle: ?Directory.Handle = null,
        source: ?Directory.RegisteredSource = null,
        source_writer: ?Run = null,
        writer_rank: usize = 0,
        states: [1]*const State = undefined,
        row: usize = 0,
        rows: State.EntryCursor = .{},
        logical_bytes: u64 = 0,
        started_ns: u64 = 0,
        first_id: u64 = 0,
        end_id: u64 = 0,
        builder: ?*Builder.Owner = null,
        batch: ?*Batch = null,
        outputs: ?Batch.Cursor = null,
        output: ?Batch.View = null,
        entry: ?Directory.EntryLease = null,
        record: ?*Scope.RegisteredWriter = null,
        run: ?Run = null,
        phase: enum { remove, remove_resume, next, entry, writer, append, applying, bind, validate, ready } = .remove,

        pub fn prepareWithAdmission(self: *Self, b: *Backend, original: admission.Port) !void {
            try original.checkpoint();
            if (self.closed or self.captured or (self.backend != null and self.backend != b)) return error.InvalidArgument;
            if (b.options.resource_manager == null or b.options.unpublished_outputs == null or b.storage == null or b.root_dir == null) return error.ExecutionWaitUnsupported;
            self.backend = b;
            if (self.scope == null) self.scope = try b.beginPublicationResourcesWithAdmission(original);
            try self.scope.?.resumeWithAdmission(original);
            if (self.frame == null) self.frame = try self.scope.?.preparePublicationWithAdmission(original);
        }

        /// Fixed root capture. A busy lane returns without waiting under mu.
        pub fn captureWithAdmission(self: *Self, original: admission.Port) !bool {
            try original.checkLifetime();
            const b = self.backend orelse return error.InvalidArgument;
            if (self.closed or self.captured or self.frame == null) return error.InvalidArgument;
            if (b.closing.load(.acquire)) return error.BackendClosing;
            if (b.options.backend.read_only) return error.ReadOnly;
            if (b.manifest_recovery_required) return error.RecoveryRequired;
            if (b.registered_publication.seal == null or b.run_directory_dirty) return error.ExecutionWaitUnsupported;
            const directory = b.run_directory orelse return error.ExecutionWaitUnsupported;
            if (directory.memory_run_count == 0) return false;
            if (b.options.run_partition_key != null and b.options.run_partition_key_with_admission == null) return error.ExecutionWaitUnsupported;
            if (self.turn == null) self.turn = (try b.tryBeginManifestTurnWithAdmission(original)) orelse return error.ResourceBudgetExceeded;
            try b.capturePreparedPublicationWithAdmission(self.frame.?, &b.registered_publication, b.run_directory_generation, original);
            self.started_ns = b.writeStatsNowNs();
            self.captured = true;
            return true;
        }

        /// Select only from the retained private tuple. Failed source acquisition
        /// keeps the exact handle, so resuming cannot skip an input.
        pub fn selectWithAdmission(self: *Self, original: admission.Port) !void {
            try original.checkpoint();
            if (self.closed or !self.captured or self.live) return error.InvalidArgument;
            if (self.selected) return;
            const frame = self.frame.?;
            try frame.prepareUnchangedWithAdmission(original);
            const roots = try frame.preparedRootsWithAdmission(original);
            if (self.handle == null) {
                if (self.selection == null) self.selection = roots.directory.?.readCursor();
                while (true) {
                    try original.checkpoint();
                    const handle = self.selection.?.next() orelse return error.CompactionPlanningStale;
                    if (handle.run.path == null) {
                        self.handle = handle;
                        self.selection = null;
                        break;
                    }
                }
            }
            if (self.source == null) self.source = try self.handle.?.retainRegisteredSourceWithAdmission(original);
            const source = self.source.?.run();
            if (!self.source.?.routing.matches(.fromOptions(self.backend.?.options))) return error.CompactionPlanningStale;
            if (source.path != null or source.state == null) return error.RunStateUnavailable;
            if (self.source_writer == null) {
                const writer = roots.writer.?;
                while (self.writer_rank < writer.count()) {
                    try original.checkpoint();
                    const run = writer.at(self.writer_rank);
                    if (run.id == source.id) {
                        const record = try Scope.RegisteredWriter.validate(run, original);
                        if (record.source.?.lease.?.entry.payload != self.source.?.lease.?.entry.payload) return error.CompactionPlanningStale;
                        self.source_writer = run.retainOwned();
                        break;
                    }
                    self.writer_rank += 1;
                }
                if (self.source_writer == null) return error.CompactionPlanningStale;
            }
            self.states[0] = &source.state.?;
            if (self.states[0].entryCount() == 0) return error.EmptyRun;
            while (self.row < self.states[0].entryCount()) {
                try original.checkpoint();
                const entry = self.rows.at(self.states[0], self.row);
                self.logical_bytes +|= @sizeOf(@import("state.zig").OwnedEntry) + entry.key.len + entry.value.len + (if (entry.namespace_name) |name| name.len else @as(usize, 0));
                self.row += 1;
            }
            self.selected = true;
        }

        pub fn reserveWithAdmission(self: *Self, original: admission.Port) !void {
            try original.checkLifetime();
            const b = self.backend orelse return error.InvalidArgument;
            if (self.closed or !self.selected or self.live) return error.InvalidArgument;
            if (self.reserved) return;
            if (b.closing.load(.acquire)) return error.BackendClosing;
            if (b.manifest_recovery_required) return error.RecoveryRequired;
            try b.validateCapturedPublicationWithAdmission(self.frame.?, &b.registered_publication, original);
            const end = std.math.add(u64, b.next_run_id, self.states[0].entryCount()) catch return error.ResourceBudgetExceeded;
            self.first_id = b.next_run_id;
            self.end_id = end;
            b.next_run_id = end;
            self.reserved = true;
        }

        pub fn buildWithAdmission(self: *Self, config: Builder.Config, original: admission.Port) !void {
            try original.checkpoint();
            const b = self.backend orelse return error.InvalidArgument;
            if (self.closed or !self.reserved or self.live) return error.InvalidArgument;
            if (self.builder == null) {
                var effective = config;
                effective.first_id = self.first_id;
                effective.end_id = self.end_id;
                const source = self.source.?.run();
                effective.output_level = source.level;
                effective.visibility_id = if (source.visibility_id != 0) source.visibility_id else source.id;
                effective.gc_requested = source.gc_requested;
                effective.oldest_tombstone_unix_ns = source.oldest_tombstone_unix_ns;
                effective.partition_prefix_bytes = b.options.run_partition_prefix_bytes;
                effective.partition_key = if (b.options.run_partition_key != null) b.options.run_partition_key_with_admission else null;
                self.builder = try self.scope.?.beginFlushBuilderWithAdmission(b.options.unpublished_outputs.?, b.root_dir.?, &self.states, effective, original);
            }
            try self.builder.?.driveWithAdmission(b.storage.?, original);
            if (self.batch == null) self.batch = try self.scope.?.beginFlushOutputsWithAdmission(self.builder.?, original);
            try self.batch.?.collectWithAdmission(original);
        }

        pub fn preparePublicationWithAdmission(self: *Self, original: admission.Port) !void {
            const b = self.backend orelse return error.InvalidArgument;
            const batch = self.batch orelse return error.InvalidArgument;
            if (self.closed or self.live or !batch.collected) return error.InvalidArgument;
            while (true) {
                try original.checkpoint();
                const frame = self.frame.?;
                switch (self.phase) {
                    .remove => {
                        const lease = &self.source.?.lease.?;
                        try frame.appendRemoveWithAdmission(.{ .run = lease.entry.run, .revision = lease.entry.payload.? }, &self.source_writer.?, 0, original);
                        self.phase = .remove_resume;
                    },
                    .remove_resume => {
                        try frame.resumeWithAdmission(original);
                        self.phase = .next;
                    },
                    .next => {
                        if (self.outputs == null) self.outputs = try batch.cursorWithAdmission(original);
                        self.output = try self.outputs.?.nextWithAdmission(original);
                        self.phase = if (self.output == null) .bind else .entry;
                    },
                    .entry => {
                        if (self.scope.?.entry == null) {
                            const roots = try frame.preparedRootsWithAdmission(original);
                            try self.scope.?.beginEntryFromRegisteredOutputWithAdmission(b, roots.directory.?, self.output.?.output, self.output.?.metadata, original);
                        }
                        self.entry = try self.scope.?.takeEntryWithAdmission(original);
                        self.phase = .writer;
                    },
                    .writer => {
                        if (self.record == null) self.record = try self.scope.?.beginRegisteredWriterWithAdmission(b, &self.entry.?, .{}, original);
                        self.run = try self.record.?.takeRunWithAdmission(original);
                        self.phase = .append;
                    },
                    .append => {
                        try frame.appendPutWithAdmission(&self.entry.?, &self.run.?, original);
                        self.phase = .applying;
                    },
                    .applying => {
                        try frame.resumeWithAdmission(original);
                        self.releaseOutput();
                        self.phase = .next;
                    },
                    .bind => {
                        try batch.bindPublicationWithAdmission(frame, original);
                        self.phase = .validate;
                    },
                    .validate => {
                        try batch.validatePublicationWithAdmission(original);
                        self.phase = .ready;
                    },
                    .ready => return,
                }
            }
        }

        /// Fixed coupled swap and live-ticket receipt. Input deletion is only
        /// registered reference release; WAL acknowledgment is a later checkpoint.
        pub fn installWithAdmission(self: *Self, original: admission.Port) !void {
            try original.checkLifetime();
            const b = self.backend orelse return error.InvalidArgument;
            if (self.closed or self.live or self.phase != .ready or self.turn == null) return error.InvalidArgument;
            if (b.manifest_recovery_required) return error.RecoveryRequired;
            if (!self.source.?.routing.matches(.fromOptions(b.options))) return error.CompactionPlanningStale;
            try self.batch.?.installWithAdmission(b, original);
            self.frame = null; // install returned it to independent retirement
            self.live = true;
            b.unpublished_wal_logical_bytes +|= self.logical_bytes;
            b.unpublished_wal_max_batch_logical_bytes = @max(b.unpublished_wal_max_batch_logical_bytes, self.logical_bytes);
            b.write_stats.flushes +|= 1;
            b.write_stats.flush_ns +|= b.writeStatsElapsedNs(self.started_ns);
            b.write_stats.flush_input_entries +|= self.builder.?.input_entries;
            b.write_stats.flush_output_runs +|= self.batch.?.count;
            b.write_stats.flush_output_bytes +|= self.batch.?.total_size_bytes;
            b.write_stats.table_file_writes +|= self.batch.?.count;
            b.write_stats.table_file_bytes +|= self.batch.?.total_size_bytes;
            b.recordTableCompressionWriteStats(self.batch.?.compression_stats);
        }
        fn releaseOutput(self: *Self) void {
            const b = self.backend.?;
            if (self.run) |*run| run.deinit(b.allocator);
            self.run = null;
            if (self.record) |record| record.requestRetirement();
            self.record = null;
            if (self.entry) |*entry| entry.deinit(b.allocator);
            self.entry = null;
            self.output = null;
        }
        /// All accepted physical work has joined before these fixed drops.
        /// Builder input borrows stop before releasing source/root owners.
        pub fn deinit(self: *Self) void {
            if (self.closed) return;
            if (self.backend == null) {
                self.closed = true;
                return;
            }
            self.releaseOutput();
            if (self.batch) |batch| batch.requestRetirement();
            if (self.builder) |builder| builder.requestRetirement();
            if (self.frame) |frame| frame.requestRetirement();
            if (self.scope) |scope| scope.requestRetirement();
            if (self.source_writer) |*run| run.deinit(self.backend.?.allocator);
            if (self.source) |*source| source.releaseWithAdmission(null) catch unreachable;
            if (self.turn) |*turn| turn.deinit();
            self.* = .{ .closed = true };
        }
    };
}
