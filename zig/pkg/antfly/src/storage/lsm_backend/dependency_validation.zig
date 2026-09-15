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

//! An owned dependency certificate. Callers own its selected handles until
//! cleanup completes; this object pins every epoch used by its cursors.
//! advanceLocked performs ONE bounded off-lock slice, including scratch GC.
const std = @import("std");
const Directory = @import("run_directory.zig").Directory;
const Job = @import("dependency_job.zig").Job;
const runtime = @import("runtime.zig");
const time = @import("antfly_platform").time;
const Reservation = @import("../resource_manager.zig").Reservation;

pub const Validation = struct {
    directory: *Directory,
    latest: ?*Directory = null,
    changes: ?Directory.ChangeCursor = null,
    job: Job,
    phase: enum { identities, cleanup, certificate } = .identities,
    reservation: ?Reservation = null,
    slices: usize = 0,
    rebases: usize = 0,
    yield_between_slices: bool = false,
    pub const Result = enum { pending, valid, invalid };

    pub fn init(backend: anytype, plan: anytype) !Validation {
        var reservation: ?Reservation = null;
        errdefer if (reservation) |*lease| lease.release();
        if (backend.options.resource_manager) |manager| reservation = try manager.reserve(.lsm_table_builder_working_set, @sizeOf(Validation) + 8192 + plan.input_handles.?.len * 128);
        const directory = try (try backend.planningDirectory()).fork(backend.allocator);
        return .{ .directory = directory, .job = .init(directory, plan), .reservation = reservation };
    }

    pub fn advanceLocked(self: *Validation, backend: anytype) !Result {
        return self.advanceBudgetedLocked(backend, 2048, std.math.maxInt(u64));
    }

    pub fn advanceBudgetedLocked(self: *Validation, backend: anytype, credits: usize, deadline: u64) !Result {
        if (!self.job.valid) return .invalid;
        if (backend.manifestCoordinationIo()) |io| try io.checkCancel();
        if (self.phase == .certificate and self.changes == null) {
            const current = try backend.planningDirectory();
            if (current.tree.root == self.directory.tree.root) return .valid;
            self.latest = try current.fork(backend.allocator);
            self.changes = .init(self.directory, self.latest.?);
            self.rebases += 1;
        }
        backend.retainReaderKind(.compaction);
        defer backend.releaseReaderKind(.compaction);
        runtime.unlockBackend(@TypeOf(backend.*), backend, true);
        // Unlocking may itself run bounded reclamation. Give this job its
        // own quantum afterwards; continuous retirement must not consume
        // every validation turn before the first identity can be visited.
        const advanced = self.step(backend.allocator, credits, @min(deadline, time.monotonicNs() +| 2 * std.time.ns_per_ms));
        // Maintenance hands control back to its scheduler after this call.
        // A synchronous drain must explicitly yield through std.Io instead
        // of monopolizing a cooperative executor across successive slices.
        const yielded = if (self.yield_between_slices)
            if (backend.manifestCoordinationIo()) |io| io.sleep(.fromNanoseconds(1), .awake) else @as(anyerror!void, {})
        else
            @as(anyerror!void, {});
        _ = runtime.lockBackend(@TypeOf(backend.*), backend);
        self.slices += 1;
        try advanced;
        try yielded;
        if (!self.job.valid) return .invalid;
        if (self.changes) |*changes| if (changes.done()) {
            backend.retireCheckpointDirectory(self.directory);
            self.directory = self.latest.?;
            self.latest = null;
            self.changes = null;
        };
        if (self.phase == .certificate and self.changes == null and
            (try backend.planningDirectory()).tree.root == self.directory.tree.root) return .valid;
        return .pending;
    }

    fn step(self: *Validation, allocator: std.mem.Allocator, credits_arg: usize, deadline: u64) !void {
        var credits = credits_arg;
        switch (self.phase) {
            .identities => {
                if (try self.job.step(allocator, credits, deadline)) self.phase = .cleanup;
            },
            .cleanup => {
                // Preserve result ranks while reclaiming membership scratch.
                const indices = self.job.indices;
                self.job.indices = null;
                defer self.job.indices = indices;
                while (credits != 0 and time.monotonicNs() < deadline) {
                    var quantum: usize = @min(credits, 64);
                    const before = quantum;
                    const done = self.job.deinitStep(allocator, &quantum);
                    credits -= before - quantum;
                    if (done) {
                        self.phase = .certificate;
                        self.shrinkCompletedScratchCredit(if (indices) |ranks| ranks.len * @sizeOf(usize) else 0);
                        break;
                    }
                }
            },
            .certificate => if (self.changes) |*changes| {
                while (credits != 0 and time.monotonicNs() < deadline and !changes.done()) {
                    if (changes.next(&credits)) |change| {
                        if (!self.job.acceptChange(change)) return;
                    } else break;
                }
            },
        }
    }

    pub fn cleanupStep(self: *Validation, allocator: std.mem.Allocator, credits: *usize) bool {
        return self.job.deinitStep(allocator, credits);
    }

    fn shrinkCompletedScratchCredit(self: *Validation, rank_bytes: usize) void {
        // Membership scratch is gone in the certificate phase. Only epoch/
        // cursor headers and any still-owned result ranks need admission.
        const retained = @sizeOf(Validation) + 8192 + rank_bytes;
        if (self.reservation) |*lease| lease.shrink(lease.bytes -| retained);
    }

    pub fn takeIndices(self: *Validation) []usize {
        std.debug.assert(self.phase == .certificate and self.job.valid);
        const indices = self.job.indices.?;
        self.job.indices = null;
        self.shrinkCompletedScratchCredit(0);
        return indices;
    }

    /// Maintenance calls this only after sliced cleanup. Synchronous callers
    /// explicitly drain the same cleanup before releasing their stack owner.
    pub fn deinit(self: *Validation, backend: anytype) void {
        self.job.deinit(backend.allocator);
        if (self.latest) |directory| backend.retireCheckpointDirectory(directory);
        backend.retireCheckpointDirectory(self.directory);
        if (self.reservation) |*lease| lease.release();
    }
};
