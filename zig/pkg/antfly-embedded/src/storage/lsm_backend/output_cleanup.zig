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

//! Allocate cleanup ownership before creating an SST. Abandonment is an
//! allocation-free queue transfer, never filesystem I/O from a destructor.
//! The backend transfers queued paths to its durable obsolete ledger before
//! freeing tickets; failed admission leaves the original ownership intact.
const std = @import("std");
const resources = @import("../resource_manager.zig");
const sync = @import("antfly_platform").sync;
const admission = @import("../../common/read_admission.zig");

pub const Queue = struct {
    allocator: std.mem.Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    head: ?*Ticket = null,
    tail: ?*Ticket = null,
    pending: std.atomic.Value(usize) = .init(0),
    live: std.atomic.Value(usize) = .init(0),
    bytes: @import("antfly_platform").atomic.Value(u64) = .init(0),
    manager: ?*resources.ResourceManager = null,
    wake_context: ?*anyopaque = null,
    /// Nonblocking notification only, including from admitted queue handoff.
    wake_fn: ?*const fn (*anyopaque) void = null,

    /// Reserved ticket allocations have a single accounting owner. Without
    /// a manager, include them in observational estimates for diagnostics.
    pub fn observedMemoryBytes(self: *const Queue) u64 {
        return @sizeOf(Queue) +| if (self.manager == null) self.bytes.load(.acquire) else 0;
    }

    pub fn create(self: *Queue, path: []const u8) !*Ticket {
        var reservation: ?resources.Reservation = null;
        errdefer if (reservation) |*lease| lease.release();
        const bytes = @sizeOf(Ticket) + path.len;
        if (self.manager) |manager| reservation = try manager.reserve(.lsm_table_builder_working_set, bytes);
        const owned = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(owned);
        const ticket = try self.allocator.create(Ticket);
        ticket.* = .{ .queue = self, .path = owned, .reservation = reservation };
        _ = self.live.fetchAdd(1, .monotonic);
        _ = self.bytes.fetchAdd(bytes, .monotonic);
        return ticket;
    }

    pub fn append(self: *Queue, ticket: *Ticket) void {
        sync.lockYielding(&self.mutex);
        self.appendLocked(ticket);
        self.mutex.unlock();
        if (self.wake_fn) |wake| wake(self.wake_context.?);
    }

    pub fn appendWithAdmission(self: *Queue, ticket: *Ticket, original: admission.Port) !void {
        try @import("../../common/bounded_mutex.zig").lockAtomic(&self.mutex, original);
        self.appendLocked(ticket);
        self.mutex.unlock();
        if (self.wake_fn) |wake| wake(self.wake_context.?);
    }

    fn appendLocked(self: *Queue, ticket: *Ticket) void {
        ticket.next = null;
        if (self.tail) |tail| tail.next = ticket else self.head = ticket;
        self.tail = ticket;
        _ = self.pending.fetchAdd(1, .release);
    }

    pub fn pop(self: *Queue) ?*Ticket {
        sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        return self.popLocked();
    }

    /// Rejected admission leaves the obligation in the queue.
    pub fn popWithAdmission(self: *Queue, original: admission.Port) !?*Ticket {
        try @import("../../common/bounded_mutex.zig").lockAtomic(&self.mutex, original);
        defer self.mutex.unlock();
        return self.popLocked();
    }

    fn popLocked(self: *Queue) ?*Ticket {
        const ticket = self.head orelse return null;
        self.head = ticket.next;
        if (self.head == null) self.tail = null;
        _ = self.pending.fetchSub(1, .release);
        ticket.next = null;
        return ticket;
    }

    /// Used only after all builders/owners stop, including simulated crashes.
    /// Normal sync first transfers every pending ticket to the durable ledger.
    pub fn deinit(self: *Queue) void {
        while (self.pop()) |ticket| ticket.destroy();
        if (self.manager) |manager| while (self.live.load(.acquire) != 0) {
            // Joining a registered producer can append its abandoned ticket.
            // Consume those late handoffs too, before waiting for live == 0.
            while (self.pop()) |ticket| ticket.destroy();
            if (self.live.load(.acquire) == 0) break;
            if (manager.drainPayloadRetirementQuiesced() == 0) @import("antfly_platform").time.yieldBriefly();
        };
        std.debug.assert(self.live.load(.acquire) == 0);
    }

    /// All ticket owners must first commit/abandon, and the pending deletion
    /// obligations must be handed to the durable ledger before joining memory.
    pub fn joinRetirementsWithAdmission(self: *Queue, io: std.Io, original: admission.Port) !void {
        try original.checkpoint();
        if (self.pending.load(.acquire) != 0) return error.ResourceBudgetExceeded;
        const manager = self.manager orelse return error.ExecutionWaitUnsupported;
        while (self.live.load(.acquire) != 0) {
            _ = try manager.drainForegroundPayloadRetirementWithAdmission(original);
            if (self.live.load(.acquire) == 0) return;
            try @import("../../common/cleanup_wait.zig").waitForRetry(io, original);
        }
    }
};

pub const Ticket = struct {
    queue: *Queue,
    path: []u8,
    next: ?*Ticket = null,
    reservation: ?resources.Reservation = null,
    retirement: resources.PayloadRetirement = .{},

    pub fn abandon(self: *Ticket) void {
        self.queue.append(self);
    }

    pub fn abandonWithAdmission(self: *Ticket, original: admission.Port) !void {
        try self.prepareRetirementWithAdmission(original);
        try self.queue.appendWithAdmission(self, original);
    }

    fn prepareRetirementWithAdmission(self: *Ticket, original: admission.Port) !void {
        if (self.retirement.credit != null) return;
        _ = try Creation.memoryBound(self.path.len);
        const manager = self.queue.manager orelse return error.ExecutionWaitUnsupported;
        const credit = if (self.reservation) |*reservation| reservation else return error.ResourceAccountingMismatch;
        try manager.registerExistingPayloadRetirementWithAdmission(&self.retirement, credit, self, destroyRegistered, original);
    }

    /// Upgrade a legacy ticket without fresh payload credit, then hand it to
    /// the manager. Rejected registration leaves sole ownership with caller.
    pub fn retireWithAdmission(self: *Ticket, original: admission.Port) !void {
        try self.prepareRetirementWithAdmission(original);
        self.retirement.publish();
    }

    pub fn destroy(self: *Ticket) void {
        if (self.retirement.credit != null) {
            self.retirement.publish(); // Final ticket access; manager owns it.
            return;
        }
        const queue = self.queue;
        var reservation = self.reservation;
        const bytes = @sizeOf(Ticket) + self.path.len;
        queue.allocator.free(self.path);
        queue.allocator.destroy(self);
        _ = queue.bytes.fetchSub(bytes, .monotonic);
        _ = queue.live.fetchSub(1, .release);
        if (reservation) |*lease| lease.release();
    }

    fn destroyRegistered(raw: *anyopaque) void {
        comptime std.debug.assert(@sizeOf(Ticket) + 4096 <= 16 * 1024);
        const self: *Ticket = @ptrCast(@alignCast(raw));
        const queue = self.queue;
        const bytes = @sizeOf(Ticket) + self.path.len;
        queue.allocator.free(self.path);
        queue.allocator.destroy(self);
        _ = queue.bytes.fetchSub(bytes, .monotonic);
        // Queue lifetime joins this final release before freeing its allocator.
        _ = queue.live.fetchSub(1, .release);
    }
};

/// Retained construction, including allocation/registration refusal. Fixed
/// caller-owned header; queue, manager, allocator and input outlive it. Input
/// may be released after successful construction. No original port is retained.
pub const Creation = struct {
    const Phase = enum { reserve, path, copy, ticket, register, ready, taken, retired };
    queue: *Queue,
    input: []const u8,
    path: ?[]u8 = null,
    ticket: ?*Ticket = null,
    credit: ?resources.ConstructionCredit = null,
    phase: Phase = .reserve,
    registered: bool = false,
    retiring: bool = false,

    pub fn init(queue: *Queue, path: []const u8) Creation {
        return .{ .queue = queue, .input = path };
    }
    pub fn memoryBound(path_len: usize) !usize {
        if (path_len == 0) return error.InvalidArgument;
        if (path_len >= std.fs.max_path_bytes or path_len > 4096) return error.NameTooLong;
        return @sizeOf(Ticket) + path_len;
    }
    pub fn resumeWithAdmission(self: *Creation, original: admission.Port) !void {
        if (self.retiring or self.phase == .taken or self.phase == .retired) return error.InvalidArgument;
        const manager = self.queue.manager orelse return error.ExecutionWaitUnsupported;
        const bytes = try memoryBound(self.input.len);
        while (self.phase != .ready) {
            try original.checkpoint();
            switch (self.phase) {
                .reserve => {
                    self.credit = try manager.reserveConstructionWithAdmission(.lsm_table_builder_working_set, bytes, original);
                    self.phase = .path;
                },
                .path => {
                    self.path = try self.queue.allocator.alloc(u8, self.input.len);
                    self.phase = .copy;
                },
                .copy => {
                    @memcpy(self.path.?, self.input);
                    self.phase = .ticket;
                },
                .ticket => {
                    const ticket = try self.queue.allocator.create(Ticket);
                    ticket.* = .{ .queue = self.queue, .path = self.path.? };
                    self.path = null;
                    self.ticket = ticket;
                    self.phase = .register;
                },
                .register => {
                    const ticket = self.ticket.?;
                    try manager.registerPayloadRetirementWithAdmission(&ticket.retirement, &ticket.reservation, &self.credit.?, ticket, Ticket.destroyRegistered, original);
                    self.credit = null;
                    self.registered = true;
                    _ = self.queue.live.fetchAdd(1, .monotonic);
                    _ = self.queue.bytes.fetchAdd(bytes, .monotonic);
                    self.phase = .ready;
                },
                .ready, .taken, .retired => unreachable,
            }
        }
    }
    pub fn take(self: *Creation) !*Ticket {
        if (self.phase != .ready or self.retiring) return error.InvalidArgument;
        const ticket = self.ticket.?;
        self.ticket = null;
        self.phase = .taken;
        return ticket;
    }
    pub fn retireWithAdmission(self: *Creation, original: ?admission.Port) !void {
        if (self.phase == .retired) return;
        self.retiring = true;
        if (self.ticket) |ticket| {
            if (self.registered) ticket.destroy() else {
                if (ticket.path.len != 0) {
                    if (original) |control| try control.checkpoint();
                    self.queue.allocator.free(ticket.path);
                    ticket.path = &.{};
                }
                if (original) |control| try control.checkpoint();
                self.queue.allocator.destroy(ticket);
            }
            self.ticket = null;
        }
        if (self.path) |path| {
            if (original) |control| try control.checkpoint();
            self.queue.allocator.free(path);
            self.path = null;
        }
        if (self.credit) |*credit| credit.retire();
        self.credit = null;
        self.phase = .retired;
    }
    pub fn deinit(self: *Creation) void {
        self.retireWithAdmission(null) catch unreachable;
    }
};

test "output cleanup tickets own paths through allocation failures and shared run retirement" {
    const Fixture = struct {
        fn check(allocator: std.mem.Allocator, commit: bool) !void {
            var manager = resources.ResourceManager.init(.{});
            defer std.debug.assert(manager.sliceStats(.lsm_table_builder_working_set).used_bytes == 0);
            var queue = Queue{ .allocator = allocator, .manager = &manager };
            defer queue.deinit();
            const ticket = try queue.create("unpublished.sst");
            var run: @import("repository.zig").Run = .{ .id = 1, .level = 0, .size_bytes = 1, .path = @constCast("unpublished.sst"), .smallest_namespace_name = null, .smallest_key = &.{}, .largest_namespace_name = null, .largest_key = &.{}, .entry_count = 1, .bloom_filter = null, .state = null, .owns_metadata = false, .output_ticket = ticket };
            var adopted = false;
            defer if (!adopted) run.deinit(allocator);
            var store: @import("run_store.zig").Store = .{};
            defer store.deinit(allocator);
            try store.append(allocator, run);
            adopted = true;
            var caller = store.at(0).retainOwned();
            defer caller.deinit(allocator);
            if (commit) caller.commitOutput() else try std.testing.expect(caller.abandonOutput());
            try std.testing.expectEqual(@as(usize, if (commit) 0 else 1), queue.pending.load(.acquire));
            // The candidate may remain pinned, but an abandoned file already
            // has an independent cleanup owner; it does not await retirement.
            if (!commit) {
                const pending = queue.pop().?;
                try std.testing.expectEqualStrings("unpublished.sst", pending.path);
                pending.destroy();
            }
        }
    };
    for ([_]bool{ false, true }) |commit| try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.check, .{commit});
}

test "output cleanup charges aggregate memory exactly once across ownership transitions" {
    const Backend = @import("../lsm_backend.zig").Backend;
    var manager = resources.ResourceManager.init(.{});
    var backend = Backend.init(std.testing.allocator, .{ .resource_manager = &manager });
    defer backend.close();
    try backend.initOutputCleanup();
    backend.syncTrackedInMemoryStateUsageCurrentLocked();
    const baseline = manager.snapshot().memory.used_bytes;
    const queue = backend.options.unpublished_outputs.?;
    for ([_]bool{ false, true }) |abandon| {
        const ticket = try queue.create("accounted-output.tbl");
        const bytes = queue.bytes.load(.acquire);
        backend.syncTrackedInMemoryStateUsageCurrentLocked();
        try std.testing.expectEqual(baseline + bytes, manager.snapshot().memory.used_bytes);
        if (abandon) {
            ticket.abandon();
            backend.syncTrackedInMemoryStateUsageCurrentLocked();
            try std.testing.expectEqual(baseline + bytes, manager.snapshot().memory.used_bytes);
            try std.testing.expectEqual(ticket, queue.pop().?);
        }
        ticket.destroy();
        backend.syncTrackedInMemoryStateUsageCurrentLocked();
        try std.testing.expectEqual(baseline, manager.snapshot().memory.used_bytes);
    }
}

test "output cleanup persists failed deletes and retries off-lock after reopen" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const Memory = @import("storage_io.zig").MemoryStorage;
    const compaction = @import("compaction.zig");
    const allocator = std.testing.allocator;
    const Hook = struct {
        var backend: ?*Backend = null;
        var target: []const u8 = "";
        var attempts: usize = 0;
        var failures: bool = true;
        fn delete(ptr: *anyopaque, path: []const u8) !void {
            if (std.mem.eql(u8, path, target)) {
                if (backend) |current| {
                    try std.testing.expect(current.mu.tryLock());
                    current.mu.unlock();
                }
                attempts += 1;
                if (failures) return error.InjectedDeleteFailure;
            }
            const memory: *Memory = @ptrCast(@alignCast(ptr));
            return memory.storage().vtable.delete_file_absolute(ptr, path);
        }
    };
    var memory = Memory.init(allocator);
    defer memory.deinit();
    var storage = memory.storage();
    var vtable = storage.vtable.*;
    vtable.delete_file_absolute = Hook.delete;
    storage.vtable = &vtable;
    const options: @import("../lsm_backend.zig").Options = .{ .storage = storage, .wal_enabled = false, .obsolete_retention_ns = 0, .obsolete_delete_retry_ns = 0 };
    var backend = try Backend.open(allocator, "/output-cleanup-retry", options);
    var opened = true;
    defer if (opened) backend.close();
    Hook.backend = &backend;
    Hook.attempts = 0;
    Hook.failures = true;
    defer {
        Hook.backend = null;
        Hook.target = "";
    }
    var state: @import("state.zig").State = .{};
    defer state.deinit(allocator);
    try state.upsert(allocator, .{ .name = "docs" }, "a", "one", false);
    var path: []u8 = undefined;
    {
        try std.testing.expect(backend.mu.tryLock());
        defer backend.mu.unlock();
        var outputs = try compaction.makeRunsFromStateBorrowed(Backend, &backend, &state);
        errdefer compaction.discardOutputRuns(Backend, &backend, &outputs);
        try std.testing.expect(outputs.items[0].output_ticket != null);
        path = try allocator.dupe(u8, outputs.items[0].path.?);
        Hook.target = path;
        compaction.discardOutputRuns(Backend, &backend, &outputs);
        try std.testing.expectEqual(@as(usize, 0), Hook.attempts);
        try std.testing.expectEqual(@as(usize, 1), backend.options.unpublished_outputs.?.pending.load(.acquire));
    }
    defer allocator.free(path);
    backend.manifest_admitted_wire_bytes = @import("repository.zig").maxManifestReadBytes();
    try std.testing.expectError(error.ResourceBudgetExceeded, backend.sync(true));
    backend.manifest_admitted_wire_bytes = 0;
    try std.testing.expectEqual(@as(usize, 1), backend.options.unpublished_outputs.?.pending.load(.acquire));
    try backend.sync(true);
    try std.testing.expect(Hook.attempts != 0);
    try std.testing.expect(backend.obsolete_paths.contains(path));
    try std.testing.expectEqual(@as(usize, 0), backend.options.unpublished_outputs.?.pending.load(.acquire));
    _ = try storage.fileSize(path);
    Hook.backend = null;
    backend.abandonAfterCrash();
    opened = false;
    backend = try Backend.open(allocator, "/output-cleanup-retry", options);
    opened = true;
    Hook.backend = &backend;
    try std.testing.expect(backend.obsolete_paths.contains(path));
    Hook.failures = false;
    backend.obsolete_reclaim_retry_at_ns = 0;
    for (0..8) |_| {
        _ = try backend.runMaintenanceStep();
        if (!backend.obsolete_paths.contains(path)) break;
    }
    try std.testing.expect(!backend.obsolete_paths.contains(path));
    try std.testing.expectError(error.FileNotFound, storage.fileSize(path));
}

test "output cleanup owns partial outputs after cancellation and wakes bulk maintenance" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const Memory = @import("storage_io.zig").MemoryStorage;
    const compaction = @import("compaction.zig");
    const allocator = std.testing.allocator;
    const Hook = struct {
        var tables: usize = 0;
        fn rename(ptr: *anyopaque, old: []const u8, new: []const u8) !void {
            const memory: *Memory = @ptrCast(@alignCast(ptr));
            try memory.storage().vtable.rename_absolute(ptr, old, new);
            if (std.mem.endsWith(u8, new, ".tbl")) {
                tables += 1;
                if (tables == 2) return error.Cancelled;
            }
        }
    };
    Hook.tables = 0;
    var memory = Memory.init(allocator);
    defer memory.deinit();
    var storage = memory.storage();
    var vtable = storage.vtable.*;
    vtable.rename_absolute = Hook.rename;
    storage.vtable = &vtable;
    var backend = try Backend.open(allocator, "/output-cleanup-cancel", .{ .storage = storage, .wal_enabled = false, .run_partition_prefix_bytes = 1 });
    defer backend.close();
    try backend.beginBulkIngestSession();
    var state: @import("state.zig").State = .{};
    defer state.deinit(allocator);
    for ([_][]const u8{ "a", "b", "c" }) |key| try state.upsert(allocator, .{ .name = "docs" }, key, "value", false);
    try std.testing.expectError(error.Cancelled, compaction.makeRunsFromStateBorrowed(Backend, &backend, &state));
    try std.testing.expectEqual(@as(usize, 2), Hook.tables);
    const queue = backend.options.unpublished_outputs.?;
    try std.testing.expectEqual(@as(usize, 2), queue.pending.load(.acquire));
    // Maintenance hands off at most 64 tickets within a 2 ms slice. A busy
    // runner may exhaust that slice before both cancelled outputs transfer.
    // Drive the continuation and check ownership after every bounded turn.
    for (0..128) |_| {
        if (queue.pending.load(.acquire) == 0) break;
        try std.testing.expectEqual(@as(?u64, 0), backend.nextMaintenanceWakeDelayNsBestEffort());
        _ = try backend.runMaintenanceStep();
        try std.testing.expectEqual(@as(usize, 2), queue.pending.load(.acquire) + backend.obsolete_paths.count());
    }
    try std.testing.expectEqual(@as(usize, 0), queue.pending.load(.acquire));
    try std.testing.expectEqual(@as(usize, 2), backend.obsolete_paths.count());
    // Bulk mode may defer physical deletion, but cannot lose cleanup intent.
    var cursor = backend.obsolete_paths.nextDueAfter(storage.nowNs(), null).?;
    _ = try storage.fileSize(cursor.path);
    cursor = backend.obsolete_paths.nextDueAfter(storage.nowNs(), cursor.path).?;
    _ = try storage.fileSize(cursor.path);
}

test "output cleanup off-lock handoff scaling benchmark" {
    if (@import("builtin").mode != .fast) return error.SkipZigTest;
    const Backend = @import("../lsm_backend.zig").Backend;
    const Run = @import("repository.zig").Run;
    const compaction = @import("compaction.zig");
    const allocator = std.testing.allocator;
    for ([_]usize{ 1000, 10000, 50000 }) |count| {
        var backend = Backend.init(allocator, .{ .wal_enabled = false });
        defer backend.close();
        try backend.initOutputCleanup();
        var outputs: std.ArrayListUnmanaged(Run) = .empty;
        defer compaction.discardOutputRuns(Backend, &backend, &outputs);
        try outputs.ensureTotalCapacity(allocator, count);
        for (0..count) |i| {
            const ticket = try backend.options.unpublished_outputs.?.create("unpublished.sst");
            outputs.appendAssumeCapacity(.{ .id = i + 1, .level = 0, .size_bytes = 1, .path = @constCast("unpublished.sst"), .smallest_namespace_name = null, .smallest_key = &.{}, .largest_namespace_name = null, .largest_key = &.{}, .entry_count = 1, .bloom_filter = null, .state = null, .owns_metadata = false, .output_ticket = ticket });
        }
        try std.testing.expect(backend.mu.tryLock());
        const start = std.Io.Clock.awake.now(std.testing.io);
        compaction.discardOutputRunsLocked(Backend, &backend, &outputs);
        const elapsed = start.durationTo(std.Io.Clock.awake.now(std.testing.io)).toNanoseconds();
        backend.mu.unlock();
        const queue = backend.options.unpublished_outputs.?;
        try std.testing.expectEqual(count, queue.pending.load(.acquire));
        std.debug.print("\nLSM cleanup outputs={d} offlock_handoff_ns={d} ticket_bytes={d}\n", .{ count, elapsed, queue.bytes.load(.acquire) });
        // This rootless fixture measures only the ownership handoff, not
        // durable ledger admission or physical deletion.
        while (queue.pop()) |ticket| ticket.destroy();
    }
}

const Fault = struct {
    checks: usize = 0,
    at: usize = std.math.maxInt(usize),
    failure: anyerror = error.Canceled,
    rejected: bool = false,
    reject_wait: bool = false,
    fn check(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const at = self.checks;
        self.checks += 1;
        if (!self.rejected and at == self.at) {
            self.rejected = true;
            return self.failure;
        }
    }
    fn wait(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.reject_wait) return self.failure;
        return error.UnexpectedWait;
    }
    fn payload(self: *@This()) admission.Payload {
        return .{ .ptr = self, .vtable = &.{ .checkpoint = check, .wait_for_progress = wait } };
    }
};
const FundedAllocator = struct {
    manager: *resources.ResourceManager,
    live: usize = 0,
    calls: usize = 0,
    fail_at: usize = std.math.maxInt(usize),
    fn allocator(self: *@This()) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
    }
    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        std.debug.assert(self.manager.sliceStats(.lsm_table_builder_working_set).used_bytes >= self.live + len);
        const index = self.calls;
        self.calls += 1;
        if (index == self.fail_at) return null;
        const ptr = std.testing.allocator.rawAlloc(len, alignment, ra) orelse return null;
        self.live += len;
        return ptr;
    }
    fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        std.debug.assert(self.manager.sliceStats(.lsm_table_builder_working_set).used_bytes >= self.live);
        std.testing.allocator.rawFree(bytes, alignment, ra);
        self.live -= bytes.len;
    }
};
fn finishCreation(creation: *Creation, original: admission.Port, fault: *Fault) !void {
    while (true) {
        creation.resumeWithAdmission(original) catch |cause| {
            try std.testing.expectEqual(fault.failure, cause);
            continue;
        };
        return;
    }
}
fn checkCreationRejection(at: usize, failure: anyerror) !usize {
    const a = std.testing.allocator;
    var manager = resources.ResourceManager.init(.{ .identity_allocator = a });
    defer manager.deinit(a);
    var witness: FundedAllocator = .{ .manager = &manager };
    var queue: Queue = .{ .allocator = witness.allocator(), .manager = &manager };
    defer queue.deinit();
    var fault: Fault = .{ .at = at, .failure = failure };
    const payload = fault.payload();
    const original = admission.Port.init(&payload);
    var creation = Creation.init(&queue, "registered-output");
    defer creation.deinit();
    try finishCreation(&creation, original, &fault);
    const checks = fault.checks;
    if (at != std.math.maxInt(usize)) try std.testing.expect(fault.rejected);
    try std.testing.expectEqual(@as(usize, 2), witness.calls);
    try std.testing.expectEqual(@as(usize, 1), queue.live.load(.acquire));
    const bytes = try Creation.memoryBound("registered-output".len);
    try std.testing.expectEqual(bytes, witness.live);
    try std.testing.expectEqual(@as(u64, bytes), manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
    var owned: ?*Ticket = try creation.take();
    defer if (owned) |ticket| ticket.destroy();
    try std.testing.expectEqualStrings("registered-output", owned.?.path);
    try std.testing.expectError(error.InvalidArgument, creation.take());
    owned.?.destroy();
    owned = null;
    // Retirement publication cannot release either the physical bytes or credit.
    try std.testing.expectEqual(@as(usize, 1), queue.live.load(.acquire));
    try std.testing.expectEqual(bytes, witness.live);
    try queue.joinRetirementsWithAdmission(std.testing.io, original);
    try std.testing.expectEqual(@as(usize, 0), witness.live);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
    try resources.qualifyNoLiveReservations(&manager);
    return checks;
}

test "controlled physical sync cleanup ticket prepays every allocation and preserves all original construction rejections" {
    const count = try checkCreationRejection(std.math.maxInt(usize), error.Canceled);
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.FileNotFound }) |failure| {
        for (0..count) |at| {
            _ = try checkCreationRejection(at, failure);
        }
    }
}

test "controlled physical sync cleanup ticket allocation refusal and abandoned partial construction retain exact funding" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |recover| for (0..2) |at| {
        var manager = resources.ResourceManager.init(.{ .identity_allocator = a });
        defer manager.deinit(a);
        var witness: FundedAllocator = .{ .manager = &manager, .fail_at = at };
        var queue: Queue = .{ .allocator = witness.allocator(), .manager = &manager };
        defer queue.deinit();
        var fault: Fault = .{};
        const payload = fault.payload();
        const original = admission.Port.init(&payload);
        var creation = Creation.init(&queue, "allocation-refusal");
        defer creation.deinit();
        try std.testing.expectError(error.OutOfMemory, creation.resumeWithAdmission(original));
        try std.testing.expect(creation.credit != null and creation.phase != .ready);
        try std.testing.expectEqual(@as(usize, 0), queue.live.load(.acquire));
        witness.fail_at = std.math.maxInt(usize);
        if (recover) {
            try creation.resumeWithAdmission(original);
            const ticket = try creation.take();
            ticket.destroy();
            try queue.joinRetirementsWithAdmission(std.testing.io, original);
        } else {
            if (witness.live != 0) {
                fault.at = fault.checks;
                try std.testing.expectError(error.Canceled, creation.retireWithAdmission(original));
                try std.testing.expect(creation.retiring and creation.credit != null);
                try std.testing.expectError(error.InvalidArgument, creation.resumeWithAdmission(original));
            }
            try creation.retireWithAdmission(original);
        }
        try std.testing.expectEqual(@as(usize, 0), witness.live);
        try resources.qualifyNoLiveReservations(&manager);
    };
}

test "controlled physical sync cleanup ticket admission denial allocates nothing and rejected queue waiting transfers nothing" {
    const a = std.testing.allocator;
    var budgets = resources.Options.defaultBudgets();
    budgets[@backingInt(resources.Slice.lsm_table_builder_working_set)] = .{ .hard_limit_bytes = 1 };
    var manager = resources.ResourceManager.init(.{ .identity_allocator = a, .budgets = budgets });
    defer manager.deinit(a);
    var witness: FundedAllocator = .{ .manager = &manager };
    var queue: Queue = .{ .allocator = witness.allocator(), .manager = &manager };
    defer queue.deinit();
    var fault: Fault = .{};
    const payload = fault.payload();
    const original = admission.Port.init(&payload);
    var refused = Creation.init(&queue, "unfunded");
    defer refused.deinit();
    try std.testing.expectError(error.ResourceBudgetExceeded, refused.resumeWithAdmission(original));
    try std.testing.expectEqual(@as(usize, 0), witness.calls);
    // Raise only the test slice ceiling, then construct a funded ticket.
    manager.slices[@backingInt(resources.Slice.lsm_table_builder_working_set)].budget.hard_limit_bytes = 4096;
    var creation = Creation.init(&queue, "queued-output");
    defer creation.deinit();
    try creation.resumeWithAdmission(original);
    const ticket = try creation.take();
    try std.testing.expect(queue.mutex.tryLock());
    fault.reject_wait = true;
    const rejected = ticket.abandonWithAdmission(original);
    queue.mutex.unlock();
    fault.reject_wait = false;
    try std.testing.expectError(error.Canceled, rejected);
    try std.testing.expectEqual(@as(usize, 0), queue.pending.load(.acquire));
    try std.testing.expect(queue.head == null and ticket.next == null);
    try ticket.abandonWithAdmission(original);
    try std.testing.expectEqual(@as(usize, 1), queue.pending.load(.acquire));
    try std.testing.expectError(error.ResourceBudgetExceeded, queue.joinRetirementsWithAdmission(std.testing.io, original));
    try std.testing.expect(queue.pop().? == ticket);
    ticket.destroy();
    try queue.joinRetirementsWithAdmission(std.testing.io, original);
    try std.testing.expectEqual(@as(usize, 0), witness.live);
    try resources.qualifyNoLiveReservations(&manager);
}

test "controlled physical sync cleanup ticket destruction retains parent accounting debt after releasing all physical memory" {
    const a = std.testing.allocator;
    var parent = try resources.metadata_fixture.init(.{ .identity_allocator = a });
    defer parent.deinit(a);
    var manager = try resources.metadata_fixture.init(.{ .identity_allocator = a });
    defer manager.deinit(a);
    try resources.metadata_fixture.installParent(&manager, &parent);
    var witness: FundedAllocator = .{ .manager = &manager };
    var queue: Queue = .{ .allocator = witness.allocator(), .manager = &manager };
    defer queue.deinit();
    var fault: Fault = .{};
    const payload = fault.payload();
    const original = admission.Port.init(&payload);
    var creation = Creation.init(&queue, "parent-debt");
    defer creation.deinit();
    try creation.resumeWithAdmission(original);
    const ticket = try creation.take();
    const charged = manager.sliceStats(.lsm_table_builder_working_set).used_bytes;
    ticket.destroy();
    try std.testing.expect(parent.mutex.tryLock());
    const result = manager.drainPayloadRetirementWithAdmission(original);
    parent.mutex.unlock();
    _ = try result;
    try std.testing.expectEqual(@as(usize, 0), witness.live);
    try std.testing.expectEqual(@as(usize, 0), queue.live.load(.acquire));
    try std.testing.expectEqual(charged, manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
    try std.testing.expect(manager.cleanupPending());
    try manager.drainConstructionCleanupWithAdmission(original);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
    try resources.qualifyNoLiveReservations(&manager);
}

test "controlled physical sync legacy cleanup tickets upgrade registration without new payload credit or raw scoped release" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |commit| {
        var manager = resources.ResourceManager.init(.{ .identity_allocator = a });
        defer manager.deinit(a);
        var witness: FundedAllocator = .{ .manager = &manager };
        var queue: Queue = .{ .allocator = witness.allocator(), .manager = &manager };
        defer queue.deinit();
        var owned: ?*Ticket = try queue.create("legacy-output");
        defer if (owned) |ticket| ticket.destroy();
        const ticket = owned.?;
        const before = manager.snapshot().memory.used_bytes;
        var fault: Fault = .{ .at = 0 };
        const payload = fault.payload();
        const original = admission.Port.init(&payload);
        const refused = if (commit) ticket.retireWithAdmission(original) else ticket.abandonWithAdmission(original);
        try std.testing.expectError(error.Canceled, refused);
        try std.testing.expect(ticket.retirement.credit == null and queue.pending.load(.acquire) == 0);
        try std.testing.expectEqual(before, manager.snapshot().memory.used_bytes);
        if (commit) {
            try ticket.retireWithAdmission(original);
            owned = null;
        } else {
            try ticket.abandonWithAdmission(original);
            owned = null;
            try std.testing.expectEqual(before, manager.snapshot().memory.used_bytes);
            owned = queue.pop().?;
            try std.testing.expect(owned.? == ticket);
            owned.?.destroy();
            owned = null;
        }
        try std.testing.expectEqual(@as(usize, 2), witness.calls);
        try queue.joinRetirementsWithAdmission(std.testing.io, original);
        try std.testing.expectEqual(@as(usize, 0), witness.live);
        try resources.qualifyNoLiveReservations(&manager);
    }
}
