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

//! Cache invalidation transfers pointers only. Detached projection buffers are
//! poisoned in bounded ranges off-lock; their headers remain immutable for
//! accounting until the backend reacquires its lock and finishes the handoff.
const std = @import("std");
const runtime = @import("runtime.zig");
const DomainIndex = @import("compaction.zig").DomainIndex;
const admission = @import("../../common/read_admission.zig");
pub const quantum_bytes: usize = 16 * 1024;

pub const Job = struct {
    value: union(enum) { domain: *DomainIndex, projection: *runtime.ReadVersion },
    field: usize = 0,
    group: usize = 0,
    offset: usize = 0,
    complete: bool = false,
    const Buffer = struct { bytes: []u8, alignment: std.mem.Alignment };
    fn view(comptime T: type, values: []T) Buffer {
        return .{ .bytes = std.mem.sliceAsBytes(values), .alignment = .of(T) };
    }
    fn buffer(self: *Job) ?Buffer {
        return switch (self.value) {
            .domain => |index| switch (self.field) {
                0 => view(usize, index.order),
                1 => view(@import("repository.zig").Run, index.runs),
                2 => view(usize, index.ends),
                3 => view(@typeInfo(@TypeOf(index.levels)).pointer.child, index.levels),
                4 => view(usize, index.gc_order),
                5 => view(usize, index.gc_ends),
                else => null,
            },
            .projection => |version| switch (self.field) {
                0 => if (self.group < version.l0_groups.len) view(usize, version.l0_groups[self.group].run_indices) else null,
                1 => view(@import("repository.zig").Run, version.runs),
                2 => view(@typeInfo(@TypeOf(version.l0_groups)).pointer.child, version.l0_groups),
                3 => view(@typeInfo(@TypeOf(version.levels)).pointer.child, version.levels),
                else => null,
            },
        };
    }
    /// One unit poisons at most 16 KiB and frees at most one allocation. Empty
    /// descriptors also consume a unit. No buffer header changes during work.
    pub fn step(self: *Job, allocator: std.mem.Allocator, credits: *usize) bool {
        if (self.complete) return true;
        while (credits.* != 0) {
            credits.* -= 1;
            if (self.value == .projection and self.field == 0 and self.group == self.value.projection.l0_groups.len) self.field = 1;
            const current = self.buffer() orelse {
                self.complete = true;
                return true;
            };
            const bytes = current.bytes;
            const end = self.offset + @min(quantum_bytes, bytes.len - self.offset);
            @memset(bytes[self.offset..end], undefined);
            self.offset = end;
            if (self.offset != bytes.len) continue;
            if (bytes.len != 0) allocator.rawFree(bytes, current.alignment, @returnAddress());
            self.offset = 0;
            if (self.value == .projection and self.field == 0) self.group += 1 else self.field += 1;
        }
        return false;
    }
    pub fn memoryBytes(self: *const Job, pass: u64) u64 {
        return switch (self.value) {
            .domain => |index| index.memoryBytes(),
            .projection => |version| version.accountedMemoryBytes(pass),
        };
    }
    fn backingAllocator(self: *const Job, backend: anytype) std.mem.Allocator {
        return switch (self.value) {
            .domain => backend.allocator,
            .projection => |version| version.allocator,
        };
    }
    /// Fixed tail with the backend locked. Directory retirement preserves its
    /// separately owned pins; projected Run values only borrowed that data.
    fn finish(self: *Job, backend: anytype) void {
        std.debug.assert(self.complete);
        switch (self.value) {
            .domain => |index| backend.allocator.destroy(index),
            .projection => |version| {
                backend.retireCheckpointDirectory(version.directory.?);
                version.allocator.destroy(version);
            },
        }
    }
};

pub fn pending(backend: anytype) bool {
    return !backend.read_metadata_reclaim_in_flight and (backend.read_metadata_reclaimer != null or backend.retired_domain_indexes != null or backend.retired_projection_versions != null);
}
fn next(backend: anytype) bool {
    if (backend.read_metadata_reclaimer != null) return true;
    if (backend.retired_projection_versions != null and (backend.read_metadata_next_projection or backend.retired_domain_indexes == null)) {
        const version = backend.retired_projection_versions.?;
        backend.retired_projection_versions = version.retired_next;
        if (backend.retired_projection_versions == null) backend.retired_projection_versions_tail = null;
        version.retired_next = null;
        backend.read_metadata_reclaimer = .{ .value = .{ .projection = version } };
        backend.read_metadata_next_projection = false;
        return true;
    }
    if (backend.retired_domain_indexes) |index| {
        backend.retired_domain_indexes = index.retired_next;
        if (backend.retired_domain_indexes == null) backend.retired_domain_indexes_tail = null;
        index.retired_next = null;
        backend.read_metadata_reclaimer = .{ .value = .{ .domain = index } };
        backend.read_metadata_next_projection = true;
        return true;
    }
    return false;
}

pub fn reclaimSliceLocked(backend: anytype) void {
    if (!pending(backend)) return;
    backend.read_metadata_reclaim_in_flight = true;
    defer backend.read_metadata_reclaim_in_flight = false;
    backend.retainReaderKind(.other);
    defer backend.releaseReaderKind(.other);
    const until = runtime.workNowNs(backend) +| 2 * std.time.ns_per_ms;
    var credits: usize = 128;
    while (credits != 0 and runtime.workNowNs(backend) < until and next(backend)) {
        const job = &backend.read_metadata_reclaimer.?;
        const backing = job.backingAllocator(backend);
        backend.mu.unlock();
        var unit: usize = 1;
        const done = job.step(backing, &unit);
        _ = runtime.lockBackend(@TypeOf(backend.*), backend);
        credits -= 1;
        if (done) {
            job.finish(backend);
            backend.read_metadata_reclaimer = null;
        }
    }
    backend.syncTrackedInMemoryStateUsageCurrentLocked();
}

/// Producers, readers and observers are fenced by close. Refusal leaves the
/// same queue/cursor and its full accounting reachable; no refund occurs here.
pub fn reclaimWithAdmission(backend: anytype, original: admission.Port) !void {
    std.debug.assert(backend.active_readers == 0 and !backend.read_metadata_reclaim_in_flight);
    while (true) {
        try original.checkpoint();
        if (!next(backend)) return;
        const job = &backend.read_metadata_reclaimer.?;
        var unit: usize = 1;
        if (job.step(job.backingAllocator(backend), &unit)) {
            job.finish(backend);
            backend.read_metadata_reclaimer = null;
        }
    }
}
/// Explicit joined fallback after all external users and off-lock work joined.
pub fn drainJoined(backend: anytype) void {
    std.debug.assert(!backend.read_metadata_reclaim_in_flight);
    while (next(backend)) {
        const job = &backend.read_metadata_reclaimer.?;
        var credits: usize = std.math.maxInt(usize);
        std.debug.assert(job.step(job.backingAllocator(backend), &credits));
        job.finish(backend);
        backend.read_metadata_reclaimer = null;
    }
}

const Fixture = struct {
    const Backend = @import("../lsm_backend.zig").Backend;
    const resources = @import("../resource_manager.zig");
    const Run = @import("repository.zig").Run;
    const Watch = struct {
        const Allocation = struct { pointer: [*]u8, freed: bool = false };
        backend: ?*Backend = null,
        invalidating: bool = false,
        watching: bool = false,
        buffers: [512]Allocation = undefined,
        count: usize = 0,
        frees: usize = 0,
        observations: usize = 0,
        observe_during_free: bool = true,
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
        }
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.invalidating) @panic("read cache invalidation allocated metadata");
            return std.testing.allocator.rawAlloc(len, alignment, ra);
        }
        fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.invalidating) @panic("read cache invalidation freed metadata under publication lock");
            if (self.watching) for (self.buffers[0..self.count]) |*buffer| {
                if (buffer.pointer != bytes.ptr) continue;
                std.debug.assert(!buffer.freed);
                buffer.freed = true;
                self.frees += 1;
                const backend = self.backend.?;
                if (!backend.mu.tryLock()) @panic("read metadata buffer freed while backend lock held");
                // Exercise the real concurrent observer while the current
                // job has freed buffers but retained immutable header lengths.
                if (self.observe_during_free) {
                    backend.syncTrackedInMemoryStateUsageCurrentLocked();
                    self.observations += 1;
                }
                backend.mu.unlock();
                break;
            };
            std.testing.allocator.rawFree(bytes, alignment, ra);
        }
        fn add(self: *@This(), values: anytype) void {
            if (values.len == 0) return;
            self.buffers[self.count] = .{ .pointer = @ptrCast(values.ptr) };
            self.count += 1;
        }
    };
    const cpu = @import("../../common/runnable_scheduler.zig");
    const Fault = struct {
        execution: *cpu.Execution,
        slots: *cpu.Scheduler,
        progress_turns: usize = 0,
        calls: usize = 0,
        at: usize = std.math.maxInt(usize),
        failure: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.execution.checkLifetime();
            try std.testing.expectEqual(cpu.State.running, self.slots.state(&self.execution.entry.?));
            // The original execution retains ownership while bounded CPU
            // checkpoints allow the protected control class to progress.
            try self.execution.park(.resource_blocked);
            var health: cpu.Execution = .{};
            defer health.finish();
            try health.start(self.slots, .{ .io = std.testing.io, .native_now_ns = @import("antfly_platform").time.monotonicNs }, .control);
            self.progress_turns += 1;
            health.finish();
            try self.execution.enter(.write);
            const at = self.calls;
            self.calls += 1;
            if (at == self.at) return self.failure;
        }
        fn lifetime(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.execution.checkLifetime();
        }
        fn deadline(raw: *anyopaque) !?u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.execution.entry.?.options.deadline_ns;
        }
        fn begin(_: *anyopaque) !void {
            // This fixture fences all producers/observers and leaves the
            // mutex unlocked; a physical wait would violate its setup.
            return error.TestUnexpectedWait;
        }
        fn end(_: *anyopaque) !void {
            return error.TestUnexpectedWait;
        }
    };
    const Driver = enum { normal, admitted, close_preparation };
    fn exercise(target: ?usize, driver: Driver) !usize {
        const a = std.testing.allocator;
        var manager = resources.ResourceManager.init(.{ .identity_allocator = a });
        defer manager.deinit(a);
        var slots = try cpu.Scheduler.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1 });
        defer slots.close();
        var execution: cpu.Execution = .{};
        defer execution.finish();
        try execution.start(&slots, .{ .io = std.testing.io, .native_now_ns = @import("antfly_platform").time.monotonicNs, .deadline_ns = @import("antfly_platform").time.monotonicNs() + 60 * std.time.ns_per_s }, .write);
        var watch: Watch = .{};
        var backend = Backend.init(watch.allocator(), .{ .resource_manager = &manager, .wal_enabled = false, .read_runtime = @import("storage_io.zig").ReadRuntime.init(std.testing.io), .obsolete_retention_ns = 0 });
        defer backend.close();
        watch.backend = &backend;
        const allocator = watch.allocator();
        @import("antfly_platform").sync.lockYielding(&backend.mu);
        var locked = true;
        defer if (locked) backend.mu.unlock();
        for (0..96) |i| {
            const key = try allocator.alloc(u8, 8);
            std.mem.writeInt(u64, key[0..8], i, .big);
            const run: Run = .{ .id = i + 1, .level = 0, .size_bytes = 1, .path = try std.fmt.allocPrint(allocator, "read-metadata-{d}.sst", .{i}), .smallest_namespace_name = null, .smallest_key = key, .largest_namespace_name = null, .largest_key = try allocator.dupe(u8, key), .entry_count = 1, .bloom_filter = null, .state = null };
            try backend.runs.append(allocator, run);
        }
        const domain = try backend.domainIndex();
        const version = try backend.createReadVersionFromDirectory();
        try backend.prepareReadVersion(version);
        try std.testing.expect(version.runs.len * @sizeOf(Run) > quantum_bytes);
        inline for (.{ "order", "runs", "ends", "levels", "gc_order", "gc_ends" }) |field| watch.add(@field(domain, field));
        watch.add(version.runs);
        watch.add(version.l0_groups);
        watch.add(version.levels);
        for (version.l0_groups) |group| watch.add(group.run_indices);
        backend.syncTrackedInMemoryStateUsageCurrentLocked();
        const before = manager.sliceStats(.lsm_in_memory_state).used_bytes;
        watch.invalidating = true;
        backend.invalidateReadVersion();
        watch.invalidating = false;
        try std.testing.expect(backend.read_version == null and backend.domain_index == null and backend.live_read_versions == null);
        try std.testing.expect(backend.retired_domain_indexes != null and backend.retired_projection_versions != null);
        backend.syncTrackedInMemoryStateUsageCurrentLocked();
        try std.testing.expectEqual(before, manager.sliceStats(.lsm_in_memory_state).used_bytes);
        watch.watching = true;
        watch.observe_during_free = driver != .close_preparation;
        var fault: Fault = .{ .execution = &execution, .slots = &slots, .at = target orelse std.math.maxInt(usize), .failure = switch ((target orelse 0) % 3) {
            0 => error.Canceled,
            1 => error.DeadlineExceeded,
            else => error.FileNotFound,
        } };
        const payload: admission.Payload = .{ .ptr = &fault, .vtable = &.{ .checkpoint = Fault.check, .check_lifetime = Fault.lifetime, .deadline_ns = Fault.deadline }, .execution_wait = .{ .ptr = &fault, .begin = Fault.begin, .end = Fault.end } };
        const original = admission.Port.init(&payload);
        if (driver == .normal) {
            // Drive the actual unlock path, including lifecycle pins and its
            // ordinary accounting reconciliation; no controller-only driver.
            while (pending(&backend)) {
                backend.unlockWithReclamation();
                locked = false;
                @import("antfly_platform").sync.lockYielding(&backend.mu);
                locked = true;
            }
        } else {
            backend.mu.unlock();
            locked = false;
            var close: Backend.QuiescedClosePreparation = .{};
            const result = if (driver == .close_preparation) close.resumeWithAdmission(&backend, std.testing.io, original) else reclaimWithAdmission(&backend, original);
            if (target == null) try result else try std.testing.expectError(fault.failure, result);
            // Headers and their old charge stay owned across every refusal.
            if (target != null and driver != .close_preparation) {
                const held = manager.sliceStats(.lsm_in_memory_state).used_bytes;
                @import("antfly_platform").sync.lockYielding(&backend.mu);
                backend.syncTrackedInMemoryStateUsageCurrentLocked();
                backend.mu.unlock();
                try std.testing.expect(held >= manager.sliceStats(.lsm_in_memory_state).used_bytes);
            }
            if (driver == .close_preparation) try close.resumeWithAdmission(&backend, std.testing.io, original) else try reclaimWithAdmission(&backend, original);
            @import("antfly_platform").sync.lockYielding(&backend.mu);
            locked = true;
        }
        const checks = fault.calls;
        try std.testing.expectEqual(checks, fault.progress_turns);
        if (driver != .normal) try std.testing.expect(checks != 0);
        try std.testing.expectEqual(watch.count, watch.frees);
        try std.testing.expectEqual(if (watch.observe_during_free) watch.count else @as(usize, 0), watch.observations);
        watch.watching = false;
        try std.testing.expectEqual(@as(usize, 96), backend.runs.count());
        try std.testing.expect(backend.run_directory.?.byId(42) != null);
        backend.syncTrackedInMemoryStateUsageCurrentLocked();
        try std.testing.expect(manager.sliceStats(.lsm_in_memory_state).used_bytes < before);
        return checks;
    }
};

test "controlled physical sync read metadata invalidation queues normal caches without allocator work" {
    _ = try Fixture.exercise(null, .normal);
}

test "controlled physical sync read metadata retirement preserves every original refusal and concurrent accounting" {
    const checks = try Fixture.exercise(null, .admitted);
    for (0..checks - 1) |at| _ = try Fixture.exercise(at, .admitted);
}

test "controlled physical sync read metadata epoch removal preserves intrusive links in arbitrary release order" {
    const a = std.testing.allocator;
    var backend = Fixture.Backend.init(a, .{ .wal_enabled = false });
    defer backend.close();
    @import("antfly_platform").sync.lockYielding(&backend.mu);
    defer backend.mu.unlock();
    var versions: [5]*runtime.ReadVersion = undefined;
    for (&versions) |*slot| {
        slot.* = try backend.createReadVersionFromDirectory();
        _ = slot.*.references.fetchAdd(1, .monotonic);
        backend.invalidateReadVersion();
    }
    for ([_]usize{ 2, 4, 0, 3, 1 }, 0..) |i, retired| {
        versions[i].release(&backend);
        try std.testing.expect(!versions[i].registered and versions[i].live_next == null and versions[i].live_previous == null);
        var previous: ?*runtime.ReadVersion = null;
        var current = backend.live_read_versions;
        var count: usize = 0;
        while (current) |version| : (current = version.live_next) {
            try std.testing.expect(version.live_previous == previous);
            previous = version;
            count += 1;
        }
        try std.testing.expectEqual(versions.len - retired - 1, count);
    }
}

test "controlled physical sync read metadata poisoning consumes fixed byte quanta before physical free" {
    const Counter = struct {
        frees: usize = 0,
        fn alloc(_: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            return std.testing.allocator.rawAlloc(len, alignment, ra);
        }
        fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.frees += 1;
            std.testing.allocator.rawFree(bytes, alignment, ra);
        }
    };
    var counter: Counter = .{};
    const backing: std.mem.Allocator = .{ .ptr = &counter, .vtable = &.{ .alloc = Counter.alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = Counter.free } };
    const Run = @import("repository.zig").Run;
    const index = try std.testing.allocator.create(DomainIndex);
    defer std.testing.allocator.destroy(index);
    index.* = .{ .order = &.{}, .runs = try backing.alloc(Run, 2 * quantum_bytes / @sizeOf(Run) + 1), .ends = &.{}, .levels = &.{}, .mixed = false, .gc_order = &.{}, .gc_ends = &.{} };
    var job: Job = .{ .value = .{ .domain = index } };
    defer {
        var remaining: usize = std.math.maxInt(usize);
        std.debug.assert(job.step(backing, &remaining));
    }
    var credits: usize = 0;
    try std.testing.expect(!job.step(backing, &credits));
    credits = 1;
    try std.testing.expect(!job.step(backing, &credits)); // Empty order descriptor.
    for (1..3) |quanta| {
        credits = 1;
        try std.testing.expect(!job.step(backing, &credits));
        try std.testing.expectEqual(quanta * quantum_bytes, job.offset);
        try std.testing.expectEqual(@as(usize, 0), counter.frees);
    }
    credits = 1;
    try std.testing.expect(!job.step(backing, &credits));
    try std.testing.expectEqual(@as(usize, 1), counter.frees);
    try std.testing.expectEqual(@as(usize, 2), job.field);
    while (!job.complete) {
        credits = 1;
        _ = job.step(backing, &credits);
    }
    credits = 0;
    try std.testing.expect(job.step(backing, &credits));
}

test "controlled physical sync read metadata close preparation preserves every observation and retirement refusal" {
    const checks = try Fixture.exercise(null, .close_preparation);
    for (0..checks - 1) |at| _ = try Fixture.exercise(at, .close_preparation);
}

test "controlled physical sync read metadata retirement preserves FIFO age across new invalidations" {
    const Backend = Fixture.Backend;
    var backend = Backend.init(std.testing.allocator, .{ .wal_enabled = false, .read_runtime = @import("storage_io.zig").ReadRuntime.init(std.testing.io) });
    defer backend.close();
    @import("antfly_platform").sync.lockYielding(&backend.mu);
    defer backend.mu.unlock();
    const Complete = struct {
        fn run(b: *Backend) void {
            const job = &b.read_metadata_reclaimer.?;
            var credits: usize = std.math.maxInt(usize);
            std.debug.assert(job.step(job.backingAllocator(b), &credits));
            job.finish(b);
            b.read_metadata_reclaimer = null;
        }
    };
    var domains: [4]*const DomainIndex = undefined;
    var versions: [4]*runtime.ReadVersion = undefined;
    for (0..2) |i| {
        domains[i] = try backend.domainIndex();
        versions[i] = try backend.createReadVersionFromDirectory();
        backend.invalidateReadVersion();
    }
    try std.testing.expect(next(&backend));
    try std.testing.expect(backend.read_metadata_reclaimer.?.value.domain == domains[0]);
    // Arrival during an active job must queue behind both older roots.
    domains[2] = try backend.domainIndex();
    versions[2] = try backend.createReadVersionFromDirectory();
    backend.invalidateReadVersion();
    Complete.run(&backend);
    for (0..3) |i| {
        try std.testing.expect(next(&backend));
        try std.testing.expect(backend.read_metadata_reclaimer.?.value.projection == versions[i]);
        Complete.run(&backend);
        if (i + 1 < 3) {
            try std.testing.expect(next(&backend));
            try std.testing.expect(backend.read_metadata_reclaimer.?.value.domain == domains[i + 1]);
            Complete.run(&backend);
        }
    }
    try std.testing.expect(!next(&backend));
    try std.testing.expect(backend.retired_domain_indexes_tail == null and backend.retired_projection_versions_tail == null);
    // Reuse an empty queue: stale tails must never retain freed headers.
    domains[3] = try backend.domainIndex();
    versions[3] = try backend.createReadVersionFromDirectory();
    backend.invalidateReadVersion();
    try std.testing.expect(next(&backend));
    try std.testing.expect(backend.read_metadata_reclaimer.?.value.domain == domains[3]);
    Complete.run(&backend);
    try std.testing.expect(next(&backend));
    try std.testing.expect(backend.read_metadata_reclaimer.?.value.projection == versions[3]);
    Complete.run(&backend);
    try std.testing.expect(!next(&backend));
    try std.testing.expect(backend.retired_domain_indexes_tail == null and backend.retired_projection_versions_tail == null);
}
