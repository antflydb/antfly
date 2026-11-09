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

//! File-lifetime queue before registry/runtime/writer admission. Enrolled
//! origins retain their own request buffers and public registry pins. A leader
//! borrows only selected jobs until it records their durable outcomes.
const std = @import("std");
const receipts = @import("outcome_receipts.zig");
const cohort = @import("../storage/db/publication_cohort.zig");
const Budget = @import("../storage/immutable_artifact.zig").AdmissionBudget;
const capi = @import("types.zig");

pub const Job = struct {
    context: *anyopaque,
    scope: *anyopaque,
    table_name: ?[]const u8,
    table_id: u64,
    timestamp_ns: u64,
    sync_level: u8,
    bytes: usize,
    operations: usize,
    writer: *receipts.Writer,
    marker: receipts.Marker = undefined,
    resolution_marker: receipts.Marker = undefined,
    budget: Budget,
    bounded: bool,
    enrolled_ns: i96 = 0,
    deadline_ns: i96 = 0,
    next: ?*Job = null,
    phase: enum { queued, leader, selected, done } = .queued,
    wake: std.Io.Event = .unset,
    code: capi.ErrorCode = .ok,
    detail: [1024]u8 = undefined,
    detail_len: usize = 0,

    pub fn complete(self: *Job, code: capi.ErrorCode, message: []const u8) void {
        self.code = code;
        self.detail_len = @min(message.len, self.detail.len);
        @memcpy(self.detail[0..self.detail_len], message[0..self.detail_len]);
    }
    fn compatible(self: *const Job, other: *const Job) bool {
        if (self.scope != other.scope or self.table_id != other.table_id or self.timestamp_ns != other.timestamp_ns or self.sync_level != other.sync_level or self.bounded != other.bounded) return false;
        if ((self.table_name == null) != (other.table_name == null)) return false;
        return if (self.table_name) |name| std.mem.eql(u8, name, other.table_name.?) else true;
    }
};

pub const Queue = struct {
    const max_pending = 256;
    const max_pending_bytes = 64 * 1024 * 1024;
    const max_pending_operations = 8 * cohort.maximum_operations;
    mutex: std.Io.Mutex = .init,
    head: ?*Job = null,
    tail: ?*Job = null,
    pending: usize = 0,
    pending_bytes: usize = 0,
    pending_operations: usize = 0,
    running: bool = false,
    selected_groups: u64 = 0,
    selected_requests: u64 = 0,
    test_before_process: if (@import("builtin").is_test) ?struct { ptr: *anyopaque, notify: *const fn (*anyopaque, []const *Job) void } else void = if (@import("builtin").is_test) null else {},

    pub fn submit(self: *Queue, io: std.Io, job: *Job, process: *const fn ([]const *Job) void) void {
        self.mutex.lockUncancelable(io);
        if (self.pending == max_pending or job.bytes > max_pending_bytes -| self.pending_bytes or job.operations > max_pending_operations -| self.pending_operations) {
            self.mutex.unlock(io);
            job.complete(.busy, "native publication queue is full; no mutation applied");
            return;
        }
        // Marker order and enrollment order are one decision. A caller must
        // never reserve a lower marker and enqueue behind a higher marker.
        job.marker = job.writer.next(job.table_name);
        job.resolution_marker = job.marker;
        job.enrolled_ns = std.Io.Clock.awake.now(io).nanoseconds;
        job.deadline_ns = job.enrolled_ns +| @as(i96, job.budget.remaining_ns);
        if (self.tail) |tail| tail.next = job else self.head = job;
        self.tail = job;
        self.pending += 1;
        self.pending_bytes += job.bytes;
        self.pending_operations += job.operations;
        if (!self.running) {
            self.running = true;
            job.phase = .leader;
        }
        while (job.phase == .queued) {
            self.mutex.unlock(io);
            var canceled = false;
            if (job.bounded) job.wake.waitTimeout(io, .{ .duration = .{
                .raw = std.Io.Clock.awake.now(io).durationTo(.{ .nanoseconds = job.deadline_ns }),
                .clock = .awake,
            } }) catch |err| {
                canceled = @as(anyerror, err) == error.Canceled or @as(anyerror, err) == error.Cancelled;
            } else job.wake.waitUncancelable(io);
            self.mutex.lockUncancelable(io);
            if (canceled and (job.phase == .queued or job.phase == .leader)) {
                const was_leader = job.phase == .leader;
                self.removeQueued(job);
                if (was_leader) {
                    if (self.head) |next| {
                        next.phase = .leader;
                        next.wake.set(io);
                    } else self.running = false;
                }
                self.mutex.unlock(io);
                job.complete(.cancelled, "native publication queue wait canceled; no mutation applied");
                return;
            }
            if (job.phase == .queued and job.bounded and std.Io.Clock.awake.now(io).nanoseconds >= job.deadline_ns) {
                self.removeQueued(job);
                self.mutex.unlock(io);
                job.complete(.busy, "native publication queue deadline expired; no mutation applied");
                return;
            }
        }
        if (job.phase == .done) {
            self.mutex.unlock(io);
            return;
        }
        if (job.phase == .selected) {
            self.mutex.unlock(io);
            job.wake.waitUncancelable(io);
            self.mutex.lockUncancelable(io);
            std.debug.assert(job.phase == .done);
            self.mutex.unlock(io);
            return;
        }
        std.debug.assert(job.phase == .leader and self.head == job);
        var selected: [cohort.maximum_requests]*Job = undefined;
        var count: usize = 0;
        var bytes: usize = 0;
        var operations: usize = 0;
        while (self.head) |next| {
            if (count == selected.len or (count != 0 and (!job.compatible(next) or next.bytes > cohort.maximum_bytes -| bytes or next.operations > cohort.maximum_operations -| operations))) break;
            self.head = next.next;
            self.pending -= 1;
            self.pending_bytes -= next.bytes;
            self.pending_operations -= next.operations;
            next.next = null;
            next.phase = .selected;
            selected[count] = next;
            bytes +|= next.bytes;
            operations +|= next.operations;
            count += 1;
            // Followers stay asleep while selected: selected work owns their
            // buffers through the durable decision, even after their deadline.
            next.budget.chargeElapsed(io, next.enrolled_ns);
        }
        if (self.head == null) self.tail = null;
        self.selected_groups +|= 1;
        self.selected_requests +|= count;
        self.mutex.unlock(io);
        if (@import("builtin").is_test) if (self.test_before_process) |hook| hook.notify(hook.ptr, selected[0..count]);
        process(selected[0..count]);
        self.mutex.lockUncancelable(io);
        for (selected[0..count]) |finished| {
            finished.phase = .done;
            finished.wake.set(io);
        }
        if (self.head) |next| {
            next.phase = .leader;
            next.wake.set(io);
        } else self.running = false;
        self.mutex.unlock(io);
    }

    fn removeQueued(self: *Queue, job: *Job) void {
        var previous: ?*Job = null;
        var next = self.head;
        while (next) |current| {
            if (current == job) {
                if (previous) |before| before.next = current.next else self.head = current.next;
                if (self.tail == current) self.tail = previous;
                self.pending -= 1;
                self.pending_bytes -= job.bytes;
                self.pending_operations -= job.operations;
                job.next = null;
                return;
            }
            previous = current;
            next = current.next;
        }
        unreachable;
    }
};

test "capi native publication queue expires unselected origins without borrowing writer admission" {
    if (@import("builtin").single_threaded or @import("builtin").os.tag == .freestanding) return error.SkipZigTest;
    const io = std.testing.io;
    var queue: Queue = .{};
    const Probe = struct {
        entered: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        calls: std.atomic.Value(usize) = .init(0),
        fn process(jobs: []const *Job) void {
            const self: *@This() = @ptrCast(@alignCast(jobs[0].context));
            _ = self.calls.fetchAdd(1, .monotonic);
            self.entered.set(std.testing.io);
            self.release.waitUncancelable(std.testing.io);
            for (jobs) |job| job.complete(.ok, "");
        }
    };
    var probe: Probe = .{};
    var first_writer: receipts.Writer = .{};
    defer first_writer.deinit();
    var second_writer: receipts.Writer = .{};
    defer second_writer.deinit();
    var first: Job = .{ .context = &probe, .scope = &queue, .table_name = null, .table_id = 0, .timestamp_ns = 0, .sync_level = 0, .bytes = 1, .operations = 1, .writer = &first_writer, .budget = .{ .remaining_ns = std.time.ns_per_s }, .bounded = true };
    var second: Job = .{ .context = &probe, .scope = &queue, .table_name = null, .table_id = 0, .timestamp_ns = 0, .sync_level = 0, .bytes = 1, .operations = 1, .writer = &second_writer, .budget = .{ .remaining_ns = 10 * std.time.ns_per_ms }, .bounded = true };
    const Worker = struct {
        queue: *Queue,
        job: *Job,
        returned: std.Io.Event = .unset,
        fn run(self: *@This()) void {
            self.queue.submit(std.testing.io, self.job, Probe.process);
            self.returned.set(std.testing.io);
        }
    };
    var leading: Worker = .{ .queue = &queue, .job = &first };
    var waiting: Worker = .{ .queue = &queue, .job = &second };
    const leader = try std.Thread.spawn(.{}, Worker.run, .{&leading});
    defer leader.join();
    defer probe.release.set(io);
    try probe.entered.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    const follower = try std.Thread.spawn(.{}, Worker.run, .{&waiting});
    defer {
        probe.release.set(io);
        follower.join();
    }
    // The selected leader remains blocked. The follower must unlink its own
    // stack node before returning, rather than leaving a later dangling job.
    try waiting.returned.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try std.testing.expectEqual(capi.ErrorCode.busy, second.code);
    try std.testing.expectEqual(@as(usize, 1), probe.calls.load(.monotonic));
    queue.mutex.lockUncancelable(io);
    const pending = queue.pending;
    const pending_bytes = queue.pending_bytes;
    const pending_operations = queue.pending_operations;
    queue.mutex.unlock(io);
    try std.testing.expectEqual(@as(usize, 0), pending);
    try std.testing.expectEqual(@as(usize, 0), pending_bytes);
    try std.testing.expectEqual(@as(usize, 0), pending_operations);
    probe.release.set(io);
    try leading.returned.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try std.testing.expectEqual(capi.ErrorCode.ok, first.code);
}
