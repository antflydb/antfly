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

const std = @import("std");
const raft_engine = @import("raft_engine");
const db_types = @import("antfly_local_sources").storage_db_types;
const read_state_observer_mod = @import("antfly_read_state_observer");

/// Tracks quorum ReadIndex requests until the matching ReadState has crossed
/// this replica's state-machine apply boundary. Registration is request
/// bounded, cancellation removes ownership immediately, and context identity
/// is canonical rather than delegated to caller string conventions.
pub const AppliedReadTracker = struct {
    pub const context_prefix = "antfly-read-safe-v2:";

    pub const Token = struct {
        group_id: u64,
        request_id: u64,
    };

    pub const Registration = struct {
        token: Token,
        request_ctx: []const u8,
    };

    const Waiter = struct {
        target_index: ?u64 = null,
        complete: bool = false,
    };

    allocator: std.mem.Allocator,
    incarnation: u128,
    mutex: std.atomic.Mutex = .unlocked,
    next_request_id: u64 = 1,
    applied_indexes: std.AutoHashMapUnmanaged(u64, u64) = .empty,
    waiters: std.AutoHashMapUnmanaged(Token, Waiter) = .empty,

    /// The owner supplies a fresh identity on every restart. VOPR obtains it
    /// from its controlled I/O stream, never from hidden host randomness.
    pub fn init(allocator: std.mem.Allocator, incarnation: u128) AppliedReadTracker {
        return .{ .allocator = allocator, .incarnation = incarnation };
    }

    pub fn deinit(self: *AppliedReadTracker) void {
        self.applied_indexes.deinit(self.allocator);
        self.waiters.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn register(
        self: *AppliedReadTracker,
        group_id: u64,
        context_buffer: []u8,
    ) !Registration {
        lock(&self.mutex);
        if (self.next_request_id == 0) {
            self.mutex.unlock();
            return error.AppliedReadIdentityExhausted;
        }
        const request_id = self.next_request_id;
        self.next_request_id +%= 1;
        const token = Token{ .group_id = group_id, .request_id = request_id };
        const result = self.waiters.getOrPut(self.allocator, token) catch |err| {
            self.mutex.unlock();
            return err;
        };
        if (result.found_existing) {
            self.mutex.unlock();
            return error.DuplicateAppliedReadToken;
        }
        result.value_ptr.* = .{};
        self.mutex.unlock();
        errdefer self.cancel(token);

        const request_ctx = std.fmt.bufPrint(
            context_buffer,
            context_prefix ++ "{x:0>32}:{x}",
            .{ self.incarnation, request_id },
        ) catch return error.ReadIndexContextTooLong;
        return .{ .token = token, .request_ctx = request_ctx };
    }

    pub fn cancel(self: *AppliedReadTracker, token: Token) void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        _ = self.waiters.remove(token);
    }

    pub fn takeCompleted(self: *AppliedReadTracker, token: Token) bool {
        lock(&self.mutex);
        defer self.mutex.unlock();
        const waiter = self.waiters.get(token) orelse return false;
        if (!waiter.complete) return false;
        _ = self.waiters.remove(token);
        return true;
    }

    /// Retransmit only until one quorum proof arrives. Local apply can lag
    /// independently; that must not keep issuing fresh ReadIndex requests.
    pub fn needsReadIndex(self: *AppliedReadTracker, token: Token) bool {
        lock(&self.mutex);
        defer self.mutex.unlock();
        const waiter = self.waiters.get(token) orelse return false;
        return waiter.target_index == null;
    }

    pub fn noteApplied(self: *AppliedReadTracker, group_id: u64, applied_index: u64) !void {
        if (applied_index == 0) return;
        lock(&self.mutex);
        defer self.mutex.unlock();
        const entry = try self.applied_indexes.getOrPut(self.allocator, group_id);
        if (entry.found_existing and applied_index <= entry.value_ptr.*) return;
        entry.value_ptr.* = applied_index;
        const visible_index = entry.value_ptr.*;
        var waiters = self.waiters.iterator();
        while (waiters.next()) |waiter| {
            if (waiter.key_ptr.group_id != group_id) continue;
            const target_index = waiter.value_ptr.target_index orelse continue;
            if (visible_index >= target_index) waiter.value_ptr.complete = true;
        }
    }

    pub fn appliedIndex(self: *AppliedReadTracker, group_id: u64) u64 {
        lock(&self.mutex);
        defer self.mutex.unlock();
        return self.applied_indexes.get(group_id) orelse 0;
    }

    /// Consume the quorum proof independently of apply completion. Frozen
    /// statement validation must reject a proof beyond its captured applied
    /// index, not wait for the apply lock that the statement itself owns.
    pub fn takeObservedIndex(self: *AppliedReadTracker, token: Token) ?u64 {
        lock(&self.mutex);
        defer self.mutex.unlock();
        const waiter = self.waiters.get(token) orelse return null;
        const index = waiter.target_index orelse return null;
        _ = self.waiters.remove(token);
        return index;
    }

    pub fn observeReadStates(
        self: *AppliedReadTracker,
        group_id: u64,
        read_states: []const raft_engine.core.ReadState,
    ) void {
        if (read_states.len == 0) return;
        lock(&self.mutex);
        defer self.mutex.unlock();
        const applied_index = self.applied_indexes.get(group_id) orelse 0;
        for (read_states) |read_state| {
            if (!std.mem.startsWith(u8, read_state.request_ctx, context_prefix)) continue;
            const suffix = read_state.request_ctx[context_prefix.len..];
            if (suffix.len < 34 or suffix[32] != ':') continue;
            const incarnation = std.fmt.parseUnsigned(u128, suffix[0..32], 16) catch continue;
            if (incarnation != self.incarnation) continue;
            const encoded_id = suffix[33..];
            if (encoded_id.len == 0) continue;
            const request_id = std.fmt.parseUnsigned(u64, encoded_id, 16) catch continue;
            const waiter = self.waiters.getPtr(.{
                .group_id = group_id,
                .request_id = request_id,
            }) orelse continue;
            // A retransmission may return a later proof for this same logical
            // read. Keep the first valid proof and never move its apply target
            // or turn a completed waiter back into pending work.
            if (waiter.target_index != null) continue;
            waiter.target_index = read_state.index;
            waiter.complete = applied_index >= read_state.index;
        }
    }

    pub fn retireGroup(self: *AppliedReadTracker, group_id: u64) void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        _ = self.applied_indexes.remove(group_id);
        var waiters = self.waiters.iterator();
        while (waiters.next()) |waiter| {
            if (waiter.key_ptr.group_id == group_id) self.waiters.removeByPtr(waiter.key_ptr);
        }
    }

    pub fn pendingCount(self: *AppliedReadTracker) usize {
        lock(&self.mutex);
        defer self.mutex.unlock();
        return self.waiters.count();
    }

    pub fn observer(self: *AppliedReadTracker) read_state_observer_mod.ReadStateObserver {
        return .{
            .ptr = self,
            .vtable = &.{ .on_read_states = onReadStates },
        };
    }

    fn onReadStates(
        ptr: *anyopaque,
        group_id: raft_engine.core.types.GroupId,
        read_states: []const raft_engine.core.ReadState,
    ) !void {
        const self: *AppliedReadTracker = @ptrCast(@alignCast(ptr));
        self.observeReadStates(group_id, read_states);
    }

    fn lock(mutex: *std.atomic.Mutex) void {
        while (!mutex.tryLock()) std.atomic.spinLoopHint();
    }
};

const read_contract = @import("antfly_local_sources").storage_read_consistency;
pub const EnrichmentReadKind = read_contract.EnrichmentReadKind;
pub const ReadConsistency = read_contract.ReadConsistency;
pub const ReadIndexRequester = read_contract.ReadIndexRequester;
pub const ReadSafetyBarrier = read_contract.ReadSafetyBarrier;
pub const ReadSafetyCallback = read_contract.ReadSafetyCallback;
pub const CallbackReadSafetyBarrier = read_contract.CallbackReadSafetyBarrier;
pub const alreadyReadSafeBarrier = read_contract.alreadyReadSafeBarrier;
pub const unavailableReadSafetyBarrier = read_contract.unavailableReadSafetyBarrier;
pub const EnrichmentReadIndexRequester = read_contract.EnrichmentReadIndexRequester;
pub const EnrichmentReadGate = read_contract.EnrichmentReadGate;

test "read-index execution waits keep stale and already-safe reads free of false yields" {
    const Wait = @import("antfly_local_sources").common_execution_wait.Boundary;
    const Recorder = struct {
        calls: usize = 0,
        begins: usize = 0,
        ends: usize = 0,
        fn legacy(raw: *anyopaque, _: u64, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.begins += 1;
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.ends += 1;
        }
    };
    var recorder: Recorder = .{};
    const wait: Wait = .{ .ptr = &recorder, .begin = Recorder.begin, .end = Recorder.end };
    const legacy: ReadSafetyBarrier = .{ .ptr = &recorder, .vtable = &.{ .wait_read_safe = Recorder.legacy } };
    const gate = EnrichmentReadGate.init(legacy);
    try gate.prepareScan(7, "", "", .{ .execution_wait = wait }, .stale);
    try std.testing.expectEqual(@as(usize, 0), recorder.calls);
    try std.testing.expectError(error.ExecutionWaitUnsupported, gate.prepareScan(7, "", "", .{ .execution_wait = wait }, .read_index));
    try std.testing.expectEqual(@as(usize, 0), recorder.calls);
    try gate.prepareScan(7, "", "", .{}, .read_index);
    try std.testing.expectEqual(@as(usize, 1), recorder.calls);
    try EnrichmentReadGate.init(alreadyReadSafeBarrier()).prepareScan(7, "", "", .{ .execution_wait = wait }, .read_index);
    try std.testing.expectError(error.ReadSafetyBarrierUnavailable, EnrichmentReadGate.init(unavailableReadSafetyBarrier()).prepareScan(7, "", "", .{ .execution_wait = wait }, .read_index));
    try std.testing.expectEqual(@as(usize, 0), recorder.begins);
    try std.testing.expectEqual(@as(usize, 0), recorder.ends);
}

test "read-index execution waits preserve request identity and retire ownership after failed resume" {
    const runnable = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    const Wait = @import("antfly_local_sources").common_execution_wait.Boundary;
    const Case = enum { success, canceled, deadline, shutdown, io_error, forbidden };
    const Owner = struct {
        execution: runnable.Execution = .{},
        canceled: std.atomic.Value(bool) = .init(false),
        revoked: bool = false,
        fn requested(raw: *const anyopaque) bool {
            const flag: *const std.atomic.Value(bool) = @ptrCast(@alignCast(raw));
            return flag.load(.acquire);
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.execution.park(.resource_blocked);
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.execution.enter(.query);
            if (self.revoked) return error.Forbidden;
        }
    };
    const Provider = struct {
        io: std.Io,
        owner: *Owner,
        entered: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        fail: bool,
        registered: bool = false,
        continued: bool = false,
        fn legacy(_: *anyopaque, _: u64, _: []const u8) !void {
            return error.LegacyBarrierMustNotRun;
        }
        fn scheduled(raw: *anyopaque, group: u64, ctx: []const u8, wait: Wait) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(u64, 77), group);
            try std.testing.expectEqualStrings("enrichment:scan:read_index", ctx);
            self.registered = true;
            defer self.registered = false;
            // Registration/probing retains execution; only the actual event
            // wait runs without a permit, under the same request identity.
            const entry = &self.owner.execution.entry.?;
            try std.testing.expectEqual(@import("antfly_local_sources").common_request_admission.runnable_scheduler.State.running, entry.scheduler.?.state(entry));
            try wait.run(awaitReady, .{self});
            try std.testing.expectEqualStrings("enrichment:scan:read_index", ctx);
            self.continued = true;
        }
        fn awaitReady(self: *@This()) !void {
            try std.testing.expect(self.registered);
            self.entered.set(self.io);
            try self.release.wait(self.io);
            if (self.fail) return error.Timeout;
        }
    };
    const Worker = struct {
        provider: *Provider,
        wait: Wait,
        failure: ?anyerror = null,
        fn run(self: *@This()) void {
            const barrier: ReadSafetyBarrier = .{ .ptr = self.provider, .vtable = &.{ .wait_read_safe = Provider.legacy, .wait_read_safe_with_execution = Provider.scheduled } };
            EnrichmentReadGate.init(barrier).prepareScan(77, "", "", .{ .execution_wait = self.wait }, .read_index) catch |err| {
                self.failure = err;
            };
        }
    };
    const io = std.testing.io;
    for (std.enums.values(Case)) |case| {
        var scheduler = try runnable.Scheduler.init(.{ .slots = 2, .max_operations = 8, .max_bytes = 100, .control_bytes = 10, .max_wait_ms = 2000 });
        defer scheduler.close();
        var owner: Owner = .{};
        defer owner.execution.finish();
        const deadline: u64 = @intCast(std.Io.Clock.awake.now(io).addDuration(.fromMilliseconds(if (case == .deadline) 500 else 5000)).nanoseconds);
        try owner.execution.start(&scheduler, .{ .io = io, .retained_bytes = 17, .deadline_ns = deadline, .cancellation = .{ .ptr = &owner.canceled, .requested = Owner.requested } }, .query);
        var provider: Provider = .{ .io = io, .owner = &owner, .fail = case == .io_error };
        var worker: Worker = .{ .provider = &provider, .wait = .{ .ptr = &owner, .begin = Owner.begin, .end = Owner.end } };
        var group: std.Io.Group = .init;
        defer group.cancel(io);
        defer provider.release.set(io);
        try group.concurrent(io, Worker.run, .{&worker});
        try provider.entered.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(3), .clock = .awake } });
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().resource_blocked);
        try std.testing.expectEqual(@as(u64, 17), scheduler.stats().retained_bytes);
        var other: runnable.Execution = .{};
        defer other.finish();
        try other.start(&scheduler, .{ .io = io }, .query);
        var control: runnable.Execution = .{};
        defer control.finish();
        try control.start(&scheduler, .{ .io = io }, .control);
        try std.testing.expectEqual(@as(usize, 2), scheduler.stats().runnable_slots);
        control.finish();
        switch (case) {
            .canceled => owner.canceled.store(true, .release),
            .deadline => try io.sleep(.fromMilliseconds(600), .awake),
            .shutdown => scheduler.close(),
            .forbidden => owner.revoked = true,
            else => {},
        }
        provider.release.set(io);
        if (case == .success or case == .io_error or case == .forbidden) {
            const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(3));
            while (scheduler.stats().queued != 1) {
                if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.ResumeDidNotQueue;
                try io.sleep(.fromMilliseconds(1), .awake);
            }
            other.finish();
        }
        try group.await(io);
        try std.testing.expect(!provider.registered);
        try std.testing.expectEqual(case == .success, provider.continued);
        try std.testing.expectEqual(deadline, owner.execution.entry.?.options.deadline_ns.?);
        try std.testing.expectEqual(@as(?anyerror, switch (case) {
            .success => null,
            .canceled => error.Canceled,
            .deadline => error.DeadlineExceeded,
            .shutdown => error.SchedulerClosed,
            .io_error => error.Timeout,
            .forbidden => error.Forbidden,
        }), worker.failure);
        other.finish();
        owner.execution.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
        std.debug.print("read-index wait case {s}: request identity preserved, operations=0 slots=0\n", .{@tagName(case)});
    }
}
test "frozen read quorum proof is observable without falsely completing apply" {
    var tracker = AppliedReadTracker.init(std.testing.allocator, 7);
    defer tracker.deinit();
    var first_buffer: [160]u8 = undefined;
    var second_buffer: [160]u8 = undefined;
    const frozen = try tracker.register(2, &first_buffer);
    const ordinary = try tracker.register(2, &second_buffer);
    try tracker.noteApplied(2, 10);
    tracker.observeReadStates(2, &.{
        .{ .index = 11, .request_ctx = @constCast(frozen.request_ctx) },
        .{ .index = 11, .request_ctx = @constCast(ordinary.request_ctx) },
    });
    try std.testing.expect(!tracker.takeCompleted(ordinary.token));
    try std.testing.expectEqual(@as(?u64, 11), tracker.takeObservedIndex(frozen.token));
    try std.testing.expectEqual(@as(?u64, null), tracker.takeObservedIndex(frozen.token));
    try tracker.noteApplied(2, 11);
    try std.testing.expect(tracker.takeCompleted(ordinary.token));
    const proof = ReadSafetyBarrier.FrozenProof{ .incarnation = 7, .term = 3, .applied_index = 10 };
    try proof.validate(7, 3, true, 10);
    try std.testing.expectError(error.ReadUnavailable, proof.validate(7, 3, true, 11));
    try std.testing.expectError(error.NotLeader, proof.validate(7, 4, true, 10));
    try std.testing.expectError(error.NotLeader, proof.validate(8, 3, true, 10));
    try std.testing.expectError(error.NotLeader, proof.validate(7, 3, false, 10));
}

test "enrichment read gate supports explicit consistency modes" {
    const Recorder = struct {
        group_id: u64 = 0,
        request_ctx: [64]u8 = undefined,
        request_ctx_len: usize = 0,
        request_count: usize = 0,

        fn requester(self: *@This()) ReadSafetyBarrier {
            return .{
                .ptr = self,
                .vtable = &.{
                    .wait_read_safe = waitReadSafe,
                },
            };
        }

        fn waitReadSafe(ptr: *anyopaque, group_id: u64, request_ctx: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.group_id = group_id;
            @memcpy(self.request_ctx[0..request_ctx.len], request_ctx);
            self.request_ctx_len = request_ctx.len;
            self.request_count += 1;
        }
    };

    var recorder = Recorder{};
    const gate = EnrichmentReadGate.init(recorder.requester());
    try gate.prepare(77, .search, .read_index);

    try std.testing.expectEqual(@as(u64, 77), recorder.group_id);
    try std.testing.expectEqualStrings("enrichment:search:read_index", recorder.request_ctx[0..recorder.request_ctx_len]);

    try gate.prepare(77, .lookup, .leader_lease);
    try std.testing.expectEqualStrings("enrichment:lookup:leader_lease", recorder.request_ctx[0..recorder.request_ctx_len]);

    const requests_before_stale = recorder.request_count;
    try gate.prepare(77, .scan, .stale);
    try std.testing.expectEqual(requests_before_stale, recorder.request_count);

    try gate.prepareSearch(77, .{}, .read_index);
    try std.testing.expectEqualStrings("enrichment:search:read_index", recorder.request_ctx[0..recorder.request_ctx_len]);

    try gate.prepareLookup(77, "doc:a", .{}, .leader_lease);
    try std.testing.expectEqualStrings("enrichment:lookup:leader_lease", recorder.request_ctx[0..recorder.request_ctx_len]);

    try gate.prepareScan(77, "doc:a", "doc:z", .{}, .read_index);
    try std.testing.expectEqualStrings("enrichment:scan:read_index", recorder.request_ctx[0..recorder.request_ctx_len]);
}

test "callback read safety barrier forwards calls" {
    const Recorder = struct {
        group_id: u64 = 0,
        request_ctx: [64]u8 = undefined,
        request_ctx_len: usize = 0,

        fn callback(ctx: ?*anyopaque, group_id: u64, request_ctx: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.group_id = group_id;
            @memcpy(self.request_ctx[0..request_ctx.len], request_ctx);
            self.request_ctx_len = request_ctx.len;
        }
    };

    var recorder = Recorder{};
    const callback_barrier = CallbackReadSafetyBarrier.init(&recorder, Recorder.callback);
    try callback_barrier.barrier().waitReadSafe(91, "enrichment:lookup");

    try std.testing.expectEqual(@as(u64, 91), recorder.group_id);
    try std.testing.expectEqualStrings("enrichment:lookup", recorder.request_ctx[0..recorder.request_ctx_len]);
}

test "unavailable read safety barrier fails closed" {
    try std.testing.expectError(
        error.ReadSafetyBarrierUnavailable,
        unavailableReadSafetyBarrier().waitReadSafe(91, "read-before-runtime-wiring"),
    );
}

test "enrichment ReadIndex requester is initiation only" {
    const Recorder = struct {
        calls: usize = 0,
        context: [64]u8 = undefined,
        context_len: usize = 0,

        fn requester(self: *@This()) ReadIndexRequester {
            return .{ .ptr = self, .vtable = &.{ .request_read_index = requestReadIndex } };
        }

        fn requestReadIndex(ptr: *anyopaque, _: u64, context: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            @memcpy(self.context[0..context.len], context);
            self.context_len = context.len;
        }
    };

    var recorder = Recorder{};
    const requests = EnrichmentReadIndexRequester.init(recorder.requester());
    try requests.requestSearch(7, .read_index);
    try std.testing.expectEqual(@as(usize, 1), recorder.calls);
    try std.testing.expectEqualStrings("enrichment:search:read_index", recorder.context[0..recorder.context_len]);
    try requests.requestLookup(7, .stale);
    try std.testing.expectEqual(@as(usize, 1), recorder.calls);
}

test "applied read tracker completes only after matching ReadState and applied index" {
    var tracker = AppliedReadTracker.init(std.testing.allocator, 1);
    defer tracker.deinit();

    var first_context: [96]u8 = undefined;
    const first = try tracker.register(7001, &first_context);
    try std.testing.expect(std.mem.startsWith(u8, first.request_ctx, AppliedReadTracker.context_prefix));
    try std.testing.expectEqual(@as(usize, 1), tracker.pendingCount());

    try tracker.noteApplied(7001, 7);
    tracker.observeReadStates(7002, &.{.{
        .index = 7,
        .request_ctx = @constCast(first.request_ctx),
    }});
    try std.testing.expect(!tracker.takeCompleted(first.token));
    tracker.observeReadStates(7001, &.{.{
        .index = 7,
        .request_ctx = @constCast("unrelated-read-context"),
    }});
    try std.testing.expect(!tracker.takeCompleted(first.token));
    var removed_context_buffer: [96]u8 = undefined;
    const removed_context = try std.fmt.bufPrint(
        &removed_context_buffer,
        "lookup:read_index:vopr-read-v1:{x}",
        .{first.token.request_id},
    );
    tracker.observeReadStates(7001, &.{.{
        .index = 7,
        .request_ctx = @constCast(removed_context),
    }});
    try std.testing.expect(!tracker.takeCompleted(first.token));
    try tracker.observer().onReadStates(7001, &.{.{
        .index = 7,
        .request_ctx = @constCast(first.request_ctx),
    }});
    try std.testing.expect(tracker.takeCompleted(first.token));
    try std.testing.expect(!tracker.takeCompleted(first.token));

    var second_context: [96]u8 = undefined;
    const second = try tracker.register(7001, &second_context);
    tracker.observeReadStates(7001, &.{.{
        .index = 9,
        .request_ctx = @constCast(second.request_ctx),
    }});
    try std.testing.expect(!tracker.takeCompleted(second.token));
    try tracker.noteApplied(7001, 8);
    try std.testing.expect(!tracker.takeCompleted(second.token));
    try tracker.noteApplied(7001, 9);
    try std.testing.expect(tracker.takeCompleted(second.token));
}

test "applied read tracker retransmissions retain the first quorum proof" {
    var tracker = AppliedReadTracker.init(std.testing.allocator, 7);
    defer tracker.deinit();
    var context: [96]u8 = undefined;
    const read = try tracker.register(7001, &context);
    try std.testing.expect(tracker.needsReadIndex(read.token));
    tracker.observeReadStates(7001, &.{.{ .index = 9, .request_ctx = @constCast(read.request_ctx) }});
    try std.testing.expect(!tracker.needsReadIndex(read.token));
    try std.testing.expect(!tracker.takeCompleted(read.token));
    tracker.observeReadStates(7001, &.{.{ .index = 20, .request_ctx = @constCast(read.request_ctx) }});
    try tracker.noteApplied(7001, 9);
    tracker.observeReadStates(7001, &.{.{ .index = 30, .request_ctx = @constCast(read.request_ctx) }});
    try std.testing.expect(tracker.takeCompleted(read.token));
    try std.testing.expect(!tracker.needsReadIndex(read.token));
    const retired = try tracker.register(7001, &context);
    tracker.retireGroup(7001);
    try std.testing.expect(!tracker.needsReadIndex(retired.token));
}

test "applied read tracker rejects a delayed response from before restart" {
    var old = AppliedReadTracker.init(std.testing.allocator, 17);
    var old_buffer: [96]u8 = undefined;
    const old_request = try old.register(7, &old_buffer);
    old.deinit();

    var restarted = AppliedReadTracker.init(std.testing.allocator, 18);
    defer restarted.deinit();
    var new_buffer: [96]u8 = undefined;
    const new_request = try restarted.register(7, &new_buffer);
    try std.testing.expectEqual(old_request.token, new_request.token);
    try restarted.noteApplied(7, 10);
    restarted.observeReadStates(7, &.{.{ .index = 10, .request_ctx = @constCast(old_request.request_ctx) }});
    try std.testing.expect(!restarted.takeCompleted(new_request.token));
    restarted.observeReadStates(7, &.{.{ .index = 10, .request_ctx = @constCast(new_request.request_ctx) }});
    try std.testing.expect(restarted.takeCompleted(new_request.token));
}

test "applied read tracker cancellation retirement and context errors release ownership" {
    var tracker = AppliedReadTracker.init(std.testing.allocator, 1);
    defer tracker.deinit();

    var canceled_context: [96]u8 = undefined;
    const canceled = try tracker.register(91, &canceled_context);
    tracker.cancel(canceled.token);
    try std.testing.expectEqual(@as(usize, 0), tracker.pendingCount());

    var retired_context: [96]u8 = undefined;
    _ = try tracker.register(92, &retired_context);
    tracker.retireGroup(92);
    try std.testing.expectEqual(@as(usize, 0), tracker.pendingCount());

    var too_short: [1]u8 = undefined;
    try std.testing.expectError(
        error.ReadIndexContextTooLong,
        tracker.register(93, &too_short),
    );
    try std.testing.expectEqual(@as(usize, 0), tracker.pendingCount());
}

test "applied read tracker never reuses identities after counter exhaustion" {
    var tracker = AppliedReadTracker.init(std.testing.allocator, 1);
    defer tracker.deinit();
    tracker.next_request_id = std.math.maxInt(u64);
    var buffer: [96]u8 = undefined;
    const last = try tracker.register(1, &buffer);
    tracker.cancel(last.token);
    try std.testing.expectError(error.AppliedReadIdentityExhausted, tracker.register(1, &buffer));
    try std.testing.expectError(error.AppliedReadIdentityExhausted, tracker.register(1, &buffer));
    try std.testing.expectEqual(@as(usize, 0), tracker.pendingCount());
}

test "frozen quorum admission lends CPU preserves captured proof and retires registrations" {
    const a = std.testing.allocator;
    const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    const admission = @import("antfly_local_sources").common_read_admission;
    const time = @import("antfly_platform").time;
    const Mode = enum { success, newer_index, changed_term, changed_incarnation, lost_leadership, regressed_apply, older_quorum, unavailable_quorum, canceled, expired, shutdown };
    const Fixture = struct {
        execution: *native.Execution,
        scheduler: *native.Scheduler,
        tracker: *AppliedReadTracker,
        mode: Mode,
        retained: bool,
        deadline: u64,
        term: u64 = 3,
        index: u64 = 10,
        failure: ?anyerror = null,
        live: bool = false,
        waits: usize = 0,
        callbacks: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            try self.checkLifetime();
            try self.execution.checkpoint();
        }
        pub fn checkLifetime(self: *@This()) !void {
            if (self.failure) |cause| return cause;
            try self.execution.checkLifetime();
        }
        pub fn deadlineNs(self: *@This()) !?u64 {
            return self.deadline;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestWrongProgressClock;
        }
        pub fn executionBoundary(self: *@This()) ?@import("antfly_local_sources").common_execution_wait.Boundary {
            return .{ .ptr = self, .begin = begin, .end = end };
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(self.live);
            try std.testing.expectEqual(@as(usize, 1), self.tracker.waiters.count());
            try self.execution.park(.resource_blocked);
            try self.checkLifetime();
            self.waits += 1;
            try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, 1), self.scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 17), self.scheduler.stats().retained_bytes);
            var health: native.Execution = .{};
            try health.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
            health.finish();
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            switch (self.mode) {
                .success, .changed_incarnation, .lost_leadership, .regressed_apply, .older_quorum, .unavailable_quorum => {},
                .newer_index => self.index = 11,
                .changed_term => self.term = 4,
                .canceled => self.failure = error.Canceled,
                .expired => self.failure = error.DeadlineExceeded,
                .shutdown => self.scheduler.close(),
            }
            try self.checkLifetime();
            try self.execution.enter(.query);
        }
        fn waitSafe(_: *anyopaque, _: u64, _: []const u8) !void {}
        fn legacy(raw: *anyopaque, _: u64, _: ReadSafetyBarrier.FrozenProof, _: ?u64, _: @import("antfly_cancellation").CancellationToken) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(usize, 1), self.scheduler.stats().runnable_slots);
            return error.TestLegacyValidation;
        }
        fn validateRetained(raw: *anyopaque, group: u64, proof: ReadSafetyBarrier.FrozenProof, deadline: ?u64, cancellation: @import("antfly_cancellation").CancellationToken, control: ?admission.Port) !void {
            return validate(raw, group, proof, deadline, cancellation, control orelse return error.TestMissingAdmission);
        }
        fn validate(raw: *anyopaque, group: u64, proof: ReadSafetyBarrier.FrozenProof, deadline: ?u64, cancellation: @import("antfly_cancellation").CancellationToken, control: admission.Port) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.callbacks += 1;
            try std.testing.expectEqual(@as(u64, 7), group);
            try std.testing.expectEqual(@as(?u64, self.deadline), deadline);
            try cancellation.check();
            var buffer: [160]u8 = undefined;
            const registration = try self.tracker.register(group, &buffer);
            defer self.tracker.cancel(registration.token);
            self.live = true;
            defer self.live = false;
            try ReadSafetyBarrier.waitFrozenProgress(std.testing.io, control);
            if (self.mode == .unavailable_quorum) return error.ReadUnavailable;
            if (self.mode == .older_quorum) self.index = 9;
            self.tracker.observeReadStates(group, &.{.{ .index = self.index, .request_ctx = @constCast(registration.request_ctx) }});
            const index = self.tracker.takeObservedIndex(registration.token) orelse return error.TestMissingQuorumProof;
            const incarnation: u128 = if (self.mode == .changed_incarnation) 8 else 7;
            const leader = self.mode != .lost_leadership;
            if (self.retained)
                try proof.validateRetained(incarnation, self.term, leader, if (self.mode == .regressed_apply) 9 else 20, index)
            else
                try proof.validate(incarnation, self.term, leader, index);
        }
    };
    for ([_]bool{ false, true }) |retained| {
        for ([_]Mode{ .success, .newer_index, .changed_term, .changed_incarnation, .lost_leadership, .regressed_apply, .older_quorum, .unavailable_quorum, .canceled, .expired, .shutdown }) |mode| {
            var scheduler = try native.Scheduler.init(.{ .slots = 2 });
            var execution: native.Execution = .{};
            try execution.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 17 }, .query);
            defer execution.finish();
            var tracker = AppliedReadTracker.init(a, 7);
            defer tracker.deinit();
            try tracker.noteApplied(7, 10);
            const deadline = time.monotonicNs() + 5 * std.time.ns_per_s;
            var fixture: Fixture = .{ .execution = &execution, .scheduler = &scheduler, .tracker = &tracker, .mode = mode, .retained = retained, .deadline = deadline };
            var bridge: admission.Adapter(*Fixture) = .{ .budget = &fixture };
            const payload = bridge.payload();
            const port = admission.Port.init(&payload);
            const barrier: ReadSafetyBarrier = .{ .ptr = &fixture, .vtable = &.{ .wait_read_safe = Fixture.waitSafe, .validate_frozen = Fixture.legacy, .validate_frozen_with_admission = Fixture.validate, .validate_retained_snapshot = Fixture.validateRetained } };
            const proof: ReadSafetyBarrier.FrozenProof = .{ .incarnation = 7, .term = 3, .applied_index = 10 };
            const result = if (retained) barrier.validateRetainedSnapshotWithAdmission(7, proof, deadline + std.time.ns_per_s, .none, port) else barrier.validateFrozenWithAdmission(7, proof, deadline + std.time.ns_per_s, .none, port);
            const success = mode == .success or (retained and mode == .newer_index) or (!retained and (mode == .regressed_apply or mode == .older_quorum));
            if (success) {
                try result;
                const legacy: ReadSafetyBarrier = .{ .ptr = &fixture, .vtable = &.{ .wait_read_safe = Fixture.waitSafe, .validate_frozen = Fixture.legacy } };
                try std.testing.expectError(error.TestLegacyValidation, legacy.validateFrozenWithAdmission(7, proof, null, .none, port));
                try std.testing.expectError(error.TestLegacyValidation, legacy.validateRetainedSnapshotWithAdmission(7, proof, null, .none, port));
            } else try std.testing.expectError(switch (mode) {
                .newer_index, .regressed_apply, .older_quorum, .unavailable_quorum => error.ReadUnavailable,
                .changed_term, .changed_incarnation, .lost_leadership => error.NotLeader,
                .canceled => error.Canceled,
                .expired => error.DeadlineExceeded,
                .shutdown => error.SchedulerClosed,
                .success => unreachable,
            }, result);
            try std.testing.expect(!fixture.live);
            try std.testing.expectEqual(@as(usize, 1), fixture.callbacks);
            try std.testing.expectEqual(@as(usize, 1), fixture.waits);
            try std.testing.expectEqual(@as(usize, 0), tracker.waiters.count());
            execution.finish();
            try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
        }
    }
}
