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

//! Query cancellation: native tasks share one bounded background watch per
//! active query; hosted tasks poll only at cooperative execution checkpoints.
const std = @import("std");
const state = @import("state.zig");
const Clock = @import("runtime.zig").Clock;
const Context = @import("antfly_local_sources").serverless_query_lake_read_context.Context;
const A = std.mem.Allocator;

fn cancelled(a: A, original: state.Store, query: state.Query, tenant: state.Digest, context: Context, clock: Clock) !bool {
    if (query.expires_ms <= clock.now()) return true;
    var store = original;
    store.context = context;
    store.tenant = tenant;
    const key = try store.key(a, query, null, 0);
    var value = (try store.load(a, key)) orelse return true;
    defer value.deinit(a);
    if (!std.meta.eql(value.value.plan, query.id) or value.value.expires_ms != query.expires_ms) return error.InvalidSqlExecutionState;
    return value.value.cancel_requested or value.value.terminal();
}
fn invalidAuthority(err: anyerror) bool {
    return err == error.SqlExecutionUnauthorized or err == error.InvalidSqlExecutionState;
}

pub const Registry = struct {
    const Entry = struct {
        query: ?state.Query = null,
        tenant: state.Digest = undefined,
        users: u16 = 0,
        polls: u8 = 0,
        next_ns: u64 = 0,
        cancelled: std.atomic.Value(bool) = .init(false),
        idle: std.atomic.Value(bool) = .init(false),
    };
    a: A,
    io: std.Io,
    store: state.Store,
    clock: Clock = .{},
    interval_ms: u32 = 1000,
    budget_ms: u32 = 2000,
    entries: [256]Entry = @splat(.{}),
    cursor: usize = 0,
    mutex: std.atomic.Mutex = .unlocked,
    wake: std.Io.Event = .unset,
    group: std.Io.Group = .init,
    started: bool = false,
    stopping: std.atomic.Value(bool) = .init(false),
    fn lock(self: *Registry) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    pub fn start(self: *Registry) !void {
        if (self.started) return;
        self.stopping.store(false, .release);
        try self.group.concurrent(self.io, run, .{self});
        self.started = true;
    }
    pub fn stop(self: *Registry) void {
        self.stopping.store(true, .release);
        if (self.started) self.group.cancel(self.io);
        self.started = false;
    }
    fn acquire(self: *Registry, query: state.Query, tenant: state.Digest) !*Entry {
        self.lock();
        defer self.mutex.unlock();
        if (self.stopping.load(.acquire) or !self.started) return error.SqlWorkerUnavailable;
        var free: ?*Entry = null;
        for (&self.entries) |*entry| {
            if (entry.query) |value| {
                if (std.meta.eql(value, query)) {
                    if (!std.meta.eql(entry.tenant, tenant)) return error.SqlExecutionUnauthorized;
                    if (entry.users == 256) return error.SqlWorkerUnavailable;
                    entry.users += 1;
                    entry.idle.store(false, .release);
                    return entry;
                }
            } else free = entry;
        }
        const entry = free orelse return error.SqlWorkerUnavailable;
        entry.* = .{ .query = query, .tenant = tenant, .users = 1, .next_ns = @import("antfly_platform").time.monotonicNs() +| @as(u64, self.interval_ms) * std.time.ns_per_ms };
        self.wake.set(self.io);
        return entry;
    }
    fn release(self: *Registry, entry: *Entry) void {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(entry.users != 0);
        entry.users -= 1;
        if (entry.users == 0) entry.idle.store(true, .release);
        if (entry.users == 0 and entry.polls == 0) entry.* = .{};
        self.wake.set(self.io);
    }
    pub fn retire(self: *Registry, query: state.Query) void {
        self.lock();
        defer self.mutex.unlock();
        for (&self.entries) |*entry| if (entry.query) |value| {
            if (std.meta.eql(value, query)) entry.cancelled.store(true, .release);
        };
        self.wake.set(self.io);
    }
    fn take(self: *Registry, out: []*Entry, delay_ms: *u32) usize {
        self.lock();
        defer self.mutex.unlock();
        const now = @import("antfly_platform").time.monotonicNs();
        delay_ms.* = 30000;
        var count: usize = 0;
        const first_index = self.cursor;
        for (0..self.entries.len) |offset| {
            const index = (first_index + offset) % self.entries.len;
            const entry = &self.entries[index];
            if (entry.users == 0 or entry.polls != 0 or entry.cancelled.load(.acquire)) continue;
            if (entry.next_ns > now) {
                delay_ms.* = @min(delay_ms.*, @as(u32, @intCast(@min(@as(u64, 30000), (entry.next_ns - now) / std.time.ns_per_ms + 1))));
                continue;
            }
            entry.polls += 1;
            entry.next_ns = now +| @as(u64, self.interval_ms) * std.time.ns_per_ms;
            out[count] = entry;
            count += 1;
            self.cursor = (index + 1) % self.entries.len;
            if (count == out.len) break;
        }
        return count;
    }
    fn poll(self: *Registry, entry: *Entry) void {
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const Guard = struct {
            registry: *Registry,
            entry: *Entry,
            fn check(raw: *const anyopaque) bool {
                const guard: *const @This() = @ptrCast(@alignCast(raw));
                return guard.registry.stopping.load(.acquire) or guard.entry.idle.load(.acquire) or guard.entry.cancelled.load(.acquire);
            }
        };
        const guard: Guard = .{ .registry = self, .entry = entry };
        const context: Context = .{ .io = self.io, .deadline_ns = @import("antfly_platform").time.monotonicNs() +| @as(u64, self.budget_ms) * std.time.ns_per_ms, .cancellation = .{ .ptr = &guard, .is_cancelled_fn = Guard.check } };
        const ended = cancelled(arena.allocator(), self.store, entry.query.?, entry.tenant, context, self.clock) catch |err| {
            if (invalidAuthority(err)) entry.cancelled.store(true, .release);
            return; // Transient watch failures do not fail otherwise healthy work.
        };
        if (ended) entry.cancelled.store(true, .release);
    }
    fn releasePolls(self: *Registry, entries: []const *Entry) void {
        self.lock();
        defer self.mutex.unlock();
        for (entries) |entry| {
            entry.polls -= 1;
            if (entry.users == 0 and entry.polls == 0) entry.* = .{};
        }
    }
    fn pollOwned(self: *Registry, entry: *Entry) void {
        defer {
            self.releasePolls(&.{entry});
            self.wake.set(self.io);
        }
        if (!self.stopping.load(.acquire)) self.poll(entry);
    }
    fn run(self: *Registry) void {
        while (!self.stopping.load(.acquire)) {
            self.wake.reset();
            // One in-flight read per active query, bounded by the 256 entries.
            // A slow query never occupies another query's watch lane.
            var entries: [256]*Entry = undefined;
            var delay_ms: u32 = 0;
            const count = self.take(&entries, &delay_ms);
            var retry_from: ?usize = null;
            for (entries[0..count]) |entry| {
                if (self.stopping.load(.acquire)) {
                    self.releasePolls(&.{entry});
                    continue;
                }
                // run itself keeps this group alive until every spawn returns;
                // stop joins both the scheduler and all outstanding reads.
                self.group.concurrent(self.io, pollOwned, .{ self, entry }) catch {
                    if (retry_from == null) retry_from = (@intFromPtr(entry) - @intFromPtr(&self.entries[0])) / @sizeOf(Entry);
                    self.releasePolls(&.{entry});
                };
            }
            if (retry_from) |index| {
                self.lock();
                self.cursor = index; // Resource pressure must not starve later queries.
                self.mutex.unlock();
            }
            if (count != 0) continue;
            self.wake.waitTimeout(self.io, .{ .duration = .{ .raw = .fromMilliseconds(delay_ms), .clock = .awake } }) catch |err| switch (err) {
                error.Timeout => continue,
                error.Canceled => return,
            };
        }
    }
};

pub const Task = struct {
    a: A,
    store: state.Store,
    query: state.Query,
    tenant: state.Digest,
    clock: Clock,
    registry: ?*Registry = null,
    entry: ?*Registry.Entry = null,
    hosted: bool = false,
    parent: Context,
    next_ns: u64 = 0,
    ended: bool = false,
    pub fn enter(self: *Task) !void {
        if (self.registry) |registry| self.entry = try registry.acquire(self.query, self.tenant);
        self.next_ns = @import("antfly_platform").time.monotonicNs() + std.time.ns_per_s;
    }
    pub fn deinit(self: *Task) void {
        if (self.entry) |entry| self.registry.?.release(entry);
    }
    pub fn isCancelled(self: *const Task) bool {
        return self.ended or (if (self.entry) |entry| entry.cancelled.load(.acquire) else false);
    }
    pub fn scoped(self: *Task, parent: Context) Context {
        self.parent = parent;
        var context = parent;
        context.checkpoint = .{ .ptr = self, .check = check };
        context.additional_checkpoint = null; // parent.ensureActive preserves both hooks.
        return context;
    }
    fn check(raw: *anyopaque) !void {
        const self: *Task = @ptrCast(@alignCast(raw));
        try self.parent.ensureActive();
        if (self.registry) |registry| if (registry.stopping.load(.acquire)) return error.Canceled;
        if (self.isCancelled()) return error.SqlQueryCancelled;
        if (!self.hosted or self.entry != null) return; // Native callbacks never perform I/O.
        const now = @import("antfly_platform").time.monotonicNs();
        if (now < self.next_ns) return;
        self.next_ns = now + std.time.ns_per_s;
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        var context = self.parent;
        const deadline = now + 2 * std.time.ns_per_s;
        context.deadline_ns = if (context.deadline_ns) |parent| @min(parent, deadline) else deadline;
        self.ended = cancelled(arena.allocator(), self.store, self.query, self.tenant, context, self.clock) catch |err| {
            try self.parent.ensureActive();
            if (invalidAuthority(err)) return err;
            return;
        };
        if (self.ended) return error.SqlQueryCancelled;
    }
};

test "distributed SQL worker watches coalesce queries and observe remote durable cancellation" {
    const a = std.testing.allocator;
    const storage = @import("objectstore");
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try temporary.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(path);
    var filesystem = try storage.FilesystemClient.initWithIo(a, path, std.testing.io);
    defer filesystem.deinit();
    var base = filesystem.client();
    try base.makeBucket("queries");
    const records: state.Store = .{ .client = base, .bucket = "queries", .prefix = "sql", .now_ms = 1000, .tenant = @splat('a') };
    const query: state.Query = .{ .id = @splat('b'), .expires_ms = 1000000 };
    const key = try records.key(a, query, null, 0);
    defer a.free(key);
    try records.create(a, key, query.id, query.expires_ms);
    const Gate = struct {
        var current: *@This() = undefined;
        base_get: @FieldType(storage.Client.VTable, "get_object"),
        entered: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        reads: std.atomic.Value(u32) = .init(0),
        fn get(raw: *anyopaque, allocator: A, bucket: []const u8, key_: []const u8, options: storage.GetOptions) !storage.GetResult {
            const self = current;
            _ = self.reads.fetchAdd(1, .monotonic);
            self.entered.set(std.testing.io);
            while (!self.release.isSet()) {
                if (options.cancellation) |token| try token.check();
                self.release.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromMilliseconds(10), .clock = .awake } }) catch |err| switch (err) {
                    error.Timeout => continue,
                    error.Canceled => return err,
                };
            }
            return self.base_get(raw, allocator, bucket, key_, options);
        }
    };
    var gate: Gate = .{ .base_get = base.vtable.get_object };
    Gate.current = &gate;
    var table = base.vtable.*;
    table.get_object = Gate.get;
    var watched = records;
    watched.client.vtable = &table;
    const Time = struct {
        fn read(_: ?*anyopaque) u64 {
            return 1000;
        }
    };
    const clock: Clock = .{ .read = Time.read };
    var registry: Registry = .{ .a = a, .io = std.testing.io, .store = watched, .clock = clock, .interval_ms = 20 };
    try registry.start();
    defer {
        gate.release.set(std.testing.io);
        registry.stop();
    }
    var first: Task = .{ .a = a, .store = records, .query = query, .tenant = @splat('a'), .clock = clock, .parent = .{}, .registry = &registry };
    try first.enter();
    defer first.deinit();
    var second = first;
    second.entry = null;
    try second.enter();
    defer second.deinit();
    try std.testing.expect(first.entry == second.entry);
    const first_context = first.scoped(.{});
    const second_context = second.scoped(.{});
    try gate.entered.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromMilliseconds(2000), .clock = .awake } });
    // A blocked watch never blocks execution checkpoints or starts a second GET.
    for (0..1000) |_| {
        try first_context.ensureActive();
        try second_context.ensureActive();
    }
    try std.testing.expectEqual(@as(u32, 1), gate.reads.load(.acquire));
    try records.requestCancel(a, key);
    gate.release.set(std.testing.io);
    const until = @import("antfly_platform").time.monotonicNs() + 2 * std.time.ns_per_s;
    while (!first.isCancelled()) {
        if (@import("antfly_platform").time.monotonicNs() >= until) return error.CancellationWatchDidNotPropagate;
        try std.Io.sleep(std.testing.io, .fromMilliseconds(1), .awake);
    }
    try std.testing.expectError(error.SqlQueryCancelled, first_context.ensureActive());
    try std.testing.expectError(error.SqlQueryCancelled, second_context.ensureActive());
    try std.testing.expectEqual(@as(u32, 1), gate.reads.load(.acquire));
}

test "distributed SQL hosted cancellation checkpoints preserve parent hooks" {
    const a = std.testing.allocator;
    const storage = @import("objectstore");
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    const records: state.Store = .{ .client = memory.client(), .bucket = "queries", .prefix = "sql", .now_ms = 1000, .tenant = @splat('a') };
    const query: state.Query = .{ .id = @splat('b'), .expires_ms = 1000000 };
    const key = try records.key(a, query, null, 0);
    defer a.free(key);
    try records.create(a, key, query.id, query.expires_ms);
    const Time = struct {
        fn read(_: ?*anyopaque) u64 {
            return 1000;
        }
    };
    var checks: usize = 0;
    const Hook = struct {
        fn check(raw: *anyopaque) !void {
            const counter: *usize = @ptrCast(@alignCast(raw));
            counter.* += 1;
        }
    };
    var task: Task = .{ .a = a, .store = records, .query = query, .tenant = @splat('a'), .clock = .{ .read = Time.read }, .parent = .{}, .hosted = true };
    try task.enter();
    defer task.deinit();
    const context = task.scoped(.{ .checkpoint = .{ .ptr = &checks, .check = Hook.check }, .additional_checkpoint = .{ .ptr = &checks, .check = Hook.check } });
    try context.ensureActive();
    try std.testing.expectEqual(@as(usize, 2), checks);
    try records.requestCancel(a, key);
    task.next_ns = 0;
    try std.testing.expectError(error.SqlQueryCancelled, context.ensureActive());
    try std.testing.expect(task.isCancelled());
}

test "distributed SQL independent watches observe healthy cancellation while other reads stall" {
    const a = std.testing.allocator;
    const storage = @import("objectstore");
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try temporary.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(path);
    var filesystem = try storage.FilesystemClient.initWithIo(a, path, std.testing.io);
    defer filesystem.deinit();
    var base = filesystem.client();
    try base.makeBucket("queries");
    const records: state.Store = .{ .client = base, .bucket = "queries", .prefix = "sql", .now_ms = 1000, .tenant = @splat('a') };
    const target: state.Query = .{ .id = @splat('8'), .expires_ms = 1000000 };
    const fast_key = try records.key(a, target, null, 0);
    defer a.free(fast_key);
    for (0..9) |i| {
        const query: state.Query = .{ .id = @splat(@as(u8, @intCast('0' + i))), .expires_ms = 1000000 };
        const key = try records.key(a, query, null, 0);
        defer a.free(key);
        try records.create(a, key, query.id, query.expires_ms);
    }
    try records.requestCancel(a, fast_key);
    const Gate = struct {
        var current: *@This() = undefined;
        base_get: @FieldType(storage.Client.VTable, "get_object"),
        fast_key: []const u8,
        fast_seen: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        fn get(raw: *anyopaque, allocator: A, bucket: []const u8, key: []const u8, options: storage.GetOptions) !storage.GetResult {
            const self = current;
            if (std.mem.eql(u8, key, self.fast_key)) self.fast_seen.set(std.testing.io) else while (!self.release.isSet()) {
                if (options.cancellation) |token| try token.check();
                self.release.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromMilliseconds(1), .clock = .awake } }) catch |err| switch (err) {
                    error.Timeout => continue,
                    error.Canceled => return err,
                };
            }
            return self.base_get(raw, allocator, bucket, key, options);
        }
    };
    var gate: Gate = .{ .base_get = base.vtable.get_object, .fast_key = fast_key };
    Gate.current = &gate;
    var table = base.vtable.*;
    table.get_object = Gate.get;
    var watched = records;
    watched.client.vtable = &table;
    const Time = struct {
        fn read(_: ?*anyopaque) u64 {
            return 1000;
        }
    };
    var registry: Registry = .{ .a = a, .io = std.testing.io, .store = watched, .clock = .{ .read = Time.read } };
    for (registry.entries[0..9], 0..) |*entry, i| entry.* = .{ .query = .{ .id = @splat(@as(u8, @intCast('0' + i))), .expires_ms = 1000000 }, .tenant = @splat('a'), .users = 1 };
    const started = @import("antfly_platform").time.monotonicNs();
    try registry.start();
    defer {
        gate.release.set(std.testing.io);
        registry.stop();
    }
    try gate.fast_seen.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromMilliseconds(1000), .clock = .awake } });
    try std.testing.expect(@import("antfly_platform").time.monotonicNs() - started < std.time.ns_per_s);
    try std.testing.expect(!gate.release.isSet());
}
