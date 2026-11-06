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

//! Process-owned, single-flight DNS snapshots. Request cancellation never
//! waits for libc resolver cleanup or borrows request memory into a lookup.
const std = @import("std");
const Context = @import("antfly_local_sources").serverless_query_lake_read_context.Context;
const A = std.mem.Allocator;
fn now() u64 {
    return @import("antfly_platform").time.monotonicNs();
}
const Endpoint = struct {
    text: [128]u8 = undefined,
    len: u8 = 0,
    fn value(self: *const Endpoint) []const u8 {
        return self.text[0..self.len];
    }
};
pub const Discovery = struct {
    name: []const u8,
    port: u16,
    io: std.Io,
    ttl_ms: u32 = 1000,
    stale_ms: u32 = 5000,
    budget_ms: u32 = 2000,
    retry_ms: u32 = 1000,
    mutex: std.Io.Mutex = .init,
    wake: std.Io.Event = .unset,
    changed: std.Io.Condition = .init,
    group: std.Io.Group = .init,
    stopping: std.atomic.Value(bool) = .init(false),
    started: bool = false,
    requested: bool = false,
    refreshing: bool = false,
    endpoints: [256]Endpoint = undefined,
    count: usize = 0,
    valid_until_ns: u64 = 0,
    stale_until_ns: u64 = 0,
    retry_after_ns: u64 = 0,
    failure: ?anyerror = null,
    fn lock(self: *Discovery) void {
        self.mutex.lockUncancelable(self.io);
    }
    pub fn start(self: *Discovery) !void {
        if (self.started) return;
        self.stopping.store(false, .release);
        try self.group.concurrent(self.io, run, .{self});
        self.started = true;
    }
    pub fn stop(self: *Discovery) void {
        self.stopping.store(true, .release);
        if (self.started) self.group.cancel(self.io);
        self.started = false;
    }
    pub fn resolve(self: *Discovery, a: A, context: Context) ![]const []const u8 {
        const until = now() +| @as(u64, self.budget_ms) * std.time.ns_per_ms;
        while (true) {
            try context.ensureActive();
            if (self.stopping.load(.acquire)) return error.Canceled;
            self.lock();
            if (!self.started) {
                self.mutex.unlock(self.io);
                return error.SqlWorkerUnavailable;
            }
            const time = now();
            const usable = self.count != 0 and time < self.stale_until_ns;
            if (time >= self.valid_until_ns and time >= self.retry_after_ns and !self.refreshing and !self.requested) {
                self.requested = true;
                self.wake.set(self.io);
            }
            if (usable) {
                // Copy fixed-size data while locked; no allocation/I/O under lock.
                var snapshot: [256]Endpoint = undefined;
                const count = self.count;
                @memcpy(snapshot[0..count], self.endpoints[0..count]);
                self.mutex.unlock(self.io);
                const urls = try a.alloc([]const u8, count);
                errdefer a.free(urls);
                var copied: usize = 0;
                errdefer for (urls[0..copied]) |url| a.free(url);
                for (urls, snapshot[0..count]) |*url, *endpoint| {
                    url.* = try a.dupe(u8, endpoint.value());
                    copied += 1;
                }
                try context.ensureActive();
                return urls;
            }
            const failure = if (time < self.retry_after_ns) self.failure else null;
            if (failure) |err| {
                self.mutex.unlock(self.io);
                try context.ensureActive();
                return err;
            }
            if (time >= until) {
                self.mutex.unlock(self.io);
                return error.SqlWorkerUnavailable;
            }
            // Poll only the caller's signal. The one resolver has process-owned
            // lifetime, including on platforms with uncancellable getaddrinfo.
            const remaining = @min(until - time, @as(u64, 10) * std.time.ns_per_ms);
            self.changed.waitTimeout(self.io, &self.mutex, .{ .duration = .{ .raw = .fromNanoseconds(@intCast(remaining)), .clock = .awake } }) catch |err| switch (err) {
                error.Timeout => {},
                error.Canceled => {
                    self.mutex.unlock(self.io);
                    return error.Canceled;
                },
            };
            self.mutex.unlock(self.io);
        }
    }
    const Lookup = struct {
        owner: *Discovery,
        queue: std.Io.Queue(std.Io.net.HostName.LookupResult),
        endpoints: [256]Endpoint = undefined,
        count: usize = 0,
        failure: ?anyerror = null,
        done: std.Io.Event = .unset,
        fn produce(self: *Lookup) void {
            defer self.queue.close(self.owner.io);
            const host = std.Io.net.HostName.init(self.owner.name) catch {
                self.failure = error.InvalidSqlDeployment;
                return;
            };
            host.lookup(self.owner.io, &self.queue, .{ .port = self.owner.port }) catch |err| {
                self.failure = err;
            };
        }
        fn consume(self: *Lookup) void {
            defer self.done.set(self.owner.io);
            while (self.queue.getOne(self.owner.io)) |result| {
                if (result != .address or self.count == self.endpoints.len) continue;
                const entry = &self.endpoints[self.count];
                const url = std.fmt.bufPrint(&entry.text, "http://{f}", .{result.address}) catch continue;
                entry.len = @intCast(url.len);
                self.count += 1;
            } else |_| {}
        }
        fn execute(self: *Lookup) !void {
            const io = self.owner.io;
            var group: std.Io.Group = .init;
            defer group.cancel(io);
            // Drain concurrently even if a resolver supplies more than 256
            // answers; excess answers cannot fill the queue and pin shutdown.
            try group.concurrent(io, consume, .{self});
            try group.concurrent(io, produce, .{self});
            self.done.waitTimeout(io, .{ .duration = .{ .raw = .fromMilliseconds(self.owner.budget_ms), .clock = .awake } }) catch |err| switch (err) {
                error.Timeout => return error.SqlWorkerUnavailable,
                error.Canceled => return error.Canceled,
            };
            try group.await(io);
            if (self.failure) |err| return err;
            if (self.count == 0) return error.SqlWorkerUnavailable;
            std.mem.sort(Endpoint, self.endpoints[0..self.count], {}, struct {
                fn less(_: void, left: Endpoint, right: Endpoint) bool {
                    return std.mem.lessThan(u8, left.value(), right.value());
                }
            }.less);
            var used: usize = 0;
            for (0..self.count) |i| {
                if (used != 0 and std.mem.eql(u8, self.endpoints[used - 1].value(), self.endpoints[i].value())) continue;
                self.endpoints[used] = self.endpoints[i];
                used += 1;
            }
            self.count = used;
        }
    };
    fn run(self: *Discovery) void {
        while (!self.stopping.load(.acquire)) {
            self.wake.reset();
            self.lock();
            const requested = self.requested;
            self.requested = false;
            if (requested) {
                self.refreshing = true;
            }
            self.mutex.unlock(self.io);
            if (!requested) {
                self.wake.wait(self.io) catch return;
                continue;
            }
            var buffer: [32]std.Io.net.HostName.LookupResult = undefined;
            var lookup: Lookup = .{ .owner = self, .queue = .init(&buffer) };
            var failure: ?anyerror = null;
            lookup.execute() catch |err| {
                failure = err;
            };
            self.lock();
            self.refreshing = false;
            self.failure = failure;
            const time = now();
            if (failure == null) {
                @memcpy(self.endpoints[0..lookup.count], lookup.endpoints[0..lookup.count]);
                self.count = lookup.count;
                self.valid_until_ns = time +| @as(u64, self.ttl_ms) * std.time.ns_per_ms;
                self.stale_until_ns = self.valid_until_ns +| @as(u64, self.stale_ms) * std.time.ns_per_ms;
                self.retry_after_ns = 0;
            } else self.retry_after_ns = time +| @as(u64, self.retry_ms) * std.time.ns_per_ms;
            self.changed.broadcast(self.io);
            self.mutex.unlock(self.io);
        }
    }
};

const TestResolver = struct {
    var current: *TestResolver = undefined;
    calls: std.atomic.Value(u32) = .init(0),
    fail: std.atomic.Value(bool) = .init(false),
    entered: std.Io.Event = .unset,
    release: std.Io.Event = .unset,
    fn lookup(_: ?*anyopaque, _: std.Io.net.HostName, queue: *std.Io.Queue(std.Io.net.HostName.LookupResult), options: std.Io.net.HostName.LookupOptions) std.Io.net.HostName.LookupError!void {
        const self = current;
        defer queue.close(std.testing.io);
        _ = self.calls.fetchAdd(1, .monotonic);
        self.entered.set(std.testing.io);
        try self.release.wait(std.testing.io);
        if (self.fail.load(.acquire)) return error.NameServerFailure;
        for ([_]u8{ 2, 1, 2 }) |last| queue.putOne(std.testing.io, .{ .address = .{ .ip4 = .{ .bytes = .{ 10, 0, 0, last }, .port = options.port } } }) catch |err| switch (err) {
            error.Canceled => return error.Canceled,
            error.Closed => return error.Unexpected,
        };
    }
};

test "distributed SQL DNS request deadlines and cancellation do not join a pending shared lookup" {
    var resolver: TestResolver = .{};
    TestResolver.current = &resolver;
    var table = std.testing.io.vtable.*;
    table.netLookup = TestResolver.lookup;
    var io = std.testing.io;
    io.vtable = &table;
    var discovery: Discovery = .{ .name = "query.namespace.svc.cluster.local", .port = 12390, .io = io };
    try discovery.start();
    defer {
        resolver.release.set(std.testing.io);
        discovery.stop();
    }
    const started = now();
    const expired: Context = .{ .deadline_ns = started + 20 * std.time.ns_per_ms };
    try std.testing.expectError(error.DeadlineExceeded, discovery.resolve(std.testing.allocator, expired));
    try std.testing.expect(now() - started < 500 * std.time.ns_per_ms);
    try std.testing.expect(resolver.entered.isSet());
    try std.testing.expect(!resolver.release.isSet());
    const Cancel = struct {
        flag: std.atomic.Value(bool) = .init(false),
        fn check(raw: *const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            return self.flag.load(.acquire);
        }
        fn run(self: *@This()) void {
            std.Io.sleep(std.testing.io, .fromMilliseconds(10), .awake) catch return;
            self.flag.store(true, .release);
        }
    };
    var cancel: Cancel = .{};
    var group: std.Io.Group = .init;
    defer group.cancel(std.testing.io);
    try group.concurrent(std.testing.io, Cancel.run, .{&cancel});
    try std.testing.expectError(error.Canceled, discovery.resolve(std.testing.allocator, .{ .cancellation = .{ .ptr = &cancel, .is_cancelled_fn = Cancel.check } }));
    try std.testing.expect(!resolver.release.isSet());
    resolver.release.set(std.testing.io);
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const urls = try discovery.resolve(arena.allocator(), .{ .deadline_ns = now() + std.time.ns_per_s });
    try std.testing.expectEqual(@as(usize, 2), urls.len);
    try std.testing.expectEqualStrings("http://10.0.0.1:12390", urls[0]);
    try std.testing.expectEqualStrings("http://10.0.0.2:12390", urls[1]);
    _ = try discovery.resolve(arena.allocator(), .{});
    try std.testing.expectEqual(@as(u32, 1), resolver.calls.load(.acquire));
}

test "distributed SQL DNS coalesces cold callers bounds stale fallback and recovers after failure" {
    var resolver: TestResolver = .{};
    TestResolver.current = &resolver;
    var table = std.testing.io.vtable.*;
    table.netLookup = TestResolver.lookup;
    var io = std.testing.io;
    io.vtable = &table;
    var discovery: Discovery = .{ .name = "query.namespace.svc.cluster.local", .port = 12390, .io = io };
    try discovery.start();
    defer {
        resolver.release.set(std.testing.io);
        discovery.stop();
    }
    const Job = struct {
        discovery: *Discovery,
        failure: ?anyerror = null,
        fn run(self: *@This()) void {
            var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer arena.deinit();
            const urls = self.discovery.resolve(arena.allocator(), .{ .deadline_ns = now() + 2 * std.time.ns_per_s }) catch |err| {
                self.failure = err;
                return;
            };
            std.testing.expectEqual(@as(usize, 2), urls.len) catch |err| {
                self.failure = err;
            };
        }
    };
    var jobs: [16]Job = undefined;
    var group: std.Io.Group = .init;
    defer group.cancel(std.testing.io);
    for (&jobs) |*job| {
        job.* = .{ .discovery = &discovery };
        try group.concurrent(std.testing.io, Job.run, .{job});
    }
    try resolver.entered.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromMilliseconds(1000), .clock = .awake } });
    resolver.release.set(std.testing.io);
    try group.await(std.testing.io);
    for (jobs) |job| try std.testing.expect(job.failure == null);
    try std.testing.expectEqual(@as(u32, 1), resolver.calls.load(.acquire));
    // An expired snapshot can serve only within the fixed stale grace while
    // exactly one background refresh is blocked. Failed refreshes never extend it.
    resolver.entered.reset();
    resolver.release.reset();
    resolver.fail.store(true, .release);
    discovery.lock();
    discovery.valid_until_ns = 0;
    discovery.mutex.unlock(discovery.io);
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    _ = try discovery.resolve(arena.allocator(), .{});
    try resolver.entered.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromMilliseconds(1000), .clock = .awake } });
    discovery.lock();
    discovery.stale_until_ns = 0;
    discovery.mutex.unlock(discovery.io);
    resolver.release.set(std.testing.io);
    try std.testing.expectError(error.NameServerFailure, discovery.resolve(arena.allocator(), .{}));
    try std.testing.expectError(error.NameServerFailure, discovery.resolve(arena.allocator(), .{}));
    try std.testing.expectEqual(@as(u32, 2), resolver.calls.load(.acquire));
    resolver.fail.store(false, .release);
    discovery.lock();
    discovery.retry_after_ns = 0;
    discovery.mutex.unlock(discovery.io);
    _ = try discovery.resolve(arena.allocator(), .{});
    try std.testing.expectEqual(@as(u32, 3), resolver.calls.load(.acquire));
}
