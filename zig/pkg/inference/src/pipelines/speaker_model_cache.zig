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

//! Cancellable cold-load coordination and leased speaker model generations.
const std = @import("std");
const Embedder = @import("speaker_embedding.zig").Embedder;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Flights = @import("media_speaker_flights.zig").Registry;
pub const Cache = struct {
    pub const max_entries = 64;
    pub const max_idle = 4;
    mutex: std.atomic.Mutex = .unlocked,
    flights: Flights = .{},
    entries: std.ArrayList(*Entry) = .empty,
    pending_loads: usize = 0,
    clock: u64 = 0,
    imports: std.atomic.Value(u64) = .init(0),
    loader_context: ?*anyopaque = null,
    loader: *const fn (?*anyopaque, std.mem.Allocator, []const u8) anyerror!Embedder = load,
    const Entry = struct {
        a: std.mem.Allocator,
        path: []u8,
        key: []u8,
        model: Embedder,
        refs: usize = 1,
        retired: bool = false,
        used: u64 = 0,
        fn destroy(self: *Entry) void {
            const a = self.a;
            self.model.deinit();
            a.free(self.path);
            a.free(self.key);
            a.destroy(self);
        }
    };
    pub const Lease = struct {
        cache: *Cache,
        entry: *Entry,
        pub fn model(self: Lease) *Embedder {
            return &self.entry.model;
        }
        pub fn verificationFailed(self: Lease, err: anyerror) void {
            switch (err) {
                error.ModelConfigurationChanged, error.ChecksumMismatch, error.InvalidManagedDownload => self.retire(),
                else => {},
            }
        }
        pub fn retire(self: Lease) void {
            self.cache.lock();
            defer self.cache.mutex.unlock();
            self.entry.retired = true;
        }
        pub fn deinit(self: Lease) void {
            self.cache.release(self.entry);
        }
    };
    fn load(_: ?*anyopaque, a: std.mem.Allocator, path: []const u8) !Embedder {
        return Embedder.load(a, path);
    }
    fn lock(self: *Cache) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn touch(self: *Cache, entry: *Entry) void {
        self.clock += 1;
        entry.used = self.clock;
        entry.retired = false;
        for (self.entries.items) |other| if (other != entry and std.mem.eql(u8, other.path, entry.path)) {
            other.retired = true;
        };
    }
    fn collect(self: *Cache, removed: *[max_entries]*Entry) usize {
        var count: usize = 0;
        var i: usize = 0;
        while (i < self.entries.items.len) {
            const entry = self.entries.items[i];
            if (entry.refs == 0 and entry.retired) {
                removed[count] = self.entries.swapRemove(i);
                count += 1;
            } else i += 1;
        }
        while (true) {
            var idle: usize = 0;
            var oldest: ?usize = null;
            for (self.entries.items, 0..) |entry, index| if (entry.refs == 0) {
                idle += 1;
                if (oldest == null or entry.used < self.entries.items[oldest.?].used) oldest = index;
            };
            if (idle <= max_idle) break;
            removed[count] = self.entries.swapRemove(oldest.?);
            count += 1;
        }
        return count;
    }
    fn release(self: *Cache, entry: *Entry) void {
        var removed: [max_entries]*Entry = undefined;
        self.lock();
        std.debug.assert(entry.refs > 0);
        entry.refs -= 1;
        const count = self.collect(&removed);
        self.mutex.unlock();
        for (removed[0..count]) |old| old.destroy();
    }
    fn cancelLoad(self: *Cache) void {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(self.pending_loads > 0);
        self.pending_loads -= 1;
    }
    pub fn acquire(self: *Cache, a: std.mem.Allocator, path: []const u8, key: []const u8, control: Control) !Lease {
        var identity: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(key, &identity, .{});
        const flight = try self.flights.acquire(a, identity, control);
        defer flight.deinit();
        try control.lockWithPoll(&self.mutex, 100 * std.time.ns_per_us);
        for (self.entries.items) |entry| if (std.mem.eql(u8, entry.key, key)) {
            entry.refs += 1;
            self.touch(entry);
            self.mutex.unlock();
            return .{ .cache = self, .entry = entry };
        };
        // Count imports in progress so independent cold keys cannot overbook
        // capacity. Reclaim an idle session before importing its replacement.
        var evicted: ?*Entry = null;
        if (self.entries.items.len + self.pending_loads >= max_entries) {
            var oldest: ?usize = null;
            for (self.entries.items, 0..) |entry, index| if (entry.refs == 0) {
                if (oldest == null or entry.used < self.entries.items[oldest.?].used) oldest = index;
            };
            if (oldest) |index| evicted = self.entries.swapRemove(index) else {
                self.mutex.unlock();
                return error.ResourceTemporarilyUnavailable;
            }
        }
        self.pending_loads += 1;
        self.mutex.unlock();
        errdefer self.cancelLoad();
        if (evicted) |old| old.destroy();
        try control.check();
        _ = self.imports.fetchAdd(1, .monotonic);
        var model = try self.loader(self.loader_context, a, path);
        errdefer model.deinit();
        try control.check();
        const entry = try a.create(Entry);
        errdefer a.destroy(entry);
        const owned_path = try a.dupe(u8, path);
        errdefer a.free(owned_path);
        const owned_key = try a.dupe(u8, key);
        errdefer a.free(owned_key);
        entry.* = .{ .a = a, .path = owned_path, .key = owned_key, .model = model };
        var removed: [max_entries]*Entry = undefined;
        try control.lockWithPoll(&self.mutex, 100 * std.time.ns_per_us);
        self.entries.append(a, entry) catch |err| {
            self.mutex.unlock();
            return err;
        };
        self.pending_loads -= 1;
        self.touch(entry);
        const count = self.collect(&removed);
        self.mutex.unlock();
        for (removed[0..count]) |old| old.destroy();
        return .{ .cache = self, .entry = entry };
    }
    pub fn deinit(self: *Cache, a: std.mem.Allocator) void {
        std.debug.assert(self.pending_loads == 0);
        for (self.entries.items) |entry| {
            std.debug.assert(entry.refs == 0);
            entry.destroy();
        }
        self.entries.deinit(a);
        self.flights.deinit(a);
    }
};

const TestLoader = struct {
    calls: std.atomic.Value(usize) = .init(0),
    closes: std.atomic.Value(usize) = .init(0),
    blocked: std.atomic.Value(bool) = .init(false),
    entered: std.atomic.Value(bool) = .init(false),
    fn create(raw: ?*anyopaque, a: std.mem.Allocator, _: []const u8) !Embedder {
        const self: *TestLoader = @ptrCast(@alignCast(raw.?));
        _ = self.calls.fetchAdd(1, .monotonic);
        self.entered.store(true, .release);
        while (self.blocked.load(.acquire)) std.atomic.spinLoopHint();
        return .{ .allocator = a, .session = .{ .ptr = self, .vtable = &vtable } };
    }
    const backends = @import("../backends/backends.zig");
    const Session = @import("../backends/session.zig").Session;
    fn run(_: *anyopaque, _: []const backends.Tensor, _: std.mem.Allocator) ![]backends.Tensor {
        return error.SessionShouldNotRun;
    }
    fn controlled(p: *anyopaque, input: []const backends.Tensor, a: std.mem.Allocator, _: Control) ![]backends.Tensor {
        return run(p, input, a);
    }
    fn info(_: *anyopaque) []const backends.TensorInfo {
        return &.{};
    }
    fn backend(_: *anyopaque) backends.BackendType {
        return .native;
    }
    fn close(raw: *anyopaque) void {
        const self: *TestLoader = @ptrCast(@alignCast(raw));
        _ = self.closes.fetchAdd(1, .monotonic);
    }
    const vtable = Session.VTable{ .run = run, .runWithControl = controlled, .inputInfo = info, .outputInfo = info, .backend = backend, .close = close };
};

test "model aware media speaker generations stay leased until requests finish and bound idle residency" {
    const a = std.testing.allocator;
    var loader: TestLoader = .{};
    {
        var cache = Cache{ .loader_context = &loader, .loader = TestLoader.create };
        defer cache.deinit(a);
        const old = try cache.acquire(a, "same.onnx", "same:r1", .{});
        const newer = try cache.acquire(a, "same.onnx", "same:r2", .{});
        try std.testing.expect(old.model() != newer.model());
        newer.deinit();
        try std.testing.expectEqual(@as(usize, 0), loader.closes.load(.monotonic));
        try std.testing.expect(old.entry.retired);
        old.deinit();
        try std.testing.expectEqual(@as(usize, 1), loader.closes.load(.monotonic));
        for (0..12) |i| {
            var path_buf: [32]u8 = undefined;
            const path = try std.fmt.bufPrint(&path_buf, "model-{d}.onnx", .{i});
            const lease = try cache.acquire(a, path, path, .{});
            lease.deinit();
            try std.testing.expect(cache.entries.items.len <= Cache.max_idle);
        }
    }
    try std.testing.expectEqual(loader.calls.load(.monotonic), loader.closes.load(.monotonic));
}

test "model aware media cold speaker imports coalesce while waiting requests remain cancellable" {
    const a = std.testing.allocator;
    var loader: TestLoader = .{};
    loader.blocked.store(true, .release);
    var cache = Cache{ .loader_context = &loader, .loader = TestLoader.create };
    defer cache.deinit(a);
    const Worker = struct {
        cache: *Cache,
        failure: ?anyerror = null,
        model: ?*Embedder = null,
        fn run(self: *@This()) void {
            const lease = self.cache.acquire(std.testing.allocator, "same.onnx", "same:r1", .{}) catch |err| {
                self.failure = err;
                return;
            };
            defer lease.deinit();
            self.model = lease.model();
        }
    };
    var workers: [4]Worker = undefined;
    var threads: [4]std.Thread = undefined;
    for (&workers, &threads) |*worker, *thread| {
        worker.* = .{ .cache = &cache };
        thread.* = try std.Thread.spawn(.{}, Worker.run, .{worker});
    }
    while (!loader.entered.load(.acquire)) std.atomic.spinLoopHint();
    const deadline = @import("antfly_platform").time.monotonicNs() + std.time.ns_per_ms;
    try std.testing.expectError(error.Timeout, cache.acquire(a, "same.onnx", "same:r1", .{ .deadline_ns = deadline }));
    loader.blocked.store(false, .release);
    for (threads) |thread| thread.join();
    for (workers) |worker| {
        if (worker.failure) |err| return err;
        try std.testing.expect(worker.model == workers[0].model);
    }
    try std.testing.expectEqual(@as(usize, 1), loader.calls.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), cache.imports.load(.monotonic));
}

test "model aware media speaker cache allocation failures retire every imported model" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            var loader: TestLoader = .{};
            defer std.debug.assert(loader.calls.load(.monotonic) == loader.closes.load(.monotonic));
            var cache = Cache{ .loader_context = &loader, .loader = TestLoader.create };
            defer cache.deinit(a);
            const first = try cache.acquire(a, "same.onnx", "same:r1", .{});
            defer first.deinit();
            const second = try cache.acquire(a, "same.onnx", "same:r2", .{});
            second.deinit();
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "model aware media cancelled verification preserves healthy cached speaker models" {
    const a = std.testing.allocator;
    var loader: TestLoader = .{};
    var cache = Cache{ .loader_context = &loader, .loader = TestLoader.create };
    defer cache.deinit(a);
    for ([_]anyerror{ error.Cancelled, error.Timeout, error.OutOfMemory, error.ResourceTemporarilyUnavailable }) |err| {
        const lease = try cache.acquire(a, "same.onnx", "same:r1", .{});
        lease.verificationFailed(err);
        lease.deinit();
        try std.testing.expectEqual(@as(usize, 1), cache.entries.items.len);
        try std.testing.expectEqual(@as(usize, 0), loader.closes.load(.monotonic));
    }
    try std.testing.expectEqual(@as(u64, 1), cache.imports.load(.monotonic));
    const invalid = try cache.acquire(a, "same.onnx", "same:r1", .{});
    invalid.verificationFailed(error.ModelConfigurationChanged);
    invalid.deinit();
    try std.testing.expectEqual(@as(usize, 0), cache.entries.items.len);
    try std.testing.expectEqual(@as(usize, 1), loader.closes.load(.monotonic));
}

test "model aware media full speaker cache evicts idle models before importing and rejects only busy capacity" {
    const a = std.testing.allocator;
    var loader: TestLoader = .{};
    var cache = Cache{ .loader_context = &loader, .loader = TestLoader.create };
    defer cache.deinit(a);
    var held: [Cache.max_entries]?Cache.Lease = @splat(null);
    defer for (held) |maybe| if (maybe) |lease| lease.deinit();
    for (&held, 0..) |*slot, i| {
        var name_buf: [32]u8 = undefined;
        const name = try std.fmt.bufPrint(&name_buf, "capacity-{d}", .{i});
        slot.* = try cache.acquire(a, name, name, .{});
    }
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, cache.acquire(a, "new", "new", .{}));
    try std.testing.expectEqual(@as(usize, Cache.max_entries), loader.calls.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 0), loader.closes.load(.monotonic));
    held[0].?.deinit();
    held[0] = null;
    const replacement = try cache.acquire(a, "new", "new", .{});
    defer replacement.deinit();
    try std.testing.expectEqual(@as(usize, Cache.max_entries), cache.entries.items.len);
    try std.testing.expectEqual(@as(usize, Cache.max_entries + 1), loader.calls.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 1), loader.closes.load(.monotonic));
    const hit = try cache.acquire(a, "capacity-1", "capacity-1", .{});
    defer hit.deinit();
    try std.testing.expect(hit.model() == held[1].?.model());
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, cache.acquire(a, "another", "another", .{}));
    try std.testing.expectEqual(@as(u64, Cache.max_entries + 1), cache.imports.load(.monotonic));
}

test "model aware media pending speaker imports reserve the last slot and cancellation releases capacity" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |cancel| {
        var loader: TestLoader = .{};
        var cache = Cache{ .loader_context = &loader, .loader = TestLoader.create };
        defer cache.deinit(a);
        var held: [Cache.max_entries - 1]?Cache.Lease = @splat(null);
        defer for (held) |maybe| if (maybe) |lease| lease.deinit();
        for (&held, 0..) |*slot, i| {
            var name_buf: [32]u8 = undefined;
            const name = try std.fmt.bufPrint(&name_buf, "capacity-{d}", .{i});
            slot.* = try cache.acquire(a, name, name, .{});
        }
        const Worker = struct {
            cache: *Cache,
            cancelled: std.atomic.Value(bool) = .init(false),
            failure: ?anyerror = null,
            fn check(raw: ?*anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw.?));
                if (self.cancelled.load(.acquire)) return error.Cancelled;
            }
            fn run(self: *@This()) void {
                const lease = self.cache.acquire(std.testing.allocator, "pending", "pending", .{ .ptr = self, .check_fn = check }) catch |err| {
                    self.failure = err;
                    return;
                };
                lease.deinit();
            }
        };
        loader.entered.store(false, .release);
        loader.blocked.store(true, .release);
        var worker = Worker{ .cache = &cache };
        {
            const thread = try std.Thread.spawn(.{}, Worker.run, .{&worker});
            defer {
                loader.blocked.store(false, .release);
                thread.join();
            }
            while (!loader.entered.load(.acquire)) std.atomic.spinLoopHint();
            try std.testing.expectError(error.ResourceTemporarilyUnavailable, cache.acquire(a, "overflow", "overflow", .{}));
            try std.testing.expectEqual(@as(usize, Cache.max_entries), loader.calls.load(.monotonic));
            const hit = try cache.acquire(a, "capacity-0", "capacity-0", .{});
            hit.deinit();
            worker.cancelled.store(cancel, .release);
        }
        try std.testing.expectEqual(@as(usize, 0), cache.pending_loads);
        if (cancel) {
            try std.testing.expectEqual(error.Cancelled, worker.failure.?);
            try std.testing.expectEqual(@as(usize, Cache.max_entries - 1), cache.entries.items.len);
            try std.testing.expectEqual(@as(usize, 1), loader.closes.load(.monotonic));
        } else try std.testing.expectEqual(@as(?anyerror, null), worker.failure);
        // A failed import frees its reservation; a successful, now-idle import
        // can be evicted. Both paths permit another cold model at capacity.
        const retry = try cache.acquire(a, "retry", "retry", .{});
        retry.deinit();
        try std.testing.expectEqual(@as(usize, Cache.max_entries), cache.entries.items.len);
    }
}

test "model aware media failed speaker imports release their capacity reservations" {
    const Failure = struct {
        fn load(_: ?*anyopaque, _: std.mem.Allocator, _: []const u8) !Embedder {
            return error.TestImportFailed;
        }
    };
    const a = std.testing.allocator;
    var cache = Cache{ .loader = Failure.load };
    defer cache.deinit(a);
    try std.testing.expectError(error.TestImportFailed, cache.acquire(a, "failed", "failed", .{}));
    try std.testing.expectEqual(@as(usize, 0), cache.pending_loads);
    try std.testing.expectEqual(@as(usize, 0), cache.entries.items.len);
    var loader: TestLoader = .{};
    cache.loader_context = &loader;
    cache.loader = TestLoader.create;
    const retry = try cache.acquire(a, "failed", "failed", .{});
    retry.deinit();
}
