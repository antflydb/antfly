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

//! A serving request's deadline and cancellation reach object providers through
//! one owner. No inventory or object request may refresh outside that context.
const std = @import("std");
const storage = @import("../../storage/object_storage.zig");
const Allocator = std.mem.Allocator;

/// Reservations include failures and concurrent workers. Retrying cannot turn
/// a selective query into an unbounded speculative scan.
pub const ReadAheadBudget = struct {
    used: std.atomic.Value(u64) = .init(0),
    pub fn reserve(self: *ReadAheadBudget, bytes: u64) bool {
        const maximum = 8 * 1024 * 1024;
        var used = self.used.load(.monotonic);
        while (bytes <= maximum - used) {
            if (self.used.cmpxchgWeak(used, used + bytes, .monotonic, .monotonic)) |observed| {
                used = observed;
            } else return true;
        }
        return false;
    }
};

/// Request-owned, bounded diagnostics. Workers share atomics, never a request
/// arena or a callback retained by a cache. Elapsed I/O sums may overlap.
pub const IoTrace = struct {
    pub const Snapshot = struct {
        ram_hits: u64 = 0,
        disk_hits: u64 = 0,
        provider_reads: u64 = 0,
        provider_bytes: u64 = 0,
        provider_errors: u64 = 0,
        provider_nanoseconds: u64 = 0,
        peak_inflight: u64 = 0,
        opening_nanoseconds: u64 = 0,
        read_ahead_bytes: u64 = 0,
        source_provider_reads: u64 = 0,
        source_provider_bytes: u64 = 0,
        source_provider_nanoseconds: u64 = 0,
        source_provider_errors: u64 = 0,
        pub fn add(self: *Snapshot, other: Snapshot) void {
            inline for (@typeInfo(Snapshot).@"struct".field_names) |field| {
                if (comptime std.mem.eql(u8, field, "peak_inflight")) @field(self, field) = @max(@field(self, field), @field(other, field)) else @field(self, field) +|= @field(other, field);
            }
        }
        pub fn since(self: Snapshot, before: Snapshot) Snapshot {
            var result: Snapshot = .{};
            inline for (@typeInfo(Snapshot).@"struct".field_names) |field| @field(result, field) = if (comptime std.mem.eql(u8, field, "peak_inflight")) @field(self, field) else @field(self, field) -| @field(before, field);
            return result;
        }
    };
    const fields = @typeInfo(Snapshot).@"struct".field_names.len;
    values: [fields]std.atomic.Value(u64) = @splat(.init(0)),
    active: std.atomic.Value(u64) = .init(0),
    read_ahead_budget: ReadAheadBudget = .{},
    /// Composed leaves/refills share their outer request's one allowance.
    read_ahead_shared: ?*ReadAheadBudget = null,
    pub fn reserveReadAhead(self: *IoTrace, bytes: u64) bool {
        const budget = self.read_ahead_shared orelse &self.read_ahead_budget;
        if (!budget.reserve(bytes)) return false;
        self.add("read_ahead_bytes", bytes);
        return true;
    }
    pub fn add(self: *IoTrace, comptime field: []const u8, value: u64) void {
        inline for (@typeInfo(Snapshot).@"struct".field_names, 0..) |name, i| if (comptime std.mem.eql(u8, field, name)) {
            _ = self.values[i].fetchAdd(value, .monotonic);
            return;
        };
        @compileError("unknown I/O trace counter");
    }
    pub fn snapshot(self: *const IoTrace) Snapshot {
        var result: Snapshot = .{};
        inline for (@typeInfo(Snapshot).@"struct".field_names, 0..) |field, i| @field(result, field) = self.values[i].load(.monotonic);
        return result;
    }
    pub fn begin(self: *IoTrace) u64 {
        const active = self.active.fetchAdd(1, .monotonic) + 1;
        inline for (@typeInfo(Snapshot).@"struct".field_names, 0..) |field, i| if (comptime std.mem.eql(u8, field, "peak_inflight")) {
            _ = self.values[i].fetchMax(active, .monotonic);
        };
        return @import("antfly_platform").time.monotonicNs();
    }
    pub fn end(self: *IoTrace, start: u64, requests: u64, bytes: u64, failed: bool) void {
        _ = self.active.fetchSub(1, .monotonic);
        self.add("provider_reads", requests);
        self.add("provider_bytes", bytes);
        self.add("provider_errors", @intFromBool(failed));
        self.add("provider_nanoseconds", @import("antfly_platform").time.monotonicNs() -| start);
    }
};

pub const Context = struct {
    io: ?std.Io = null,
    /// The owner has synchronized the construction allocator of every client
    /// used in this scope. Providers must separately advertise concurrency.
    parallel_object_io: bool = false,
    trace: ?*IoTrace = null,
    deadline_ns: ?u64 = null,
    cancellation: ?storage.CancellationToken = null,
    checkpoint: ?struct { ptr: *anyopaque, check: *const fn (*anyopaque) anyerror!void } = null,
    additional_checkpoint: ?struct { ptr: *anyopaque, check: *const fn (*anyopaque) anyerror!void } = null,
    pub fn ensureActive(self: Context) !void {
        // The scoped deadline remains precise when an in-flight transport
        // adapter reports that same expiration through a boolean callback.
        if (self.deadline_ns) |deadline| if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.DeadlineExceeded;
        if (self.cancellation) |token| try token.check();
        if (self.checkpoint) |hook| try hook.check(hook.ptr);
        if (self.additional_checkpoint) |hook| try hook.check(hook.ptr);
    }
};

pub const Store = struct {
    base: storage.ObjectStorage,
    context: Context,
    extra_checkpoint: ?struct { ptr: *anyopaque, check: *const fn (*anyopaque) anyerror!void } = null,
    pub fn ensureActive(self: *const Store) !void {
        try self.context.ensureActive();
        if (self.extra_checkpoint) |hook| try hook.check(hook.ptr);
    }
    pub fn client(self: *Store, alloc: Allocator) storage.ObjectStorage {
        return .{ .allocator = alloc, .ptr = self, .vtable = &.{ .deinit = deinit, .bucket_exists = bucketExists, .make_bucket = makeBucket, .put_object = putObject, .get_object = getObject, .get_object_attributes = attributes, .stat_object = statObject, .stat_object_with_options = statObjectWithOptions, .delete_object = deleteObject, .list_objects = listObjects } };
    }
    fn from(raw: *anyopaque) *Store {
        return @ptrCast(@alignCast(raw));
    }
    fn canceled(raw: *const anyopaque) bool {
        const self: *const Store = @ptrCast(@alignCast(raw));
        self.ensureActive() catch return true;
        return false;
    }
    fn token(self: *Store) storage.CancellationToken {
        return .{ .ptr = self, .is_cancelled_fn = canceled };
    }
    fn deinit(_: Allocator, _: *anyopaque) void {}
    fn makeBucket(_: *anyopaque, _: []const u8, _: storage.BucketOptions) !void {
        return error.ExternalLakeReadOnly;
    }
    fn putObject(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: []const u8, _: storage.PutOptions) !storage.PutResult {
        return error.ExternalLakeReadOnly;
    }
    fn deleteObject(_: *anyopaque, _: []const u8, _: []const u8, _: storage.DeleteOptions) !void {
        return error.ExternalLakeReadOnly;
    }
    fn bucketExists(raw: *anyopaque, bucket: []const u8, options: storage.BucketOptions) !bool {
        const self = from(raw);
        try self.ensureActive();
        var opts = options;
        opts.cancellation = self.token();
        const result = self.base.bucketExistsWithOptions(bucket, opts) catch |err| {
            try self.ensureActive();
            return err;
        };
        try self.ensureActive();
        return result;
    }
    fn getObject(raw: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8, options: storage.GetOptions) !storage.GetResult {
        const self = from(raw);
        try self.ensureActive();
        var opts = options;
        const Combined = struct {
            store: *Store,
            parent: ?storage.CancellationToken,
            fn canceled(raw_token: *const anyopaque) bool {
                const value: *const @This() = @ptrCast(@alignCast(raw_token));
                value.store.ensureActive() catch return true;
                if (value.parent) |parent| parent.check() catch return true;
                return false;
            }
        };
        const combined: Combined = .{ .store = self, .parent = options.cancellation };
        opts.cancellation = .{ .ptr = &combined, .is_cancelled_fn = Combined.canceled };
        try opts.cancellation.?.check();
        var base = self.base;
        base.allocator = alloc;
        var result = base.getObject(bucket, key, opts) catch |err| {
            try self.ensureActive();
            return err;
        };
        errdefer result.deinit(alloc);
        try self.ensureActive();
        return result;
    }
    fn statObject(raw: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8) !storage.ObjectMetadata {
        return statObjectWithOptions(raw, alloc, bucket, key, .{});
    }
    fn statObjectWithOptions(raw: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8, options: @import("objectstore").StatOptions) !storage.ObjectMetadata {
        const self = from(raw);
        try self.ensureActive();
        var opts = options;
        opts.cancellation = self.token();
        var base = self.base;
        base.allocator = alloc;
        var result = base.statObjectWithOptions(bucket, key, opts) catch |err| {
            try self.ensureActive();
            return err;
        };
        errdefer result.deinit(alloc);
        try self.ensureActive();
        return result;
    }
    fn attributes(raw: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8) !storage.ObjectAttributes {
        const self = from(raw);
        try self.ensureActive();
        var base = self.base;
        base.allocator = alloc;
        var result = try base.getObjectAttributes(bucket, key);
        errdefer result.deinit(alloc);
        try self.ensureActive();
        return result;
    }
    fn listObjects(raw: *anyopaque, alloc: Allocator, bucket: []const u8, options: storage.ListOptions) !storage.ListResult {
        const self = from(raw);
        try self.ensureActive();
        var opts = options;
        opts.cancellation = self.token();
        var base = self.base;
        base.allocator = alloc;
        var result = base.listObjects(bucket, opts) catch |err| {
            try self.ensureActive();
            return err;
        };
        errdefer result.deinit(alloc);
        try self.ensureActive();
        return result;
    }
};

test "external lake object requests reject cancellation and expired deadlines before I/O" {
    const alloc = std.testing.allocator;
    var memory = storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var signal = std.atomic.Value(bool).init(true);
    var store: Store = .{ .base = memory.client(), .context = .{ .cancellation = storage.CancellationToken.fromAtomic(&signal) } };
    var client = store.client(alloc);
    try std.testing.expectError(error.Canceled, client.getObject("bucket", "part", .{}));
    try std.testing.expectError(error.Canceled, client.listObjects("bucket", .{}));
    signal.store(false, .release);
    store.context.deadline_ns = 0;
    try std.testing.expectError(error.DeadlineExceeded, client.statObject("bucket", "part"));
}

test "external lake bounded window profiles deadline identity survives flattened transport cancellation" {
    const Bool = struct {
        fn canceled(_: *const anyopaque) bool {
            return true;
        }
    };
    var state: u8 = 0;
    const expired: Context = .{ .deadline_ns = 1, .cancellation = storage.CancellationToken.fromCallback(&state, Bool.canceled) };
    try std.testing.expectError(error.DeadlineExceeded, expired.ensureActive());
    const canceled_context: Context = .{ .cancellation = expired.cancellation };
    try std.testing.expectError(error.Canceled, canceled_context.ensureActive());
}

test "external lake opening I/O traces separate request traffic and overlapping reads" {
    var first: IoTrace = .{};
    var second: IoTrace = .{};
    const left = first.begin();
    const right = first.begin();
    first.end(left, 1, 12, false);
    first.end(right, 1, 0, true);
    first.add("disk_hits", 1);
    const before = first.snapshot();
    try std.testing.expectEqual(@as(u64, 2), before.peak_inflight);
    try std.testing.expectEqual(@as(u64, 2), before.provider_reads);
    try std.testing.expectEqual(@as(u64, 1), before.provider_errors);
    try std.testing.expectEqual(@as(u64, 12), before.provider_bytes);
    try std.testing.expectEqual(@as(u64, 0), second.snapshot().provider_reads);
    first.add("ram_hits", 1);
    const delta = first.snapshot().since(before);
    try std.testing.expectEqual(@as(u64, 1), delta.ram_hits);
    try std.testing.expectEqual(@as(u64, 0), delta.provider_reads);
    var merged: IoTrace.Snapshot = .{};
    merged.add(before);
    merged.add(delta);
    try std.testing.expectEqual(@as(u64, 2), merged.peak_inflight);
}

test "external lake read ahead reservations are bounded across retries and readers" {
    var trace: IoTrace = .{};
    for (0..8) |_| try std.testing.expect(trace.reserveReadAhead(1024 * 1024));
    try std.testing.expect(!trace.reserveReadAhead(1));
    try std.testing.expect(!trace.reserveReadAhead(std.math.maxInt(u64)));
    try std.testing.expectEqual(@as(u64, 8 * 1024 * 1024), trace.snapshot().read_ahead_bytes);
    var fresh: IoTrace = .{};
    try std.testing.expect(fresh.reserveReadAhead(1));
    var shared: ReadAheadBudget = .{};
    {
        trace.read_ahead_shared = &shared;
        fresh.read_ahead_shared = &shared;
        try std.testing.expect(trace.reserveReadAhead(7 * 1024 * 1024));
        try std.testing.expect(fresh.reserveReadAhead(1024 * 1024));
        try std.testing.expect(!trace.reserveReadAhead(1));
        try std.testing.expect(!fresh.reserveReadAhead(1));
    }
}
