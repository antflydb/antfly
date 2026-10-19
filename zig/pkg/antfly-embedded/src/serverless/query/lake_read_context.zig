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

pub const Context = struct {
    io: ?std.Io = null,
    deadline_ns: ?u64 = null,
    cancellation: ?storage.CancellationToken = null,
    checkpoint: ?struct { ptr: *anyopaque, check: *const fn (*anyopaque) anyerror!void } = null,
    additional_checkpoint: ?struct { ptr: *anyopaque, check: *const fn (*anyopaque) anyerror!void } = null,
    /// Original read authority is independent of CPU ownership and may be
    /// polled while parked. Its callback must not acquire a runnable grant.
    authority: ?struct { ptr: *anyopaque, check: *const fn (*anyopaque) anyerror!void } = null,
    execution: ?*@import("../../common/runnable_scheduler.zig").Execution = null,
    wait: ?@import("../../sql/execution_wait.zig").Boundary = null,
    /// A synchronous physical object-provider operation. Encoding, decoding and
    /// authority validation stay outside this borrow. Failed CPU return discards
    /// owned provider output without reacquiring another execution grant.
    pub fn objectCall(self: Context, comptime operation: anytype, args: anytype, alloc: Allocator) anyerror!@typeInfo(@typeInfo(@TypeOf(operation)).@"fn".return_type.?).error_union.payload {
        try self.ensureActive();
        if (self.wait) |wait| try wait.begin(wait.ptr);
        const result = @call(.auto, operation, args);
        if (self.wait) |wait| wait.end(wait.ptr) catch |err| {
            if (result) |value| discardObjectResult(value, alloc) else |_| {}
            try self.checkLifetime();
            return err;
        };
        const value = result catch |err| {
            // Even absence/error results require current CPU and authority.
            // A missing WAL/pin object cannot bypass a resumed policy fence.
            try self.ensureActive();
            return err;
        };
        errdefer discardObjectResult(value, alloc);
        try self.ensureActive();
        return value;
    }
    fn discardObjectResult(value: anytype, alloc: Allocator) void {
        if (@TypeOf(value) != void) {
            var owned = value;
            owned.deinit(alloc);
        }
    }
    pub fn withExecution(self: Context, execution: *@import("../../common/runnable_scheduler.zig").Execution) Context {
        var child = self;
        child.execution = execution;
        child.wait = @import("../../sql/spill.zig").childWait(execution);
        child.checkpoint = .{ .ptr = execution, .check = executionCheckpoint };
        return child;
    }
    fn executionCheckpoint(raw: *anyopaque) !void {
        try @as(*@import("../../common/runnable_scheduler.zig").Execution, @ptrCast(@alignCast(raw))).checkpoint();
    }
    pub fn checkLifetime(self: Context) !void {
        if (self.cancellation) |token| try token.check();
        if (self.deadline_ns) |deadline| if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.DeadlineExceeded;
        if (self.execution) |execution| try execution.checkLifetime();
        if (self.authority) |hook| try hook.check(hook.ptr);
    }
    pub fn ensureActive(self: Context) !void {
        try self.checkLifetime();
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
        return .{ .allocator = alloc, .ptr = self, .vtable = &vtable };
    }
    const vtable: storage.ObjectStorage.VTable = .{ .deinit = deinit, .bucket_exists = bucketExists, .make_bucket = makeBucket, .put_object = putObject, .get_object = getObject, .get_object_attributes = attributes, .stat_object = statObject, .stat_object_with_options = statObjectWithOptions, .delete_object = deleteObject, .list_objects = listObjects };
    /// A new request owner replaces these lifetime wrappers; provider handles,
    /// credentials and their physical owner remain borrowed through joining.
    pub fn provider(base: storage.ObjectStorage) storage.ObjectStorage {
        var current = base;
        while (current.vtable == &vtable) current = from(current.ptr).base;
        return current;
    }
    const Cancellation = struct {
        store: *Store,
        parent: ?storage.CancellationToken,
        fn canceled(raw: *const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            self.store.context.checkLifetime() catch return true;
            if (self.parent) |parent| parent.check() catch return true;
            return false;
        }
        fn token(self: *const @This()) storage.CancellationToken {
            return .{ .ptr = self, .is_cancelled_fn = canceled };
        }
    };
    fn discard(value: anytype, alloc: Allocator) void {
        var owned = value;
        owned.deinit(alloc);
    }
    fn from(raw: *anyopaque) *Store {
        return @ptrCast(@alignCast(raw));
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
        const cancellation: Cancellation = .{ .store = self, .parent = options.cancellation };
        opts.cancellation = cancellation.token();
        const result = @import("../../sql/execution_wait.zig").run(self.context.wait, storage.ObjectStorage.bucketExistsWithOptions, .{ &self.base, bucket, opts }) catch |err| {
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
        const cancellation: Cancellation = .{ .store = self, .parent = options.cancellation };
        opts.cancellation = cancellation.token();
        try opts.cancellation.?.check();
        var base = self.base;
        base.allocator = alloc;
        var result = @import("../../sql/execution_wait.zig").runOwned(self.context.wait, storage.ObjectStorage.getObject, .{ &base, bucket, key, opts }, discard, .{alloc}) catch |err| {
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
        const cancellation: Cancellation = .{ .store = self, .parent = options.cancellation };
        opts.cancellation = cancellation.token();
        var base = self.base;
        base.allocator = alloc;
        var result = @import("../../sql/execution_wait.zig").runOwned(self.context.wait, storage.ObjectStorage.statObjectWithOptions, .{ &base, bucket, key, opts }, discard, .{alloc}) catch |err| {
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
        var result = try @import("../../sql/execution_wait.zig").runOwned(self.context.wait, storage.ObjectStorage.getObjectAttributes, .{ &base, bucket, key }, discard, .{alloc});
        errdefer result.deinit(alloc);
        try self.ensureActive();
        return result;
    }
    fn listObjects(raw: *anyopaque, alloc: Allocator, bucket: []const u8, options: storage.ListOptions) !storage.ListResult {
        const self = from(raw);
        try self.ensureActive();
        var opts = options;
        const cancellation: Cancellation = .{ .store = self, .parent = options.cancellation };
        opts.cancellation = cancellation.token();
        var base = self.base;
        base.allocator = alloc;
        var result = @import("../../sql/execution_wait.zig").runOwned(self.context.wait, storage.ObjectStorage.listObjects, .{ &base, bucket, opts }, discard, .{alloc}) catch |err| {
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

test "lake SQL owned object requests suspend metadata and preserve owned results on denied resume" {
    const Runnable = @import("../../common/runnable_scheduler.zig");
    const Operation = enum { get, stat, attributes, list, bucket };
    const Case = enum { success, resume_denied, canceled, deadline, caller_canceled };
    const Clock = struct {
        var now_ns: std.atomic.Value(u64) = .init(0);
        fn now() u64 {
            return now_ns.load(.acquire);
        }
    };
    const Fixture = struct {
        scheduler: *Runnable.Scheduler,
        execution: *Runnable.Execution,
        signal: *std.atomic.Value(bool),
        case: Case,
        ends: usize = 0,
        invalid: usize = 0,
        fn requested(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.execution.checkpoint();
            try self.execution.park(.resource_blocked);
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.ends += 1;
            const stats = self.scheduler.stats();
            if (stats.runnable_slots != 0 or stats.resource_blocked != 1) self.invalid += 1;
            var control: Runnable.Execution = .{};
            try control.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
            try control.checkpoint();
            control.finish();
            if (self.case == .canceled) self.signal.store(true, .release);
            if (self.case == .deadline) Clock.now_ns.store(10, .release);
            try self.execution.enter(.query);
            if (self.case == .resume_denied) return error.Forbidden;
        }
        fn perform(client: *storage.ObjectStorage, operation: Operation, caller: ?storage.CancellationToken) !void {
            const a = std.testing.allocator;
            switch (operation) {
                .get => {
                    var result = try client.getObject("bucket", "part", .{ .cancellation = caller });
                    defer result.deinit(a);
                    try std.testing.expectEqualStrings("data", result.body);
                },
                .stat => {
                    var result = try client.statObjectWithOptions("bucket", "part", .{ .cancellation = caller });
                    defer result.deinit(a);
                    try std.testing.expectEqual(@as(u64, 4), result.content_length);
                },
                .attributes => {
                    var result = try client.getObjectAttributes("bucket", "part");
                    defer result.deinit(a);
                    try std.testing.expectEqual(@as(u64, 4), result.content_length);
                },
                .list => {
                    var result = try client.listObjects("bucket", .{ .cancellation = caller });
                    defer result.deinit(a);
                    try std.testing.expectEqual(@as(usize, 1), result.entries.len);
                },
                .bucket => try std.testing.expect(try client.bucketExistsWithOptions("bucket", .{ .cancellation = caller })),
            }
        }
    };
    const a = std.testing.allocator;
    var memory = storage.MemoryObjectStorage.init(a);
    defer memory.deinit();
    var raw_client = memory.client();
    try raw_client.makeBucket("bucket");
    var written = try raw_client.putObject("bucket", "part", "data", .{});
    written.deinit(a);
    for (std.enums.values(Operation)) |operation| for (std.enums.values(Case)) |case| {
        if (operation == .attributes and case == .caller_canceled) continue;
        Clock.now_ns.store(0, .release);
        var scheduler = try Runnable.Scheduler.init(.{ .slots = 2 });
        var execution: Runnable.Execution = .{};
        defer execution.finish();
        var signal: std.atomic.Value(bool) = .init(false);
        try execution.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 10, .cancellation = .{ .ptr = &signal, .requested = Fixture.requested } }, .query);
        var fixture = Fixture{ .scheduler = &scheduler, .execution = &execution, .signal = &signal, .case = case };
        var context = (Context{}).withExecution(&execution);
        context.wait = .{ .ptr = &fixture, .begin = Fixture.begin, .end = Fixture.end };
        var store = Store{ .base = raw_client, .context = context };
        var client = store.client(a);
        var caller_signal: std.atomic.Value(bool) = .init(case == .caller_canceled);
        const result = Fixture.perform(&client, operation, storage.CancellationToken.fromAtomic(&caller_signal));
        switch (case) {
            .success => try result,
            .resume_denied => try std.testing.expectError(error.Forbidden, result),
            .canceled, .caller_canceled => try std.testing.expectError(error.Canceled, result),
            .deadline => try std.testing.expectError(error.DeadlineExceeded, result),
        }
        try std.testing.expectEqual(@as(usize, if (case == .caller_canceled) 0 else 1), fixture.ends);
        try std.testing.expectEqual(@as(usize, 0), fixture.invalid);
    };
}

test "lake SQL borrowed lifetime polling ignores another worker IO cancellation and preserves original causes" {
    const Runnable = @import("../../common/runnable_scheduler.zig");
    const Fixture = struct {
        var io_canceled: bool = false;
        var io_checks: usize = 0;
        var now_ns: u64 = 0;
        fn checkIo(_: ?*anyopaque) std.Io.Cancelable!void {
            io_checks += 1;
            if (io_canceled) return error.Canceled;
        }
        fn now() u64 {
            return now_ns;
        }
        fn requested(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
    };
    Fixture.io_canceled = false;
    Fixture.io_checks = 0;
    Fixture.now_ns = 0;
    var vtable = std.testing.io.vtable.*;
    vtable.checkCancel = Fixture.checkIo;
    const io: std.Io = .{ .userdata = std.testing.io.userdata, .vtable = &vtable };
    var scheduler = try Runnable.Scheduler.init(.{ .slots = 2 });
    var execution: Runnable.Execution = .{};
    defer execution.finish();
    var signal: std.atomic.Value(bool) = .init(false);
    try execution.start(&scheduler, .{ .io = io, .native_now_ns = Fixture.now, .deadline_ns = 10, .cancellation = .{ .ptr = &signal, .requested = Fixture.requested } }, .query);
    try execution.park(.resource_blocked);
    const context = (Context{}).withExecution(&execution);
    const checks = Fixture.io_checks;
    Fixture.io_canceled = true;
    defer Fixture.io_canceled = false;
    try context.checkLifetime();
    try std.testing.expectEqual(checks, Fixture.io_checks);
    try std.testing.expectEqual(Runnable.State.resource_blocked, scheduler.state(&execution.entry.?));
    signal.store(true, .release);
    try std.testing.expectError(error.Canceled, context.checkLifetime());
    signal.store(false, .release);
    Fixture.now_ns = 10;
    try std.testing.expectError(error.DeadlineExceeded, context.checkLifetime());
    Fixture.now_ns = 0;
    Fixture.io_canceled = false;
    try execution.enter(.query);
    try execution.checkpoint();
    try execution.park(.resource_blocked);
    scheduler.close();
    try std.testing.expectError(error.SchedulerClosed, context.checkLifetime());
}

test "lake SQL lifetime authority survives execution binding without borrowing original CPU" {
    const Runnable = @import("../../common/runnable_scheduler.zig");
    const Fixture = struct {
        revoked: bool = false,
        checks: usize = 0,
        borrowed: usize = 0,
        fn authority(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.revoked) return error.LakeIndexReaderLeaseExpired;
        }
        fn parentCPU(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.borrowed += 1;
            return error.BorrowedParentCpu;
        }
    };
    var scheduler = try Runnable.Scheduler.init(.{ .slots = 2 });
    var execution: Runnable.Execution = .{};
    try execution.start(&scheduler, .{ .io = std.testing.io }, .query);
    defer execution.finish();
    var fixture: Fixture = .{};
    const original: Context = .{ .checkpoint = .{ .ptr = &fixture, .check = Fixture.parentCPU }, .authority = .{ .ptr = &fixture, .check = Fixture.authority } };
    const active = original.withExecution(&execution);
    try active.ensureActive();
    try execution.park(.resource_blocked);
    try active.checkLifetime();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
    fixture.revoked = true;
    try std.testing.expectError(error.LakeIndexReaderLeaseExpired, active.checkLifetime());
    try std.testing.expectError(error.LakeIndexReaderLeaseExpired, active.ensureActive());
    try std.testing.expectEqual(@as(usize, 0), fixture.borrowed);
    try std.testing.expectEqual(@as(usize, 4), fixture.checks);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
}
