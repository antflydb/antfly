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
const d = @import("distributed.zig");
const b = @import("backend.zig");
const v = @import("value.zig");
const A = std.mem.Allocator;
const atomic = std.atomic.Value(usize);
const Activity = struct {
    io: std.Io,
    active: atomic = .init(0),
    peak: atomic = .init(0),
    fn enter(self: *@This()) !void {
        const n = self.active.fetchAdd(1, .monotonic) + 1;
        _ = self.peak.fetchMax(n, .monotonic);
        try self.io.sleep(.fromMilliseconds(3), .awake);
    }
    fn leave(self: *@This()) void {
        _ = self.active.fetchSub(1, .monotonic);
    }
};
test "parallel acquisition preserves cohort barriers and cleans failed busy and revoked waves" {
    const Fixture = struct {
        activity: Activity,
        controller: std.Thread.Id,
        mode: enum { normal, busy, capture_error, fork_error, revoked, deadline },
        owners: [12]Owner = undefined,
        admitted: atomic = .init(0),
        held: atomic = .init(0),
        forks: atomic = .init(0),
        closed: atomic = .init(0),
        busy: std.atomic.Value(bool) = .init(true),
        revoked: std.atomic.Value(bool) = .init(false),
        expired: std.atomic.Value(bool) = .init(false),
        const Owner = struct {
            f: *ThisFixture,
            index: usize,
            allocator: A = undefined,
            token: []u8 = undefined,
            fn checkThread(self: *@This()) void {
                if (self.index % 2 == 0) std.testing.expectEqual(self.f.controller, std.Thread.getCurrentId()) catch @panic("local capability moved across threads");
            }
            fn release(ptr: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.checkThread();
                _ = self.f.held.fetchSub(1, .monotonic);
                self.allocator.free(self.token);
            }
            fn check(_: *anyopaque) !void {}
            fn close(ptr: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.checkThread();
                _ = self.f.closed.fetchAdd(1, .monotonic);
            }
            fn fork(ptr: *anyopaque, _: A) !d.Snapshot {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                const f = self.f;
                self.checkThread();
                try f.activity.enter();
                defer f.activity.leave();
                try std.testing.expectEqual(@as(usize, 12), f.held.load(.monotonic));
                try std.testing.expectEqualStrings("retained", self.token);
                if (self.index == 3) switch (f.mode) {
                    .fork_error => return error.Forbidden,
                    .revoked => {
                        f.revoked.store(true, .release);
                        return error.Forbidden;
                    },
                    else => {},
                };
                _ = f.forks.fetchAdd(1, .monotonic);
                return .{ .ptr = self, .validate = @This().check, .close = @This().close, .backend = .{ .ptr = self, .vtable = &.{ .bind = undefined, .checkpoint = @This().check, .scan = undefined, .get = undefined, .expand = undefined } } };
            }
        };
        const ThisFixture = @This();
        fn check(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.revoked.load(.acquire)) return error.Forbidden;
            if (self.expired.load(.acquire)) return error.DeadlineExceeded;
        }
        fn admit(ptr: *anyopaque, target: d.Target) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.activity.enter();
            defer self.activity.leave();
            if (self.mode == .deadline and target.group_id == 3) self.expired.store(true, .release);
            _ = self.admitted.fetchAdd(1, .monotonic);
        }
        fn capture(ptr: *anyopaque, a: A, target: d.Target) !?d.Fence {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.activity.enter();
            defer self.activity.leave();
            try std.testing.expectEqual(@as(usize, 12), self.admitted.load(.monotonic));
            if (target.group_id == 3) {
                if (self.mode == .capture_error) return error.Forbidden;
                if (self.mode == .busy and self.busy.swap(false, .acq_rel)) return null;
            }
            const owner = &self.owners[target.group_id - 1];
            owner.checkThread();
            owner.allocator = a;
            owner.token = try a.dupe(u8, "retained");
            _ = self.held.fetchAdd(1, .monotonic);
            return .{ .ptr = owner, .validate = Owner.check, .fork = Owner.fork, .release = Owner.release };
        }
    };
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .async_limit = .limited(4) });
    defer threaded.deinit();
    const io = threaded.io();
    var shared: b.FanoutAllocator = .{ .backing = std.testing.allocator, .io = io };
    var targets: [12]d.Target = undefined;
    for (&targets, 0..) |*target, i| target.* = .{ .parallel = i % 2 == 1, .table = "docs", .table_id = 1, .group_id = i + 1 };
    for (std.enums.values(@FieldType(Fixture, "mode"))) |mode| {
        var f: Fixture = .{ .activity = .{ .io = io }, .mode = mode, .controller = std.Thread.getCurrentId() };
        for (&f.owners, 0..) |*owner, i| owner.* = .{ .f = &f, .index = i };
        const source: d.Source = .{ .ptr = &f, .checkpoint = Fixture.check, .admit = Fixture.admit, .capture = Fixture.capture, .fanout_io = io, .fanout_width = 4 };
        if (mode == .normal or mode == .busy) {
            var set = try d.ReadSet.acquire(shared.allocator(), source, &targets);
            try std.testing.expectEqual(@as(usize, 0), f.held.load(.monotonic));
            set.deinit();
        } else if (mode == .deadline) {
            try std.testing.expectError(error.DeadlineExceeded, d.ReadSet.acquire(shared.allocator(), source, &targets));
        } else try std.testing.expectError(error.Forbidden, d.ReadSet.acquire(shared.allocator(), source, &targets));
        try std.testing.expectEqual(f.forks.load(.monotonic), f.closed.load(.monotonic));
        try std.testing.expectEqual(@as(usize, 0), f.held.load(.monotonic));
        try std.testing.expectEqual(@as(usize, 0), f.activity.active.load(.monotonic));
        try std.testing.expect(f.activity.peak.load(.monotonic) > 1);
        try std.testing.expect(f.activity.peak.load(.monotonic) <= 4);
    }
}

test "parallel search redistributes quotas charges retries and bounds retained memory" {
    const Fixture = struct {
        activity: Activity,
        owners: [128]Owner = undefined,
        mode: enum { normal, anchors, work, fatal, revoked, memory, serial },
        revoked: std.atomic.Value(bool) = .init(false),
        payload: [4096]u8 = @splat('x'),
        const ThisFixture = @This();
        const Owner = struct {
            f: *ThisFixture,
            index: usize,
            calls: usize = 0,
            work: ?b.Work = null,
            fn check(ptr: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                try ThisFixture.check(self.f);
            }
            fn configure(ptr: *anyopaque, work: ?b.Work) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.work = work;
            }
            fn search(ptr: *anyopaque, a: A, _: []const u8, input: b.SearchInput) !b.SearchPage {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.calls += 1;
                if (input.usage) |usage| usage.examined = 0;
                try self.f.activity.enter();
                defer self.f.activity.leave();
                if (self.f.mode == .revoked and self.index == 0) self.f.revoked.store(true, .release);
                if (self.f.mode == .fatal and self.index == 3) return error.Forbidden;
                const anchors: usize = if (self.index == 0 and self.f.mode == .anchors) 20 else 1;
                for (0..anchors) |i| {
                    if (i == input.max_examined) return error.CypherAnchorBudgetExceeded;
                    if (input.usage) |usage| usage.examined = i + 1;
                }
                const units: usize = if (self.index == 0 and (self.f.mode == .work or self.f.mode == .serial)) 2000 else 1;
                for (0..units) |_| if (self.work) |work| try work.charge(work.ptr, 1);
                const node = try a.create(v.Node);
                const properties = try a.alloc(v.Entry, 1);
                properties[0] = .{ .name = "payload", .value = .{ .string = try a.dupe(u8, &self.f.payload) } };
                node.* = .{ .identity = .{ .table = "docs", .key = try std.fmt.allocPrint(a, "n{d}", .{self.index}) }, .properties = properties };
                const seeds = try a.alloc(b.Seed, 1);
                seeds[0] = .{ .node = node, .score = @floatFromInt(self.index) };
                return .{ .seeds = seeds, .examined = anchors };
            }
            fn backend(self: *@This()) b.Backend {
                return .{ .ptr = self, .vtable = &.{ .bind = undefined, .checkpoint = @This().check, .scan = undefined, .get = undefined, .expand = undefined, .search = search, .set_work = configure } };
            }
        };
        fn check(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.revoked.load(.acquire)) return error.Forbidden;
        }
    };
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .async_limit = .limited(4) });
    defer threaded.deinit();
    const io = threaded.io();
    for (std.enums.values(@FieldType(Fixture, "mode"))) |mode| {
        var f: Fixture = .{ .activity = .{ .io = io }, .mode = mode };
        const count: usize = if (mode == .normal) 128 else 8;
        var targets: [128]d.Target = undefined;
        var snapshots: [128]d.Snapshot = undefined;
        for (f.owners[0..count], targets[0..count], snapshots[0..count], 0..) |*owner, *target, *snapshot, i| {
            owner.* = .{ .f = &f, .index = i };
            target.* = .{ .table = "docs", .table_id = 1, .group_id = i + 1 };
            snapshot.* = .{ .backend = owner.backend(), .ptr = owner, .validate = Fixture.Owner.check, .close = undefined };
        }
        var read: d.ReadSet = .{ .a = std.testing.allocator, .targets = targets[0..count], .snapshots = snapshots[0..count], .independent_validations = &.{}, .source = .{ .ptr = &f, .checkpoint = Fixture.check, .admit = undefined, .capture = undefined, .fanout_io = if (mode == .serial) null else io, .fanout_width = 4 } };
        var memory: @import("budget.zig") = .{ .backing = std.testing.allocator, .limit = if (mode == .memory) 16 << 10 else 512 << 10 };
        var arena = std.heap.ArenaAllocator.init(memory.allocator());
        defer arena.deinit();
        var meter: b.Meter = .{ .a = arena.allocator(), .backend = read.backend(), .limit = if (mode == .normal) 100_000 else if (mode == .serial) 1000 else 8192 };
        const backend = read.backend();
        backend.vtable.set_work.?(backend.ptr, meter.work());
        const parameters = v.Value{ .map = &.{.{ .name = "limit", .value = .{ .integer = 2 } }} };
        var usage: b.SearchUsage = .{};
        const input: b.SearchInput = .{ .usage = &usage, .parameters = parameters, .max_examined = if (mode == .anchors) 64 else 128 };
        if (mode == .serial) {
            try std.testing.expectError(error.CypherStateBudgetExceeded, backend.vtable.search.?(backend.ptr, arena.allocator(), "docs", input));
            try std.testing.expectEqual(@as(?usize, 1), usage.examined);
        } else if (mode == .memory) {
            try std.testing.expectError(error.CypherMemoryBudgetExceeded, backend.vtable.search.?(backend.ptr, arena.allocator(), "docs", input));
        } else if (mode == .fatal or mode == .revoked) {
            try std.testing.expectError(error.Forbidden, backend.vtable.search.?(backend.ptr, arena.allocator(), "docs", input));
        } else {
            const page = try backend.vtable.search.?(backend.ptr, arena.allocator(), "docs", input);
            try std.testing.expectEqual(@as(usize, 2), page.seeds.len);
            try std.testing.expectEqual(@as(f64, @floatFromInt(count - 1)), page.seeds[0].score);
            try std.testing.expectEqual(@as(usize, 4096), page.seeds[0].node.properties[0].value.string.len);
            if (mode == .anchors or mode == .work) try std.testing.expectEqual(@as(usize, 2), f.owners[0].calls);
            if (mode == .anchors) try std.testing.expectEqual(@as(usize, 43), page.examined);
            if (mode == .work) try std.testing.expect(meter.units > 3638);
        }
        try std.testing.expect(meter.units <= meter.limit);
        try std.testing.expect(memory.peak < 256 << 10);
        try std.testing.expectEqual(@as(usize, 0), f.activity.active.load(.monotonic));
        try std.testing.expect(f.activity.peak.load(.monotonic) <= 4);
        if (mode != .serial) try std.testing.expect(f.activity.peak.load(.monotonic) > 1);
    }
}

test "native group cancellation drains started workers and cleanup still releases every wave" {
    const Runtime = struct {
        backing: std.Io,
        vtable: std.Io.VTable = undefined,
        awaits: usize = 0,
        calls: atomic = .init(0),
        fn schedule(ptr: ?*anyopaque, group: *std.Io.Group, context: []const u8, alignment: std.mem.Alignment, start: *const fn (*const anyopaque) void) void {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.backing.vtable.groupAsync(self.backing.userdata, group, context, alignment, start);
        }
        fn awaitGroup(ptr: ?*anyopaque, group: *std.Io.Group, token: *anyopaque) std.Io.Cancelable!void {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            try self.backing.vtable.groupAwait(self.backing.userdata, group, token);
            self.awaits += 1;
            return error.Canceled;
        }
        fn worker(self: *@This(), _: usize) void {
            self.backing.sleep(.fromMilliseconds(3), .awake) catch {};
            _ = self.calls.fetchAdd(1, .monotonic);
        }
    };
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .async_limit = .limited(4) });
    defer threaded.deinit();
    var runtime: Runtime = .{ .backing = threaded.io() };
    runtime.vtable = runtime.backing.vtable.*;
    runtime.vtable.groupAsync = Runtime.schedule;
    runtime.vtable.groupAwait = Runtime.awaitGroup;
    const io: std.Io = .{ .userdata = &runtime, .vtable = &runtime.vtable };
    try std.testing.expect(@import("fanout.zig").run(io, 2, 8, &runtime, Runtime.worker));
    try std.testing.expectEqual(@as(usize, 2), runtime.calls.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 1), runtime.awaits);
    runtime.calls.store(0, .monotonic);
    runtime.awaits = 0;
    @import("fanout.zig").finish(io, 2, 8, &runtime, Runtime.worker);
    try std.testing.expectEqual(@as(usize, 8), runtime.calls.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 4), runtime.awaits);
}
