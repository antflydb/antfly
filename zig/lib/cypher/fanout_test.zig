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
        mode: enum { normal, anchors, work, fatal, revoked, memory, serial, sequential, local },
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
        const count: usize = if (mode == .normal or mode == .sequential or mode == .local) 128 else 8;
        var targets: [128]d.Target = undefined;
        var snapshots: [128]d.Snapshot = undefined;
        for (f.owners[0..count], targets[0..count], snapshots[0..count], 0..) |*owner, *target, *snapshot, i| {
            owner.* = .{ .f = &f, .index = i };
            target.* = .{ .parallel = mode != .local, .table = "docs", .table_id = 1, .group_id = i + 1 };
            snapshot.* = .{ .backend = owner.backend(), .ptr = owner, .validate = Fixture.Owner.check, .close = undefined };
        }
        var read: d.ReadSet = .{ .a = std.testing.allocator, .targets = targets[0..count], .snapshots = snapshots[0..count], .independent_validations = &.{}, .source = .{ .ptr = &f, .checkpoint = Fixture.check, .admit = undefined, .capture = undefined, .fanout_io = if (mode == .serial or mode == .sequential) null else io, .fanout_width = 4 } };
        var memory: @import("budget.zig") = .{ .backing = std.testing.allocator, .limit = if (mode == .memory) 16 << 10 else 512 << 10 };
        var arena = std.heap.ArenaAllocator.init(memory.allocator());
        defer arena.deinit();
        var meter: b.Meter = .{ .a = arena.allocator(), .backend = read.backend(), .limit = if (mode == .normal or mode == .sequential or mode == .local) 100_000 else if (mode == .serial) 1000 else 8192 };
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
        if (mode == .serial or mode == .sequential or mode == .local) try std.testing.expectEqual(@as(usize, 1), f.activity.peak.load(.monotonic)) else try std.testing.expect(f.activity.peak.load(.monotonic) > 1);
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

test "bounded owner reads preserve ordering routing quotas and controller affinity" {
    const Fixture = struct {
        activity: Activity,
        controller: std.Thread.Id,
        mode: enum { normal, work, anchors, fatal, revoked, deadline, canceled, malformed, overlap, decline, empty, memory },
        owners: [12]Owner = undefined,
        stop: std.atomic.Value(bool) = .init(false),
        const ThisFixture = @This();
        const Owner = struct {
            f: *ThisFixture,
            index: usize,
            key: [8]u8 = undefined,
            work: ?b.Work = null,
            lookups: usize = 0,
            heads: usize = 0,
            fn checkThread(self: *@This()) void {
                if (self.index % 2 == 0) std.testing.expectEqual(self.f.controller, std.Thread.getCurrentId()) catch @panic("local read moved across threads");
            }
            fn check(ptr: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.checkThread();
                try ThisFixture.check(self.f);
            }
            fn configure(ptr: *anyopaque, work: ?b.Work) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.checkThread();
                self.work = work;
            }
            fn enter(self: *@This()) !void {
                self.checkThread();
                try self.f.activity.enter();
            }
            fn demand(self: *@This()) !void {
                if (self.index == 1) switch (self.f.mode) {
                    .fatal => return error.Forbidden,
                    .revoked, .deadline, .canceled => self.f.stop.store(true, .release),
                    .memory => {},
                    else => {},
                };
                const units: usize = if (self.index == 1 and self.f.mode == .work) 5000 else 1;
                for (0..units) |_| if (self.work) |work| try work.charge(work.ptr, 1);
            }
            fn node(self: *@This(), a: A, key: []const u8, score: usize) !*const v.Node {
                if (self.index == 1 and self.f.mode == .memory) _ = try a.alloc(u8, 1 << 20);
                const out = try a.create(v.Node);
                const fields = try a.alloc(v.Entry, 1);
                fields[0] = .{ .name = "score", .value = .{ .integer = @intCast(score) } };
                out.* = .{ .identity = .{ .table = if (self.f.mode == .malformed) "wrong" else "docs", .key = try a.dupe(u8, key) }, .properties = fields };
                return out;
            }
            fn getMany(ptr: *anyopaque, a: A, ids: []const v.Identity) ![]const ?*const v.Node {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.lookups += 1;
                try self.enter();
                defer self.f.activity.leave();
                try self.demand();
                const out = try a.alloc(?*const v.Node, ids.len);
                @memset(out, null);
                const key = if (self.f.mode == .overlap) "k00" else self.key[0..3];
                for (ids, out) |id, *item| if (std.mem.eql(u8, id.key, key)) {
                    item.* = try self.node(a, key, self.index);
                };
                return out;
            }
            fn ordered(ptr: *anyopaque, a: A, _: []const u8, _: []const b.PropertyFilter, _: []const b.PropertyOrder, after: ?[]const u8, limit: usize, maximum: usize) !?b.Page(*const v.Node) {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                try self.enter();
                defer self.f.activity.leave();
                if (limit == 0) {
                    if (self.f.mode == .decline and self.index == 1) return null;
                    return .{ .items = &.{}, .cursors = &.{}, .examined = 0, .next = "0", .exact_keys = true };
                }
                self.heads += 1;
                try self.demand();
                const start = if (after) |bytes| try std.fmt.parseInt(usize, bytes, 10) else 0;
                if (self.f.mode == .empty and self.index == 1 and start == 0) return .{ .items = &.{}, .examined = 1, .next = "1", .exact_keys = true };
                const examined: usize = if (self.f.mode == .anchors and self.index == 1 and start == 0) 1800 else 1;
                if (maximum < examined) return error.CypherAnchorBudgetExceeded;
                const items = try a.alloc(*const v.Node, 1);
                const key = try std.fmt.allocPrint(a, "k{d:0>2}-{d}", .{ self.index, start });
                items[0] = try self.node(a, key, self.index * 100 + start);
                const cursors = try a.alloc(?[]const u8, 1);
                cursors[0] = if (start == 0) "1" else null;
                return .{ .items = items, .cursors = cursors, .examined = examined, .next = cursors[0], .exact_keys = true };
            }
            fn backend(self: *@This()) b.Backend {
                return .{ .ptr = self, .vtable = &.{ .bind = undefined, .checkpoint = @This().check, .scan = undefined, .get = undefined, .expand = undefined, .get_many = getMany, .scan_ordered = ordered, .set_work = configure } };
            }
        };
        fn check(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.stop.load(.acquire)) return switch (self.mode) {
                .deadline => error.DeadlineExceeded,
                .canceled => error.Canceled,
                else => error.Forbidden,
            };
        }
    };
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .async_limit = .limited(4) });
    defer threaded.deinit();
    const io = threaded.io();
    for (std.enums.values(@FieldType(Fixture, "mode"))) |mode| for ([_]bool{ false, true }) |ordered| {
        if ((mode == .decline or mode == .anchors or mode == .empty) and !ordered) continue;
        if (mode == .overlap and ordered) continue;
        var f: Fixture = .{ .activity = .{ .io = io }, .controller = std.Thread.getCurrentId(), .mode = mode };
        var targets: [12]d.Target = undefined;
        var snapshots: [12]d.Snapshot = undefined;
        var ids: [12]v.Identity = undefined;
        var ranges: [12][1]d.KeyRange = undefined;
        for (&f.owners, &targets, &snapshots, &ids, 0..) |*owner, *target, *snapshot, *id, i| {
            owner.* = .{ .f = &f, .index = i };
            const key = try std.fmt.bufPrint(&owner.key, "k{d:0>2}", .{i});
            id.* = .{ .table = "docs", .key = key };
            target.* = .{ .parallel = i % 2 != 0, .table = "docs", .table_id = 1, .group_id = i + 1 };
            snapshot.* = .{ .backend = owner.backend(), .ptr = owner, .validate = Fixture.Owner.check, .close = undefined };
        }
        if (mode != .overlap) for (&targets, &ranges, 0..) |*target, *range, i| {
            range.* = .{.{ .start = ids[i].key, .end = if (i + 1 < ids.len) ids[i + 1].key else null }};
            target.ranges = range;
        };
        var read: d.ReadSet = .{ .a = std.testing.allocator, .targets = &targets, .snapshots = &snapshots, .independent_validations = &.{}, .source = .{ .ptr = &f, .checkpoint = Fixture.check, .admit = undefined, .capture = undefined, .fanout_io = io, .fanout_width = 4 } };
        var memory: @import("budget.zig") = .{ .backing = std.testing.allocator, .limit = 512 << 10 };
        var arena = std.heap.ArenaAllocator.init(memory.allocator());
        defer arena.deinit();
        const a = arena.allocator();
        const backend = read.backend();
        var meter: b.Meter = .{ .a = a, .backend = backend, .limit = 100_000 };
        // A work-skewed owner must fail a 1/5 reservation and succeed with the
        // unused whole-wave budget; copying and the failed attempt stay charged.
        if (mode == .work) meter.limit = 20_000;
        backend.vtable.set_work.?(backend.ptr, meter.work());
        const expected: ?anyerror = switch (mode) {
            .fatal, .revoked => error.Forbidden,
            .deadline => error.DeadlineExceeded,
            .canceled => error.Canceled,
            .malformed => error.CypherInvalidBackendPage,
            .overlap => error.CypherOverlappingOwners,
            .memory => error.CypherMemoryBudgetExceeded,
            else => null,
        };
        if (ordered) {
            const result = backend.vtable.scan_ordered.?(backend.ptr, a, "docs", &.{}, &.{.{ .property = "score" }}, null, 5, 4096);
            if (expected) |err| try std.testing.expectError(err, result) else {
                const page = try result;
                if (mode == .decline) try std.testing.expect(page == null) else {
                    try std.testing.expectEqual(@as(usize, 5), page.?.items.len);
                    try std.testing.expectEqual(@as(i128, 0), page.?.items[0].properties[0].value.integer);
                    try std.testing.expect(page.?.examined.? <= 4096);
                    if (mode == .anchors) try std.testing.expect(page.?.examined.? >= 2824);
                    if (mode == .normal) {
                        var after = page.?.next;
                        var emitted: usize = 5;
                        while (after) |cursor| {
                            const next = (try backend.vtable.scan_ordered.?(backend.ptr, a, "docs", &.{}, &.{.{ .property = "score" }}, cursor, 5, 4096)).?;
                            for (next.items) |node| {
                                try std.testing.expectEqual(@as(i128, @intCast(emitted / 2 * 100 + emitted % 2)), node.properties[0].value.integer);
                                emitted += 1;
                            }
                            after = next.next;
                        }
                        try std.testing.expectEqual(@as(usize, 24), emitted);
                    }
                }
            }
        } else {
            const result = backend.vtable.get_many.?(backend.ptr, a, &ids);
            if (expected) |err| try std.testing.expectError(err, result) else {
                for (try result, 0..) |node, i| try std.testing.expectEqual(@as(i128, @intCast(i)), node.?.properties[0].value.integer);
            }
        }
        try std.testing.expectEqual(@as(usize, 0), f.activity.active.load(.monotonic));
        try std.testing.expect(f.activity.peak.load(.monotonic) > 1);
        try std.testing.expect(f.activity.peak.load(.monotonic) <= 4);
        try std.testing.expect(meter.units <= meter.limit);
        if (mode == .work) try std.testing.expect(meter.units > 9000);
        for (f.owners) |owner| try std.testing.expectEqual(meter.work().ptr, owner.work.?.ptr);
    };
}

test "parallel probes join charge and preserve EOF encounter order under skew and declines" {
    const Fixture = struct {
        activity: Activity,
        controller: std.Thread.Id,
        mode: enum { normal, skew, empty, incomplete, unsupported, malformed, fatal, revoked, deadline, canceled, quota, work },
        stop: std.atomic.Value(bool) = .init(false),
        const This = @This();
        const Owner = struct {
            fixture: *This,
            index: usize,
            calls: usize = 0,
            work: ?b.Work = null,
            key: [8]u8 = undefined,
            fn check(ptr: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                if (self.index == 0) try std.testing.expectEqual(self.fixture.controller, std.Thread.getCurrentId());
                try This.check(self.fixture);
            }
            fn configure(ptr: *anyopaque, work: ?b.Work) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                if (self.index == 0) std.testing.expectEqual(self.fixture.controller, std.Thread.getCurrentId()) catch @panic("local probe moved");
                self.work = work;
            }
            fn probe(ptr: *anyopaque, a: A, _: []const u8, _: []const b.PropertyFilter, limit: usize) !?b.Probe {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.calls += 1;
                try @This().check(ptr);
                try self.fixture.activity.enter();
                defer self.fixture.activity.leave();
                if (self.index == 1) switch (self.fixture.mode) {
                    .fatal => return error.Forbidden,
                    .revoked, .deadline, .canceled => self.fixture.stop.store(true, .release),
                    else => {},
                };
                const population: usize = switch (self.fixture.mode) {
                    .empty => 0,
                    .skew => if (self.index == 1) 12 else 0,
                    .incomplete => if (self.index == 1) limit + 2 else 0,
                    else => 1,
                };
                const used = @min(population, limit + 1);
                if (self.work) |work| try work.charge(work.ptr, if (self.fixture.mode == .work and self.index == 1) 5000 else used + 1);
                if (self.fixture.mode == .unsupported and self.index == 1) return null;
                if (self.fixture.mode == .malformed and self.index == 1) return .{ .page = .{ .items = &.{}, .examined = limit + 2 }, .complete = true };
                if (used > limit) return .{ .page = .{ .items = &.{}, .examined = used }, .complete = false };
                const nodes = try a.alloc(*const v.Node, used);
                for (nodes, 0..) |*out, i| {
                    const node = try a.create(v.Node);
                    node.* = .{ .identity = .{ .table = "docs", .key = try std.fmt.allocPrint(a, "{s}-{d:0>2}", .{ self.key[0..3], i }) } };
                    if (self.fixture.mode == .quota) {
                        const bytes = try a.alloc(u8, 300_000);
                        @memset(bytes, 'x');
                        const properties = try a.alloc(v.Entry, 1);
                        properties[0] = .{ .name = "wide", .value = .{ .string = bytes } };
                        node.properties = properties;
                    }
                    out.* = node;
                }
                return .{ .page = .{ .items = nodes, .examined = used }, .complete = true };
            }
            fn backend(self: *@This()) b.Backend {
                return .{ .ptr = self, .vtable = &.{ .bind = undefined, .checkpoint = @This().check, .scan = undefined, .get = undefined, .expand = undefined, .probe_where = probe, .set_work = configure } };
            }
        };
        fn check(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.stop.load(.acquire)) return switch (self.mode) {
                .deadline => error.DeadlineExceeded,
                .canceled => error.Canceled,
                else => error.Forbidden,
            };
        }
    };
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .async_limit = .limited(4) });
    defer threaded.deinit();
    const io = threaded.io();
    for (std.enums.values(@FieldType(Fixture, "mode"))) |mode| {
        var fixture: Fixture = .{ .activity = .{ .io = io }, .controller = std.Thread.getCurrentId(), .mode = mode };
        var owners: [12]Fixture.Owner = undefined;
        var targets: [12]d.Target = undefined;
        var ranges: [12][1]d.KeyRange = undefined;
        var snapshots: [12]d.Snapshot = undefined;
        for (&owners, &targets, &snapshots, 0..) |*owner, *target, *snapshot, i| {
            owner.* = .{ .fixture = &fixture, .index = i };
            const key = try std.fmt.bufPrint(&owner.key, "k{d:0>2}", .{i});
            target.* = .{ .table = "docs", .table_id = 1, .group_id = i + 1, .parallel = i != 0 };
            snapshot.* = .{ .backend = owner.backend(), .ptr = owner, .validate = Fixture.Owner.check, .close = undefined };
            ranges[i] = .{.{ .start = key }};
        }
        for (&targets, &ranges, 0..) |*target, *range, i| {
            range[0].end = if (i + 1 < owners.len) owners[i + 1].key[0..3] else null;
            target.ranges = range;
        }
        var read: d.ReadSet = .{ .a = std.testing.allocator, .targets = &targets, .snapshots = &snapshots, .independent_validations = &.{}, .source = .{ .ptr = &fixture, .checkpoint = Fixture.check, .admit = undefined, .capture = undefined, .fanout_io = io, .fanout_width = 4 } };
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        const backend = read.backend();
        var meter: b.Meter = .{ .a = a, .backend = backend, .limit = if (mode == .work) 20_000 else 100_000 };
        backend.vtable.set_work.?(backend.ptr, meter.work());
        const result = backend.vtable.probe_where.?(backend.ptr, a, "docs", &.{}, 12);
        const expected: ?anyerror = switch (mode) {
            .fatal, .revoked => error.Forbidden,
            .deadline => error.DeadlineExceeded,
            .canceled => error.Canceled,
            .malformed => error.CypherInvalidBackendPage,
            else => null,
        };
        if (expected) |err| try std.testing.expectError(err, result) else {
            const page = (try result).?;
            const complete = mode != .incomplete and mode != .unsupported and mode != .quota;
            try std.testing.expectEqual(complete, page.complete);
            if (complete) {
                try std.testing.expectEqual(@as(usize, if (mode == .empty) 0 else 12), page.page.items.len);
                try std.testing.expectEqual(page.page.items.len, page.page.examined.?);
                for (page.page.items, 0..) |node, i| {
                    const key = try std.fmt.allocPrint(a, "k{d:0>2}-{d:0>2}", .{ if (mode == .skew) @as(usize, 1) else i, if (mode == .skew) i else 0 });
                    try std.testing.expectEqualStrings(key, node.identity.key);
                }
            } else try std.testing.expectEqual(@as(usize, 0), page.page.items.len);
        }
        try std.testing.expectEqual(@as(usize, 0), fixture.activity.active.load(.monotonic));
        try std.testing.expect(fixture.activity.peak.load(.monotonic) > 1);
        try std.testing.expect(fixture.activity.peak.load(.monotonic) <= 4);
        try std.testing.expect(meter.units > 0 and meter.units <= meter.limit);
        if (mode == .skew or mode == .work) try std.testing.expectEqual(@as(usize, 2), owners[1].calls);
        if (mode == .skew) try std.testing.expect(meter.units > 24);
        for (owners) |owner| try std.testing.expectEqual(meter.work().ptr, owner.work.?.ptr);
    }
}
