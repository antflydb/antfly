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

//! Actual native file operations and checked API-owned scheduler capsules.
const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const owner_module = @import("runnable_owner_execution.zig");
const runnable = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
const files = @import("inference_model_file");

test "native model files lend actual owner capacity and preserve rejected return ownership" {
    const Clock = struct {
        fn now() u64 {
            return 0;
        }
    };
    const Probe = struct {
        state: *owner_module.State,
        owner: runtime.runnable_abi.Owner,
        parent: *anyopaque,
        io: *const runtime.native_abi.IoBorrow,
        canceled: std.atomic.Value(bool) = .init(false),
        reject_at: usize,
        waits: usize = 0,
        retained_bytes: u64,
        fn checked(status: runtime.error_abi.Status) !void {
            if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
        }
        fn cancellation(raw: ?*const anyopaque) callconv(.c) u8 {
            const self: *const @This() = @ptrCast(@alignCast(raw.?));
            return @intFromBool(self.canceled.load(.acquire));
        }
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try checked(self.owner.checkpoint(self.parent));
        }
        fn park(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try checked(self.owner.park(self.parent, @backingInt(runtime.runnable_abi.Wait.resource)));
            self.waits += 1;
            const stats = self.state.owner.scheduler.stats();
            try std.testing.expectEqual(@as(usize, 0), stats.runnable_slots);
            try std.testing.expectEqual(@as(usize, 1), stats.resource_blocked);
            try std.testing.expectEqual(self.retained_bytes, stats.retained_bytes);
            var query: ?*anyopaque = null;
            try checked(self.owner.start(self.owner.context, &.{ .io = self.io, .class = @backingInt(runtime.runnable_abi.Class.query), .allow_wait = 0, .out_handle = &query }));
            defer self.owner.finish(query.?);
            var control: ?*anyopaque = null;
            try checked(self.owner.start(self.owner.context, &.{ .io = self.io, .class = @backingInt(runtime.runnable_abi.Class.control), .allow_wait = 0, .out_handle = &control }));
            defer self.owner.finish(control.?);
            try std.testing.expectEqual(@as(usize, 2), self.state.owner.scheduler.stats().runnable_slots);
            if (self.waits == self.reject_at) self.canceled.store(true, .release);
        }
        fn enter(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try checked(self.owner.enter(self.parent, @backingInt(runtime.runnable_abi.Class.inference)));
        }
    };
    const io = std.testing.io;
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "model", .data = "model bytes" });
    const path = try tmp.dir.realPathFileAlloc(io, "model", alloc);
    defer alloc.free(path);
    for ([_]bool{ false, true }) |mapped| for (0..4) |reject_at| {
        var state: owner_module.State = .{ .owner_alloc = alloc, .owner = try runnable.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = Clock.now };
        defer state.owner.tryDeinit() catch @panic("file owner leaked");
        const owner = state.port();
        const borrow = runtime.native_abi.IoBorrow.init(&io);
        var probe: Probe = .{ .state = &state, .owner = owner, .parent = undefined, .io = &borrow, .reject_at = reject_at, .retained_bytes = 0 };
        var parent: ?*anyopaque = null;
        try Probe.checked(owner.start(owner.context, &.{ .io = &borrow, .class = @backingInt(runtime.runnable_abi.Class.inference), .slots = 2, .retained_bytes = 123, .cancellation = .{ .context = &probe, .is_cancelled = Probe.cancellation }, .out_handle = &parent }));
        defer if (parent) |handle| owner.finish(handle);
        probe.parent = parent.?;
        probe.retained_bytes = state.owner.scheduler.stats().retained_bytes;
        const result = files.testFilePhysicalOwnership(alloc, path, mapped, &probe, Probe.check, Probe.park, Probe.enter);
        if (reject_at == 0) {
            try std.testing.expectEqual(@as(u64, 'm') + 's', try result);
            try std.testing.expectEqual(@as(usize, 2), state.owner.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, 3), probe.waits);
        } else {
            try std.testing.expectError(error.Canceled, result);
            try std.testing.expectEqual(reject_at, probe.waits);
            try std.testing.expectEqual(@as(usize, 0), state.owner.scheduler.stats().runnable_slots);
        }
        try std.testing.expectEqual(@as(usize, 1), state.owner.scheduler.stats().operations);
        try std.testing.expectEqual(probe.retained_bytes, state.owner.scheduler.stats().retained_bytes);
        owner.finish(parent.?);
        parent = null;
        try std.testing.expectEqual(@as(usize, 0), state.owner.scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), state.owner.scheduler.stats().retained_bytes);
    };
}

test "native model artifact receipts lend actual owner capacity and preserve rejected return ownership" {
    try testPhysicalArtifactOwnership(false);
}

test "native model directory discovery lends actual owner capacity and closes every refused traversal" {
    try testPhysicalArtifactOwnership(true);
}

fn testPhysicalArtifactOwnership(comptime directory: bool) !void {
    const Clock = struct {
        fn now() u64 {
            return 0;
        }
    };
    const Probe = struct {
        state: *owner_module.State,
        owner: runtime.runnable_abi.Owner,
        parent: *anyopaque,
        io: *const runtime.native_abi.IoBorrow,
        canceled: std.atomic.Value(bool) = .init(false),
        reject_at: usize,
        waits: usize = 0,
        retained_bytes: u64,
        fn checked(status: runtime.error_abi.Status) !void {
            if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
        }
        fn cancellation(raw: ?*const anyopaque) callconv(.c) u8 {
            const self: *const @This() = @ptrCast(@alignCast(raw.?));
            return @intFromBool(self.canceled.load(.acquire));
        }
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try checked(self.owner.checkpoint(self.parent));
        }
        fn park(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try checked(self.owner.park(self.parent, @backingInt(runtime.runnable_abi.Wait.resource)));
            self.waits += 1;
            const stats = self.state.owner.scheduler.stats();
            try std.testing.expectEqual(@as(usize, 0), stats.runnable_slots);
            try std.testing.expectEqual(@as(usize, 1), stats.resource_blocked);
            try std.testing.expectEqual(self.retained_bytes, stats.retained_bytes);
            var query: ?*anyopaque = null;
            try checked(self.owner.start(self.owner.context, &.{ .io = self.io, .class = @backingInt(runtime.runnable_abi.Class.query), .allow_wait = 0, .out_handle = &query }));
            defer self.owner.finish(query.?);
            var control: ?*anyopaque = null;
            try checked(self.owner.start(self.owner.context, &.{ .io = self.io, .class = @backingInt(runtime.runnable_abi.Class.control), .allow_wait = 0, .out_handle = &control }));
            defer self.owner.finish(control.?);
            try std.testing.expectEqual(@as(usize, 2), self.state.owner.scheduler.stats().runnable_slots);
            if (self.waits == self.reject_at) self.canceled.store(true, .release);
        }
        fn enter(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try checked(self.owner.enter(self.parent, @backingInt(runtime.runnable_abi.Class.inference)));
        }
    };
    const io = std.testing.io;
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const body = try alloc.alloc(u8, 192 * 1024 + 7);
    defer alloc.free(body);
    @memset(body, 'a');
    try tmp.dir.writeFile(io, .{ .sub_path = "model.gguf", .data = body });
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(body, &hash, .{});
    const hex = std.fmt.bytesToHex(hash, .lower);
    const receipt = try std.fmt.allocPrint(alloc, "{{\"version\":2,\"artifacts\":[{{\"path\":\"model.gguf\",\"size\":{d},\"sha256\":\"{s}\"}}]}}", .{ body.len, hex });
    defer alloc.free(receipt);
    try tmp.dir.writeFile(io, .{ .sub_path = ".antfly-download-complete.json", .data = receipt });
    const path = try tmp.dir.realPathFileAlloc(io, ".", alloc);
    defer alloc.free(path);
    if (directory) {
        try tmp.dir.createDirPath(io, "a/b/c");
        try tmp.dir.writeFile(io, .{ .sub_path = "a/b/c/other.gguf", .data = "other" });
        try tmp.dir.createDirPath(io, ".hidden/cache");
        try tmp.dir.writeFile(io, .{ .sub_path = ".hidden/cache/stale.gguf", .data = "stale" });
    }
    const Fds = struct {
        fn count() !usize {
            var dir = try std.Io.Dir.openDirAbsolute(std.testing.io, if (@import("builtin").os.tag == .linux) "/proc/self/fd" else "/dev/fd", .{ .iterate = true });
            defer dir.close(std.testing.io);
            var iterator = dir.iterate();
            var result: usize = 0;
            while (try iterator.next(std.testing.io)) |_| result += 1;
            return result;
        }
    };
    var total_waits: usize = 1;
    var reject_at: usize = 0;
    while (reject_at <= total_waits) : (reject_at += 1) {
        const descriptors = try Fds.count();
        var state: owner_module.State = .{ .owner_alloc = alloc, .owner = try runnable.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = Clock.now };
        defer state.owner.tryDeinit() catch @panic("file owner leaked");
        const owner = state.port();
        const borrow = runtime.native_abi.IoBorrow.init(&io);
        var probe: Probe = .{ .state = &state, .owner = owner, .parent = undefined, .io = &borrow, .reject_at = reject_at, .retained_bytes = 0 };
        var parent: ?*anyopaque = null;
        try Probe.checked(owner.start(owner.context, &.{ .io = &borrow, .class = @backingInt(runtime.runnable_abi.Class.inference), .slots = 2, .retained_bytes = 123, .cancellation = .{ .context = &probe, .is_cancelled = Probe.cancellation }, .out_handle = &parent }));
        defer if (parent) |handle| owner.finish(handle);
        probe.parent = parent.?;
        probe.retained_bytes = state.owner.scheduler.stats().retained_bytes;
        const result = if (directory) files.testDirectoryPhysicalOwnership(alloc, io, path, &probe, Probe.check, Probe.park, Probe.enter) else files.testArtifactPhysicalOwnership(alloc, io, path, &probe, Probe.check, Probe.park, Probe.enter);
        if (reject_at == 0) {
            try std.testing.expectEqual(if (directory) @as(u64, 2) else @as(u64, body.len), try result);
            try std.testing.expectEqual(@as(usize, 2), state.owner.scheduler.stats().runnable_slots);
            total_waits = probe.waits;
            try std.testing.expect(total_waits > 10);
        } else {
            try std.testing.expectError(error.Canceled, result);
            try std.testing.expectEqual(reject_at, probe.waits);
            try std.testing.expectEqual(@as(usize, 0), state.owner.scheduler.stats().runnable_slots);
        }
        try std.testing.expectEqual(@as(usize, 1), state.owner.scheduler.stats().operations);
        try std.testing.expectEqual(probe.retained_bytes, state.owner.scheduler.stats().retained_bytes);
        owner.finish(parent.?);
        parent = null;
        try std.testing.expectEqual(@as(usize, 0), state.owner.scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), state.owner.scheduler.stats().retained_bytes);
        try std.testing.expectEqual(descriptors, try Fds.count());
    }
}

test "embedded inference shared owner funds parked capsules and preserves original refusal" {
    const local = @import("antfly_local_sources");
    const ResourceManager = local.storage_resource_manager.ResourceManager;
    const Ref = struct {
        count: usize = 1,
        fn retain(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.count += 1;
            return .ok;
        }
        fn release(raw: *anyopaque) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.count -= 1;
        }
    };
    const Cancel = struct {
        stopped: bool = false,
        fn check(raw: ?*const anyopaque) callconv(.c) u8 {
            const self: *const @This() = @ptrCast(@alignCast(raw.?));
            return @intFromBool(self.stopped);
        }
    };
    const alloc = std.testing.allocator;
    var refs: Ref = .{};
    var cancellation: Cancel = .{};
    var manager = ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 1 << 20 }, .identity_allocator = alloc });
    defer manager.deinit(alloc);
    var node = try runnable.NodeOwner.init(.{ .slots = 3 });
    defer node.tryDeinit() catch @panic("embedded inference owner leaked");
    var adapter: local.storage_inference_execution_owner.Port = .{ .scheduler = &node.scheduler, .manager = &manager, .backing = alloc, .lifetime_context = &refs, .retain_lifetime = Ref.retain, .release_lifetime = Ref.release };
    const port = adapter.owner();
    const io = std.testing.io;
    const borrow = runtime.native_abi.IoBorrow.init(&io);
    var parent: ?*anyopaque = null;
    try std.testing.expect(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(runtime.runnable_abi.Class.inference), .slots = 2, .retained_bytes = 123, .cancellation = .{ .context = &cancellation, .is_cancelled = Cancel.check }, .out_handle = &parent }).isOk());
    defer if (parent) |value| port.finish(value);
    try std.testing.expectEqual(@as(usize, 2), refs.count);
    const retained = node.scheduler.stats().retained_bytes;
    try std.testing.expect(retained > 123);
    var refused: ?*anyopaque = null;
    try std.testing.expect(!port.start(port.context, &.{ .io = &borrow, .class = @backingInt(runtime.runnable_abi.Class.inference), .allow_wait = 0, .out_handle = &refused }).isOk());
    try std.testing.expect(refused == null);
    try std.testing.expectEqual(@as(usize, 2), refs.count);
    try std.testing.expectEqual(retained, node.scheduler.stats().retained_bytes);
    try std.testing.expect(port.park(parent.?, @backingInt(runtime.runnable_abi.Wait.resource)).isOk());
    for ([_]runtime.runnable_abi.Class{ .query, .control }) |class| {
        var sibling: ?*anyopaque = null;
        try std.testing.expect(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(class), .allow_wait = 0, .out_handle = &sibling }).isOk());
        port.finish(sibling.?);
    }
    try std.testing.expect(port.enter(parent.?, @backingInt(runtime.runnable_abi.Class.inference)).isOk());
    try std.testing.expectEqual(@as(usize, 2), node.scheduler.stats().runnable_slots);
    try std.testing.expect(port.park(parent.?, @backingInt(runtime.runnable_abi.Wait.resource)).isOk());
    const Fence = struct {
        denied: bool = true,
        fn check(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return if (self.denied) runtime.error_abi.statusFromError(error.DeadlineExceeded) else .ok;
        }
    };
    var fence: Fence = .{};
    const enter_checked = port.enter_checked orelse return error.TestExpectedCheckedReentry;
    const restriction: runtime.runnable_abi.Reentry = .{ .context = &fence, .check = Fence.check };
    const denied = enter_checked(parent.?, @backingInt(runtime.runnable_abi.Class.inference), &restriction);
    try std.testing.expect(!denied.isOk());
    try std.testing.expectEqual(error.DeadlineExceeded, runtime.error_abi.errorFromStatus(denied));
    try std.testing.expectEqual(@as(usize, 0), node.scheduler.stats().runnable_slots);
    try std.testing.expectEqual(retained, node.scheduler.stats().retained_bytes);
    fence.denied = false;
    const still_denied = enter_checked(parent.?, @backingInt(runtime.runnable_abi.Class.inference), &restriction);
    try std.testing.expect(!still_denied.isOk());
    try std.testing.expectEqual(error.DeadlineExceeded, runtime.error_abi.errorFromStatus(still_denied));
    try std.testing.expectEqual(@as(usize, 0), node.scheduler.stats().runnable_slots);
    port.finish(parent.?);
    parent = null;
    try std.testing.expectEqual(@as(usize, 1), refs.count);
    try std.testing.expectEqual(@as(usize, 0), node.scheduler.stats().operations);
    // Retry starts a fresh operation; the refused root can never be revived.
    try std.testing.expect(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(runtime.runnable_abi.Class.inference), .slots = 2, .retained_bytes = 123, .cancellation = .{ .context = &cancellation, .is_cancelled = Cancel.check }, .out_handle = &parent }).isOk());
    try std.testing.expect(port.park(parent.?, @backingInt(runtime.runnable_abi.Wait.resource)).isOk());
    cancellation.stopped = true;
    const returned = port.enter(parent.?, @backingInt(runtime.runnable_abi.Class.inference));
    try std.testing.expect(!returned.isOk());
    try std.testing.expectEqual(error.Canceled, runtime.error_abi.errorFromStatus(returned));
    try std.testing.expectEqual(@as(usize, 0), node.scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), node.scheduler.stats().operations);
    try std.testing.expectEqual(retained, node.scheduler.stats().retained_bytes);
    port.finish(parent.?);
    parent = null;
    try std.testing.expectEqual(@as(usize, 1), refs.count);
    try std.testing.expectEqual(@as(usize, 0), node.scheduler.stats().operations);
}

test "native model registry planning yields queued query work and preserves every original refusal" {
    const Clock = struct {
        var instant: std.atomic.Value(u64) = .init(0);
        fn now() u64 {
            return instant.load(.acquire);
        }
    };
    const Mode = enum { normal, canceled, deadline, lease };
    const Probe = struct {
        state: *owner_module.State,
        owner: runtime.runnable_abi.Owner,
        parent: *anyopaque = undefined,
        io: *const runtime.native_abi.IoBorrow,
        canceled: std.atomic.Value(bool) = .init(false),
        query_done: std.atomic.Value(bool) = .init(false),
        checks: usize = 0,
        reject_at: usize,
        mode: Mode,
        fn checked(status: runtime.error_abi.Status) !void {
            if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
        }
        fn cancellation(raw: ?*const anyopaque) callconv(.c) u8 {
            const self: *const @This() = @ptrCast(@alignCast(raw.?));
            return @intFromBool(self.canceled.load(.acquire));
        }
        fn query(self: *@This()) anyerror!void {
            var work: ?*anyopaque = null;
            try checked(self.owner.start(self.owner.context, &.{ .io = self.io, .class = @backingInt(runtime.runnable_abi.Class.query), .out_handle = &work }));
            defer self.owner.finish(work.?);
            self.query_done.store(true, .release);
        }
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == 1) {
                var control: ?*anyopaque = null;
                try checked(self.owner.start(self.owner.context, &.{ .io = self.io, .class = @backingInt(runtime.runnable_abi.Class.control), .allow_wait = 0, .out_handle = &control }));
                self.owner.finish(control.?);
            }
            if (self.checks == self.reject_at) switch (self.mode) {
                .normal => unreachable,
                .canceled => self.canceled.store(true, .release),
                .deadline => Clock.instant.store(1000, .release),
                .lease => return error.ExecutionLeaseLost,
            };
            try checked(self.owner.checkpoint(self.parent));
        }
    };
    const io = std.testing.io;
    const alloc = std.testing.allocator;
    for (std.enums.values(files.RegistryPlanning)) |kind| {
        for ([_]bool{ true, false }) |large| {
            const fixture = try files.registryPlanningFixture(alloc, kind, large);
            defer alloc.free(fixture);
            var tmp = std.testing.tmpDir(.{});
            defer tmp.cleanup();
            const directory = try tmp.dir.realPathFileAlloc(io, ".", alloc);
            defer alloc.free(directory);
            if (kind == .receipt) {
                try tmp.dir.writeFile(io, .{ .sub_path = "model.gguf", .data = "x" });
                try tmp.dir.writeFile(io, .{ .sub_path = ".antfly-download-complete.json", .data = fixture });
            }
            const input = if (kind == .receipt) directory else fixture;
            var total_checks: usize = 0;
            for (std.enums.values(Mode)) |mode| {
                if (large and mode != .normal) continue;
                var reject_at: usize = if (mode == .normal) 0 else 1;
                while (reject_at <= total_checks) : (reject_at += 1) {
                    Clock.instant.store(0, .release);
                    var state: owner_module.State = .{ .owner_alloc = alloc, .owner = try runnable.NodeOwner.init(.{ .slots = 3, .checkpoint_budget = 1 }), .native_now_ns = Clock.now };
                    defer state.owner.tryDeinit() catch @panic("registry planning owner leaked");
                    const owner = state.port();
                    const borrow = runtime.native_abi.IoBorrow.init(&io);
                    var probe: Probe = .{ .state = &state, .owner = owner, .io = &borrow, .reject_at = reject_at, .mode = mode };
                    var parent: ?*anyopaque = null;
                    try Probe.checked(owner.start(owner.context, &.{ .io = &borrow, .class = @backingInt(runtime.runnable_abi.Class.background), .slots = 2, .retained_bytes = input.len, .has_deadline = 1, .native_deadline = 1, .deadline_ns = 1000, .cancellation = .{ .context = &probe, .is_cancelled = Probe.cancellation }, .out_handle = &parent }));
                    defer if (parent) |value| owner.finish(value);
                    probe.parent = parent.?;
                    const retained = state.owner.scheduler.stats().retained_bytes;
                    var query: ?std.Io.Future(anyerror!void) = null;
                    defer {
                        if (query) |*task| _ = task.cancel(io) catch {};
                    }
                    if (mode == .normal) {
                        query = try io.concurrent(Probe.query, .{&probe});
                        const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(2));
                        while (state.owner.scheduler.stats().queued == 0) {
                            if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.TestExpectedQueuedQuery;
                            try io.sleep(.fromMilliseconds(1), .awake);
                        }
                    }
                    const result = files.testRegistryPlanning(alloc, io, kind, input, &probe, Probe.check);
                    if (mode == .normal) {
                        try std.testing.expectEqual(@as(usize, switch (kind) {
                            .metadata => if (large) 256 else 4,
                            .safetensors, .onnx => if (large) 32 else 4,
                            .receipt => 1,
                        }), try result);
                        try query.?.await(io);
                        query = null;
                        try std.testing.expect(probe.query_done.load(.acquire));
                        try std.testing.expectEqual(@as(usize, 2), state.owner.scheduler.stats().runnable_slots);
                        total_checks = probe.checks;
                        try std.testing.expect(total_checks > (if (large) @as(usize, 8) else 0));
                    } else {
                        try std.testing.expectError(switch (mode) {
                            .canceled => error.Canceled,
                            .deadline => error.DeadlineExceeded,
                            .lease => error.ExecutionLeaseLost,
                            .normal => unreachable,
                        }, result);
                        // A one-shot caller refusal must not be polled again and
                        // mistaken for malformed receipt data or a fresh lifetime.
                        try std.testing.expectEqual(reject_at, probe.checks);
                    }
                    try std.testing.expectEqual(@as(usize, 1), state.owner.scheduler.stats().operations);
                    try std.testing.expectEqual(retained, state.owner.scheduler.stats().retained_bytes);
                    owner.finish(parent.?);
                    parent = null;
                    try std.testing.expectEqual(@as(usize, 0), state.owner.scheduler.stats().operations);
                    try std.testing.expectEqual(@as(u64, 0), state.owner.scheduler.stats().retained_bytes);
                    if (mode == .normal) break;
                }
            }
        }
    }
}
