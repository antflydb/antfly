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
        const result = files.testArtifactPhysicalOwnership(alloc, io, path, &probe, Probe.check, Probe.park, Probe.enter);
        if (reject_at == 0) {
            try std.testing.expectEqual(@as(u64, body.len), try result);
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
