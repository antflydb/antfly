// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Process-local broker admission through the API kernel's checked owner port.
//! The host must retain the owner and join all calls before destroying Provider.
//! Parent requests must park before invoking independently admitted work.
const std = @import("std");
const linalg = @import("inference_linalg");
const broker = @import("executor_microbatch.zig");
pub const microbatch = broker;

pub const Provider = ProviderFor(@import("inference_execution_control"), @import("antfly_runtime_abi"));

/// Production supplies its existing control module so the host and every
/// backend share one TLS scope rather than independently compiled copies.
pub fn ProviderFor(comptime execution_control: type, comptime runtime: type) type {
    return struct {
        const Self = @This();
        allocator: std.mem.Allocator,
        owner: runtime.runnable_abi.Owner,

        pub fn init(allocator: std.mem.Allocator, owner: runtime.runnable_abi.Owner) !Self {
            if (!owner.valid()) return error.UnsupportedVersion;
            return .{ .allocator = allocator, .owner = owner };
        }

        pub fn admission(self: *Self) broker.ExecutionAdmission {
            return .{ .context = self, .begin = begin, .finish = finish, .checkpoint = checkpoint, .caller_wait_begin = beginCallerWait, .caller_wait_end = endCallerWait };
        }

        const Invocation = struct {
            control: broker.ExecutionControl,
            handle: ?*anyopaque = null,
            scope: ?linalg.pool.WorkerScope = null,
            checkpoint_scope: ?execution_control.CheckpointScope = null,
            owner: runtime.runnable_abi.Owner,
            publication: bool = false,
            wait_error: ?anyerror = null,

            fn canceled(raw: ?*const anyopaque) callconv(.c) u8 {
                const self: *const Invocation = @ptrCast(@alignCast(raw.?));
                var control = self.control;
                (if (self.publication) control.checkPublication() else broker.ExecutionControl.check(&control)) catch return 1;
                return 0;
            }
        };

        fn begin(raw: *anyopaque, io: std.Io, _: broker.Key, payload_bytes: usize, control: broker.ExecutionControl) !*anyopaque {
            const self: *Self = @ptrCast(@alignCast(raw));
            var collective = control;
            try broker.ExecutionControl.check(&collective);
            const invocation = try self.allocator.create(Invocation);
            errdefer self.allocator.destroy(invocation);
            invocation.* = .{ .control = control, .owner = self.owner };
            const borrow = runtime.native_abi.IoBorrow.init(&io);
            const status = self.owner.start(self.owner.context, &.{
                .io = &borrow,
                .class = @backingInt(runtime.runnable_abi.Class.inference),
                .slots = 1,
                // Original per-member deadlines remain in collective control.
                // An earliest deadline would incorrectly retire healthy peers.
                .cancellation = .{ .context = invocation, .is_cancelled = Invocation.canceled },
                .retained_bytes = @as(u64, payload_bytes) +| @sizeOf(Invocation) +|
                    (@as(u64, control.items.len) *| @sizeOf(broker.ExecuteItem)),
                .out_handle = &invocation.handle,
            });
            if (!status.isOk()) {
                try broker.ExecutionControl.check(&collective);
                return runtime.error_abi.errorFromStatus(status);
            }
            const handle = invocation.handle orelse return error.InvalidRunnableTransition;
            errdefer self.owner.finish(handle);
            // A single admitted CPU slot selects bounded native fallback when a
            // foreign BLAS team cannot be proven to fit. This scope is synchronous
            // and must remain on this OS worker until every backend child joins.
            invocation.scope = try linalg.pool.WorkerScope.init(1);
            invocation.checkpoint_scope = execution_control.CheckpointScope.init(.{
                .context = invocation,
                .check_fn = checkInvocation,
                .wait_begin_fn = parkInvocation,
                .wait_end_fn = enterInvocation,
            });
            return invocation;
        }

        fn beginCallerWait(raw: *anyopaque) !*anyopaque {
            const self: *Self = @ptrCast(@alignCast(raw));
            const wait = try self.allocator.create(execution_control.ResourceWait);
            errdefer self.allocator.destroy(wait);
            wait.* = try execution_control.ResourceWait.init();
            return wait;
        }

        fn endCallerWait(raw: *anyopaque, handle: *anyopaque) !void {
            const self: *Self = @ptrCast(@alignCast(raw));
            const wait: *execution_control.ResourceWait = @ptrCast(@alignCast(handle));
            defer self.allocator.destroy(wait);
            try wait.finish();
        }

        fn checked(status: runtime.error_abi.Status) !void {
            if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
        }

        fn checkInvocation(raw: *anyopaque) !void {
            const invocation: *Invocation = @ptrCast(@alignCast(raw));
            if (invocation.wait_error) |err| return err;
            if (invocation.publication) try invocation.control.checkPublication() else try broker.ExecutionControl.check(&invocation.control);
            try checked(invocation.owner.checkpoint(invocation.handle.?));
        }

        fn parkInvocation(raw: *anyopaque) !void {
            const invocation: *Invocation = @ptrCast(@alignCast(raw));
            checked(invocation.owner.park(invocation.handle.?, @backingInt(runtime.runnable_abi.Wait.resource))) catch |err| {
                invocation.wait_error = err;
                return err;
            };
        }

        fn enterInvocation(raw: *anyopaque) !void {
            const invocation: *Invocation = @ptrCast(@alignCast(raw));
            checked(invocation.owner.enter(invocation.handle.?, @backingInt(runtime.runnable_abi.Class.inference))) catch |err| {
                // Retain failures even if a backend swallows its lock/check error.
                // The final publication fence must not expose that backend output.
                var cause = err;
                // The owner cancellation callback is boolean. Preserve an original
                // collective deadline cause before publishing its generic error.
                broker.ExecutionControl.check(&invocation.control) catch |member_error| {
                    cause = member_error;
                };
                invocation.wait_error = cause;
                return cause;
            };
        }

        fn checkpoint(_: *anyopaque, handle: *anyopaque) !void {
            const invocation: *Invocation = @ptrCast(@alignCast(handle));
            // Internal Io cancellation still returns joined owned output. Explicit
            // member cancellation/deadlines and owner authority remain mandatory.
            invocation.publication = true;
            try checkInvocation(invocation);
        }

        fn finish(raw: *anyopaque, handle: *anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(raw));
            const invocation: *Invocation = @ptrCast(@alignCast(handle));
            invocation.checkpoint_scope.?.deinit();
            invocation.scope.?.deinit();
            self.owner.finish(invocation.handle.?);
            self.allocator.destroy(invocation);
        }
    };
}

test "inference owner provider scopes the actual broker worker and releases its capsule" {
    const runtime = @import("antfly_runtime_abi");
    const Owner = struct {
        active: bool = false,
        starts: usize = 0,
        finishes: usize = 0,
        reject: bool = false,
        retained_bytes: u64 = 0,
        fn retain(_: *anyopaque) callconv(.c) runtime.error_abi.Status {
            return .ok;
        }
        fn release(_: *anyopaque) callconv(.c) void {}
        fn start(raw: *anyopaque, options: *const runtime.runnable_abi.Start) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            options.validate() catch |err| return runtime.error_abi.statusFromError(err);
            std.debug.assert(options.class == @backingInt(runtime.runnable_abi.Class.inference));
            std.debug.assert(options.slots == 1 and options.has_deadline == 0);
            std.debug.assert(!options.cancellation.requested());
            self.starts += 1;
            if (self.reject) return runtime.error_abi.statusFromError(error.QueueFull);
            std.debug.assert(!self.active);
            self.active = true;
            self.retained_bytes = options.retained_bytes;
            options.out_handle.* = self;
            return .ok;
        }
        fn finish(raw: *anyopaque) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.active and linalg.pool.scopedWorkerLimit() == null);
            self.active = false;
            self.finishes += 1;
        }
        fn check(_: *anyopaque) callconv(.c) runtime.error_abi.Status {
            return .ok;
        }
        fn transition(_: *anyopaque, _: c_int) callconv(.c) runtime.error_abi.Status {
            return .ok;
        }
        fn execute(raw: *anyopaque, items: []const broker.ExecuteItem) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.active and linalg.pool.scopedWorkerLimit() == 1);
            for (items) |item| item.slot.setValue(usize, item.payloadAs(usize).*, .serial);
        }
    };
    var owner = Owner{};
    var provider = try Provider.init(std.testing.allocator, .{ .context = &owner, .retain = Owner.retain, .release = Owner.release, .start = Owner.start, .checkpoint = Owner.check, .park = Owner.transition, .enter = Owner.transition, .finish = Owner.finish });
    var executor = broker.Broker.initWithAdmission(std.testing.allocator, provider.admission());
    defer executor.deinit();
    const payload: usize = 42;
    for ([_]bool{ false, true }) |reject| {
        owner.reject = reject;
        const result = try executor.submit(usize, usize, std.testing.io, std.testing.allocator, .{ .model = "scoped", .task = .read, .resource_class = .cpu }, .{ .mode = .serial_compatibility, .preferred_items = 1, .max_items = 1 }, .{ .bytes = 17 }, .{}, null, null, &payload, &owner, Owner.execute);
        if (reject) try std.testing.expectEqual(error.QueueFull, result.result.item_error.cause) else try std.testing.expectEqual(payload, result.result.value);
        try std.testing.expect(!owner.active);
        try std.testing.expectEqual(@as(?usize, null), linalg.pool.scopedWorkerLimit());
    }
    try std.testing.expectEqual(@as(usize, 2), owner.starts);
    try std.testing.expectEqual(@as(usize, 1), owner.finishes);
    try std.testing.expect(owner.retained_bytes >= 17 + @sizeOf(broker.ExecuteItem));
}
