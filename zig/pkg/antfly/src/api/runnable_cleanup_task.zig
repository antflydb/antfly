// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Reserve an actual cleanup task before publishing physical resources. The
//! caller retains execution, bounded allocator, callback context and every
//! resource through joined deinit. Activation failure leaves physical ownership
//! with that caller; it must join or transfer before finishing the execution.
const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const abi = runtime.runnable_abi;

pub const Task = struct {
    node: *Node,
    future: std.Io.Future(anyerror!void),
    started: bool = false,
    joined: bool = false,

    pub const Outcome = union(enum) { pending, succeeded, failed: anyerror };

    const Node = struct {
        allocator: std.mem.Allocator,
        io: std.Io,
        borrow: abi.Borrowed,
        go: std.Io.Event = .unset,
        done: std.Io.Event = .unset,
        outcome: anyerror!void = undefined,
        context: *anyopaque,
        callback: *const fn (*anyopaque, abi.Borrowed, std.Io) anyerror!void,
        fn run(self: *Node) anyerror!void {
            const result = self.runInner();
            self.outcome = result;
            self.done.set(self.io);
            return result;
        }
        fn runInner(self: *Node) anyerror!void {
            try self.go.wait(self.io);
            // Activate on the reserved child, so a canceled foreground task
            // does not lend its cancellation scope to the independent fence.
            try self.borrow.activateCleanup();
            const status = self.borrow.checkpoint(self.borrow.context);
            if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
            try self.callback(self.context, self.borrow, self.io);
        }
    };

    pub fn reserve(allocator: std.mem.Allocator, io: std.Io, borrow: abi.Borrowed, options: *const abi.Cleanup, context: *anyopaque, callback: *const fn (*anyopaque, abi.Borrowed, std.Io) anyerror!void) !Task {
        try borrow.validate();
        try options.validate();
        const local = runtime.native_abi.IoBorrow.init(&io);
        if (options.io.userdata != local.userdata or options.io.vtable != local.vtable or options.io.dispatch != local.dispatch or
            !options.io.contract.matches(local.contract)) return error.InvalidArgument;
        if (borrow.reserve_cleanup == null or borrow.activate_cleanup == null) return error.UnsupportedOperation;
        const node = try allocator.create(Node);
        errdefer allocator.destroy(node);
        node.* = .{ .allocator = allocator, .io = io, .borrow = borrow, .context = context, .callback = callback };
        // Concurrent admission happens now; async could defer it until failure.
        var future = try io.concurrent(Node.run, .{node});
        errdefer {
            _ = future.cancel(io) catch {};
        }
        try borrow.reserveCleanup(options);
        return .{ .node = node, .future = future };
    }

    /// Fence foreground children before starting. The handle is move-only;
    /// its caller owns every callback/allocator/execution borrow until join.
    pub fn start(self: *Task) !void {
        if (self.started or self.joined) return error.InvalidRunnableTransition;
        self.started = true;
        self.node.go.set(self.node.io);
    }

    /// Observe callback completion only. Even succeeded/failed outcomes still
    /// require a physical future join before retiring metadata or borrows.
    pub fn outcome(self: *const Task) Outcome {
        if (!self.node.done.isSet()) return .pending;
        self.node.outcome catch |err| return .{ .failed = err };
        return .succeeded;
    }

    /// Wait for the callback against one absolute observer deadline. Spurious
    /// wakeups cannot renew it. Timeout leaves the live task and all resources
    /// with the caller for an explicit move to a retained supervisor owner.
    /// Callback completion is not future retirement: this never sets joined.
    pub fn waitUntil(self: *Task, deadline: std.Io.Clock.Timestamp) !void {
        if (deadline.clock != .awake) return error.InvalidArgument;
        if (!self.started or self.joined) return error.InvalidRunnableTransition;
        const io = self.node.io;
        const protection = io.swapCancelProtection(.blocked);
        defer _ = io.swapCancelProtection(protection);
        while (!self.node.done.isSet()) {
            if (deadline.raw.nanoseconds <= deadline.clock.now(io).nanoseconds) return error.Timeout;
            self.node.done.waitTimeout(io, .{ .deadline = deadline }) catch |err| switch (err) {
                error.Timeout => continue,
                error.Canceled => unreachable,
            };
        }
        try self.node.outcome;
    }

    pub fn runAndWaitUntil(self: *Task, deadline: std.Io.Clock.Timestamp) !void {
        if (deadline.clock != .awake) return error.InvalidArgument;
        try self.start();
        try self.waitUntil(deadline);
    }

    /// Physical retirement fence. The caller/supervisor keeps all retained
    /// ownership through this join, including on callback failure.
    pub fn join(self: *Task) !void {
        if (!self.started or self.joined) return error.InvalidRunnableTransition;
        const protection = self.node.io.swapCancelProtection(.blocked);
        defer _ = self.node.io.swapCancelProtection(protection);
        defer self.joined = true;
        try self.future.await(self.node.io);
    }

    pub fn runAndJoin(self: *Task) !void {
        try self.start();
        try self.join();
    }

    /// Abandonment cancels/joins even after a timed observer wait. It does not
    /// retire published physical ownership or release the caller's execution.
    pub fn deinit(self: *Task) void {
        const node = self.node;
        if (!self.joined) _ = self.future.cancel(node.io) catch {};
        node.allocator.destroy(node);
        self.* = undefined;
    }
};
