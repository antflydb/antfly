// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Preauthorized, joined completion on an existing execution. Reserve before
//! SQL sealing or participant acceptance. The callback owns all storage-child
//! joins and authoritative recovery transfer before returning. The caller keeps
//! its execution, bounded allocator, executor and callback context alive through
//! deinit, and finishes the execution only afterwards.
const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const abi = runtime.runnable_abi;

pub const Task = struct {
    node: *Node,
    future: std.Io.Future(anyerror!void),
    joined: bool = false,

    /// Fixed retained task metadata, charged before reserving the worker.
    pub fn retainedBytes() usize {
        return @sizeOf(Node) + @sizeOf(Task);
    }

    const Node = struct {
        allocator: std.mem.Allocator,
        io: std.Io,
        borrow: abi.Borrowed,
        go: std.Io.Event = .unset,
        context: *anyopaque,
        callback: *const fn (*anyopaque, abi.Borrowed, std.Io) anyerror!void,

        fn run(self: *Node) anyerror!void {
            try self.go.wait(self.io);
            // The parent has activated completion before releasing this gate.
            // No physical write may precede this independent task's first fence.
            const status = self.borrow.checkpoint(self.borrow.context);
            if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
            try self.callback(self.context, self.borrow, self.io);
        }
    };

    pub fn reserve(
        allocator: std.mem.Allocator,
        io: std.Io,
        borrow: abi.Borrowed,
        options: *const abi.Completion,
        context: *anyopaque,
        callback: *const fn (*anyopaque, abi.Borrowed, std.Io) anyerror!void,
    ) !Task {
        try borrow.validate();
        try options.validate();
        // Reserve only a task on the exact executor supplied to the retained
        // completion. A different executor could carry another cancellation
        // scope, allocation owner or deadline implementation.
        const local = runtime.native_abi.IoBorrow.init(&io);
        if (options.io.userdata != local.userdata or options.io.vtable != local.vtable or options.io.dispatch != local.dispatch or
            !options.io.contract.matches(local.contract)) return error.InvalidArgument;
        if (borrow.reserve_completion == null or borrow.activate_completion == null) return error.UnsupportedOperation;
        const node = try allocator.create(Node);
        errdefer allocator.destroy(node);
        node.* = .{ .allocator = allocator, .io = io, .borrow = borrow, .context = context, .callback = callback };
        // concurrent must succeed now. async could defer admission until after
        // acceptance; no synchronous fallback is safe for this contract.
        var future = try io.concurrent(Node.run, .{node});
        errdefer {
            _ = future.cancel(io) catch {};
        }
        try borrow.reserveCompletion(options);
        return .{ .node = node, .future = future };
    }

    /// Activate before any potentially durable work, then join independently
    /// of foreground cancellation. Future.await otherwise forwards a canceled
    /// parent's cancellation to its child, which would violate this handoff.
    /// The callback must use the finite completion fence for every wait/phase.
    pub fn runAndJoin(self: *Task) !void {
        if (self.joined) return error.InvalidRunnableTransition;
        try self.node.io.checkCancel();
        try self.node.borrow.activateCompletion();
        const protection = self.node.io.swapCancelProtection(.blocked);
        defer _ = self.node.io.swapCancelProtection(protection);
        self.node.go.set(self.node.io);
        defer self.joined = true;
        try self.future.await(self.node.io);
    }

    /// Abandoning before activation is safe: the gate forbids physical work.
    /// After activation runAndJoin owns the mandatory join on every exit.
    pub fn deinit(self: *Task) void {
        const node = self.node;
        if (!self.joined) _ = self.future.cancel(node.io) catch {};
        node.allocator.destroy(node);
        self.* = undefined;
    }
};
