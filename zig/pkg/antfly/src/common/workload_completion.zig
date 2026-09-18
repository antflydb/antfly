// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2); see https://www.antfly.io/licensing/ELv2-license.

//! Private prepaid scratch for one mandatory completion at a time. Its host
//! owns a separate aggregate reservation before publishing foreground service.
//! Busy callers defer durable work; they never infer an aborted transaction.
const std = @import("std");
const admission = @import("workload_admission.zig");
const Owner = @import("workload_allocator.zig").Owner;

pub const Workspace = struct {
    backing: std.mem.Allocator,
    gate: admission.Controller,
    memory: *Owner,
    capacity: usize,
    in_use: std.atomic.Value(bool) = .init(false),

    /// All fixed allocator/controller bookkeeping is charged to the host too.
    pub fn hostBytes(capacity: usize) !usize {
        return std.math.add(usize, capacity, @sizeOf(Workspace) + @sizeOf(Owner) + @sizeOf(admission.MemoryAccount));
    }

    pub fn create(backing: std.mem.Allocator, capacity: usize) !*Workspace {
        if (capacity == 0) return error.InvalidTransactionCompletionCapacity;
        const ceiling = try std.math.add(usize, capacity, @sizeOf(Owner));
        const self = try backing.create(Workspace);
        errdefer backing.destroy(self);
        self.* = .{
            .backing = backing,
            .gate = admission.Controller.initConfigured(0, .{ .max_retained_bytes = ceiling }),
            .memory = undefined,
            .capacity = capacity,
        };
        errdefer self.gate.deinitMemory();
        self.memory = try Owner.createReserved(backing, &self.gate, capacity);
        return self;
    }

    pub fn destroy(self: *Workspace) void {
        std.debug.assert(!self.in_use.load(.acquire));
        std.debug.assert(self.memory.live.load(.acquire) == 0);
        self.memory.release();
        self.gate.close();
        self.gate.deinitMemory();
        const backing = self.backing;
        backing.destroy(self);
    }

    pub fn tryAcquire(self: *Workspace) !Borrow {
        if (self.in_use.cmpxchgStrong(false, true, .acq_rel, .acquire) != null) return error.TransactionCompletionBusy;
        std.debug.assert(self.memory.live.load(.acquire) == 0);
        self.memory.budget_exhausted.store(false, .release);
        return .{ .workspace = self };
    }

    pub const Borrow = struct {
        workspace: *Workspace,

        pub fn allocator(self: *const Borrow) std.mem.Allocator {
            return self.workspace.memory.allocator();
        }

        pub fn denied(self: *const Borrow) bool {
            return self.workspace.memory.budget_exhausted.load(.acquire);
        }

        pub fn release(self: *Borrow) void {
            // Returning capacity while a decoder, batch, or callback still
            // owns a buffer would let another recovery spend the same credits.
            std.debug.assert(self.workspace.memory.live.load(.acquire) == 0);
            self.workspace.in_use.store(false, .release);
            self.* = undefined;
        }
    };
};

test "workload admission completion workspace excludes peers and keeps prepaid capacity" {
    const workspace = try Workspace.create(std.testing.allocator, 1024);
    defer workspace.destroy();
    const before = workspace.gate.stats().retained_bytes;
    var first = try workspace.tryAcquire();
    const bytes = try first.allocator().alloc(u8, 1024);
    try std.testing.expectError(error.OutOfMemory, first.allocator().alloc(u8, 1));
    try std.testing.expect(first.denied());
    try std.testing.expectError(error.TransactionCompletionBusy, workspace.tryAcquire());
    first.allocator().free(bytes);
    first.release();
    try std.testing.expectEqual(before, workspace.gate.stats().retained_bytes);
    workspace.gate.close();
    var second = try workspace.tryAcquire();
    defer second.release();
    try std.testing.expect(!second.denied());
    const cleanup = try second.allocator().alloc(u8, 1024);
    second.allocator().free(cleanup);
}

test "workload admission completion workspace rolls back backing allocation failure" {
    const Fixture = struct {
        fn run(backing: std.mem.Allocator) !void {
            const workspace = try Workspace.create(backing, 128);
            defer workspace.destroy();
            var borrowed = try workspace.tryAcquire();
            defer borrowed.release();
            const bytes = try borrowed.allocator().alloc(u8, 128);
            borrowed.allocator().free(bytes);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}
