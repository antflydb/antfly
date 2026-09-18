// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2); see https://www.antfly.io/licensing/ELv2-license.

//! Heap-stable allocation owner for request/response memory. References belong
//! to the producer and each exported response; bytes retire only after freeing.
const std = @import("std");
const Controller = @import("workload_admission.zig").Controller;
const MemoryAccount = @import("workload_admission.zig").MemoryAccount;

pub const Owner = struct {
    backing: std.mem.Allocator,
    account: *MemoryAccount,
    refs: std.atomic.Value(usize) = .init(1),
    live: std.atomic.Value(usize) = .init(0),
    budget_exhausted: std.atomic.Value(bool) = .init(false),

    pub fn create(backing: std.mem.Allocator, gate: *Controller) !*Owner {
        const account = try gate.memoryAccount(backing);
        errdefer account.release();
        try account.reserve(@sizeOf(Owner));
        errdefer account.free(@sizeOf(Owner));
        const self = try backing.create(Owner);
        self.* = .{ .backing = backing, .account = account };
        return self;
    }

    pub fn retain(self: *Owner) void {
        const previous = self.refs.fetchAdd(1, .monotonic);
        std.debug.assert(previous > 0 and previous < std.math.maxInt(usize));
    }

    pub fn release(self: *Owner) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        std.debug.assert(self.live.load(.acquire) == 0);
        const account = self.account;
        const backing = self.backing;
        backing.destroy(self);
        account.free(@sizeOf(Owner));
        account.release();
    }

    pub fn allocator(self: *Owner) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    fn reserve(self: *Owner, bytes: usize) !void {
        self.account.reserve(bytes) catch |err| {
            self.budget_exhausted.store(true, .release);
            return err;
        };
    }

    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *Owner = @ptrCast(@alignCast(raw));
        self.reserve(len) catch return null;
        const result = self.backing.rawAlloc(len, alignment, ret_addr) orelse {
            self.account.free(len);
            return null;
        };
        _ = self.live.fetchAdd(len, .monotonic);
        return result;
    }

    fn resize(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *Owner = @ptrCast(@alignCast(raw));
        const growth = new_len -| memory.len;
        if (growth > 0) self.reserve(growth) catch return false;
        if (!self.backing.rawResize(memory, alignment, new_len, ret_addr)) {
            self.account.free(growth);
            return false;
        }
        self.resized(memory.len, new_len);
        return true;
    }

    fn remap(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *Owner = @ptrCast(@alignCast(raw));
        const growth = new_len -| memory.len;
        if (growth > 0) self.reserve(growth) catch return null;
        const result = self.backing.rawRemap(memory, alignment, new_len, ret_addr) orelse {
            self.account.free(growth);
            return null;
        };
        self.resized(memory.len, new_len);
        return result;
    }

    fn resized(self: *Owner, old_len: usize, new_len: usize) void {
        if (new_len >= old_len) {
            _ = self.live.fetchAdd(new_len - old_len, .monotonic);
        } else {
            const freed = old_len - new_len;
            _ = self.live.fetchSub(freed, .monotonic);
            self.account.free(freed);
        }
    }

    fn free(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        self.backing.rawFree(memory, alignment, ret_addr);
        _ = self.live.fetchSub(memory.len, .monotonic);
        self.account.free(memory.len);
    }
};

test "workload admission allocation ownership outlives execution and rolls back failed growth" {
    var gate = Controller.initConfigured(1, .{ .max_retained_bytes = @sizeOf(Owner) + 128 });
    defer gate.deinitMemory();
    var lease = try gate.acquire(.{ .io = std.testing.io, .retained_bytes = 16 });
    const owner = try Owner.create(std.testing.allocator, &gate);
    const alloc = owner.allocator();
    const body = try alloc.alloc(u8, 100);
    try std.testing.expectError(error.OutOfMemory, alloc.alloc(u8, 13));
    try std.testing.expectEqual(@sizeOf(Owner) + 116, gate.stats().retained_bytes);
    owner.retain(); // exported response
    owner.release(); // producer retires
    lease.release();
    try std.testing.expectEqual(@as(usize, 0), gate.stats().in_flight);
    try std.testing.expectEqual(@sizeOf(Owner) + 100, gate.stats().retained_bytes);
    try std.testing.expectError(error.AdmissionBusy, gate.configure(.{}));
    try gate.reconfigure(1, .{ .max_retained_bytes = 16 });
    try std.testing.expectError(error.OutOfMemory, alloc.alloc(u8, 1));
    alloc.free(body);
    owner.release();
    try std.testing.expectEqual(@as(usize, 0), gate.stats().retained_bytes);
}

test "workload admission backing allocation failure returns byte reservation" {
    var gate = Controller.initConfigured(1, .{ .max_retained_bytes = 1024 });
    defer gate.deinitMemory();
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, Owner.create(failing.allocator(), &gate));
    try std.testing.expectEqual(@as(usize, 0), gate.stats().retained_bytes);
    failing.fail_index = 2;
    const owner = try Owner.create(failing.allocator(), &gate);
    try std.testing.expectError(error.OutOfMemory, owner.allocator().alloc(u8, 10));
    try std.testing.expectEqual(@sizeOf(Owner), gate.stats().retained_bytes);
    owner.release();
    try std.testing.expectEqual(@as(usize, 0), gate.stats().retained_bytes);
}

test "workload admission output remains charged after controller storage is destroyed" {
    const alloc = std.testing.allocator;
    const gate = try alloc.create(Controller);
    gate.* = Controller.initConfigured(1, .{ .max_retained_bytes = 1024 });
    const owner = try Owner.create(alloc, gate);
    const body = try owner.allocator().dupe(u8, "retained output");
    const charged = owner.account.retainedBytes();
    try std.testing.expectEqual(@sizeOf(Owner) + body.len, charged);
    gate.deinitMemory();
    alloc.destroy(gate);
    try std.testing.expectEqual(charged, owner.account.retainedBytes());
    try std.testing.expectError(error.OutOfMemory, owner.allocator().alloc(u8, 1));
    try std.testing.expectEqualStrings("retained output", body);
    owner.allocator().free(body);
    try std.testing.expectEqual(@as(usize, @sizeOf(Owner)), owner.account.retainedBytes());
    owner.release();
}

test "workload admission allocation retirement races controller teardown safely" {
    const alloc = std.testing.allocator;
    var threaded = std.Io.Threaded.init(alloc, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const gate = try alloc.create(Controller);
    gate.* = Controller.initConfigured(1, .{ .max_retained_bytes = 4096 });
    const owner = try Owner.create(alloc, gate);
    defer owner.release();
    const Work = struct {
        owner: *Owner,
        io: std.Io,
        started: std.Io.Event = .unset,
        fn run(self: *@This()) !void {
            const memory = try self.owner.allocator().alloc(u8, 64);
            self.started.set(self.io);
            self.owner.allocator().free(memory);
            for (0..1000) |_| {
                const next = self.owner.allocator().alloc(u8, 64) catch break;
                self.owner.allocator().free(next);
            }
        }
    };
    var work: Work = .{ .owner = owner, .io = io };
    var task = try io.concurrent(Work.run, .{&work});
    defer task.cancel(io) catch {};
    try work.started.wait(io);
    gate.deinitMemory();
    alloc.destroy(gate);
    try task.await(io);
    try std.testing.expectEqual(@as(usize, @sizeOf(Owner)), owner.account.retainedBytes());
}
