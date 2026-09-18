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
    parent: ?*Owner = null,
    refs: std.atomic.Value(usize) = .init(1),
    live: std.atomic.Value(usize) = .init(0),
    budget_exhausted: std.atomic.Value(bool) = .init(false),
    prepaid_capacity: ?usize = null,
    prepaid_remaining: std.atomic.Value(usize) = .init(0),

    pub fn create(backing: std.mem.Allocator, gate: *Controller) !*Owner {
        const account = try gate.memoryAccount(backing);
        return createWithAccount(backing, account);
    }

    /// Reserve a verified maximum completion working set before accepting an
    /// irreversible obligation. The charge stays owned until final retirement;
    /// freeing a buffer replenishes these private credits, not foreground work.
    /// Already reserved cleanup may allocate after admission is closed.
    pub fn createReserved(backing: std.mem.Allocator, gate: *Controller, capacity: usize) !*Owner {
        const self = try create(backing, gate);
        errdefer self.release();
        try self.account.reserve(capacity);
        self.prepaid_capacity = capacity;
        self.prepaid_remaining.store(capacity, .release);
        return self;
    }

    /// Check a class ceiling and its enclosing ingress envelope for the same
    /// allocation. A denied parent/backing allocation rolls back the class
    /// charge; no allocator waits while holding a partial reservation.
    pub fn createChild(parent: *Owner, gate: *Controller) !*Owner {
        const account = try gate.memoryAccount(parent.account.allocator);
        if (account == parent.account) {
            account.release();
            return error.DuplicateMemoryAccount;
        }
        const self = try createWithAccount(parent.allocator(), account);
        parent.retain();
        self.parent = parent;
        return self;
    }

    /// A joined offload or separately retained output can use a different,
    /// thread-safe backing allocator without escaping its originating budget.
    /// The new owner retains the shared account, never the request allocator.
    pub fn fork(self: *Owner, backing: std.mem.Allocator) !*Owner {
        // A reserved owner's buffers must share its completion working set;
        // retain that owner instead of silently borrowing foreground bytes.
        if (self.prepaid_capacity != null) return error.ReservedOwnerCannotFork;
        if (self.parent) |parent| {
            const forked_parent = try parent.fork(backing);
            errdefer forked_parent.release();
            self.account.retain();
            const result = try createWithAccount(forked_parent.allocator(), self.account);
            result.parent = forked_parent;
            return result;
        }
        self.account.retain();
        return createWithAccount(backing, self.account);
    }

    fn createWithAccount(backing: std.mem.Allocator, account: *MemoryAccount) !*Owner {
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
        const parent = self.parent;
        const prepaid = self.prepaid_capacity orelse 0;
        std.debug.assert(self.prepaid_remaining.load(.acquire) == prepaid);
        backing.destroy(self);
        account.free(prepaid);
        account.free(@sizeOf(Owner));
        account.release();
        if (parent) |owner| owner.release();
    }

    pub fn allocator(self: *Owner) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    fn reserve(self: *Owner, bytes: usize) !void {
        if (self.prepaid_capacity != null) {
            var remaining = self.prepaid_remaining.load(.acquire);
            while (true) {
                if (bytes > remaining) {
                    self.budget_exhausted.store(true, .release);
                    return error.AdmissionBytesExhausted;
                }
                remaining = self.prepaid_remaining.cmpxchgWeak(remaining, remaining - bytes, .acq_rel, .acquire) orelse return;
            }
        }
        self.account.reserve(bytes) catch |err| {
            self.budget_exhausted.store(true, .release);
            return err;
        };
    }

    fn returnBytes(self: *Owner, bytes: usize) void {
        if (self.prepaid_capacity) |capacity| {
            const previous = self.prepaid_remaining.fetchAdd(bytes, .acq_rel);
            std.debug.assert(previous <= capacity and bytes <= capacity - previous);
        } else self.account.free(bytes);
    }

    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *Owner = @ptrCast(@alignCast(raw));
        self.reserve(len) catch return null;
        const result = self.backing.rawAlloc(len, alignment, ret_addr) orelse {
            self.returnBytes(len);
            if (self.parent) |parent| if (parent.budget_exhausted.load(.acquire)) self.budget_exhausted.store(true, .release);
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
            self.returnBytes(growth);
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
            self.returnBytes(growth);
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
            self.returnBytes(freed);
        }
    }

    fn free(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        self.backing.rawFree(memory, alignment, ret_addr);
        _ = self.live.fetchSub(memory.len, .monotonic);
        self.returnBytes(memory.len);
    }
};

test "workload admission child allocations enforce class and process envelopes together" {
    const alloc = std.testing.allocator;
    var ingress = Controller.initConfigured(1, .{ .max_retained_bytes = 2 * @sizeOf(Owner) + 192 });
    defer ingress.deinitMemory();
    var foreground = Controller.initConfigured(1, .{ .max_retained_bytes = @sizeOf(Owner) + 128 });
    defer foreground.deinitMemory();
    const parent = try Owner.create(alloc, &ingress);
    var parent_live = true;
    defer if (parent_live) parent.release();
    const child = try Owner.createChild(parent, &foreground);
    defer child.release();
    const body = try child.allocator().alloc(u8, 100);
    defer child.allocator().free(body);
    const planning = try parent.allocator().alloc(u8, 92);
    try std.testing.expectError(error.OutOfMemory, child.allocator().alloc(u8, 1));
    try std.testing.expect(child.budget_exhausted.load(.acquire));
    try std.testing.expectEqual(@sizeOf(Owner) + 100, foreground.stats().retained_bytes);
    parent.allocator().free(planning);
    try std.testing.expectError(error.OutOfMemory, child.allocator().alloc(u8, 29));
    parent.release();
    parent_live = false;
    ingress.deinitMemory();
    foreground.deinitMemory();
    try std.testing.expectEqual(2 * @sizeOf(Owner) + 100, child.parent.?.account.retainedBytes());
}

test "workload admission child offloads preserve both enclosing budgets" {
    const alloc = std.testing.allocator;
    var ingress = Controller.initConfigured(1, .{ .max_retained_bytes = 4 * @sizeOf(Owner) + 128 });
    defer ingress.deinitMemory();
    var foreground = Controller.initConfigured(1, .{ .max_retained_bytes = 2 * @sizeOf(Owner) + 128 });
    defer foreground.deinitMemory();
    const parent = try Owner.create(alloc, &ingress);
    const child = try Owner.createChild(parent, &foreground);
    const offload = try child.fork(alloc);
    defer offload.release();
    const body = try offload.allocator().alloc(u8, 128);
    defer offload.allocator().free(body);
    try std.testing.expectError(error.OutOfMemory, offload.allocator().alloc(u8, 1));
    child.release();
    parent.release();
    try std.testing.expectEqual(2 * @sizeOf(Owner) + 128, ingress.stats().retained_bytes);
    try std.testing.expectEqual(@sizeOf(Owner) + 128, foreground.stats().retained_bytes);
}

test "workload admission prepaid completion cannot be consumed by foreground or shutdown" {
    const alloc = std.testing.allocator;
    var gate = Controller.initConfigured(1, .{ .max_retained_bytes = @sizeOf(Owner) + 128 });
    defer gate.deinitMemory();
    const owner = try Owner.createReserved(alloc, &gate, 128);
    var owner_live = true;
    defer if (owner_live) owner.release();
    const account = owner.account;
    account.retain();
    defer account.release();
    try std.testing.expectError(error.AdmissionBytesExhausted, Owner.create(alloc, &gate));
    try std.testing.expectError(error.ReservedOwnerCannotFork, owner.fork(alloc));
    const state = try owner.allocator().alloc(u8, 64);
    try std.testing.expectError(error.OutOfMemory, owner.allocator().alloc(u8, 65));
    gate.close();
    gate.deinitMemory();
    const completion = try owner.allocator().alloc(u8, 64);
    try std.testing.expectEqual(@sizeOf(Owner) + 128, account.retainedBytes());
    owner.allocator().free(state);
    owner.allocator().free(completion);
    try std.testing.expectEqual(@sizeOf(Owner) + 128, account.retainedBytes());
    const retry = try owner.allocator().alloc(u8, 128);
    owner.allocator().free(retry);
    owner.release();
    owner_live = false;
    try std.testing.expectEqual(@as(usize, 0), account.retainedBytes());
}

test "workload admission prepaid completion backing failures return private credits" {
    var gate = Controller.initConfigured(1, .{ .max_retained_bytes = @sizeOf(Owner) + 128 });
    defer gate.deinitMemory();
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 2 });
    const owner = try Owner.createReserved(failing.allocator(), &gate, 128);
    defer owner.release();
    try std.testing.expectError(error.OutOfMemory, owner.allocator().alloc(u8, 128));
    try std.testing.expect(!owner.budget_exhausted.load(.acquire));
    try std.testing.expectEqual(@as(usize, 128), owner.prepaid_remaining.load(.acquire));
    try std.testing.expectEqual(@sizeOf(Owner) + 128, gate.stats().retained_bytes);
}

test "workload admission offload fork shares bytes while outliving request backing" {
    const alloc = std.testing.allocator;
    var gate = Controller.initConfigured(1, .{ .max_retained_bytes = 2 * @sizeOf(Owner) + 80 });
    defer gate.deinitMemory();
    const request = try Owner.create(alloc, &gate);
    var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, request.fork(failing.allocator()));
    try std.testing.expectEqual(@sizeOf(Owner), gate.stats().retained_bytes);

    const offload = try request.fork(alloc);
    defer offload.release();
    const input = try request.allocator().alloc(u8, 32);
    const output = try offload.allocator().alloc(u8, 48);
    try std.testing.expectError(error.OutOfMemory, offload.allocator().alloc(u8, 1));
    try std.testing.expectEqual(2 * @sizeOf(Owner) + 80, gate.stats().retained_bytes);
    request.allocator().free(input);
    request.release();
    try std.testing.expectEqual(@sizeOf(Owner) + 48, gate.stats().retained_bytes);
    gate.deinitMemory();
    try std.testing.expectError(error.OutOfMemory, offload.allocator().alloc(u8, 1));
    offload.allocator().free(output);
    try std.testing.expectEqual(@sizeOf(Owner), offload.account.retainedBytes());
}

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
