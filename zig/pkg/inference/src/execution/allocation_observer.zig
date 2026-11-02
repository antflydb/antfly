// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Opt-in compute-allocation accounting. Requested owned spans created after
//! enable are measured; allocator padding, previous allocations and model-owned
//! mapped/resident weights are separate scopes. Observer metadata has an
//! independent lifetime, so sampling a retired compute never touches its caller
//! arena. Each escaped allocation retains the observer owner.
const std = @import("std");
const platform = @import("antfly_platform");

pub const Coverage = enum { since_enable, from_owner_creation };

pub const Snapshot = struct {
    requested_live_bytes: u64 = 0,
    requested_lifetime_peak_bytes: u64 = 0,
    allocations: u64 = 0,
    frees: u64 = 0,
    resizes: u64 = 0,
    remaps: u64 = 0,
    unobserved_frees: u64 = 0,
    unobserved_resizes: u64 = 0,
    unobserved_remaps: u64 = 0,
    pub fn since(self: Snapshot, before: Snapshot) Snapshot {
        var result = self;
        inline for (@typeInfo(Snapshot).@"struct".field_names) |field| {
            if (comptime !std.mem.eql(u8, field, "requested_live_bytes") and !std.mem.eql(u8, field, "requested_lifetime_peak_bytes"))
                @field(result, field) = @field(self, field) -| @field(before, field);
        }
        return result;
    }
};

pub const State = struct {
    backing: std.mem.Allocator,
    metadata: std.mem.Allocator,
    parent_owner: ?*State = null,
    coverage: Coverage,
    references: std.atomic.Value(usize) = .init(1),
    mutex: std.atomic.Mutex = .unlocked,
    records: std.AutoHashMapUnmanaged(usize, usize) = .empty,
    usage: Snapshot = .{},

    pub fn create(backing: std.mem.Allocator, coverage: Coverage) !*State {
        return createWithMetadata(backing, platform.allocator.processAllocator(platform.allocator.concurrentFallback()), coverage);
    }

    /// Metadata must outlive every State reference, including observations that
    /// survive a temporary compute. The data allocator keeps its original
    /// lifetime contract. The explicit allocator permits bookkeeping OOM tests.
    pub fn createWithMetadata(backing: std.mem.Allocator, metadata: std.mem.Allocator, coverage: Coverage) !*State {
        const self = try metadata.create(State);
        self.* = .{ .backing = backing, .metadata = metadata, .coverage = coverage, .parent_owner = if (forAllocator(backing)) |owner| owner.retain() else null };
        return self;
    }

    pub fn forAllocator(value: std.mem.Allocator) ?*State {
        if (value.vtable != &vtable) return null;
        return @ptrCast(@alignCast(value.ptr));
    }

    /// Distinct observers can wrap another observed owner. Publish the direct
    /// parent identity so callers do not sum overlapping ownership histories.
    pub fn parent(self: *State) ?*State {
        return self.parent_owner;
    }

    pub fn retain(self: *State) *State {
        const previous = self.references.fetchAdd(1, .monotonic);
        std.debug.assert(previous > 0 and previous < std.math.maxInt(usize));
        return self;
    }

    pub fn release(self: *State) void {
        if (self.references.fetchSub(1, .acq_rel) != 1) return;
        std.debug.assert(self.records.count() == 0);
        const metadata = self.metadata;
        const parent_owner = self.parent_owner;
        self.records.deinit(metadata);
        metadata.destroy(self);
        if (parent_owner) |owner| owner.release();
    }

    pub fn allocator(self: *State) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    pub fn snapshot(self: *State) Snapshot {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        return self.usage;
    }

    fn replaceBytes(self: *State, before: usize, after: usize) void {
        self.usage.requested_live_bytes -= before;
        self.usage.requested_live_bytes += after;
        self.usage.requested_lifetime_peak_bytes = @max(self.usage.requested_lifetime_peak_bytes, self.usage.requested_live_bytes);
    }

    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *State = @ptrCast(@alignCast(raw));
        const memory = self.backing.rawAlloc(len, alignment, ret_addr) orelse return null;
        platform.sync.lockYielding(&self.mutex);
        self.records.put(self.metadata, @intFromPtr(memory), len) catch {
            self.mutex.unlock();
            self.backing.rawFree(memory[0..len], alignment, ret_addr);
            return null;
        };
        _ = self.references.fetchAdd(1, .monotonic);
        self.usage.allocations +|= 1;
        self.replaceBytes(0, len);
        self.mutex.unlock();
        return memory;
    }

    fn resize(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ret_addr: usize) bool {
        const self: *State = @ptrCast(@alignCast(raw));
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (!self.backing.rawResize(memory, alignment, len, ret_addr)) return false;
        if (self.records.getPtr(@intFromPtr(memory.ptr))) |record| {
            std.debug.assert(record.* == memory.len);
            self.replaceBytes(record.*, len);
            record.* = len;
            self.usage.resizes +|= 1;
        } else self.usage.unobserved_resizes +|= 1;
        return true;
    }

    fn remap(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ret_addr: usize) ?[*]u8 {
        const self: *State = @ptrCast(@alignCast(raw));
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        // Replacing a pointer preserves entry count. Removing the old key
        // after a successful backing remap provides the insertion slot.
        if (self.records.contains(@intFromPtr(memory.ptr))) {
            const replacement = self.backing.rawRemap(memory, alignment, len, ret_addr) orelse return null;
            const old = self.records.fetchRemove(@intFromPtr(memory.ptr)).?;
            std.debug.assert(old.value == memory.len);
            self.records.putAssumeCapacity(@intFromPtr(replacement), len);
            self.replaceBytes(old.value, len);
            self.usage.remaps +|= 1;
            return replacement;
        }
        const replacement = self.backing.rawRemap(memory, alignment, len, ret_addr) orelse return null;
        self.usage.unobserved_remaps +|= 1;
        return replacement;
    }

    fn free(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *State = @ptrCast(@alignCast(raw));
        platform.sync.lockYielding(&self.mutex);
        const tracked = self.records.fetchRemove(@intFromPtr(memory.ptr));
        // Serialize retirement with snapshots: a real span stays live until
        // the backing allocator has completed its release.
        self.backing.rawFree(memory, alignment, ret_addr);
        if (tracked) |old| {
            std.debug.assert(old.value == memory.len);
            self.replaceBytes(old.value, 0);
            self.usage.frees +|= 1;
        } else self.usage.unobserved_frees +|= 1;
        self.mutex.unlock();
        if (tracked != null) self.release();
    }

    const vtable = std.mem.Allocator.VTable{ .alloc = alloc, .resize = resize, .remap = remap, .free = free };
};

test "observed compute allocations retain their owner and forward earlier spans" {
    const a = std.testing.allocator;
    const legacy = try a.alloc(u8, 11);
    const state = try State.create(a, .since_enable);
    const observed = state.allocator();
    const current = try observed.alloc(u8, 19);
    try std.testing.expectEqual(@as(u64, 19), state.snapshot().requested_live_bytes);
    observed.free(legacy);
    try std.testing.expectEqual(@as(u64, 1), state.snapshot().unobserved_frees);
    // Drop the compute owner before the allocation it published is retired.
    state.release();
    observed.free(current);
}

test "observed compute allocation bookkeeping is failure atomic" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            const state = try State.createWithMetadata(a, a, .since_enable);
            defer state.release();
            const observed = state.allocator();
            var bytes = try observed.alloc(u8, 31);
            defer observed.free(bytes);
            bytes = try observed.realloc(bytes, 127);
            const snapshot = state.snapshot();
            try std.testing.expectEqual(@as(u64, 127), snapshot.requested_live_bytes);
            try std.testing.expect(snapshot.requested_lifetime_peak_bytes >= snapshot.requested_live_bytes);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "late observed allocators forward unknown resize remap and retirement without claiming history" {
    var buffer: [4096]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&buffer);
    const backing = fixed.allocator();
    var legacy = try backing.alloc(u8, 64);
    const state = try State.create(backing, .since_enable);
    defer state.release();
    const observed = state.allocator();
    try std.testing.expect(observed.resize(legacy, 32));
    legacy = legacy[0..32];
    legacy = observed.remap(legacy, 16).?;
    observed.free(legacy);
    const usage = state.snapshot();
    try std.testing.expectEqual(@as(u64, 1), usage.unobserved_resizes);
    try std.testing.expectEqual(@as(u64, 1), usage.unobserved_remaps);
    try std.testing.expectEqual(@as(u64, 1), usage.unobserved_frees);
    try std.testing.expectEqual(@as(u64, 0), usage.requested_live_bytes);
    try std.testing.expectEqual(@as(u64, 0), usage.allocations);
    try std.testing.expectEqual(Coverage.since_enable, state.coverage);
}

test "observed in-place remap needs no new bookkeeping allocation at full map capacity" {
    var bytes: [65536]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&bytes);
    var failing = std.testing.FailingAllocator.init(fixed.allocator(), .{});
    const state = try State.createWithMetadata(failing.allocator(), failing.allocator(), .since_enable);
    defer state.release();
    const observed = state.allocator();
    var payloads: [64][]u8 = undefined;
    var count: usize = 0;
    defer for (payloads[0..count]) |payload| observed.free(payload);
    while (count == 0 or state.records.available != 0) {
        try std.testing.expect(count < payloads.len);
        payloads[count] = try observed.alloc(u8, 8);
        count += 1;
    }
    // FixedBufferAllocator supports shrinking a non-last allocation in place.
    // A pointer-key replacement consumes the slot retired from the same map.
    // Denying every new allocation must not deny that supported raw remap.
    failing.fail_index = failing.alloc_index;
    payloads[0] = observed.remap(payloads[0], 7) orelse return error.ExpectedInPlaceRemap;
    try std.testing.expect(!failing.has_induced_failure);
    try std.testing.expectEqual(@as(usize, 0), state.records.available);
    try std.testing.expectEqual(@as(u64, 1), state.snapshot().remaps);
    try std.testing.expectEqual(@as(u64, count * 8 - 1), state.snapshot().requested_live_bytes);
}

test "observed metadata survives the retired backing arena for final sampling" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    const state = try State.createWithMetadata(arena.allocator(), std.testing.allocator, .from_owner_creation);
    defer state.release();
    const observed = state.allocator();
    const bytes = try observed.alloc(u8, 47);
    observed.free(bytes);
    arena.deinit();
    // A registry can safely sample/release State after the temporary compute
    // and its now-empty caller arena have both retired. No data operation is
    // permitted after arena.deinit.
    const measured = state.snapshot();
    try std.testing.expectEqual(@as(u64, 0), measured.requested_live_bytes);
    try std.testing.expectEqual(@as(u64, 47), measured.requested_lifetime_peak_bytes);
    try std.testing.expectEqual(@as(u64, 1), measured.frees);
}

test "nested observed owners retain parent identity without counting metadata as payload" {
    const a = std.testing.allocator;
    const parent_owner = try State.createWithMetadata(a, a, .since_enable);
    const child = try State.createWithMetadata(parent_owner.allocator(), a, .since_enable);
    defer child.release();
    try std.testing.expect(child.parent().? == parent_owner);
    const bytes = try child.allocator().alloc(u8, 29);
    try std.testing.expectEqual(@as(u64, 29), parent_owner.snapshot().requested_live_bytes);
    parent_owner.release();
    child.allocator().free(bytes);
    try std.testing.expectEqual(@as(u64, 0), child.parent().?.snapshot().requested_live_bytes);
    try std.testing.expectEqual(@as(u64, 29), child.snapshot().requested_lifetime_peak_bytes);
}
