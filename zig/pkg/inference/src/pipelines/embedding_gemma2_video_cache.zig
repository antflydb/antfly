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

//! Request-local exact video projections. No borrowed compute-wrapper tensors.
const std = @import("std");
const Session = @import("../backends/session.zig").Session;
const Permit = @import("../backends/session.zig").RunPermit;
const ops = @import("../ops/ops.zig");
const metal = @import("../ops/metal_compute.zig").MetalCompute;
const enabled = @import("build_options").enable_metal;
const Device = if (enabled) @import("../backends/metal_tensor.zig").MetalTensor else struct {};
pub const Key = struct { clip: [32]u8, picture: usize };
pub const Stats = struct { hits: usize = 0, stored: usize = 0, evicted: usize = 0, denied: usize = 0, unsupported: usize = 0 };
const Value = union(enum) { cpu: []f32, device: Device };
const Entry = struct { value: Value, tokens: usize, bytes: usize, permit: Permit };
const Slot = struct { key: Key = undefined, age: u64 = 0, entry: ?Entry = null, borrowed: bool = false };
/// A ticket moves ownership without touching Metal's confined buffer refcount.
/// Producers may borrow entries; only the model consumer imports device tensors.
pub const Ticket = struct { slot: usize, entry: Entry };
pub const Cache = struct {
    allocator: std.mem.Allocator,
    session: Session,
    control: @import("../execution_control.zig").InferenceExecutionControl = .{},
    guard: ?@import("../execution_control.zig").UninterruptibleGuard = null,
    limit: usize = 64 * 1024 * 1024,
    bytes: usize = 0,
    slots: [128]Slot = @splat(.{}),
    clock: u64 = 0,
    mutex: std.atomic.Mutex = .unlocked,
    stats: Stats = .{},
    fn equal(a: Key, b: Key) bool {
        return a.picture == b.picture and std.mem.eql(u8, &a.clip, &b.clip);
    }
    fn touch(self: *Cache, slot: usize) void {
        self.clock +|= 1;
        self.slots[slot].age = self.clock;
    }
    pub fn take(self: *Cache, key: Key) ?Ticket {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        for (&self.slots, 0..) |*slot, index| if (!slot.borrowed and slot.entry != null and equal(slot.key, key)) {
            const entry = slot.entry.?;
            slot.entry = null;
            slot.borrowed = true;
            self.touch(index);
            self.stats.hits += 1;
            return .{ .slot = index, .entry = entry };
        };
        return null;
    }
    pub fn restore(self: *Cache, ticket: Ticket) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        const slot = &self.slots[ticket.slot];
        std.debug.assert(slot.borrowed and slot.entry == null);
        slot.borrowed = false;
        slot.entry = ticket.entry;
    }
    fn drop(self: *Cache, entry: *Entry) void {
        switch (entry.value) {
            .cpu => |values| self.allocator.free(values),
            .device => |*value| if (comptime enabled) value.deinit(),
        }
        entry.permit.deinit();
        self.bytes -= entry.bytes;
    }
    pub fn deinit(self: *Cache) void {
        defer {
            if (self.guard) |*guard| guard.deinit();
            self.guard = null;
        }
        for (&self.slots) |*slot| {
            std.debug.assert(!slot.borrowed);
            if (slot.entry) |*entry| self.drop(entry);
            slot.entry = null;
        }
        std.debug.assert(self.bytes == 0);
    }
    fn reserve(self: *Cache, key: Key, bytes: usize) ?usize {
        if (bytes > self.limit) return null;
        for (self.slots) |slot| if ((slot.borrowed or slot.entry != null) and equal(slot.key, key)) return null;
        while (true) {
            var free: ?usize = null;
            var oldest: ?usize = null;
            for (self.slots, 0..) |slot, i| {
                if (slot.borrowed) continue;
                if (slot.entry == null) {
                    free = i;
                    continue;
                }
                if (oldest == null or slot.age < self.slots[oldest.?].age) oldest = i;
            }
            if (free != null and self.bytes <= self.limit - bytes) return free;
            const victim = oldest orelse return null;
            self.drop(&self.slots[victim].entry.?);
            self.slots[victim].entry = null;
            self.stats.evicted += 1;
        }
    }
    pub fn storeCpu(self: *Cache, key: Key, values: []const f32, tokens: usize) void {
        if (tokens == 0 or tokens > 140 or values.len != tokens * 512) return;
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        const bytes = values.len * @sizeOf(f32);
        const slot = self.reserve(key, bytes) orelse return;
        var permit = self.session.admitRetainedMedia(bytes, 0) catch {
            self.stats.denied += 1;
            return;
        };
        const owned = self.allocator.dupe(f32, values) catch {
            permit.deinit();
            self.stats.denied += 1;
            return;
        };
        self.slots[slot] = .{ .key = key, .entry = .{ .value = .{ .cpu = owned }, .tokens = tokens, .bytes = bytes, .permit = permit } };
        self.bytes += bytes;
        self.touch(slot);
        self.stats.stored += 1;
    }
    pub fn storeDevice(self: *Cache, key: Key, tensor: ops.CT, tokens: usize) void {
        if (comptime !enabled) return;
        if (tokens == 0 or tokens > 140) return;
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        if (self.guard == null) self.guard = self.control.enterUninterruptible(self.session.interruption()) catch {
            self.stats.denied += 1;
            return;
        };
        var retained = (metal.retainDenseDeviceTensor(tensor) catch {
            self.stats.denied += 1;
            return;
        }) orelse {
            self.stats.unsupported += 1;
            return;
        };
        if (retained.dtype != .f32 or retained.elemCount() != tokens * 512) {
            retained.deinit();
            self.stats.unsupported += 1;
            return;
        }
        const bytes = retained.retainedAllocationBytes();
        const slot = self.reserve(key, bytes) orelse {
            retained.deinit();
            return;
        };
        const permit = self.session.admitRetainedMedia(0, bytes) catch {
            retained.deinit();
            self.stats.denied += 1;
            return;
        };
        self.slots[slot] = .{ .key = key, .entry = .{ .value = .{ .device = retained }, .tokens = tokens, .bytes = bytes, .permit = permit } };
        self.bytes += bytes;
        self.touch(slot);
        self.stats.stored += 1;
    }
    pub fn import(self: *Cache, cb: *const ops.ComputeBackend, ticket: Ticket) !ops.CT {
        _ = self;
        return switch (ticket.entry.value) {
            .cpu => |values| cb.fromFloat32Shape(values, &.{ @intCast(ticket.entry.tokens), 512 }),
            .device => |*tensor| if (comptime enabled) (if (cb.kind() == .metal) @as(*metal, @ptrCast(@alignCast(cb.ptr))).ctFromRetainedDeviceTensor(tensor) else error.UnsupportedVideoBackend) else error.UnsupportedVideoBackend,
        };
    }
};

fn testSession(controller: *@import("../runtime/tier/memory.zig").AdmissionController, marker: *u8, limit: usize) Session {
    return .{ .ptr = marker, .vtable = &.{ .run = undefined, .inputInfo = undefined, .outputInfo = undefined, .backend = undefined, .close = undefined, .interruption = struct {
        fn classify(_: *anyopaque) @import("../execution_control.zig").Interruption {
            return .cooperative;
        }
    }.classify }, .run_admission = .{ .controller = controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = limit }, .static_workspace_bytes = 0, .check_live_memory = false } };
}
test "embeddinggemma2 video cache borrows survive eviction pressure and preserve exact ownership" {
    const a = std.testing.allocator;
    var controller = @import("../runtime/tier/memory.zig").AdmissionController{};
    var marker: u8 = 0;
    var cache = Cache{ .allocator = a, .session = testSession(&controller, &marker, 4096), .limit = 2048 };
    defer cache.deinit();
    var values: [512]f32 = @splat(1.25);
    const key = Key{ .clip = @splat(1), .picture = 7 };
    cache.storeCpu(key, &values, 1);
    values[0] = 99;
    const ticket = cache.take(key).?;
    try std.testing.expectEqual(@as(f32, 1.25), ticket.entry.value.cpu[0]);
    try std.testing.expectEqual(@as(usize, 2048), controller.snapshot().host_scratch_bytes);
    cache.storeCpu(.{ .clip = @splat(2), .picture = 7 }, &values, 1);
    try std.testing.expectEqual(@as(usize, 1), cache.stats.stored);
    try std.testing.expect(cache.take(key) == null);
    cache.restore(ticket);
    cache.storeCpu(.{ .clip = @splat(2), .picture = 7 }, &values, 1);
    try std.testing.expectEqual(@as(usize, 1), cache.stats.evicted);
    try std.testing.expect(cache.take(key) == null);
    const replacement = cache.take(.{ .clip = @splat(2), .picture = 7 }).?;
    try std.testing.expectEqual(@as(f32, 99), replacement.entry.value.cpu[0]);
    cache.restore(replacement);
    cache.deinit();
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_scratch_bytes);
}
test "embeddinggemma2 video cache shared admission denies additional request owners" {
    const a = std.testing.allocator;
    var controller = @import("../runtime/tier/memory.zig").AdmissionController{};
    var marker: u8 = 0;
    const session = testSession(&controller, &marker, 2048);
    var first = Cache{ .allocator = a, .session = session };
    defer first.deinit();
    var second = Cache{ .allocator = a, .session = session };
    defer second.deinit();
    const values: [512]f32 = @splat(2);
    const key = Key{ .clip = @splat(0), .picture = 0 };
    first.storeCpu(key, &values, 1);
    second.storeCpu(key, &values, 1);
    try std.testing.expectEqual(@as(usize, 2048), first.bytes);
    try std.testing.expectEqual(@as(usize, 0), second.bytes);
    try std.testing.expectEqual(@as(usize, 1), second.stats.denied);
    first.deinit();
    second.storeCpu(key, &values, 1);
    try std.testing.expectEqual(@as(usize, 2048), second.bytes);
}
test "embeddinggemma2 video cache allocation denial returns the retained lease" {
    var controller = @import("../runtime/tier/memory.zig").AdmissionController{};
    var marker: u8 = 0;
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var cache = Cache{ .allocator = failing.allocator(), .session = testSession(&controller, &marker, 4096) };
    defer cache.deinit();
    const values: [512]f32 = @splat(2);
    cache.storeCpu(.{ .clip = @splat(0), .picture = 0 }, &values, 1);
    try std.testing.expectEqual(@as(usize, 0), cache.bytes);
    try std.testing.expectEqual(@as(usize, 1), cache.stats.denied);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_scratch_bytes);
}

test "embeddinggemma2 Metal video cache survives compute wrappers without intermediate downloads" {
    if (comptime !enabled) return error.SkipZigTest;
    const a = std.testing.allocator;
    const hosted = @import("../ops/gpu_hosted_store.zig");
    var store = hosted.WeightStore{ .allocator = a, .prefix = "", .lazy_weights = .empty };
    defer {
        @import("../ops/metal_compute.zig").deinitPrefetchQueue(&store);
        store.lazy_weights.deinit(a);
    }
    @import("../ops/metal_compute.zig").initPrefetchQueue(&store, a);
    var controller = @import("../runtime/tier/memory.zig").AdmissionController{};
    var marker: u8 = 0;
    var session = testSession(&controller, &marker, 1024 * 1024);
    session.run_admission.?.backend_class = .gpu;
    const Probe = struct {
        arms: usize = 0,
        disarms: usize = 0,
        fn arm(raw: *anyopaque, _: @import("../execution_control.zig").MonitorControl) !u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.arms += 1;
            return 17;
        }
        fn disarm(raw: *anyopaque, token: u64) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(token == 17);
            self.disarms += 1;
        }
        fn classify(_: *anyopaque) @import("../execution_control.zig").Interruption {
            return .uninterruptible;
        }
    };
    var probe = Probe{};
    var vtable = session.vtable.*;
    vtable.interruption = Probe.classify;
    session.vtable = &vtable;
    var cache = Cache{ .allocator = a, .session = session, .control = .{ .hard_cancellation = .{ .ptr = &probe, .arm_fn = Probe.arm, .disarm_fn = Probe.disarm } } };
    defer cache.deinit();
    const key = Key{ .clip = @splat(3), .picture = 1 };
    var values: [1024]f32 = undefined;
    @memset(values[0..512], -0.25);
    @memset(values[512..], 0.75);
    {
        var first = try metal.init(a, &store, null);
        defer first.deinit();
        const cb = first.computeBackend();
        if (!cb.decoderRuntimeReady()) return error.SkipZigTest;
        const input = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&values, &.{ 2, 512 }));
        defer cb.free(input);
        const view = try cb.primSlice(input, &.{ 1, 0 }, &.{ 2, 512 }, &.{ 1, 1 }, &.{ 2, 512 });
        defer cb.free(view);
        const before = cb.debugTimingSnapshot().provider.metal_tensor_host_mirror_download_bytes;
        cache.storeDevice(key, view, 1);
        try std.testing.expectEqual(@as(usize, 1), cache.stats.stored);
        try std.testing.expectEqual(before, cb.debugTimingSnapshot().provider.metal_tensor_host_mirror_download_bytes);
        const retained = (try metal.retainDenseDeviceTensor(view)).?;
        var owned = retained;
        defer owned.deinit();
        try std.testing.expectEqual(owned.retainedAllocationBytes(), controller.snapshot().backend_scratch_bytes);
    }
    try std.testing.expect(controller.snapshot().backend_scratch_bytes > 0);
    try std.testing.expectEqual(@as(usize, 1), probe.arms);
    try std.testing.expectEqual(@as(usize, 0), probe.disarms);
    {
        var second = try metal.init(a, &store, null);
        defer second.deinit();
        const cb = second.computeBackend();
        const ticket = cache.take(key).?;
        defer cache.restore(ticket);
        const before = cb.debugTimingSnapshot().provider.metal_tensor_host_mirror_download_bytes;
        const imported = try cache.import(&cb, ticket);
        defer cb.free(imported);
        try std.testing.expectEqual(before, cb.debugTimingSnapshot().provider.metal_tensor_host_mirror_download_bytes);
        const actual = try cb.toFloat32(imported, a);
        defer a.free(actual);
        try std.testing.expectEqualSlices(f32, values[512..], actual);
    }
    cache.deinit();
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 1), probe.disarms);
}
