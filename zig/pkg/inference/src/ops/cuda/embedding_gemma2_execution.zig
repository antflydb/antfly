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

//! Bounded model-owned buffers. Graph addresses never enter the temporary pool;
//! shared admission remains held until synchronized eviction or model unload.
const std = @import("std");
const Context = @import("context.zig").CudaContext;
const Buffer = @import("buffer.zig").DeviceBuffer;
const driver = @import("driver.zig");
const memory = @import("../../runtime/tier/memory.zig");
const prepared = @import("prepared_cache.zig");
const CacheQuota = @import("cache_quota.zig").Quota;
const Cleanup = @import("../../execution/cleanup_context.zig").Context;
pub const packed_limit = 512 * 1024 * 1024;
pub const arena_limit = 128 * 1024 * 1024;
pub const graph_rows_limit = 4096;
pub const Arena = struct {
    kind: enum { encoder, vision, audio } = .encoder,
    grid_x: usize = 0,
    grid_y: usize = 0,
    geometry_ready: bool = false,
    sequence: usize = 0,
    batch: usize = 0,
    mask_kind: u8 = 0,
    attention_workspace: usize = 0,
    weight_generation: usize = 0,
    buffers: [11]Buffer = @splat(.{}),
    quota: CacheQuota = .none,
    graph: driver.CUgraphExec = null,
    warm: bool = false,
    disabled: bool = false,
    eager_samples: u8 = 0,
    replay_samples: u8 = 0,
    eager_ns: u64 = 0,
    replay_ns: u64 = 0,
    timed_uses: u64 = 0,
    last_used: u64 = 0,
    uses: u64 = 0,
    timing: ?@import("context.zig").ProfileEventPair = null,
    timing_replay: ?bool = null,
    completion: driver.CUevent = null,
    pending: bool = false,
    fence_valid: bool = false,
    fn sizesMatch(self: *const Arena, sizes: [11]usize) bool {
        for (self.buffers, sizes) |buffer, size| if (buffer.len != size) return false;
        return true;
    }
    pub fn hasBacking(self: *const Arena) bool {
        return self.bytes() != 0 or self.graph != null or self.completion != null or self.timing != null or self.quota.hasLease();
    }
    pub fn noteUse(self: *Arena, ctx: *Context) void {
        self.pending = true;
        self.fence_valid = false;
        if (self.completion == null) self.completion = ctx.createStreamEvent() catch return;
        ctx.recordEvent(self.completion) catch return;
        self.fence_valid = true;
    }
    fn drain(self: *Arena, ctx: *Context) !void {
        if (!self.pending) return;
        if (self.fence_valid) {
            try ctx.makeCurrent();
            try ctx.driver.check(ctx.synchronizeEventResult(self.completion));
        } else try ctx.synchronize();
        self.pending = false;
    }
    pub fn bytes(self: *const Arena) usize {
        var total: usize = 0;
        for (self.buffers) |b| total += b.len;
        return total;
    }
    pub fn preferReplay(self: *const Arena) bool {
        if (self.replay_samples < 2) return true;
        if (self.eager_samples < 2) return false;
        const replay_wins = @as(u128, self.replay_ns) * 100 <= @as(u128, self.eager_ns) * 95;
        // One real request every 64 samples probes the alternate route. No
        // duplicate inference or synchronous GPU timing is introduced.
        return if (self.timed_uses % 64 == 0) !replay_wins else replay_wins;
    }
    pub fn recordCompleted(self: *Arena, replay: bool, ns: u64) void {
        if (ns == 0) return;
        const samples = if (replay) &self.replay_samples else &self.eager_samples;
        const elapsed = if (replay) &self.replay_ns else &self.eager_ns;
        elapsed.* = if (samples.* == 0) ns else @intCast((@as(u128, elapsed.*) * 3 + ns) / 4);
        samples.* +|= 1;
        self.timed_uses +%= 1;
    }
    pub fn resetTiming(self: *Arena) void {
        self.eager_samples = 0;
        self.replay_samples = 0;
        self.eager_ns = 0;
        self.replay_ns = 0;
        self.timed_uses = 0;
    }

    pub fn captureFailed(self: *Arena, err: anyerror) void {
        self.warm = false;
        // A request cancellation must not poison later requests' replay.
        self.disabled = err != error.Cancelled and err != error.Timeout;
    }
    fn deinit(self: *Arena, ctx: *Context) void {
        self.quota.validateLegacyRaw() catch @import("antfly_platform").inference_process_supervisor.restartWorker(); // BEFORE graph/event/buffer SDK destruction
        if (self.timing) |*pair| {
            ctx.destroyEventChecked(&pair.start) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
            ctx.destroyEventChecked(&pair.end) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
            self.timing = null;
        }
        ctx.destroyEventChecked(&self.completion) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        ctx.destroyGraphExecChecked(&self.graph) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        for (&self.buffers) |*buffer| _ = buffer.freeChecked(ctx) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        self.quota.releaseLegacyRawAfterSdkDeath() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        self.* = .{};
    }
};
const Packed = struct { buffer: Buffer, quota: CacheQuota };
/// Values-only preview of the actual eight arena owners. No SDK handle,
/// quota/lease, task pointer, or allocation authority can escape through it.
/// The producer and chronological cold planner call the SAME selection body.
pub const ArenaPreview = struct {
    const Entry = struct {
        kind: @FieldType(Arena, "kind") = .encoder,
        grid_x: usize = 0,
        grid_y: usize = 0,
        sequence: usize = 0,
        batch: usize = 0,
        mask_kind: u8 = 0,
        sizes: [11]usize = @splat(0),
        last_used: u64 = 0,
        uses: u64 = 0,
        has_backing: bool = false,
        fn sizesMatch(self: *const @This(), sizes: [11]usize) bool {
            return std.mem.eql(usize, &self.sizes, &sizes);
        }
        fn bytes(self: *const @This()) usize {
            var total: usize = 0;
            for (self.sizes) |size| total += size;
            return total;
        }
        fn hasBacking(self: *const @This()) bool {
            return self.has_backing;
        }
    };
    arenas: [8]Entry = @splat(.{}),
    clock: u64 = 0,
    /// Marks only newly projected entries; a combined invocation cannot fund
    /// a key once and evict/recreate it within the same unyielding callback.
    projected: u8 = 0,
    pub fn fromCache(cache: *const Cache) ArenaPreview {
        var result = ArenaPreview{ .clock = cache.clock };
        for (cache.arenas, &result.arenas) |entry, *out| {
            out.* = .{ .kind = entry.kind, .grid_x = entry.grid_x, .grid_y = entry.grid_y, .sequence = entry.sequence, .batch = entry.batch, .mask_kind = entry.mask_kind, .last_used = entry.last_used, .uses = entry.uses, .has_backing = entry.hasBacking() };
            for (entry.buffers, &out.sizes) |buffer, *size| size.* = buffer.len;
        }
        return result;
    }
    pub fn select(self: *const ArenaPreview, request: prepared.Arena) !?Cache.ArenaSelection {
        return selectArenaLayout(self, request);
    }
    /// One actual chronological arena attempt, including original optional
    /// declines and saturating clock/usage. No SDK/allocation happens here.
    pub fn advance(self: *ArenaPreview, request: prepared.Arena) !?Cache.ArenaSelection {
        // arenaFor's pre-selector refusal does not advance its clock.
        if (request.batch == 0 or request.batch > (if (request.kind == .encoder) @as(usize, 4) else 8) or request.sequence == 0 or request.sequence > 8192 or request.batch > graph_rows_limit / request.sequence) return null;
        const selected = self.select(request) catch |err| {
            self.clock +|= 1;
            return err;
        };
        self.clock +|= 1;
        const choice = selected orelse return null;
        if (!choice.hit) {
            if (choice.victims & self.projected != 0) return error.UnsupportedPreparedArenaChronology;
            for (&self.arenas, 0..) |*entry, index| if (choice.victims & (@as(u8, 1) << @intCast(index)) != 0) {
                entry.* = .{};
            };
            self.arenas[choice.index] = .{ .kind = @fromBackingInt(@intCast(@backingInt(request.kind))), .grid_x = request.grid_x, .grid_y = request.grid_y, .sequence = request.sequence, .batch = request.batch, .mask_kind = request.mask_kind, .sizes = request.sizes, .has_backing = choice.bytes != 0 };
            self.projected |= @as(u8, 1) << @intCast(choice.index);
        }
        const entry = &self.arenas[choice.index];
        entry.last_used = self.clock;
        entry.uses +|= 1;
        return choice;
    }
};
fn selectArenaLayout(self: anytype, request: prepared.Arena) !?Cache.ArenaSelection {
    if (request.batch == 0 or request.batch > (if (request.kind == .encoder) @as(usize, 4) else 8) or request.sequence == 0 or request.sequence > 8192 or request.batch > graph_rows_limit / request.sequence) return null;
    for (self.arenas, 0..) |entry, index| if (@backingInt(entry.kind) == @backingInt(request.kind) and entry.grid_x == request.grid_x and entry.grid_y == request.grid_y and entry.sequence == request.sequence and entry.batch == request.batch and entry.mask_kind == request.mask_kind) {
        if (!entry.sizesMatch(request.sizes)) return error.InvalidCudaState;
        return .{ .index = index, .hit = true, .victims = 0, .bytes = entry.bytes() };
    };
    var bytes: usize = 0;
    for (request.sizes) |size| bytes = try std.math.add(usize, bytes, size);
    if (bytes > arena_limit) return null;
    var selected: ?usize = null;
    for (self.arenas, 0..) |entry, index| {
        if (entry.bytes() == 0) {
            selected = index;
            break;
        }
        if (selected == null or entry.last_used < self.arenas[selected.?].last_used) selected = index;
    }
    const index = selected.?;
    const entry = &self.arenas[index];
    const clock = self.clock +| 1;
    if (entry.bytes() != 0 and entry.uses >= 2 and clock -| entry.last_used < 4) return null;
    var retained: usize = 0;
    for (self.arenas) |value| retained = try std.math.add(usize, retained, value.bytes());
    var victims: u8 = if (entry.hasBacking()) @as(u8, 1) << @intCast(index) else 0;
    while (retained - entry.bytes() + bytes > 4 * arena_limit) {
        var victim: ?usize = null;
        for (self.arenas, 0..) |value, candidate| {
            if (candidate == index or value.bytes() == 0 or victims & (@as(u8, 1) << @intCast(candidate)) != 0) continue;
            if (victim == null or value.last_used < self.arenas[victim.?].last_used) victim = candidate;
        }
        const candidate = victim orelse return null;
        const value = &self.arenas[candidate];
        if (value.uses >= 2 and clock -| value.last_used < 4) return null;
        retained -= value.bytes();
        victims |= @as(u8, 1) << @intCast(candidate);
    }
    return .{ .index = index, .hit = false, .victims = victims, .bytes = bytes };
}

pub const Cache = struct {
    weights: std.StringHashMapUnmanaged(Packed) = .empty,
    packed_bytes: usize = 0,
    weight_generation: usize = 0,
    arenas: [8]Arena = @splat(.{}),
    clock: u64 = 0,
    next_arena: usize = 0,

    pub fn retirementLeaseCount(self: *const Cache, controller: *?*memory.AdmissionController) !usize {
        const quotas = @import("cache_retirement.zig");
        var count: usize = 0;
        for (self.arenas) |arena_value| count += try quotas.scanLease(arena_value.quota.peek(), controller);
        var iterator = self.weights.iterator();
        while (iterator.next()) |entry| count += try quotas.scanLease(entry.value_ptr.quota.peek(), controller);
        return count;
    }
    pub fn parkRetirementLeases(self: *Cache, quotas: *@import("cache_retirement.zig").PreparedQuotas) void {
        for (&self.arenas) |*entry| entry.quota.park(quotas);
        var iterator = self.weights.iterator();
        while (iterator.next()) |entry| entry.value_ptr.quota.park(quotas);
    }
    pub fn validateCleanup(self: *const Cache, current: Cleanup) !void {
        for (self.arenas) |entry| try entry.quota.validateCleanup(current);
        var iterator = self.weights.iterator();
        while (iterator.next()) |entry| try entry.value_ptr.quota.validateCleanup(current);
    }
    pub fn clear(self: *Cache, allocator: std.mem.Allocator, ctx: *Context) !void {
        return self.clearWithCleanup(allocator, ctx, .{ .outside_execution = true });
    }
    pub fn clearWithCleanup(self: *Cache, allocator: std.mem.Allocator, ctx: *Context, current: Cleanup) !void {
        try self.validateCleanup(current);
        if (self.weights.capacity() == 0 and self.next_arena == 0) return;
        try ctx.synchronize();
        try self.clearRetiredWithCleanup(allocator, ctx, current);
    }
    /// Fenced detach cleanup. Failure preserves the remaining SDK handles and
    /// all old quota. No second wait or hidden replacement storage is created.
    pub fn clearRetired(self: *Cache, allocator: std.mem.Allocator, ctx: *Context) !void {
        return self.clearRetiredWithCleanup(allocator, ctx, .{ .outside_execution = true });
    }
    pub fn clearRetiredWithCleanup(self: *Cache, allocator: std.mem.Allocator, ctx: *Context, current: Cleanup) !void {
        try self.validateCleanup(current);
        for (&self.arenas) |*entry| {
            if (entry.timing) |*pair| {
                try ctx.destroyEventChecked(&pair.start);
                try ctx.destroyEventChecked(&pair.end);
                entry.timing = null;
            }
            try ctx.destroyEventChecked(&entry.completion);
            try ctx.destroyGraphExecChecked(&entry.graph);
            for (&entry.buffers) |*buffer| _ = try buffer.freeChecked(ctx);
            try entry.quota.releaseAfterSdkDeath(current);
            entry.* = .{};
        }
        var iterator = self.weights.iterator();
        while (iterator.next()) |entry| _ = try entry.value_ptr.buffer.freeChecked(ctx);
        // Host key/table destructors follow every fallible SDK release, so
        // retry after a partial driver failure cannot observe freed map keys.
        iterator = self.weights.iterator();
        while (iterator.next()) |entry| {
            try entry.value_ptr.quota.releaseAfterSdkDeath(current);
            allocator.free(entry.key_ptr.*);
        }
        self.weights.deinit(allocator);
        self.* = .{};
    }

    /// A failed GPU barrier prevents ordinary eviction. After context
    /// destruction owns all device cleanup, release host keys and admission
    /// without making further calls through the lost driver context.
    pub fn releaseAfterContextDestroyed(self: *Cache, allocator: std.mem.Allocator) void {
        for (&self.arenas) |*entry| entry.quota.releaseOutsideExecution();
        var iterator = self.weights.iterator();
        while (iterator.next()) |entry| {
            entry.value_ptr.quota.releaseOutsideExecution();
            allocator.free(entry.key_ptr.*);
        }
        self.weights.deinit(allocator);
        self.* = .{};
    }

    pub fn discardPacked(self: *Cache, allocator: std.mem.Allocator, ctx: *Context, key: []const u8) !void {
        const value = self.weights.getPtr(key) orelse return;
        if (ctx.prepared_cache_funding == null) try value.quota.validateLegacyRaw();
        try ctx.synchronize();
        const bytes = value.buffer.len;
        const request = if (ctx.prepared_cache_funding != null) try prepared.Request.packedSpan(key, bytes) else undefined;
        _ = try value.buffer.freeChecked(ctx); // Failed SDK death preserves map/key/quota.
        var removed = self.weights.fetchRemove(key).?;
        self.packed_bytes -= bytes;
        self.weight_generation +%= 1;
        if (ctx.prepared_cache_funding) |funding|
            funding.rollbackPublished(request, &removed.value.quota)
        else
            try removed.value.quota.releaseLegacyRawAfterSdkDeath();
        allocator.free(removed.key);
    }

    pub fn findPacked(self: *Cache, key: []const u8) ?Buffer {
        return if (self.weights.get(key)) |entry| entry.buffer else null;
    }

    /// None is a policy/admission decline before any work has been queued.
    pub fn allocatePacked(self: *Cache, allocator: std.mem.Allocator, ctx: *Context, key: []const u8, bytes: usize) !?Buffer {
        if (bytes == 0 or bytes > packed_limit - self.packed_bytes) return null;
        var lease = if (ctx.prepared_cache_funding != null) funded: {
            const request = prepared.Request.packedSpan(key, bytes) catch return null;
            break :funded prepared.acquire(ctx, request, .{ .backend_scratch_bytes = bytes }) catch return null;
        } else prepared.acquireRaw(ctx, .{ .backend_scratch_bytes = bytes }) catch return null;
        errdefer lease.releaseAfterSdkDeath();
        var buffer = Buffer.allocWithPurpose(ctx, bytes, .model_cache) catch {
            lease.releaseAfterSdkDeath();
            return null;
        };
        errdefer if (lease.funded()) {
            _ = buffer.freeChecked(ctx) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        } else buffer.free(ctx);
        const name = try allocator.dupe(u8, key);
        errdefer allocator.free(name);
        try self.weights.ensureUnusedCapacity(allocator, 1);
        self.weights.putAssumeCapacity(name, .{ .buffer = buffer, .quota = lease.publishQuota() });
        self.packed_bytes += bytes;
        self.weight_generation +%= 1;
        return buffer;
    }

    pub fn arena(self: *Cache, ctx: *Context, batch: usize, sequence: usize, mask_kind: u8, sizes: [11]usize) !?*Arena {
        return self.arenaFor(ctx, .encoder, batch, sequence, mask_kind, 0, 0, sizes);
    }

    pub fn visionArena(self: *Cache, ctx: *Context, batch: usize, grid_x: usize, grid_y: usize, sizes: [11]usize) !?*Arena {
        if (grid_x == 0 or grid_y == 0) return null;
        const sequence = try std.math.mul(usize, grid_x, grid_y);
        return self.arenaFor(ctx, .vision, batch, sequence, 0, grid_x, grid_y, sizes);
    }

    pub fn audioArena(self: *Cache, ctx: *Context, key: u64, bytes: usize) !?*Arena {
        return self.arenaFor(ctx, .audio, 1, 1, 0, @intCast(key), 0, .{ bytes, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 });
    }
    pub const ArenaSelection = struct { index: usize, hit: bool, victims: u8, bytes: usize };
    pub fn selectArena(self: *const Cache, request: prepared.Arena) !?ArenaSelection {
        return selectArenaLayout(self, request);
    }
    fn arenaFor(self: *Cache, ctx: *Context, kind: @FieldType(Arena, "kind"), batch: usize, sequence: usize, mask_kind: u8, grid_x: usize, grid_y: usize, sizes: [11]usize) !?*Arena {
        const request: prepared.Arena = .{ .kind = @fromBackingInt(@intCast(@backingInt(kind))), .batch = batch, .sequence = sequence, .mask_kind = mask_kind, .grid_x = grid_x, .grid_y = grid_y, .sizes = sizes };
        if (batch == 0 or batch > (if (kind == .encoder) @as(usize, 4) else 8) or sequence == 0 or sequence > 8192 or batch > graph_rows_limit / sequence) return null;
        const preview = self.selectArena(request) catch |err| {
            self.clock +|= 1;
            return err;
        };
        self.clock +|= 1;
        const selected = preview orelse return null;
        if (!selected.hit and ctx.prepared_cache_funding == null) {
            for (self.arenas, 0..) |value, index| if (selected.victims & (@as(u8, 1) << @intCast(index)) != 0 and value.quota == .prepared) return null;
        }
        if (!selected.hit and selected.victims != 0 and ctx.prepared_cache_funding != null) return null;
        const entry = &self.arenas[selected.index];
        if (selected.hit) {
            entry.last_used = self.clock;
            entry.uses +|= 1;
            return entry;
        }
        for (&self.arenas, 0..) |*value, index| if (selected.victims & (@as(u8, 1) << @intCast(index)) != 0) {
            try value.drain(ctx);
            value.deinit(ctx); // Explicit raw path only; bound path proved empty.
        };
        var lease = prepared.acquire(ctx, .{ .arena = request }, .{ .backend_scratch_bytes = selected.bytes }) catch return null;
        errdefer lease.releaseAfterSdkDeath();
        var candidate = Arena{ .kind = kind, .grid_x = grid_x, .grid_y = grid_y, .sequence = sequence, .batch = batch, .mask_kind = mask_kind };
        errdefer for (&candidate.buffers) |*buffer| {
            if (lease.funded()) {
                _ = buffer.freeChecked(ctx) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
            } else buffer.free(ctx);
        };
        for (&candidate.buffers, sizes) |*buffer, size| {
            buffer.* = Buffer.allocWithPurpose(ctx, size, .model_cache) catch {
                for (&candidate.buffers) |*allocated| {
                    if (lease.funded()) {
                        _ = allocated.freeChecked(ctx) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
                    } else allocated.free(ctx);
                }
                lease.releaseAfterSdkDeath();
                return null;
            };
        }
        candidate.last_used = self.clock;
        candidate.uses = 1;
        candidate.quota = lease.publishQuota();
        entry.* = candidate;
        self.next_arena += 1;
        return entry;
    }
};
test "embeddinggemma2 execution cache bounds cohort graph buckets and packed retention" {
    try std.testing.expectEqual(@as(usize, 8), @as(Cache, .{}).arenas.len);
    try std.testing.expect(arena_limit * 4 + packed_limit <= 1024 * 1024 * 1024);
}

test "embeddinggemma2 execution cache retains shared admission and releases graphs before storage" {
    const Mock = struct {
        var next: usize = 256;
        var frees: usize = 0;
        var graphs: usize = 0;
        fn current(_: driver.CUcontext) callconv(.c) driver.CUresult {
            return 0;
        }
        fn sync(_: driver.CUstream) callconv(.c) driver.CUresult {
            return 0;
        }
        fn alloc(ptr: *driver.CUdeviceptr, _: usize) callconv(.c) driver.CUresult {
            ptr.* = next;
            next += 256;
            return 0;
        }
        fn free(_: driver.CUdeviceptr) callconv(.c) driver.CUresult {
            frees += 1;
            return 0;
        }
        fn graph(_: driver.CUgraphExec) callconv(.c) driver.CUresult {
            graphs += 1;
            return 0;
        }
    };
    Mock.next = 256;
    Mock.frees = 0;
    Mock.graphs = 0;
    var controller = memory.AdmissionController{};
    var ctx: Context = undefined;
    ctx.prepared_cache_funding = null;
    ctx.observation.store(null, .release);
    ctx.observation_enable_failed.store(false, .release);
    ctx.ctx = @ptrFromInt(1);
    ctx.stream = @ptrFromInt(2);
    ctx.stats = .{};
    ctx.device_allocations = .{ .limit = 4096 };
    ctx.video_cache_budget = .{ .admission = .{ .controller = &controller, .limits = .{ .backend_limit_bytes = 1024 }, .check_live_memory = false } };
    ctx.driver.fns.cuCtxSetCurrent = Mock.current;
    ctx.driver.fns.cuStreamSynchronize = Mock.sync;
    ctx.driver.fns.cuMemAlloc = Mock.alloc;
    ctx.driver.fns.cuMemFree = Mock.free;
    ctx.driver.fns.cuGraphExecDestroy = Mock.graph;
    var cache = Cache{};
    defer cache.clear(std.testing.allocator, &ctx) catch unreachable;
    try std.testing.expect((try cache.arena(&ctx, 4, 1025, 0, @splat(16))) == null);
    const first = (try cache.arena(&ctx, 1, 64, 1, @splat(16))).?;
    try std.testing.expect(first == (try cache.arena(&ctx, 1, 64, 1, @splat(16))).?);
    try std.testing.expectError(error.InvalidCudaState, cache.arena(&ctx, 1, 64, 1, @splat(32)));
    const pointer = first.buffers[0].ptr;
    first.graph = @ptrFromInt(3);
    const cohort = (try cache.arena(&ctx, 1, 64, 2, @splat(16))).?;
    try std.testing.expect(first != cohort);
    try std.testing.expectEqual(pointer, first.buffers[0].ptr);
    _ = (try cache.allocatePacked(std.testing.allocator, &ctx, "qkv", 512)).?;
    try std.testing.expectEqual(@as(usize, 864), controller.snapshot().backend_scratch_bytes);
    // The second owner has physical room but must respect shared pressure.
    var other = Cache{};
    defer other.clear(std.testing.allocator, &ctx) catch unreachable;
    try std.testing.expect((try other.allocatePacked(std.testing.allocator, &ctx, "gate", 256)) == null);
    try std.testing.expectEqual(@as(usize, 864), ctx.device_allocations.snapshot().live);
    try cache.clear(std.testing.allocator, &ctx);
    try std.testing.expectEqual(@as(usize, 1), Mock.graphs);
    try std.testing.expectEqual(@as(usize, 23), Mock.frees);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), ctx.device_allocations.snapshot().live);
    _ = (try other.allocatePacked(std.testing.allocator, &ctx, "gate", 256)).?;
    try other.discardPacked(std.testing.allocator, &ctx, "gate");
    try std.testing.expectEqual(@as(usize, 0), ctx.device_allocations.snapshot().live);
    const large = (try cache.arena(&ctx, 4, 1024, 1, @splat(16))).?;
    try std.testing.expect(large == (try cache.arena(&ctx, 4, 1024, 1, @splat(16))).?);
    const smaller_batch = (try cache.arena(&ctx, 2, 1024, 1, @splat(16))).?;
    try std.testing.expect(large != smaller_batch);
    const vision = (try cache.visionArena(&ctx, 2, 32, 32, @splat(16))).?;
    try std.testing.expect(vision != smaller_batch);
    vision.geometry_ready = true;
    try std.testing.expect(vision == (try cache.visionArena(&ctx, 2, 32, 32, @splat(16))).?);
    try std.testing.expect(vision.geometry_ready);
    const rotated = (try cache.visionArena(&ctx, 2, 64, 16, @splat(16))).?;
    try std.testing.expect(vision != rotated and !rotated.geometry_ready);
    try std.testing.expectEqual(@as(usize, 704), controller.snapshot().backend_scratch_bytes);
    try std.testing.expect((try cache.visionArena(&ctx, 8, 32, 32, @splat(16))) == null);
}

test "embeddinggemma2 graph cancellation keeps capture retryable" {
    var entry = Arena{ .warm = true };
    entry.captureFailed(error.Cancelled);
    try std.testing.expect(!entry.disabled and !entry.warm);
    entry.warm = true;
    entry.captureFailed(error.Timeout);
    try std.testing.expect(!entry.disabled and !entry.warm);
    entry.captureFailed(error.CudaGraphCaptureUnsafeHostCopy);
    try std.testing.expect(entry.disabled);
}

test "embeddinggemma2 execution cache releases admission after a lost context is destroyed" {
    var controller = memory.AdmissionController{};
    const lease = try controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 8 }, false);
    var cache = Cache{};
    const name = try std.testing.allocator.dupe(u8, "lost");
    try cache.weights.put(std.testing.allocator, name, .{ .buffer = .{ .ptr = 1, .len = 8 }, .quota = CacheQuota.fromRawAdmission(lease) });
    try std.testing.expectEqual(@as(usize, 8), controller.snapshot().backend_scratch_bytes);
    cache.releaseAfterContextDestroyed(std.testing.allocator);
    cache.releaseAfterContextDestroyed(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), cache.weights.capacity());
}

test "embeddinggemma2 graph selection measures completed warm routes with bounded exploration" {
    var entry = Arena{};
    try std.testing.expect(entry.preferReplay());
    entry.recordCompleted(true, 100);
    entry.recordCompleted(true, 100);
    try std.testing.expect(!entry.preferReplay());
    entry.recordCompleted(false, 50);
    entry.recordCompleted(false, 50);
    try std.testing.expect(!entry.preferReplay());
    entry.timed_uses = 64;
    try std.testing.expect(entry.preferReplay());
    entry.resetTiming();
    entry.recordCompleted(true, 40);
    entry.recordCompleted(true, 40);
    entry.recordCompleted(false, 100);
    entry.recordCompleted(false, 100);
    try std.testing.expect(entry.preferReplay());
}

test "embeddinggemma2 graph retention keeps mixed stages and fences only replaced arenas" {
    const Mock = struct {
        var next: usize = 256;
        var barriers: usize = 0;
        var waits: usize = 0;
        fn current(_: driver.CUcontext) callconv(.c) driver.CUresult {
            return 0;
        }
        fn sync(_: driver.CUstream) callconv(.c) driver.CUresult {
            barriers += 1;
            return 0;
        }
        fn alloc(p: *driver.CUdeviceptr, _: usize) callconv(.c) driver.CUresult {
            p.* = next;
            next += 256;
            return 0;
        }
        fn free(_: driver.CUdeviceptr) callconv(.c) driver.CUresult {
            return 0;
        }
        fn event(p: *driver.CUevent, _: c_uint) callconv(.c) driver.CUresult {
            p.* = @ptrFromInt(next);
            next += 256;
            return 0;
        }
        fn record(_: driver.CUevent, _: driver.CUstream) callconv(.c) driver.CUresult {
            return 0;
        }
        fn wait(_: driver.CUevent) callconv(.c) driver.CUresult {
            waits += 1;
            return 0;
        }
        fn destroy(_: driver.CUevent) callconv(.c) driver.CUresult {
            return 0;
        }
    };
    Mock.next = 256;
    Mock.barriers = 0;
    Mock.waits = 0;
    var ctx: Context = undefined;
    ctx.prepared_cache_funding = null;
    ctx.observation.store(null, .release);
    ctx.observation_enable_failed.store(false, .release);
    ctx.ctx = @ptrFromInt(1);
    ctx.stream = @ptrFromInt(2);
    ctx.stats = .{};
    ctx.device_allocations = .{ .limit = 1024 * 1024 };
    ctx.video_cache_budget = .{};
    ctx.driver.fns.cuCtxSetCurrent = Mock.current;
    ctx.driver.fns.cuStreamSynchronize = Mock.sync;
    ctx.driver.fns.cuMemAlloc = Mock.alloc;
    ctx.driver.fns.cuMemFree = Mock.free;
    ctx.driver.fns.cuEventCreate = Mock.event;
    ctx.driver.fns.cuEventRecord = Mock.record;
    ctx.driver.fns.cuEventSynchronize = Mock.wait;
    ctx.driver.fns.cuEventDestroy = Mock.destroy;
    var cache = Cache{};
    defer cache.clear(std.testing.allocator, &ctx) catch unreachable;
    var sizes: [11]usize = @splat(0);
    sizes[0] = 16;
    var first: usize = 0;
    for (1..9) |seq| {
        const slot = (try cache.arena(&ctx, 1, seq, 0, sizes)).?;
        if (seq == 1) first = slot.buffers[0].ptr;
        slot.noteUse(&ctx);
    }
    try std.testing.expectEqual(@as(usize, 0), Mock.barriers);
    try std.testing.expectEqual(@as(usize, 0), Mock.waits);
    try std.testing.expectEqual(first, (try cache.arena(&ctx, 1, 1, 0, sizes)).?.buffers[0].ptr);
    _ = try cache.visionArena(&ctx, 1, 3, 3, sizes);
    try std.testing.expectEqual(@as(usize, 1), Mock.waits);
    try std.testing.expectEqual(@as(usize, 0), Mock.barriers);
    try std.testing.expectEqual(first, (try cache.arena(&ctx, 1, 1, 0, sizes)).?.buffers[0].ptr);
}

test "arena cold chronology uses same selector for full and partial batches without mutating original cache" {
    const cache: Cache = .{};
    var cursor = ArenaPreview.fromCache(&cache);
    const full: prepared.Arena = .{ .kind = .vision, .batch = 4, .sequence = 16, .grid_x = 4, .grid_y = 4, .sizes = .{ 1024, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } };
    var partial = full;
    partial.batch = 1;
    partial.sizes[0] = 256;
    try std.testing.expectEqualDeep(try cache.selectArena(full), try cursor.select(full));
    const first = (try cursor.advance(full)).?;
    try std.testing.expect(!first.hit and first.index == 0 and first.victims == 0);
    for (0..2) |_| {
        const next = (try cursor.advance(full)).?;
        try std.testing.expect(next.hit and next.index == first.index);
    }
    const last = (try cursor.advance(partial)).?;
    try std.testing.expect(!last.hit and last.index == 1 and last.victims == 0);
    try std.testing.expectEqual(@as(u64, 3), cursor.arenas[first.index].uses);
    try std.testing.expectEqual(@as(u64, 4), cursor.clock);
    try std.testing.expectEqualDeep(Cache{}, cache);
    var invalid = full;
    invalid.batch = 0;
    try std.testing.expect(try cursor.advance(invalid) == null);
    try std.testing.expectEqual(@as(u64, 4), cursor.clock);
}

test "arena cold chronology preserves hot-shape aging and exact aggregate victims" {
    var cache: Cache = .{ .clock = 100 };
    for (&cache.arenas, 0..) |*entry, index| {
        entry.kind = .vision;
        entry.batch = 1;
        entry.sequence = 16;
        entry.grid_x = 10 + index;
        entry.grid_y = 1;
        entry.buffers[0].len = 16; // Pure selection fixture; no SDK owner exists.
        entry.last_used = 100;
        entry.uses = 2;
    }
    var cursor = ArenaPreview.fromCache(&cache);
    const request: prepared.Arena = .{ .kind = .vision, .batch = 4, .sequence = 16, .grid_x = 4, .grid_y = 4, .sizes = .{ 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } };
    for (0..3) |_| try std.testing.expect(try cursor.advance(request) == null);
    const aged = (try cursor.advance(request)).?;
    try std.testing.expect(!aged.hit and aged.index == 0 and aged.victims == 1);
    try std.testing.expectEqual(@as(u64, 104), cursor.clock);
    try std.testing.expectEqual(@as(u64, 100), cache.clock);
    var bounded: Cache = .{ .clock = 20 };
    for (&bounded.arenas, 0..) |*entry, index| {
        entry.batch = 1;
        entry.sequence = index + 1;
        entry.buffers[0].len = arena_limit / 2;
        entry.last_used = index + 1;
        entry.uses = 1;
    }
    var replacement = ArenaPreview.fromCache(&bounded);
    var wide = request;
    wide.sizes[0] = arena_limit;
    const first = (try replacement.advance(wide)).?;
    try std.testing.expectEqual(@as(u8, 3), first.victims);
    // Four distinct one-limit arenas fit the exact existing four-limit cache.
    for (1..4) |index| {
        wide.grid_x += 1;
        const selected = (try replacement.advance(wide)).?;
        try std.testing.expectEqual(index, selected.index);
    }
    wide.grid_x += 1;
    try std.testing.expectError(error.UnsupportedPreparedArenaChronology, replacement.advance(wide));
}

test "CUDA retained cache purposes reach uniform report across checked free failure and warm replay ownership" {
    const roots = @import("../../execution/device_allocation_registry.zig");
    const Purpose = @import("allocation_budget.zig").OwnedPurpose;
    const Mock = struct {
        var next: usize = 256;
        var fail_free: bool = false;
        fn current(_: driver.CUcontext) callconv(.c) driver.CUresult {
            return 0;
        }
        fn sync(_: driver.CUstream) callconv(.c) driver.CUresult {
            return 0;
        }
        fn alloc(ptr: *driver.CUdeviceptr, _: usize) callconv(.c) driver.CUresult {
            ptr.* = next;
            next += 256;
            return 0;
        }
        fn free(_: driver.CUdeviceptr) callconv(.c) driver.CUresult {
            return if (fail_free) 1 else 0;
        }
    };
    Mock.next = 256;
    Mock.fail_free = false;
    var controller = memory.AdmissionController{};
    var ctx: Context = undefined;
    ctx.prepared_cache_funding = null;
    ctx.observation.store(null, .release);
    ctx.observation_enable_failed.store(false, .release);
    ctx.ctx = @ptrFromInt(1);
    ctx.stream = @ptrFromInt(2);
    ctx.stats = .{};
    ctx.device_allocations = .{ .limit = 4096 };
    ctx.video_cache_budget = .{ .admission = .{ .controller = &controller, .limits = .{ .backend_limit_bytes = 4096 }, .check_live_memory = false } };
    ctx.driver.fns.cuCtxSetCurrent = Mock.current;
    ctx.driver.fns.cuStreamSynchronize = Mock.sync;
    ctx.driver.fns.cuMemAlloc = Mock.alloc;
    ctx.driver.fns.cuMemFree = Mock.free;
    var registry: roots.Registry = undefined;
    registry.begin(std.testing.allocator);
    var registry_live = true;
    defer {
        if (registry_live) _ = registry.end();
    }
    defer ctx.device_allocations.releaseObservation();
    ctx.device_allocations.registerObservation();
    var cache: Cache = .{};
    defer {
        Mock.fail_free = false;
        cache.clear(std.testing.allocator, &ctx) catch unreachable;
    }
    const packed_buffer = (try cache.allocatePacked(std.testing.allocator, &ctx, "telemetry.packed", 96)).?;
    try std.testing.expectEqual(Purpose.model_cache, packed_buffer.allocation_purpose);
    Mock.fail_free = true;
    try std.testing.expectError(error.CudaDriverError, cache.clear(std.testing.allocator, &ctx));
    const failed = registry.snapshot().reports[0];
    try std.testing.expectEqual(@as(u64, 96), failed.live_bytes);
    try std.testing.expectEqual(@as(u64, 96), failed.purpose_live_bytes[@backingInt(Purpose.model_cache)]);
    try std.testing.expectEqual(@as(u64, 0), failed.frees);
    try std.testing.expectEqual(packed_buffer.ptr, cache.findPacked("telemetry.packed").?.ptr);
    Mock.fail_free = false;
    const arena = (try cache.arena(&ctx, 1, 16, 0, @splat(16))).?;
    for (arena.buffers) |buffer| try std.testing.expectEqual(Purpose.model_cache, buffer.allocation_purpose);
    const allocations = ctx.device_allocations.snapshot().allocations;
    try std.testing.expect(arena == (try cache.arena(&ctx, 1, 16, 0, @splat(16))).?);
    try std.testing.expectEqual(allocations, ctx.device_allocations.snapshot().allocations);
    var unknown = try Buffer.alloc(&ctx, 32);
    defer _ = unknown.freeChecked(&ctx) catch unreachable;
    var command = try Buffer.allocWithPurpose(&ctx, 64, .command_workspace);
    defer _ = command.freeChecked(&ctx) catch unreachable;
    const live = registry.snapshot().reports[0];
    try std.testing.expectEqual(@as(u64, 272), live.purpose_live_bytes[@backingInt(Purpose.model_cache)]);
    try std.testing.expectEqual(@as(u64, 32), live.purpose_live_bytes[@backingInt(Purpose.unclassified)]);
    try std.testing.expectEqual(@as(u64, 64), live.purpose_live_bytes[@backingInt(Purpose.command_workspace)]);
    try cache.clear(std.testing.allocator, &ctx);
    _ = try unknown.freeChecked(&ctx);
    _ = try command.freeChecked(&ctx);
    const summary = registry.end();
    registry_live = false;
    try std.testing.expectEqual(@as(u64, 0), summary.reports[0].live_bytes);
    try std.testing.expectEqual(@as(u64, 272), summary.reports[0].purpose_interval_peak_bytes[@backingInt(Purpose.model_cache)]);
    var diagnostics = @import("../../execution/diagnostics.zig").Diagnostics{};
    diagnostics.configure(true);
    diagnostics.publish(.{ .runtime = .{ .device_allocator_roots = summary } });
    const retained = diagnostics.snapshot().?.report.?.device_allocator_roots.?;
    diagnostics.configure(false);
    try std.testing.expectEqualDeep(summary, retained);
    try std.testing.expectEqual(.exact_authored_owning_lifetimes_with_explicit_unclassified_remainder, retained.reports[0].semantic_lifetime_classes);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}
