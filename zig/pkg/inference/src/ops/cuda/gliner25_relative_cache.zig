// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Bounded immutable BF16 relative projections. Inputs never depend on text.
//! Entries own independent snapshots and one retained shared-admission lease;
//! a denied reservation preserves the ordinary uncached forward.
const std = @import("std");
const Buffer = @import("buffer.zig").DeviceBuffer;
const Context = @import("context.zig").CudaContext;
const Kernels = @import("kernels.zig").KernelModule;
const Memory = @import("../../runtime/tier/memory.zig");
pub const max_bytes = 96 * 1024 * 1024;
pub const max_entries = 96;
pub const Key = struct {
    sources: [7]u64,
    generation: u64,
    sequence: usize,
    rows: usize,
    hidden: usize,
    layout: u32,
    eps_bits: u32,
    buckets: u32,
    positions: u32,
};
pub const BiasKey = struct { source: u64, generation: u64, count: usize };
pub const BiasEntry = struct { buffer: Buffer, lease: ?Memory.AdmissionLease = null };
pub const max_bias_bytes = 512 * 1024;
pub const max_norm_bytes = 8 * 1024 * 1024;
pub const Entry = struct { buffers: [2]Buffer, lease: ?Memory.AdmissionLease = null };
pub const Cache = struct {
    entries: std.AutoHashMapUnmanaged(Key, Entry) = .empty,
    bytes: usize = 0,
    biases: std.AutoHashMapUnmanaged(BiasKey, BiasEntry) = .empty,
    bias_bytes: usize = 0,
    norm: Buffer = .{},
    norm_lease: ?Memory.AdmissionLease = null,
    norm_generation: u64 = 0,
    norm_source: u64 = 0,
    norm_count: usize = 0,
    pub fn get(self: *const Cache, key: Key) ?[2]Buffer {
        return if (self.entries.get(key)) |entry| entry.buffers else null;
    }
    pub fn publish(self: *Cache, allocator: std.mem.Allocator, ctx: *Context, key: Key, sources: [2]Buffer) !bool {
        return self.publishUsingCopy(allocator, ctx, key, sources, Buffer.copyFromDevice);
    }
    fn publishUsingCopy(self: *Cache, allocator: std.mem.Allocator, ctx: *Context, key: Key, sources: [2]Buffer, comptime copy: anytype) !bool {
        if (self.entries.contains(key)) return true;
        if (key.rows == 0 or key.hidden == 0) return error.InvalidShape;
        if (key.rows > 512 or key.hidden > 1024) return false;
        const single = key.rows * key.hidden * 2;
        const bytes = single * 2;
        if (self.bytes > max_bytes or bytes > max_bytes - self.bytes or self.entries.count() >= max_entries or try ctx.streamCaptureActive()) return false;
        for (sources) |source| if (source.ptr == 0 or source.len < single) return error.InvalidShape;
        self.entries.ensureUnusedCapacity(allocator, 1) catch return false;
        var lease: ?Memory.AdmissionLease = null;
        if (ctx.video_cache_budget.admission) |policy| lease = policy.controller.tryAcquire(.gpu, policy.limits, .{ .backend_weight_bytes = bytes }, policy.check_live_memory) catch return false;
        var committed = false;
        defer if (!committed) if (lease) |*active| active.release();
        var buffers: [2]Buffer = @splat(.{});
        var submitted = false;
        defer if (!committed) {
            if (submitted) ctx.synchronize() catch {};
            for (&buffers) |*buffer| buffer.free(ctx);
        };
        for (&buffers) |*buffer| buffer.* = Buffer.alloc(ctx, single) catch return false;
        for (buffers, sources) |buffer, source| {
            // A failed later submission must retire any earlier queued copy
            // before releasing these private buffers and their admission.
            submitted = true;
            try copy(buffer, ctx, source, single);
        }
        // Copies and all consumers use ctx.stream. Publish queued snapshots;
        // source recycling on that stream remains ordered after these reads.
        // Teardown drains before freeing the retained snapshot buffers.
        self.entries.putAssumeCapacityNoClobber(key, .{ .buffers = buffers, .lease = lease });
        self.bytes += bytes;
        committed = true;
        return true;
    }
    pub fn getBias(self: *const Cache, key: BiasKey) ?Buffer {
        return if (self.biases.get(key)) |entry| entry.buffer else null;
    }
    pub fn publishBias(self: *Cache, allocator: std.mem.Allocator, ctx: *Context, kernels: *Kernels, key: BiasKey, source: Buffer) !?Buffer {
        if (self.getBias(key)) |existing| return existing;
        if (key.count == 0 or source.ptr == 0) return error.InvalidShape;
        if (key.count > 4096) return null;
        const bytes = key.count * 2;
        if (source.len < bytes * 2) return error.InvalidShape;
        if (self.bytes > max_bytes or bytes > max_bytes - self.bytes or self.bias_bytes > max_bias_bytes or bytes > max_bias_bytes - self.bias_bytes or self.biases.count() >= 256 or try ctx.streamCaptureActive()) return null;
        self.biases.ensureUnusedCapacity(allocator, 1) catch return null;
        var lease: ?Memory.AdmissionLease = null;
        if (ctx.video_cache_budget.admission) |policy| lease = policy.controller.tryAcquire(.gpu, policy.limits, .{ .backend_weight_bytes = bytes }, policy.check_live_memory) catch return null;
        var committed = false;
        defer if (!committed) if (lease) |*active| active.release();
        var converted = Buffer.alloc(ctx, bytes) catch return null;
        defer if (!committed) {
            // Preserve the original launch error while retiring partial work.
            ctx.synchronize() catch {};
            converted.free(ctx);
        };
        // The immutable weight source outlives the conversion, and same-stream
        // GEMMs consume it only after this launch. No host drain is needed.
        try kernels.launchF32ToBf16(ctx, converted, source, key.count);
        self.biases.putAssumeCapacityNoClobber(key, .{ .buffer = converted, .lease = lease });
        self.bytes += bytes;
        self.bias_bytes += bytes;
        committed = true;
        return converted;
    }
    /// One bounded mutable activation slot. A generation and the exact F32
    /// source extent prevent a retired value or sliced view from borrowing it.
    pub fn normInput(self: *const Cache, source: u64, count: usize, generation: ?u64) ?Buffer {
        if (generation == null or generation.? != self.norm_generation or source != self.norm_source or count != self.norm_count or count == 0 or count > max_norm_bytes / 2 or self.norm.ptr == 0 or self.norm.len < count * 2) return null;
        return .{ .ptr = self.norm.ptr, .len = count * 2 };
    }
    pub fn reserveNorm(self: *Cache, ctx: *Context, count: usize) !?Buffer {
        if (count == 0 or count > max_norm_bytes / 2 or try ctx.streamCaptureActive()) return null;
        const bytes = count * 2;
        if (self.norm.ptr != 0 and self.norm.len >= bytes) return .{ .ptr = self.norm.ptr, .len = bytes };
        if (self.bytes > max_bytes or bytes > max_bytes - self.bytes) return null;
        var lease: ?Memory.AdmissionLease = null;
        if (ctx.video_cache_budget.admission) |policy| lease = policy.controller.tryAcquire(.gpu, policy.limits, .{ .backend_weight_bytes = bytes }, policy.check_live_memory) catch return null;
        var committed = false;
        defer if (!committed) if (lease) |*active| active.release();
        var next = Buffer.alloc(ctx, bytes) catch return null;
        defer if (!committed) next.free(ctx);
        // Retiring the old slot must drain earlier GEMMs that borrowed it.
        if (self.norm.ptr != 0) try ctx.synchronize();
        self.bytes -= self.norm.len;
        self.norm.free(ctx);
        if (self.norm_lease) |*active| active.release();
        self.norm = next;
        self.norm_lease = lease;
        self.bytes += bytes;
        self.norm_generation +%= 1;
        self.norm_source = 0;
        self.norm_count = 0;
        committed = true;
        return next;
    }
    pub fn invalidateNormForWrite(self: *Cache, pointer: u64, bytes: usize) void {
        if (self.norm_source == 0 or pointer == 0 or bytes == 0) return;
        // Differences avoid overflowing a device address at the range end.
        const overlaps = if (pointer >= self.norm_source)
            pointer - self.norm_source < self.norm_count * 4
        else
            self.norm_source - pointer < bytes;
        if (overlaps) self.invalidateNorm();
    }
    pub fn invalidateNorm(self: *Cache) void {
        self.norm_generation +%= 1;
        self.norm_source = 0;
        self.norm_count = 0;
    }
    pub fn publishNorm(self: *Cache, source: u64, count: usize) u64 {
        std.debug.assert(source != 0 and count > 0 and count <= self.norm.len / 2);
        self.norm_generation +%= 1;
        self.norm_source = source;
        self.norm_count = count;
        return self.norm_generation;
    }
    pub fn deinit(self: *Cache, allocator: std.mem.Allocator, ctx: *Context) void {
        if (self.entries.count() != 0 or self.biases.count() != 0 or self.norm.ptr != 0) ctx.synchronize() catch {};
        var values = self.entries.valueIterator();
        while (values.next()) |entry| {
            for (&entry.buffers) |*buffer| buffer.free(ctx);
            if (entry.lease) |*lease| lease.release();
        }
        var bias_values = self.biases.valueIterator();
        while (bias_values.next()) |entry| {
            entry.buffer.free(ctx);
            if (entry.lease) |*lease| lease.release();
        }
        self.norm.free(ctx);
        if (self.norm_lease) |*lease| lease.release();
        self.biases.deinit(allocator);
        self.entries.deinit(allocator);
        self.* = .{};
    }
};
test "GLiNER BF16 relative cache keys include source generation and full geometry" {
    const key = Key{ .sources = .{ 1, 2, 3, 4, 5, 6, 7 }, .generation = 8, .sequence = 128, .rows = 256, .hidden = 768, .layout = 0, .eps_bits = @bitCast(@as(f32, 1e-7)), .buckets = 256, .positions = 512 };
    var changed = key;
    changed.generation += 1;
    try std.testing.expect(!std.meta.eql(key, changed));
    changed = key;
    changed.sources[2] += 1;
    try std.testing.expect(!std.meta.eql(key, changed));
    changed = key;
    changed.layout += 1;
    try std.testing.expect(!std.meta.eql(key, changed));
    changed = key;
    changed.eps_bits ^= 1;
    try std.testing.expect(!std.meta.eql(key, changed));
}
test "GLiNER BF16 relative cache snapshots are independent bounded and released" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    const a = std.testing.allocator;
    var ctx = Context.initDefault() catch |err| {
        if (@import("antfly_platform").env.getenvBool("TERMITE_REQUIRE_CUDA_TESTS")) return err;
        return error.SkipZigTest;
    };
    defer ctx.deinit();
    var controller = Memory.AdmissionController{};
    ctx.video_cache_budget.admission = .{ .controller = &controller, .limits = .{ .backend_limit_bytes = 1 }, .check_live_memory = false };
    var cache = Cache{};
    defer cache.deinit(a, &ctx);
    const key = Key{ .sources = .{ 1, 2, 3, 4, 5, 6, 7 }, .generation = 8, .sequence = 128, .rows = 128, .hidden = 64, .layout = 0, .eps_bits = @bitCast(@as(f32, 1e-7)), .buckets = 256, .positions = 512 };
    var input = try Buffer.alloc(&ctx, key.rows * key.hidden * 2);
    defer input.free(&ctx);
    const original = try a.alloc(u16, key.rows * key.hidden);
    defer a.free(original);
    @memset(original, 0x3f80);
    try input.copyFromHost(&ctx, std.mem.sliceAsBytes(original));
    try std.testing.expect(!try cache.publish(a, &ctx, key, .{ input, input }));
    try std.testing.expectEqual(@as(usize, 0), cache.entries.count());
    try std.testing.expectEqual(@as(u64, 0), controller.snapshot().backend_weight_bytes);
    ctx.video_cache_budget.admission.?.limits.backend_limit_bytes = max_bytes;
    const syncs_before_publish = ctx.stats.stream_syncs;
    try std.testing.expect(try cache.publish(a, &ctx, key, .{ input, input }));
    try std.testing.expectEqual(syncs_before_publish, ctx.stats.stream_syncs);
    try std.testing.expectEqual(@as(u64, key.rows * key.hidden * 4), controller.snapshot().backend_weight_bytes);
    var next = key;
    next.generation += 1;
    const saved_bytes = cache.bytes;
    cache.bytes = max_bytes;
    try std.testing.expect(!try cache.publish(a, &ctx, next, @splat(.{})));
    cache.bytes = saved_bytes;
    @memset(original, 0x4000);
    try input.copyFromHost(&ctx, std.mem.sliceAsBytes(original));
    const stored = cache.get(key).?;
    try stored[0].copyToHost(&ctx, std.mem.sliceAsBytes(original));
    try ctx.synchronize();
    for (original) |v| try std.testing.expectEqual(@as(u16, 0x3f80), v);
    var replaced = key;
    replaced.generation += 1;
    try std.testing.expect(cache.get(replaced) == null);
    const retained = cache.bytes;
    cache.bytes = max_bytes;
    try std.testing.expect(!try cache.publish(a, &ctx, replaced, .{ input, input }));
    cache.bytes = retained;
    cache.deinit(a, &ctx);
    try std.testing.expectEqual(@as(usize, 0), cache.bytes);
    try std.testing.expectEqual(@as(usize, 0), cache.entries.count());
    try std.testing.expectEqual(@as(u64, 0), controller.snapshot().backend_weight_bytes);
}

test "GLiNER BF16 bias snapshots retain rounding identity and admission" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    var ctx = Context.initDefault() catch |err| {
        if (@import("antfly_platform").env.getenvBool("TERMITE_REQUIRE_CUDA_TESTS")) return err;
        return error.SkipZigTest;
    };
    defer ctx.deinit();
    var kernels = try Kernels.load(&ctx);
    defer kernels.unload(&ctx);
    var cache = Cache{};
    defer cache.deinit(std.testing.allocator, &ctx);
    var source = try Buffer.alloc(&ctx, 16);
    defer source.free(&ctx);
    const values = [_]f32{ 1.001, -0.501, 0.0, 9.125 };
    try source.copyFromHost(&ctx, std.mem.sliceAsBytes(&values));
    const key = BiasKey{ .source = source.ptr, .generation = 1, .count = values.len };
    var controller = Memory.AdmissionController{};
    ctx.video_cache_budget.admission = .{ .controller = &controller, .limits = .{ .backend_limit_bytes = 1 }, .check_live_memory = false };
    try std.testing.expect((try cache.publishBias(std.testing.allocator, &ctx, &kernels, key, source)) == null);
    ctx.video_cache_budget.admission.?.limits.backend_limit_bytes = 8;
    const syncs_before_publish = ctx.stats.stream_syncs;
    const snapshot = (try cache.publishBias(std.testing.allocator, &ctx, &kernels, key, source)).?;
    try std.testing.expectEqual(syncs_before_publish, ctx.stats.stream_syncs);
    var changed = key;
    changed.generation += 1;
    try std.testing.expect(cache.getBias(changed) == null);
    changed = key;
    changed.count -= 1;
    try std.testing.expect(cache.getBias(changed) == null);
    const zeros = [_]f32{ 0, 0, 0, 0 };
    try source.copyFromHost(&ctx, std.mem.sliceAsBytes(&zeros));
    var bits: [4]u16 = undefined;
    try snapshot.copyToHost(&ctx, std.mem.sliceAsBytes(&bits));
    try ctx.synchronize();
    for (bits, values) |actual, value| {
        const raw: u32 = @bitCast(value);
        const expected: u16 = @intCast((raw + 0x7fff + ((raw >> 16) & 1)) >> 16);
        try std.testing.expectEqual(expected, actual);
    }
    try std.testing.expectEqual(@as(u64, 8), controller.snapshot().backend_weight_bytes);
    try std.testing.expectEqual(@as(usize, 8), cache.bias_bytes);
    cache.bias_bytes = max_bias_bytes;
    changed = key;
    changed.generation += 1;
    try std.testing.expect((try cache.publishBias(std.testing.allocator, &ctx, &kernels, changed, source)) == null);
    cache.bias_bytes = 8;
    cache.deinit(std.testing.allocator, &ctx);
    try std.testing.expectEqual(@as(u64, 0), controller.snapshot().backend_weight_bytes);
}

test "GLiNER BF16 norm slot invalidates generations and retains bounded admission" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    var ctx = Context.initDefault() catch |err| {
        if (@import("antfly_platform").env.getenvBool("TERMITE_REQUIRE_CUDA_TESTS")) return err;
        return error.SkipZigTest;
    };
    defer ctx.deinit();
    var controller = Memory.AdmissionController{};
    ctx.video_cache_budget.admission = .{ .controller = &controller, .limits = .{ .backend_limit_bytes = 32 }, .check_live_memory = false };
    var cache = Cache{};
    defer cache.deinit(std.testing.allocator, &ctx);
    try std.testing.expect((try cache.reserveNorm(&ctx, max_norm_bytes)) == null);
    try std.testing.expect((try cache.reserveNorm(&ctx, 32)) == null);
    const first = (try cache.reserveNorm(&ctx, 8)).?;
    const generation = cache.publishNorm(1234, 8);
    try std.testing.expectEqual(first.ptr, cache.normInput(1234, 8, generation).?.ptr);
    try std.testing.expect(cache.normInput(1235, 8, generation) == null);
    try std.testing.expect(cache.normInput(1234, 7, generation) == null);
    _ = cache.publishNorm(1234, 8);
    try std.testing.expect(cache.normInput(1234, 8, generation) == null);
    ctx.video_cache_budget.admission.?.limits.backend_limit_bytes = 64;
    _ = try cache.reserveNorm(&ctx, 16);
    try std.testing.expect(cache.normInput(1234, 8, cache.norm_generation) == null);
    try std.testing.expectEqual(@as(u64, 32), controller.snapshot().backend_weight_bytes);
    cache.deinit(std.testing.allocator, &ctx);
    try std.testing.expectEqual(@as(u64, 0), controller.snapshot().backend_weight_bytes);
}

test "GLiNER BF16 norm writes invalidate partial overlaps without touching adjacent ranges" {
    var cache = Cache{ .norm = .{ .ptr = 200, .len = 16 } };
    for ([_]struct { pointer: u64, bytes: usize, overlaps: bool }{
        .{ .pointer = 1000, .bytes = 32, .overlaps = true },
        .{ .pointer = 1004, .bytes = 4, .overlaps = true },
        .{ .pointer = 996, .bytes = 8, .overlaps = true },
        .{ .pointer = 1031, .bytes = 4, .overlaps = true },
        .{ .pointer = 996, .bytes = 4, .overlaps = false },
        .{ .pointer = 1032, .bytes = 4, .overlaps = false },
        .{ .pointer = 1004, .bytes = 0, .overlaps = false },
        .{ .pointer = 0, .bytes = 8, .overlaps = false },
    }) |write| {
        const generation = cache.publishNorm(1000, 8);
        cache.invalidateNormForWrite(write.pointer, write.bytes);
        try std.testing.expectEqual(!write.overlaps, cache.normInput(1000, 8, generation) != null);
    }
}

test "GLiNER BF16 relative cache failed second copy drains and releases unpublished snapshots" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    const Fault = struct {
        var calls: usize = 0;
        fn copy(dst: Buffer, ctx: *Context, source: Buffer, bytes: usize) !void {
            calls += 1;
            if (calls == 2) return error.InjectedSnapshotCopyFailure;
            try dst.copyFromDevice(ctx, source, bytes);
        }
    };
    var ctx = Context.initDefault() catch |err| {
        if (@import("antfly_platform").env.getenvBool("TERMITE_REQUIRE_CUDA_TESTS")) return err;
        return error.SkipZigTest;
    };
    defer ctx.deinit();
    var controller = Memory.AdmissionController{};
    ctx.video_cache_budget.admission = .{ .controller = &controller, .limits = .{ .backend_limit_bytes = max_bytes }, .check_live_memory = false };
    var cache = Cache{};
    defer cache.deinit(std.testing.allocator, &ctx);
    const key = Key{ .sources = .{ 1, 2, 3, 4, 5, 6, 7 }, .generation = 1, .sequence = 128, .rows = 256, .hidden = 768, .layout = 0, .eps_bits = 0, .buckets = 256, .positions = 512 };
    var source = try Buffer.alloc(&ctx, key.rows * key.hidden * 2);
    defer source.free(&ctx);
    const live = ctx.device_allocations.snapshot().live;
    const syncs = ctx.stats.stream_syncs;
    Fault.calls = 0;
    try std.testing.expectError(error.InjectedSnapshotCopyFailure, cache.publishUsingCopy(std.testing.allocator, &ctx, key, .{ source, source }, Fault.copy));
    try std.testing.expectEqual(@as(usize, 2), Fault.calls);
    try std.testing.expectEqual(syncs + 1, ctx.stats.stream_syncs);
    try std.testing.expectEqual(live, ctx.device_allocations.snapshot().live);
    try std.testing.expectEqual(@as(usize, 0), cache.entries.count());
    try std.testing.expectEqual(@as(usize, 0), cache.bytes);
    try std.testing.expectEqual(@as(u64, 0), controller.snapshot().backend_weight_bytes);
}
