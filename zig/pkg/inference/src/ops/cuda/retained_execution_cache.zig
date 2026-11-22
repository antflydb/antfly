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

//! Retained CUDA execution storage. A comptime recipe supplies typed capture
//! identity and validated workload policy; buffers, graphs, fences and quotas
//! retain one implementation with no model configuration dependency.
const std = @import("std");
const Context = @import("context.zig").CudaContext;
const Buffer = @import("buffer.zig").DeviceBuffer;
const driver = @import("driver.zig");
const memory = @import("../../runtime/tier/memory.zig");
const prepared = @import("prepared_cache.zig");
const CacheQuota = @import("cache_quota.zig").Quota;
const Cleanup = @import("../../execution/cleanup_context.zig").Context;

pub fn Program(comptime Recipe: type) type {
    return struct {
        pub const packed_limit = Recipe.packed_limit;
        pub const arena_limit = Recipe.arena_limit;
        pub const graph_rows_limit = Recipe.graph_rows_limit;
        pub const LogicalUpload = struct {
            values: [2]u32 = .{ 0, 0 },
            captured: ?Recipe.Capture = null,
        };
        pub fn logicalUploadHostBytes(kind: @FieldType(prepared.Arena, "kind")) usize {
            return if (kind == .encoder) @sizeOf(LogicalUpload) else 0;
        }
        /// Whole-source PLE projection/normalization bank. Slicing the GEMM output
        /// width changes vendor reduction arithmetic, so every layer shares full N.
        pub fn layerInputBytes(rows: usize, hidden: usize, layers: usize) !usize {
            return std.math.mul(usize, try std.math.mul(usize, try std.math.mul(usize, rows, hidden), layers), @sizeOf(u16));
        }
        pub fn maskStorageBytes(rows: usize) !usize {
            return std.math.add(usize, try std.math.mul(usize, rows, 8), 8);
        }
        pub const AttentionSource = struct {
            sequence: usize,
            all_valid: bool,
            mask_kind: u8 = 0,
            pub fn eql(self: AttentionSource, other: AttentionSource) bool {
                return self.sequence == other.sequence and self.all_valid == other.all_valid and self.mask_kind == other.mask_kind;
            }
        };

        pub const Arena = struct {
            kind: enum { encoder, vision, audio } = .encoder,
            grid_x: usize = 0,
            grid_y: usize = 0,
            geometry_ready: bool = false,
            sequence: usize = 0,
            batch: usize = 0,
            mask_kind: u8 = 0,
            attention_workspace: usize = 0,
            attention_program_key: u32 = 0,
            /// Captured scalar arguments, separate from the reusable physical banks.
            attention_source: ?AttentionSource = null,
            logical_upload: ?*LogicalUpload = null,
            global_attention_workspace: usize = 0,
            global_attention_generation: u64 = 0,
            blas_workspace: usize = 0,
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
            /// Ambiguous driver failure makes this graph ineligible for reuse.
            /// Retire it after the stream completes and keep the funded banks
            /// available to the existing ordinary schedule.
            pub fn launch(self: *Arena, ctx: *Context) !void {
                if (self.graph == null) return error.InvalidCudaState;
                ctx.launchGraph(self.graph) catch |err| {
                    ctx.synchronize() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
                    self.invalidateGraph(ctx) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
                    self.disabled = true;
                    return err;
                };
            }

            pub fn invalidateGraph(self: *Arena, ctx: *Context) !void {
                // Bank allocation and quota stay untouched. Publish no replacement
                // captured-argument stamp until the actual eager/capture path succeeds.
                try ctx.destroyGraphExecChecked(&self.graph);
                self.warm = false;
                self.attention_source = null;
                self.resetTiming();
            }
            pub fn attentionSourceMatches(self: *const Arena, source: AttentionSource) bool {
                return if (self.attention_source) |captured| captured.eql(source) else false;
            }
            fn encoderGeometry(self: *const Arena) !Recipe.Geometry {
                return Recipe.encoderGeometry(self.kind, self.batch, self.sequence);
            }
            /// Compatibility is checked by the statically bound program recipe. The
            /// cache owns storage/capture retirement and never interprets model config.
            pub fn prepareAttentionSource(self: *Arena, ctx: *Context, source: AttentionSource, capture: Recipe.Capture) !void {
                const geometry = try self.encoderGeometry();
                try Recipe.validateCapture(capture);
                if (source.sequence == 0 or source.sequence > self.sequence or source.mask_kind > 2 or source.all_valid != (source.mask_kind == 0)) return Recipe.invalid_geometry;
                const sequence = std.math.cast(u32, source.sequence) orelse return Recipe.invalid_geometry;
                var compatible = false;
                if (self.attention_source) |old| if (self.logical_upload) |upload| if (upload.captured) |captured| {
                    compatible = try Recipe.compatible(geometry, old, source, captured, capture, ctx.info.multiprocessor_count);
                };
                _ = sequence;
                if (!compatible) try self.invalidateGraph(ctx) else if (!self.attentionSourceMatches(source)) self.resetTiming();
            }
            pub fn noteAttentionSource(self: *Arena, source: AttentionSource, capture: Recipe.Capture) void {
                self.attention_source = source;
                if (self.logical_upload) |upload| upload.captured = capture;
            }
            /// The host cell survives cache moves and every asynchronous upload. Drain
            /// the actual previous use BEFORE rewriting it; no ring/stack address is saved.
            pub fn uploadLogical(self: *Arena, ctx: *Context, device: Buffer, source: AttentionSource) !void {
                _ = try self.encoderGeometry();
                const source_sequence = std.math.cast(u32, source.sequence) orelse return Recipe.invalid_geometry;
                const upload = self.logical_upload orelse return error.InvalidCudaState;
                if (device.ptr == 0 or device.len != 8 or device.ptr % 4 != 0 or source.sequence == 0 or source.sequence > self.sequence or source.mask_kind > 2 or source.all_valid != (source.mask_kind == 0)) return error.InvalidCudaState;
                if (try ctx.streamCaptureActive()) return error.CudaGraphCaptureUnsafeHostCopy;
                try self.drain(ctx);
                upload.values = .{ source_sequence, source.mask_kind };
                // Even an upload error may have queued work: retirement falls back to
                // the actual stream barrier until noteUse publishes its event.
                self.pending = true;
                self.fence_valid = false;
                device.copyFromHost(ctx, std.mem.asBytes(&upload.values)) catch |err| {
                    self.attention_source = null;
                    self.warm = false;
                    return err;
                };
            }
            fn sizesMatch(self: *const Arena, sizes: [11]usize) bool {
                for (self.buffers, sizes) |buffer, size| if (buffer.len != size) return false;
                return true;
            }
            pub fn hasBacking(self: *const Arena) bool {
                return self.bytes() != 0 or self.logical_upload != null or self.graph != null or self.completion != null or self.timing != null or self.quota.hasLease();
            }
            pub fn attentionWorkspacesMatch(self: *const Arena, attention: usize, global_attention: usize, global_generation: u64, blas: usize) bool {
                return self.attention_workspace == attention and self.global_attention_workspace == global_attention and self.global_attention_generation == global_generation and self.blas_workspace == blas;
            }
            pub fn setAttentionWorkspaces(self: *Arena, attention: usize, global_attention: usize, global_generation: u64, blas: usize) void {
                self.attention_workspace = attention;
                self.global_attention_workspace = global_attention;
                self.global_attention_generation = global_generation;
                self.blas_workspace = blas;
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
            pub const GraphAction = enum { eager, capture, replay };
            pub fn graphAction(self: *const Arena) GraphAction {
                if (self.graph != null) return if (self.preferReplay()) .replay else .eager;
                return if (self.warm and !self.disabled) .capture else .eager;
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
                if (self.logical_upload) |upload| std.heap.smp_allocator.destroy(upload);
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
                if (!Recipe.acceptsArena(request.kind, request.batch, request.sequence)) return null;
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
                    self.arenas[choice.index] = .{ .kind = @fromBackingInt(@intCast(@backingInt(request.kind))), .grid_x = request.grid_x, .grid_y = request.grid_y, .sequence = request.sequence, .batch = request.batch, .mask_kind = request.mask_kind, .sizes = request.sizes, .has_backing = choice.bytes != 0 or logicalUploadHostBytes(request.kind) != 0 };
                    self.projected |= @as(u8, 1) << @intCast(choice.index);
                }
                const entry = &self.arenas[choice.index];
                entry.last_used = self.clock;
                entry.uses +|= 1;
                return choice;
            }
        };
        fn selectArenaLayout(self: anytype, request: prepared.Arena) !?Cache.ArenaSelection {
            if (!Recipe.acceptsArena(request.kind, request.batch, request.sequence)) return null;
            for (self.arenas, 0..) |entry, index| if (@backingInt(entry.kind) == @backingInt(request.kind) and entry.grid_x == request.grid_x and entry.grid_y == request.grid_y and entry.sequence == request.sequence and entry.batch == request.batch and entry.mask_kind == request.mask_kind) {
                if (!entry.sizesMatch(request.sizes)) return error.InvalidCudaState;
                return .{ .index = index, .hit = true, .victims = 0, .bytes = entry.bytes() };
            };
            var bytes: usize = 0;
            for (request.sizes) |size| bytes = try std.math.add(usize, bytes, size);
            const entry_allowance = try std.math.add(usize, arena_limit, try Recipe.extraEntryBytes(request));
            if (bytes > entry_allowance) return null;
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
                    if (entry.logical_upload) |upload| {
                        std.heap.smp_allocator.destroy(upload);
                        entry.logical_upload = null;
                    }
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
                for (&self.arenas) |*entry| {
                    if (entry.logical_upload) |upload| std.heap.smp_allocator.destroy(upload);
                    entry.quota.releaseOutsideExecution();
                }
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

            pub fn sourceVisionArena(self: *Cache, ctx: *Context, batch: usize, sequence: usize, grid_x: usize, grid_y: usize, sizes: [11]usize) !?*Arena {
                const logical = try std.math.mul(usize, grid_x, grid_y);
                _ = try @import("../kernel_geometry.zig").SourceGemmRows.resolve(logical, sequence);
                return self.arenaFor(ctx, .vision, batch, sequence, 1, grid_x, grid_y, sizes);
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
                if (!Recipe.acceptsArena(kind, batch, sequence)) return null;
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
                var lease = prepared.acquire(ctx, .{ .arena = request }, .{ .backend_scratch_bytes = selected.bytes, .host_scratch_bytes = logicalUploadHostBytes(request.kind) }) catch return null;
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
                if (kind == .encoder) {
                    candidate.logical_upload = std.heap.smp_allocator.create(LogicalUpload) catch {
                        for (&candidate.buffers) |*allocated| {
                            if (lease.funded()) {
                                _ = allocated.freeChecked(ctx) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
                            } else allocated.free(ctx);
                        }
                        lease.releaseAfterSdkDeath();
                        return null;
                    };
                    candidate.logical_upload.?.* = .{};
                }
                candidate.last_used = self.clock;
                candidate.uses = 1;
                candidate.quota = lease.publishQuota();
                entry.* = candidate;
                self.next_arena += 1;
                return entry;
            }
        };
    };
}

test "retained CUDA execution accepts an independent typed recipe without model configuration" {
    const Other = struct {
        pub const packed_limit = 64;
        pub const arena_limit = 64;
        pub const graph_rows_limit = 16;
        pub const invalid_geometry = error.InvalidCudaState;
        pub const Geometry = struct { batch: u32, sequence: u32 };
        pub const Capture = struct { generation: u64 };
        pub fn acceptsArena(_: anytype, batch: usize, sequence: usize) bool {
            return batch == 1 and sequence > 0 and sequence <= 16;
        }
        pub fn encoderGeometry(kind: anytype, batch: usize, sequence: usize) !Geometry {
            if (!acceptsArena(kind, batch, sequence)) return invalid_geometry;
            return .{ .batch = @intCast(batch), .sequence = @intCast(sequence) };
        }
        pub fn validateCapture(_: Capture) !void {}
        pub fn compatible(_: Geometry, _: anytype, _: anytype, a: Capture, b: Capture, _: i32) !bool {
            return std.meta.eql(a, b);
        }
        pub fn extraEntryBytes(_: prepared.Arena) !usize {
            return 0;
        }
    };
    const P = Program(Other);
    const cache: P.Cache = .{};
    var request = prepared.Arena{ .kind = .encoder, .batch = 1, .sequence = 8, .sizes = @splat(0) };
    request.sizes[0] = 64;
    try std.testing.expectEqual(@as(usize, 64), (try cache.selectArena(request)).?.bytes);
    request.sizes[0] = 65;
    try std.testing.expect((try cache.selectArena(request)) == null);
    var upload: P.LogicalUpload = .{};
    var arena = P.Arena{ .batch = 1, .sequence = 8, .logical_upload = &upload, .warm = true };
    var context: Context = undefined;
    context.info.multiprocessor_count = 1;
    arena.noteAttentionSource(.{ .sequence = 4, .all_valid = true }, .{ .generation = 1 });
    try arena.prepareAttentionSource(&context, .{ .sequence = 5, .all_valid = true }, .{ .generation = 1 });
    try std.testing.expect(arena.warm);
    try arena.prepareAttentionSource(&context, .{ .sequence = 5, .all_valid = true }, .{ .generation = 2 });
    try std.testing.expect(!arena.warm and arena.attention_source == null);
}
