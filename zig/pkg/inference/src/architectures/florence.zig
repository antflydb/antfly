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

// Native Florence-2 architecture support.
//
// Implements the official safetensors layout:
// - `vision_tower.*` DaViT vision encoder
// - `image_projection` + `image_proj_norm`
// - `language_model.model.encoder.*`
// - `language_model.model.decoder.*`
//
// The vision tower is hybrid: conv/window/channel pieces run in Zig over f32
// buffers, while dense linear, attention, exact-erf GELU, and LayerNorm use
// the active ComputeBackend so the same code works for native and devices.

const std = @import("std");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const ops = @import("../ops/ops.zig");
const MetalCompute = @import("../ops/metal_compute.zig").MetalCompute;
const CT = ops.CT;
const ComputeBackend = ops.ComputeBackend;
const florence_config = @import("../models/florence.zig");
const activations = @import("../backends/activations.zig");

pub const Config = florence_config.Config;
const ImageFeatureSource = florence_config.ImageFeatureSource;

/// A Florence layer is a compound synchronous operation. Observe cooperative
/// cancellation at its owning boundary, where intermediate tensors can unwind,
/// rather than during individual weight lookups inside that operation. The
/// outer managed compute scope still monitors uninterruptible GPU work.
fn layerComputeBackend(control_cb: *const ComputeBackend) ComputeBackend {
    var cb = control_cb.*;
    cb.execution_control = null;
    return cb;
}

const bart_position_offset: i64 = 2;

pub const EncoderForwardResult = struct {
    hidden: []f32,
    seq_len: usize,
};

pub const EncoderForwardTensorResult = struct {
    hidden: CT,
    seq_len: usize,
};

/// The reader's original checked patch/feature construction, shared with cold
/// decoder preparation. This creates no tensors and chooses no numerical path.
pub fn encoderSequenceLength(cfg: Config, prompt_length: usize) !usize {
    var side: usize = cfg.image_size;
    for (cfg.patch_size, cfg.patch_stride, cfg.patch_padding) |patch, stride, padding| {
        if (stride == 0) return error.InvalidInputShape;
        const padded = try std.math.add(usize, side, try std.math.mul(usize, padding, 2));
        if (padded < patch) return error.InvalidInputShape;
        side = try std.math.add(usize, (padded - patch) / stride, 1);
    }
    if (cfg.image_feature_source_count > cfg.image_feature_sources.len) return error.InvalidInputShape;
    const spatial_tokens = try std.math.mul(usize, side, side);
    var image_tokens: usize = 0;
    for (cfg.image_feature_sources[0..cfg.image_feature_source_count]) |source| image_tokens = try std.math.add(usize, image_tokens, switch (source) {
        .spatial_avg_pool => 1,
        .temporal_avg_pool, .last_frame => spatial_tokens,
    });
    return std.math.add(usize, image_tokens, prompt_length);
}
pub fn decoderKvBytes(cfg: Config, batch: usize, rows: usize, capacity: usize) !usize {
    const length = try std.math.add(usize, rows, capacity);
    return std.math.mul(usize, try std.math.mul(usize, batch, length), try std.math.mul(usize, cfg.decoder_layers, try std.math.mul(usize, cfg.d_model, 2 * @sizeOf(f32))));
}

pub const DecoderCrossCache = struct {
    keys: []CT,
    values: []CT,

    /// Views may borrow their source (CUDA) or retain it (Metal). In either
    /// case the owning cache must outlive this descriptor set.
    fn prefix(self: DecoderCrossCache, cb: *const ComputeBackend, allocator: std.mem.Allocator, rows: usize, cols: usize) !DecoderCrossCache {
        const keys = try allocator.alloc(CT, self.keys.len);
        errdefer allocator.free(keys);
        const values = try allocator.alloc(CT, self.values.len);
        errdefer allocator.free(values);
        var key_count: usize = 0;
        var value_count: usize = 0;
        errdefer {
            for (keys[0..key_count]) |tensor| cb.free(tensor);
            for (values[0..value_count]) |tensor| cb.free(tensor);
        }
        for (self.keys, self.values, 0..) |key, value, layer| {
            keys[layer] = try cb.sliceRows2D(allocator, key, 0, rows, cols);
            key_count += 1;
            values[layer] = try cb.sliceRows2D(allocator, value, 0, rows, cols);
            value_count += 1;
        }
        return .{ .keys = keys, .values = values };
    }

    pub fn deinit(self: *DecoderCrossCache, cb: *const ComputeBackend, allocator: std.mem.Allocator) void {
        for (self.keys) |key| cb.free(key);
        for (self.values) |value| cb.free(value);
        allocator.free(self.keys);
        allocator.free(self.values);
    }
};

pub const DecoderSelfCache = struct {
    keys: []?CT,
    values: []?CT,
    len: usize = 0,
    capacity: usize = 0,
    preallocated: bool = false,

    pub fn init(allocator: std.mem.Allocator, layers: usize) !DecoderSelfCache {
        const keys = try allocator.alloc(?CT, layers);
        errdefer allocator.free(keys);
        const values = try allocator.alloc(?CT, layers);
        errdefer allocator.free(values);
        @memset(keys, null);
        @memset(values, null);
        return .{ .keys = keys, .values = values };
    }

    pub fn tryInitPreallocated(
        cb: *const ComputeBackend,
        allocator: std.mem.Allocator,
        layers: usize,
        batch: usize,
        capacity: usize,
        d_model: usize,
    ) !?DecoderSelfCache {
        if (batch == 0 or capacity == 0) return null;
        const keys = try allocator.alloc(?CT, layers);
        const values = allocator.alloc(?CT, layers) catch |err| {
            allocator.free(keys);
            return err;
        };
        @memset(keys, null);
        @memset(values, null);

        var cache = DecoderSelfCache{
            .keys = keys,
            .values = values,
            .capacity = capacity,
            .preallocated = true,
        };
        errdefer cache.deinit(cb, allocator);

        const rows = std.math.mul(usize, batch, capacity) catch return error.InvalidInputShape;
        if (rows > @as(usize, @intCast(std.math.maxInt(i32))) or d_model > @as(usize, @intCast(std.math.maxInt(i32)))) return error.InvalidInputShape;
        const shape = [_]i32{ @intCast(rows), @intCast(d_model) };
        for (0..layers) |layer| {
            try cb.checkExecutionControl();
            cache.keys[layer] = (try cb.allocUninitF32Shape(&shape)) orelse {
                cache.deinit(cb, allocator);
                return null;
            };
            cache.values[layer] = (try cb.allocUninitF32Shape(&shape)) orelse {
                cache.deinit(cb, allocator);
                return null;
            };
        }
        return cache;
    }

    pub fn deinit(self: *DecoderSelfCache, cb: *const ComputeBackend, allocator: std.mem.Allocator) void {
        for (self.keys) |maybe_key| if (maybe_key) |key| cb.free(key);
        for (self.values) |maybe_value| if (maybe_value) |value| cb.free(value);
        allocator.free(self.keys);
        allocator.free(self.values);
    }
};

test "Florence cache preparation cancels between layers and releases partial tensors" {
    const Probe = struct {
        allocations: usize = 0,
        frees: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.allocations == 2) return error.Cancelled;
        }
        fn alloc(raw: *anyopaque, _: []const i32) !?CT {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const tensor = try std.testing.allocator.create(u8);
            self.allocations += 1;
            return tensor;
        }
        fn free(raw: *anyopaque, tensor: CT) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.frees += 1;
            std.testing.allocator.destroy(@as(*u8, @ptrCast(tensor)));
        }
    };
    var probe = Probe{};
    var vtable: ComputeBackend.VTable = undefined;
    vtable.allocUninitF32Shape = Probe.alloc;
    vtable.freeTensor = Probe.free;
    const cb = ComputeBackend{
        .ptr = &probe,
        .vtable = &vtable,
        .execution_control = .{ .ptr = &probe, .check_fn = Probe.check },
    };
    try std.testing.expectError(error.Cancelled, DecoderSelfCache.tryInitPreallocated(&cb, std.testing.allocator, 3, 1, 4, 4));
    try std.testing.expectEqual(@as(usize, 2), probe.allocations);
    try std.testing.expectEqual(probe.allocations, probe.frees);
}

pub const DecoderIncrementalCache = struct {
    assigned: ?*@import("../execution/dense_decoder_schedule.zig").Schedule = null,
    /// Full backing allocations remain owned even when logical rows shrink.
    cross: DecoderCrossCache,
    cross_active: ?DecoderCrossCache = null,
    self: DecoderSelfCache,
    encoder_mask: []i64,
    self_mask: []i64,
    batch: usize,
    enc_seq: usize,

    fn attentionCross(self: *const DecoderIncrementalCache) DecoderCrossCache {
        return self.cross_active orelse self.cross;
    }

    pub fn deinit(self: *DecoderIncrementalCache, cb: *const ComputeBackend, allocator: std.mem.Allocator) void {
        if (self.cross_active) |*active| active.deinit(cb, allocator);
        self.cross.deinit(cb, allocator);
        self.self.deinit(cb, allocator);
        allocator.free(self.encoder_mask);
        allocator.free(self.self_mask);
    }
};

/// Remove completed decoder rows while preserving their original relative
/// order. Preallocated device caches are compacted in place, copying only the
/// populated self-attention prefix rather than reallocating every layer's full
/// decode capacity. Other cache layouts retain the transactional row-gather
/// fallback. Device backends can therefore shrink decoder and LM-head batch
/// dimensions as soon as pages reach EOS without a second full KV-cache peak.
pub fn compactDecoderIncrementalCache(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    cache: *DecoderIncrementalCache,
    retained_rows: []const u32,
) !bool {
    if (retained_rows.len == 0 or retained_rows.len > cache.batch) return error.InvalidInputShape;
    if (retained_rows.len == cache.batch) return true;
    const expected_encoder_mask_len = std.math.mul(usize, cache.batch, cache.enc_seq) catch
        return error.InvalidInputShape;
    if (cache.batch == 0 or cache.enc_seq == 0 or
        cache.cross.keys.len != config.decoder_layers or
        cache.cross.values.len != config.decoder_layers or
        cache.self.keys.len != config.decoder_layers or
        cache.self.values.len != config.decoder_layers or
        cache.encoder_mask.len != expected_encoder_mask_len or
        cache.self_mask.len == 0 or cache.self_mask.len % cache.batch != 0 or
        (cache.self.preallocated and cache.self.len > cache.self.capacity)) return error.InvalidInputShape;
    var previous: ?u32 = null;
    for (retained_rows) |row| {
        if (row >= cache.batch or (previous != null and row <= previous.?)) return error.InvalidInputShape;
        previous = row;
    }

    if (cache.self.preallocated and cb.supportsCopyRows2D()) {
        return compactPreallocatedDecoderIncrementalCacheInPlace(
            cb,
            allocator,
            config,
            cache,
            retained_rows,
        );
    }

    const old_batch = cache.batch;
    const old_enc_seq = cache.enc_seq;
    const next_batch = retained_rows.len;
    const cross_rows = std.math.mul(usize, next_batch, cache.enc_seq) catch return error.InvalidInputShape;
    const cross_row_ids = try allocator.alloc(u32, cross_rows);
    defer allocator.free(cross_row_ids);
    for (retained_rows, 0..) |source_batch, dst_batch| {
        for (0..cache.enc_seq) |token| {
            const source = std.math.add(
                usize,
                std.math.mul(usize, source_batch, cache.enc_seq) catch return error.InvalidInputShape,
                token,
            ) catch return error.InvalidInputShape;
            cross_row_ids[dst_batch * cache.enc_seq + token] = std.math.cast(u32, source) orelse
                return error.InvalidInputShape;
        }
    }

    const self_row_stride = if (cache.self.preallocated) cache.self.capacity else cache.self.len;
    if (self_row_stride == 0) return error.InvalidInputShape;
    const self_rows = std.math.mul(usize, next_batch, self_row_stride) catch return error.InvalidInputShape;
    const self_row_ids = try allocator.alloc(u32, self_rows);
    defer allocator.free(self_row_ids);
    for (retained_rows, 0..) |source_batch, dst_batch| {
        for (0..self_row_stride) |token| {
            const source = std.math.add(
                usize,
                std.math.mul(usize, source_batch, self_row_stride) catch return error.InvalidInputShape,
                token,
            ) catch return error.InvalidInputShape;
            self_row_ids[dst_batch * self_row_stride + token] = std.math.cast(u32, source) orelse
                return error.InvalidInputShape;
        }
    }

    const next_cross_keys = try allocator.alloc(CT, config.decoder_layers);
    var next_cross_keys_len: usize = 0;
    const next_cross_values = allocator.alloc(CT, config.decoder_layers) catch |err| {
        allocator.free(next_cross_keys);
        return err;
    };
    var next_cross_values_len: usize = 0;
    const next_self_keys = allocator.alloc(?CT, config.decoder_layers) catch |err| {
        allocator.free(next_cross_keys);
        allocator.free(next_cross_values);
        return err;
    };
    @memset(next_self_keys, null);
    const next_self_values = allocator.alloc(?CT, config.decoder_layers) catch |err| {
        allocator.free(next_cross_keys);
        allocator.free(next_cross_values);
        allocator.free(next_self_keys);
        return err;
    };
    @memset(next_self_values, null);
    var next_cache_owned = true;
    defer if (next_cache_owned) {
        for (next_cross_keys[0..next_cross_keys_len]) |tensor| cb.free(tensor);
        for (next_cross_values[0..next_cross_values_len]) |tensor| cb.free(tensor);
        for (next_self_keys) |maybe| if (maybe) |tensor| cb.free(tensor);
        for (next_self_values) |maybe| if (maybe) |tensor| cb.free(tensor);
        allocator.free(next_cross_keys);
        allocator.free(next_cross_values);
        allocator.free(next_self_keys);
        allocator.free(next_self_values);
    };

    for (0..config.decoder_layers) |layer| {
        next_cross_keys[layer] = (try cb.takeRows(
            cache.cross.keys[layer],
            cross_row_ids,
            cross_rows,
            config.d_model,
        )) orelse return false;
        next_cross_keys_len += 1;
        next_cross_values[layer] = (try cb.takeRows(
            cache.cross.values[layer],
            cross_row_ids,
            cross_rows,
            config.d_model,
        )) orelse return false;
        next_cross_values_len += 1;
        next_self_keys[layer] = (try cb.takeRows(
            cache.self.keys[layer] orelse return error.InvalidInputShape,
            self_row_ids,
            self_rows,
            config.d_model,
        )) orelse return false;
        next_self_values[layer] = (try cb.takeRows(
            cache.self.values[layer] orelse return error.InvalidInputShape,
            self_row_ids,
            self_rows,
            config.d_model,
        )) orelse return false;
    }

    const encoder_mask = try allocator.alloc(i64, cross_rows);
    errdefer allocator.free(encoder_mask);
    for (retained_rows, 0..) |source_batch, dst_batch| {
        const source = @as(usize, source_batch) * cache.enc_seq;
        @memcpy(
            encoder_mask[dst_batch * cache.enc_seq ..][0..cache.enc_seq],
            cache.encoder_mask[source..][0..cache.enc_seq],
        );
    }
    const old_self_mask_stride = cache.self_mask.len / old_batch;
    const self_mask_len = std.math.mul(usize, next_batch, old_self_mask_stride) catch
        return error.InvalidInputShape;
    const self_mask = try allocator.alloc(i64, self_mask_len);
    errdefer allocator.free(self_mask);
    for (retained_rows, 0..) |source_batch, dst_batch| {
        const source = @as(usize, source_batch) * old_self_mask_stride;
        @memcpy(
            self_mask[dst_batch * old_self_mask_stride ..][0..old_self_mask_stride],
            cache.self_mask[source..][0..old_self_mask_stride],
        );
    }

    var old_cross = cache.cross;
    var old_self = cache.self;
    var old_cross_active = cache.cross_active;
    const old_encoder_mask = cache.encoder_mask;
    const old_self_mask = cache.self_mask;
    cache.* = .{
        .cross = .{ .keys = next_cross_keys, .values = next_cross_values },
        .self = .{
            .keys = next_self_keys,
            .values = next_self_values,
            .len = old_self.len,
            .capacity = old_self.capacity,
            .preallocated = old_self.preallocated,
        },
        .encoder_mask = encoder_mask,
        .self_mask = self_mask,
        .batch = next_batch,
        .enc_seq = old_enc_seq,
    };
    next_cross_keys_len = 0;
    next_cross_values_len = 0;
    next_cache_owned = false;
    if (old_cross_active) |*active| active.deinit(cb, allocator);
    old_cross.deinit(cb, allocator);
    old_self.deinit(cb, allocator);
    allocator.free(old_encoder_mask);
    allocator.free(old_self_mask);
    return true;
}

fn compactPreallocatedDecoderIncrementalCacheInPlace(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    cache: *DecoderIncrementalCache,
    retained_rows: []const u32,
) !bool {
    const old_batch = cache.batch;
    const next_batch = retained_rows.len;
    const encoder_mask_len = std.math.mul(usize, next_batch, cache.enc_seq) catch
        return error.InvalidInputShape;
    const encoder_mask = try allocator.alloc(i64, encoder_mask_len);
    errdefer allocator.free(encoder_mask);
    for (retained_rows, 0..) |source_batch, dst_batch| {
        const source = @as(usize, source_batch) * cache.enc_seq;
        @memcpy(
            encoder_mask[dst_batch * cache.enc_seq ..][0..cache.enc_seq],
            cache.encoder_mask[source..][0..cache.enc_seq],
        );
    }

    const self_mask_stride = cache.self_mask.len / old_batch;
    const self_mask_len = std.math.mul(usize, next_batch, self_mask_stride) catch
        return error.InvalidInputShape;
    const self_mask = try allocator.alloc(i64, self_mask_len);
    errdefer allocator.free(self_mask);
    for (retained_rows, 0..) |source_batch, dst_batch| {
        const source = @as(usize, source_batch) * self_mask_stride;
        @memcpy(
            self_mask[dst_batch * self_mask_stride ..][0..self_mask_stride],
            cache.self_mask[source..][0..self_mask_stride],
        );
    }

    // Retained rows are strictly increasing, so every moved source slab is
    // above its destination and cannot be overwritten before it is consumed.
    // The unpopulated self-cache tail is deliberately ignored: future decode
    // steps write it at the same capacity-strided offsets.
    for (0..config.decoder_layers) |layer| {
        const self_key = cache.self.keys[layer] orelse return error.InvalidInputShape;
        const self_value = cache.self.values[layer] orelse return error.InvalidInputShape;
        for (retained_rows, 0..) |source_batch_u32, dst_batch| {
            const source_batch = @as(usize, source_batch_u32);
            if (source_batch == dst_batch) continue;
            const source_cross_row = std.math.mul(usize, source_batch, cache.enc_seq) catch
                return error.InvalidInputShape;
            const dest_cross_row = std.math.mul(usize, dst_batch, cache.enc_seq) catch
                return error.InvalidInputShape;
            if (!try cb.copyRows2D(
                allocator,
                cache.cross.keys[layer],
                dest_cross_row,
                cache.cross.keys[layer],
                source_cross_row,
                cache.enc_seq,
                config.d_model,
            )) return error.UnsupportedOperation;
            if (!try cb.copyRows2D(
                allocator,
                cache.cross.values[layer],
                dest_cross_row,
                cache.cross.values[layer],
                source_cross_row,
                cache.enc_seq,
                config.d_model,
            )) return error.UnsupportedOperation;

            if (cache.self.len == 0) continue;
            const source_self_row = std.math.mul(usize, source_batch, cache.self.capacity) catch
                return error.InvalidInputShape;
            const dest_self_row = std.math.mul(usize, dst_batch, cache.self.capacity) catch
                return error.InvalidInputShape;
            if (!try cb.copyRows2D(
                allocator,
                self_key,
                dest_self_row,
                self_key,
                source_self_row,
                cache.self.len,
                config.d_model,
            )) return error.UnsupportedOperation;
            if (!try cb.copyRows2D(
                allocator,
                self_value,
                dest_self_row,
                self_value,
                source_self_row,
                cache.self.len,
                config.d_model,
            )) return error.UnsupportedOperation;
        }
    }

    // In-place row copies do not change tensor metadata. Publish exact active
    // shapes for attention, keeping the original allocations alive underneath
    // borrowed CUDA views as well as retained Metal views. Device slicing is
    // descriptor-only; it does not allocate another KV payload. As with a
    // failed row copy, a view failure aborts this invocation and its cache.
    const active = try cache.cross.prefix(cb, allocator, encoder_mask_len, config.d_model);
    if (cache.cross_active) |*previous| previous.deinit(cb, allocator);
    cache.cross_active = active;

    allocator.free(cache.encoder_mask);
    allocator.free(cache.self_mask);
    cache.encoder_mask = encoder_mask;
    cache.self_mask = self_mask;
    cache.batch = next_batch;
    return true;
}

test "Florence compaction publishes active shapes and preserves borrowed backing through repeated EOS" {
    try testCompactedCrossViews(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, testCompactedCrossViews, .{});
}

fn testCompactedCrossViews(allocator: std.mem.Allocator) !void {
    const Probe = struct {
        allocator: std.mem.Allocator,
        roots: usize = 0,
        views: usize = 0,
        payload_bytes: usize = 0,
        const Tensor = struct { data: []f32, rows: usize, cols: usize, owned: bool };
        fn tensor(raw: CT) *Tensor {
            return @ptrCast(@alignCast(raw));
        }
        fn alloc(raw: *anyopaque, shape: []const i32) !?CT {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const rows: usize = @intCast(shape[0]);
            const cols: usize = @intCast(shape[1]);
            const data = try self.allocator.alloc(f32, rows * cols);
            errdefer self.allocator.free(data);
            const output = try self.allocator.create(Tensor);
            output.* = .{ .data = data, .rows = rows, .cols = cols, .owned = true };
            for (data, 0..) |*value, index| value.* = @floatFromInt(index);
            self.roots += 1;
            self.payload_bytes += data.len * @sizeOf(f32);
            return output;
        }
        fn free(raw: *anyopaque, ct: CT) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const value = tensor(ct);
            if (value.owned) {
                // Emulate CUDA's borrowed views: releasing their owner first
                // is invalid even if Metal would retain its buffer for us.
                std.debug.assert(self.views == 0);
                self.roots -= 1;
                self.payload_bytes -= value.data.len * @sizeOf(f32);
                self.allocator.free(value.data);
            } else self.views -= 1;
            self.allocator.destroy(value);
        }
        fn copy(_: *anyopaque, dst: CT, dst_row: usize, src: CT, src_row: usize, rows: usize, cols: usize) !bool {
            const target = tensor(dst);
            const source = tensor(src);
            if (target.cols != cols or source.cols != cols or dst_row + rows > target.rows or src_row + rows > source.rows) return error.InvalidShape;
            std.mem.copyForwards(f32, target.data[dst_row * cols ..][0 .. rows * cols], source.data[src_row * cols ..][0 .. rows * cols]);
            return true;
        }
        fn slice(raw: *anyopaque, input: CT, start: usize, rows: usize, cols: usize) !CT {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const source = tensor(input);
            if (source.cols != cols or start + rows > source.rows) return error.InvalidShape;
            const output = try self.allocator.create(Tensor);
            output.* = .{ .data = source.data[start * cols ..][0 .. rows * cols], .rows = rows, .cols = cols, .owned = false };
            self.views += 1;
            return output;
        }
        fn cross(self: *@This(), layers: usize) !DecoderCrossCache {
            const keys = try self.allocator.alloc(CT, layers);
            errdefer self.allocator.free(keys);
            const values = try self.allocator.alloc(CT, layers);
            errdefer self.allocator.free(values);
            var initialized: usize = 0;
            errdefer for (0..initialized) |i| {
                free(self, keys[i]);
                free(self, values[i]);
            };
            for (keys, values) |*key, *value| {
                key.* = (try alloc(self, &.{ 14, 2 })).?;
                errdefer free(self, key.*);
                value.* = (try alloc(self, &.{ 14, 2 })).?;
                initialized += 1;
            }
            return .{ .keys = keys, .values = values };
        }
    };
    var probe = Probe{ .allocator = allocator };
    defer std.debug.assert(probe.roots == 0 and probe.views == 0 and probe.payload_bytes == 0);
    var vtable: ComputeBackend.VTable = undefined;
    vtable.allocUninitF32Shape = Probe.alloc;
    vtable.freeTensor = Probe.free;
    vtable.copyRows2D = Probe.copy;
    vtable.sliceRows2D = Probe.slice;
    const cb = ComputeBackend{ .ptr = &probe, .vtable = &vtable };
    var cache: DecoderIncrementalCache = init: {
        var cross = try probe.cross(2);
        errdefer cross.deinit(&cb, allocator);
        var self = (try DecoderSelfCache.tryInitPreallocated(&cb, allocator, 2, 7, 4, 2)).?;
        errdefer self.deinit(&cb, allocator);
        self.len = 2;
        const encoder_mask = try allocator.alloc(i64, 14);
        errdefer allocator.free(encoder_mask);
        const self_mask = try allocator.alloc(i64, 28);
        @memset(encoder_mask, 1);
        @memset(self_mask, 1);
        break :init .{ .cross = cross, .self = self, .encoder_mask = encoder_mask, .self_mask = self_mask, .batch = 7, .enc_seq = 2 };
    };
    defer cache.deinit(&cb, allocator);
    const payload_bytes = probe.payload_bytes;
    const backing = cache.cross.keys[0];
    const config = Config{ .decoder_layers = 2, .d_model = 2 };
    try std.testing.expectError(error.InvalidInputShape, compactDecoderIncrementalCache(&cb, allocator, config, &cache, &.{ 3, 1 }));
    try std.testing.expect(try compactDecoderIncrementalCache(&cb, allocator, config, &cache, &.{ 1, 3, 5 }));
    try std.testing.expectEqual(@as(usize, 3), cache.batch);
    try std.testing.expectEqual(@as(usize, 6), Probe.tensor(cache.attentionCross().keys[0]).rows);
    try std.testing.expectEqualSlices(f32, &.{ 4, 5, 6, 7, 12, 13, 14, 15, 20, 21, 22, 23 }, Probe.tensor(cache.attentionCross().keys[0]).data);
    try std.testing.expect(try compactDecoderIncrementalCache(&cb, allocator, config, &cache, &.{1}));
    try std.testing.expectEqual(@as(usize, 1), cache.batch);
    for (cache.attentionCross().keys, cache.attentionCross().values) |key, value| {
        try std.testing.expectEqual(@as(usize, 2), Probe.tensor(key).rows);
        try std.testing.expectEqualSlices(f32, &.{ 12, 13, 14, 15 }, Probe.tensor(key).data);
        try std.testing.expectEqualSlices(f32, &.{ 12, 13, 14, 15 }, Probe.tensor(value).data);
    }
    try std.testing.expectEqualSlices(f32, &.{ 24, 25, 26, 27 }, Probe.tensor(cache.self.keys[0].?).data[0..4]);
    try std.testing.expectEqual(backing, cache.cross.keys[0]);
    try std.testing.expectEqual(payload_bytes, probe.payload_bytes);
    try std.testing.expectEqual(@as(usize, 4), probe.views);
}

const VisionForwardResult = struct {
    features: []f32,
    seq_len: usize,
};

const VisionForwardTensorResult = struct {
    features: CT,
    seq_len: usize,
};

pub fn encoderForward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    pixel_values: []const f32,
    batch: usize,
    prompt_input_ids: []const i64,
    prompt_seq_len: usize,
) !EncoderForwardResult {
    const tensor = try encoderForwardTensor(cb, allocator, config, pixel_values, batch, prompt_input_ids, prompt_seq_len);
    defer cb.free(tensor.hidden);
    const profile = readProfileEnabled();
    const download_start = nowNs();
    const result = try cb.toFloat32(tensor.hidden, allocator);
    if (profile) logFlorenceProfile("encoder_output_download", download_start);
    return .{ .hidden = result, .seq_len = tensor.seq_len };
}

fn cudaEncoderMicrobatchSize(kind: ops.BackendKind, batch: usize, configured: ?usize) usize {
    if (kind != .cuda or batch <= 1 or batch > 64) return batch;
    const limit = configured orelse return batch;
    return if (limit >= 1 and limit <= 64) @min(batch, limit) else batch;
}

/// Keep the decoder cohort large while bounding the much larger vision and
/// encoder activations. Finished encoder rows stay on device in page order.
/// The caller's original full-cohort admission permit remains authoritative.
pub fn encoderForwardTensor(
    control_cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    pixel_values: []const f32,
    batch: usize,
    prompt_input_ids: []const i64,
    prompt_seq_len: usize,
) !EncoderForwardTensorResult {
    const result = try encoderForwardTensorMicrobatched(control_cb, allocator, config, pixel_values, batch, prompt_input_ids, prompt_seq_len);
    errdefer control_cb.free(result.hidden);
    // Diagnostic only: compare actual image/weight encodings with independently
    // executed single-page encoders. Keep outputs and all OCR oracles intact.
    if (control_cb.kind() == .cuda and batch > 1 and batch <= 64 and
        platform.env.getenvBool("ANTFLY_FLORENCE2_CUDA_CHECK_ENCODER_BATCH"))
    {
        if (pixel_values.len % batch != 0 or prompt_input_ids.len != (std.math.mul(usize, batch, prompt_seq_len) catch return error.InvalidInputShape)) return error.InvalidInputShape;
        const host = try control_cb.toFloat32(result.hidden, allocator);
        defer allocator.free(host);
        const page_values = std.math.mul(usize, result.seq_len, config.d_model) catch return error.InvalidInputShape;
        if (host.len != (std.math.mul(usize, batch, page_values) catch return error.InvalidInputShape)) return error.InvalidInputShape;
        const pixels_per_page = pixel_values.len / batch;
        for (0..batch) |page| {
            try control_cb.checkExecutionControl();
            const serial = try encoderForwardTensorWhole(control_cb, allocator, config, pixel_values[page * pixels_per_page ..][0..pixels_per_page], 1, prompt_input_ids[page * prompt_seq_len ..][0..prompt_seq_len], prompt_seq_len);
            defer control_cb.free(serial.hidden);
            if (serial.seq_len != result.seq_len) return error.InvalidInputShape;
            const reference = try control_cb.toFloat32(serial.hidden, allocator);
            defer allocator.free(reference);
            if (reference.len != page_values) return error.InvalidInputShape;
            var different_bits: usize = 0;
            var nonfinite: usize = 0;
            var max_abs: f32 = 0;
            var squared_error: f64 = 0;
            for (reference, host[page * page_values ..][0..page_values]) |expected, actual| {
                if (@as(u32, @bitCast(expected)) != @as(u32, @bitCast(actual))) different_bits += 1;
                if (!std.math.isFinite(expected) or !std.math.isFinite(actual)) {
                    nonfinite += 1;
                    continue;
                }
                const difference = @abs(expected - actual);
                max_abs = @max(max_abs, difference);
                squared_error += @as(f64, difference) * difference;
            }
            std.debug.print("FLORENCE2 encoder_batch_check items={d} page={d} values={d} different_bits={d} nonfinite={d} max_abs={d} rms_error={d}\n", .{ batch, page, page_values, different_bits, nonfinite, max_abs, @sqrt(squared_error / @as(f64, @floatFromInt(page_values))) });
        }
    }
    return result;
}

fn encoderForwardTensorMicrobatched(
    control_cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    pixel_values: []const f32,
    batch: usize,
    prompt_input_ids: []const i64,
    prompt_seq_len: usize,
) !EncoderForwardTensorResult {
    try control_cb.checkExecutionControl();
    const limit = cudaEncoderMicrobatchSize(control_cb.kind(), batch, platform.env.getenvUsize("ANTFLY_FLORENCE2_CUDA_ENCODER_MICROBATCH"));
    if (limit == batch) return encoderForwardTensorWhole(control_cb, allocator, config, pixel_values, batch, prompt_input_ids, prompt_seq_len);
    if (pixel_values.len % batch != 0 or prompt_input_ids.len != (std.math.mul(usize, batch, prompt_seq_len) catch return error.InvalidInputShape)) return error.InvalidInputShape;
    const pixels_per_page = pixel_values.len / batch;
    var output: ?CT = null;
    errdefer if (output) |value| control_cb.free(value);
    var sequence: usize = 0;
    var offset: usize = 0;
    var chunks: usize = 0;
    while (offset < batch) {
        const count = @min(limit, batch - offset);
        const encoded = try encoderForwardTensorWhole(control_cb, allocator, config, pixel_values[offset * pixels_per_page ..][0 .. count * pixels_per_page], count, prompt_input_ids[offset * prompt_seq_len ..][0 .. count * prompt_seq_len], prompt_seq_len);
        defer control_cb.free(encoded.hidden);
        if (output == null) {
            sequence = encoded.seq_len;
            const rows = std.math.mul(usize, batch, sequence) catch return error.InvalidInputShape;
            output = (try control_cb.allocUninitF32Shape(&.{ @intCast(rows), @intCast(config.d_model) })) orelse return error.UnsupportedTensorType;
        } else if (sequence != encoded.seq_len) return error.InvalidInputShape;
        if (!try control_cb.copyRows2D(allocator, output.?, offset * sequence, encoded.hidden, 0, count * sequence, config.d_model)) return error.UnsupportedTensorType;
        offset += count;
        chunks += 1;
    }
    if (platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3"))
        std.debug.print("FLORENCE2 cuda_encoder_microbatch_chunks={d} items={d} limit={d}\n", .{ chunks, batch, limit });
    return .{ .hidden = output.?, .seq_len = sequence };
}

test "LightOnOCR3 CUDA Florence encoder microbatch keeps backend and cohort limits isolated" {
    try std.testing.expectEqual(@as(usize, 64), cudaEncoderMicrobatchSize(.metal, 64, 4));
    try std.testing.expectEqual(@as(usize, 64), cudaEncoderMicrobatchSize(.native, 64, 4));
    try std.testing.expectEqual(@as(usize, 64), cudaEncoderMicrobatchSize(.cuda, 64, null));
    try std.testing.expectEqual(@as(usize, 64), cudaEncoderMicrobatchSize(.cuda, 64, 0));
    try std.testing.expectEqual(@as(usize, 64), cudaEncoderMicrobatchSize(.cuda, 64, 65));
    try std.testing.expectEqual(@as(usize, 1), cudaEncoderMicrobatchSize(.cuda, 1, 4));
    try std.testing.expectEqual(@as(usize, 4), cudaEncoderMicrobatchSize(.cuda, 64, 4));
    try std.testing.expectEqual(@as(usize, 17), cudaEncoderMicrobatchSize(.cuda, 17, 32));
}

fn encoderForwardTensorWhole(
    control_cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    pixel_values: []const f32,
    batch: usize,
    prompt_input_ids: []const i64,
    prompt_seq_len: usize,
) !EncoderForwardTensorResult {
    const layer_cb = layerComputeBackend(control_cb);
    const cb = &layer_cb;
    const encoder_scope_before = MetalCompute.ocrEncoderScopeOpens(cb)[2];
    defer if (platform.env.getenvBool("ANTFLY_FLORENCE2_TRACE_NO_REPEAT_ARGMAX")) {
        const opens = MetalCompute.ocrEncoderScopeOpens(cb)[2] -| encoder_scope_before;
        std.debug.print("FLORENCE2 encoder_scope_opens={d}\n", .{opens});
    };
    try control_cb.checkExecutionControl();
    // Large encoder matrices benefit from the backend's bounded F16 mirrors
    // of quantized weights. Keep incremental decoding on its original path.
    const eager_mirrors = if (comptime build_options.enable_metal)
        cb.kind() == .metal and @import("../backends/metal_runtime.zig").appleSimdgroupFeatureEnabled("TERMITE_METAL_FLORENCE2_ENCODER_MIRRORS", true)
    else
        false;
    if (eager_mirrors) cb.preferEagerQuantMirrors(true);
    defer if (eager_mirrors) cb.preferEagerQuantMirrors(false);
    const debug_cuda_session = platform.env.getenvBool("ANTFLY_INFERENCE_DEBUG_CUDA_SESSION");
    const profile = readProfileEnabled();
    if (debug_cuda_session) std.log.info("florence: encoder forward start batch={d} prompt_seq={d}", .{ batch, prompt_seq_len });
    const vision_start = nowNs();
    if (try visionEncoderForwardTensorTailMicrobatched(control_cb, allocator, config, pixel_values, batch)) |image_tensor| {
        if (profile) logFlorenceProfile("vision_total", vision_start);
        if (debug_cuda_session) std.log.info("florence: device vision encoder done seq={d}", .{image_tensor.seq_len});
        defer cb.free(image_tensor.features);
        if (debugStatsEnabled()) logDebugStatsTensor(cb, allocator, "vision_features(device)", image_tensor.features);

        const total_seq = image_tensor.seq_len + prompt_seq_len;
        const embeddings_start = nowNs();
        const hidden = try applyEncoderImagePromptEmbeddingsTensor(cb, allocator, image_tensor, config, batch, prompt_input_ids, prompt_seq_len);
        if (profile) logFlorenceProfile("encoder_embeddings", embeddings_start);
        if (debug_cuda_session) std.log.info("florence: encoder embeddings done total_seq={d}", .{total_seq});
        if (debugStatsEnabled()) logDebugStatsTensor(cb, allocator, "post_embeddings(device)", hidden);

        return runTextEncoderLayersTensor(control_cb, allocator, config, hidden, batch, total_seq);
    }

    const image = try visionEncoderForward(control_cb, allocator, config, pixel_values, batch);
    if (profile) logFlorenceProfile("vision_total", vision_start);
    if (debug_cuda_session) std.log.info("florence: vision encoder done seq={d}", .{image.seq_len});
    defer allocator.free(image.features);
    if (debugStatsEnabled()) logDebugStatsF32("vision_features(host)", image.features);

    const d_model: usize = config.d_model;
    const total_seq = image.seq_len + prompt_seq_len;
    const total_tokens = batch * total_seq;

    const prompt_embeddings = if (prompt_seq_len > 0)
        try tokenEmbeddingsData(cb, allocator, try promptEmbedWeight(cb), prompt_input_ids, batch * prompt_seq_len, d_model)
    else
        &[_]f32{};
    defer if (prompt_seq_len > 0) allocator.free(prompt_embeddings);

    const merged = try allocator.alloc(f32, total_tokens * d_model);
    defer allocator.free(merged);

    for (0..batch) |b| {
        const dst = b * total_seq * d_model;
        const image_src = b * image.seq_len * d_model;
        @memcpy(merged[dst..][0 .. image.seq_len * d_model], image.features[image_src..][0 .. image.seq_len * d_model]);
        if (prompt_seq_len > 0) {
            const prompt_src = b * prompt_seq_len * d_model;
            @memcpy(
                merged[dst + image.seq_len * d_model ..][0 .. prompt_seq_len * d_model],
                prompt_embeddings[prompt_src..][0 .. prompt_seq_len * d_model],
            );
        }
    }

    const embeddings_start = nowNs();
    const hidden = try applyEncoderEmbeddings(cb, allocator, merged, config, batch, total_seq);
    if (profile) logFlorenceProfile("encoder_embeddings", embeddings_start);
    if (debug_cuda_session) std.log.info("florence: encoder embeddings done total_seq={d}", .{total_seq});
    if (debugStatsEnabled()) logDebugStatsTensor(cb, allocator, "post_embeddings(host)", hidden);
    return runTextEncoderLayersTensor(control_cb, allocator, config, hidden, batch, total_seq);
}

fn runTextEncoderLayersTensor(
    control_cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_hidden: CT,
    batch: usize,
    total_seq: usize,
) !EncoderForwardTensorResult {
    const layer_cb = layerComputeBackend(control_cb);
    const cb = &layer_cb;
    const debug_cuda_session = platform.env.getenvBool("ANTFLY_INFERENCE_DEBUG_CUDA_SESSION");
    const profile = readProfileEnabled();
    const total_tokens = batch * total_seq;
    var hidden = input_hidden;
    var hidden_owned = true;
    errdefer if (hidden_owned) cb.free(hidden);

    const attn_mask = try allocator.alloc(i64, total_tokens);
    defer allocator.free(attn_mask);
    @memset(attn_mask, 1);

    var frame_active = try beginFlorenceMetalFrame(cb, .prefill);
    errdefer if (frame_active) cb.decoderRuntimeCancelFrame() catch {};

    var buf: [256]u8 = undefined;
    for (0..config.encoder_layers) |layer| {
        try control_cb.checkExecutionControl();
        if (debug_cuda_session) std.log.info("florence: text encoder layer start {d}", .{layer});
        const layer_start = nowNs();
        const next = try encoderBlock(cb, config, hidden, attn_mask, batch, total_seq, layer, &buf);
        if (profile) logFlorenceProfileStep("text_encoder_layer", layer, layer_start);
        cb.free(hidden);
        hidden = next;
        if (debugStatsEnabled()) {
            var label_buf: [64]u8 = undefined;
            const label = std.fmt.bufPrint(&label_buf, "text_encoder_layer_{d}", .{layer}) catch "text_encoder_layer";
            logDebugStatsTensor(cb, allocator, label, hidden);
        }
        if (debug_cuda_session) std.log.info("florence: text encoder layer done {d}", .{layer});
    }

    if (frame_active) {
        const submit_start = nowNs();
        try cb.decoderRuntimeSubmitAndWaitFrame();
        frame_active = false;
        if (profile) logFlorenceProfile("text_encoder_frame_submit", submit_start);
    }

    hidden_owned = false;
    return .{ .hidden = hidden, .seq_len = total_seq };
}

test "Florence text layer cancellation releases its owned input" {
    const Probe = struct {
        frees: usize = 0,
        fn kind(_: *anyopaque) ops.BackendKind {
            return .native;
        }
        fn free(raw: *anyopaque, tensor: CT) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.frees += 1;
            std.testing.allocator.destroy(@as(*u8, @ptrCast(tensor)));
        }
    };
    var probe = Probe{};
    var vtable: ComputeBackend.VTable = undefined;
    vtable.backendKind = Probe.kind;
    vtable.freeTensor = Probe.free;
    const cb = ComputeBackend{
        .ptr = &probe,
        .vtable = &vtable,
        .execution_control = .{ .deadline_ns = 0 },
    };
    const hidden = try std.testing.allocator.create(u8);
    try std.testing.expectError(error.Timeout, runTextEncoderLayersTensor(&cb, std.testing.allocator, .{ .encoder_layers = 1 }, hidden, 1, 1));
    try std.testing.expectEqual(@as(usize, 1), probe.frees);
}

/// Run the Florence/BART text encoder without any vision inputs.
pub fn textEncoderForward(
    control_cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    batch: usize,
    seq_len: usize,
) ![]f32 {
    try control_cb.checkExecutionControl();
    const layer_cb = layerComputeBackend(control_cb);
    const cb = &layer_cb;
    const d_model: usize = config.d_model;
    const total = batch * seq_len;
    const merged = try tokenEmbeddingsData(cb, allocator, try promptEmbedWeight(cb), input_ids, total, d_model);
    defer allocator.free(merged);

    var hidden = try applyEncoderEmbeddings(cb, allocator, merged, config, batch, seq_len);
    defer cb.free(hidden);

    const attn_mask = try allocator.alloc(i64, total);
    defer allocator.free(attn_mask);
    @memset(attn_mask, 1);

    var buf: [256]u8 = undefined;
    for (0..config.encoder_layers) |layer| {
        try control_cb.checkExecutionControl();
        const next = try encoderBlock(cb, config, hidden, attn_mask, batch, seq_len, layer, &buf);
        cb.free(hidden);
        hidden = next;
    }

    const result = try cb.toFloat32(hidden, allocator);
    return result;
}

/// Run the Florence-2 decoder in hidden-state mode.
pub fn decoderHiddenForward(
    control_cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    batch: usize,
    seq_len: usize,
) ![]f32 {
    try control_cb.checkExecutionControl();
    const layer_cb = layerComputeBackend(control_cb);
    const cb = &layer_cb;
    var hidden = try applyDecoderEmbeddings(cb, allocator, config, input_ids, batch, seq_len);
    defer cb.free(hidden);

    var buf: [256]u8 = undefined;
    for (0..config.decoder_layers) |layer| {
        try control_cb.checkExecutionControl();
        const new_hidden = try decoderBlockSelfOnly(cb, config, hidden, batch, seq_len, layer, &buf);
        cb.free(hidden);
        hidden = new_hidden;
    }

    if (try tryOptionalWeight(cb, "language_model.model.decoder.layer_norm.weight")) |ln_w| {
        const ln_b = (try tryOptionalWeight(cb, "language_model.model.decoder.layer_norm.bias")) orelse return error.MissingWeight;
        const normed = try cb.layerNorm(hidden, ln_w, ln_b, config.d_model, 1e-5);
        cb.free(hidden);
        hidden = normed;
    } else if (try tryOptionalWeight(cb, "model.decoder.layer_norm.weight")) |ln_w| {
        const ln_b = (try tryOptionalWeight(cb, "model.decoder.layer_norm.bias")) orelse return error.MissingWeight;
        const normed = try cb.layerNorm(hidden, ln_w, ln_b, config.d_model, 1e-5);
        cb.free(hidden);
        hidden = normed;
    }

    const result = try cb.toFloat32(hidden, allocator);
    return result;
}

/// Run the Florence-2 decoder with cross-attention to encoder hidden states.
pub fn decoderForward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    decoder_input_ids: []const i64,
    encoder_hidden: CT,
    encoder_mask: []const i64,
    batch: usize,
    dec_seq: usize,
    enc_seq: usize,
) ![]f32 {
    const logits = try decoderForwardTensor(
        cb,
        allocator,
        config,
        decoder_input_ids,
        encoder_hidden,
        encoder_mask,
        batch,
        dec_seq,
        enc_seq,
    );
    defer cb.free(logits);

    return try cb.toFloat32(logits, allocator);
}

pub fn decoderForwardTensor(
    control_cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    decoder_input_ids: []const i64,
    encoder_hidden: CT,
    encoder_mask: []const i64,
    batch: usize,
    dec_seq: usize,
    enc_seq: usize,
) !CT {
    try control_cb.checkExecutionControl();
    const layer_cb = layerComputeBackend(control_cb);
    const cb = &layer_cb;
    const dec_total = batch * dec_seq;
    var hidden = try applyDecoderEmbeddings(cb, allocator, config, decoder_input_ids, batch, dec_seq);
    var hidden_live = true;
    errdefer if (hidden_live) cb.free(hidden);

    var buf: [256]u8 = undefined;
    for (0..config.decoder_layers) |layer| {
        try control_cb.checkExecutionControl();
        const new_hidden = try decoderBlock(cb, config, hidden, encoder_hidden, encoder_mask, batch, dec_seq, enc_seq, layer, &buf);
        cb.free(hidden);
        hidden = new_hidden;
    }

    if (try tryOptionalWeight(cb, "language_model.model.decoder.layer_norm.weight")) |ln_w| {
        const ln_b = (try tryOptionalWeight(cb, "language_model.model.decoder.layer_norm.bias")) orelse return error.MissingWeight;
        const normed = try cb.layerNorm(hidden, ln_w, ln_b, config.d_model, 1e-5);
        cb.free(hidden);
        hidden = normed;
    } else if (try tryOptionalWeight(cb, "model.decoder.layer_norm.weight")) |ln_w| {
        const ln_b = (try tryOptionalWeight(cb, "model.decoder.layer_norm.bias")) orelse return error.MissingWeight;
        const normed = try cb.layerNorm(hidden, ln_w, ln_b, config.d_model, 1e-5);
        cb.free(hidden);
        hidden = normed;
    }

    const lm_w = try lmHeadWeight(cb);
    var logits = try cb.linearNoBias(hidden, lm_w, dec_total, config.d_model, config.vocab_size);
    cb.free(hidden);
    hidden_live = false;
    errdefer cb.free(logits);

    if (try tryOptionalWeight(cb, "language_model.final_logits_bias")) |logits_bias| {
        const bias = try logitsBiasSlice(cb, allocator, logits_bias, config.vocab_size);
        defer if (bias.owned) cb.free(bias.tensor);

        if (try cb.addBiasRowsConsume(logits, bias.tensor, dec_total, config.vocab_size)) |biased| {
            logits = biased;
        } else {
            const biased = try cb.add(logits, bias.tensor);
            cb.free(logits);
            logits = biased;
        }
    }
    return logits;
}

pub fn buildDecoderIncrementalCache(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    encoder_hidden: CT,
    batch: usize,
    enc_seq: usize,
    max_decode_len: usize,
) !DecoderIncrementalCache {
    var cross = try buildDecoderCrossCache(cb, allocator, config, encoder_hidden, batch, enc_seq);
    errdefer cross.deinit(cb, allocator);
    var self_cache = if (try DecoderSelfCache.tryInitPreallocated(cb, allocator, config.decoder_layers, batch, max_decode_len, config.d_model)) |preallocated|
        preallocated
    else
        try DecoderSelfCache.init(allocator, config.decoder_layers);
    errdefer self_cache.deinit(cb, allocator);
    const encoder_mask_len = std.math.mul(usize, batch, enc_seq) catch return error.InvalidInputShape;
    const self_mask_len = std.math.mul(usize, batch, max_decode_len) catch return error.InvalidInputShape;
    const encoder_mask = try allocator.alloc(i64, encoder_mask_len);
    errdefer allocator.free(encoder_mask);
    @memset(encoder_mask, 1);
    const self_mask = try allocator.alloc(i64, self_mask_len);
    @memset(self_mask, 1);
    return .{ .cross = cross, .self = self_cache, .encoder_mask = encoder_mask, .self_mask = self_mask, .batch = batch, .enc_seq = enc_seq };
}

pub fn decoderForwardIncrementalStepTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    token_id: i64,
    cache: *DecoderIncrementalCache,
) !CT {
    const hidden = try decoderForwardIncrementalStepFinalHiddenTensor(cb, allocator, config, token_id, cache);
    errdefer cb.free(hidden);
    return try decoderLmHeadLogitsRowsFromFinalHiddenTensor(cb, allocator, config, hidden, 1);
}

pub fn decoderForwardIncrementalStepFinalHiddenTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    token_id: i64,
    cache: *DecoderIncrementalCache,
) !CT {
    const ids = [_]i64{token_id};
    return decoderForwardIncrementalBatchStepFinalHiddenTensor(cb, allocator, config, &ids, cache);
}

pub fn decoderForwardIncrementalBatchStepFinalHiddenTensor(
    control_cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    token_ids: []const i64,
    cache: *DecoderIncrementalCache,
) !CT {
    return decoderForwardIncrementalBatchStepWithWeights(control_cb, allocator, config, token_ids, cache, null);
}
fn decoderForwardIncrementalBatchStepWithWeights(
    control_cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    token_ids: []const i64,
    cache: *DecoderIncrementalCache,
    held: ?*const PreparedSingleToken.Weights,
) !CT {
    try control_cb.checkExecutionControl();
    const layer_cb = layerComputeBackend(control_cb);
    const cb = &layer_cb;
    const assigned = cache.assigned;
    if (cache.cross.keys.len != config.decoder_layers or cache.cross.values.len != config.decoder_layers) return error.InvalidInputShape;
    if (cache.self.keys.len != config.decoder_layers or cache.self.values.len != config.decoder_layers) return error.InvalidInputShape;

    const batch = cache.batch;
    if (token_ids.len != batch) return error.InvalidInputShape;
    const old_len = cache.self.len;
    const new_len = old_len + 1;
    if (cache.self.preallocated and new_len > cache.self.capacity) return error.InvalidInputShape;
    const active_mask_len = std.math.mul(usize, batch, new_len) catch return error.InvalidInputShape;
    if (active_mask_len > cache.self_mask.len) return error.InvalidInputShape;

    var frame_active = try beginFlorenceMetalFrame(cb, .decode);
    errdefer if (frame_active) cb.decoderRuntimeCancelFrame() catch {};
    var hidden = try applyDecoderEmbeddingsAtAssigned(cb, allocator, config, token_ids, batch, 1, old_len, assigned, held);
    var hidden_live = true;
    errdefer if (hidden_live) stepRelease(cb, assigned, hidden);

    var buf: [256]u8 = undefined;
    for (0..config.decoder_layers) |layer| {
        try control_cb.checkExecutionControl();
        const new_hidden = try decoderBlockIncrementalCached(
            cb,
            allocator,
            config,
            hidden,
            cache.self_mask[0..active_mask_len],
            batch,
            old_len,
            new_len,
            layer,
            cache,
            &buf,
            if (held) |weights| &weights.layers[layer] else null,
        );
        stepRelease(cb, assigned, hidden);
        hidden = new_hidden;
    }
    cache.self.len = new_len;

    if (held) |weights| {
        if (weights.final_norm) |norm| {
            const normed = try stepNorm(cb, assigned, hidden, norm.w, norm.b, config.d_model);
            stepRelease(cb, assigned, hidden);
            hidden = normed;
        }
    } else if (try tryOptionalWeight(cb, "language_model.model.decoder.layer_norm.weight")) |ln_w| {
        const ln_b = (try tryOptionalWeight(cb, "language_model.model.decoder.layer_norm.bias")) orelse return error.MissingWeight;
        const normed = try stepNorm(cb, assigned, hidden, ln_w, ln_b, config.d_model);
        stepRelease(cb, assigned, hidden);
        hidden = normed;
    } else if (try tryOptionalWeight(cb, "model.decoder.layer_norm.weight")) |ln_w| {
        const ln_b = (try tryOptionalWeight(cb, "model.decoder.layer_norm.bias")) orelse return error.MissingWeight;
        const normed = try stepNorm(cb, assigned, hidden, ln_w, ln_b, config.d_model);
        stepRelease(cb, assigned, hidden);
        hidden = normed;
    }

    if (frame_active) {
        try cb.decoderRuntimeSubmitAndWaitFrame();
        frame_active = false;
    }

    hidden_live = false;
    return hidden;
}

pub fn decoderLmHeadLogitsFromFinalHiddenTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
) !CT {
    return decoderLmHeadLogitsRowsFromFinalHiddenTensor(cb, allocator, config, hidden, 1);
}

pub fn decoderLmHeadLogitsRowsFromFinalHiddenTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
    rows: usize,
) !CT {
    const lm_w = try lmHeadWeight(cb);
    var logits = try cb.linearNoBias(hidden, lm_w, rows, config.d_model, config.vocab_size);
    errdefer cb.free(logits);

    if (try tryOptionalWeight(cb, "language_model.final_logits_bias")) |logits_bias| {
        const bias = try logitsBiasSlice(cb, allocator, logits_bias, config.vocab_size);
        defer if (bias.owned) cb.free(bias.tensor);

        if (try cb.addBiasRowsConsume(logits, bias.tensor, rows, config.vocab_size)) |biased| {
            logits = biased;
        } else {
            const biased = try cb.add(logits, bias.tensor);
            cb.free(logits);
            logits = biased;
        }
    }
    cb.free(hidden);
    return logits;
}

pub fn decoderFusedTokenFromFinalHiddenTensor(
    cb: *const ComputeBackend,
    config: Config,
    hidden: CT,
    suppress_tokens: []const i32,
) !?CT {
    return try cb.linearNoBiasArgmaxLastRowSuppressTensor(hidden, try lmHeadWeight(cb), 1, config.d_model, config.vocab_size, suppress_tokens);
}

/// Project one final-hidden row per active batch item and return only the
/// selected token ids. Backends implementing this avoid materializing the
/// `[rows, vocab_size]` logits tensor on every autoregressive step.
pub fn decoderFusedTokensFromFinalHiddenTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
    rows: usize,
    suppress_tokens: []const i32,
) !?[]u32 {
    return try cb.linearNoBiasArgmaxRowsSuppress(
        hidden,
        try lmHeadWeight(cb),
        rows,
        config.d_model,
        config.vocab_size,
        suppress_tokens,
        allocator,
    );
}

pub fn decoderFinalLogitsBiasIsZero(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    vocab_size: usize,
) !bool {
    return finalLogitsBiasIsZero(cb, allocator, vocab_size);
}

const LogitsBiasSlice = struct {
    tensor: CT,
    owned: bool,
};

fn logitsBiasSlice(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    logits_bias: CT,
    vocab_size: usize,
) !LogitsBiasSlice {
    const shape = try cb.tensorShape(logits_bias, allocator);
    defer allocator.free(shape);

    var element_count: usize = 1;
    for (shape) |dim| {
        if (dim <= 0) return error.InvalidTensorShape;
        element_count *= @intCast(dim);
    }
    if (element_count < vocab_size) return error.InvalidTensorShape;
    if (element_count == vocab_size) {
        if (shape.len == 1 and @as(usize, @intCast(shape[0])) == vocab_size) {
            return .{ .tensor = logits_bias, .owned = false };
        }
        if (shape.len == 2 and shape[0] == 1 and @as(usize, @intCast(shape[1])) == vocab_size) {
            return .{ .tensor = logits_bias, .owned = false };
        }
        const row_shape = [_]i64{@intCast(vocab_size)};
        return .{ .tensor = try cb.primReshape(logits_bias, &row_shape), .owned = true };
    }

    const start: usize = if (element_count == vocab_size + 1) 1 else 0;
    const flat_shape = [_]i64{ 1, @intCast(element_count) };
    const flat_bias = try cb.primReshape(logits_bias, &flat_shape);
    defer cb.free(flat_bias);
    return .{
        .tensor = try cb.sliceLastDim(flat_bias, start, start + vocab_size),
        .owned = true,
    };
}

fn finalLogitsBiasIsZero(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    vocab_size: usize,
) !bool {
    const logits_bias = (try tryOptionalWeight(cb, "language_model.final_logits_bias")) orelse return true;
    const bias = try logitsBiasSlice(cb, allocator, logits_bias, vocab_size);
    defer if (bias.owned) cb.free(bias.tensor);
    const values = try cb.toFloat32(bias.tensor, allocator);
    defer allocator.free(values);
    if (values.len != vocab_size) return error.InvalidTensorShape;
    for (values) |value| {
        if (value != 0.0) return false;
    }
    return true;
}

pub fn buildDecoderCrossCache(
    control_cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    encoder_hidden: CT,
    batch: usize,
    enc_seq: usize,
) !DecoderCrossCache {
    try control_cb.checkExecutionControl();
    const layer_cb = layerComputeBackend(control_cb);
    const cb = &layer_cb;
    const keys = try allocator.alloc(CT, config.decoder_layers);
    errdefer allocator.free(keys);
    const values = try allocator.alloc(CT, config.decoder_layers);
    errdefer allocator.free(values);

    var filled: usize = 0;
    errdefer {
        for (keys[0..filled]) |key| cb.free(key);
        for (values[0..filled]) |value| cb.free(value);
    }

    const d_model = config.d_model;
    const enc_total = batch * enc_seq;
    var frame_active = try beginFlorenceMetalFrame(cb, .prefill);
    errdefer if (frame_active) cb.decoderRuntimeCancelFrame() catch {};
    var buf: [256]u8 = undefined;
    for (0..config.decoder_layers) |layer| {
        try control_cb.checkExecutionControl();
        const k_weights = try decoderLinearWeights(cb, layer, "encoder_attn.k_proj", &buf);
        const v_weights = try decoderLinearWeights(cb, layer, "encoder_attn.v_proj", &buf);
        const kv = try cb.linearPair(encoder_hidden, k_weights.weight, k_weights.bias, v_weights.weight, v_weights.bias, enc_total, d_model, d_model);
        var key = kv.first;
        errdefer cb.free(key);
        var value = kv.second;
        errdefer cb.free(value);
        // Dense Metal pairs borrow projection scratch reused by the next
        // layer. Persistent cache entries must own storage before that reuse.
        if (try MetalCompute.cloneOutputTensorOwned(cb, key)) |owned| {
            cb.free(key);
            key = owned;
        }
        if (try MetalCompute.cloneOutputTensorOwned(cb, value)) |owned| {
            cb.free(value);
            value = owned;
        }
        keys[layer] = key;
        values[layer] = value;
        filled += 1;
    }

    if (frame_active) {
        try cb.decoderRuntimeSubmitAndWaitFrame();
        frame_active = false;
    }

    return .{ .keys = keys, .values = values };
}

fn populateFlorenceCrossCacheTestWeights(store: *@import("../ops/native_compute.zig").WeightStore) !void {
    const fixture = @import("../ops/media_residency_test.zig");
    const projections = [_]struct {
        suffix: []const u8,
        weights: [2]f32,
        biases: [2]f32,
    }{
        .{ .suffix = "encoder_attn.k_proj", .weights = .{ 0.125, 0.75 }, .biases = .{ 0.25, -0.5 } },
        .{ .suffix = "encoder_attn.v_proj", .weights = .{ -0.25, 1.25 }, .biases = .{ 1.0, 2.0 } },
    };
    var name_buf: [256]u8 = undefined;
    for (0..2) |layer| {
        for (projections) |projection| {
            const weight_name = try std.fmt.bufPrint(
                &name_buf,
                "language_model.model.decoder.layers.{d}.{s}.weight",
                .{ layer, projection.suffix },
            );
            try fixture.putWeight(store, weight_name, &.{ 64, 64 }, projection.weights[layer]);
            const bias_name = try std.fmt.bufPrint(
                &name_buf,
                "language_model.model.decoder.layers.{d}.{s}.bias",
                .{ layer, projection.suffix },
            );
            try fixture.putWeight(store, bias_name, &.{64}, projection.biases[layer]);
        }
    }
}

fn buildFlorenceCrossCacheInExternalMetalFrame(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    encoder_hidden: CT,
    batch: usize,
    enc_seq: usize,
) !DecoderCrossCache {
    if (!try cb.decoderRuntimeBeginFrame()) return error.UnsupportedOperation;
    errdefer cb.decoderRuntimeCancelFrame() catch {};
    try cb.decoderRuntimeSetActiveFrameRegime(.prefill);
    var cache = try buildDecoderCrossCache(cb, allocator, config, encoder_hidden, batch, enc_seq);
    errdefer cache.deinit(cb, allocator);
    try cb.decoderRuntimeSubmitAndWaitFrame();
    return cache;
}

fn expectFlorenceCrossCacheMatches(
    actual_cb: *const ComputeBackend,
    reference_cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    actual: DecoderCrossCache,
    expected: DecoderCrossCache,
) !void {
    try std.testing.expectEqual(expected.keys.len, actual.keys.len);
    try std.testing.expectEqual(expected.values.len, actual.values.len);
    for ([_][]CT{ actual.keys, actual.values }, [_][]CT{ expected.keys, expected.values }) |actual_projection, expected_projection| {
        for (actual_projection, expected_projection) |actual_tensor, expected_tensor| {
            try std.testing.expect(MetalCompute.debugHasDeviceTensor(actual_cb, actual_tensor));
            const actual_values = try actual_cb.toFloat32(actual_tensor, allocator);
            defer allocator.free(actual_values);
            const expected_values = try reference_cb.toFloat32(expected_tensor, allocator);
            defer allocator.free(expected_values);
            try std.testing.expectEqual(expected_values.len, actual_values.len);
            for (expected_values, actual_values) |want, got| {
                try std.testing.expectApproxEqAbs(want, got, 2e-5);
            }
        }
    }
}

fn checkFlorenceCrossCacheMetalFrameReuse(cb: ComputeBackend, reference: ComputeBackend) !void {
    const allocator = std.testing.allocator;
    const dim = 64;
    const config = Config{ .d_model = dim, .decoder_layers = 2 };
    const compute: *MetalCompute = @ptrCast(@alignCast(cb.ptr));
    compute.require_f32_linears = true;
    var first_values: [3 * dim]f32 = undefined;
    var second_values: [3 * dim]f32 = undefined;
    for (&first_values, &second_values, 0..) |*first_value, *second_value, i| {
        first_value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 7) % 23)) - 11)) * 0.03125;
        second_value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 11 + 3) % 29)) - 14)) * 0.025;
    }
    const shape = [_]i32{ 3, dim };
    const expected_first_input = try reference.fromFloat32Shape(&first_values, &shape);
    defer reference.free(expected_first_input);
    var expected_first = try buildDecoderCrossCache(&reference, allocator, config, expected_first_input, 1, 3);
    defer expected_first.deinit(&reference, allocator);
    const expected_second_input = try reference.fromFloat32Shape(&second_values, &shape);
    defer reference.free(expected_second_input);
    var expected_second = try buildDecoderCrossCache(&reference, allocator, config, expected_second_input, 1, 3);
    defer expected_second.deinit(&reference, allocator);
    const first_input = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&first_values, &shape));
    defer cb.free(first_input);
    const second_input = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&second_values, &shape));
    defer cb.free(second_input);
    var first = try buildFlorenceCrossCacheInExternalMetalFrame(&cb, allocator, config, first_input, 1, 3);
    defer first.deinit(&cb, allocator);
    var second = try buildFlorenceCrossCacheInExternalMetalFrame(&cb, allocator, config, second_input, 1, 3);
    defer second.deinit(&cb, allocator);
    // Inspect both caches only after different inputs have reused the next
    // layer's projection scratch and a later frame has completed.
    try expectFlorenceCrossCacheMatches(&cb, &reference, allocator, first, expected_first);
    try expectFlorenceCrossCacheMatches(&cb, &reference, allocator, second, expected_second);
}

test "Florence Metal cross cache preserves per-layer dense projections across frame reuse" {
    const fixture = @import("../ops/media_residency_test.zig");
    try fixture.onMetal(populateFlorenceCrossCacheTestWeights, checkFlorenceCrossCacheMetalFrameReuse);
}

pub fn decoderForwardCached(
    control_cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    decoder_input_ids: []const i64,
    encoder_mask: []const i64,
    batch: usize,
    dec_seq: usize,
    enc_seq: usize,
    cross_cache: *const DecoderCrossCache,
) ![]f32 {
    try control_cb.checkExecutionControl();
    const layer_cb = layerComputeBackend(control_cb);
    const cb = &layer_cb;
    if (cross_cache.keys.len != config.decoder_layers or cross_cache.values.len != config.decoder_layers) return error.InvalidInputShape;

    const dec_total = batch * dec_seq;
    var hidden = try applyDecoderEmbeddings(cb, allocator, config, decoder_input_ids, batch, dec_seq);
    var hidden_live = true;
    errdefer if (hidden_live) cb.free(hidden);

    var buf: [256]u8 = undefined;
    for (0..config.decoder_layers) |layer| {
        try control_cb.checkExecutionControl();
        const new_hidden = try decoderBlockWithCrossCache(
            cb,
            config,
            hidden,
            cross_cache.keys[layer],
            cross_cache.values[layer],
            encoder_mask,
            batch,
            dec_seq,
            enc_seq,
            layer,
            &buf,
        );
        cb.free(hidden);
        hidden = new_hidden;
    }

    if (try tryOptionalWeight(cb, "language_model.model.decoder.layer_norm.weight")) |ln_w| {
        const ln_b = (try tryOptionalWeight(cb, "language_model.model.decoder.layer_norm.bias")) orelse return error.MissingWeight;
        const normed = try cb.layerNorm(hidden, ln_w, ln_b, config.d_model, 1e-5);
        cb.free(hidden);
        hidden = normed;
    } else if (try tryOptionalWeight(cb, "model.decoder.layer_norm.weight")) |ln_w| {
        const ln_b = (try tryOptionalWeight(cb, "model.decoder.layer_norm.bias")) orelse return error.MissingWeight;
        const normed = try cb.layerNorm(hidden, ln_w, ln_b, config.d_model, 1e-5);
        cb.free(hidden);
        hidden = normed;
    }

    const lm_w = try lmHeadWeight(cb);
    const logits = try cb.linearNoBias(hidden, lm_w, dec_total, config.d_model, config.vocab_size);
    defer cb.free(logits);
    cb.free(hidden);
    hidden_live = false;

    const result = try cb.toFloat32(logits, allocator);
    errdefer allocator.free(result);
    if (try tryOptionalWeight(cb, "language_model.final_logits_bias")) |logits_bias| {
        const bias = try cb.toFloat32(logits_bias, allocator);
        defer allocator.free(bias);
        const bias_offset: usize = if (bias.len == config.vocab_size) 0 else if (bias.len == config.vocab_size + 1) 1 else 0;
        const bias_slice = bias[bias_offset..][0..config.vocab_size];
        for (0..dec_total) |row| {
            const row_slice = result[row * config.vocab_size ..][0..config.vocab_size];
            for (row_slice, bias_slice) |*value, bias_value| value.* += bias_value;
        }
    }
    return result;
}

fn visionEncoderForward(
    control_cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    pixel_values: []const f32,
    batch: usize,
) !VisionForwardResult {
    try control_cb.checkExecutionControl();
    const layer_cb = layerComputeBackend(control_cb);
    const cb = &layer_cb;
    const debug_cuda_session = platform.env.getenvBool("ANTFLY_INFERENCE_DEBUG_CUDA_SESSION");
    const profile = readProfileEnabled();
    if (debug_cuda_session) std.log.info("florence: vision start backend={s}", .{@tagName(cb.kind())});
    var stage_tokens: ?[]f32 = null;
    errdefer if (stage_tokens) |tokens| allocator.free(tokens);
    var stage_hidden: ?CT = null;
    errdefer if (stage_hidden) |hidden| cb.free(hidden);
    var stage_h: usize = config.image_size;
    var stage_w: usize = config.image_size;

    for (0..Config.stage_count) |stage| {
        try control_cb.checkExecutionControl();
        const stage_start = nowNs();
        var hidden_for_stage: ?CT = null;
        if (useTensorNativeVision(cb) and stage == 0) {
            if (debug_cuda_session) std.log.info("florence: stage 0 device conv embed start h={d} w={d}", .{ stage_h, stage_w });
            const conv_start = nowNs();
            const embedded = try convEmbedImageTensor(cb, config, batch, pixel_values, stage_h, stage_w);
            if (profile) logFlorenceProfileStage("vision_stage_conv_embed_device", stage, conv_start);
            hidden_for_stage = embedded.tensor;
            stage_h = embedded.height;
            stage_w = embedded.width;
        } else if (useTensorNativeVision(cb) and stage_hidden != null) {
            if (debug_cuda_session) std.log.info("florence: stage {d} tensor conv embed start h={d} w={d}", .{ stage, stage_h, stage_w });
            const conv_start = nowNs();
            const embedded = try convEmbedTensor(cb, allocator, config, stage, batch, stage_hidden.?, stage_h, stage_w);
            cb.free(stage_hidden.?);
            stage_hidden = null;
            if (profile) logFlorenceProfileStage("vision_stage_conv_embed", stage, conv_start);
            if (debug_cuda_session) std.log.info("florence: stage {d} tensor conv embed done h={d} w={d}", .{ stage, embedded.height, embedded.width });
            hidden_for_stage = embedded.tensor;
            stage_h = embedded.height;
            stage_w = embedded.width;
        } else {
            if (debug_cuda_session) std.log.info("florence: stage {d} conv embed start h={d} w={d}", .{ stage, stage_h, stage_w });
            const conv_start = nowNs();
            const embedded = try convEmbed(cb, allocator, config, stage, batch, pixel_values, stage_tokens, stage_h, stage_w);
            if (profile) logFlorenceProfileStage("vision_stage_conv_embed", stage, conv_start);
            if (debug_cuda_session) std.log.info("florence: stage {d} conv embed done h={d} w={d}", .{ stage, embedded.height, embedded.width });
            if (stage_tokens) |old| allocator.free(old);
            stage_tokens = embedded.tokens;
            stage_h = embedded.height;
            stage_w = embedded.width;
        }

        const depth: usize = config.depths[stage];
        if (useTensorNativeVision(cb)) {
            var hidden = if (hidden_for_stage) |tensor| tensor else blk: {
                if (debug_cuda_session) std.log.info("florence: stage {d} tensor native upload start depth={d}", .{ stage, depth });
                const upload_start = nowNs();
                const stage_shape = [_]i32{ @intCast(batch * stage_h * stage_w), @intCast(config.dim_embed[stage]) };
                const tensor = try cb.fromFloat32Shape(stage_tokens.?, &stage_shape);
                if (profile) logFlorenceProfileStage("vision_stage_upload", stage, upload_start);
                allocator.free(stage_tokens.?);
                stage_tokens = null;
                break :blk tensor;
            };
            var hidden_owned = true;
            errdefer if (hidden_owned) cb.free(hidden);

            var stage_frame_active = if (florenceMetalStageFramesEnabled())
                try beginFlorenceMetalFrame(cb, .prefill)
            else
                false;
            errdefer if (stage_frame_active) cb.decoderRuntimeCancelFrame() catch {};

            for (0..depth) |layer| {
                try control_cb.checkExecutionControl();
                if (debug_cuda_session) std.log.info("florence: stage {d} davit layer {d} start", .{ stage, layer });
                const layer_start = nowNs();
                const next = try daViTBlockFramed(cb, allocator, config, hidden, batch, stage_h, stage_w, stage, layer);
                if (profile) logFlorenceProfileStageLayer("vision_stage_davit_layer", stage, layer, layer_start);
                hidden = next;
                if (debug_cuda_session) std.log.info("florence: stage {d} davit layer {d} done", .{ stage, layer });
            }
            if (stage_frame_active) {
                const submit_start = nowNs();
                try cb.decoderRuntimeSubmitAndWaitFrame();
                stage_frame_active = false;
                if (profile) logFlorenceProfileStage("vision_stage_frame_submit", stage, submit_start);
            }

            if (stage + 1 == Config.stage_count) {
                if (debug_cuda_session) std.log.info("florence: stage {d} download start", .{stage});
                const download_start = nowNs();
                stage_tokens = try cb.toFloat32(hidden, allocator);
                cb.free(hidden);
                hidden_owned = false;
                if (profile) logFlorenceProfileStage("vision_stage_download", stage, download_start);
                if (debug_cuda_session) std.log.info("florence: stage {d} download done", .{stage});
            } else {
                stage_hidden = hidden;
                hidden_owned = false;
            }
        } else {
            for (0..depth) |layer| {
                try control_cb.checkExecutionControl();
                stage_tokens = try daViTBlockData(cb, allocator, config, stage_tokens.?, batch, stage_h, stage_w, stage, layer);
            }
        }
        if (profile) logFlorenceProfileStage("vision_stage_total", stage, stage_start);
    }

    if (debug_cuda_session) std.log.info("florence: vision stages complete", .{});
    const tokens = stage_tokens orelse return error.MissingInputs;
    const vision_dim: usize = config.dim_embed[Config.stage_count - 1];
    const token_count = stage_h * stage_w;

    if (debug_cuda_session) std.log.info("florence: add 2d pos start token_count={d} dim={d}", .{ token_count, vision_dim });
    const pos_start = nowNs();
    try add2dPositionalEmbedding(cb, allocator, tokens, batch, token_count, vision_dim, stage_h, stage_w);
    if (profile) logFlorenceProfile("vision_add_2d_pos", pos_start);
    if (debug_cuda_session) std.log.info("florence: add 2d pos done", .{});
    if (debug_cuda_session) std.log.info("florence: add temporal start", .{});
    const temporal_start = nowNs();
    try addTemporalEmbedding(cb, allocator, tokens, batch, token_count, vision_dim);
    if (profile) logFlorenceProfile("vision_add_temporal", temporal_start);
    if (debug_cuda_session) std.log.info("florence: add temporal done", .{});

    if (debug_cuda_session) std.log.info("florence: feature source concat start", .{});
    const concat_start = nowNs();
    const sourced = try imageFeatureSourceConcat(allocator, config, tokens, batch, token_count, vision_dim);
    if (profile) logFlorenceProfile("vision_feature_source_concat", concat_start);
    if (debug_cuda_session) std.log.info("florence: feature source concat done seq={d}", .{sourced.seq_len});
    defer allocator.free(sourced.data);
    allocator.free(tokens);

    const proj_rows = batch * sourced.seq_len;
    if (debug_cuda_session) std.log.info("florence: projection linear start rows={d}", .{proj_rows});
    const proj_start = nowNs();
    const projected = try projectImageFeaturesData(cb, allocator, sourced.data, proj_rows, vision_dim, config);
    if (profile) logFlorenceProfile("vision_projection_linear", proj_start);
    if (debug_cuda_session) std.log.info("florence: projection linear done", .{});
    defer allocator.free(projected);

    if (debug_cuda_session) std.log.info("florence: image proj norm start", .{});
    const norm_start = nowNs();
    const normed = try backendLayerNormData(
        cb,
        allocator,
        projected,
        proj_rows,
        config.projection_dim,
        try cb.getWeight("image_proj_norm.weight"),
        try cb.getWeight("image_proj_norm.bias"),
        1e-5,
    );
    if (profile) logFlorenceProfile("vision_image_proj_norm", norm_start);
    if (debug_cuda_session) std.log.info("florence: image proj norm done", .{});

    return .{ .features = normed, .seq_len = sourced.seq_len };
}

/// Bound DaViT activations independently of the text encoder batch. A large
/// decoder cohort can retain tensor-core batching in BART without materializing
/// every page's much larger early-stage vision tensors at the same time.
/// The caller still holds the original complete-cohort admission permit.
fn visionEncoderForwardTensorTailMicrobatched(
    control_cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    pixel_values: []const f32,
    batch: usize,
) !?VisionForwardTensorResult {
    const limit = cudaEncoderMicrobatchSize(control_cb.kind(), batch, platform.env.getenvUsize("ANTFLY_FLORENCE2_CUDA_VISION_MICROBATCH"));
    const layer_cb = layerComputeBackend(control_cb);
    if (limit == batch or !useDeviceVisionTail(&layer_cb, config, limit))
        return visionEncoderForwardTensorTail(control_cb, allocator, config, pixel_values, batch);
    if (pixel_values.len % batch != 0) return error.InvalidInputShape;
    const pixels_per_page = pixel_values.len / batch;
    var output: ?CT = null;
    errdefer if (output) |value| control_cb.free(value);
    var sequence: usize = 0;
    var offset: usize = 0;
    var chunks: usize = 0;
    while (offset < batch) {
        try control_cb.checkExecutionControl();
        const count = @min(limit, batch - offset);
        // The supported source configuration was qualified before executing
        // any chunk; declining after a partial execution is an error.
        const encoded = (try visionEncoderForwardTensorTail(control_cb, allocator, config, pixel_values[offset * pixels_per_page ..][0 .. count * pixels_per_page], count)) orelse return error.UnsupportedTensorType;
        defer control_cb.free(encoded.features);
        if (output == null) {
            sequence = encoded.seq_len;
            const rows = std.math.mul(usize, batch, sequence) catch return error.InvalidInputShape;
            output = (try control_cb.allocUninitF32Shape(&.{ @intCast(rows), @intCast(config.d_model) })) orelse return error.UnsupportedTensorType;
        } else if (sequence != encoded.seq_len) return error.InvalidInputShape;
        if (!try control_cb.copyRows2D(allocator, output.?, offset * sequence, encoded.features, 0, count * sequence, config.d_model)) return error.UnsupportedTensorType;
        offset += count;
        chunks += 1;
    }
    if (platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3"))
        std.debug.print("FLORENCE2 cuda_vision_microbatch_chunks={d} items={d} limit={d}\n", .{ chunks, batch, limit });
    return .{ .features = output.?, .seq_len = sequence };
}

fn visionEncoderForwardTensorTail(
    control_cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    pixel_values: []const f32,
    batch: usize,
) !?VisionForwardTensorResult {
    try control_cb.checkExecutionControl();
    const layer_cb = layerComputeBackend(control_cb);
    const cb = &layer_cb;
    if (!useDeviceVisionTail(cb, config, batch)) return null;

    const debug_cuda_session = platform.env.getenvBool("ANTFLY_INFERENCE_DEBUG_CUDA_SESSION");
    const profile = readProfileEnabled();
    if (debug_cuda_session) std.log.info("florence: device vision tail start backend={s}", .{@tagName(cb.kind())});
    var stage_hidden: ?CT = null;
    errdefer if (stage_hidden) |hidden| cb.free(hidden);
    var stage_h: usize = config.image_size;
    var stage_w: usize = config.image_size;

    for (0..Config.stage_count) |stage| {
        try control_cb.checkExecutionControl();
        const stage_start = nowNs();
        var hidden_for_stage: ?CT = null;
        if (stage == 0) {
            if (debug_cuda_session) std.log.info("florence: stage 0 device conv embed start h={d} w={d}", .{ stage_h, stage_w });
            const conv_start = nowNs();
            const embedded = try convEmbedImageTensor(cb, config, batch, pixel_values, stage_h, stage_w);
            if (profile) logFlorenceProfileStage("vision_stage_conv_embed_device", stage, conv_start);
            hidden_for_stage = embedded.tensor;
            stage_h = embedded.height;
            stage_w = embedded.width;
        } else if (stage_hidden != null) {
            if (debug_cuda_session) std.log.info("florence: stage {d} tensor conv embed start h={d} w={d}", .{ stage, stage_h, stage_w });
            const conv_start = nowNs();
            const embedded = try convEmbedTensor(cb, allocator, config, stage, batch, stage_hidden.?, stage_h, stage_w);
            cb.free(stage_hidden.?);
            stage_hidden = null;
            if (profile) logFlorenceProfileStage("vision_stage_conv_embed", stage, conv_start);
            if (debug_cuda_session) std.log.info("florence: stage {d} tensor conv embed done h={d} w={d}", .{ stage, embedded.height, embedded.width });
            hidden_for_stage = embedded.tensor;
            stage_h = embedded.height;
            stage_w = embedded.width;
        } else return error.MissingInputs;

        const depth: usize = config.depths[stage];
        var hidden = hidden_for_stage orelse return error.MissingInputs;
        var hidden_owned = true;
        errdefer if (hidden_owned) cb.free(hidden);

        var stage_frame_active = if (florenceMetalStageFramesEnabled())
            try beginFlorenceMetalFrame(cb, .prefill)
        else
            false;
        errdefer if (stage_frame_active) cb.decoderRuntimeCancelFrame() catch {};

        for (0..depth) |layer| {
            try control_cb.checkExecutionControl();
            if (debug_cuda_session) std.log.info("florence: stage {d} davit layer {d} start", .{ stage, layer });
            const layer_start = nowNs();
            const next = try daViTBlockFramed(cb, allocator, config, hidden, batch, stage_h, stage_w, stage, layer);
            if (profile) logFlorenceProfileStageLayer("vision_stage_davit_layer", stage, layer, layer_start);
            hidden = next;
            if (debug_cuda_session) std.log.info("florence: stage {d} davit layer {d} done", .{ stage, layer });
        }
        if (stage_frame_active) {
            const submit_start = nowNs();
            try cb.decoderRuntimeSubmitAndWaitFrame();
            stage_frame_active = false;
            if (profile) logFlorenceProfileStage("vision_stage_frame_submit", stage, submit_start);
        }

        if (stage + 1 == Config.stage_count) {
            const result = (try finishVisionTailTensor(cb, allocator, config, hidden, batch, stage_h, stage_w)) orelse {
                cb.free(hidden);
                hidden_owned = false;
                return null;
            };
            cb.free(hidden);
            hidden_owned = false;
            if (profile) logFlorenceProfileStage("vision_stage_total", stage, stage_start);
            return result;
        }

        stage_hidden = hidden;
        hidden_owned = false;
        if (profile) logFlorenceProfileStage("vision_stage_total", stage, stage_start);
    }

    return null;
}

fn useDeviceVisionTail(cb: *const ComputeBackend, config: Config, batch: usize) bool {
    return cb.kind() == .cuda and
        (batch == 1 or (batch > 1 and batch <= 64 and platform.env.getenvBool("ANTFLY_CUDA_ENABLE_FLORENCE_BATCH_VISION_TAIL"))) and
        !florenceDeviceVisionTailDisabled() and
        config.image_feature_source_count == 2 and
        config.image_feature_sources[0] == .spatial_avg_pool and
        config.image_feature_sources[1] == .temporal_avg_pool;
}

fn finishVisionTailTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    tokens: CT,
    batch: usize,
    height: usize,
    width: usize,
) !?VisionForwardTensorResult {
    const debug_cuda_session = platform.env.getenvBool("ANTFLY_INFERENCE_DEBUG_CUDA_SESSION");
    const profile = readProfileEnabled();
    const vision_dim: usize = config.dim_embed[Config.stage_count - 1];
    const token_count = height * width;

    if (debug_cuda_session) std.log.info("florence: device feature source tail start token_count={d} dim={d}", .{ token_count, vision_dim });
    const concat_start = nowNs();
    const temporal_w = tryOptionalWeight(cb, "visual_temporal_embed.pos_idx_to_embed") catch null;

    const sourced = (try cb.florenceVisionTailSources(
        tokens,
        try cb.getWeight("image_pos_embed.row_embeddings.weight"),
        try cb.getWeight("image_pos_embed.column_embeddings.weight"),
        temporal_w,
        batch,
        height,
        width,
        vision_dim,
    )) orelse return null;
    var sourced_owned = true;
    errdefer if (sourced_owned) cb.free(sourced);
    if (profile) logFlorenceProfile("vision_device_tail_sources", concat_start);
    if (debug_cuda_session) std.log.info("florence: device feature source tail done seq={d}", .{token_count + 1});

    const proj_rows = batch * (token_count + 1);
    if (debug_cuda_session) std.log.info("florence: projection linear start rows={d}", .{proj_rows});
    const proj_start = nowNs();
    const projected = try projectImageFeaturesTensor(cb, allocator, sourced, proj_rows, vision_dim, config);
    cb.free(sourced);
    sourced_owned = false;
    var projected_owned = true;
    errdefer if (projected_owned) cb.free(projected);
    if (profile) logFlorenceProfile("vision_projection_linear", proj_start);
    if (debug_cuda_session) std.log.info("florence: projection linear done", .{});

    if (debug_cuda_session) std.log.info("florence: image proj norm start", .{});
    const norm_start = nowNs();
    const normed = try cb.layerNorm(
        projected,
        try cb.getWeight("image_proj_norm.weight"),
        try cb.getWeight("image_proj_norm.bias"),
        config.projection_dim,
        1e-5,
    );
    cb.free(projected);
    projected_owned = false;
    if (profile) logFlorenceProfile("vision_image_proj_norm", norm_start);
    if (debug_cuda_session) std.log.info("florence: image proj norm done", .{});

    return .{ .features = normed, .seq_len = token_count + 1 };
}

fn projectImageFeaturesTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: CT,
    rows: usize,
    vision_dim: usize,
    config: Config,
) !CT {
    const proj_weight = try cb.getWeight("image_projection");
    if (try cb.florenceProjectImageFeatures(input, proj_weight, rows, vision_dim, config.projection_dim)) |projected| {
        return projected;
    }

    const input_shape = [_]i64{ @intCast(vision_dim), @intCast(config.projection_dim) };
    const proj_ct = cb.primTranspose(proj_weight, &.{ 1, 0 }, &input_shape) catch |err| switch (err) {
        error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.InvalidShape, error.UnsupportedTensorType => {
            const proj_weight_data = try cb.toFloat32(proj_weight, allocator);
            defer allocator.free(proj_weight_data);
            const proj_weight_t = try transposeMatrix(allocator, proj_weight_data, vision_dim, config.projection_dim);
            defer allocator.free(proj_weight_t);
            const proj_shape = [_]i32{ @intCast(config.projection_dim), @intCast(vision_dim) };
            const fallback_proj_ct = try cb.fromFloat32Shape(proj_weight_t, &proj_shape);
            defer cb.free(fallback_proj_ct);
            return try cb.linearNoBias(input, fallback_proj_ct, rows, vision_dim, config.projection_dim);
        },
        else => return err,
    };
    defer cb.free(proj_ct);
    return try cb.linearNoBias(input, proj_ct, rows, vision_dim, config.projection_dim);
}

fn useTensorNativeVision(cb: *const ComputeBackend) bool {
    return cb.kind() == .metal or cb.kind() == .cuda;
}

fn florenceDeviceVisionTailDisabled() bool {
    return platform.env.getenvBool("ANTFLY_INFERENCE_FLORENCE_DISABLE_DEVICE_VISION_TAIL");
}

fn daViTBlockData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input: []f32,
    batch: usize,
    height: usize,
    width: usize,
    stage: usize,
    layer: usize,
) ![]f32 {
    var hidden = input;
    hidden = try spatialBlockData(cb, allocator, config, hidden, batch, height, width, stage, layer);
    hidden = try channelBlockData(cb, allocator, config, hidden, batch, height, width, stage, layer);
    return hidden;
}

fn spatialBlockData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input: []f32,
    batch: usize,
    height: usize,
    width: usize,
    stage: usize,
    layer: usize,
) ![]f32 {
    const dim: usize = config.dim_embed[stage];
    var hidden = input;

    hidden = try residualDepthwiseConvData(cb, allocator, hidden, batch, height, width, dim, stage, layer, "spatial_block.conv1");
    hidden = try residualWindowAttentionData(cb, allocator, hidden, batch, height, width, dim, config.num_heads[stage], config.window_size, stage, layer);
    hidden = try residualDepthwiseConvData(cb, allocator, hidden, batch, height, width, dim, stage, layer, "spatial_block.conv2");
    hidden = try residualMlpData(cb, allocator, hidden, batch, height * width, dim, dim * 4, stage, layer, "spatial_block.ffn");
    return hidden;
}

fn channelBlockData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input: []f32,
    batch: usize,
    height: usize,
    width: usize,
    stage: usize,
    layer: usize,
) ![]f32 {
    const dim: usize = config.dim_embed[stage];
    var hidden = input;

    hidden = try residualDepthwiseConvData(cb, allocator, hidden, batch, height, width, dim, stage, layer, "channel_block.conv1");
    hidden = try residualChannelAttentionData(cb, allocator, hidden, batch, height * width, dim, config.num_groups[stage], stage, layer);
    hidden = try residualDepthwiseConvData(cb, allocator, hidden, batch, height, width, dim, stage, layer, "channel_block.conv2");
    hidden = try residualMlpData(cb, allocator, hidden, batch, height * width, dim, dim * 4, stage, layer, "channel_block.ffn");
    return hidden;
}

fn residualDepthwiseConvData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: []f32,
    batch: usize,
    height: usize,
    width: usize,
    channels: usize,
    stage: usize,
    layer: usize,
    block_prefix: []const u8,
) ![]f32 {
    const weight_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.fn.dw.weight", .{ stage, layer, block_prefix });
    defer allocator.free(weight_name);
    const bias_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.fn.dw.bias", .{ stage, layer, block_prefix });
    defer allocator.free(bias_name);

    const input_shape = [_]i32{ @intCast(batch * height * width), @intCast(channels) };
    const input_ct = try cb.fromFloat32Shape(input, &input_shape);
    defer cb.free(input_ct);
    const updates_ct = try cb.tokenGridConv2d(
        input_ct,
        try cb.getWeight(weight_name),
        try cb.getWeight(bias_name),
        batch,
        channels,
        channels,
        height,
        width,
        3,
        3,
        1,
        1,
        1,
        1,
        channels,
    );
    defer cb.free(updates_ct);
    const updates = try cb.toFloat32(updates_ct, allocator);
    defer allocator.free(updates);

    addInPlace(input, updates);
    return input;
}

fn residualWindowAttentionData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: []f32,
    batch: usize,
    height: usize,
    width: usize,
    dim: usize,
    num_heads: usize,
    window_size: usize,
    stage: usize,
    layer: usize,
) ![]f32 {
    const norm_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.spatial_block.window_attn.norm.weight", .{ stage, layer });
    defer allocator.free(norm_w_name);
    const norm_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.spatial_block.window_attn.norm.bias", .{ stage, layer });
    defer allocator.free(norm_b_name);
    const qkv_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.spatial_block.window_attn.fn.qkv.weight", .{ stage, layer });
    defer allocator.free(qkv_w_name);
    const qkv_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.spatial_block.window_attn.fn.qkv.bias", .{ stage, layer });
    defer allocator.free(qkv_b_name);
    const proj_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.spatial_block.window_attn.fn.proj.weight", .{ stage, layer });
    defer allocator.free(proj_w_name);
    const proj_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.spatial_block.window_attn.fn.proj.bias", .{ stage, layer });
    defer allocator.free(proj_b_name);

    const input_shape = [_]i32{ @intCast(batch * height * width), @intCast(dim) };
    const input_ct = try cb.fromFloat32Shape(input, &input_shape);
    defer cb.free(input_ct);
    const out_ct = try cb.windowedSelfAttention(
        input_ct,
        try cb.getWeight(norm_w_name),
        try cb.getWeight(norm_b_name),
        try cb.getWeight(qkv_w_name),
        try cb.getWeight(qkv_b_name),
        try cb.getWeight(proj_w_name),
        try cb.getWeight(proj_b_name),
        batch,
        height,
        width,
        dim,
        num_heads,
        window_size,
    );
    defer cb.free(out_ct);
    const out = try cb.toFloat32(out_ct, allocator);
    defer allocator.free(out);
    addInPlace(input, out);
    return input;
}

fn residualChannelAttentionData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: []f32,
    batch: usize,
    seq_len: usize,
    dim: usize,
    groups: usize,
    stage: usize,
    layer: usize,
) ![]f32 {
    const norm_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.channel_block.channel_attn.norm.weight", .{ stage, layer });
    defer allocator.free(norm_w_name);
    const norm_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.channel_block.channel_attn.norm.bias", .{ stage, layer });
    defer allocator.free(norm_b_name);
    const qkv_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.channel_block.channel_attn.fn.qkv.weight", .{ stage, layer });
    defer allocator.free(qkv_w_name);
    const qkv_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.channel_block.channel_attn.fn.qkv.bias", .{ stage, layer });
    defer allocator.free(qkv_b_name);
    const proj_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.channel_block.channel_attn.fn.proj.weight", .{ stage, layer });
    defer allocator.free(proj_w_name);
    const proj_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.channel_block.channel_attn.fn.proj.bias", .{ stage, layer });
    defer allocator.free(proj_b_name);

    const input_shape = [_]i32{ @intCast(batch * seq_len), @intCast(dim) };
    const input_ct = try cb.fromFloat32Shape(input, &input_shape);
    defer cb.free(input_ct);
    const out_ct = try cb.channelSelfAttention(
        input_ct,
        try cb.getWeight(norm_w_name),
        try cb.getWeight(norm_b_name),
        try cb.getWeight(qkv_w_name),
        try cb.getWeight(qkv_b_name),
        try cb.getWeight(proj_w_name),
        try cb.getWeight(proj_b_name),
        batch,
        seq_len,
        dim,
        groups,
    );
    defer cb.free(out_ct);
    const out = try cb.toFloat32(out_ct, allocator);
    defer allocator.free(out);
    addInPlace(input, out);
    return input;
}

fn residualMlpData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: []f32,
    batch: usize,
    seq_len: usize,
    dim: usize,
    hidden_dim: usize,
    stage: usize,
    layer: usize,
    block_prefix: []const u8,
) ![]f32 {
    const norm_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.norm.weight", .{ stage, layer, block_prefix });
    defer allocator.free(norm_w_name);
    const norm_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.norm.bias", .{ stage, layer, block_prefix });
    defer allocator.free(norm_b_name);
    const fc1_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.fn.net.fc1.weight", .{ stage, layer, block_prefix });
    defer allocator.free(fc1_w_name);
    const fc1_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.fn.net.fc1.bias", .{ stage, layer, block_prefix });
    defer allocator.free(fc1_b_name);
    const fc2_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.fn.net.fc2.weight", .{ stage, layer, block_prefix });
    defer allocator.free(fc2_w_name);
    const fc2_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.fn.net.fc2.bias", .{ stage, layer, block_prefix });
    defer allocator.free(fc2_b_name);

    const updates = try backendNormedExactGeluMlpData(
        cb,
        allocator,
        input,
        batch * seq_len,
        dim,
        hidden_dim,
        try cb.getWeight(norm_w_name),
        try cb.getWeight(norm_b_name),
        try cb.getWeight(fc1_w_name),
        try cb.getWeight(fc1_b_name),
        try cb.getWeight(fc2_w_name),
        try cb.getWeight(fc2_b_name),
    );
    defer allocator.free(updates);
    addInPlace(input, updates);
    return input;
}

const ConvEmbedResult = struct {
    tokens: []f32,
    height: usize,
    width: usize,
};

const ConvEmbedTensorResult = struct {
    tensor: CT,
    height: usize,
    width: usize,
};

fn convEmbed(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    stage: usize,
    batch: usize,
    pixel_values: []const f32,
    input_tokens: ?[]const f32,
    input_h: usize,
    input_w: usize,
) !ConvEmbedResult {
    const in_channels: usize = if (stage == 0) 3 else config.dim_embed[stage - 1];
    const out_channels: usize = config.dim_embed[stage];
    const patch: usize = config.patch_size[stage];
    const stride: usize = config.patch_stride[stage];
    const padding: usize = config.patch_padding[stage];

    const proj_w_name = try fmtAlloc(allocator, "vision_tower.convs.{d}.proj.weight", .{stage});
    defer allocator.free(proj_w_name);
    const proj_b_name = try fmtAlloc(allocator, "vision_tower.convs.{d}.proj.bias", .{stage});
    defer allocator.free(proj_b_name);
    const norm_w_name = try fmtAlloc(allocator, "vision_tower.convs.{d}.norm.weight", .{stage});
    defer allocator.free(norm_w_name);
    const norm_b_name = try fmtAlloc(allocator, "vision_tower.convs.{d}.norm.bias", .{stage});
    defer allocator.free(norm_b_name);

    const norm_w = try cb.getWeight(norm_w_name);
    const norm_b = try cb.getWeight(norm_b_name);

    const proj_w = try cb.getWeight(proj_w_name);
    const proj_b = try cb.getWeight(proj_b_name);

    var tokens = if (stage == 0) blk: {
        const conv = try backendConv2dImageData(
            cb,
            allocator,
            pixel_values,
            batch,
            in_channels,
            input_h,
            input_w,
            out_channels,
            patch,
            patch,
            stride,
            stride,
            padding,
            padding,
            1,
            proj_w,
            proj_b,
        );
        defer allocator.free(conv.data);
        break :blk try imageToTokens(allocator, conv.data, batch, conv.height, conv.width, out_channels);
    } else blk: {
        const input = input_tokens orelse return error.MissingInputs;
        const prepped = if (config.patch_prenorm[stage])
            try backendLayerNormData(cb, allocator, input, batch * input_h * input_w, in_channels, norm_w, norm_b, 1e-5)
        else
            try allocator.dupe(f32, input);
        defer allocator.free(prepped);
        const prepped_shape = [_]i32{ @intCast(batch * input_h * input_w), @intCast(in_channels) };
        const prepped_ct = try cb.fromFloat32Shape(prepped, &prepped_shape);
        defer cb.free(prepped_ct);
        const tokens_ct = try cb.tokenGridConv2d(
            prepped_ct,
            proj_w,
            proj_b,
            batch,
            in_channels,
            out_channels,
            input_h,
            input_w,
            patch,
            patch,
            stride,
            stride,
            padding,
            padding,
            1,
        );
        defer cb.free(tokens_ct);
        break :blk try cb.toFloat32(tokens_ct, allocator);
    };

    const out_h = (input_h + 2 * padding - patch) / stride + 1;
    const out_w = (input_w + 2 * padding - patch) / stride + 1;
    if (!config.patch_prenorm[stage]) {
        const normed = try backendLayerNormData(cb, allocator, tokens, batch * out_h * out_w, out_channels, norm_w, norm_b, 1e-5);
        allocator.free(tokens);
        tokens = normed;
    }

    return .{ .tokens = tokens, .height = out_h, .width = out_w };
}

fn convEmbedTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    stage: usize,
    batch: usize,
    input: CT,
    input_h: usize,
    input_w: usize,
) !ConvEmbedTensorResult {
    if (stage == 0) return error.InvalidStage;
    const in_channels: usize = config.dim_embed[stage - 1];
    const out_channels: usize = config.dim_embed[stage];
    const patch: usize = config.patch_size[stage];
    const stride: usize = config.patch_stride[stage];
    const padding: usize = config.patch_padding[stage];

    const proj_w_name = try fmtAlloc(allocator, "vision_tower.convs.{d}.proj.weight", .{stage});
    defer allocator.free(proj_w_name);
    const proj_b_name = try fmtAlloc(allocator, "vision_tower.convs.{d}.proj.bias", .{stage});
    defer allocator.free(proj_b_name);
    const norm_w_name = try fmtAlloc(allocator, "vision_tower.convs.{d}.norm.weight", .{stage});
    defer allocator.free(norm_w_name);
    const norm_b_name = try fmtAlloc(allocator, "vision_tower.convs.{d}.norm.bias", .{stage});
    defer allocator.free(norm_b_name);

    const norm_w = try cb.getWeight(norm_w_name);
    const norm_b = try cb.getWeight(norm_b_name);
    const proj_w = try cb.getWeight(proj_w_name);
    const proj_b = try cb.getWeight(proj_b_name);

    const conv_input = if (config.patch_prenorm[stage])
        try cb.layerNorm(input, norm_w, norm_b, in_channels, 1e-5)
    else
        input;
    const owns_conv_input = config.patch_prenorm[stage];
    defer if (owns_conv_input) cb.free(conv_input);

    var tokens = try cb.tokenGridConv2d(
        conv_input,
        proj_w,
        proj_b,
        batch,
        in_channels,
        out_channels,
        input_h,
        input_w,
        patch,
        patch,
        stride,
        stride,
        padding,
        padding,
        1,
    );
    errdefer cb.free(tokens);

    const out_h = (input_h + 2 * padding - patch) / stride + 1;
    const out_w = (input_w + 2 * padding - patch) / stride + 1;
    if (!config.patch_prenorm[stage]) {
        const normed = try cb.layerNorm(tokens, norm_w, norm_b, out_channels, 1e-5);
        cb.free(tokens);
        tokens = normed;
    }

    return .{ .tensor = tokens, .height = out_h, .width = out_w };
}

/// Stage-zero patch embedding for the device-resident vision path. The
/// normalized NCHW pixels are uploaded once; convolution output is reordered
/// to token-major form on the backend and never materialized as a host f32
/// array. At Florence's default 768px input this avoids a ~36 MiB host round
/// trip per page before the first vision block.
fn convEmbedImageTensor(
    cb: *const ComputeBackend,
    config: Config,
    batch: usize,
    pixel_values: []const f32,
    input_h: usize,
    input_w: usize,
) !ConvEmbedTensorResult {
    const stage: usize = 0;
    const in_channels: usize = 3;
    const out_channels: usize = config.dim_embed[stage];
    const patch: usize = config.patch_size[stage];
    const stride: usize = config.patch_stride[stage];
    const padding: usize = config.patch_padding[stage];
    const out_h = (input_h + 2 * padding - patch) / stride + 1;
    const out_w = (input_w + 2 * padding - patch) / stride + 1;

    const input_shape = [_]i32{
        @intCast(batch),
        @intCast(in_channels),
        @intCast(input_h),
        @intCast(input_w),
    };
    const input = try cb.fromFloat32Shape(pixel_values, &input_shape);
    defer cb.free(input);
    const conv = try cb.conv2d(
        input,
        try cb.getWeight("vision_tower.convs.0.proj.weight"),
        try cb.getWeight("vision_tower.convs.0.proj.bias"),
        batch,
        in_channels,
        out_channels,
        input_h,
        input_w,
        patch,
        patch,
        stride,
        stride,
        padding,
        padding,
        1,
    );
    defer cb.free(conv);

    const conv_shape = [_]i64{
        @intCast(batch),
        @intCast(out_channels),
        @intCast(out_h),
        @intCast(out_w),
    };
    const nhwc = try cb.primTranspose(conv, &.{ 0, 2, 3, 1 }, &conv_shape);
    defer cb.free(nhwc);
    const token_shape = [_]i64{
        @intCast(batch * out_h * out_w),
        @intCast(out_channels),
    };
    var tokens = try cb.primReshape(nhwc, &token_shape);
    errdefer cb.free(tokens);
    if (!config.patch_prenorm[stage]) {
        const normed = try cb.layerNorm(
            tokens,
            try cb.getWeight("vision_tower.convs.0.norm.weight"),
            try cb.getWeight("vision_tower.convs.0.norm.bias"),
            out_channels,
            1e-5,
        );
        cb.free(tokens);
        tokens = normed;
    }
    return .{ .tensor = tokens, .height = out_h, .width = out_w };
}

fn daViTBlock(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input: CT,
    batch: usize,
    height: usize,
    width: usize,
    stage: usize,
    layer: usize,
) !CT {
    var hidden = input;
    var next = try spatialBlock(cb, allocator, config, hidden, batch, height, width, stage, layer);
    hidden = next;
    next = try channelBlock(cb, allocator, config, hidden, batch, height, width, stage, layer);
    hidden = next;
    return hidden;
}

fn daViTBlockFramed(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input: CT,
    batch: usize,
    height: usize,
    width: usize,
    stage: usize,
    layer: usize,
) !CT {
    var frame_active = try beginFlorenceMetalFrame(cb, .prefill);
    errdefer if (frame_active) cb.decoderRuntimeCancelFrame() catch {};

    const owns_encoder_scope = try MetalCompute.beginOcrEncoderScope(cb, .florence);
    defer if (owns_encoder_scope) cb.decoderRuntimeEndPlannedComputeScope();

    const output = try daViTBlock(cb, allocator, config, input, batch, height, width, stage, layer);
    errdefer cb.free(output);
    if (frame_active) {
        try cb.decoderRuntimeSubmitAndWaitFrame();
        frame_active = false;
    }
    return output;
}

fn spatialBlock(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input: CT,
    batch: usize,
    height: usize,
    width: usize,
    stage: usize,
    layer: usize,
) !CT {
    const profile = readProfileEnabled();
    const dim: usize = config.dim_embed[stage];
    var hidden = input;
    var op_start = nowNs();
    var next = try residualDepthwiseConv(cb, allocator, hidden, batch, height, width, dim, stage, layer, "spatial_block.conv1");
    if (profile) logFlorenceProfileStageLayerOp("vision_spatial_conv1", stage, layer, op_start);
    try profileFlorenceVisionOpFrameBoundary(cb, "vision_spatial_conv1_gpu", stage, layer, op_start);
    cb.free(hidden);
    hidden = next;
    op_start = nowNs();
    next = try residualWindowAttention(cb, allocator, hidden, batch, height, width, dim, config.num_heads[stage], config.window_size, stage, layer);
    if (profile) logFlorenceProfileStageLayerOp("vision_spatial_window_attn", stage, layer, op_start);
    try profileFlorenceVisionOpFrameBoundary(cb, "vision_spatial_window_attn_gpu", stage, layer, op_start);
    cb.free(hidden);
    hidden = next;
    op_start = nowNs();
    next = try residualDepthwiseConv(cb, allocator, hidden, batch, height, width, dim, stage, layer, "spatial_block.conv2");
    if (profile) logFlorenceProfileStageLayerOp("vision_spatial_conv2", stage, layer, op_start);
    try profileFlorenceVisionOpFrameBoundary(cb, "vision_spatial_conv2_gpu", stage, layer, op_start);
    cb.free(hidden);
    hidden = next;
    op_start = nowNs();
    next = try residualMlp(cb, allocator, hidden, batch, height * width, dim, dim * 4, stage, layer, "spatial_block.ffn");
    if (profile) logFlorenceProfileStageLayerOp("vision_spatial_mlp", stage, layer, op_start);
    try profileFlorenceVisionOpFrameBoundary(cb, "vision_spatial_mlp_gpu", stage, layer, op_start);
    cb.free(hidden);
    hidden = next;
    return hidden;
}

fn channelBlock(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input: CT,
    batch: usize,
    height: usize,
    width: usize,
    stage: usize,
    layer: usize,
) !CT {
    const profile = readProfileEnabled();
    const dim: usize = config.dim_embed[stage];
    var hidden = input;
    var op_start = nowNs();
    var next = try residualDepthwiseConv(cb, allocator, hidden, batch, height, width, dim, stage, layer, "channel_block.conv1");
    if (profile) logFlorenceProfileStageLayerOp("vision_channel_conv1", stage, layer, op_start);
    try profileFlorenceVisionOpFrameBoundary(cb, "vision_channel_conv1_gpu", stage, layer, op_start);
    cb.free(hidden);
    hidden = next;
    op_start = nowNs();
    next = try residualChannelAttention(cb, allocator, hidden, batch, height * width, dim, config.num_groups[stage], stage, layer);
    if (profile) logFlorenceProfileStageLayerOp("vision_channel_attn", stage, layer, op_start);
    try profileFlorenceVisionOpFrameBoundary(cb, "vision_channel_attn_gpu", stage, layer, op_start);
    cb.free(hidden);
    hidden = next;
    op_start = nowNs();
    next = try residualDepthwiseConv(cb, allocator, hidden, batch, height, width, dim, stage, layer, "channel_block.conv2");
    if (profile) logFlorenceProfileStageLayerOp("vision_channel_conv2", stage, layer, op_start);
    try profileFlorenceVisionOpFrameBoundary(cb, "vision_channel_conv2_gpu", stage, layer, op_start);
    cb.free(hidden);
    hidden = next;
    op_start = nowNs();
    next = try residualMlp(cb, allocator, hidden, batch, height * width, dim, dim * 4, stage, layer, "channel_block.ffn");
    if (profile) logFlorenceProfileStageLayerOp("vision_channel_mlp", stage, layer, op_start);
    try profileFlorenceVisionOpFrameBoundary(cb, "vision_channel_mlp_gpu", stage, layer, op_start);
    cb.free(hidden);
    hidden = next;
    return hidden;
}

fn residualDepthwiseConv(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: CT,
    batch: usize,
    height: usize,
    width: usize,
    channels: usize,
    stage: usize,
    layer: usize,
    block_prefix: []const u8,
) !CT {
    const weight_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.fn.dw.weight", .{ stage, layer, block_prefix });
    defer allocator.free(weight_name);
    const bias_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.fn.dw.bias", .{ stage, layer, block_prefix });
    defer allocator.free(bias_name);

    if (cb.kind() == .cuda) {
        if (try @import("../ops/cuda/cuda_compute.zig").CudaCompute.florenceDepthwiseResidual(cb, input, try cb.getWeight(weight_name), try cb.getWeight(bias_name), batch, height, width, channels)) |result| return result;
    }
    const updates_ct = try cb.tokenGridConv2d(
        input,
        try cb.getWeight(weight_name),
        try cb.getWeight(bias_name),
        batch,
        channels,
        channels,
        height,
        width,
        3,
        3,
        1,
        1,
        1,
        1,
        channels,
    );
    defer cb.free(updates_ct);
    return cb.add(input, updates_ct);
}

fn residualWindowAttention(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: CT,
    batch: usize,
    height: usize,
    width: usize,
    dim: usize,
    num_heads: usize,
    window_size: usize,
    stage: usize,
    layer: usize,
) !CT {
    const norm_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.spatial_block.window_attn.norm.weight", .{ stage, layer });
    defer allocator.free(norm_w_name);
    const norm_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.spatial_block.window_attn.norm.bias", .{ stage, layer });
    defer allocator.free(norm_b_name);
    const qkv_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.spatial_block.window_attn.fn.qkv.weight", .{ stage, layer });
    defer allocator.free(qkv_w_name);
    const qkv_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.spatial_block.window_attn.fn.qkv.bias", .{ stage, layer });
    defer allocator.free(qkv_b_name);
    const proj_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.spatial_block.window_attn.fn.proj.weight", .{ stage, layer });
    defer allocator.free(proj_w_name);
    const proj_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.spatial_block.window_attn.fn.proj.bias", .{ stage, layer });
    defer allocator.free(proj_b_name);

    const out_ct = try cb.windowedSelfAttention(
        input,
        try cb.getWeight(norm_w_name),
        try cb.getWeight(norm_b_name),
        try cb.getWeight(qkv_w_name),
        try cb.getWeight(qkv_b_name),
        try cb.getWeight(proj_w_name),
        try cb.getWeight(proj_b_name),
        batch,
        height,
        width,
        dim,
        num_heads,
        window_size,
    );
    defer cb.free(out_ct);
    return cb.add(input, out_ct);
}

fn residualChannelAttention(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: CT,
    batch: usize,
    seq_len: usize,
    dim: usize,
    groups: usize,
    stage: usize,
    layer: usize,
) !CT {
    const norm_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.channel_block.channel_attn.norm.weight", .{ stage, layer });
    defer allocator.free(norm_w_name);
    const norm_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.channel_block.channel_attn.norm.bias", .{ stage, layer });
    defer allocator.free(norm_b_name);
    const qkv_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.channel_block.channel_attn.fn.qkv.weight", .{ stage, layer });
    defer allocator.free(qkv_w_name);
    const qkv_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.channel_block.channel_attn.fn.qkv.bias", .{ stage, layer });
    defer allocator.free(qkv_b_name);
    const proj_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.channel_block.channel_attn.fn.proj.weight", .{ stage, layer });
    defer allocator.free(proj_w_name);
    const proj_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.channel_block.channel_attn.fn.proj.bias", .{ stage, layer });
    defer allocator.free(proj_b_name);

    const out_ct = try cb.channelSelfAttention(
        input,
        try cb.getWeight(norm_w_name),
        try cb.getWeight(norm_b_name),
        try cb.getWeight(qkv_w_name),
        try cb.getWeight(qkv_b_name),
        try cb.getWeight(proj_w_name),
        try cb.getWeight(proj_b_name),
        batch,
        seq_len,
        dim,
        groups,
    );
    defer cb.free(out_ct);
    return cb.add(input, out_ct);
}

fn residualMlp(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: CT,
    batch: usize,
    seq_len: usize,
    dim: usize,
    hidden_dim: usize,
    stage: usize,
    layer: usize,
    block_prefix: []const u8,
) !CT {
    const norm_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.norm.weight", .{ stage, layer, block_prefix });
    defer allocator.free(norm_w_name);
    const norm_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.norm.bias", .{ stage, layer, block_prefix });
    defer allocator.free(norm_b_name);
    const fc1_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.fn.net.fc1.weight", .{ stage, layer, block_prefix });
    defer allocator.free(fc1_w_name);
    const fc1_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.fn.net.fc1.bias", .{ stage, layer, block_prefix });
    defer allocator.free(fc1_b_name);
    const fc2_w_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.fn.net.fc2.weight", .{ stage, layer, block_prefix });
    defer allocator.free(fc2_w_name);
    const fc2_b_name = try fmtAlloc(allocator, "vision_tower.blocks.{d}.{d}.{s}.fn.net.fc2.bias", .{ stage, layer, block_prefix });
    defer allocator.free(fc2_b_name);

    if (cb.kind() == .cuda) {
        if (try @import("../ops/cuda/cuda_compute.zig").CudaCompute.florenceMlp(cb, input, .{
            try cb.getWeight(norm_w_name), try cb.getWeight(norm_b_name),
            try cb.getWeight(fc1_w_name),  try cb.getWeight(fc1_b_name),
            try cb.getWeight(fc2_w_name),  try cb.getWeight(fc2_b_name),
        }, batch * seq_len, dim, hidden_dim)) |result| return result;
    }

    const rows = batch * seq_len;
    const normed_ct = try cb.layerNorm(input, try cb.getWeight(norm_w_name), try cb.getWeight(norm_b_name), dim, 1e-5);
    defer cb.free(normed_ct);

    const fc1_w = try cb.getWeight(fc1_w_name);
    const fc1_b = try cb.getWeight(fc1_b_name);
    const fc1_ct = try cb.linear(normed_ct, fc1_w, fc1_b, rows, dim, hidden_dim);
    defer cb.free(fc1_ct);
    const activated_ct = try florenceExactGelu(cb, fc1_ct);
    defer cb.free(activated_ct);

    const fc2_w = try cb.getWeight(fc2_w_name);
    const fc2_b = try cb.getWeight(fc2_b_name);
    if (try cb.linearAdd(activated_ct, fc2_w, fc2_b, input, rows, hidden_dim, dim)) |fused| {
        return fused;
    }

    const fc2_ct = try cb.linear(activated_ct, fc2_w, fc2_b, rows, hidden_dim, dim);
    defer cb.free(fc2_ct);
    return cb.add(input, fc2_ct);
}

const PaddedWindows = struct {
    data: []f32,
    padded_h: usize,
    padded_w: usize,
    window_count: usize,
    window_area: usize,
};

fn unpadWindowTokens(
    allocator: std.mem.Allocator,
    window_tokens: []const f32,
    padded: PaddedWindows,
    batch: usize,
    height: usize,
    width: usize,
    dim: usize,
) ![]f32 {
    const window_size = std.math.sqrt(padded.window_area);
    const windows_h = padded.padded_h / window_size;
    const windows_w = padded.padded_w / window_size;
    const result = try allocator.alloc(f32, batch * height * width * dim);
    for (0..batch) |b| {
        for (0..windows_h) |wh| {
            for (0..windows_w) |ww| {
                const window_index = ((b * windows_h) + wh) * windows_w + ww;
                for (0..window_size) |dy| {
                    for (0..window_size) |dx| {
                        const dst_y = wh * window_size + dy;
                        const dst_x = ww * window_size + dx;
                        if (dst_y >= height or dst_x >= width) continue;
                        const src_token = (window_index * padded.window_area + dy * window_size + dx) * dim;
                        const dst_token = (b * height * width + dst_y * width + dst_x) * dim;
                        @memcpy(result[dst_token..][0..dim], window_tokens[src_token..][0..dim]);
                    }
                }
            }
        }
    }
    return result;
}

fn add2dPositionalEmbedding(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    tokens: []f32,
    batch: usize,
    token_count: usize,
    dim: usize,
    height: usize,
    width: usize,
) !void {
    const row_w = try cb.getWeight("image_pos_embed.row_embeddings.weight");
    const col_w = try cb.getWeight("image_pos_embed.column_embeddings.weight");
    const row = try cb.toFloat32(row_w, allocator);
    defer allocator.free(row);
    const col = try cb.toFloat32(col_w, allocator);
    defer allocator.free(col);

    const row_dim = dim / 2;
    const col_dim = dim - row_dim;
    for (0..batch) |b| {
        for (0..height) |y| {
            for (0..width) |x| {
                const token = (b * token_count + y * width + x) * dim;
                for (0..col_dim) |i| tokens[token + i] += col[x * col_dim + i];
                for (0..row_dim) |i| tokens[token + col_dim + i] += row[y * row_dim + i];
            }
        }
    }
}

fn addTemporalEmbedding(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    tokens: []f32,
    batch: usize,
    token_count: usize,
    dim: usize,
) !void {
    const temporal_w = tryOptionalWeight(cb, "visual_temporal_embed.pos_idx_to_embed") catch null;
    const weight = temporal_w orelse return;
    const temporal = try cb.toFloat32(weight, allocator);
    defer allocator.free(temporal);
    for (0..batch) |b| {
        for (0..token_count) |t| {
            const dst = (b * token_count + t) * dim;
            for (0..dim) |i| tokens[dst + i] += temporal[i];
        }
    }
}

const SourcedFeatures = struct {
    data: []f32,
    seq_len: usize,
};

fn imageFeatureSourceConcat(
    allocator: std.mem.Allocator,
    config: Config,
    tokens: []const f32,
    batch: usize,
    token_count: usize,
    dim: usize,
) !SourcedFeatures {
    var seq_len: usize = 0;
    for (0..config.image_feature_source_count) |idx| {
        seq_len += switch (config.image_feature_sources[idx]) {
            .spatial_avg_pool => 1,
            .temporal_avg_pool, .last_frame => token_count,
        };
    }

    const out = try allocator.alloc(f32, batch * seq_len * dim);
    var cursor: usize = 0;
    for (0..config.image_feature_source_count) |idx| {
        switch (config.image_feature_sources[idx]) {
            .spatial_avg_pool => {
                for (0..batch) |b| {
                    const dst = (b * seq_len + cursor) * dim;
                    @memset(out[dst..][0..dim], 0.0);
                    for (0..token_count) |t| {
                        const src = (b * token_count + t) * dim;
                        for (0..dim) |i| out[dst + i] += tokens[src + i];
                    }
                    const scale = 1.0 / @as(f32, @floatFromInt(token_count));
                    for (0..dim) |i| out[dst + i] *= scale;
                }
                cursor += 1;
            },
            .temporal_avg_pool, .last_frame => {
                for (0..batch) |b| {
                    const dst = (b * seq_len + cursor) * dim;
                    const src = b * token_count * dim;
                    @memcpy(out[dst..][0 .. token_count * dim], tokens[src..][0 .. token_count * dim]);
                }
                cursor += token_count;
            },
        }
    }
    return .{ .data = out, .seq_len = seq_len };
}

fn applyEncoderEmbeddings(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    merged: []const f32,
    config: Config,
    batch: usize,
    seq_len: usize,
) !CT {
    const d_model: usize = config.d_model;
    const total = batch * seq_len;
    const shape = [_]i32{ @intCast(total), @intCast(d_model) };
    var hidden = try cb.fromFloat32Shape(merged, &shape);

    const pos_ids = try allocator.alloc(i64, total);
    defer allocator.free(pos_ids);
    for (0..batch) |b| {
        for (0..seq_len) |s| pos_ids[b * seq_len + s] = @intCast(s + bart_position_offset);
    }

    const pos_emb = try cb.embeddingLookup(try cb.getWeight("language_model.model.encoder.embed_positions.weight"), pos_ids, total, d_model);
    defer cb.free(pos_emb);
    const with_pos = try cb.add(hidden, pos_emb);
    cb.free(hidden);
    hidden = with_pos;

    const ln_w = try cb.getWeight("language_model.model.encoder.layernorm_embedding.weight");
    const ln_b = try cb.getWeight("language_model.model.encoder.layernorm_embedding.bias");
    const normed = try cb.layerNorm(hidden, ln_w, ln_b, d_model, 1e-5);
    cb.free(hidden);
    return normed;
}

fn applyEncoderImagePromptEmbeddingsTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    image: VisionForwardTensorResult,
    config: Config,
    batch: usize,
    prompt_input_ids: []const i64,
    prompt_seq_len: usize,
) !CT {
    if (batch == 0 or batch > 64) return error.InvalidInputShape;
    if (batch != 1 and cb.kind() != .cuda) return error.UnsupportedShape;
    const d_model: usize = config.d_model;
    const total_seq = image.seq_len + prompt_seq_len;

    const merged = if (prompt_seq_len > 0) blk: {
        const prompt_embeddings = try cb.embeddingLookup(try promptEmbedWeight(cb), prompt_input_ids, batch * prompt_seq_len, d_model);
        defer cb.free(prompt_embeddings);
        if (batch > 1) break :blk try @import("../ops/cuda/cuda_compute.zig").CudaCompute.concatFlorenceImagePrompt(cb, image.features, prompt_embeddings, batch, image.seq_len, prompt_seq_len, d_model);
        break :blk try cb.concatRows2D(allocator, image.features, prompt_embeddings, image.seq_len, prompt_seq_len, d_model);
    } else try cb.sliceRows2D(allocator, image.features, 0, batch * image.seq_len, d_model);

    return applyEncoderEmbeddingsTensor(cb, allocator, merged, config, batch, total_seq);
}

fn applyEncoderEmbeddingsTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input_hidden: CT,
    config: Config,
    batch: usize,
    seq_len: usize,
) !CT {
    const d_model: usize = config.d_model;
    const total = batch * seq_len;
    var hidden = input_hidden;
    var hidden_owned = true;
    errdefer if (hidden_owned) cb.free(hidden);

    const pos_ids = try allocator.alloc(i64, total);
    defer allocator.free(pos_ids);
    for (0..batch) |b| {
        for (0..seq_len) |s| pos_ids[b * seq_len + s] = @intCast(s + bart_position_offset);
    }

    const pos_emb = try cb.embeddingLookup(try cb.getWeight("language_model.model.encoder.embed_positions.weight"), pos_ids, total, d_model);
    defer cb.free(pos_emb);
    const with_pos = try cb.add(hidden, pos_emb);
    cb.free(hidden);
    hidden = with_pos;

    const ln_w = try cb.getWeight("language_model.model.encoder.layernorm_embedding.weight");
    const ln_b = try cb.getWeight("language_model.model.encoder.layernorm_embedding.bias");
    const normed = try cb.layerNorm(hidden, ln_w, ln_b, d_model, 1e-5);
    cb.free(hidden);
    hidden_owned = false;
    return normed;
}

fn applyDecoderEmbeddings(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    batch: usize,
    seq_len: usize,
) !CT {
    return applyDecoderEmbeddingsAt(cb, allocator, config, input_ids, batch, seq_len, 0);
}

fn applyDecoderEmbeddingsAt(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    batch: usize,
    seq_len: usize,
    position_start: usize,
) !CT {
    return applyDecoderEmbeddingsAtAssigned(cb, allocator, config, input_ids, batch, seq_len, position_start, null, null);
}

fn applyDecoderEmbeddingsAtAssigned(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    batch: usize,
    seq_len: usize,
    position_start: usize,
    assigned: ?*@import("../execution/dense_decoder_schedule.zig").Schedule,
    held: ?*const PreparedSingleToken.Weights,
) !CT {
    const d_model: usize = config.d_model;
    const total = batch * seq_len;
    const embed_w = if (held) |weights| weights.embed else try promptEmbedWeight(cb);
    var hidden = try stepEmbedding(cb, assigned, embed_w, input_ids, total, d_model);
    // Position metadata can fail independently of backend tensor allocation.
    // Keep the current residual owned until final normalization succeeds.
    errdefer stepRelease(cb, assigned, hidden);

    var position_cell: [1]i64 = undefined;
    const pos_ids = if (assigned != null) position_cell[0..] else try allocator.alloc(i64, total);
    defer if (assigned == null) allocator.free(pos_ids);
    if (assigned != null and total != 1) return error.InvalidInputShape;
    for (0..batch) |b| {
        for (0..seq_len) |s| pos_ids[b * seq_len + s] = @intCast(position_start + s + bart_position_offset);
    }

    const pos_emb = try stepEmbedding(cb, assigned, if (held) |weights| weights.positions else try decoderPosWeight(cb), pos_ids, total, d_model);
    defer stepRelease(cb, assigned, pos_emb);
    const with_pos = try stepAdd(cb, assigned, hidden, pos_emb);
    stepRelease(cb, assigned, hidden);
    hidden = with_pos;

    const ln_w = if (held) |weights| weights.embed_norm.w else try decoderLayerNormEmbeddingWeight(cb);
    const ln_b = if (held) |weights| weights.embed_norm.b else try decoderLayerNormEmbeddingBias(cb);
    const normed = try stepNorm(cb, assigned, hidden, ln_w, ln_b, d_model);
    stepRelease(cb, assigned, hidden);
    return normed;
}

fn encoderBlock(
    cb: *const ComputeBackend,
    config: Config,
    hidden: CT,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
    layer: usize,
    buf: *[256]u8,
) !CT {
    const owns_encoder_scope = try MetalCompute.beginOcrEncoderScope(cb, .florence);
    defer if (owns_encoder_scope) cb.decoderRuntimeEndPlannedComputeScope();
    const d_model = config.d_model;
    const num_heads = config.encoder_attention_heads;
    const head_dim = config.encoderHeadDim();
    const ffn_dim = config.encoder_ffn_dim;
    const total = batch * seq_len;

    const profile = readProfileEnabled();

    const qkv_start = nowNs();
    const q_weights = try encoderLinearWeights(cb, layer, "self_attn.q_proj", buf);
    const k_weights = try encoderLinearWeights(cb, layer, "self_attn.k_proj", buf);
    const v_weights = try encoderLinearWeights(cb, layer, "self_attn.v_proj", buf);
    const qkv = try cb.linearTriple(hidden, q_weights.weight, q_weights.bias, k_weights.weight, k_weights.bias, v_weights.weight, v_weights.bias, total, d_model, d_model);
    defer cb.free(qkv.first);
    defer cb.free(qkv.second);
    defer cb.free(qkv.third);
    if (profile) logFlorenceProfileStep("enc_qkv_linear", layer, qkv_start);

    const attn_start = nowNs();
    const attn = try cb.scaledDotProductAttention(qkv.first, qkv.second, qkv.third, attention_mask, null, batch, seq_len, num_heads, head_dim);
    defer cb.free(attn);
    if (profile) logFlorenceProfileStep("enc_sdpa", layer, attn_start);
    const proj_start = nowNs();
    const proj = try encoderLinearProj(cb, attn, layer, "self_attn.out_proj", total, d_model, d_model, buf);
    defer cb.free(proj);
    const attn_res = try cb.add(hidden, proj);
    if (profile) logFlorenceProfileStep("enc_out_proj_add", layer, proj_start);

    const ln0_start = nowNs();
    const ln0_w = try encoderLayerWeight(cb, layer, "self_attn_layer_norm.weight", buf);
    const ln0_b = try encoderLayerWeight(cb, layer, "self_attn_layer_norm.bias", buf);
    const attn_normed = try cb.layerNorm(attn_res, ln0_w, ln0_b, d_model, 1e-5);
    cb.free(attn_res);
    if (profile) logFlorenceProfileStep("enc_ln0", layer, ln0_start);

    const fc1_start = nowNs();
    const fc1_weights = try encoderLinearWeights(cb, layer, "fc1", buf);
    if (comptime build_options.enable_cuda) {
        if (cb.kind() == .cuda and platform.env.getenvBool("ANTFLY_CUDA_ENABLE_FLORENCE_BART_FFN")) {
            errdefer cb.free(attn_normed);
            const second = try encoderLinearWeights(cb, layer, "fc2", buf);
            const nw = try encoderLayerWeight(cb, layer, "final_layer_norm.weight", buf);
            const nb = try encoderLayerWeight(cb, layer, "final_layer_norm.bias", buf);
            if (try @import("../ops/cuda/cuda_compute.zig").CudaCompute.florenceBartFfn(cb, attn_normed, .{ fc1_weights.weight, fc1_weights.bias, second.weight, second.bias, nw, nb }, total, d_model, ffn_dim)) |fused| {
                errdefer cb.free(fused);
                if (profile) logFlorenceProfileStep("enc_fused_ffn", layer, fc1_start);
                cb.free(attn_normed);
                return fused;
            }
        }
    }
    const fc1 = try cb.linear(attn_normed, fc1_weights.weight, fc1_weights.bias, total, d_model, ffn_dim);
    defer cb.free(fc1);
    const activated = try florenceExactGelu(cb, fc1);
    defer cb.free(activated);
    if (profile) logFlorenceProfileStep("enc_fc1_gelu", layer, fc1_start);

    const fc2_start = nowNs();
    const fc2_weights = try encoderLinearWeights(cb, layer, "fc2", buf);
    const ffn_res = if (try cb.linearAdd(activated, fc2_weights.weight, fc2_weights.bias, attn_normed, total, ffn_dim, d_model)) |fused|
        fused
    else blk: {
        const fc2 = try cb.linear(activated, fc2_weights.weight, fc2_weights.bias, total, ffn_dim, d_model);
        defer cb.free(fc2);
        break :blk try cb.add(attn_normed, fc2);
    };
    cb.free(attn_normed);
    if (profile) logFlorenceProfileStep("enc_fc2_add", layer, fc2_start);

    const ln1_w = try encoderLayerWeight(cb, layer, "final_layer_norm.weight", buf);
    const ln1_b = try encoderLayerWeight(cb, layer, "final_layer_norm.bias", buf);
    const result = try cb.layerNorm(ffn_res, ln1_w, ln1_b, d_model, 1e-5);
    cb.free(ffn_res);
    return result;
}

fn decoderBlockSelfOnly(
    cb: *const ComputeBackend,
    config: Config,
    hidden: CT,
    batch: usize,
    seq_len: usize,
    layer: usize,
    buf: *[256]u8,
) !CT {
    const d_model = config.d_model;
    const num_heads = config.decoder_attention_heads;
    const head_dim = config.decoderHeadDim();
    const ffn_dim = config.decoder_ffn_dim;
    const total = batch * seq_len;

    const q_weights = try decoderLinearWeights(cb, layer, "self_attn.q_proj", buf);
    const k_weights = try decoderLinearWeights(cb, layer, "self_attn.k_proj", buf);
    const v_weights = try decoderLinearWeights(cb, layer, "self_attn.v_proj", buf);
    const qkv = try cb.linearTriple(hidden, q_weights.weight, q_weights.bias, k_weights.weight, k_weights.bias, v_weights.weight, v_weights.bias, total, d_model, d_model);
    defer cb.free(qkv.first);
    defer cb.free(qkv.second);
    defer cb.free(qkv.third);
    const attn = try cb.causalSelfAttention(qkv.first, qkv.second, qkv.third, null, batch, seq_len, num_heads, head_dim);
    defer cb.free(attn);
    const proj = try decoderLinearProj(cb, attn, layer, "self_attn.out_proj", total, d_model, d_model, buf);
    defer cb.free(proj);
    const attn_res = try cb.add(hidden, proj);

    const ln0_w = try decoderLayerWeight(cb, layer, "self_attn_layer_norm.weight", buf);
    const ln0_b = try decoderLayerWeight(cb, layer, "self_attn_layer_norm.bias", buf);
    const attn_normed = try cb.layerNorm(attn_res, ln0_w, ln0_b, d_model, 1e-5);
    cb.free(attn_res);

    const fc1_weights = try decoderLinearWeights(cb, layer, "fc1", buf);
    if (comptime build_options.enable_cuda) {
        if (cb.kind() == .cuda and platform.env.getenvBool("ANTFLY_CUDA_ENABLE_FLORENCE_BART_FFN")) {
            errdefer cb.free(attn_normed);
            const second = try decoderLinearWeights(cb, layer, "fc2", buf);
            const nw = try decoderLayerWeight(cb, layer, "final_layer_norm.weight", buf);
            const nb = try decoderLayerWeight(cb, layer, "final_layer_norm.bias", buf);
            if (try @import("../ops/cuda/cuda_compute.zig").CudaCompute.florenceBartFfn(cb, attn_normed, .{ fc1_weights.weight, fc1_weights.bias, second.weight, second.bias, nw, nb }, total, d_model, ffn_dim)) |fused| {
                errdefer cb.free(fused);
                cb.free(attn_normed);
                return fused;
            }
        }
    }
    const fc1 = try cb.linear(attn_normed, fc1_weights.weight, fc1_weights.bias, total, d_model, ffn_dim);
    defer cb.free(fc1);
    const activated = try florenceExactGelu(cb, fc1);
    defer cb.free(activated);

    const fc2_weights = try decoderLinearWeights(cb, layer, "fc2", buf);
    const ffn_res = if (try cb.linearAdd(activated, fc2_weights.weight, fc2_weights.bias, attn_normed, total, ffn_dim, d_model)) |fused|
        fused
    else blk: {
        const fc2 = try cb.linear(activated, fc2_weights.weight, fc2_weights.bias, total, ffn_dim, d_model);
        defer cb.free(fc2);
        break :blk try cb.add(attn_normed, fc2);
    };
    cb.free(attn_normed);

    const ln1_w = try decoderLayerWeight(cb, layer, "final_layer_norm.weight", buf);
    const ln1_b = try decoderLayerWeight(cb, layer, "final_layer_norm.bias", buf);
    const result = try cb.layerNorm(ffn_res, ln1_w, ln1_b, d_model, 1e-5);
    cb.free(ffn_res);
    return result;
}

fn decoderBlock(
    cb: *const ComputeBackend,
    config: Config,
    hidden: CT,
    encoder_hidden: CT,
    encoder_mask: []const i64,
    batch: usize,
    dec_seq: usize,
    enc_seq: usize,
    layer: usize,
    buf: *[256]u8,
) !CT {
    return decoderBlockWithOptionalCrossCache(cb, config, hidden, encoder_hidden, null, null, encoder_mask, batch, dec_seq, enc_seq, layer, buf);
}

fn decoderBlockWithCrossCache(
    cb: *const ComputeBackend,
    config: Config,
    hidden: CT,
    cached_k_cross: CT,
    cached_v_cross: CT,
    encoder_mask: []const i64,
    batch: usize,
    dec_seq: usize,
    enc_seq: usize,
    layer: usize,
    buf: *[256]u8,
) !CT {
    return decoderBlockWithOptionalCrossCache(cb, config, hidden, null, cached_k_cross, cached_v_cross, encoder_mask, batch, dec_seq, enc_seq, layer, buf);
}

fn decoderBlockWithOptionalCrossCache(
    cb: *const ComputeBackend,
    config: Config,
    hidden: CT,
    encoder_hidden: ?CT,
    cached_k_cross: ?CT,
    cached_v_cross: ?CT,
    encoder_mask: []const i64,
    batch: usize,
    dec_seq: usize,
    enc_seq: usize,
    layer: usize,
    buf: *[256]u8,
) !CT {
    const d_model = config.d_model;
    const num_heads = config.decoder_attention_heads;
    const head_dim = config.decoderHeadDim();
    const ffn_dim = config.decoder_ffn_dim;
    const dec_total = batch * dec_seq;
    const enc_total = batch * enc_seq;

    const q_self_weights = try decoderLinearWeights(cb, layer, "self_attn.q_proj", buf);
    const k_self_weights = try decoderLinearWeights(cb, layer, "self_attn.k_proj", buf);
    const v_self_weights = try decoderLinearWeights(cb, layer, "self_attn.v_proj", buf);
    const qkv_self = try cb.linearTriple(hidden, q_self_weights.weight, q_self_weights.bias, k_self_weights.weight, k_self_weights.bias, v_self_weights.weight, v_self_weights.bias, dec_total, d_model, d_model);
    defer cb.free(qkv_self.first);
    defer cb.free(qkv_self.second);
    defer cb.free(qkv_self.third);
    const self_attn = try cb.causalSelfAttention(qkv_self.first, qkv_self.second, qkv_self.third, null, batch, dec_seq, num_heads, head_dim);
    defer cb.free(self_attn);
    const self_proj = try decoderLinearProj(cb, self_attn, layer, "self_attn.out_proj", dec_total, d_model, d_model, buf);
    defer cb.free(self_proj);
    const self_res = try cb.add(hidden, self_proj);

    const ln0_w = try decoderLayerWeight(cb, layer, "self_attn_layer_norm.weight", buf);
    const ln0_b = try decoderLayerWeight(cb, layer, "self_attn_layer_norm.bias", buf);
    const self_normed = try cb.layerNorm(self_res, ln0_w, ln0_b, d_model, 1e-5);
    cb.free(self_res);

    if ((cached_k_cross == null) != (cached_v_cross == null)) return error.InvalidInputShape;
    const q_cross = try decoderLinearProj(cb, self_normed, layer, "encoder_attn.q_proj", dec_total, d_model, d_model, buf);
    defer cb.free(q_cross);
    const owns_cross_k = cached_k_cross == null;
    const owns_cross_v = cached_v_cross == null;
    const k_cross = if (cached_k_cross) |cached| cached else blk: {
        const source = encoder_hidden orelse return error.MissingInputs;
        break :blk try decoderLinearProj(cb, source, layer, "encoder_attn.k_proj", enc_total, d_model, d_model, buf);
    };
    defer if (owns_cross_k) cb.free(k_cross);
    const v_cross = if (cached_v_cross) |cached| cached else blk: {
        const source = encoder_hidden orelse return error.MissingInputs;
        break :blk try decoderLinearProj(cb, source, layer, "encoder_attn.v_proj", enc_total, d_model, d_model, buf);
    };
    defer if (owns_cross_v) cb.free(v_cross);
    const cross_attn = try cb.crossAttention(q_cross, k_cross, v_cross, encoder_mask, batch, dec_seq, enc_seq, num_heads, head_dim);
    defer cb.free(cross_attn);
    const cross_proj = try decoderLinearProj(cb, cross_attn, layer, "encoder_attn.out_proj", dec_total, d_model, d_model, buf);
    defer cb.free(cross_proj);
    const cross_res = try cb.add(self_normed, cross_proj);
    cb.free(self_normed);

    const ln1_w = try decoderLayerWeight(cb, layer, "encoder_attn_layer_norm.weight", buf);
    const ln1_b = try decoderLayerWeight(cb, layer, "encoder_attn_layer_norm.bias", buf);
    const cross_normed = try cb.layerNorm(cross_res, ln1_w, ln1_b, d_model, 1e-5);
    cb.free(cross_res);

    const fc1_weights = try decoderLinearWeights(cb, layer, "fc1", buf);
    if (comptime build_options.enable_cuda) {
        if (cb.kind() == .cuda and platform.env.getenvBool("ANTFLY_CUDA_ENABLE_FLORENCE_BART_FFN")) {
            errdefer cb.free(cross_normed);
            const second = try decoderLinearWeights(cb, layer, "fc2", buf);
            const nw = try decoderLayerWeight(cb, layer, "final_layer_norm.weight", buf);
            const nb = try decoderLayerWeight(cb, layer, "final_layer_norm.bias", buf);
            if (try @import("../ops/cuda/cuda_compute.zig").CudaCompute.florenceBartFfn(cb, cross_normed, .{ fc1_weights.weight, fc1_weights.bias, second.weight, second.bias, nw, nb }, dec_total, d_model, ffn_dim)) |fused| {
                errdefer cb.free(fused);
                cb.free(cross_normed);
                return fused;
            }
        }
    }
    const fc1 = try cb.linear(cross_normed, fc1_weights.weight, fc1_weights.bias, dec_total, d_model, ffn_dim);
    defer cb.free(fc1);
    const activated = try florenceExactGelu(cb, fc1);
    defer cb.free(activated);

    const fc2_weights = try decoderLinearWeights(cb, layer, "fc2", buf);
    const ffn_res = if (try cb.linearAdd(activated, fc2_weights.weight, fc2_weights.bias, cross_normed, dec_total, ffn_dim, d_model)) |fused|
        fused
    else blk: {
        const fc2 = try cb.linear(activated, fc2_weights.weight, fc2_weights.bias, dec_total, ffn_dim, d_model);
        defer cb.free(fc2);
        break :blk try cb.add(cross_normed, fc2);
    };
    cb.free(cross_normed);
    const ln2_w = try decoderLayerWeight(cb, layer, "final_layer_norm.weight", buf);
    const ln2_b = try decoderLayerWeight(cb, layer, "final_layer_norm.bias", buf);
    const result = try cb.layerNorm(ffn_res, ln2_w, ln2_b, d_model, 1e-5);
    cb.free(ffn_res);
    return result;
}

/// The real prepared request holds/stamps these CTs before numerical entry.
/// This lexical adapter borrows them only through the original compound layer;
/// ordinary callers retain all historical lookup/alias/error/cache behavior.
fn decoderStepLinearWeights(cb: *const ComputeBackend, layer: usize, comptime role: []const u8, buf: *[256]u8, held: ?*const PreparedSingleToken.Layer, comptime field: []const u8) !LinearWeights {
    if (held) |weights| {
        const pair = @field(weights, field);
        return .{ .weight = pair.w, .bias = pair.b };
    }
    return decoderLinearWeights(cb, layer, role, buf);
}
fn decoderStepNormWeights(cb: *const ComputeBackend, layer: usize, comptime role: []const u8, buf: *[256]u8, held: ?*const PreparedSingleToken.Layer, comptime field: []const u8) !PreparedSingleToken.Pair {
    if (held) |weights| return @field(weights, field);
    // Same ordered old lookups; the original caller's error is preserved.
    const w = try decoderLayerWeight(cb, layer, role ++ ".weight", buf);
    const b = try decoderLayerWeight(cb, layer, role ++ ".bias", buf);
    return .{ .w = w, .b = b };
}

fn decoderBlockIncrementalCached(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
    self_mask: []const i64,
    batch: usize,
    old_len: usize,
    new_len: usize,
    layer: usize,
    cache: *DecoderIncrementalCache,
    buf: *[256]u8,
    held: ?*const PreparedSingleToken.Layer,
) !CT {
    const expected_mask_len = std.math.mul(usize, batch, new_len) catch return error.InvalidInputShape;
    if (new_len != old_len + 1 or self_mask.len != expected_mask_len) return error.InvalidInputShape;

    const assigned = cache.assigned;
    const d_model = config.d_model;
    const num_heads = config.decoder_attention_heads;
    const head_dim = config.decoderHeadDim();
    const ffn_dim = config.decoder_ffn_dim;
    var profile_stage_start = if (florenceDecoderStageFrameProfilingEnabled(cb)) nowNs() else 0;

    const q_self_weights = try decoderStepLinearWeights(cb, layer, "self_attn.q_proj", buf, held, "q");
    const k_self_weights = try decoderStepLinearWeights(cb, layer, "self_attn.k_proj", buf, held, "k");
    const v_self_weights = try decoderStepLinearWeights(cb, layer, "self_attn.v_proj", buf, held, "v");
    const qkv_self = try stepTriple(cb, assigned, hidden, q_self_weights, k_self_weights, v_self_weights, batch, d_model);
    defer stepRelease(cb, assigned, qkv_self.first);

    const cached_pair = if (assigned != null)
        try appendPreparedSelfPair(cb, allocator, &cache.self, layer, qkv_self.second, qkv_self.third, batch, old_len, new_len, d_model)
    else
        try appendSelfCachePair(cb, allocator, &cache.self, &cache.self.keys[layer], &cache.self.values[layer], qkv_self.second, qkv_self.third, batch, old_len, new_len, d_model);
    const cached_k_ref = cached_pair[0];
    defer cached_k_ref.deinit(cb);
    const cached_v_ref = cached_pair[1];
    defer cached_v_ref.deinit(cb);

    const self_attn = try stepPrefix(cb, assigned, qkv_self.first, cached_k_ref.tensor, cached_v_ref.tensor, self_mask, batch, new_len, num_heads, head_dim);
    defer stepRelease(cb, assigned, self_attn);
    const self_proj = block: {
        const weights = try decoderStepLinearWeights(cb, layer, "self_attn.out_proj", buf, held, "o");
        break :block try stepLinear(cb, assigned, self_attn, weights.weight, weights.bias, batch, d_model, d_model);
    };
    defer stepRelease(cb, assigned, self_proj);
    const self_res = try stepAdd(cb, assigned, hidden, self_proj);
    var self_res_live = true;
    errdefer if (self_res_live) stepRelease(cb, assigned, self_res);

    const ln0 = try decoderStepNormWeights(cb, layer, "self_attn_layer_norm", buf, held, "self_norm");
    const ln0_w = ln0.w;
    const ln0_b = ln0.b;
    const self_normed = try stepNorm(cb, assigned, self_res, ln0_w, ln0_b, d_model);
    var self_normed_live = true;
    errdefer if (self_normed_live) stepRelease(cb, assigned, self_normed);
    stepRelease(cb, assigned, self_res);
    self_res_live = false;
    profile_stage_start = try profileFlorenceDecoderStageFrameBoundary(cb, "decoder_self", layer, profile_stage_start);

    const q_cross = block: {
        const weights = try decoderStepLinearWeights(cb, layer, "encoder_attn.q_proj", buf, held, "cross_q");
        break :block try stepLinear(cb, assigned, self_normed, weights.weight, weights.bias, batch, d_model, d_model);
    };
    defer stepRelease(cb, assigned, q_cross);
    const cross = cache.attentionCross();
    const cross_attn = try stepPrefix(cb, assigned, q_cross, cross.keys[layer], cross.values[layer], cache.encoder_mask, batch, cache.enc_seq, num_heads, head_dim);
    defer stepRelease(cb, assigned, cross_attn);
    const cross_proj = block: {
        const weights = try decoderStepLinearWeights(cb, layer, "encoder_attn.out_proj", buf, held, "cross_o");
        break :block try stepLinear(cb, assigned, cross_attn, weights.weight, weights.bias, batch, d_model, d_model);
    };
    defer stepRelease(cb, assigned, cross_proj);
    const cross_res = try stepAdd(cb, assigned, self_normed, cross_proj);
    var cross_res_live = true;
    errdefer if (cross_res_live) stepRelease(cb, assigned, cross_res);
    stepRelease(cb, assigned, self_normed);
    self_normed_live = false;

    const ln1 = try decoderStepNormWeights(cb, layer, "encoder_attn_layer_norm", buf, held, "cross_norm");
    const ln1_w = ln1.w;
    const ln1_b = ln1.b;
    const cross_normed = try stepNorm(cb, assigned, cross_res, ln1_w, ln1_b, d_model);
    var cross_normed_live = true;
    errdefer if (cross_normed_live) stepRelease(cb, assigned, cross_normed);
    stepRelease(cb, assigned, cross_res);
    cross_res_live = false;
    profile_stage_start = try profileFlorenceDecoderStageFrameBoundary(cb, "decoder_cross", layer, profile_stage_start);

    const fc1_weights = try decoderStepLinearWeights(cb, layer, "fc1", buf, held, "fc1");
    if (comptime build_options.enable_cuda) {
        if (cb.kind() == .cuda and platform.env.getenvBool("ANTFLY_CUDA_ENABLE_FLORENCE_BART_FFN")) {
            const second = try decoderStepLinearWeights(cb, layer, "fc2", buf, held, "fc2");
            const nw = try decoderLayerWeight(cb, layer, "final_layer_norm.weight", buf);
            const nb = try decoderLayerWeight(cb, layer, "final_layer_norm.bias", buf);
            if (try @import("../ops/cuda/cuda_compute.zig").CudaCompute.florenceBartFfn(cb, cross_normed, .{ fc1_weights.weight, fc1_weights.bias, second.weight, second.bias, nw, nb }, batch, d_model, ffn_dim)) |fused| {
                errdefer stepRelease(cb, assigned, fused);
                _ = try profileFlorenceDecoderStageFrameBoundary(cb, "decoder_ffn", layer, profile_stage_start);
                stepRelease(cb, assigned, cross_normed);
                cross_normed_live = false;
                return fused;
            }
        }
    }
    const fc1 = try stepLinear(cb, assigned, cross_normed, fc1_weights.weight, fc1_weights.bias, batch, d_model, ffn_dim);
    defer stepRelease(cb, assigned, fc1);
    const activated = if (assigned) |schedule| try schedule.execute(&.{fc1}, null) else try florenceExactGelu(cb, fc1);
    defer stepRelease(cb, assigned, activated);

    const fc2_weights = try decoderStepLinearWeights(cb, layer, "fc2", buf, held, "fc2");
    const ffn_res = if (assigned) |schedule| prepared: {
        const second = try schedule.execute(&.{ activated, fc2_weights.weight, fc2_weights.bias }, null);
        break :prepared try schedule.execute(&.{ cross_normed, second }, null);
    } else if (try cb.linearAdd(activated, fc2_weights.weight, fc2_weights.bias, cross_normed, batch, ffn_dim, d_model)) |fused|
        fused
    else blk: {
        const fc2 = try cb.linear(activated, fc2_weights.weight, fc2_weights.bias, batch, ffn_dim, d_model);
        defer stepRelease(cb, assigned, fc2);
        break :blk try cb.add(cross_normed, fc2);
    };
    var ffn_res_live = true;
    errdefer if (ffn_res_live) stepRelease(cb, assigned, ffn_res);
    stepRelease(cb, assigned, cross_normed);
    cross_normed_live = false;

    const ln2 = try decoderStepNormWeights(cb, layer, "final_layer_norm", buf, held, "final_norm");
    const ln2_w = ln2.w;
    const ln2_b = ln2.b;
    const result = try stepNorm(cb, assigned, ffn_res, ln2_w, ln2_b, d_model);
    stepRelease(cb, assigned, ffn_res);
    ffn_res_live = false;
    _ = try profileFlorenceDecoderStageFrameBoundary(cb, "decoder_ffn", layer, profile_stage_start);
    return result;
}

const CachedSelfTensor = struct {
    tensor: CT,
    owned_view: bool = false,

    pub fn deinit(self: CachedSelfTensor, cb: *const ComputeBackend) void {
        if (self.owned_view) cb.free(self.tensor);
    }
};

// Own both projected rows on every route, including a failed first append.
fn appendSelfCachePair(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    self_cache: *DecoderSelfCache,
    key_slot: *?CT,
    value_slot: *?CT,
    next_key: CT,
    next_value: CT,
    batch: usize,
    old_len: usize,
    new_len: usize,
    d_model: usize,
) ![2]CachedSelfTensor {
    var own_key = true;
    defer if (own_key) cb.free(next_key);
    var own_value = true;
    defer if (own_value) cb.free(next_value);
    if (comptime build_options.enable_cuda) {
        if (self_cache.preallocated and old_len < self_cache.capacity and new_len == old_len + 1 and key_slot.* != null and value_slot.* != null) {
            if (try @import("../ops/cuda/cuda_compute.zig").CudaCompute.florenceAppendSelfCachePair(cb, .{ key_slot.*.?, value_slot.*.? }, .{ next_key, next_value }, batch, self_cache.capacity, old_len, d_model)) |pair|
                return .{ .{ .tensor = pair[0], .owned_view = true }, .{ .tensor = pair[1], .owned_view = true } };
        }
    }
    own_key = false;
    const key = try appendSelfCacheTensor(cb, allocator, self_cache, key_slot, next_key, batch, old_len, new_len, d_model);
    errdefer key.deinit(cb);
    own_value = false;
    const value = try appendSelfCacheTensor(cb, allocator, self_cache, value_slot, next_value, batch, old_len, new_len, d_model);
    return .{ key, value };
}

fn appendSelfCacheTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    self_cache: *DecoderSelfCache,
    slot: *?CT,
    next: CT,
    batch: usize,
    old_len: usize,
    new_len: usize,
    d_model: usize,
) !CachedSelfTensor {
    if (batch == 0 or new_len != old_len + 1) {
        cb.free(next);
        return error.InvalidInputShape;
    }
    if (self_cache.preallocated) {
        if (new_len > self_cache.capacity) {
            cb.free(next);
            return error.InvalidInputShape;
        }
        const slab = slot.* orelse {
            cb.free(next);
            return error.InvalidInputShape;
        };
        var copied = true;
        for (0..batch) |b| {
            const dst_base = std.math.mul(usize, b, self_cache.capacity) catch {
                cb.free(next);
                return error.InvalidInputShape;
            };
            const dst_row = std.math.add(usize, dst_base, old_len) catch {
                cb.free(next);
                return error.InvalidInputShape;
            };
            copied = copied and (cb.copyRows2D(allocator, slab, dst_row, next, b, 1, d_model) catch |err| {
                cb.free(next);
                return err;
            });
        }
        cb.free(next);
        if (!copied) return error.UnsupportedOperation;
        const view = try compactSelfCacheRows(cb, allocator, slab, batch, self_cache.capacity, new_len, d_model);
        return .{ .tensor = view, .owned_view = true };
    } else if (slot.*) |old| {
        const merged = appendBatchSelfRows(cb, allocator, old, next, batch, old_len, d_model) catch |err| {
            cb.free(next);
            return err;
        };
        cb.free(old);
        cb.free(next);
        slot.* = merged;
        return .{ .tensor = merged };
    } else {
        if (old_len != 0) {
            cb.free(next);
            return error.InvalidInputShape;
        }
        slot.* = next;
        return .{ .tensor = next };
    }
}

fn appendBatchSelfRows(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    old: CT,
    next: CT,
    batch: usize,
    old_len: usize,
    d_model: usize,
) !CT {
    if (batch == 1) return cb.concatRows2D(allocator, old, next, old_len, 1, d_model);
    const new_len = old_len + 1;
    var acc: ?CT = null;
    var acc_rows: usize = 0;
    errdefer if (acc) |tensor| cb.free(tensor);

    for (0..batch) |b| {
        const old_start = std.math.mul(usize, b, old_len) catch return error.InvalidInputShape;
        const old_slice = try cb.sliceRows2D(allocator, old, old_start, old_len, d_model);
        defer cb.free(old_slice);
        const next_slice = try cb.sliceRows2D(allocator, next, b, 1, d_model);
        defer cb.free(next_slice);
        const item = try cb.concatRows2D(allocator, old_slice, next_slice, old_len, 1, d_model);
        var item_live = true;
        errdefer if (item_live) cb.free(item);

        if (acc) |current| {
            const merged = try cb.concatRows2D(allocator, current, item, acc_rows, new_len, d_model);
            cb.free(current);
            cb.free(item);
            item_live = false;
            acc = merged;
        } else {
            acc = item;
            item_live = false;
        }
        acc_rows += new_len;
    }

    const result = acc orelse return error.InvalidInputShape;
    acc = null;
    return result;
}

fn compactSelfCacheRows(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    slab: CT,
    batch: usize,
    capacity: usize,
    active_len: usize,
    d_model: usize,
) !CT {
    if (batch == 1) return cb.sliceRows2D(allocator, slab, 0, active_len, d_model);
    if (cb.kind() == .cuda and platform.env.getenvBool("ANTFLY_CUDA_ENABLE_FLORENCE_PACKED_SELF_KV"))
        return compactSelfCacheRowsDevice(cb, slab, batch, capacity, active_len, d_model);

    var acc: ?CT = null;
    var acc_rows: usize = 0;
    errdefer if (acc) |tensor| cb.free(tensor);

    for (0..batch) |b| {
        const start_row = std.math.mul(usize, b, capacity) catch return error.InvalidInputShape;
        const item = try cb.sliceRows2D(allocator, slab, start_row, active_len, d_model);
        var item_live = true;
        errdefer if (item_live) cb.free(item);
        if (acc) |current| {
            const merged = try cb.concatRows2D(allocator, current, item, acc_rows, active_len, d_model);
            cb.free(current);
            cb.free(item);
            item_live = false;
            acc = merged;
        } else {
            acc = item;
            item_live = false;
        }
        acc_rows += active_len;
    }

    const result = acc orelse return error.InvalidInputShape;
    acc = null;
    return result;
}

// Slice all active page prefixes in one device operation. Repeated pairwise
// concatenation otherwise copies the growing prefix once for every page.
fn compactSelfCacheRowsDevice(cb: *const ComputeBackend, slab: CT, batch: usize, capacity: usize, active_len: usize, d_model: usize) !CT {
    if (batch == 0 or batch > 64 or capacity == 0 or active_len == 0 or active_len > capacity or d_model == 0)
        return error.InvalidInputShape;
    return @import("../ops/cuda/cuda_compute.zig").CudaCompute.compactFlorenceSelfCache(cb, slab, batch, capacity, active_len, d_model);
}

test "LightOnOCR3 CUDA Florence packed self KV preserves page prefixes and returned ownership" {
    if (comptime !build_options.enable_cuda) return error.SkipZigTest;
    const CudaCompute = @import("../ops/cuda/cuda_compute.zig").CudaCompute;
    const allocator = std.testing.allocator;
    var compute = CudaCompute.init(allocator) catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
    defer compute.deinit();
    const cb = compute.computeBackend();
    const batch = 3;
    const allocated_pages = 5;
    const capacity = 7;
    const width = 4;
    // Finished pages leave spare physical slots after in-place compaction.
    var data: [allocated_pages * capacity * width]f32 = undefined;
    for (&data, 0..) |*x, i| x.* = @floatFromInt(i);
    const slab = try cb.fromFloat32Shape(&data, &.{ allocated_pages * capacity, width });
    defer cb.free(slab);
    for ([_]usize{ 1, 5, capacity }) |active| {
        const compacted = try compactSelfCacheRowsDevice(&cb, slab, batch, capacity, active, width);
        defer cb.free(compacted);
        // Both intermediate descriptors have been released before downloading.
        const actual = try cb.toFloat32(compacted, allocator);
        defer allocator.free(actual);
        for (0..batch) |b|
            try std.testing.expectEqualSlices(f32, data[b * capacity * width ..][0 .. active * width], actual[b * active * width ..][0 .. active * width]);
    }
    const after = try cb.toFloat32(slab, allocator);
    defer allocator.free(after);
    try std.testing.expectEqualSlices(f32, &data, after);
    try std.testing.expectError(error.InvalidInputShape, compactSelfCacheRowsDevice(&cb, slab, batch, capacity, capacity + 1, width));
}

fn tokenEmbeddingsData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    embed_w: CT,
    input_ids: []const i64,
    total: usize,
    dim: usize,
) ![]f32 {
    const emb = try cb.embeddingLookup(embed_w, input_ids, total, dim);
    defer cb.free(emb);
    return cb.toFloat32(emb, allocator);
}

fn promptEmbedWeight(cb: *const ComputeBackend) !CT {
    return getAnyWeight(cb, &.{
        "language_model.model.shared.weight",
        "language_model.model.encoder.embed_tokens.weight",
        "language_model.model.decoder.embed_tokens.weight",
        "model.decoder.embed_tokens.weight",
    });
}

fn decoderPosWeight(cb: *const ComputeBackend) !CT {
    return getAnyWeight(cb, &.{
        "language_model.model.decoder.embed_positions.weight",
        "model.decoder.embed_positions.weight",
    });
}

fn decoderLayerNormEmbeddingWeight(cb: *const ComputeBackend) !CT {
    return getAnyWeight(cb, &.{
        "language_model.model.decoder.layernorm_embedding.weight",
        "model.decoder.layernorm_embedding.weight",
    });
}

fn decoderLayerNormEmbeddingBias(cb: *const ComputeBackend) !CT {
    return getAnyWeight(cb, &.{
        "language_model.model.decoder.layernorm_embedding.bias",
        "model.decoder.layernorm_embedding.bias",
    });
}

fn lmHeadWeight(cb: *const ComputeBackend) !CT {
    return getAnyWeight(cb, &.{
        "language_model.lm_head.weight",
        "lm_head.weight",
        "language_model.model.shared.weight",
        "model.decoder.embed_tokens.weight",
    });
}

const LinearWeights = struct {
    weight: CT,
    bias: CT,
};

fn encoderLinearWeights(cb: *const ComputeBackend, layer: usize, proj: []const u8, buf: *[256]u8) !LinearWeights {
    const w_name = try std.fmt.bufPrint(buf, "language_model.model.encoder.layers.{d}.{s}.weight", .{ layer, proj });
    const w = try cb.getWeight(w_name);
    const b_name = try std.fmt.bufPrint(buf, "language_model.model.encoder.layers.{d}.{s}.bias", .{ layer, proj });
    const b = try cb.getWeight(b_name);
    return .{ .weight = w, .bias = b };
}

fn encoderLinearProj(
    cb: *const ComputeBackend,
    input: CT,
    layer: usize,
    proj: []const u8,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    buf: *[256]u8,
) !CT {
    const weights = try encoderLinearWeights(cb, layer, proj, buf);
    return cb.linear(input, weights.weight, weights.bias, rows, in_dim, out_dim);
}

fn encoderLayerWeight(cb: *const ComputeBackend, layer: usize, suffix: []const u8, buf: *[256]u8) !CT {
    const name = try std.fmt.bufPrint(buf, "language_model.model.encoder.layers.{d}.{s}", .{ layer, suffix });
    return cb.getWeight(name);
}

fn decoderLinearProj(
    cb: *const ComputeBackend,
    input: CT,
    layer: usize,
    proj: []const u8,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    buf: *[256]u8,
) !CT {
    const weights = try decoderLinearWeights(cb, layer, proj, buf);
    return cb.linear(input, weights.weight, weights.bias, rows, in_dim, out_dim);
}

fn decoderLinearWeights(cb: *const ComputeBackend, layer: usize, proj: []const u8, buf: *[256]u8) !LinearWeights {
    const w_name = try std.fmt.bufPrint(buf, "language_model.model.decoder.layers.{d}.{s}.weight", .{ layer, proj });
    const w = cb.getWeight(w_name) catch blk: {
        const legacy = try std.fmt.bufPrint(buf, "model.decoder.layers.{d}.{s}.weight", .{ layer, proj });
        break :blk try cb.getWeight(legacy);
    };
    const b_name = try std.fmt.bufPrint(buf, "language_model.model.decoder.layers.{d}.{s}.bias", .{ layer, proj });
    const b = cb.getWeight(b_name) catch blk: {
        const legacy = try std.fmt.bufPrint(buf, "model.decoder.layers.{d}.{s}.bias", .{ layer, proj });
        break :blk try cb.getWeight(legacy);
    };
    return .{ .weight = w, .bias = b };
}

fn decoderLayerWeight(cb: *const ComputeBackend, layer: usize, suffix: []const u8, buf: *[256]u8) !CT {
    const name = try std.fmt.bufPrint(buf, "language_model.model.decoder.layers.{d}.{s}", .{ layer, suffix });
    return cb.getWeight(name) catch blk: {
        const legacy = try std.fmt.bufPrint(buf, "model.decoder.layers.{d}.{s}", .{ layer, suffix });
        break :blk try cb.getWeight(legacy);
    };
}

fn getAnyWeight(cb: *const ComputeBackend, names: []const []const u8) !CT {
    var last_err: ?anyerror = null;
    for (names) |name| {
        const weight = cb.getWeight(name) catch |err| {
            last_err = err;
            continue;
        };
        return weight;
    }
    return last_err orelse error.MissingWeight;
}

fn tryOptionalWeight(cb: *const ComputeBackend, name: []const u8) !?CT {
    return cb.getWeight(name) catch |err| switch (err) {
        error.MissingWeight, error.WeightNotFound => null,
        else => err,
    };
}

fn backendLinearNoBiasData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: []const f32,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    weight: CT,
) ![]f32 {
    const shape = [_]i32{ @intCast(rows), @intCast(in_dim) };
    const input_ct = try cb.fromFloat32Shape(input, &shape);
    defer cb.free(input_ct);
    const out_ct = try cb.linearNoBias(input_ct, weight, rows, in_dim, out_dim);
    defer cb.free(out_ct);
    return cb.toFloat32(out_ct, allocator);
}

fn projectImageFeaturesData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: []const f32,
    rows: usize,
    vision_dim: usize,
    config: Config,
) ![]f32 {
    const proj_weight = try cb.getWeight("image_projection");
    if (cb.kind() == .cuda and !florenceDeviceVisionTailDisabled()) {
        const input_shape = [_]i64{ @intCast(vision_dim), @intCast(config.projection_dim) };
        if (cb.primTranspose(proj_weight, &.{ 1, 0 }, &input_shape)) |proj_ct| {
            defer cb.free(proj_ct);
            return try backendLinearNoBiasData(cb, allocator, input, rows, vision_dim, config.projection_dim, proj_ct);
        } else |err| switch (err) {
            error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.InvalidShape, error.UnsupportedTensorType => {},
            else => return err,
        }
    }

    const proj_weight_data = try cb.toFloat32(proj_weight, allocator);
    defer allocator.free(proj_weight_data);
    const proj_weight_t = try transposeMatrix(allocator, proj_weight_data, vision_dim, config.projection_dim);
    defer allocator.free(proj_weight_t);
    const proj_shape = [_]i32{ @intCast(config.projection_dim), @intCast(vision_dim) };
    const proj_ct = try cb.fromFloat32Shape(proj_weight_t, &proj_shape);
    defer cb.free(proj_ct);
    return try backendLinearNoBiasData(cb, allocator, input, rows, vision_dim, config.projection_dim, proj_ct);
}

fn backendLayerNormData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: []const f32,
    rows: usize,
    dim: usize,
    weight: CT,
    bias: CT,
    eps: f32,
) ![]f32 {
    const shape = [_]i32{ @intCast(rows), @intCast(dim) };
    const input_ct = try cb.fromFloat32Shape(input, &shape);
    defer cb.free(input_ct);
    const out_ct = try cb.layerNorm(input_ct, weight, bias, dim, eps);
    defer cb.free(out_ct);
    return cb.toFloat32(out_ct, allocator);
}

/// Florence checkpoints use Hugging Face's exact-erf `gelu`, not the
/// `gelu_pytorch_tanh` approximation exposed by `ComputeBackend.gelu`.
/// Every built-in backend supplies this hook. An external backend that does
/// not must fail explicitly rather than silently changing model semantics.
fn florenceExactGelu(cb: *const ComputeBackend, input: CT) !CT {
    return (try cb.geluExact(input)) orelse error.UnsupportedFlorenceExactGelu;
}

test "Florence GELU preserves exact-erf values without approximate fallback" {
    const Probe = struct {
        exact_calls: usize = 0,
        approximate_calls: usize = 0,

        fn exact(raw: *anyopaque, input: CT) !CT {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const values: *[4]f32 = @ptrCast(@alignCast(input));
            self.exact_calls += 1;
            activations.geluExact(values[0..]);
            return input;
        }

        fn approximate(raw: *anyopaque, input: CT) !CT {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const values: *[4]f32 = @ptrCast(@alignCast(input));
            self.approximate_calls += 1;
            activations.gelu(values[0..]);
            return input;
        }
    };

    var probe = Probe{};
    var vtable: ComputeBackend.VTable = undefined;
    vtable.gelu = Probe.approximate;
    vtable.geluExact = Probe.exact;
    const cb = ComputeBackend{ .ptr = &probe, .vtable = &vtable };
    var values = [_]f32{ 0.0, 1.0, -1.0, 2.0 };
    const input: CT = @ptrCast(&values);
    try std.testing.expectEqual(input, try florenceExactGelu(&cb, input));

    const expected = [_]f32{ 0.0, 0.8413447, -0.15865526, 1.9544997 };
    const tolerance = 4 * std.math.floatEps(f32);
    for (expected, values) |want, actual| {
        try std.testing.expectApproxEqAbs(want, actual, tolerance);
    }
    try std.testing.expectEqual(@as(usize, 1), probe.exact_calls);
    try std.testing.expectEqual(@as(usize, 0), probe.approximate_calls);

    vtable.geluExact = null;
    try std.testing.expectError(error.UnsupportedFlorenceExactGelu, florenceExactGelu(&cb, input));
    try std.testing.expectEqual(@as(usize, 0), probe.approximate_calls);
}

fn backendNormedExactGeluMlpData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: []const f32,
    rows: usize,
    in_dim: usize,
    hidden_dim: usize,
    norm_weight: CT,
    norm_bias: CT,
    fc1_weight: CT,
    fc1_bias: CT,
    fc2_weight: CT,
    fc2_bias: CT,
) ![]f32 {
    const input_shape = [_]i32{ @intCast(rows), @intCast(in_dim) };
    const input_ct = try cb.fromFloat32Shape(input, &input_shape);
    defer cb.free(input_ct);

    const normed_ct = try cb.layerNorm(input_ct, norm_weight, norm_bias, in_dim, 1e-5);
    defer cb.free(normed_ct);
    const fc1_ct = try cb.linear(normed_ct, fc1_weight, fc1_bias, rows, in_dim, hidden_dim);
    defer cb.free(fc1_ct);
    const activated_ct = try florenceExactGelu(cb, fc1_ct);
    defer cb.free(activated_ct);
    const fc2_ct = try cb.linear(activated_ct, fc2_weight, fc2_bias, rows, hidden_dim, in_dim);
    defer cb.free(fc2_ct);
    return cb.toFloat32(fc2_ct, allocator);
}

fn channelAttention(
    out: []f32,
    qkv: []const f32,
    batch: usize,
    seq_len: usize,
    dim: usize,
    groups: usize,
) !void {
    const channels_per_group = dim / groups;
    const scale = 1.0 / std.math.sqrt(@as(f32, @floatFromInt(seq_len)));
    var scores_buf: [64 * 64]f32 = undefined;

    for (0..batch) |b| {
        for (0..groups) |g| {
            const group_offset = g * channels_per_group;
            const score_slice = scores_buf[0 .. channels_per_group * channels_per_group];
            @memset(score_slice, 0.0);

            for (0..channels_per_group) |qc| {
                for (0..channels_per_group) |kc| {
                    var acc: f32 = 0.0;
                    for (0..seq_len) |n| {
                        const base = ((b * seq_len + n) * dim * 3) + group_offset;
                        acc += qkv[base + qc] * qkv[base + dim + kc];
                    }
                    score_slice[qc * channels_per_group + kc] = acc * scale;
                }
                softmaxInPlace(score_slice[qc * channels_per_group ..][0..channels_per_group]);
            }

            for (0..seq_len) |n| {
                const dst = (b * seq_len + n) * dim + group_offset;
                for (0..channels_per_group) |qc| {
                    var acc: f32 = 0.0;
                    for (0..channels_per_group) |vc| {
                        const base = ((b * seq_len + n) * dim * 3) + 2 * dim + group_offset;
                        acc += score_slice[qc * channels_per_group + vc] * qkv[base + vc];
                    }
                    out[dst + qc] = acc;
                }
            }
        }
    }
}

fn softmaxInPlace(values: []f32) void {
    var max_val = values[0];
    for (values[1..]) |value| {
        if (value > max_val) max_val = value;
    }
    var sum: f32 = 0.0;
    for (values) |*value| {
        value.* = @exp(value.* - max_val);
        sum += value.*;
    }
    if (sum == 0.0) return;
    const inv = 1.0 / sum;
    for (values) |*value| value.* *= inv;
}

fn addInPlace(dst: []f32, src: []const f32) void {
    for (dst, src) |*d, s| d.* += s;
}

const Conv2dResult = struct {
    data: []f32,
    height: usize,
    width: usize,
};

fn backendConv2dImageData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: []const f32,
    batch: usize,
    in_channels: usize,
    height: usize,
    width: usize,
    out_channels: usize,
    kernel_h: usize,
    kernel_w: usize,
    stride_h: usize,
    stride_w: usize,
    padding_h: usize,
    padding_w: usize,
    groups: usize,
    weight: CT,
    bias: CT,
) !Conv2dResult {
    const out_h = (height + 2 * padding_h - kernel_h) / stride_h + 1;
    const out_w = (width + 2 * padding_w - kernel_w) / stride_w + 1;
    const shape = [_]i32{ @intCast(batch), @intCast(in_channels), @intCast(height), @intCast(width) };
    const input_ct = try cb.fromFloat32Shape(input, &shape);
    defer cb.free(input_ct);
    const out_ct = try cb.conv2d(input_ct, weight, bias, batch, in_channels, out_channels, height, width, kernel_h, kernel_w, stride_h, stride_w, padding_h, padding_w, groups);
    defer cb.free(out_ct);
    return .{
        .data = try cb.toFloat32(out_ct, allocator),
        .height = out_h,
        .width = out_w,
    };
}

fn conv2d(
    allocator: std.mem.Allocator,
    input: []const f32,
    batch: usize,
    in_channels: usize,
    height: usize,
    width: usize,
    weight: []const f32,
    bias: []const f32,
    out_channels: usize,
    kernel: usize,
    stride: usize,
    padding: usize,
) !Conv2dResult {
    const out_h = (height + 2 * padding - kernel) / stride + 1;
    const out_w = (width + 2 * padding - kernel) / stride + 1;
    const out = try allocator.alloc(f32, batch * out_channels * out_h * out_w);

    for (0..batch) |b| {
        for (0..out_channels) |oc| {
            for (0..out_h) |oy| {
                for (0..out_w) |ox| {
                    var acc = bias[oc];
                    for (0..in_channels) |ic| {
                        for (0..kernel) |ky| {
                            for (0..kernel) |kx| {
                                const in_y_signed = @as(isize, @intCast(oy * stride + ky)) - @as(isize, @intCast(padding));
                                const in_x_signed = @as(isize, @intCast(ox * stride + kx)) - @as(isize, @intCast(padding));
                                if (in_y_signed < 0 or in_x_signed < 0) continue;
                                const in_y: usize = @intCast(in_y_signed);
                                const in_x: usize = @intCast(in_x_signed);
                                if (in_y >= height or in_x >= width) continue;
                                const in_idx = ((b * in_channels + ic) * height + in_y) * width + in_x;
                                const w_idx = (((oc * in_channels + ic) * kernel) + ky) * kernel + kx;
                                acc += input[in_idx] * weight[w_idx];
                            }
                        }
                    }
                    const out_idx = ((b * out_channels + oc) * out_h + oy) * out_w + ox;
                    out[out_idx] = acc;
                }
            }
        }
    }

    return .{ .data = out, .height = out_h, .width = out_w };
}

fn tokensToImage(
    allocator: std.mem.Allocator,
    tokens: []const f32,
    batch: usize,
    height: usize,
    width: usize,
    channels: usize,
) ![]f32 {
    const image = try allocator.alloc(f32, batch * channels * height * width);
    for (0..batch) |b| {
        for (0..height) |y| {
            for (0..width) |x| {
                const token_idx = (b * height * width + y * width + x) * channels;
                for (0..channels) |c| {
                    image[((b * channels + c) * height + y) * width + x] = tokens[token_idx + c];
                }
            }
        }
    }
    return image;
}

fn imageToTokens(
    allocator: std.mem.Allocator,
    image: []const f32,
    batch: usize,
    height: usize,
    width: usize,
    channels: usize,
) ![]f32 {
    const tokens = try allocator.alloc(f32, batch * height * width * channels);
    for (0..batch) |b| {
        for (0..height) |y| {
            for (0..width) |x| {
                const token_idx = (b * height * width + y * width + x) * channels;
                for (0..channels) |c| {
                    tokens[token_idx + c] = image[((b * channels + c) * height + y) * width + x];
                }
            }
        }
    }
    return tokens;
}

fn transposeMatrix(allocator: std.mem.Allocator, input: []const f32, rows: usize, cols: usize) ![]f32 {
    const transposed = try allocator.alloc(f32, input.len);
    for (0..rows) |row| {
        for (0..cols) |col| transposed[col * rows + row] = input[row * cols + col];
    }
    return transposed;
}

fn readProfileEnabled() bool {
    return platform.env.getenvBool("ANTFLY_INFERENCE_READ_PROFILE");
}

fn florenceMetalFramesEnabled() bool {
    return !platform.env.getenvBool("TERMITE_FLORENCE2_METAL_DISABLE_FRAMES");
}

fn florenceMetalStageFramesEnabled() bool {
    return !platform.env.getenvBool("TERMITE_FLORENCE2_METAL_DISABLE_STAGE_FRAMES");
}

fn beginFlorenceMetalFrame(cb: *const ComputeBackend, regime: ops.DecoderRuntimeFrameRegime) !bool {
    if (cb.kind() != .metal or !florenceMetalFramesEnabled() or debugStatsEnabled() or cb.decoderRuntimeHasActiveFrame()) return false;
    const active = try cb.decoderRuntimeBeginFrame();
    if (!active) return false;
    errdefer cb.decoderRuntimeCancelFrame() catch {};
    try cb.decoderRuntimeSetActiveFrameRegime(regime);
    return true;
}

fn florenceDecoderStageFrameProfilingEnabled(cb: *const ComputeBackend) bool {
    return cb.kind() == .metal and
        readProfileEnabled() and
        platform.env.getenvBool("TERMITE_FLORENCE2_METAL_PROFILE_DECODER_STAGE_FRAMES");
}

fn florenceVisionOpFrameProfilingEnabled(cb: *const ComputeBackend) bool {
    return cb.kind() == .metal and
        readProfileEnabled() and
        platform.env.getenvBool("TERMITE_FLORENCE2_METAL_PROFILE_VISION_OP_FRAMES");
}

fn profileFlorenceVisionOpFrameBoundary(
    cb: *const ComputeBackend,
    phase: []const u8,
    stage: usize,
    layer: usize,
    start_ns: u64,
) !void {
    if (!florenceVisionOpFrameProfilingEnabled(cb)) return;
    if (!cb.decoderRuntimeHasActiveFrame()) return error.UnsupportedOperation;
    const gpu_before = cb.debugTimingSnapshot().provider.decoder_runtime_frame_gpu_nanos;
    try cb.decoderRuntimeSubmitAndWaitFrame();
    const gpu_after = cb.debugTimingSnapshot().provider.decoder_runtime_frame_gpu_nanos;
    const gpu_ns = gpu_after -| gpu_before;
    std.log.info(
        "florence-profile phase={s} stage={d} layer={d} elapsed_ms={d:.3} gpu_ms={d:.3}",
        .{
            phase,
            stage,
            layer,
            nsToMs(nowNs() - start_ns),
            @as(f64, @floatFromInt(gpu_ns)) / @as(f64, std.time.ns_per_ms),
        },
    );
    if (!try beginFlorenceMetalFrame(cb, .prefill)) return error.UnsupportedOperation;
}

fn profileFlorenceDecoderStageFrameBoundary(
    cb: *const ComputeBackend,
    phase: []const u8,
    layer: usize,
    start_ns: u64,
) !u64 {
    if (!florenceDecoderStageFrameProfilingEnabled(cb)) return start_ns;
    if (!cb.decoderRuntimeHasActiveFrame()) return error.UnsupportedOperation;
    const gpu_before = cb.debugTimingSnapshot().provider.decoder_runtime_frame_gpu_nanos;
    try cb.decoderRuntimeSubmitAndWaitFrame();
    const gpu_after = cb.debugTimingSnapshot().provider.decoder_runtime_frame_gpu_nanos;
    const gpu_ns = gpu_after -| gpu_before;
    std.log.info(
        "florence-profile phase={s} layer={d} elapsed_ms={d:.3} gpu_ms={d:.3}",
        .{
            phase,
            layer,
            nsToMs(nowNs() - start_ns),
            @as(f64, @floatFromInt(gpu_ns)) / @as(f64, std.time.ns_per_ms),
        },
    );
    if (!try beginFlorenceMetalFrame(cb, .decode)) return error.UnsupportedOperation;
    return nowNs();
}

fn debugStatsEnabled() bool {
    return platform.env.getenvBool("ANTFLY_INFERENCE_FLORENCE_DEBUG_STATS");
}

fn logDebugStatsF32(label: []const u8, values: []const f32) void {
    if (values.len == 0) {
        std.log.info("florence-debug {s}: empty", .{label});
        return;
    }
    var sum: f64 = 0;
    var absmax: f32 = 0;
    for (values) |v| {
        sum += v;
        const a = @abs(v);
        if (a > absmax) absmax = a;
    }
    const mean = sum / @as(f64, @floatFromInt(values.len));
    const n_first = @min(values.len, 4);
    std.log.info("florence-debug {s}: n={d} mean={d:.6} absmax={d:.4} first={any}", .{
        label, values.len, mean, absmax, values[0..n_first],
    });
}

fn logDebugStatsTensor(cb: *const ComputeBackend, allocator: std.mem.Allocator, label: []const u8, tensor: CT) void {
    const host = cb.toFloat32(tensor, allocator) catch |err| {
        std.log.info("florence-debug {s}: download failed ({t})", .{ label, err });
        return;
    };
    defer allocator.free(host);
    logDebugStatsF32(label, host);
}

fn logFlorenceProfile(phase: []const u8, start_ns: u64) void {
    std.log.info("florence-profile phase={s} elapsed_ms={d:.3}", .{ phase, nsToMs(nowNs() - start_ns) });
}

fn logFlorenceProfileStep(phase: []const u8, step: usize, start_ns: u64) void {
    std.log.info("florence-profile phase={s} step={d} elapsed_ms={d:.3}", .{ phase, step, nsToMs(nowNs() - start_ns) });
}

fn logFlorenceProfileStage(phase: []const u8, stage: usize, start_ns: u64) void {
    std.log.info("florence-profile phase={s} stage={d} elapsed_ms={d:.3}", .{ phase, stage, nsToMs(nowNs() - start_ns) });
}

fn logFlorenceProfileStageLayer(phase: []const u8, stage: usize, layer: usize, start_ns: u64) void {
    std.log.info("florence-profile phase={s} stage={d} layer={d} elapsed_ms={d:.3}", .{ phase, stage, layer, nsToMs(nowNs() - start_ns) });
}

fn logFlorenceProfileStageLayerOp(phase: []const u8, stage: usize, layer: usize, start_ns: u64) void {
    std.log.info("florence-profile phase={s} stage={d} layer={d} elapsed_ms={d:.3}", .{ phase, stage, layer, nsToMs(nowNs() - start_ns) });
}

fn nowNs() u64 {
    return @import("antfly_platform").time.monotonicNs();
}

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1.0e6;
}

fn fmtAlloc(allocator: std.mem.Allocator, comptime fmt: []const u8, args: anytype) ![]u8 {
    return std.fmt.allocPrint(allocator, fmt, args);
}

fn stepRelease(cb: *const ComputeBackend, assigned: ?*@import("../execution/dense_decoder_schedule.zig").Schedule, value: CT) void {
    if (assigned) |schedule| schedule.releaseTemporary(cb, value) else cb.free(value);
}
fn stepAdd(cb: *const ComputeBackend, assigned: ?*@import("../execution/dense_decoder_schedule.zig").Schedule, a: CT, b: CT) !CT {
    if (assigned) |schedule| return schedule.execute(&.{ a, b }, null);
    return cb.add(a, b);
}
fn stepNorm(cb: *const ComputeBackend, assigned: ?*@import("../execution/dense_decoder_schedule.zig").Schedule, x: CT, w: CT, b: CT, width: usize) !CT {
    if (assigned) |schedule| return schedule.execute(&.{ x, w, b }, null);
    return cb.layerNorm(x, w, b, width, 1e-5);
}
fn stepLinear(cb: *const ComputeBackend, assigned: ?*@import("../execution/dense_decoder_schedule.zig").Schedule, x: CT, w: CT, b: CT, rows: usize, width: usize, out: usize) !CT {
    if (assigned) |schedule| return schedule.execute(&.{ x, w, b }, null);
    return cb.linear(x, w, b, rows, width, out);
}
fn stepTriple(cb: *const ComputeBackend, assigned: ?*@import("../execution/dense_decoder_schedule.zig").Schedule, x: CT, q: LinearWeights, k: LinearWeights, v: LinearWeights, rows: usize, width: usize) !ops.LinearTripleResult {
    if (assigned) |schedule| return .{ .first = try schedule.execute(&.{ x, q.weight, q.bias }, null), .second = try schedule.execute(&.{ x, k.weight, k.bias }, null), .third = try schedule.execute(&.{ x, v.weight, v.bias }, null) };
    return cb.linearTriple(x, q.weight, q.bias, k.weight, k.bias, v.weight, v.bias, rows, width, width);
}
fn stepEmbedding(cb: *const ComputeBackend, assigned: ?*@import("../execution/dense_decoder_schedule.zig").Schedule, weight: CT, ids: []const i64, rows: usize, width: usize) !CT {
    if (assigned) |schedule| {
        if (rows != 1 or ids.len != 1 or schedule.cursor >= schedule.steps.len) return error.DenseScheduleOrderChanged;
        const node = schedule.graph.node(schedule.steps[schedule.cursor]);
        if (node.op != .gather) return error.DenseScheduleOrderChanged;
        return schedule.execute(&.{ weight, schedule.values[node.inputs[1]] orelse return error.MissingValue }, null);
    }
    return cb.embeddingLookup(weight, ids, rows, width);
}
fn stepPrefix(cb: *const ComputeBackend, assigned: ?*@import("../execution/dense_decoder_schedule.zig").Schedule, q: CT, k: CT, v: CT, mask: []const i64, batch: usize, live: usize, heads: usize, dim: usize) !CT {
    if (assigned) |schedule| {
        if (batch != 1 or schedule.cursor >= schedule.steps.len) return error.DenseScheduleOrderChanged;
        const node = schedule.graph.node(schedule.steps[schedule.cursor]);
        if (schedule.values[node.inputs[1]] != k or schedule.values[node.inputs[2]] != v) return error.PreparedDensePrefixBindingChanged;
        const attention = ops.AttentionContext{ .total_sequence_len = live, .query_sequence_len = 1, .kv_sequence_len = live, .dense_attention_prefix = .{ .live_key_rows = live, .mask = mask } };
        return schedule.execute(&.{ q, k, v }, &attention);
    }
    return cb.crossAttention(q, k, v, mask, batch, 1, live, heads, dim);
}
fn appendPreparedSelfPair(cb: *const ComputeBackend, a: std.mem.Allocator, cache: *DecoderSelfCache, index: usize, k: CT, v: CT, batch: usize, old_len: usize, new_len: usize, width: usize) ![2]CachedSelfTensor {
    if (cb.vtable != &@import("../ops/native_compute.zig").vtable_impl or batch != 1 or !cache.preallocated or new_len != old_len + 1 or old_len >= cache.capacity or index >= cache.keys.len or index >= cache.values.len) return error.InvalidInputShape;
    const key = cache.keys[index] orelse return error.InvalidInputShape;
    const value = cache.values[index] orelse return error.InvalidInputShape;
    if (!try cb.copyRows2D(a, key, old_len, k, 0, 1, width) or !try cb.copyRows2D(a, value, old_len, v, 0, 1, width)) return error.UnsupportedDenseSchedule;
    return .{ .{ .tensor = key }, .{ .tensor = value } };
}

/// Request-private F32 single-query layer schedule. The final hidden value is
/// borrowed only by its enclosing scalar request; ordinary vocabulary-head
/// policy remains outside this graph and retains its original admission floor.
pub const PreparedSingleToken = struct {
    const Dense = @import("../execution/dense_decoder_schedule.zig");
    const Shape = @import("ml").graph.Shape;
    const Node = @import("ml").graph.NodeId;
    const Native = @import("../ops/native_compute.zig").NativeCompute;
    const Cleanup = @import("../execution/cleanup_context.zig").Context;
    const DomainPin = @import("../runtime/tier/admission_domain.zig").Pin;
    const Namespace = enum(u1) { language_model, model };
    const Pair = struct {
        w: CT,
        b: CT,
        // These choices belong to the checkpoint lookup, independently of CT.
        w_namespace: Namespace = .language_model,
        b_namespace: Namespace = .language_model,
    };
    const Layer = struct { q: Pair, k: Pair, v: Pair, o: Pair, cross_q: Pair, cross_k: Pair, cross_v: Pair, cross_o: Pair, self_norm: Pair, cross_norm: Pair, final_norm: Pair, fc1: Pair, fc2: Pair };
    const Weights = struct {
        a: std.mem.Allocator,
        handles: std.ArrayListUnmanaged(CT) = .empty,
        embed: CT = undefined,
        embed_source: u8 = 0,
        positions: CT = undefined,
        positions_source: u8 = 0,
        embed_norm: Pair = undefined,
        final_norm: ?Pair = null,
        head: CT = undefined,
        head_source: u8 = 0,
        head_bias: ?CT = null,
        layers: []Layer = &.{},
        fn fetch(self: *@This(), cb: *const ComputeBackend, data: *const anyopaque, pin: *const DomainPin, name: []const u8) !CT {
            try cb.checkExecutionControl();
            _ = try Native.prepareTypedWeightOutsideExecution(cb, data, name, pin);
            const query = Native.authoredResidentSourceQuery(cb, name) catch |err| switch (err) {
                error.WeightNotFound => return error.MissingWeight,
                error.UnsupportedAuthoredWeight => return error.UnsupportedDenseSchedule,
                else => return err,
            };
            const shape = query.shape orelse return error.UnsupportedDenseSchedule;
            if (shape.dtype != .f32 or query.quantized != null) return error.UnsupportedDenseSchedule;
            const value = try cb.getWeight(name);
            errdefer cb.free(value);
            try self.handles.append(self.a, value);
            return value;
        }
        fn optional(self: *@This(), cb: *const ComputeBackend, data: *const anyopaque, pin: *const DomainPin, name: []const u8) !?CT {
            return self.fetch(cb, data, pin, name) catch |err| switch (err) {
                error.MissingWeight, error.WeightNotFound => null,
                else => return err,
            };
        }
        const Resolved = struct { value: CT, choice: u8 };
        fn any(self: *@This(), cb: *const ComputeBackend, data: *const anyopaque, pin: *const DomainPin, names: []const []const u8) !Resolved {
            for (names, 0..) |name, index| if (try self.optional(cb, data, pin, name)) |value| return .{ .value = value, .choice = std.math.cast(u8, index) orelse return error.NameTooLong };
            return error.MissingWeight;
        }
        fn decoder(self: *@This(), cb: *const ComputeBackend, data: *const anyopaque, pin: *const DomainPin, layer: usize, suffix: []const u8) !Resolved {
            var name: [256]u8 = undefined;
            if (try self.optional(cb, data, pin, try std.fmt.bufPrint(&name, "language_model.model.decoder.layers.{d}.{s}", .{ layer, suffix }))) |value| return .{ .value = value, .choice = 0 };
            return .{ .value = try self.fetch(cb, data, pin, try std.fmt.bufPrint(&name, "model.decoder.layers.{d}.{s}", .{ layer, suffix })), .choice = 1 };
        }
        fn pair(self: *@This(), cb: *const ComputeBackend, data: *const anyopaque, pin: *const DomainPin, layer: usize, role: []const u8) !Pair {
            var suffix: [128]u8 = undefined;
            const w = try self.decoder(cb, data, pin, layer, try std.fmt.bufPrint(&suffix, "{s}.weight", .{role}));
            const b = try self.decoder(cb, data, pin, layer, try std.fmt.bufPrint(&suffix, "{s}.bias", .{role}));
            return .{ .w = w.value, .b = b.value, .w_namespace = @fromBackingInt(@intCast(w.choice)), .b_namespace = @fromBackingInt(@intCast(b.choice)) };
        }
        fn init(a: std.mem.Allocator, cb: *const ComputeBackend, data: *const anyopaque, pin: *const DomainPin, cfg: Config) !Weights {
            var self = Weights{ .a = a };
            errdefer self.deinit(cb, .{ .outside_execution = true });
            const embedding = try self.any(cb, data, pin, &.{ "language_model.model.shared.weight", "language_model.model.encoder.embed_tokens.weight", "language_model.model.decoder.embed_tokens.weight", "model.decoder.embed_tokens.weight" });
            self.embed = embedding.value;
            self.embed_source = embedding.choice;
            const positions = try self.any(cb, data, pin, &.{ "language_model.model.decoder.embed_positions.weight", "model.decoder.embed_positions.weight" });
            self.positions = positions.value;
            self.positions_source = positions.choice;
            const embed_w = try self.any(cb, data, pin, &.{ "language_model.model.decoder.layernorm_embedding.weight", "model.decoder.layernorm_embedding.weight" });
            const embed_b = try self.any(cb, data, pin, &.{ "language_model.model.decoder.layernorm_embedding.bias", "model.decoder.layernorm_embedding.bias" });
            self.embed_norm = .{ .w = embed_w.value, .b = embed_b.value, .w_namespace = @fromBackingInt(@intCast(embed_w.choice)), .b_namespace = @fromBackingInt(@intCast(embed_b.choice)) };
            self.layers = try a.alloc(Layer, cfg.decoder_layers);
            for (self.layers, 0..) |*layer, index| layer.* = .{
                .q = try self.pair(cb, data, pin, index, "self_attn.q_proj"),
                .k = try self.pair(cb, data, pin, index, "self_attn.k_proj"),
                .v = try self.pair(cb, data, pin, index, "self_attn.v_proj"),
                .o = try self.pair(cb, data, pin, index, "self_attn.out_proj"),
                .cross_q = try self.pair(cb, data, pin, index, "encoder_attn.q_proj"),
                .cross_k = try self.pair(cb, data, pin, index, "encoder_attn.k_proj"),
                .cross_v = try self.pair(cb, data, pin, index, "encoder_attn.v_proj"),
                .cross_o = try self.pair(cb, data, pin, index, "encoder_attn.out_proj"),
                .self_norm = try self.pair(cb, data, pin, index, "self_attn_layer_norm"),
                .cross_norm = try self.pair(cb, data, pin, index, "encoder_attn_layer_norm"),
                .final_norm = try self.pair(cb, data, pin, index, "final_layer_norm"),
                .fc1 = try self.pair(cb, data, pin, index, "fc1"),
                .fc2 = try self.pair(cb, data, pin, index, "fc2"),
            };
            if (try self.optional(cb, data, pin, "language_model.model.decoder.layer_norm.weight")) |w| {
                self.final_norm = .{ .w = w, .b = try self.fetch(cb, data, pin, "language_model.model.decoder.layer_norm.bias") };
            } else if (try self.optional(cb, data, pin, "model.decoder.layer_norm.weight")) |w| self.final_norm = .{ .w = w, .b = try self.fetch(cb, data, pin, "model.decoder.layer_norm.bias"), .w_namespace = .model, .b_namespace = .model };
            const head = try self.any(cb, data, pin, &.{ "language_model.lm_head.weight", "lm_head.weight", "language_model.model.shared.weight", "model.decoder.embed_tokens.weight" });
            self.head = head.value;
            self.head_source = head.choice;
            self.head_bias = try self.optional(cb, data, pin, "language_model.final_logits_bias");
            try Native.stampHeldF32WeightSources(cb, data, self.handles.items);
            for (self.handles.items) |value| _ = try Native.heldF32WeightPrimary(cb, data, value);
            return self;
        }
        fn deinit(self: *@This(), cb: *const ComputeBackend, current: Cleanup) void {
            for (self.handles.items) |value| cb.freeWithCleanup(value, current);
            self.handles.deinit(self.a);
            self.a.free(self.layers);
        }
    };
    pub const SpecSource = struct {
        program: *const PreparedSingleToken,
        const Role = Dense.RoleKey;
        const Kind = @FieldType(Role, "kind");
        const Resolved = struct {
            source_kind: @FieldType(Dense.ParameterSpec, "source_kind"),
            shape: ?@import("ml").graph.Shape = null,
            source_name: []const u8 = "",
            value: ?CT = null,
        };
        const layer_kinds = [_]Kind{ .self_q, .self_k, .self_v, .self_o, .self_norm, .cross_q, .cross_o, .cross_norm, .fc1, .fc2, .ffn_norm };
        pub fn header(self: @This()) Dense.ProgramSpec {
            const p = self.program;
            const c = p.config;
            return .{ .family = .florence, .phase = .decode, .geometry = .{ .layers = c.decoder_layers, .width = c.d_model, .heads = c.decoder_attention_heads, .ffn_dim = c.decoder_ffn_dim, .vocab_size = c.vocab_size, .encoder_rows = p.encoder_rows, .capacity = p.capacity, .query_rows = 1 }, .semantics = .{ .epsilon = 1e-5, .activation = .gelu_exact, .norm_order = .post, .position_offset = @intCast(bart_position_offset), .global_final_norm = p.weights.final_norm != null }, .parameters = &.{} };
        }
        pub fn len(self: @This()) usize {
            return 6 + self.program.weights.layers.len * 26 + (if (self.program.weights.final_norm != null) @as(usize, 2) else 0);
        }
        fn roleAt(self: @This(), index: usize) !Role {
            const globals = [_]Role{ .{ .kind = .token_ids }, .{ .kind = .position_ids }, .{ .kind = .embedding, .part = .weight }, .{ .kind = .positions, .part = .weight }, .{ .kind = .embed_norm, .part = .weight }, .{ .kind = .embed_norm, .part = .bias } };
            if (index < globals.len) return globals[index];
            const slot = index - globals.len;
            const layer = slot / 26;
            const local = slot % 26;
            if (layer < self.program.weights.layers.len) {
                if (local < 22) return .{ .kind = layer_kinds[local / 2], .layer = @intCast(layer), .part = if (local % 2 == 0) .weight else .bias };
                return .{ .kind = ([_]Kind{ .self_slab_k, .self_slab_v, .cross_slab_k, .cross_slab_v })[local - 22], .layer = @intCast(layer) };
            }
            if (self.program.weights.final_norm != null and layer == self.program.weights.layers.len and local < 2) return .{ .kind = .final_norm, .part = if (local == 0) .weight else .bias };
            return error.DenseDefinitionChanged;
        }
        fn layerPair(layer: Layer, kind: Kind) !Pair {
            return switch (kind) {
                .self_q => layer.q,
                .self_k => layer.k,
                .self_v => layer.v,
                .self_o => layer.o,
                .self_norm => layer.self_norm,
                .cross_q => layer.cross_q,
                .cross_k => layer.cross_k,
                .cross_v => layer.cross_v,
                .cross_o => layer.cross_o,
                .cross_norm => layer.cross_norm,
                .fc1 => layer.fc1,
                .fc2 => layer.fc2,
                .ffn_norm => layer.final_norm,
                else => error.DenseDefinitionChanged,
            };
        }
        fn pairName(pair: Pair, role: Role, buffer: *[256]u8) ![]const u8 {
            if (role.part == .value) return error.DenseDefinitionChanged;
            const namespace = if (role.part == .weight) pair.w_namespace else pair.b_namespace;
            const prefix = if (namespace == .language_model) "language_model.model.decoder" else "model.decoder";
            const part = if (role.part == .weight) "weight" else "bias";
            if (role.layer) |layer| {
                const suffix = switch (role.kind) {
                    .self_q => "self_attn.q_proj",
                    .self_k => "self_attn.k_proj",
                    .self_v => "self_attn.v_proj",
                    .self_o => "self_attn.out_proj",
                    .self_norm => "self_attn_layer_norm",
                    .cross_q => "encoder_attn.q_proj",
                    .cross_k => "encoder_attn.k_proj",
                    .cross_v => "encoder_attn.v_proj",
                    .cross_o => "encoder_attn.out_proj",
                    .cross_norm => "encoder_attn_layer_norm",
                    .fc1 => "fc1",
                    .fc2 => "fc2",
                    .ffn_norm => "final_layer_norm",
                    else => return error.DenseDefinitionChanged,
                };
                return std.fmt.bufPrint(buffer, "{s}.layers.{d}.{s}.{s}", .{ prefix, layer, suffix, part });
            }
            const suffix = switch (role.kind) {
                .embed_norm => "layernorm_embedding",
                .final_norm => "layer_norm",
                else => return error.DenseDefinitionChanged,
            };
            return std.fmt.bufPrint(buffer, "{s}.{s}.{s}", .{ prefix, suffix, part });
        }
        fn resolve(self: @This(), role: Role, buffer: *[256]u8) !Resolved {
            const p = self.program;
            const g = self.header().geometry;
            if (role.kind == .token_ids or role.kind == .position_ids) {
                if (role.layer != null or role.part != .value) return error.DenseDefinitionChanged;
                return .{ .shape = Shape.init(.i64, &.{1}), .source_kind = .host };
            }
            if (role.kind == .self_slab_k or role.kind == .self_slab_v or role.kind == .cross_slab_k or role.kind == .cross_slab_v) {
                const layer = role.layer orelse return error.DenseDefinitionChanged;
                if (layer >= g.layers or role.part != .value) return error.DenseDefinitionChanged;
                const rows = if (role.kind == .self_slab_k or role.kind == .self_slab_v) g.capacity else g.encoder_rows;
                return .{ .shape = Shape.init(.f32, &.{ @intCast(rows), @intCast(g.width) }), .source_kind = .external };
            }
            var value: CT = undefined;
            var name: []const u8 = undefined;
            if (role.layer) |layer| {
                if (layer >= p.weights.layers.len or role.part == .value) return error.DenseDefinitionChanged;
                const pair = try layerPair(p.weights.layers[layer], role.kind);
                value = if (role.part == .weight) pair.w else pair.b;
                name = try pairName(pair, role, buffer);
            } else switch (role.kind) {
                .embedding => {
                    if (role.part != .weight) return error.DenseDefinitionChanged;
                    value = p.weights.embed;
                    name = switch (p.weights.embed_source) {
                        0 => "language_model.model.shared.weight",
                        1 => "language_model.model.encoder.embed_tokens.weight",
                        2 => "language_model.model.decoder.embed_tokens.weight",
                        3 => "model.decoder.embed_tokens.weight",
                        else => return error.NativeWeightRoleChanged,
                    };
                },
                .positions => {
                    if (role.part != .weight) return error.DenseDefinitionChanged;
                    value = p.weights.positions;
                    name = switch (p.weights.positions_source) {
                        0 => "language_model.model.decoder.embed_positions.weight",
                        1 => "model.decoder.embed_positions.weight",
                        else => return error.NativeWeightRoleChanged,
                    };
                },
                .embed_norm, .final_norm => {
                    if (role.part == .value) return error.DenseDefinitionChanged;
                    const pair = if (role.kind == .embed_norm) p.weights.embed_norm else p.weights.final_norm orelse return error.DenseDefinitionChanged;
                    value = if (role.part == .weight) pair.w else pair.b;
                    name = try pairName(pair, role, buffer);
                },
                else => return error.DenseDefinitionChanged,
            }
            return .{ .source_kind = .held_weight, .source_name = name, .value = value };
        }
        /// Pure checkpoint-role name sizing. No Native/source query or
        /// allocation: current held shape/stamp/name validation stays in at().
        pub fn expectedNameBytes(self: @This()) !usize {
            var bytes: usize = 0;
            var buffer: [256]u8 = undefined;
            for (0..self.len()) |index| {
                const resolved = try self.resolve(try self.roleAt(index), &buffer);
                bytes = try std.math.add(usize, bytes, resolved.source_name.len);
            }
            return bytes;
        }
        pub fn at(self: @This(), index: usize, buffer: *[256]u8) !Dense.ParameterSpec {
            const role = try self.roleAt(index);
            const resolved = try self.resolve(role, buffer);
            const shape = if (resolved.source_kind == .held_weight)
                try Native.heldF32WeightShapeNamed(&self.program.backend, self.program.source_data, resolved.value.?, resolved.source_name)
            else
                resolved.shape.?;
            if ((shape.numElements() orelse return error.UnsupportedDenseSchedule) <= 0) return error.UnsupportedDenseSchedule;
            return .{ .role = role, .shape = shape, .source_kind = resolved.source_kind, .source_name = resolved.source_name };
        }
        fn bind(self: @This(), a: std.mem.Allocator, spec: *const Dense.ProgramSpec) ![]Dense.BoundParameter {
            const expected_header = self.header();
            if (spec.family != expected_header.family or spec.phase != expected_header.phase or !std.meta.eql(spec.geometry, expected_header.geometry) or !std.meta.eql(spec.semantics, expected_header.semantics) or spec.parameters.len != self.len()) return error.DenseDefinitionChanged;
            const result = try a.alloc(Dense.BoundParameter, spec.parameters.len);
            errdefer a.free(result);
            for (spec.parameters, result, 0..) |declared, *bound, index| {
                if (!std.meta.eql(declared.role, try self.roleAt(index))) return error.DenseDefinitionChanged;
                var buffer: [256]u8 = undefined;
                const resolved = try self.resolve(declared.role, &buffer);
                if (declared.source_kind != resolved.source_kind or !std.mem.eql(u8, declared.source_name, resolved.source_name)) return error.DenseDefinitionChanged;
                if (resolved.shape) |shape| if (!declared.shape.eq(shape)) return error.DenseDefinitionChanged;
                bound.* = .{ .role = declared.role, .value = resolved.value, .source_data = if (declared.source_kind == .held_weight) self.program.source_data else null };
                if (declared.source_kind == .external) {
                    const root = self.program.roots[declared.role.layer.?];
                    const value = switch (declared.role.kind) {
                        .self_slab_k => root.self_k,
                        .self_slab_v => root.self_v,
                        .cross_slab_k => root.cross_k,
                        .cross_slab_v => root.cross_v,
                        else => return error.DenseDefinitionChanged,
                    };
                    bound.value = value;
                }
            }
            return result;
        }
    };
    pub fn specSource(self: *const @This()) !SpecSource {
        try self.validateCache();
        return .{ .program = self };
    }
    fn validateExtraNames(self: *const @This()) !void {
        // These projections remain outside the authored Frame and earn no
        // ProgramSpec coverage. Preserve current named sources before writes.
        var buffer: [256]u8 = undefined;
        for (self.weights.layers, 0..) |layer, index| for ([_]Dense.RoleKey{ .{ .kind = .cross_k, .layer = @intCast(index), .part = .weight }, .{ .kind = .cross_k, .layer = @intCast(index), .part = .bias }, .{ .kind = .cross_v, .layer = @intCast(index), .part = .weight }, .{ .kind = .cross_v, .layer = @intCast(index), .part = .bias } }) |role| {
            const pair = try SpecSource.layerPair(layer, role.kind);
            const value = if (role.part == .weight) pair.w else pair.b;
            _ = try Native.heldF32WeightPrimaryNamed(&self.backend, self.source_data, value, try SpecSource.pairName(pair, role, &buffer));
        };
        const head_name = switch (self.weights.head_source) {
            0 => "language_model.lm_head.weight",
            1 => "lm_head.weight",
            2 => "language_model.model.shared.weight",
            3 => "model.decoder.embed_tokens.weight",
            else => return error.NativeWeightRoleChanged,
        };
        _ = try Native.heldF32WeightPrimaryNamed(&self.backend, self.source_data, self.weights.head, head_name);
        if (self.weights.head_bias) |bias| _ = try Native.heldF32WeightPrimaryNamed(&self.backend, self.source_data, bias, "language_model.final_logits_bias");
    }

    allocator: std.mem.Allocator,
    backend: ComputeBackend,
    source_data: *const anyopaque,
    config: Config,
    weights: Weights,
    cache: ?DecoderIncrementalCache = null,
    roots: []const struct { self_k: CT, self_v: CT, cross_k: CT, cross_v: CT } = &.{},
    capacity: usize = 0,
    encoder_rows: usize = 0,
    token_rows: usize = 0,
    schedule: ?*Dense.Schedule = null,
    token: [1]i64 = .{0},
    position: [1]i64 = .{bart_position_offset},
    token_node: Node = 0,
    position_node: Node = 0,
    poisoned: bool = false,
    initialized: bool = false,
    completed_steps: usize = 0,

    /// All payloads are ordinary private cache roots funded by the ORIGINAL
    /// task's KV class. Only metadata uses a; Frame banks have separate funding.
    pub fn create(a: std.mem.Allocator, cb: *const ComputeBackend, data: *const anyopaque, pin: *const DomainPin, cfg: Config, rows: usize, capacity: usize) !?*PreparedSingleToken {
        if (cb.vtable != &@import("../ops/native_compute.zig").vtable_impl or cfg.decoder_layers == 0 or capacity == 0 or rows == 0 or cfg.d_model == 0 or cfg.decoder_attention_heads == 0 or cfg.d_model % cfg.decoder_attention_heads != 0 or cfg.decoder_ffn_dim == 0) return null;
        if (rows > std.math.maxInt(i32) or capacity > std.math.maxInt(i32) or cfg.d_model > std.math.maxInt(i32) or cfg.decoder_ffn_dim > std.math.maxInt(i32)) return null;
        const self = try a.create(PreparedSingleToken);
        errdefer a.destroy(self);
        self.* = .{ .allocator = a, .backend = cb.withoutSynchronousBorrows(), .source_data = data, .config = cfg, .capacity = capacity, .encoder_rows = rows, .weights = try Weights.init(a, cb, data, pin, cfg) };
        errdefer self.weights.deinit(cb, .{ .outside_execution = true });
        const position_shape = (try Native.heldF32WeightPrimary(cb, data, self.weights.positions)).descriptor.shape;
        const token_shape = (try Native.heldF32WeightPrimary(cb, data, self.weights.embed)).descriptor.shape;
        if (position_shape.rank() != 2 or token_shape.rank() != 2 or position_shape.dims[1] != cfg.d_model or token_shape.dims[1] != cfg.d_model or token_shape.dims[0] < cfg.vocab_size or (std.math.cast(usize, position_shape.dims[0]) orelse return error.UnsupportedDenseSchedule) < try std.math.add(usize, capacity, @as(usize, @intCast(bart_position_offset)))) return error.UnsupportedDenseSchedule;
        _ = try decoderKvBytes(cfg, 1, rows, capacity);
        self.token_rows = std.math.cast(usize, token_shape.dims[0]) orelse return error.UnsupportedDenseSchedule;
        self.cache = try emptyCache(a, cb, cfg, rows, capacity);
        errdefer self.cache.?.deinit(cb, a);
        const Root = @typeInfo(@TypeOf(self.roots)).pointer.child;
        const roots = try a.alloc(Root, cfg.decoder_layers);
        for (roots, 0..) |*root, index| root.* = .{ .self_k = self.cache.?.self.keys[index].?, .self_v = self.cache.?.self.values[index].?, .cross_k = self.cache.?.cross.keys[index], .cross_v = self.cache.?.cross.values[index] };
        self.roots = roots;
        return self;
    }
    fn emptyCache(a: std.mem.Allocator, cb: *const ComputeBackend, cfg: Config, rows: usize, capacity: usize) !DecoderIncrementalCache {
        const keys = try a.alloc(CT, cfg.decoder_layers);
        errdefer a.free(keys);
        const values = try a.alloc(CT, cfg.decoder_layers);
        errdefer a.free(values);
        var built: usize = 0;
        errdefer for (keys[0..built], values[0..built]) |k, v| {
            cb.free(k);
            cb.free(v);
        };
        const shape = [_]i32{ @intCast(rows), @intCast(cfg.d_model) };
        for (keys, values) |*k, *v| {
            k.* = (try cb.allocUninitF32Shape(&shape)) orelse return error.UnsupportedDenseSchedule;
            v.* = (cb.allocUninitF32Shape(&shape) catch |err| {
                cb.free(k.*);
                return err;
            }) orelse {
                cb.free(k.*);
                return error.UnsupportedDenseSchedule;
            };
            built += 1;
        }
        var self_cache = (try DecoderSelfCache.tryInitPreallocated(cb, a, cfg.decoder_layers, 1, capacity, cfg.d_model)) orelse return error.UnsupportedDenseSchedule;
        errdefer self_cache.deinit(cb, a);
        const encoder_mask = try a.alloc(i64, rows);
        errdefer a.free(encoder_mask);
        const self_mask = try a.alloc(i64, capacity);
        @memset(encoder_mask, 1);
        @memset(self_mask, 1);
        return .{ .cross = .{ .keys = keys, .values = values }, .self = self_cache, .encoder_mask = encoder_mask, .self_mask = self_mask, .batch = 1, .enc_seq = rows };
    }
    pub fn author(self: *@This(), preparation: ?*Dense.Preparation, spec: *const Dense.ProgramSpec, storage: ?*Dense.ProgramSpecStorage) !void {
        const source = try self.specSource();
        try self.validateExtraNames();
        const bound = try source.bind(self.allocator, spec);
        defer self.allocator.free(bound);
        if (preparation) |prepared| if (prepared.cached) |definition| {
            const schedule = try Dense.Schedule.instantiate(self.allocator, &self.backend, definition, spec, bound);
            errdefer schedule.deinit(.{ .outside_execution = true });
            const token_node = try definition.parameterNode(.{ .kind = .token_ids });
            const position_node = try definition.parameterNode(.{ .kind = .position_ids });
            self.token_node = token_node;
            self.position_node = position_node;
            self.schedule = schedule;
            self.cache.?.assigned = schedule;
            return;
        };
        if (spec.family != .florence or spec.phase != .decode or spec.geometry.query_rows != 1 or spec.semantics.activation != .gelu_exact or spec.semantics.norm_order != .post or spec.semantics.epsilon != 1e-5 or spec.semantics.position_offset != bart_position_offset) return error.DenseDefinitionChanged;
        var builder = if (storage) |owned| Dense.Builder.initSpecStorage(self.allocator, &self.backend, preparation, owned, bound) else Dense.Builder.initSpec(self.allocator, &self.backend, preparation, spec, bound);
        defer builder.deinit();
        const nodes = try Dense.authorSemanticGraph(&builder);
        self.schedule = try Dense.Schedule.create(&builder);
        self.token_node = nodes.token_node;
        self.position_node = nodes.position_node;
        self.cache.?.assigned = self.schedule;
    }
    pub fn prepareInto(self: *@This(), capability: ?@import("../execution/storage_admission.zig").Capability, current: Cleanup) !void {
        try self.bindInputs(capability, current, true);
    }
    fn bindInputs(self: *@This(), capability: ?@import("../execution/storage_admission.zig").Capability, current: Cleanup, first: bool) !void {
        const Tensor = @import("../backends/tensor.zig").Tensor;
        var token = Tensor{ .allocator = self.allocator, .name = "decoder.token", .dtype = .i64, .shape = &.{1}, .data = std.mem.asBytes(&self.token), .owns_data = false, .owns_shape = false };
        var position = Tensor{ .allocator = self.allocator, .name = "decoder.position", .dtype = .i64, .shape = &.{1}, .data = std.mem.asBytes(&self.position), .owns_data = false, .owns_shape = false };
        const inputs = [_]@import("../execution/host_ingress.zig").Input{ .{ .node_id = self.token_node, .value = &token }, .{ .node_id = self.position_node, .value = &position } };
        const schedule = self.schedule orelse return error.UnpreparedDenseSchedule;
        if (first) try schedule.enter(capability, current, &inputs) else {
            const frame = schedule.frame orelse return error.DenseScheduleNotEntered;
            for (try frame.rebindNativeProgramHostInputs(&inputs)) |input| schedule.values[input.node_id] = input.value;
        }
    }
    fn validateCache(self: *const @This()) !void {
        const cache = self.cache orelse return error.UnpreparedDenseSchedule;
        if (cache.assigned != self.schedule or self.weights.layers.len != self.roots.len) return error.PreparedFlorenceSourceChanged;
        if (cache.batch != 1 or cache.cross_active != null or !cache.self.preallocated or cache.self.capacity != self.capacity or cache.enc_seq != self.encoder_rows or cache.self_mask.len != self.capacity or cache.encoder_mask.len != self.encoder_rows or cache.self.keys.len != self.roots.len or cache.self.values.len != self.roots.len or cache.cross.keys.len != self.roots.len or cache.cross.values.len != self.roots.len) return error.PreparedFlorenceSourceChanged;
        for (self.roots, 0..) |root, index| if (cache.self.keys[index] != root.self_k or cache.self.values[index] != root.self_v or cache.cross.keys[index] != root.cross_k or cache.cross.values[index] != root.cross_v) return error.PreparedFlorenceSourceChanged;
    }
    pub fn initializeCross(self: *@This(), encoder: CT, rows: usize, control: ?@import("../execution_control.zig").InferenceExecutionControl) !void {
        try self.validateCache();
        if (self.initialized or self.poisoned or rows != self.cache.?.enc_seq) return error.PreparedFlorenceSourceChanged;
        try Native.validateHeldF32WeightSources(&self.backend, self.source_data, self.weights.handles.items);
        try self.validateExtraNames();
        const cache = &self.cache.?;
        for (self.weights.layers, 0..) |layer, index| {
            if (control) |active| try active.check();
            const k = try Dense.projectionDescriptor(&self.backend, self.source_data, encoder, layer.cross_k.w, layer.cross_k.b, cache.cross.keys[index], rows, self.config.d_model, self.config.d_model);
            const v = try Dense.projectionDescriptor(&self.backend, self.source_data, encoder, layer.cross_v.w, layer.cross_v.b, cache.cross.values[index], rows, self.config.d_model, self.config.d_model);
            if (!Native.canApplyDenseProjectionInto(&self.backend, &k, &.{ encoder, layer.cross_k.w, layer.cross_k.b }, cache.cross.keys[index]) or !Native.canApplyDenseProjectionInto(&self.backend, &v, &.{ encoder, layer.cross_v.w, layer.cross_v.b }, cache.cross.values[index])) return error.UnsupportedDenseSchedule;
        }
        errdefer self.poisoned = true;
        for (self.weights.layers, 0..) |layer, index| {
            if (control) |active| try active.check();
            const k = try Dense.projectionDescriptor(&self.backend, self.source_data, encoder, layer.cross_k.w, layer.cross_k.b, cache.cross.keys[index], rows, self.config.d_model, self.config.d_model);
            const v = try Dense.projectionDescriptor(&self.backend, self.source_data, encoder, layer.cross_v.w, layer.cross_v.b, cache.cross.values[index], rows, self.config.d_model, self.config.d_model);
            if (!try Native.applyDenseProjectionInto(&self.backend, &k, &.{ encoder, layer.cross_k.w, layer.cross_k.b }, cache.cross.keys[index]) or !try Native.applyDenseProjectionInto(&self.backend, &v, &.{ encoder, layer.cross_v.w, layer.cross_v.b }, cache.cross.values[index])) return error.PreparedFlorenceSourceChanged;
        }
        self.initialized = true;
    }
    pub fn beginExecution(self: *@This(), current: Cleanup) !void {
        if (!self.initialized or self.poisoned) return error.UnpreparedDenseSchedule;
        try self.schedule.?.beginExecution(current);
    }
    /// Borrow lasts only until the next step; only the enclosing scalar request
    /// can consume it through an owning retained header in the shared head call.
    pub fn stepBorrowed(self: *@This(), token: i64) !CT {
        return self.stepWithControl(token, null);
    }
    pub fn stepWithControl(self: *@This(), token: i64, control: ?@import("../execution_control.zig").InferenceExecutionControl) !CT {
        // The current caller owns this control only through synchronous math.
        // Retained backend/Definition/Frame never capture its request pointer.
        var receiving = self.backend;
        receiving.execution_control = control;
        try self.validateCache();
        const cache = &self.cache.?;
        if (self.poisoned or !self.initialized or token < 0 or @as(u64, @intCast(token)) >= self.token_rows or cache.self.len >= cache.self.capacity) return error.InvalidInputShape;
        self.token[0] = token;
        self.position[0] = std.math.cast(i64, try std.math.add(usize, cache.self.len, bart_position_offset)) orelse return error.InvalidInputShape;
        errdefer self.poisoned = true;
        try self.bindInputs(null, .{}, false);
        self.schedule.?.cursor = 0;
        const result = try decoderForwardIncrementalBatchStepWithWeights(&receiving, self.allocator, self.config, &.{token}, cache, &self.weights);
        if (self.schedule.?.cursor != self.schedule.?.steps.len) return error.DenseScheduleOrderChanged;
        self.completed_steps += 1;
        return result;
    }
    pub fn deinit(self: *@This(), current: Cleanup) void {
        if (self.schedule) |schedule| schedule.deinit(current);
        if (self.cache) |*cache| cache.deinit(&self.backend, self.allocator);
        self.weights.deinit(&self.backend, current);
        self.allocator.free(self.roots);
        self.allocator.destroy(self);
    }
};

test "Florence private held tuple supplies exact original roles without a repeated provider lookup" {
    var layer: PreparedSingleToken.Layer = undefined;
    inline for (@typeInfo(PreparedSingleToken.Layer).@"struct".field_names, 0..) |field, i| {
        @field(layer, field) = .{ .w = @ptrFromInt(0x1000 + i * 32), .b = @ptrFromInt(0x1010 + i * 32) };
    }
    // These are only metadata identities: invoking Native getWeight would
    // touch invalid ctx16. No numerical/ownership credit is inferred here.
    const cb = ComputeBackend{ .ptr = @ptrFromInt(16), .vtable = &@import("../ops/native_compute.zig").vtable_impl };
    var buf: [256]u8 = undefined;
    inline for (.{ .{ "self_attn.q_proj", "q" }, .{ "self_attn.k_proj", "k" }, .{ "self_attn.v_proj", "v" }, .{ "self_attn.out_proj", "o" }, .{ "encoder_attn.q_proj", "cross_q" }, .{ "encoder_attn.k_proj", "cross_k" }, .{ "encoder_attn.v_proj", "cross_v" }, .{ "encoder_attn.out_proj", "cross_o" }, .{ "fc1", "fc1" }, .{ "fc2", "fc2" } }) |entry| {
        const pair = try decoderStepLinearWeights(&cb, 0, entry[0], &buf, &layer, entry[1]);
        try std.testing.expectEqual(@field(layer, entry[1]).w, pair.weight);
        try std.testing.expectEqual(@field(layer, entry[1]).b, pair.bias);
    }
    inline for (.{ .{ "self_attn_layer_norm", "self_norm" }, .{ "encoder_attn_layer_norm", "cross_norm" }, .{ "final_layer_norm", "final_norm" } }) |entry| {
        const pair = try decoderStepNormWeights(&cb, 0, entry[0], &buf, &layer, entry[1]);
        try std.testing.expectEqual(@field(layer, entry[1]), pair);
    }
}
