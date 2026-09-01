// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const t5_arch = @import("../architectures/t5.zig");
const wasm_compute = @import("../ops/wasm_compute.zig");

pub const max_caches = 16;

pub const WasmKvCache = wasm_compute.WasmKvCache;
pub const GpuKvCache = wasm_compute.GpuKvCache;
pub const GpuKvKeyFormat = wasm_compute.GpuKvKeyFormat;
pub const GpuKvValueFormat = wasm_compute.GpuKvValueFormat;

pub const CacheState = struct {
    kv_caches: [max_caches]?WasmKvCache = @as([max_caches]?WasmKvCache, @splat(null)),
    gpu_kv_caches: [max_caches]?GpuKvCache = @as([max_caches]?GpuKvCache, @splat(null)),
    t5_cross_caches: [max_caches]?t5_arch.T5CrossCache = @as([max_caches]?t5_arch.T5CrossCache, @splat(null)),

    pub fn createGpt(
        self: *CacheState,
        allocator: std.mem.Allocator,
        use_gpu: bool,
        num_layers: u32,
        num_kv_heads: u32,
        head_dim: u32,
        max_len: u32,
        key_format: GpuKvKeyFormat,
        value_format: GpuKvValueFormat,
    ) !u32 {
        for (&self.kv_caches, 1..) |*slot, handle| {
            if (slot.* == null) {
                slot.* = try WasmKvCache.init(
                    allocator,
                    num_layers,
                    num_kv_heads,
                    head_dim,
                    max_len,
                );
                if (use_gpu) {
                    self.gpu_kv_caches[handle - 1] = GpuKvCache.initWithFormats(
                        allocator,
                        num_layers,
                        num_kv_heads,
                        head_dim,
                        max_len,
                        key_format,
                        value_format,
                    ) catch null;
                }
                return @intCast(handle);
            }
        }
        return error.TooManyCaches;
    }

    pub fn createT5(
        self: *CacheState,
        allocator: std.mem.Allocator,
        use_gpu: bool,
        num_layers: u32,
        num_heads: u32,
        head_dim: u32,
        max_len: u32,
    ) !u32 {
        for (&self.kv_caches, 1..) |*slot, handle| {
            if (slot.* == null) {
                slot.* = try WasmKvCache.init(allocator, num_layers, num_heads, head_dim, max_len);
                if (use_gpu) {
                    self.gpu_kv_caches[handle - 1] = GpuKvCache.init(allocator, num_layers, num_heads, head_dim, max_len) catch null;
                }
                self.t5_cross_caches[handle - 1] = try t5_arch.T5CrossCache.init(allocator, num_layers);
                return @intCast(handle);
            }
        }
        return error.TooManyCaches;
    }

    pub fn getCache(self: *CacheState, handle: u32) !*WasmKvCache {
        if (handle == 0 or handle > max_caches) return error.InvalidHandle;
        return &(self.kv_caches[handle - 1] orelse return error.InvalidHandle);
    }

    pub fn getGpuCache(self: *CacheState, handle: u32) ?*GpuKvCache {
        if (handle == 0 or handle > max_caches) return null;
        if (self.gpu_kv_caches[handle - 1] == null) return null;
        return &(self.gpu_kv_caches[handle - 1].?);
    }

    pub fn getT5CrossCache(self: *CacheState, handle: u32) !*t5_arch.T5CrossCache {
        if (handle == 0 or handle > max_caches) return error.InvalidHandle;
        return &(self.t5_cross_caches[handle - 1] orelse return error.InvalidHandle);
    }

    pub fn syncGpuCachedLen(self: *CacheState, handle: u32, cached_len: usize) void {
        if (self.getGpuCache(handle)) |gpu_cache| {
            gpu_cache.cached_len = cached_len;
        }
    }

    pub fn resetGpt(self: *CacheState, handle: u32) void {
        if (handle == 0 or handle > max_caches) return;
        if (self.kv_caches[handle - 1]) |*cache| cache.reset();
        if (self.gpu_kv_caches[handle - 1]) |*gpu_cache| gpu_cache.reset();
    }

    pub fn freeGpt(self: *CacheState, handle: u32) void {
        if (handle == 0 or handle > max_caches) return;
        if (self.gpu_kv_caches[handle - 1]) |*gpu_cache| {
            gpu_cache.deinit();
            self.gpu_kv_caches[handle - 1] = null;
        }
        if (self.kv_caches[handle - 1]) |*cache| {
            cache.deinit();
            self.kv_caches[handle - 1] = null;
        }
    }

    pub fn truncateGpt(self: *CacheState, handle: u32, new_len: u32) void {
        if (handle == 0 or handle > max_caches) return;
        if (self.kv_caches[handle - 1]) |*cache| {
            cache.truncateTo(new_len);
        }
        if (self.gpu_kv_caches[handle - 1]) |*gpu_cache| {
            gpu_cache.truncateTo(new_len);
        }
    }

    pub fn resetT5(self: *CacheState, handle: u32) void {
        if (handle == 0 or handle > max_caches) return;
        if (self.kv_caches[handle - 1]) |*cache| cache.reset();
        if (self.gpu_kv_caches[handle - 1]) |*gpu_cache| gpu_cache.reset();
        if (self.t5_cross_caches[handle - 1]) |*cross_cache| cross_cache.reset();
    }

    pub fn freeT5(self: *CacheState, handle: u32) void {
        if (handle == 0 or handle > max_caches) return;
        if (self.gpu_kv_caches[handle - 1]) |*gpu_cache| {
            gpu_cache.deinit();
            self.gpu_kv_caches[handle - 1] = null;
        }
        if (self.kv_caches[handle - 1]) |*cache| {
            cache.deinit();
            self.kv_caches[handle - 1] = null;
        }
        if (self.t5_cross_caches[handle - 1]) |*cross_cache| {
            cross_cache.deinit();
            self.t5_cross_caches[handle - 1] = null;
        }
    }
};
