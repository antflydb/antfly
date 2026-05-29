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
const web_cache = @import("cache_state.zig");
const web_runtime = @import("runtime_state.zig");

pub var runtime = web_runtime.Runtime{};
pub var caches = web_cache.CacheState{};

pub const allocator = std.heap.wasm_allocator;
pub const GpuKvKeyFormat = web_cache.GpuKvKeyFormat;
pub const GpuKvValueFormat = web_cache.GpuKvValueFormat;

pub fn getModel(model_handle: u32) !*web_runtime.Model {
    return runtime.getModel(model_handle);
}

pub fn parseGpuKvKeyFormat(raw: u32) !GpuKvKeyFormat {
    return switch (raw) {
        0 => .f32,
        1 => .polar4,
        2 => .turbo3,
        else => error.InvalidCacheDtype,
    };
}

pub fn parseGpuKvValueFormat(raw: u32) !GpuKvValueFormat {
    return switch (raw) {
        0 => .f32,
        1 => .int8_per_head,
        else => error.InvalidCacheDtype,
    };
}
