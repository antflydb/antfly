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

const builtin = @import("builtin");
const std = @import("std");
const build_options = @import("build_options");
const linalg_primitives = @import("termite_linalg").primitives;

pub const MemoryModel = enum {
    wasm32,
    wasm64,
};

pub const configured_memory_model: MemoryModel =
    std.meta.stringToEnum(MemoryModel, build_options.wasm_memory_model) orelse .wasm32;

pub const is_wasm_target = builtin.cpu.arch == .wasm32 or builtin.cpu.arch == .wasm64;
pub const effective_memory_model: MemoryModel = switch (builtin.cpu.arch) {
    .wasm32 => .wasm32,
    .wasm64 => .wasm64,
    else => configured_memory_model,
};

pub const is_wasm32 = effective_memory_model == .wasm32;
pub const is_wasm64 = effective_memory_model == .wasm64;

pub const HostSize = if (is_wasm64) u64 else u32;

/// Comptime-selected SIMD width.  Re-exported from `lib/linalg/primitives`
/// so termite and lib/linalg cannot drift -- both used to carry their own
/// copies with the same per-arch logic, and a wasm64 update applied to one
/// without the other was a real near-miss before this dedupe.  Selection
/// rules live in `linalg.primitives.vec_len`.
pub const simd_f32_lanes: comptime_int = linalg_primitives.vec_len;

comptime {
    if (is_wasm_target and configured_memory_model != effective_memory_model) {
        @compileError("build_options.wasm_memory_model must match the selected WASM target architecture");
    }
}

pub inline fn lenToHost(value: usize) HostSize {
    return @intCast(value);
}

pub inline fn hostToLen(value: HostSize) usize {
    return @intCast(value);
}
