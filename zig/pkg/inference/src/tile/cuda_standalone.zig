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

//! Offline CUDA programs: operation, storage and fixed schedule are authoritative.
//! Historical exported symbols remain ABI adapters. This catalog is consumed by
//! host launch owners and exported to the offline producers by tile codegen.
const std = @import("std");
pub const Profile = struct {
    family: []const u8,
    file: []const u8,
    entry: [:0]const u8,
    producer: []const u8,
    exports: []const []const u8 = &.{},
    target: u16 = 121,
    threads: u32 = 0,
    shared: u32 = 0,
    hidden: u32 = 0,
    wide: u32 = 0,
    bf16: bool = false,
};
pub const attention: Profile = .{ .family = "attention_inputs", .file = "attention_inputs_partial_mrope_f32.ptx", .entry = "lighton_qk_norm_partial_mrope_gate", .producer = "attention_inputs_partial_mrope_f32.cu", .target = 75 };
pub const encoder: Profile = .{ .family = "encoder_block", .file = "encoder_block_phases_bf16.ptx", .entry = "lighton_bf16_vision_norm", .producer = "encoder_block_phases_bf16.cu", .exports = &.{ encoder_entries.norm, encoder_entries.add, encoder_entries.gelu, encoder_entries.qkv_rope }, .target = 80 };
pub const encoder_entries = .{ .norm = "lighton_bf16_vision_norm", .add = "lighton_bf16_vision_add", .gelu = "lighton_bf16_vision_gelu", .qkv_rope = "lighton_bf16_vision_qkv_rope" };

fn projection(comptime is_gelu: bool, comptime hidden: u32, comptime bf16: bool) Profile {
    const wide = hidden * 4;
    return .{
        .family = if (is_gelu) "projection_gelu" else "projection_residual",
        .file = if (is_gelu) std.fmt.comptimePrint("projection_gelu_sm121_k{d}_n{d}_bias_{s}.ptx", .{ hidden, wide, if (bf16) "bf16" else "f32" }) else std.fmt.comptimePrint("projection_residual_sm121_k{d}_n{d}_io_{s}.ptx", .{ wide, hidden, if (bf16) "bf16" else "f32" }),
        .entry = if (is_gelu) "lighton_vision_up" else "lighton_vision_down",
        .producer = if (is_gelu) "projection_gelu_bf16.py" else "projection_residual_bf16.py",
        .threads = 128,
        .shared = if (is_gelu) 24576 else 49152,
        .hidden = hidden,
        .wide = wide,
        .bf16 = bf16,
    };
}
fn projections(comptime is_gelu: bool) [4]Profile {
    return .{ projection(is_gelu, 768, false), projection(is_gelu, 768, true), projection(is_gelu, 1024, false), projection(is_gelu, 1024, true) };
}
pub const gelu = projections(true);
pub const residual = projections(false);
pub const top1 = [_]Profile{
    .{ .family = "projection_top1_partial", .file = "projection_top1_bf16_k1024_n248320_partial.ptx", .entry = "lighton_head_top1_partial", .producer = "projection_top1_bf16.py", .hidden = 1024, .wide = 248320, .threads = 256, .shared = 32768 },
    .{ .family = "projection_top1_finish", .file = "projection_top1_bf16_k1024_n248320_finish.ptx", .entry = "lighton_head_top1_finish", .producer = "projection_top1_bf16.py", .threads = 128, .shared = 16 },
};
pub const profiles = [_]Profile{ attention, encoder } ++ gelu ++ residual ++ top1;
