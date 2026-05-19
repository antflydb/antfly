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

// Shared LoRA gradient accumulation helper used by real-forward trainers.
//
// Bridges the layout mismatch between `LoRALayer` (native: A=[rank,in_f],
// B=[out_f,rank]) and `lora.accumulateLinearLoRAGrads` (expects A=[in_f,rank],
// B=[rank,out_f]) by transposing forward, accumulating into scratch buffers,
// and transposing back. Extracted to keep reranker_real_forward and
// colqwen2_real_forward from carrying verbatim copies of the same routine.

const std = @import("std");
const lora = @import("lora.zig");
const fused_chunker_lora = @import("lora_adapter_set.zig");

pub const LoRALayer = fused_chunker_lora.LoRALayer;
pub const LoRAAdapterSet = fused_chunker_lora.LoRAAdapterSet;

/// Accumulate LoRA grads on one (layer, module) pair. The pre-layer hidden
/// state `inputs` is [input_rows, in_features], and `output_grads` is
/// [input_rows, out_features]. Gradients are added into `layer.grad_A` and
/// `layer.grad_B` in the layer's native layout.
pub fn accumulateForLayer(
    allocator: std.mem.Allocator,
    layer: *LoRALayer,
    adapter_set: *LoRAAdapterSet,
    inputs: []const f32,
    output_grads: []const f32,
    input_rows: usize,
) !void {
    const in_f = layer.in_features;
    const out_f = layer.out_features;
    const rank = layer.A.len / in_f;
    if (rank == 0) return;

    const a_cpu = try allocator.alloc(f32, in_f * rank);
    defer allocator.free(a_cpu);
    const b_cpu = try allocator.alloc(f32, rank * out_f);
    defer allocator.free(b_cpu);

    for (0..rank) |r| {
        for (0..in_f) |i| {
            a_cpu[i * rank + r] = layer.A[r * in_f + i];
        }
    }
    for (0..out_f) |o| {
        for (0..rank) |r| {
            b_cpu[r * out_f + o] = layer.B[o * rank + r];
        }
    }

    const grad_a_cpu = try allocator.alloc(f32, in_f * rank);
    defer allocator.free(grad_a_cpu);
    const grad_b_cpu = try allocator.alloc(f32, rank * out_f);
    defer allocator.free(grad_b_cpu);
    @memset(grad_a_cpu, 0);
    @memset(grad_b_cpu, 0);

    const a_mat: lora.Matrix = .{ .rows = in_f, .cols = rank, .data = a_cpu };
    const b_mat: lora.Matrix = .{ .rows = rank, .cols = out_f, .data = b_cpu };

    const scale = lora.effectiveScaleMode(
        adapter_set.config.alpha,
        @intCast(adapter_set.config.rank),
        adapter_set.config.scaling,
    );
    // accumulateLinearLoRAGrads re-divides by rank internally, so rebuild the
    // effective alpha from our chosen ScalingMode.
    const effective_alpha = scale * @as(f32, @floatFromInt(adapter_set.config.rank));

    lora.accumulateLinearLoRAGrads(
        grad_a_cpu,
        grad_b_cpu,
        input_rows,
        in_f,
        inputs,
        out_f,
        output_grads,
        a_mat,
        b_mat,
        effective_alpha,
    );

    for (0..in_f) |i| {
        for (0..rank) |r| {
            layer.grad_A[r * in_f + i] += grad_a_cpu[i * rank + r];
        }
    }
    for (0..rank) |r| {
        for (0..out_f) |o| {
            layer.grad_B[o * rank + r] += grad_b_cpu[r * out_f + o];
        }
    }
}

/// Iterate a list of candidate module names and call `accumulateForLayer` for
/// each one that exists on `adapter_set` at the given layer index. Missing
/// modules are silently skipped.
pub fn accumulateLastLayerCandidates(
    allocator: std.mem.Allocator,
    adapter_set: *LoRAAdapterSet,
    layer_idx: u32,
    candidate_modules: []const []const u8,
    inputs: []const f32,
    output_grads: []const f32,
    input_rows: usize,
) !void {
    for (candidate_modules) |mod_name| {
        const layer = adapter_set.get(layer_idx, mod_name) orelse continue;
        try accumulateForLayer(allocator, layer, adapter_set, inputs, output_grads, input_rows);
    }
}

test "accumulateForLayer transposes grads correctly for a trivial layer" {
    const alloc = std.testing.allocator;

    const cfg: fused_chunker_lora.LoRAConfig = .{
        .rank = 2,
        .alpha = 4.0,
        .target_modules = &.{"query_proj"},
        .num_layers = 1,
    };
    var adapter_set = try LoRAAdapterSet.init(alloc, cfg, 4, 16);
    defer adapter_set.deinit();

    const layer = adapter_set.get(0, "query_proj").?;
    // Write a known A/B pattern (small values).
    for (layer.A, 0..) |*v, i| v.* = 0.01 * @as(f32, @floatFromInt(i + 1));
    for (layer.B, 0..) |*v, i| v.* = 0.02 * @as(f32, @floatFromInt(i + 1));
    @memset(layer.grad_A, 0);
    @memset(layer.grad_B, 0);

    // One sample, in=4, out=4 — hidden layer.in_features == 4.
    const inputs = [_]f32{ 1.0, 0.5, -0.25, 0.75 };
    const output_grads = [_]f32{ 0.1, -0.2, 0.3, -0.4 };

    try accumulateForLayer(alloc, layer, &adapter_set, &inputs, &output_grads, 1);

    var any_a: bool = false;
    var any_b: bool = false;
    for (layer.grad_A) |v| if (v != 0) {
        any_a = true;
    };
    for (layer.grad_B) |v| if (v != 0) {
        any_b = true;
    };
    try std.testing.expect(any_a);
    try std.testing.expect(any_b);
}
