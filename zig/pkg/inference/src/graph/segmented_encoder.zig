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

// Segmented backpropagation through the encoder for LoRA gradient computation.
//
// During training the boundary head is differentiated through autodiff on the
// graph IR.  To also update the LoRA adapters inside the encoder we need
// gradients with respect to the encoder's linear projections.
//
// Approach:
//   1. During the encoder forward pass, capture the *input* to each targeted
//      linear projection (query_proj, value_proj, etc.) as a LayerActivation.
//   2. After the boundary-head autodiff step produces dL/d(encoder_output),
//      call backwardLoRA to distribute that gradient signal back through every
//      captured activation and accumulate grad_A / grad_B in each LoRALayer.
//
// This is a first-order approximation: the same output gradient is broadcast
// to all captured (layer, module) pairs rather than being chained through all
// intervening computations.  The approximation is cheap and works well with
// low-rank adapters whose contribution is small relative to the base weights.

const std = @import("std");
const lora = @import("../finetune/lora.zig");
const fused_chunker_lora = @import("../finetune/lora_adapter_set.zig");
const ops = @import("../ops/ops.zig");

// ----------------------------------------------------------------------------
// LayerActivation
// ----------------------------------------------------------------------------

/// The input tensor captured at a single linear projection during a forward
/// pass.  Used as the left-hand side when computing LoRA gradients.
pub const LayerActivation = struct {
    layer_idx: u32,
    /// Module name, e.g. "query_proj" or "value_proj".  Not owned — points
    /// into a string literal or the LoRALayer's module_name buffer.
    module_name: []const u8,
    /// Owned flat buffer: [seq_len * in_features] in row-major order.
    input: []f32,
    in_features: usize,
    out_features: usize,
    seq_len: usize,

    pub fn deinit(self: *LayerActivation, allocator: std.mem.Allocator) void {
        allocator.free(self.input);
        self.* = undefined;
    }
};

// ----------------------------------------------------------------------------
// EncoderActivations
// ----------------------------------------------------------------------------

/// A collection of LayerActivation records captured during one encoder forward
/// pass — one entry per targeted (layer_idx, module_name) pair.
pub const EncoderActivations = struct {
    allocator: std.mem.Allocator,
    /// Owned slice grown via addActivation.
    activations: std.ArrayListUnmanaged(LayerActivation),
    seq_len: usize,
    hidden_size: usize,

    pub fn init(
        allocator: std.mem.Allocator,
        capacity: usize,
        seq_len: usize,
        hidden_size: usize,
    ) !EncoderActivations {
        var list = std.ArrayListUnmanaged(LayerActivation).empty;
        try list.ensureTotalCapacity(allocator, capacity);
        return .{
            .allocator = allocator,
            .activations = list,
            .seq_len = seq_len,
            .hidden_size = hidden_size,
        };
    }

    pub fn deinit(self: *EncoderActivations) void {
        for (self.activations.items) |*act| {
            act.deinit(self.allocator);
        }
        self.activations.deinit(self.allocator);
        self.* = undefined;
    }

    /// Append an activation record, copying the input slice.
    ///
    /// `input` must have length `seq_len * in_features`; the data is copied
    /// into an owned buffer so the caller may safely reuse its memory.
    pub fn addActivation(
        self: *EncoderActivations,
        layer_idx: u32,
        module_name: []const u8,
        input: []const f32,
        in_features: usize,
        out_features: usize,
    ) !void {
        const owned_input = try self.allocator.dupe(f32, input);
        errdefer self.allocator.free(owned_input);

        try self.activations.append(self.allocator, .{
            .layer_idx = layer_idx,
            .module_name = module_name,
            .input = owned_input,
            .in_features = in_features,
            .out_features = out_features,
            .seq_len = self.seq_len,
        });
    }
};

// ----------------------------------------------------------------------------
// backwardLoRA
// ----------------------------------------------------------------------------

/// Accumulate LoRA gradients for all captured activations given the gradient
/// of the loss with respect to the final encoder output.
///
/// Parameters
/// ----------
/// allocator   — scratch allocator forwarded to lora.accumulateLinearLoRAGrads
/// activations — captured inputs from the encoder forward pass
/// d_output    — [seq_len * hidden_size] — dL/d(encoder_final_output)
/// lora_layers — mutable slice of LoRALayer; grad_A and grad_B are accumulated
/// alpha       — LoRA scaling factor (usually LoRAConfig.alpha)
///
/// For each activation, the function looks up the matching LoRALayer by
/// (layer_idx, module_name) and calls accumulateLayerLoRAGrads.  Activations
/// whose corresponding LoRALayer cannot be found are silently skipped.
pub fn backwardLoRA(
    allocator: std.mem.Allocator,
    activations: *const EncoderActivations,
    d_output: []const f32,
    lora_layers: []fused_chunker_lora.LoRALayer,
    alpha: f32,
) !void {
    for (activations.activations.items) |*act| {
        // Find the LoRALayer matching this (layer_idx, module_name) pair.
        const matched_layer: ?*fused_chunker_lora.LoRALayer = blk: {
            for (lora_layers) |*ll| {
                if (ll.layer_idx == act.layer_idx and
                    std.mem.eql(u8, ll.module_name, act.module_name))
                {
                    break :blk ll;
                }
            }
            break :blk null;
        };

        const lora_layer = matched_layer orelse continue;

        // Build an output-gradient slice of the right length for this projection.
        // Because we are using the encoder's final output gradient as an approximation
        // we need a buffer of [seq_len * out_features].  When out_features equals
        // hidden_size (the common case for query/value projections) d_output can be
        // used directly.  Otherwise we allocate a zero-padded / truncated view.
        const needed = act.seq_len * act.out_features;
        const out_grad: []const f32 = if (d_output.len == needed) blk: {
            break :blk d_output;
        } else blk: {
            const buf = try allocator.alloc(f32, needed);
            errdefer allocator.free(buf);
            const copy_len = @min(d_output.len, needed);
            @memcpy(buf[0..copy_len], d_output[0..copy_len]);
            if (copy_len < needed) @memset(buf[copy_len..], 0);
            break :blk buf;
        };
        defer if (out_grad.ptr != d_output.ptr) allocator.free(@constCast(out_grad));

        try accumulateLayerLoRAGrads(allocator, act, out_grad, lora_layer, alpha);
    }
}

// ----------------------------------------------------------------------------
// accumulateLayerLoRAGrads  (private helper)
// ----------------------------------------------------------------------------

/// Accumulate grad_A and grad_B for one (activation, LoRALayer) pair.
///
/// Delegates to lora.accumulateLinearLoRAGrads which expects:
///   grad_a         [rank * in_features]
///   grad_b         [out_features * rank]
///   input_rows     seq_len
///   input_cols     in_features
///   inputs         [seq_len * in_features]
///   output_cols    out_features
///   output_grads   [seq_len * out_features]
///   adapter_a      Matrix — A: [rank, in_features]
///   adapter_b      Matrix — B: [out_features, rank]
///   alpha          f32
fn accumulateLayerLoRAGrads(
    allocator: std.mem.Allocator,
    act: *const LayerActivation,
    output_grad: []const f32,
    lora_layer: *fused_chunker_lora.LoRALayer,
    alpha: f32,
) !void {
    const rank = lora_layer.A.len / lora_layer.in_features;
    if (rank == 0) return;

    const adapter_a = try allocator.alloc(f32, lora_layer.in_features * rank);
    defer allocator.free(adapter_a);
    const adapter_b = try allocator.alloc(f32, rank * lora_layer.out_features);
    defer allocator.free(adapter_b);
    const grad_a = try allocator.alloc(f32, lora_layer.in_features * rank);
    defer allocator.free(grad_a);
    const grad_b = try allocator.alloc(f32, rank * lora_layer.out_features);
    defer allocator.free(grad_b);

    for (0..rank) |r| {
        for (0..lora_layer.in_features) |i| {
            adapter_a[i * rank + r] = lora_layer.A[r * lora_layer.in_features + i];
        }
    }
    for (0..lora_layer.out_features) |o| {
        for (0..rank) |r| {
            adapter_b[r * lora_layer.out_features + o] = lora_layer.B[o * rank + r];
        }
    }
    @memset(grad_a, 0);
    @memset(grad_b, 0);

    lora.accumulateLinearLoRAGrads(
        grad_a,
        grad_b,
        act.seq_len, // input_rows
        act.in_features,
        act.input, // inputs
        act.out_features,
        output_grad, // output_grads
        .{ .rows = lora_layer.in_features, .cols = rank, .data = adapter_a },
        .{ .rows = rank, .cols = lora_layer.out_features, .data = adapter_b },
        alpha,
    );

    for (0..lora_layer.in_features) |i| {
        for (0..rank) |r| {
            lora_layer.grad_A[r * lora_layer.in_features + i] += grad_a[i * rank + r];
        }
    }
    for (0..rank) |r| {
        for (0..lora_layer.out_features) |o| {
            lora_layer.grad_B[o * rank + r] += grad_b[r * lora_layer.out_features + o];
        }
    }
}

// ----------------------------------------------------------------------------
// backwardLoRADirect
// ----------------------------------------------------------------------------

/// Apply LoRA backprop using captures from modern_bert.ActivationBuffer.
///
/// Accepts raw slices extracted from an ActivationBuffer rather than importing
/// modern_bert.zig (to avoid circular dependencies).
///
/// cap_layer_indices: []const u32    — layer_idx per capture
/// cap_module_names:  []const []const u8  — module_name per capture
/// cap_inputs:        []const []const f32 — input[i] is [total * in_features] for capture i
/// cap_in_features:   []const usize
/// cap_out_features:  []const usize
/// d_output:          []const f32    — dL/d(encoder_output), [total * hidden_size]
/// lora_layers:       []LoRALayer    — grad_A/grad_B accumulated in-place
/// alpha:             f32
pub fn backwardLoRADirect(
    cb: ops.ComputeBackend,
    allocator: std.mem.Allocator,
    cap_layer_indices: []const u32,
    cap_module_names: []const []const u8,
    cap_inputs: []const []const f32,
    cap_in_features: []const usize,
    cap_out_features: []const usize,
    d_output: []const f32,
    lora_layers: []fused_chunker_lora.LoRALayer,
    alpha: f32,
) !void {
    const n = cap_layer_indices.len;
    std.debug.assert(cap_module_names.len == n);
    std.debug.assert(cap_inputs.len == n);
    std.debug.assert(cap_in_features.len == n);
    std.debug.assert(cap_out_features.len == n);

    for (0..n) |i| {
        const layer_idx = cap_layer_indices[i];
        const module_name = cap_module_names[i];
        const input = cap_inputs[i];
        const in_features = cap_in_features[i];
        const out_features = cap_out_features[i];

        // Find the LoRALayer matching this (layer_idx, module_name) pair.
        const matched_layer: ?*fused_chunker_lora.LoRALayer = blk: {
            for (lora_layers) |*ll| {
                if (ll.layer_idx == layer_idx and
                    std.mem.eql(u8, ll.module_name, module_name))
                {
                    break :blk ll;
                }
            }
            break :blk null;
        };

        const lora_layer = matched_layer orelse continue;

        // Derive seq_len from the input buffer length and in_features.
        const seq_len = if (in_features > 0) input.len / in_features else 0;

        // Build an output-gradient slice of [seq_len * out_features].
        const needed = seq_len * out_features;
        const out_grad: []const f32 = if (d_output.len == needed) blk: {
            break :blk d_output;
        } else blk: {
            const buf = try allocator.alloc(f32, needed);
            errdefer allocator.free(buf);
            const copy_len = @min(d_output.len, needed);
            @memcpy(buf[0..copy_len], d_output[0..copy_len]);
            if (copy_len < needed) @memset(buf[copy_len..], 0);
            break :blk buf;
        };
        defer if (out_grad.ptr != d_output.ptr) allocator.free(@constCast(out_grad));

        // Try the GPU-accelerated path first; fall back to the CPU path if the
        // backend does not provide it.
        const rank = if (in_features > 0) lora_layer.A.len / in_features else 0;
        const scale = if (rank > 0) alpha / @as(f32, @floatFromInt(rank)) else 0.0;
        const gpu_used = try cb.accumulateLoRAGrads(
            allocator,
            lora_layer.grad_A,
            lora_layer.grad_B,
            input,
            out_grad,
            lora_layer.A,
            lora_layer.B,
            seq_len,
            in_features,
            out_features,
            rank,
            scale,
        );
        if (!gpu_used) {
            // Build a synthetic LayerActivation from the raw slices so we can
            // reuse accumulateLayerLoRAGrads without duplicating BLAS logic.
            // The input slice is borrowed (not owned); we do not free it here.
            const act = LayerActivation{
                .layer_idx = layer_idx,
                .module_name = module_name,
                .input = @constCast(input),
                .in_features = in_features,
                .out_features = out_features,
                .seq_len = seq_len,
            };
            try accumulateLayerLoRAGrads(allocator, &act, out_grad, lora_layer, alpha);
        }
    }
}

// ----------------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------------

test "EncoderActivations init and deinit" {
    const allocator = std.testing.allocator;

    var acts = try EncoderActivations.init(allocator, 4, 8, 16);
    defer acts.deinit();

    try std.testing.expectEqual(@as(usize, 8), acts.seq_len);
    try std.testing.expectEqual(@as(usize, 16), acts.hidden_size);
    try std.testing.expectEqual(@as(usize, 0), acts.activations.items.len);
}

test "EncoderActivations addActivation copies data" {
    const allocator = std.testing.allocator;

    const seq_len: usize = 4;
    const in_features: usize = 8;

    var acts = try EncoderActivations.init(allocator, 2, seq_len, in_features);
    defer acts.deinit();

    // Build an input buffer with a known pattern.
    var input_buf: [4 * 8]f32 = undefined;
    for (&input_buf, 0..) |*v, i| v.* = @as(f32, @floatFromInt(i)) * 0.1;

    try acts.addActivation(0, "query_proj", &input_buf, in_features, in_features);

    try std.testing.expectEqual(@as(usize, 1), acts.activations.items.len);
    const act = &acts.activations.items[0];
    try std.testing.expectEqual(@as(u32, 0), act.layer_idx);
    try std.testing.expectEqualStrings("query_proj", act.module_name);
    try std.testing.expectEqual(@as(usize, seq_len * in_features), act.input.len);

    // Mutate the original buffer — the stored copy must be unaffected.
    input_buf[0] = 9999.0;
    try std.testing.expect(act.input[0] != 9999.0);
}

test "backwardLoRA smoke — grad_A and grad_B become non-zero" {
    const allocator = std.testing.allocator;

    const seq_len: usize = 3;
    const hidden_size: usize = 8;
    const rank: u32 = 2;
    const alpha: f32 = 4.0;

    // Create one activation with a constant input pattern.
    var acts = try EncoderActivations.init(allocator, 1, seq_len, hidden_size);
    defer acts.deinit();

    var input_data: [3 * 8]f32 = undefined;
    for (&input_data, 0..) |*v, i| v.* = @as(f32, @floatFromInt(i + 1)) * 0.1;

    try acts.addActivation(0, "query_proj", &input_data, hidden_size, hidden_size);

    // Create one LoRALayer matching (layer_idx=0, module_name="query_proj").
    // Use a fixed-length stack array so backwardLoRA can take a mutable slice of it.
    var lora_layers: [1]fused_chunker_lora.LoRALayer = undefined;
    lora_layers[0] = try fused_chunker_lora.LoRALayer.init(
        allocator,
        0,
        "query_proj",
        hidden_size,
        hidden_size,
        rank,
    );
    defer lora_layers[0].deinit();
    for (lora_layers[0].B, 0..) |*v, i| {
        v.* = @as(f32, @floatFromInt(i + 1)) * 0.001;
    }

    // Ensure grad accumulators start at zero.
    lora_layers[0].zeroGrads();

    // Build a non-zero output gradient [seq_len * hidden_size].
    var d_output: [3 * 8]f32 = undefined;
    for (&d_output, 0..) |*v, i| v.* = @as(f32, @floatFromInt(i + 1)) * 0.05;

    try backwardLoRA(allocator, &acts, &d_output, &lora_layers, alpha);

    // After backward, at least one element in grad_A and grad_B must be non-zero.
    var any_nonzero_a = false;
    for (lora_layers[0].grad_A) |v| {
        if (v != 0.0) {
            any_nonzero_a = true;
            break;
        }
    }
    var any_nonzero_b = false;
    for (lora_layers[0].grad_B) |v| {
        if (v != 0.0) {
            any_nonzero_b = true;
            break;
        }
    }

    try std.testing.expect(any_nonzero_a);
    try std.testing.expect(any_nonzero_b);
}

test "backwardLoRA skips unmatched activations" {
    const allocator = std.testing.allocator;

    const seq_len: usize = 2;
    const hidden_size: usize = 4;

    // Activation targets layer 0, "query_proj".
    var acts = try EncoderActivations.init(allocator, 1, seq_len, hidden_size);
    defer acts.deinit();

    var input_data: [2 * 4]f32 = undefined;
    @memset(&input_data, 1.0);
    try acts.addActivation(0, "query_proj", &input_data, hidden_size, hidden_size);

    // LoRALayer targets layer 1, "value_proj" — no match with the activation above.
    var lora_layers: [1]fused_chunker_lora.LoRALayer = undefined;
    lora_layers[0] = try fused_chunker_lora.LoRALayer.init(
        allocator,
        1, // different layer_idx
        "value_proj", // different module_name
        hidden_size,
        hidden_size,
        2,
    );
    defer lora_layers[0].deinit();
    lora_layers[0].zeroGrads();

    var d_output: [2 * 4]f32 = undefined;
    @memset(&d_output, 0.5);

    try backwardLoRA(allocator, &acts, &d_output, &lora_layers, 4.0);

    // Grads must remain zero because there was no matching layer.
    for (lora_layers[0].grad_A) |v| try std.testing.expectEqual(@as(f32, 0.0), v);
    for (lora_layers[0].grad_B) |v| try std.testing.expectEqual(@as(f32, 0.0), v);
}
