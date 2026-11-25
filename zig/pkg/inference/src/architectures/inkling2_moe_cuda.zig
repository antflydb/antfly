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

//! Resident CUDA expert weights and the fixed-loop decoder's MoE sublayer.
const std = @import("std");
const checkpoint = @import("inkling2_checkpoint.zig");
const cuda = @import("../ops/cuda/routed_block_scaled_projection.zig");
const cublas = @import("../ops/cuda/cublaslt.zig");
const staging_mod = @import("../ops/cuda/pinned_input_arena.zig");
const Context = @import("../ops/cuda/context.zig").CudaContext;
const buffer = @import("../ops/cuda/buffer.zig");
const DeviceBuffer = buffer.DeviceBuffer;
pub const ActivationPrecision = cuda.ActivationPrecision;

fn freeWeights(ctx: *Context, weights: *cuda.PackedWeights) void {
    weights.denominator.free(ctx);
    weights.amax.free(ctx);
    weights.scales.free(ctx);
    weights.values.free(ctx);
}

fn uploadWeights(ctx: *Context, staging: *staging_mod.Arena, host: checkpoint.ExpertWeights) !cuda.PackedWeights {
    var weights: cuda.PackedWeights = .{ .values = .{}, .scales = .{} };
    errdefer freeWeights(ctx, &weights);
    weights.values = try staging.uploadWeight(ctx, host.weights);
    weights.scales = try staging.uploadWeight(ctx, host.block_scales);
    if (host.outer_amax) |bytes| weights.amax = try staging.uploadWeight(ctx, bytes);
    if (host.outer_denominator) |bytes| weights.denominator = try staging.uploadWeight(ctx, bytes);
    return weights;
}

pub const Layer = struct {
    router_weight: DeviceBuffer = .{},
    router_bias: DeviceBuffer = .{},
    gate_up: cuda.PackedWeights = .{ .values = .{}, .scales = .{} },
    down: cuda.PackedWeights = .{ .values = .{}, .scales = .{} },
    format: cuda.Format,

    /// The caller can reuse the staging arena across stored layers. GPU
    /// weight ownership is returned only after this layer's uploads finish.
    pub fn load(ctx: *Context, source: *const checkpoint.Checkpoint, staging: *staging_mod.Arena, layer: usize) !Layer {
        const router = try source.router(layer);
        const gate_up = try source.experts(layer, .w13);
        const down = try source.experts(layer, .w2);
        var result: Layer = .{ .format = switch (gate_up.format) {
            .nvfp4 => .nvfp4,
            .mxfp8 => .mxfp8,
        } };
        errdefer result.deinit(ctx);
        result.router_weight = try staging.uploadWeight(ctx, router.weight);
        result.router_bias = try staging.uploadWeight(ctx, router.bias);
        result.gate_up = try uploadWeights(ctx, staging, gate_up);
        result.down = try uploadWeights(ctx, staging, down);
        try ctx.synchronize();
        return result;
    }

    pub fn deinit(self: *Layer, ctx: *Context) void {
        freeWeights(ctx, &self.down);
        freeWeights(ctx, &self.gate_up);
        self.router_bias.free(ctx);
        self.router_weight.free(ctx);
    }
};

pub const Scratch = struct {
    logits: DeviceBuffer,
    blas: DeviceBuffer,
    routed: DeviceBuffer,
    activation_precision: ActivationPrecision = .block_scaled,
    rowwise_router: bool = false,
};

pub const ScratchBytes = struct {
    logits: usize,
    routed: usize,
};

fn request(format: cuda.Format, text: checkpoint.TextConfig, rows: u32) cuda.FfnRequest {
    return .{
        .format = format,
        .input = .{},
        .logits = .{},
        .router_bias = .{},
        .gate_up = .{ .values = .{}, .scales = .{} },
        .down = .{ .values = .{}, .scales = .{} },
        .output = .{},
        .scratch = .{},
        .rows = rows,
        .hidden = text.hidden_size,
        .intermediate = text.intermediate_size,
        .experts = text.n_routed_experts,
        .topk = text.num_experts_per_tok,
        .route_scale = text.route_scale,
        .gate_cap = text.mlp_gate_softcap,
        .up_cap = text.mlp_up_softcap,
    };
}

pub fn scratchBytes(format: cuda.Format, text: checkpoint.TextConfig, rows: u32) !ScratchBytes {
    const routed = try cuda.ffnScratchBytes(request(format, text, rows));
    return .{ .logits = @as(usize, rows) * text.n_routed_experts * @sizeOf(f32), .routed = routed };
}

pub const Forward = struct {
    projections: cuda.Module = .{},

    pub fn prepareResident(self: *Forward, ctx: *Context, text: checkpoint.TextConfig, layer: *Layer) !void {
        try self.projections.interleaveWeights(ctx, &layer.gate_up, layer.format, text.hidden_size, @as(u32, text.intermediate_size) * 2, text.n_routed_experts);
        try self.projections.interleaveWeights(ctx, &layer.down, layer.format, text.intermediate_size, text.hidden_size, text.n_routed_experts);
    }

    pub fn deinit(self: *Forward, ctx: *Context) void {
        self.projections.deinit(ctx);
    }

    pub fn run(
        self: *Forward,
        ctx: *Context,
        blas: *cublas.CublasLt,
        text: checkpoint.TextConfig,
        layer: *const Layer,
        rows: u32,
        input: DeviceBuffer,
        output: DeviceBuffer,
        scratch: Scratch,
    ) !void {
        const amounts = try scratchBytes(layer.format, text, rows);
        if (scratch.logits.len < amounts.logits or scratch.routed.len < amounts.routed) return error.InvalidShape;
        var ffn = request(layer.format, text, rows);
        ffn.input = input;
        ffn.logits = scratch.logits;
        ffn.router_bias = layer.router_bias;
        ffn.gate_up = layer.gate_up;
        ffn.down = layer.down;
        ffn.output = output;
        ffn.scratch = scratch.routed;
        ffn.activation_precision = scratch.activation_precision;
        if (scratch.rowwise_router) {
            // The router's scores determine discrete expert selection. Keep
            // the same cuBLAS plan as a single request for each decode lane.
            const input_bytes = @as(usize, text.hidden_size) * 2;
            const logits_bytes = @as(usize, text.n_routed_experts) * @sizeOf(f32);
            for (0..rows) |lane| {
                const source: DeviceBuffer = .{ .ptr = input.ptr + lane * input_bytes, .len = input_bytes, .borrowed = true };
                const logits: DeviceBuffer = .{ .ptr = scratch.logits.ptr + lane * logits_bytes, .len = logits_bytes, .borrowed = true };
                try blas.matmulBf16WeightF32Out(ctx, logits, source, layer.router_weight, scratch.blas, 1, text.hidden_size, text.n_routed_experts);
            }
        } else try blas.matmulBf16WeightF32Out(ctx, scratch.logits, input, layer.router_weight, scratch.blas, rows, text.hidden_size, text.n_routed_experts);
        try self.projections.runFfn(ctx, ffn);
    }
};
