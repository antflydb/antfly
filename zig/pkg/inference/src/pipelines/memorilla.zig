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

//! Literal learned-slot readout with split Q/K/V weights. All activations use
//! the selected ComputeBackend. Tracing is opt-in and explicitly owns readback.
const std = @import("std");
const ops = @import("../ops/ops.zig");
const model = @import("../models/memorilla.zig");
const CT = ops.CT;
const A = std.mem.Allocator;
const CB = ops.ComputeBackend;

pub const Input = struct {
    documents: CT,
    question: CT,
    /// True means excluded, matching the upstream PyTorch padding polarity.
    padding_mask: []const bool,
    batch: usize,
    num_documents: usize,
    max_document_rows: usize = 4096,
};

pub const Trace = struct {
    context: *anyopaque,
    record: *const fn (*anyopaque, *const CB, []const u8, CT) anyerror!void,
};

fn record(trace: ?Trace, cb: *const CB, name: []const u8, tensor: CT) !void {
    if (trace) |sink| try sink.record(sink.context, cb, name, tensor);
}

fn norm(cb: *const CB, prefix: []const u8, input: CT, width: usize) !CT {
    var buf: [160]u8 = undefined;
    const weight = try cb.getWeight(try std.fmt.bufPrint(&buf, "{s}.weight", .{prefix}));
    defer cb.free(weight);
    const bias = try cb.getWeight(try std.fmt.bufPrint(&buf, "{s}.bias", .{prefix}));
    defer cb.free(bias);
    return cb.layerNorm(input, weight, bias, width, 1e-5);
}

fn linear(cb: *const CB, prefix: []const u8, input: CT, rows: usize, in_dim: usize, out_dim: usize) !CT {
    var buf: [160]u8 = undefined;
    const weight = try cb.getWeight(try std.fmt.bufPrint(&buf, "{s}.weight", .{prefix}));
    defer cb.free(weight);
    const bias = try cb.getWeight(try std.fmt.bufPrint(&buf, "{s}.bias", .{prefix}));
    defer cb.free(bias);
    return cb.linear(input, weight, bias, rows, in_dim, out_dim);
}

fn attention(cb: *const CB, cfg: model.Config, prefix: []const u8, queries: CT, keys: CT, mask: []const i64, batch: usize, query_count: usize, key_count: usize) !CT {
    const d = cfg.embedding_dim;
    var buf: [160]u8 = undefined;
    const q = try linear(cb, try std.fmt.bufPrint(&buf, "{s}.q_proj", .{prefix}), queries, batch * query_count, d, d);
    defer cb.free(q);
    const k = try linear(cb, try std.fmt.bufPrint(&buf, "{s}.k_proj", .{prefix}), keys, batch * key_count, d, d);
    defer cb.free(k);
    const v = try linear(cb, try std.fmt.bufPrint(&buf, "{s}.v_proj", .{prefix}), keys, batch * key_count, d, d);
    defer cb.free(v);
    const weighted = try cb.crossAttention(q, k, v, mask, batch, query_count, key_count, cfg.num_heads, d / cfg.num_heads);
    defer cb.free(weighted);
    return linear(cb, try std.fmt.bufPrint(&buf, "{s}.out_proj", .{prefix}), weighted, batch * query_count, d, d);
}

fn validateElements(cb: *const CB, a: A, value: CT, expected: usize) !void {
    const shape = try cb.tensorShape(value, a);
    defer a.free(shape);
    var count: usize = 1;
    for (shape) |dim| {
        if (dim < 0) return error.InvalidMemorillaInput;
        count = std.math.mul(usize, count, @intCast(dim)) catch return error.InvalidMemorillaInput;
    }
    if (count != expected or try cb.tensorDType(value) != .f32) return error.InvalidMemorillaInput;
}

/// Returns owned [batch * slots, decoder_width] F32 input embedding rows.
/// Caller retains documents/question and frees the output on the same backend.
/// This profile deliberately does not claim BF16 intermediate rounding parity.
pub fn forward(cb: *const CB, a: A, cfg: model.Config, input: Input, trace: ?Trace) !CT {
    try cfg.validate(false);
    try cb.checkExecutionControl();
    if (input.batch == 0 or input.batch > 64 or input.num_documents == 0) return error.EmptyMemorillaCollection;
    const document_rows = std.math.mul(usize, input.batch, input.num_documents) catch return error.InvalidMemorillaInput;
    if (input.max_document_rows == 0 or document_rows > input.max_document_rows) return error.MemorillaCollectionBudgetExceeded;
    const slot_rows = std.math.mul(usize, input.batch, cfg.num_memories) catch return error.InvalidMemorillaInput;
    const d = cfg.embedding_dim;
    try validateElements(cb, a, input.documents, try std.math.mul(usize, document_rows, d));
    try validateElements(cb, a, input.question, try std.math.mul(usize, input.batch, d));
    if (input.padding_mask.len != document_rows) return error.InvalidMemorillaInput;
    const mask = try a.alloc(i64, document_rows);
    defer a.free(mask);
    for (0..input.batch) |batch| {
        var visible: usize = 0;
        for (0..input.num_documents) |i| {
            const index = batch * input.num_documents + i;
            mask[index] = if (input.padding_mask[index]) 0 else 1;
            visible += @intFromBool(!input.padding_mask[index]);
        }
        if (visible == 0) return error.EmptyMemorillaCollection;
    }
    // These convenience operations otherwise have a host fallback. Requiring
    // their implementations keeps an explicitly selected GPU readout on device.
    if (cb.vtable.sliceRows2D == null or cb.vtable.concatRows2D == null or cb.vtable.geluExact == null)
        return error.UnsupportedMemorillaBackend;
    const learned = try cb.getWeight("memory_queries");
    defer cb.free(learned);
    var slots = try cb.sliceRows2D(a, learned, 0, cfg.num_memories, d);
    defer cb.free(slots);
    for (1..input.batch) |batch| {
        const combined = try cb.concatRows2D(a, slots, learned, batch * cfg.num_memories, cfg.num_memories, d);
        cb.free(slots);
        slots = combined;
    }
    try record(trace, cb, "slots.initial", slots);
    const question_mask = try a.alloc(i64, input.batch);
    defer a.free(question_mask);
    @memset(question_mask, 1);
    {
        const conditioned = try attention(cb, cfg, "query_cond_attn", slots, input.question, question_mask, input.batch, cfg.num_memories, 1);
        defer cb.free(conditioned);
        try record(trace, cb, "question.attended", conditioned);
        const sum = try cb.add(slots, conditioned);
        defer cb.free(sum);
        const next = try norm(cb, "query_cond_norm", sum, d);
        cb.free(slots);
        slots = next;
    }
    try record(trace, cb, "slots.conditioned", slots);
    const documents = try norm(cb, "doc_norm", input.documents, d);
    defer cb.free(documents);
    try record(trace, cb, "documents.normalized", documents);
    for (0..cfg.num_cross_attn_layers) |i| {
        try cb.checkExecutionControl();
        var buf: [160]u8 = undefined;
        const normalized = try norm(cb, try std.fmt.bufPrint(&buf, "cross_attn_norms.{d}", .{i}), slots, d);
        defer cb.free(normalized);
        try record(trace, cb, try std.fmt.bufPrint(&buf, "block{d}.cross.normalized", .{i}), normalized);
        const attended = try attention(cb, cfg, try std.fmt.bufPrint(&buf, "cross_attn_layers.{d}", .{i}), normalized, documents, mask, input.batch, cfg.num_memories, input.num_documents);
        defer cb.free(attended);
        try record(trace, cb, try std.fmt.bufPrint(&buf, "block{d}.cross.attended", .{i}), attended);
        const residual = try cb.add(slots, attended);
        cb.free(slots);
        slots = residual;
        try record(trace, cb, try std.fmt.bufPrint(&buf, "block{d}.slots.after_attention", .{i}), slots);
        const ffn_normalized = try norm(cb, try std.fmt.bufPrint(&buf, "ffn_norms.{d}", .{i}), slots, d);
        defer cb.free(ffn_normalized);
        try record(trace, cb, try std.fmt.bufPrint(&buf, "block{d}.ffn.normalized", .{i}), ffn_normalized);
        const hidden = try linear(cb, try std.fmt.bufPrint(&buf, "ffn_layers.{d}.0", .{i}), ffn_normalized, slot_rows, d, 4 * d);
        defer cb.free(hidden);
        try record(trace, cb, try std.fmt.bufPrint(&buf, "block{d}.ffn.hidden", .{i}), hidden);
        const activated = (try cb.geluExact(hidden)) orelse return error.UnsupportedMemorillaBackend;
        defer cb.free(activated);
        try record(trace, cb, try std.fmt.bufPrint(&buf, "block{d}.ffn.activated", .{i}), activated);
        const ffn = try linear(cb, try std.fmt.bufPrint(&buf, "ffn_layers.{d}.3", .{i}), activated, slot_rows, 4 * d, d);
        defer cb.free(ffn);
        try record(trace, cb, try std.fmt.bufPrint(&buf, "block{d}.ffn.output", .{i}), ffn);
        const result = try cb.add(slots, ffn);
        cb.free(slots);
        slots = result;
        try record(trace, cb, try std.fmt.bufPrint(&buf, "block{d}.slots.output", .{i}), slots);
    }
    const normalized = try norm(cb, "layer_norm", slots, d);
    defer cb.free(normalized);
    try record(trace, cb, "slots.final_normalized", normalized);
    const output = try linear(cb, "memory_projection", normalized, slot_rows, d, cfg.output_dim);
    errdefer cb.free(output);
    try record(trace, cb, "memory_tokens", output);
    try cb.checkExecutionControl();
    return output;
}
