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

//! Checkpoint roles and qualified projection geometry for structured encoders.
//! Shared CUDA numerical programs consume the resolved physical operations.
const std = @import("std");
const weight_source_mod = @import("../models/weight_source.zig");
const types = @import("../gguf/tensor_types.zig");
fn knownQuantTensorType(value: types.TensorType) ?types.KnownTensorType {
    return switch (value) {
        .known => |known| known,
        else => null,
    };
}

pub fn ettinProjectionShape(in_dim: usize, out_dim: usize) bool {
    return (in_dim == 1792 and (out_dim == 5376 or out_dim == 1792 or out_dim == 7680)) or
        (in_dim == 3840 and out_dim == 1792);
}

pub fn ettinQ8EncoderWeight(enabled: bool, name: []const u8, loaded: *const weight_source_mod.LoadedWeight) bool {
    if (!enabled) return false;
    const storage = loaded.quantized_storage orelse return false;
    if (knownQuantTensorType(storage.tensor_type) != .Q8_0 or storage.shape.len != 2) return false;
    if (std.mem.eql(u8, name, "model.embeddings.tok_embeddings.weight"))
        return std.mem.eql(i64, storage.shape, &.{ 50378, 1792 });
    const prefix = "model.layers.";
    if (!std.mem.startsWith(u8, name, prefix)) return false;
    const suffix_start = std.mem.indexOfScalarPos(u8, name, prefix.len, '.') orelse return false;
    const layer = std.fmt.parseInt(u32, name[prefix.len..suffix_start], 10) catch return false;
    if (layer >= 28) return false;
    const suffix = name[suffix_start..];
    const shape: []const i64 = if (std.mem.eql(u8, suffix, ".attn.Wqkv.weight")) &.{ 5376, 1792 } else if (std.mem.eql(u8, suffix, ".attn.Wo.weight")) &.{ 1792, 1792 } else if (std.mem.eql(u8, suffix, ".mlp.Wi.weight")) &.{ 7680, 1792 } else if (std.mem.eql(u8, suffix, ".mlp.Wo.weight")) &.{ 1792, 3840 } else return false;
    return std.mem.eql(i64, storage.shape, shape);
}

pub fn ettinQ8ProjectionWeight(enabled: bool, name: []const u8, loaded: *const weight_source_mod.LoadedWeight) bool {
    return std.mem.startsWith(u8, name, "model.layers.") and ettinQ8EncoderWeight(enabled, name, loaded);
}

pub fn debertaQ8ProjectionWeight(enabled: bool, name: []const u8, loaded: *const weight_source_mod.LoadedWeight) bool {
    if (!enabled) return false;
    const storage = loaded.quantized_storage orelse return false;
    if (knownQuantTensorType(storage.tensor_type) != .Q8_0 or storage.shape.len != 2) return false;
    const prefix = "encoder.layer.";
    if (!std.mem.startsWith(u8, name, prefix)) return false;
    const suffix_start = std.mem.indexOfScalarPos(u8, name, prefix.len, '.') orelse return false;
    const layer = std.fmt.parseInt(u32, name[prefix.len..suffix_start], 10) catch return false;
    if (layer >= 24) return false;
    const suffix = name[suffix_start..];
    const shape: []const i64 = if (std.mem.eql(u8, suffix, ".intermediate.dense.weight")) &.{ 4096, 1024 } else if (std.mem.eql(u8, suffix, ".output.dense.weight")) &.{ 1024, 4096 } else if (std.mem.eql(u8, suffix, ".attention.self.query_proj.weight") or
        std.mem.eql(u8, suffix, ".attention.self.key_proj.weight") or
        std.mem.eql(u8, suffix, ".attention.self.value_proj.weight") or
        std.mem.eql(u8, suffix, ".attention.self.pos_query_proj.weight") or
        std.mem.eql(u8, suffix, ".attention.self.pos_key_proj.weight") or
        std.mem.eql(u8, suffix, ".attention.output.dense.weight")) &.{ 1024, 1024 } else return false;
    return std.mem.eql(i64, storage.shape, shape);
}

pub fn spanQ4ProjectionShape(rows: usize, in_dim: usize, out_dim: usize) bool {
    if (rows < 2) return false;
    if (in_dim == 768 and out_dim == 3072) return true;
    if (in_dim == 3072 and out_dim == 768) return true;
    if (in_dim == 1536 and out_dim == 3072) return true;
    return false;
}

/// Only retain FP16 tensors for operations with a fully device-resident FP16
/// implementation. GLiNER's DeBERTa token embedding, per-layer dense
/// projections, and span projection MLPs are consumed by FP16 tensor-core
/// GEMM routes. The relative position table and the CountLSTM/transformer head
/// still have F32-only consumers, so those exceptional tensors remain F32.
/// Keep this operation-capability policy explicit rather than shape-dependent:
/// silently promoting every two-dimensional head tensor can make an otherwise
/// supported model fail much later in an unrelated elementwise operation.
pub fn keepsF16Weight(name: []const u8) bool {
    if (std.mem.eql(u8, name, "embeddings.word_embeddings.weight")) return true;
    if (std.mem.startsWith(u8, name, "encoder.layer.") and std.mem.endsWith(u8, name, ".weight")) return true;
    return std.mem.startsWith(u8, name, "span_rep.span_rep_layer.") and
        std.mem.endsWith(u8, name, ".weight");
}

test "structured encoder binding preserves model namespaces and numerical eligibility" {
    const a = std.testing.allocator;
    var loaded = weight_source_mod.LoadedWeight{
        .tensor = .{ .data = &.{}, .dtype = .f32, .shape = &.{}, .name = "", .allocator = a, .owns_data = false, .owns_shape = false },
        .quantized = true,
        .quantized_storage = .{ .tensor_type = .{ .known = .Q8_0 }, .raw_bytes = &.{}, .shape = &.{ 5376, 1792 }, .allocator = a },
    };
    try std.testing.expect(ettinQ8ProjectionWeight(true, "model.layers.27.attn.Wqkv.weight", &loaded));
    try std.testing.expect(!ettinQ8ProjectionWeight(true, "model.layers.28.attn.Wqkv.weight", &loaded));
    try std.testing.expect(!ettinQ8ProjectionWeight(true, "classifier.weight", &loaded));
    loaded.quantized_storage.?.shape = &.{ 1024, 4096 };
    try std.testing.expect(debertaQ8ProjectionWeight(true, "encoder.layer.23.output.dense.weight", &loaded));
    try std.testing.expect(!debertaQ8ProjectionWeight(true, "encoder.layer.24.output.dense.weight", &loaded));
    try std.testing.expect(!debertaQ8ProjectionWeight(false, "encoder.layer.0.output.dense.weight", &loaded));
    try std.testing.expect(ettinProjectionShape(3840, 1792));
    try std.testing.expect(!ettinProjectionShape(1792, 3840));
    try std.testing.expect(spanQ4ProjectionShape(2, 768, 3072));
    try std.testing.expect(!spanQ4ProjectionShape(1, 768, 3072));
    try std.testing.expect(keepsF16Weight("encoder.layer.0.attention.self.query_proj.weight"));
    try std.testing.expect(!keepsF16Weight("classifier.weight"));
}
