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

// Integration test for WasmCompute: runs a tiny BERT forward pass with random weights.
//
// Verifies the full compute pipeline: weight registration, embedding lookup,
// linear, layerNorm, gelu, scaledDotProductAttention, add, toFloat32, free.
// Runs on the native target (not WASM) since both use the same Zig code paths.

const std = @import("std");
const wasm_compute = @import("wasm_compute.zig");
const bert_arch = @import("../architectures/bert.zig");

// Tiny BERT config: 1 layer, 4 heads, 32-dim, 64 intermediate.
const test_config = bert_arch.Config{
    .model_type = .bert,
    .vocab_size = 64,
    .hidden_size = 32,
    .num_hidden_layers = 1,
    .num_attention_heads = 4,
    .intermediate_size = 64,
    .max_position_embeddings = 16,
    .type_vocab_size = 2,
};

fn randomF32(allocator: std.mem.Allocator, count: usize) ![]f32 {
    const data = try allocator.alloc(f32, count);
    var rng = std.Random.DefaultPrng.init(0xdeadbeef);
    const rand = rng.random();
    for (data) |*v| {
        v.* = rand.float(f32) * 0.02 - 0.01; // small random init
    }
    return data;
}

fn onesF32(allocator: std.mem.Allocator, count: usize) ![]f32 {
    const data = try allocator.alloc(f32, count);
    @memset(data, 1.0);
    return data;
}

fn zerosF32(allocator: std.mem.Allocator, count: usize) ![]f32 {
    const data = try allocator.alloc(f32, count);
    @memset(data, 0.0);
    return data;
}

fn registerTestWeights(compute: *wasm_compute.WasmCompute, allocator: std.mem.Allocator) !void {
    const H = test_config.hidden_size;
    const I = test_config.intermediate_size;
    const V = test_config.vocab_size;
    const max_pos = test_config.max_position_embeddings;
    const type_vocab = test_config.type_vocab_size;

    // Embeddings
    compute.registerWeight("embeddings.word_embeddings.weight", try randomF32(allocator, V * H));
    compute.registerWeight("embeddings.position_embeddings.weight", try randomF32(allocator, max_pos * H));
    compute.registerWeight("embeddings.token_type_embeddings.weight", try randomF32(allocator, type_vocab * H));
    compute.registerWeight("embeddings.LayerNorm.weight", try onesF32(allocator, H));
    compute.registerWeight("embeddings.LayerNorm.bias", try zerosF32(allocator, H));

    // Encoder layer 0
    const layer_prefixes = [_][]const u8{
        "encoder.layer.0.attention.self.query",
        "encoder.layer.0.attention.self.key",
        "encoder.layer.0.attention.self.value",
        "encoder.layer.0.attention.output.dense",
        "encoder.layer.0.intermediate.dense",
        "encoder.layer.0.output.dense",
    };
    const layer_sizes = [_][2]usize{
        .{ H, H }, // query
        .{ H, H }, // key
        .{ H, H }, // value
        .{ H, H }, // attn output
        .{ I, H }, // intermediate (out_dim x in_dim for TransB)
        .{ H, I }, // output
    };

    for (layer_prefixes, layer_sizes) |prefix, sizes| {
        var w_name_buf: [128]u8 = undefined;
        var b_name_buf: [128]u8 = undefined;
        const w_name = std.fmt.bufPrint(&w_name_buf, "{s}.weight", .{prefix}) catch unreachable;
        const b_name = std.fmt.bufPrint(&b_name_buf, "{s}.bias", .{prefix}) catch unreachable;
        compute.registerWeight(w_name, try randomF32(allocator, sizes[0] * sizes[1]));
        compute.registerWeight(b_name, try zerosF32(allocator, sizes[0]));
    }

    // LayerNorm weights for attention output and layer output
    compute.registerWeight("encoder.layer.0.attention.output.LayerNorm.weight", try onesF32(allocator, H));
    compute.registerWeight("encoder.layer.0.attention.output.LayerNorm.bias", try zerosF32(allocator, H));
    compute.registerWeight("encoder.layer.0.output.LayerNorm.weight", try onesF32(allocator, H));
    compute.registerWeight("encoder.layer.0.output.LayerNorm.bias", try zerosF32(allocator, H));
}

test "wasm_compute: tiny BERT forward pass" {
    const allocator = std.testing.allocator;

    var compute = wasm_compute.WasmCompute.init(allocator);
    var cb = compute.computeBackend();
    defer cb.deinit();

    try registerTestWeights(&compute, allocator);

    const batch: usize = 1;
    const seq_len: usize = 4;
    const H = test_config.hidden_size;

    // Input: token IDs [0, 1, 2, 3], all attended
    const input_ids = [_]i64{ 0, 1, 2, 3 };
    const attention_mask = [_]i64{ 1, 1, 1, 1 };

    const result = try bert_arch.forward(
        &cb,
        allocator,
        test_config,
        &input_ids,
        &attention_mask,
        null,
        batch,
        seq_len,
    );
    defer allocator.free(result);

    // Output shape: [batch * seq_len * hidden_size]
    try std.testing.expectEqual(batch * seq_len * H, result.len);

    // Verify values are finite (not NaN or inf)
    for (result) |v| {
        try std.testing.expect(!std.math.isNan(v));
        try std.testing.expect(!std.math.isInf(v));
    }
}

test "wasm_compute: batched embedding" {
    const allocator = std.testing.allocator;

    var compute = wasm_compute.WasmCompute.init(allocator);
    var cb = compute.computeBackend();
    defer cb.deinit();

    try registerTestWeights(&compute, allocator);

    const batch: usize = 2;
    const seq_len: usize = 3;
    const H = test_config.hidden_size;

    // Two sequences, padded to length 3
    const input_ids = [_]i64{ 0, 1, 2, 3, 4, 5 };
    const attention_mask = [_]i64{ 1, 1, 1, 1, 1, 0 }; // second seq has padding

    const result = try bert_arch.forward(
        &cb,
        allocator,
        test_config,
        &input_ids,
        &attention_mask,
        null,
        batch,
        seq_len,
    );
    defer allocator.free(result);

    try std.testing.expectEqual(batch * seq_len * H, result.len);

    for (result) |v| {
        try std.testing.expect(!std.math.isNan(v));
        try std.testing.expect(!std.math.isInf(v));
    }
}

test "wasm_compute: individual ops" {
    const allocator = std.testing.allocator;

    var compute = wasm_compute.WasmCompute.init(allocator);
    var cb = compute.computeBackend();
    defer cb.deinit();

    // Test fromFloat32 + toFloat32 roundtrip
    const data = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    const tensor = try cb.fromFloat32(&data);
    const back = try cb.toFloat32(tensor, allocator);
    defer allocator.free(back);
    cb.free(tensor);

    try std.testing.expectEqualSlices(f32, &data, back);

    // Test add
    const a_data = [_]f32{ 1.0, 2.0, 3.0 };
    const b_data = [_]f32{ 10.0, 20.0, 30.0 };
    const a = try cb.fromFloat32(&a_data);
    const b = try cb.fromFloat32(&b_data);
    const sum = try cb.add(a, b);
    const sum_out = try cb.toFloat32(sum, allocator);
    defer allocator.free(sum_out);
    cb.free(a);
    cb.free(b);
    cb.free(sum);

    try std.testing.expectApproxEqAbs(@as(f32, 11.0), sum_out[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 22.0), sum_out[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 33.0), sum_out[2], 1e-6);

    // Test gelu
    const gelu_in = [_]f32{ 0.0, 1.0, -1.0, 2.0 };
    const gelu_t = try cb.fromFloat32(&gelu_in);
    const gelu_out_t = try cb.gelu(gelu_t);
    const gelu_out = try cb.toFloat32(gelu_out_t, allocator);
    defer allocator.free(gelu_out);
    cb.free(gelu_t);
    cb.free(gelu_out_t);

    // GELU(0) = 0
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), gelu_out[0], 1e-5);
    // GELU(1) ≈ 0.8412
    try std.testing.expectApproxEqAbs(@as(f32, 0.8412), gelu_out[1], 1e-3);
}

test "wasm_compute: embeddingLookupTensor from i32 token tensor" {
    const allocator = std.testing.allocator;

    var compute = wasm_compute.WasmCompute.init(allocator);
    const embed = try allocator.dupe(f32, &[_]f32{
        1.0, 2.0, 3.0,
        4.0, 5.0, 6.0,
        7.0, 8.0, 9.0,
    });
    compute.registerWeight("embed.weight", embed);

    var cb = compute.computeBackend();
    defer cb.deinit();

    const ids = [_]i32{2};
    const ids_shape = [_]i32{1};
    const ids_tensor = (try cb.fromInt32Shape(&ids, &ids_shape)) orelse return error.UnsupportedOperation;
    defer cb.free(ids_tensor);

    const weight = try cb.getWeight("embed.weight");
    defer cb.free(weight);

    const embedded = (try cb.embeddingLookupTensor(weight, ids_tensor, 1, 3)) orelse return error.UnsupportedOperation;
    defer cb.free(embedded);

    const shape = try cb.tensorShape(embedded, allocator);
    defer allocator.free(shape);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 3 }, shape);

    const out = try cb.toFloat32(embedded, allocator);
    defer allocator.free(out);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 7.0, 8.0, 9.0 }, out);
}

test "wasm_compute: sampleLastRow and argmaxLastRow" {
    const allocator = std.testing.allocator;

    var compute = wasm_compute.WasmCompute.init(allocator);
    var cb = compute.computeBackend();
    defer cb.deinit();

    const logits = [_]f32{
        0.1, 0.2, 0.3,
        0.4, 3.5, 1.2,
    };
    const logits_shape = [_]i32{ 2, 3 };
    const logits_tensor = try cb.fromFloat32Shape(&logits, &logits_shape);
    defer cb.free(logits_tensor);

    const argmax = (try cb.argmaxLastRow(logits_tensor, 2, 3)) orelse return error.UnsupportedOperation;
    try std.testing.expectEqual(@as(u32, 1), argmax);

    const sampled = (try cb.sampleLastRow(&.{
        .tensor = logits_tensor,
        .rows = 2,
        .dim = 3,
        .temperature = 0.0,
        .top_k = 0,
        .top_p = 0.0,
        .min_p = 0.0,
        .repetition_penalty = 1.0,
        .frequency_penalty = 0.0,
        .presence_penalty = 0.0,
        .token_history = &.{},
    })) orelse return error.UnsupportedOperation;
    try std.testing.expectEqual(@as(u32, 1), sampled);
}

test "wasm_compute: decoder runtime absolute embeddings" {
    const allocator = std.testing.allocator;

    var compute = wasm_compute.WasmCompute.init(allocator);
    var cb = compute.computeBackend();
    defer cb.deinit();

    compute.registerWeight("tok", try allocator.dupe(f32, &[_]f32{
        1.0,  2.0,  3.0,
        10.0, 20.0, 30.0,
    }));
    compute.registerWeight("pos", try allocator.dupe(f32, &[_]f32{
        0.5, 0.25, 0.125,
        1.0, 1.5,  2.0,
    }));
    const tok = try cb.getWeight("tok");
    const pos = try cb.getWeight("pos");

    try std.testing.expect(try cb.decoderRuntimePrepareAbsoluteEmbeddings(&.{
        .token_embedding = tok,
        .position_embedding = pos,
        .vocab_size = 2,
        .max_position_embeddings = 2,
        .hidden_size = 3,
    }));

    const embedded = (try cb.decoderRuntimeEmbedAbsolutePosition(&.{
        .token_id = 1,
        .position_id = 1,
        .hidden_size = 3,
    })) orelse return error.UnsupportedOperation;
    defer cb.free(embedded);

    const out = try cb.toFloat32(embedded, allocator);
    defer allocator.free(out);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 11.0, 21.5, 32.0 }, out);
}

test "wasm_compute: decoder runtime prepared linear argmax" {
    const allocator = std.testing.allocator;

    var compute = wasm_compute.WasmCompute.init(allocator);
    var cb = compute.computeBackend();
    defer cb.deinit();

    const weight_data = [_]f32{
        1.0, 0.0,
        0.0, 1.0,
        0.5, 0.5,
    };
    const bias_data = [_]f32{ 0.0, 0.0, 0.0 };
    const input_data = [_]f32{ 1.0, 5.0 };
    const input_shape = [_]i32{ 1, 2 };

    const weight = try cb.fromFloat32Shape(&weight_data, &[_]i32{ 3, 2 });
    defer cb.free(weight);
    const bias = try cb.fromFloat32Shape(&bias_data, &[_]i32{3});
    defer cb.free(bias);
    const input = try cb.fromFloat32Shape(&input_data, &input_shape);
    defer cb.free(input);

    try std.testing.expect(try cb.decoderRuntimePrepareLinear(&.{
        .slot = 7,
        .weight = weight,
        .bias = bias,
        .in_dim = 2,
        .out_dim = 3,
    }));

    const logits = (try cb.decoderRuntimeApplyLinear(&.{
        .slot = 7,
        .input = input,
        .in_dim = 2,
        .out_dim = 3,
    })) orelse return error.UnsupportedOperation;
    defer cb.free(logits);

    const out = try cb.toFloat32(logits, allocator);
    defer allocator.free(out);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 1.0, 5.0, 3.0 }, out);

    const argmax = (try cb.decoderRuntimeApplyLinearArgmax(&.{
        .slot = 7,
        .input = input,
        .in_dim = 2,
        .out_dim = 3,
    })) orelse return error.UnsupportedOperation;
    try std.testing.expectEqual(@as(usize, 1), argmax);
}

test "wasm_compute: decoder runtime dense ffn residual" {
    const allocator = std.testing.allocator;

    var compute = wasm_compute.WasmCompute.init(allocator);
    var cb = compute.computeBackend();
    defer cb.deinit();

    const first_weight = try cb.fromFloat32Shape(&[_]f32{
        1.0, 0.0,
        0.0, 1.0,
        1.0, 1.0,
    }, &[_]i32{ 3, 2 });
    defer cb.free(first_weight);
    const second_weight = try cb.fromFloat32Shape(&[_]f32{
        1.0, 0.0, 0.0,
        0.0, 1.0, 0.0,
    }, &[_]i32{ 2, 3 });
    defer cb.free(second_weight);
    const first_bias = try cb.fromFloat32Shape(&[_]f32{ 0.0, 0.0, 0.0 }, &[_]i32{3});
    defer cb.free(first_bias);
    const second_bias = try cb.fromFloat32Shape(&[_]f32{ 0.0, 0.0 }, &[_]i32{2});
    defer cb.free(second_bias);
    const input = try cb.fromFloat32Shape(&[_]f32{ 2.0, 3.0 }, &[_]i32{ 1, 2 });
    defer cb.free(input);
    const residual = try cb.fromFloat32Shape(&[_]f32{ 10.0, 20.0 }, &[_]i32{ 1, 2 });
    defer cb.free(residual);

    try std.testing.expect(try cb.decoderRuntimePrepareLinear(&.{
        .slot = 11,
        .weight = first_weight,
        .bias = first_bias,
        .in_dim = 2,
        .out_dim = 3,
    }));
    try std.testing.expect(try cb.decoderRuntimePrepareLinear(&.{
        .slot = 12,
        .weight = second_weight,
        .bias = second_bias,
        .in_dim = 3,
        .out_dim = 2,
    }));

    const hidden = (try cb.runDenseFfnResidual(&.{
        .first_linear_slot = 11,
        .second_linear_slot = 12,
        .input = input,
        .residual = residual,
        .hidden_size = 2,
        .intermediate_size = 3,
        .activation = .relu,
    })) orelse return error.UnsupportedOperation;
    defer cb.free(hidden);

    const out = try cb.toFloat32(hidden, allocator);
    defer allocator.free(out);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 12.0, 23.0 }, out);
}

test "wasm_compute: runAttention dense causal" {
    const allocator = std.testing.allocator;

    var compute = wasm_compute.WasmCompute.init(allocator);
    var cb = compute.computeBackend();
    defer cb.deinit();

    const q = try cb.fromFloat32Shape(&[_]f32{
        1.0, 0.0,
        0.0, 1.0,
    }, &[_]i32{ 2, 2 });
    defer cb.free(q);
    const k = try cb.fromFloat32Shape(&[_]f32{
        1.0, 0.0,
        0.0, 1.0,
    }, &[_]i32{ 2, 2 });
    defer cb.free(k);
    const v = try cb.fromFloat32Shape(&[_]f32{
        2.0, 4.0,
        8.0, 16.0,
    }, &[_]i32{ 2, 2 });
    defer cb.free(v);

    const out = (try cb.runAttention(&.{
        .q = q,
        .k = k,
        .v = v,
        .attention = .{
            .mode = .dense_causal,
            .total_sequence_len = 2,
            .query_sequence_len = 2,
            .kv_sequence_len = 2,
        },
        .num_heads = 1,
        .num_kv_heads = 1,
        .head_dim = 2,
    })) orelse return error.UnsupportedOperation;
    defer cb.free(out);

    const out_f32 = try cb.toFloat32(out, allocator);
    defer allocator.free(out_f32);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), out_f32[0], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 4.0), out_f32[1], 1e-5);
    try std.testing.expect(out_f32[2] > 2.0 and out_f32[2] < 8.0);
    try std.testing.expect(out_f32[3] > 4.0 and out_f32[3] < 16.0);
}

test "wasm_compute: runAttentionResidual dense causal" {
    const allocator = std.testing.allocator;

    var compute = wasm_compute.WasmCompute.init(allocator);
    var cb = compute.computeBackend();
    defer cb.deinit();

    const linear_weight = try cb.fromFloat32Shape(&[_]f32{
        1.0, 0.0,
        0.0, 1.0,
    }, &[_]i32{ 2, 2 });
    defer cb.free(linear_weight);
    const linear_bias = try cb.fromFloat32Shape(&[_]f32{ 0.0, 0.0 }, &[_]i32{2});
    defer cb.free(linear_bias);
    try std.testing.expect(try cb.decoderRuntimePrepareLinear(&.{
        .slot = 21,
        .weight = linear_weight,
        .bias = linear_bias,
        .in_dim = 2,
        .out_dim = 2,
    }));

    const q = try cb.fromFloat32Shape(&[_]f32{
        1.0, 0.0,
        0.0, 1.0,
    }, &[_]i32{ 2, 2 });
    defer cb.free(q);
    const k = try cb.fromFloat32Shape(&[_]f32{
        1.0, 0.0,
        0.0, 1.0,
    }, &[_]i32{ 2, 2 });
    defer cb.free(k);
    const v = try cb.fromFloat32Shape(&[_]f32{
        2.0, 4.0,
        8.0, 16.0,
    }, &[_]i32{ 2, 2 });
    defer cb.free(v);
    const residual = try cb.fromFloat32Shape(&[_]f32{
        1.0, 1.0,
        1.0, 1.0,
    }, &[_]i32{ 2, 2 });
    defer cb.free(residual);

    const out = (try cb.runAttentionResidual(&.{
        .q = q,
        .k = k,
        .v = v,
        .residual = residual,
        .attention = .{
            .mode = .dense_causal,
            .total_sequence_len = 2,
            .query_sequence_len = 2,
            .kv_sequence_len = 2,
        },
        .num_heads = 1,
        .num_kv_heads = 1,
        .head_dim = 2,
        .linear_slot = 21,
        .hidden_size = 2,
        .eps = 1e-5,
    })) orelse return error.UnsupportedOperation;
    defer cb.free(out);

    const out_f32 = try cb.toFloat32(out, allocator);
    defer allocator.free(out_f32);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), out_f32[0], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 5.0), out_f32[1], 1e-5);
    try std.testing.expect(out_f32[2] > 3.0 and out_f32[2] < 9.0);
    try std.testing.expect(out_f32[3] > 5.0 and out_f32[3] < 17.0);
}

test "wasm_compute: runDenseDecoderBlock dense causal" {
    const allocator = std.testing.allocator;

    var compute = wasm_compute.WasmCompute.init(allocator);
    var cb = compute.computeBackend();
    defer cb.deinit();

    const attn_weight = try cb.fromFloat32Shape(&[_]f32{
        1.0, 0.0,
        0.0, 1.0,
    }, &[_]i32{ 2, 2 });
    defer cb.free(attn_weight);
    const attn_bias = try cb.fromFloat32Shape(&[_]f32{ 0.0, 0.0 }, &[_]i32{2});
    defer cb.free(attn_bias);
    try std.testing.expect(try cb.decoderRuntimePrepareLinear(&.{
        .slot = 31,
        .weight = attn_weight,
        .bias = attn_bias,
        .in_dim = 2,
        .out_dim = 2,
    }));

    const fc1_weight = try cb.fromFloat32Shape(&[_]f32{
        1.0, 0.0,
        0.0, 1.0,
    }, &[_]i32{ 2, 2 });
    defer cb.free(fc1_weight);
    const fc1_bias = try cb.fromFloat32Shape(&[_]f32{ 0.0, 0.0 }, &[_]i32{2});
    defer cb.free(fc1_bias);
    try std.testing.expect(try cb.decoderRuntimePrepareLinear(&.{
        .slot = 32,
        .weight = fc1_weight,
        .bias = fc1_bias,
        .in_dim = 2,
        .out_dim = 2,
    }));

    const fc2_weight = try cb.fromFloat32Shape(&[_]f32{
        1.0, 0.0,
        0.0, 1.0,
    }, &[_]i32{ 2, 2 });
    defer cb.free(fc2_weight);
    const fc2_bias = try cb.fromFloat32Shape(&[_]f32{ 0.0, 0.0 }, &[_]i32{2});
    defer cb.free(fc2_bias);
    try std.testing.expect(try cb.decoderRuntimePrepareLinear(&.{
        .slot = 33,
        .weight = fc2_weight,
        .bias = fc2_bias,
        .in_dim = 2,
        .out_dim = 2,
    }));

    const q = try cb.fromFloat32Shape(&[_]f32{ 0.0, 0.0, 0.0, 0.0 }, &[_]i32{ 2, 2 });
    defer cb.free(q);
    const k = try cb.fromFloat32Shape(&[_]f32{ 0.0, 0.0, 0.0, 0.0 }, &[_]i32{ 2, 2 });
    defer cb.free(k);
    const v = try cb.fromFloat32Shape(&[_]f32{ 0.0, 0.0, 0.0, 0.0 }, &[_]i32{ 2, 2 });
    defer cb.free(v);
    const residual = try cb.fromFloat32Shape(&[_]f32{ 1.0, 2.0, 3.0, 4.0 }, &[_]i32{ 2, 2 });
    defer cb.free(residual);

    const out = (try cb.runDenseDecoderBlock(&.{
        .q = q,
        .k = k,
        .v = v,
        .residual = residual,
        .attention = .{
            .mode = .dense_causal,
            .total_sequence_len = 2,
            .query_sequence_len = 2,
            .kv_sequence_len = 2,
        },
        .num_heads = 1,
        .num_kv_heads = 1,
        .head_dim = 2,
        .attention_linear_slot = 31,
        .hidden_size = 2,
        .eps = 1e-5,
        .first_ffn_linear_slot = 32,
        .second_ffn_linear_slot = 33,
        .intermediate_size = 2,
        .activation = .relu,
    })) orelse return error.UnsupportedOperation;
    defer cb.free(out);

    const out_f32 = try cb.toFloat32(out, allocator);
    defer allocator.free(out_f32);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 2.0, 4.0, 6.0, 8.0 }, out_f32);
}

test "wasm_compute: runGatedDecoderBlock dense causal" {
    const allocator = std.testing.allocator;

    var compute = wasm_compute.WasmCompute.init(allocator);
    var cb = compute.computeBackend();
    defer cb.deinit();

    const attn_weight = try cb.fromFloat32Shape(&[_]f32{
        1.0, 0.0,
        0.0, 1.0,
    }, &[_]i32{ 2, 2 });
    defer cb.free(attn_weight);
    const attn_bias = try cb.fromFloat32Shape(&[_]f32{ 0.0, 0.0 }, &[_]i32{2});
    defer cb.free(attn_bias);
    try std.testing.expect(try cb.decoderRuntimePrepareLinear(&.{
        .slot = 41,
        .weight = attn_weight,
        .bias = attn_bias,
        .in_dim = 2,
        .out_dim = 2,
    }));

    const gate_weight = try cb.fromFloat32Shape(&[_]f32{
        1.0, 0.0,
        0.0, 1.0,
    }, &[_]i32{ 2, 2 });
    defer cb.free(gate_weight);
    const gate_bias = try cb.fromFloat32Shape(&[_]f32{ 0.0, 0.0 }, &[_]i32{2});
    defer cb.free(gate_bias);
    try std.testing.expect(try cb.decoderRuntimePrepareLinear(&.{
        .slot = 42,
        .weight = gate_weight,
        .bias = gate_bias,
        .in_dim = 2,
        .out_dim = 2,
    }));

    const up_weight = try cb.fromFloat32Shape(&[_]f32{
        1.0, 0.0,
        0.0, 1.0,
    }, &[_]i32{ 2, 2 });
    defer cb.free(up_weight);
    const up_bias = try cb.fromFloat32Shape(&[_]f32{ 0.0, 0.0 }, &[_]i32{2});
    defer cb.free(up_bias);
    try std.testing.expect(try cb.decoderRuntimePrepareLinear(&.{
        .slot = 43,
        .weight = up_weight,
        .bias = up_bias,
        .in_dim = 2,
        .out_dim = 2,
    }));

    const down_weight = try cb.fromFloat32Shape(&[_]f32{
        1.0, 0.0,
        0.0, 1.0,
    }, &[_]i32{ 2, 2 });
    defer cb.free(down_weight);
    const down_bias = try cb.fromFloat32Shape(&[_]f32{ 0.0, 0.0 }, &[_]i32{2});
    defer cb.free(down_bias);
    try std.testing.expect(try cb.decoderRuntimePrepareLinear(&.{
        .slot = 44,
        .weight = down_weight,
        .bias = down_bias,
        .in_dim = 2,
        .out_dim = 2,
    }));

    const q = try cb.fromFloat32Shape(&[_]f32{ 0.0, 0.0, 0.0, 0.0 }, &[_]i32{ 2, 2 });
    defer cb.free(q);
    const k = try cb.fromFloat32Shape(&[_]f32{ 0.0, 0.0, 0.0, 0.0 }, &[_]i32{ 2, 2 });
    defer cb.free(k);
    const v = try cb.fromFloat32Shape(&[_]f32{ 0.0, 0.0, 0.0, 0.0 }, &[_]i32{ 2, 2 });
    defer cb.free(v);
    const residual = try cb.fromFloat32Shape(&[_]f32{ 1.0, 2.0, 3.0, 4.0 }, &[_]i32{ 2, 2 });
    defer cb.free(residual);

    const out = (try cb.runGatedDecoderBlock(&.{
        .q = q,
        .k = k,
        .v = v,
        .residual = residual,
        .attention = .{
            .mode = .dense_causal,
            .total_sequence_len = 2,
            .query_sequence_len = 2,
            .kv_sequence_len = 2,
        },
        .num_heads = 1,
        .num_kv_heads = 1,
        .head_dim = 2,
        .attention_linear_slot = 41,
        .hidden_size = 2,
        .eps = 1e-5,
        .gate_ffn_linear_slot = 42,
        .up_ffn_linear_slot = 43,
        .down_ffn_linear_slot = 44,
        .intermediate_size = 2,
        .activation = .relu,
    })) orelse return error.UnsupportedOperation;
    defer cb.free(out);

    const out_f32 = try cb.toFloat32(out, allocator);
    defer allocator.free(out_f32);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 2.0, 6.0, 12.0, 20.0 }, out_f32);
}

test "wasm_compute: runGatedDecoderBlock attention_input projection path" {
    const allocator = std.testing.allocator;

    var compute = wasm_compute.WasmCompute.init(allocator);
    var cb = compute.computeBackend();
    defer cb.deinit();

    const q_weight = try cb.fromFloat32Shape(&[_]f32{
        1.0, 0.0,
        0.0, 1.0,
    }, &[_]i32{ 2, 2 });
    defer cb.free(q_weight);
    const q_bias = try cb.fromFloat32Shape(&[_]f32{ 0.0, 0.0 }, &[_]i32{2});
    defer cb.free(q_bias);
    try std.testing.expect(try cb.decoderRuntimePrepareLinear(&.{
        .slot = 51,
        .weight = q_weight,
        .bias = q_bias,
        .in_dim = 2,
        .out_dim = 2,
    }));

    const k_weight = try cb.fromFloat32Shape(&[_]f32{
        1.0, 0.0,
        0.0, 1.0,
    }, &[_]i32{ 2, 2 });
    defer cb.free(k_weight);
    const k_bias = try cb.fromFloat32Shape(&[_]f32{ 0.0, 0.0 }, &[_]i32{2});
    defer cb.free(k_bias);
    try std.testing.expect(try cb.decoderRuntimePrepareLinear(&.{
        .slot = 52,
        .weight = k_weight,
        .bias = k_bias,
        .in_dim = 2,
        .out_dim = 2,
    }));

    const v_weight = try cb.fromFloat32Shape(&[_]f32{
        0.0, 0.0,
        0.0, 0.0,
    }, &[_]i32{ 2, 2 });
    defer cb.free(v_weight);
    const v_bias = try cb.fromFloat32Shape(&[_]f32{ 0.0, 0.0 }, &[_]i32{2});
    defer cb.free(v_bias);
    try std.testing.expect(try cb.decoderRuntimePrepareLinear(&.{
        .slot = 53,
        .weight = v_weight,
        .bias = v_bias,
        .in_dim = 2,
        .out_dim = 2,
    }));

    const attn_weight = try cb.fromFloat32Shape(&[_]f32{
        1.0, 0.0,
        0.0, 1.0,
    }, &[_]i32{ 2, 2 });
    defer cb.free(attn_weight);
    const attn_bias = try cb.fromFloat32Shape(&[_]f32{ 0.0, 0.0 }, &[_]i32{2});
    defer cb.free(attn_bias);
    try std.testing.expect(try cb.decoderRuntimePrepareLinear(&.{
        .slot = 54,
        .weight = attn_weight,
        .bias = attn_bias,
        .in_dim = 2,
        .out_dim = 2,
    }));

    const gate_weight = try cb.fromFloat32Shape(&[_]f32{
        1.0, 0.0,
        0.0, 1.0,
    }, &[_]i32{ 2, 2 });
    defer cb.free(gate_weight);
    const gate_bias = try cb.fromFloat32Shape(&[_]f32{ 0.0, 0.0 }, &[_]i32{2});
    defer cb.free(gate_bias);
    try std.testing.expect(try cb.decoderRuntimePrepareLinear(&.{
        .slot = 55,
        .weight = gate_weight,
        .bias = gate_bias,
        .in_dim = 2,
        .out_dim = 2,
    }));

    const up_weight = try cb.fromFloat32Shape(&[_]f32{
        1.0, 0.0,
        0.0, 1.0,
    }, &[_]i32{ 2, 2 });
    defer cb.free(up_weight);
    const up_bias = try cb.fromFloat32Shape(&[_]f32{ 0.0, 0.0 }, &[_]i32{2});
    defer cb.free(up_bias);
    try std.testing.expect(try cb.decoderRuntimePrepareLinear(&.{
        .slot = 56,
        .weight = up_weight,
        .bias = up_bias,
        .in_dim = 2,
        .out_dim = 2,
    }));

    const down_weight = try cb.fromFloat32Shape(&[_]f32{
        1.0, 0.0,
        0.0, 1.0,
    }, &[_]i32{ 2, 2 });
    defer cb.free(down_weight);
    const down_bias = try cb.fromFloat32Shape(&[_]f32{ 0.0, 0.0 }, &[_]i32{2});
    defer cb.free(down_bias);
    try std.testing.expect(try cb.decoderRuntimePrepareLinear(&.{
        .slot = 57,
        .weight = down_weight,
        .bias = down_bias,
        .in_dim = 2,
        .out_dim = 2,
    }));

    const attention_input = try cb.fromFloat32Shape(&[_]f32{ 0.0, 0.0, 0.0, 0.0 }, &[_]i32{ 2, 2 });
    defer cb.free(attention_input);
    const residual = try cb.fromFloat32Shape(&[_]f32{ 1.0, 2.0, 3.0, 4.0 }, &[_]i32{ 2, 2 });
    defer cb.free(residual);

    const out = (try cb.runGatedDecoderBlock(&.{
        .attention_input = attention_input,
        .residual = residual,
        .attention = .{
            .mode = .dense_causal,
            .total_sequence_len = 2,
            .query_sequence_len = 2,
            .kv_sequence_len = 2,
        },
        .num_heads = 1,
        .num_kv_heads = 1,
        .head_dim = 2,
        .q_linear_slot = 51,
        .k_linear_slot = 52,
        .v_linear_slot = 53,
        .attention_linear_slot = 54,
        .hidden_size = 2,
        .eps = 1e-5,
        .gate_ffn_linear_slot = 55,
        .up_ffn_linear_slot = 56,
        .down_ffn_linear_slot = 57,
        .intermediate_size = 2,
        .activation = .relu,
    })) orelse return error.UnsupportedOperation;
    defer cb.free(out);

    const out_f32 = try cb.toFloat32(out, allocator);
    defer allocator.free(out_f32);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 2.0, 6.0, 12.0, 20.0 }, out_f32);
}
