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

// End-to-end test: load real bge-small-en-v1.5 SafeTensors model through WasmCompute.
//
// Requires models/BAAI/bge-small-en-v1.5/model.safetensors to exist.
// Run with: zig build test
//
// This exercises the full pipeline: SafeTensors parsing, f32 conversion,
// weight registration, and BERT forward pass through the WasmCompute backend.

const std = @import("std");
const wasm_compute = @import("wasm_compute.zig");
const bert_arch = @import("../architectures/bert.zig");
const bert_config_mod = @import("../models/bert.zig");
const safetensors = @import("../models/safetensors.zig");

const model_dir = "models/BAAI/bge-small-en-v1.5";

fn loadSafetensorsModel(allocator: std.mem.Allocator) !struct { compute: wasm_compute.WasmCompute, config: bert_arch.Config } {
    // Read config
    const config_bytes = try std.fs.cwd().readFileAlloc(allocator, model_dir ++ "/config.json", 1024 * 1024);
    defer allocator.free(config_bytes);
    const config = try bert_config_mod.parseConfig(allocator, config_bytes);

    // Read SafeTensors
    const st_bytes = try std.fs.cwd().readFileAlloc(allocator, model_dir ++ "/model.safetensors", 256 * 1024 * 1024);
    defer allocator.free(st_bytes);

    const result = try safetensors.parseHeader(allocator, st_bytes);
    var header = result.header;
    defer header.deinit();
    const data_offset = result.data_offset;

    var compute = wasm_compute.WasmCompute.init(allocator);

    var it = header.tensors.iterator();
    while (it.next()) |entry| {
        const name = entry.key_ptr.*;
        const meta = entry.value_ptr.*;

        // Skip non-weight tensors
        if (std.mem.endsWith(u8, name, ".position_ids")) continue;

        const abs_start: usize = @intCast(data_offset + meta.data_start);
        const abs_end: usize = @intCast(data_offset + meta.data_end);
        if (abs_end > st_bytes.len) continue;
        const raw = st_bytes[abs_start..abs_end];

        const n_elements = blk: {
            var count: usize = 1;
            for (meta.shape) |dim| count *= @intCast(dim);
            break :blk count;
        };

        // All bge-small weights are F32
        if (meta.dtype != .f32) continue;
        const src = @as([*]const f32, @ptrCast(@alignCast(raw.ptr)))[0..n_elements];
        const f32_data = try allocator.dupe(f32, src);
        compute.registerWeight(name, f32_data);
    }

    return .{ .compute = compute, .config = config };
}

test "e2e: bge-small-en-v1.5 SafeTensors forward pass" {
    const allocator = std.testing.allocator;

    const loaded = loadSafetensorsModel(allocator) catch |err| {
        if (err == error.FileNotFound) {
            // Model not downloaded — skip test gracefully
            return;
        }
        return err;
    };
    var compute = loaded.compute;
    const config = loaded.config;
    defer {
        var it = compute.weights.valueIterator();
        while (it.next()) |buf| buf.*.deinit();
        compute.weights.deinit();
    }

    // Verify config
    try std.testing.expectEqual(@as(usize, 384), config.hidden_size);
    try std.testing.expectEqual(@as(usize, 12), config.num_hidden_layers);
    try std.testing.expectEqual(@as(usize, 12), config.num_attention_heads);

    var cb = compute.computeBackend();

    const batch: usize = 1;
    const seq_len: usize = 8;

    // "[CLS] this is a test sentence . [SEP]"
    const input_ids = [_]i64{ 101, 2023, 2003, 1037, 3231, 6251, 1012, 102 };
    const attention_mask = [_]i64{ 1, 1, 1, 1, 1, 1, 1, 1 };

    const result = try bert_arch.forward(
        &cb,
        allocator,
        config,
        &input_ids,
        &attention_mask,
        null,
        batch,
        seq_len,
    );
    defer allocator.free(result);

    // Output shape: [batch * seq_len * hidden_size] = 1 * 8 * 384 = 3072
    try std.testing.expectEqual(@as(usize, batch * seq_len * config.hidden_size), result.len);

    // All values should be finite
    for (result) |v| {
        try std.testing.expect(!std.math.isNan(v));
        try std.testing.expect(!std.math.isInf(v));
    }

    // Values should be in a reasonable range for normalized BERT outputs
    var max_abs: f32 = 0;
    for (result) |v| {
        const abs = @abs(v);
        if (abs > max_abs) max_abs = abs;
    }
    // BERT hidden states should be finite, typically < 100
    try std.testing.expect(max_abs < 1000.0);
    try std.testing.expect(max_abs > 0.001);
}

test "e2e: bge-small-en-v1.5 forward pass seq_len=16" {
    const allocator = std.testing.allocator;

    const loaded = loadSafetensorsModel(allocator) catch |err| {
        if (err == error.FileNotFound) return;
        return err;
    };
    var compute = loaded.compute;
    const config = loaded.config;
    defer {
        var it = compute.weights.valueIterator();
        while (it.next()) |buf| buf.*.deinit();
        compute.weights.deinit();
    }

    var cb = compute.computeBackend();

    const batch: usize = 1;
    const seq_len: usize = 16;

    // "[CLS] this is a test sentence . [SEP] [PAD] [PAD] [PAD] [PAD] [PAD] [PAD] [PAD] [PAD]"
    const input_ids = [_]i64{ 101, 2023, 2003, 1037, 3231, 6251, 1012, 102, 0, 0, 0, 0, 0, 0, 0, 0 };
    const attention_mask = [_]i64{ 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0 };

    const result = try bert_arch.forward(
        &cb,
        allocator,
        config,
        &input_ids,
        &attention_mask,
        null,
        batch,
        seq_len,
    );
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, batch * seq_len * config.hidden_size), result.len);

    for (result) |v| {
        try std.testing.expect(!std.math.isNan(v));
        try std.testing.expect(!std.math.isInf(v));
    }

    // Check CLS embedding is non-trivial
    var cls_norm: f32 = 0;
    for (0..config.hidden_size) |h| {
        cls_norm += result[h] * result[h];
    }
    try std.testing.expect(cls_norm > 0.01);
}
