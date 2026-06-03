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
const deberta_arch = @import("../architectures/deberta.zig");
const gliner_head = @import("../architectures/gliner_head.zig");
const deberta_config = @import("../models/deberta.zig");
const safetensors = @import("../models/safetensors.zig");
const wasm_compute = @import("../ops/wasm_compute.zig");
const web_runtime = @import("runtime_state.zig");
const web_weights = @import("weight_loader.zig");

pub fn load(
    allocator: std.mem.Allocator,
    runtime: *web_runtime.Runtime,
    st_data: []const u8,
    config_json: []const u8,
) !u32 {
    const config = try deberta_config.parseConfig(allocator, config_json);

    const result = try safetensors.parseHeader(allocator, st_data);
    var header = result.header;
    defer header.deinit();
    const data_offset = result.data_offset;

    var compute = wasm_compute.WasmCompute.init(allocator);

    var it = header.tensors.iterator();
    while (it.next()) |entry| {
        const name = entry.key_ptr.*;
        const meta = entry.value_ptr.*;

        if (std.mem.endsWith(u8, name, ".position_ids")) continue;

        const abs_start: usize = @intCast(data_offset + meta.data_start);
        const abs_end: usize = @intCast(data_offset + meta.data_end);
        if (abs_end > st_data.len) continue;
        const raw = st_data[abs_start..abs_end];

        const n_elements = blk: {
            var count: usize = 1;
            for (meta.shape) |dim| count *= @intCast(dim);
            break :blk count;
        };

        try web_weights.registerSafetensorsWeight(allocator, &compute, stripPrefix(name), meta.dtype, raw, n_elements);
    }

    return runtime.storeModel(compute, .{ .deberta = config });
}

pub fn run(
    allocator: std.mem.Allocator,
    model: *web_runtime.Model,
    input_ids: []const i64,
    attention_mask: []const i64,
    words_mask: []const i64,
    span_idx: []const i64,
    batch: u32,
    seq_len: u32,
    out_logits_ptr: [*]f32,
    out_meta_ptr: [*]u32,
) !u32 {
    const config = switch (model.config) {
        .deberta => |cfg| cfg,
        .bert, .clap, .clip, .florence, .gpt, .t5, .whisper => return error.UnsupportedModelType,
    };

    var cb = model.compute.computeBackend();

    const hidden = try deberta_arch.forward(
        &cb,
        allocator,
        config,
        input_ids,
        attention_mask,
        batch,
        seq_len,
    );
    defer allocator.free(hidden);

    const head_result = try gliner_head.forward(
        &cb,
        allocator,
        hidden,
        input_ids,
        words_mask,
        span_idx,
        batch,
        seq_len,
        config.hidden_size,
        config.entity_token_id,
    );
    defer allocator.free(head_result.logits);

    const n_logits: u32 = @intCast(head_result.logits.len);
    @memcpy(out_logits_ptr[0..n_logits], head_result.logits);
    out_meta_ptr[0] = @intCast(head_result.num_words);
    out_meta_ptr[1] = @intCast(head_result.max_width);
    out_meta_ptr[2] = @intCast(head_result.num_labels);
    return n_logits;
}

fn stripPrefix(name: []const u8) []const u8 {
    if (std.mem.startsWith(u8, name, "encoder."))
        return name["encoder.".len..];
    return name;
}
