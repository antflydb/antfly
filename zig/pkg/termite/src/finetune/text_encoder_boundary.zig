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
const build_options = @import("build_options");
const hf_tokenizer = @import("termite_hf_tokenizer");

const c_file = @import("../util/c_file.zig");
const backends = @import("../backends/backends.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;
const ComputeBackend = @import("../ops/ops.zig").ComputeBackend;
const manifest_mod = @import("../models/manifest.zig");
const session_factory = @import("../architectures/session_factory.zig");
const bert_arch = @import("../architectures/bert.zig");
const deberta_arch = @import("../architectures/deberta.zig");
const reranker = @import("reranker.zig");

pub const BackendChoice = reranker.BackendChoice;

pub const Boundary = struct {
    hidden_in: []f32,
    attention_mask: []i64,
    token_type_ids: ?[]i64 = null,
    seq_len: usize,

    pub fn deinit(self: *Boundary, allocator: std.mem.Allocator) void {
        allocator.free(self.hidden_in);
        allocator.free(self.attention_mask);
        if (self.token_type_ids) |value| allocator.free(value);
        self.* = undefined;
    }
};

const EncoderRuntime = struct {
    allocator: std.mem.Allocator,
    session: backends.Session,
    hf_tok: *hf_tokenizer.HfTokenizer,
    max_length: usize,
    compute_backend: ComputeBackend,
    arch_config: session_factory.GenericEncoderArchConfig,

    fn deinit(self: *EncoderRuntime) void {
        self.compute_backend.deinit();
        self.session.close();
        self.hf_tok.deinitSelf();
    }
};

const EncodedPairInputs = struct {
    ids_i64: []i64,
    mask_i64: []i64,
    type_i64: ?[]i64,
    seq_len: usize,
};

pub fn encodePairTopLayerBoundary(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    backend: BackendChoice,
    query: []const u8,
    document: []const u8,
    top_layer_count: usize,
) !Boundary {
    var encoder = try openEncoder(allocator, model_dir, backend);
    defer encoder.deinit();

    const encoded_inputs = try encodePairInputs(allocator, &encoder, query, document);
    defer freeEncodedPairInputs(allocator, &encoded_inputs);
    return captureBoundaryWithEncoder(
        allocator,
        &encoder,
        encoded_inputs.ids_i64,
        encoded_inputs.mask_i64,
        encoded_inputs.type_i64,
        encoded_inputs.seq_len,
        top_layer_count,
    );
}

pub fn captureTopLayerBoundaryFromEncodedInputs(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    backend: BackendChoice,
    input_ids: []const i64,
    attention_mask: []const i64,
    token_type_ids: ?[]const i64,
    seq_len: usize,
    top_layer_count: usize,
) !Boundary {
    var encoder = try openEncoder(allocator, model_dir, backend);
    defer encoder.deinit();
    return captureBoundaryWithEncoder(
        allocator,
        &encoder,
        input_ids,
        attention_mask,
        token_type_ids,
        seq_len,
        top_layer_count,
    );
}

pub fn replayTopLayersFromBoundary(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    backend: BackendChoice,
    hidden_in: []const f32,
    attention_mask: []const i64,
    seq_len: usize,
    top_layer_count: usize,
) ![]f32 {
    if (top_layer_count == 0) return error.InvalidTopLayerCount;
    var encoder = try openEncoder(allocator, model_dir, backend);
    defer encoder.deinit();
    return replayTopLayersWithEncoder(allocator, &encoder, hidden_in, attention_mask, seq_len, top_layer_count);
}

fn openEncoder(allocator: std.mem.Allocator, model_dir: []const u8, backend: BackendChoice) !EncoderRuntime {
    var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer manifest.deinit();

    const tok_bytes = try c_file.readFileFromDir(allocator, model_dir, "tokenizer.json");
    defer allocator.free(tok_bytes);
    const hf_tok = try hf_tokenizer.HfTokenizer.loadFromBytes(allocator, tok_bytes);

    const task_override: ?session_factory.TaskOverride = if (manifest.gliner_model_type.len > 0) null else .generic;
    const session = switch (backend) {
        .native => try session_factory.createNativeSessionWithTaskOverride(allocator, model_dir, task_override),
        .mlx => try session_factory.createMlxSessionWithTaskOverride(allocator, model_dir, task_override),
        .auto => blk: {
            if (build_options.enable_mlx) {
                break :blk session_factory.createMlxSessionWithTaskOverride(allocator, model_dir, task_override) catch try session_factory.createNativeSessionWithTaskOverride(allocator, model_dir, task_override);
            }
            break :blk try session_factory.createNativeSessionWithTaskOverride(allocator, model_dir, task_override);
        },
    };
    const compute_backend = try session_factory.getComputeBackend(session, allocator);
    return .{
        .allocator = allocator,
        .session = session,
        .hf_tok = hf_tok,
        .max_length = manifest.max_position_embeddings,
        .compute_backend = compute_backend,
        .arch_config = try session_factory.getGenericEncoderArchConfig(session),
    };
}

fn replayTopLayersWithEncoder(
    allocator: std.mem.Allocator,
    encoder: *EncoderRuntime,
    hidden_in: []const f32,
    attention_mask: []const i64,
    seq_len: usize,
    top_layer_count: usize,
) ![]f32 {
    const batch: usize = 1;
    return switch (encoder.arch_config) {
        .bert => |cfg| blk: {
            const start = cfg.num_hidden_layers - @min(top_layer_count, cfg.num_hidden_layers);
            break :blk try bert_arch.forwardFromHidden(
                &encoder.compute_backend,
                allocator,
                cfg,
                hidden_in,
                attention_mask,
                batch,
                seq_len,
                start,
            );
        },
        .deberta => |cfg| blk: {
            const start = cfg.num_hidden_layers - @min(top_layer_count, cfg.num_hidden_layers);
            break :blk try deberta_arch.forwardFromHidden(
                &encoder.compute_backend,
                allocator,
                cfg,
                hidden_in,
                attention_mask,
                batch,
                seq_len,
                start,
            );
        },
    };
}

fn captureBoundaryWithEncoder(
    allocator: std.mem.Allocator,
    encoder: *EncoderRuntime,
    input_ids: []const i64,
    attention_mask: []const i64,
    token_type_ids: ?[]const i64,
    seq_len: usize,
    top_layer_count: usize,
) !Boundary {
    if (top_layer_count == 0) return error.InvalidTopLayerCount;
    const batch: usize = 1;
    const start = switch (encoder.arch_config) {
        .bert => |cfg| cfg.num_hidden_layers - @min(top_layer_count, cfg.num_hidden_layers),
        .deberta => |cfg| cfg.num_hidden_layers - @min(top_layer_count, cfg.num_hidden_layers),
    };
    const hidden_in = switch (encoder.arch_config) {
        .bert => |cfg| try bert_arch.forwardUntilLayer(
            &encoder.compute_backend,
            allocator,
            cfg,
            input_ids,
            attention_mask,
            token_type_ids,
            batch,
            seq_len,
            start,
        ),
        .deberta => |cfg| try deberta_arch.forwardUntilLayer(
            &encoder.compute_backend,
            allocator,
            cfg,
            input_ids,
            attention_mask,
            batch,
            seq_len,
            start,
        ),
    };
    return .{
        .hidden_in = hidden_in,
        .attention_mask = try allocator.dupe(i64, attention_mask),
        .token_type_ids = if (token_type_ids) |value| try allocator.dupe(i64, value) else null,
        .seq_len = seq_len,
    };
}

fn encodePairInputs(allocator: std.mem.Allocator, encoder: *EncoderRuntime, query: []const u8, document: []const u8) !EncodedPairInputs {
    const tok = encoder.hf_tok.tokenizer();
    const special = tok.specialTokens();
    var encoded = try tok.encodeForPair(allocator, query, document, encoder.max_length);
    defer encoded.deinit();

    const max_len = encoded.ids.len;
    const ids_i64 = try allocator.alloc(i64, max_len);
    errdefer allocator.free(ids_i64);
    const mask_i64 = try allocator.alloc(i64, max_len);
    errdefer allocator.free(mask_i64);
    const type_i64 = try allocator.alloc(i64, max_len);
    errdefer allocator.free(type_i64);
    for (0..max_len) |i| {
        ids_i64[i] = encoded.ids[i];
        mask_i64[i] = encoded.attention_mask[i];
        type_i64[i] = 0;
    }
    buildCrossEncoderTokenTypes(type_i64, encoded.ids, encoded.attention_mask, special.sep_id);

    return .{
        .ids_i64 = ids_i64,
        .mask_i64 = mask_i64,
        .type_i64 = type_i64,
        .seq_len = max_len,
    };
}

fn freeEncodedPairInputs(allocator: std.mem.Allocator, encoded_inputs: *const EncodedPairInputs) void {
    allocator.free(encoded_inputs.ids_i64);
    allocator.free(encoded_inputs.mask_i64);
    if (encoded_inputs.type_i64) |value| allocator.free(value);
}

fn buildCrossEncoderTokenTypes(dst: []i64, ids: []const i32, attention_mask: []const i32, sep_id: i32) void {
    var in_segment_b = false;
    var sep_count: usize = 0;
    for (ids, attention_mask, 0..) |id, mask, idx| {
        if (mask == 0) {
            dst[idx] = 0;
        } else if (in_segment_b) {
            dst[idx] = 1;
        } else {
            dst[idx] = 0;
            if (id == sep_id) {
                sep_count += 1;
                if (sep_count == 1) in_segment_b = true;
            }
        }
    }
}
