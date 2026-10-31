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

//! Artifact semantics and typed weight sources, independent of backend preparation.
const std = @import("std");
const platform = @import("antfly_platform");
const compat = @import("../io/compat.zig");
const bert = @import("../models/bert.zig");
const t5_mod = @import("../models/t5.zig");
const gpt_mod = @import("../models/gpt.zig");
const whisper_mod = @import("../models/whisper.zig");
const florence_mod = @import("../models/florence.zig");
const clip_mod = @import("../models/clip.zig");
const clap_mod = @import("../models/clap.zig");
const deberta_mod = @import("../models/deberta.zig");
const layoutlmv3_mod = @import("../models/layoutlmv3.zig");
const modern_bert_arch = @import("modern_bert.zig");
const nomic_bert_arch = @import("nomic_bert.zig");
const embedding_gemma2_arch = @import("embedding_gemma2.zig");
const gliner_boundary_model = @import("../models/gliner_boundary.zig");
const gliner_decide_qualification = @import("../models/gliner_decide_qualification.zig");
const manifest_mod = @import("../models/manifest.zig");
const tensor_store_mod = @import("../models/tensor_store.zig");
const gguf_mod = @import("../gguf/root.zig");
const c_file = @import("../util/c_file.zig");

pub const ArchType = enum {
    bert,
    modern_bert,
    nomic_bert,
    embedding_gemma2,
    deberta,
    t5,
    gpt,
    whisper,
    florence,
    clip,
    clap,
    gliner,
    gliner_modern_bert,
    gliner_boundary,
    layoutlmv3,
};

/// Architecture-specific config, tagged union.
pub const ArchConfig = union(ArchType) {
    bert: bert.Config,
    modern_bert: modern_bert_arch.Config,
    nomic_bert: nomic_bert_arch.Config,
    embedding_gemma2: embedding_gemma2_arch.Config,
    deberta: deberta_mod.Config,
    t5: t5_mod.Config,
    gpt: gpt_mod.Config,
    whisper: whisper_mod.Config,
    florence: florence_mod.Config,
    clip: clip_mod.Config,
    clap: clap_mod.Config,
    gliner: deberta_mod.Config,
    gliner_modern_bert: modern_bert_arch.Config,
    gliner_boundary: gliner_boundary_model.Config,
    layoutlmv3: layoutlmv3_mod.Config,
};

/// Backend-independent load result. Owns artifact metadata and the original
/// typed weight source; preparation selects residency, packing and schedules.
pub const LoadedModel = struct {
    manifest: manifest_mod.ModelManifest,
    architecture: ArchConfig,
    weights: ?tensor_store_mod.TensorStore,

    pub fn open(allocator: std.mem.Allocator, path: []const u8) !LoadedModel {
        var manifest = try manifest_mod.loadFromDir(allocator, path);
        errdefer manifest.deinit();
        const weights = try tensor_store_mod.openFromManifest(allocator, manifest);
        errdefer weights.deinit();
        // Reuse parsed GGUF metadata instead of mapping and parsing it again.
        var architecture = try detectArchitectureWithGgufFile(allocator, path, manifest, weights.ggufFile());
        if (architecture == .gpt) {
            if (weights.ggufFile()) |file| {
                if (weights.ggufArtifactBytes()) |bytes| refineRopeDimFromFreqs(&architecture.gpt, file, bytes);
            }
        }
        return .{ .manifest = manifest, .architecture = architecture, .weights = weights };
    }

    pub fn takeWeights(self: *LoadedModel) tensor_store_mod.TensorStore {
        const weights = self.weights.?;
        self.weights = null;
        return weights;
    }

    pub fn deinit(self: *LoadedModel) void {
        if (self.weights) |weights| weights.deinit();
        self.manifest.deinit();
        self.* = undefined;
    }
};

pub fn detectArchitecture(allocator: std.mem.Allocator, model_path: []const u8, mf: manifest_mod.ModelManifest) !ArchConfig {
    return detectArchitectureWithGgufFile(allocator, model_path, mf, null);
}

pub fn detectArchitectureWithGgufFile(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    mf: manifest_mod.ModelManifest,
    parsed_gguf: ?*const gguf_mod.format.File,
) !ArchConfig {
    // Recognize a boundary checkpoint before the legacy span branches. Public
    // qualification remains gated independently of this internal typed loader.
    try mf.requireRecognizedGlinerArchitecture();
    if (mf.gliner_architecture == .boundary) {
        const config = mf.gliner_boundary_config orelse return error.InvalidGlinerBoundaryConfig;
        return .{ .gliner_boundary = config };
    }
    // Try to read config.json for model_type
    const config_path = try std.fmt.allocPrint(allocator, "{s}/config.json", .{model_path});
    defer allocator.free(config_path);

    if (c_file.readFile(allocator, config_path)) |config_bytes| {
        defer allocator.free(config_bytes);

        if (try detectModelType(allocator, config_bytes)) |model_type| {
            defer allocator.free(model_type);
            if (std.mem.eql(u8, model_type, "extractor")) {
                if (mf.gliner_architecture == .span and mf.gliner_span_declared)
                    try verifyGlinerDecisionSnapshot(config_bytes, mf.gliner_span_config_digest);
                const wrapper = try parseLegacyGlinerWrapper(allocator, config_bytes);
                if (mf.gliner_span_encoder_family == .modern_bert) {
                    return .{ .modern_bert = try loadGlinerModernBertEncoderConfigVerified(allocator, model_path, mf.gliner_span_encoder_digest) };
                }
                // Original Fastino checkpoints keep wrapper metadata here and
                // the actual DeBERTa geometry/activation in a local sidecar.
                var cfg = try loadLegacyGlinerEncoderConfigVerified(allocator, model_path, wrapper, mf.gliner_span_encoder_digest);
                cfg.gliner_count_layer = wrapper.count_layer;
                cfg.gliner_quantized_weights = try glinerGgufMatricesQuantized(allocator, mf, parsed_gguf);
                try applyGlinerLabelTokenIds(allocator, model_path, mf, &cfg);
                return .{ .gliner = cfg };
            }
            if (mf.gliner_model_type.len > 0) {
                // Split GLiNER bundles keep the DeBERTa encoder config in
                // config.json and use antfly_inference_bundle/gliner_config sidecars
                // to identify the GLiNER wrapper.
                var cfg = try deberta_mod.parseConfig(allocator, config_bytes);
                cfg.gliner_quantized_weights = try glinerGgufMatricesQuantized(allocator, mf, parsed_gguf);
                try applyGlinerLabelTokenIds(allocator, model_path, mf, &cfg);
                return .{ .gliner = cfg };
            }
            if (embedding_gemma2_arch.isModel(model_type)) {
                if (mf.usesGgufWeights()) return error.UnsupportedEmbeddingGemma2Precision;
                return .{ .embedding_gemma2 = try embedding_gemma2_arch.parseConfig(allocator, config_bytes) };
            }
            if (modern_bert_arch.isModernBertModel(model_type)) {
                return .{ .modern_bert = try modern_bert_arch.parseConfig(allocator, config_bytes) };
            }
            if (nomic_bert_arch.isNomicBertModel(model_type)) {
                return .{ .nomic_bert = try nomic_bert_arch.parseConfig(allocator, config_bytes) };
            }
            if (deberta_mod.isDebertaModel(model_type)) {
                return .{ .deberta = try deberta_mod.parseConfig(allocator, config_bytes) };
            }
            if (t5_mod.isT5Model(model_type)) {
                return .{ .t5 = try t5_mod.parseConfig(allocator, config_bytes) };
            }
            if (gpt_mod.isGenerativeModel(model_type) or
                std.mem.eql(u8, model_type, "colqwen2") or
                std.mem.eql(u8, model_type, "jina_embeddings_v5"))
            {
                var cfg = try gpt_mod.parseConfig(allocator, config_bytes);
                if (mf.usesGgufWeights()) {
                    const gguf_path = mf.gguf_path.?;
                    if (try detectArchitectureFromOptionalGgufFile(allocator, gguf_path, parsed_gguf)) |gguf_config| {
                        switch (gguf_config) {
                            .gpt => |gguf_cfg| overlayGptStructuralConfig(&cfg, gguf_cfg),
                            else => {},
                        }
                    }
                }
                return .{ .gpt = cfg };
            }
            if (whisper_mod.isWhisperModel(model_type)) {
                return .{ .whisper = try whisper_mod.parseConfig(allocator, config_bytes) };
            }
            if (florence_mod.isFlorenceModel(model_type)) {
                return .{ .florence = try florence_mod.parseConfig(allocator, config_bytes) };
            }
            if (clip_mod.isClipModel(model_type)) {
                return .{ .clip = try clip_mod.parseConfig(allocator, config_bytes) };
            }
            if (clap_mod.isClapModel(model_type)) {
                return .{ .clap = try clap_mod.parseConfig(allocator, config_bytes) };
            }
            if (std.mem.eql(u8, model_type, "layoutlmv3")) {
                return .{ .layoutlmv3 = try layoutlmv3_mod.parseConfig(allocator, config_bytes) };
            }
            if (bert.isBertModel(model_type)) {
                return .{ .bert = try bert.parseConfig(allocator, config_bytes) };
            }
        }
    } else |err| {
        if (err == error.OutOfMemory) return err;
    }

    if (mf.usesGgufWeights()) {
        const gguf_path = mf.gguf_path.?;
        if (try detectArchitectureFromOptionalGgufFile(allocator, gguf_path, parsed_gguf)) |gguf_config| {
            return gguf_config;
        }
    }

    // Default: BERT
    return .{ .bert = makeBertConfig(mf) };
}

pub const legacy_gliner_encoder_config_max_bytes: usize = 1024 * 1024;

const LegacyGlinerWrapper = struct {
    count_layer: deberta_mod.GlinerCountLayer = .count_lstm_v2,
    /// deberta.Config defaults describe deberta-v3-base; any other backbone
    /// needs its encoder sidecar.
    base_encoder: bool = true,
};

/// Wrapper fields of a span (`SpanExtractor`) config.json that change the
/// executed graph. Unsupported values fail closed instead of running the
/// wrong head.
fn parseLegacyGlinerWrapper(allocator: std.mem.Allocator, config_bytes: []const u8) !LegacyGlinerWrapper {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, config_bytes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidGlinerConfig;
    const obj = parsed.value.object;
    var wrapper = LegacyGlinerWrapper{};
    if (obj.get("counting_layer")) |value| {
        if (value != .string) return error.InvalidGlinerConfig;
        wrapper.count_layer = std.meta.stringToEnum(deberta_mod.GlinerCountLayer, value.string) orelse
            return error.UnsupportedGlinerCountingLayer;
    }
    if (obj.get("token_pooling")) |value| {
        if (value != .string or !std.mem.eql(u8, value.string, "first")) return error.UnsupportedGlinerTokenPooling;
    }
    if (obj.get("use_moe")) |value| {
        if (value != .bool or value.bool) return error.UnsupportedGlinerMoe;
    }
    if (obj.get("span_head")) |value| if (value == .object) {
        if (value.object.get("span_mode")) |mode| {
            if (mode != .string or !std.mem.eql(u8, mode.string, "markerV0")) return error.UnsupportedGlinerSpanMode;
        }
    };
    if (obj.get("model_name")) |value| if (value == .string) {
        wrapper.base_encoder = std.mem.endsWith(u8, value.string, "deberta-v3-base");
    };
    return wrapper;
}

test "legacy GLiNER wrapper selects the counting layer and rejects unsupported heads" {
    const a = std.testing.allocator;
    const decide = try parseLegacyGlinerWrapper(a,
        \\{"architecture":"span","counting_layer":"count_lstm","model_name":"microsoft/deberta-v3-large","token_pooling":"first","use_moe":false,"span_head":{"span_mode":"markerV0"}}
    );
    try std.testing.expectEqual(deberta_mod.GlinerCountLayer.count_lstm, decide.count_layer);
    try std.testing.expect(!decide.base_encoder);
    const base = try parseLegacyGlinerWrapper(a,
        \\{"model_name":"microsoft/deberta-v3-base"}
    );
    try std.testing.expectEqual(deberta_mod.GlinerCountLayer.count_lstm_v2, base.count_layer);
    try std.testing.expect(base.base_encoder);
    try std.testing.expectError(error.UnsupportedGlinerCountingLayer, parseLegacyGlinerWrapper(a,
        \\{"counting_layer":"count_lstm_moe"}
    ));
    try std.testing.expectError(error.UnsupportedGlinerMoe, parseLegacyGlinerWrapper(a,
        \\{"use_moe":true}
    ));
}

pub fn loadLegacyGlinerEncoderConfig(allocator: std.mem.Allocator, model_path: []const u8, wrapper: LegacyGlinerWrapper) !deberta_mod.Config {
    return loadLegacyGlinerEncoderConfigVerified(allocator, model_path, wrapper, null);
}

fn loadLegacyGlinerEncoderConfigVerified(allocator: std.mem.Allocator, model_path: []const u8, wrapper: LegacyGlinerWrapper, expected: ?gliner_decide_qualification.Digest) !deberta_mod.Config {
    const io = compat.io();
    const managed_receipt = @import("../registry/managed_receipt.zig");
    var receipt = try managed_receipt.loadValidated(allocator, io, model_path);
    defer if (receipt) |*validated| validated.deinit();
    // Match manifest/ModelManager sidecar resolution: a managed receipt is
    // authoritative, and unmanaged local aliases must stay inside the model
    // root. Never resolve the wrapper's encoder-name/remote locator.
    const config_path: ?[]u8 = if (receipt) |*validated|
        if (validated.find("encoder_config/config.json")) |artifact|
            try allocator.dupe(u8, artifact.canonical_path)
        else
            null
    else
        managed_receipt.resolveContainedArtifactPath(allocator, io, model_path, "encoder_config/config.json") catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
    defer if (config_path) |path| allocator.free(path);
    // Built-in geometry is deberta-v3-base; never guess it for other backbones.
    const path = config_path orelse return if (wrapper.base_encoder) .{} else error.MissingGlinerEncoderConfig;
    const snapshot = @import("../runtime/file_snapshot.zig");
    const bytes = try snapshot.read(allocator, io, compat.cwd(), path, legacy_gliner_encoder_config_max_bytes, null);
    defer allocator.free(bytes);
    if (expected != null) try verifyGlinerDecisionSnapshot(bytes, expected);
    return deberta_mod.parseConfig(allocator, bytes);
}

fn loadGlinerModernBertEncoderConfigVerified(allocator: std.mem.Allocator, model_path: []const u8, expected: ?gliner_decide_qualification.Digest) !modern_bert_arch.Config {
    const io = compat.io();
    const managed_receipt = @import("../registry/managed_receipt.zig");
    var receipt = try managed_receipt.loadValidated(allocator, io, model_path);
    defer if (receipt) |*validated| validated.deinit();
    const config_path = if (receipt) |*validated|
        if (validated.find("encoder_config/config.json")) |artifact|
            try allocator.dupe(u8, artifact.canonical_path)
        else
            return error.MissingGlinerEncoderConfig
    else
        try managed_receipt.resolveContainedArtifactPath(allocator, io, model_path, "encoder_config/config.json");
    defer allocator.free(config_path);
    const snapshot = @import("../runtime/file_snapshot.zig");
    const bytes = try snapshot.read(allocator, io, compat.cwd(), config_path, legacy_gliner_encoder_config_max_bytes, null);
    defer allocator.free(bytes);
    if (expected != null) try verifyGlinerDecisionSnapshot(bytes, expected);
    return modern_bert_arch.parseConfig(allocator, bytes);
}

/// Whether every encoder-layer matrix in the selected GGUF is quantized. A
/// filtered export may retain dense projections; those still need mirrors to
/// avoid the Metal path's implicit Q8 staging. Embedding tables and the head
/// do not participate in the encoder linear mirror policy.
fn glinerGgufMatricesQuantized(allocator: std.mem.Allocator, mf: manifest_mod.ModelManifest, parsed_gguf: ?*const gguf_mod.format.File) !bool {
    if (!mf.usesGgufWeights()) return false;
    if (parsed_gguf) |file| return ggufEncoderLayerMatricesAllQuantized(file.tensors);
    const store = try tensor_store_mod.GgufStore.initAbsolute(allocator, mf.gguf_path.?);
    defer store.tensorStore().deinit();
    const file = store.tensorStore().ggufFile() orelse return false;
    return ggufEncoderLayerMatricesAllQuantized(file.tensors);
}

fn ggufEncoderLayerMatricesAllQuantized(tensors: []const gguf_mod.format.TensorInfo) bool {
    var found = false;
    for (tensors) |tensor| {
        if (!std.mem.startsWith(u8, tensor.name, "encoder.layer.") or tensor.dimensions.len != 2) continue;
        found = true;
        if (!tensor.tensor_type.isQuantized()) return false;
    }
    return found;
}

test "GLiNER mixed GGUF encoder matrices retain Metal mirrors" {
    var matrix_dims = [_]u64{ 1024, 1024 };
    var embedding_dims = [_]u64{ 1024, 128011 };
    const tensors = [_]gguf_mod.format.TensorInfo{
        .{ .name = "embeddings.word_embeddings.weight", .dimensions = &embedding_dims, .tensor_type = .{ .known = .F32 }, .offset = 0, .data_offset = 0 },
        .{ .name = "encoder.layer.0.attention.self.query_proj.weight", .dimensions = &matrix_dims, .tensor_type = .{ .known = .Q8_0 }, .offset = 0, .data_offset = 0 },
        .{ .name = "encoder.layer.0.attention.self.key_proj.weight", .dimensions = &matrix_dims, .tensor_type = .{ .known = .F32 }, .offset = 0, .data_offset = 0 },
    };
    try std.testing.expect(!ggufEncoderLayerMatricesAllQuantized(&tensors));
    try std.testing.expect(ggufEncoderLayerMatricesAllQuantized(tensors[0..2]));
    try std.testing.expect(!ggufEncoderLayerMatricesAllQuantized(tensors[0..1]));
}

test "GLiNER Decide Q8 bundle can use quantized encoder path" {
    const directory = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_DECIDE_Q8_BUNDLE_DIR") orelse return error.SkipZigTest;
    const allocator = std.testing.allocator;
    const path = try std.fs.path.join(allocator, &.{ directory, "gliner2-encoder.Q8_0.gguf" });
    defer allocator.free(path);
    const store = try tensor_store_mod.GgufStore.initAbsolute(allocator, path);
    defer store.tensorStore().deinit();
    const file = store.tensorStore().ggufFile() orelse return error.InvalidGguf;
    try std.testing.expect(ggufEncoderLayerMatricesAllQuantized(file.tensors));
}

fn detectArchitectureFromOptionalGgufFile(
    allocator: std.mem.Allocator,
    gguf_path: []const u8,
    parsed_gguf: ?*const gguf_mod.format.File,
) !?ArchConfig {
    if (parsed_gguf) |file| return detectArchitectureFromGgufFile(file);
    return detectArchitectureFromGguf(allocator, gguf_path);
}

test "architecture detection ignores an unselected colocated GGUF" {
    const allocator = std.testing.allocator;
    var manifest = manifest_mod.ModelManifest{
        .allocator = allocator,
        .safetensors_path = try allocator.dupe(u8, "model.safetensors"),
        // If architecture detection attempts to inspect this optional export,
        // the deliberately missing path makes the regression fail immediately.
        .gguf_path = try allocator.dupe(u8, "missing-export.gguf"),
    };
    defer manifest.deinit();

    const detected = try detectArchitectureWithGgufFile(
        allocator,
        "missing-model-directory",
        manifest,
        null,
    );
    try std.testing.expect(detected == .bert);
}

fn applyGlinerLabelTokenIds(allocator: std.mem.Allocator, model_path: []const u8, mf: manifest_mod.ModelManifest, cfg: *deberta_mod.Config) !void {
    cfg.label_marker_decision_head = mf.gliner_classification_head == .label_marker_mlp;
    if (mf.gliner_classification_head == .label_marker_mlp and mf.gliner_token_l != 0)
        cfg.classification_token_id = mf.gliner_token_l
    else if (mf.gliner_token_c != 0)
        cfg.classification_token_id = mf.gliner_token_c;
    if (mf.gliner_token_e != 0) cfg.entity_token_id = mf.gliner_token_e;
    if (mf.gliner_token_r != 0) cfg.relation_token_id = mf.gliner_token_r;

    const at_path = try std.fmt.allocPrint(allocator, "{s}/added_tokens.json", .{model_path});
    defer allocator.free(at_path);
    if (c_file.readFile(allocator, at_path)) |at_bytes| {
        defer allocator.free(at_bytes);
        const at_parsed = try std.json.parseFromSlice(std.json.Value, allocator, at_bytes, .{});
        defer at_parsed.deinit();
        const classification_marker = if (mf.gliner_classification_head == .label_marker_mlp) "[L]" else "[C]";
        if (at_parsed.value.object.get(classification_marker)) |v| {
            if (v == .integer) cfg.classification_token_id = v.integer;
        }
        if (at_parsed.value.object.get("[E]")) |v| {
            if (v == .integer) cfg.entity_token_id = v.integer;
        }
        if (at_parsed.value.object.get("[R]")) |v| {
            if (v == .integer) cfg.relation_token_id = v.integer;
        }
    } else |err| {
        if (err == error.OutOfMemory) return err;
    }
}

pub fn detectArchitectureFromGguf(allocator: std.mem.Allocator, gguf_path: []const u8) !?ArchConfig {
    const store = try tensor_store_mod.GgufStore.initAbsolute(allocator, gguf_path);
    defer store.tensorStore().deinit();

    const file = store.tensorStore().ggufFile() orelse return null;
    const detected = (try detectArchitectureFromGgufFile(file)) orelse return null;
    return switch (detected) {
        .gpt => |cfg| blk: {
            var refined = cfg;
            if (store.mmap_region) |region| {
                refineRopeDimFromFreqs(&refined, file, region.data);
            }
            break :blk .{ .gpt = refined };
        },
        else => detected,
    };
}

fn detectArchitectureFromGgufFile(file: *const gguf_mod.format.File) !?ArchConfig {
    const meta = gguf_mod.metadata.View.init(file);
    if (gpt_mod.parseGgufMetadata(meta)) |cfg| {
        var refined = cfg;
        refineGptConfigFromGgufFile(&refined, file);
        return .{ .gpt = refined };
    }
    if (bert.parseGgufMetadata(meta)) |cfg| {
        var refined = cfg;
        refineBertConfigFromGgufTensorInfo(&refined, file);
        return .{ .bert = refined };
    }
    if (t5_mod.parseGgufMetadata(meta)) |cfg| {
        return .{ .t5 = cfg };
    }
    if (whisper_mod.parseGgufMetadata(meta)) |cfg| {
        return .{ .whisper = cfg };
    }
    if (deberta_mod.parseGgufMetadata(meta)) |cfg| {
        return .{ .deberta = cfg };
    }
    if (layoutlmv3_mod.parseGgufMetadata(meta)) |cfg| {
        return .{ .layoutlmv3 = cfg };
    }
    if (florence_mod.parseGgufMetadata(meta)) |cfg| {
        return .{ .florence = cfg };
    }
    if (clip_mod.parseGgufMetadata(meta)) |cfg| {
        return .{ .clip = cfg };
    }
    if (clap_mod.parseGgufMetadata(meta)) |cfg| {
        return .{ .clap = cfg };
    }
    return null;
}

/// Detect effective RoPE dimension from rope_freqs.weight tensor.
/// Gemma 4 uses rope.dimension_count for the frequency domain and may also
/// provide custom frequency factors where a leading prefix is 1.0 and the
/// remaining lanes are ~1e30 (effectively disabling rotation). llama.cpp
/// consumes both contracts. Count that exact mask-shaped prefix to derive the
/// active rotary width while preserving the metadata-derived frequency width.
fn refineRopeDimFromFreqs(config: *gpt_mod.Config, file: *const gguf_mod.format.File, raw_data: []const u8) void {
    const tensor = findGgufTensor(file, "rope_freqs.weight") orelse return;
    if (tensor.dimensions.len < 1) return;
    switch (tensor.tensor_type) {
        .known => |k| if (k != .F32) return,
        .bitnet_tl2 => return,
        .unknown => return,
    }

    const n_vals = std.math.cast(usize, tensor.dimensions[0]) orelse return;
    const byte_offset_u64 = std.math.add(u64, file.data_region_offset, tensor.offset) catch return;
    const byte_offset = std.math.cast(usize, byte_offset_u64) orelse return;
    const byte_len = std.math.mul(usize, n_vals, @sizeOf(f32)) catch return;
    const end = std.math.add(usize, byte_offset, byte_len) catch return;
    if (end > raw_data.len) return;

    const factors = raw_data[byte_offset..end];
    var n_active: u32 = 0;
    for (0..n_vals) |i| {
        const bits = std.mem.readInt(u32, factors[i * @sizeOf(f32) ..][0..4], .little);
        const factor: f32 = @bitCast(bits);
        if (factor == 1.0) {
            n_active += 1;
        } else break;
    }
    if (n_active == 0 or n_active >= n_vals) return;
    // This runtime representation models Gemma's binary active/disabled mask,
    // not arbitrary per-lane scaling. Fail closed for any other factor table.
    for (n_active..n_vals) |i| {
        const bits = std.mem.readInt(u32, factors[i * @sizeOf(f32) ..][0..4], .little);
        const factor: f32 = @bitCast(bits);
        if (!std.math.isFinite(factor) or factor < 1.0e20) return;
    }
    config.rope_dim_override = n_active * 2; // each factor covers a pair of dimensions
    std.log.info("rope_freqs.weight: {d}/{d} active entries -> rope_dim_override={d}", .{ n_active, n_vals, config.rope_dim_override });
}

test "Gemma4 GGUF rope factors refine active lanes despite explicit frequency dimension" {
    var dimensions = [_]u64{256};
    var metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "gemma4" } },
        .{ .key = "gemma4.rope.dimension_count", .value = .{ .u32 = 512 } },
        .{ .key = "gemma4.rope.dimension_count_swa", .value = .{ .u32 = 256 } },
    };
    var tensors = [_]gguf_mod.format.TensorInfo{.{
        .name = "rope_freqs.weight",
        .dimensions = &dimensions,
        .tensor_type = .{ .known = .F32 },
        .offset = 0,
        .data_offset = 0,
    }};
    const file = gguf_mod.format.File{
        .header = .{ .version = 3, .tensor_count = 1, .metadata_count = metadata.len },
        .metadata = &metadata,
        .tensors = &tensors,
        .alignment = 32,
        .data_region_offset = 0,
    };
    var factors: [256]f32 = undefined;
    @memset(factors[0..64], 1.0);
    @memset(factors[64..], 1.0e30);

    var config = gpt_mod.Config{
        .family = .gemma,
        .attention_head_dim = 256,
        .global_head_dim = 512,
        .sliding_window = 512,
        .sliding_window_pattern = 5,
        .rope_partial_factor = 1.0,
    };
    refineRopeDimFromFreqs(&config, &file, std.mem.sliceAsBytes(factors[0..]));

    try std.testing.expectEqual(@as(u32, 128), config.rope_dim_override);
    try std.testing.expectEqual(@as(u32, 256), config.layerRopeActiveDim(0));
    try std.testing.expectEqual(@as(u32, 128), config.layerRopeActiveDim(4));
    try std.testing.expectEqual(@as(u32, 512), config.layerRopeFrequencyDim(4));
}

test "GGUF rope refinement rejects non-mask frequency factors" {
    var dimensions = [_]u64{4};
    var tensors = [_]gguf_mod.format.TensorInfo{.{
        .name = "rope_freqs.weight",
        .dimensions = &dimensions,
        .tensor_type = .{ .known = .F32 },
        .offset = 0,
        .data_offset = 0,
    }};
    const file = gguf_mod.format.File{
        .header = .{ .version = 3, .tensor_count = 1, .metadata_count = 0 },
        .metadata = &.{},
        .tensors = &tensors,
        .alignment = 32,
        .data_region_offset = 0,
    };
    var factors = [_]f32{ 1.0, 1.0, 2.0, 2.0 };
    var config = gpt_mod.Config{ .family = .gemma, .rope_dim_override = 0 };
    refineRopeDimFromFreqs(&config, &file, std.mem.sliceAsBytes(factors[0..]));
    try std.testing.expectEqual(@as(u32, 0), config.rope_dim_override);
}

pub fn overlayGptStructuralConfig(target: *gpt_mod.Config, source: gpt_mod.Config) void {
    // Lightweight config.json sidecars often declare only model_type. Keep
    // GGUF stop-token defaults when that sidecar supplies no EOS policy;
    // otherwise generation ignores turn boundaries and runs to max_tokens.
    // An explicit sidecar policy remains authoritative.
    if (target.eos_token_id < 0 and source.eos_token_id >= 0) {
        target.eos_token_id = source.eos_token_id;
        target.extra_eos_token_ids = source.extra_eos_token_ids;
        target.extra_eos_token_ids_len = source.extra_eos_token_ids_len;
    }
    if (target.family == .other or source.family != .llama or target.family == .llama) {
        target.family = source.family;
    }
    target.hidden_size = source.hidden_size;
    target.num_hidden_layers = source.num_hidden_layers;
    target.num_attention_heads = source.num_attention_heads;
    target.num_key_value_heads = source.num_key_value_heads;
    target.attention_head_dim = source.attention_head_dim;
    target.intermediate_size = source.intermediate_size;
    if (source.vocab_size != 0) {
        target.vocab_size = source.vocab_size;
    }
    target.max_position_embeddings = source.max_position_embeddings;
    if (source.sliding_window != 0 or target.sliding_window == 0) {
        target.sliding_window = source.sliding_window;
    }
    target.num_local_experts = source.num_local_experts;
    target.num_experts_per_tok = source.num_experts_per_tok;
    if (target.family == .other or source.family != .llama or target.family == .llama) {
        target.norm_type = source.norm_type;
        target.position_encoding = source.position_encoding;
        target.activation = source.activation;
        target.norm_eps = source.norm_eps;
        target.weight_prefix = source.weight_prefix;
    }
    target.norm_weight_offset = source.norm_weight_offset;
    if (source.rope_theta != 10000.0 or target.rope_theta == 10000.0) {
        target.rope_theta = source.rope_theta;
    }
    // Gemma 4: overlay per-layer GQA and shared KV fields from GGUF.
    if (source.num_kv_shared_layers > 0) target.num_kv_shared_layers = source.num_kv_shared_layers;
    if (source.global_head_dim > 0) target.global_head_dim = source.global_head_dim;
    if (source.num_global_key_value_heads > 0) target.num_global_key_value_heads = source.num_global_key_value_heads;
    // A scalar GGUF feed_forward_length means every layer uses the same size.
    // Keep zero authoritative here: HF Gemma 4 config.json derives a doubled
    // shared-tail size, but some valid GGUF conversions store uniform 10240-wide
    // FFNs. Retaining the sidecar's 20480 makes quant kernels read past the
    // 10240-wide weight and can crash the Metal backend.
    target.shared_layer_intermediate_size = source.shared_layer_intermediate_size;
    if (source.sliding_window_pattern != 6) target.sliding_window_pattern = source.sliding_window_pattern;
    if (source.rope_local_theta != 10000.0 or target.rope_local_theta == 10000.0) {
        target.rope_local_theta = source.rope_local_theta;
    }
    // Gemma 4: Per-Layer Embeddings (PLE).
    if (source.ple_hidden_size > 0) target.ple_hidden_size = source.ple_hidden_size;
    // Active RoPE lanes refined from a mask-shaped rope_freqs.weight tensor.
    if (source.rope_dim_override > 0) target.rope_dim_override = source.rope_dim_override;

    // Fields below describe the GGUF artifact itself rather than a preference, so the
    // GGUF-derived value wins over config.json. `source` is always a fully GGUF-derived
    // config here; the alternative is silently running weights under a description that
    // does not match them.
    //
    // weight_tying is set by observing whether the file carries a separate output.weight.
    // Gemma 3 GGUFs have no output.weight, and their config.json omits tie_word_embeddings,
    // so without this the runtime looks for a lm_head.weight that does not exist.
    target.weight_tying = source.weight_tying;
    // rope_partial_factor is derived from rope.dimension_count against the head dim, which
    // is how llama.cpp reads the same file. unsloth's Gemma 4 config.json claims 0.25 while
    // its GGUF declares full-width rotation, and honoring config.json produced empty output.
    target.rope_partial_factor = source.rope_partial_factor;
    // Softcapping is often absent from GGUF metadata, so only a positive value is authoritative.
    if (source.final_logit_softcapping != 0.0) target.final_logit_softcapping = source.final_logit_softcapping;
}

pub fn refineGptConfigFromGgufTensorInfo(config: *gpt_mod.Config, file: *const gguf_mod.format.File) void {
    refineGemma4AttentionKEqualVFromGgufTensors(config, file);
    if (findGgufTensor(file, "token_embd.weight")) |tensor| {
        if (tensor.dimensions.len >= 2) {
            config.vocab_size = @intCast(tensor.dimensions[tensor.dimensions.len - 1]);
            config.hidden_size = @intCast(tensor.dimensions[tensor.dimensions.len - 2]);
        }
        // No separate output.weight → lm_head reuses embedding weights.
        if (findGgufTensor(file, "output.weight") == null) {
            config.weight_tying = true;
        }
        return;
    }
    if (findGgufTensor(file, "tok_embeddings.weight")) |tensor| {
        if (tensor.dimensions.len >= 2) {
            config.vocab_size = @intCast(tensor.dimensions[tensor.dimensions.len - 1]);
            config.hidden_size = @intCast(tensor.dimensions[tensor.dimensions.len - 2]);
        }
        if (findGgufTensor(file, "output.weight") == null) {
            config.weight_tying = true;
        }
        return;
    }
    if (findGgufTensor(file, "output.weight")) |tensor| {
        if (tensor.dimensions.len >= 2) {
            config.vocab_size = @intCast(tensor.dimensions[tensor.dimensions.len - 1]);
            config.hidden_size = @intCast(tensor.dimensions[tensor.dimensions.len - 2]);
        }
    }
}

fn refineGptConfigFromGgufFile(config: *gpt_mod.Config, file: *const gguf_mod.format.File) void {
    refineGptConfigFromGgufTensorInfo(config, file);
}

/// BERT-family GGUF conversions (e.g. bge-m3) may omit `bert.vocab_size`;
/// the embedding tensor's row count is authoritative.
fn refineBertConfigFromGgufTensorInfo(config: *bert.Config, file: *const gguf_mod.format.File) void {
    if (findGgufTensor(file, "token_embd.weight")) |tensor| {
        if (tensor.dimensions.len >= 2) {
            config.vocab_size = @intCast(tensor.dimensions[tensor.dimensions.len - 1]);
        }
    }
}

fn findGgufTensor(file: *const gguf_mod.format.File, name: []const u8) ?*const gguf_mod.format.TensorInfo {
    for (file.tensors) |*tensor| {
        if (std.mem.eql(u8, tensor.name, name)) return tensor;
    }
    return null;
}

test "gemma4 gguf explicit shared-tail k weights do not disable shared kv" {
    var emb_dims = [_]u64{ 1536, 262144 };
    var k_dims = [_]u64{ 1536, 256 };
    var tensors = [_]gguf_mod.format.TensorInfo{
        .{
            .name = "token_embd.weight",
            .dimensions = &emb_dims,
            .tensor_type = .{ .known = .F16 },
            .offset = 0,
            .data_offset = 64,
        },
        .{
            .name = "blk.15.attn_k.weight",
            .dimensions = &k_dims,
            .tensor_type = .{ .known = .F16 },
            .offset = 0,
            .data_offset = 64,
        },
    };
    const file = gguf_mod.format.File{
        .header = .{ .version = 3, .tensor_count = tensors.len, .metadata_count = 0 },
        .metadata = &.{},
        .tensors = &tensors,
        .alignment = 32,
        .data_region_offset = 64,
    };

    var config = gpt_mod.Config{
        .family = .gemma,
        .num_hidden_layers = 35,
        .num_attention_heads = 8,
        .num_key_value_heads = 1,
        .attention_head_dim = 256,
        .global_head_dim = 512,
        .sliding_window = 512,
        .sliding_window_pattern = 5,
        .num_kv_shared_layers = 20,
        .shared_layer_intermediate_size = 12288,
    };

    refineGptConfigFromGgufFile(&config, &file);

    try std.testing.expectEqual(@as(u32, 20), config.num_kv_shared_layers);
    try std.testing.expect(config.layerUsesSharedTail(15));
    try std.testing.expect(config.layerSharesKv(15));
    try std.testing.expectEqual(@as(?usize, 13), config.kvDonorLayerIndex(15));
    try std.testing.expectEqual(@as(u32, 12288), config.intermediateSize(15));
}

pub fn sameDigest(left: gliner_decide_qualification.Digest, right: gliner_decide_qualification.Digest) bool {
    return left.size_bytes == right.size_bytes and std.mem.eql(u8, &left.sha256, &right.sha256);
}

pub fn verifyGlinerDecisionSnapshot(bytes: []const u8, expected: ?gliner_decide_qualification.Digest) !void {
    const wanted = expected orelse return error.MissingGlinerDecisionIdentity;
    if (!sameDigest(gliner_decide_qualification.Digest.of(bytes), wanted))
        return error.GlinerDecisionArtifactMismatch;
}

pub fn detectModelType(allocator: std.mem.Allocator, json: []const u8) !?[]const u8 {
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, json, .{}) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return null,
    };
    defer parsed.deinit();

    if (parsed.value != .object) return null;
    const value = parsed.value.object.get("model_type") orelse return null;
    if (value != .string) return null;
    return try allocator.dupe(u8, value.string);
}

pub fn makeBertConfig(mf: manifest_mod.ModelManifest) bert.Config {
    return .{
        .model_type = mf.bert_model_type,
        .vocab_size = mf.bert_vocab_size,
        .hidden_size = mf.hidden_size,
        .num_hidden_layers = mf.num_hidden_layers,
        .num_attention_heads = mf.num_attention_heads,
        .intermediate_size = mf.intermediate_size,
        .max_position_embeddings = mf.max_position_embeddings,
        .type_vocab_size = mf.bert_type_vocab_size,
        .layer_norm_eps = mf.bert_layer_norm_eps,
        .num_labels = mf.num_labels,
        .pad_token_id = mf.bert_pad_token_id,
        .position_embedding_offset = mf.bert_position_embedding_offset,
        .position_id_mode = if (mf.bert_model_type == .roberta) .roberta_padding else .absolute,
    };
}

fn refineGemma4AttentionKEqualVFromGgufTensors(config: *gpt_mod.Config, file: *const gguf_mod.format.File) void {
    if (config.family != .gemma or !config.usesGemmaSlidingAttention()) return;

    const layer = firstFullAttentionLayer(config.*) orelse return;
    if (hasGgufLayerWeight(file, layer, "attn_v.weight", "self_attn.v_proj.weight")) {
        config.attention_k_eq_v = false;
        return;
    }
    if (hasGgufLayerWeight(file, layer, "attn_k.weight", "self_attn.k_proj.weight")) {
        config.attention_k_eq_v = true;
    }
}

fn firstFullAttentionLayer(config: gpt_mod.Config) ?usize {
    if (config.num_hidden_layers == 0) return null;
    for (0..config.num_hidden_layers) |layer| {
        if (!config.layerUsesSlidingAttention(layer)) return layer;
    }
    return null;
}

fn hasGgufLayerWeight(file: *const gguf_mod.format.File, layer: usize, comptime raw_suffix: []const u8, comptime hf_suffix: []const u8) bool {
    var raw_buf: [128]u8 = undefined;
    const raw_name = std.fmt.bufPrint(&raw_buf, "blk.{d}.{s}", .{ layer, raw_suffix }) catch return false;
    if (findGgufTensor(file, raw_name) != null) return true;

    var hf_buf: [160]u8 = undefined;
    const hf_name = std.fmt.bufPrint(&hf_buf, "model.layers.{d}.{s}", .{ layer, hf_suffix }) catch return false;
    return findGgufTensor(file, hf_name) != null;
}

test "LoadedModel transfers the typed weight source independently of metadata and unwinds failures" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "config.json", .data = "{\"model_type\":\"bert\",\"hidden_size\":4,\"num_hidden_layers\":1,\"num_attention_heads\":1,\"intermediate_size\":8,\"vocab_size\":2}" });
    const header = "{\"embeddings.word_embeddings.weight\":{\"dtype\":\"F32\",\"shape\":[2,4],\"data_offsets\":[0,32]}}";
    const padded_len = comptime std.mem.alignForward(usize, header.len, 8);
    var bytes: [8 + padded_len + 32]u8 = undefined;
    @memset(&bytes, ' ');
    std.mem.writeInt(u64, bytes[0..8], padded_len, .little);
    @memcpy(bytes[8..][0..header.len], header);
    const payload = [_]f32{ 1, 2, 3, 4, 5, 6, 7, 8 };
    @memcpy(bytes[8 + padded_len ..], std.mem.asBytes(&payload));
    try tmp.dir.writeFile(io, .{ .sub_path = "model.safetensors", .data = &bytes });
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer allocator.free(path);
    const Check = struct {
        fn run(a: std.mem.Allocator, model_path: []const u8) !void {
            var model = try LoadedModel.open(a, model_path);
            var owns_model = true;
            defer if (owns_model) model.deinit();
            try std.testing.expect(model.architecture == .bert);
            try std.testing.expectEqual(@as(u32, 4), model.architecture.bert.hidden_size);
            const weights = model.takeWeights();
            defer weights.deinit();
            model.deinit();
            owns_model = false;
            const source = (try weights.weightSource()).?;
            var weight = try source.getTensor("embeddings.word_embeddings.weight");
            defer weight.deinit();
            try std.testing.expectEqualSlices(i64, &.{ 2, 4 }, weight.tensor.shape);
            try std.testing.expectEqualSlices(u8, std.mem.asBytes(&[_]f32{ 1, 2, 3, 4, 5, 6, 7, 8 }), weight.tensor.data);
        }
    };
    try platform.allocator.checkAllAllocationFailures(allocator, Check.run, .{path});
    try Check.run(allocator, path);
}

test "context generation keeps GGUF EOS defaults with a minimal model sidecar" {
    const allocator = std.testing.allocator;
    var gguf_config = gpt_mod.Config{ .eos_token_id = 1 };
    gguf_config.extra_eos_token_ids[0] = 106;
    gguf_config.extra_eos_token_ids_len = 1;
    var minimal = try gpt_mod.parseConfig(allocator,
        \\{"model_type":"gemma4"}
    );
    overlayGptStructuralConfig(&minimal, gguf_config);
    try std.testing.expect(minimal.isEosToken(1));
    try std.testing.expect(minimal.isEosToken(106));
    try std.testing.expect(!minimal.isEosToken(105));

    var explicit = try gpt_mod.parseConfig(allocator,
        \\{"model_type":"gemma4","eos_token_id":[17,23]}
    );
    overlayGptStructuralConfig(&explicit, gguf_config);
    try std.testing.expect(explicit.isEosToken(17));
    try std.testing.expect(explicit.isEosToken(23));
    try std.testing.expect(!explicit.isEosToken(1));
    try std.testing.expect(!explicit.isEosToken(106));
}
