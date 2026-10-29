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

//! Immutable loaded-artifact qualification for public GLiNER Decide serving.
//! Registry publication and request execution consume this same contract. An
//! alias, manifest capability, or geometry match never substitutes for the
//! weight and sidecar bytes held by the live session.
const std = @import("std");
const bundle = @import("gliner_boundary_bundle.zig");

pub const Digest = bundle.Digest;
pub const FilePin = bundle.FilePin;

pub const EncoderFamily = enum { deberta, modern_bert };

pub const Geometry = struct {
    hidden_size: u32,
    intermediate_size: u32,
    num_hidden_layers: u32,
    num_attention_heads: u32,
    vocab_size: u32,
    max_position_embeddings: u32,
};

pub const Markers = struct {
    p: i32,
    c: i32,
    e: i32,
    r: i32,
    l: i32,
    sep_struct: i32,
    sep_text: i32,
};

pub const TensorInventory = struct {
    count: usize,
    all_f32: bool,
};

pub const Sidecars = struct {
    config: Digest,
    encoder_config: Digest,
    tokenizer: Digest,
    tokenizer_config: Digest,
    special_tokens_map: ?Digest,
};

/// Exact bytes and parsed execution contract consumed by one live session.
pub const Identity = struct {
    encoder_family: EncoderFamily,
    geometry: Geometry,
    markers: Markers,
    inventory: TensorInventory,
    weight: Digest,
    weight_companion: ?Digest = null,
    sidecars: Sidecars,
};

pub const QualifiedVariant = enum { decide_deberta, decide_deberta_q8, decide_1b };
pub const Backend = enum { native, metal, cuda, other };

const legacy_weight = FilePin{ .path = "model.safetensors", .size_bytes = 1_945_828_140, .sha256 = "40a5a23ff860dc3dff426cecd1048cacdd29c648c96db209dad818e9686dc997" };
const legacy_q8_encoder = FilePin{ .path = "gliner2-encoder.Q8_0.gguf", .size_bytes = 465_145_600, .sha256 = "8d9bea7df21ab4a8a17333f51f246faab5280159e5751ebbadd81a85ec96c871" };
const legacy_q8_head = FilePin{ .path = "gliner2-head.Q8_0.gguf", .size_bytes = 62_064_576, .sha256 = "52cb6197d3dcfe5ad6276ceb037a3333d2921bbcf3dfb8289aeff82fbd5693dc" };
const legacy_config = FilePin{ .path = "config.json", .size_bytes = 594, .sha256 = "e748e5b80575471c91b3f0dd00f513ba58242544fcb7e1236e0021e61abd7673" };
const legacy_encoder_config = FilePin{ .path = "encoder_config/config.json", .size_bytes = 920, .sha256 = "bd32f1484ba5a199f7a63df44df3814b839fffcf6e64478323c4689868ef6015" };
const legacy_special_tokens = FilePin{ .path = "special_tokens_map.json", .size_bytes = 2414, .sha256 = "84ea70143f533d7e99b393d87f20010887a9ac2cba955828ef313886e4e83f4f" };
const legacy_tokenizer = FilePin{ .path = "tokenizer.json", .size_bytes = 8_333_952, .sha256 = "3ad87d9ffe669147063e70850927dd2da90249e2acc5c8527f1eb65df467bcc8" };
const legacy_tokenizer_config = FilePin{ .path = "tokenizer_config.json", .size_bytes = 3356, .sha256 = "323199a4e946039410899f3779f2aa3eaef1500213c512727ad0f623d4f21309" };

// Native and physical-Metal full-session parity cover the same exact ten-case
// capture. Runtime request admission remains bounded separately below.
pub const decide_1b_production_qualified = true;
const decide_1b_weight = FilePin{ .path = "model.safetensors", .size_bytes = 4_755_208_228, .sha256 = "02c567d791aed26550d300064c7f0c0094fd65291503c65969b45b30786e33b3" };
const decide_1b_q8_encoder = FilePin{ .path = "gliner2-encoder.Q8_0.gguf", .size_bytes = 1_118_356_576, .sha256 = "9ba4aea751bc8ce3586e70c4dfb3db93d0a850da5023b39d1bb68d21247f1694" };
const decide_1b_q8_head = FilePin{ .path = "gliner2-head.Q8_0.gguf", .size_bytes = 617_195_872, .sha256 = "9a6c1a8ae0f58158404ea6875012e0c27e5923188906492ec7d948968741f558" };
const decide_1b_config = FilePin{ .path = "config.json", .size_bytes = 464, .sha256 = "d2732928820b95649bc87f051345b2394fff87fc28c01f945610f943d6f402b2" };
const decide_1b_encoder_config = FilePin{ .path = "encoder_config/config.json", .size_bytes = 2160, .sha256 = "c2cc4c15c7504b9e15651f2a32dff82b84fa9a99060a3218671a930da0a9b50d" };
const decide_1b_tokenizer = FilePin{ .path = "tokenizer.json", .size_bytes = 3_585_055, .sha256 = "ddb379b6a4679ee16646bf0b726de9ec538d7c80ea9d4d4e33b069f19c9e1efb" };
const decide_1b_tokenizer_config = FilePin{ .path = "tokenizer_config.json", .size_bytes = 559, .sha256 = "8bb8d094c7cde84942866ef98fb5b24f7387245573a7f707c8349729484c27b3" };

fn digestMatches(digest: Digest, pin: FilePin) bool {
    digest.verify(pin) catch return false;
    return true;
}

fn sidecarsMatch(sidecars: Sidecars, config: FilePin, encoder: FilePin, tokenizer: FilePin, tokenizer_config: FilePin, special: ?FilePin) bool {
    if (!digestMatches(sidecars.config, config) or
        !digestMatches(sidecars.encoder_config, encoder) or
        !digestMatches(sidecars.tokenizer, tokenizer) or
        !digestMatches(sidecars.tokenizer_config, tokenizer_config)) return false;
    if (special) |pin| return sidecars.special_tokens_map != null and digestMatches(sidecars.special_tokens_map.?, pin);
    return sidecars.special_tokens_map == null;
}

fn isLegacyDecide(identity: Identity) bool {
    return identity.encoder_family == .deberta and
        std.meta.eql(identity.geometry, Geometry{
            .hidden_size = 1024,
            .intermediate_size = 4096,
            .num_hidden_layers = 24,
            .num_attention_heads = 16,
            .vocab_size = 128011,
            .max_position_embeddings = 512,
        }) and
        std.meta.eql(identity.markers, Markers{
            .p = 128003,
            .c = 128004,
            .e = 128005,
            .r = 128006,
            .l = 128007,
            .sep_struct = 128001,
            .sep_text = 128002,
        }) and
        identity.inventory.count == 419 and identity.inventory.all_f32 and identity.weight_companion == null and
        digestMatches(identity.weight, legacy_weight) and
        sidecarsMatch(identity.sidecars, legacy_config, legacy_encoder_config, legacy_tokenizer, legacy_tokenizer_config, legacy_special_tokens);
}

fn isDecide1B(identity: Identity) bool {
    return identity.encoder_family == .modern_bert and
        std.meta.eql(identity.geometry, Geometry{
            .hidden_size = 1792,
            .intermediate_size = 3840,
            .num_hidden_layers = 28,
            .num_attention_heads = 28,
            .vocab_size = 50378,
            .max_position_embeddings = 7999,
        }) and
        std.meta.eql(identity.markers, Markers{
            .p = 50370,
            .c = 50371,
            .e = 50372,
            .r = 50373,
            .l = 50374,
            .sep_struct = 50368,
            .sep_text = 50369,
        }) and
        identity.inventory.count == 199 and
        (if (identity.weight_companion) |head|
            !identity.inventory.all_f32 and digestMatches(identity.weight, decide_1b_q8_encoder) and digestMatches(head, decide_1b_q8_head)
        else
            identity.inventory.all_f32 and digestMatches(identity.weight, decide_1b_weight)) and
        sidecarsMatch(identity.sidecars, decide_1b_config, decide_1b_encoder_config, decide_1b_tokenizer, decide_1b_tokenizer_config, null);
}

fn isLegacyDecideQ8(identity: Identity) bool {
    if (identity.inventory.all_f32 or identity.weight_companion == null or
        !digestMatches(identity.weight, legacy_q8_encoder) or
        !digestMatches(identity.weight_companion.?, legacy_q8_head)) return false;
    // Geometry, markers, complete tensor count and every sidecar must retain
    // the reviewed legacy contract; only the two exact weight files differ.
    var fp32_shape = identity;
    fp32_shape.inventory.all_f32 = true;
    fp32_shape.weight = digestFromPin(legacy_weight);
    fp32_shape.weight_companion = null;
    return isLegacyDecide(fp32_shape);
}

/// The only shared authority used by pull-time publication and live serving.
pub fn qualifiedVariant(identity: Identity) ?QualifiedVariant {
    if (isLegacyDecide(identity)) return .decide_deberta;
    if (isLegacyDecideQ8(identity)) return .decide_deberta_q8;
    if (decide_1b_production_qualified and isDecide1B(identity)) return .decide_1b;
    return null;
}

pub fn require(identity: Identity) !QualifiedVariant {
    return qualifiedVariant(identity) orelse error.UnsupportedGlinerDecisionArtifact;
}

pub const RequestLimits = struct {
    request_items: usize = 1,
    prepared_sequences: usize = 1,
    sequence_tokens: usize = 198,
};
pub const cuda_batch_items: usize = 8;
const legacy_cuda_limits = RequestLimits{ .request_items = cuda_batch_items, .prepared_sequences = cuda_batch_items, .sequence_tokens = 512 };
pub const metal_batch_items: usize = 8;

/// The two immutable precision variants share the measured request geometry.
/// Their numerical and serving evidence remain independently captured.
pub fn requestLimits(identity: Identity) !?RequestLimits {
    const variant = try require(identity);
    if (variant == .decide_deberta_q8) return legacy_cuda_limits;
    if (variant != .decide_1b) return null;
    return .{ .prepared_sequences = 64, .sequence_tokens = 1024 };
}

pub fn supportsSpanExtraction(identity: Identity) bool {
    return qualifiedVariant(identity) == .decide_1b and identity.weight_companion == null;
}

pub fn requireRequestIdentity(identity: Identity, backend: Backend, items: usize, sequences: usize, tokens: usize) !void {
    const variant = try require(identity);
    // Native and Metal retain the existing request envelope. Metal's physical
    // batches are bounded independently; larger requests use multiple forwards.
    // The processor still enforces per-sequence and per-item split limits.
    if (variant == .decide_deberta and backend != .cuda) return;
    if (variant == .decide_deberta_q8) {
        if (backend != .cuda) return error.UnsupportedGlinerDecisionBackend;
    } else if (variant == .decide_1b and backend != .native and backend != .metal) return error.UnsupportedGlinerDecisionBackend;
    const limits = if (variant == .decide_deberta) legacy_cuda_limits else (try requestLimits(identity)).?;
    if ((backend == .cuda and variant != .decide_1b and sequences != items) or items == 0 or items > limits.request_items or sequences == 0 or sequences > limits.prepared_sequences or tokens == 0 or tokens > limits.sequence_tokens)
        return error.UnsupportedGlinerDecisionGeometry;
}

fn digestFromPin(pin: FilePin) Digest {
    return .{ .size_bytes = pin.size_bytes, .sha256 = pin.sha256[0..64].* };
}

fn legacyIdentity() Identity {
    return .{
        .encoder_family = .deberta,
        .geometry = .{ .hidden_size = 1024, .intermediate_size = 4096, .num_hidden_layers = 24, .num_attention_heads = 16, .vocab_size = 128011, .max_position_embeddings = 512 },
        .markers = .{ .p = 128003, .c = 128004, .e = 128005, .r = 128006, .l = 128007, .sep_struct = 128001, .sep_text = 128002 },
        .inventory = .{ .count = 419, .all_f32 = true },
        .weight = digestFromPin(legacy_weight),
        .sidecars = .{
            .config = digestFromPin(legacy_config),
            .encoder_config = digestFromPin(legacy_encoder_config),
            .tokenizer = digestFromPin(legacy_tokenizer),
            .tokenizer_config = digestFromPin(legacy_tokenizer_config),
            .special_tokens_map = digestFromPin(legacy_special_tokens),
        },
    };
}

test "GLiNER Decide qualification binds exact loaded legacy identity" {
    var identity = legacyIdentity();
    try std.testing.expectEqual(QualifiedVariant.decide_deberta, try require(identity));
    identity.weight.sha256[0] = if (identity.weight.sha256[0] == '0') '1' else '0';
    try std.testing.expectError(error.UnsupportedGlinerDecisionArtifact, require(identity));
    identity = legacyIdentity();
    identity.markers.l += 1;
    try std.testing.expectError(error.UnsupportedGlinerDecisionArtifact, require(identity));
    identity = legacyIdentity();
    identity.sidecars.special_tokens_map = null;
    try std.testing.expectError(error.UnsupportedGlinerDecisionArtifact, require(identity));
    identity = legacyIdentity();
    identity.sidecars.encoder_config.sha256[0] = if (identity.sidecars.encoder_config.sha256[0] == '0') '1' else '0';
    try std.testing.expectError(error.UnsupportedGlinerDecisionArtifact, require(identity));
    identity = legacyIdentity();
    identity.inventory.count -= 1;
    try std.testing.expectError(error.UnsupportedGlinerDecisionArtifact, require(identity));
}

pub const pinned_fp32_identity: Identity = blk: {
    var identity = legacyIdentity();
    identity.encoder_family = .modern_bert;
    identity.geometry = .{ .hidden_size = 1792, .intermediate_size = 3840, .num_hidden_layers = 28, .num_attention_heads = 28, .vocab_size = 50378, .max_position_embeddings = 7999 };
    identity.markers = .{ .p = 50370, .c = 50371, .e = 50372, .r = 50373, .l = 50374, .sep_struct = 50368, .sep_text = 50369 };
    identity.inventory = .{ .count = 199, .all_f32 = true };
    identity.weight = digestFromPin(decide_1b_weight);
    identity.sidecars = .{
        .config = digestFromPin(decide_1b_config),
        .encoder_config = digestFromPin(decide_1b_encoder_config),
        .tokenizer = digestFromPin(decide_1b_tokenizer),
        .tokenizer_config = digestFromPin(decide_1b_tokenizer_config),
        .special_tokens_map = null,
    };
    break :blk identity;
};

test "GLiNER Decide 1B qualification binds exact loaded identity" {
    var identity = pinned_fp32_identity;
    try std.testing.expect(isDecide1B(identity));
    try std.testing.expectEqual(QualifiedVariant.decide_1b, try require(identity));
    try std.testing.expect(supportsSpanExtraction(identity));
    try requireRequestIdentity(identity, .native, 1, 1, 1024);
    try requireRequestIdentity(identity, .metal, 1, 64, 1024);
    try std.testing.expectError(error.UnsupportedGlinerDecisionGeometry, requireRequestIdentity(identity, .native, 1, 1, 1025));
    try std.testing.expectError(error.UnsupportedGlinerDecisionGeometry, requireRequestIdentity(identity, .native, 2, 1, 700));
    try std.testing.expectError(error.UnsupportedGlinerDecisionGeometry, requireRequestIdentity(identity, .metal, 1, 65, 700));

    identity.inventory.all_f32 = false;
    try std.testing.expect(!isDecide1B(identity));
    try std.testing.expectError(error.UnsupportedGlinerDecisionArtifact, require(identity));

    identity.weight = digestFromPin(decide_1b_q8_encoder);
    identity.weight_companion = digestFromPin(decide_1b_q8_head);
    try std.testing.expectEqual(QualifiedVariant.decide_1b, try require(identity));
    const quantized = identity;
    try std.testing.expect(!supportsSpanExtraction(identity));
    try requireRequestIdentity(identity, .metal, 1, 64, 1024);
    try requireRequestIdentity(identity, .native, 1, 64, 1024);
    try std.testing.expectError(error.UnsupportedGlinerDecisionGeometry, requireRequestIdentity(identity, .metal, 1, 1, 1025));
    try std.testing.expectError(error.UnsupportedGlinerDecisionGeometry, requireRequestIdentity(identity, .metal, 1, 65, 700));
    try std.testing.expectError(error.UnsupportedGlinerDecisionGeometry, requireRequestIdentity(identity, .native, 2, 1, 700));

    identity.weight_companion = null;
    try std.testing.expectError(error.UnsupportedGlinerDecisionArtifact, require(identity));
    identity = quantized;
    identity.weight_companion.?.sha256[0] ^= 1;
    try std.testing.expectError(error.UnsupportedGlinerDecisionArtifact, require(identity));
    identity = quantized;
    identity.weight.sha256[0] ^= 1;
    try std.testing.expectError(error.UnsupportedGlinerDecisionArtifact, require(identity));
    identity = quantized;
    identity.sidecars.tokenizer.sha256[0] ^= 1;
    try std.testing.expectError(error.UnsupportedGlinerDecisionArtifact, require(identity));
    identity = quantized;
    identity.inventory.count -= 1;
    try std.testing.expectError(error.UnsupportedGlinerDecisionArtifact, require(identity));
    identity = quantized;
    identity.inventory.all_f32 = true;
    try std.testing.expectError(error.UnsupportedGlinerDecisionArtifact, require(identity));
    identity = quantized;
    identity.weight = digestFromPin(decide_1b_weight);
    identity.inventory.all_f32 = true;
    try std.testing.expectError(error.UnsupportedGlinerDecisionArtifact, require(identity));
}

test "GLiNER Decide Q8 CUDA qualification binds both exact files and bounded requests" {
    var identity = legacyIdentity();
    identity.inventory.all_f32 = false;
    identity.weight = digestFromPin(legacy_q8_encoder);
    identity.weight_companion = digestFromPin(legacy_q8_head);
    try std.testing.expectEqual(QualifiedVariant.decide_deberta_q8, try require(identity));
    for ([_]usize{ 1, 2, 4, 8 }) |n| try requireRequestIdentity(identity, .cuda, n, n, 512);
    for ([_]Backend{ .native, .metal, .other }) |backend|
        try std.testing.expectError(error.UnsupportedGlinerDecisionBackend, requireRequestIdentity(identity, backend, 1, 1, 64));
    try std.testing.expectError(error.UnsupportedGlinerDecisionGeometry, requireRequestIdentity(identity, .cuda, 9, 9, 64));
    try std.testing.expectError(error.UnsupportedGlinerDecisionGeometry, requireRequestIdentity(identity, .cuda, 1, 2, 64));
    try std.testing.expectError(error.UnsupportedGlinerDecisionGeometry, requireRequestIdentity(identity, .cuda, 1, 1, 513));
    try std.testing.expect(!supportsSpanExtraction(identity));
    const original = identity;
    identity.weight.sha256[0] ^= 1;
    try std.testing.expectError(error.UnsupportedGlinerDecisionArtifact, require(identity));
    identity = original;
    identity.weight_companion.?.sha256[0] ^= 1;
    try std.testing.expectError(error.UnsupportedGlinerDecisionArtifact, require(identity));
    identity = original;
    identity.inventory.count -= 1;
    try std.testing.expectError(error.UnsupportedGlinerDecisionArtifact, require(identity));
    identity = original;
    identity.sidecars.tokenizer.sha256[0] ^= 1;
    try std.testing.expectError(error.UnsupportedGlinerDecisionArtifact, require(identity));
    identity = original;
    identity.markers.l += 1;
    try std.testing.expectError(error.UnsupportedGlinerDecisionArtifact, require(identity));
}

test "GLiNER Decide CUDA legacy geometry stays bounded without changing native policy" {
    const identity = legacyIdentity();
    for ([_]usize{ 1, 2, 4, 8 }) |n| try requireRequestIdentity(identity, .cuda, n, n, 512);
    try std.testing.expectError(error.UnsupportedGlinerDecisionGeometry, requireRequestIdentity(identity, .cuda, 9, 9, 64));
    try std.testing.expectError(error.UnsupportedGlinerDecisionGeometry, requireRequestIdentity(identity, .cuda, 1, 2, 64));
    try std.testing.expectError(error.UnsupportedGlinerDecisionGeometry, requireRequestIdentity(identity, .cuda, 1, 1, 513));
    try requireRequestIdentity(identity, .native, 2, 2, 512);
    try requireRequestIdentity(identity, .metal, 2, 2, 512);
}

test "GLiNER Decide Metal FP32 batching preserves split tasks and rejects Q8" {
    const identity = legacyIdentity();
    // Eight is a physical forward limit, not a replacement for the established
    // public request cardinality and serial split-task envelope.
    for ([_]usize{ 1, 2, 4, 8, 9, 17, 128 }) |n| try requireRequestIdentity(identity, .metal, n, n, 512);
    try requireRequestIdentity(identity, .metal, 8, 64, 512);
    var q8 = identity;
    q8.weight = digestFromPin(legacy_q8_encoder);
    q8.weight_companion = digestFromPin(legacy_q8_head);
    q8.inventory.all_f32 = false;
    try std.testing.expectError(error.UnsupportedGlinerDecisionBackend, requireRequestIdentity(q8, .metal, 2, 2, 64));
}
