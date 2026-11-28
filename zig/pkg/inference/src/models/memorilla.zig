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

//! Strict split-projection memory bundle loading. This is a separate model;
//! it neither substitutes nor mutates the frozen encoder/decoder weights.
const std = @import("std");
const safetensors = @import("safetensors.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;
const weight_source = @import("weight_source.zig");
const native = @import("../ops/native_compute.zig");
const c_file = @import("../util/c_file.zig");
const A = std.mem.Allocator;

pub const Config = struct {
    embedding_dim: usize,
    output_dim: usize,
    num_memories: usize,
    num_heads: usize,
    num_self_attn_layers: usize,
    num_cross_attn_layers: usize,
    dropout: f64,
    retrieval_init: bool,

    pub fn validate(self: Config, release: bool) !void {
        for ([_]usize{ self.embedding_dim, self.output_dim, self.num_memories, self.num_heads }) |n|
            if (n == 0 or n > 16384) return error.InvalidMemorillaConfig;
        if (self.embedding_dim % self.num_heads != 0 or self.num_self_attn_layers != 0 or
            self.num_cross_attn_layers == 0 or self.num_cross_attn_layers > 8 or self.retrieval_init or
            !std.math.isFinite(self.dropout) or self.dropout < 0 or self.dropout >= 1)
            return error.InvalidMemorillaConfig;
        if (release and (self.embedding_dim != 2560 or self.output_dim != 4096 or
            self.num_memories != 16 or self.num_heads != 8 or self.num_cross_attn_layers != 2 or self.dropout != 0.1))
            return error.UnsupportedMemorillaRelease;
    }
};

pub fn digest(bytes: []const u8) [64]u8 {
    var raw: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &raw, .{});
    return std.fmt.bytesToHex(raw, .lower);
}

fn member(value: std.json.Value, name: []const u8) !std.json.Value {
    if (value != .object) return error.InvalidMemorillaManifest;
    return value.object.get(name) orelse error.InvalidMemorillaManifest;
}

fn equalText(value: std.json.Value, expected: []const u8) !void {
    if (value != .string or !std.mem.eql(u8, value.string, expected)) return error.InvalidMemorillaManifest;
}

const Execution = struct {
    layer_norm_epsilon: f64,
    gelu: []const u8,
    attention: []const u8,
    padding_true_means: []const u8,
    projection_layout: []const u8,
    qkv_layout: []const u8,
    encoder_pooling: []const u8,
    document_prefix: []const u8,
    question_prefix: []const u8,
    cached_embedding_dtype: []const u8,
    memory_input_dtype: []const u8,
    enable_thinking: bool,
    soft_tokens: []const u8,
};

/// Released bundles read one pooled vector per document. A research bundle
/// may instead read per-token rows from two encoder layers; it never ships as
/// a release, so the value is accepted only alongside `release: false`.
pub const released_encoder_pooling = "last-token-l2-normalized";
pub const per_token_dual_layer_encoder_pooling = "per-token-dual-layer-l2-normalized-v1";

fn validateExecution(a: A, value: std.json.Value, release: bool) !void {
    var parsed = try std.json.parseFromValue(Execution, a, value, .{});
    defer parsed.deinit();
    const v = parsed.value;
    if (v.layer_norm_epsilon != 1e-5 or v.enable_thinking) return error.InvalidMemorillaManifest;
    inline for (.{
        .{ "gelu", "erf" },                                   .{ "attention", "noncausal-mha" },        .{ "padding_true_means", "masked" },
        .{ "projection_layout", "out_features,in_features" }, .{ "qkv_layout", "split-q-k-v" },         .{ "document_prefix", "" },
        .{ "question_prefix", "" },                           .{ "cached_embedding_dtype", "float16" }, .{ "memory_input_dtype", "bfloat16" },
        .{ "soft_tokens", "input-embedding-rows" },
    }) |entry| if (!std.mem.eql(u8, @field(v, entry[0]), entry[1])) return error.InvalidMemorillaManifest;
    if (std.mem.eql(u8, v.encoder_pooling, released_encoder_pooling)) return;
    if (!release and std.mem.eql(u8, v.encoder_pooling, per_token_dual_layer_encoder_pooling)) return;
    return error.InvalidMemorillaManifest;
}

fn validateRelease(manifest: std.json.Value) !void {
    const source = try member(manifest, "source");
    try equalText(try member(source, "model"), "memorilla/Memorilla-Qwen3-8B");
    try equalText(try member(source, "revision"), "f6b15af56aa687861c8d2d03b1cd8f7979881361");
    const checkpoint = try member(source, "checkpoint");
    if (checkpoint != .string) return error.InvalidMemorillaManifest;
    var matched = false;
    for ([_][3][]const u8{
        .{ "stage1", "14044002c5cb9f14de15fac5b96f01c4044d1c238151c7dcf71cfd3deea022ed", "32497c063bc1a6667925f5ed7b7acdb08d0b1b32e96241c3f650a5a6ccd163d8" },
        .{ "stage2", "5e8476db360b1326fb6026e892e9e42c19bcf96a79208c7022c20dba8d2a5fca", "a3f3e1d42f53ccc108005587bfd39718b913b2c727128e02e254b63665c32dbd" },
        .{ "stage3", "782d4a84ac13a2a4e1b72be3284e078e752c2a595b96301741028539aa48305a", "3a7ec610dc29818c2aa29c3867d3be3e69908a191a94d0d6f0f416d564d3470e" },
        .{ "pv4", "9c343c7e7b1c143c54118796bcdb96823b8d2c8461098a2afe521947d13c37da", "1d054e866b01fd4b3345ba90787f7bfafa915dcdff4065bd9341cd44f0c03445" },
        .{ "pmv2", "04bc3f82989dc3c756bc2dc1f9a3e06941dc88e796cfe6f0aa2e06ae726ac047", "c5a6a671d8223189e1015889715e310cf912254ebfbfd698e3d14064000ce301" },
    }) |pair| if (std.mem.eql(u8, checkpoint.string, pair[0])) {
        try equalText(try member(source, "sha256"), pair[1]);
        const converted = try member(try member(manifest, "files"), "memory.safetensors");
        try equalText(try member(converted, "sha256"), pair[2]);
        matched = true;
        break;
    };
    if (!matched) return error.UnsupportedMemorillaRelease;
    const models = try member(manifest, "base_models");
    const encoder = try member(models, "encoder");
    try equalText(try member(encoder, "model"), "Qwen/Qwen3-Embedding-4B");
    try equalText(try member(encoder, "revision"), "5cf2132abc99cad020ac570b19d031efec650f2b");
    const decoder = try member(models, "decoder");
    try equalText(try member(decoder, "model"), "Qwen/Qwen3-8B");
    try equalText(try member(decoder, "revision"), "b968826d9c46dd6066d109eabc6255188de91218");
    const implementation = try member(manifest, "implementation");
    try equalText(try member(implementation, "repository"), "snap-stanford/memorilla");
    try equalText(try member(implementation, "revision"), "d8f18891a50bf33c18f04fe5167c22f51e58c9cd");
    try equalText(try member(implementation, "memory_py_sha256"), "09acdf9ec6b8be550bd90d01cd825365de5dde6d6e19eccf4ece436ed94626a7");
}

/// Size and sha256 of `bytes` must equal the manifest's `files.<name>` pin.
pub fn verifyFile(manifest: std.json.Value, name: []const u8, bytes: []const u8) !void {
    const item = try member(try member(manifest, "files"), name);
    const size = try member(item, "size_bytes");
    if (size != .integer or size.integer < 0 or size.integer != bytes.len) return error.MemorillaArtifactMismatch;
    const expected = try member(item, "sha256");
    const actual = digest(bytes);
    if (expected != .string or !std.mem.eql(u8, expected.string, &actual)) return error.MemorillaArtifactMismatch;
}

/// Extra pinned files of a research (per-token) bundle; release bundles never
/// carry them.
pub const research_copy_head_file = "copy-head.safetensors";
pub const research_candidate_file = "candidate.json";
pub const research_protocol_file = "protocol.json";
pub const research_copy_head_max_bytes: usize = 1024 * 1024 * 1024;
pub const research_candidate_max_bytes: usize = 1024 * 1024;
pub const research_protocol_max_bytes: usize = 64 * 1024;
pub const research_system_prompt_max_bytes: usize = 4096;

/// Decoding protocol of a research bundle, read from `protocol.json`.
pub const Protocol = struct {
    /// Decoder system prompt (<= 4096 bytes, never contains the memory marker).
    system_prompt: []u8,
    /// Copy head softmax temperature, 0 < t <= 1.
    copy_head_temperature: f32,
    /// Must equal `config.num_memories`.
    memory_slots: usize,
};

fn parseProtocol(a: A, bytes: []const u8, num_memories: usize) !Protocol {
    var parsed = std.json.parseFromSlice(std.json.Value, a, bytes, .{}) catch return error.InvalidMemorillaProtocol;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidMemorillaProtocol;
    const prompt = parsed.value.object.get("system_prompt") orelse return error.InvalidMemorillaProtocol;
    if (prompt != .string or prompt.string.len == 0 or prompt.string.len > research_system_prompt_max_bytes or
        std.mem.indexOf(u8, prompt.string, "<|memory|>") != null or !std.unicode.utf8ValidateSlice(prompt.string))
        return error.InvalidMemorillaProtocol;
    const temperature = parsed.value.object.get("copy_head_temperature") orelse return error.InvalidMemorillaProtocol;
    const t: f64 = switch (temperature) {
        .float => |v| v,
        .integer => |v| @floatFromInt(v),
        else => return error.InvalidMemorillaProtocol,
    };
    if (!std.math.isFinite(t) or !(t > 0 and t <= 1)) return error.InvalidMemorillaProtocol;
    const slots = parsed.value.object.get("memory_slots") orelse return error.InvalidMemorillaProtocol;
    if (slots != .integer or slots.integer <= 0 or slots.integer != num_memories) return error.InvalidMemorillaProtocol;
    const owned = try a.dupe(u8, prompt.string);
    return .{ .system_prompt = owned, .copy_head_temperature = @floatCast(t), .memory_slots = @intCast(slots.integer) };
}

pub const Bundle = struct {
    allocator: A,
    config: Config,
    release: bool,
    reader: safetensors.MMapReader,
    manifest: std.json.Parsed(std.json.Value),
    manifest_sha256: [64]u8,
    weights_sha256: [64]u8,
    /// Research bundles only: sha256 of the pinned `candidate.json`, equal to
    /// `source.candidate_sha256`; the encoder lock must carry the same value.
    candidate_sha256: ?[64]u8 = null,
    /// Research bundles only.
    protocol: ?Protocol = null,

    /// Whether `execution.encoder_pooling` reads per-token rows (research).
    pub fn perTokenPooling(self: *const Bundle) bool {
        const execution = self.manifest.value.object.get("execution") orelse return false;
        if (execution != .object) return false;
        const pooling = execution.object.get("encoder_pooling") orelse return false;
        return pooling == .string and std.mem.eql(u8, pooling.string, per_token_dual_layer_encoder_pooling);
    }

    /// Verifies and reads the three extra pinned research files. Release
    /// bundles are untouched: `validateExecution` already refused per-token
    /// pooling for them.
    fn openResearchFiles(self: *Bundle, directory: []const u8) !void {
        const a = self.allocator;
        const copy_head_path = try std.fs.path.join(a, &.{ directory, research_copy_head_file });
        defer a.free(copy_head_path);
        var copy_head = try c_file.MmapRegion.initLimited(a, copy_head_path, research_copy_head_max_bytes);
        defer copy_head.deinit();
        copy_head.discard_on_deinit = false;
        try verifyFile(self.manifest.value, research_copy_head_file, copy_head.data);

        const candidate_path = try std.fs.path.join(a, &.{ directory, research_candidate_file });
        defer a.free(candidate_path);
        const candidate_bytes = try c_file.readFileMax(a, candidate_path, research_candidate_max_bytes);
        defer a.free(candidate_bytes);
        try verifyFile(self.manifest.value, research_candidate_file, candidate_bytes);
        const candidate = digest(candidate_bytes);
        const source = try member(self.manifest.value, "source");
        const declared = try member(source, "candidate_sha256");
        if (declared != .string or !std.mem.eql(u8, declared.string, &candidate)) return error.MemorillaCandidateMismatch;

        const protocol_path = try std.fs.path.join(a, &.{ directory, research_protocol_file });
        defer a.free(protocol_path);
        const protocol_bytes = try c_file.readFileMax(a, protocol_path, research_protocol_max_bytes);
        defer a.free(protocol_bytes);
        try verifyFile(self.manifest.value, research_protocol_file, protocol_bytes);
        self.protocol = try parseProtocol(a, protocol_bytes, self.config.num_memories);
        self.candidate_sha256 = candidate;
    }

    pub fn open(a: A, directory: []const u8, allow_synthetic: bool) !Bundle {
        const manifest_path = try std.fs.path.join(a, &.{ directory, "manifest.json" });
        defer a.free(manifest_path);
        const manifest_bytes = try c_file.readFileMax(a, manifest_path, 2 * 1024 * 1024);
        defer a.free(manifest_bytes);
        var manifest = try std.json.parseFromSlice(std.json.Value, a, manifest_bytes, .{ .allocate = .alloc_always });
        errdefer manifest.deinit();
        try equalText(try member(manifest.value, "schema"), "antfly.memorilla.bundle.v1");
        const released = try member(manifest.value, "release");
        if (released != .bool) return error.InvalidMemorillaManifest;
        if (!released.bool and !allow_synthetic) return error.SyntheticMemorillaBundleNotAllowed;
        try validateExecution(a, try member(manifest.value, "execution"), released.bool);
        if (released.bool) try validateRelease(manifest.value);

        const config_path = try std.fs.path.join(a, &.{ directory, "config.json" });
        defer a.free(config_path);
        const config_bytes = try c_file.readFileMax(a, config_path, 64 * 1024);
        defer a.free(config_bytes);
        try verifyFile(manifest.value, "config.json", config_bytes);
        const parsed = try std.json.parseFromSlice(Config, a, config_bytes, .{});
        defer parsed.deinit();
        try parsed.value.validate(released.bool);
        const manifest_config = try std.json.parseFromValue(Config, a, try member(manifest.value, "config"), .{});
        defer manifest_config.deinit();
        if (!std.meta.eql(parsed.value, manifest_config.value)) return error.InvalidMemorillaManifest;

        const weights_path = try std.fs.path.join(a, &.{ directory, "memory.safetensors" });
        defer a.free(weights_path);
        var reader = try safetensors.MMapReader.openFileAbsoluteLimited(a, weights_path, 4 * 1024 * 1024 * 1024, 2 * 1024 * 1024);
        errdefer reader.deinit();
        try safetensors.validateReader(a, &reader);
        try verifyFile(manifest.value, "memory.safetensors", reader.file_bytes);
        var result = Bundle{
            .allocator = a,
            .config = parsed.value,
            .release = released.bool,
            .reader = reader,
            .manifest = manifest,
            .manifest_sha256 = digest(manifest_bytes),
            .weights_sha256 = digest(reader.file_bytes),
        };
        try result.validateInventory();
        if (!result.release and result.perTokenPooling()) try result.openResearchFiles(directory);
        return result;
    }

    pub fn deinit(self: *Bundle) void {
        if (self.protocol) |protocol| self.allocator.free(protocol.system_prompt);
        self.reader.deinit();
        self.manifest.deinit();
        self.* = undefined;
    }

    fn shape(self: *const Bundle, name: []const u8, dims: []const i64) !void {
        const actual = self.reader.header.tensors.get(name) orelse return error.MissingMemorillaTensor;
        if (!std.mem.eql(i64, actual.shape, dims)) return error.InvalidMemorillaTensor;
        if (actual.dtype != .f32 and actual.dtype != .f16 and actual.dtype != .bf16) return error.InvalidMemorillaTensor;
        if (self.release and actual.dtype != .bf16) return error.InvalidMemorillaTensor;
    }

    fn linear(self: *const Bundle, name: []const u8, out: usize, inp: usize) !void {
        var buf: [160]u8 = undefined;
        try self.shape(try std.fmt.bufPrint(&buf, "{s}.weight", .{name}), &.{ @intCast(out), @intCast(inp) });
        try self.shape(try std.fmt.bufPrint(&buf, "{s}.bias", .{name}), &.{@intCast(out)});
    }

    fn norm(self: *const Bundle, name: []const u8) !void {
        var buf: [160]u8 = undefined;
        try self.shape(try std.fmt.bufPrint(&buf, "{s}.weight", .{name}), &.{@intCast(self.config.embedding_dim)});
        try self.shape(try std.fmt.bufPrint(&buf, "{s}.bias", .{name}), &.{@intCast(self.config.embedding_dim)});
    }

    fn attention(self: *const Bundle, name: []const u8) !void {
        var buf: [160]u8 = undefined;
        for ([_][]const u8{ "q_proj", "k_proj", "v_proj", "out_proj" }) |part|
            try self.linear(try std.fmt.bufPrint(&buf, "{s}.{s}", .{ name, part }), self.config.embedding_dim, self.config.embedding_dim);
    }

    fn validateInventory(self: *const Bundle) !void {
        const d = self.config.embedding_dim;
        try self.shape("memory_queries", &.{ 1, @intCast(self.config.num_memories), @intCast(d) });
        try self.attention("query_cond_attn");
        try self.norm("query_cond_norm");
        try self.norm("doc_norm");
        for (0..self.config.num_cross_attn_layers) |i| {
            var buf: [128]u8 = undefined;
            try self.norm(try std.fmt.bufPrint(&buf, "cross_attn_norms.{d}", .{i}));
            try self.attention(try std.fmt.bufPrint(&buf, "cross_attn_layers.{d}", .{i}));
            try self.norm(try std.fmt.bufPrint(&buf, "ffn_norms.{d}", .{i}));
            try self.linear(try std.fmt.bufPrint(&buf, "ffn_layers.{d}.0", .{i}), 4 * d, d);
            try self.linear(try std.fmt.bufPrint(&buf, "ffn_layers.{d}.3", .{i}), d, 4 * d);
        }
        try self.norm("layer_norm");
        try self.linear("memory_projection", self.config.output_dim, d);
        if (self.reader.header.tensors.count() != 17 + self.config.num_cross_attn_layers * 16)
            return error.InvalidMemorillaTensorInventory;
        const inventory = try member(self.manifest.value, "tensors");
        if (inventory != .object or inventory.object.count() != self.reader.header.tensors.count())
            return error.InvalidMemorillaTensorInventory;
        const dtype = try member(self.manifest.value, "dtype");
        var parameter_count: usize = 0;
        var iter = self.reader.header.tensors.iterator();
        while (iter.next()) |entry| {
            const meta = entry.value_ptr.*;
            const info = try member(inventory, entry.key_ptr.*);
            const dims = try member(info, "shape");
            if (dims != .array or dims.array.items.len != meta.shape.len) return error.InvalidMemorillaTensorInventory;
            var count: usize = 1;
            for (dims.array.items, meta.shape) |want, actual| {
                if (want != .integer or want.integer != actual) return error.InvalidMemorillaTensorInventory;
                count = try std.math.mul(usize, count, @intCast(actual));
            }
            const name = switch (meta.dtype) {
                .f32 => "float32",
                .f16 => "float16",
                .bf16 => "bfloat16",
                else => return error.InvalidMemorillaTensor,
            };
            try equalText(dtype, name);
            try equalText(try member(info, "dtype"), name);
            parameter_count = try std.math.add(usize, parameter_count, count);
        }
        const declared = try member(self.manifest.value, "parameter_count");
        if (declared != .integer or declared.integer < 0 or declared.integer != parameter_count)
            return error.InvalidMemorillaTensorInventory;
    }

    /// The first qualification profile widens exact source weights to F32.
    /// It is intentionally distinct from BF16 activation execution.
    pub fn loadFloat32Store(self: *const Bundle) !native.WeightStore {
        const a = self.allocator;
        var result = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
        errdefer result.deinitOwned();
        var iter = self.reader.header.tensors.iterator();
        while (iter.next()) |entry| {
            const key = try a.dupe(u8, entry.key_ptr.*);
            errdefer a.free(key);
            var original = try self.reader.readTensor(entry.key_ptr.*);
            defer original.deinit();
            var converted = if (original.dtype == .f32)
                try Tensor.initFloat32(a, key, original.shape, original.asFloat32())
            else
                try weight_source.convertToF32(a, &original);
            errdefer converted.deinit();
            converted.name = key;
            for (converted.asFloat32()) |value| if (!std.math.isFinite(value)) return error.InvalidMemorillaTensor;
            try result.resident_weights.put(a, key, .{ .tensor = converted });
        }
        return result;
    }
};

test "memorilla config requires supported geometry and explicit release dimensions" {
    const valid = Config{ .embedding_dim = 8, .output_dim = 12, .num_memories = 16, .num_heads = 2, .num_self_attn_layers = 0, .num_cross_attn_layers = 2, .dropout = 0.1, .retrieval_init = false };
    try valid.validate(false);
    try std.testing.expectError(error.UnsupportedMemorillaRelease, valid.validate(true));
    var bad = valid;
    bad.embedding_dim = 7;
    try std.testing.expectError(error.InvalidMemorillaConfig, bad.validate(false));
    bad = valid;
    bad.retrieval_init = true;
    try std.testing.expectError(error.InvalidMemorillaConfig, bad.validate(false));
}

test "memorilla execution contract admits per-token pooling only for research bundles" {
    const a = std.testing.allocator;
    const template =
        \\{"layer_norm_epsilon":1e-5,"gelu":"erf","attention":"noncausal-mha","padding_true_means":"masked",
        \\"projection_layout":"out_features,in_features","qkv_layout":"split-q-k-v","encoder_pooling":"POOLING",
        \\"document_prefix":"","question_prefix":"","cached_embedding_dtype":"float16","memory_input_dtype":"bfloat16",
        \\"enable_thinking":false,"soft_tokens":"input-embedding-rows"}
    ;
    const cases = [_]struct { pooling: []const u8, release: bool, ok: bool }{
        .{ .pooling = released_encoder_pooling, .release = true, .ok = true },
        .{ .pooling = released_encoder_pooling, .release = false, .ok = true },
        .{ .pooling = per_token_dual_layer_encoder_pooling, .release = false, .ok = true },
        .{ .pooling = per_token_dual_layer_encoder_pooling, .release = true, .ok = false },
        .{ .pooling = "mean", .release = false, .ok = false },
    };
    for (cases) |case| {
        const text = try std.mem.replaceOwned(u8, a, template, "POOLING", case.pooling);
        defer a.free(text);
        var parsed = try std.json.parseFromSlice(std.json.Value, a, text, .{});
        defer parsed.deinit();
        if (case.ok) try validateExecution(a, parsed.value, case.release) else try std.testing.expectError(error.InvalidMemorillaManifest, validateExecution(a, parsed.value, case.release));
    }
}

/// Minimal synthetic bundle (`scripts/memorilla/test_exporter.py` CONFIG:
/// embedding_dim 8, output_dim 12, num_heads 2, 16 memories, two layers).
const TinyBundle = struct {
    const dim: usize = 8;
    const out_dim: usize = 12;
    const memories: usize = 16;
    const layers: usize = 2;
    const config_json = "{\"embedding_dim\":8,\"output_dim\":12,\"num_memories\":16,\"num_heads\":2,\"num_self_attn_layers\":0,\"num_cross_attn_layers\":2,\"dropout\":0.1,\"retrieval_init\":false}";
    const candidate_json = "{\"schema\":\"antfly.memorilla.encoder-lora-candidate.v1\",\"reader_sha256\":\"00\"}";
    const protocol_json = "{\"system_prompt\":\"Answer from the memory.\",\"copy_head_temperature\":0.05,\"memory_slots\":16,\"dual_layer\":24}";
    const copy_head_header = "{\"gate.bias\":{\"dtype\":\"F32\",\"shape\":[1],\"data_offsets\":[0,4]},\"gate.weight\":{\"dtype\":\"F32\",\"shape\":[1,2],\"data_offsets\":[4,12]},\"query.weight\":{\"dtype\":\"F32\",\"shape\":[2,2],\"data_offsets\":[12,28]}}";

    const Entry = struct { name: [96]u8, len: usize, shape: [3]usize, rank: usize };
    const Shapes = struct {
        entries: std.ArrayList(Entry) = .empty,

        fn add(self: *Shapes, a: A, name: []const u8, shape: []const usize) !void {
            var entry = Entry{ .name = undefined, .len = name.len, .shape = .{ 0, 0, 0 }, .rank = shape.len };
            @memcpy(entry.name[0..name.len], name);
            @memcpy(entry.shape[0..shape.len], shape);
            try self.entries.append(a, entry);
        }

        fn linear(self: *Shapes, a: A, prefix: []const u8, out: usize, inp: usize) !void {
            var buf: [96]u8 = undefined;
            try self.add(a, try std.fmt.bufPrint(&buf, "{s}.weight", .{prefix}), &.{ out, inp });
            try self.add(a, try std.fmt.bufPrint(&buf, "{s}.bias", .{prefix}), &.{out});
        }

        fn norm(self: *Shapes, a: A, prefix: []const u8) !void {
            var buf: [96]u8 = undefined;
            try self.add(a, try std.fmt.bufPrint(&buf, "{s}.weight", .{prefix}), &.{dim});
            try self.add(a, try std.fmt.bufPrint(&buf, "{s}.bias", .{prefix}), &.{dim});
        }

        fn attention(self: *Shapes, a: A, prefix: []const u8) !void {
            var buf: [96]u8 = undefined;
            for ([_][]const u8{ "q_proj", "k_proj", "v_proj", "out_proj" }) |part|
                try self.linear(a, try std.fmt.bufPrint(&buf, "{s}.{s}", .{ prefix, part }), dim, dim);
        }
    };

    const Written = struct { weights: []u8, inventory: []u8, parameter_count: usize };

    /// Writes `memory.safetensors` bytes (all F32, every value 0.5) and the
    /// manifest `tensors` inventory for the release layout scaled down.
    fn weights(a: A) !Written {
        var shapes = Shapes{};
        defer shapes.entries.deinit(a);
        try shapes.add(a, "memory_queries", &.{ 1, memories, dim });
        try shapes.attention(a, "query_cond_attn");
        try shapes.norm(a, "query_cond_norm");
        try shapes.norm(a, "doc_norm");
        var buf: [96]u8 = undefined;
        for (0..layers) |i| {
            try shapes.norm(a, try std.fmt.bufPrint(&buf, "cross_attn_norms.{d}", .{i}));
            try shapes.attention(a, try std.fmt.bufPrint(&buf, "cross_attn_layers.{d}", .{i}));
            try shapes.norm(a, try std.fmt.bufPrint(&buf, "ffn_norms.{d}", .{i}));
            try shapes.linear(a, try std.fmt.bufPrint(&buf, "ffn_layers.{d}.0", .{i}), 4 * dim, dim);
            try shapes.linear(a, try std.fmt.bufPrint(&buf, "ffn_layers.{d}.3", .{i}), dim, 4 * dim);
        }
        try shapes.norm(a, "layer_norm");
        try shapes.linear(a, "memory_projection", out_dim, dim);

        var header: std.Io.Writer.Allocating = .init(a);
        defer header.deinit();
        var inventory: std.Io.Writer.Allocating = .init(a);
        defer inventory.deinit();
        try header.writer.writeAll("{");
        try inventory.writer.writeAll("{");
        var running: usize = 0;
        for (shapes.entries.items, 0..) |entry, index| {
            const shape = entry.shape[0..entry.rank];
            var count: usize = 1;
            for (shape) |n| count *= n;
            const separator: []const u8 = if (index == 0) "" else ",";
            try header.writer.print("{s}\"{s}\":{{\"dtype\":\"F32\",\"shape\":[", .{ separator, entry.name[0..entry.len] });
            try inventory.writer.print("{s}\"{s}\":{{\"shape\":[", .{ separator, entry.name[0..entry.len] });
            for (shape, 0..) |n, i| {
                try header.writer.print("{s}{d}", .{ if (i == 0) "" else ",", n });
                try inventory.writer.print("{s}{d}", .{ if (i == 0) "" else ",", n });
            }
            try header.writer.print("],\"data_offsets\":[{d},{d}]}}", .{ running, running + count * 4 });
            try inventory.writer.writeAll("],\"dtype\":\"float32\"}");
            running += count * 4;
        }
        try header.writer.writeAll("}");
        try inventory.writer.writeAll("}");
        const header_bytes = header.written();
        const padded_len = (header_bytes.len + 7) / 8 * 8;
        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(a);
        var size: [8]u8 = undefined;
        std.mem.writeInt(u64, &size, padded_len, .little);
        try out.appendSlice(a, &size);
        try out.appendSlice(a, header_bytes);
        try out.appendNTimes(a, ' ', padded_len - header_bytes.len);
        const values = running / 4;
        for (0..values) |_| try out.appendSlice(a, std.mem.asBytes(&@as(f32, 0.5)));
        return .{ .weights = try out.toOwnedSlice(a), .inventory = try a.dupe(u8, inventory.written()), .parameter_count = values };
    }

    fn copyHeadBytes(a: A) ![]u8 {
        const payload = [_]f32{ -2, 0.5, 0.25, 1, 0, 0, 1 };
        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(a);
        var size: [8]u8 = undefined;
        std.mem.writeInt(u64, &size, copy_head_header.len, .little);
        try out.appendSlice(a, &size);
        try out.appendSlice(a, copy_head_header);
        try out.appendSlice(a, std.mem.sliceAsBytes(&payload));
        return out.toOwnedSlice(a);
    }

    fn fileEntry(w: *std.Io.Writer, first: bool, name: []const u8, bytes: []const u8) !void {
        try w.print("{s}\"{s}\":{{\"sha256\":\"{s}\",\"size_bytes\":{d}}}", .{ if (first) "" else ",", name, &digest(bytes), bytes.len });
    }

    /// Writes a complete bundle into `dir/<sub>`; `pooling` selects the
    /// execution contract, `candidate_sha256` overrides the source pin.
    fn write(a: A, io: std.Io, dir: std.Io.Dir, sub: []const u8, pooling: []const u8, candidate_override: ?[]const u8, protocol: []const u8) !void {
        try dir.createDirPath(io, sub);
        const written = try weights(a);
        defer a.free(written.weights);
        defer a.free(written.inventory);
        const copy_head = try copyHeadBytes(a);
        defer a.free(copy_head);
        var manifest: std.Io.Writer.Allocating = .init(a);
        defer manifest.deinit();
        const w = &manifest.writer;
        try w.writeAll("{\"schema\":\"antfly.memorilla.bundle.v1\",\"release\":false,\"config\":");
        try w.writeAll(config_json);
        try w.print(",\"execution\":{{\"layer_norm_epsilon\":1e-5,\"gelu\":\"erf\",\"attention\":\"noncausal-mha\",\"padding_true_means\":\"masked\",\"projection_layout\":\"out_features,in_features\",\"qkv_layout\":\"split-q-k-v\",\"encoder_pooling\":\"{s}\",\"document_prefix\":\"\",\"question_prefix\":\"\",\"cached_embedding_dtype\":\"float16\",\"memory_input_dtype\":\"bfloat16\",\"enable_thinking\":false,\"soft_tokens\":\"input-embedding-rows\"}}", .{pooling});
        try w.print(",\"source\":{{\"model\":null,\"checkpoint\":null,\"candidate_sha256\":\"{s}\"}}", .{candidate_override orelse &digest(candidate_json)});
        try w.print(",\"dtype\":\"float32\",\"parameter_count\":{d},\"tensors\":", .{written.parameter_count});
        try w.writeAll(written.inventory);
        try w.writeAll(",\"files\":{");
        try fileEntry(w, true, "config.json", config_json);
        try fileEntry(w, false, "memory.safetensors", written.weights);
        try fileEntry(w, false, research_copy_head_file, copy_head);
        try fileEntry(w, false, research_candidate_file, candidate_json);
        try fileEntry(w, false, research_protocol_file, protocol);
        try w.writeAll("}}\n");
        const Pair = struct { name: []const u8, data: []const u8 };
        for ([_]Pair{
            .{ .name = "manifest.json", .data = manifest.written() }, .{ .name = "config.json", .data = config_json },              .{ .name = "memory.safetensors", .data = written.weights },
            .{ .name = research_copy_head_file, .data = copy_head },  .{ .name = research_candidate_file, .data = candidate_json }, .{ .name = research_protocol_file, .data = protocol },
        }) |pair| {
            const path = try std.fs.path.join(a, &.{ sub, pair.name });
            defer a.free(path);
            try dir.writeFile(io, .{ .sub_path = path, .data = pair.data });
        }
    }
};

test "Memorilla Metal released-reader layout matches native soft rows" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    try TinyBundle.write(a, io, temp.dir, "pooled", released_encoder_pooling, null, TinyBundle.protocol_json);
    const directory = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", temp.sub_path[0..], "pooled" });
    defer a.free(directory);
    var bundle = try Bundle.open(a, directory, true);
    defer bundle.deinit();
    var store = try bundle.loadFloat32Store();
    defer store.deinitOwned();
    var bytes: usize = 0;
    var weights = store.resident_weights.valueIterator();
    while (weights.next()) |weight| {
        const values = weight.tensor.asFloat32Mut();
        for (values, 0..) |*value, i| value.* = @cos(@as(f32, @floatFromInt(i + 1)) * 0.23) * 0.2;
        bytes += weight.tensor.data.len;
    }
    const Compare = struct {
        fn run(cb: *const @import("../ops/ops.zig").ComputeBackend, config: Config) ![]f32 {
            const docs = try cb.fromFloat32Shape(&.{ 0, 1, 2, 3, 4, 5, 6, 7, 7, 6, 5, 4, 3, 2, 1, 0 }, &.{ 2, TinyBundle.dim });
            defer cb.free(docs);
            const question = try cb.fromFloat32Shape(&.{ 1, 0, 0, 1, 0, 1, 0, 1 }, &.{ 1, TinyBundle.dim });
            defer cb.free(question);
            const output = try @import("../pipelines/memorilla.zig").forward(cb, a, config, .{
                .documents = docs,
                .question = question,
                .padding_mask = &.{ false, false },
                .batch = 1,
                .num_documents = 2,
            }, null);
            defer cb.free(output);
            return cb.toFloat32(output, a);
        }
    };
    const expected = blk: {
        var compute = @import("../ops/native_compute.zig").NativeCompute.init(a, &store, null);
        defer compute.deinit();
        break :blk try Compare.run(&compute.computeBackend(), bundle.config);
    };
    defer a.free(expected);
    var resident = try @import("memorilla_metal.zig").Weights.init(a);
    defer resident.deinit();
    try resident.adopt(&store, bytes);
    try std.testing.expectEqual(@as(usize, 0), store.resident_weights.count());
    var cb = try resident.compute(io);
    defer cb.deinit();
    const actual = try Compare.run(&cb, bundle.config);
    defer a.free(actual);
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |want, got| try std.testing.expectApproxEqAbs(want, got, 0.0003);
}

test "memorilla research bundle verifies its pinned files and reads the protocol" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    try TinyBundle.write(a, io, temp.dir, "research", per_token_dual_layer_encoder_pooling, null, TinyBundle.protocol_json);
    try TinyBundle.write(a, io, temp.dir, "pooled", released_encoder_pooling, null, TinyBundle.protocol_json);
    const research_dir = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", temp.sub_path[0..], "research" });
    defer a.free(research_dir);
    const pooled_dir = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", temp.sub_path[0..], "pooled" });
    defer a.free(pooled_dir);

    try std.testing.expectError(error.SyntheticMemorillaBundleNotAllowed, Bundle.open(a, research_dir, false));
    var bundle = try Bundle.open(a, research_dir, true);
    defer bundle.deinit();
    try std.testing.expect(!bundle.release);
    try std.testing.expect(bundle.perTokenPooling());
    try std.testing.expectEqual(@as(usize, 17 + 16 * TinyBundle.layers), bundle.reader.header.tensors.count());
    try std.testing.expectEqualStrings(&digest(TinyBundle.candidate_json), &bundle.candidate_sha256.?);
    const protocol = bundle.protocol.?;
    try std.testing.expectEqualStrings("Answer from the memory.", protocol.system_prompt);
    try std.testing.expectEqual(@as(f32, 0.05), protocol.copy_head_temperature);
    try std.testing.expectEqual(@as(usize, 16), protocol.memory_slots);
    var store = try bundle.loadFloat32Store();
    defer store.deinitOwned();
    try std.testing.expectEqual(bundle.reader.header.tensors.count(), store.resident_weights.count());

    // A pooled synthetic bundle ignores the research files entirely.
    var pooled = try Bundle.open(a, pooled_dir, true);
    defer pooled.deinit();
    try std.testing.expect(!pooled.perTokenPooling());
    try std.testing.expect(pooled.candidate_sha256 == null and pooled.protocol == null);
    try temp.dir.deleteFile(io, "pooled/copy-head.safetensors");
    var pooled_again = try Bundle.open(a, pooled_dir, true);
    pooled_again.deinit();

    // Research pins fail closed: a replaced copy head, a candidate that no
    // longer matches `source.candidate_sha256`, a mutated protocol.
    try temp.dir.writeFile(io, .{ .sub_path = "research/copy-head.safetensors", .data = "not the pinned copy head" });
    try std.testing.expectError(error.MemorillaArtifactMismatch, Bundle.open(a, research_dir, true));
    const copy_head = try TinyBundle.copyHeadBytes(a);
    defer a.free(copy_head);
    try temp.dir.writeFile(io, .{ .sub_path = "research/copy-head.safetensors", .data = copy_head });
    try temp.dir.deleteFile(io, "research/candidate.json");
    try std.testing.expectError(error.FileNotFound, Bundle.open(a, research_dir, true));
    try temp.dir.writeFile(io, .{ .sub_path = "research/candidate.json", .data = TinyBundle.candidate_json });
    try temp.dir.writeFile(io, .{ .sub_path = "research/protocol.json", .data = "{\"system_prompt\":\"x\",\"copy_head_temperature\":0.05,\"memory_slots\":16}" });
    try std.testing.expectError(error.MemorillaArtifactMismatch, Bundle.open(a, research_dir, true));
    var reopened = try Bundle.open(a, pooled_dir, true);
    reopened.deinit();
    const bad_candidate: [64]u8 = @splat('0');
    try TinyBundle.write(a, io, temp.dir, "mismatch", per_token_dual_layer_encoder_pooling, &bad_candidate, TinyBundle.protocol_json);
    const mismatch_dir = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", temp.sub_path[0..], "mismatch" });
    defer a.free(mismatch_dir);
    try std.testing.expectError(error.MemorillaCandidateMismatch, Bundle.open(a, mismatch_dir, true));

    // Protocol shape: marker in the prompt, out-of-range temperature, wrong slot count.
    for ([_][]const u8{
        "{\"system_prompt\":\"Use <|memory|> here\",\"copy_head_temperature\":0.05,\"memory_slots\":16}",
        "{\"system_prompt\":\"ok\",\"copy_head_temperature\":1.5,\"memory_slots\":16}",
        "{\"system_prompt\":\"ok\",\"copy_head_temperature\":0,\"memory_slots\":16}",
        "{\"system_prompt\":\"ok\",\"copy_head_temperature\":0.05,\"memory_slots\":8}",
        "{\"system_prompt\":\"\",\"copy_head_temperature\":0.05,\"memory_slots\":16}",
        "{\"copy_head_temperature\":0.05,\"memory_slots\":16}",
    }, 0..) |protocol_text, index| {
        var name_buf: [32]u8 = undefined;
        const sub = try std.fmt.bufPrint(&name_buf, "protocol{d}", .{index});
        try TinyBundle.write(a, io, temp.dir, sub, per_token_dual_layer_encoder_pooling, null, protocol_text);
        const dir = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", temp.sub_path[0..], sub });
        defer a.free(dir);
        try std.testing.expectError(error.InvalidMemorillaProtocol, Bundle.open(a, dir, true));
    }
    const good_temperature = try std.mem.replaceOwned(u8, a, TinyBundle.protocol_json, "0.05", "1");
    defer a.free(good_temperature);
    try TinyBundle.write(a, io, temp.dir, "unit", per_token_dual_layer_encoder_pooling, null, good_temperature);
    const unit_dir = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", temp.sub_path[0..], "unit" });
    defer a.free(unit_dir);
    var unit = try Bundle.open(a, unit_dir, true);
    defer unit.deinit();
    try std.testing.expectEqual(@as(f32, 1), unit.protocol.?.copy_head_temperature);
}
