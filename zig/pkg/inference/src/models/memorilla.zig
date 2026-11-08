// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

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

fn validateExecution(a: A, value: std.json.Value) !void {
    var parsed = try std.json.parseFromValue(Execution, a, value, .{});
    defer parsed.deinit();
    const v = parsed.value;
    if (v.layer_norm_epsilon != 1e-5 or v.enable_thinking) return error.InvalidMemorillaManifest;
    inline for (.{
        .{ "gelu", "erf" },                                   .{ "attention", "noncausal-mha" },          .{ "padding_true_means", "masked" },
        .{ "projection_layout", "out_features,in_features" }, .{ "qkv_layout", "split-q-k-v" },           .{ "encoder_pooling", "last-token-l2-normalized" },
        .{ "document_prefix", "" },                           .{ "question_prefix", "" },                 .{ "cached_embedding_dtype", "float16" },
        .{ "memory_input_dtype", "bfloat16" },                .{ "soft_tokens", "input-embedding-rows" },
    }) |entry| if (!std.mem.eql(u8, @field(v, entry[0]), entry[1])) return error.InvalidMemorillaManifest;
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

fn verifyFile(manifest: std.json.Value, name: []const u8, bytes: []const u8) !void {
    const item = try member(try member(manifest, "files"), name);
    const size = try member(item, "size_bytes");
    if (size != .integer or size.integer < 0 or size.integer != bytes.len) return error.MemorillaArtifactMismatch;
    const expected = try member(item, "sha256");
    const actual = digest(bytes);
    if (expected != .string or !std.mem.eql(u8, expected.string, &actual)) return error.MemorillaArtifactMismatch;
}

pub const Bundle = struct {
    allocator: A,
    config: Config,
    release: bool,
    reader: safetensors.MMapReader,
    manifest: std.json.Parsed(std.json.Value),
    manifest_sha256: [64]u8,
    weights_sha256: [64]u8,

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
        try validateExecution(a, try member(manifest.value, "execution"));
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
        return result;
    }

    pub fn deinit(self: *Bundle) void {
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
