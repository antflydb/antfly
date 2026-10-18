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

//! Backend-neutral identity for the exact weights, tokenizer, media processor,
//! and embedding recipe. Hash once during admitted model construction.
const std = @import("std");
const file = @import("../util/c_file.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const receipt = @import("../registry/managed_receipt.zig");
pub const recipe = "embeddinggemma2-f32-mean-v6-webm-presentation";
/// CUDA also uses BF16-specific token scaling and media preparation. Version
/// this complete execution recipe independently of the qualified F32 route.
pub const cuda_recipe = "embeddinggemma2-cuda-bf16-mean-v2-audio-frontend";

pub fn forBackend(artifact_identity: [64]u8, backend: @import("../backends/backends.zig").BackendType) [64]u8 {
    if (backend != .cuda) return artifact_identity;
    return forCudaPolicy(artifact_identity, @import("../ops/cuda/embedding_gemma2_policy.zig").configured());
}

pub fn forCudaPolicy(artifact_identity: [64]u8, policy: @import("../ops/cuda/embedding_gemma2_policy.zig").Policy) [64]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(cuda_recipe);
    if (policy.arithmeticBits() != 0) {
        hash.update("\x00optimized-v1");
        const bits = policy.arithmeticBits();
        hash.update(&.{@as(u8, @truncate(bits))});
        if (bits > 255) hash.update(&.{@as(u8, @intCast(bits >> 8))});
    }
    if (policy.extended_flash) hash.update("\x00extended-dispatch-v3");
    if (policy.packed_projections) hash.update("\x00packed-consumers-v3");
    if (policy.fused_backbone) hash.update("\x00vision-residual-v2");
    if (policy.audio_conv_gemm) hash.update("\x00audio-conv-tiled-f32-v1");
    if (policy.audio_conv_bf16) hash.update("\x00audio-conv-bf16-wmma-v1");
    if (policy.audio_graph) hash.update("\x00audio-conformer-graph-v1");
    if (policy.audio_frontend_fused) hash.update("\x00audio-frontend-fused-v1");
    if (policy.audio_fft) hash.update("\x00audio-fft512-v1");
    if (policy.spatial_pool) hash.update("\x00spatial-pool-v1");
    if (policy.tuned_gemm) hash.update("\x00tuned-bf16-output-v1");
    if (policy.image_batch or policy.media_batch) hash.update("\x00batched-patches-v2");
    if (policy.media_batch) hash.update("\x00audio-subsample-batch-v2");
    if (policy.graph_replay) hash.update("\x00media-graphs-v3");
    if (policy.native_vision) hash.update("\x00native-vision-bf16-v2");
    hash.update(&.{0});
    hash.update(&artifact_identity);
    return std.fmt.bytesToHex(hash.finalResult(), .lower);
}

pub const sidecars = [_][]const u8{ "config.json", "tokenizer.json", "tokenizer_config.json", "processor_config.json", "config_sentence_transformers.json", "1_Pooling/config.json" };
pub const Snapshot = struct { digest: [64]u8, signature: [32]u8 };

fn same(a: std.json.Value, b: std.json.Value) bool {
    if ((a == .integer or a == .float) and (b == .integer or b == .float)) {
        const x: f64 = if (a == .integer) @floatFromInt(a.integer) else a.float;
        const y: f64 = if (b == .integer) @floatFromInt(b.integer) else b.float;
        return x == y;
    }
    if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
    return switch (a) {
        .null => true,
        .bool => a.bool == b.bool,
        .string => std.mem.eql(u8, a.string, b.string),
        .array => blk: {
            if (a.array.items.len != b.array.items.len) break :blk false;
            for (a.array.items, b.array.items) |x, y| if (!same(x, y)) break :blk false;
            break :blk true;
        },
        .object => blk: {
            if (a.object.count() != b.object.count()) break :blk false;
            for (a.object.keys(), a.object.values()) |key, value| if (!same(value, b.object.get(key) orelse break :blk false)) break :blk false;
            break :blk true;
        },
        else => false,
    };
}

pub fn validateProcessor(a: std.mem.Allocator, bytes: []const u8) !void {
    const parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{ .duplicate_field_behavior = .@"error" });
    defer parsed.deinit();
    const reference = try std.json.parseFromSlice(std.json.Value, a, @embedFile("embedding_gemma2_processor.json"), .{});
    defer reference.deinit();
    if (parsed.value != .object) return error.InvalidEmbeddingGemma2Processor;
    for ([_][]const u8{ "processor_class", "audio_ms_per_token", "audio_seq_length", "image_seq_length", "feature_extractor", "image_processor", "video_processor" }) |key| {
        if (!same(parsed.value.object.get(key) orelse return error.InvalidEmbeddingGemma2Processor, reference.value.object.get(key).?)) return error.InvalidEmbeddingGemma2Processor;
    }
}

pub fn validateTokenizer(a: std.mem.Allocator, bytes: []const u8) !void {
    const parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{ .duplicate_field_behavior = .@"error" });
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidEmbeddingGemma2Tokenizer;
    const processor = parsed.value.object.get("post_processor") orelse return error.InvalidEmbeddingGemma2Tokenizer;
    const reference = try std.json.parseFromSlice(std.json.Value, a,
        \\{"type":"TemplateProcessing","single":[{"SpecialToken":{"id":"<bos>","type_id":0}},{"Sequence":{"id":"A","type_id":0}},{"SpecialToken":{"id":"<eos>","type_id":0}}],"special_tokens":{"<bos>":{"id":"<bos>","ids":[2],"tokens":["<bos>"]},"<eos>":{"id":"<eos>","ids":[1],"tokens":["<eos>"]}}}
    , .{});
    defer reference.deinit();
    if (processor != .object) return error.InvalidEmbeddingGemma2Tokenizer;
    for ([_][]const u8{ "type", "single", "special_tokens" }) |key| if (!same(processor.object.get(key) orelse return error.InvalidEmbeddingGemma2Tokenizer, reference.value.object.get(key).?)) return error.InvalidEmbeddingGemma2Tokenizer;
    const added = parsed.value.object.get("added_tokens") orelse return error.InvalidEmbeddingGemma2Tokenizer;
    if (added != .array) return error.InvalidEmbeddingGemma2Tokenizer;
    const required = [_]struct { id: i64, token: []const u8 }{
        .{ .id = 0, .token = "<pad>" },          .{ .id = 1, .token = "<eos>" },         .{ .id = 2, .token = "<bos>" },
        .{ .id = 255999, .token = "<|image>" },  .{ .id = 256000, .token = "<|audio>" }, .{ .id = 258880, .token = "<|image|>" },
        .{ .id = 258881, .token = "<|audio|>" }, .{ .id = 258882, .token = "<image|>" }, .{ .id = 258883, .token = "<audio|>" },
        .{ .id = 258884, .token = "<|video|>" },
    };
    for (required) |want| {
        var found = false;
        for (added.array.items) |entry| {
            if (entry != .object) return error.InvalidEmbeddingGemma2Tokenizer;
            const id = entry.object.get("id") orelse return error.InvalidEmbeddingGemma2Tokenizer;
            if (id != .integer) return error.InvalidEmbeddingGemma2Tokenizer;
            if (id.integer != want.id) continue;
            const content = entry.object.get("content") orelse return error.InvalidEmbeddingGemma2Tokenizer;
            const special = entry.object.get("special") orelse return error.InvalidEmbeddingGemma2Tokenizer;
            if (found or content != .string or !std.mem.eql(u8, content.string, want.token) or special != .bool or !special.bool) return error.InvalidEmbeddingGemma2Tokenizer;
            found = true;
        }
        if (!found) return error.InvalidEmbeddingGemma2Tokenizer;
    }
}

pub fn signature(a: std.mem.Allocator, io: std.Io, directory: []const u8, weights: []const u8) ![32]u8 {
    const actual_weights = try receipt.resolveRegularFilePath(a, io, weights);
    defer a.free(actual_weights);
    const contained_weights = try receipt.resolveContainedArtifactPath(a, io, directory, std.fs.path.basename(weights));
    defer a.free(contained_weights);
    if (!std.mem.eql(u8, actual_weights, contained_weights)) return error.ModelArtifactOutsideRoot;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    for (0..sidecars.len + 1) |i| {
        const path = try receipt.resolveContainedArtifactPath(a, io, directory, if (i == sidecars.len) std.fs.path.basename(weights) else sidecars[i]);
        defer a.free(path);
        const stat = try std.Io.Dir.cwd().statFile(io, path, .{});
        hash.update(std.mem.asBytes(&stat.inode));
        hash.update(std.mem.asBytes(&stat.size));
        const mtime: i128 = stat.mtime.toNanoseconds();
        const ctime: i128 = stat.ctime.toNanoseconds();
        hash.update(std.mem.asBytes(&mtime));
        hash.update(std.mem.asBytes(&ctime));
    }
    return hash.finalResult();
}

/// Filesystem and browser loaders bind the same exact bytes and recipe.
pub fn fromDigests(digests: [sidecars.len + 1][32]u8) [64]u8 {
    var aggregate = std.crypto.hash.sha2.Sha256.init(.{});
    aggregate.update(recipe);
    for (digests, 0..) |digest, i| {
        aggregate.update(if (i == sidecars.len) "weights" else sidecars[i]);
        aggregate.update(&.{0});
        aggregate.update(&digest);
    }
    return std.fmt.bytesToHex(aggregate.finalResult(), .lower);
}

pub fn snapshot(a: std.mem.Allocator, io: std.Io, directory: []const u8, weights: []const u8, control: Control) !Snapshot {
    const before = try signature(a, io, directory, weights);
    var digests: [sidecars.len + 1][32]u8 = undefined;
    for (0..sidecars.len + 1) |i| {
        const name = if (i == sidecars.len) "weights" else sidecars[i];
        const path = try receipt.resolveContainedArtifactPath(a, io, directory, if (i == sidecars.len) std.fs.path.basename(weights) else name);
        defer a.free(path);
        var region = try file.MmapRegion.initLimited(a, path, if (i == sidecars.len) 4 * 1024 * 1024 * 1024 else 64 * 1024 * 1024);
        defer region.deinit();
        region.preserveFileCacheOnDeinit();
        if (std.mem.eql(u8, name, "processor_config.json")) try validateProcessor(a, region.data);
        if (std.mem.eql(u8, name, "tokenizer.json")) try validateTokenizer(a, region.data);
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        var offset: usize = 0;
        while (offset < region.data.len) {
            try control.check();
            const end = @min(region.data.len, offset + 4 * 1024 * 1024);
            hash.update(region.data[offset..end]);
            offset = end;
        }
        digests[i] = hash.finalResult();
    }
    if (!std.mem.eql(u8, &before, &try signature(a, io, directory, weights))) return error.ModelArtifactsChanging;
    return .{ .digest = fromDigests(digests), .signature = before };
}

/// Cache only fully verified digests, guarded by the same before/after file
/// signature as execution. Warm planning does not touch checkpoint pages.
pub fn snapshotCached(a: std.mem.Allocator, io: std.Io, directory: []const u8, weights: []const u8, control: Control) !Snapshot {
    const Cache = struct {
        var mutex: std.atomic.Mutex = .unlocked;
        var entries: [8]?Snapshot = @splat(null);
        var next: usize = 0;
    };
    try control.check();
    const current = try signature(a, io, directory, weights);
    while (!Cache.mutex.tryLock()) std.atomic.spinLoopHint();
    for (Cache.entries) |entry| if (entry) |cached| if (std.mem.eql(u8, &cached.signature, &current)) {
        Cache.mutex.unlock();
        return cached;
    };
    Cache.mutex.unlock();
    const value = try snapshot(a, io, directory, weights, control);
    while (!Cache.mutex.tryLock()) std.atomic.spinLoopHint();
    defer Cache.mutex.unlock();
    Cache.entries[Cache.next] = value;
    Cache.next = (Cache.next + 1) % Cache.entries.len;
    return value;
}

test "embeddinggemma2 processor changes fail closed" {
    const a = std.testing.allocator;
    try validateProcessor(a, @embedFile("embedding_gemma2_processor.json"));
    const changed = try a.dupe(u8, @embedFile("embedding_gemma2_processor.json"));
    defer a.free(changed);
    const index = std.mem.indexOf(u8, changed, "\"fft_length\": 512").?;
    changed[index + "\"fft_length\": ".len] = '6';
    try std.testing.expectError(error.InvalidEmbeddingGemma2Processor, validateProcessor(a, changed));
}

test "embeddinggemma2 video processor sampling and token budgets fail closed" {
    const a = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, a, @embedFile("embedding_gemma2_processor.json"), .{});
    defer parsed.deinit();
    const processor = parsed.value.object.getPtr("video_processor").?;
    for ([_][]const u8{ "fps", "max_frames", "max_soft_tokens" }) |key| {
        const value = processor.object.getPtr(key).?;
        const original = value.*;
        value.* = .{ .integer = 0 };
        const changed = try std.json.Stringify.valueAlloc(a, parsed.value, .{});
        defer a.free(changed);
        try std.testing.expectError(error.InvalidEmbeddingGemma2Processor, validateProcessor(a, changed));
        value.* = original;
    }
}

test "embeddinggemma2 precision identities preserve F32 pins and isolate CUDA recipes" {
    const artifacts = fromDigests(@splat(@splat(7)));
    try std.testing.expectEqual(artifacts, forBackend(artifacts, .native));
    try std.testing.expectEqual(artifacts, forBackend(artifacts, .metal));
    const cuda = forBackend(artifacts, .cuda);
    try std.testing.expect(!std.mem.eql(u8, &artifacts, &cuda));
    try std.testing.expectEqual(cuda, forBackend(artifacts, .cuda));
    try std.testing.expect(!std.mem.eql(u8, &cuda, &forBackend(fromDigests(@splat(@splat(8))), .cuda)));
}

test "embeddinggemma2 CUDA optimized identities isolate every arithmetic profile" {
    const Policy = @import("../ops/cuda/embedding_gemma2_policy.zig").Policy;
    const profiles = [_]Policy{ .{}, .{ .extended_flash = true }, .{ .fused_vision = true }, .{ .packed_projections = true }, .{ .graph_replay = true }, .{ .image_batch = true }, .{ .native_vision = true }, .{ .media_batch = true }, .{ .fused_backbone = true }, .{ .tuned_gemm = true }, .{ .spatial_pool = true }, .{ .audio_conv_gemm = true }, .{ .audio_conv_bf16 = true }, .{ .audio_fft = true }, .{ .audio_frontend_fused = true }, .{ .audio_graph = true }, .{ .tuned_gemm = true, .packed_projections = true }, .{ .tuned_gemm = true, .fused_backbone = true, .graph_replay = true } };
    const artifacts = fromDigests(@splat(@splat(7)));
    for (profiles, 0..) |left, i| for (profiles[i + 1 ..]) |right| try std.testing.expect(!std.mem.eql(u8, &forCudaPolicy(artifacts, left), &forCudaPolicy(artifacts, right)));
}
