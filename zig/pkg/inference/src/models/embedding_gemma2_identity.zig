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
pub const recipe = "embeddinggemma2-f32-mean-v1";
const sidecars = [_][]const u8{ "config.json", "tokenizer.json", "tokenizer_config.json", "processor_config.json", "config_sentence_transformers.json", "1_Pooling/config.json" };
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
    for ([_][]const u8{ "processor_class", "audio_ms_per_token", "audio_seq_length", "image_seq_length", "feature_extractor", "image_processor" }) |key| {
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

pub fn snapshot(a: std.mem.Allocator, io: std.Io, directory: []const u8, weights: []const u8, control: Control) !Snapshot {
    const before = try signature(a, io, directory, weights);
    var aggregate = std.crypto.hash.sha2.Sha256.init(.{});
    aggregate.update(recipe);
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
        aggregate.update(name);
        aggregate.update(&.{0});
        aggregate.update(&hash.finalResult());
    }
    if (!std.mem.eql(u8, &before, &try signature(a, io, directory, weights))) return error.ModelArtifactsChanging;
    return .{ .digest = std.fmt.bytesToHex(aggregate.finalResult(), .lower), .signature = before };
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
