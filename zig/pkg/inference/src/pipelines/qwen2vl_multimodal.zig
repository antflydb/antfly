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
const platform = @import("antfly_platform");
const c_file = @import("../util/c_file.zig");
const image = @import("image.zig");
const gpt_arch = @import("../architectures/gpt.zig");
const gpt_mod = @import("../models/gpt.zig");
const ops = @import("../ops/ops.zig");
const qwen2vl_vision = @import("../architectures/qwen2vl_vision.zig");
const tokenizer_mod = @import("inference_tokenizer");
const qwen2vl_types = @import("../architectures/qwen2vl_types.zig");

pub const PreprocessorConfig = qwen2vl_types.PreprocessorConfig;
pub const PreparedImage = qwen2vl_types.PreparedImage;

pub const PromptConfig = struct {
    query_prefix: []const u8 = "Query -- ",
    visual_prompt_prefix: []const u8 = "<|im_start|>user\n<|vision_start|><|image_pad|><|vision_end|>Describe the image.<|im_end|><|endoftext|>",
};

pub const PreparedTextInput = struct {
    allocator: std.mem.Allocator,
    input_ids: []i32,
    attention_mask: []i32,

    pub fn deinit(self: *PreparedTextInput) void {
        self.allocator.free(self.input_ids);
        self.allocator.free(self.attention_mask);
    }
};

pub const ExpandedPrompt = struct {
    allocator: std.mem.Allocator,
    token_ids: []i64,
    image_offsets: []usize,

    pub fn deinit(self: *ExpandedPrompt) void {
        self.allocator.free(self.token_ids);
        self.allocator.free(self.image_offsets);
    }
};

pub const PreparedPrompt = struct {
    allocator: std.mem.Allocator,
    token_ids: []i64,
    ple_token_ids: ?[]i64 = null,
    input_embeddings: ?ops.CT,
    attn_or_mask: ?[]u8 = null,

    pub fn deinit(self: *PreparedPrompt, cb: *const ops.ComputeBackend) void {
        self.allocator.free(self.token_ids);
        if (self.ple_token_ids) |ids| self.allocator.free(ids);
        if (self.input_embeddings) |embeddings| cb.free(embeddings);
        if (self.attn_or_mask) |mask| self.allocator.free(mask);
    }
};

pub fn prepareQueryText(
    allocator: std.mem.Allocator,
    tok: tokenizer_mod.Tokenizer,
    prompt_cfg: PromptConfig,
    query: []const u8,
    max_length: usize,
    add_bos_token: bool,
) !PreparedTextInput {
    const full = try std.fmt.allocPrint(allocator, "{s}{s}", .{ prompt_cfg.query_prefix, query });
    defer allocator.free(full);
    const encoded = try tok.encodeForGenerationConfigured(allocator, full, max_length, add_bos_token);
    return .{
        .allocator = allocator,
        .input_ids = encoded.ids,
        .attention_mask = encoded.attention_mask,
    };
}

pub fn prepareDocumentPrompt(
    allocator: std.mem.Allocator,
    tok: tokenizer_mod.Tokenizer,
    prompt_cfg: PromptConfig,
    config: gpt_mod.Config,
    prepared: PreparedImage,
    max_length: usize,
    add_bos_token: bool,
) !PreparedTextInput {
    return buildVisualPromptWithImageExpansion(allocator, tok, prompt_cfg.visual_prompt_prefix, config, prepared.image_token_count, max_length, add_bos_token);
}

fn buildVisualPromptWithImageExpansion(
    allocator: std.mem.Allocator,
    tok: tokenizer_mod.Tokenizer,
    prompt: []const u8,
    config: gpt_mod.Config,
    image_token_count: usize,
    max_length: usize,
    add_bos_token: bool,
) !PreparedTextInput {
    const marker = "<|image_pad|>";
    const split = std.mem.indexOf(u8, prompt, marker) orelse return error.ImageMarkerMissing;
    const before = prompt[0..split];
    const after = prompt[split + marker.len ..];

    const before_ids = try tok.encode(allocator, before);
    defer allocator.free(before_ids);
    const after_ids = try tok.encode(allocator, after);
    defer allocator.free(after_ids);

    const prefix_bos: usize = if (add_bos_token and tok.specialTokens().cls_id >= 0 and max_length > 0) 1 else 0;
    const total_unclamped = prefix_bos + before_ids.len + image_token_count + after_ids.len;
    const total = @min(total_unclamped, max_length);
    const ids = try allocator.alloc(i32, max_length);
    const mask = try allocator.alloc(i32, max_length);

    var pos: usize = 0;
    if (prefix_bos == 1) {
        ids[0] = tok.specialTokens().cls_id;
        mask[0] = 1;
        pos = 1;
    }
    pos = appendSliceInto(ids, mask, pos, total, before_ids);
    const remaining = total - @min(pos, total);
    const image_tokens = @min(image_token_count, remaining);
    for (0..image_tokens) |_| {
        ids[pos] = config.image_token_index;
        mask[pos] = 1;
        pos += 1;
    }
    pos = appendSliceInto(ids, mask, pos, total, after_ids);
    for (pos..max_length) |i| {
        ids[i] = tok.specialTokens().pad_id;
        mask[i] = 0;
    }

    return .{ .allocator = allocator, .input_ids = ids, .attention_mask = mask };
}

pub fn appendSliceInto(ids: []i32, mask: []i32, start: usize, limit: usize, values: []const i32) usize {
    var pos = start;
    for (values) |value| {
        if (pos >= limit) break;
        ids[pos] = value;
        mask[pos] = 1;
        pos += 1;
    }
    return pos;
}

pub fn loadPreprocessorConfig(allocator: std.mem.Allocator, model_dir: []const u8) !PreprocessorConfig {
    if (c_file.readFileFromDir(allocator, model_dir, "preprocessor_config.json")) |bytes| {
        defer allocator.free(bytes);
        return parsePreprocessorConfig(allocator, bytes);
    } else |_| {}

    const bytes = try c_file.readFileFromDir(allocator, model_dir, "processor_config.json");
    defer allocator.free(bytes);
    return parsePreprocessorConfig(allocator, bytes);
}

pub fn parsePreprocessorConfig(allocator: std.mem.Allocator, json_bytes: []const u8) !PreprocessorConfig {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidPreprocessorConfig;
    const root = parsed.value.object;
    const obj = blk: {
        if (root.get("image_processor")) |value| {
            if (value == .object) break :blk value.object;
        }
        break :blk root;
    };

    var config = PreprocessorConfig{};
    if (obj.get("do_resize")) |v| {
        if (jsonBool(v)) |value| config.do_resize = value;
    }
    if (obj.get("do_rescale")) |v| {
        if (jsonBool(v)) |value| config.do_rescale = value;
    }
    if (obj.get("do_normalize")) |v| {
        if (jsonBool(v)) |value| config.do_normalize = value;
    }
    if (obj.get("do_convert_rgb")) |v| {
        if (jsonBool(v)) |value| config.do_convert_rgb = value;
    }
    if (obj.get("rescale_factor")) |v| {
        if (jsonF32(v)) |value| config.rescale_factor = value;
    }
    if (obj.get("min_pixels")) |v| {
        if (jsonU32(v)) |value| config.min_pixels = value;
    }
    if (obj.get("max_pixels")) |v| {
        if (jsonU32(v)) |value| config.max_pixels = value;
    }
    if (obj.get("size")) |v| {
        if (v == .object) {
            if (v.object.get("shortest_edge")) |edge| {
                if (jsonU32(edge)) |value| config.min_pixels = value;
            }
            if (v.object.get("longest_edge")) |edge| {
                if (jsonU32(edge)) |value| config.max_pixels = value;
            }
        }
    }
    if (obj.get("patch_size")) |v| {
        if (jsonU32(v)) |value| config.patch_size = value;
    }
    if (obj.get("temporal_patch_size")) |v| {
        if (jsonU32(v)) |value| config.temporal_patch_size = value;
    }
    if (obj.get("merge_size")) |v| {
        if (jsonU32(v)) |value| config.merge_size = value;
    }
    if (obj.get("image_mean")) |v| {
        if (parseRgbTriple(v)) |value| config.image_mean = value;
    }
    if (obj.get("image_std")) |v| {
        if (parseRgbTriple(v)) |value| config.image_std = value;
    }
    return config;
}

pub fn prepareImage(allocator: std.mem.Allocator, image_bytes: []const u8, config: PreprocessorConfig) !PreparedImage {
    if (!config.do_resize or !config.do_rescale or !config.do_normalize or !config.do_convert_rgb) {
        return error.UnsupportedQwen2VlPreprocessorConfig;
    }
    const decoded = try image.decode(allocator, image_bytes);
    defer decoded.deinit(allocator);

    const dims = try smartResize(decoded.width, decoded.height, config);
    const pixel_values = try image.preprocessDecodedToSize(allocator, decoded, dims.width, dims.height, config.image_mean, config.image_std);
    errdefer allocator.free(pixel_values);

    const patch_grid_h = dims.height / config.patch_size;
    const patch_grid_w = dims.width / config.patch_size;
    const merge = @max(config.merge_size, 1);
    if (patch_grid_h % merge != 0 or patch_grid_w % merge != 0) return error.InvalidPatchMergeFactor;
    const grid_t: u32 = 1;
    return .{
        .allocator = allocator,
        .pixel_values = pixel_values,
        .resized_width = dims.width,
        .resized_height = dims.height,
        .image_grid_thw = .{ grid_t, patch_grid_h, patch_grid_w },
        .image_token_count = @as(usize, grid_t) * (patch_grid_h / merge) * (patch_grid_w / merge),
    };
}

pub fn expandPromptTokensForPreparedImages(
    allocator: std.mem.Allocator,
    prompt_token_ids: []const i32,
    config: gpt_mod.Config,
    prepared_images: []const PreparedImage,
) !ExpandedPrompt {
    if (config.image_token_index < 0) return error.InvalidMultimodalConfig;
    if (prepared_images.len == 0) return error.NoImages;

    var token_ids = std.ArrayListUnmanaged(i64).empty;
    errdefer token_ids.deinit(allocator);
    var image_offsets = std.ArrayListUnmanaged(usize).empty;
    errdefer image_offsets.deinit(allocator);

    var image_idx: usize = 0;
    for (prompt_token_ids) |token_id| {
        if (token_id == config.image_token_index) {
            if (image_idx >= prepared_images.len) return error.ImagePlaceholderCountMismatch;
            try image_offsets.append(allocator, token_ids.items.len);
            for (0..prepared_images[image_idx].image_token_count) |_| {
                try token_ids.append(allocator, config.image_token_index);
            }
            image_idx += 1;
            continue;
        }
        try token_ids.append(allocator, token_id);
    }

    if (image_idx != prepared_images.len) return error.ImagePlaceholderCountMismatch;
    return .{
        .allocator = allocator,
        .token_ids = try token_ids.toOwnedSlice(allocator),
        .image_offsets = try image_offsets.toOwnedSlice(allocator),
    };
}

pub fn prepareExpandedPromptEmbeddings(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    config: gpt_mod.Config,
    prep_cfg: PreprocessorConfig,
    expanded_token_ids: []const i32,
    images: []const []const u8,
) !PreparedPrompt {
    if (images.len == 0) return error.NoImages;
    if (config.hidden_size == 0 or config.image_token_index < 0) return error.InvalidMultimodalConfig;
    debugQwenVlStage("prepare begin images={d} expanded_input_tokens={d}", .{ images.len, expanded_token_ids.len });

    var prepared_images = try allocator.alloc(PreparedImage, images.len);
    defer allocator.free(prepared_images);
    var prepared_count: usize = 0;
    errdefer {
        for (prepared_images[0..prepared_count]) |*prepared| prepared.deinit();
    }
    for (images, 0..) |image_bytes, idx| {
        debugQwenVlStage("prepare image {d} bytes={d}", .{ idx, image_bytes.len });
        prepared_images[idx] = try prepareImage(allocator, image_bytes, prep_cfg);
        debugQwenVlStage("prepared image {d} resized={}x{} tokens={d}", .{ idx, prepared_images[idx].resized_width, prepared_images[idx].resized_height, prepared_images[idx].image_token_count });
        prepared_count += 1;
    }
    defer {
        for (prepared_images[0..prepared_count]) |*prepared| prepared.deinit();
    }

    debugQwenVlStage("expand prompt tokens", .{});
    var expanded = try expandPromptTokensForPreparedImages(allocator, expanded_token_ids, config, prepared_images);
    errdefer expanded.deinit();
    debugQwenVlStage("expanded prompt tokens={d}", .{expanded.token_ids.len});

    debugQwenVlStage("load embedding weight", .{});
    const embed_w = try gpt_arch.getEmbeddingWeight(cb, config);
    defer cb.free(embed_w);
    debugQwenVlStage("embedding lookup", .{});
    const base_embeddings = try cb.embeddingLookup(embed_w, expanded.token_ids, expanded.token_ids.len, config.hidden_size);
    defer cb.free(base_embeddings);

    debugQwenVlStage("base embeddings to f32", .{});
    const hidden_size: usize = config.hidden_size;
    const prompt_embeddings = try cb.toFloat32(base_embeddings, allocator);
    defer allocator.free(prompt_embeddings);

    const embedding_scale = config.tokenEmbeddingScale();
    if (!std.math.approxEqAbs(f32, embedding_scale, 1.0, 1e-6)) {
        for (prompt_embeddings) |*value| value.* *= embedding_scale;
    }

    for (prepared_images, 0..) |prepared, idx| {
        debugQwenVlStage("encode projected image {d}", .{idx});
        const projected_ct = try qwen2vl_vision.encodePreparedImageTokensTensor(cb, allocator, config, prep_cfg, prepared);
        defer cb.free(projected_ct);
        debugQwenVlStage("projected image {d} to f32", .{idx});
        const projected = try cb.toFloat32(projected_ct, allocator);
        defer allocator.free(projected);

        const expected_len = prepared.image_token_count * hidden_size;
        if (projected.len != expected_len) return error.ImageTokenLengthMismatch;
        const dst = expanded.image_offsets[idx] * hidden_size;
        @memcpy(prompt_embeddings[dst..][0..expected_len], projected);
    }

    const embedding_shape = [_]i32{ @intCast(expanded.token_ids.len), @intCast(hidden_size) };
    const input_embeddings = try cb.fromFloat32Shape(prompt_embeddings, &embedding_shape);
    errdefer cb.free(input_embeddings);

    return .{
        .allocator = allocator,
        .token_ids = expanded.token_ids,
        .input_embeddings = input_embeddings,
    };
}

fn debugQwenVlStage(comptime fmt: []const u8, args: anytype) void {
    if (!platform.env.getenvBoolDefault("TERMITE_QWEN_VL_STAGE_DEBUG", false)) return;
    std.debug.print("qwen_vl_debug: " ++ fmt ++ "\n", args);
}

pub const ResizeDims = struct { width: u32, height: u32 };

pub fn smartResize(width: u32, height: u32, config: PreprocessorConfig) !ResizeDims {
    if (width == 0 or height == 0) return error.InvalidImageSize;
    const factor = @max(config.patch_size * config.merge_size, 1);
    var target_w = alignNearest(width, factor);
    var target_h = alignNearest(height, factor);
    const aspect = @as(f64, @floatFromInt(width)) / @as(f64, @floatFromInt(height));

    var pixels: u64 = @as(u64, target_w) * target_h;
    if (pixels < config.min_pixels) {
        const scale = std.math.sqrt(@as(f64, @floatFromInt(config.min_pixels)) / @as(f64, @floatFromInt(@max(pixels, 1))));
        target_w = alignNearest(@max(1, @as(u32, @intFromFloat(@round(@as(f64, @floatFromInt(target_w)) * scale)))), factor);
        target_h = alignNearest(@max(1, @as(u32, @intFromFloat(@round(@as(f64, @floatFromInt(target_h)) * scale)))), factor);
        pixels = @as(u64, target_w) * target_h;
    }
    if (pixels > config.max_pixels) {
        const scale = std.math.sqrt(@as(f64, @floatFromInt(config.max_pixels)) / @as(f64, @floatFromInt(pixels)));
        target_w = alignFloor(@max(factor, @as(u32, @intFromFloat(@floor(@as(f64, @floatFromInt(target_w)) * scale)))), factor);
        target_h = alignFloor(@max(factor, @as(u32, @intFromFloat(@floor(@as(f64, @floatFromInt(target_h)) * scale)))), factor);
    }

    if (target_w == 0) target_w = factor;
    if (target_h == 0) target_h = factor;

    // Restore aspect ratio after alignment drift.
    if (aspect >= 1.0) {
        target_h = alignNearest(@max(factor, @as(u32, @intFromFloat(@round(@as(f64, @floatFromInt(target_w)) / aspect)))), factor);
    } else {
        target_w = alignNearest(@max(factor, @as(u32, @intFromFloat(@round(@as(f64, @floatFromInt(target_h)) * aspect)))), factor);
    }

    while (@as(u64, target_w) * target_h > config.max_pixels and target_w > factor and target_h > factor) {
        if (target_w >= target_h) target_w -= factor else target_h -= factor;
    }
    while (@as(u64, target_w) * target_h < config.min_pixels) {
        if (aspect >= 1.0) target_w += factor else target_h += factor;
    }

    return .{ .width = target_w, .height = target_h };
}

fn alignNearest(value: u32, factor: u32) u32 {
    const rounded = ((value + factor / 2) / factor) * factor;
    return @max(factor, rounded);
}

fn alignFloor(value: u32, factor: u32) u32 {
    return @max(factor, (value / factor) * factor);
}

fn parseRgbTriple(v: std.json.Value) ?[3]f32 {
    if (v != .array or v.array.items.len != 3) return null;
    var result: [3]f32 = undefined;
    for (v.array.items, 0..) |item, idx| {
        result[idx] = switch (item) {
            .float => |f| @floatCast(f),
            .integer => |i| @floatFromInt(i),
            else => return null,
        };
    }
    return result;
}

fn jsonBool(v: std.json.Value) ?bool {
    return switch (v) {
        .bool => |b| b,
        else => null,
    };
}
fn jsonU32(v: std.json.Value) ?u32 {
    return switch (v) {
        .integer => |i| @intCast(i),
        else => null,
    };
}
fn jsonF32(v: std.json.Value) ?f32 {
    return switch (v) {
        .float => |f| @floatCast(f),
        .integer => |i| @floatFromInt(i),
        else => null,
    };
}

test "parse qwen2vl preprocessor config" {
    const allocator = std.testing.allocator;
    const json =
        \\{
        \\  "do_resize": true,
        \\  "do_rescale": true,
        \\  "do_normalize": true,
        \\  "do_convert_rgb": true,
        \\  "min_pixels": 3136,
        \\  "max_pixels": 1003520,
        \\  "patch_size": 14,
        \\  "temporal_patch_size": 2,
        \\  "merge_size": 2
        \\}
    ;
    const cfg = try parsePreprocessorConfig(allocator, json);
    try std.testing.expectEqual(@as(u32, 14), cfg.patch_size);
    try std.testing.expectEqual(@as(u32, 2), cfg.merge_size);
    try std.testing.expectEqual(@as(u32, 3136), cfg.min_pixels);
}

test "parse qwen3.5 processor config image_processor wrapper" {
    const allocator = std.testing.allocator;
    const json =
        \\{
        \\  "image_processor": {
        \\    "do_resize": true,
        \\    "do_rescale": true,
        \\    "do_normalize": true,
        \\    "do_convert_rgb": true,
        \\    "image_mean": [0.5, 0.5, 0.5],
        \\    "image_std": [0.5, 0.5, 0.5],
        \\    "merge_size": 2,
        \\    "patch_size": 16,
        \\    "rescale_factor": 0.00392156862745098,
        \\    "size": {
        \\      "longest_edge": 786432,
        \\      "shortest_edge": 65536
        \\    },
        \\    "temporal_patch_size": 2
        \\  }
        \\}
    ;
    const cfg = try parsePreprocessorConfig(allocator, json);
    try std.testing.expectEqual(@as(u32, 16), cfg.patch_size);
    try std.testing.expectEqual(@as(u32, 2), cfg.merge_size);
    try std.testing.expectEqual(@as(u32, 65536), cfg.min_pixels);
    try std.testing.expectEqual(@as(u32, 786432), cfg.max_pixels);
    try std.testing.expectEqual(@as(f32, 0.5), cfg.image_mean[0]);
    try std.testing.expectEqual(@as(f32, 0.5), cfg.image_std[2]);
}

test "smart resize preserves factor divisibility" {
    const cfg = PreprocessorConfig{ .min_pixels = 56 * 56, .max_pixels = 28 * 28 * 1280, .patch_size = 14, .merge_size = 2 };
    const dims = try smartResize(640, 480, cfg);
    try std.testing.expectEqual(@as(u32, 0), dims.width % (cfg.patch_size * cfg.merge_size));
    try std.testing.expectEqual(@as(u32, 0), dims.height % (cfg.patch_size * cfg.merge_size));
    try std.testing.expect(@as(u64, dims.width) * dims.height >= cfg.min_pixels);
    try std.testing.expect(@as(u64, dims.width) * dims.height <= cfg.max_pixels);
}

const StubTokenizer = struct {
    fn tokenizer() tokenizer_mod.Tokenizer {
        return .{ .ptr = undefined, .vtable = &vtable };
    }

    const vtable = tokenizer_mod.Tokenizer.VTable{
        .encode = encode,
        .encodeInto = encodeInto,
        .encodeForModel = encodeForModel,
        .encodeGeneration = encodeGeneration,
        .decode = decode,
        .specialTokens = specialTokens,
        .vocabSize = vocabSize,
        .deinit = deinit,
    };

    fn encode(_: *anyopaque, allocator: std.mem.Allocator, text: []const u8) ![]i32 {
        var out = std.ArrayListUnmanaged(i32).empty;
        errdefer out.deinit(allocator);
        var it = std.mem.tokenizeScalar(u8, text, ' ');
        while (it.next()) |tok| {
            if (tok.len == 0) continue;
            const id: i32 = if (std.mem.eql(u8, tok, "<|im_start|>user\n<|vision_start|>")) 10 else if (std.mem.eql(u8, tok, "<|vision_end|>Describe")) 11 else if (std.mem.eql(u8, tok, "the")) 12 else if (std.mem.eql(u8, tok, "image.<|im_end|><|endoftext|>")) 13 else if (std.mem.eql(u8, tok, "Query")) 21 else if (std.mem.eql(u8, tok, "--")) 22 else if (std.mem.eql(u8, tok, "invoice")) 23 else if (std.mem.eql(u8, tok, "due")) 24 else if (std.mem.eql(u8, tok, "date")) 25 else 99;
            try out.append(allocator, id);
        }
        return out.toOwnedSlice(allocator);
    }

    fn encodeInto(ptr: *anyopaque, allocator: std.mem.Allocator, text: []const u8, out: *std.ArrayListUnmanaged(i32)) anyerror!void {
        const ids = try encode(ptr, allocator, text);
        defer allocator.free(ids);
        try out.appendSlice(allocator, ids);
    }

    fn encodeGeneration(_: *anyopaque, allocator: std.mem.Allocator, text: []const u8, max_length: usize, add_bos_token: bool) !tokenizer_mod.EncodeResult {
        const base = try encode(undefined, allocator, text);
        errdefer allocator.free(base);
        const prefix: usize = if (add_bos_token) 1 else 0;
        const total = @min(max_length, prefix + base.len);
        const ids = try allocator.alloc(i32, max_length);
        const mask = try allocator.alloc(i32, max_length);
        var pos: usize = 0;
        if (add_bos_token) {
            ids[0] = 101;
            mask[0] = 1;
            pos = 1;
        }
        for (base) |id| {
            if (pos >= total) break;
            ids[pos] = id;
            mask[pos] = 1;
            pos += 1;
        }
        for (pos..max_length) |i| {
            ids[i] = 0;
            mask[i] = 0;
        }
        allocator.free(base);
        return .{ .ids = ids, .attention_mask = mask, .allocator = allocator };
    }

    fn encodeForModel(_: *anyopaque, allocator: std.mem.Allocator, text: []const u8, max_length: usize) !tokenizer_mod.EncodeResult {
        const base = try encode(undefined, allocator, text);
        defer allocator.free(base);
        const total = @min(max_length, base.len + 2);
        const ids = try allocator.alloc(i32, max_length);
        const mask = try allocator.alloc(i32, max_length);
        var pos: usize = 0;
        if (max_length > 0) {
            ids[pos] = 101;
            mask[pos] = 1;
            pos += 1;
        }
        for (base) |id| {
            if (pos + 1 >= total) break;
            ids[pos] = id;
            mask[pos] = 1;
            pos += 1;
        }
        if (pos < max_length) {
            ids[pos] = 102;
            mask[pos] = 1;
            pos += 1;
        }
        for (pos..max_length) |i| {
            ids[i] = 0;
            mask[i] = 0;
        }
        return .{ .ids = ids, .attention_mask = mask, .allocator = allocator };
    }

    fn decode(_: *anyopaque, allocator: std.mem.Allocator, ids: []const i32) ![]u8 {
        _ = ids;
        return allocator.dupe(u8, "");
    }
    fn specialTokens(_: *anyopaque) tokenizer_mod.SpecialTokens {
        return .{ .cls_id = 101, .sep_id = 102, .pad_id = 0, .unk_id = 100, .mask_id = 103 };
    }
    fn vocabSize(_: *anyopaque) usize {
        return 1024;
    }
    fn deinit(_: *anyopaque) void {}
};

test "prepare query text prefixes query" {
    const allocator = std.testing.allocator;
    var prepared = try prepareQueryText(allocator, StubTokenizer.tokenizer(), .{}, "invoice due date", 8, true);
    defer prepared.deinit();
    try std.testing.expectEqual(@as(i32, 101), prepared.input_ids[0]);
    try std.testing.expectEqual(@as(i32, 21), prepared.input_ids[1]);
    try std.testing.expectEqual(@as(i32, 22), prepared.input_ids[2]);
}

test "prepare document prompt expands image tokens" {
    const allocator = std.testing.allocator;
    const cfg = gpt_mod.Config{ .image_token_index = 151655 };
    const prepared_image = PreparedImage{ .allocator = allocator, .pixel_values = &[_]f32{}, .resized_width = 280, .resized_height = 420, .image_grid_thw = .{ 1, 10, 7 }, .image_token_count = 3 };
    var prepared = try prepareDocumentPrompt(allocator, StubTokenizer.tokenizer(), .{}, cfg, prepared_image, 16, false);
    defer prepared.deinit();
    try std.testing.expectEqual(@as(i32, 10), prepared.input_ids[0]);
    try std.testing.expectEqual(@as(i32, 151655), prepared.input_ids[1]);
    try std.testing.expectEqual(@as(i32, 151655), prepared.input_ids[2]);
    try std.testing.expectEqual(@as(i32, 151655), prepared.input_ids[3]);
}

test "expand qwen prompt tokens uses dynamic prepared image token counts" {
    const allocator = std.testing.allocator;
    const cfg = gpt_mod.Config{ .image_token_index = 248056 };
    const prepared_images = [_]PreparedImage{
        .{
            .allocator = allocator,
            .pixel_values = &[_]f32{},
            .resized_width = 448,
            .resized_height = 448,
            .image_grid_thw = .{ 1, 28, 28 },
            .image_token_count = 196,
        },
        .{
            .allocator = allocator,
            .pixel_values = &[_]f32{},
            .resized_width = 224,
            .resized_height = 448,
            .image_grid_thw = .{ 1, 14, 28 },
            .image_token_count = 98,
        },
    };
    var expanded = try expandPromptTokensForPreparedImages(
        allocator,
        &.{ 1, 248053, 248056, 248054, 2, 248053, 248056, 248054, 3 },
        cfg,
        &prepared_images,
    );
    defer expanded.deinit();

    try std.testing.expectEqual(@as(usize, 1 + 1 + 196 + 1 + 1 + 1 + 98 + 1 + 1), expanded.token_ids.len);
    try std.testing.expectEqual(@as(usize, 2), expanded.image_offsets.len);
    try std.testing.expectEqual(@as(usize, 2), expanded.image_offsets[0]);
    try std.testing.expectEqual(@as(usize, 201), expanded.image_offsets[1]);
    try std.testing.expectEqual(@as(i64, 248056), expanded.token_ids[expanded.image_offsets[0]]);
    try std.testing.expectEqual(@as(i64, 248056), expanded.token_ids[expanded.image_offsets[1] + 97]);
}
