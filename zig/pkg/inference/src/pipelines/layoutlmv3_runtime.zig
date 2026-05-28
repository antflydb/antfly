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
const c_file = @import("../util/c_file.zig");
const image = @import("image.zig");
const hf_tokenizer = @import("inference_hf_tokenizer");
const common = @import("document_shared.zig");
pub const default_max_length: usize = 512;
pub const default_mean = [3]f32{ 0.5, 0.5, 0.5 };
pub const default_std = [3]f32{ 0.5, 0.5, 0.5 };

pub const OcrToken = common.OcrToken;

pub const PreparedInputs = struct {
    allocator: std.mem.Allocator,
    input_ids: []i32,
    attention_mask: []i32,
    bbox: []i32,
    pixel_values: []f32,
    source_width: usize,
    source_height: usize,
    input_width: usize,
    input_height: usize,
    token_count: usize,
    wordpiece_token_count: usize,
    special_token_count: usize,
    cls_token_id: i32,
    sep_token_id: i32,
    pad_token_id: i32,
    max_length: usize,

    pub fn deinit(self: *PreparedInputs) void {
        self.allocator.free(self.input_ids);
        self.allocator.free(self.attention_mask);
        self.allocator.free(self.bbox);
        self.allocator.free(self.pixel_values);
    }
};

pub const PreparationSummary = struct {
    task: []const u8 = "probe_layoutlmv3_prepare",
    model_dir: []const u8,
    image_path: []const u8,
    token_count: usize,
    max_length: usize,
    input_ids_length: usize,
    attention_mask_length: usize,
    bbox_length: usize,
    bbox_rows: usize,
    pixel_values_length: usize,
    source_width: usize,
    source_height: usize,
    input_width: usize,
    input_height: usize,
    wordpiece_token_count: usize,
    special_token_count: usize,
    cls_token_id: i32,
    sep_token_id: i32,
    pad_token_id: i32,
    sample_input_ids: []const i32,
    sample_bboxes: []const [4]i32,
};

pub const PreprocessorConfig = struct {
    do_resize: bool = true,
    do_normalize: bool = true,
    input_height: usize = 224,
    input_width: usize = 224,
    image_mean: [3]f32 = default_mean,
    image_std: [3]f32 = default_std,
};

const ModelConfig = struct {
    max_position_embeddings: ?usize = null,
};

const TokenizerConfig = struct {
    model_max_length: ?usize = null,
};

pub fn prepareFromFiles(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    image_path: []const u8,
    tokens: []const OcrToken,
    max_length_override: ?usize,
) !PreparedInputs {
    const tokenizer_bytes = try readBundleFile(allocator, model_dir, "tokenizer.json");
    defer allocator.free(tokenizer_bytes);
    const hf_tok = try hf_tokenizer.HfTokenizer.loadFromBytes(allocator, tokenizer_bytes);
    defer hf_tok.deinitSelf();

    const max_length = try resolveMaxLength(allocator, model_dir, max_length_override);
    const encoded = try encodeTokensForLayoutLMv3(allocator, hf_tok.tokenizer(), tokens, max_length);
    const input_ids = encoded.input_ids;
    errdefer allocator.free(input_ids);
    const attention_mask = encoded.attention_mask;
    errdefer allocator.free(attention_mask);
    const bbox = encoded.bbox;
    errdefer allocator.free(bbox);

    const image_bytes = try c_file.readFile(allocator, image_path);
    defer allocator.free(image_bytes);
    const decoded = try image.decode(allocator, image_bytes);
    defer decoded.deinit(allocator);

    const preprocessor = try loadPreprocessorConfig(allocator, model_dir);
    if (!preprocessor.do_resize or !preprocessor.do_normalize) {
        return error.UnsupportedPreprocessorConfig;
    }
    const pixel_values = try image.preprocessDecodedToSize(
        allocator,
        decoded,
        @intCast(preprocessor.input_width),
        @intCast(preprocessor.input_height),
        preprocessor.image_mean,
        preprocessor.image_std,
    );
    errdefer allocator.free(pixel_values);

    const special = hf_tok.tokenizer().specialTokens();
    return .{
        .allocator = allocator,
        .input_ids = input_ids,
        .attention_mask = attention_mask,
        .bbox = bbox,
        .pixel_values = pixel_values,
        .source_width = decoded.width,
        .source_height = decoded.height,
        .input_width = preprocessor.input_width,
        .input_height = preprocessor.input_height,
        .token_count = tokens.len,
        .wordpiece_token_count = encoded.wordpiece_token_count,
        .special_token_count = encoded.special_token_count,
        .cls_token_id = special.cls_id,
        .sep_token_id = special.sep_id,
        .pad_token_id = special.pad_id,
        .max_length = max_length,
    };
}

pub fn summarizePreparedInputs(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    image_path: []const u8,
    prepared: *const PreparedInputs,
) !PreparationSummary {
    const sample_id_len = @min(prepared.input_ids.len, 16);
    const sample_box_rows = @min(prepared.input_ids.len, 8);
    const sample_input_ids = try allocator.dupe(i32, prepared.input_ids[0..sample_id_len]);
    errdefer allocator.free(sample_input_ids);
    const sample_bboxes = try allocator.alloc([4]i32, sample_box_rows);
    errdefer allocator.free(sample_bboxes);
    for (0..sample_box_rows) |idx| {
        const base = idx * 4;
        sample_bboxes[idx] = .{
            prepared.bbox[base + 0],
            prepared.bbox[base + 1],
            prepared.bbox[base + 2],
            prepared.bbox[base + 3],
        };
    }
    return .{
        .model_dir = model_dir,
        .image_path = image_path,
        .token_count = prepared.token_count,
        .max_length = prepared.max_length,
        .input_ids_length = prepared.input_ids.len,
        .attention_mask_length = prepared.attention_mask.len,
        .bbox_length = prepared.bbox.len,
        .bbox_rows = prepared.bbox.len / 4,
        .pixel_values_length = prepared.pixel_values.len,
        .source_width = prepared.source_width,
        .source_height = prepared.source_height,
        .input_width = prepared.input_width,
        .input_height = prepared.input_height,
        .wordpiece_token_count = prepared.wordpiece_token_count,
        .special_token_count = prepared.special_token_count,
        .cls_token_id = prepared.cls_token_id,
        .sep_token_id = prepared.sep_token_id,
        .pad_token_id = prepared.pad_token_id,
        .sample_input_ids = sample_input_ids,
        .sample_bboxes = sample_bboxes,
    };
}

pub fn loadPreprocessorConfig(allocator: std.mem.Allocator, model_dir: []const u8) !PreprocessorConfig {
    var config = PreprocessorConfig{};
    const path = try std.fmt.allocPrint(allocator, "{s}/preprocessor_config.json", .{model_dir});
    defer allocator.free(path);
    const bytes = c_file.readFile(allocator, path) catch return config;
    defer allocator.free(bytes);

    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return config;
    const obj = parsed.value.object;

    if (obj.get("do_resize")) |value| {
        if (value == .bool) config.do_resize = value.bool;
    }
    if (obj.get("do_normalize")) |value| {
        if (value == .bool) config.do_normalize = value.bool;
    }
    if (obj.get("size")) |value| {
        switch (value) {
            .integer => |raw| {
                const size: usize = @intCast(raw);
                config.input_height = size;
                config.input_width = size;
            },
            .object => |size_obj| {
                var saw_height = false;
                var saw_width = false;
                if (size_obj.get("height")) |height| {
                    if (jsonUsize(height)) |parsed_size| {
                        config.input_height = parsed_size;
                        saw_height = true;
                    }
                }
                if (size_obj.get("width")) |width| {
                    if (jsonUsize(width)) |parsed_size| {
                        config.input_width = parsed_size;
                        saw_width = true;
                    }
                }
                if (size_obj.get("shortest_edge")) |edge| {
                    if (jsonUsize(edge)) |parsed_size| {
                        if (!saw_height) config.input_height = parsed_size;
                        if (!saw_width) config.input_width = parsed_size;
                    }
                }
            },
            else => {},
        }
    }
    if (obj.get("image_mean")) |value| {
        if (jsonFloatArray3(value)) |parsed_mean| config.image_mean = parsed_mean;
    }
    if (obj.get("image_std")) |value| {
        if (jsonFloatArray3(value)) |parsed_std| config.image_std = parsed_std;
    }
    return config;
}

fn resolveMaxLength(allocator: std.mem.Allocator, model_dir: []const u8, max_length_override: ?usize) !usize {
    if (max_length_override) |value| return value;
    if (try loadOptionalJson(ModelConfig, allocator, model_dir, "config.json")) |cfg| {
        if (cfg.max_position_embeddings) |value| return value;
    }
    if (try loadOptionalJson(TokenizerConfig, allocator, model_dir, "tokenizer_config.json")) |cfg| {
        if (cfg.model_max_length) |value| return value;
    }
    return default_max_length;
}

fn loadOptionalJson(comptime T: type, allocator: std.mem.Allocator, model_dir: []const u8, basename: []const u8) !?T {
    const path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ model_dir, basename });
    defer allocator.free(path);
    const bytes = c_file.readFile(allocator, path) catch return null;
    defer allocator.free(bytes);
    return try std.json.parseFromSliceLeaky(T, allocator, bytes, .{ .ignore_unknown_fields = true });
}

fn readBundleFile(allocator: std.mem.Allocator, model_dir: []const u8, basename: []const u8) ![]u8 {
    const path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ model_dir, basename });
    defer allocator.free(path);
    return c_file.readFile(allocator, path);
}

fn jsonUsize(value: std.json.Value) ?usize {
    return switch (value) {
        .integer => |raw| @intCast(raw),
        else => null,
    };
}

fn jsonFloatArray3(value: std.json.Value) ?[3]f32 {
    if (value != .array or value.array.items.len < 3) return null;
    var out: [3]f32 = undefined;
    for (0..3) |idx| {
        out[idx] = switch (value.array.items[idx]) {
            .float => |raw| @floatCast(raw),
            .integer => |raw| @floatFromInt(raw),
            else => return null,
        };
    }
    return out;
}

fn encodeTokensForLayoutLMv3(
    allocator: std.mem.Allocator,
    tokenizer: anytype,
    tokens: []const OcrToken,
    max_length: usize,
) !struct {
    input_ids: []i32,
    attention_mask: []i32,
    bbox: []i32,
    wordpiece_token_count: usize,
    special_token_count: usize,
} {
    const special = tokenizer.specialTokens();
    const input_ids = try allocator.alloc(i32, max_length);
    errdefer allocator.free(input_ids);
    const attention_mask = try allocator.alloc(i32, max_length);
    errdefer allocator.free(attention_mask);
    const bbox = try allocator.alloc(i32, max_length * 4);
    errdefer allocator.free(bbox);

    for (0..max_length) |idx| {
        input_ids[idx] = special.pad_id;
        attention_mask[idx] = 0;
        const base = idx * 4;
        bbox[base + 0] = 0;
        bbox[base + 1] = 0;
        bbox[base + 2] = 0;
        bbox[base + 3] = 0;
    }

    if (max_length == 0) {
        return .{
            .input_ids = input_ids,
            .attention_mask = attention_mask,
            .bbox = bbox,
            .wordpiece_token_count = 0,
            .special_token_count = 0,
        };
    }

    var write_idx: usize = 0;
    input_ids[write_idx] = special.cls_id;
    attention_mask[write_idx] = 1;
    write_idx += 1;

    var wordpiece_token_count: usize = 1;
    for (tokens) |tok| {
        if (write_idx + 1 >= max_length) break;
        const ids = try tokenizer.encode(allocator, tok.text);
        defer allocator.free(ids);
        for (ids) |id| {
            if (write_idx + 1 >= max_length) break;
            input_ids[write_idx] = id;
            attention_mask[write_idx] = 1;
            const base = write_idx * 4;
            bbox[base + 0] = tok.bbox[0];
            bbox[base + 1] = tok.bbox[1];
            bbox[base + 2] = tok.bbox[2];
            bbox[base + 3] = tok.bbox[3];
            write_idx += 1;
            wordpiece_token_count += 1;
        }
    }

    if (write_idx < max_length) {
        input_ids[write_idx] = special.sep_id;
        attention_mask[write_idx] = 1;
        write_idx += 1;
        wordpiece_token_count += 1;
    }

    const sep_count: usize = if (write_idx > 0 and input_ids[write_idx - 1] == special.sep_id) 1 else 0;
    return .{
        .input_ids = input_ids,
        .attention_mask = attention_mask,
        .bbox = bbox,
        .wordpiece_token_count = wordpiece_token_count,
        .special_token_count = 1 + sep_count + (max_length - write_idx),
    };
}

test "encode tokens for layoutlmv3 expands per-token boxes across wordpieces" {
    const alloc = std.testing.allocator;
    const tokenizer_json =
        \\{
        \\  "model": {
        \\    "type": "WordPiece",
        \\    "unk_token": "[UNK]",
        \\    "continuing_subword_prefix": "##",
        \\    "vocab": {
        \\      "[PAD]": 0,
        \\      "[UNK]": 100,
        \\      "[CLS]": 101,
        \\      "[SEP]": 102,
        \\      "hello": 200,
        \\      "world": 201
        \\    }
        \\  }
        \\}
    ;
    const hf_tok = try hf_tokenizer.HfTokenizer.loadFromBytes(alloc, tokenizer_json);
    defer hf_tok.deinitSelf();
    const tokens = [_]OcrToken{
        .{ .text = "hello", .bbox = .{ 1, 2, 3, 4 } },
        .{ .text = "world", .bbox = .{ 5, 6, 7, 8 } },
    };
    const encoded = try encodeTokensForLayoutLMv3(alloc, hf_tok.tokenizer(), &tokens, 8);
    defer alloc.free(encoded.input_ids);
    defer alloc.free(encoded.attention_mask);
    defer alloc.free(encoded.bbox);

    try std.testing.expectEqual(@as(i32, 101), encoded.input_ids[0]);
    try std.testing.expectEqual(@as(i32, 200), encoded.input_ids[1]);
    try std.testing.expectEqual(@as(i32, 201), encoded.input_ids[2]);
    try std.testing.expectEqual(@as(i32, 102), encoded.input_ids[3]);
    try std.testing.expectEqualSlices(i32, &.{ 1, 2, 3, 4 }, encoded.bbox[4..8]);
    try std.testing.expectEqualSlices(i32, &.{ 5, 6, 7, 8 }, encoded.bbox[8..12]);
}
