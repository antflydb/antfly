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
const hf_tokenizer_mod = @import("inference_hf_tokenizer");
const gguf_format = @import("../gguf/format.zig");
const gguf_metadata = @import("../gguf/metadata.zig");
const web_runtime = @import("runtime_state.zig");

pub fn loadTokenizer(
    allocator: std.mem.Allocator,
    runtime: *web_runtime.Runtime,
    json_bytes: []const u8,
) !u32 {
    const tok = try hf_tokenizer_mod.HfTokenizer.loadFromBytes(allocator, json_bytes);
    return runtime.storeTokenizer(tok);
}

pub fn loadTokenizerFromGguf(
    allocator: std.mem.Allocator,
    runtime: *web_runtime.Runtime,
    gguf_bytes: []const u8,
) !u32 {
    var parsed = try gguf_format.parse(allocator, gguf_bytes);
    defer parsed.deinit(allocator);

    const tokenizer_json = try tokenizerJsonFromParsedGguf(allocator, &parsed);
    defer allocator.free(tokenizer_json);

    const tok = try hf_tokenizer_mod.HfTokenizer.loadFromBytes(allocator, tokenizer_json);
    tok.applySpecialTokenIds(
        metadataTokenId(&parsed, "tokenizer.ggml.bos_token_id"),
        metadataTokenId(&parsed, "tokenizer.ggml.eos_token_id"),
        metadataTokenId(&parsed, "tokenizer.ggml.padding_token_id"),
        metadataTokenId(&parsed, "tokenizer.ggml.unknown_token_id"),
    );
    return runtime.storeTokenizer(tok);
}

pub fn tokenizerJsonFromGguf(allocator: std.mem.Allocator, gguf_bytes: []const u8) ![]u8 {
    var parsed = try gguf_format.parse(allocator, gguf_bytes);
    defer parsed.deinit(allocator);
    return tokenizerJsonFromParsedGguf(allocator, &parsed);
}

pub fn chatTemplateFromGguf(allocator: std.mem.Allocator, gguf_bytes: []const u8) !?[]u8 {
    var parsed = try gguf_format.parse(allocator, gguf_bytes);
    defer parsed.deinit(allocator);

    const view = gguf_metadata.View.init(&parsed);
    const template = view.getString("tokenizer.chat_template") orelse return null;
    const trimmed = std.mem.trim(u8, template, &std.ascii.whitespace);
    if (trimmed.len == 0) return null;
    return try allocator.dupe(u8, template);
}

pub fn tokenize(
    allocator: std.mem.Allocator,
    runtime: *web_runtime.Runtime,
    tok_handle: u32,
    text: []const u8,
    max_len: u32,
    out_ids_ptr: [*]i32,
    out_mask_ptr: [*]i32,
) !u32 {
    const hf_tok = try runtime.getTokenizer(tok_handle);
    var tok = hf_tok.tokenizer();
    var result = try tok.encodeForModel(allocator, text, max_len);
    defer result.deinit();
    @memcpy(out_ids_ptr[0..max_len], result.ids);
    @memcpy(out_mask_ptr[0..max_len], result.attention_mask);
    return max_len;
}

pub fn tokenizePair(
    allocator: std.mem.Allocator,
    runtime: *web_runtime.Runtime,
    tok_handle: u32,
    text_a: []const u8,
    text_b: []const u8,
    max_len: u32,
    out_ids_ptr: [*]i32,
    out_mask_ptr: [*]i32,
) !u32 {
    const hf_tok = try runtime.getTokenizer(tok_handle);
    var tok = hf_tok.tokenizer();
    var result = try tok.encodeForPair(allocator, text_a, text_b, max_len);
    defer result.deinit();
    @memcpy(out_ids_ptr[0..max_len], result.ids);
    @memcpy(out_mask_ptr[0..max_len], result.attention_mask);
    return max_len;
}

pub fn tokenizeRaw(
    allocator: std.mem.Allocator,
    runtime: *web_runtime.Runtime,
    tok_handle: u32,
    text: []const u8,
    out_ids_ptr: [*]i32,
    max_ids: u32,
) !u32 {
    const hf_tok = try runtime.getTokenizer(tok_handle);
    var tok = hf_tok.tokenizer();
    const raw_ids = try tok.encode(allocator, text);
    defer allocator.free(raw_ids);
    const n: u32 = @intCast(@min(raw_ids.len, max_ids));
    @memcpy(out_ids_ptr[0..n], raw_ids[0..n]);
    return n;
}

pub fn decode(
    allocator: std.mem.Allocator,
    runtime: *web_runtime.Runtime,
    tok_handle: u32,
    token_ids: []const i32,
) ![]u8 {
    const hf_tok = try runtime.getTokenizer(tok_handle);
    var tok = hf_tok.tokenizer();
    return tok.decode(allocator, token_ids);
}

fn tokenizerJsonFromParsedGguf(allocator: std.mem.Allocator, parsed: *const gguf_format.File) ![]u8 {
    const view = gguf_metadata.View.init(parsed);
    const model_name = view.getString("tokenizer.ggml.model") orelse return error.NoTokenizerFound;

    if (supportsBpeFallback(parsed, model_name)) {
        return bpeTokenizerJsonFromGguf(allocator, parsed);
    }
    if (supportsUnigramFallback(parsed, model_name)) {
        return unigramTokenizerJsonFromGguf(allocator, parsed);
    }
    return error.NoTokenizerFound;
}

fn appendJsonString(buf: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, value: []const u8) !void {
    try buf.append(allocator, '"');
    for (value) |c| switch (c) {
        '"' => try buf.appendSlice(allocator, "\\\""),
        '\\' => try buf.appendSlice(allocator, "\\\\"),
        '\n' => try buf.appendSlice(allocator, "\\n"),
        '\r' => try buf.appendSlice(allocator, "\\r"),
        '\t' => try buf.appendSlice(allocator, "\\t"),
        else => {
            if (c < 0x20) {
                const escaped = try std.fmt.allocPrint(allocator, "\\u{X:0>4}", .{@as(u8, c)});
                defer allocator.free(escaped);
                try buf.appendSlice(allocator, escaped);
            } else {
                try buf.append(allocator, c);
            }
        },
    };
    try buf.append(allocator, '"');
}

fn appendAddedToken(
    buf: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    first: *bool,
    token: []const u8,
    id: i64,
) !void {
    if (!first.*) try buf.append(allocator, ',');
    first.* = false;
    try buf.appendSlice(allocator, "{\"id\":");
    const id_bytes = try std.fmt.allocPrint(allocator, "{d}", .{id});
    defer allocator.free(id_bytes);
    try buf.appendSlice(allocator, id_bytes);
    try buf.appendSlice(allocator, ",\"content\":");
    try appendJsonString(buf, allocator, token);
    try buf.appendSlice(allocator, ",\"special\":true}");
}

fn supportsBpeFallback(parsed: *const gguf_format.File, model_name: []const u8) bool {
    _ = model_name;
    return hasStringMetadataArray(parsed, "tokenizer.ggml.tokens") and
        hasStringMetadataArray(parsed, "tokenizer.ggml.merges");
}

fn supportsUnigramFallback(parsed: *const gguf_format.File, model_name: []const u8) bool {
    return (std.mem.eql(u8, model_name, "llama") or std.mem.startsWith(u8, model_name, "gemma")) and
        hasStringMetadataArray(parsed, "tokenizer.ggml.tokens") and
        hasNumericMetadataArray(parsed, "tokenizer.ggml.scores") and
        hasNumericMetadataArray(parsed, "tokenizer.ggml.token_type");
}

fn hasStringMetadataArray(parsed: *const gguf_format.File, key: []const u8) bool {
    const entry = findMetadataEntry(parsed, key) orelse return false;
    return entry.value == .array and entry.value.array.element_type == .string;
}

fn hasNumericMetadataArray(parsed: *const gguf_format.File, key: []const u8) bool {
    const entry = findMetadataEntry(parsed, key) orelse return false;
    if (entry.value != .array) return false;
    return switch (entry.value.array.element_type) {
        .f32, .f64, .i32, .i64, .u32, .u64 => true,
        else => false,
    };
}

fn findMetadataEntry(parsed: *const gguf_format.File, key: []const u8) ?*const gguf_format.MetadataEntry {
    for (parsed.metadata) |*entry| {
        if (std.mem.eql(u8, entry.key, key)) return entry;
    }
    return null;
}

fn getRequiredMetadataArray(
    parsed: *const gguf_format.File,
    key: []const u8,
    expected_element_type: ?gguf_format.MetadataValueType,
) !gguf_format.MetadataArray {
    const entry = findMetadataEntry(parsed, key) orelse return error.NoTokenizerFound;
    const arr = switch (entry.value) {
        .array => |value| value,
        else => return error.InvalidTokenizerMetadata,
    };
    if (expected_element_type) |expected| {
        if (arr.element_type != expected) return error.InvalidTokenizerMetadata;
    }
    return arr;
}

fn metadataTokenId(parsed: *const gguf_format.File, key: []const u8) ?i32 {
    const view = gguf_metadata.View.init(parsed);
    const raw_id = view.getU64(key) orelse return null;
    return std.math.cast(i32, raw_id);
}

fn metadataTokenStringById(tokens: gguf_format.MetadataArray, token_id: i32) ?[]const u8 {
    if (token_id < 0) return null;
    const token_index: usize = @intCast(token_id);
    if (token_index >= tokens.values.len) return null;
    return switch (tokens.values[token_index]) {
        .string => |value| value,
        else => null,
    };
}

fn metadataI64At(arr: gguf_format.MetadataArray, index: usize) !i64 {
    if (index >= arr.values.len) return error.InvalidTokenizerMetadata;
    return switch (arr.values[index]) {
        .i32 => |value| value,
        .i64 => |value| value,
        .u32 => |value| value,
        .u64 => |value| std.math.cast(i64, value) orelse return error.InvalidTokenizerMetadata,
        else => return error.InvalidTokenizerMetadata,
    };
}

fn metadataF32At(arr: gguf_format.MetadataArray, index: usize) !f32 {
    if (index >= arr.values.len) return error.InvalidTokenizerMetadata;
    return switch (arr.values[index]) {
        .f32 => |value| value,
        .f64 => |value| @floatCast(value),
        .i32 => |value| @floatFromInt(value),
        .i64 => |value| @floatFromInt(value),
        .u32 => |value| @floatFromInt(value),
        .u64 => |value| @floatFromInt(value),
        else => return error.InvalidTokenizerMetadata,
    };
}

fn appendSpecialTokensFromMetadata(
    buf: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    parsed: *const gguf_format.File,
    tokens: gguf_format.MetadataArray,
    token_types: ?gguf_format.MetadataArray,
) !void {
    var first_added = true;
    var seen = std.AutoHashMapUnmanaged(i64, void){};
    defer seen.deinit(allocator);

    const special_id_keys = [_][]const u8{
        "tokenizer.ggml.bos_token_id",
        "tokenizer.ggml.eos_token_id",
        "tokenizer.ggml.padding_token_id",
        "tokenizer.ggml.unknown_token_id",
    };
    for (special_id_keys) |key| {
        const token_id = metadataTokenId(parsed, key) orelse continue;
        const token = metadataTokenStringById(tokens, token_id) orelse continue;
        if (seen.contains(token_id)) continue;
        try seen.put(allocator, token_id, {});
        try appendAddedToken(buf, allocator, &first_added, token, token_id);
    }

    if (token_types) |types| {
        for (tokens.values, 0..) |token_value, idx| {
            const token = switch (token_value) {
                .string => |value| value,
                else => return error.InvalidTokenizerMetadata,
            };
            const token_type = try metadataI64At(types, idx);
            if (token_type == 1 or token_type == 6) continue;
            const token_id: i64 = @intCast(idx);
            if (seen.contains(token_id)) continue;
            try seen.put(allocator, token_id, {});
            try appendAddedToken(buf, allocator, &first_added, token, token_id);
        }
    }
}

fn bpeTokenizerJsonFromGguf(allocator: std.mem.Allocator, parsed: *const gguf_format.File) ![]u8 {
    const tokens = try getRequiredMetadataArray(parsed, "tokenizer.ggml.tokens", .string);
    const merges = try getRequiredMetadataArray(parsed, "tokenizer.ggml.merges", .string);
    const token_types = if (findMetadataEntry(parsed, "tokenizer.ggml.token_type") != null)
        try getRequiredMetadataArray(parsed, "tokenizer.ggml.token_type", null)
    else
        null;

    var tokenizer_json = std.ArrayListUnmanaged(u8).empty;
    defer tokenizer_json.deinit(allocator);

    try tokenizer_json.appendSlice(allocator, "{\"model\":{\"type\":\"BPE\",\"byte_fallback\":false,\"vocab\":{");
    for (tokens.values, 0..) |token_value, idx| {
        const token = switch (token_value) {
            .string => |value| value,
            else => return error.InvalidTokenizerMetadata,
        };
        if (idx > 0) try tokenizer_json.append(allocator, ',');
        try appendJsonString(&tokenizer_json, allocator, token);
        try tokenizer_json.append(allocator, ':');
        const id_bytes = try std.fmt.allocPrint(allocator, "{d}", .{idx});
        defer allocator.free(id_bytes);
        try tokenizer_json.appendSlice(allocator, id_bytes);
    }
    try tokenizer_json.appendSlice(allocator, "},\"merges\":[");
    for (merges.values, 0..) |merge_value, idx| {
        const merge = switch (merge_value) {
            .string => |value| value,
            else => return error.InvalidTokenizerMetadata,
        };
        if (idx > 0) try tokenizer_json.append(allocator, ',');
        try appendJsonString(&tokenizer_json, allocator, merge);
    }
    try tokenizer_json.appendSlice(allocator, "]},\"pre_tokenizer\":{\"type\":\"ByteLevel\"},\"added_tokens\":[");
    try appendSpecialTokensFromMetadata(&tokenizer_json, allocator, parsed, tokens, token_types);
    try tokenizer_json.appendSlice(allocator, "]}");
    return tokenizer_json.toOwnedSlice(allocator);
}

fn unigramTokenizerJsonFromGguf(allocator: std.mem.Allocator, parsed: *const gguf_format.File) ![]u8 {
    const view = gguf_metadata.View.init(parsed);
    const tokens = try getRequiredMetadataArray(parsed, "tokenizer.ggml.tokens", .string);
    const scores = try getRequiredMetadataArray(parsed, "tokenizer.ggml.scores", null);
    const token_types = try getRequiredMetadataArray(parsed, "tokenizer.ggml.token_type", null);
    if (tokens.values.len != scores.values.len or tokens.values.len != token_types.values.len) {
        return error.InvalidTokenizerMetadata;
    }

    const unk_id = metadataTokenId(parsed, "tokenizer.ggml.unknown_token_id") orelse 0;
    const add_space_prefix = view.getBool("tokenizer.ggml.add_space_prefix") orelse true;

    var tokenizer_json = std.ArrayListUnmanaged(u8).empty;
    defer tokenizer_json.deinit(allocator);

    try tokenizer_json.appendSlice(allocator, "{\"model\":{\"type\":\"Unigram\",\"unk_id\":");
    const unk_id_bytes = try std.fmt.allocPrint(allocator, "{d}", .{unk_id});
    defer allocator.free(unk_id_bytes);
    try tokenizer_json.appendSlice(allocator, unk_id_bytes);
    try tokenizer_json.appendSlice(allocator, ",\"vocab\":[");
    for (tokens.values, 0..) |token_value, idx| {
        const token = switch (token_value) {
            .string => |value| value,
            else => return error.InvalidTokenizerMetadata,
        };
        const score = try metadataF32At(scores, idx);
        if (idx > 0) try tokenizer_json.append(allocator, ',');
        try tokenizer_json.append(allocator, '[');
        try appendJsonString(&tokenizer_json, allocator, token);
        try tokenizer_json.append(allocator, ',');
        const score_bytes = try std.fmt.allocPrint(allocator, "{d}", .{score});
        defer allocator.free(score_bytes);
        try tokenizer_json.appendSlice(allocator, score_bytes);
        try tokenizer_json.append(allocator, ']');
    }
    try tokenizer_json.appendSlice(allocator, "]},\"pre_tokenizer\":{\"type\":\"Metaspace\",\"replacement\":\"\\u2581\",\"prepend_scheme\":");
    try appendJsonString(&tokenizer_json, allocator, if (add_space_prefix) "always" else "never");
    try tokenizer_json.appendSlice(allocator, ",\"split\":true");
    try tokenizer_json.appendSlice(allocator, "},\"added_tokens\":[");
    try appendSpecialTokensFromMetadata(&tokenizer_json, allocator, parsed, tokens, token_types);
    try tokenizer_json.appendSlice(allocator, "]}");
    return tokenizer_json.toOwnedSlice(allocator);
}
