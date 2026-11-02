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

// Model manager: lazy-loads models and caches ready-to-use pipelines.
//
// Given a model directory path, loads the manifest, creates a tokenizer
// and backend session, and returns a pipeline ready for inference.

//! Model tokenizer artifact loading and checkpoint conventions. This owner
//! performs no serving-cache publication, eviction or admission lease handling;
//! callers admit construction peaks before entering it and retain the result.
const std = @import("std");
const platform = @import("antfly_platform");
const manifest_mod = @import("manifest.zig");
const c_file = @import("../util/c_file.zig");
const hf_tokenizer = @import("inference_hf_tokenizer");
const sentencepiece = @import("inference_tokenizer").sentencepiece;
const gguf_format = @import("../gguf/format.zig");
const gguf_metadata = @import("../gguf/metadata.zig");

pub fn shouldPreferSentencePieceOverride(man: manifest_mod.ModelManifest, model_dir: []const u8, allocator: std.mem.Allocator) bool {
    if (!c_file.fileExistsInDir(allocator, model_dir, "tokenizer.model")) return false;
    return manifestLooksLikeGemma(man, model_dir, allocator);
}

pub fn shouldEnableGemmaSentencePieceCompat(man: manifest_mod.ModelManifest, model_dir: []const u8, allocator: std.mem.Allocator) bool {
    return manifestLooksLikeGemma(man, model_dir, allocator);
}

pub const sentencepiece_model_filename = "tokenizer.model";
pub const sentencepiece_sidecars = [_]struct { filename: []const u8, format: enum { token_map, tokenizer_json } }{
    .{ .filename = "added_tokens.json", .format = .token_map },
    .{ .filename = "tokenizer.json", .format = .tokenizer_json },
};

fn readOptionalFile(allocator: std.mem.Allocator, path: []const u8) !?[]u8 {
    return c_file.readFile(allocator, path) catch |err| switch (err) {
        error.FileNotFound => null,
        else => return err,
    };
}

pub fn loadSentencePieceAddedTokens(model_dir: []const u8, allocator: std.mem.Allocator, sp: *sentencepiece.Processor) !void {
    inline for (sentencepiece_sidecars) |sidecar| {
        const path = try std.fs.path.join(allocator, &.{ model_dir, sidecar.filename });
        defer allocator.free(path);
        if (try readOptionalFile(allocator, path)) |bytes| {
            defer allocator.free(bytes);
            switch (sidecar.format) {
                .token_map => try loadSentencePieceAddedTokenMap(allocator, bytes, sp),
                .tokenizer_json => try loadSentencePieceAddedTokenArray(allocator, bytes, sp),
            }
        }
    }
}

fn externalTokenId(value: i64) !i32 {
    if (value < 0) return error.InvalidTokenizerMetadata;
    return std.math.cast(i32, value) orelse error.InvalidTokenizerMetadata;
}

fn loadSentencePieceAddedTokenMap(allocator: std.mem.Allocator, json_bytes: []const u8, sp: *sentencepiece.Processor) !void {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return;
    var tokens = std.ArrayListUnmanaged(sentencepiece.Processor.ExternalSpecialToken).empty;
    defer tokens.deinit(allocator);
    var it = parsed.value.object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .integer) continue;
        try tokens.append(allocator, .{ .text = entry.key_ptr.*, .id = try externalTokenId(entry.value_ptr.integer) });
    }
    try sp.addExternalSpecialTokens(tokens.items);
}

fn loadSentencePieceAddedTokenArray(allocator: std.mem.Allocator, json_bytes: []const u8, sp: *sentencepiece.Processor) !void {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return;
    const added_tokens = parsed.value.object.get("added_tokens") orelse return;
    if (added_tokens != .array) return;
    var tokens = std.ArrayListUnmanaged(sentencepiece.Processor.ExternalSpecialToken).empty;
    defer tokens.deinit(allocator);
    for (added_tokens.array.items) |item| {
        if (item != .object) continue;
        const content = item.object.get("content") orelse continue;
        const id = item.object.get("id") orelse continue;
        if (content != .string or id != .integer) continue;
        try tokens.append(allocator, .{ .text = content.string, .id = try externalTokenId(id.integer) });
    }
    try sp.addExternalSpecialTokens(tokens.items);
}

fn manifestLooksLikeGemma(man: manifest_mod.ModelManifest, model_dir: []const u8, allocator: std.mem.Allocator) bool {
    if (std.mem.startsWith(u8, man.config_model_arch, "gemma")) return true;
    if (std.mem.indexOf(u8, model_dir, "gemma") != null) return true;

    const cfg_path = std.fmt.allocPrint(allocator, "{s}/config.json", .{model_dir}) catch return false;
    defer allocator.free(cfg_path);
    const cfg_bytes = c_file.readFile(allocator, cfg_path) catch return false;
    defer allocator.free(cfg_bytes);

    const parsed = std.json.parseFromSlice(std.json.Value, allocator, cfg_bytes, .{}) catch return false;
    defer parsed.deinit();
    if (parsed.value != .object) return false;
    const obj = parsed.value.object;
    const model_type = obj.get("model_type") orelse return false;
    if (model_type != .string) return false;
    return std.mem.startsWith(u8, model_type.string, "gemma");
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

const LegacyWordPieceMeta = struct {
    do_lower_case: bool = false,
    unk_token: []const u8 = "[UNK]",
    pad_token: []const u8 = "[PAD]",
    cls_token: []const u8 = "[CLS]",
    sep_token: []const u8 = "[SEP]",
    mask_token: []const u8 = "[MASK]",
    unk_token_owned: ?[]u8 = null,
    pad_token_owned: ?[]u8 = null,
    cls_token_owned: ?[]u8 = null,
    sep_token_owned: ?[]u8 = null,
    mask_token_owned: ?[]u8 = null,

    pub fn deinit(self: *LegacyWordPieceMeta, allocator: std.mem.Allocator) void {
        if (self.unk_token_owned) |buf| allocator.free(buf);
        if (self.pad_token_owned) |buf| allocator.free(buf);
        if (self.cls_token_owned) |buf| allocator.free(buf);
        if (self.sep_token_owned) |buf| allocator.free(buf);
        if (self.mask_token_owned) |buf| allocator.free(buf);
    }
};

fn replaceLegacyToken(allocator: std.mem.Allocator, slot: *[]const u8, owned_slot: *?[]u8, value: []const u8) !void {
    const duped = try allocator.dupe(u8, value);
    if (owned_slot.*) |buf| allocator.free(buf);
    owned_slot.* = duped;
    slot.* = duped;
}

fn extractLegacyTokenString(val: std.json.Value) ?[]const u8 {
    return switch (val) {
        .string => |s| s,
        .object => |obj| blk: {
            if (obj.get("content")) |content| {
                if (content == .string) break :blk content.string;
            }
            break :blk null;
        },
        else => null,
    };
}

fn applyLegacyTokenizerJson(meta: *LegacyWordPieceMeta, json_bytes: []const u8, allocator: std.mem.Allocator) !void {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidTokenizerMetadata;
    const obj = parsed.value.object;

    if (obj.get("do_lower_case")) |v| {
        if (v == .bool) meta.do_lower_case = v.bool;
    }
    if (obj.get("unk_token")) |v| {
        if (extractLegacyTokenString(v)) |s| try replaceLegacyToken(allocator, &meta.unk_token, &meta.unk_token_owned, s);
    }
    if (obj.get("pad_token")) |v| {
        if (extractLegacyTokenString(v)) |s| try replaceLegacyToken(allocator, &meta.pad_token, &meta.pad_token_owned, s);
    }
    if (obj.get("cls_token")) |v| {
        if (extractLegacyTokenString(v)) |s| try replaceLegacyToken(allocator, &meta.cls_token, &meta.cls_token_owned, s);
    }
    if (obj.get("sep_token")) |v| {
        if (extractLegacyTokenString(v)) |s| try replaceLegacyToken(allocator, &meta.sep_token, &meta.sep_token_owned, s);
    }
    if (obj.get("mask_token")) |v| {
        if (extractLegacyTokenString(v)) |s| try replaceLegacyToken(allocator, &meta.mask_token, &meta.mask_token_owned, s);
    }
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

fn loadLegacyWordPieceTokenizerFromDir(allocator: std.mem.Allocator, model_dir: []const u8) !*hf_tokenizer.HfTokenizer {
    const vocab_path = try std.fmt.allocPrint(allocator, "{s}/vocab.txt", .{model_dir});
    defer allocator.free(vocab_path);
    const config_path = try std.fs.path.join(allocator, &.{ model_dir, "tokenizer_config.json" });
    defer allocator.free(config_path);
    const special_path = try std.fs.path.join(allocator, &.{ model_dir, "special_tokens_map.json" });
    defer allocator.free(special_path);
    return loadLegacyWordPieceTokenizer(allocator, vocab_path, config_path, special_path);
}

fn loadLegacyWordPieceTokenizer(
    allocator: std.mem.Allocator,
    vocab_path: []const u8,
    config_path: ?[]const u8,
    special_path: ?[]const u8,
) !*hf_tokenizer.HfTokenizer {
    const vocab_bytes = try c_file.readFile(allocator, vocab_path);
    defer allocator.free(vocab_bytes);
    var meta = LegacyWordPieceMeta{};
    defer meta.deinit(allocator);
    var metadata_bytes: [2]?[]u8 = .{ null, null };
    defer for (metadata_bytes) |bytes| {
        if (bytes) |data| allocator.free(data);
    };
    for ([_]?[]const u8{ config_path, special_path }, 0..) |path, i| {
        if (path) |selected| {
            const bytes = (try readOptionalFile(allocator, selected)) orelse continue;
            metadata_bytes[i] = bytes;
            try applyLegacyTokenizerJson(&meta, bytes, allocator);
        }
    }

    var vocab_entries = std.ArrayListUnmanaged([]const u8).empty;
    defer vocab_entries.deinit(allocator);

    var line_it = std.mem.tokenizeScalar(u8, vocab_bytes, '\n');
    while (line_it.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, "\r");
        if (line.len == 0) continue;
        try vocab_entries.append(allocator, line);
    }

    var unk_id: i64 = -1;
    var pad_id: i64 = -1;
    var cls_id: i64 = -1;
    var sep_id: i64 = -1;
    var mask_id: i64 = -1;
    for (vocab_entries.items, 0..) |token, idx| {
        const id: i64 = @intCast(idx);
        if (std.mem.eql(u8, token, meta.unk_token)) unk_id = id;
        if (std.mem.eql(u8, token, meta.pad_token)) pad_id = id;
        if (std.mem.eql(u8, token, meta.cls_token)) cls_id = id;
        if (std.mem.eql(u8, token, meta.sep_token)) sep_id = id;
        if (std.mem.eql(u8, token, meta.mask_token)) mask_id = id;
    }

    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(allocator);

    try buf.appendSlice(allocator, "{\"model\":{\"type\":\"WordPiece\",\"unk_token\":");
    try appendJsonString(&buf, allocator, meta.unk_token);
    try buf.appendSlice(allocator, ",\"continuing_subword_prefix\":\"##\",\"max_input_chars_per_word\":100,\"vocab\":{");
    for (vocab_entries.items, 0..) |token, idx| {
        if (idx > 0) try buf.append(allocator, ',');
        try appendJsonString(&buf, allocator, token);
        try buf.append(allocator, ':');
        const id_bytes = try std.fmt.allocPrint(allocator, "{d}", .{idx});
        defer allocator.free(id_bytes);
        try buf.appendSlice(allocator, id_bytes);
    }
    try buf.appendSlice(allocator, "}},\"normalizer\":{\"type\":\"BertNormalizer\",\"lowercase\":");
    try buf.appendSlice(allocator, if (meta.do_lower_case) "true" else "false");
    try buf.appendSlice(allocator, "},\"pre_tokenizer\":{\"type\":\"BertPreTokenizer\"},\"added_tokens\":[");

    var first_added = true;
    if (pad_id >= 0) try appendAddedToken(&buf, allocator, &first_added, meta.pad_token, pad_id);
    if (unk_id >= 0) try appendAddedToken(&buf, allocator, &first_added, meta.unk_token, unk_id);
    if (cls_id >= 0) try appendAddedToken(&buf, allocator, &first_added, meta.cls_token, cls_id);
    if (sep_id >= 0) try appendAddedToken(&buf, allocator, &first_added, meta.sep_token, sep_id);
    if (mask_id >= 0) try appendAddedToken(&buf, allocator, &first_added, meta.mask_token, mask_id);
    try buf.appendSlice(allocator, "]");

    if (cls_id >= 0 and sep_id >= 0) {
        try buf.appendSlice(allocator, ",\"post_processor\":{\"type\":\"BertProcessing\",\"cls\":[");
        try appendJsonString(&buf, allocator, meta.cls_token);
        const cls_id_bytes = try std.fmt.allocPrint(allocator, ",{d}],\"sep\":[", .{cls_id});
        defer allocator.free(cls_id_bytes);
        try buf.appendSlice(allocator, cls_id_bytes);
        try appendJsonString(&buf, allocator, meta.sep_token);
        const sep_id_bytes = try std.fmt.allocPrint(allocator, ",{d}]", .{sep_id});
        defer allocator.free(sep_id_bytes);
        try buf.appendSlice(allocator, sep_id_bytes);
        try buf.appendSlice(allocator, "}");
    }

    try buf.append(allocator, '}');
    const tokenizer_json = try buf.toOwnedSlice(allocator);
    defer allocator.free(tokenizer_json);
    return hf_tokenizer.HfTokenizer.loadFromBytes(allocator, tokenizer_json);
}

pub fn loadHuggingFaceTokenizerFile(allocator: std.mem.Allocator, path: []const u8) !*hf_tokenizer.HfTokenizer {
    const bytes = try c_file.readFile(allocator, path);
    defer allocator.free(bytes);
    return hf_tokenizer.HfTokenizer.loadFromBytes(allocator, bytes);
}

pub fn loadHuggingFaceTokenizerFromDir(allocator: std.mem.Allocator, model_dir: []const u8) !*hf_tokenizer.HfTokenizer {
    return loadHuggingFaceTokenizerFromDirOrGguf(allocator, model_dir, null);
}

pub fn loadHuggingFaceTokenizerFromDirOrGguf(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    gguf_path: ?[]const u8,
) !*hf_tokenizer.HfTokenizer {
    const tok_path = try std.fmt.allocPrint(allocator, "{s}/tokenizer.json", .{model_dir});
    defer allocator.free(tok_path);
    if (c_file.readFile(allocator, tok_path)) |tok_bytes| {
        defer allocator.free(tok_bytes);
        return hf_tokenizer.HfTokenizer.loadFromBytes(allocator, tok_bytes);
    } else |_| {}

    if (c_file.fileExistsInDir(allocator, model_dir, "vocab.txt")) {
        return loadLegacyWordPieceTokenizerFromDir(allocator, model_dir);
    }

    if (gguf_path) |path| {
        return loadHuggingFaceTokenizerFromGguf(allocator, path);
    }

    return error.NoTokenizerFound;
}

pub fn loadHuggingFaceTokenizerFromManifest(allocator: std.mem.Allocator, man: *const manifest_mod.ModelManifest) !*hf_tokenizer.HfTokenizer {
    if (man.tokenizer_json_path) |path| {
        return loadHuggingFaceTokenizerFile(allocator, path);
    }
    if (man.vocab_txt_path) |path| {
        return loadLegacyWordPieceTokenizer(allocator, path, man.tokenizer_config_path, man.special_tokens_map_path);
    }
    if (man.gguf_path) |path| return loadHuggingFaceTokenizerFromGguf(allocator, path);
    return error.NoTokenizerFound;
}

fn loadHuggingFaceTokenizerFromGguf(allocator: std.mem.Allocator, gguf_path: []const u8) !*hf_tokenizer.HfTokenizer {
    var region = try c_file.MmapRegion.init(allocator, gguf_path);
    defer region.deinit();
    // Tokenizer extraction is metadata-only. A whole-file DONTNEED here would
    // discard a warmed checkpoint immediately before CUDA model admission.
    region.preserveFileCacheOnDeinit();

    const parse_allocator = platform.allocator.processAllocator(allocator);
    var parsed = try gguf_format.parse(parse_allocator, region.data);
    defer parsed.deinit(parse_allocator);

    const view = gguf_metadata.View.init(&parsed);
    const model_name = view.getString("tokenizer.ggml.model") orelse return error.NoTokenizerFound;

    const tokenizer_bytes = if (std.mem.eql(u8, model_name, "t5"))
        try unigramTokenizerJsonFromGguf(allocator, &parsed)
    else blk: {
        const flavor: GgufBpeTokenizerFlavor = if (std.mem.eql(u8, model_name, "gpt2"))
            .byte_level
        else if (std.mem.eql(u8, model_name, "gemma4"))
            .gemma4
        else
            return error.NoTokenizerFound;
        break :blk try bpeTokenizerJsonFromGguf(allocator, &parsed, flavor);
    };
    defer allocator.free(tokenizer_bytes);

    const tok = try hf_tokenizer.HfTokenizer.loadFromBytesAllowDuplicateFields(allocator, tokenizer_bytes);
    errdefer tok.deinitSelf();
    const tokens = try getRequiredMetadataArray(&parsed, "tokenizer.ggml.tokens", .string);
    for (tokens.values, 0..) |token_value, index| {
        const token = switch (token_value) {
            .string => |value| value,
            else => return error.InvalidTokenizerMetadata,
        };
        const id = std.math.cast(i32, index) orelse return error.InvalidTokenizerMetadata;
        try tok.addTokenIdAliasForDecode(token, id);
    }
    const bos_id = metadataTokenId(&parsed, "tokenizer.ggml.bos_token_id");
    const eos_id = metadataTokenId(&parsed, "tokenizer.ggml.eos_token_id");
    tok.applySpecialTokenIds(
        bos_id,
        eos_id,
        metadataTokenId(&parsed, "tokenizer.ggml.padding_token_id"),
        metadataTokenId(&parsed, "tokenizer.ggml.unknown_token_id"),
    );
    // Honor the checkpoint's declared wrap semantics when present. Without
    // this, encodeForModel wraps with bos AND eos unconditionally — for
    // Qwen3-Embedding GGUFs (add_bos_token=false, add_eos_token=true) that
    // injects a leading <|endoftext|> the HF reference does not produce,
    // shifting every position and breaking embedding parity. GGUFs that
    // declare neither flag keep the legacy wrap unchanged.
    const gguf_view = gguf_metadata.View.init(&parsed);
    const add_bos = gguf_view.getBool("tokenizer.ggml.add_bos_token");
    const add_eos = gguf_view.getBool("tokenizer.ggml.add_eos_token");
    if (add_bos != null or add_eos != null) {
        var prepend_buf: [1]i32 = undefined;
        var append_buf: [1]i32 = undefined;
        var prepend: []const i32 = &.{};
        var append: []const i32 = &.{};
        if ((add_bos orelse false) and bos_id != null) {
            prepend_buf[0] = bos_id.?;
            prepend = prepend_buf[0..1];
        }
        if ((add_eos orelse false) and eos_id != null) {
            append_buf[0] = eos_id.?;
            append = append_buf[0..1];
        }
        try tok.applyModelWrapTemplate(prepend, append);
    }
    return tok;
}

const GgufBpeTokenizerFlavor = enum {
    byte_level,
    gemma4,
};

pub fn unigramTokenizerJsonFromGguf(
    allocator: std.mem.Allocator,
    parsed: *const gguf_format.File,
) ![]u8 {
    const tokens = try getRequiredMetadataArray(parsed, "tokenizer.ggml.tokens", .string);
    const scores = try getRequiredMetadataArray(parsed, "tokenizer.ggml.scores", null);
    const token_types = try getRequiredMetadataArray(parsed, "tokenizer.ggml.token_type", null);
    if (tokens.values.len != scores.values.len or tokens.values.len != token_types.values.len) {
        return error.InvalidTokenizerMetadata;
    }
    const unknown_id = metadataTokenId(parsed, "tokenizer.ggml.unknown_token_id") orelse
        return error.InvalidTokenizerMetadata;
    const add_space_prefix = gguf_metadata.View.init(parsed).getBool("tokenizer.ggml.add_space_prefix") orelse true;

    var tokenizer_json = std.ArrayListUnmanaged(u8).empty;
    defer tokenizer_json.deinit(allocator);
    try tokenizer_json.appendSlice(allocator, "{\"model\":{\"type\":\"Unigram\",\"unk_id\":");
    var unknown_id_buf: [32]u8 = undefined;
    try tokenizer_json.appendSlice(allocator, try std.fmt.bufPrint(&unknown_id_buf, "{d}", .{unknown_id}));
    try tokenizer_json.appendSlice(allocator, ",\"vocab\":[");
    for (tokens.values, scores.values, 0..) |token_value, score_value, index| {
        const token = switch (token_value) {
            .string => |value| value,
            else => return error.InvalidTokenizerMetadata,
        };
        const score = switch (score_value) {
            .f32 => |value| value,
            .f64 => |value| @as(f32, @floatCast(value)),
            else => return error.InvalidTokenizerMetadata,
        };
        if (index > 0) try tokenizer_json.append(allocator, ',');
        try tokenizer_json.append(allocator, '[');
        try appendJsonString(&tokenizer_json, allocator, token);
        var score_buf: [64]u8 = undefined;
        try tokenizer_json.appendSlice(allocator, try std.fmt.bufPrint(&score_buf, ",{d}]", .{score}));
    }
    try tokenizer_json.appendSlice(
        allocator,
        if (add_space_prefix)
            "]},\"pre_tokenizer\":{\"type\":\"Metaspace\",\"replacement\":\"\\u2581\",\"prepend_scheme\":\"always\",\"split\":true},\"added_tokens\":["
        else
            "]},\"pre_tokenizer\":{\"type\":\"Metaspace\",\"replacement\":\"\\u2581\",\"prepend_scheme\":\"never\",\"split\":true},\"added_tokens\":[",
    );
    try appendSpecialTokensFromMetadata(&tokenizer_json, allocator, parsed, tokens, token_types);
    try tokenizer_json.appendSlice(allocator, "]");
    if (findMetadataEntry(parsed, "tokenizer.ggml.precompiled_charsmap") != null) {
        const map = try getRequiredMetadataArray(parsed, "tokenizer.ggml.precompiled_charsmap", .u8);
        if (map.values.len > 12 * 1024 * 1024) return error.InvalidTokenizerMetadata;
        if (map.values.len > 0) {
            const bytes = try allocator.alloc(u8, map.values.len);
            defer allocator.free(bytes);
            for (map.values, bytes) |value, *byte| byte.* = switch (value) {
                .u8 => |v| v,
                else => return error.InvalidTokenizerMetadata,
            };
            const encoded = try allocator.alloc(u8, std.base64.standard.Encoder.calcSize(bytes.len));
            defer allocator.free(encoded);
            _ = std.base64.standard.Encoder.encode(encoded, bytes);
            try tokenizer_json.appendSlice(allocator, ",\"normalizer\":{\"type\":\"Sequence\",\"normalizers\":[{\"type\":\"Precompiled\",\"precompiled_charsmap\":");
            try appendJsonString(&tokenizer_json, allocator, encoded);
            try tokenizer_json.appendSlice(allocator, "}");
            if (gguf_metadata.View.init(parsed).getBool("tokenizer.ggml.remove_extra_whitespaces") orelse true)
                try tokenizer_json.appendSlice(allocator, ",{\"type\":\"Replace\",\"pattern\":{\"Regex\":\" {2,}\"},\"content\":\" \"}");
            try tokenizer_json.appendSlice(allocator, "]}");
        }
    }
    try tokenizer_json.appendSlice(allocator, "}");
    return tokenizer_json.toOwnedSlice(allocator);
}

fn bpeTokenizerJsonFromGguf(
    allocator: std.mem.Allocator,
    parsed: *const gguf_format.File,
    flavor: GgufBpeTokenizerFlavor,
) ![]u8 {
    const tokens = try getRequiredMetadataArray(parsed, "tokenizer.ggml.tokens", .string);
    const merges = try getRequiredMetadataArray(parsed, "tokenizer.ggml.merges", .string);
    const token_types = if (findMetadataEntry(parsed, "tokenizer.ggml.token_type") != null)
        try getRequiredMetadataArray(parsed, "tokenizer.ggml.token_type", null)
    else
        null;

    var tokenizer_json = std.ArrayListUnmanaged(u8).empty;
    defer tokenizer_json.deinit(allocator);

    switch (flavor) {
        .byte_level => {
            try tokenizer_json.appendSlice(allocator, "{\"model\":{\"type\":\"BPE\",\"byte_fallback\":false,\"vocab\":{");
        },
        .gemma4 => {
            try tokenizer_json.appendSlice(
                allocator,
                "{\"normalizer\":{\"type\":\"Replace\",\"pattern\":{\"String\":\" \"},\"content\":\"▁\"},\"pre_tokenizer\":{\"type\":\"Split\",\"pattern\":{\"String\":\" \"},\"behavior\":\"MergedWithPrevious\",\"invert\":false},\"model\":{\"type\":\"BPE\",\"fuse_unk\":true,\"byte_fallback\":true,\"vocab\":{",
            );
        },
    }
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
    switch (flavor) {
        .byte_level => try tokenizer_json.appendSlice(allocator, "]},\"pre_tokenizer\":{\"type\":\"ByteLevel\"},\"added_tokens\":["),
        .gemma4 => try tokenizer_json.appendSlice(allocator, "]},\"added_tokens\":["),
    }

    try appendSpecialTokensFromMetadata(&tokenizer_json, allocator, parsed, tokens, token_types);
    try tokenizer_json.appendSlice(allocator, "]}");

    return tokenizer_json.toOwnedSlice(allocator);
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

fn findMetadataEntry(parsed: *const gguf_format.File, key: []const u8) ?*const gguf_format.MetadataEntry {
    for (parsed.metadata) |*entry| {
        if (std.mem.eql(u8, entry.key, key)) return entry;
    }
    return null;
}

pub fn loadSentencePieceTokenizerFromDirOrGguf(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    gguf_path: ?[]const u8,
) !*sentencepiece.Processor {
    const sp = try allocator.create(sentencepiece.Processor);
    errdefer allocator.destroy(sp);

    if (c_file.fileExistsInDir(allocator, model_dir, "tokenizer.model")) {
        const sp_path = try std.fmt.allocPrint(allocator, "{s}/tokenizer.model", .{model_dir});
        defer allocator.free(sp_path);
        sp.* = try sentencepiece.Processor.initFromPath(allocator, sp_path);
        return sp;
    }

    const resolved_gguf_path = gguf_path orelse return error.NoTokenizerFound;
    sp.* = try loadSentencePieceTokenizerFromGguf(allocator, resolved_gguf_path);
    return sp;
}

pub fn adoptAndConfigureSentencePieceTokenizer(
    owned: *?*sentencepiece.Processor,
    sp: *sentencepiece.Processor,
    man: manifest_mod.ModelManifest,
    model_dir: []const u8,
    allocator: std.mem.Allocator,
) !void {
    std.debug.assert(owned.* == null);
    owned.* = sp;
    if (shouldEnableGemmaSentencePieceCompat(man, model_dir, allocator)) {
        sp.setPreserveInlineSpecialsAfterLiteralBos(true);
    }
    try loadSentencePieceAddedTokens(model_dir, allocator, sp);
}

fn loadSentencePieceTokenizerFromGguf(allocator: std.mem.Allocator, gguf_path: []const u8) !sentencepiece.Processor {
    var region = try c_file.MmapRegion.init(allocator, gguf_path);
    defer region.deinit();
    // Keep already-warm tensor payload pages; only metadata is consumed here.
    region.preserveFileCacheOnDeinit();

    const parse_allocator = platform.allocator.processAllocator(allocator);
    var parsed = try gguf_format.parse(parse_allocator, region.data);
    defer parsed.deinit(parse_allocator);

    const view = gguf_metadata.View.init(&parsed);
    const model_name = view.getString("tokenizer.ggml.model") orelse return error.NoTokenizerFound;
    if (!(std.mem.eql(u8, model_name, "llama") or
        std.mem.eql(u8, model_name, "t5") or
        std.mem.startsWith(u8, model_name, "gemma")))
    {
        return error.NoTokenizerFound;
    }

    const tokens = try getRequiredMetadataArray(&parsed, "tokenizer.ggml.tokens", .string);
    const scores = try getRequiredMetadataArray(&parsed, "tokenizer.ggml.scores", null);
    const token_types = try getRequiredMetadataArray(&parsed, "tokenizer.ggml.token_type", null);
    if (tokens.values.len != scores.values.len or tokens.values.len != token_types.values.len) {
        return error.InvalidTokenizerMetadata;
    }

    const unknown_token_index = view.getU64("tokenizer.ggml.unknown_token_id");
    const pieces = try allocator.alloc(sentencepiece.PieceInit, tokens.values.len);
    defer allocator.free(pieces);

    var saw_byte_piece = false;
    var saw_unknown_piece = false;
    for (tokens.values, 0..) |token_value, idx| {
        const token_text = switch (token_value) {
            .string => |value| value,
            else => return error.InvalidTokenizerMetadata,
        };
        const score = switch (scores.values[idx]) {
            .f32 => |value| value,
            .f64 => |value| @as(f32, @floatCast(value)),
            else => return error.InvalidTokenizerMetadata,
        };
        const token_type_i64 = switch (token_types.values[idx]) {
            .i32 => |value| value,
            .i64 => |value| value,
            .u32 => |value| @as(i64, value),
            .u64 => |value| std.math.cast(i64, value) orelse return error.InvalidTokenizerMetadata,
            else => return error.InvalidTokenizerMetadata,
        };
        if (token_type_i64 < 0 or token_type_i64 > std.math.maxInt(u8)) {
            return error.InvalidTokenizerMetadata;
        }
        var token_type: u8 = @intCast(token_type_i64);
        if (unknown_token_index) |unknown_id| {
            if (unknown_id == idx) token_type = 2;
        }
        if (token_type == 6) saw_byte_piece = true;
        if (token_type == 2) saw_unknown_piece = true;
        pieces[idx] = .{
            .text = token_text,
            .score = score,
            .piece_type = token_type,
        };
    }
    if (!saw_unknown_piece) return error.InvalidTokenizerMetadata;

    const add_dummy_prefix = view.getBool("tokenizer.ggml.add_space_prefix") orelse true;
    const remove_extra_whitespaces = view.getBool("tokenizer.ggml.remove_extra_whitespaces") orelse true;
    const unk_surface = blk: {
        const unk_id = unknown_token_index orelse break :blk " \xe2\x81\x87 ";
        if (unk_id >= tokens.values.len) break :blk " \xe2\x81\x87 ";
        break :blk switch (tokens.values[@intCast(unk_id)]) {
            .string => |value| value,
            else => " \xe2\x81\x87 ",
        };
    };

    return sentencepiece.Processor.initFromPieces(allocator, pieces, .{
        .byte_fallback = saw_byte_piece,
        .unk_surface = unk_surface,
        .add_dummy_prefix = add_dummy_prefix,
        .remove_extra_whitespaces = remove_extra_whitespaces,
    });
}

fn getRequiredMetadataArray(
    parsed: *const gguf_format.File,
    key: []const u8,
    expected_element_type: ?gguf_format.MetadataValueType,
) !gguf_format.MetadataArray {
    for (parsed.metadata) |entry| {
        if (!std.mem.eql(u8, entry.key, key)) continue;
        const arr = switch (entry.value) {
            .array => |value| value,
            else => return error.InvalidTokenizerMetadata,
        };
        if (expected_element_type) |elem_type| {
            if (arr.element_type != elem_type) return error.InvalidTokenizerMetadata;
        }
        return arr;
    }
    return error.InvalidTokenizerMetadata;
}

test "tokenizer source consumes tokenizer JSON when the separate added-token map is absent" {
    const a = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try dir.dir.writeFile(std.testing.io, .{ .sub_path = "tokenizer.json", .data = "{\"added_tokens\":[{\"content\":\"<control>\",\"id\":11}]}" });
    const root = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", dir.sub_path[0..] });
    defer a.free(root);
    var processor = try sentencepiece.Processor.initFromPieces(a, &.{
        .{ .text = "<unk>", .score = 0, .piece_type = 2 },
        .{ .text = "token", .score = -1, .piece_type = 1 },
    }, .{});
    defer processor.deinit();
    try loadSentencePieceAddedTokens(root, a, &processor);
    try std.testing.expectEqual(@as(?i32, 11), processor.extra_reserved_map.get("<control>"));
    try std.testing.expectEqual(@as(usize, 9), processor.special_matcher.findPrefixLen("<control>tail"));
    const decoded = try processor.decode(a, &.{11});
    defer a.free(decoded);
    try std.testing.expectEqualStrings("<control>", decoded);
}

test "tokenizer source keeps optional absence distinct from allocator and filesystem failures" {
    const a = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    const root = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", dir.sub_path[0..] });
    defer a.free(root);
    var processor = try sentencepiece.Processor.initFromPieces(a, &.{.{ .text = "<unk>", .score = 0, .piece_type = 2 }}, .{});
    defer processor.deinit();
    try loadSentencePieceAddedTokens(root, a, &processor);
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, loadSentencePieceAddedTokens(root, failing.allocator(), &processor));
    try dir.dir.writeFile(std.testing.io, .{ .sub_path = "blocked", .data = "file" });
    const blocked = try std.fs.path.join(a, &.{ root, "blocked" });
    defer a.free(blocked);
    try std.testing.expectError(error.NotDir, loadSentencePieceAddedTokens(blocked, a, &processor));
    try std.testing.expectEqual(@as(usize, 0), processor.extra_reserved_map.count());
}

test "tokenizer source rejects out-of-range IDs before publishing either overlay representation" {
    const a = std.testing.allocator;
    var processor = try sentencepiece.Processor.initFromPieces(a, &.{.{ .text = "<unk>", .score = 0, .piece_type = 2 }}, .{});
    defer processor.deinit();
    const invalid = [_]i64{ -1, 2147483648, std.math.maxInt(i64) };
    for (invalid) |id| {
        const map = try std.fmt.allocPrint(a, "{{\"<valid>\":7,\"<invalid>\":{d}}}", .{id});
        defer a.free(map);
        try std.testing.expectError(error.InvalidTokenizerMetadata, loadSentencePieceAddedTokenMap(a, map, &processor));
        const array = try std.fmt.allocPrint(a, "{{\"added_tokens\":[{{\"content\":\"<valid>\",\"id\":7}},{{\"content\":\"<invalid>\",\"id\":{d}}}]}}", .{id});
        defer a.free(array);
        try std.testing.expectError(error.InvalidTokenizerMetadata, loadSentencePieceAddedTokenArray(a, array, &processor));
        try std.testing.expectEqual(@as(usize, 0), processor.extra_reserved_map.count());
    }
}

test "tokenizer source compatibility uses resolved architecture and rejects non-object sidecars" {
    const a = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    const root = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", dir.sub_path[0..] });
    defer a.free(root);
    const man = manifest_mod.ModelManifest{ .allocator = a };
    for ([_][]const u8{ "null", "[]", "42", "\"gemma\"", "{\"model_type\":42}" }) |json| {
        try dir.dir.writeFile(std.testing.io, .{ .sub_path = "config.json", .data = json });
        try std.testing.expect(!shouldEnableGemmaSentencePieceCompat(man, root, a));
    }
    var resolved = man;
    resolved.config_model_arch = "gemma4";
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expect(shouldEnableGemmaSentencePieceCompat(resolved, root, failing.allocator()));
}
