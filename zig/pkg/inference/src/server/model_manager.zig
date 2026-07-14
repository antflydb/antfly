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

// Model manager: lazy-loads models and caches ready-to-use pipelines.
//
// Given a model directory path, loads the manifest, creates a tokenizer
// and backend session, and returns a pipeline ready for inference.

const std = @import("std");
const build_options = @import("build_options");
const platform = @import("antfly_platform");

const backends = @import("../backends/backends.zig");
const model_caps = @import("../models/capabilities.zig");
const manifest_mod = @import("../models/manifest.zig");
const c_file = @import("../util/c_file.zig");
const gguf_format = @import("../gguf/format.zig");
const gguf_metadata = @import("../gguf/metadata.zig");
const gguf_tensor_types = @import("../gguf/tensor_types.zig");
const gguf_writer = @import("../gguf/writer.zig");
const hf_tokenizer = @import("inference_hf_tokenizer");
const sentencepiece = @import("inference_tokenizer").sentencepiece;
const tokenizer_mod = @import("inference_tokenizer");
const embedding_mod = @import("../pipelines/embedding.zig");
const EmbeddingPipeline = embedding_mod.EmbeddingPipeline;
const EmbeddingConfig = embedding_mod.EmbeddingConfig;
const PoolingStrategy = embedding_mod.PoolingStrategy;
const RerankingPipeline = @import("../pipelines/reranking.zig").RerankingPipeline;
const RerankingConfig = @import("../pipelines/reranking.zig").RerankingConfig;
const ScoringMode = @import("../pipelines/reranking.zig").ScoringMode;
const ClassificationPipeline = @import("../pipelines/classification.zig").ClassificationPipeline;
const ClassificationConfig = @import("../pipelines/classification.zig").ClassificationConfig;
const cleanup_model_mod = @import("../finetune/entity_cleanup_model.zig");
const NerPipeline = @import("../pipelines/ner.zig").NerPipeline;
const NerConfig = @import("../pipelines/ner.zig").NerConfig;
const GlinerPipeline = @import("../pipelines/gliner.zig").GlinerPipeline;
const GlinerConfig = @import("../pipelines/gliner.zig").GlinerConfig;
const generation = @import("../pipelines/generation.zig");
const ChatTemplate = generation.ChatTemplate;
const session_factory = @import("../architectures/session_factory.zig");
const graph_mod = @import("../graph/root.zig");
const runtime = @import("../runtime/root.zig");

fn shouldPreferNativeSession(man: manifest_mod.ModelManifest) bool {
    // GLiNER has a native DeBERTa + span-head path. When native weights are
    // present, prefer the directory-backed session so the model does not get
    // pinned to ONNX just because an export also exists.
    if (!manifestHasNativeAssets(man)) return false;
    if (man.model_type == .embedder and
        man.visual_model_path == null and
        man.audio_model_path == null and
        man.text_projection_path == null and
        man.visual_projection_path == null and
        man.audio_projection_path == null)
    {
        return true;
    }
    if (man.gliner_model_type.len > 0) return true;
    switch (man.model_type) {
        .classifier, .recognizer => return true,
        else => {},
    }
    return switch (man.native_arch_hint) {
        .clip, .whisper, .florence, .layoutlmv3 => true,
        .clap, .none => false,
    };
}

fn nativeBackendsAvailable() bool {
    return build_options.enable_native or build_options.enable_metal or build_options.enable_cuda;
}

fn manifestHasNativeAssets(man: manifest_mod.ModelManifest) bool {
    return man.gguf_path != null or man.safetensors_path != null or man.safetensors_index_path != null;
}

fn shouldUseMetalWholeModelExecutor(session: backends.Session) bool {
    return session.backend() == .metal;
}

fn spinLock(m: *std.atomic.Mutex) void {
    while (!m.tryLock()) {
        std.atomic.spinLoopHint();
    }
}

fn lockIoMutexWithoutIo(m: *std.Io.Mutex) std.Io {
    while (!m.tryLock()) platform.time.yieldBriefly();
    return std.Io.Threaded.global_single_threaded.io();
}

pub fn shouldPreferSentencePieceOverride(man: manifest_mod.ModelManifest, model_dir: []const u8, allocator: std.mem.Allocator) bool {
    if (!c_file.fileExistsInDir(allocator, model_dir, "tokenizer.model")) return false;
    return manifestLooksLikeGemma(man, model_dir, allocator);
}

pub fn shouldEnableGemmaSentencePieceCompat(man: manifest_mod.ModelManifest, model_dir: []const u8, allocator: std.mem.Allocator) bool {
    return manifestLooksLikeGemma(man, model_dir, allocator);
}

pub fn loadSentencePieceAddedTokens(model_dir: []const u8, allocator: std.mem.Allocator, sp: *sentencepiece.Processor) !void {
    const added_tokens_path = std.fmt.allocPrint(allocator, "{s}/added_tokens.json", .{model_dir}) catch return;
    defer allocator.free(added_tokens_path);
    const added_tokens_bytes = c_file.readFile(allocator, added_tokens_path) catch return;
    defer allocator.free(added_tokens_bytes);
    try loadSentencePieceAddedTokenMap(allocator, added_tokens_bytes, sp);

    const tokenizer_json_path = std.fmt.allocPrint(allocator, "{s}/tokenizer.json", .{model_dir}) catch return;
    defer allocator.free(tokenizer_json_path);
    const tokenizer_json_bytes = c_file.readFile(allocator, tokenizer_json_path) catch return;
    defer allocator.free(tokenizer_json_bytes);
    try loadSentencePieceAddedTokenArray(allocator, tokenizer_json_bytes, sp);
}

fn loadSentencePieceAddedTokenMap(
    allocator: std.mem.Allocator,
    json_bytes: []const u8,
    sp: *sentencepiece.Processor,
) !void {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return;
    var it = parsed.value.object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .integer) continue;
        try sp.addExternalSpecialToken(entry.key_ptr.*, @intCast(entry.value_ptr.integer));
    }
}

fn loadSentencePieceAddedTokenArray(
    allocator: std.mem.Allocator,
    json_bytes: []const u8,
    sp: *sentencepiece.Processor,
) !void {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return;
    const added_tokens = parsed.value.object.get("added_tokens") orelse return;
    if (added_tokens != .array) return;
    for (added_tokens.array.items) |item| {
        if (item != .object) continue;
        const content = item.object.get("content") orelse continue;
        const id = item.object.get("id") orelse continue;
        if (content != .string or id != .integer) continue;
        try sp.addExternalSpecialToken(content.string, @intCast(id.integer));
    }
}

fn manifestLooksLikeGemma(man: manifest_mod.ModelManifest, model_dir: []const u8, allocator: std.mem.Allocator) bool {
    _ = man;
    if (std.mem.indexOf(u8, model_dir, "gemma") != null) return true;

    const cfg_path = std.fmt.allocPrint(allocator, "{s}/config.json", .{model_dir}) catch return false;
    defer allocator.free(cfg_path);
    const cfg_bytes = c_file.readFile(allocator, cfg_path) catch return false;
    defer allocator.free(cfg_bytes);

    const parsed = std.json.parseFromSlice(std.json.Value, allocator, cfg_bytes, .{}) catch return false;
    defer parsed.deinit();
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

    fn deinit(self: *LegacyWordPieceMeta, allocator: std.mem.Allocator) void {
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

fn applyLegacyTokenizerJson(meta: *LegacyWordPieceMeta, json_bytes: []const u8, allocator: std.mem.Allocator) void {
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{}) catch return;
    defer parsed.deinit();
    if (parsed.value != .object) return;
    const obj = parsed.value.object;

    if (obj.get("do_lower_case")) |v| {
        if (v == .bool) meta.do_lower_case = v.bool;
    }
    if (obj.get("unk_token")) |v| {
        if (extractLegacyTokenString(v)) |s| replaceLegacyToken(allocator, &meta.unk_token, &meta.unk_token_owned, s) catch {};
    }
    if (obj.get("pad_token")) |v| {
        if (extractLegacyTokenString(v)) |s| replaceLegacyToken(allocator, &meta.pad_token, &meta.pad_token_owned, s) catch {};
    }
    if (obj.get("cls_token")) |v| {
        if (extractLegacyTokenString(v)) |s| replaceLegacyToken(allocator, &meta.cls_token, &meta.cls_token_owned, s) catch {};
    }
    if (obj.get("sep_token")) |v| {
        if (extractLegacyTokenString(v)) |s| replaceLegacyToken(allocator, &meta.sep_token, &meta.sep_token_owned, s) catch {};
    }
    if (obj.get("mask_token")) |v| {
        if (extractLegacyTokenString(v)) |s| replaceLegacyToken(allocator, &meta.mask_token, &meta.mask_token_owned, s) catch {};
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
    const vocab_bytes = try c_file.readFile(allocator, vocab_path);
    defer allocator.free(vocab_bytes);

    var meta = LegacyWordPieceMeta{};
    defer meta.deinit(allocator);
    var tokenizer_config_bytes_opt: ?[]u8 = null;
    defer if (tokenizer_config_bytes_opt) |bytes| allocator.free(bytes);
    var special_tokens_map_bytes_opt: ?[]u8 = null;
    defer if (special_tokens_map_bytes_opt) |bytes| allocator.free(bytes);

    const tokenizer_config_path = try std.fmt.allocPrint(allocator, "{s}/tokenizer_config.json", .{model_dir});
    defer allocator.free(tokenizer_config_path);
    if (c_file.readFile(allocator, tokenizer_config_path)) |tokenizer_config_bytes| {
        tokenizer_config_bytes_opt = tokenizer_config_bytes;
        applyLegacyTokenizerJson(&meta, tokenizer_config_bytes, allocator);
    } else |_| {}

    const special_tokens_map_path = try std.fmt.allocPrint(allocator, "{s}/special_tokens_map.json", .{model_dir});
    defer allocator.free(special_tokens_map_path);
    if (c_file.readFile(allocator, special_tokens_map_path)) |special_tokens_map_bytes| {
        special_tokens_map_bytes_opt = special_tokens_map_bytes;
        applyLegacyTokenizerJson(&meta, special_tokens_map_bytes, allocator);
    } else |_| {}

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

fn loadHuggingFaceTokenizerFromGguf(allocator: std.mem.Allocator, gguf_path: []const u8) !*hf_tokenizer.HfTokenizer {
    var region = try c_file.MmapRegion.init(allocator, gguf_path);
    defer region.deinit();

    const parse_allocator = platform.allocator.processAllocator(allocator);
    var parsed = try gguf_format.parse(parse_allocator, region.data);
    defer parsed.deinit(parse_allocator);

    const view = gguf_metadata.View.init(&parsed);
    const model_name = view.getString("tokenizer.ggml.model") orelse return error.NoTokenizerFound;

    const flavor: GgufBpeTokenizerFlavor = if (std.mem.eql(u8, model_name, "gpt2"))
        .byte_level
    else if (std.mem.eql(u8, model_name, "gemma4"))
        .gemma4
    else
        return error.NoTokenizerFound;

    const tokenizer_bytes = try bpeTokenizerJsonFromGguf(allocator, &parsed, flavor);
    defer allocator.free(tokenizer_bytes);

    const tok = try hf_tokenizer.HfTokenizer.loadFromBytes(allocator, tokenizer_bytes);
    tok.applySpecialTokenIds(
        metadataTokenId(&parsed, "tokenizer.ggml.bos_token_id"),
        metadataTokenId(&parsed, "tokenizer.ggml.eos_token_id"),
        metadataTokenId(&parsed, "tokenizer.ggml.padding_token_id"),
        metadataTokenId(&parsed, "tokenizer.ggml.unknown_token_id"),
    );
    return tok;
}

const GgufBpeTokenizerFlavor = enum {
    byte_level,
    gemma4,
};

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

fn loadSentencePieceTokenizerFromGguf(allocator: std.mem.Allocator, gguf_path: []const u8) !sentencepiece.Processor {
    var region = try c_file.MmapRegion.init(allocator, gguf_path);
    defer region.deinit();

    const parse_allocator = platform.allocator.processAllocator(allocator);
    var parsed = try gguf_format.parse(parse_allocator, region.data);
    defer parsed.deinit(parse_allocator);

    const view = gguf_metadata.View.init(&parsed);
    const model_name = view.getString("tokenizer.ggml.model") orelse return error.NoTokenizerFound;
    if (!(std.mem.eql(u8, model_name, "llama") or std.mem.startsWith(u8, model_name, "gemma"))) {
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

pub fn isModelDirPotentiallyLoadableInCurrentBuild(allocator: std.mem.Allocator, model_dir: []const u8) bool {
    var man = manifest_mod.loadFromDir(allocator, model_dir) catch return false;
    defer man.deinit();
    return isManifestPotentiallyLoadableInCurrentBuild(man);
}

pub fn isManifestPotentiallyLoadableInCurrentBuild(man: manifest_mod.ModelManifest) bool {
    if (man.hasIncompleteGlinerBundle()) return false;
    if (man.hasIncompleteColqwenBundle()) return false;
    if (man.hasIncompleteClipclapGgufBundle()) return false;
    if (man.hasIncompleteFlorence2GgufBundle()) return false;
    if (man.onnx_path != null or
        man.visual_model_path != null or
        man.audio_model_path != null or
        man.text_projection_path != null or
        man.visual_projection_path != null or
        man.audio_projection_path != null)
    {
        return true;
    }
    if (nativeBackendsAvailable() and manifestHasNativeAssets(man)) {
        return true;
    }
    return false;
}

fn attachSessionIo(session: backends.Session, io: std.Io) void {
    session_factory.attachIo(session, io);
    if (backends.imported_onnx_session.sharedBackendContext(session)) |shared| shared.attachIo(io);
}

pub const LoadedModel = struct {
    manifest: manifest_mod.ModelManifest,
    hf_tok: ?*hf_tokenizer.HfTokenizer,
    sp_tok: ?*sentencepiece.Processor,
    session: backends.Session,
    session_manager: *backends.SessionManager,
    model_dir: []const u8,
    allocator: std.mem.Allocator,
    chat_tmpl: ?*ChatTemplate = null,
    shared_moe_cache: ?*runtime.moe.shared.SharedExpertCache = null,
    shared_prefetch: ?*runtime.tier.shared.SharedPrefetchState = null,
    prompt_prefix_cache: runtime.kv.prompt_cache.PromptPrefixCache,
    native_generate_coordinator: ?*runtime.scheduler.native_generate.NativeGenerateCoordinator = null,
    native_generation_graph_cache: graph_mod.cache.GraphCache,
    // ponytail: per-model native generation lock; replace with Metal-safe batching if throughput matters.
    native_generate_lock: std.Io.Mutex = .init,
    // Multimodal sessions (CLIP/CLAP/CLIPCLAP)
    embedding_session_lock: std.Io.Mutex = .init,
    reranking_session_lock: std.atomic.Mutex = .unlocked,
    imported_pipeline_lock: std.Io.Mutex = .init,
    vision_session: ?backends.Session = null,
    audio_session: ?backends.Session = null,
    text_projection: ?backends.Session = null,
    visual_projection: ?backends.Session = null,
    audio_projection: ?backends.Session = null,
    resident_projection_stats: embedding_mod.AtomicResidentProjectionStats = .{},
    cleanup_head: ?*cleanup_model_mod.CleanupHead = null,
    cleanup_head_loaded: bool = false,
    manager_last_used_ms: u64 = 0,
    manager_lru_tick: u64 = 0,
    manager_active_protections: usize = 0,
    manager_active_uses: usize = 0,

    pub fn getTokenizer(self: *LoadedModel) tokenizer_mod.Tokenizer {
        if (self.hf_tok) |ht| return ht.tokenizer();
        if (self.sp_tok) |sp| return sp.tokenizer();
        unreachable;
    }

    pub fn attachIo(self: *LoadedModel, io: std.Io) void {
        attachSessionIo(self.session, io);
        if (self.vision_session) |session| attachSessionIo(session, io);
        if (self.audio_session) |session| attachSessionIo(session, io);
        if (self.text_projection) |session| attachSessionIo(session, io);
        if (self.visual_projection) |session| attachSessionIo(session, io);
        if (self.audio_projection) |session| attachSessionIo(session, io);
    }

    pub fn usesImportedOnnxRuntime(self: *const LoadedModel) bool {
        if (backends.imported_onnx_session.sharedBackendContext(self.session) != null) return true;
        if (self.vision_session) |session| if (backends.imported_onnx_session.sharedBackendContext(session) != null) return true;
        if (self.audio_session) |session| if (backends.imported_onnx_session.sharedBackendContext(session) != null) return true;
        if (self.text_projection) |session| if (backends.imported_onnx_session.sharedBackendContext(session) != null) return true;
        if (self.visual_projection) |session| if (backends.imported_onnx_session.sharedBackendContext(session) != null) return true;
        if (self.audio_projection) |session| if (backends.imported_onnx_session.sharedBackendContext(session) != null) return true;
        return false;
    }

    pub fn lockImportedPipeline(self: *LoadedModel, io: std.Io) void {
        self.imported_pipeline_lock.lockUncancelable(io);
    }

    pub fn unlockImportedPipeline(self: *LoadedModel, io: std.Io) void {
        self.imported_pipeline_lock.unlock(io);
    }

    pub fn lockNativeGeneration(self: *LoadedModel) void {
        _ = lockIoMutexWithoutIo(&self.native_generate_lock);
    }

    pub fn unlockNativeGeneration(self: *LoadedModel) void {
        self.native_generate_lock.unlock(std.Io.Threaded.global_single_threaded.io());
    }

    pub fn lockNativeGenerationWithIo(self: *LoadedModel, io: std.Io) void {
        self.native_generate_lock.lockUncancelable(io);
    }

    pub fn unlockNativeGenerationWithIo(self: *LoadedModel, io: std.Io) void {
        self.native_generate_lock.unlock(io);
    }

    pub fn wholeModelExecutor(self: *LoadedModel, allocator: std.mem.Allocator, kv_dtype: ?runtime.kv.pool.KvDType) !?graph_mod.model_runtime.ModelExecutor {
        const gpt_config = session_factory.getGptConfig(self.session) orelse return null;
        if (build_options.enable_metal and shouldUseMetalWholeModelExecutor(self.session) and graph_mod.metal_executor.supportsSession(self.session)) {
            return try graph_mod.metal_executor.createModelExecutor(
                allocator,
                self.session,
                gpt_config,
                kv_dtype,
                self.shared_moe_cache,
            );
        }
        if (!graph_mod.live_model_executor.supportsSession(self.session)) return null;
        return try graph_mod.live_model_executor.createModelExecutor(
            allocator,
            self.session,
            gpt_config,
            kv_dtype,
            self.shared_moe_cache,
        );
    }

    fn ensureOptionalSession(self: *LoadedModel, slot: *?backends.Session, path: ?[]const u8) !void {
        if (slot.* != null) return;
        const session_path = path orelse return;
        const shared_ctx = backends.imported_onnx_session.sharedBackendContext(self.session);
        slot.* = try self.session_manager.loadModelWithImportedOnnxContext(session_path, shared_ctx);
    }

    pub fn ensureVisionSession(self: *LoadedModel) !void {
        const io = lockIoMutexWithoutIo(&self.embedding_session_lock);
        defer self.embedding_session_lock.unlock(io);
        try self.ensureOptionalSession(&self.vision_session, self.manifest.visual_model_path);
    }

    pub fn ensureVisionSessionWithIo(self: *LoadedModel, io: std.Io) !void {
        self.embedding_session_lock.lockUncancelable(io);
        defer self.embedding_session_lock.unlock(io);
        try self.ensureOptionalSession(&self.vision_session, self.manifest.visual_model_path);
    }

    pub fn ensureEmbeddingAssets(self: *LoadedModel, include_text: bool, include_image: bool, include_audio: bool) !void {
        const io = lockIoMutexWithoutIo(&self.embedding_session_lock);
        defer self.embedding_session_lock.unlock(io);
        try self.ensureEmbeddingAssetsLocked(include_text, include_image, include_audio);
    }

    pub fn ensureEmbeddingAssetsWithIo(self: *LoadedModel, io: std.Io, include_text: bool, include_image: bool, include_audio: bool) !void {
        self.embedding_session_lock.lockUncancelable(io);
        defer self.embedding_session_lock.unlock(io);

        try self.ensureEmbeddingAssetsLocked(include_text, include_image, include_audio);
    }

    fn ensureEmbeddingAssetsLocked(self: *LoadedModel, include_text: bool, include_image: bool, include_audio: bool) !void {
        if (include_text) {
            try self.ensureOptionalSession(&self.text_projection, self.manifest.text_projection_path);
        }
        if (include_image) {
            try self.ensureOptionalSession(&self.vision_session, self.manifest.visual_model_path);
            try self.ensureOptionalSession(&self.visual_projection, self.manifest.visual_projection_path);
        }
        if (include_audio) {
            try self.ensureOptionalSession(&self.audio_session, self.manifest.audio_model_path);
            try self.ensureOptionalSession(&self.audio_projection, self.manifest.audio_projection_path);
        }
    }

    pub fn embeddingPipeline(self: *LoadedModel, allocator: std.mem.Allocator) EmbeddingPipeline {
        const tok = self.getTokenizer();
        var pipeline = EmbeddingPipeline.init(allocator, self.session, tok, .{
            .max_length = self.manifest.max_position_embeddings,
            .normalize = self.manifest.normalize,
            .pooling = switch (self.manifest.pooling) {
                .mean => .mean,
                .cls => .cls,
                .max => .max,
                .last => .last,
            },
            .text_prefix = self.manifest.embedding_text_prefix,
            .trim_padding_to_batch_max = isJinaStyleEmbeddingManifest(&self.manifest),
            .resident_qwen3_embedding = isJinaStyleEmbeddingManifest(&self.manifest),
        });
        if (usesClipImagePreprocessProfile(&self.manifest)) {
            pipeline.config.image_preprocess_profile = .clip;
        }
        if (session_factory.getClipConfig(self.session)) |cfg| {
            pipeline.config.image_size = cfg.image_size;
            if (cfg.family == .clip) pipeline.config.image_preprocess_profile = .clip;
        } else if (self.vision_session) |vs| {
            if (session_factory.getClipConfig(vs)) |cfg| {
                pipeline.config.image_size = cfg.image_size;
                if (cfg.family == .clip) pipeline.config.image_preprocess_profile = .clip;
            }
        }
        pipeline.vision_session = self.vision_session;
        pipeline.audio_session = self.audio_session;
        pipeline.text_projection = self.text_projection;
        pipeline.visual_projection = self.visual_projection;
        pipeline.audio_projection = self.audio_projection;
        pipeline.resident_projection_stats = &self.resident_projection_stats;
        return pipeline;
    }

    pub fn rerankingPipeline(self: *LoadedModel, allocator: std.mem.Allocator) RerankingPipeline {
        const tok = self.getTokenizer();
        return RerankingPipeline.init(allocator, self.session, tok, .{
            .max_length = self.manifest.max_position_embeddings,
            .mode = if (self.manifest.hasCapability("late_interaction") or
                self.manifest.hasCapability("colbert") or
                self.manifest.hasCapability("colqwen") or
                self.manifest.hasCapability("multimodal_late_interaction"))
                ScoringMode.late_interaction
            else
                ScoringMode.cross_encoder,
            .single_text_encoding = if (self.manifest.prefersGenerationEncodingForLateInteraction()) .generation else .encoder,
            .add_bos_token = self.manifest.add_bos_token,
            .distributed = runtime.distributed.configFromEnv(),
        });
    }

    pub fn lockRerankingSession(self: *LoadedModel) void {
        spinLock(&self.reranking_session_lock);
    }

    pub fn unlockRerankingSession(self: *LoadedModel) void {
        self.reranking_session_lock.unlock();
    }

    pub fn classificationPipeline(self: *LoadedModel, allocator: std.mem.Allocator, config: ClassificationConfig) ClassificationPipeline {
        const tok = self.getTokenizer();
        var effective = config;
        effective.distributed = runtime.distributed.configFromEnv();
        return ClassificationPipeline.init(allocator, self.session, tok, effective);
    }

    pub fn nerPipeline(self: *LoadedModel, allocator: std.mem.Allocator) NerPipeline {
        const tok = self.getTokenizer();
        // Cast id2label from ?[][]const u8 to ?[]const []const u8
        const id2label: ?[]const []const u8 = if (self.manifest.id2label) |labels| labels else null;
        return NerPipeline.init(allocator, self.session, tok, .{
            .max_length = self.manifest.max_position_embeddings,
            .id2label = id2label,
            .distributed = runtime.distributed.configFromEnv(),
        });
    }

    pub fn isGlinerModel(self: *LoadedModel) bool {
        return self.manifest.gliner_model_type.len > 0;
    }

    pub fn supportsClassification(self: *LoadedModel) bool {
        return model_caps.modelSupportsCapability(
            @tagName(self.manifest.model_type),
            self.manifest.gliner_model_type,
            self.manifest.capabilities,
            "classification",
        );
    }

    pub fn supportsExtraction(self: *LoadedModel) bool {
        return model_caps.modelSupportsCapability(
            @tagName(self.manifest.model_type),
            self.manifest.gliner_model_type,
            self.manifest.capabilities,
            "extraction",
        );
    }

    pub fn supportsRelationExtraction(self: *LoadedModel) bool {
        return model_caps.modelSupportsCapability(
            @tagName(self.manifest.model_type),
            self.manifest.gliner_model_type,
            self.manifest.capabilities,
            "relations",
        );
    }

    pub fn glinerPipeline(self: *LoadedModel, allocator: std.mem.Allocator) GlinerPipeline {
        const tok = self.getTokenizer();
        return .{
            .allocator = allocator,
            .session = self.session,
            .tok = tok,
            .config = .{
                .max_width = self.manifest.gliner_max_width,
                .max_length = self.manifest.max_position_embeddings,
                .threshold = self.manifest.gliner_threshold,
                .flat_ner = self.manifest.gliner_flat_ner,
                .default_labels = self.manifest.gliner_default_labels,
                .relation_labels = self.manifest.gliner_relation_labels,
                .relation_threshold = self.manifest.gliner_relation_threshold,
                .model_type = self.manifest.gliner_model_type,
                .capabilities = self.manifest.capabilities,
                .token_p = self.manifest.gliner_token_p,
                .token_c = self.manifest.gliner_token_c,
                .token_e = self.manifest.gliner_token_e,
                .token_r = self.manifest.gliner_token_r,
                .token_sep_text = self.manifest.gliner_token_sep_text,
                .distributed = runtime.distributed.configFromEnv(),
            },
        };
    }

    pub fn getCleanupHead(self: *LoadedModel) !?*const cleanup_model_mod.CleanupHead {
        if (self.cleanup_head_loaded) return self.cleanup_head;

        const loaded = (try cleanup_model_mod.loadHeadIfPresent(self.allocator, self.model_dir)) orelse {
            self.cleanup_head_loaded = true;
            return null;
        };
        const head = try self.allocator.create(cleanup_model_mod.CleanupHead);
        head.* = loaded;
        self.cleanup_head = head;
        self.cleanup_head_loaded = true;
        return head;
    }

    pub fn deinit(self: *LoadedModel) void {
        self.native_generation_graph_cache.deinit();
        self.prompt_prefix_cache.deinit();
        self.session.close();
        if (self.vision_session) |vs| vs.close();
        if (self.audio_session) |as_| as_.close();
        if (self.text_projection) |tp| tp.close();
        if (self.visual_projection) |vp| vp.close();
        if (self.audio_projection) |ap| ap.close();
        if (self.hf_tok) |ht| ht.deinitSelf();
        if (self.sp_tok) |sp| {
            sp.deinit();
            self.allocator.destroy(sp);
        }
        if (self.chat_tmpl) |ct| {
            var ct_mut = @constCast(ct);
            ct_mut.deinit();
            self.allocator.destroy(ct_mut);
        }
        if (self.shared_moe_cache) |cache| {
            cache.deinit();
            self.allocator.destroy(cache);
        }
        if (self.shared_prefetch) |state| {
            state.deinit();
            self.allocator.destroy(state);
        }
        if (self.native_generate_coordinator) |coordinator| {
            coordinator.deinit();
            self.allocator.destroy(coordinator);
        }
        if (self.cleanup_head) |head| {
            head.deinit();
            self.allocator.destroy(head);
        }
        self.manifest.deinit();
        self.allocator.free(self.model_dir);
    }
};

fn isJinaStyleEmbeddingManifest(manifest: *const manifest_mod.ModelManifest) bool {
    return std.mem.eql(u8, manifest.config_model_arch, "jina_embeddings_v5") or
        (manifest.pooling == .last and std.mem.eql(u8, manifest.embedding_text_prefix, "Document: "));
}

fn usesClipImagePreprocessProfile(manifest: *const manifest_mod.ModelManifest) bool {
    return std.mem.eql(u8, manifest.config_model_arch, "clip") or
        std.mem.eql(u8, manifest.config_model_arch, "clipclap") or
        manifest.isClipclapGgufBundle();
}

pub const TransientModelLease = struct {
    const State = enum { pending, active, released };

    manager: *ModelManager,
    request_id: u64,
    state: State = .pending,

    pub fn activate(self: *TransientModelLease) !void {
        if (self.state != .pending) return error.InvalidTransientModelLease;
        self.manager.activateTransientModelLoadForRequest(self.request_id) catch |err| {
            self.state = .released;
            return err;
        };
        self.state = .active;
    }

    pub fn deinit(self: *TransientModelLease) void {
        switch (self.state) {
            .pending => self.manager.abortTransientModelLoadForRequest(self.request_id),
            .active => self.manager.releaseTransientModelForRequest(self.request_id),
            .released => return,
        }
        self.state = .released;
    }
};

pub const ModelManager = struct {
    pub const LifetimeStats = struct {
        evictions: u64,
        ttl_evictions: u64,
        capacity_rejections: u64,
    };

    const EvictionCandidate = struct {
        key: []const u8,
        model: *LoadedModel,
    };

    const EvictedModel = struct {
        key: []const u8,
        model: *LoadedModel,
    };

    const PendingModelLoad = struct {
        condition: std.Io.Condition = .init,
        users: usize = 1,
        done: bool = false,
        model: ?*LoadedModel = null,
        err: ?anyerror = null,
        preferred_backends: []const backends.BackendType = &.{},
    };

    const RequestModel = struct {
        model: *LoadedModel,
        used: bool,
    };

    const ActiveRequest = struct {
        models: std.ArrayListUnmanaged(RequestModel) = .empty,
        transient_leases: usize = 0,
    };

    allocator: std.mem.Allocator,
    session_manager: backends.SessionManager,
    loaded: std.StringHashMapUnmanaged(*LoadedModel),
    loaded_aliases: std.StringHashMapUnmanaged(*LoadedModel),
    max_loaded_models: usize = 0,
    keep_alive_ms: u64 = 0,
    active_requests: std.AutoHashMapUnmanaged(u64, ActiveRequest) = .empty,
    next_request_id: u64 = 0,
    latest_now_ms: u64 = 0,
    lru_tick: u64 = 0,
    pending_loads: usize = 0,
    pending_transient_loads: usize = 0,
    active_transient_models: usize = 0,
    state_lock: std.atomic.Mutex = .unlocked,
    pending_model_loads: std.StringHashMapUnmanaged(*PendingModelLoad) = .empty,
    pending_model_loads_lock: std.Io.Mutex = .init,
    prompt_cache_rebalance_lock: std.Io.Mutex = .init,
    lifetime_maintenance_io: ?std.Io.Threaded = null,
    lifetime_maintenance_future: ?std.Io.Future(void) = null,
    lifetime_maintenance_stop: std.atomic.Value(bool) = .init(false),
    model_evictions: std.atomic.Value(u64) = .init(0),
    ttl_model_evictions: std.atomic.Value(u64) = .init(0),
    load_capacity_rejections: std.atomic.Value(u64) = .init(0),

    pub fn init(allocator: std.mem.Allocator, session_manager: backends.SessionManager) ModelManager {
        return .{
            .allocator = allocator,
            .session_manager = session_manager,
            .loaded = std.StringHashMapUnmanaged(*LoadedModel){},
            .loaded_aliases = std.StringHashMapUnmanaged(*LoadedModel){},
        };
    }

    pub fn deinit(self: *ModelManager) void {
        self.stopLifetimeMaintenance();
        var it = self.loaded.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
            self.allocator.free(entry.key_ptr.*);
        }
        self.loaded.deinit(self.allocator);
        var alias_it = self.loaded_aliases.iterator();
        while (alias_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.loaded_aliases.deinit(self.allocator);
        std.debug.assert(self.active_requests.count() == 0);
        std.debug.assert(self.pending_transient_loads == 0);
        std.debug.assert(self.active_transient_models == 0);
        self.active_requests.deinit(self.allocator);
        std.debug.assert(self.pending_model_loads.count() == 0);
        self.pending_model_loads.deinit(self.allocator);
    }

    pub fn attachIo(self: *ModelManager, io: std.Io) void {
        // attachIo is normally startup-only. If a caller reconfigures a live
        // manager, block new cold-load flights and wait for both requests and
        // existing flights to quiesce before replacing any stored Io handle.
        while (true) {
            const coordination_io = lockIoMutexWithoutIo(&self.pending_model_loads_lock);
            self.lockState();
            if (self.active_requests.count() == 0 and self.pending_model_loads.count() == 0) {
                self.session_manager.io = io;
                var it = self.loaded.iterator();
                while (it.next()) |entry| entry.value_ptr.*.attachIo(io);
                self.unlockState();
                self.pending_model_loads_lock.unlock(coordination_io);
                break;
            }
            self.unlockState();
            self.pending_model_loads_lock.unlock(coordination_io);
            platform.time.yieldBriefly();
        }
        self.startLifetimeMaintenance();
    }

    fn startLifetimeMaintenance(self: *ModelManager) void {
        if (self.keep_alive_ms == 0 or self.lifetime_maintenance_future != null) return;
        self.lifetime_maintenance_stop.store(false, .release);
        self.lifetime_maintenance_io = std.Io.Threaded.init(self.allocator, .{});
        const maintenance_io = self.lifetime_maintenance_io.?.io();
        self.lifetime_maintenance_future = maintenance_io.concurrent(lifetimeMaintenanceTask, .{self}) catch |err| {
            std.log.err("model TTL maintenance disabled: {s}", .{@errorName(err)});
            self.lifetime_maintenance_io.?.deinit();
            self.lifetime_maintenance_io = null;
            return;
        };
    }

    fn stopLifetimeMaintenance(self: *ModelManager) void {
        self.lifetime_maintenance_stop.store(true, .release);
        if (self.lifetime_maintenance_future) |*future| {
            if (self.lifetime_maintenance_io) |*io_impl| _ = future.cancel(io_impl.io());
            self.lifetime_maintenance_future = null;
        }
        if (self.lifetime_maintenance_io) |*io_impl| {
            io_impl.deinit();
            self.lifetime_maintenance_io = null;
        }
    }

    fn lifetimeMaintenanceTask(self: *ModelManager) void {
        const maintenance_io = if (self.lifetime_maintenance_io) |*io_impl| io_impl.io() else return;
        while (!self.lifetime_maintenance_stop.load(.acquire)) {
            maintenance_io.sleep(std.Io.Duration.fromMilliseconds(1000), .awake) catch return;
            if (self.lifetime_maintenance_stop.load(.acquire)) return;
            self.lockState();
            self.observeNowLocked(platform.time.monotonicNs() / std.time.ns_per_ms);
            self.unlockState();
            self.sweepModels();
        }
    }

    /// Model-manager map access is serialized, but callers still own the raw
    /// LoadedModel pointer contract: bracket use with beginRequest/endRequest
    /// and load through the matching request-aware API.
    pub fn lockState(self: *ModelManager) void {
        spinLock(&self.state_lock);
    }

    pub fn unlockState(self: *ModelManager) void {
        self.state_lock.unlock();
    }

    pub fn configureModelLifetime(self: *ModelManager, max_loaded_models: usize, keep_alive_ms: u64) void {
        self.lockState();
        self.max_loaded_models = max_loaded_models;
        self.keep_alive_ms = keep_alive_ms;
        self.unlockState();
        self.sweepModels();
    }

    pub fn setMaxLoadedModels(self: *ModelManager, max_loaded_models: usize) void {
        self.configureModelLifetime(max_loaded_models, self.keep_alive_ms);
    }

    fn protectModelForRequestLocked(self: *ModelManager, request_id: u64, model: *LoadedModel, used: bool) !void {
        const request = self.active_requests.getPtr(request_id) orelse return error.InvalidRequestLifecycle;
        for (request.models.items) |*entry| {
            if (entry.model != model) continue;
            if (used and !entry.used) {
                if (model.manager_active_uses == std.math.maxInt(usize)) return error.RequestUseOverflow;
                model.manager_active_uses += 1;
                entry.used = true;
            }
            return;
        }
        if (model.manager_active_protections == std.math.maxInt(usize) or
            (used and model.manager_active_uses == std.math.maxInt(usize)))
        {
            return error.RequestUseOverflow;
        }
        try request.models.append(self.allocator, .{ .model = model, .used = used });
        model.manager_active_protections += 1;
        if (used) model.manager_active_uses += 1;
    }

    pub fn touchModelForRequest(self: *ModelManager, request_id: u64, model: *LoadedModel) !void {
        self.lockState();
        defer self.unlockState();
        _ = try self.touchLocked(model, request_id);
    }

    pub fn beginRequest(self: *ModelManager, now_ms: u64) !u64 {
        self.lockState();
        errdefer self.unlockState();
        self.observeNowLocked(now_ms);
        const request_id = std.math.add(u64, self.next_request_id, 1) catch return error.RequestIdExhausted;
        try self.active_requests.put(self.allocator, request_id, .{});
        self.next_request_id = request_id;
        self.unlockState();
        return request_id;
    }

    pub fn endRequest(self: *ModelManager, request_id: u64, now_ms: u64) void {
        self.lockState();
        self.observeNowLocked(now_ms);
        var request = self.active_requests.fetchRemove(request_id).?.value;
        std.debug.assert(request.transient_leases == 0);
        for (request.models.items) |entry| {
            const model = entry.model;
            std.debug.assert(model.manager_active_protections > 0);
            model.manager_active_protections -= 1;
            if (entry.used) {
                std.debug.assert(model.manager_active_uses > 0);
                model.manager_active_uses -= 1;
                if (model.manager_active_uses == 0) model.manager_last_used_ms = self.latest_now_ms;
            }
        }
        request.models.deinit(self.allocator);
        self.unlockState();
        self.sweepModels();
    }

    pub fn loadedModelCount(self: *ModelManager) usize {
        self.lockState();
        defer self.unlockState();
        return self.loaded.count();
    }

    pub fn residentModelCount(self: *ModelManager) usize {
        self.lockState();
        defer self.unlockState();
        return self.residentModelCountLocked();
    }

    /// Capture loaded-model pointers and associate each with the lifecycle
    /// request before the map lock drops.
    pub fn snapshotLoadedModelsForRequest(
        self: *ModelManager,
        allocator: std.mem.Allocator,
        request_id: u64,
    ) ![]*LoadedModel {
        self.lockState();
        defer self.unlockState();
        if (!self.active_requests.contains(request_id)) return error.InvalidRequestLifecycle;

        const models = try allocator.alloc(*LoadedModel, self.loaded.count());
        errdefer allocator.free(models);
        var index: usize = 0;
        var it = self.loaded.iterator();
        while (it.next()) |entry| {
            const model = entry.value_ptr.*;
            try self.protectModelForRequestLocked(request_id, model, false);
            models[index] = model;
            index += 1;
        }
        return models;
    }

    pub fn loadCapacityRejections(self: *const ModelManager) u64 {
        return self.load_capacity_rejections.load(.monotonic);
    }

    pub fn lifetimeStats(self: *const ModelManager) LifetimeStats {
        return .{
            .evictions = self.model_evictions.load(.monotonic),
            .ttl_evictions = self.ttl_model_evictions.load(.monotonic),
            .capacity_rejections = self.load_capacity_rejections.load(.monotonic),
        };
    }

    fn observeNowLocked(self: *ModelManager, now_ms: u64) void {
        self.latest_now_ms = @max(self.latest_now_ms, now_ms);
    }

    fn touchLocked(self: *ModelManager, model: *LoadedModel, request_id: ?u64) !*LoadedModel {
        if (request_id) |id| try self.protectModelForRequestLocked(id, model, true);
        self.lru_tick +|= 1;
        model.manager_last_used_ms = self.latest_now_ms;
        model.manager_lru_tick = self.lru_tick;
        return model;
    }

    fn lookupAndTouch(self: *ModelManager, key: []const u8, request_id: ?u64) !?*LoadedModel {
        self.lockState();
        defer self.unlockState();
        if (self.loaded.get(key)) |model| return try self.touchLocked(model, request_id);
        if (self.loaded_aliases.get(key)) |model| return try self.touchLocked(model, request_id);
        return null;
    }

    fn lookupPreferredAndTouch(
        self: *ModelManager,
        model_dir: []const u8,
        preferred_backends: []const backends.BackendType,
        request_id: ?u64,
    ) !?*LoadedModel {
        for (preferred_backends) |backend| {
            if (!backend.supportsDirectSessionLoad()) continue;
            const variant_key = try backendVariantCacheKey(self.allocator, model_dir, backend);
            defer self.allocator.free(variant_key);
            if (try self.lookupAndTouch(variant_key, request_id)) |model| return model;
        }
        return null;
    }

    fn ensureDefaultAlias(self: *ModelManager, model_dir: []const u8, model: *LoadedModel) !void {
        const alias_key = try self.allocator.dupe(u8, model_dir);
        errdefer self.allocator.free(alias_key);

        self.lockState();
        defer self.unlockState();
        if (self.loaded.get(model_dir) != null or self.loaded_aliases.get(model_dir) != null) {
            self.allocator.free(alias_key);
            return;
        }
        try self.loaded_aliases.put(self.allocator, alias_key, model);
    }

    fn ensureAliasPublicationCapacityLocked(self: *ModelManager) !void {
        const capacity = std.math.cast(u32, self.loaded_aliases.count() + 1) orelse return error.OutOfMemory;
        try self.loaded_aliases.ensureTotalCapacity(self.allocator, capacity);
    }

    fn pendingLoadKey(
        self: *ModelManager,
        model_dir: []const u8,
        _: []const backends.BackendType,
    ) ![]u8 {
        // Serialize cold loads per model. Waiters re-check their own backend
        // preferences after the active flight completes, preserving fallback
        // semantics without loading the same backend twice.
        return self.allocator.dupe(u8, model_dir);
    }

    fn lockPendingModelLoads(self: *ModelManager) std.Io {
        if (self.session_manager.io) |io| {
            self.pending_model_loads_lock.lockUncancelable(io);
            return io;
        }
        while (!self.pending_model_loads_lock.tryLock()) platform.time.yieldBriefly();
        return std.Io.Threaded.global_single_threaded.io();
    }

    fn releasePendingModelLoadLocked(
        self: *ModelManager,
        key: []const u8,
        pending: *PendingModelLoad,
    ) void {
        std.debug.assert(pending.users > 0);
        pending.users -= 1;
        if (pending.users != 0) return;
        const removed = self.pending_model_loads.fetchRemove(key).?;
        self.allocator.free(removed.key);
        if (removed.value.preferred_backends.len != 0) {
            self.allocator.free(removed.value.preferred_backends);
        }
        self.allocator.destroy(removed.value);
    }

    fn waitForPendingModelLoadLocked(
        self: *ModelManager,
        io: std.Io,
        key: []const u8,
        pending: *PendingModelLoad,
    ) !*LoadedModel {
        pending.users += 1;
        while (!pending.done) pending.condition.waitUncancelable(io, &self.pending_model_loads_lock);
        const result = pending.model;
        const load_err = pending.err;
        self.releasePendingModelLoadLocked(key, pending);
        self.pending_model_loads_lock.unlock(io);
        if (load_err) |err| return err;
        return result.?;
    }

    fn finishPendingModelLoad(
        self: *ModelManager,
        key: []const u8,
        model: ?*LoadedModel,
        load_err: ?anyerror,
    ) void {
        const io = self.lockPendingModelLoads();
        const pending = self.pending_model_loads.get(key).?;
        pending.model = model;
        pending.err = load_err;
        pending.done = true;
        pending.condition.broadcast(io);
        self.releasePendingModelLoadLocked(key, pending);
        self.pending_model_loads_lock.unlock(io);
    }

    fn residentModelCountLocked(self: *const ModelManager) usize {
        return self.loaded.count() +| self.active_transient_models;
    }

    fn pendingModelLoadCountLocked(self: *const ModelManager) usize {
        return self.pending_loads +| self.pending_transient_loads;
    }

    fn ensureLoadCanStartLocked(self: *ModelManager) !void {
        const resident = self.residentModelCountLocked();
        const pending = self.pendingModelLoadCountLocked();
        const occupied = resident +| pending;
        const can_add = !modelLimitReached(self.max_loaded_models, occupied);
        // At capacity, admit at most one replacement load when an idle victim
        // exists, but keep that healthy model alive until the replacement has
        // loaded successfully.
        const can_replace = self.max_loaded_models != 0 and
            resident == self.max_loaded_models and
            pending == 0 and
            self.findLruCandidateLocked(false) != null;
        if (!can_add and !can_replace) {
            _ = self.load_capacity_rejections.fetchAdd(1, .monotonic);
            return error.ModelCapacityReached;
        }
    }

    fn reserveLoadSlot(self: *ModelManager) !void {
        self.lockState();
        defer self.unlockState();
        try self.ensureLoadCanStartLocked();

        const loaded_capacity = std.math.cast(u32, self.loaded.count() +| self.pending_loads +| 1) orelse
            return error.OutOfMemory;
        try self.loaded.ensureTotalCapacity(self.allocator, loaded_capacity);
        self.pending_loads += 1;
    }

    fn releaseLoadSlot(self: *ModelManager) void {
        self.lockState();
        defer self.unlockState();
        std.debug.assert(self.pending_loads > 0);
        self.pending_loads -= 1;
    }

    pub fn beginTransientModelLoadForRequest(self: *ModelManager, request_id: u64) !TransientModelLease {
        self.lockState();
        defer self.unlockState();
        const request = self.active_requests.getPtr(request_id) orelse return error.InvalidRequestLifecycle;
        if (self.pending_transient_loads == std.math.maxInt(usize) or
            request.transient_leases == std.math.maxInt(usize))
        {
            return error.RequestUseOverflow;
        }
        try self.ensureLoadCanStartLocked();
        self.pending_transient_loads += 1;
        request.transient_leases += 1;
        return .{ .manager = self, .request_id = request_id };
    }

    fn activateTransientModelLoadForRequest(self: *ModelManager, request_id: u64) !void {
        self.lockState();
        const request = self.active_requests.getPtr(request_id) orelse {
            self.unlockState();
            return error.InvalidRequestLifecycle;
        };
        if (self.pending_transient_loads == 0 or request.transient_leases == 0) {
            self.unlockState();
            return error.InvalidTransientModelLease;
        }
        if (self.active_transient_models == std.math.maxInt(usize)) {
            self.pending_transient_loads -= 1;
            request.transient_leases -= 1;
            self.unlockState();
            return error.RequestUseOverflow;
        }

        var evicted: ?EvictedModel = null;
        const resident = self.residentModelCountLocked();
        if (modelLimitReached(self.max_loaded_models, resident)) {
            const candidate = if (resident == self.max_loaded_models)
                self.findLruCandidateLocked(false)
            else
                null;
            if (candidate) |selected| {
                const expired = modelTtlExpired(
                    self.keep_alive_ms,
                    self.latest_now_ms,
                    selected.model.manager_last_used_ms,
                );
                evicted = self.removeCandidateLocked(selected, expired);
            } else {
                self.pending_transient_loads -= 1;
                request.transient_leases -= 1;
                _ = self.load_capacity_rejections.fetchAdd(1, .monotonic);
                self.unlockState();
                return error.ModelCapacityReached;
            }
        }

        self.pending_transient_loads -= 1;
        self.active_transient_models += 1;
        self.unlockState();
        if (evicted) |model| self.destroyEvicted(model);
    }

    fn abortTransientModelLoadForRequest(self: *ModelManager, request_id: u64) void {
        self.lockState();
        defer self.unlockState();
        const request = self.active_requests.getPtr(request_id) orelse unreachable;
        std.debug.assert(self.pending_transient_loads > 0);
        std.debug.assert(request.transient_leases > 0);
        self.pending_transient_loads -= 1;
        request.transient_leases -= 1;
    }

    fn releaseTransientModelForRequest(self: *ModelManager, request_id: u64) void {
        self.lockState();
        defer self.unlockState();
        const request = self.active_requests.getPtr(request_id) orelse unreachable;
        std.debug.assert(self.active_transient_models > 0);
        std.debug.assert(request.transient_leases > 0);
        self.active_transient_models -= 1;
        request.transient_leases -= 1;
    }

    fn sweepModels(self: *ModelManager) void {
        while (true) {
            self.lockState();
            var expired = true;
            var candidate = self.findLruCandidateLocked(true);
            if (candidate == null and modelLimitExceeded(
                self.max_loaded_models,
                self.residentModelCountLocked(),
            )) {
                expired = false;
                candidate = self.findLruCandidateLocked(false);
            }
            const selected = candidate orelse {
                self.unlockState();
                return;
            };
            const evicted = self.removeCandidateLocked(selected, expired);
            self.unlockState();
            self.destroyEvicted(evicted);
        }
    }

    fn findLruCandidateLocked(self: *ModelManager, expired_only: bool) ?EvictionCandidate {
        var best: ?EvictionCandidate = null;
        var it = self.loaded.iterator();
        while (it.next()) |entry| {
            const model = entry.value_ptr.*;
            if (model.manager_active_protections != 0) continue;
            if (expired_only and !modelTtlExpired(
                self.keep_alive_ms,
                self.latest_now_ms,
                model.manager_last_used_ms,
            )) continue;
            const candidate: EvictionCandidate = .{ .key = entry.key_ptr.*, .model = model };
            if (best == null or lruCandidateComesFirst(
                candidate.model.manager_lru_tick,
                candidate.key,
                best.?.model.manager_lru_tick,
                best.?.key,
            )) best = candidate;
        }
        return best;
    }

    fn removeCandidateLocked(self: *ModelManager, candidate: EvictionCandidate, expired: bool) EvictedModel {
        // ponytail: aliases are few; replace this scan with a reverse index only if profiling says otherwise.
        while (true) {
            var alias_key: ?[]const u8 = null;
            var aliases = self.loaded_aliases.iterator();
            while (aliases.next()) |entry| {
                if (entry.value_ptr.* == candidate.model) {
                    alias_key = entry.key_ptr.*;
                    break;
                }
            }
            const key = alias_key orelse break;
            const removed_alias = self.loaded_aliases.fetchRemove(key).?;
            self.allocator.free(removed_alias.key);
        }

        const removed = self.loaded.fetchRemove(candidate.key).?;
        _ = self.model_evictions.fetchAdd(1, .monotonic);
        if (expired) _ = self.ttl_model_evictions.fetchAdd(1, .monotonic);
        return .{ .key = removed.key, .model = removed.value };
    }

    fn destroyEvicted(self: *ModelManager, evicted: EvictedModel) void {
        evicted.model.deinit();
        self.allocator.destroy(evicted.model);
        self.allocator.free(evicted.key);
    }

    /// Split the node target across every cache participating in the budget.
    /// configure() is the reservation, so a peer is counted even before it
    /// creates its pool or device storage.
    fn promptCacheParticipantCountLocked(self: *ModelManager, include: *LoadedModel) usize {
        var count: usize = 0;
        var it = self.loaded.iterator();
        while (it.next()) |entry| {
            const model = entry.value_ptr.*;
            if (model == include) continue;
            if (model.prompt_prefix_cache.participatesInBudget()) count += 1;
        }
        return count + 1;
    }

    /// Apply one node-wide prompt-cache target to the cache being activated and
    /// every participating cache. configure() synchronously evicts
    /// entries against their estimated logical cache bytes.
    pub fn rebalancePromptCaches(
        self: *ModelManager,
        include: *LoadedModel,
        node_config: runtime.kv.prompt_cache.Config,
    ) void {
        self.rebalancePromptCachesWithIo(
            std.Io.Threaded.global_single_threaded.io(),
            include,
            node_config,
        );
    }

    pub fn rebalancePromptCachesWithIo(
        self: *ModelManager,
        io: std.Io,
        include: *LoadedModel,
        node_config: runtime.kv.prompt_cache.Config,
    ) void {
        // Preserve one monotonic node-wide budget split while allowing waiters
        // to suspend cooperatively during cache-local eviction work.
        self.prompt_cache_rebalance_lock.lockUncancelable(io);
        defer self.prompt_cache_rebalance_lock.unlock(io);
        self.lockState();
        var per_cache = node_config;
        per_cache.max_bytes /= self.promptCacheParticipantCountLocked(include);

        var targets = std.ArrayListUnmanaged(*LoadedModel).empty;
        defer targets.deinit(self.allocator);
        targets.ensureTotalCapacity(self.allocator, self.loaded.count() + 1) catch {
            self.unlockState();
            std.log.warn("prompt cache rebalance skipped: out of memory", .{});
            return;
        };
        targets.appendAssumeCapacity(include);

        var it = self.loaded.iterator();
        while (it.next()) |entry| {
            const model = entry.value_ptr.*;
            if (model != include and model.prompt_prefix_cache.participatesInBudget()) {
                targets.appendAssumeCapacity(model);
            }
        }
        for (targets.items) |model| {
            if (model.manager_active_protections == std.math.maxInt(usize)) {
                self.unlockState();
                std.log.warn("prompt cache rebalance skipped: model pin overflow", .{});
                return;
            }
        }
        for (targets.items) |model| model.manager_active_protections += 1;
        self.unlockState();
        defer {
            self.lockState();
            for (targets.items) |model| {
                std.debug.assert(model.manager_active_protections > 0);
                model.manager_active_protections -= 1;
            }
            self.unlockState();
        }

        // configure() owns its cache-local mutex and may synchronously evict;
        // never do that work while blocking unrelated model lookups/lifecycle.
        for (targets.items) |model| model.prompt_prefix_cache.configure(per_cache);
    }

    /// Standalone/quiescent load path. Servers with lifetime eviction enabled
    /// must use loadFromDirForRequest so the returned pointer stays resident.
    pub fn loadFromDir(self: *ModelManager, model_dir: []const u8) !*LoadedModel {
        return self.loadFromDirTracked(null, model_dir);
    }

    pub fn loadFromDirForRequest(self: *ModelManager, request_id: u64, model_dir: []const u8) !*LoadedModel {
        return self.loadFromDirTracked(request_id, model_dir);
    }

    fn loadFromDirTracked(self: *ModelManager, request_id: ?u64, model_dir: []const u8) !*LoadedModel {
        if (try self.lookupAndTouch(model_dir, request_id)) |model| return model;
        return self.loadFromDirWithPreferredBackendsTracked(
            request_id,
            model_dir,
            self.session_manager.preferred_backends,
            true,
        );
    }

    pub fn loadFromDirWithPreferredBackends(
        self: *ModelManager,
        model_dir: []const u8,
        preferred_backends: []const backends.BackendType,
        cache_default_alias: bool,
    ) !*LoadedModel {
        return self.loadFromDirWithPreferredBackendsTracked(
            null,
            model_dir,
            preferred_backends,
            cache_default_alias,
        );
    }

    pub fn loadFromDirWithPreferredBackendsForRequest(
        self: *ModelManager,
        request_id: u64,
        model_dir: []const u8,
        preferred_backends: []const backends.BackendType,
        cache_default_alias: bool,
    ) !*LoadedModel {
        return self.loadFromDirWithPreferredBackendsTracked(
            request_id,
            model_dir,
            preferred_backends,
            cache_default_alias,
        );
    }

    fn loadFromDirWithPreferredBackendsTracked(
        self: *ModelManager,
        request_id: ?u64,
        model_dir: []const u8,
        preferred_backends: []const backends.BackendType,
        cache_default_alias: bool,
    ) !*LoadedModel {
        const pending_key = try self.pendingLoadKey(model_dir, preferred_backends);
        defer self.allocator.free(pending_key);

        while (true) {
            if (try self.lookupPreferredAndTouch(model_dir, preferred_backends, request_id)) |model| {
                if (cache_default_alias) try self.ensureDefaultAlias(model_dir, model);
                return model;
            }

            const coordination_io = self.lockPendingModelLoads();
            if (self.pending_model_loads.get(pending_key)) |pending| {
                const same_preferences = std.mem.eql(
                    backends.BackendType,
                    pending.preferred_backends,
                    preferred_backends,
                );
                if (pending.done and !same_preferences) {
                    self.pending_model_loads_lock.unlock(coordination_io);
                    platform.time.yieldBriefly();
                    continue;
                }
                _ = self.waitForPendingModelLoadLocked(coordination_io, pending_key, pending) catch |err| {
                    if (same_preferences) return err;
                    continue;
                };
                continue;
            }

            // Close the cache-miss/flight-claim race while holding the flight
            // map lock. Publication never takes this lock while holding state.
            const cached = self.lookupPreferredAndTouch(model_dir, preferred_backends, request_id) catch |err| {
                self.pending_model_loads_lock.unlock(coordination_io);
                return err;
            };
            if (cached) |model| {
                self.pending_model_loads_lock.unlock(coordination_io);
                if (cache_default_alias) try self.ensureDefaultAlias(model_dir, model);
                return model;
            }

            const stored_key = self.allocator.dupe(u8, pending_key) catch |err| {
                self.pending_model_loads_lock.unlock(coordination_io);
                return err;
            };
            const stored_preferences = self.allocator.dupe(backends.BackendType, preferred_backends) catch |err| {
                self.allocator.free(stored_key);
                self.pending_model_loads_lock.unlock(coordination_io);
                return err;
            };
            const pending = self.allocator.create(PendingModelLoad) catch |err| {
                self.allocator.free(stored_preferences);
                self.allocator.free(stored_key);
                self.pending_model_loads_lock.unlock(coordination_io);
                return err;
            };
            pending.* = .{ .preferred_backends = stored_preferences };
            self.pending_model_loads.put(self.allocator, stored_key, pending) catch |err| {
                self.allocator.destroy(pending);
                self.allocator.free(stored_preferences);
                self.allocator.free(stored_key);
                self.pending_model_loads_lock.unlock(coordination_io);
                return err;
            };
            self.pending_model_loads_lock.unlock(coordination_io);

            self.reserveLoadSlot() catch |err| {
                self.finishPendingModelLoad(pending_key, null, err);
                return err;
            };
            var session_manager = sessionManagerForPreferredBackends(self.allocator, preferred_backends, &self.session_manager);
            const loaded = self.loadFromDirUncached(request_id, model_dir, &session_manager, cache_default_alias) catch |err| {
                self.releaseLoadSlot();
                self.finishPendingModelLoad(pending_key, null, err);
                return err;
            };
            self.releaseLoadSlot();

            self.finishPendingModelLoad(pending_key, loaded, null);
            return loaded;
        }
    }

    fn loadFromDirUncached(
        self: *ModelManager,
        request_id: ?u64,
        model_dir: []const u8,
        sm: *backends.SessionManager,
        cache_default_alias: bool,
    ) !*LoadedModel {

        // Load manifest
        var resources_owned_by_model = false;
        var man = try manifest_mod.loadFromDir(self.allocator, model_dir);
        errdefer if (!resources_owned_by_model) man.deinit();
        if (man.hasIncompleteGlinerBundle()) return error.IncompleteGlinerBundle;
        if (man.hasIncompleteColqwenBundle()) return error.IncompleteColqwenBundle;
        if (man.hasIncompleteClipclapGgufBundle()) return error.IncompleteClipclapGgufBundle;
        if (man.hasIncompleteFlorence2GgufBundle()) return error.IncompleteFlorence2Bundle;

        // Load tokenizer
        var hf_tok: ?*hf_tokenizer.HfTokenizer = null;
        errdefer if (!resources_owned_by_model) if (hf_tok) |ht| ht.deinitSelf();
        var sp_tok: ?*sentencepiece.Processor = null;
        errdefer if (!resources_owned_by_model) if (sp_tok) |sp| {
            sp.deinit();
            self.allocator.destroy(sp);
        };

        const tokenizer_type = blk: {
            if (shouldPreferSentencePieceOverride(man, model_dir, self.allocator)) {
                break :blk manifest_mod.TokenizerType.sentencepiece;
            }
            break :blk man.tokenizer_type orelse return error.NoTokenizerFound;
        };

        switch (tokenizer_type) {
            .huggingface => {
                hf_tok = try loadHuggingFaceTokenizerFromDirOrGguf(self.allocator, model_dir, man.gguf_path);
            },
            .sentencepiece => {
                const sp = try loadSentencePieceTokenizerFromDirOrGguf(self.allocator, model_dir, man.gguf_path);
                // Publish ownership immediately so every later fallible step is
                // covered by the tokenizer cleanup errdefer above.
                sp_tok = sp;
                if (shouldEnableGemmaSentencePieceCompat(man, model_dir, self.allocator)) {
                    sp.setPreserveInlineSpecialsAfterLiteralBos(true);
                }
                try loadSentencePieceAddedTokens(model_dir, self.allocator, sp);
            },
        }

        // Load session.
        const session = try loadSessionForPreferredBackends(self.allocator, sm.preferred_backends, model_dir, man, sm);
        errdefer if (!resources_owned_by_model) session.close();

        // Load chat template if available (for generator models)
        const chat_tmpl: ?*ChatTemplate = if (man.chat_template) |ct_source| blk2: {
            const ct = self.allocator.create(ChatTemplate) catch break :blk2 null;
            ct.* = ChatTemplate.init(
                self.allocator,
                ct_source,
                man.bos_token,
                man.eos_token,
                man.unk_token,
                man.pad_token,
            ) catch |err| {
                std.log.warn("chat template init failed for {s}: {s}", .{ model_dir, @errorName(err) });
                self.allocator.destroy(ct);
                break :blk2 null;
            };
            break :blk2 ct;
        } else null;
        errdefer if (!resources_owned_by_model) if (chat_tmpl) |ct| {
            ct.deinit();
            self.allocator.destroy(ct);
        };

        // Create loaded model
        const shared_moe_cache: ?*runtime.moe.shared.SharedExpertCache = blk: {
            if (session_factory.getGptConfig(session)) |cfg| {
                if (cfg.usesMoe()) {
                    const cache = try self.allocator.create(runtime.moe.shared.SharedExpertCache);
                    cache.* = runtime.moe.shared.SharedExpertCache.init(self.allocator);
                    break :blk cache;
                }
            }
            break :blk null;
        };
        errdefer if (!resources_owned_by_model) if (shared_moe_cache) |cache| {
            cache.deinit();
            self.allocator.destroy(cache);
        };
        const shared_prefetch: ?*runtime.tier.shared.SharedPrefetchState = if (session_factory.getGptConfig(session)) |_| blk: {
            const state = try self.allocator.create(runtime.tier.shared.SharedPrefetchState);
            state.* = runtime.tier.shared.SharedPrefetchState.init(self.allocator);
            try session_factory.attachSharedPrefetchState(session, state);
            break :blk state;
        } else null;
        errdefer if (!resources_owned_by_model) if (shared_prefetch) |state| {
            state.deinit();
            self.allocator.destroy(state);
        };
        const native_generate_coordinator: ?*runtime.scheduler.native_generate.NativeGenerateCoordinator = if (session_factory.getGptConfig(session)) |_| blk: {
            const coordinator = try self.allocator.create(runtime.scheduler.native_generate.NativeGenerateCoordinator);
            coordinator.* = runtime.scheduler.native_generate.NativeGenerateCoordinator.init(self.allocator);
            break :blk coordinator;
        } else null;
        errdefer if (!resources_owned_by_model) if (native_generate_coordinator) |coordinator| self.allocator.destroy(coordinator);
        const owned_model_dir = try self.allocator.dupe(u8, model_dir);
        errdefer if (!resources_owned_by_model) self.allocator.free(owned_model_dir);
        const model = try self.allocator.create(LoadedModel);
        errdefer if (!resources_owned_by_model) self.allocator.destroy(model);
        model.* = .{
            .manifest = man,
            .hf_tok = hf_tok,
            .sp_tok = sp_tok,
            .session = session,
            .session_manager = &self.session_manager,
            .model_dir = owned_model_dir,
            .allocator = self.allocator,
            .chat_tmpl = chat_tmpl,
            .shared_moe_cache = shared_moe_cache,
            .shared_prefetch = shared_prefetch,
            .prompt_prefix_cache = runtime.kv.prompt_cache.PromptPrefixCache.init(self.allocator),
            .native_generate_coordinator = native_generate_coordinator,
            .native_generation_graph_cache = graph_mod.cache.GraphCache.init(self.allocator),
            .vision_session = null,
            .audio_session = null,
            .text_projection = null,
            .visual_projection = null,
            .audio_projection = null,
        };
        resources_owned_by_model = true;
        errdefer {
            model.deinit();
            self.allocator.destroy(model);
        }

        if (build_options.enable_metal and shouldUseMetalWholeModelExecutor(session)) {
            if (session_factory.getGptConfig(session)) |gpt_config| {
                if (graph_mod.metal_executor.supportsSession(session)) {
                    _ = graph_mod.metal_executor.prewarmSharedDecoderRuntime(self.allocator, session, gpt_config) catch |err| {
                        std.log.warn("metal decoder-runtime prewarm failed for {s}: {s}", .{ model_dir, @errorName(err) });
                    };
                }
            }
        }

        // Publish by actual loaded session backend. Cold I/O stays outside the
        // manager lock; a concurrent duplicate load is discarded here.
        const variant_key = try backendVariantCacheKey(self.allocator, model_dir, model.session.backend());
        errdefer self.allocator.free(variant_key);
        const alias_key = if (cache_default_alias) try self.allocator.dupe(u8, model_dir) else null;
        errdefer if (alias_key) |key| self.allocator.free(key);

        self.lockState();
        std.debug.assert(self.pending_loads > 0);
        if (self.loaded.get(variant_key) orelse self.loaded_aliases.get(variant_key)) |cached| {
            const result = self.touchLocked(cached, request_id) catch |err| {
                self.unlockState();
                return err;
            };
            self.unlockState();
            self.allocator.free(variant_key);
            if (alias_key) |key| self.allocator.free(key);
            model.deinit();
            self.allocator.destroy(model);
            return result;
        }

        if (alias_key != null) {
            self.ensureAliasPublicationCapacityLocked() catch |err| {
                self.unlockState();
                return err;
            };
        }

        var eviction_candidate: ?EvictionCandidate = null;
        var eviction_expired = false;
        const resident_models = self.residentModelCountLocked();
        if (modelLimitReached(self.max_loaded_models, resident_models)) {
            const candidate = (if (resident_models == self.max_loaded_models)
                self.findLruCandidateLocked(false)
            else
                null) orelse {
                _ = self.load_capacity_rejections.fetchAdd(1, .monotonic);
                self.unlockState();
                return error.ModelCapacityReached;
            };
            const expired = modelTtlExpired(
                self.keep_alive_ms,
                self.latest_now_ms,
                candidate.model.manager_last_used_ms,
            );
            eviction_candidate = candidate;
            eviction_expired = expired;
        }

        const result = self.touchLocked(model, request_id) catch |err| {
            self.unlockState();
            return err;
        };
        const evicted = if (eviction_candidate) |candidate|
            self.removeCandidateLocked(candidate, eviction_expired)
        else
            null;
        self.loaded.putAssumeCapacity(variant_key, model);
        var alias_inserted = false;
        if (alias_key) |key| {
            if (self.loaded.get(model_dir) == null and self.loaded_aliases.get(model_dir) == null) {
                self.loaded_aliases.putAssumeCapacity(key, model);
                alias_inserted = true;
            }
        }
        self.unlockState();
        if (evicted) |victim| self.destroyEvicted(victim);
        if (alias_key) |key| if (!alias_inserted) self.allocator.free(key);

        return result;
    }
};

fn modelLimitReached(max_loaded_models: usize, loaded_models: usize) bool {
    return max_loaded_models != 0 and loaded_models >= max_loaded_models;
}

fn modelLimitExceeded(max_loaded_models: usize, loaded_models: usize) bool {
    return max_loaded_models != 0 and loaded_models > max_loaded_models;
}

fn modelTtlExpired(keep_alive_ms: u64, now_ms: u64, last_used_ms: u64) bool {
    return keep_alive_ms != 0 and now_ms >= last_used_ms and now_ms - last_used_ms >= keep_alive_ms;
}

fn lruCandidateComesFirst(
    candidate_tick: u64,
    candidate_key: []const u8,
    current_tick: u64,
    current_key: []const u8,
) bool {
    if (candidate_tick != current_tick) return candidate_tick < current_tick;
    return std.mem.order(u8, candidate_key, current_key) == .lt;
}

fn backendVariantCacheKey(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    backend: backends.BackendType,
) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}\nbackend={s}", .{ model_dir, @tagName(backend) });
}

fn preferredModelPathForBackend(
    model_dir: []const u8,
    man: manifest_mod.ModelManifest,
    backend: backends.BackendType,
) ?[]const u8 {
    return switch (backend) {
        .onnx => man.onnx_path orelse model_dir,
        .native, .metal, .cuda, .wasm => if (!manifestHasNativeAssets(man) and man.onnx_path != null)
            man.onnx_path.?
        else
            model_dir,
        .pjrt => null,
    };
}

fn effectiveLoadBackends(
    scratch: *[7]backends.BackendType,
    preferred_backends: []const backends.BackendType,
    man: manifest_mod.ModelManifest,
) []const backends.BackendType {
    if (!shouldPreferNativeSession(man)) return preferred_backends;

    var idx: usize = 0;
    for (preferred_backends) |backend| {
        if (backend == .onnx) continue;
        scratch[idx] = backend;
        idx += 1;
    }
    for (preferred_backends) |backend| {
        if (backend == .onnx) {
            scratch[idx] = backend;
            idx += 1;
        }
    }
    return scratch[0..idx];
}

fn sessionManagerForPreferredBackends(
    allocator: std.mem.Allocator,
    preferred_backends: []const backends.BackendType,
    source: *const backends.SessionManager,
) backends.SessionManager {
    return .{
        .allocator = allocator,
        .preferred_backends = preferred_backends,
        .graph_runtime_strategy = source.graph_runtime_strategy,
        .pool_size = source.pool_size,
        .io = source.io,
    };
}

fn loadSessionForPreferredBackends(
    allocator: std.mem.Allocator,
    preferred_backends: []const backends.BackendType,
    model_dir: []const u8,
    man: manifest_mod.ModelManifest,
    source_session_manager: *const backends.SessionManager,
) !backends.Session {
    var effective_scratch: [7]backends.BackendType = undefined;
    const effective_backends = effectiveLoadBackends(&effective_scratch, preferred_backends, man);
    for (effective_backends) |backend| {
        if (!backend.supportsDirectSessionLoad()) continue;
        const candidate_path = preferredModelPathForBackend(model_dir, man, backend) orelse continue;
        var single_backend = [_]backends.BackendType{backend};
        var backend_session_manager = sessionManagerForPreferredBackends(allocator, single_backend[0..], source_session_manager);
        if (backend_session_manager.loadModel(candidate_path)) |session| {
            return session;
        } else |_| {}
    }

    std.log.err("loadModel({s}) failed: no backend accepted model", .{model_dir});
    std.log.err("manifest paths onnx={?s} visual={?s} audio={?s} text_projection={?s} visual_projection={?s} audio_projection={?s}", .{
        man.onnx_path,
        man.visual_model_path,
        man.audio_model_path,
        man.text_projection_path,
        man.visual_projection_path,
        man.audio_projection_path,
    });
    return error.NoModelFileFound;
}

test "shouldPreferNativeSession prefers native GLiNER weights" {
    const allocator = std.testing.allocator;
    var man = manifest_mod.ModelManifest{ .allocator = allocator };
    defer man.deinit();

    try std.testing.expect(!shouldPreferNativeSession(man));

    man.gliner_model_type = try allocator.dupe(u8, "gliner2");
    try std.testing.expect(!shouldPreferNativeSession(man));

    man.safetensors_path = try allocator.dupe(u8, "model.safetensors");
    try std.testing.expect(shouldPreferNativeSession(man));
}

test "model manager backend clones preserve explicit graph runtime" {
    var source = backends.SessionManager.init(std.testing.allocator);
    source.graph_runtime_strategy = .partitioned;
    const preferred = [_]backends.BackendType{.onnx};

    const cloned = sessionManagerForPreferredBackends(std.testing.allocator, preferred[0..], &source);
    try std.testing.expectEqual(source.graph_runtime_strategy, cloned.graph_runtime_strategy);
    try std.testing.expectEqual(source.pool_size, cloned.pool_size);
    try std.testing.expectEqualSlices(backends.BackendType, preferred[0..], cloned.preferred_backends);
}

test "model manager loaded-model limit is hard and zero is unlimited" {
    try std.testing.expect(!modelLimitReached(0, std.math.maxInt(usize)));
    try std.testing.expect(!modelLimitReached(2, 1));
    try std.testing.expect(modelLimitReached(2, 2));
    try std.testing.expect(modelLimitReached(2, 3));
    try std.testing.expect(!modelLimitExceeded(0, std.math.maxInt(usize)));
    try std.testing.expect(!modelLimitExceeded(2, 2));
    try std.testing.expect(modelLimitExceeded(2, 3));
}

test "model manager transient lease consumes and releases request capacity" {
    var manager = ModelManager.init(std.testing.allocator, backends.SessionManager.init(std.testing.allocator));
    defer manager.deinit();
    manager.configureModelLifetime(1, 0);

    const first_request = try manager.beginRequest(1);
    defer manager.endRequest(first_request, 4);
    var first = try manager.beginTransientModelLoadForRequest(first_request);
    defer first.deinit();
    try std.testing.expectEqual(@as(usize, 1), manager.pending_transient_loads);
    try first.activate();
    try std.testing.expectEqual(@as(usize, 0), manager.loadedModelCount());
    try std.testing.expectEqual(@as(usize, 1), manager.residentModelCount());

    const second_request = try manager.beginRequest(2);
    defer manager.endRequest(second_request, 5);
    try std.testing.expectError(
        error.ModelCapacityReached,
        manager.beginTransientModelLoadForRequest(second_request),
    );
    try std.testing.expectError(error.ModelCapacityReached, manager.reserveLoadSlot());

    first.deinit();
    var second = try manager.beginTransientModelLoadForRequest(second_request);
    defer second.deinit();
    try second.activate();
    try std.testing.expectEqual(@as(usize, 1), manager.residentModelCount());
}

test "model manager transient pending lease aborts cleanly" {
    var manager = ModelManager.init(std.testing.allocator, backends.SessionManager.init(std.testing.allocator));
    defer manager.deinit();
    manager.configureModelLifetime(1, 0);

    try std.testing.expectError(
        error.InvalidRequestLifecycle,
        manager.beginTransientModelLoadForRequest(99),
    );

    const first_request = try manager.beginRequest(1);
    defer manager.endRequest(first_request, 3);
    var first = try manager.beginTransientModelLoadForRequest(first_request);
    defer first.deinit();

    const second_request = try manager.beginRequest(2);
    defer manager.endRequest(second_request, 4);
    try std.testing.expectError(
        error.ModelCapacityReached,
        manager.beginTransientModelLoadForRequest(second_request),
    );

    first.deinit();
    try std.testing.expectEqual(@as(usize, 0), manager.pending_transient_loads);
    var second = try manager.beginTransientModelLoadForRequest(second_request);
    defer second.deinit();
    try second.activate();
}

test "model manager transient activation keeps a newly protected cached model" {
    var manager = ModelManager.init(std.testing.allocator, backends.SessionManager.init(std.testing.allocator));
    defer manager.deinit();
    manager.configureModelLifetime(1, 0);

    var cached: LoadedModel = undefined;
    cached.manager_last_used_ms = 0;
    cached.manager_lru_tick = 0;
    cached.manager_active_protections = 0;
    cached.manager_active_uses = 0;
    try manager.loaded.put(std.testing.allocator, "cached", &cached);
    defer _ = manager.loaded.remove("cached");

    const loading_request = try manager.beginRequest(1);
    defer manager.endRequest(loading_request, 4);
    var transient = try manager.beginTransientModelLoadForRequest(loading_request);
    defer transient.deinit();

    const cached_request = try manager.beginRequest(2);
    defer manager.endRequest(cached_request, 3);
    _ = (try manager.lookupAndTouch("cached", cached_request)).?;

    try std.testing.expectError(error.ModelCapacityReached, transient.activate());
    try std.testing.expectEqual(@as(usize, 0), manager.pending_transient_loads);
    try std.testing.expectEqual(@as(usize, 0), manager.active_transient_models);
    try std.testing.expectEqual(@as(usize, 1), manager.loadedModelCount());
}

test "model manager transient activation evicts an idle cached model" {
    const FakeSession = struct {
        closed: bool = false,

        fn session(self: *@This()) backends.Session {
            return .{ .ptr = self, .vtable = &vtable };
        }

        fn run(_: *anyopaque, _: []const backends.Tensor, _: std.mem.Allocator) anyerror![]backends.Tensor {
            return error.TestSessionDoesNotRun;
        }

        fn inputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{};
        }

        fn outputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{};
        }

        fn backend(_: *anyopaque) backends.BackendType {
            return .native;
        }

        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.closed = true;
        }

        const vtable: backends.Session.VTable = .{
            .run = run,
            .inputInfo = inputInfo,
            .outputInfo = outputInfo,
            .backend = backend,
            .close = close,
        };
    };

    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer manager.deinit();
    manager.configureModelLifetime(1, 0);

    var fake_session: FakeSession = .{};
    const cached = try allocator.create(LoadedModel);
    const model_dir = try allocator.dupe(u8, "cached");
    cached.* = .{
        .manifest = .{ .allocator = allocator },
        .hf_tok = null,
        .sp_tok = null,
        .session = fake_session.session(),
        .session_manager = &manager.session_manager,
        .model_dir = model_dir,
        .allocator = allocator,
        .prompt_prefix_cache = runtime.kv.prompt_cache.PromptPrefixCache.init(allocator),
        .native_generation_graph_cache = graph_mod.cache.GraphCache.init(allocator),
    };
    const key = try allocator.dupe(u8, "cached");
    var published = false;
    defer if (!published) {
        cached.deinit();
        allocator.destroy(cached);
        allocator.free(key);
    };
    try manager.loaded.put(allocator, key, cached);
    published = true;
    defer if (manager.loaded.fetchRemove("cached")) |removed| {
        removed.value.deinit();
        allocator.destroy(removed.value);
        allocator.free(removed.key);
    };

    const request_id = try manager.beginRequest(1);
    defer manager.endRequest(request_id, 2);
    var transient = try manager.beginTransientModelLoadForRequest(request_id);
    defer transient.deinit();

    try transient.activate();
    try std.testing.expectEqual(@as(usize, 0), manager.loadedModelCount());
    try std.testing.expectEqual(@as(usize, 1), manager.residentModelCount());
    try std.testing.expect(fake_session.closed);
    try std.testing.expectEqual(@as(u64, 1), manager.lifetimeStats().evictions);
}

test "model manager cold-load flight key is shared across backend preferences" {
    var manager = ModelManager.init(std.testing.allocator, backends.SessionManager.init(std.testing.allocator));
    defer manager.deinit();

    const automatic = try manager.pendingLoadKey("model", &.{ .metal, .native });
    defer std.testing.allocator.free(automatic);
    const explicit = try manager.pendingLoadKey("model", &.{.metal});
    defer std.testing.allocator.free(explicit);

    try std.testing.expectEqualStrings(automatic, explicit);
}

test "model manager refreshes alias capacity when publishing a cold load" {
    var manager = ModelManager.init(std.testing.allocator, backends.SessionManager.init(std.testing.allocator));
    defer manager.deinit();

    try manager.reserveLoadSlot();
    defer manager.releaseLoadSlot();

    var concurrent_model: LoadedModel = undefined;
    try manager.ensureDefaultAlias("concurrent", &concurrent_model);

    const alias_key = try std.testing.allocator.dupe(u8, "published");
    var published_model: LoadedModel = undefined;
    manager.lockState();
    defer manager.unlockState();
    try manager.ensureAliasPublicationCapacityLocked();
    manager.loaded_aliases.putAssumeCapacity(alias_key, &published_model);
}

test "model manager evicts a completed request model while an older request remains active" {
    var manager = ModelManager.init(std.testing.allocator, backends.SessionManager.init(std.testing.allocator));
    defer manager.deinit();
    manager.configureModelLifetime(2, 0);

    var first_model: LoadedModel = undefined;
    first_model.manager_last_used_ms = 0;
    first_model.manager_lru_tick = 0;
    first_model.manager_active_protections = 0;
    first_model.manager_active_uses = 0;
    var second_model: LoadedModel = undefined;
    second_model.manager_last_used_ms = 0;
    second_model.manager_lru_tick = 0;
    second_model.manager_active_protections = 0;
    second_model.manager_active_uses = 0;
    try manager.loaded.put(std.testing.allocator, "first", &first_model);
    defer _ = manager.loaded.remove("first");
    try manager.loaded.put(std.testing.allocator, "second", &second_model);
    defer _ = manager.loaded.remove("second");

    const first_request = try manager.beginRequest(1);
    _ = (try manager.lookupAndTouch("first", first_request)).?;
    _ = (try manager.lookupAndTouch("first", first_request)).?;
    try std.testing.expectEqual(@as(usize, 1), first_model.manager_active_protections);
    try std.testing.expectEqual(@as(usize, 1), first_model.manager_active_uses);

    const second_request = try manager.beginRequest(2);
    _ = (try manager.lookupAndTouch("second", second_request)).?;
    manager.endRequest(second_request, 3);

    manager.lockState();
    const candidate = manager.findLruCandidateLocked(false);
    manager.unlockState();
    try std.testing.expectEqual(&second_model, candidate.?.model);
    try manager.reserveLoadSlot();
    manager.releaseLoadSlot();
    manager.endRequest(first_request, 4);
}

test "model manager outer request protects a configured preload batch" {
    var manager = ModelManager.init(std.testing.allocator, backends.SessionManager.init(std.testing.allocator));
    defer manager.deinit();
    manager.configureModelLifetime(1, 0);

    var model: LoadedModel = undefined;
    model.manager_last_used_ms = 0;
    model.manager_lru_tick = 0;
    model.manager_active_protections = 0;
    model.manager_active_uses = 0;
    try manager.loaded.put(std.testing.allocator, "first", &model);
    defer _ = manager.loaded.remove("first");

    const outer = try manager.beginRequest(1);
    const inner = try manager.beginRequest(2);
    _ = (try manager.lookupAndTouch("first", inner)).?;
    const protected = try manager.snapshotLoadedModelsForRequest(std.testing.allocator, outer);
    std.testing.allocator.free(protected);
    manager.endRequest(inner, 3);
    try std.testing.expectError(error.ModelCapacityReached, manager.reserveLoadSlot());

    manager.endRequest(outer, 4);
    try manager.reserveLoadSlot();
    manager.releaseLoadSlot();
}

test "model manager TTL is disabled by zero and handles monotonic ages" {
    try std.testing.expect(!modelTtlExpired(0, 1_000, 1));
    try std.testing.expect(!modelTtlExpired(100, 99, 100));
    try std.testing.expect(!modelTtlExpired(100, 199, 100));
    try std.testing.expect(modelTtlExpired(100, 200, 100));
}

test "model manager LRU selection is stable across equal ticks" {
    try std.testing.expect(lruCandidateComesFirst(1, "z", 2, "a"));
    try std.testing.expect(!lruCandidateComesFirst(2, "a", 1, "z"));
    try std.testing.expect(lruCandidateComesFirst(2, "a", 2, "b"));
    try std.testing.expect(!lruCandidateComesFirst(2, "b", 2, "a"));
}

test "model manager tracks rolling request lifecycles" {
    var manager = ModelManager.init(std.testing.allocator, backends.SessionManager.init(std.testing.allocator));
    defer manager.deinit();

    const first = try manager.beginRequest(10);
    const second = try manager.beginRequest(11);
    try std.testing.expectEqual(@as(usize, 2), manager.active_requests.count());
    manager.endRequest(first, 12);
    try std.testing.expectEqual(@as(usize, 1), manager.active_requests.count());

    const third = try manager.beginRequest(9);
    try std.testing.expect(third > second);
    manager.endRequest(second, 13);
    try std.testing.expect(manager.active_requests.contains(third));
    try std.testing.expectEqual(@as(u64, 13), manager.latest_now_ms);
    manager.endRequest(third, 14);
    try std.testing.expectEqual(@as(usize, 0), manager.active_requests.count());
}

test "model manager snapshots protect pointers without refreshing model use" {
    var manager = ModelManager.init(std.testing.allocator, backends.SessionManager.init(std.testing.allocator));
    defer manager.deinit();

    var model: LoadedModel = undefined;
    model.manager_last_used_ms = 7;
    model.manager_lru_tick = 0;
    model.manager_active_protections = 0;
    model.manager_active_uses = 0;
    try manager.loaded.put(std.testing.allocator, "model", &model);
    defer _ = manager.loaded.remove("model");

    const request_id = try manager.beginRequest(10);
    const models = try manager.snapshotLoadedModelsForRequest(std.testing.allocator, request_id);
    defer std.testing.allocator.free(models);

    try std.testing.expectEqual(@as(usize, 1), models.len);
    try std.testing.expectEqual(&model, models[0]);
    try std.testing.expectEqual(@as(usize, 1), model.manager_active_protections);
    try std.testing.expectEqual(@as(usize, 0), model.manager_active_uses);
    manager.endRequest(request_id, 11);
    try std.testing.expectEqual(@as(u64, 7), model.manager_last_used_ms);
}

test "model manager coalesces an in-flight load result for waiters" {
    var manager = ModelManager.init(
        std.testing.allocator,
        backends.SessionManager.initWithIo(std.testing.allocator, std.testing.io),
    );
    defer manager.deinit();

    const key = "model\xff\x00";
    const stored_key = try std.testing.allocator.dupe(u8, key);
    const pending = try std.testing.allocator.create(ModelManager.PendingModelLoad);
    pending.* = .{};
    try manager.pending_model_loads.put(std.testing.allocator, stored_key, pending);

    const Waiter = struct {
        fn run(model_manager: *ModelManager, pending_key: []const u8) anyerror!*LoadedModel {
            const io = model_manager.lockPendingModelLoads();
            const in_flight = model_manager.pending_model_loads.get(pending_key).?;
            return model_manager.waitForPendingModelLoadLocked(io, pending_key, in_flight);
        }
    };
    var waiter = try std.testing.io.concurrent(Waiter.run, .{ &manager, key });

    while (true) {
        const io = manager.lockPendingModelLoads();
        const users = manager.pending_model_loads.get(key).?.users;
        manager.pending_model_loads_lock.unlock(io);
        if (users == 2) break;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }

    manager.finishPendingModelLoad(key, null, error.TestLoadFailed);
    try std.testing.expectError(error.TestLoadFailed, waiter.await(std.testing.io));
    try std.testing.expectEqual(@as(usize, 0), manager.pending_model_loads.count());
}

test "preferredModelPathForBackend keeps metal/native on model directory when native assets exist" {
    const allocator = std.testing.allocator;
    var man = manifest_mod.ModelManifest{ .allocator = allocator };
    defer man.deinit();

    man.onnx_path = try allocator.dupe(u8, "/tmp/model.onnx");
    man.safetensors_path = try allocator.dupe(u8, "/tmp/model.safetensors");

    try std.testing.expectEqualStrings("/tmp/model.onnx", preferredModelPathForBackend("/tmp/model", man, .onnx).?);
    try std.testing.expectEqualStrings("/tmp/model", preferredModelPathForBackend("/tmp/model", man, .metal).?);
    try std.testing.expectEqualStrings("/tmp/model", preferredModelPathForBackend("/tmp/model", man, .native).?);
    try std.testing.expectEqualStrings("/tmp/model", preferredModelPathForBackend("/tmp/model", man, .metal).?);
}

test "preferredModelPathForBackend routes direct compute backends to onnx path for onnx-only bundle" {
    const allocator = std.testing.allocator;
    var man = manifest_mod.ModelManifest{ .allocator = allocator };
    defer man.deinit();

    man.onnx_path = try allocator.dupe(u8, "/tmp/text_model.onnx");
    man.visual_model_path = try allocator.dupe(u8, "/tmp/visual_model.onnx");
    man.audio_model_path = try allocator.dupe(u8, "/tmp/audio_model.onnx");

    try std.testing.expectEqualStrings("/tmp/text_model.onnx", preferredModelPathForBackend("/tmp/model", man, .onnx).?);
    try std.testing.expectEqualStrings("/tmp/text_model.onnx", preferredModelPathForBackend("/tmp/model", man, .native).?);
    try std.testing.expectEqualStrings("/tmp/text_model.onnx", preferredModelPathForBackend("/tmp/model", man, .metal).?);
    try std.testing.expectEqualStrings("/tmp/text_model.onnx", preferredModelPathForBackend("/tmp/model", man, .metal).?);
}

test "shouldPreferNativeSession prefers split GLiNER gguf bundle" {
    const allocator = std.testing.allocator;
    var man = manifest_mod.ModelManifest{ .allocator = allocator };
    defer man.deinit();

    man.gliner_model_type = try allocator.dupe(u8, "gliner2");
    man.gguf_path = try allocator.dupe(u8, "encoder.gguf");
    man.gliner_head_gguf_path = try allocator.dupe(u8, "gliner_head.gguf");
    try std.testing.expect(shouldPreferNativeSession(man));
}

test "isManifestPotentiallyLoadableInCurrentBuild rejects incomplete GLiNER bundle" {
    const allocator = std.testing.allocator;
    var man = manifest_mod.ModelManifest{ .allocator = allocator };
    defer man.deinit();

    man.gliner_model_type = try allocator.dupe(u8, "gliner2");
    man.gguf_path = try allocator.dupe(u8, "encoder.gguf");
    try std.testing.expect(!isManifestPotentiallyLoadableInCurrentBuild(man));
}

test "shouldPreferNativeSession prefers native CLIP, Whisper, and Florence weights" {
    const allocator = std.testing.allocator;

    var clip = manifest_mod.ModelManifest{ .allocator = allocator, .native_arch_hint = .clip };
    defer clip.deinit();
    try std.testing.expect(!shouldPreferNativeSession(clip));
    clip.safetensors_path = try allocator.dupe(u8, "model.safetensors");
    try std.testing.expect(shouldPreferNativeSession(clip));

    var whisper = manifest_mod.ModelManifest{ .allocator = allocator, .native_arch_hint = .whisper };
    defer whisper.deinit();
    try std.testing.expect(!shouldPreferNativeSession(whisper));
    whisper.safetensors_path = try allocator.dupe(u8, "model.safetensors");
    try std.testing.expect(shouldPreferNativeSession(whisper));

    var florence = manifest_mod.ModelManifest{ .allocator = allocator, .native_arch_hint = .florence };
    defer florence.deinit();
    try std.testing.expect(!shouldPreferNativeSession(florence));
    florence.safetensors_path = try allocator.dupe(u8, "model.safetensors");
    try std.testing.expect(shouldPreferNativeSession(florence));
}

test "shouldPreferNativeSession prefers native classifier and recognizer weights" {
    const allocator = std.testing.allocator;

    var classifier = manifest_mod.ModelManifest{ .allocator = allocator, .model_type = .classifier };
    defer classifier.deinit();
    try std.testing.expect(!shouldPreferNativeSession(classifier));
    classifier.safetensors_path = try allocator.dupe(u8, "model.safetensors");
    try std.testing.expect(shouldPreferNativeSession(classifier));

    var recognizer = manifest_mod.ModelManifest{ .allocator = allocator, .model_type = .recognizer };
    defer recognizer.deinit();
    try std.testing.expect(!shouldPreferNativeSession(recognizer));
    recognizer.safetensors_path = try allocator.dupe(u8, "model.safetensors");
    try std.testing.expect(shouldPreferNativeSession(recognizer));
}

test "effectiveLoadBackends keeps gpu native backends ahead of cpu native before onnx" {
    const allocator = std.testing.allocator;
    const preferred = [_]backends.BackendType{ .onnx, .metal, .native };
    var scratch: [7]backends.BackendType = undefined;

    var classifier = manifest_mod.ModelManifest{ .allocator = allocator, .model_type = .classifier };
    defer classifier.deinit();
    classifier.safetensors_path = try allocator.dupe(u8, "model.safetensors");

    const effective = effectiveLoadBackends(&scratch, &preferred, classifier);
    try std.testing.expectEqualSlices(backends.BackendType, &.{ .metal, .native, .onnx }, effective);
}

test "effectiveLoadBackends preserves explicit onnx-only classifier preference" {
    const allocator = std.testing.allocator;
    const preferred = [_]backends.BackendType{.onnx};
    var scratch: [7]backends.BackendType = undefined;

    var classifier = manifest_mod.ModelManifest{ .allocator = allocator, .model_type = .classifier };
    defer classifier.deinit();
    classifier.safetensors_path = try allocator.dupe(u8, "model.safetensors");

    const effective = effectiveLoadBackends(&scratch, &preferred, classifier);
    try std.testing.expectEqualSlices(backends.BackendType, &preferred, effective);
}

test "isManifestPotentiallyLoadableInCurrentBuild accepts onnx-only models when onnx model support is enabled" {
    const allocator = std.testing.allocator;

    var onnx_only = manifest_mod.ModelManifest{ .allocator = allocator };
    defer onnx_only.deinit();
    onnx_only.onnx_path = try allocator.dupe(u8, "model.onnx");

    try std.testing.expect(isManifestPotentiallyLoadableInCurrentBuild(onnx_only));

    var native_model = manifest_mod.ModelManifest{ .allocator = allocator };
    defer native_model.deinit();
    native_model.safetensors_path = try allocator.dupe(u8, "model.safetensors");
    try std.testing.expect(isManifestPotentiallyLoadableInCurrentBuild(native_model));
}

test "isManifestPotentiallyLoadableInCurrentBuild hides incomplete colqwen bundles" {
    const allocator = std.testing.allocator;
    var colqwen = manifest_mod.ModelManifest{ .allocator = allocator };
    defer colqwen.deinit();
    colqwen.inference_bundle_family = try allocator.dupe(u8, "colqwen2_gguf_bundle/v1");
    colqwen.config_model_arch = try allocator.dupe(u8, "qwen2");
    colqwen.gguf_path = try allocator.dupe(u8, "model.gguf");
    colqwen.config_path = try allocator.dupe(u8, "config.json");
    colqwen.model_manifest_path = try allocator.dupe(u8, "model_manifest.json");
    colqwen.tokenizer_json_path = try allocator.dupe(u8, "tokenizer.json");
    colqwen.tokenizer_config_path = try allocator.dupe(u8, "tokenizer_config.json");
    colqwen.preprocessor_config_path = try allocator.dupe(u8, "preprocessor_config.json");
    try std.testing.expect(!isManifestPotentiallyLoadableInCurrentBuild(colqwen));

    colqwen.processor_config_path = try allocator.dupe(u8, "processor_config.json");
    try std.testing.expect(isManifestPotentiallyLoadableInCurrentBuild(colqwen));
}

test "ClipClap manifest selects CLIP image preprocessing profile" {
    const allocator = std.testing.allocator;
    var clipclap = manifest_mod.ModelManifest{ .allocator = allocator };
    defer clipclap.deinit();

    clipclap.config_model_arch = try allocator.dupe(u8, "clipclap");
    try std.testing.expect(usesClipImagePreprocessProfile(&clipclap));

    var siglip = manifest_mod.ModelManifest{ .allocator = allocator };
    defer siglip.deinit();
    siglip.config_model_arch = try allocator.dupe(u8, "siglip");
    try std.testing.expect(!usesClipImagePreprocessProfile(&siglip));
}

test "ModelManager loads split gliner bundle and exposes runtime pipeline" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data =
        \\{"model_type":"recognizer","hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"intermediate_size":8,"vocab_size":16,"max_position_embeddings":16,"position_buckets":16}
        ,
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "gliner_config.json",
        .data = "{\"model_type\":\"gliner2\",\"max_width\":4,\"capabilities\":[\"extraction\"]}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "model_manifest.json",
        .data = "{\"type\":\"recognizer\",\"capabilities\":[\"extraction\"]}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "antfly_inference_bundle.json",
        .data = "{\"family\":\"gliner2_split_bundle/v1\",\"wrapper\":\"gliner2\",\"encoder\":\"encoder.gguf\",\"head\":\"gliner_head.safetensors\"}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "tokenizer.json",
        .data =
        \\{
        \\  "version":"1.0",
        \\  "normalizer":{"type":"BertNormalizer","lowercase":true},
        \\  "pre_tokenizer":{"type":"BertPreTokenizer"},
        \\  "post_processor":{"type":"BertProcessing","sep":["[SEP]",3],"cls":["[CLS]",2]},
        \\  "added_tokens":[
        \\    {"id":0,"content":"[PAD]"},
        \\    {"id":1,"content":"[UNK]"},
        \\    {"id":2,"content":"[CLS]"},
        \\    {"id":3,"content":"[SEP]"}
        \\  ],
        \\  "model":{
        \\    "type":"WordPiece",
        \\    "unk_token":"[UNK]",
        \\    "continuing_subword_prefix":"##",
        \\    "max_input_chars_per_word":100,
        \\    "vocab":{"[PAD]":0,"[UNK]":1,"[CLS]":2,"[SEP]":3,"hello":4,"person":5}
        \\  }
        \\}
        ,
    });
    try writeTinyDebertaEncoderGgufForModelManagerTest(tmp.dir, allocator, "encoder.gguf");
    try writeTinyHeadSafetensorsForModelManagerTest(tmp.dir, allocator, "gliner_head.safetensors");

    const dir_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(dir_path);

    var manager = ModelManager.init(allocator, .{
        .allocator = allocator,
        .preferred_backends = &.{.native},
    });
    defer manager.deinit();

    const model = try manager.loadFromDir(dir_path);
    try std.testing.expect(model.isGlinerModel());
    try std.testing.expect(model.supportsExtraction());
    try std.testing.expectEqualStrings("gliner2_split_bundle/v1", model.manifest.inference_bundle_family);

    var pipeline = model.glinerPipeline(allocator);
    try std.testing.expectEqualStrings("gliner2", pipeline.config.model_type);
    try std.testing.expectError(error.MissingSpecialTokenIds, pipeline.recognizeBatch(&.{"hello"}, &.{"person"}));
}

test "ModelManager loads split gliner gguf-head bundle and exposes runtime pipeline" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data =
        \\{"model_type":"recognizer","hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"intermediate_size":8,"vocab_size":16,"max_position_embeddings":16,"position_buckets":16}
        ,
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "gliner_config.json",
        .data = "{\"model_type\":\"gliner2\",\"max_width\":4,\"capabilities\":[\"extraction\"]}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "model_manifest.json",
        .data = "{\"type\":\"recognizer\",\"capabilities\":[\"extraction\"]}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "antfly_inference_bundle.json",
        .data = "{\"family\":\"gliner2_split_bundle/v1\",\"wrapper\":\"gliner2\",\"encoder\":\"encoder.gguf\",\"head\":\"gliner_head.gguf\"}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "tokenizer.json",
        .data =
        \\{
        \\  "version":"1.0",
        \\  "normalizer":{"type":"BertNormalizer","lowercase":true},
        \\  "pre_tokenizer":{"type":"BertPreTokenizer"},
        \\  "post_processor":{"type":"BertProcessing","sep":["[SEP]",3],"cls":["[CLS]",2]},
        \\  "added_tokens":[
        \\    {"id":0,"content":"[PAD]"},
        \\    {"id":1,"content":"[UNK]"},
        \\    {"id":2,"content":"[CLS]"},
        \\    {"id":3,"content":"[SEP]"}
        \\  ],
        \\  "model":{
        \\    "type":"WordPiece",
        \\    "unk_token":"[UNK]",
        \\    "continuing_subword_prefix":"##",
        \\    "max_input_chars_per_word":100,
        \\    "vocab":{"[PAD]":0,"[UNK]":1,"[CLS]":2,"[SEP]":3,"hello":4,"person":5}
        \\  }
        \\}
        ,
    });
    try writeTinyDebertaEncoderGgufForModelManagerTest(tmp.dir, allocator, "encoder.gguf");
    try writeTinyHeadGgufForModelManagerTest(tmp.dir, allocator, "gliner_head.gguf");

    const dir_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(dir_path);

    var manager = ModelManager.init(allocator, .{
        .allocator = allocator,
        .preferred_backends = &.{.native},
    });
    defer manager.deinit();

    const model = try manager.loadFromDir(dir_path);
    try std.testing.expect(model.isGlinerModel());
    try std.testing.expect(model.supportsExtraction());
    try std.testing.expectEqualStrings("gliner2_split_bundle/v1", model.manifest.inference_bundle_family);

    var pipeline = model.glinerPipeline(allocator);
    try std.testing.expectEqualStrings("gliner2", pipeline.config.model_type);
    try std.testing.expectError(error.MissingSpecialTokenIds, pipeline.recognizeBatch(&.{"hello"}, &.{"person"}));
}

test "Metal encoder Session.run reuses prepared embedding table without a run budget" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const metal_runtime = @import("../backends/metal_runtime.zig");
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data =
        \\{"model_type":"deberta-v2","hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"intermediate_size":8,"vocab_size":16,"max_position_embeddings":16,"position_buckets":16}
        ,
    });
    try writeTinyDebertaEncoderGgufForModelManagerTest(tmp.dir, allocator, "model.gguf");

    const dir_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(dir_path);

    var session = try session_factory.createMetalSession(allocator, dir_path);
    defer session.close();
    session_factory.attachIo(session, std.testing.io);

    var input_ids = try backends.Tensor.initInt64(allocator, "input_ids", &.{ 1, 1 }, &.{1});
    defer input_ids.deinit();
    var attention_mask = try backends.Tensor.initInt64(allocator, "attention_mask", &.{ 1, 1 }, &.{1});
    defer attention_mask.deinit();

    const before = session_factory.getMetalEmbeddingCacheProcessStats();
    const first_outputs = try session.run(&.{ input_ids, attention_mask }, allocator);
    defer {
        for (first_outputs) |*output| output.deinit();
        allocator.free(first_outputs);
    }
    const after_first = session_factory.getMetalEmbeddingCacheProcessStats();
    const pool_after_first = session_factory.getMetalNativeProviderPoolStats(session).?;
    try std.testing.expect(after_first.misses_total > before.misses_total);
    try std.testing.expect(pool_after_first.embedding_cache_bytes > 0);

    const second_outputs = try session.run(&.{ input_ids, attention_mask }, allocator);
    defer {
        for (second_outputs) |*output| output.deinit();
        allocator.free(second_outputs);
    }
    const after_second = session_factory.getMetalEmbeddingCacheProcessStats();
    try std.testing.expect(after_second.hits_total > after_first.hits_total);
    try std.testing.expectEqual(after_first.misses_total, after_second.misses_total);
}

test "shouldPreferSentencePieceOverride still prefers sentencepiece for multimodal gemma" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "tokenizer.model", .data = "fake-spm" });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "added_tokens.json",
        .data = "{\n  \"<image_soft_token>\": 262144\n}\n",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data = "{\n  \"model_type\": \"gemma3\"\n}\n",
    });

    var man = manifest_mod.ModelManifest{ .allocator = allocator };
    defer man.deinit();

    const dir_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(dir_path);

    try std.testing.expect(shouldPreferSentencePieceOverride(man, dir_path, allocator));
}

test "shouldEnableGemmaSentencePieceCompat applies to gguf-only gemma dirs" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var man = manifest_mod.ModelManifest{ .allocator = allocator };
    defer man.deinit();
    man.tokenizer_type = .sentencepiece;

    const dir_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "gemma-4-e2b-it-gguf" });
    defer allocator.free(dir_path);

    try std.testing.expect(shouldEnableGemmaSentencePieceCompat(man, dir_path, allocator));
    try std.testing.expect(!shouldPreferSentencePieceOverride(man, dir_path, allocator));
}

test "loadSentencePieceAddedTokens overlays gemma special tokens from tokenizer json" {
    const allocator = std.testing.allocator;
    const model_dir = "models/google/gemma-3-4b-it";
    if (!c_file.fileExistsInDir(allocator, model_dir, "tokenizer.model")) return error.SkipZigTest;
    if (!c_file.fileExistsInDir(allocator, model_dir, "tokenizer.json")) return error.SkipZigTest;

    const sp_path = try std.fmt.allocPrint(allocator, "{s}/tokenizer.model", .{model_dir});
    defer allocator.free(sp_path);
    var sp = try sentencepiece.Processor.initFromPath(allocator, sp_path);
    defer sp.deinit();

    try loadSentencePieceAddedTokens(model_dir, allocator, &sp);
    try std.testing.expectEqual(@as(?i32, 105), sp.piece_map.get("<start_of_turn>"));
    try std.testing.expectEqual(@as(?i32, 262144), sp.extra_reserved_map.get("<image_soft_token>"));
    try std.testing.expectEqual("<start_of_turn>".len, sp.special_matcher.findPrefixLen("<start_of_turn>"));

    const encoded = try sp.tokenizer().encodeForGenerationConfigured(allocator, "<start_of_turn>", 16, false);
    defer {
        var encoded_mut = encoded;
        encoded_mut.deinit();
    }
    var found = false;
    for (encoded.ids[0..encoded.attention_mask.len], 0..) |id, idx| {
        if (encoded.attention_mask[idx] == 0) break;
        if (id == 105) {
            found = true;
            break;
        }
    }
    try std.testing.expect(found);
}

test "gemma sentencepiece prompt parity against hf tokenizer" {
    const allocator = std.testing.allocator;
    const model_dir = "models/google/gemma-3-4b-it";
    if (!c_file.fileExistsInDir(allocator, model_dir, "tokenizer.model")) return error.SkipZigTest;
    if (!c_file.fileExistsInDir(allocator, model_dir, "tokenizer.json")) return error.SkipZigTest;

    const prompt =
        "<bos><start_of_turn>user\n" ++
        "<start_of_image>Describe this image.<end_of_turn>\n" ++
        "<start_of_turn>model\n";

    const sp_path = try std.fmt.allocPrint(allocator, "{s}/tokenizer.model", .{model_dir});
    defer allocator.free(sp_path);
    var sp = try sentencepiece.Processor.initFromPath(allocator, sp_path);
    defer sp.deinit();
    try loadSentencePieceAddedTokens(model_dir, allocator, &sp);

    const tokenizer_path = try std.fmt.allocPrint(allocator, "{s}/tokenizer.json", .{model_dir});
    defer allocator.free(tokenizer_path);
    const tokenizer_bytes = try c_file.readFile(allocator, tokenizer_path);
    defer allocator.free(tokenizer_bytes);
    var hf = try hf_tokenizer.HfTokenizer.loadFromBytes(allocator, tokenizer_bytes);
    defer hf.deinitSelf();

    var sp_encoded = try sp.tokenizer().encodeForGenerationConfigured(allocator, prompt, 512, false);
    defer sp_encoded.deinit();
    var hf_encoded = try hf.tokenizer().encodeForGenerationConfigured(allocator, prompt, 512, false);
    defer hf_encoded.deinit();

    var sp_count: usize = 0;
    while (sp_count < sp_encoded.attention_mask.len and sp_encoded.attention_mask[sp_count] != 0) : (sp_count += 1) {}
    var hf_count: usize = 0;
    while (hf_count < hf_encoded.attention_mask.len and hf_encoded.attention_mask[hf_count] != 0) : (hf_count += 1) {}
    try std.testing.expectEqual(sp_count, hf_count);
    try std.testing.expectEqualSlices(i32, sp_encoded.ids[0..sp_count], hf_encoded.ids[0..hf_count]);
}

fn writeTinyDebertaEncoderGgufForModelManagerTest(
    dir: anytype,
    allocator: std.mem.Allocator,
    sub_path: []const u8,
) !void {
    const metadata = [_]gguf_format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "deberta" } },
        .{ .key = "general.alignment", .value = .{ .u32 = @intCast(gguf_format.default_alignment) } },
        .{ .key = "deberta.vocab_size", .value = .{ .u32 = 16 } },
        .{ .key = "deberta.embedding_length", .value = .{ .u32 = 4 } },
        .{ .key = "deberta.block_count", .value = .{ .u32 = 1 } },
        .{ .key = "deberta.attention.head_count", .value = .{ .u32 = 2 } },
        .{ .key = "deberta.feed_forward_length", .value = .{ .u32 = 8 } },
        .{ .key = "deberta.context_length", .value = .{ .u32 = 16 } },
        .{ .key = "deberta.position_buckets", .value = .{ .u32 = 16 } },
        .{ .key = "deberta.label_count", .value = .{ .u32 = 1 } },
    };
    const dims_vocab = [_]u64{ 4, 16 };
    const dims_hidden = [_]u64{4};
    const dims_rel = [_]u64{ 4, 16 };
    const dims_dense = [_]u64{ 4, 4 };
    const dims_intermediate = [_]u64{ 4, 8 };
    const dims_output = [_]u64{ 8, 4 };
    const dims_intermediate_bias = [_]u64{8};
    const tensors = [_]gguf_writer.TensorSpec{
        .{ .name = "embeddings.word_embeddings.weight", .dimensions = &dims_vocab, .tensor_type = .{ .known = .F32 } },
        .{ .name = "embeddings.LayerNorm.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "embeddings.LayerNorm.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.rel_embeddings.weight", .dimensions = &dims_rel, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.LayerNorm.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.LayerNorm.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.self.query_proj.weight", .dimensions = &dims_dense, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.self.query_proj.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.self.key_proj.weight", .dimensions = &dims_dense, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.self.key_proj.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.self.value_proj.weight", .dimensions = &dims_dense, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.self.value_proj.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.output.dense.weight", .dimensions = &dims_dense, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.output.dense.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.output.LayerNorm.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.output.LayerNorm.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.intermediate.dense.weight", .dimensions = &dims_intermediate, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.intermediate.dense.bias", .dimensions = &dims_intermediate_bias, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.output.dense.weight", .dimensions = &dims_output, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.output.dense.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.output.LayerNorm.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.output.LayerNorm.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
    };

    var layout = try gguf_writer.buildLayout(allocator, &metadata, &tensors);
    defer layout.deinit(allocator);

    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);
    try data.appendSlice(allocator, layout.header_bytes);
    const data_region_offset = std.mem.alignForward(usize, layout.header_bytes.len, @intCast(layout.alignment));
    try data.appendNTimes(allocator, 0, data_region_offset - layout.header_bytes.len);

    var written_offset: u64 = 0;
    for (tensors, layout.offsets) |tensor, offset| {
        if (offset > written_offset) {
            try data.appendNTimes(allocator, 0, @intCast(offset - written_offset));
            written_offset = offset;
        }
        const byte_len = gguf_tensor_types.byteLen(tensor.tensor_type, tensor.dimensions) orelse return error.UnsupportedTensorType;
        try data.appendNTimes(allocator, 0, byte_len);
        written_offset += @intCast(byte_len);
    }

    try dir.writeFile(std.testing.io, .{ .sub_path = sub_path, .data = data.items });
}

fn writeTinyHeadSafetensorsForModelManagerTest(
    dir: anytype,
    allocator: std.mem.Allocator,
    sub_path: []const u8,
) !void {
    const json =
        \\{"span_rep.test":{"dtype":"F32","shape":[2],"data_offsets":[0,8]}}
    ;
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);
    try appendLeModelManagerTest(u64, allocator, &data, json.len);
    try data.appendSlice(allocator, json);
    try data.appendSlice(allocator, std.mem.asBytes(&[_]f32{ 0.0, 0.0 }));
    try dir.writeFile(std.testing.io, .{ .sub_path = sub_path, .data = data.items });
}

fn writeTinyHeadGgufForModelManagerTest(
    dir: anytype,
    allocator: std.mem.Allocator,
    sub_path: []const u8,
) !void {
    const metadata = [_]gguf_format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "antfly-gliner-head" } },
        .{ .key = "general.alignment", .value = .{ .u32 = @intCast(gguf_format.default_alignment) } },
    };
    const dims = [_]u64{2};
    const tensors = [_]gguf_writer.TensorSpec{
        .{ .name = "span_rep.test", .dimensions = &dims, .tensor_type = .{ .known = .F32 } },
    };

    var layout = try gguf_writer.buildLayout(allocator, &metadata, &tensors);
    defer layout.deinit(allocator);

    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);
    try data.appendSlice(allocator, layout.header_bytes);
    const data_region_offset = std.mem.alignForward(usize, layout.header_bytes.len, @intCast(layout.alignment));
    try data.appendNTimes(allocator, 0, data_region_offset - layout.header_bytes.len);
    try data.appendSlice(allocator, std.mem.asBytes(&[_]f32{ 0.0, 0.0 }));

    try dir.writeFile(std.testing.io, .{ .sub_path = sub_path, .data = data.items });
}

fn appendLeModelManagerTest(
    comptime T: type,
    allocator: std.mem.Allocator,
    data: *std.ArrayListUnmanaged(u8),
    value: T,
) !void {
    var buf: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &buf, value, .little);
    try data.appendSlice(allocator, &buf);
}

test "load huggingface tokenizer from gguf gpt2 metadata" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const gguf_bytes = try buildTestGgufWithGpt2Tokenizer(allocator);
    defer allocator.free(gguf_bytes);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "ggml-model-i2_s.gguf", .data = gguf_bytes });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);
    const gguf_path = try std.fs.path.join(allocator, &.{ model_dir, "ggml-model-i2_s.gguf" });
    defer allocator.free(gguf_path);

    var tok = try loadHuggingFaceTokenizerFromDirOrGguf(allocator, model_dir, gguf_path);
    defer tok.deinitSelf();

    var encoded = try tok.tokenizer().encodeForGenerationConfigured(allocator, "hello", 8, true);
    defer encoded.deinit();

    try std.testing.expectEqual(@as(i32, 0), encoded.ids[0]);
    try std.testing.expectEqual(@as(i32, 1), encoded.ids[1]);
    try std.testing.expectEqual(@as(i32, 1), encoded.attention_mask[0]);
    try std.testing.expectEqual(@as(i32, 1), encoded.attention_mask[1]);
}

test "load huggingface tokenizer from gguf gemma4 bpe metadata" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const gguf_bytes = try buildTestGgufWithGemma4Tokenizer(allocator);
    defer allocator.free(gguf_bytes);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "gemma4-q4_0.gguf", .data = gguf_bytes });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);
    const gguf_path = try std.fs.path.join(allocator, &.{ model_dir, "gemma4-q4_0.gguf" });
    defer allocator.free(gguf_path);

    var tok = try loadHuggingFaceTokenizerFromDirOrGguf(allocator, model_dir, gguf_path);
    defer tok.deinitSelf();

    var encoded = try tok.tokenizer().encodeForGenerationConfigured(allocator, "hello world", 8, true);
    defer encoded.deinit();

    try std.testing.expectEqual(@as(i32, 2), encoded.ids[0]);
    try std.testing.expectEqual(@as(i32, 4), encoded.ids[1]);
    try std.testing.expectEqual(@as(i32, 5), encoded.ids[2]);
    try std.testing.expectEqual(@as(i32, 1), encoded.attention_mask[0]);
    try std.testing.expectEqual(@as(i32, 1), encoded.attention_mask[1]);
    try std.testing.expectEqual(@as(i32, 1), encoded.attention_mask[2]);

    const special_ids = try tok.tokenizer().encode(allocator, "<|turn>hello");
    defer allocator.free(special_ids);
    try std.testing.expectEqualSlices(i32, &.{ 6, 4 }, special_ids);

    const decoded = try tok.tokenizer().decode(allocator, &.{ 4, 5 });
    defer allocator.free(decoded);
    try std.testing.expectEqualStrings("hello world", decoded);
}

fn buildTestGgufWithGpt2Tokenizer(allocator: std.mem.Allocator) ![]u8 {
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);

    try data.appendSlice(allocator, gguf_format.magic);
    try appendTestLe(u32, allocator, &data, 3);
    try appendTestLe(u64, allocator, &data, 0);
    try appendTestLe(u64, allocator, &data, 7);

    try appendTestMetadataString(allocator, &data, "general.architecture", "bitnet-b1.58");
    try appendTestMetadataString(allocator, &data, "tokenizer.ggml.model", "gpt2");
    try appendTestMetadataStringArray(allocator, &data, "tokenizer.ggml.tokens", &.{
        "<|begin_of_text|>",
        "hello",
        "<|end_of_text|>",
    });
    try appendTestMetadataStringArray(allocator, &data, "tokenizer.ggml.merges", &.{});
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.bos_token_id", 0);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.eos_token_id", 2);
    try appendTestMetadataBool(allocator, &data, "tokenizer.ggml.add_bos_token", true);

    return data.toOwnedSlice(allocator);
}

fn buildTestGgufWithGemma4Tokenizer(allocator: std.mem.Allocator) ![]u8 {
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);

    try data.appendSlice(allocator, gguf_format.magic);
    try appendTestLe(u32, allocator, &data, 3);
    try appendTestLe(u64, allocator, &data, 0);
    try appendTestLe(u64, allocator, &data, 10);

    try appendTestMetadataString(allocator, &data, "general.architecture", "gemma4");
    try appendTestMetadataString(allocator, &data, "tokenizer.ggml.model", "gemma4");
    try appendTestMetadataStringArray(allocator, &data, "tokenizer.ggml.tokens", &.{
        "<pad>",
        "<eos>",
        "<bos>",
        "<unk>",
        "hello",
        "▁world",
        "<|turn>",
    });
    try appendTestMetadataStringArray(allocator, &data, "tokenizer.ggml.merges", &.{});
    try appendTestMetadataI32Array(allocator, &data, "tokenizer.ggml.token_type", &.{ 3, 3, 3, 2, 1, 1, 3 });
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.bos_token_id", 2);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.eos_token_id", 1);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.padding_token_id", 0);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.unknown_token_id", 3);
    try appendTestMetadataBool(allocator, &data, "tokenizer.ggml.add_bos_token", true);

    return data.toOwnedSlice(allocator);
}

fn appendTestLe(comptime T: type, allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), value: T) !void {
    const bytes = std.mem.asBytes(&std.mem.nativeToLittle(T, value));
    try data.appendSlice(allocator, bytes);
}

fn appendTestString(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    try appendTestLe(u64, allocator, data, value.len);
    try data.appendSlice(allocator, value);
}

fn appendTestMetadataString(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, value: []const u8) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @intFromEnum(gguf_format.MetadataValueType.string));
    try appendTestString(allocator, data, value);
}

fn appendTestMetadataU32(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, value: u32) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @intFromEnum(gguf_format.MetadataValueType.u32));
    try appendTestLe(u32, allocator, data, value);
}

fn appendTestMetadataBool(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, value: bool) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @intFromEnum(gguf_format.MetadataValueType.bool_));
    try appendTestLe(u8, allocator, data, @intFromBool(value));
}

fn appendTestMetadataStringArray(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, values: []const []const u8) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @intFromEnum(gguf_format.MetadataValueType.array));
    try appendTestLe(u32, allocator, data, @intFromEnum(gguf_format.MetadataValueType.string));
    try appendTestLe(u64, allocator, data, values.len);
    for (values) |value| try appendTestString(allocator, data, value);
}

fn appendTestMetadataI32Array(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, values: []const i32) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @intFromEnum(gguf_format.MetadataValueType.array));
    try appendTestLe(u32, allocator, data, @intFromEnum(gguf_format.MetadataValueType.i32));
    try appendTestLe(u64, allocator, data, values.len);
    for (values) |value| try appendTestLe(i32, allocator, data, value);
}
