// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

const std = @import("std");
const c_file = @import("../util/c_file.zig");
const tokenizer_mod = @import("inference_tokenizer");

/// A nullable token is intentional: Hugging Face uses `[position, null]` for
/// decoder slots the model must generate, most notably Whisper's automatic
/// language-detection slot.
pub const ForcedDecoderId = struct {
    position: usize,
    token_id: ?i32,
};

/// Resolve Whisper's decoder prompt without changing the API semantics.
/// Explicit request languages are forced. With no hint, nullable artifact
/// slots remain dynamic and legacy concrete language slots are made dynamic so
/// `/transcribe` continues to provide automatic language detection.
pub fn resolveForcedDecoderIds(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    tokenizer: tokenizer_mod.Tokenizer,
    language: ?[]const u8,
) ![]const ForcedDecoderId {
    if (language) |value| {
        return buildExplicitLanguagePrompt(allocator, tokenizer, value) catch |err| switch (err) {
            // English-only Whisper tokenizers intentionally have no language
            // tokens. An explicit `en` hint is still valid; retain their
            // artifact task prompt instead of rejecting the request.
            error.UnsupportedWhisperLanguage => if (std.mem.eql(u8, value, "en"))
                (try loadForcedDecoderIds(allocator, model_dir)) orelse return err
            else
                return err,
            else => return err,
        };
    }

    if (try loadForcedDecoderIds(allocator, model_dir)) |ids| {
        makeLanguageSlotDynamic(allocator, tokenizer, ids);
        return ids;
    }
    return buildAutomaticLanguagePrompt(allocator, tokenizer);
}

pub fn loadForcedDecoderIds(allocator: std.mem.Allocator, model_dir: []const u8) !?[]ForcedDecoderId {
    for ([_][]const u8{ "generation_config.json", "config.json" }) |filename| {
        const path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ model_dir, filename });
        defer allocator.free(path);

        const data = c_file.readFile(allocator, path) catch continue;
        defer allocator.free(data);
        if (try parseForcedDecoderIds(allocator, data)) |ids| return ids;
    }
    return null;
}

fn parseForcedDecoderIds(allocator: std.mem.Allocator, data: []const u8) !?[]ForcedDecoderId {
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, data, .{}) catch
        return error.InvalidWhisperDecoderConfig;
    defer parsed.deinit();

    if (parsed.value != .object) return error.InvalidWhisperDecoderConfig;
    const value = parsed.value.object.get("forced_decoder_ids") orelse return null;
    if (value == .null) return null;
    if (value != .array or value.array.items.len == 0) return error.InvalidWhisperDecoderConfig;

    var result = std.ArrayListUnmanaged(ForcedDecoderId).empty;
    defer result.deinit(allocator);
    var previous_position: usize = 0;
    for (value.array.items) |pair| {
        if (pair != .array or pair.array.items.len != 2) return error.InvalidWhisperDecoderConfig;
        const position = jsonPosition(pair.array.items[0]) orelse return error.InvalidWhisperDecoderConfig;
        if (position <= previous_position) return error.InvalidWhisperDecoderConfig;
        previous_position = position;
        const token_id: ?i32 = switch (pair.array.items[1]) {
            .null => null,
            else => jsonI32(pair.array.items[1]) orelse return error.InvalidWhisperDecoderConfig,
        };
        try result.append(allocator, .{ .position = position, .token_id = token_id });
    }
    return try result.toOwnedSlice(allocator);
}

fn jsonPosition(value: std.json.Value) ?usize {
    const raw = switch (value) {
        .integer => |integer| integer,
        else => return null,
    };
    if (raw <= 0) return null;
    return std.math.cast(usize, raw);
}

fn jsonI32(value: std.json.Value) ?i32 {
    const raw = switch (value) {
        .integer => |integer| integer,
        else => return null,
    };
    return std.math.cast(i32, raw);
}

fn makeLanguageSlotDynamic(
    allocator: std.mem.Allocator,
    tokenizer: tokenizer_mod.Tokenizer,
    ids: []ForcedDecoderId,
) void {
    for (ids) |*entry| {
        if (entry.position != 1) return;
        const token_id = entry.token_id orelse return;
        if (languageCodeForToken(allocator, tokenizer, token_id) != null) entry.token_id = null;
        return;
    }
}

fn buildExplicitLanguagePrompt(
    allocator: std.mem.Allocator,
    tokenizer: tokenizer_mod.Tokenizer,
    language: []const u8,
) ![]const ForcedDecoderId {
    const language_id = encodeLanguageToken(allocator, tokenizer, language) orelse
        return error.UnsupportedWhisperLanguage;
    const transcribe_id = try requireSpecialToken(allocator, tokenizer, "<|transcribe|>");
    const no_timestamps_id = try requireSpecialToken(allocator, tokenizer, "<|notimestamps|>");
    const result = try allocator.alloc(ForcedDecoderId, 3);
    result[0] = .{ .position = 1, .token_id = language_id };
    result[1] = .{ .position = 2, .token_id = transcribe_id };
    result[2] = .{ .position = 3, .token_id = no_timestamps_id };
    return result;
}

fn buildAutomaticLanguagePrompt(
    allocator: std.mem.Allocator,
    tokenizer: tokenizer_mod.Tokenizer,
) ![]const ForcedDecoderId {
    const transcribe_id = try requireSpecialToken(allocator, tokenizer, "<|transcribe|>");
    const no_timestamps_id = try requireSpecialToken(allocator, tokenizer, "<|notimestamps|>");
    const multilingual = encodeLanguageToken(allocator, tokenizer, "en") != null;
    const result = try allocator.alloc(ForcedDecoderId, if (multilingual) 3 else 2);
    if (multilingual) {
        result[0] = .{ .position = 1, .token_id = null };
        result[1] = .{ .position = 2, .token_id = transcribe_id };
        result[2] = .{ .position = 3, .token_id = no_timestamps_id };
    } else {
        result[0] = .{ .position = 1, .token_id = transcribe_id };
        result[1] = .{ .position = 2, .token_id = no_timestamps_id };
    }
    return result;
}

fn requireSpecialToken(
    allocator: std.mem.Allocator,
    tokenizer: tokenizer_mod.Tokenizer,
    text: []const u8,
) !i32 {
    return encodeSingleSpecialToken(allocator, tokenizer, text) orelse error.MissingWhisperSpecialToken;
}

fn encodeLanguageToken(
    allocator: std.mem.Allocator,
    tokenizer: tokenizer_mod.Tokenizer,
    language: []const u8,
) ?i32 {
    const language_token = std.fmt.allocPrint(allocator, "<|{s}|>", .{language}) catch return null;
    defer allocator.free(language_token);
    return encodeSingleSpecialToken(allocator, tokenizer, language_token);
}

fn encodeSingleSpecialToken(
    allocator: std.mem.Allocator,
    tokenizer: tokenizer_mod.Tokenizer,
    text: []const u8,
) ?i32 {
    const ids = tokenizer.encode(allocator, text) catch return null;
    defer allocator.free(ids);
    if (ids.len != 1 or ids[0] == tokenizer.specialTokens().unk_id) return null;
    return ids[0];
}

/// Map a generated Whisper language token back to the ISO code returned by the
/// public API. The list is the complete multilingual Whisper language set.
pub fn languageCodeForToken(
    allocator: std.mem.Allocator,
    tokenizer: tokenizer_mod.Tokenizer,
    token_id: i32,
) ?[]const u8 {
    for (whisper_language_codes) |language| {
        if (encodeLanguageToken(allocator, tokenizer, language)) |candidate| {
            if (candidate == token_id) return language;
        }
    }
    return null;
}

pub const DetectedLanguage = struct {
    token_id: i32,
    code: []const u8,
};

/// Whisper language detection is a constrained classification step, not an
/// unrestricted decoder argmax. Compare only language-token logits so ordinary
/// text and task tokens cannot occupy the nullable language slot.
pub fn detectLanguageToken(
    allocator: std.mem.Allocator,
    tokenizer: tokenizer_mod.Tokenizer,
    logits: []const f32,
) ?DetectedLanguage {
    var best: ?DetectedLanguage = null;
    var best_logit: f32 = -std.math.inf(f32);
    for (whisper_language_codes) |language| {
        const token_id = encodeLanguageToken(allocator, tokenizer, language) orelse continue;
        if (token_id < 0 or @as(usize, @intCast(token_id)) >= logits.len) continue;
        const logit = logits[@intCast(token_id)];
        if (std.math.isNan(logit)) continue;
        if (best == null or logit > best_logit) {
            best = .{ .token_id = token_id, .code = language };
            best_logit = logit;
        }
    }
    return best;
}

const whisper_language_codes = [_][]const u8{
    "en", "zh", "de", "es", "ru", "ko", "fr", "ja", "pt", "tr", "pl", "ca", "nl", "ar",  "sv", "it", "id", "hi", "fi", "vi",
    "he", "uk", "el", "ms", "cs", "ro", "da", "hu", "ta", "no", "th", "ur", "hr", "bg",  "lt", "la", "mi", "ml", "cy", "sk",
    "te", "fa", "lv", "bn", "sr", "az", "sl", "kn", "et", "mk", "br", "eu", "is", "hy",  "ne", "mn", "bs", "kk", "sq", "sw",
    "gl", "mr", "pa", "si", "km", "sn", "yo", "so", "af", "oc", "ka", "be", "tg", "sd",  "gu", "am", "yi", "lo", "uz", "fo",
    "ht", "ps", "tk", "nn", "mt", "sa", "lb", "my", "bo", "tl", "mg", "as", "tt", "haw", "ln", "ha", "ba", "jw", "su", "yue",
};

test "forced decoder ids preserve a dynamic language slot" {
    const allocator = std.testing.allocator;
    const ids = (try parseForcedDecoderIds(
        allocator,
        "{\"forced_decoder_ids\":[[1,null],[2,50359]]}",
    )).?;
    defer allocator.free(ids);
    try std.testing.expectEqual(@as(usize, 1), ids[0].position);
    try std.testing.expectEqual(@as(?i32, null), ids[0].token_id);
    try std.testing.expectEqual(@as(?i32, 50359), ids[1].token_id);
}

test "forced decoder ids parse a concrete artifact prefix" {
    const allocator = std.testing.allocator;
    const ids = (try parseForcedDecoderIds(
        allocator,
        "{\"forced_decoder_ids\":[[1,50265],[2,50359],[3,50363]]}",
    )).?;
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(ForcedDecoderId, &.{
        .{ .position = 1, .token_id = 50265 },
        .{ .position = 2, .token_id = 50359 },
        .{ .position = 3, .token_id = 50363 },
    }, ids);
}

test "forced decoder ids reject unsorted or invalid positions" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(error.InvalidWhisperDecoderConfig, parseForcedDecoderIds(
        allocator,
        "{\"forced_decoder_ids\":[[2,50359],[1,null]]}",
    ));
    try std.testing.expectError(error.InvalidWhisperDecoderConfig, parseForcedDecoderIds(
        allocator,
        "{\"forced_decoder_ids\":[[0,null]]}",
    ));
}

test "language detection ignores higher non-language logits" {
    const allocator = std.testing.allocator;
    const hf_tokenizer = @import("inference_hf_tokenizer");
    var tokenizer = try hf_tokenizer.HfTokenizer.loadFromBytes(allocator,
        \\{"version":"1.0","added_tokens":[{"id":0,"content":"<unk>"},{"id":10,"content":"<|en|>"},{"id":11,"content":"<|es|>"},{"id":12,"content":"<|transcribe|>"},{"id":13,"content":"<|notimestamps|>"}],"model":{"type":"BPE","vocab":{"<unk>":0,"<|en|>":10,"<|es|>":11,"<|transcribe|>":12,"<|notimestamps|>":13},"merges":[]}}
    );
    defer tokenizer.deinitSelf();

    var logits = [_]f32{0} ** 16;
    logits[10] = 1;
    logits[11] = 4;
    logits[15] = 100;
    const detected = detectLanguageToken(allocator, tokenizer.tokenizer(), &logits).?;
    try std.testing.expectEqual(@as(i32, 11), detected.token_id);
    try std.testing.expectEqualStrings("es", detected.code);
}
