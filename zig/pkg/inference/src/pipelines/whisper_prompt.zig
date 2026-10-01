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

pub const LanguageToken = struct {
    token_id: i32,
    code: []const u8,
};

/// Immutable prompt metadata prepared once for a loaded Whisper model. Request
/// handling only performs a small language-token lookup and writes at most
/// three prompt entries into caller-owned stack storage.
/// Decoder-side generation settings from `generation_config.json` that the
/// transcription loop applies to logits: token suppression and timestamp
/// rules. Defaults match the multilingual Whisper checkpoints.
pub const DecodeSettings = struct {
    /// `<|startofprev|>`: prefix token for conditioning text. Null when the
    /// tokenizer lacks it (then no conditioning is possible).
    start_of_prev_id: ?i32 = null,
    /// `<|endoftext|>`.
    eot_id: i32 = 50257,
    /// `<|0.00|>`; timestamps are `timestamp_begin_id + offset_ms / 20`.
    timestamp_begin_id: i32 = 50364,
    max_initial_timestamp_index: usize = 50,
    suppress_tokens: []const i32 = &.{},
    begin_suppress_tokens: []const i32 = &.{},
};

pub const PromptCache = struct {
    allocator: std.mem.Allocator,
    automatic_ids: []ForcedDecoderId,
    language_tokens: []LanguageToken,
    transcribe_id: i32,
    no_timestamps_id: i32,
    decode: DecodeSettings = .{},

    pub fn init(
        allocator: std.mem.Allocator,
        model_dir: []const u8,
        tokenizer: tokenizer_mod.Tokenizer,
    ) !PromptCache {
        const generation_config_path = try std.fs.path.join(
            allocator,
            &.{ model_dir, "generation_config.json" },
        );
        defer allocator.free(generation_config_path);
        const config_path = try std.fs.path.join(allocator, &.{ model_dir, "config.json" });
        defer allocator.free(config_path);
        return initFromPaths(
            allocator,
            generation_config_path,
            config_path,
            tokenizer,
        );
    }

    /// Build prompt metadata from already-resolved artifact paths. Managed
    /// serving uses this entry point so completion receipts remain authoritative
    /// and a stale, unreceipted sidecar can never influence decoding.
    pub fn initFromPaths(
        allocator: std.mem.Allocator,
        generation_config_path: ?[]const u8,
        config_path: ?[]const u8,
        tokenizer: tokenizer_mod.Tokenizer,
    ) !PromptCache {
        var languages = std.ArrayListUnmanaged(LanguageToken).empty;
        defer languages.deinit(allocator);
        for (whisper_language_codes) |code| {
            const token_id = encodeLanguageToken(allocator, tokenizer, code) orelse continue;
            try languages.append(allocator, .{ .token_id = token_id, .code = code });
        }
        const language_tokens = try languages.toOwnedSlice(allocator);
        errdefer allocator.free(language_tokens);

        const transcribe_id = try requireSpecialToken(allocator, tokenizer, "<|transcribe|>");
        const no_timestamps_id = try requireSpecialToken(allocator, tokenizer, "<|notimestamps|>");
        const automatic_ids = if (try loadForcedDecoderIdsFromPaths(
            allocator,
            generation_config_path,
            config_path,
        )) |ids| blk: {
            makeLanguageSlotDynamic(ids, language_tokens);
            break :blk ids;
        } else try buildAutomaticLanguagePromptFromTokens(
            allocator,
            language_tokens,
            transcribe_id,
            no_timestamps_id,
        );
        errdefer allocator.free(automatic_ids);

        // Bundles without the HF sidecars (GGUF exports, whisper.cpp-style
        // vocabularies) still follow Whisper's fixed token layout:
        // <|startofprev|> and <|nospeech|> sit two and one slots before
        // <|notimestamps|>, and <|endoftext|> one before <|startoftranscript|>.
        var decode = DecodeSettings{
            .start_of_prev_id = encodeSingleSpecialToken(allocator, tokenizer, "<|startofprev|>") orelse no_timestamps_id - 2,
            .eot_id = encodeSingleSpecialToken(allocator, tokenizer, "<|endoftext|>") orelse 50257,
            .timestamp_begin_id = encodeSingleSpecialToken(allocator, tokenizer, "<|0.00|>") orelse no_timestamps_id + 1,
        };
        try loadDecodeSettingsFromPaths(allocator, generation_config_path, &decode);
        errdefer allocator.free(decode.suppress_tokens);
        errdefer allocator.free(decode.begin_suppress_tokens);
        if (decode.suppress_tokens.len == 0) {
            allocator.free(decode.suppress_tokens);
            decode.suppress_tokens = try nonSpeechTokens(allocator, tokenizer, decode.eot_id);
        }
        if (decode.begin_suppress_tokens.len == 0) {
            allocator.free(decode.begin_suppress_tokens);
            decode.begin_suppress_tokens = try beginSuppressTokens(allocator, tokenizer, decode.eot_id);
        }

        return .{
            .allocator = allocator,
            .automatic_ids = automatic_ids,
            .language_tokens = language_tokens,
            .transcribe_id = transcribe_id,
            .no_timestamps_id = no_timestamps_id,
            .decode = decode,
        };
    }

    pub fn deinit(self: *PromptCache) void {
        self.allocator.free(self.automatic_ids);
        self.allocator.free(self.language_tokens);
        self.allocator.free(self.decode.suppress_tokens);
        self.allocator.free(self.decode.begin_suppress_tokens);
        self.* = undefined;
    }

    /// Like `resolve`, but with `timestamps` the `<|notimestamps|>` slot is
    /// dropped so the decoder emits timestamp tokens.
    pub fn resolveWithTimestamps(
        self: *const PromptCache,
        scratch: *[3]ForcedDecoderId,
        language: ?[]const u8,
        timestamps: bool,
    ) ![]const ForcedDecoderId {
        const ids = try self.resolve(scratch, language);
        if (!timestamps) return ids;
        if (ids.len > scratch.len) return error.InvalidWhisperDecoderPrompt;
        var kept: usize = 0;
        var copy: [3]ForcedDecoderId = undefined;
        for (ids) |entry| {
            if (entry.token_id) |token| if (token == self.no_timestamps_id) continue;
            copy[kept] = entry;
            kept += 1;
        }
        @memcpy(scratch[0..kept], copy[0..kept]);
        return scratch[0..kept];
    }

    pub fn resolve(
        self: *const PromptCache,
        scratch: *[3]ForcedDecoderId,
        language: ?[]const u8,
    ) ![]const ForcedDecoderId {
        const requested = language orelse return self.automatic_ids;
        if (findLanguageToken(self.language_tokens, requested)) |language_id| {
            scratch.* = .{
                .{ .position = 1, .token_id = language_id },
                .{ .position = 2, .token_id = self.transcribe_id },
                .{ .position = 3, .token_id = self.no_timestamps_id },
            };
            return scratch;
        }
        // English-only Whisper tokenizers intentionally omit language tokens.
        // Their immutable artifact/default task prompt is already English.
        if (std.mem.eql(u8, requested, "en") and self.language_tokens.len == 0) {
            return self.automatic_ids;
        }
        return error.UnsupportedWhisperLanguage;
    }
};

fn loadDecodeSettingsFromPaths(
    allocator: std.mem.Allocator,
    generation_config_path: ?[]const u8,
    decode: *DecodeSettings,
) !void {
    const path = generation_config_path orelse return;
    const data = c_file.readFile(allocator, path) catch return;
    defer allocator.free(data);
    try parseDecodeSettings(allocator, data, decode);
}

fn parseDecodeSettings(allocator: std.mem.Allocator, data: []const u8, decode: *DecodeSettings) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, data, .{}) catch
        return error.InvalidWhisperDecoderConfig;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidWhisperDecoderConfig;
    const object = parsed.value.object;
    if (object.get("suppress_tokens")) |value| decode.suppress_tokens = try parseTokenList(allocator, value);
    errdefer allocator.free(decode.suppress_tokens);
    if (object.get("begin_suppress_tokens")) |value| decode.begin_suppress_tokens = try parseTokenList(allocator, value);
    if (object.get("max_initial_timestamp_index")) |value| if (jsonPosition(value)) |index| {
        decode.max_initial_timestamp_index = index;
    };
    if (object.get("prev_sot_token_id")) |value| if (jsonI32(value)) |id| {
        decode.start_of_prev_id = id;
    };
    if (object.get("eos_token_id")) |value| if (jsonI32(value)) |id| {
        decode.eot_id = id;
    };
}

/// Whisper's non-speech suppression list, derived from the vocabulary the
/// way the reference implementation does when a bundle ships no explicit
/// list: single-token symbols and bracket runs, with and without a leading
/// space, plus the musical-note characters.
pub fn nonSpeechTokens(allocator: std.mem.Allocator, tokenizer: tokenizer_mod.Tokenizer, eot_id: i32) ![]const i32 {
    var out = std.ArrayListUnmanaged(i32).empty;
    errdefer out.deinit(allocator);
    const symbols = [_][]const u8{
        "\"", "#",  "(",   ")",   "*",  "+",   "/",  ":",  ";",  "<",   "=",  ">",  "@",   "[",   "\\",
        "]",  "^",  "_",   "`",   "{",  "|",   "}",  "~",
        "「",
        "」",
        "『",
        "』",
        "<<", ">>", "<<<", ">>>", "--", "---", "-(", "-[", "('", "(\"", "((", "))", "(((", ")))", "[[",
        "]]", "{{", "}}",
        "♪♪",
        "♪♪♪",
    };
    const musical = [_][]const u8{ "♩", "♪", "♫", "♬", "♭", "♮", "♯" };
    for ([_][]const u8{ " -", " '" }) |leading| {
        if (firstToken(allocator, tokenizer, leading)) |id| try appendUnique(allocator, &out, id);
    }
    var spaced_buf: [32]u8 = undefined;
    for (symbols) |symbol| {
        if (singleToken(allocator, tokenizer, symbol)) |id| try appendUnique(allocator, &out, id);
        const spaced = std.fmt.bufPrint(&spaced_buf, " {s}", .{symbol}) catch continue;
        if (singleToken(allocator, tokenizer, spaced)) |id| try appendUnique(allocator, &out, id);
    }
    for (musical) |symbol| {
        if (firstToken(allocator, tokenizer, symbol)) |id| try appendUnique(allocator, &out, id);
        const spaced = std.fmt.bufPrint(&spaced_buf, " {s}", .{symbol}) catch continue;
        if (firstToken(allocator, tokenizer, spaced)) |id| try appendUnique(allocator, &out, id);
    }
    // Control tokens never appear inside a transcript.
    var control: i32 = eot_id + 1;
    while (control < eot_id + 8) : (control += 1) try appendUnique(allocator, &out, control);
    return out.toOwnedSlice(allocator);
}

/// Tokens suppressed at the first free position: a bare space and EOT.
pub fn beginSuppressTokens(allocator: std.mem.Allocator, tokenizer: tokenizer_mod.Tokenizer, eot_id: i32) ![]const i32 {
    var out = std.ArrayListUnmanaged(i32).empty;
    errdefer out.deinit(allocator);
    if (singleToken(allocator, tokenizer, " ")) |id| try appendUnique(allocator, &out, id);
    try appendUnique(allocator, &out, eot_id);
    return out.toOwnedSlice(allocator);
}

fn singleToken(allocator: std.mem.Allocator, tokenizer: tokenizer_mod.Tokenizer, text: []const u8) ?i32 {
    const ids = tokenizer.encode(allocator, text) catch return null;
    defer allocator.free(ids);
    if (ids.len != 1 or ids[0] == tokenizer.specialTokens().unk_id) return null;
    return ids[0];
}

fn firstToken(allocator: std.mem.Allocator, tokenizer: tokenizer_mod.Tokenizer, text: []const u8) ?i32 {
    const ids = tokenizer.encode(allocator, text) catch return null;
    defer allocator.free(ids);
    if (ids.len == 0 or ids[0] == tokenizer.specialTokens().unk_id) return null;
    return ids[0];
}

fn appendUnique(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(i32), id: i32) !void {
    for (out.items) |existing| if (existing == id) return;
    try out.append(allocator, id);
}

pub fn languageTokenForCode(language_tokens: []const LanguageToken, code: []const u8) ?i32 {
    return findLanguageToken(language_tokens, code);
}

fn parseTokenList(allocator: std.mem.Allocator, value: std.json.Value) ![]const i32 {
    if (value != .array) return &.{};
    const out = try allocator.alloc(i32, value.array.items.len);
    errdefer allocator.free(out);
    for (value.array.items, 0..) |item, i| out[i] = jsonI32(item) orelse return error.InvalidWhisperDecoderConfig;
    return out;
}

pub fn loadForcedDecoderIds(allocator: std.mem.Allocator, model_dir: []const u8) !?[]ForcedDecoderId {
    const generation_config_path = try std.fs.path.join(
        allocator,
        &.{ model_dir, "generation_config.json" },
    );
    defer allocator.free(generation_config_path);
    const config_path = try std.fs.path.join(allocator, &.{ model_dir, "config.json" });
    defer allocator.free(config_path);
    return loadForcedDecoderIdsFromPaths(allocator, generation_config_path, config_path);
}

fn loadForcedDecoderIdsFromPaths(
    allocator: std.mem.Allocator,
    generation_config_path: ?[]const u8,
    config_path: ?[]const u8,
) !?[]ForcedDecoderId {
    for ([_]?[]const u8{ generation_config_path, config_path }) |maybe_path| {
        const path = maybe_path orelse continue;
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
    ids: []ForcedDecoderId,
    language_tokens: []const LanguageToken,
) void {
    for (ids) |*entry| {
        if (entry.position != 1) return;
        const token_id = entry.token_id orelse return;
        if (languageCodeForToken(language_tokens, token_id) != null) entry.token_id = null;
        return;
    }
}

fn buildAutomaticLanguagePromptFromTokens(
    allocator: std.mem.Allocator,
    language_tokens: []const LanguageToken,
    transcribe_id: i32,
    no_timestamps_id: i32,
) ![]ForcedDecoderId {
    const multilingual = language_tokens.len > 0;
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
    language_tokens: []const LanguageToken,
    token_id: i32,
) ?[]const u8 {
    for (language_tokens) |language| {
        if (language.token_id == token_id) return language.code;
    }
    return null;
}

fn findLanguageToken(language_tokens: []const LanguageToken, code: []const u8) ?i32 {
    for (language_tokens) |language| {
        if (std.mem.eql(u8, language.code, code)) return language.token_id;
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
    language_tokens: []const LanguageToken,
    logits: []const f32,
) ?DetectedLanguage {
    var best: ?DetectedLanguage = null;
    var best_logit: f32 = -std.math.inf(f32);
    for (language_tokens) |language| {
        const token_id = language.token_id;
        if (token_id < 0 or @as(usize, @intCast(token_id)) >= logits.len) continue;
        const logit = logits[@intCast(token_id)];
        if (std.math.isNan(logit)) continue;
        if (best == null or logit > best_logit) {
            best = .{ .token_id = token_id, .code = language.code };
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
    const languages = [_]LanguageToken{
        .{ .token_id = 10, .code = "en" },
        .{ .token_id = 11, .code = "es" },
    };
    var logits = [_]f32{0} ** 16;
    logits[10] = 1;
    logits[11] = 4;
    logits[15] = 100;
    const detected = detectLanguageToken(&languages, &logits).?;
    try std.testing.expectEqual(@as(i32, 11), detected.token_id);
    try std.testing.expectEqualStrings("es", detected.code);
}

test "prompt cache resolves automatic and explicit languages without request tokenization" {
    const allocator = std.testing.allocator;
    const hf_tokenizer = @import("inference_hf_tokenizer");
    var tokenizer = try hf_tokenizer.HfTokenizer.loadFromBytes(allocator,
        \\{"version":"1.0","added_tokens":[{"id":0,"content":"<unk>"},{"id":10,"content":"<|en|>"},{"id":11,"content":"<|es|>"},{"id":12,"content":"<|transcribe|>"},{"id":13,"content":"<|notimestamps|>"}],"model":{"type":"BPE","vocab":{"<unk>":0,"<|en|>":10,"<|es|>":11,"<|transcribe|>":12,"<|notimestamps|>":13},"merges":[]}}
    );
    defer tokenizer.deinitSelf();

    var cache = try PromptCache.init(allocator, "/definitely/not/a/model", tokenizer.tokenizer());
    defer cache.deinit();
    try std.testing.expectEqual(@as(usize, 2), cache.language_tokens.len);

    var scratch: [3]ForcedDecoderId = undefined;
    const automatic = try cache.resolve(&scratch, null);
    try std.testing.expectEqual(@as(?i32, null), automatic[0].token_id);
    const spanish = try cache.resolve(&scratch, "es");
    try std.testing.expectEqual(@as(?i32, 11), spanish[0].token_id);
    try std.testing.expectEqual(@as(?i32, 12), spanish[1].token_id);
    try std.testing.expectEqual(@as(?i32, 13), spanish[2].token_id);
    try std.testing.expectError(error.UnsupportedWhisperLanguage, cache.resolve(&scratch, "zz"));
}

test "decode settings parse suppression and timestamp fields" {
    const allocator = std.testing.allocator;
    var decode = DecodeSettings{};
    try parseDecodeSettings(allocator, "{\"suppress_tokens\":[1,2,3],\"begin_suppress_tokens\":[220,50257],\"max_initial_timestamp_index\":25,\"prev_sot_token_id\":50361,\"eos_token_id\":50257}", &decode);
    defer allocator.free(decode.suppress_tokens);
    defer allocator.free(decode.begin_suppress_tokens);
    try std.testing.expectEqualSlices(i32, &.{ 1, 2, 3 }, decode.suppress_tokens);
    try std.testing.expectEqualSlices(i32, &.{ 220, 50257 }, decode.begin_suppress_tokens);
    try std.testing.expectEqual(@as(usize, 25), decode.max_initial_timestamp_index);
    try std.testing.expectEqual(@as(?i32, 50361), decode.start_of_prev_id);
}

test "resolveWithTimestamps drops the notimestamps slot and keeps the rest" {
    const allocator = std.testing.allocator;
    const ids = try allocator.alloc(ForcedDecoderId, 3);
    ids[0] = .{ .position = 1, .token_id = null };
    ids[1] = .{ .position = 2, .token_id = 12 };
    ids[2] = .{ .position = 3, .token_id = 13 };
    var cache = PromptCache{
        .allocator = allocator,
        .automatic_ids = ids,
        .language_tokens = try allocator.alloc(LanguageToken, 0),
        .transcribe_id = 12,
        .no_timestamps_id = 13,
        .decode = .{ .suppress_tokens = try allocator.alloc(i32, 0), .begin_suppress_tokens = try allocator.alloc(i32, 0) },
    };
    defer cache.deinit();
    var scratch: [3]ForcedDecoderId = undefined;
    const plain = try cache.resolveWithTimestamps(&scratch, null, false);
    try std.testing.expectEqual(@as(usize, 3), plain.len);
    const timed = try cache.resolveWithTimestamps(&scratch, null, true);
    try std.testing.expectEqual(@as(usize, 2), timed.len);
    try std.testing.expectEqual(@as(?i32, null), timed[0].token_id);
    try std.testing.expectEqual(@as(?i32, 12), timed[1].token_id);
}

test "decode settings fall back to the fixed Whisper token layout" {
    // <|notimestamps|> at 13 implies <|startofprev|> at 11 and <|0.00|> at 14.
    var decode = DecodeSettings{
        .start_of_prev_id = null,
        .timestamp_begin_id = 13 + 1,
    };
    decode.start_of_prev_id = decode.start_of_prev_id orelse 13 - 2;
    try std.testing.expectEqual(@as(?i32, 11), decode.start_of_prev_id);
    try std.testing.expectEqual(@as(i32, 14), decode.timestamp_begin_id);
}
