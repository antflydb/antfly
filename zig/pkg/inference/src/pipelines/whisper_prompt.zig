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

/// Resolve Whisper's decoder prefix. Newer Hugging Face
/// generation_config.json files leave the language slot null for runtime
/// detection, while the model config still carries the artifact's default
/// concrete prefix. Prefer an explicit request language, then the first fully
/// specified artifact prefix, and finally a deterministic English prefix.
pub fn resolveForcedDecoderIds(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    tokenizer: tokenizer_mod.Tokenizer,
    language: ?[]const u8,
) ?[]const [2]i32 {
    if (language) |value| return buildForcedDecoderIds(allocator, tokenizer, value);
    if (loadForcedDecoderIds(allocator, model_dir)) |ids| return ids;
    return buildForcedDecoderIds(allocator, tokenizer, "en");
}

pub fn loadForcedDecoderIds(allocator: std.mem.Allocator, model_dir: []const u8) ?[]const [2]i32 {
    for ([_][]const u8{ "generation_config.json", "config.json" }) |filename| {
        const path = std.fmt.allocPrint(allocator, "{s}/{s}", .{ model_dir, filename }) catch return null;
        defer allocator.free(path);

        const data = c_file.readFile(allocator, path) catch continue;
        defer allocator.free(data);
        if (parseForcedDecoderIds(allocator, data)) |ids| return ids;
    }
    return null;
}

fn parseForcedDecoderIds(allocator: std.mem.Allocator, data: []const u8) ?[]const [2]i32 {
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, data, .{}) catch return null;
    defer parsed.deinit();

    if (parsed.value != .object) return null;
    const value = parsed.value.object.get("forced_decoder_ids") orelse return null;
    if (value != .array or value.array.items.len == 0) return null;

    var result = std.ArrayListUnmanaged([2]i32).empty;
    defer result.deinit(allocator);
    for (value.array.items) |pair| {
        if (pair != .array or pair.array.items.len != 2) return null;
        const position = jsonI32(pair.array.items[0]) orelse return null;
        const token_id = jsonI32(pair.array.items[1]) orelse return null;
        result.append(allocator, .{ position, token_id }) catch return null;
    }
    return result.toOwnedSlice(allocator) catch null;
}

fn jsonI32(value: std.json.Value) ?i32 {
    const raw = switch (value) {
        .integer => |integer| integer,
        else => return null,
    };
    return std.math.cast(i32, raw);
}

fn buildForcedDecoderIds(
    allocator: std.mem.Allocator,
    tokenizer: tokenizer_mod.Tokenizer,
    language: []const u8,
) ?[]const [2]i32 {
    const language_token = std.fmt.allocPrint(allocator, "<|{s}|>", .{language}) catch return null;
    defer allocator.free(language_token);

    const language_id = encodeSingleSpecialToken(allocator, tokenizer, language_token) orelse return null;
    const transcribe_id = encodeSingleSpecialToken(allocator, tokenizer, "<|transcribe|>") orelse return null;
    const no_timestamps_id = encodeSingleSpecialToken(allocator, tokenizer, "<|notimestamps|>") orelse return null;
    const result = allocator.alloc([2]i32, 3) catch return null;
    result[0] = .{ 1, language_id };
    result[1] = .{ 2, transcribe_id };
    result[2] = .{ 3, no_timestamps_id };
    return result;
}

fn encodeSingleSpecialToken(
    allocator: std.mem.Allocator,
    tokenizer: tokenizer_mod.Tokenizer,
    text: []const u8,
) ?i32 {
    const ids = tokenizer.encode(allocator, text) catch return null;
    defer allocator.free(ids);
    if (ids.len != 1) return null;
    return ids[0];
}

test "forced decoder ids reject a dynamic language slot" {
    const allocator = std.testing.allocator;
    try std.testing.expect(parseForcedDecoderIds(
        allocator,
        "{\"forced_decoder_ids\":[[1,null],[2,50359]]}",
    ) == null);
}

test "forced decoder ids parse a concrete artifact prefix" {
    const allocator = std.testing.allocator;
    const ids = parseForcedDecoderIds(
        allocator,
        "{\"forced_decoder_ids\":[[1,50265],[2,50359],[3,50363]]}",
    ) orelse return error.TestExpectedEqual;
    defer allocator.free(ids);
    try std.testing.expectEqualSlices([2]i32, &.{ .{ 1, 50265 }, .{ 2, 50359 }, .{ 3, 50363 } }, ids);
}
