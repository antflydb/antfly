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

const std = @import("std");
pub const Config = struct { output_name: []const u8 };
pub fn parse(a: std.mem.Allocator, bytes: []const u8) !std.json.Parsed(Config) {
    var parsed = try std.json.parseFromSlice(Config, a, bytes, .{});
    errdefer parsed.deinit();
    if (parsed.value.output_name.len == 0 or parsed.value.output_name.len > 128) return error.InvalidAssetProducerConfig;
    return parsed;
}

const shared = @import("inference_chunker").outputs;
pub const Result = shared.Result;
pub const Row = shared.Row;
pub const Projection = shared.Projection;
pub const TranscriptSpan = shared.TranscriptSpan;
pub const project = shared.project;

test "model aware media transcript assets retain UTF8 offsets source clocks and local speaker scopes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const base = Result{ .name = "speech", .kind = .transcribe, .required = true, .model = "whisper", .model_identity = "identity", .model_revision = "revision", .recipe_id = "recipe", .status = .complete };
    var first = base;
    first.spans = &.{.{ .text = "世界", .start_ns = 10, .end_ns = 20, .speaker_index = 0 }};
    var second = base;
    second.sub_index = 1;
    second.spans = &.{.{ .text = "hello", .start_ns = 30, .end_ns = 40, .speaker_index = 0 }};
    var optional = base;
    optional.required = false;
    optional.status = .failed;
    optional.terminal = true;
    const projected = try project(arena.allocator(), &.{ .{ .chunk_id = 4, .outputs = &.{ first, second } }, .{ .chunk_id = 5, .outputs = &.{optional} } });
    try std.testing.expectEqualStrings("世界 hello", projected.text);
    try std.testing.expectEqual(@as(u32, 7), projected.transcript_spans[1].char_start);
    try std.testing.expectEqual(@as(i64, 30), projected.transcript_spans[1].start_ns);
    try std.testing.expectEqual(@as(u32, 4), projected.transcript_spans[1].parent_ordinal);
    try std.testing.expectEqual(@as(u32, 1), projected.transcript_spans[1].sub_index);
    try std.testing.expect(projected.status == .degraded);
    try std.testing.expectEqual(@as(u64, 1), projected.complete_chunks);
    try std.testing.expectEqual(@as(u64, 1), projected.terminal_chunks);
}

/// Text cuts keep whole phrase clocks. A partial phrase has the original
/// timing envelope; we never invent word timing by interpolating bytes.
pub fn stampTranscriptChunks(a: std.mem.Allocator, chunks: []@import("../../../chunking/chunk.zig").Chunk, raw: []const u8, source_name: []const u8) !void {
    var asset = try std.json.parseFromSlice(struct { text: []const u8, source_artifact_name: []const u8 = "", transcript_spans: []const TranscriptSpan }, a, raw, .{ .ignore_unknown_fields = true });
    defer asset.deinit();
    for (chunks) |*chunk| {
        const first = chunk.start_offset orelse return error.InvalidTranscriptCoordinates;
        const last = chunk.end_offset orelse return error.InvalidTranscriptCoordinates;
        if (first > last or last > asset.value.text.len) return error.InvalidTranscriptCoordinates;
        var spans = std.ArrayList(TranscriptSpan).empty;
        defer spans.deinit(a);
        var start: ?i64 = null;
        var end: i64 = 0;
        for (asset.value.transcript_spans) |phrase| {
            if (phrase.char_end <= first or phrase.char_start >= last) continue;
            if (phrase.char_start > phrase.char_end or phrase.char_end > asset.value.text.len or phrase.start_ns < 0 or phrase.end_ns < phrase.start_ns) return error.InvalidTranscriptCoordinates;
            var local = phrase;
            local.char_start = @max(first, phrase.char_start) - first;
            local.char_end = @min(last, phrase.char_end) - first;
            try spans.append(a, local);
            start = @min(start orelse phrase.start_ns, phrase.start_ns);
            end = @max(end, phrase.end_ns);
        }
        if (start) |begin| {
            chunk.start_time_ms = @as(f32, @floatFromInt(begin)) / std.time.ns_per_ms;
            chunk.end_time_ms = @as(f32, @floatFromInt(end)) / std.time.ns_per_ms;
            chunk.media_spans_json = try std.json.Stringify.valueAlloc(a, .{ .source_artifact_name = source_name, .media_chunk_artifact_name = asset.value.source_artifact_name, .start_ns = begin, .end_ns = end, .transcript_spans = spans.items }, .{ .emit_null_optional_fields = false });
        }
    }
}

test "model aware media transcript rechunking preserves exact phrase clocks and local parent scopes" {
    const chunker = @import("chunker.zig");
    const a = std.testing.allocator;
    const chunks = try chunker.chunkText(a, "hello 世界", 7, 0);
    defer chunker.freeChunks(a, chunks);
    try stampTranscriptChunks(a, chunks, "{\"text\":\"hello 世界\",\"source_artifact_name\":\"clips\",\"transcript_spans\":[{\"char_start\":0,\"char_end\":12,\"start_ns\":123,\"end_ns\":999,\"parent_ordinal\":4,\"sub_index\":1,\"speaker_index\":0}]}", "transcript");
    try std.testing.expectEqual(@as(usize, 2), chunks.len);
    for (chunks) |chunk| {
        var media = try std.json.parseFromSlice(struct { start_ns: i64, end_ns: i64, transcript_spans: []const TranscriptSpan }, a, chunk.media_spans_json.?, .{ .ignore_unknown_fields = true });
        defer media.deinit();
        try std.testing.expectEqual(@as(i64, 123), media.value.start_ns);
        try std.testing.expectEqual(@as(i64, 999), media.value.end_ns);
        try std.testing.expectEqual(@as(u32, 4), media.value.transcript_spans[0].parent_ordinal);
        try std.testing.expectEqual(@as(u32, 1), media.value.transcript_spans[0].sub_index);
    }
}
