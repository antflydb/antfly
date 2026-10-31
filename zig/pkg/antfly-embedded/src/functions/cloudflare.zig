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

//! Workers AI native decision payload, shared by SQL and enrichment deciders.
const std = @import("std");
const d = @import("decisions.zig");
const cf = @import("antfly_decisions").cloudflare;
const image = @import("antfly_image");

pub fn request(a: std.mem.Allocator, cfg: d.DeciderConfig, input: d.Input, questions: d.Json) !d.WirePayload {
    const initial = try d.wireRequest(a, cfg, if (input == .text) input.text else "visual evidence", questions);
    var payload = try std.json.parseFromSliceLeaky(d.Json, a, initial, .{});
    var images: std.array_list.Managed(d.Json) = .init(a);
    var messages: std.array_list.Managed(d.Json) = .init(a);
    var total: usize = 0;
    if (input != .text) {
        for (input.messageItems()) |message| {
            var content: std.array_list.Managed(d.Json) = .init(a);
            const source = message.object.get("content").?;
            if (source == .string) {
                try content.append(source);
            } else for (source.array.items) |part| {
                if (std.mem.eql(u8, part.object.get("type").?.string, "input_text")) {
                    try content.append(part.object.get("text").?);
                } else {
                    const url = part.object.get("image_url").?.string;
                    const inline_image = try @import("antfly_decisions").evidence.inlineImage(url);
                    if (!std.mem.eql(u8, inline_image.mime, "image/png") and !std.mem.eql(u8, inline_image.mime, "image/jpeg") and !std.mem.eql(u8, inline_image.mime, "image/webp")) return error.UnsupportedInferenceFeature;
                    const size = try std.base64.standard.Decoder.calcSizeForSlice(inline_image.encoded);
                    if (size > 4 * 1024 * 1024 or size > 8 * 1024 * 1024 -| total or images.items.len >= 4) return error.DecisionLimitExceeded;
                    total += size;
                    const bytes = try a.alloc(u8, size);
                    try std.base64.standard.Decoder.decode(bytes, inline_image.encoded);
                    const expected = image.inferenceFormatForMimeEssence(inline_image.mime) orelse return error.UnsupportedInferenceFeature;
                    if (image.detectFormat(bytes) != expected) return error.InvalidInferenceMedia;
                    const info = try image.inspectEncoded(bytes);
                    if (try info.pixels() > 16_000_000) return error.DecisionLimitExceeded;
                    try images.append(cf.string(url));
                }
            }
            var row = cf.object();
            try cf.put(a, &row, "role", message.object.get("role").?);
            try cf.put(a, &row, "content", .{ .array = content });
            try messages.append(row);
        }
        try cf.put(a, &payload, "state", .{ .array = messages });
        if (images.items.len > 0) try cf.put(a, &payload, "images", .{ .array = images });
    }
    try cf.validatePayload(a, payload);
    const body = try std.json.Stringify.valueAlloc(a, payload, .{});
    if (body.len > 13 * 1024 * 1024) return error.DecisionLimitExceeded;
    return .{ .body = body, .image_bytes = input.inlineImageBytes() };
}

test "decision functions Cloudflare decider preserves native choices and strips private score metadata" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const questions = try d.questionsFor(a, .ai_probability, &.{ .{ .string = "Receipt" }, .{ .string = "Needs OCR?" }, .{ .string = "cloudflare" } });
    const result = try request(a, .{ .provider = .cloudflare, .account_id = "account", .model = "@cf/cloudflare/clef" }, .{ .text = "Receipt" }, questions);
    const parsed = try std.json.parseFromSliceLeaky(d.Json, a, result.body, .{});
    try std.testing.expectEqualStrings("clef", parsed.object.get("model").?.string);
    try std.testing.expect(parsed.object.get("questions").?.object.count() == 1);
}
