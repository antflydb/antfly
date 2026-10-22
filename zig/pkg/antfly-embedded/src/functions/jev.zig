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

//! Jev's image transport follows the official TypeSafe SDK: a JSON `request`
//! multipart part, an assets map, and one binary part per referenced image.
const std = @import("std");
const d = @import("decisions.zig");
const evidence = @import("decision_input.zig");

pub fn request(a: std.mem.Allocator, io: std.Io, cfg: d.DeciderConfig, input: d.Input, questions: d.Json) !d.WirePayload {
    try input.validate();
    if (input == .text) return .{ .body = try d.wireRequest(a, cfg, input.text, questions) };
    // Random references cannot alias image-like text supplied by the caller;
    // the same random value also makes multipart boundary collisions negligible.
    var random: [16]u8 = undefined;
    io.random(&random);
    const boundary = try std.fmt.allocPrint(a, "antfly-{s}", .{std.fmt.bytesToHex(random, .lower)});
    var root = try std.json.parseFromSliceLeaky(d.Json, a, try d.wireRequest(a, cfg, "", questions), .{});
    var state: d.Json = .{ .array = std.array_list.Managed(d.Json).init(a) };
    var assets = d.jsonObject();
    const Part = struct { header: []const u8, bytes: []const u8 };
    var parts: std.ArrayList(Part) = try .initCapacity(a, input.imageCount());
    for (input.messages) |message| {
        var upstream = d.jsonObject();
        try d.put(a, &upstream, "role", .{ .string = "user" });
        const content = message.object.get("content").?;
        if (content == .string) {
            try d.put(a, &upstream, "content", content);
        } else {
            var out: d.Json = .{ .array = std.array_list.Managed(d.Json).init(a) };
            for (content.array.items) |part| {
                const kind = part.object.get("type").?.string;
                if (std.mem.eql(u8, kind, "input_text")) {
                    try out.array.append(part.object.get("text").?);
                    continue;
                }
                if (part.object.get("detail")) |detail| if (!std.mem.eql(u8, detail.string, "auto")) return error.UnsupportedInferenceFeature;
                const image = try evidence.inlineImage(part.object.get("image_url").?.string);
                if (!std.mem.eql(u8, image.mime, "image/png") and !std.mem.eql(u8, image.mime, "image/jpeg")) return error.UnsupportedInferenceFeature;
                const bytes = try a.alloc(u8, std.base64.standard.Decoder.calcSizeForSlice(image.encoded) catch return error.InvalidDecisionSpecification);
                std.base64.standard.Decoder.decode(bytes, image.encoded) catch return error.InvalidDecisionSpecification;
                const signature: []const u8 = if (std.mem.eql(u8, image.mime, "image/png")) "\x89PNG\r\n\x1a\n" else "\xff\xd8\xff";
                if (!std.mem.startsWith(u8, bytes, signature)) return error.InvalidDecisionSpecification;
                const name = try std.fmt.allocPrint(a, "asset-{d}", .{assets.object.count()});
                const reference = try std.fmt.allocPrint(a, "<{s}:{s}>", .{ boundary, name });
                var asset = d.jsonObject();
                try d.put(a, &asset, "type", .{ .string = "image" });
                try d.put(a, &asset, "part", .{ .string = name });
                try d.put(a, &assets, reference, asset);
                try out.array.append(.{ .string = reference });
                parts.appendAssumeCapacity(.{
                    .header = try std.fmt.allocPrint(a, "--{s}\r\nContent-Disposition: form-data; name=\"{s}\"; filename=\"{s}\"\r\nContent-Type: {s}\r\n\r\n", .{ boundary, name, name, image.mime }),
                    .bytes = bytes,
                });
            }
            try d.put(a, &upstream, "content", out);
        }
        try state.array.append(upstream);
    }
    try d.put(a, &root, "state", state);
    if (assets.object.count() == 0) return .{ .body = try std.json.Stringify.valueAlloc(a, root, .{}) };
    try d.put(a, &root, "assets", assets);
    const header = try std.fmt.allocPrint(a, "--{s}\r\nContent-Disposition: form-data; name=\"request\"\r\nContent-Type: application/json\r\n\r\n{s}\r\n", .{ boundary, try std.json.Stringify.valueAlloc(a, root, .{}) });
    const footer = try std.fmt.allocPrint(a, "--{s}--\r\n", .{boundary});
    var size = header.len + footer.len;
    var image_bytes: usize = 0;
    for (parts.items) |part| {
        size += part.header.len + part.bytes.len + 2;
        image_bytes += part.bytes.len;
    }
    // Allocate once: growing byte arrays inside an arena retain every previous
    // buffer. Only metadata and decoded image bytes precede this final copy.
    var body: std.ArrayList(u8) = try .initCapacity(a, size);
    body.appendSliceAssumeCapacity(header);
    for (parts.items) |part| {
        body.appendSliceAssumeCapacity(part.header);
        body.appendSliceAssumeCapacity(part.bytes);
        body.appendSliceAssumeCapacity("\r\n");
    }
    body.appendSliceAssumeCapacity(footer);
    return .{ .body = try body.toOwnedSlice(a), .image_bytes = image_bytes, .content_type = try std.fmt.allocPrint(a, "multipart/form-data; boundary={s}", .{boundary}), .headers = &.{.{ "TypeSafe-Danger-Zone-Images", "true" }} };
}

test "decision functions Jev rejects unsupported image encodings before dispatch" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const questions = try d.questionsFor(a, .ai_probability, &.{ .{ .string = "item" }, .{ .string = "Damaged?" }, .{ .string = "jev" } });
    for ([_][]const u8{ "https://example.org/image.png", "data:image/webp;base64,UklGRg==", "data:image/gif;base64,R0lGODlh" }) |url| {
        const source = try std.fmt.allocPrint(a, "[{{\"role\":\"user\",\"content\":[{{\"type\":\"input_image\",\"image_url\":\"{s}\"}}]}}]", .{url});
        const input = try d.Input.fromJson(try std.json.parseFromSliceLeaky(d.Json, a, source, .{}));
        try std.testing.expectError(error.UnsupportedInferenceFeature, request(a, std.testing.io, .{ .provider = .jev }, input, questions));
    }
    const bad_signature = try d.Input.fromJson(try std.json.parseFromSliceLeaky(d.Json, a, "[{\"role\":\"user\",\"content\":[{\"type\":\"input_image\",\"image_url\":\"data:image/png;base64,aGVsbG8=\"}]}]", .{}));
    try std.testing.expectError(error.InvalidDecisionSpecification, request(a, std.testing.io, .{ .provider = .jev }, bad_signature, questions));
}

test "decision functions Jev multipart stays within the production arena budget" {
    var outer = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer outer.deinit();
    const a = outer.allocator();
    const questions = try d.questionsFor(a, .ai_probability, &.{ .{ .string = "item" }, .{ .string = "Damaged?" }, .{ .string = "jev" } });
    for ([_]usize{ 1, 8, 32, 128 }) |count| {
        const bytes = try a.alloc(u8, 720000 / count);
        @memset(bytes, 0);
        @memcpy(bytes[0..8], "\x89PNG\r\n\x1a\n");
        const encoded = try a.alloc(u8, std.base64.standard.Encoder.calcSize(bytes.len));
        _ = std.base64.standard.Encoder.encode(encoded, bytes);
        var part = d.jsonObject();
        try d.put(a, &part, "type", .{ .string = "input_image" });
        try d.put(a, &part, "image_url", .{ .string = try std.fmt.allocPrint(a, "data:image/png;base64,{s}", .{encoded}) });
        var content: d.Json = .{ .array = std.array_list.Managed(d.Json).init(a) };
        for (0..count) |_| try content.array.append(part);
        var message = d.jsonObject();
        try d.put(a, &message, "role", .{ .string = "user" });
        try d.put(a, &message, "content", content);
        const input: d.Input = .{ .messages = &.{message} };
        try input.checkLimit(a, 1024 * 1024);
        var budget: @import("../sql/memory_budget.zig") = .{ .backing = std.testing.allocator, .limit = 8 * 1024 * 1024 };
        var arena = std.heap.ArenaAllocator.init(budget.allocator());
        defer arena.deinit();
        const payload = try request(arena.allocator(), std.testing.io, .{ .provider = .jev }, input, questions);
        try std.testing.expect(payload.body.len >= 720000);
        try std.testing.expect(!budget.exhausted);
        // Leave room for HTTP's owned request copy and a normal decision response.
        try std.testing.expect(budget.peak < 6 * 1024 * 1024);
    }
}
