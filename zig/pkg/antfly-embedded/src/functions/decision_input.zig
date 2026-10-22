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

//! Borrowed decision evidence. Messages use the OpenAI Decisions common subset;
//! adapters must preserve media, or reject it before provider I/O.
const std = @import("std");
const Json = std.json.Value;

pub const Input = union(enum) {
    text: []const u8,
    messages: []const Json,

    pub fn fromJson(value: Json) !Input {
        const result: Input = switch (value) {
            .string => |s| .{ .text = s },
            .array => |v| .{ .messages = v.items },
            else => return error.InvalidDecisionSpecification,
        };
        try result.validate();
        return result;
    }

    pub fn jsonValue(self: Input, a: std.mem.Allocator) Json {
        return switch (self) {
            .text => |s| .{ .string = s },
            .messages => |items| .{ .array = .{ .items = @constCast(items), .capacity = items.len, .allocator = a, .pointer_stability = .{} } },
        };
    }

    pub fn requireText(self: Input) ![]const u8 {
        return switch (self) {
            .text => |s| s,
            .messages => error.UnsupportedInferenceFeature,
        };
    }

    /// Call after validation. Image references cost tokens independently of
    /// their URL length or compressed file size.
    pub fn imageCount(self: Input) usize {
        if (self == .text) return 0;
        var count: usize = 0;
        for (self.messages) |message| {
            const content = message.object.get("content").?;
            if (content == .array) for (content.array.items) |part| {
                if (std.mem.eql(u8, part.object.get("type").?.string, "input_image")) count += 1;
            };
        }
        return count;
    }

    /// Inline base64 bytes as written by JSON adapters, excluding MIME framing.
    /// Requires a validated input; ordinary text and remote URLs remain text cost.
    pub fn inlineImageBytes(self: Input) usize {
        if (self == .text) return 0;
        var bytes: usize = 0;
        for (self.messages) |message| {
            const content = message.object.get("content").?;
            if (content == .array) for (content.array.items) |part| {
                if (!std.mem.eql(u8, part.object.get("type").?.string, "input_image")) continue;
                const url = part.object.get("image_url").?.string;
                if (std.mem.startsWith(u8, url, "data:")) bytes += (inlineImage(url) catch unreachable).encoded.len;
            };
        }
        return bytes;
    }

    pub fn validate(self: Input) !void {
        switch (self) {
            .text => |s| _ = try nonempty(s),
            .messages => |messages| {
                if (messages.len == 0 or messages.len > 128) return error.DecisionLimitExceeded;
                var images: usize = 0;
                for (messages) |message| {
                    try fields(message, &.{ "role", "content", "type" });
                    if (!std.mem.eql(u8, try string(message.object.get("role")), "user")) return error.UnsupportedInferenceFeature;
                    if (message.object.get("type")) |kind| if (!std.mem.eql(u8, try string(kind), "message")) return error.InvalidDecisionSpecification;
                    const content = message.object.get("content") orelse return error.InvalidDecisionSpecification;
                    if (content == .string) {
                        _ = try nonempty(content.string);
                        continue;
                    }
                    if (content != .array or content.array.items.len == 0) return error.InvalidDecisionSpecification;
                    if (content.array.items.len > 256) return error.DecisionLimitExceeded;
                    for (content.array.items) |part| {
                        if (part != .object) return error.InvalidDecisionSpecification;
                        const kind = try string(part.object.get("type"));
                        if (std.mem.eql(u8, kind, "input_text")) {
                            try fields(part, &.{ "type", "text" });
                            _ = try nonempty(try string(part.object.get("text")));
                        } else if (std.mem.eql(u8, kind, "input_image")) {
                            try fields(part, &.{ "type", "image_url", "detail" });
                            images += 1;
                            if (images > 128) return error.DecisionLimitExceeded;
                            const url = try string(part.object.get("image_url"));
                            try validateImageUrl(url);
                            if (part.object.get("detail")) |value| {
                                const detail = try string(value);
                                if (!std.mem.eql(u8, detail, "auto") and !std.mem.eql(u8, detail, "low") and !std.mem.eql(u8, detail, "high")) return error.InvalidDecisionSpecification;
                            }
                        } else return error.UnsupportedInferenceFeature;
                    }
                }
            },
        }
    }

    /// Enforce the same serialized evidence limit for text and multimodal calls.
    /// This includes inline image bytes and message framing.
    pub fn checkLimit(self: Input, a: std.mem.Allocator, limit: usize) !void {
        try self.validate();
        if (self == .text) {
            if (self.text.len > limit) return error.DecisionLimitExceeded;
        } else {
            const bytes = try std.json.Stringify.valueAlloc(a, self.jsonValue(a), .{});
            defer a.free(bytes);
            if (bytes.len > limit) return error.DecisionLimitExceeded;
        }
    }
};

fn nonempty(s: []const u8) ![]const u8 {
    if (std.mem.trim(u8, s, " \r\n\t").len == 0 or !std.unicode.utf8ValidateSlice(s)) return error.InvalidDecisionSpecification;
    return s;
}
fn string(v: ?Json) ![]const u8 {
    const value = v orelse return error.InvalidDecisionSpecification;
    if (value != .string or !std.unicode.utf8ValidateSlice(value.string)) return error.InvalidDecisionSpecification;
    return value.string;
}
fn fields(value: Json, allowed: []const []const u8) !void {
    if (value != .object) return error.InvalidDecisionSpecification;
    for (value.object.keys()) |key| {
        for (allowed) |name| {
            if (std.mem.eql(u8, name, key)) break;
        } else return error.InvalidDecisionSpecification;
    }
}

pub const InlineImage = struct { mime: []const u8, encoded: []const u8 };
pub fn inlineImage(url: []const u8) !InlineImage {
    if (!std.mem.startsWith(u8, url, "data:")) return error.UnsupportedInferenceFeature;
    const separator = std.mem.indexOf(u8, url, ";base64,") orelse return error.InvalidDecisionSpecification;
    const mime = url[5..separator];
    if (!std.mem.eql(u8, mime, "image/png") and !std.mem.eql(u8, mime, "image/jpeg") and !std.mem.eql(u8, mime, "image/webp") and !std.mem.eql(u8, mime, "image/gif")) return error.UnsupportedInferenceFeature;
    const encoded = url[separator + 8 ..];
    if (encoded.len == 0) return error.InvalidDecisionSpecification;
    _ = std.base64.standard.Decoder.calcSizeForSlice(encoded) catch return error.InvalidDecisionSpecification;
    // calcSizeForSlice checks padding/length; check every alphabet byte as well.
    const unpadded = std.mem.trimEnd(u8, encoded, "=");
    if (encoded.len - unpadded.len > 2) return error.InvalidDecisionSpecification;
    for (unpadded) |c| if (!std.ascii.isAlphanumeric(c) and c != '+' and c != '/') return error.InvalidDecisionSpecification;
    return .{ .mime = mime, .encoded = encoded };
}
fn validateImageUrl(url: []const u8) !void {
    if (std.mem.startsWith(u8, url, "data:")) {
        _ = try inlineImage(url);
        return;
    }
    const uri = std.Uri.parse(url) catch return error.InvalidDecisionSpecification;
    if ((!std.mem.eql(u8, uri.scheme, "https") and !std.mem.eql(u8, uri.scheme, "http")) or uri.host == null or uri.user != null or uri.password != null or std.mem.indexOfAny(u8, url, "\r\n\t ") != null) return error.InvalidDecisionSpecification;
}

test "decision functions evidence rejects malformed and unsupported media" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const cases = [_]struct { json: []const u8, failure: anyerror }{
        .{ .json = "null", .failure = error.InvalidDecisionSpecification },
        .{ .json = "[]", .failure = error.DecisionLimitExceeded },
        .{ .json = "[{\"role\":\"system\",\"content\":\"secret\"}]", .failure = error.UnsupportedInferenceFeature },
        .{ .json = "[{\"role\":\"user\",\"content\":[{\"type\":\"input_audio\",\"data\":\"AAAA\"}]}]", .failure = error.UnsupportedInferenceFeature },
        .{ .json = "[{\"role\":\"user\",\"content\":[{\"type\":\"input_image\",\"image_url\":\"file:///etc/passwd\"}]}]", .failure = error.InvalidDecisionSpecification },
        .{ .json = "[{\"role\":\"user\",\"content\":[{\"type\":\"input_image\",\"image_url\":\"data:image/png;base64,!!!!\"}]}]", .failure = error.InvalidDecisionSpecification },
        .{ .json = "[{\"role\":\"user\",\"content\":[{\"type\":\"input_image\",\"image_url\":\"https://example.org/image.png\",\"detail\":\"unknown\"}]}]", .failure = error.InvalidDecisionSpecification },
        .{ .json = "[{\"role\":\"user\",\"content\":\"hello\",\"ignored\":true}]", .failure = error.InvalidDecisionSpecification },
    };
    for (cases) |case| try std.testing.expectError(case.failure, Input.fromJson(try std.json.parseFromSliceLeaky(Json, a, case.json, .{})));
    const valid = try Input.fromJson(try std.json.parseFromSliceLeaky(Json, a, "[{\"role\":\"user\",\"content\":[{\"type\":\"input_image\",\"image_url\":\"data:image/png;base64,iVBORw0KGgo=\"}]}]", .{}));
    try std.testing.expectError(error.DecisionLimitExceeded, valid.checkLimit(a, 8));
    try std.testing.expectError(error.UnsupportedInferenceFeature, valid.requireText());
    const part = valid.messages[0].object.get("content").?.array.items[0];
    const many = try a.alloc(Json, 129);
    @memset(many, part);
    var content: Json = .{ .array = .{ .items = many, .capacity = many.len, .allocator = a, .pointer_stability = .{} } };
    var message = valid.messages[0];
    try message.object.put(a, "content", content);
    try std.testing.expectError(error.DecisionLimitExceeded, (Input{ .messages = &.{message} }).validate());
    _ = &content;
}
