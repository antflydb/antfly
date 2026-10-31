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

//! Consumer-local adaptation for the compiled inference provider. Storage and
//! standalone use one implementation for cancellation, progress, binary media,
//! numeric results, and invocation lifetime; the inference node stays opaque.
const std = @import("std");
const inference = struct {
    pub const ChatMessage = @import("antfly_inference_types").ChatMessage;
    pub const ContentPart = @import("antfly_inference_types").ContentPart;
    pub const work = @import("antfly_inference_work");
    pub const execution_context = @import("antfly_inference_execution_context");
};
pub const VerifiedMessages = struct {
    arena: std.heap.ArenaAllocator,
    messages: []const inference.ChatMessage,
    attachments: []const inference.work.Attachment,
    pub fn deinit(self: *@This()) void {
        self.arena.deinit();
    }
};
// Move already verified codec bytes over the existing typed binary boundary.
// Each occurrence has a transport slot while duplicate slots borrow one buffer.
pub fn prepare(a: std.mem.Allocator, messages: []const inference.ChatMessage, media: []const inference.execution_context.VerifiedMedia, explicit_references: bool) !?VerifiedMessages {
    if (media.len == 0) return null;
    // Find the first reusable occurrence before allocating headers. Text-only,
    // unmatched and high-detail requests keep the ordinary path allocation-free
    // here. Save its verified view so copied URLs are compared only once.
    const First = struct { message: usize, part: usize, media: inference.execution_context.VerifiedMedia };
    var first: ?First = null;
    for (messages, 0..) |message, mi| if (message.content) |content| if (content == .parts) for (content.parts, 0..) |part, pi| {
        // Scan all parts even after finding a match: an unrelated invalid
        // media part must not acquire an attachment through this adaptation.
        if (part == .media and part.media.url == null and (part.media.data.len == 0 or std.mem.startsWith(u8, part.media.data, "attachment:"))) return null;
        if (first != null or part != .image_url) continue;
        if (part.image_url.detail) |detail| if (!std.mem.eql(u8, detail, "auto")) continue;
        for (media) |item| if (item.matches(part.image_url.url)) {
            first = .{ .message = mi, .part = pi, .media = item };
            break;
        };
    };
    const initial = first orelse return null;
    var arena = std.heap.ArenaAllocator.init(a);
    var moved = false;
    defer if (!moved) arena.deinit();
    const scratch = arena.allocator();
    const copied = try scratch.dupe(inference.ChatMessage, messages);
    var attachments: std.ArrayList(inference.work.Attachment) = .empty;
    for (copied, 0..) |*message, mi| if (message.content) |content| if (content == .parts) {
        if (mi < initial.message) continue;
        const parts = try scratch.dupe(inference.ContentPart, content.parts);
        for (parts, 0..) |*part, pi| {
            if (mi == initial.message and pi < initial.part) continue;
            if (part.* != .image_url) continue;
            if (part.image_url.detail) |detail| if (!std.mem.eql(u8, detail, "auto")) continue;
            const found = if (mi == initial.message and pi == initial.part) initial.media else for (media) |item| {
                if (item.matches(part.image_url.url)) break item;
            } else continue;
            try attachments.append(scratch, .{ .bytes = found.bytes, .content_type = found.mime_type });
            part.* = .{ .media = .{ .mime_type = found.mime_type, .data = if (explicit_references) try std.fmt.allocPrint(scratch, "attachment:{d}", .{attachments.items.len - 1}) else "" } };
        }
        message.content = .{ .parts = parts };
    };
    if (attachments.items.len == 0) return null;
    moved = true;
    return .{ .arena = arena, .messages = copied, .attachments = attachments.items };
}

/// Render expanded URLs only for legacy/hosted consumers selected after source
/// verification. Ordinary and already materialized messages allocate nothing.
pub fn materialize(a: std.mem.Allocator, messages: []const inference.ChatMessage, media: []const inference.execution_context.VerifiedMedia) !?VerifiedMessages {
    const has_handles = for (media) |item| {
        if (item.logical_url_bytes != 0) break true;
    } else false;
    if (!has_handles) return null;
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const scratch = arena.allocator();
    const copied = try scratch.dupe(inference.ChatMessage, messages);
    var urls: std.StringHashMapUnmanaged([]const u8) = .empty;
    for (copied) |*message| if (message.content) |content| if (content == .parts) {
        const parts = try scratch.dupe(inference.ContentPart, content.parts);
        for (parts) |*part| {
            if (part.* != .image_url) continue;
            const found = for (media) |item| {
                if (item.logical_url_bytes != 0 and item.matches(part.image_url.url)) break item;
            } else continue;
            const url = urls.get(found.url) orelse rendered: {
                const prefix = try std.fmt.allocPrint(scratch, "data:{s};base64,", .{found.mime_type});
                const data = try scratch.alloc(u8, prefix.len + std.base64.standard.Encoder.calcSize(found.bytes.len));
                @memcpy(data[0..prefix.len], prefix);
                _ = std.base64.standard.Encoder.encode(data[prefix.len..], found.bytes);
                try urls.put(scratch, found.url, data);
                break :rendered data;
            };
            part.image_url.url = url;
        }
        message.content = .{ .parts = parts };
    };
    return .{ .arena = arena, .messages = copied, .attachments = &.{} };
}
