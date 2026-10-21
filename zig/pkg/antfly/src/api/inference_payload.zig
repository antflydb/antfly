// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Transport framing is independent of model selection and task adaptation.
//! Metadata may change while attachment bytes and indices remain borrowed.
const std = @import("std");
const httpx = @import("httpx");
const envelope = httpx.attachment_envelope;

pub const Payload = struct {
    value: std.json.Value,
    attachments: ?[]const envelope.Attachment = null,

    pub fn parse(a: std.mem.Allocator, ctx: *httpx.Context) !Payload {
        const body = (try ctx.body()) orelse return error.InvalidInferenceRequest;
        const raw_type = ctx.header("content-type") orelse "application/json";
        const media_type = std.mem.trim(u8, raw_type[0 .. std.mem.indexOfScalar(u8, raw_type, ';') orelse raw_type.len], " \t");
        var metadata = body;
        var attachments: ?[]const envelope.Attachment = null;
        if (std.ascii.eqlIgnoreCase(media_type, envelope.content_type)) {
            const framed = envelope.parseAlloc(a, body, .{ .max_metadata_bytes = ctx.max_request_body_size }) catch |err| return switch (err) {
                error.OutOfMemory => err,
                error.AttachmentEnvelopeTooLarge => error.InferenceRequestTooLarge,
                else => error.InvalidInferenceRequest,
            };
            metadata = framed.metadata;
            attachments = framed.attachments;
        }
        return .{ .value = try std.json.parseFromSliceLeaky(std.json.Value, a, metadata, .{}), .attachments = attachments };
    }

    pub fn encode(self: Payload, a: std.mem.Allocator) ![]const u8 {
        const metadata = try std.json.Stringify.valueAlloc(a, self.value, .{});
        return encodeMetadata(a, metadata, self.attachments);
    }

    pub fn contentType(self: Payload) []const u8 {
        return if (self.attachments != null) envelope.content_type else "application/json";
    }
};

pub fn encodeMetadata(a: std.mem.Allocator, metadata: []const u8, attachments: ?[]const envelope.Attachment) ![]const u8 {
    return if (attachments) |items| try envelope.encodeAlloc(a, metadata, items) else metadata;
}

pub fn attachmentIndex(value: []const u8) !?usize {
    if (!std.mem.startsWith(u8, value, "attachment:")) return null;
    const digits = value["attachment:".len..];
    if (digits.len == 0 or (digits.len > 1 and digits[0] == '0')) return error.InvalidInferenceRequest;
    for (digits) |byte| if (!std.ascii.isDigit(byte)) return error.InvalidInferenceRequest;
    return std.fmt.parseInt(usize, digits, 10) catch error.InvalidInferenceRequest;
}

pub fn base64(a: std.mem.Allocator, bytes: []const u8) ![]const u8 {
    const result = try a.alloc(u8, std.base64.standard.Encoder.calcSize(bytes.len));
    return std.base64.standard.Encoder.encode(result, bytes);
}

pub fn dataUri(a: std.mem.Allocator, attachment: envelope.Attachment) ![]const u8 {
    return std.fmt.allocPrint(a, "data:{s};base64,{s}", .{ attachment.mime_type, try base64(a, attachment.data) });
}

// A batch item receives only its own attachments, with references rebased to
// its envelope. Walk media fields, never arbitrary prompt/tool string values.
pub fn batchItem(a: std.mem.Allocator, value: std.json.Value, attachments: []const envelope.Attachment, seen: []bool) !Payload {
    var selected: std.ArrayList(envelope.Attachment) = .empty;
    try selectMedia(a, value, attachments, seen, &selected);
    return .{ .value = value, .attachments = try selected.toOwnedSlice(a) };
}

fn selectMedia(a: std.mem.Allocator, value: std.json.Value, attachments: []const envelope.Attachment, seen: []bool, selected: *std.ArrayList(envelope.Attachment)) !void {
    switch (value) {
        .array => |items| for (items.items) |item| try selectMedia(a, item, attachments, seen, selected),
        .object => |object| {
            if (object.get("type")) |kind| if (kind == .string and std.mem.eql(u8, kind.string, "media")) {
                const data = object.getPtr("data") orelse return error.InvalidInferenceRequest;
                if (data.* != .string) return error.InvalidInferenceRequest;
                if (try attachmentIndex(data.string)) |index| {
                    if (index >= attachments.len or seen[index]) return error.InvalidInferenceRequest;
                    seen[index] = true;
                    data.* = .{ .string = try std.fmt.allocPrint(a, "attachment:{d}", .{selected.items.len}) };
                    try selected.append(a, attachments[index]);
                }
                return;
            };
            // Only messages/content carry generation media; tools and schemas
            // can contain arbitrary user objects with identical field names.
            inline for (.{ "messages", "content" }) |field| if (object.get(field)) |child| try selectMedia(a, child, attachments, seen, selected);
        },
        else => {},
    }
}
