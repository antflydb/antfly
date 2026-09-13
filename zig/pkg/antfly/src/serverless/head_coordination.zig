// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Shared durable record for serverless head publication and work ownership.
//!
//! Keeping the visible head and fencing token in one conditionally-written
//! object makes lease takeover and publication linearizable. A worker cannot
//! validate one object and then publish through another after ownership moved.

const std = @import("std");

pub const format_version: u32 = 1;
pub const metadata_content_type_prefix = "application/vnd.antfly.serverless-head.v1;base64url=";
pub const payload_fingerprint_suffix_len = 1 + std.crypto.hash.sha2.Sha256.digest_length * 4;

pub const Record = struct {
    format_version: u32 = format_version,
    head_version: u64 = 0,
    owner_id: ?[]const u8 = null,
    fencing_token: u64 = 0,
    expires_at_unix_ns: u64 = 0,
    released: bool = true,

    pub fn fromLegacyHead(head_version: u64) Record {
        return .{ .head_version = head_version };
    }
};

pub const Fence = struct {
    owner_id: []const u8,
    fencing_token: u64,
};

/// Preserve the legacy decimal HEAD value while making every coordination
/// state produce a distinct object body. Legacy readers trim ASCII whitespace
/// before parsing the integer, while body-derived ETags now advance when only
/// the lease owner, token, expiry, or release state changes.
pub fn payloadAlloc(alloc: std.mem.Allocator, record: Record) ![]u8 {
    const head = try std.fmt.allocPrint(alloc, "{d}", .{record.head_version});
    defer alloc.free(head);
    const json = try std.json.Stringify.valueAlloc(alloc, record, .{});
    defer alloc.free(json);

    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(json, &digest, .{});
    const payload = try alloc.alloc(u8, head.len + payload_fingerprint_suffix_len);
    @memcpy(payload[0..head.len], head);
    payload[head.len] = '\n';
    var offset = head.len + 1;
    const whitespace = " \t\r\n";
    for (digest) |byte| {
        for (0..4) |part| {
            const shift: u3 = @intCast(part * 2);
            payload[offset] = whitespace[(byte >> shift) & 0b11];
            offset += 1;
        }
    }
    return payload;
}

pub fn hasPayloadFingerprint(body: []const u8) bool {
    const trimmed = std.mem.trim(u8, body, " \t\r\n");
    return body.len == trimmed.len + payload_fingerprint_suffix_len;
}

/// Keep the complete lease record in Content-Type for new readers. The body's
/// whitespace fingerprint mirrors this state so stores whose ETag is derived
/// only from body bytes still fence metadata-only coordination changes.
pub fn contentTypeAlloc(alloc: std.mem.Allocator, record: Record) ![]u8 {
    const json = try std.json.Stringify.valueAlloc(alloc, record, .{});
    defer alloc.free(json);
    const encoder = std.base64.url_safe_no_pad.Encoder;
    const encoded_len = encoder.calcSize(json.len);
    const content_type = try alloc.alloc(u8, metadata_content_type_prefix.len + encoded_len);
    @memcpy(content_type[0..metadata_content_type_prefix.len], metadata_content_type_prefix);
    _ = encoder.encode(content_type[metadata_content_type_prefix.len..], json);
    return content_type;
}

pub fn parseContentTypeAlloc(
    alloc: std.mem.Allocator,
    content_type: ?[]const u8,
) !?std.json.Parsed(Record) {
    const value = content_type orelse return null;
    if (!std.mem.startsWith(u8, value, metadata_content_type_prefix)) return null;
    const encoded = value[metadata_content_type_prefix.len..];
    const decoder = std.base64.url_safe_no_pad.Decoder;
    const decoded_len = decoder.calcSizeForSlice(encoded) catch return error.InvalidHeadCoordinationRecord;
    const decoded = try alloc.alloc(u8, decoded_len);
    defer alloc.free(decoded);
    decoder.decode(decoded, encoded) catch return error.InvalidHeadCoordinationRecord;
    return std.json.parseFromSlice(Record, alloc, decoded, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = false,
    }) catch return error.InvalidHeadCoordinationRecord;
}

pub fn valid(record: Record) bool {
    if (record.format_version != format_version) return false;
    if (record.owner_id) |owner_id| {
        return owner_id.len != 0 and record.fencing_token != 0;
    }
    return record.fencing_token == 0 and record.released;
}
