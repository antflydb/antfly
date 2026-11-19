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

//! Private bounded binary transport for pages of an immutable bootstrap pin.
const std = @import("std");
const pages = @import("history_page.zig");
const control = @import("history_consumer.zig");
const max_schema_bytes = @import("committed_history.zig").frame.max_schema_bytes;
const keys = @import("internal_keys.zig");
const A = std.mem.Allocator;
const header_size = 56;
const digest_size = 32;
pub const max_bytes = header_size + control.max_bootstrap_bytes + digest_size;

pub const OrderedRead = struct {
    request: pages.BootstrapRequest,
    minimum_applied_index: u64,
};

fn requestDigest(request: pages.BootstrapRequest) [32]u8 {
    var sha = std.crypto.hash.sha2.Sha256.init(.{});
    sha.update("antfly:history-bootstrap-page:v1");
    const ref = request.reference;
    inline for (.{ ref.namespace, ref.authority, ref.consumer, ref.definition, request.digest }) |field| sha.update(&field);
    for ([_]u64{ ref.epoch, ref.grant, request.now_seconds, request.after_rows, request.max_rows, request.max_bytes }) |value| {
        var encoded: [8]u8 = undefined;
        std.mem.writeInt(u64, &encoded, value, .little);
        sha.update(&encoded);
    }
    return sha.finalResult();
}

fn validatePosition(request: pages.BootstrapRequest, next: u32, count: u32, done: bool) !void {
    if (count > request.max_rows or next != request.after_rows + count or next > control.max_bootstrap_examined_keys or (count == 0 and !done))
        return error.HistoryCorrupt;
}

pub fn encode(a: A, page: pages.BootstrapPage, request: pages.BootstrapRequest) ![]u8 {
    try request.validate();
    if (page.rows.len > request.max_rows or page.schema.len > request.max_bytes) return error.HistoryTooLarge;
    if (page.schema.len > max_schema_bytes) return error.HistoryCorrupt;
    try validatePosition(request, page.next_row, @intCast(page.rows.len), page.done);
    var size = page.schema.len;
    for (page.rows) |row| {
        if (!keys.isStoredDocumentRowKey(row.key)) return error.HistoryCorrupt;
        const charged = 8 +| row.key.len +| row.value.len;
        if (charged > request.max_bytes - size) return error.HistoryTooLarge;
        size += charged;
    }

    const bytes = try a.alloc(u8, header_size + size + digest_size);
    @memset(bytes[0..header_size], 0);
    @memcpy(bytes[0..4], "HBP1");
    @memcpy(bytes[4..36], &requestDigest(request));
    std.mem.writeInt(u32, bytes[36..40], page.next_row, .little);
    std.mem.writeInt(u32, bytes[40..44], @intCast(page.rows.len), .little);
    std.mem.writeInt(u32, bytes[44..48], @intCast(page.schema.len), .little);
    bytes[48] = @intFromBool(page.done);
    @memcpy(bytes[header_size..][0..page.schema.len], page.schema);
    var offset = header_size + page.schema.len;
    for (page.rows) |row| {
        std.mem.writeInt(u32, bytes[offset..][0..4], @intCast(row.key.len), .little);
        std.mem.writeInt(u32, bytes[offset..][4..8], @intCast(row.value.len), .little);
        offset += 8;
        @memcpy(bytes[offset..][0..row.key.len], row.key);
        offset += row.key.len;
        @memcpy(bytes[offset..][0..row.value.len], row.value);
        offset += row.value.len;
    }
    std.crypto.hash.sha2.Sha256.hash(bytes[0..offset], bytes[offset..][0..digest_size], .{});
    return bytes;
}

pub fn decode(a: A, raw: []const u8, request: pages.BootstrapRequest) !pages.BootstrapPage {
    try request.validate();
    if (raw.len < header_size + digest_size or raw.len > header_size + request.max_bytes + digest_size or
        !std.mem.eql(u8, raw[0..4], "HBP1") or !std.mem.eql(u8, raw[4..36], &requestDigest(request)) or
        raw[48] > 1 or !std.mem.allEqual(u8, raw[49..header_size], 0)) return error.HistoryCorrupt;
    const payload_end = raw.len - digest_size;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw[0..payload_end], &digest, .{});
    if (!std.mem.eql(u8, &digest, raw[payload_end..])) return error.HistoryCorrupt;

    const next = std.mem.readInt(u32, raw[36..40], .little);
    const count = std.mem.readInt(u32, raw[40..44], .little);
    const schema_size = std.mem.readInt(u32, raw[44..48], .little);
    const done = raw[48] != 0;
    try validatePosition(request, next, count, done);
    const payload = raw[header_size..payload_end];
    if (schema_size > max_schema_bytes or schema_size > payload.len) return error.HistoryCorrupt;

    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const data = try owned.dupe(u8, payload);
    const rows = try owned.alloc(control.BootstrapImage.Row, count);
    var decoder: control.BootstrapImage.Rows = .{ .remaining = data[schema_size..], .count = count };
    for (rows) |*row| row.* = (try decoder.next()).?;
    _ = try decoder.next();
    return .{ .arena = arena, .schema = data[0..schema_size], .rows = rows, .next_row = next, .done = done };
}
