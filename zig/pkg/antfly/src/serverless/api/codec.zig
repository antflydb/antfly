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
const Allocator = std.mem.Allocator;
const api_types = @import("types.zig");

pub fn encodeMutationAlloc(alloc: Allocator, mutation: api_types.DocumentMutation) ![]u8 {
    const body_len: usize = if (mutation.body) |body| body.len else 0;
    const buf = try alloc.alloc(u8, 1 + 4 + 4 + mutation.doc_id.len + body_len);
    var pos: usize = 0;
    buf[pos] = @backingInt(mutation.kind);
    pos += 1;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(mutation.doc_id.len), .little);
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(body_len), .little);
    pos += 4;
    @memcpy(buf[pos..][0..mutation.doc_id.len], mutation.doc_id);
    pos += mutation.doc_id.len;
    if (mutation.body) |body| {
        @memcpy(buf[pos..][0..body.len], body);
    }
    return buf;
}

pub fn decodeMutationAlloc(alloc: Allocator, payload: []const u8) !api_types.DocumentMutation {
    if (payload.len < 1 + 4 + 4) return error.InvalidMutationPayload;
    var pos: usize = 0;
    const kind = std.enums.fromInt(api_types.MutationKind, payload[pos]) orelse return error.InvalidMutationPayload;
    pos += 1;
    const doc_id_len = std.mem.readInt(u32, payload[pos..][0..4], .little);
    pos += 4;
    const body_len = std.mem.readInt(u32, payload[pos..][0..4], .little);
    pos += 4;
    if (doc_id_len > payload.len - pos or body_len != payload.len - pos - doc_id_len) return error.InvalidMutationPayload;

    if (kind == .delete and body_len != 0) return error.InvalidMutationPayload;
    const doc_id = try alloc.dupe(u8, payload[pos .. pos + doc_id_len]);
    pos += doc_id_len;
    errdefer alloc.free(doc_id);

    const body = if (body_len == 0)
        null
    else
        try alloc.dupe(u8, payload[pos .. pos + body_len]);

    return .{
        .kind = kind,
        .doc_id = doc_id,
        .body = body,
    };
}

test "mutation codec round-trips upsert and delete" {
    const alloc = std.testing.allocator;

    const upsert = api_types.DocumentMutation{
        .kind = .upsert,
        .doc_id = "doc-a",
        .body = "hello",
    };
    const encoded_upsert = try encodeMutationAlloc(alloc, upsert);
    defer alloc.free(encoded_upsert);
    var decoded_upsert = try decodeMutationAlloc(alloc, encoded_upsert);
    defer decoded_upsert.deinit(alloc);
    try std.testing.expectEqual(api_types.MutationKind.upsert, decoded_upsert.kind);
    try std.testing.expectEqualStrings("doc-a", decoded_upsert.doc_id);
    try std.testing.expectEqualStrings("hello", decoded_upsert.body.?);

    const delete = api_types.DocumentMutation{
        .kind = .delete,
        .doc_id = "doc-b",
        .body = null,
    };
    const encoded_delete = try encodeMutationAlloc(alloc, delete);
    defer alloc.free(encoded_delete);
    var decoded_delete = try decodeMutationAlloc(alloc, encoded_delete);
    defer decoded_delete.deinit(alloc);
    try std.testing.expectEqual(api_types.MutationKind.delete, decoded_delete.kind);
    try std.testing.expectEqualStrings("doc-b", decoded_delete.doc_id);
    try std.testing.expectEqual(@as(?[]const u8, null), decoded_delete.body);
}

test "serverless mutation decoder rejects unknown tags, trailing bytes and delete bodies" {
    const a = std.testing.allocator;
    const unknown = [_]u8{ 9, 0, 0, 0, 0, 0, 0, 0, 0 };
    try std.testing.expectError(error.InvalidMutationPayload, decodeMutationAlloc(a, &unknown));
    const malformed = [_]u8{ 1, 255, 255, 255, 255, 255, 255, 255, 255 };
    try std.testing.expectError(error.InvalidMutationPayload, decodeMutationAlloc(a, &malformed));
    const delete = try encodeMutationAlloc(a, .{ .kind = .delete, .doc_id = "a", .body = "invalid" });
    defer a.free(delete);
    try std.testing.expectError(error.InvalidMutationPayload, decodeMutationAlloc(a, delete));
    const empty = [_]u8{ 1, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    try std.testing.expectError(error.InvalidMutationPayload, decodeMutationAlloc(a, &empty));
}
