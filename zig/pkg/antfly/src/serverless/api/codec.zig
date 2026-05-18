// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const Allocator = std.mem.Allocator;
const api_types = @import("types.zig");

pub fn encodeMutationAlloc(alloc: Allocator, mutation: api_types.DocumentMutation) ![]u8 {
    const body_len: usize = if (mutation.body) |body| body.len else 0;
    const buf = try alloc.alloc(u8, 1 + 4 + 4 + mutation.doc_id.len + body_len);
    var pos: usize = 0;
    buf[pos] = @intFromEnum(mutation.kind);
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
    const kind: api_types.MutationKind = @enumFromInt(payload[pos]);
    pos += 1;
    const doc_id_len = std.mem.readInt(u32, payload[pos..][0..4], .little);
    pos += 4;
    const body_len = std.mem.readInt(u32, payload[pos..][0..4], .little);
    pos += 4;
    if (pos + doc_id_len + body_len > payload.len) return error.InvalidMutationPayload;

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
