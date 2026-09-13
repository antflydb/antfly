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

pub const prefix = "enrich-v2/";

pub fn format(
    buf: []u8,
    head_version: u64,
    stage_id: u8,
    doc_index: usize,
    pipeline_version: u32,
) ![]const u8 {
    var key_buf: [20]u8 = undefined;
    return formatDocument(buf, head_version, stage_id, try std.fmt.bufPrint(&key_buf, "{d}", .{doc_index}), pipeline_version);
}

pub fn formatDocument(buf: []u8, head_version: u64, stage_id: u8, doc_id: []const u8, pipeline_version: u32) ![]const u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(doc_id, &digest, .{});
    return try std.fmt.bufPrint(buf, prefix ++ "{d}/{d}/{d}/{s}", .{
        head_version,
        stage_id,
        pipeline_version,
        std.fmt.bytesToHex(&digest, .lower),
    });
}

/// Returns the publication generation captured by an enrichment operation.
/// Non-enrichment identities return null. An identity that claims this
/// protocol prefix but is malformed fails closed so it cannot be applied as a
/// user mutation by a newer builder.
pub fn sourceHeadVersion(operation_id: ?[]const u8) !?u64 {
    const value = operation_id orelse return null;
    if (!std.mem.startsWith(u8, value, prefix)) {
        if (std.mem.startsWith(u8, value, "enrich-v")) return error.InvalidEnrichmentOperationId;
        return null;
    }

    var fields = std.mem.splitScalar(u8, value[prefix.len..], '/');
    const head = try parseField(u64, fields.next());
    _ = try parseField(u8, fields.next());
    _ = try parseField(u32, fields.next());
    const digest = fields.next() orelse return error.InvalidEnrichmentOperationId;
    if (digest.len != 64 or std.mem.indexOfNone(u8, digest, "0123456789abcdef") != null) return error.InvalidEnrichmentOperationId;
    if (fields.next() != null) return error.InvalidEnrichmentOperationId;
    return head;
}

fn parseField(comptime T: type, field: ?[]const u8) !T {
    const bytes = field orelse return error.InvalidEnrichmentOperationId;
    if (bytes.len == 0) return error.InvalidEnrichmentOperationId;
    return std.fmt.parseInt(T, bytes, 10) catch error.InvalidEnrichmentOperationId;
}

test "serverless enrichment operation identity exposes source head and rejects malformed claims" {
    var buf: [128]u8 = undefined;
    const value = try format(&buf, 42, 3, 7, 2);
    try std.testing.expect(std.mem.startsWith(u8, value, "enrich-v2/42/3/2/"));
    try std.testing.expectEqual(@as(?u64, 42), try sourceHeadVersion(value));
    try std.testing.expectEqual(@as(?u64, null), try sourceHeadVersion("request-1"));
    try std.testing.expectError(error.InvalidEnrichmentOperationId, sourceHeadVersion("enrich-v2/42/3/7"));
    try std.testing.expectError(error.InvalidEnrichmentOperationId, sourceHeadVersion("enrich-v1/42/3/7/2"));
    var first_buffer: [128]u8 = undefined;
    var second_buffer: [128]u8 = undefined;
    const first = try formatDocument(&first_buffer, 42, 3, "a", 2);
    const second = try formatDocument(&second_buffer, 42, 3, "b", 2);
    try std.testing.expect(!std.mem.eql(u8, first, second));
    try std.testing.expectEqualStrings(first, try formatDocument(&second_buffer, 42, 3, "a", 2));
}
