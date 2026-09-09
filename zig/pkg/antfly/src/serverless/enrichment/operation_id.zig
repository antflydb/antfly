// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

const std = @import("std");

pub const prefix = "enrich-v1/";

pub fn format(
    buf: []u8,
    head_version: u64,
    stage_id: u8,
    doc_index: usize,
    pipeline_version: u32,
) ![]const u8 {
    return try std.fmt.bufPrint(buf, prefix ++ "{d}/{d}/{d}/{d}", .{
        head_version,
        stage_id,
        doc_index,
        pipeline_version,
    });
}

/// Returns the publication generation captured by an enrichment operation.
/// Non-enrichment identities return null. An identity that claims this
/// protocol prefix but is malformed fails closed so it cannot be applied as a
/// user mutation by a newer builder.
pub fn sourceHeadVersion(operation_id: ?[]const u8) !?u64 {
    const value = operation_id orelse return null;
    if (!std.mem.startsWith(u8, value, prefix)) return null;

    var fields = std.mem.splitScalar(u8, value[prefix.len..], '/');
    const head = try parseField(u64, fields.next());
    _ = try parseField(u8, fields.next());
    _ = try parseField(usize, fields.next());
    _ = try parseField(u32, fields.next());
    if (fields.next() != null) return error.InvalidEnrichmentOperationId;
    return head;
}

fn parseField(comptime T: type, field: ?[]const u8) !T {
    const bytes = field orelse return error.InvalidEnrichmentOperationId;
    if (bytes.len == 0) return error.InvalidEnrichmentOperationId;
    return std.fmt.parseInt(T, bytes, 10) catch error.InvalidEnrichmentOperationId;
}

test "enrichment operation identity exposes source head and rejects malformed claims" {
    var buf: [128]u8 = undefined;
    const value = try format(&buf, 42, 3, 7, 2);
    try std.testing.expectEqualStrings("enrich-v1/42/3/7/2", value);
    try std.testing.expectEqual(@as(?u64, 42), try sourceHeadVersion(value));
    try std.testing.expectEqual(@as(?u64, null), try sourceHeadVersion("request-1"));
    try std.testing.expectError(error.InvalidEnrichmentOperationId, sourceHeadVersion("enrich-v1/42/3/7"));
}
