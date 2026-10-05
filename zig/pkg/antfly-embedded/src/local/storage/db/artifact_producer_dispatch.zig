// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Bounded, borrowed pages of an immutable catalog's producer requests.
//! The caller retains its catalog pin through atomic journal admission.
const std = @import("std");
const types = @import("enrichment/enrichment_types.zig");
const obligations = @import("artifact_producer_obligations.zig");
pub const max_items = 128;
pub const target_bytes = 64 * 1024;
pub const Buffer = [max_items]types.GeneratedEnrichmentRef;
pub const Page = struct { items: []const types.GeneratedEnrichmentRef, progress: obligations.Dispatch };

pub fn prepare(templates: []const types.GeneratedEnrichmentRequest, first: u32, document: []const u8, buffer: *Buffer) !Page {
    if (templates.len > std.math.maxInt(u32) or first >= templates.len) return error.ArtifactCatalogCorrupt;
    var bytes: usize = 0;
    var count: usize = 0;
    for (templates[first..]) |template| {
        // Worst-case JSON escaping plus fixed field names/enum framing. Charge
        // the repeated document key for every request, not just once per page.
        const strings = document.len +| template.index_name.len +| template.artifact_name.len +| template.embedding_name.len;
        const cost = 512 +| strings *| 6;
        if (count != 0 and (count == max_items or bytes +| cost > target_bytes)) break;
        if (cost > @import("artifact_publication.zig").max_payload_bytes) return error.ResourceBudgetExceeded;
        buffer[count] = .{ .kind = template.kind, .index_name = template.index_name, .artifact_name = template.artifact_name, .embedding_name = template.embedding_name, .doc_key = document };
        bytes += cost;
        count += 1;
    }
    const next: u32 = first + @as(u32, @intCast(count));
    return .{ .items = buffer[0..count], .progress = .{ .next_template = next, .complete = next == templates.len } };
}

test "ordered artifact inventory producer dispatch pages bound items and escaped key bytes" {
    var templates: [300]types.GeneratedEnrichmentRequest = @splat(.{ .kind = .asset, .index_name = "producer", .doc_key = "", .source_field = "body" });
    var buffer: Buffer = undefined;
    var next: u32 = 0;
    var pages: usize = 0;
    while (true) {
        const page = try prepare(&templates, next, "doc", &buffer);
        try std.testing.expect(page.items.len > 0 and page.items.len <= max_items);
        try std.testing.expect(page.progress.next_template > next);
        next = page.progress.next_template;
        pages += 1;
        if (page.progress.complete) break;
    }
    try std.testing.expectEqual(@as(u32, templates.len), next);
    try std.testing.expect(pages >= 3);
    const large: [target_bytes]u8 = @splat(0xff);
    const single = try prepare(&templates, 0, &large, &buffer);
    try std.testing.expectEqual(@as(usize, 1), single.items.len);
    try std.testing.expect(!single.progress.complete);
    try std.testing.expectError(error.ArtifactCatalogCorrupt, prepare(&templates, templates.len, "doc", &buffer));
}
