// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

/// Legacy tuple contributors use owner; explicit relationships use
/// owner_document (or implicitly source). Never feed an explicit identity
/// into the tuple-only membership directory.
pub fn validate(edge_id: []const u8, owner_document: []const u8, owner: []const u8) !void {
    if ((owner_document.len > 0 and edge_id.len == 0) or
        (owner.len > 0 and edge_id.len > 0)) return error.InvalidGraphEdges;
}
