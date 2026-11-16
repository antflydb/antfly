// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
/// Acknowledges the acquired model generation for JSON and numeric responses.
pub const response_header = "X-Antfly-Model-Revision";

pub fn parse(raw: []const u8) ![32]u8 {
    if (raw.len != 64) return error.InvalidEmbeddingRevision;
    for (raw) |c| if (!(c >= '0' and c <= '9') and !(c >= 'a' and c <= 'f')) return error.InvalidEmbeddingRevision;
    var revision: [32]u8 = undefined;
    _ = std.fmt.hexToBytes(&revision, raw) catch return error.InvalidEmbeddingRevision;
    return revision;
}

pub fn require(expected: ?[32]u8, actual: ?[32]u8) !void {
    if (expected) |revision| {
        const loaded = actual orelse return error.InferenceCapabilitiesStale;
        if (!std.mem.eql(u8, &revision, &loaded)) return error.InferenceCapabilitiesStale;
    }
}
