// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Cumulative accepted output coverage, independent of the retry-status record.
const std = @import("std");
pub const Values = @import("inference_chunker").outputs.CoverageValues;
pub const Counts = @import("inference_chunker").outputs.CoverageCounts;
pub const encoded_len = 132;
pub fn key(a: std.mem.Allocator, scope: []const u8) ![]u8 {
    return std.fmt.allocPrint(a, "\x00\x00__metadata__:enrichment_output_counts:{s}", .{scope});
}
pub fn encode(counts: Counts) [encoded_len]u8 {
    var raw: [encoded_len]u8 = undefined;
    @memcpy(raw[0..4], "AMO1");
    var position: usize = 4;
    inline for (.{ "embedding", "transcribe", "classify" }) |kind| inline for (.{ "complete", "failed", "terminal", "degraded" }) |name| {
        std.mem.writeInt(u64, raw[position..][0..8], @field(@field(counts, kind), name), .little);
        position += 8;
    };
    std.crypto.hash.sha2.Sha256.hash(raw[0..100], raw[100..132], .{});
    return raw;
}
pub fn decode(raw: []const u8) !Counts {
    if (raw.len != encoded_len or !std.mem.eql(u8, raw[0..4], "AMO1")) return error.InvalidEnrichmentState;
    var checksum: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw[0..100], &checksum, .{});
    if (!std.mem.eql(u8, &checksum, raw[100..132])) return error.InvalidEnrichmentState;
    var counts = Counts{};
    var position: usize = 4;
    inline for (.{ "embedding", "transcribe", "classify" }) |kind| inline for (.{ "complete", "failed", "terminal", "degraded" }) |name| {
        @field(@field(counts, kind), name) = std.mem.readInt(u64, raw[position..][0..8], .little);
        position += 8;
    };
    return counts;
}
test "model aware media output counters are checksummed saturating and independent by kind" {
    const counts = (Counts{ .embedding = .{ .complete = std.math.maxInt(u64) }, .transcribe = .{ .failed = 2 } }).add(.{ .embedding = .{ .complete = 1 }, .transcribe = .{ .terminal = 1 } });
    var raw = encode(counts);
    try std.testing.expectEqualDeep(counts, try decode(&raw));
    raw[10] ^= 1;
    try std.testing.expectError(error.InvalidEnrichmentState, decode(&raw));
}
