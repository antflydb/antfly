// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Ordered immutable incarnation proofs. Fixed records support bounded random
//! lookup during term-major merging and sequential publication without ID maps.
const std = @import("std");
const spill = @import("../spill_sort.zig");
const Run = @import("../postings_run.zig").Run;
const A = std.mem.Allocator;
pub const Entry = struct { doc_num: u32, epoch: u64, live: bool };
pub const Proofs = struct {
    run: *Run,
    pub fn init(a: A, options: spill.Options) !Proofs {
        return .{ .run = try Run.createWithResources(a, options.io, options.directory, options.resource_manager) };
    }
    pub fn deinit(self: *Proofs) void {
        self.run.deinit();
        self.* = undefined;
    }
    pub fn count(self: Proofs) usize {
        return self.run.len() / 13;
    }
    pub fn append(self: *Proofs, entry: Entry) !void {
        var bytes: [13]u8 = undefined;
        std.mem.writeInt(u32, bytes[0..4], entry.doc_num, .little);
        std.mem.writeInt(u64, bytes[4..12], entry.epoch, .little);
        bytes[12] = @intFromBool(entry.live);
        try self.run.appendSlice(&bytes);
    }
    pub fn seal(self: *Proofs) !void {
        try self.run.seal(0);
    }
    pub fn at(self: Proofs, index: usize) !Entry {
        if (index >= self.count()) return error.InvalidSparseSegment;
        var bytes: [13]u8 = undefined;
        try (try self.run.sealedView()).readInto(index * 13, &bytes);
        if (bytes[12] > 1) return error.InvalidSparseSegment;
        return .{ .doc_num = std.mem.readInt(u32, bytes[0..4], .little), .epoch = std.mem.readInt(u64, bytes[4..12], .little), .live = bytes[12] != 0 };
    }
    pub fn find(self: Proofs, doc: u32) !?Entry {
        var position: usize = 0;
        return self.findFrom(doc, &position);
    }
    /// Each posting term and document-map stream owns its ordinal hint. Gallop
    /// from that hint, keeping sequential merges on the bounded run page cache
    /// instead of repeating an archive-wide binary search for every posting.
    pub fn findFrom(self: Proofs, doc: u32, position: *usize) !?Entry {
        const length = self.count();
        var lo = @min(position.*, length);
        var hi = length;
        if (lo == length) {
            if (length == 0 or (try self.at(length - 1)).doc_num < doc) return null;
            lo = 0;
        } else if ((try self.at(lo)).doc_num > doc) {
            hi = lo;
            lo = 0;
        } else {
            var step: usize = 1;
            while (lo < hi and (try self.at(lo)).doc_num < doc) {
                const next = @min(hi, lo +| step);
                if (next == hi or (try self.at(next)).doc_num >= doc) {
                    hi = next;
                    lo += 1;
                    break;
                }
                lo = next;
                step *|= 2;
            }
        }
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if ((try self.at(mid)).doc_num < doc) lo = mid + 1 else hi = mid;
        }
        position.* = lo;
        if (lo == length) return null;
        const entry = try self.at(lo);
        return if (entry.doc_num == doc) entry else null;
    }
};

test "sparse disk proofs preserve ordered lookup and bounded records" {
    var proofs = try Proofs.init(std.testing.allocator, .{ .io = std.testing.io, .directory = "/tmp" });
    defer proofs.deinit();
    for (0..20000) |i| try proofs.append(.{ .doc_num = @intCast(i * 3), .epoch = i + 1, .live = i % 2 == 0 });
    try proofs.seal();
    try std.testing.expectEqual(@as(usize, 20000), proofs.count());
    try std.testing.expectEqual(@as(u64, 20000), (try proofs.find(59997)).?.epoch);
    try std.testing.expect((try proofs.find(59998)) == null);
    try std.testing.expect(!(try proofs.find(3)).?.live);
    const before = proofs.run.read_calls;
    var position: usize = 0;
    for (0..20000) |i| try std.testing.expectEqual(@as(u64, i + 1), (try proofs.findFrom(@intCast(i * 3), &position)).?.epoch);
    try std.testing.expect(proofs.run.read_calls - before < 64);
    try std.testing.expect((try proofs.findFrom(59998, &position)) == null);
    try std.testing.expectEqual(@as(u64, 2), (try proofs.findFrom(3, &position)).?.epoch);
    try std.testing.expect((try proofs.findFrom(4, &position)) == null);
    try std.testing.expectEqual(@as(u64, 3), (try proofs.findFrom(6, &position)).?.epoch);
}
