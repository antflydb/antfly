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
const change_journal_mod = @import("../derived/change_journal.zig");
const replay_source_mod = @import("../derived/replay_source.zig");
const platform_time = @import("../../../platform/time.zig");
pub const PendingDocumentGroup = replay_source_mod.PendingDocumentGroup;

pub fn collectPendingDocumentGroups(
    alloc: Allocator,
    replay_source: replay_source_mod.Source,
    from_sequence: u64,
) ![]PendingDocumentGroup {
    return try replay_source.collectEnrichmentDocumentGroups(alloc, from_sequence);
}

pub fn freePendingDocumentGroups(alloc: Allocator, groups: []PendingDocumentGroup) void {
    replay_source_mod.freePendingDocumentGroups(alloc, groups);
}

test "enrichment worker collects changed documents from thin change journal" {
    const alloc = std.testing.allocator;

    var temp_path_nonce: u64 = 0;
    var path_buf: [256]u8 = undefined;
    const path = blk: {
        const base = "/tmp/antfly-enrichment-worker-journal-doc-test-";
        const ts = platform_time.monotonicNs();
        const nonce = @atomicRmw(u64, &temp_path_nonce, .Add, 1, .monotonic);
        const path = std.fmt.bufPrint(&path_buf, "{s}{d}-{d}\x00", .{ base, ts, nonce }) catch unreachable;
        break :blk @as([*:0]const u8, @ptrCast(path.ptr));
    };
    defer {
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
    }

    var journal = try change_journal_mod.Journal.open(path, .{});
    defer journal.close();

    const first_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{ "doc:a", "doc:b" },
        .target_hints = &.{ .enrichment },
    });
    defer alloc.free(first_payload);
    _ = try journal.appendOpaque(first_payload);

    const second_payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 2,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{ .enrichment, .dense_vector },
    });
    defer alloc.free(second_payload);
    _ = try journal.appendOpaque(second_payload);

    const groups = try collectPendingDocumentGroups(alloc, replay_source_mod.Source.fromJournal(&journal), 0);
    defer freePendingDocumentGroups(alloc, groups);

    try std.testing.expectEqual(@as(usize, 2), groups.len);
    try std.testing.expectEqual(@as(u64, 1), groups[0].sequence);
    try std.testing.expectEqualStrings("doc:b", groups[0].doc_key);
    try std.testing.expectEqual(@as(u64, 2), groups[1].sequence);
    try std.testing.expectEqualStrings("doc:a", groups[1].doc_key);
}
