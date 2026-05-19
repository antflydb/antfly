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
const format = @import("format.zig");

pub fn countEntriesForTest(
    allocator: std.mem.Allocator,
    txn: anytype,
    dbi: anytype,
    comptime RawEntryType: type,
    collect_db_entries: anytype,
) !usize {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var raw_entries: std.ArrayListUnmanaged(RawEntryType) = .empty;
    try collect_db_entries(txn, try txn.db(dbi), arena.allocator(), &raw_entries);
    return raw_entries.items.len;
}

pub fn initCrashTestFile(
    dir: anytype,
    sub_path: []const u8,
    page_size: usize,
    map_size: usize,
    empty_main_db: format.Db,
    write_meta_page: anytype,
) !void {
    var bytes: [4096 * 2]u8 = undefined;
    std.debug.assert(page_size == 4096);

    const empty_free_db = format.Db{
        .md_pad = @intCast(page_size),
        .md_flags = format.DbFlags.integer_key,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    write_meta_page(bytes[0..page_size], 0, empty_free_db, empty_main_db, map_size, 1, 0);
    write_meta_page(bytes[page_size .. page_size * 2], 1, empty_free_db, empty_main_db, map_size, 1, 1);
    try dir.writeFile(std.testing.io, .{ .sub_path = sub_path, .data = &bytes });
}

pub fn expectCrashPhaseReopenSnapshot(
    comptime EnvType: type,
    file_path: []const u8,
    opts: anytype,
    phase: anytype,
    expect_beta: ?bool,
    begin_txn: anytype,
    publish_commit_phase: anytype,
) anyerror!void {
    {
        var create_opts = opts;
        create_opts.no_subdir = true;
        create_opts.read_only = false;
        var env = try EnvType.open(file_path, create_opts);
        defer env.close();

        var txn = try begin_txn(&env, false);
        errdefer txn.abort();

        const main = try txn.openDb(null, .{ .create = true });
        try txn.put(main, "alpha", "one", .{});
        try txn.commit();
    }

    {
        var write_opts = opts;
        write_opts.no_subdir = true;
        write_opts.read_only = false;
        var env = try EnvType.open(file_path, write_opts);
        defer env.close();

        var txn = try begin_txn(&env, false);
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        try txn.put(main, "beta", "two", .{});
        try publish_commit_phase(&txn, phase);
    }

    {
        var reopen_opts = opts;
        reopen_opts.no_subdir = true;
        reopen_opts.read_only = false;
        var env = try EnvType.open(file_path, reopen_opts);
        defer env.close();

        var txn = try begin_txn(&env, false);
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        try std.testing.expectEqualStrings("one", try txn.get(main, "alpha"));
        if (expect_beta) |beta_present| {
            if (beta_present) {
                try std.testing.expectEqualStrings("two", try txn.get(main, "beta"));
            } else {
                try std.testing.expectError(error.NotFound, txn.get(main, "beta"));
            }
        } else {
            const beta = txn.get(main, "beta");
            if (beta) |value| {
                try std.testing.expectEqualStrings("two", value);
            } else |err| switch (err) {
                error.NotFound => {},
                else => return err,
            }
        }
    }
}
