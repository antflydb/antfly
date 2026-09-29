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
const env_mod = @import("env.zig");
const format = @import("format.zig");
const page = @import("page.zig");
const node = @import("node.zig");
const mutate_leaf = @import("mutate_leaf.zig");
const rebalance_branch = @import("rebalance_branch.zig");
const read_support = @import("read_support.zig");
const materialize_support = @import("materialize_support.zig");
const commit_support = @import("commit_support.zig");
const txn_support = @import("txn_support.zig");
const txn_mod = @import("txn.zig");

pub const Error = txn_mod.Error || env_mod.Error || page.Error || node.Error || std.mem.Allocator.Error || error{
    NotFound,
    Corrupted,
    Incompatible,
    MapFull,
};

const ImageBuilder = materialize_support.ImageBuilder;
const SerializedLeafEntry = materialize_support.SerializedLeafEntry;
const BranchPageEntry = materialize_support.BranchPageEntry;
const PageImage = commit_support.PageImage;
const SerializedPage = commit_support.SerializedPage;
const SerializedWriteSpan = commit_support.SerializedWriteSpan;

pub const SplitBuild = struct {
    arena: std.heap.ArenaAllocator,
    page_size: usize,
    total_size: usize,
    main_db: format.Db,
    page_images: []const PageImage,
    serialized_pages: []const SerializedPage,
    serialized_spans: []const SerializedWriteSpan,

    pub fn deinit(self: *SplitBuild) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

const SplitChildRef = struct {
    first_key: []const u8,
    pgno: format.Pgno,
    depth: u16,
    leaf_pages: format.Pgno,
    branch_pages: format.Pgno,
    overflow_pages: format.Pgno,
    entry_count: usize,
};

pub fn buildRightSplit(allocator: std.mem.Allocator, txn: anytype, db: format.Db, split_key: []const u8) Error!SplitBuild {
    if ((db.md_flags & format.DbFlags.dup_sort) != 0) return error.Incompatible;

    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();

    const page_size = try txn.pageSize();
    var builder = ImageBuilder{
        .allocator = arena.allocator(),
        .page_size = page_size,
    };

    const root = try buildRightSplitSubtree(arena.allocator(), &builder, txn, db.md_root, db.md_flags, "", null, split_key);
    const main_db: format.Db = if (root) |child|
        format.Db{
            .md_pad = db.md_pad,
            .md_flags = db.md_flags,
            .md_depth = child.depth,
            .md_branch_pages = child.branch_pages,
            .md_leaf_pages = child.leaf_pages,
            .md_overflow_pages = child.overflow_pages,
            .md_entries = child.entry_count,
            .md_root = child.pgno,
        }
    else
        .{
            .md_pad = db.md_pad,
            .md_flags = db.md_flags,
            .md_depth = 0,
            .md_branch_pages = 0,
            .md_leaf_pages = 0,
            .md_overflow_pages = 0,
            .md_entries = 0,
            .md_root = format.invalid_pgno,
        };

    const serialized_pages = try commit_support.serializePageImages(arena.allocator(), page_size, .{}, builder.pages.items);
    const serialized_spans = try commit_support.coalesceSerializedPages(arena.allocator(), serialized_pages);
    const total_size = @max(builder.next_pgno, format.num_metas) * page_size;

    return .{
        .arena = arena,
        .page_size = page_size,
        .total_size = total_size,
        .main_db = main_db,
        .page_images = builder.pages.items,
        .serialized_pages = serialized_pages,
        .serialized_spans = serialized_spans,
    };
}

pub fn buildLeftSplit(allocator: std.mem.Allocator, txn: anytype, db: format.Db, split_key: []const u8) Error!SplitBuild {
    if ((db.md_flags & format.DbFlags.dup_sort) != 0) return error.Incompatible;

    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();

    const page_size = try txn.pageSize();
    var builder = ImageBuilder{
        .allocator = arena.allocator(),
        .page_size = page_size,
    };

    const root = try buildLeftSplitSubtree(arena.allocator(), &builder, txn, db.md_root, db.md_flags, "", null, split_key);
    const main_db: format.Db = if (root) |child|
        format.Db{
            .md_pad = db.md_pad,
            .md_flags = db.md_flags,
            .md_depth = child.depth,
            .md_branch_pages = child.branch_pages,
            .md_leaf_pages = child.leaf_pages,
            .md_overflow_pages = child.overflow_pages,
            .md_entries = child.entry_count,
            .md_root = child.pgno,
        }
    else
        .{
            .md_pad = db.md_pad,
            .md_flags = db.md_flags,
            .md_depth = 0,
            .md_branch_pages = 0,
            .md_leaf_pages = 0,
            .md_overflow_pages = 0,
            .md_entries = 0,
            .md_root = format.invalid_pgno,
        };

    const serialized_pages = try commit_support.serializePageImages(arena.allocator(), page_size, .{}, builder.pages.items);
    const serialized_spans = try commit_support.coalesceSerializedPages(arena.allocator(), serialized_pages);
    const total_size = @max(builder.next_pgno, format.num_metas) * page_size;

    return .{
        .arena = arena,
        .page_size = page_size,
        .total_size = total_size,
        .main_db = main_db,
        .page_images = builder.pages.items,
        .serialized_pages = serialized_pages,
        .serialized_spans = serialized_spans,
    };
}

pub fn writeRightSplitDataFile(allocator: std.mem.Allocator, txn: anytype, db: format.Db, split_key: []const u8, file_path: []const u8) Error!void {
    var build = try buildRightSplit(allocator, txn, db, split_key);
    defer build.deinit();

    const total_size = build.total_size;
    var bytes = try allocator.alloc(u8, total_size);
    defer allocator.free(bytes);
    @memset(bytes, 0);

    for (build.serialized_spans) |span| {
        @memcpy(bytes[span.offset .. span.offset + span.bytes.len], span.bytes);
    }

    const empty_free_db = format.Db{
        .md_pad = @intCast(build.page_size),
        .md_flags = format.DbFlags.integer_key,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const last_pg: format.Pgno = @intCast(@max(builderLastPgno(build.total_size, build.page_size), format.num_metas - 1));
    txn_support.writeMetaPage(bytes[0..build.page_size], 0, empty_free_db, build.main_db, total_size * 8, last_pg, 1);
    txn_support.writeMetaPage(bytes[build.page_size .. build.page_size * 2], 1, empty_free_db, build.main_db, total_size * 8, last_pg, 2);

    const fd = try std.posix.openat(std.posix.AT.FDCWD, file_path, .{
        .ACCMODE = .WRONLY,
        .CREAT = true,
        .TRUNC = true,
    }, 0o644);
    defer _ = std.posix.system.close(fd);
    var written: usize = 0;
    while (written < bytes.len) {
        const rc = std.posix.system.write(fd, bytes[written..].ptr, bytes.len - written);
        if (rc <= 0) return error.Unexpected;
        written += @intCast(rc);
    }
}

pub fn writeLeftSplitDataFile(allocator: std.mem.Allocator, txn: anytype, db: format.Db, split_key: []const u8, file_path: []const u8) Error!void {
    var build = try buildLeftSplit(allocator, txn, db, split_key);
    defer build.deinit();

    const total_size = build.total_size;
    var bytes = try allocator.alloc(u8, total_size);
    defer allocator.free(bytes);
    @memset(bytes, 0);

    for (build.serialized_spans) |span| {
        @memcpy(bytes[span.offset .. span.offset + span.bytes.len], span.bytes);
    }

    const empty_free_db = format.Db{
        .md_pad = @intCast(build.page_size),
        .md_flags = format.DbFlags.integer_key,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const last_pg: format.Pgno = @intCast(@max(builderLastPgno(build.total_size, build.page_size), format.num_metas - 1));
    txn_support.writeMetaPage(bytes[0..build.page_size], 0, empty_free_db, build.main_db, total_size * 8, last_pg, 1);
    txn_support.writeMetaPage(bytes[build.page_size .. build.page_size * 2], 1, empty_free_db, build.main_db, total_size * 8, last_pg, 2);

    const fd = try std.posix.openat(std.posix.AT.FDCWD, file_path, .{
        .ACCMODE = .WRONLY,
        .CREAT = true,
        .TRUNC = true,
    }, 0o644);
    defer _ = std.posix.system.close(fd);
    var written: usize = 0;
    while (written < bytes.len) {
        const rc = std.posix.system.write(fd, bytes[written..].ptr, bytes.len - written);
        if (rc <= 0) return error.Unexpected;
        written += @intCast(rc);
    }
}

fn buildRightSplitSubtree(
    allocator: std.mem.Allocator,
    builder: *ImageBuilder,
    txn: anytype,
    pgno: format.Pgno,
    db_flags: u16,
    lower_bound: []const u8,
    upper_bound: ?[]const u8,
    split_key: []const u8,
) Error!?SplitChildRef {
    if (pgno == format.invalid_pgno) return null;
    if (lowerBoundIsRight(db_flags, lower_bound, split_key) and upper_bound == null) {
        return cloneFullSubtree(allocator, builder, txn, pgno);
    }

    const current_page = try txn.pageView(pgno);
    switch (current_page.kind()) {
        .leaf => return splitLeafPage(allocator, builder, txn, current_page, db_flags, split_key),
        .leaf2 => return error.Incompatible,
        .branch => {
            const source_entries = try rebalance_branch.cloneEntries(allocator, current_page);
            var children = std.ArrayListUnmanaged(SplitChildRef).empty;
            for (source_entries, 0..) |entry, i| {
                const child_lower = if (i == 0) lower_bound else entry.key;
                const child_upper = if (i + 1 < source_entries.len) source_entries[i + 1].key else upper_bound;
                if (upperBoundIsLeft(db_flags, child_upper, split_key)) continue;

                const child = if (lowerBoundIsRight(db_flags, child_lower, split_key))
                    try cloneFullSubtree(allocator, builder, txn, entry.child_pgno)
                else
                    try buildRightSplitSubtree(allocator, builder, txn, entry.child_pgno, db_flags, child_lower, child_upper, split_key);
                if (child) |ref| try children.append(allocator, ref);
            }

            if (children.items.len == 0) return null;
            if (children.items.len == 1) return children.items[0];

            const new_pgno = builder.allocPgno(1);
            const branch_entries = try allocator.alloc(BranchPageEntry, children.items.len);
            var leaf_pages: format.Pgno = 0;
            var branch_pages: format.Pgno = 1;
            var overflow_pages: format.Pgno = 0;
            var entry_count: usize = 0;
            var depth: u16 = 0;
            for (children.items, 0..) |child, i| {
                branch_entries[i] = .{
                    .key = if (i == 0) "" else child.first_key,
                    .child_pgno = child.pgno,
                };
                leaf_pages += child.leaf_pages;
                branch_pages += child.branch_pages;
                overflow_pages += child.overflow_pages;
                entry_count += child.entry_count;
                if (child.depth > depth) depth = child.depth;
            }
            try builder.pages.append(builder.allocator, .{
                .branch = .{
                    .pgno = new_pgno,
                    .entries = branch_entries,
                },
            });
            return .{
                .first_key = children.items[0].first_key,
                .pgno = new_pgno,
                .depth = depth + 1,
                .leaf_pages = leaf_pages,
                .branch_pages = branch_pages,
                .overflow_pages = overflow_pages,
                .entry_count = entry_count,
            };
        },
        else => return error.Corrupted,
    }
}

fn buildLeftSplitSubtree(
    allocator: std.mem.Allocator,
    builder: *ImageBuilder,
    txn: anytype,
    pgno: format.Pgno,
    db_flags: u16,
    lower_bound: []const u8,
    upper_bound: ?[]const u8,
    split_key: []const u8,
) Error!?SplitChildRef {
    if (pgno == format.invalid_pgno) return null;
    if (upperBoundIsLeft(db_flags, upper_bound, split_key)) {
        return cloneFullSubtree(allocator, builder, txn, pgno);
    }

    const current_page = try txn.pageView(pgno);
    switch (current_page.kind()) {
        .leaf => return splitLeafPageLeft(allocator, builder, txn, current_page, db_flags, split_key),
        .leaf2 => return error.Incompatible,
        .branch => {
            const source_entries = try rebalance_branch.cloneEntries(allocator, current_page);
            var children = std.ArrayListUnmanaged(SplitChildRef).empty;
            for (source_entries, 0..) |entry, i| {
                const child_lower = if (i == 0) lower_bound else entry.key;
                const child_upper = if (i + 1 < source_entries.len) source_entries[i + 1].key else upper_bound;
                if (lowerBoundIsRight(db_flags, child_lower, split_key)) break;

                const child = if (upperBoundIsLeft(db_flags, child_upper, split_key))
                    try cloneFullSubtree(allocator, builder, txn, entry.child_pgno)
                else
                    try buildLeftSplitSubtree(allocator, builder, txn, entry.child_pgno, db_flags, child_lower, child_upper, split_key);
                if (child) |ref| try children.append(allocator, ref);
            }

            if (children.items.len == 0) return null;
            if (children.items.len == 1) return children.items[0];

            const new_pgno = builder.allocPgno(1);
            const branch_entries = try allocator.alloc(BranchPageEntry, children.items.len);
            var leaf_pages: format.Pgno = 0;
            var branch_pages: format.Pgno = 1;
            var overflow_pages: format.Pgno = 0;
            var entry_count: usize = 0;
            var depth: u16 = 0;
            for (children.items, 0..) |child, i| {
                branch_entries[i] = .{
                    .key = if (i == 0) "" else child.first_key,
                    .child_pgno = child.pgno,
                };
                leaf_pages += child.leaf_pages;
                branch_pages += child.branch_pages;
                overflow_pages += child.overflow_pages;
                entry_count += child.entry_count;
                if (child.depth > depth) depth = child.depth;
            }
            try builder.pages.append(builder.allocator, .{
                .branch = .{
                    .pgno = new_pgno,
                    .entries = branch_entries,
                },
            });
            return .{
                .first_key = children.items[0].first_key,
                .pgno = new_pgno,
                .depth = depth + 1,
                .leaf_pages = leaf_pages,
                .branch_pages = branch_pages,
                .overflow_pages = overflow_pages,
                .entry_count = entry_count,
            };
        },
        else => return error.Corrupted,
    }
}

fn cloneFullSubtree(allocator: std.mem.Allocator, builder: *ImageBuilder, txn: anytype, pgno: format.Pgno) Error!?SplitChildRef {
    if (pgno == format.invalid_pgno) return null;

    const current_page = try txn.pageView(pgno);
    switch (current_page.kind()) {
        .leaf => return splitLeafPage(allocator, builder, txn, current_page, 0, null),
        .leaf2 => return error.Incompatible,
        .branch => {
            const source_entries = try rebalance_branch.cloneEntries(allocator, current_page);
            const branch_entries = try allocator.alloc(BranchPageEntry, source_entries.len);
            var leaf_pages: format.Pgno = 0;
            var branch_pages: format.Pgno = 1;
            var overflow_pages: format.Pgno = 0;
            var entry_count: usize = 0;
            var depth: u16 = 0;
            var first_key: []const u8 = "";
            for (source_entries, 0..) |entry, i| {
                const child = (try cloneFullSubtree(allocator, builder, txn, entry.child_pgno)).?;
                branch_entries[i] = .{
                    .key = if (i == 0) "" else child.first_key,
                    .child_pgno = child.pgno,
                };
                if (i == 0) first_key = child.first_key;
                leaf_pages += child.leaf_pages;
                branch_pages += child.branch_pages;
                overflow_pages += child.overflow_pages;
                entry_count += child.entry_count;
                if (child.depth > depth) depth = child.depth;
            }
            const new_pgno = builder.allocPgno(1);
            try builder.pages.append(builder.allocator, .{
                .branch = .{
                    .pgno = new_pgno,
                    .entries = branch_entries,
                },
            });
            return .{
                .first_key = first_key,
                .pgno = new_pgno,
                .depth = depth + 1,
                .leaf_pages = leaf_pages,
                .branch_pages = branch_pages,
                .overflow_pages = overflow_pages,
                .entry_count = entry_count,
            };
        },
        else => return error.Corrupted,
    }
}

fn splitLeafPage(
    allocator: std.mem.Allocator,
    builder: *ImageBuilder,
    txn: anytype,
    leaf_page: page.View,
    db_flags: u16,
    split_key: ?[]const u8,
) Error!?SplitChildRef {
    var entries = try mutate_leaf.cloneEntries(allocator, txn, leaf_page);
    var start_index: usize = 0;
    if (split_key) |key| {
        while (start_index < entries.len and format.compareDbKeys(db_flags, entries[start_index].key, key) == .lt) : (start_index += 1) {}
        if (start_index == entries.len) return null;
        entries = entries[start_index..];
    }
    if (entries.len == 0) return null;

    var overflow_pages: format.Pgno = 0;
    for (entries, 0..) |*entry, i| {
        const source_leaf = try node.View.fromPage(leaf_page, i + start_index);
        if ((entry.flags & format.NodeFlags.bigdata) != 0) {
            const overflow_ref = source_leaf.inlineValue();
            const page_count = try materialize_support.overflowPageCountFromRef(txn, overflow_ref);
            const value = try read_support.readLeafValue(txn, source_leaf);
            const new_pgno = builder.allocPgno(page_count);
            try builder.pages.append(builder.allocator, .{
                .overflow = .{
                    .pgno = new_pgno,
                    .page_count = @intCast(page_count),
                    .data = try allocator.dupe(u8, value),
                },
            });
            const ref_bytes = try allocator.alloc(u8, @sizeOf(format.Pgno));
            format.writeNativeInt(format.Pgno, ref_bytes, new_pgno);
            entry.value = ref_bytes;
            overflow_pages += page_count;
        }
    }

    const new_pgno = builder.allocPgno(1);
    try builder.pages.append(builder.allocator, .{
        .leaf = .{
            .pgno = new_pgno,
            .entries = entries,
        },
    });
    return .{
        .first_key = entries[0].key,
        .pgno = new_pgno,
        .depth = 1,
        .leaf_pages = 1,
        .branch_pages = 0,
        .overflow_pages = overflow_pages,
        .entry_count = entries.len,
    };
}

fn splitLeafPageLeft(
    allocator: std.mem.Allocator,
    builder: *ImageBuilder,
    txn: anytype,
    leaf_page: page.View,
    db_flags: u16,
    split_key: []const u8,
) Error!?SplitChildRef {
    var entries = try mutate_leaf.cloneEntries(allocator, txn, leaf_page);
    var end_index: usize = 0;
    while (end_index < entries.len and format.compareDbKeys(db_flags, entries[end_index].key, split_key) == .lt) : (end_index += 1) {}
    if (end_index == 0) return null;
    entries = entries[0..end_index];

    var overflow_pages: format.Pgno = 0;
    for (entries, 0..) |*entry, i| {
        const source_leaf = try node.View.fromPage(leaf_page, i);
        if ((entry.flags & format.NodeFlags.bigdata) != 0) {
            const overflow_ref = source_leaf.inlineValue();
            const page_count = try materialize_support.overflowPageCountFromRef(txn, overflow_ref);
            const value = try read_support.readLeafValue(txn, source_leaf);
            const new_pgno = builder.allocPgno(page_count);
            try builder.pages.append(builder.allocator, .{
                .overflow = .{
                    .pgno = new_pgno,
                    .page_count = @intCast(page_count),
                    .data = try allocator.dupe(u8, value),
                },
            });
            const ref_bytes = try allocator.alloc(u8, @sizeOf(format.Pgno));
            format.writeNativeInt(format.Pgno, ref_bytes, new_pgno);
            entry.value = ref_bytes;
            overflow_pages += page_count;
        }
    }

    const new_pgno = builder.allocPgno(1);
    try builder.pages.append(builder.allocator, .{
        .leaf = .{
            .pgno = new_pgno,
            .entries = entries,
        },
    });
    return .{
        .first_key = entries[0].key,
        .pgno = new_pgno,
        .depth = 1,
        .leaf_pages = 1,
        .branch_pages = 0,
        .overflow_pages = overflow_pages,
        .entry_count = entries.len,
    };
}

fn lowerBoundIsRight(db_flags: u16, lower_bound: []const u8, split_key: []const u8) bool {
    return lower_bound.len > 0 and format.compareDbKeys(db_flags, lower_bound, split_key) != .lt;
}

fn upperBoundIsLeft(db_flags: u16, upper_bound: ?[]const u8, split_key: []const u8) bool {
    const upper = upper_bound orelse return false;
    return format.compareDbKeys(db_flags, upper, split_key) != .gt;
}

fn builderLastPgno(total_size: usize, page_size: usize) usize {
    return (total_size / page_size) - 1;
}

test "right split rebuilds a mixed leaf into a child data file" {
    const alloc = std.testing.allocator;
    const page_size = 4096;

    var bytes = try alloc.alloc(u8, page_size * 4);
    defer alloc.free(bytes);
    @memset(bytes, 0);

    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const main_db = format.Db{
        .md_pad = 0,
        .md_flags = 0,
        .md_depth = 1,
        .md_branch_pages = 0,
        .md_leaf_pages = 1,
        .md_overflow_pages = 0,
        .md_entries = 3,
        .md_root = 2,
    };
    txn_support.writeMetaPage(bytes[0..page_size], 0, empty_free_db, main_db, bytes.len * 8, 2, 1);
    txn_support.writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, main_db, bytes.len * 8, 2, 2);
    try mutate_leaf.writePage(bytes[page_size * 2 .. page_size * 3], 2, &.{
        .{ .key = "doc:a", .value = "alpha", .flags = 0, .data_size = 5 },
        .{ .key = "doc:m", .value = "middle", .flags = 0, .data_size = 6 },
        .{ .key = "doc:z", .value = "zeta", .flags = 0, .data_size = 4 },
    });

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "source.mdb", .data = bytes });

    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try std.fmt.bufPrint(&src_path_buf, ".zig-cache/tmp/{s}/source.mdb", .{tmp.sub_path});
    var child_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const child_path = try std.fmt.bufPrint(&child_path_buf, ".zig-cache/tmp/{s}/child.mdb", .{tmp.sub_path});

    var env = try env_mod.Environment.open(src_path, .{ .no_subdir = true });
    defer env.close();

    var txn = try txn_mod.Transaction.begin(&env, .{ .read_only = true });
    defer txn.abort();

    try writeRightSplitDataFile(alloc, &txn, main_db, "doc:m", child_path);

    var child_env = try env_mod.Environment.open(child_path, .{ .no_subdir = true });
    defer child_env.close();

    var child_txn = try txn_mod.Transaction.begin(&child_env, .{ .read_only = true });
    defer child_txn.abort();
    try std.testing.expectEqualStrings("middle", try child_txn.get(txn_mod.main_dbi, "doc:m"));
    try std.testing.expectEqualStrings("zeta", try child_txn.get(txn_mod.main_dbi, "doc:z"));
    try std.testing.expectError(error.NotFound, child_txn.get(txn_mod.main_dbi, "doc:a"));
}

test "left split rebuilds a mixed leaf into a child data file" {
    const alloc = std.testing.allocator;
    const page_size = 4096;

    var bytes = try alloc.alloc(u8, page_size * 4);
    defer alloc.free(bytes);
    @memset(bytes, 0);

    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const main_db = format.Db{
        .md_pad = 0,
        .md_flags = 0,
        .md_depth = 1,
        .md_branch_pages = 0,
        .md_leaf_pages = 1,
        .md_overflow_pages = 0,
        .md_entries = 3,
        .md_root = 2,
    };
    txn_support.writeMetaPage(bytes[0..page_size], 0, empty_free_db, main_db, bytes.len * 8, 3, 1);
    txn_support.writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, main_db, bytes.len * 8, 3, 2);
    try mutate_leaf.writePage(bytes[page_size * 2 .. page_size * 3], 2, &.{
        .{ .key = "doc:a", .value = "a", .flags = 0, .data_size = 1 },
        .{ .key = "doc:m", .value = "m", .flags = 0, .data_size = 1 },
        .{ .key = "doc:z", .value = "z", .flags = 0, .data_size = 1 },
    });

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "left_source.mdb", .data = bytes });

    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try std.fmt.bufPrint(&src_path_buf, ".zig-cache/tmp/{s}/left_source.mdb", .{tmp.sub_path});
    var child_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const child_path = try std.fmt.bufPrint(&child_path_buf, ".zig-cache/tmp/{s}/left_child.mdb", .{tmp.sub_path});
    var env = try env_mod.Environment.open(src_path, .{ .no_subdir = true });
    defer env.close();

    var txn = try txn_mod.Transaction.begin(&env, .{ .read_only = true });
    defer txn.abort();
    try writeLeftSplitDataFile(alloc, &txn, main_db, "doc:m", child_path);

    var child_env = try env_mod.Environment.open(child_path, .{ .no_subdir = true });
    defer child_env.close();

    var child_txn = try txn_mod.Transaction.begin(&child_env, .{ .read_only = true });
    defer child_txn.abort();
    try std.testing.expectEqualStrings("a", try child_txn.get(txn_mod.main_dbi, "doc:a"));
    try std.testing.expectError(error.NotFound, child_txn.get(txn_mod.main_dbi, "doc:m"));
    try std.testing.expectError(error.NotFound, child_txn.get(txn_mod.main_dbi, "doc:z"));
}

test "right split clones a fully right branch subtree into child image" {
    const alloc = std.testing.allocator;
    const page_size = 4096;

    var bytes = try alloc.alloc(u8, page_size * 5);
    defer alloc.free(bytes);
    @memset(bytes, 0);

    const empty_free_db = format.Db{
        .md_pad = page_size,
        .md_flags = format.DbFlags.integer_key,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = format.invalid_pgno,
    };
    const main_db = format.Db{
        .md_pad = 0,
        .md_flags = 0,
        .md_depth = 2,
        .md_branch_pages = 1,
        .md_leaf_pages = 2,
        .md_overflow_pages = 0,
        .md_entries = 4,
        .md_root = 2,
    };
    txn_support.writeMetaPage(bytes[0..page_size], 0, empty_free_db, main_db, bytes.len * 8, 4, 1);
    txn_support.writeMetaPage(bytes[page_size .. page_size * 2], 1, empty_free_db, main_db, bytes.len * 8, 4, 2);
    try rebalance_branch.writePage(bytes[page_size * 2 .. page_size * 3], 2, &.{
        .{ .key = "", .child_pgno = 3 },
        .{ .key = "doc:n", .child_pgno = 4 },
    });
    try mutate_leaf.writePage(bytes[page_size * 3 .. page_size * 4], 3, &.{
        .{ .key = "doc:a", .value = "alpha", .flags = 0, .data_size = 5 },
        .{ .key = "doc:f", .value = "foxtrot", .flags = 0, .data_size = 7 },
    });
    try mutate_leaf.writePage(bytes[page_size * 4 .. page_size * 5], 4, &.{
        .{ .key = "doc:n", .value = "november", .flags = 0, .data_size = 8 },
        .{ .key = "doc:z", .value = "zulu", .flags = 0, .data_size = 4 },
    });

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "source_branch.mdb", .data = bytes });

    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try std.fmt.bufPrint(&src_path_buf, ".zig-cache/tmp/{s}/source_branch.mdb", .{tmp.sub_path});
    var child_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const child_path = try std.fmt.bufPrint(&child_path_buf, ".zig-cache/tmp/{s}/child_branch.mdb", .{tmp.sub_path});

    var env = try env_mod.Environment.open(src_path, .{ .no_subdir = true });
    defer env.close();

    var txn = try txn_mod.Transaction.begin(&env, .{ .read_only = true });
    defer txn.abort();

    try writeRightSplitDataFile(alloc, &txn, main_db, "doc:m", child_path);

    var child_env = try env_mod.Environment.open(child_path, .{ .no_subdir = true });
    defer child_env.close();

    var child_txn = try txn_mod.Transaction.begin(&child_env, .{ .read_only = true });
    defer child_txn.abort();
    try std.testing.expectEqualStrings("november", try child_txn.get(txn_mod.main_dbi, "doc:n"));
    try std.testing.expectEqualStrings("zulu", try child_txn.get(txn_mod.main_dbi, "doc:z"));
    try std.testing.expectError(error.NotFound, child_txn.get(txn_mod.main_dbi, "doc:a"));
}
