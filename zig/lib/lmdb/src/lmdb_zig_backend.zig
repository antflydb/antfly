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
const builtin = @import("builtin");
const platform = @import("antfly_platform");
const zig_lmdb = @import("lmdb_engine");

fn heapAllocator() std.mem.Allocator {
    return platform.allocator.processAllocator(std.heap.smp_allocator);
}

pub fn openEnvironment(path_owned: [:0]u8, opts: anytype) !zig_lmdb.env.Environment {
    return try zig_lmdb.env.Environment.open(path_owned, .{
        .no_subdir = opts.no_subdir,
        .read_only = opts.read_only,
        .fixed_map = opts.fixed_map,
        .write_map = opts.write_map,
        .map_async = opts.map_async,
        .no_read_ahead = opts.no_read_ahead,
        .no_sync = opts.no_sync,
        .no_meta_sync = opts.no_meta_sync,
        .no_tls = opts.no_tls,
        .no_lock = opts.no_lock,
        .no_mem_init = opts.no_mem_init,
        .defer_page_mutation = opts.defer_page_mutation,
        .artificial_sync_delay_ns = opts.artificial_sync_delay_ns,
        .commit_backend = opts.commit_backend,
    });
}

pub fn bootstrapDataFile(path: [*:0]const u8, opts: anytype) !void {
    const page_size = std.heap.page_size_min;
    const file_len = page_size * zig_lmdb.format.num_metas;
    const mapsize = @max(opts.map_size, file_len);
    const path_span = std.mem.span(path);

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const data_path = if (opts.no_subdir)
        path_span
    else
        try std.fmt.bufPrint(&path_buf, "{s}/data.mdb", .{path_span});

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    if (opts.no_subdir) {
        if (std.fs.path.dirname(data_path)) |parent| {
            if (parent.len > 0) try std.Io.Dir.cwd().createDirPath(io, parent);
        }
    } else {
        try std.Io.Dir.cwd().createDirPath(io, path_span);
    }

    var bytes: [std.heap.page_size_min * zig_lmdb.format.num_metas]u8 = undefined;
    const empty_free_db = zig_lmdb.format.Db{
        .md_pad = @intCast(page_size),
        .md_flags = zig_lmdb.format.DbFlags.integer_key,
        .md_depth = 0,
        .md_branch_pages = 0,
        .md_leaf_pages = 0,
        .md_overflow_pages = 0,
        .md_entries = 0,
        .md_root = zig_lmdb.format.invalid_pgno,
    };
    const empty_main_db = zig_lmdb.txn_support.emptyDb(0);
    zig_lmdb.txn_support.writeMetaPage(bytes[0..page_size], 0, empty_free_db, empty_main_db, mapsize, 1, 0);
    zig_lmdb.txn_support.writeMetaPage(bytes[page_size..file_len], 1, empty_free_db, empty_main_db, mapsize, 1, 1);

    var file = try createBootstrapFile(io, data_path, .{ .truncate = true });
    defer file.close(io);

    var writer_buf: [4096]u8 = undefined;
    var writer = file.writer(io, &writer_buf);
    try writer.interface.writeAll(&bytes);
    try writer.flush();
}

fn createBootstrapFile(io: anytype, path: []const u8, flags: std.Io.Dir.CreateFileOptions) !std.Io.File {
    if (std.fs.path.isAbsolute(path)) {
        return try std.Io.Dir.createFileAbsolute(io, path, flags);
    }

    const base_name = std.fs.path.basename(path);
    if (std.fs.path.dirname(path)) |parent_path| {
        var parent = try std.Io.Dir.cwd().openDir(io, parent_path, .{});
        defer parent.close(io);
        return try parent.createFile(io, base_name, flags);
    }
    return try std.Io.Dir.cwd().createFile(io, base_name, flags);
}

pub fn beginTransaction(env: *zig_lmdb.env.Environment, opts: anytype) !zig_lmdb.txn.Transaction {
    return try zig_lmdb.txn.Transaction.begin(env, .{
        .read_only = opts.read_only,
        .defer_page_mutation = opts.defer_page_mutation,
    });
}

pub fn beginChildTransaction(txn: *zig_lmdb.txn.Transaction) !zig_lmdb.txn.Transaction {
    return try zig_lmdb.txn.Transaction.beginChild(txn);
}

pub fn writeRightSplit(
    zig_env: *zig_lmdb.env.Environment,
    split_key: []const u8,
    dest_file_path: []const u8,
) !void {
    try zig_env.refresh();
    var txn = try zig_lmdb.txn.Transaction.begin(zig_env, .{ .read_only = true });
    defer txn.abort();

    const main_db = try txn.db(zig_lmdb.txn.main_dbi);
    try zig_lmdb.split_support.writeRightSplitDataFile(heapAllocator(), &txn, main_db, split_key, dest_file_path);
}

pub fn writeRightSplitNamedDb(
    zig_env: *zig_lmdb.env.Environment,
    db_name: ?[]const u8,
    split_key: []const u8,
    dest_file_path: []const u8,
) !void {
    try zig_env.refresh();
    var txn = try zig_lmdb.txn.Transaction.begin(zig_env, .{ .read_only = true });
    defer txn.abort();

    const dbi = if (db_name) |name|
        try txn.openDb(name, .{})
    else
        zig_lmdb.txn.main_dbi;
    const db = try txn.db(dbi);
    try zig_lmdb.split_support.writeRightSplitDataFile(heapAllocator(), &txn, db, split_key, dest_file_path);
}

pub fn writeLeftSplit(
    zig_env: *zig_lmdb.env.Environment,
    split_key: []const u8,
    dest_file_path: []const u8,
) !void {
    try zig_env.refresh();
    var txn = try zig_lmdb.txn.Transaction.begin(zig_env, .{ .read_only = true });
    defer txn.abort();

    const main_db = try txn.db(zig_lmdb.txn.main_dbi);
    try zig_lmdb.split_support.writeLeftSplitDataFile(heapAllocator(), &txn, main_db, split_key, dest_file_path);
}

pub fn writeLeftSplitNamedDb(
    zig_env: *zig_lmdb.env.Environment,
    db_name: ?[]const u8,
    split_key: []const u8,
    dest_file_path: []const u8,
) !void {
    try zig_env.refresh();
    var txn = try zig_lmdb.txn.Transaction.begin(zig_env, .{ .read_only = true });
    defer txn.abort();

    const dbi = if (db_name) |name|
        try txn.openDb(name, .{})
    else
        zig_lmdb.txn.main_dbi;
    const db = try txn.db(dbi);
    try zig_lmdb.split_support.writeLeftSplitDataFile(heapAllocator(), &txn, db, split_key, dest_file_path);
}
