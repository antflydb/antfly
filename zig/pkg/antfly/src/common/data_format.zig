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
const fs_paths = @import("fs_paths.zig");

pub const marker_file_name = "ANTFLY_FORMAT";
pub const current_storage_format: u32 = 1;

const marker_contents =
    \\{
    \\  "product": "antfly",
    \\  "engine": "zig",
    \\  "storage_format": 1,
    \\  "min_reader_storage_format": 1,
    \\  "migration": "Go runtime data directories are not opened in place; use portable backup and restore to migrate."
    \\}
    \\
;

const FormatMarker = struct {
    product: []const u8,
    engine: []const u8,
    storage_format: u32,
    min_reader_storage_format: u32 = 1,
};

var marker_tmp_counter = std.atomic.Value(u64).init(0);

pub const Error = error{
    IncompatibleAntflyDataDir,
    UnsupportedAntflyDataFormat,
    InvalidAntflyFormatMarker,
};

pub fn ensureCompatible(alloc: std.mem.Allocator, data_dir: []const u8) !void {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const marker_path = try std.fs.path.join(alloc, &.{ data_dir, marker_file_name });
    defer alloc.free(marker_path);

    if (pathExists(io, marker_path)) {
        try validateMarker(alloc, marker_path);
        return;
    }

    if (try looksLikeLegacyGoDataDir(alloc, io, data_dir)) {
        printLegacyGoDataDirError(data_dir);
        return Error.IncompatibleAntflyDataDir;
    }

    try fs_paths.createDirPathPortable(io, data_dir);
    try writeMarkerAtomically(alloc, io, marker_path);
}

fn validateMarker(alloc: std.mem.Allocator, marker_path: []const u8) !void {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const raw = std.Io.Dir.cwd().readFileAlloc(io_impl.io(), marker_path, alloc, .limited(16 * 1024)) catch |err| {
        std.debug.print("failed to read Antfly data-dir format marker {s}: {}\n", .{ marker_path, err });
        return err;
    };
    defer alloc.free(raw);

    const parsed = std.json.parseFromSlice(FormatMarker, alloc, raw, .{ .ignore_unknown_fields = true }) catch |err| {
        std.debug.print(
            \\invalid Antfly data-dir format marker: {s}
            \\
            \\The marker could not be parsed. Refusing to start so the data directory is not modified.
            \\
        , .{marker_path});
        return switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            else => Error.InvalidAntflyFormatMarker,
        };
    };
    defer parsed.deinit();
    const marker = parsed.value;

    if (!std.mem.eql(u8, marker.product, "antfly") or !std.mem.eql(u8, marker.engine, "zig")) {
        std.debug.print(
            \\incompatible Antfly data directory marker: {s}
            \\
            \\Expected product "antfly" and engine "zig"; found product "{s}" and engine "{s}".
            \\Use a fresh --data-dir, or migrate by taking a portable backup with the previous runtime and restoring into a fresh Zig data directory.
            \\
        , .{ marker_path, marker.product, marker.engine });
        return Error.IncompatibleAntflyDataDir;
    }

    if (marker.storage_format > current_storage_format or marker.min_reader_storage_format > current_storage_format) {
        std.debug.print(
            \\unsupported Antfly data directory format: {s}
            \\
            \\This binary supports Zig storage format {d}; the data directory requires format {d}.
            \\Use a newer Antfly binary, or restore a compatible portable backup into a fresh data directory.
            \\
        , .{ marker_path, current_storage_format, marker.storage_format });
        return Error.UnsupportedAntflyDataFormat;
    }
}

fn looksLikeLegacyGoDataDir(alloc: std.mem.Allocator, io: std.Io, data_dir: []const u8) !bool {
    const store_path = try std.fs.path.join(alloc, &.{ data_dir, "store" });
    defer alloc.free(store_path);
    if (pathExists(io, store_path)) return true;

    const metadata_path = try std.fs.path.join(alloc, &.{ data_dir, "metadata" });
    defer alloc.free(metadata_path);
    return try metadataLooksLikeGoRuntime(alloc, io, metadata_path);
}

fn metadataLooksLikeGoRuntime(alloc: std.mem.Allocator, io: std.Io, metadata_path: []const u8) !bool {
    var metadata_dir = std.Io.Dir.cwd().openDir(io, metadata_path, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return false,
        else => return err,
    };
    defer metadata_dir.close(io);

    var iter = metadata_dir.iterateAssumeFirstIteration();
    while (try iter.next(io)) |entry| {
        if (entry.kind != .directory) continue;
        inline for (.{ "storage", "log", "snap" }) |child| {
            const child_path = try std.fs.path.join(alloc, &.{ metadata_path, entry.name, child });
            defer alloc.free(child_path);
            if (pathExists(io, child_path)) return true;
        }
    }
    return false;
}

fn writeMarkerAtomically(alloc: std.mem.Allocator, io: std.Io, marker_path: []const u8) !void {
    const pid = std.posix.system.getpid();
    const counter = marker_tmp_counter.fetchAdd(1, .monotonic);
    const tmp_path = try std.fmt.allocPrint(alloc, "{s}.{d}.{d}.tmp", .{ marker_path, pid, counter });
    defer alloc.free(tmp_path);

    {
        var file = try fs_paths.createFilePortable(io, tmp_path, .{ .truncate = true });
        defer file.close(io);
        var buf: [1024]u8 = undefined;
        var writer = file.writer(io, &buf);
        try writer.interface.writeAll(marker_contents);
        try writer.end();
    }

    std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), marker_path, io) catch |err| {
        std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};
        if (pathExists(io, marker_path)) return;
        return err;
    };
}

fn pathExists(io: std.Io, path: []const u8) bool {
    std.Io.Dir.cwd().access(io, path, .{}) catch return false;
    return true;
}

fn printLegacyGoDataDirError(data_dir: []const u8) void {
    std.debug.print(
        \\incompatible Antfly data directory: found legacy Go runtime data at {s}
        \\
        \\This Antfly binary uses the Zig storage engine and cannot open Go runtime data in place.
        \\No files were modified.
        \\
        \\To migrate:
        \\  1. Run the previous Go/omni Antfly binary against this data directory.
        \\  2. Create a portable backup with `antfly backup`.
        \\  3. Start this Zig Antfly binary with an empty --data-dir.
        \\  4. Restore the backup with `antfly restore`.
        \\
    , .{data_dir});
}

test "ensureCompatible writes marker for new data dir" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const alloc = std.testing.allocator;
    const data_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data", .{tmp.sub_path});
    defer alloc.free(data_dir);

    try ensureCompatible(alloc, data_dir);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const marker_path = try std.fs.path.join(alloc, &.{ data_dir, marker_file_name });
    defer alloc.free(marker_path);
    try std.testing.expect(pathExists(io_impl.io(), marker_path));
}

test "ensureCompatible rejects legacy Go store dir" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const alloc = std.testing.allocator;
    const data_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data", .{tmp.sub_path});
    defer alloc.free(data_dir);
    const store_dir = try std.fs.path.join(alloc, &.{ data_dir, "store", "1", "2", "storage" });
    defer alloc.free(store_dir);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), store_dir);

    try std.testing.expectError(Error.IncompatibleAntflyDataDir, ensureCompatible(alloc, data_dir));
}

test "ensureCompatible accepts existing marker" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const alloc = std.testing.allocator;
    const data_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data", .{tmp.sub_path});
    defer alloc.free(data_dir);

    try ensureCompatible(alloc, data_dir);
    try ensureCompatible(alloc, data_dir);
}

test "ensureCompatible tolerates concurrent marker creation" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const alloc = std.testing.allocator;
    const data_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data", .{tmp.sub_path});
    defer alloc.free(data_dir);

    const Worker = struct {
        data_dir: []const u8,
        result: ?anyerror = null,

        fn run(self: *@This()) void {
            ensureCompatible(std.heap.smp_allocator, self.data_dir) catch |err| {
                self.result = err;
            };
        }
    };

    var workers: [8]Worker = undefined;
    var threads: [8]std.Thread = undefined;
    for (&workers, &threads) |*worker, *thread| {
        worker.* = .{ .data_dir = data_dir };
        thread.* = try std.Thread.spawn(.{}, Worker.run, .{worker});
    }
    for (&threads) |*thread| thread.join();
    for (&workers) |worker| {
        if (worker.result) |err| return err;
    }

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const marker_path = try std.fs.path.join(alloc, &.{ data_dir, marker_file_name });
    defer alloc.free(marker_path);
    try std.testing.expect(pathExists(io_impl.io(), marker_path));
}

test "ensureCompatible rejects newer storage format" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const alloc = std.testing.allocator;
    const data_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data", .{tmp.sub_path});
    defer alloc.free(data_dir);
    const marker_path = try std.fs.path.join(alloc, &.{ data_dir, marker_file_name });
    defer alloc.free(marker_path);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), data_dir);
    {
        var file = try fs_paths.createFilePortable(io_impl.io(), marker_path, .{ .truncate = true });
        defer file.close(io_impl.io());
        var buf: [256]u8 = undefined;
        var writer = file.writer(io_impl.io(), &buf);
        try writer.interface.writeAll(
            \\{"product":"antfly","engine":"zig","storage_format":2,"min_reader_storage_format":2}
        );
        try writer.end();
    }

    try std.testing.expectError(Error.UnsupportedAntflyDataFormat, ensureCompatible(alloc, data_dir));
}
