// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Restart-stable, owner/attempt-bound backup pin inventories. Seals contain
//! hardlinks to immutable files and bounded copied committed WAL prefixes;
//! they never retain pointers, live paths or process-local descriptor leases.
const std = @import("std");
const backup = @import("native_backup.zig");
const topology = @import("relational_integrity_topology.zig");
const fs_paths = @import("antfly_runtime_fs").fs_paths;
const Cancellation = @import("antfly_cancellation").CancellationToken;
const json = @import("relational_integrity_json.zig");
const Allocator = std.mem.Allocator;
pub const wal_budget_bytes: u64 = 16 * 1024 * 1024;
pub const max_files: usize = 100_000;
pub const manifest_name = "seal.json";
const digest_name = "seal.sha256";
const export_receipt_name = "backup-seal-receipt.json";
const ExportReceipt = struct { handle: Handle, bytes: u64 };

/// Serializes pin publication/export/reclamation even after the live catalog
/// generation has gone away. The file is scoped to the immutable group path;
/// it is never a user-selected path or a public table-name lookup.
pub const StoreLock = struct {
    file: std.Io.File,
    io: std.Io,
    pub fn acquire(alloc: Allocator, io: std.Io, db_path: []const u8, cancellation: Cancellation) !StoreLock {
        const parent = try std.fmt.allocPrint(alloc, "{s}.backup-pins", .{db_path});
        defer alloc.free(parent);
        try fs_paths.createDirPathPortable(io, parent);
        const path = try std.fmt.allocPrint(alloc, "{s}/.lock", .{parent});
        defer alloc.free(path);
        const file = try std.Io.Dir.cwd().createFile(io, path, .{ .truncate = false });
        errdefer file.close(io);
        while (!try file.tryLock(io, .exclusive)) {
            try cancellation.check();
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        return .{ .file = file, .io = io };
    }
    pub fn deinit(self: *StoreLock) void {
        self.file.unlock(self.io);
        self.file.close(self.io);
        self.* = undefined;
    }
};

/// Cleanup is authorized by an exact immutable cohort fence, not by whether
/// its former table name still resolves. Tombstone first so delayed seal
/// delivery cannot recreate a reclaimed pin after this acknowledgement.
pub fn reclaim(alloc: Allocator, io: std.Io, db_path: []const u8, request: Request, cancellation: Cancellation) !void {
    const fence = switch (request) {
        .seal => return error.InvalidBackupSeal,
        .release => |handle| handle.fence,
        .cancel => |value| value,
    };
    if (fence.role != .backup_snapshot) return error.InvalidBackupSeal;
    const root = try pathAlloc(alloc, db_path, fence);
    defer alloc.free(root);
    var lock = try StoreLock.acquire(alloc, io, db_path, cancellation);
    defer lock.deinit();
    try cancellation.check();
    const tombstone = try std.fmt.allocPrint(alloc, "{s}.released", .{root});
    defer alloc.free(tombstone);
    const old = backup.readFileAlloc(alloc, io, tombstone, 33) catch |err| switch (err) {
        error.FileNotFound => null,
        else => return err,
    };
    defer if (old) |value| alloc.free(value);
    const digest = switch (request) {
        .seal => unreachable,
        .release => |handle| handle.digest,
        .cancel => if (old) |value| blk: {
            if (value.len != 32) return error.BackupSealMismatch;
            break :blk value[0..32].*;
        } else (readHandle(alloc, io, root, fence) catch |err| switch (err) {
            error.FileNotFound => Handle{ .fence = fence, .digest = @splat(0) },
            else => return err,
        }).digest,
    };
    if (old) |value| {
        if (!std.mem.eql(u8, value, &digest)) return error.BackupSealMismatch;
    } else {
        if (request == .release) {
            // Missing files can be the result of whole-generation retirement.
            // Existing files must still match the exact requested seal.
            var opened = open(alloc, io, root, request.release) catch |err| switch (err) {
                error.FileNotFound => null,
                else => return err,
            };
            if (opened) |*value| value.deinit();
        }
        _ = try backup.writeFileDurable(io, tombstone, &digest);
    }
    try std.Io.Dir.cwd().deleteTree(io, root);
    const abandoned = try std.fmt.allocPrint(alloc, "{s}.staging", .{root});
    defer alloc.free(abandoned);
    try std.Io.Dir.cwd().deleteTree(io, abandoned);
    try fs_paths.syncDirPortable(io, std.fs.path.dirname(root).?);
}

pub fn recordExport(alloc: Allocator, io: std.Io, root: []const u8, handle: Handle, bytes: u64) !void {
    const path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ root, export_receipt_name });
    defer alloc.free(path);
    var output = std.Io.Writer.Allocating.init(alloc);
    defer output.deinit();
    var stream: std.json.Stringify = .{ .writer = &output.writer, .options = .{} };
    try json.write(ExportReceipt{ .handle = handle, .bytes = bytes }, &stream);
    _ = try backup.writeFileDurable(io, path, output.written());
}

pub fn exportedBytes(alloc: Allocator, io: std.Io, root: []const u8, handle: Handle) !u64 {
    const path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ root, export_receipt_name });
    defer alloc.free(path);
    const bytes = try backup.readFileAlloc(alloc, io, path, 4096);
    defer alloc.free(bytes);
    var parsed = try std.json.parseFromSlice(ExportReceipt, alloc, bytes, .{});
    defer parsed.deinit();
    if (!parsed.value.handle.fence.eql(handle.fence) or !std.mem.eql(u8, &parsed.value.handle.digest, &handle.digest)) return error.BackupSealMismatch;
    return parsed.value.bytes;
}

pub const Handle = @import("native_backup_seal_contract.zig").Handle;
pub const Request = @import("native_backup_seal_contract.zig").Request;
pub const File = struct { path: []const u8, size: u64, inode: u64, mtime_ns: i128 };
pub const Manifest = struct {
    version: u32 = 1,
    fence: topology.Fence,
    sequence: u64,
    primary: backup.Primary,
    projections: []const backup.Projection,
    files: []const File,
};

pub fn nameAlloc(alloc: Allocator, fence: topology.Fence) ![]u8 {
    var encoded: [136]u8 = undefined;
    // Fence codec validates every identity field before deriving a path.
    encoded = try fence.encode();
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(&encoded, &digest, .{});
    return std.fmt.allocPrint(alloc, "cohort-pin-{s}", .{std.fmt.bytesToHex(digest, .lower)});
}

pub fn pathAlloc(alloc: Allocator, db_path: []const u8, fence: topology.Fence) ![]u8 {
    const name = try nameAlloc(alloc, fence);
    defer alloc.free(name);
    return std.fmt.allocPrint(alloc, "{s}.backup-pins/{s}", .{ db_path, name });
}

fn checkRelative(path: []const u8) !void {
    if (path.len == 0 or std.fs.path.isAbsolute(path) or std.mem.indexOfAny(u8, path, "\\\x00") != null) return error.InvalidBackupSeal;
    var parts = std.mem.splitScalar(u8, path, '/');
    while (parts.next()) |part| if (part.len == 0 or std.mem.eql(u8, part, ".") or std.mem.eql(u8, part, "..")) return error.InvalidBackupSeal;
}

/// Only metadata is inspected here: all corpus hashes are computed by export.
/// Directory entries are fsynced before the seal itself becomes publishable.
pub fn finish(alloc: Allocator, io: std.Io, root: []const u8, fence: topology.Fence, sequence: u64, primary: backup.Primary, projections: []const backup.Projection, cancellation: Cancellation) !Handle {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var files = std.ArrayListUnmanaged(File).empty;
    var dir = try std.Io.Dir.cwd().openDir(io, root, .{ .iterate = true });
    defer dir.close(io);
    var walker = try dir.walk(a);
    defer walker.deinit();
    while (try walker.next(io)) |entry| {
        try cancellation.check();
        // Temporary process-local pin trees are not part of the durable seal.
        if (std.mem.startsWith(u8, entry.path, ".generated-")) continue;
        // Walker paths are native; the durable inventory uses '/' on every host.
        const relative = try fs_paths.joinStoragePathAlloc(a, &.{entry.path});
        const absolute = try std.fmt.allocPrint(a, "{s}/{s}", .{ root, relative });
        if (entry.kind == .directory) {
            try fs_paths.syncDirPortable(io, absolute);
            continue;
        }
        if (entry.kind != .file or files.items.len == max_files) return error.BackupSealInventoryTooLarge;
        try checkRelative(relative);
        const stat = try backup.statRegularFile(io, absolute);
        try files.append(a, .{ .path = relative, .size = stat.size, .inode = @bitCast(stat.inode), .mtime_ns = stat.mtime.toNanoseconds() });
    }
    std.mem.sort(File, files.items, {}, struct {
        fn less(_: void, x: File, y: File) bool {
            return std.mem.order(u8, x.path, y.path) == .lt;
        }
    }.less);
    const manifest: Manifest = .{ .fence = fence, .sequence = sequence, .primary = primary, .projections = projections, .files = files.items };
    var output = std.Io.Writer.Allocating.init(a);
    var stream: std.json.Stringify = .{ .writer = &output.writer, .options = .{} };
    try json.write(manifest, &stream);
    if (output.written().len > backup.max_manifest_bytes) return error.BackupSealInventoryTooLarge;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(output.written(), &digest, .{});
    const path = try std.fmt.allocPrint(a, "{s}/{s}", .{ root, manifest_name });
    _ = try backup.writeFileDurable(io, path, output.written());
    const digest_path = try std.fmt.allocPrint(a, "{s}/{s}", .{ root, digest_name });
    _ = try backup.writeFileDurable(io, digest_path, &digest);
    try fs_paths.syncDirPortable(io, root);
    return .{ .fence = fence, .digest = digest };
}

test "backup seal inventories nested files with portable relative paths" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "primary-lsm/nested");
    {
        const file = try tmp.dir.createFile(io, "primary-lsm/nested/segment.afps", .{});
        defer file.close(io);
        try file.writeStreamingAll(io, "immutable");
    }
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(root);
    const fence: topology.Fence = .{
        .transition_id = 1,
        .attempt = 1,
        .peer_group_id = 1,
        .owner_group_id = 1,
        .role = .backup_snapshot,
        .namespace = .{ .table_id = 1 },
        .catalog_digest = @splat(1),
    };
    const handle = try finish(alloc, io, root, fence, 1, .{
        .artifact_format = "test",
        .artifact_version = 1,
        .source_backend = "test",
    }, &.{}, .none);
    var opened = try open(alloc, io, root, handle);
    defer opened.deinit();
    try std.testing.expectEqual(@as(usize, 1), opened.parsed.value.files.len);
    try std.testing.expectEqualStrings("primary-lsm/nested/segment.afps", opened.parsed.value.files[0].path);
    try std.testing.expectEqual(@as(u64, 9), opened.parsed.value.files[0].size);
}

pub const Opened = struct {
    parsed: std.json.Parsed(Manifest),
    pub fn deinit(self: *Opened) void {
        self.parsed.deinit();
    }
};

pub var test_export_hook: ?struct { ptr: *anyopaque, run: *const fn (*anyopaque) anyerror!void } = null;

pub fn open(alloc: Allocator, io: std.Io, root: []const u8, handle: Handle) !Opened {
    const path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ root, manifest_name });
    defer alloc.free(path);
    const bytes = try backup.readFileAlloc(alloc, io, path, backup.max_manifest_bytes);
    defer alloc.free(bytes);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    if (!std.mem.eql(u8, &digest, &handle.digest)) return error.BackupSealMismatch;
    var parsed = try std.json.parseFromSlice(Manifest, alloc, bytes, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    if (parsed.value.version != 1 or !parsed.value.fence.eql(handle.fence) or parsed.value.files.len > max_files) return error.InvalidBackupSeal;
    var previous: ?[]const u8 = null;
    for (parsed.value.files) |file| {
        try checkRelative(file.path);
        if (previous) |prev| if (std.mem.order(u8, prev, file.path) != .lt) return error.InvalidBackupSeal;
        previous = file.path;
    }
    return .{ .parsed = parsed };
}

pub fn readHandle(alloc: Allocator, io: std.Io, root: []const u8, expected: topology.Fence) !Handle {
    const path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ root, digest_name });
    defer alloc.free(path);
    const bytes = try backup.readFileAlloc(alloc, io, path, 33);
    defer alloc.free(bytes);
    if (bytes.len != 32) return error.InvalidBackupSeal;
    var handle: Handle = .{ .fence = expected, .digest = undefined };
    @memcpy(&handle.digest, bytes);
    var opened = try open(alloc, io, root, handle);
    opened.deinit();
    return handle;
}

/// Reopen exclusively from the sealed inventory. A newer live generation can
/// never satisfy a missing seal or a mismatched owner/attempt/digest.
pub fn exportTo(alloc: Allocator, io: std.Io, root: []const u8, handle: Handle, destination: []const u8, cancellation: Cancellation) !u64 {
    var opened = try open(alloc, io, root, handle);
    defer opened.deinit();
    const manifest = opened.parsed.value;
    if (@import("builtin").is_test) if (test_export_hook) |hook| try hook.run(hook.ptr);
    var receipts = backup.ArtifactReceiptCollector.init(alloc, destination, manifest.projections);
    defer receipts.deinit();
    var total: u64 = 0;
    for (manifest.files) |file| {
        try cancellation.check();
        const source = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ root, file.path });
        defer alloc.free(source);
        const target = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ destination, file.path });
        defer alloc.free(target);
        const stat = try backup.statRegularFile(io, source);
        if (stat.inode != file.inode or stat.size != file.size or stat.mtime.toNanoseconds() != file.mtime_ns) return error.BackupSealSourceChanged;
        total = std.math.add(u64, total, try backup.copyFileDurableCancellableWithSink(io, source, target, cancellation, receipts.sink())) catch return error.FileTooBig;
    }
    return std.math.add(u64, total, try backup.finalizeCaptureGenerationFromReceiptsWithCancellation(alloc, io, &receipts, manifest.sequence, manifest.primary, cancellation)) catch error.FileTooBig;
}
