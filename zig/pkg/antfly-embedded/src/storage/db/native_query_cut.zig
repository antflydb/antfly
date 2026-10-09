// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Durable query generations. Immutable runs are linked, committed WAL
//! prefixes are copied within a fixed budget, and readers never lock apply.
const std = @import("std");
const backup = @import("native_backup.zig");
const seal = @import("native_backup_seal.zig");
const Namespace = @import("doc_identity_namespace.zig").Namespace;
const Cancellation = @import("antfly_cancellation").CancellationToken;
const fs_paths = @import("antfly_runtime_fs").fs_paths;
const A = std.mem.Allocator;
pub const ttl_ms = 60_000;
pub const max_cuts = 64;
pub const Request = @import("native_query_cut_contract.zig").Request;
pub const Control = struct {
    parent: Cancellation,
    deadline_ns: u64,
    fn check(raw: *const anyopaque) !void {
        const self: *const Control = @ptrCast(@alignCast(raw));
        try self.parent.check();
        if (@import("antfly_platform").time.monotonicNs() >= self.deadline_ns) return error.DeadlineExceeded;
    }
    fn cancelled(raw: *const anyopaque) bool {
        check(raw) catch return true;
        return false;
    }
    pub fn token(self: *const Control) Cancellation {
        return .{ .ptr = self, .is_cancelled_fn = cancelled, .check_fn = check };
    }
};
pub const Manifest = struct {
    version: u16 = 1,
    request: Request,
    namespace: Namespace,
    sequence: u64,
    files: []const seal.File,
};
pub fn nowMs() u64 {
    return @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms;
}
pub fn pathAlloc(a: A, db_path: []const u8, id: []const u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}.query-pins/{s}", .{ db_path, id });
}
pub fn finish(a: A, io: std.Io, root: []const u8, request: Request, namespace: Namespace, sequence: u64, cancellation: Cancellation) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const pa = arena.allocator();
    const files = try seal.inventoryAlloc(pa, io, root, cancellation);
    const bytes = try std.json.Stringify.valueAlloc(pa, Manifest{ .request = request, .namespace = namespace, .sequence = sequence, .files = files }, .{});
    if (bytes.len > backup.max_manifest_bytes) return error.QueryCandidateBudgetExceeded;
    _ = try backup.writeFileDurable(io, try std.fmt.allocPrint(pa, "{s}/query-cut.json", .{root}), bytes);
    try fs_paths.syncDirPortable(io, root);
}
pub fn validate(a: A, io: std.Io, root: []const u8, request: Request, namespace: Namespace, cancellation: Cancellation) !void {
    try request.validate(nowMs());
    const path = try std.fmt.allocPrint(a, "{s}/query-cut.json", .{root});
    defer a.free(path);
    const bytes = backup.readFileAlloc(a, io, path, backup.max_manifest_bytes) catch |err| switch (err) {
        error.FileNotFound => return error.CatalogGenerationChanged,
        else => return err,
    };
    defer a.free(bytes);
    var parsed = try std.json.parseFromSlice(Manifest, a, bytes, .{});
    defer parsed.deinit();
    const manifest = parsed.value;
    if (manifest.version != 1 or !manifest.namespace.eql(namespace) or manifest.request.table_id != request.table_id or manifest.request.expires_ms != request.expires_ms or !std.mem.eql(u8, manifest.request.id, request.id)) return error.CatalogGenerationChanged;
    if (manifest.files.len > seal.max_files) return error.CatalogGenerationChanged;
    for (manifest.files) |file| {
        try cancellation.check();
        if (std.fs.path.isAbsolute(file.path) or std.mem.indexOf(u8, file.path, "..") != null or std.mem.indexOfAny(u8, file.path, "\\\x00") != null) return error.CatalogGenerationChanged;
        const absolute = try std.fmt.allocPrint(a, "{s}/{s}", .{ root, file.path });
        defer a.free(absolute);
        const stat = backup.statRegularFile(io, absolute) catch |err| switch (err) {
            error.FileNotFound => return error.CatalogGenerationChanged,
            else => return err,
        };
        if (stat.inode != file.inode or stat.size != file.size or stat.mtime.toNanoseconds() != file.mtime_ns) return error.CatalogGenerationChanged;
    }
}

pub const Guard = struct {
    io: std.Io,
    file: std.Io.File,
    pub fn deinit(self: *Guard) void {
        self.file.unlock(self.io);
        self.file.close(self.io);
    }
};
/// Acquire while holding the parent guard, before validating/opening files.
/// The returned lease belongs to the frozen DB and outlives every file read.
pub fn readLease(a: A, io: std.Io, db_path: []const u8, id: []const u8, cancellation: Cancellation) !Guard {
    const path = try std.fmt.allocPrint(a, "{s}.query-pins/{s}.lease", .{ db_path, id });
    defer a.free(path);
    const file = try std.Io.Dir.cwd().createFile(io, path, .{ .truncate = false });
    errdefer file.close(io);
    while (!try file.tryLock(io, .shared)) {
        try cancellation.check();
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    return .{ .io = io, .file = file };
}
pub fn lock(a: A, io: std.Io, db_path: []const u8, cancellation: Cancellation) !Guard {
    const parent = try std.fmt.allocPrint(a, "{s}.query-pins", .{db_path});
    defer a.free(parent);
    try fs_paths.createDirPathPortable(io, parent);
    const path = try std.fmt.allocPrint(a, "{s}/.lock", .{parent});
    defer a.free(path);
    const file = try std.Io.Dir.cwd().createFile(io, path, .{ .truncate = false });
    errdefer file.close(io);
    while (!try file.tryLock(io, .exclusive)) {
        try cancellation.check();
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    return .{ .io = io, .file = file };
}
/// Bounded admission and restart-safe expiry. Call under the store guard.
/// Active readers hold a per-cut lease; GC never removes their files.
pub fn admit(a: A, io: std.Io, db_path: []const u8, id: []const u8, cancellation: Cancellation) !void {
    const parent = try std.fmt.allocPrint(a, "{s}.query-pins", .{db_path});
    defer a.free(parent);
    var dir = try std.Io.Dir.cwd().openDir(io, parent, .{ .iterate = true });
    defer dir.close(io);
    var iterator = dir.iterate();
    var retained: usize = 0;
    while (try iterator.next(io)) |entry| {
        try cancellation.check();
        if (entry.kind != .directory) continue;
        // The parent guard excludes every capture. These unpublished roots
        // can only belong to an interrupted predecessor.
        if (entry.name.len == 72 and std.mem.endsWith(u8, entry.name, ".staging")) {
            const stale = try std.fmt.allocPrint(a, "{s}/{s}", .{ parent, entry.name });
            defer a.free(stale);
            try std.Io.Dir.cwd().deleteTree(io, stale);
            continue;
        }
        if (entry.name.len != 64) continue;
        if (std.mem.eql(u8, entry.name, id)) return;
        const root = try std.fmt.allocPrint(a, "{s}/{s}", .{ parent, entry.name });
        defer a.free(root);
        const manifest = try std.fmt.allocPrint(a, "{s}/query-cut.json", .{root});
        defer a.free(manifest);
        const bytes = try backup.readFileAlloc(a, io, manifest, backup.max_manifest_bytes);
        defer a.free(bytes);
        var parsed = try std.json.parseFromSlice(Manifest, a, bytes, .{});
        defer parsed.deinit();
        if (parsed.value.request.expires_ms +| 30_000 < nowMs()) {
            const lease_path = try std.fmt.allocPrint(a, "{s}/{s}.lease", .{ parent, entry.name });
            defer a.free(lease_path);
            const lease = try std.Io.Dir.cwd().createFile(io, lease_path, .{ .truncate = false });
            defer lease.close(io);
            if (try lease.tryLock(io, .exclusive)) {
                defer lease.unlock(io);
                try std.Io.Dir.cwd().deleteTree(io, root);
                try std.Io.Dir.cwd().deleteFile(io, lease_path);
            } else retained += 1;
        } else retained += 1;
        if (retained >= max_cuts) return error.QueryCandidateBudgetExceeded;
    }
}

test "retained native query identities reject expiration and path injection" {
    const id: [64]u8 = @splat('a');
    const request: Request = .{ .id = &id, .table_id = 1, .expires_ms = 60_001 };
    try request.validate(1);
    try std.testing.expectError(error.CatalogGenerationChanged, request.validate(60_001));
    var invalid = id;
    invalid[0] = '.';
    try std.testing.expectError(error.InvalidQueryRequest, (Request{ .id = &invalid, .table_id = 1, .expires_ms = 2 }).validate(1));
}
