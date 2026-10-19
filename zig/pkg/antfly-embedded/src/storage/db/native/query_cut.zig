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

//! Durable query generations. Immutable runs are linked, committed WAL
//! prefixes are copied within a fixed budget, and readers never lock apply.
const std = @import("std");
const backup = @import("backup.zig");
const seal = @import("backup_seal.zig");
const Namespace = @import("../doc_identity_namespace.zig").Namespace;
const Cancellation = @import("antfly_cancellation").CancellationToken;
const fs_paths = @import("antfly_runtime_fs").fs_paths;
const A = std.mem.Allocator;
pub const ttl_ms = @import("query_cut_contract.zig").ttl_ms;
pub const max_cuts = 64;
pub const Limits = struct { max_cuts: usize = 64, max_bytes: u64 = 64 * 1024 * 1024 * 1024 };
pub const Request = @import("query_cut_contract.zig").Request;
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
    reuse_digest: ?[32]u8 = null,
};
pub fn nowMs() u64 {
    return @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms;
}
pub fn pathAlloc(a: A, db_path: []const u8, id: []const u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}.query-pins/{s}", .{ db_path, id });
}
pub fn finish(a: A, io: std.Io, root: []const u8, request: Request, namespace: Namespace, sequence: u64, cancellation: Cancellation) !void {
    return finishWithReuseDigest(a, io, root, request, namespace, sequence, null, cancellation);
}
pub fn finishWithReuseDigest(a: A, io: std.Io, root: []const u8, request: Request, namespace: Namespace, sequence: u64, reuse_digest: ?[32]u8, cancellation: Cancellation) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const pa = arena.allocator();
    const inventory = try seal.inventoryAlloc(pa, io, root, cancellation);
    var files: std.ArrayList(seal.File) = .empty;
    for (inventory) |file| {
        if (std.mem.eql(u8, file.path, "query-cut.json") or std.mem.eql(u8, file.path, "query-remote.json")) continue;
        try files.append(pa, file);
    }
    const bytes = try std.json.Stringify.valueAlloc(pa, Manifest{ .request = request, .namespace = namespace, .sequence = sequence, .files = files.items, .reuse_digest = reuse_digest }, .{});
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
    var parsed = std.json.parseFromSlice(Manifest, a, bytes, .{}) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.CatalogGenerationChanged,
    };
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
/// Invalid local generation metadata is disposable, but active local readers
/// still own their files. Call while holding the parent publication guard.
pub fn discardInvalidCache(a: A, io: std.Io, parent: []const u8, id: []const u8, root: []const u8) !void {
    const lease_path = try std.fmt.allocPrint(a, "{s}/{s}.lease", .{ parent, id });
    defer a.free(lease_path);
    const lease = try std.Io.Dir.cwd().createFile(io, lease_path, .{ .truncate = false });
    defer lease.close(io);
    if (!try lease.tryLock(io, .exclusive)) return error.QueryCandidateBudgetExceeded;
    defer lease.unlock(io);
    try std.Io.Dir.cwd().deleteTree(io, root);
    try std.Io.Dir.cwd().deleteFile(io, lease_path);
}

/// Bounded admission and restart-safe expiry. Call under the store guard.
/// Active readers hold a per-cut lease; GC never removes their files.
pub const Durability = struct {
    ptr: *anyopaque,
    confirm: *const fn (*anyopaque, std.Io, []const u8, Manifest, Cancellation) anyerror!bool,
};
pub fn admit(a: A, io: std.Io, db_path: []const u8, id: []const u8, limits: Limits, cancellation: Cancellation) !void {
    return admitWithDurability(a, io, db_path, id, limits, cancellation, null);
}
pub fn admitWithDurability(a: A, io: std.Io, db_path: []const u8, id: []const u8, limits: Limits, cancellation: Cancellation, durability: ?Durability) !void {
    while (true) {
        try cancellation.check();
        if (try admitPass(a, io, db_path, id, limits, cancellation, durability)) return;
    }
}
fn admitPass(a: A, io: std.Io, db_path: []const u8, id: []const u8, limits: Limits, cancellation: Cancellation, durability: ?Durability) !bool {
    const parent = try std.fmt.allocPrint(a, "{s}.query-pins", .{db_path});
    defer a.free(parent);
    var dir = try std.Io.Dir.cwd().openDir(io, parent, .{ .iterate = true });
    defer dir.close(io);
    var iterator = dir.iterate();
    var retained: usize = 0;
    var existing = false;
    var bytes_retained: u64 = 0;
    var metadata_bytes: usize = 0;
    const Candidate = struct { id: [64]u8, expires_ms: u64 };
    var candidates: std.ArrayList(Candidate) = .empty;
    defer candidates.deinit(a);
    const Extent = struct { inode: u64, size: u64, mtime_ns: i128 };
    var extents: std.AutoHashMapUnmanaged(Extent, void) = .empty;
    defer extents.deinit(a);
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
        if (std.mem.eql(u8, entry.name, id)) existing = true;
        const root = try std.fmt.allocPrint(a, "{s}/{s}", .{ parent, entry.name });
        defer a.free(root);
        const manifest = try std.fmt.allocPrint(a, "{s}/query-cut.json", .{root});
        defer a.free(manifest);
        const bytes = backup.readFileAlloc(a, io, manifest, backup.max_manifest_bytes) catch |err| switch (err) {
            error.FileNotFound => {
                try discardInvalidCache(a, io, parent, entry.name, root);
                if (std.mem.eql(u8, entry.name, id)) existing = false;
                continue;
            },
            else => return err,
        };
        defer a.free(bytes);
        metadata_bytes = std.math.add(usize, metadata_bytes, bytes.len) catch return error.QueryCandidateBudgetExceeded;
        if (metadata_bytes > 64 * 1024 * 1024) return error.QueryCandidateBudgetExceeded;
        var parsed = std.json.parseFromSlice(Manifest, a, bytes, .{}) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => {
                try discardInvalidCache(a, io, parent, entry.name, root);
                if (std.mem.eql(u8, entry.name, id)) existing = false;
                continue;
            },
        };
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
                continue;
            }
        }
        retained += 1;
        if (durability != null and !std.mem.eql(u8, entry.name, id)) {
            try candidates.append(a, .{ .id = entry.name[0..64].*, .expires_ms = parsed.value.request.expires_ms });
        }
        if (parsed.value.files.len > seal.max_files) return error.QueryCandidateBudgetExceeded;
        for (parsed.value.files) |file| {
            try cancellation.check();
            const extent: Extent = .{ .inode = file.inode, .size = file.size, .mtime_ns = file.mtime_ns };
            if (extents.count() == 262144 and !extents.contains(extent)) return error.QueryCandidateBudgetExceeded;
            const entry_extent = try extents.getOrPut(a, extent);
            if (entry_extent.found_existing) continue;
            bytes_retained = std.math.add(u64, bytes_retained, file.size) catch return error.QueryCandidateBudgetExceeded;
        }
    }
    if (bytes_retained <= limits.max_bytes and retained <= limits.max_cuts and (existing or retained < limits.max_cuts)) return true;
    const proof = durability orelse return error.QueryCandidateBudgetExceeded;
    // Evict only idle local copies with a matching durable generation. A cursor
    // retains its remote authority and can reopen it without materializing files.
    // The parent publication guard prevents a new local reader during this check.
    std.mem.sort(Candidate, candidates.items, {}, struct {
        fn less(_: void, left: Candidate, right: Candidate) bool {
            if (left.expires_ms != right.expires_ms) return left.expires_ms < right.expires_ms;
            return std.mem.lessThan(u8, &left.id, &right.id);
        }
    }.less);
    for (candidates.items) |candidate| {
        try cancellation.check();
        const lease_path = try std.fmt.allocPrint(a, "{s}/{s}.lease", .{ parent, candidate.id });
        defer a.free(lease_path);
        const lease = try std.Io.Dir.cwd().createFile(io, lease_path, .{ .truncate = false });
        defer lease.close(io);
        if (!try lease.tryLock(io, .exclusive)) continue;
        defer lease.unlock(io);
        const root = try std.fmt.allocPrint(a, "{s}/{s}", .{ parent, candidate.id });
        defer a.free(root);
        const manifest_path = try std.fmt.allocPrint(a, "{s}/query-cut.json", .{root});
        defer a.free(manifest_path);
        const bytes = try backup.readFileAlloc(a, io, manifest_path, backup.max_manifest_bytes);
        defer a.free(bytes);
        var manifest = try std.json.parseFromSlice(Manifest, a, bytes, .{});
        defer manifest.deinit();
        if (!try proof.confirm(proof.ptr, io, root, manifest.value, cancellation)) continue;
        try cancellation.check();
        try std.Io.Dir.cwd().deleteTree(io, root);
        try std.Io.Dir.cwd().deleteFile(io, lease_path);
        return false; // Recompute shared-extent accounting after deletion.
    }
    return error.QueryCandidateBudgetExceeded;
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

test "external lake native eviction requires idle durable proof and honors cancellation" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var directory = try @import("../../../common/test_directory.zig").TestDirectory.init("native-cache-proof");
    defer directory.cleanup();
    const db_path = try std.fmt.allocPrint(a, "{s}/db", .{directory.path()});
    defer a.free(db_path);
    const old_id: [64]u8 = @splat('a');
    const new_id: [64]u8 = @splat('b');
    const root = try pathAlloc(a, db_path, &old_id);
    defer a.free(root);
    try fs_paths.createDirPathPortable(io, root);
    const path = try std.fmt.allocPrint(a, "{s}/query-cut.json", .{root});
    defer a.free(path);
    const manifest = try std.json.Stringify.valueAlloc(a, Manifest{ .request = .{ .id = &old_id, .table_id = 7, .expires_ms = nowMs() + 300_000 }, .namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 }, .sequence = 1, .files = &.{} }, .{});
    defer a.free(manifest);
    _ = try backup.writeFileDurable(io, path, manifest);
    const Proof = struct {
        allowed: bool = false,
        cancel: bool = false,
        cancelled: bool = false,
        calls: usize = 0,
        fn confirm(raw: *anyopaque, _: std.Io, _: []const u8, _: Manifest, _: Cancellation) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            self.cancelled = self.cancel;
            return self.allowed;
        }
        fn check(raw: *const anyopaque) !void {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            if (self.cancelled) return error.Cancelled;
        }
        fn isCancelled(raw: *const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            return self.cancelled;
        }
    };
    var proof: Proof = .{};
    const capability: Durability = .{ .ptr = &proof, .confirm = Proof.confirm };
    const limits: Limits = .{ .max_cuts = 1 };
    var guard = try lock(a, io, db_path, .none);
    defer guard.deinit();
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, admit(a, io, db_path, &new_id, limits, .none));
    {
        var reader = try readLease(a, io, db_path, &old_id, .none);
        defer reader.deinit();
        try std.testing.expectError(error.QueryCandidateBudgetExceeded, admitWithDurability(a, io, db_path, &new_id, limits, .none, capability));
        try std.testing.expectEqual(@as(usize, 0), proof.calls);
    }
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, admitWithDurability(a, io, db_path, &new_id, limits, .none, capability));
    try std.Io.Dir.accessAbsolute(io, root, .{});
    proof.allowed = true;
    proof.cancel = true;
    const token: Cancellation = .{ .ptr = &proof, .check_fn = Proof.check, .is_cancelled_fn = Proof.isCancelled };
    try std.testing.expectError(error.Cancelled, admitWithDurability(a, io, db_path, &new_id, limits, token, capability));
    try std.Io.Dir.accessAbsolute(io, root, .{});
    proof.cancel = false;
    proof.cancelled = false;
    try admitWithDurability(a, io, db_path, &new_id, limits, token, capability);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.accessAbsolute(io, root, .{}));
}

test "external lake native cache recomputes shared extent bytes after idle eviction" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var directory = try @import("../../../common/test_directory.zig").TestDirectory.init("native-cache-shared-bytes");
    defer directory.cleanup();
    const db_path = try std.fmt.allocPrint(a, "{s}/db", .{directory.path()});
    defer a.free(db_path);
    const ids = [_][64]u8{ @splat('a'), @splat('b'), @splat('c') };
    for (ids, 0..) |id, i| {
        const root = try pathAlloc(a, db_path, &id);
        defer a.free(root);
        try fs_paths.createDirPathPortable(io, root);
        const path = try std.fmt.allocPrint(a, "{s}/query-cut.json", .{root});
        defer a.free(path);
        const files = [_]seal.File{.{ .path = "data.log", .size = 8, .inode = if (i < 2) 1 else 2, .mtime_ns = 1 }};
        const bytes = try std.json.Stringify.valueAlloc(a, Manifest{ .request = .{ .id = &id, .table_id = 7, .expires_ms = nowMs() + 300_000 + i }, .namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 }, .sequence = 1, .files = &files }, .{});
        defer a.free(bytes);
        _ = try backup.writeFileDurable(io, path, bytes);
    }
    const Proof = struct {
        fn confirm(raw: *anyopaque, _: std.Io, _: []const u8, _: Manifest, _: Cancellation) !bool {
            const calls: *usize = @ptrCast(@alignCast(raw));
            calls.* += 1;
            return true;
        }
    };
    var calls: usize = 0;
    var guard = try lock(a, io, db_path, .none);
    defer guard.deinit();
    try admitWithDurability(a, io, db_path, &ids[2], .{ .max_cuts = 3, .max_bytes = 8 }, .none, .{ .ptr = &calls, .confirm = Proof.confirm });
    // Removing one of two links cannot release their shared extent's budget.
    try std.testing.expectEqual(@as(usize, 2), calls);
    for (ids, 0..) |id, i| {
        const root = try pathAlloc(a, db_path, &id);
        defer a.free(root);
        if (i < 2) try std.testing.expectError(error.FileNotFound, std.Io.Dir.accessAbsolute(io, root, .{})) else try std.Io.Dir.accessAbsolute(io, root, .{});
    }
}
