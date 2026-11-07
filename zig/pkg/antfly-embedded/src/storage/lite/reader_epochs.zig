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

//! Revision-6 cross-process reader epochs with an immutable namespace binding. Registration holds the data inode's
//! shared lock until a locked, immutable marker is complete. A writer holds the
//! exclusive data lock while scanning markers and promoting/reusing pages. Thus
//! registration cannot cross a writer's scan-to-allocation window. Kernel locks
//! reclaim crashed registrations; no clock, PID reuse, or timeout decides safety.
const std = @import("std");
const Allocator = std.mem.Allocator;
pub const Frontier = struct { data: ?u64 = null, metadata: ?u64 = null };
pub const Lease = struct {
    file: std.Io.File,
    directory: std.Io.Dir,
    name: [32]u8,
    path: []u8,
    pub fn close(self: Lease, a: Allocator, io: std.Io) void {
        // Use the captured directory inode even if a parent was renamed.
        self.directory.deleteFile(io, &self.name) catch {};
        self.file.close(io);
        self.directory.close(io);
        a.free(self.path);
    }
};

const Namespace = struct {
    canonical: [:0]u8,
    directory: std.Io.Dir,
    guard: std.Io.File,
    digest: [32]u8,
    /// Inode of `path`'s resolved parent directory at open time. A cache
    /// that keeps this `Namespace` open across commits uses this to cheaply
    /// detect that the parent has been replaced out from under it (e.g. an
    /// external rename/recreate dance, as a backup/restore or vacuum can
    /// do): the already-open `directory`/`guard` handles stay valid by
    /// inode regardless of what happens to the path, but they stop being
    /// the directory a *fresh* lookup from the current parent would land
    /// in, and a cache must not keep trusting them once that is true. See
    /// `parentUnchanged`.
    parent_inode: u64,
    fn open(a: Allocator, io: std.Io, path: []const u8, publication: bool, no_sync: bool) !Namespace {
        // Resolve the `.aflite` file's parent directory *once*, as an
        // already-open handle, and reuse that same handle for everything
        // below -- rather than asking the OS to canonicalize a cwd-relative
        // *string* (`std.fs.path.dirname(path) orelse "."`) and then
        // re-opening whatever string comes back. `std.Io.Dir.realPathFile`
        // -- what the old resolution used -- documents itself as having
        // "limited platform support" and warns "using it can lead to
        // unnecessary failures and race conditions"; a relative `path`
        // resolved against a cwd that is not exactly what the caller
        // expects at that instant silently points the whole reader-epoch
        // namespace, and the "identity" marker inside it, at the wrong
        // directory entirely -- observed in practice as a bare `identity`
        // file landing directly in the process's cwd instead of inside its
        // own `.aflite-readers-*` directory next to the `.aflite` file.
        // Resolving the already-open parent handle's own identity via
        // `Dir.realPath` (no `sub_path`, no fresh cwd lookup) and reusing
        // that handle removes any dependency on cwd for the rest of this
        // call.
        var parent = try std.Io.Dir.cwd().openDir(io, std.fs.path.dirname(path) orelse ".", .{});
        var parent_open = true;
        errdefer if (parent_open) parent.close(io);
        if (!publication) {
            // Preserve the original contract: a missing `.aflite` file
            // must surface as `error.FileNotFound` (translated by callers
            // into `UnsafeNativeReaderAlias`/`WouldBlock`), not silently
            // resolve only its still-existing parent directory.
            try parent.access(io, std.fs.path.basename(path), .{});
        }
        var parent_path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const parent_path_len = try parent.realPath(io, &parent_path_buf);
        const canonical = try std.fs.path.joinZ(a, &.{ parent_path_buf[0..parent_path_len], std.fs.path.basename(path) });
        errdefer a.free(canonical);
        // The path chooses a bounded location, never the authority to reuse.
        var path_digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(canonical, &path_digest, .{});
        var name: [80]u8 = undefined;
        const directory_name = try std.fmt.bufPrint(&name, ".aflite-readers-{x}", .{path_digest});
        if (publication) parent.createDir(io, directory_name, .fromMode(0o700)) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };
        const directory = try parent.openDir(io, directory_name, .{ .iterate = true });
        errdefer directory.close(io);
        const parent_inode: u64 = @intCast((try parent.stat(io)).inode);
        parent.close(io);
        parent_open = false;
        // Never recreate this authority for an already published inode. Missing
        // or replaced namespaces must retain inode fencing, including writers.
        const guard = if (publication)
            try directory.createFile(io, "identity", .{ .read = true, .truncate = false, .lock = .exclusive, .permissions = .fromMode(0o600) })
        else
            try directory.openFile(io, "identity", .{ .mode = .read_write });
        errdefer guard.close(io);
        const stat = try guard.stat(io);
        if (stat.kind != .file or stat.size > 32) return error.InvalidNativeReaderEpoch;
        var seed: [32]u8 = undefined;
        if (publication and stat.size < 32) {
            try io.randomSecure(&seed);
            try guard.writePositionalAll(io, &seed, 0);
        } else if (try guard.readPositionalAll(io, &seed, 0) != 32) return error.InvalidNativeReaderEpoch;
        if (publication) {
            if (!no_sync) {
                try guard.sync(io);
                const directory_path = try std.fs.path.join(a, &.{ std.fs.path.dirname(canonical).?, directory_name });
                defer a.free(directory_path);
                try @import("antfly_runtime_fs").fs_paths.syncDirPortable(io, directory_path);
                try @import("antfly_runtime_fs").fs_paths.syncDirPortable(io, std.fs.path.dirname(canonical).?);
            }
            guard.unlock(io);
        }
        var id: [8]u8 = undefined;
        std.mem.writeInt(u64, &id, @intCast((try directory.stat(io)).inode), .little);
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("AFLITE reader namespace v6");
        hash.update(&seed);
        hash.update(&id);
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        return .{ .canonical = canonical, .directory = directory, .guard = guard, .digest = digest, .parent_inode = parent_inode };
    }
    fn close(self: Namespace, a: Allocator, io: std.Io) void {
        self.guard.close(io);
        self.directory.close(io);
        a.free(self.canonical);
    }
};

/// Only a newly created/private inode may establish or change this binding.
/// Identity persists across inode generations, but cannot be reconstructed from
/// an alias or a recreated directory. Existing markers retain their authority.
pub fn namespaceForPublication(a: Allocator, io: std.Io, path: []const u8) ![32]u8 {
    return namespaceForPublicationWithDurability(a, io, path, true);
}
pub fn namespaceForPublicationWithDurability(a: Allocator, io: std.Io, path: []const u8, no_sync: bool) ![32]u8 {
    const namespace = try Namespace.open(a, io, path, true, no_sync);
    defer namespace.close(a, io);
    return namespace.digest;
}

pub fn register(a: Allocator, io: std.Io, path: []const u8, binding: [32]u8, inode: u64, sequence: u64, metadata: bool) !Lease {
    const namespace = Namespace.open(a, io, path, false, true) catch |err| switch (err) {
        error.FileNotFound => return error.UnsafeNativeReaderAlias,
        else => return err,
    };
    defer namespace.close(a, io);
    if (!std.mem.eql(u8, &binding, &namespace.digest)) return error.UnsafeNativeReaderAlias;
    // Different file generations share this directory but have different data
    // locks. A shared guard covers create-to-flock, so a scanner for an older
    // generation cannot reap an incompletely registered new marker.
    if (!try namespace.guard.tryLock(io, .shared)) return error.UnsafeNativeReaderAlias;
    defer namespace.guard.unlock(io);
    const directory = try namespace.directory.openDir(io, ".", .{});
    errdefer directory.close(io);
    var nonce: [16]u8 = undefined;
    try io.randomSecure(&nonce);
    var name: [32]u8 = undefined;
    _ = try std.fmt.bufPrint(&name, "{x}", .{nonce});
    const directory_path = try namespace.directory.realPathFileAlloc(io, ".", a);
    defer a.free(directory_path);
    const marker_path = try std.fs.path.join(a, &.{ directory_path, &name });
    errdefer a.free(marker_path);
    const marker = try directory.createFile(io, &name, .{ .read = true, .exclusive = true, .lock = .exclusive, .lock_nonblocking = true, .permissions = .fromMode(0o600) });
    errdefer {
        directory.deleteFile(io, &name) catch {};
        marker.close(io);
    }
    var raw: [64]u8 = @splat(0);
    @memcpy(raw[0..8], "AFREAD05");
    std.mem.writeInt(u64, raw[8..16], inode, .little);
    std.mem.writeInt(u64, raw[16..24], sequence, .little);
    std.mem.writeInt(u64, raw[24..32], @intFromBool(metadata), .little);
    std.crypto.hash.Blake3.hash(raw[0..32], raw[32..64], .{});
    try marker.writePositionalAll(io, &raw, 0);
    return .{ .file = marker, .directory = directory, .name = name, .path = marker_path };
}

/// Caller holds the exclusive data inode lock. Missing/mismatched namespaces
/// deny promotion and reuse; uncooperative readers retain ordinary inode locks.
/// Bound work under admission; too many registrations conservatively deny reuse.
pub fn scan(a: Allocator, io: std.Io, path: []const u8, binding: [32]u8, inode: u64) !Frontier {
    const namespace = Namespace.open(a, io, path, false, true) catch |err| switch (err) {
        error.FileNotFound => return error.WouldBlock,
        else => return err,
    };
    defer namespace.close(a, io);
    if (!std.mem.eql(u8, &binding, &namespace.digest)) return error.WouldBlock;
    return scanNamespace(io, namespace, inode);
}

/// The actual marker walk, factored out of `scan` so a caller that keeps a
/// `Namespace` open across many calls (`CachedNamespace` below) can reuse the
/// same directory/guard handles instead of paying `Namespace.open`'s
/// resolve-canonical-path/hash/open-parent/open-directory/open-identity cost
/// on every call. No behavior change from the body `scan` used to run inline.
fn scanNamespace(io: std.Io, namespace: Namespace, inode: u64) !Frontier {
    if (!try namespace.guard.tryLock(io, .exclusive)) return error.WouldBlock;
    defer namespace.guard.unlock(io);
    const dir = namespace.directory;
    var it = dir.iterate();
    var result: Frontier = .{};
    var visited: usize = 0;
    while (try it.next(io)) |entry| {
        if (std.mem.eql(u8, entry.name, "identity")) continue;
        visited += 1;
        if (visited > 4096) return error.WouldBlock;
        if ((entry.kind != .file and entry.kind != .unknown) or entry.name.len != 32) return error.InvalidNativeReaderEpoch;
        const marker = dir.openFile(io, entry.name, .{ .mode = .read_write }) catch |err| switch (err) {
            error.FileNotFound => continue,
            error.IsDir => return error.InvalidNativeReaderEpoch,
            else => return err,
        };
        defer marker.close(io);
        // d_type is optional on Linux filesystems. Authenticate the opened
        // descriptor's type rather than refusing an unknown directory hint.
        if (entry.kind == .unknown and (try marker.stat(io)).kind != .file) return error.InvalidNativeReaderEpoch;
        if (try marker.tryLock(io, .exclusive)) {
            // No reader can acquire an existing UUID. The creating reader held
            // the data lock until it acquired the marker lock and wrote bytes.
            dir.deleteFile(io, entry.name) catch |err| switch (err) {
                error.FileNotFound => {},
                else => return err,
            };
            continue;
        }
        var raw: [64]u8 = undefined;
        const n = try marker.readPositionalAll(io, &raw, 0);
        if (n != raw.len) return error.InvalidNativeReaderEpoch;
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(raw[0..32], &digest, .{});
        if (!std.mem.eql(u8, raw[0..8], "AFREAD05") or !std.mem.eql(u8, raw[32..64], &digest) or std.mem.readInt(u64, raw[24..32], .little) > 1) return error.InvalidNativeReaderEpoch;
        if (std.mem.readInt(u64, raw[8..16], .little) != inode) continue;
        const sequence = std.mem.readInt(u64, raw[16..24], .little);
        result.data = @min(result.data orelse sequence, sequence);
        if (raw[24] == 1) result.metadata = @min(result.metadata orelse sequence, sequence);
    }
    return result;
}

/// A `Namespace` kept open across many writer commits instead of being
/// resolved, opened, and closed from scratch every time -- the dominant
/// per-commit cost this revision used to pay even with zero readers ever
/// registered (opening the parent directory, resolving its canonical path,
/// hashing it, opening the readers directory, and opening+reading+hashing
/// the `identity` guard, all repeated on every single write). The *safety*
/// decision (which markers are live) is still made exactly as `scan` makes
/// it, by `scanNamespace` above, under the same kernel locks; this only
/// amortizes the handles. Owned by a single `NativeFile` for its lifetime
/// (or until something rebinds `reader_epoch_namespace`), never shared
/// across processes or across a rebind.
pub const CachedNamespace = struct {
    namespace: Namespace,

    pub fn close(self: CachedNamespace, a: Allocator, io: std.Io) void {
        self.namespace.close(a, io);
    }
};

/// Open (or reopen) the namespace once, verifying it still matches `binding`
/// exactly as `scan`/`register` do. Returns `error.WouldBlock` on the same
/// conditions `scan` treats as "no safe reuse decision available" (missing
/// `.aflite`, or a namespace that no longer matches the caller's binding --
/// e.g. the file was replaced out from under this handle).
pub fn openCached(a: Allocator, io: std.Io, path: []const u8, binding: [32]u8) !CachedNamespace {
    const namespace = Namespace.open(a, io, path, false, true) catch |err| switch (err) {
        error.FileNotFound => return error.WouldBlock,
        else => return err,
    };
    errdefer namespace.close(a, io);
    if (!std.mem.eql(u8, &binding, &namespace.digest)) return error.WouldBlock;
    return .{ .namespace = namespace };
}

/// Cheap (one syscall on non-Windows, via `Dir.statFile`) check that a
/// fresh, path-based lookup of `path`'s parent would still resolve to the
/// same parent directory this `cached` namespace was opened against. A
/// writer's cache must re-verify this every time it wants to trust an
/// already-open namespace: the directory/guard handles stay valid by inode
/// no matter what happens to the filesystem around them, but `register`'s
/// *next* caller finds its namespace by hashing a freshly-resolved
/// canonical path starting from the parent directory's current identity --
/// not from any writer's cached handle. If something external (a
/// backup/restore or vacuum dance, or just an operator moving directories
/// around) has renamed or replaced that parent since this namespace was
/// opened, a fresh registration would land in a different (or missing)
/// directory than the one this cache still has open, so the cache is no
/// longer the authority and must not be trusted -- regardless of whether
/// its own handles still happen to work. Returns `false` (never trust) on
/// any error resolving the parent, so a transient failure fails closed to
/// the slow, fully-reopened path rather than to an unverified cache hit.
pub fn parentUnchanged(io: std.Io, path: []const u8, cached: CachedNamespace) bool {
    const stat = std.Io.Dir.cwd().statFile(io, std.fs.path.dirname(path) orelse ".", .{}) catch return false;
    return stat.inode == cached.namespace.parent_inode;
}

/// A cheap (single `fstat`-equivalent) stand-in for "has the readers
/// directory's membership changed since the last time this was captured."
/// `register` creates a file in this directory and `Lease.close`/`scan`'s own
/// reaping delete one; every one of those mutations bumps the directory's own
/// mtime for free, with no protocol change to any of them. This is *not*
/// itself what makes reuse safe -- it is only a cache-invalidation hint for a
/// caller that reads it while already holding the writer's own exclusive
/// data-file lock. `register` needs that same file's *shared* lock to reach
/// the point where it creates its marker (see this file's header comment and
/// `register`'s own comments), so once a writer holds the data file
/// exclusively, no *new* registration can complete until it releases that
/// lock -- the registered-reader set at that instant can only be a subset of
/// (or equal to) what the last real scan observed. An unchanged fingerprint
/// therefore proves nothing joined since that scan; a changed one (a join, a
/// departure, or both) always falls back to a real scan. A missed departure
/// is always safe: it only makes the cached frontier more conservative, never
/// unsafe, since it can only ever be lower than or equal to the true current
/// minimum once there are no new joiners.
pub fn directoryFingerprint(io: std.Io, cached: CachedNamespace) !i96 {
    const stat = try cached.namespace.directory.stat(io);
    return stat.mtime.nanoseconds;
}

/// Identical safety decision to `scan`, but against an already-open
/// `CachedNamespace` -- no parent/directory/identity opens, no path
/// resolution, no hashing. Callers that want the O(1) fast path should
/// prefer comparing `directoryFingerprint` against a previously cached value
/// first and only call this when it has changed (or on the first call).
pub fn scanCached(io: std.Io, cached: CachedNamespace, inode: u64) !Frontier {
    return scanNamespace(io, cached.namespace, inode);
}

test "lite reader epochs isolate inode frontiers and reclaim crashed registrations" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/epochs.aflite", .{tmp.sub_path});
    defer a.free(path);
    const data = try std.Io.Dir.cwd().createFile(io, path, .{ .read = true });
    defer data.close(io);
    const inode = (try data.stat(io)).inode;
    const binding = try namespaceForPublication(a, io, path);
    const first = try register(a, io, path, binding, inode, 10, false);
    var first_open = true;
    defer if (first_open) first.close(a, io);
    const full = try register(a, io, path, binding, inode, 20, true);
    defer full.close(a, io);
    const unrelated = try register(a, io, path, binding, inode + 1, 1, true);
    defer unrelated.close(a, io);
    var frontiers = try scan(a, io, path, binding, inode);
    try std.testing.expectEqual(@as(?u64, 10), frontiers.data);
    try std.testing.expectEqual(@as(?u64, 20), frontiers.metadata);
    // Simulate process death: leave the pathname but release its kernel lock.
    first.file.close(io);
    first.directory.close(io);
    first_open = false;
    defer a.free(first.path);
    frontiers = try scan(a, io, path, binding, inode);
    try std.testing.expectEqual(@as(?u64, 20), frontiers.data);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io, first.path, .{}));
    // A live malformed registration must fail closed, never disappear.
    try full.file.writePositionalAll(io, "broken", 0);
    try std.testing.expectError(error.InvalidNativeReaderEpoch, scan(a, io, path, binding, inode));
}

test "lite reader epochs accept unknown directory entry types using descriptor metadata" {
    const Probe = struct {
        var changed: usize = 0;
        fn read(userdata: ?*anyopaque, reader: *std.Io.Dir.Reader, entries: []std.Io.Dir.Entry) std.Io.Dir.Reader.Error!usize {
            const count = try std.Options.debug_io.vtable.dirRead(userdata, reader, entries);
            for (entries[0..count]) |*entry| {
                entry.kind = .unknown;
                changed += 1;
            }
            return count;
        }
    };
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/unknown-type.aflite", .{tmp.sub_path});
    defer a.free(path);
    const data = try std.Io.Dir.cwd().createFile(io, path, .{ .read = true });
    defer data.close(io);
    const inode = (try data.stat(io)).inode;
    const binding = try namespaceForPublication(a, io, path);
    const reader = try register(a, io, path, binding, inode, 17, true);
    defer reader.close(a, io);
    const expected = try scan(a, io, path, binding, inode);
    var vtable = std.Options.debug_io.vtable.*;
    vtable.dirRead = Probe.read;
    const unknown_io: std.Io = .{ .userdata = std.Options.debug_io.userdata, .vtable = &vtable };
    Probe.changed = 0;
    try std.testing.expectEqual(expected, try scan(a, unknown_io, path, binding, inode));
    try std.testing.expect(Probe.changed != 0);
    // Unknown must not turn a non-file entry into an accepted marker either.
    try reader.directory.createDir(io, "00000000000000000000000000000000", .fromMode(0o700));
    try std.testing.expectError(error.InvalidNativeReaderEpoch, scan(a, unknown_io, path, binding, inode));
}

test "lite reader epochs guard cross-generation registration and reject replaced namespaces" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/generation.aflite", .{tmp.sub_path});
    defer a.free(path);
    const data = try std.Io.Dir.cwd().createFile(io, path, .{ .read = true });
    defer data.close(io);
    const inode = (try data.stat(io)).inode;
    const binding = try namespaceForPublication(a, io, path);
    const namespace = try Namespace.open(a, io, path, false, true);
    defer namespace.close(a, io);
    try std.testing.expect(try namespace.guard.tryLock(io, .shared));
    // A different generation's data lock cannot protect marker initialization.
    // Its namespace guard must prevent this scan from reaping the marker.
    try std.testing.expectError(error.WouldBlock, scan(a, io, path, binding, inode + 1));
    namespace.guard.unlock(io);
    try std.testing.expect(try namespace.guard.tryLock(io, .exclusive));
    try std.testing.expectError(error.UnsafeNativeReaderAlias, register(a, io, path, binding, inode, 1, true));
    namespace.guard.unlock(io);
    const empty = try scan(a, io, path, binding, inode);
    try std.testing.expectEqual(@as(?u64, null), empty.data);
    // Recreating the identity cannot authorize reuse under an existing header.
    var changed: [32]u8 = undefined;
    try io.randomSecure(&changed);
    try namespace.guard.writePositionalAll(io, &changed, 0);
    try std.testing.expectError(error.UnsafeNativeReaderAlias, register(a, io, path, binding, inode, 1, true));
    try std.testing.expectError(error.WouldBlock, scan(a, io, path, binding, inode));
    try namespace.directory.deleteFile(io, "identity");
    try std.testing.expectError(error.UnsafeNativeReaderAlias, register(a, io, path, binding, inode, 1, true));
    try std.testing.expectError(error.WouldBlock, scan(a, io, path, binding, inode));
    try std.testing.expectError(error.FileNotFound, namespace.directory.access(io, "identity", .{}));
}

test "lite reader epochs cached namespace matches an uncached scan and tracks membership changes" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/cached.aflite", .{tmp.sub_path});
    defer a.free(path);
    const data = try std.Io.Dir.cwd().createFile(io, path, .{ .read = true });
    defer data.close(io);
    const inode = (try data.stat(io)).inode;
    const binding = try namespaceForPublication(a, io, path);

    // Opened once, with nobody registered: scanCached must agree with scan.
    const cached = try openCached(a, io, path, binding);
    defer cached.close(a, io);
    try std.testing.expectEqual(try scan(a, io, path, binding, inode), try scanCached(io, cached, inode));
    const empty_fp = try directoryFingerprint(io, cached);

    // A registration -- exactly what another process's `register` call does
    // -- must change the fingerprint (it creates a file in this directory),
    // so a cache holding `empty_fp` knows it can no longer trust an "empty"
    // result without rescanning.
    const reader = try register(a, io, path, binding, inode, 10, false);
    var reader_open = true;
    defer if (reader_open) reader.close(a, io);
    const after_join_fp = try directoryFingerprint(io, cached);
    try std.testing.expect(after_join_fp != empty_fp);
    try std.testing.expectEqual(try scan(a, io, path, binding, inode), try scanCached(io, cached, inode));
    try std.testing.expectEqual(@as(?u64, 10), (try scanCached(io, cached, inode)).data);

    // A departure (the reader's Lease closing, deleting its own marker) also
    // changes the fingerprint, even though missing it is always safe (it
    // can only make a cached frontier more conservative, never unsafe).
    reader.close(a, io);
    reader_open = false;
    const after_leave_fp = try directoryFingerprint(io, cached);
    try std.testing.expect(after_leave_fp != after_join_fp);
    try std.testing.expectEqual(@as(?u64, null), (try scanCached(io, cached, inode)).data);
}

test "lite reader epochs parentUnchanged detects an externally replaced parent directory" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const parent = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/original-parent", .{tmp.sub_path});
    defer a.free(parent);
    try std.Io.Dir.cwd().createDir(io, parent, .fromMode(0o700));
    const path = try std.fs.path.join(a, &.{ parent, "cached-parent.aflite" });
    defer a.free(path);
    const data = try std.Io.Dir.cwd().createFile(io, path, .{ .read = true });
    defer data.close(io);
    const binding = try namespaceForPublication(a, io, path);
    const cached = try openCached(a, io, path, binding);
    defer cached.close(a, io);
    // Nothing has moved yet: a fresh lookup from `path`'s current parent
    // still resolves to the same parent this cache opened against.
    try std.testing.expect(parentUnchanged(io, path, cached));

    // Move the whole parent elsewhere, then recreate an empty directory at
    // the same name and hard-link the data file back under the original
    // path -- the data file itself is unchanged, but the parent a fresh
    // lookup finds is a brand new directory.
    const moved = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/moved-parent", .{tmp.sub_path});
    defer a.free(moved);
    try std.Io.Dir.cwd().rename(parent, std.Io.Dir.cwd(), moved, io);
    try std.Io.Dir.cwd().createDir(io, parent, .fromMode(0o700));
    const moved_file = try std.fs.path.join(a, &.{ moved, "cached-parent.aflite" });
    defer a.free(moved_file);
    try std.Io.Dir.cwd().hardLink(moved_file, std.Io.Dir.cwd(), path, io, .{});
    try std.Io.Dir.cwd().deleteFile(io, moved_file);

    try std.testing.expect(!parentUnchanged(io, path, cached));
    // A cache that keeps treating this as unchanged is exactly the bug: a
    // fresh `openCached` from here must not find the old (relocated)
    // namespace directory inside the new, empty parent.
    try std.testing.expectError(error.WouldBlock, openCached(a, io, path, binding));
}
