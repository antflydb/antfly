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

const std = @import("std");
const Allocator = std.mem.Allocator;
const fs_paths = @import("antfly_runtime_fs").fs_paths;
const artifact_store = @import("store.zig");
const CancellationToken = @import("antfly_cancellation").CancellationToken;

// Serialize journal initialization and deterministic staging across separate
// in-process handles as well as external owners. Stripes keep unrelated
// publications independent; interprocess file locks remain in place.
var journal_mutexes: [64]std.Io.Mutex = @splat(.init);
var content_mutexes: [64]std.Io.Mutex = @splat(.init);

pub const FsStore = struct {
    const verified_file_cache_limit: usize = 4096;

    const VerifiedFile = struct {
        inode: std.Io.File.INode,
        byte_len: u64,
        mtime_ns: i128,
        ctime_ns: i128,

        fn fromStat(file_stat: std.Io.File.Stat) VerifiedFile {
            return .{
                .inode = file_stat.inode,
                .byte_len = file_stat.size,
                .mtime_ns = file_stat.mtime.toNanoseconds(),
                .ctime_ns = file_stat.ctime.toNanoseconds(),
            };
        }

        fn matchesStat(self: VerifiedFile, file_stat: std.Io.File.Stat) bool {
            return self.inode == file_stat.inode and
                self.byte_len == file_stat.size and
                self.mtime_ns == file_stat.mtime.toNanoseconds() and
                self.ctime_ns == file_stat.ctime.toNanoseconds();
        }
    };

    retention: @import("retained_closure.zig") = .{},
    alloc: Allocator,
    root_dir: []u8,
    inventory_root: []u8,
    durable_dirs: @import("objectstore").durable_directory.Cache = .{},
    verified_mu: std.atomic.Mutex = .unlocked,
    journal_backfills: std.atomic.Value(usize) = .init(0),
    journal_records_read: std.atomic.Value(usize) = .init(0),
    temporary_records_read: std.atomic.Value(usize) = .init(0),
    migration_entries_read: std.atomic.Value(usize) = .init(0),
    journal_attempt_scans: std.atomic.Value(u64) = .init(0),
    verified_files: std.StringHashMapUnmanaged(VerifiedFile) = .empty,

    pub fn init(alloc: Allocator, root_dir: []const u8) !FsStore {
        var io_impl = threadedIo();
        defer io_impl.deinit();
        var durable_dirs: @import("objectstore").durable_directory.Cache = .{};
        errdefer durable_dirs.deinit(alloc);
        try durable_dirs.ensure(alloc, io_impl.io(), root_dir);
        var store = try initExisting(alloc, root_dir);
        store.durable_dirs = durable_dirs;
        return store;
    }

    /// Borrow the exact payload directory without provisioning it.
    pub fn initExisting(alloc: Allocator, root_dir: []const u8) !FsStore {
        const inventory = try std.fs.path.join(alloc, &.{ root_dir, ".upload-inventory" });
        defer alloc.free(inventory);
        return initExistingWithInventory(alloc, root_dir, inventory);
    }
    pub fn initExistingWithInventory(alloc: Allocator, root_dir: []const u8, inventory_root: []const u8) !FsStore {
        const root = try alloc.dupe(u8, root_dir);
        errdefer alloc.free(root);
        return .{ .alloc = alloc, .root_dir = root, .inventory_root = try alloc.dupe(u8, inventory_root) };
    }
    pub fn registerScopedUpload(self: *FsStore, scope: artifact_store.UploadScope, checksum: []const u8, cancellation: CancellationToken) !void {
        try scope.validate();
        try artifact_store.validateSha256Checksum(checksum);
        const path = try std.fs.path.join(self.alloc, &.{ self.root_dir, "graph", &std.fmt.bytesToHex(&scope.domain, .lower), &std.fmt.bytesToHex(&scope.attempt, .lower) });
        defer self.alloc.free(path);
        var io_impl = threadedIo();
        defer io_impl.deinit();
        const io = io_impl.io();
        try self.durable_dirs.ensure(self.alloc, io, path);
        const attempt = try std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true, .follow_symlinks = false });
        defer attempt.close(io);
        const journal = try self.uploadJournal(io, attempt, scope, checksum, null, cancellation);
        journal.close(io);
    }

    pub fn deinit(self: *FsStore) void {
        self.retention.deinit(self.alloc);
        self.durable_dirs.deinit(self.alloc);
        lockAtomic(&self.verified_mu);
        var it = self.verified_files.keyIterator();
        while (it.next()) |key| self.alloc.free(key.*);
        self.verified_files.deinit(self.alloc);
        self.verified_mu.unlock();
        self.alloc.free(self.root_dir);
        self.alloc.free(self.inventory_root);
        self.* = undefined;
    }

    pub fn artifactStore(self: *FsStore) artifact_store.ArtifactStore {
        return .{
            .allocator = self.alloc,
            .ptr = self,
            .vtable = &vtable,
        };
    }

    pub fn put(self: *FsStore, alloc: Allocator, contents: []const u8) !artifact_store.ArtifactMetadata {
        return try self.putWithCancellation(alloc, contents, .none);
    }

    pub fn putWithCancellation(self: *FsStore, alloc: Allocator, contents: []const u8, cancellation: CancellationToken) !artifact_store.ArtifactMetadata {
        return self.putInScope(alloc, null, contents, cancellation);
    }

    fn putInScope(self: *FsStore, alloc: Allocator, scope: ?artifact_store.UploadScope, contents: []const u8, cancellation: CancellationToken) !artifact_store.ArtifactMetadata {
        try cancellation.check();
        const checksum = try sha256StringWithCancellationAlloc(alloc, contents, cancellation);
        errdefer alloc.free(checksum);
        const artifact_id = if (scope) |value| scoped: {
            const id = try value.artifactId(checksum);
            break :scoped try alloc.dupe(u8, &id);
        } else try makeArtifactIdAlloc(alloc, checksum);
        errdefer alloc.free(artifact_id);

        var write_io = threadedIo();
        defer write_io.deinit();
        const content_mutex = &content_mutexes[std.hash.Wyhash.hash(0, artifact_id) % content_mutexes.len];
        try content_mutex.lock(write_io.io());
        defer content_mutex.unlock(write_io.io());
        const path = try pathForArtifactIdAlloc(self.alloc, self.root_dir, artifact_id);
        defer self.alloc.free(path);

        const existing_valid = if (fileExists(path)) blk: {
            verifyPathContent(path, @intCast(contents.len), checksum, cancellation) catch |err| switch (err) {
                error.ArtifactIntegrityMismatch, error.FileNotFound => break :blk false,
                else => return err,
            };
            break :blk true;
        } else false;
        if (!existing_valid) {
            var directory_io = threadedIo();
            defer directory_io.deinit();
            try self.durable_dirs.ensure(self.alloc, directory_io.io(), std.fs.path.dirname(path) orelse ".");
            if (scope != null) {
                const attempt = try std.Io.Dir.cwd().openDir(directory_io.io(), std.fs.path.dirname(path).?, .{ .iterate = true, .follow_symlinks = false });
                defer attempt.close(directory_io.io());
                var journal = try self.uploadJournal(directory_io.io(), attempt, scope.?, checksum, null, cancellation);
                journal.close(directory_io.io());
            }
            if (scope) |value| {
                // The journal is durable before staging. A per-content lock
                // permits one deterministic staging name, discoverable by the
                // ordinary bounded journal sweep even after a process crash.
                const lock_path = try std.fs.path.join(self.alloc, &.{ self.inventory_root, &std.fmt.bytesToHex(&value.domain, .lower), &std.fmt.bytesToHex(&value.attempt, .lower), checksum });
                defer self.alloc.free(lock_path);
                const lock = try std.Io.Dir.cwd().createFile(directory_io.io(), lock_path, .{ .truncate = false });
                defer lock.close(directory_io.io());
                try lock.lock(directory_io.io(), .exclusive);
                defer lock.unlock(directory_io.io());
                try writeFileAtomicallyAt(path, contents, cancellation, "pending-v2");
            } else try writeFileAtomicallyWithCancellation(path, contents, cancellation);
        }

        return .{
            .artifact_id = artifact_id,
            .byte_len = @intCast(contents.len),
            .checksum = checksum,
        };
    }

    pub fn getAlloc(self: *FsStore, alloc: Allocator, artifact_id: []const u8) ![]u8 {
        return try self.getAllocWithCancellation(alloc, artifact_id, .none);
    }

    pub fn getAllocWithCancellation(
        self: *FsStore,
        alloc: Allocator,
        artifact_id: []const u8,
        cancellation: CancellationToken,
    ) ![]u8 {
        try cancellation.check();
        const path = try pathForArtifactIdAlloc(self.alloc, self.root_dir, artifact_id);
        defer self.alloc.free(path);
        return try readFileAllocWithCancellation(alloc, path, cancellation);
    }

    pub fn getRangeAlloc(self: *FsStore, alloc: Allocator, artifact_id: []const u8, offset: u64, len: usize) ![]u8 {
        return try self.getRangeAllocWithCancellation(alloc, artifact_id, offset, len, .none);
    }

    pub fn getRangeAllocWithCancellation(
        self: *FsStore,
        alloc: Allocator,
        artifact_id: []const u8,
        offset: u64,
        len: usize,
        cancellation: CancellationToken,
    ) ![]u8 {
        try cancellation.check();
        const path = try pathForArtifactIdAlloc(self.alloc, self.root_dir, artifact_id);
        defer self.alloc.free(path);
        return try readFileRangeAllocWithCancellation(alloc, path, offset, len, cancellation);
    }

    pub fn getVerifiedRangeAllocWithCancellation(
        self: *FsStore,
        alloc: Allocator,
        artifact_id: []const u8,
        expected_byte_len: u64,
        expected_checksum: []const u8,
        offset: u64,
        len: usize,
        cancellation: CancellationToken,
    ) ![]u8 {
        return self.getVerifiedRangeWithBudget(alloc, artifact_id, expected_byte_len, expected_checksum, offset, len, cancellation, null);
    }

    fn getVerifiedRangeWithBudget(
        self: *FsStore,
        alloc: Allocator,
        artifact_id: []const u8,
        expected_byte_len: u64,
        expected_checksum: []const u8,
        offset: u64,
        len: usize,
        cancellation: CancellationToken,
        remaining: ?*u64,
    ) ![]u8 {
        try cancellation.check();
        const checksum = try artifact_store.sha256ChecksumFromArtifactId(artifact_id);
        if (!std.mem.eql(u8, checksum, expected_checksum)) return error.ArtifactIntegrityMismatch;
        const end = std.math.add(u64, offset, std.math.cast(u64, len) orelse return error.InvalidRange) catch return error.InvalidRange;
        if (end > expected_byte_len) return error.InvalidRange;
        const path = try pathForArtifactIdAlloc(self.alloc, self.root_dir, artifact_id);
        defer self.alloc.free(path);

        var io_impl = threadedIo();
        defer io_impl.deinit();
        const io = io_impl.io();
        const file = if (std.fs.path.isAbsolute(path))
            try std.Io.Dir.openFileAbsolute(io, path, .{})
        else
            try std.Io.Dir.cwd().openFile(io, path, .{});
        defer file.close(io);
        const before = try file.stat(io);
        if (before.size != expected_byte_len) return error.ArtifactIntegrityMismatch;
        const verified = VerifiedFile.fromStat(before);
        if (!self.isVerifiedFile(artifact_id, verified)) {
            if (remaining) |budget| try artifact_store.chargeReadBudget(budget, expected_byte_len);
            try verifyOpenFileContent(file, io, expected_byte_len, expected_checksum, cancellation);
            const after = try file.stat(io);
            if (!verified.matchesStat(after)) {
                return error.ArtifactIntegrityMismatch;
            }
            try self.rememberVerifiedFile(artifact_id, verified);
        }
        const payload = try readOpenFileRangeAllocWithCancellation(alloc, file, io, offset, len, cancellation);
        errdefer alloc.free(payload);
        const after_read = try file.stat(io);
        if (!verified.matchesStat(after_read)) {
            self.forgetVerifiedFile(artifact_id);
            return error.ArtifactIntegrityMismatch;
        }
        try cancellation.check();
        return payload;
    }

    pub fn stat(self: *FsStore, alloc: Allocator, artifact_id: []const u8) !artifact_store.ArtifactMetadata {
        return try self.statWithCancellation(alloc, artifact_id, .none);
    }

    pub fn statWithCancellation(self: *FsStore, alloc: Allocator, artifact_id: []const u8, cancellation: CancellationToken) !artifact_store.ArtifactMetadata {
        try cancellation.check();
        const checksum_value = try artifact_store.sha256ChecksumFromArtifactId(artifact_id);
        const checksum = try alloc.dupe(u8, checksum_value);
        errdefer alloc.free(checksum);
        const artifact_id_copy = try alloc.dupe(u8, artifact_id);
        errdefer alloc.free(artifact_id_copy);
        const path = try pathForArtifactIdAlloc(alloc, self.root_dir, artifact_id);
        defer alloc.free(path);

        var io_impl = threadedIo();
        defer io_impl.deinit();
        const file_stat = try std.Io.Dir.cwd().statFile(io_impl.io(), path, .{});
        try cancellation.check();

        return .{
            .artifact_id = artifact_id_copy,
            .byte_len = @intCast(file_stat.size),
            .checksum = checksum,
        };
    }

    pub fn verifyContent(
        self: *FsStore,
        _: Allocator,
        artifact_id: []const u8,
        expected_byte_len: u64,
        expected_checksum: []const u8,
        cancellation: CancellationToken,
    ) !void {
        const checksum = try artifact_store.sha256ChecksumFromArtifactId(artifact_id);
        if (!std.mem.eql(u8, checksum, expected_checksum)) return error.ArtifactIntegrityMismatch;
        const path = try pathForArtifactIdAlloc(self.alloc, self.root_dir, artifact_id);
        defer self.alloc.free(path);
        var io_impl = threadedIo();
        defer io_impl.deinit();
        const before = try std.Io.Dir.cwd().statFile(io_impl.io(), path, .{});
        if (before.size != expected_byte_len) return error.ArtifactIntegrityMismatch;
        const verified = VerifiedFile.fromStat(before);
        lockAtomic(&self.verified_mu);
        if (self.verified_files.get(artifact_id)) |cached| {
            if (std.meta.eql(cached, verified)) {
                self.verified_mu.unlock();
                return;
            }
        }
        self.verified_mu.unlock();

        try verifyPathContent(path, expected_byte_len, expected_checksum, cancellation);
        const after = try std.Io.Dir.cwd().statFile(io_impl.io(), path, .{});
        if (!verified.matchesStat(after)) return error.ArtifactIntegrityMismatch;
        try self.rememberVerifiedFile(artifact_id, verified);
    }

    fn isVerifiedFile(self: *FsStore, artifact_id: []const u8, verified: VerifiedFile) bool {
        lockAtomic(&self.verified_mu);
        defer self.verified_mu.unlock();
        const cached = self.verified_files.get(artifact_id) orelse return false;
        return std.meta.eql(cached, verified);
    }

    fn rememberVerifiedFile(self: *FsStore, artifact_id: []const u8, verified: VerifiedFile) !void {
        const owned_id = try self.alloc.dupe(u8, artifact_id);
        errdefer self.alloc.free(owned_id);
        lockAtomic(&self.verified_mu);
        defer self.verified_mu.unlock();
        if (!self.verified_files.contains(artifact_id) and self.verified_files.count() >= verified_file_cache_limit) {
            var iterator = self.verified_files.keyIterator();
            if (iterator.next()) |victim| {
                const removed = self.verified_files.fetchRemove(victim.*).?;
                self.alloc.free(removed.key);
            }
        }
        const gop = try self.verified_files.getOrPut(self.alloc, owned_id);
        if (gop.found_existing) self.alloc.free(owned_id) else gop.key_ptr.* = owned_id;
        gop.value_ptr.* = verified;
    }

    fn forgetVerifiedFile(self: *FsStore, artifact_id: []const u8) void {
        lockAtomic(&self.verified_mu);
        defer self.verified_mu.unlock();
        if (self.verified_files.fetchRemove(artifact_id)) |removed| self.alloc.free(removed.key);
    }

    pub fn delete(self: *FsStore, artifact_id: []const u8) !void {
        if (try artifact_store.uploadScopeFromArtifactId(artifact_id)) |scope| try @import("retained_closure.zig").invalidate(self, scope, .none);
        const path = try pathForArtifactIdAlloc(self.alloc, self.root_dir, artifact_id);
        defer self.alloc.free(path);
        try deleteFile(path);
        try @import("retained_closure.zig").forget(self, artifact_id);
        self.forgetVerifiedFile(artifact_id);
    }

    const journal_header = "AFUPLOAD1\n";
    const temporary_header = "AFTEMP1\n";
    const temporary_name_len = 101;
    const Journal = struct {
        file: std.Io.File,
        temporaries: std.Io.File,
        identity: [16]u8,
        fn close(self: Journal, io: std.Io) void {
            self.temporaries.close(io);
            self.file.close(io);
        }
    };
    const JournalGuard = struct {
        io: std.Io,
        mutex: *std.Io.Mutex,
        file: std.Io.File,
        fn deinit(self: *JournalGuard) void {
            self.file.unlock(self.io);
            self.file.close(self.io);
            self.mutex.unlock(self.io);
        }
    };
    // These 64 lock files are permanent. Deleting an attempt must never
    // replace the lock inode while another initializer is waiting on it.
    fn journalGuard(self: *FsStore, io: std.Io, scope: artifact_store.UploadScope, cancellation: CancellationToken) !JournalGuard {
        var hash = std.hash.Wyhash.init(0);
        hash.update(&scope.domain);
        hash.update(&scope.attempt);
        const stripe = hash.final() % journal_mutexes.len;
        const mutex = &journal_mutexes[stripe];
        while (!mutex.tryLock()) {
            try cancellation.check();
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        errdefer mutex.unlock(io);
        try cancellation.check();
        const root = try std.fs.path.join(self.alloc, &.{ self.inventory_root, ".journal-locks-v1" });
        defer self.alloc.free(root);
        try self.durable_dirs.ensure(self.alloc, io, root);
        const path = try std.fmt.allocPrint(self.alloc, "{s}/{x:0>2}", .{ root, stripe });
        defer self.alloc.free(path);
        const file = try std.Io.Dir.cwd().createFile(io, path, .{ .truncate = false });
        errdefer file.close(io);
        while (!try file.tryLock(io, .exclusive)) {
            try cancellation.check();
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        errdefer file.unlock(io);
        try cancellation.check();
        return .{ .io = io, .mutex = mutex, .file = file };
    }
    fn legacyTemporary(name: []const u8) bool {
        return @import("fs_inventory_scan.zig").legacyTemporary(name);
    }
    fn readJournalIdentity(io: std.Io, directory: std.Io.Dir) !?[16]u8 {
        const file = directory.openFile(io, ".uploads-id-v1", .{}) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer file.close(io);
        var id: [16]u8 = undefined;
        if (try file.length(io) != id.len or try file.readPositionalAll(io, &id, 0) != id.len) return null;
        return id;
    }
    fn journalIdentityMatches(self: *FsStore, io: std.Io, scope: artifact_store.UploadScope, identity: [16]u8, cancellation: CancellationToken) !bool {
        var guard = try self.journalGuard(io, scope, cancellation);
        defer guard.deinit();
        const path = try std.fs.path.join(self.alloc, &.{ self.inventory_root, &std.fmt.bytesToHex(&scope.domain, .lower), &std.fmt.bytesToHex(&scope.attempt, .lower) });
        defer self.alloc.free(path);
        const dir = std.Io.Dir.cwd().openDir(io, path, .{}) catch |err| switch (err) {
            error.FileNotFound => return false,
            else => return err,
        };
        defer dir.close(io);
        dir.access(io, ".uploads-v1", .{}) catch |err| switch (err) {
            error.FileNotFound => return false,
            else => return err,
        };
        const current = (try readJournalIdentity(io, dir)) orelse return false;
        return std.mem.eql(u8, &current, &identity);
    }
    fn journalToken(self: *FsStore, name: [32]u8, identity: [16]u8, offset: u64, temporary_offset: u64) ![]u8 {
        return std.fmt.allocPrint(self.alloc, "{s}:{s}:{x:0>16}:{x:0>16}", .{ name, std.fmt.bytesToHex(&identity, .lower), offset, temporary_offset });
    }
    /// Append-before-payload keeps failed uploads discoverable. Both logs and
    /// their incarnation are installed under the same retirement guard.
    fn uploadJournal(self: *FsStore, io: std.Io, payloads: std.Io.Dir, scope: artifact_store.UploadScope, checksum: ?[]const u8, visitor: ?artifact_store.ScopedUploadVisitor, cancellation: CancellationToken) !Journal {
        var guard = try self.journalGuard(io, scope, cancellation);
        defer guard.deinit();
        const payload_path = try std.fs.path.join(self.alloc, &.{ self.root_dir, "graph", &std.fmt.bytesToHex(&scope.domain, .lower), &std.fmt.bytesToHex(&scope.attempt, .lower) });
        defer self.alloc.free(payload_path);
        const current = std.Io.Dir.cwd().openDir(io, payload_path, .{ .follow_symlinks = false }) catch |err| switch (err) {
            error.FileNotFound => return error.ArtifactUploadAttemptRetired,
            else => return err,
        };
        defer current.close(io);
        if ((try current.stat(io)).inode != (try payloads.stat(io)).inode) return error.ArtifactUploadAttemptRetired;
        const path = try std.fs.path.join(self.alloc, &.{ self.inventory_root, &std.fmt.bytesToHex(&scope.domain, .lower), &std.fmt.bytesToHex(&scope.attempt, .lower) });
        defer self.alloc.free(path);
        try self.durable_dirs.ensure(self.alloc, io, path);
        const attempt = try std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true, .follow_symlinks = false });
        defer attempt.close(io);
        if (attempt.access(io, ".retiring-v1", .{})) |_| return error.ArtifactUploadAttemptRetired else |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        }
        const lock = try attempt.createFile(io, ".uploads-lock", .{ .truncate = false });
        defer lock.close(io);
        while (!try lock.tryLock(io, .exclusive)) {
            try cancellation.check();
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        defer lock.unlock(io);
        try cancellation.check();
        const rebuild = if (attempt.access(io, ".uploads-v1", .{})) |_| false else |err| switch (err) {
            error.FileNotFound => true,
            else => return err,
        };
        const temp_exists = if (attempt.access(io, ".temporaries-v1", .{})) |_| true else |err| switch (err) {
            error.FileNotFound => false,
            else => return err,
        };
        var id = if (rebuild) null else try readJournalIdentity(io, attempt);
        if (rebuild or !temp_exists or id == null) {
            _ = try removeMaintenanceFile(io, attempt, ".uploads-id-v1", visitor, cancellation);
            try @import("fs_inventory_scan.zig").build(self, io, payloads, attempt, rebuild, visitor, cancellation);
            var identity: [16]u8 = undefined;
            io.random(&identity);
            const pending = try attempt.createFile(io, ".uploads-id-building", .{});
            defer pending.close(io);
            try pending.writePositionalAll(io, &identity, 0);
            try pending.sync(io);
            try attempt.rename(".uploads-id-building", attempt, ".uploads-id-v1", io);
            id = identity;
            try fs_paths.syncDirectoryHandlePortable(io, attempt);
        }
        const journal = try attempt.openFile(io, ".uploads-v1", .{ .mode = .read_write, .allow_directory = false });
        errdefer journal.close(io);
        var header: [journal_header.len]u8 = undefined;
        if (try journal.readPositionalAll(io, &header, 0) != header.len or !std.mem.eql(u8, &header, journal_header)) return error.InvalidArtifactUploadInventory;
        const length = try journal.length(io);
        if (length < journal_header.len) return error.InvalidArtifactUploadInventory;
        const complete = journal_header.len + (length - journal_header.len) / 64 * 64;
        if (length != complete) try journal.setLength(io, complete);
        const temporaries = try attempt.openFile(io, ".temporaries-v1", .{});
        errdefer temporaries.close(io);
        if (checksum) |value| {
            try artifact_store.validateSha256Checksum(value);
            try journal.writePositionalAll(io, value, complete);
            try journal.sync(io);
        }
        return .{ .file = journal, .temporaries = temporaries, .identity = id.? };
    }
    /// Offset continuations read each inventory record once. The attempt name
    /// is ordered; the record offset is stable across deletion and process
    /// restart. No filename sorting or suffix-wide rescanning is required.
    fn visitUploadJournal(self: *FsStore, domain: [32]u8, visitor: artifact_store.ScopedUploadVisitor, cancellation: CancellationToken) !void {
        if ((visitor.cleanup_staging or visitor.reclaim_retired_inventory) and visitor.fencing_cutoff == null) return error.InvalidArtifactUploadScope;
        var io_impl = threadedIo();
        defer io_impl.deinit();
        const io = io_impl.io();
        const path = try std.fs.path.join(self.alloc, &.{ self.root_dir, "graph", &std.fmt.bytesToHex(&domain, .lower) });
        defer self.alloc.free(path);
        const dir = std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true, .follow_symlinks = false }) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer dir.close(io);
        var after_attempt: ?[]const u8 = null;
        var after_identity: ?[16]u8 = null;
        var after_offset: u64 = journal_header.len;
        var after_temporary: u64 = temporary_header.len;
        if (visitor.continuation) |token| {
            if (token.len != 49 and token.len != 99) return error.InvalidArtifactEnumerationContinuation;
            if (token[32] != ':') return error.InvalidArtifactEnumerationContinuation;
            var decoded: [16]u8 = undefined;
            _ = std.fmt.hexToBytes(&decoded, token[0..32]) catch return error.InvalidArtifactEnumerationContinuation;
            after_attempt = token[0..32];
            if (token.len == 99) {
                if (token[65] != ':' or token[82] != ':') return error.InvalidArtifactEnumerationContinuation;
                _ = std.fmt.hexToBytes(&decoded, token[33..65]) catch return error.InvalidArtifactEnumerationContinuation;
                after_identity = decoded;
                after_offset = std.fmt.parseInt(u64, token[66..82], 16) catch return error.InvalidArtifactEnumerationContinuation;
                after_temporary = std.fmt.parseInt(u64, token[83..99], 16) catch return error.InvalidArtifactEnumerationContinuation;
                if (after_offset != std.math.maxInt(u64) and (after_offset < journal_header.len or (after_offset - journal_header.len) % 64 != 0)) return error.InvalidArtifactEnumerationContinuation;
                if (after_temporary != std.math.maxInt(u64) and (after_temporary < temporary_header.len or (after_temporary - temporary_header.len) % temporary_name_len != 0)) return error.InvalidArtifactEnumerationContinuation;
            } else {
                // Old tokens cannot prove an incarnation. Replay this one
                // attempt once and replace the token with an identified one.
                _ = std.fmt.parseInt(u64, token[33..49], 16) catch return error.InvalidArtifactEnumerationContinuation;
            }
        }
        const maximum = visitor.max_entries orelse 256;
        if (maximum == 0 or maximum > 65536) return error.InvalidArtifactEnumerationLimit;
        var visited: usize = 0;
        // Select a bounded page of attempts in one namespace scan. Selecting
        // the next minimum separately for every attempt makes a wide namespace
        // quadratic even though each upload journal itself is read linearly.
        const Candidate = [32]u8;
        const Order = struct {
            fn compare(_: void, left: Candidate, right: Candidate) std.math.Order {
                return std.mem.order(u8, &right, &left);
            }
            fn less(_: void, left: Candidate, right: Candidate) bool {
                return std.mem.order(u8, &left, &right) == .lt;
            }
        };
        var candidates = std.PriorityQueue(Candidate, void, Order.compare).initContext({});
        defer candidates.deinit(self.alloc);
        if (@import("builtin").is_test) _ = self.journal_attempt_scans.fetchAdd(1, .monotonic);
        var attempts = dir.iterate();
        while (try attempts.next(io)) |entry| {
            try cancellation.check();
            if (entry.kind != .directory or entry.name.len != 32) continue;
            var scope: artifact_store.UploadScope = .{ .domain = domain, .attempt = undefined };
            _ = std.fmt.hexToBytes(&scope.attempt, entry.name) catch continue;
            scope.validate() catch continue;
            if (scope.fencingToken() < visitor.fencing_floor or (visitor.fencing_cutoff != null and scope.fencingToken() >= visitor.fencing_cutoff.?)) continue;
            if (visitor.exclude_attempt) |excluded| if (std.mem.eql(u8, &excluded, &scope.attempt)) continue;
            if (visitor.only_attempt) |only| if (!std.mem.eql(u8, &only, &scope.attempt)) continue;
            if (after_attempt) |after| {
                if (std.mem.order(u8, entry.name, after) == .lt) continue;
                if (std.mem.eql(u8, entry.name, after) and after_offset == std.math.maxInt(u64)) {
                    if (after_identity) |identity| if (try self.journalIdentityMatches(io, scope, identity, cancellation)) continue;
                }
            }
            const name: Candidate = entry.name[0..32].*;
            if (candidates.items.len == maximum + 1) {
                if (std.mem.order(u8, &name, &candidates.peek().?) != .lt) continue;
                _ = candidates.pop();
            }
            try candidates.push(self.alloc, name);
        }
        std.mem.sort(Candidate, candidates.items, {}, Order.less);
        for (candidates.items[0..@min(maximum, candidates.items.len)]) |name| {
            var scope: artifact_store.UploadScope = .{ .domain = domain, .attempt = undefined };
            _ = try std.fmt.hexToBytes(&scope.attempt, &name);
            if (visitor.reclaim_retired_inventory and try self.retirementPending(io, scope)) {
                if (!try self.reclaimEmptyScopedAttempt(io, scope, visitor, cancellation)) {
                    if (visitor.retain_scope) |retain| try retain(visitor.ptr, scope);
                }
                const token = try self.journalToken(name, @splat(0), std.math.maxInt(u64), std.math.maxInt(u64));
                defer self.alloc.free(token);
                try visitor.checkpoint.?(visitor.ptr, token);
                continue;
            }
            var identity: [16]u8 = undefined;
            {
                const attempt = dir.openDir(io, &name, .{ .iterate = true, .follow_symlinks = false }) catch |err| switch (err) {
                    error.FileNotFound => continue,
                    else => return err,
                };
                defer attempt.close(io);
                const journal = self.uploadJournal(io, attempt, scope, null, visitor, cancellation) catch |err| switch (err) {
                    // Another initializer/collector can retire this handle
                    // after the candidate check. Retry rather than proving
                    // completion over its unfinished metadata retirement.
                    error.ArtifactUploadAttemptRetired => return error.ArtifactEnumerationPaused,
                    error.FileNotFound => continue,
                    else => return err,
                };
                defer journal.close(io);
                identity = journal.identity;
                const same = after_attempt != null and std.mem.eql(u8, &name, after_attempt.?) and after_identity != null and std.mem.eql(u8, &identity, &after_identity.?);
                var offset: u64 = if (same) after_offset else journal_header.len;
                var temporary_offset: u64 = if (same) after_temporary else temporary_header.len;
                const end = try journal.file.length(io);
                const temporary_end = try journal.temporaries.length(io);
                var temp_header: [temporary_header.len]u8 = undefined;
                if (try journal.temporaries.readPositionalAll(io, &temp_header, 0) != temp_header.len or !std.mem.eql(u8, &temp_header, temporary_header)) return error.InvalidArtifactUploadInventory;
                if (temporary_end < temporary_header.len or (temporary_end - temporary_header.len) % temporary_name_len != 0) return error.InvalidArtifactUploadInventory;
                if (offset > end or temporary_offset > temporary_end) {
                    offset = journal_header.len;
                    temporary_offset = temporary_header.len;
                }
                if (visitor.cleanup_staging) while (temporary_offset < temporary_end) {
                    try cancellation.check();
                    if (visited == maximum) return error.ArtifactEnumerationPaused;
                    var temporary: [temporary_name_len]u8 = undefined;
                    if (try journal.temporaries.readPositionalAll(io, &temporary, temporary_offset) != temporary.len or !legacyTemporary(&temporary)) return error.InvalidArtifactUploadInventory;
                    // Charge a journal work unit even if a prior turn already
                    // removed the file before persisting its cursor.
                    if (attempt.access(io, &temporary, .{})) |_| {
                        _ = try removeMaintenanceFile(io, attempt, &temporary, visitor, cancellation);
                    } else |err| switch (err) {
                        error.FileNotFound => try admitMaintenance(visitor, .inspect),
                        else => return err,
                    }
                    if (@import("builtin").is_test) _ = self.temporary_records_read.fetchAdd(1, .monotonic);
                    temporary_offset += temporary.len;
                    visited += 1;
                    const token = try self.journalToken(name, identity, offset, temporary_offset);
                    defer self.alloc.free(token);
                    try visitor.checkpoint.?(visitor.ptr, token);
                };
                while (offset < end) {
                    try cancellation.check();
                    if (visited == maximum) return error.ArtifactEnumerationPaused;
                    var suffix: [97]u8 = undefined;
                    @memcpy(suffix[0..32], &name);
                    suffix[32] = '/';
                    if (try journal.file.readPositionalAll(io, suffix[33..97], offset) != 64) return error.InvalidArtifactUploadInventory;
                    if (@import("builtin").is_test) _ = self.journal_records_read.fetchAdd(1, .monotonic);
                    try artifact_store.validateSha256Checksum(suffix[33..97]);
                    var did_work = false;
                    if (visitor.cleanup_staging) {
                        // The caller's persisted cutoff excludes active uploaders.
                        if (visitor.fencing_cutoff == null) return error.InvalidArtifactUploadScope;
                        var staging: [75]u8 = undefined;
                        @memcpy(staging[0..64], suffix[33..97]);
                        @memcpy(staging[64..], ".pending-v2");
                        did_work = try removeMaintenanceFile(io, attempt, &staging, visitor, cancellation);
                    }
                    // Backfill entries and interrupted uploads may already be gone.
                    if (attempt.access(io, suffix[33..97], .{})) |_| {
                        try artifact_store.visitScopedSuffix(domain, &suffix, visitor);
                        did_work = true;
                    } else |err| switch (err) {
                        error.FileNotFound => {},
                        else => return err,
                    }
                    if (visitor.reclaim_retired_inventory) {
                        if (attempt.access(io, suffix[33..97], .{})) |_| {} else |err| switch (err) {
                            error.FileNotFound => {
                                // Retire per-content locks in the same bounded
                                // journal quantum, before checkpointing it.
                                const lock_path = try std.fs.path.join(self.alloc, &.{ self.inventory_root, &std.fmt.bytesToHex(&domain, .lower), &name, suffix[33..97] });
                                defer self.alloc.free(lock_path);
                                const lock_dir = try std.Io.Dir.cwd().openDir(io, std.fs.path.dirname(lock_path).?, .{});
                                defer lock_dir.close(io);
                                did_work = (try removeMaintenanceFile(io, lock_dir, suffix[33..97], visitor, cancellation)) or did_work;
                            },
                            else => return err,
                        }
                    }
                    if (!did_work) try admitMaintenance(visitor, .inspect);
                    offset += 64;
                    visited += 1;
                    const token = try self.journalToken(name, identity, offset, temporary_offset);
                    defer self.alloc.free(token);
                    try visitor.checkpoint.?(visitor.ptr, token);
                }
            }
            if (visitor.reclaim_retired_inventory) {
                if (!try self.reclaimEmptyScopedAttempt(io, scope, visitor, cancellation)) {
                    if (visitor.retain_scope) |retain| try retain(visitor.ptr, scope);
                }
            }
            // Checkpoint completion only after retirement. If it is canceled,
            // retain the journal offset so the next turn retries this attempt.
            if (visitor.fencing_cutoff != null) {
                const token = try self.journalToken(name, identity, std.math.maxInt(u64), std.math.maxInt(u64));
                defer self.alloc.free(token);
                try visitor.checkpoint.?(visitor.ptr, token);
            }
        }
        if (candidates.items.len > maximum) return error.ArtifactEnumerationPaused;
    }

    fn visitScopedUploads(self: *FsStore, domain: [32]u8, visitor: artifact_store.ScopedUploadVisitor, cancellation: CancellationToken) !void {
        if (visitor.checkpoint != null) return self.visitUploadJournal(domain, visitor, cancellation);
        var io_impl = threadedIo();
        defer io_impl.deinit();
        const io = io_impl.io();
        const path = try std.fs.path.join(self.alloc, &.{ self.root_dir, "graph", &std.fmt.bytesToHex(&domain, .lower) });
        defer self.alloc.free(path);
        var dir = std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer dir.close(io);
        // Retain only one bounded lexical page. The continuation skips older
        // attempt directories; local directory scans allocate no per-object state.
        const Candidate = [97]u8;
        const Order = struct {
            fn compare(_: void, l: Candidate, r: Candidate) std.math.Order {
                return std.mem.order(u8, &r, &l);
            }
            fn less(_: void, l: Candidate, r: Candidate) bool {
                return std.mem.order(u8, &l, &r) == .lt;
            }
        };
        var candidates = std.PriorityQueue(Candidate, void, Order.compare).initContext({});
        defer candidates.deinit(self.alloc);
        var attempts = dir.iterate();
        while (try attempts.next(io)) |attempt| {
            try cancellation.check();
            if (attempt.kind != .directory or attempt.name.len != 32) continue;
            var encoded_attempt: [16]u8 = undefined;
            _ = std.fmt.hexToBytes(&encoded_attempt, attempt.name) catch continue;
            const fence = std.mem.readInt(u64, encoded_attempt[0..8], .big);
            if (visitor.exclude_attempt) |excluded| if (std.mem.eql(u8, &excluded, &encoded_attempt)) continue;
            if (visitor.only_attempt) |only| if (!std.mem.eql(u8, &only, &encoded_attempt)) continue;
            if (fence < visitor.fencing_floor) continue;
            if (visitor.fencing_cutoff) |cutoff| if (fence >= cutoff) continue;
            if (visitor.after_suffix) |after| if (after.len == 97 and std.mem.order(u8, attempt.name, after[0..32]) == .lt) continue;
            var attempt_dir = dir.openDir(io, attempt.name, .{ .iterate = true, .follow_symlinks = false }) catch |err| switch (err) {
                error.FileNotFound => continue,
                else => return err,
            };
            defer attempt_dir.close(io);
            var entries = attempt_dir.iterate();
            while (try entries.next(io)) |entry| {
                try cancellation.check();
                if (entry.kind != .file or entry.name.len != 64) continue;
                var suffix: [97]u8 = undefined;
                @memcpy(suffix[0..32], attempt.name);
                suffix[32] = '/';
                @memcpy(suffix[33..97], entry.name);
                if (visitor.after_suffix) |after| if (std.mem.order(u8, &suffix, after) != .gt) continue;
                if (visitor.max_entries) |maximum| {
                    if (maximum == 0 or maximum > 65536) return error.InvalidArtifactEnumerationLimit;
                    if (candidates.items.len == maximum + 1) {
                        if (std.mem.order(u8, &suffix, &candidates.peek().?) != .lt) continue;
                        _ = candidates.pop();
                    }
                    try candidates.push(self.alloc, suffix);
                } else try artifact_store.visitScopedSuffix(domain, &suffix, visitor);
            }
        }
        if (visitor.max_entries) |maximum| {
            std.mem.sort(Candidate, candidates.items, {}, Order.less);
            for (candidates.items[0..@min(maximum, candidates.items.len)]) |suffix| try artifact_store.visitScopedSuffix(domain, &suffix, visitor);
            if (candidates.items.len > maximum) return error.ArtifactEnumerationPaused;
        }
    }

    fn cleanupRetiredScopedTemporaries(ptr: *anyopaque, domain: [32]u8, floor: u64, cutoff: u64, cancellation: CancellationToken) !void {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        var io_impl = threadedIo();
        defer io_impl.deinit();
        const io = io_impl.io();
        const path = try std.fs.path.join(self.alloc, &.{ self.root_dir, "graph", &std.fmt.bytesToHex(&domain, .lower) });
        defer self.alloc.free(path);
        var dir = std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer dir.close(io);
        var attempts = dir.iterate();
        while (try attempts.next(io)) |entry| {
            try cancellation.check();
            if (entry.kind != .directory or entry.name.len != 32) continue;
            var scope: artifact_store.UploadScope = .{ .domain = domain, .attempt = undefined };
            _ = std.fmt.hexToBytes(&scope.attempt, entry.name) catch continue;
            scope.validate() catch continue;
            if (scope.fencingToken() < floor or scope.fencingToken() >= cutoff) continue;
            var attempt = dir.openDir(io, entry.name, .{ .iterate = true, .follow_symlinks = false }) catch |err| switch (err) {
                error.FileNotFound => continue,
                else => return err,
            };
            defer attempt.close(io);
            var files = attempt.iterate();
            var changed = false;
            while (try files.next(io)) |file| {
                try cancellation.check();
                if (file.kind != .file or file.name.len != 64 + 5 + 32 or !std.mem.eql(u8, file.name[64..69], ".tmp-")) continue;
                artifact_store.validateSha256Checksum(file.name[0..64]) catch continue;
                var nonce: [16]u8 = undefined;
                _ = std.fmt.hexToBytes(&nonce, file.name[69..]) catch continue;
                attempt.deleteFile(io, file.name) catch |err| switch (err) {
                    error.FileNotFound => continue,
                    else => return err,
                };
                changed = true;
            }
            if (changed) try fs_paths.syncDirectoryHandlePortable(io, attempt);
        }
    }

    fn admitMaintenance(visitor: ?artifact_store.ScopedUploadVisitor, operation: artifact_store.ScopedUploadVisitor.Maintenance) !void {
        if (visitor) |v| if (v.maintenance) |admit| try admit(v.ptr, operation);
    }
    fn removeMaintenanceFile(io: std.Io, dir: std.Io.Dir, name: []const u8, visitor: ?artifact_store.ScopedUploadVisitor, cancellation: CancellationToken) !bool {
        try cancellation.check();
        dir.access(io, name, .{}) catch |err| switch (err) {
            error.FileNotFound => return false,
            else => return err,
        };
        try admitMaintenance(visitor, .remove);
        dir.deleteFile(io, name) catch |err| switch (err) {
            error.FileNotFound => return true,
            else => return err,
        };
        // The next durable checkpoint must not skip a resurrected unlink.
        try fs_paths.syncDirectoryHandlePortable(io, dir);
        return true;
    }
    fn retirementPending(self: *FsStore, io: std.Io, scope: artifact_store.UploadScope) !bool {
        const path = try std.fs.path.join(self.alloc, &.{ self.inventory_root, &std.fmt.bytesToHex(&scope.domain, .lower), &std.fmt.bytesToHex(&scope.attempt, .lower), ".retiring-v1" });
        defer self.alloc.free(path);
        std.Io.Dir.cwd().access(io, path, .{}) catch |err| switch (err) {
            error.FileNotFound => return false,
            else => return err,
        };
        return true;
    }

    // Only call after processing an entire attempt behind a fencing cutoff.
    // Non-empty attempts include held payloads and are always preserved.
    fn reclaimEmptyScopedAttempt(self: *FsStore, io: std.Io, scope: artifact_store.UploadScope, visitor: ?artifact_store.ScopedUploadVisitor, cancellation: CancellationToken) !bool {
        var guard = try self.journalGuard(io, scope, cancellation);
        defer guard.deinit();
        try cancellation.check();
        const payload = try std.fs.path.join(self.alloc, &.{ self.root_dir, "graph", &std.fmt.bytesToHex(&scope.domain, .lower), &std.fmt.bytesToHex(&scope.attempt, .lower) });
        defer self.alloc.free(payload);
        const directory = std.Io.Dir.cwd().openDir(io, payload, .{ .iterate = true, .follow_symlinks = false }) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (directory) |dir| {
            defer dir.close(io);
            var entries = dir.iterate();
            if (try entries.next(io)) |entry| {
                if (legacyTemporary(entry.name)) {
                    const inventory_path = try std.fs.path.join(self.alloc, &.{ self.inventory_root, &std.fmt.bytesToHex(&scope.domain, .lower), &std.fmt.bytesToHex(&scope.attempt, .lower) });
                    defer self.alloc.free(inventory_path);
                    const metadata = try std.Io.Dir.cwd().openDir(io, inventory_path, .{});
                    defer metadata.close(io);
                    // Invalidating the identity alone forces a resumable
                    // rebuild; keep the old log until its replacement commits.
                    _ = try removeMaintenanceFile(io, metadata, ".uploads-id-v1", visitor, cancellation);
                    return error.ArtifactEnumerationPaused;
                }
                return false;
            }
        }
        const inventory = try std.fs.path.join(self.alloc, &.{ self.inventory_root, &std.fmt.bytesToHex(&scope.domain, .lower), &std.fmt.bytesToHex(&scope.attempt, .lower) });
        defer self.alloc.free(inventory);
        // Remove advisory metadata first. A crash leaves an empty payload
        // directory that enumeration can rediscover and backfill. Removing
        // the payload directory first would strand an invisible journal.
        try self.durable_dirs.invalidate(self.alloc, io, inventory);
        const metadata = std.Io.Dir.cwd().openDir(io, inventory, .{ .iterate = true, .follow_symlinks = false }) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (metadata) |dir| {
            defer dir.close(io);
            // A durable retirement marker lets future turns bypass journal
            // initialization even after some inventory files are removed.
            if (!try self.retirementPending(io, scope)) {
                try admitMaintenance(visitor, .inspect);
                const marker = try dir.createFile(io, ".retiring-v1", .{ .truncate = false });
                marker.close(io);
                try fs_paths.syncDirectoryHandlePortable(io, dir);
            }
            var entries = dir.iterate();
            while (try entries.next(io)) |entry| {
                try cancellation.check();
                if (entry.kind != .file or entry.name.len != 64) continue;
                artifact_store.validateSha256Checksum(entry.name) catch continue;
                _ = try removeMaintenanceFile(io, dir, entry.name, visitor, cancellation);
            }
            for ([_][]const u8{ ".uploads-building", ".uploads-lock", ".uploads-v1", ".uploads-id-v1", ".uploads-id-building", ".temporaries-building", ".temporaries-v1", ".inventory-scan-v1", ".inventory-scan-building" }) |name|
                _ = try removeMaintenanceFile(io, dir, name, visitor, cancellation);
            // Commit an empty namespace retirement as one final unit. The
            // marker and both empty directories are constant-size bookkeeping.
            try admitMaintenance(visitor, .remove);
            try @import("retained_closure.zig").forgetNamespace(self, scope);
            try dir.deleteFile(io, ".retiring-v1");
        }
        std.Io.Dir.cwd().deleteDir(io, inventory) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
        try fs_paths.syncDirPortable(io, std.fs.path.dirname(inventory).?);
        try self.durable_dirs.invalidate(self.alloc, io, payload);
        std.Io.Dir.cwd().deleteDir(io, payload) catch |err| switch (err) {
            error.FileNotFound => {},
            error.DirNotEmpty => return false,
            else => return err,
        };
        try fs_paths.syncDirPortable(io, std.fs.path.dirname(payload).?);
        return true;
    }

    fn reclaimRetiredScopedInventory(raw: *anyopaque, domain: [32]u8, floor: u64, cutoff: u64, cancellation: CancellationToken) !void {
        const self: *FsStore = @ptrCast(@alignCast(raw));
        var io_impl = threadedIo();
        defer io_impl.deinit();
        const io = io_impl.io();
        const path = try std.fs.path.join(self.alloc, &.{ self.inventory_root, &std.fmt.bytesToHex(&domain, .lower) });
        defer self.alloc.free(path);
        const inventory = std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true, .follow_symlinks = false }) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer inventory.close(io);
        var attempts = inventory.iterate();
        while (try attempts.next(io)) |entry| {
            try cancellation.check();
            if (entry.kind != .directory or entry.name.len != 32) continue;
            var scope: artifact_store.UploadScope = .{ .domain = domain, .attempt = undefined };
            _ = std.fmt.hexToBytes(&scope.attempt, entry.name) catch continue;
            scope.validate() catch continue;
            if (scope.fencingToken() < floor or scope.fencingToken() >= cutoff) continue;
            _ = self.reclaimEmptyScopedAttempt(io, scope, null, cancellation) catch |err| switch (err) {
                // This API only retires empty namespaces. Journal visitors
                // handle resumable temporary discovery on their next turn.
                error.ArtifactEnumerationPaused => continue,
                else => return err,
            };
        }
    }

    fn retentionWitness(raw: *anyopaque, scope: artifact_store.UploadScope, token: CancellationToken) !?artifact_store.RetentionWitness {
        return @import("retained_closure.zig").witness(@ptrCast(@alignCast(raw)), scope, token);
    }
    fn retainedClosure(raw: *anyopaque, id: []const u8, token: CancellationToken) !bool {
        return @import("retained_closure.zig").retained(@ptrCast(@alignCast(raw)), id, token);
    }
    fn rememberClosure(raw: *anyopaque, id: []const u8, witnesses: []const artifact_store.RetentionWitness, token: CancellationToken) !void {
        return @import("retained_closure.zig").remember(@ptrCast(@alignCast(raw)), id, witnesses, token);
    }
    fn inheritClosure(raw: *anyopaque, id: []const u8, previous: ?[]const u8, scope: artifact_store.UploadScope, token: CancellationToken) !void {
        const self: *FsStore = @ptrCast(@alignCast(raw));
        return self.retention.inherit(self, id, previous, scope, token);
    }
    fn sealClosures(raw: *anyopaque, token: CancellationToken) !void {
        const self: *FsStore = @ptrCast(@alignCast(raw));
        return self.retention.seal(self, token);
    }

    const vtable: artifact_store.ArtifactStore.VTable = .{
        .retention_witness = retentionWitness,
        .retained_closure = retainedClosure,
        .remember_closure = rememberClosure,
        .inherit_closure = inheritClosure,
        .seal_closures = sealClosures,
        .deinit = erasedDeinit,
        .put = erasedPut,
        .put_with_cancellation = erasedPutWithCancellation,
        .put_scoped = erasedPutScoped,
        .visit_scoped_uploads = erasedVisitScopedUploads,
        .cleanup_retired_scoped_temporaries = cleanupRetiredScopedTemporaries,
        .reclaim_retired_scoped_inventory = reclaimRetiredScopedInventory,
        .get_alloc = erasedGetAlloc,
        .get_alloc_with_cancellation = erasedGetAllocWithCancellation,
        .get_range_alloc = erasedGetRangeAlloc,
        .get_range_alloc_with_cancellation = erasedGetRangeAllocWithCancellation,
        .get_verified_range_alloc_with_cancellation = erasedGetVerifiedRangeAllocWithCancellation,
        .get_verified_range_alloc_with_budget = erasedGetVerifiedRangeWithBudget,
        .stat = erasedStat,
        .stat_with_cancellation = erasedStatWithCancellation,
        .verify_content = erasedVerifyContent,
        .delete = erasedDelete,
    };

    fn erasedDeinit(_: Allocator, ptr: *anyopaque) void {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    fn erasedPut(ptr: *anyopaque, alloc: Allocator, contents: []const u8) !artifact_store.ArtifactMetadata {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.put(alloc, contents);
    }

    fn erasedPutWithCancellation(ptr: *anyopaque, alloc: Allocator, contents: []const u8, cancellation: CancellationToken) !artifact_store.ArtifactMetadata {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.putWithCancellation(alloc, contents, cancellation);
    }

    fn erasedPutScoped(ptr: *anyopaque, alloc: Allocator, scope: artifact_store.UploadScope, contents: []const u8, cancellation: CancellationToken) !artifact_store.ArtifactMetadata {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return self.putInScope(alloc, scope, contents, cancellation);
    }

    fn erasedVisitScopedUploads(ptr: *anyopaque, domain: [32]u8, visitor: artifact_store.ScopedUploadVisitor, cancellation: CancellationToken) !void {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return self.visitScopedUploads(domain, visitor, cancellation);
    }

    fn erasedGetAlloc(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8) ![]u8 {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.getAlloc(alloc, artifact_id);
    }

    fn erasedGetAllocWithCancellation(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8, cancellation: CancellationToken) ![]u8 {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.getAllocWithCancellation(alloc, artifact_id, cancellation);
    }

    fn erasedGetRangeAlloc(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8, offset: u64, len: usize) ![]u8 {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.getRangeAlloc(alloc, artifact_id, offset, len);
    }

    fn erasedGetRangeAllocWithCancellation(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8, offset: u64, len: usize, cancellation: CancellationToken) ![]u8 {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.getRangeAllocWithCancellation(alloc, artifact_id, offset, len, cancellation);
    }

    fn erasedGetVerifiedRangeAllocWithCancellation(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8, expected_byte_len: u64, expected_checksum: []const u8, offset: u64, len: usize, cancellation: CancellationToken) ![]u8 {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.getVerifiedRangeAllocWithCancellation(alloc, artifact_id, expected_byte_len, expected_checksum, offset, len, cancellation);
    }

    fn erasedStat(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8) !artifact_store.ArtifactMetadata {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.stat(alloc, artifact_id);
    }

    fn erasedStatWithCancellation(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8, cancellation: CancellationToken) !artifact_store.ArtifactMetadata {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return try self.statWithCancellation(alloc, artifact_id, cancellation);
    }

    fn erasedVerifyContent(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8, expected_byte_len: u64, expected_checksum: []const u8, cancellation: CancellationToken) !void {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        try self.verifyContent(alloc, artifact_id, expected_byte_len, expected_checksum, cancellation);
    }

    fn erasedGetVerifiedRangeWithBudget(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8, byte_len: u64, checksum: []const u8, offset: u64, len: usize, cancellation: CancellationToken, remaining: *u64) ![]u8 {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        return self.getVerifiedRangeWithBudget(alloc, artifact_id, byte_len, checksum, offset, len, cancellation, remaining);
    }

    fn erasedDelete(ptr: *anyopaque, artifact_id: []const u8) !void {
        const self: *FsStore = @ptrCast(@alignCast(ptr));
        try self.delete(artifact_id);
    }
};

fn verifyPathContent(path: []const u8, expected_byte_len: u64, expected_checksum: []const u8, cancellation: CancellationToken) !void {
    try cancellation.check();
    var io_impl = threadedIo();
    defer io_impl.deinit();
    var file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io_impl.io(), path, .{})
    else
        try std.Io.Dir.cwd().openFile(io_impl.io(), path, .{});
    defer file.close(io_impl.io());
    return try verifyOpenFileContent(file, io_impl.io(), expected_byte_len, expected_checksum, cancellation);
}

fn verifyOpenFileContent(file: std.Io.File, io: std.Io, expected_byte_len: u64, expected_checksum: []const u8, cancellation: CancellationToken) !void {
    var reader = file.reader(io, &.{});
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    var buffer: [1024 * 1024]u8 = undefined;
    var total: u64 = 0;
    while (true) {
        try cancellation.check();
        const read = try reader.interface.readSliceShort(&buffer);
        if (read == 0) break;
        total = std.math.add(u64, total, read) catch return error.ArtifactIntegrityMismatch;
        if (total > expected_byte_len) return error.ArtifactIntegrityMismatch;
        hasher.update(buffer[0..read]);
    }
    if (total != expected_byte_len) return error.ArtifactIntegrityMismatch;
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    const actual = std.fmt.bytesToHex(digest, .lower);
    if (!std.mem.eql(u8, &actual, expected_checksum)) return error.ArtifactIntegrityMismatch;
    try cancellation.check();
}

fn threadedIo() std.Io.Threaded {
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

fn fileExists(path: []const u8) bool {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    _ = std.Io.Dir.cwd().statFile(io_impl.io(), path, .{}) catch return false;
    return true;
}

fn readFileAllocWithCancellation(alloc: Allocator, path: []const u8, cancellation: CancellationToken) ![]u8 {
    try cancellation.check();
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    const file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    const stat = try file.stat(io);
    const output_len = std.math.cast(usize, stat.size) orelse return error.ArtifactTooLarge;
    const out = try alloc.alloc(u8, output_len);
    errdefer alloc.free(out);

    const cancellation_chunk_bytes = 1024 * 1024;
    var copied: usize = 0;
    while (copied < out.len) {
        try cancellation.check();
        const chunk_len = @min(cancellation_chunk_bytes, out.len - copied);
        const chunk = out[copied..][0..chunk_len];
        if (try file.readPositionalAll(io, chunk, copied) != chunk.len) return error.ShortArtifactRead;
        copied += chunk.len;
    }
    try cancellation.check();
    return out;
}

fn readFileRangeAllocWithCancellation(
    alloc: Allocator,
    path: []const u8,
    offset: u64,
    len: usize,
    cancellation: CancellationToken,
) ![]u8 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    const file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    return try readOpenFileRangeAllocWithCancellation(alloc, file, io, offset, len, cancellation);
}

fn readOpenFileRangeAllocWithCancellation(
    alloc: Allocator,
    file: std.Io.File,
    io: std.Io,
    offset: u64,
    len: usize,
    cancellation: CancellationToken,
) ![]u8 {
    const stat = try file.stat(io);
    if (offset > stat.size) return error.InvalidRange;
    const available = stat.size - offset;
    const output_len: usize = @intCast(@min(available, len));
    const out = try alloc.alloc(u8, output_len);
    errdefer alloc.free(out);

    const cancellation_chunk_bytes = 1024 * 1024;
    var copied: usize = 0;
    while (copied < out.len) {
        try cancellation.check();
        const chunk_len = @min(cancellation_chunk_bytes, out.len - copied);
        const chunk = out[copied..][0..chunk_len];
        if (try file.readPositionalAll(io, chunk, offset + copied) != chunk.len) return error.ShortArtifactRead;
        copied += chunk.len;
    }
    try cancellation.check();
    return out;
}

fn deleteFile(path: []const u8) !void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    try std.Io.Dir.cwd().deleteFile(io_impl.io(), path);
    try fs_paths.syncDirPortable(io_impl.io(), std.fs.path.dirname(path) orelse ".");
}

fn writeFileAtomically(path: []const u8, contents: []const u8) !void {
    return writeFileAtomicallyWithCancellation(path, contents, .none);
}

pub fn writeFileAtomicallyWithCancellation(path: []const u8, contents: []const u8, cancellation: CancellationToken) !void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    var nonce: [16]u8 = undefined;
    io_impl.io().random(&nonce);
    const suffix = try std.fmt.allocPrint(std.heap.page_allocator, "tmp-{s}", .{std.fmt.bytesToHex(&nonce, .lower)});
    defer std.heap.page_allocator.free(suffix);
    return writeFileAtomicallyAt(path, contents, cancellation, suffix);
}
fn writeFileAtomicallyAt(path: []const u8, contents: []const u8, cancellation: CancellationToken, suffix: []const u8) !void {
    try cancellation.check();
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    const tmp_path = try std.fmt.allocPrint(std.heap.page_allocator, "{s}.{s}", .{ path, suffix });
    defer std.heap.page_allocator.free(tmp_path);
    if (std.mem.eql(u8, suffix, "pending-v2")) std.Io.Dir.cwd().deleteFile(io, tmp_path) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
    var owns_temp = false;
    errdefer if (owns_temp) {
        std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};
    };

    {
        var file = try std.Io.Dir.cwd().createFile(io, tmp_path, .{ .exclusive = true });
        owns_temp = true;
        defer file.close(io);

        var buf: [4096]u8 = undefined;
        var writer = file.writer(io, &buf);
        const cancellation_chunk_bytes = 1024 * 1024;
        var offset: usize = 0;
        while (offset < contents.len) {
            try cancellation.check();
            const len = @min(cancellation_chunk_bytes, contents.len - offset);
            try writer.interface.writeAll(contents[offset..][0..len]);
            offset += len;
        }
        try writer.end();
        try file.sync(io);
    }

    try cancellation.check();

    if (std.fs.path.isAbsolute(path)) {
        std.Io.Dir.renameAbsolute(tmp_path, path, io) catch |err| {
            std.Io.Dir.deleteFileAbsolute(io, tmp_path) catch {};
            return err;
        };
    } else {
        std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io) catch |err| {
            std.Io.Dir.deleteFileAbsolute(io, tmp_path) catch {};
            return err;
        };
    }
    try fs_paths.syncDirPortable(io, std.fs.path.dirname(path) orelse ".");
}

fn sha256StringAlloc(alloc: Allocator, contents: []const u8) ![]u8 {
    return sha256StringWithCancellationAlloc(alloc, contents, .none);
}

fn sha256StringWithCancellationAlloc(alloc: Allocator, contents: []const u8, cancellation: CancellationToken) ![]u8 {
    var digest: [32]u8 = undefined;
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    const chunk_bytes = 1024 * 1024;
    var offset: usize = 0;
    while (offset < contents.len) {
        try cancellation.check();
        const len = @min(chunk_bytes, contents.len - offset);
        hasher.update(contents[offset..][0..len]);
        offset += len;
    }
    hasher.final(&digest);

    const out = try alloc.alloc(u8, 64);
    for (digest, 0..) |byte, idx| {
        out[idx * 2] = hexNibble(byte >> 4);
        out[idx * 2 + 1] = hexNibble(byte & 0x0f);
    }
    return out;
}

fn makeArtifactIdAlloc(alloc: Allocator, checksum: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "sha256:{s}", .{checksum});
}

fn pathForArtifactAlloc(alloc: Allocator, root_dir: []const u8, checksum: []const u8) ![]u8 {
    try artifact_store.validateSha256Checksum(checksum);
    return try std.fs.path.join(alloc, &.{ root_dir, "sha256", checksum[0..2], checksum[2..] });
}

fn pathForArtifactIdAlloc(alloc: Allocator, root_dir: []const u8, id: []const u8) ![]u8 {
    const checksum = try artifact_store.sha256ChecksumFromArtifactId(id);
    if (id.len == 71) return pathForArtifactAlloc(alloc, root_dir, checksum);
    return std.fs.path.join(alloc, &.{ root_dir, "graph", id[78..142], id[143..175], checksum });
}

fn hexNibble(v: u8) u8 {
    return if (v < 10) '0' + v else 'a' + (v - 10);
}

var test_nonce: std.atomic.Value(u64) = .init(0);

fn nowNs() u64 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const nonce = test_nonce.fetchAdd(1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-artifacts-{s}-{d}-{d}\x00", .{
        label,
        nowNs(),
        nonce,
    }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

test "serverless filesystem retired attempt cleanup removes only owned canonical temporary files" {
    const a = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "retired-temporaries");
    defer cleanupTmp(path);
    var impl = try FsStore.init(a, std.mem.span(path));
    var store = impl.artifactStore();
    defer store.deinit();
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    const old_scope = try artifact_store.UploadScope.forPublication(@splat(1), 1, io);
    const new_scope = try artifact_store.UploadScope.forPublication(@splat(1), 2, io);
    var old = try store.putScoped(old_scope, "old committed page", .none);
    defer old.deinit(a);
    var fresh = try store.putScoped(new_scope, "new committed page", .none);
    defer fresh.deinit(a);
    const old_path = try pathForArtifactIdAlloc(a, std.mem.span(path), old.artifact_id);
    defer a.free(old_path);
    const fresh_path = try pathForArtifactIdAlloc(a, std.mem.span(path), fresh.artifact_id);
    defer a.free(fresh_path);
    const old_temp = try std.fmt.allocPrint(a, "{s}.tmp-{s}", .{ old_path, z17RepeatString("ab", 16) });
    defer a.free(old_temp);
    const fresh_temp = try std.fmt.allocPrint(a, "{s}.tmp-{s}", .{ fresh_path, z17RepeatString("cd", 16) });
    defer a.free(fresh_temp);
    const unrelated = try std.fmt.allocPrint(a, "{s}.tmp-not-a-canonical-nonce", .{old_path});
    defer a.free(unrelated);
    try writeFileAtomically(old_temp, "partial");
    try writeFileAtomically(fresh_temp, "partial");
    try writeFileAtomically(unrelated, "unrelated");
    // Rolling-upgrade uploads below the first leased generation cannot be
    // reclaimed merely because their builder attempt is old.
    try store.cleanupRetiredScopedTemporaryRange(old_scope.domain, 2, 3, .none);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().openFile(io, fresh_temp, .{}));
    const legacy_file = try std.Io.Dir.cwd().openFile(io, old_temp, .{});
    legacy_file.close(io);
    try writeFileAtomically(fresh_temp, "new partial upload");
    try store.cleanupRetiredScopedTemporaries(old_scope.domain, 2, .none);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().openFile(io, old_temp, .{}));
    for ([_][]const u8{ fresh_temp, unrelated, old_path, fresh_path }) |kept| {
        const file = try std.Io.Dir.cwd().openFile(io, kept, .{});
        file.close(io);
    }
    try writeFileAtomically(old_temp, "late partial upload");
    try store.cleanupRetiredScopedTemporaries(old_scope.domain, 2, .none);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().openFile(io, old_temp, .{}));
}

test "serverless filesystem artifacts inventory abandoned and late scoped uploads" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "scoped-uploads");
    defer cleanupTmp(path);
    var store = try FsStore.init(std.testing.allocator, std.mem.span(path));
    defer store.deinit();
    var capability = store.artifactStore();
    try @import("scoped_upload_test.zig").exercise(&capability);
}

test "fs artifact store put/get/stat are content-addressed" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "put-get");
    defer cleanupTmp(path);

    var store = try FsStore.init(std.testing.allocator, std.mem.span(path));
    defer store.deinit();

    var meta_a = try store.put(std.testing.allocator, "hello world");
    defer meta_a.deinit(std.testing.allocator);
    var meta_b = try store.put(std.testing.allocator, "hello world");
    defer meta_b.deinit(std.testing.allocator);

    try std.testing.expectEqualStrings(meta_a.artifact_id, meta_b.artifact_id);
    try std.testing.expectEqualStrings(meta_a.checksum, meta_b.checksum);
    try std.testing.expectEqual(@as(u64, 11), meta_a.byte_len);

    const full = try store.getAlloc(std.testing.allocator, meta_a.artifact_id);
    defer std.testing.allocator.free(full);
    try std.testing.expectEqualStrings("hello world", full);

    var stat = try store.stat(std.testing.allocator, meta_a.artifact_id);
    defer stat.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 11), stat.byte_len);
}

test "fs artifact store getRangeAlloc returns requested slice" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "range");
    defer cleanupTmp(path);

    var store = try FsStore.init(std.testing.allocator, std.mem.span(path));
    defer store.deinit();

    var meta = try store.put(std.testing.allocator, "abcdef");
    defer meta.deinit(std.testing.allocator);

    const mid = try store.getRangeAlloc(std.testing.allocator, meta.artifact_id, 2, 3);
    defer std.testing.allocator.free(mid);
    try std.testing.expectEqualStrings("cde", mid);
}

test "serverless fs artifact store detects and repairs same-length content corruption" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const root = tmpPath(&path_buf, "repair-corruption");
    defer cleanupTmp(root);

    var store = try FsStore.init(alloc, std.mem.span(root));
    defer store.deinit();
    var meta = try store.put(alloc, "alpha");
    defer meta.deinit(alloc);
    const path = try pathForArtifactAlloc(alloc, store.root_dir, meta.checksum);
    defer alloc.free(path);
    try writeFileAtomically(path, "omega");

    var iface = store.artifactStore();
    try std.testing.expectError(error.ArtifactIntegrityMismatch, iface.verifyContentWithCancellationUsingAllocator(
        alloc,
        meta.artifact_id,
        meta.byte_len,
        meta.checksum,
        .none,
    ));
    var repaired = try store.put(alloc, "alpha");
    defer repaired.deinit(alloc);
    try iface.verifyContentWithCancellationUsingAllocator(alloc, repaired.artifact_id, repaired.byte_len, repaired.checksum, .none);
    const payload = try store.getAlloc(alloc, repaired.artifact_id);
    defer alloc.free(payload);
    try std.testing.expectEqualStrings("alpha", payload);
}

test "serverless fs artifact verified range budgets cold authentication and amortizes warm reads" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const root = tmpPath(&path_buf, "budgeted-verified-range");
    defer cleanupTmp(root);
    var store = try FsStore.init(alloc, std.mem.span(root));
    defer store.deinit();
    var meta = try store.put(alloc, "alpha");
    defer meta.deinit(alloc);
    store.forgetVerifiedFile(meta.artifact_id);
    var iface = store.artifactStore();
    var remaining: u64 = 2;
    try std.testing.expectError(error.ArtifactReadBudgetExceeded, iface.getVerifiedRangeAllocWithBudget(alloc, meta.artifact_id, meta.byte_len, meta.checksum, 0, 1, .none, &remaining));
    try std.testing.expectEqual(@as(u64, 1), remaining);
    remaining = 6;
    const cold = try iface.getVerifiedRangeAllocWithBudget(alloc, meta.artifact_id, meta.byte_len, meta.checksum, 0, 1, .none, &remaining);
    defer alloc.free(cold);
    try std.testing.expectEqualStrings("a", cold);
    try std.testing.expectEqual(@as(u64, 0), remaining);
    remaining = 1;
    const warm = try iface.getVerifiedRangeAllocWithBudget(alloc, meta.artifact_id, meta.byte_len, meta.checksum, 0, 1, .none, &remaining);
    defer alloc.free(warm);
    try std.testing.expectEqualStrings("a", warm);
    try std.testing.expectEqual(@as(u64, 0), remaining);
    const path = try pathForArtifactAlloc(alloc, store.root_dir, meta.checksum);
    defer alloc.free(path);
    try writeFileAtomically(path, "omega");
    remaining = 6;
    try std.testing.expectError(error.ArtifactIntegrityMismatch, iface.getVerifiedRangeAllocWithBudget(alloc, meta.artifact_id, meta.byte_len, meta.checksum, 0, 1, .none, &remaining));
}

test "serverless fs artifact verification cache detects in-place mutation with restored mtime" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const root = tmpPath(&path_buf, "cached-in-place-corruption");
    defer cleanupTmp(root);

    var store = try FsStore.init(alloc, std.mem.span(root));
    defer store.deinit();
    var meta = try store.put(alloc, "alpha");
    defer meta.deinit(alloc);
    const path = try pathForArtifactAlloc(alloc, store.root_dir, meta.checksum);
    defer alloc.free(path);

    var iface = store.artifactStore();
    try iface.verifyContentWithCancellationUsingAllocator(alloc, meta.artifact_id, meta.byte_len, meta.checksum, .none);

    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    const before = try std.Io.Dir.cwd().statFile(io, path, .{});
    // Some CI filesystems expose ctime at a coarser resolution than their
    // write path. Wait for that observable clock to advance so this test
    // exercises cache invalidation instead of assuming nanosecond precision.
    var after = before;
    for (0..200) |attempt| {
        if (attempt > 0) try io.sleep(std.Io.Duration.fromMilliseconds(10), .awake);
        {
            var file = try std.Io.Dir.createFileAbsolute(io, path, .{ .truncate = true });
            defer file.close(io);
            var buffer: [32]u8 = undefined;
            var writer = file.writer(io, &buffer);
            try writer.interface.writeAll(if (attempt % 2 == 0) "omega" else "sigma");
            try writer.end();
        }
        try std.Io.Dir.cwd().setTimestamps(io, path, .{ .modify_timestamp = .{ .new = before.mtime } });
        after = try std.Io.Dir.cwd().statFile(io, path, .{});
        if (!std.meta.eql(before.ctime, after.ctime)) break;
    }
    try std.testing.expectEqual(before.inode, after.inode);
    try std.testing.expectEqual(before.size, after.size);
    try std.testing.expect(std.meta.eql(before.mtime, after.mtime));
    try std.testing.expect(!std.meta.eql(before.ctime, after.ctime));

    try std.testing.expectError(error.ArtifactIntegrityMismatch, iface.getVerifiedRangeAllocWithCancellationUsingAllocator(
        alloc,
        meta.artifact_id,
        meta.byte_len,
        meta.checksum,
        0,
        5,
        .none,
    ));
}

test "fs artifact store rejects malformed content addresses before lookup" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "invalid-id");
    defer cleanupTmp(path);

    var store = try FsStore.init(std.testing.allocator, std.mem.span(path));
    defer store.deinit();
    try std.testing.expectError(error.InvalidArtifactId, store.getAlloc(std.testing.allocator, "sha256:abcd"));
}

test "fs artifact store erased interface works" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "erased");
    defer cleanupTmp(path);

    var fs = try FsStore.init(std.testing.allocator, std.mem.span(path));
    var runtime = fs.artifactStore();
    defer runtime.deinit();

    var meta = try runtime.put("payload");
    defer meta.deinit(std.testing.allocator);

    const got = try runtime.getAlloc(meta.artifact_id);
    defer std.testing.allocator.free(got);
    try std.testing.expectEqualStrings("payload", got);
}

test "fs artifact store delete removes unreachable artifact" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "delete");
    defer cleanupTmp(path);

    var fs = try FsStore.init(std.testing.allocator, std.mem.span(path));
    var runtime = fs.artifactStore();
    defer runtime.deinit();

    var meta = try runtime.put("payload");
    defer meta.deinit(std.testing.allocator);
    try runtime.delete(meta.artifact_id);
    try std.testing.expectError(error.FileNotFound, runtime.getAlloc(meta.artifact_id));
}

fn z17RepeatString(comptime bytes: []const u8, comptime repetitions: usize) *const [bytes.len * repetitions:0]u8 {
    const result = comptime blk: {
        @setEvalBranchQuota(@intCast(@min(std.math.maxInt(u32), 100000 +| (repetitions *| 16))));
        var repeated: [bytes.len * repetitions:0]u8 = undefined;
        for (0..repetitions) |i| @memcpy(repeated[i * bytes.len ..][0..bytes.len], bytes);
        repeated[bytes.len * repetitions] = 0;
        break :blk repeated;
    };
    return &result;
}

test "external lake filesystem journal resumes across restart with linear record reads" {
    const a = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "upload-journal-resume");
    defer cleanupTmp(path);
    var initial = try FsStore.init(a, std.mem.span(path));
    const scope = try artifact_store.UploadScope.forPublication(@splat(9), 1, std.testing.io);
    var initial_store = initial.artifactStore();
    {
        defer initial.deinit();
        for (0..73) |index| {
            var bytes: [32]u8 = undefined;
            var ref = try initial_store.putScoped(scope, try std.fmt.bufPrint(&bytes, "payload-{d}", .{index}), .none);
            const payload = try pathForArtifactIdAlloc(a, std.mem.span(path), ref.artifact_id);
            defer a.free(payload);
            const staging = try std.fmt.allocPrint(a, "{s}.pending-v2", .{payload});
            defer a.free(staging);
            try writeFileAtomically(staging, "abandoned upload");
            ref.deinit(a);
        }
        try std.testing.expectEqual(@as(usize, 1), initial.journal_backfills.load(.monotonic));
    }
    const Visitor = struct {
        store: *artifact_store.ArtifactStore,
        count: usize = 0,
        continuation: ?[]u8 = null,
        fn visit(raw: *anyopaque, _: artifact_store.UploadScope, id: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const backend: *FsStore = @ptrCast(@alignCast(self.store.ptr));
            const payload = try pathForArtifactIdAlloc(std.testing.allocator, backend.root_dir, id);
            defer std.testing.allocator.free(payload);
            const staging = try std.fmt.allocPrint(std.testing.allocator, "{s}.pending-v2", .{payload});
            defer std.testing.allocator.free(staging);
            try std.testing.expect(!fileExists(staging));
            try self.store.delete(id);
            self.count += 1;
        }
        fn checkpoint(raw: *anyopaque, token: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const owned = try std.testing.allocator.dupe(u8, token);
            if (self.continuation) |old| std.testing.allocator.free(old);
            self.continuation = owned;
        }
    };
    var token: ?[]u8 = null;
    defer if (token) |value| a.free(value);
    var total: usize = 0;
    var reads: usize = 0;
    var passes: usize = 0;
    while (true) {
        var reopened = try FsStore.init(a, std.mem.span(path));
        defer reopened.deinit();
        var store = reopened.artifactStore();
        var visitor: Visitor = .{ .store = &store };
        var paused = false;
        store.visitScopedUploads(scope.domain, .{ .ptr = &visitor, .visit = Visitor.visit, .checkpoint = Visitor.checkpoint, .continuation = token, .cleanup_staging = true, .fencing_cutoff = 2, .max_entries = 7 }, .none) catch |err| {
            if (err != error.ArtifactEnumerationPaused) return err;
            paused = true;
        };
        if (visitor.continuation) |next| {
            if (token) |old| a.free(old);
            token = next;
        }
        total += visitor.count;
        reads += reopened.journal_records_read.load(.monotonic);
        try std.testing.expectEqual(@as(usize, 0), reopened.journal_backfills.load(.monotonic));
        passes += 1;
        try std.testing.expect(passes < 20);
        if (!paused) break;
    }
    try std.testing.expectEqual(@as(usize, 73), total);
    try std.testing.expectEqual(total, reads);
    try std.testing.expectEqual(@as(usize, 11), passes);
}

test "external lake filesystem journal batches attempts and advances empty journals" {
    const a = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "journal-attempt-pages");
    defer cleanupTmp(path);
    var backend = try FsStore.init(a, std.mem.span(path));
    defer backend.deinit();
    var store = backend.artifactStore();
    for (0..12) |i| {
        const scope = try artifact_store.UploadScope.forPublication(@splat(17), i + 1, std.testing.io);
        var ref = try store.putScoped(scope, "payload", .none);
        defer ref.deinit(a);
        if (i % 2 == 0) try store.delete(ref.artifact_id);
    }
    const Visitor = struct {
        count: usize = 0,
        token: ?[]u8 = null,
        fn visit(raw: *anyopaque, _: artifact_store.UploadScope, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.count += 1;
        }
        fn checkpoint(raw: *anyopaque, token: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const next = try std.testing.allocator.dupe(u8, token);
            if (self.token) |old| std.testing.allocator.free(old);
            self.token = next;
        }
    };
    // One-record turns must still advance across attempts containing only
    // tombstones; larger pages must scan the namespace only once per turn.
    for ([_]usize{ 1, 4 }) |quantum| {
        var visitor: Visitor = .{};
        defer if (visitor.token) |token| a.free(token);
        var passes: usize = 0;
        const before = backend.journal_attempt_scans.load(.monotonic);
        while (true) {
            var paused = false;
            const previous = if (visitor.token) |token| try a.dupe(u8, token) else null;
            defer if (previous) |token| a.free(token);
            store.visitScopedUploads(@splat(17), .{
                .ptr = &visitor,
                .visit = Visitor.visit,
                .checkpoint = Visitor.checkpoint,
                .continuation = previous,
                .fencing_cutoff = 13,
                .max_entries = quantum,
            }, .none) catch |err| {
                if (err != error.ArtifactEnumerationPaused) return err;
                paused = true;
            };
            passes += 1;
            try std.testing.expect(passes <= 13);
            if (!paused) break;
        }
        try std.testing.expectEqual(@as(usize, 6), visitor.count);
        try std.testing.expectEqual(@as(usize, 12 / quantum), passes);
        try std.testing.expectEqual(passes, backend.journal_attempt_scans.load(.monotonic) - before);
    }
}

test "external lake filesystem journal reclamation preserves payloads and repairs interrupted retirement" {
    const a = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "journal-reclaim-recovery");
    defer cleanupTmp(path);
    var backend = try FsStore.init(a, std.mem.span(path));
    defer backend.deinit();
    var store = backend.artifactStore();
    const scope = try artifact_store.UploadScope.forPublication(@splat(21), 1, std.testing.io);
    var ref = try store.putScoped(scope, "protected payload", .none);
    defer ref.deinit(a);
    const payload = try std.fs.path.join(a, &.{ backend.root_dir, "graph", &std.fmt.bytesToHex(&scope.domain, .lower), &std.fmt.bytesToHex(&scope.attempt, .lower) });
    defer a.free(payload);
    const inventory = try std.fs.path.join(a, &.{ backend.inventory_root, &std.fmt.bytesToHex(&scope.domain, .lower), &std.fmt.bytesToHex(&scope.attempt, .lower) });
    defer a.free(inventory);
    const Visitor = struct {
        store: *artifact_store.ArtifactStore,
        remove: bool = false,
        count: usize = 0,
        stop_after_record: bool = false,
        stopped: bool = false,
        token: [99]u8 = undefined,
        fn visit(raw: *anyopaque, _: artifact_store.UploadScope, id: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.remove) try self.store.delete(id);
            self.count += 1;
        }
        fn checkpoint(raw: *anyopaque, token: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            @memcpy(&self.token, token);
            if (self.stop_after_record) self.stopped = true;
        }
        fn check(raw: *const anyopaque) !void {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            if (self.stopped) return error.DeadlineExceeded;
        }
    };
    var visitor: Visitor = .{ .store = &store };
    const options: artifact_store.ScopedUploadVisitor = .{
        .ptr = &visitor,
        .visit = Visitor.visit,
        .checkpoint = Visitor.checkpoint,
        .cleanup_staging = true,
        .reclaim_retired_inventory = true,
        .fencing_cutoff = 2,
        .max_entries = 8,
    };
    try store.visitScopedUploads(scope.domain, options, .none);
    try std.Io.Dir.cwd().access(std.testing.io, payload, .{});
    try std.Io.Dir.cwd().access(std.testing.io, inventory, .{});
    var unfenced = options;
    unfenced.fencing_cutoff = null;
    try std.testing.expectError(error.InvalidArtifactUploadScope, store.visitScopedUploads(scope.domain, unfenced, .none));
    visitor.remove = true;
    visitor.stop_after_record = true;
    try std.testing.expectError(error.DeadlineExceeded, store.visitScopedUploads(scope.domain, options, .{ .ptr = &visitor, .check_fn = Visitor.check }));
    try std.testing.expect(!std.mem.endsWith(u8, &visitor.token, "ffffffffffffffff"));
    try std.Io.Dir.cwd().access(std.testing.io, inventory, .{});
    visitor.stop_after_record = false;
    visitor.stopped = false;
    var resumed = options;
    const record_token = visitor.token;
    resumed.continuation = &record_token;
    try store.visitScopedUploads(scope.domain, resumed, .none);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, payload, .{}));
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, inventory, .{}));
    // Reusing this handle must recreate directories, rather than trusting a
    // cached durability proof for a directory that reclamation removed.
    var repeated = try store.putScoped(scope, "protected payload", .none);
    defer repeated.deinit(a);
    const bytes = try store.getAlloc(repeated.artifact_id);
    defer a.free(bytes);
    try std.testing.expectEqualStrings("protected payload", bytes);
    try store.delete(repeated.artifact_id);
    // Crash after advisory inventory deletion but before payload-directory
    // removal. A reopened collector must rediscover and finish this attempt.
    try std.Io.Dir.cwd().deleteTree(std.testing.io, inventory);
    var reopened = try FsStore.init(a, std.mem.span(path));
    defer reopened.deinit();
    var reopened_store = reopened.artifactStore();
    visitor.store = &reopened_store;
    visitor.count = 0;
    resumed.continuation = &record_token;
    try reopened_store.visitScopedUploads(scope.domain, resumed, .none);
    try std.testing.expectEqual(@as(usize, 0), visitor.count);
    try std.testing.expectEqual(@as(usize, 1), reopened.journal_backfills.load(.monotonic));
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, payload, .{}));
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, inventory, .{}));
    try reopened_store.visitScopedUploads(scope.domain, options, .none);
    try std.testing.expectEqual(@as(usize, 0), reopened.journal_records_read.load(.monotonic));
}

test "external lake filesystem concurrent scoped uploads preserve journal and duplicate content" {
    const a = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "concurrent-scoped-uploads");
    defer cleanupTmp(path);
    var first = try FsStore.init(a, std.mem.span(path));
    defer first.deinit();
    var second = try FsStore.init(a, std.mem.span(path));
    defer second.deinit();
    const scope = try artifact_store.UploadScope.forPublication(@splat(13), 1, std.testing.io);
    var io_impl = std.Io.Threaded.init(a, .{});
    defer io_impl.deinit();
    var failures: [64]?anyerror = @splat(null);
    var group: std.Io.Group = .init;
    const Worker = struct {
        fn run(fs: *FsStore, upload_scope: artifact_store.UploadScope, n: usize, failure: *?anyerror) void {
            execute(fs, upload_scope, n) catch |err| {
                failure.* = err;
            };
        }
        fn execute(fs: *FsStore, upload_scope: artifact_store.UploadScope, n: usize) !void {
            var buffer: [32]u8 = undefined;
            const payload = try std.fmt.bufPrint(&buffer, "payload-{d}", .{n % 32});
            var store = fs.artifactStore();
            var metadata = try store.putScoped(upload_scope, payload, .none);
            defer metadata.deinit(fs.alloc);
            const actual = try store.getAlloc(metadata.artifact_id);
            defer fs.alloc.free(actual);
            try std.testing.expectEqualSlices(u8, payload, actual);
        }
    };
    for (0..64) |n| group.async(io_impl.io(), Worker.run, .{ if (n % 2 == 0) &first else &second, scope, n, &failures[n] });
    try group.await(io_impl.io());
    for (failures) |failure| if (failure) |err| return err;
    const Visitor = struct {
        store: *artifact_store.ArtifactStore,
        seen: std.StringHashMapUnmanaged(void) = .empty,
        fn visit(raw: *anyopaque, _: artifact_store.UploadScope, id: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const bytes = try self.store.getAlloc(id);
            defer a.free(bytes);
            const entry = try self.seen.getOrPut(a, bytes);
            if (!entry.found_existing) entry.key_ptr.* = try a.dupe(u8, bytes);
        }
    };
    var store = second.artifactStore();
    var visitor: Visitor = .{ .store = &store };
    defer {
        var keys = visitor.seen.keyIterator();
        while (keys.next()) |key| a.free(key.*);
        visitor.seen.deinit(a);
    }
    try store.visitScopedUploads(scope.domain, .{ .ptr = &visitor, .visit = Visitor.visit }, .none);
    try std.testing.expectEqual(@as(u32, 32), visitor.seen.count());
}

test "external lake filesystem journal rejects retired and replaced directory handles" {
    const a = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "journal-retired-handles");
    defer cleanupTmp(path);
    var backend = try FsStore.init(a, std.mem.span(path));
    defer backend.deinit();
    var store = backend.artifactStore();
    const scope = try artifact_store.UploadScope.forPublication(@splat(41), 1, std.testing.io);
    var value = try store.putScoped(scope, "old payload", .none);
    defer value.deinit(a);
    try store.delete(value.artifact_id);
    const payload = try std.fs.path.join(a, &.{ backend.root_dir, "graph", &std.fmt.bytesToHex(&scope.domain, .lower), &std.fmt.bytesToHex(&scope.attempt, .lower) });
    defer a.free(payload);
    const inventory = try std.fs.path.join(a, &.{ backend.inventory_root, &std.fmt.bytesToHex(&scope.domain, .lower), &std.fmt.bytesToHex(&scope.attempt, .lower) });
    defer a.free(inventory);
    var threaded = threadedIo();
    defer threaded.deinit();
    const io = threaded.io();
    const held = try std.Io.Dir.cwd().openDir(io, payload, .{ .iterate = true });
    defer held.close(io);
    try std.testing.expect(try backend.reclaimEmptyScopedAttempt(io, scope, null, .none));
    try std.testing.expectError(error.ArtifactUploadAttemptRetired, backend.uploadJournal(io, held, scope, null, null, .none));
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io, inventory, .{}));
    var fresh = try store.putScoped(scope, "new payload", .none);
    defer fresh.deinit(a);
    try std.testing.expectError(error.ArtifactUploadAttemptRetired, backend.uploadJournal(io, held, scope, null, null, .none));
    const bytes = try store.getAlloc(fresh.artifact_id);
    defer a.free(bytes);
    try std.testing.expectEqualStrings("new payload", bytes);
}

test "external lake filesystem journal guard aliases share cancellation and ownership" {
    const a = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "journal-guard-aliases");
    defer cleanupTmp(path);
    var first = try FsStore.init(a, std.mem.span(path));
    defer first.deinit();
    const alias = try std.fmt.allocPrint(a, "{s}/.", .{std.mem.span(path)});
    defer a.free(alias);
    var second = try FsStore.init(a, alias);
    defer second.deinit();
    const scope = try artifact_store.UploadScope.forPublication(@splat(47), 1, std.testing.io);
    var threaded = threadedIo();
    defer threaded.deinit();
    const io = threaded.io();
    var guard = try first.journalGuard(io, scope, .none);
    var held = true;
    defer if (held) guard.deinit();
    const Stop = struct {
        count: usize = 0,
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.count += 1;
            if (self.count > 3) return error.DeadlineExceeded;
        }
    };
    var stop: Stop = .{};
    try std.testing.expectError(error.DeadlineExceeded, second.journalGuard(io, scope, .{ .ptr = &stop, .check_fn = Stop.check }));
    guard.deinit();
    held = false;
    var resumed = try second.journalGuard(io, scope, .none);
    resumed.deinit();
}

test "external lake filesystem journal migrates unidentified legacy continuations conservatively" {
    const a = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "journal-legacy-cursor");
    defer cleanupTmp(path);
    var backend = try FsStore.init(a, std.mem.span(path));
    defer backend.deinit();
    var store = backend.artifactStore();
    const scope = try artifact_store.UploadScope.forPublication(@splat(53), 1, std.testing.io);
    for ([_][]const u8{ "first", "second" }) |text| {
        var value = try store.putScoped(scope, text, .none);
        value.deinit(a);
    }
    const Visitor = struct {
        store: *artifact_store.ArtifactStore,
        count: usize = 0,
        fn visit(raw: *anyopaque, _: artifact_store.UploadScope, id: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.store.delete(id);
            self.count += 1;
        }
        fn checkpoint(_: *anyopaque, token: []const u8) !void {
            try std.testing.expectEqual(@as(usize, 99), token.len);
        }
    };
    var visitor: Visitor = .{ .store = &store };
    const legacy = try std.fmt.allocPrint(a, "{s}:ffffffffffffffff", .{std.fmt.bytesToHex(&scope.attempt, .lower)});
    defer a.free(legacy);
    try store.visitScopedUploads(scope.domain, .{ .ptr = &visitor, .visit = Visitor.visit, .checkpoint = Visitor.checkpoint, .continuation = legacy, .cleanup_staging = true, .reclaim_retired_inventory = true, .fencing_cutoff = 2, .max_entries = 8 }, .none);
    try std.testing.expectEqual(@as(usize, 2), visitor.count);
}

test "external lake filesystem journal generic retirement preserves nonempty legacy attempts" {
    const a = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "journal-generic-legacy");
    defer cleanupTmp(path);
    var backend = try FsStore.init(a, std.mem.span(path));
    defer backend.deinit();
    var store = backend.artifactStore();
    const scope = try artifact_store.UploadScope.forPublication(@splat(59), 1, std.testing.io);
    var value = try store.putScoped(scope, "legacy pending", .none);
    defer value.deinit(a);
    const payload = try pathForArtifactIdAlloc(a, backend.root_dir, value.artifact_id);
    defer a.free(payload);
    const temporary = try std.fmt.allocPrint(a, "{s}.tmp-00000000000000000000000000000001", .{payload});
    defer a.free(temporary);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = temporary, .data = "legacy pending" });
    try store.delete(value.artifact_id);
    try store.reclaimRetiredScopedInventory(scope.domain, 0, 2, .none);
    try std.Io.Dir.cwd().access(std.testing.io, temporary, .{});
    try store.cleanupRetiredScopedTemporaries(scope.domain, 2, .none);
    try store.reclaimRetiredScopedInventory(scope.domain, 0, 2, .none);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, std.fs.path.dirname(payload).?, .{}));
}

test "external lake filesystem journal migration resumes canceled scans and rejects changed or corrupt cookies" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var directory = try @import("antfly_local_sources").common_test_directory.TestDirectory.init("fs-inventory-migration");
    defer directory.cleanup();
    const scope = try artifact_store.UploadScope.forPublication(@splat(79), 1, std.testing.io);
    const payload_path = try std.fs.path.join(scratch, &.{ directory.path(), "graph", &std.fmt.bytesToHex(&scope.domain, .lower), &std.fmt.bytesToHex(&scope.attempt, .lower) });
    const metadata_path = try std.fs.path.join(scratch, &.{ directory.path(), ".upload-inventory", &std.fmt.bytesToHex(&scope.domain, .lower), &std.fmt.bytesToHex(&scope.attempt, .lower) });
    {
        var fs = try FsStore.init(a, directory.path());
        defer fs.deinit();
        var target = fs.artifactStore();
        for (0..20) |i| {
            const bytes = try std.fmt.allocPrint(scratch, "payload-{d}", .{i});
            var value = try target.putScoped(scope, bytes, .none);
            value.deinit(a);
        }
    }
    for (0..10) |i| {
        const path = try std.fmt.allocPrint(scratch, "{s}/{s}.tmp-{x:0>32}", .{ payload_path, z17RepeatString("0", 64), i });
        try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = path, .data = "legacy" });
    }
    const metadata = try std.Io.Dir.cwd().openDir(std.testing.io, metadata_path, .{});
    defer metadata.close(std.testing.io);
    for ([_][]const u8{ ".uploads-v1", ".uploads-id-v1", ".temporaries-v1" }) |name| try metadata.deleteFile(std.testing.io, name);
    const Visitor = struct {
        remaining: usize = 3,
        fn maintenance(raw: *anyopaque, _: artifact_store.ScopedUploadVisitor.Maintenance) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.remaining == 0) return error.DeadlineExceeded;
            self.remaining -= 1;
        }
        fn visit(_: *anyopaque, _: artifact_store.UploadScope, _: []const u8) !void {}
    };
    var inspected: usize = 0;
    var complete = false;
    for (0..20) |turn| {
        var fs = try FsStore.initExisting(a, directory.path());
        defer fs.deinit();
        const payloads = try std.Io.Dir.cwd().openDir(std.testing.io, payload_path, .{ .iterate = true });
        defer payloads.close(std.testing.io);
        var visitor: Visitor = .{};
        const journal = fs.uploadJournal(std.testing.io, payloads, scope, null, .{ .ptr = &visitor, .visit = Visitor.visit, .maintenance = Visitor.maintenance }, .none) catch |err| {
            if (err != error.DeadlineExceeded) return err;
            inspected += fs.migration_entries_read.load(.monotonic);
            if (turn == 0) {
                // Invalidates the saved cookie. One replay is required, then
                // all subsequent reopenings must resume without prefix scans.
                const path = try std.fmt.allocPrint(scratch, "{s}/{s}.tmp-{x:0>32}", .{ payload_path, z17RepeatString("0", 64), 10 });
                try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = path, .data = "late legacy" });
            }
            if (turn == 1) {
                const encoded = try metadata.readFileAlloc(std.testing.io, ".inventory-scan-v1", scratch, .limited(16384));
                const Envelope = struct { body: []const u8, checksum: [32]u8 };
                var envelope = try std.json.parseFromSliceLeaky(Envelope, scratch, encoded, .{});
                const state = try std.json.parseFromSliceLeaky(std.json.Value, scratch, envelope.body, .{});
                state.object.getPtr("offset").?.* = .{ .integer = 0 };
                envelope.body = try std.json.Stringify.valueAlloc(scratch, state, .{});
                const corrupt = try std.json.Stringify.valueAlloc(scratch, envelope, .{});
                try metadata.writeFile(std.testing.io, .{ .sub_path = ".inventory-scan-v1", .data = corrupt });
            }
            if (turn == 2) {
                // Simulate output reaching disk before its checkpoint. Resume
                // must discard the uncommitted tail of both output logs.
                for ([_][]const u8{ ".uploads-building", ".temporaries-building" }) |name| {
                    const file = try metadata.openFile(std.testing.io, name, .{ .mode = .read_write });
                    defer file.close(std.testing.io);
                    try file.writePositionalAll(std.testing.io, "uncommitted tail", try file.length(std.testing.io));
                }
            }
            continue;
        };
        defer journal.close(std.testing.io);
        inspected += fs.migration_entries_read.load(.monotonic);
        try std.testing.expectEqual(@as(u64, FsStore.journal_header.len + 20 * 64), try journal.file.length(std.testing.io));
        try std.testing.expectEqual(@as(u64, FsStore.temporary_header.len + 11 * 101), try journal.temporaries.length(std.testing.io));
        complete = true;
        break;
    }
    try std.testing.expect(complete);
    try std.testing.expectEqual(@as(usize, 37), inspected);
}
