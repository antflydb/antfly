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
const platform_sync = @import("antfly_platform").sync;
const Allocator = std.mem.Allocator;
const fs_paths = @import("../../common/fs_paths.zig");
const catalog_types = @import("types.zig");
const progress_store = @import("progress_store.zig");
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;
const work_lease = @import("../build/work_lease.zig");
const head_coordination = @import("../head_coordination.zig");

const retired_head_doc_offset = std.math.maxInt(u64);

pub const FsProgressStore = struct {
    alloc: Allocator,
    root_dir: []u8,
    mutex: std.atomic.Mutex = .unlocked,

    pub fn init(alloc: Allocator, root_dir: []const u8) !FsProgressStore {
        var io_impl = threadedIo();
        defer io_impl.deinit();
        try fs_paths.createDirPathPortable(io_impl.io(), root_dir);
        return .{
            .alloc = alloc,
            .root_dir = try alloc.dupe(u8, root_dir),
        };
    }

    pub fn deinit(self: *FsProgressStore) void {
        self.alloc.free(self.root_dir);
        self.* = undefined;
    }

    pub fn progressStore(self: *FsProgressStore) progress_store.ProgressStore {
        return .{
            .allocator = self.alloc,
            .ptr = self,
            .vtable = &vtable,
        };
    }

    pub fn getHead(self: *FsProgressStore, namespace: []const u8) !u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var current = try self.readHeadUnlocked(namespace);
        defer current.deinit();
        if (current.value.head_version == 0) return error.FileNotFound;
        return current.value.head_version;
    }

    pub fn compareAndSwapHead(self: *FsProgressStore, namespace: []const u8, expected: ?u64, version: u64) !bool {
        return self.compareAndSwapHeadMaybeFenced(namespace, expected, version, null);
    }

    fn compareAndSwapHeadMaybeFenced(self: *FsProgressStore, namespace: []const u8, expected: ?u64, version: u64, fence: ?progress_store.PublicationFence) !bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var lock_io_impl = threadedIo();
        defer lock_io_impl.deinit();
        const lock_io = lock_io_impl.io();
        var namespace_lock = try openNamespaceLock(self.alloc, self.root_dir, namespace, lock_io);
        defer namespace_lock.close(lock_io);
        try namespace_lock.lock(lock_io, .exclusive);
        defer namespace_lock.unlock(lock_io);

        var current = try self.readHeadUnlocked(namespace);
        defer current.deinit();
        const current_version: ?u64 = if (current.value.head_version == 0) null else current.value.head_version;
        if (current_version != expected) return false;
        if (fence == null and !current.value.released) return error.PublicationFenceRequired;
        if (fence) |required| try requireFence(current.value, required.owner_id, required.fencing_token);
        var proposed = current.value;
        proposed.head_version = version;
        try self.writeHeadUnlocked(namespace, proposed);
        return true;
    }

    fn readHeadUnlocked(self: *FsProgressStore, namespace: []const u8) !std.json.Parsed(head_coordination.Record) {
        const path = try headPathAlloc(self.alloc, self.root_dir, namespace);
        defer self.alloc.free(path);
        const raw = readFileAlloc(self.alloc, path) catch |err| switch (err) {
            error.FileNotFound => return std.json.parseFromSlice(head_coordination.Record, self.alloc, "{}", .{}),
            else => return err,
        };
        defer self.alloc.free(raw);
        const parsed = try std.json.parseFromSlice(head_coordination.Record, self.alloc, raw, .{ .allocate = .alloc_always });
        errdefer parsed.deinit();
        if (!head_coordination.valid(parsed.value)) return error.InvalidHeadCoordinationRecord;
        return parsed;
    }

    fn writeHeadUnlocked(self: *FsProgressStore, namespace: []const u8, record: head_coordination.Record) !void {
        const path = try headPathAlloc(self.alloc, self.root_dir, namespace);
        defer self.alloc.free(path);
        try ensureParentDir(path);
        const payload = try std.json.Stringify.valueAlloc(self.alloc, record, .{});
        defer self.alloc.free(payload);
        try writeFileAtomically(path, payload);
        var sync_io = threadedIo();
        defer sync_io.deinit();
        try fs_paths.syncDirPortable(sync_io.io(), self.root_dir);
    }

    fn requireFence(record: head_coordination.Record, owner: []const u8, token: u64) !void {
        if (record.released or record.fencing_token != token or record.owner_id == null or !std.mem.eql(u8, record.owner_id.?, owner)) return error.WorkLeaseLost;
    }

    fn erasedWorkLeaseProvider(ptr: *anyopaque) work_lease.Provider {
        return .{ .ptr = ptr, .vtable = &lease_vtable };
    }

    const lease_vtable: work_lease.Provider.VTable = .{
        .acquire = leaseAcquire,
        .validate = leaseValidate,
        .renew = leaseRenew,
        .release = leaseRelease,
        .acquire_bootstrap = leaseAcquire,
        .renew_bootstrap = leaseRenew,
        .release_bootstrap = leaseRelease,
    };

    fn leaseAcquire(ptr: *anyopaque, ns: []const u8, owner: []const u8, now: u64, ttl: u64) !?work_lease.Acquisition {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var io_impl = threadedIo();
        defer io_impl.deinit();
        const io = io_impl.io();
        var file_lock = try openNamespaceLock(self.alloc, self.root_dir, ns, io);
        defer file_lock.close(io);
        try file_lock.lock(io, .exclusive);
        defer file_lock.unlock(io);
        var current = try self.readHeadUnlocked(ns);
        defer current.deinit();
        if (!current.value.released and current.value.expires_at_unix_ns > now) return null;
        const token = std.math.add(u64, current.value.fencing_token, 1) catch return error.LeaseFencingTokenOverflow;
        const expiry = std.math.add(u64, now, ttl) catch return error.LeaseExpiryOverflow;
        try self.writeHeadUnlocked(ns, .{ .head_version = current.value.head_version, .owner_id = owner, .fencing_token = token, .expires_at_unix_ns = expiry, .released = false });
        return .{ .fencing_token = token, .expires_at_unix_ns = expiry, .took_over = !current.value.released };
    }

    fn leaseValidate(ptr: *anyopaque, ns: []const u8, owner: []const u8, token: u64, now: u64) !void {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var current = try self.readHeadUnlocked(ns);
        defer current.deinit();
        try requireFence(current.value, owner, token);
        if (current.value.expires_at_unix_ns <= now) return error.WorkLeaseLost;
    }

    fn changeLease(self: *FsProgressStore, ns: []const u8, owner: []const u8, token: u64, expiry: ?u64) !void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var io_impl = threadedIo();
        defer io_impl.deinit();
        const io = io_impl.io();
        var file_lock = try openNamespaceLock(self.alloc, self.root_dir, ns, io);
        defer file_lock.close(io);
        try file_lock.lock(io, .exclusive);
        defer file_lock.unlock(io);
        var current = try self.readHeadUnlocked(ns);
        defer current.deinit();
        try requireFence(current.value, owner, token);
        var proposed = current.value;
        proposed.expires_at_unix_ns = expiry orelse 0;
        proposed.released = expiry == null;
        try self.writeHeadUnlocked(ns, proposed);
    }

    fn leaseRenew(ptr: *anyopaque, ns: []const u8, owner: []const u8, token: u64, now: u64, ttl: u64) !u64 {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        const expiry = std.math.add(u64, now, ttl) catch return error.LeaseExpiryOverflow;
        try self.changeLease(ns, owner, token, expiry);
        return expiry;
    }

    fn leaseRelease(ptr: *anyopaque, ns: []const u8, owner: []const u8, token: u64) !bool {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        self.changeLease(ns, owner, token, null) catch |err| switch (err) {
            error.WorkLeaseLost => return false,
            else => return err,
        };
        return true;
    }

    pub fn getGcWatermark(self: *FsProgressStore, namespace: []const u8) !?u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return self.readOptionalU64Unlocked(namespace, .gc_watermark) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
    }

    pub fn compareAndSwapGcWatermark(self: *FsProgressStore, namespace: []const u8, expected: ?u64, watermark: u64) !bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var lock_io_impl = threadedIo();
        defer lock_io_impl.deinit();
        const lock_io = lock_io_impl.io();
        var namespace_lock = try openNamespaceLock(self.alloc, self.root_dir, namespace, lock_io);
        defer namespace_lock.close(lock_io);
        try namespace_lock.lock(lock_io, .exclusive);
        defer namespace_lock.unlock(lock_io);

        const current = self.readOptionalU64Unlocked(namespace, .gc_watermark) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (current != expected) return false;
        if (current != null and watermark < current.?) return false;
        try self.writeU64Unlocked(namespace, .gc_watermark, watermark);
        return true;
    }

    pub fn getManifestGcFloor(self: *FsProgressStore, namespace: []const u8) !?u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return self.readOptionalU64Unlocked(namespace, .manifest_gc_floor) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
    }

    fn readDeadlinePathAlloc(self: *FsProgressStore, namespace: []const u8, version: u64) ![]u8 {
        const leaf = try std.fmt.allocPrint(self.alloc, "READ_PINS/{d}", .{version});
        defer self.alloc.free(leaf);
        return std.fs.path.join(self.alloc, &.{ self.root_dir, namespace, leaf });
    }

    fn readDeadlineUnlocked(self: *FsProgressStore, path: []const u8) !?u64 {
        const raw = readFileAlloc(self.alloc, path) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer self.alloc.free(raw);
        return try std.fmt.parseInt(u64, std.mem.trim(u8, raw, " \t\r\n"), 10);
    }

    pub fn getManifestReadDeadline(self: *FsProgressStore, namespace: []const u8, version: u64) !?u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const path = try self.readDeadlinePathAlloc(namespace, version);
        defer self.alloc.free(path);
        return self.readDeadlineUnlocked(path);
    }

    pub fn compareAndSwapManifestReadDeadline(self: *FsProgressStore, namespace: []const u8, version: u64, expected: ?u64, deadline: u64) !bool {
        if (expected) |prior| if (deadline < prior) return false;
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var io_impl = threadedIo();
        defer io_impl.deinit();
        const io = io_impl.io();
        var namespace_lock = try openNamespaceLock(self.alloc, self.root_dir, namespace, io);
        defer namespace_lock.close(io);
        try namespace_lock.lock(io, .exclusive);
        defer namespace_lock.unlock(io);
        const path = try self.readDeadlinePathAlloc(namespace, version);
        defer self.alloc.free(path);
        if (try self.readDeadlineUnlocked(path) != expected) return false;
        try ensureParentDir(path);
        var buffer: [20]u8 = undefined;
        const payload = try std.fmt.bufPrint(&buffer, "{d}", .{deadline});
        try writeFileAtomically(path, payload);
        // Also persist newly created READ_PINS and namespace directory entries,
        // not only the pin file's rename inside READ_PINS.
        const namespace_path = try std.fs.path.join(self.alloc, &.{ self.root_dir, namespace });
        defer self.alloc.free(namespace_path);
        try fs_paths.syncDirPortable(io, namespace_path);
        try fs_paths.syncDirPortable(io, self.root_dir);
        return true;
    }

    fn pruneManifestReadDeadlines(self: *FsProgressStore, namespace: []const u8, floor: u64, expired_before: u64, cancellation: CancellationToken) !void {
        var io_impl = threadedIo();
        defer io_impl.deinit();
        const io = io_impl.io();
        const path = try std.fs.path.join(self.alloc, &.{ self.root_dir, namespace, "READ_PINS" });
        defer self.alloc.free(path);
        var dir = std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer dir.close(io);
        var iterator = dir.iterate();
        var deleted = false;
        while (try iterator.next(io)) |entry| {
            try cancellation.check();
            if (entry.kind != .file) continue;
            const version = std.fmt.parseInt(u64, entry.name, 10) catch continue;
            if (version >= floor) continue;
            const deadline = try self.getManifestReadDeadline(namespace, version) orelse continue;
            if (deadline >= expired_before) continue;
            dir.deleteFile(io, entry.name) catch |err| switch (err) {
                error.FileNotFound => continue,
                else => return err,
            };
            deleted = true;
        }
        if (deleted) try fs_paths.syncDirectoryHandlePortable(io, dir);
    }

    pub fn compareAndSwapManifestGcFloor(self: *FsProgressStore, namespace: []const u8, expected: ?u64, floor: u64) !bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var lock_io_impl = threadedIo();
        defer lock_io_impl.deinit();
        const lock_io = lock_io_impl.io();
        var namespace_lock = try openNamespaceLock(self.alloc, self.root_dir, namespace, lock_io);
        defer namespace_lock.close(lock_io);
        try namespace_lock.lock(lock_io, .exclusive);
        defer namespace_lock.unlock(lock_io);

        const current = self.readOptionalU64Unlocked(namespace, .manifest_gc_floor) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (current != expected) return false;
        if (current != null and floor < current.?) return false;
        try self.writeU64Unlocked(namespace, .manifest_gc_floor, floor);
        return true;
    }

    pub fn getEnrichmentHeadVersion(self: *FsProgressStore, namespace: []const u8) !?u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return self.readOptionalU64Unlocked(namespace, .enrichment_head_version) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
    }

    pub fn compareAndSwapEnrichmentHeadVersion(self: *FsProgressStore, namespace: []const u8, expected: ?u64, head_version: u64) !bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var lock_io_impl = threadedIo();
        defer lock_io_impl.deinit();
        const lock_io = lock_io_impl.io();
        var namespace_lock = try openNamespaceLock(self.alloc, self.root_dir, namespace, lock_io);
        defer namespace_lock.close(lock_io);
        try namespace_lock.lock(lock_io, .exclusive);
        defer namespace_lock.unlock(lock_io);

        const current = self.readOptionalU64Unlocked(namespace, .enrichment_head_version) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (current != expected) return false;
        try self.writeU64Unlocked(namespace, .enrichment_head_version, head_version);
        return true;
    }

    pub fn getEnrichmentStage(self: *FsProgressStore, namespace: []const u8) !?u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return self.readOptionalU64Unlocked(namespace, .enrichment_stage) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
    }

    pub fn compareAndSwapEnrichmentStage(self: *FsProgressStore, namespace: []const u8, expected: ?u64, stage: u64) !bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var lock_io_impl = threadedIo();
        defer lock_io_impl.deinit();
        const lock_io = lock_io_impl.io();
        var namespace_lock = try openNamespaceLock(self.alloc, self.root_dir, namespace, lock_io);
        defer namespace_lock.close(lock_io);
        try namespace_lock.lock(lock_io, .exclusive);
        defer namespace_lock.unlock(lock_io);

        const current = self.readOptionalU64Unlocked(namespace, .enrichment_stage) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (current != expected) return false;
        try self.writeU64Unlocked(namespace, .enrichment_stage, stage);
        return true;
    }

    pub fn getEnrichmentDocOffset(self: *FsProgressStore, namespace: []const u8) !?u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return self.readOptionalU64Unlocked(namespace, .enrichment_doc_offset) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
    }

    pub fn compareAndSwapEnrichmentDocOffset(self: *FsProgressStore, namespace: []const u8, expected: ?u64, doc_offset: u64) !bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var lock_io_impl = threadedIo();
        defer lock_io_impl.deinit();
        const lock_io = lock_io_impl.io();
        var namespace_lock = try openNamespaceLock(self.alloc, self.root_dir, namespace, lock_io);
        defer namespace_lock.close(lock_io);
        try namespace_lock.lock(lock_io, .exclusive);
        defer namespace_lock.unlock(lock_io);

        const current = self.readOptionalU64Unlocked(namespace, .enrichment_doc_offset) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (current != expected) return false;
        try self.writeU64Unlocked(namespace, .enrichment_doc_offset, doc_offset);
        return true;
    }

    pub fn getEnrichmentStageHeadVersion(self: *FsProgressStore, namespace: []const u8, stage: catalog_types.EnrichmentStage) !?u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return self.readOptionalStageU64Unlocked(namespace, stage, "HEAD_VERSION") catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
    }

    pub fn compareAndSwapEnrichmentStageHeadVersion(
        self: *FsProgressStore,
        namespace: []const u8,
        stage: catalog_types.EnrichmentStage,
        expected: ?u64,
        head_version: u64,
    ) !bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var lock_io_impl = threadedIo();
        defer lock_io_impl.deinit();
        const lock_io = lock_io_impl.io();
        var namespace_lock = try openNamespaceLock(self.alloc, self.root_dir, namespace, lock_io);
        defer namespace_lock.close(lock_io);
        try namespace_lock.lock(lock_io, .exclusive);
        defer namespace_lock.unlock(lock_io);

        const current = self.readOptionalStageU64Unlocked(namespace, stage, "HEAD_VERSION") catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (current != expected) return false;
        if (current) |version| {
            if (head_version < version) return false;
        }
        try self.writeStageU64Unlocked(namespace, stage, "HEAD_VERSION", head_version);
        return true;
    }

    pub fn getEnrichmentStageDocOffset(self: *FsProgressStore, namespace: []const u8, stage: catalog_types.EnrichmentStage) !?u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return self.readOptionalStageU64Unlocked(namespace, stage, "DOC_OFFSET") catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
    }

    pub fn compareAndSwapEnrichmentStageDocOffset(
        self: *FsProgressStore,
        namespace: []const u8,
        stage: catalog_types.EnrichmentStage,
        expected: ?u64,
        doc_offset: u64,
    ) !bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var lock_io_impl = threadedIo();
        defer lock_io_impl.deinit();
        const lock_io = lock_io_impl.io();
        var namespace_lock = try openNamespaceLock(self.alloc, self.root_dir, namespace, lock_io);
        defer namespace_lock.close(lock_io);
        try namespace_lock.lock(lock_io, .exclusive);
        defer namespace_lock.unlock(lock_io);

        const current = self.readOptionalStageU64Unlocked(namespace, stage, "DOC_OFFSET") catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (current != expected) return false;
        try self.writeStageU64Unlocked(namespace, stage, "DOC_OFFSET", doc_offset);
        return true;
    }

    pub fn getEnrichmentStageHeadDocOffset(
        self: *FsProgressStore,
        namespace: []const u8,
        stage: catalog_types.EnrichmentStage,
        head_version: u64,
    ) !?u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const value = self.readOptionalStageHeadU64Unlocked(namespace, stage, head_version) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        return if (value == retired_head_doc_offset) null else value;
    }

    pub fn compareAndSwapEnrichmentStageHeadDocOffset(
        self: *FsProgressStore,
        namespace: []const u8,
        stage: catalog_types.EnrichmentStage,
        head_version: u64,
        expected: ?u64,
        doc_offset: u64,
    ) !bool {
        if (doc_offset == retired_head_doc_offset) return error.InvalidEnrichmentDocOffset;
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var lock_io_impl = threadedIo();
        defer lock_io_impl.deinit();
        const lock_io = lock_io_impl.io();
        var namespace_lock = try openNamespaceLock(self.alloc, self.root_dir, namespace, lock_io);
        defer namespace_lock.close(lock_io);
        try namespace_lock.lock(lock_io, .exclusive);
        defer namespace_lock.unlock(lock_io);

        const current = self.readOptionalStageHeadU64Unlocked(namespace, stage, head_version) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (current != expected) return false;
        try self.writeStageHeadU64Unlocked(namespace, stage, head_version, doc_offset);
        return true;
    }

    pub fn deleteEnrichmentStageHeadDocOffset(
        self: *FsProgressStore,
        namespace: []const u8,
        stage: catalog_types.EnrichmentStage,
        head_version: u64,
    ) !void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var lock_io_impl = threadedIo();
        defer lock_io_impl.deinit();
        const lock_io = lock_io_impl.io();
        var namespace_lock = try openNamespaceLock(self.alloc, self.root_dir, namespace, lock_io);
        defer namespace_lock.close(lock_io);
        try namespace_lock.lock(lock_io, .exclusive);
        defer namespace_lock.unlock(lock_io);
        // Logical deletion is a durable tombstone. Removing the file would
        // let a worker that loaded the old manifest before retention recreate
        // the offset with a null-expected CAS after the manifest is gone.
        try self.writeStageHeadU64Unlocked(namespace, stage, head_version, retired_head_doc_offset);
    }

    pub fn getEnrichmentStageProgress(
        self: *FsProgressStore,
        namespace: []const u8,
        stage: catalog_types.EnrichmentStage,
    ) !?progress_store.EnrichmentStageProgress {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return self.readOptionalStageProgressUnlocked(namespace, stage) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
    }

    pub fn compareAndSwapEnrichmentStageProgress(
        self: *FsProgressStore,
        namespace: []const u8,
        stage: catalog_types.EnrichmentStage,
        expected: ?progress_store.EnrichmentStageProgress,
        desired: progress_store.EnrichmentStageProgress,
    ) !bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var lock_io_impl = threadedIo();
        defer lock_io_impl.deinit();
        const lock_io = lock_io_impl.io();
        var namespace_lock = try openNamespaceLock(self.alloc, self.root_dir, namespace, lock_io);
        defer namespace_lock.close(lock_io);
        try namespace_lock.lock(lock_io, .exclusive);
        defer namespace_lock.unlock(lock_io);

        var current = self.readOptionalStageProgressUnlocked(namespace, stage) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        defer if (current) |*value| value.deinit(self.alloc);
        if (!stageProgressOptionalEql(current, expected)) return false;
        if (current) |value| {
            if (desired.head_version < value.head_version) return false;
            if (desired.revision < value.revision or desired.completed_cycles < value.completed_cycles) return false;
            if (desired.head_version == value.head_version and desired.revision == value.revision and desired.doc_offset < value.doc_offset) return false;
        }
        try self.writeStageProgressUnlocked(namespace, stage, desired);
        return true;
    }

    const Kind = enum {
        head,
        gc_watermark,
        manifest_gc_floor,
        enrichment_head_version,
        enrichment_stage,
        enrichment_doc_offset,
    };

    fn readOptionalU64Unlocked(self: *FsProgressStore, namespace: []const u8, kind: Kind) !u64 {
        const path = try pathForAlloc(self.alloc, self.root_dir, namespace, kind);
        defer self.alloc.free(path);
        const raw = try readFileAlloc(self.alloc, path);
        defer self.alloc.free(raw);
        return try std.fmt.parseInt(u64, std.mem.trim(u8, raw, " \t\r\n"), 10);
    }

    fn writeU64Unlocked(self: *FsProgressStore, namespace: []const u8, kind: Kind, value: u64) !void {
        const path = try pathForAlloc(self.alloc, self.root_dir, namespace, kind);
        defer self.alloc.free(path);
        try ensureParentDir(path);
        const payload = try std.fmt.allocPrint(self.alloc, "{d}", .{value});
        defer self.alloc.free(payload);
        try writeFileAtomically(path, payload);
    }

    fn readOptionalStageU64Unlocked(self: *FsProgressStore, namespace: []const u8, stage: catalog_types.EnrichmentStage, leaf: []const u8) !u64 {
        const path = try stagePathAlloc(self.alloc, self.root_dir, namespace, stage, leaf);
        defer self.alloc.free(path);
        const raw = try readFileAlloc(self.alloc, path);
        defer self.alloc.free(raw);
        return try std.fmt.parseInt(u64, std.mem.trim(u8, raw, " \t\r\n"), 10);
    }

    fn writeStageU64Unlocked(self: *FsProgressStore, namespace: []const u8, stage: catalog_types.EnrichmentStage, leaf: []const u8, value: u64) !void {
        const path = try stagePathAlloc(self.alloc, self.root_dir, namespace, stage, leaf);
        defer self.alloc.free(path);
        try ensureParentDir(path);
        const payload = try std.fmt.allocPrint(self.alloc, "{d}", .{value});
        defer self.alloc.free(payload);
        try writeFileAtomically(path, payload);
    }

    fn readOptionalStageProgressUnlocked(
        self: *FsProgressStore,
        namespace: []const u8,
        stage: catalog_types.EnrichmentStage,
    ) !progress_store.EnrichmentStageProgress {
        const path = try stagePathAlloc(self.alloc, self.root_dir, namespace, stage, "STATE");
        defer self.alloc.free(path);
        var io_impl = threadedIo();
        defer io_impl.deinit();
        const raw = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, self.alloc, .limited(progress_store.EnrichmentStageProgress.max_encoded_bytes));
        defer self.alloc.free(raw);
        return try progress_store.EnrichmentStageProgress.decodeAlloc(self.alloc, raw);
    }

    fn writeStageProgressUnlocked(
        self: *FsProgressStore,
        namespace: []const u8,
        stage: catalog_types.EnrichmentStage,
        value: progress_store.EnrichmentStageProgress,
    ) !void {
        const path = try stagePathAlloc(self.alloc, self.root_dir, namespace, stage, "STATE");
        defer self.alloc.free(path);
        try ensureParentDir(path);
        const payload = try value.encodeAlloc(self.alloc);
        defer self.alloc.free(payload);
        try writeFileAtomically(path, payload);
    }

    fn readOptionalStageHeadU64Unlocked(
        self: *FsProgressStore,
        namespace: []const u8,
        stage: catalog_types.EnrichmentStage,
        head_version: u64,
    ) !u64 {
        const path = try stageHeadOffsetPathAlloc(self.alloc, self.root_dir, namespace, stage, head_version);
        defer self.alloc.free(path);
        const raw = try readFileAlloc(self.alloc, path);
        defer self.alloc.free(raw);
        return try std.fmt.parseInt(u64, std.mem.trim(u8, raw, " \t\r\n"), 10);
    }

    fn writeStageHeadU64Unlocked(
        self: *FsProgressStore,
        namespace: []const u8,
        stage: catalog_types.EnrichmentStage,
        head_version: u64,
        value: u64,
    ) !void {
        const path = try stageHeadOffsetPathAlloc(self.alloc, self.root_dir, namespace, stage, head_version);
        defer self.alloc.free(path);
        try ensureParentDir(path);
        const payload = try std.fmt.allocPrint(self.alloc, "{d}", .{value});
        defer self.alloc.free(payload);
        try writeFileAtomically(path, payload);
    }

    const vtable: progress_store.ProgressStore.VTable = .{
        .work_lease_provider = erasedWorkLeaseProvider,
        .deinit = erasedDeinit,
        .get_head = erasedGetHead,
        .compare_and_swap_head = erasedCompareAndSwapHead,
        .compare_and_swap_head_fenced = erasedCompareAndSwapHeadFenced,
        .get_gc_watermark = erasedGetGcWatermark,
        .compare_and_swap_gc_watermark = erasedCompareAndSwapGcWatermark,
        .get_manifest_gc_floor = erasedGetManifestGcFloor,
        .compare_and_swap_manifest_gc_floor = erasedCompareAndSwapManifestGcFloor,
        .get_manifest_read_deadline = erasedGetManifestReadDeadline,
        .compare_and_swap_manifest_read_deadline = erasedCompareAndSwapManifestReadDeadline,
        .prune_manifest_read_deadlines = erasedPruneManifestReadDeadlines,
        .get_enrichment_head_version = erasedGetEnrichmentHeadVersion,
        .compare_and_swap_enrichment_head_version = erasedCompareAndSwapEnrichmentHeadVersion,
        .get_enrichment_stage = erasedGetEnrichmentStage,
        .compare_and_swap_enrichment_stage = erasedCompareAndSwapEnrichmentStage,
        .get_enrichment_doc_offset = erasedGetEnrichmentDocOffset,
        .compare_and_swap_enrichment_doc_offset = erasedCompareAndSwapEnrichmentDocOffset,
        .get_enrichment_stage_head_version = erasedGetEnrichmentStageHeadVersion,
        .compare_and_swap_enrichment_stage_head_version = erasedCompareAndSwapEnrichmentStageHeadVersion,
        .get_enrichment_stage_doc_offset = erasedGetEnrichmentStageDocOffset,
        .compare_and_swap_enrichment_stage_doc_offset = erasedCompareAndSwapEnrichmentStageDocOffset,
        .get_enrichment_stage_head_doc_offset = erasedGetEnrichmentStageHeadDocOffset,
        .compare_and_swap_enrichment_stage_head_doc_offset = erasedCompareAndSwapEnrichmentStageHeadDocOffset,
        .delete_enrichment_stage_head_doc_offset = erasedDeleteEnrichmentStageHeadDocOffset,
        .get_enrichment_stage_progress = erasedGetEnrichmentStageProgress,
        .compare_and_swap_enrichment_stage_progress = erasedCompareAndSwapEnrichmentStageProgress,
    };

    fn erasedDeinit(_: Allocator, ptr: *anyopaque) void {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    fn erasedGetHead(ptr: *anyopaque, namespace: []const u8) !u64 {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.getHead(namespace);
    }

    fn erasedCompareAndSwapHead(ptr: *anyopaque, namespace: []const u8, expected: ?u64, version: u64) !bool {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.compareAndSwapHead(namespace, expected, version);
    }

    fn erasedCompareAndSwapHeadFenced(
        ptr: *anyopaque,
        namespace: []const u8,
        expected: ?u64,
        version: u64,
        fence: progress_store.PublicationFence,
    ) !bool {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return self.compareAndSwapHeadMaybeFenced(namespace, expected, version, fence);
    }

    fn erasedGetGcWatermark(ptr: *anyopaque, namespace: []const u8) !?u64 {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.getGcWatermark(namespace);
    }

    fn erasedCompareAndSwapGcWatermark(ptr: *anyopaque, namespace: []const u8, expected: ?u64, watermark: u64) !bool {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.compareAndSwapGcWatermark(namespace, expected, watermark);
    }

    fn erasedGetManifestGcFloor(ptr: *anyopaque, namespace: []const u8) !?u64 {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.getManifestGcFloor(namespace);
    }

    fn erasedGetManifestReadDeadline(ptr: *anyopaque, namespace: []const u8, version: u64) !?u64 {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return self.getManifestReadDeadline(namespace, version);
    }

    fn erasedPruneManifestReadDeadlines(ptr: *anyopaque, namespace: []const u8, floor: u64, expired_before: u64, cancellation: CancellationToken) !void {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return self.pruneManifestReadDeadlines(namespace, floor, expired_before, cancellation);
    }

    fn erasedCompareAndSwapManifestReadDeadline(ptr: *anyopaque, namespace: []const u8, version: u64, expected: ?u64, deadline: u64) !bool {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return self.compareAndSwapManifestReadDeadline(namespace, version, expected, deadline);
    }

    fn erasedCompareAndSwapManifestGcFloor(ptr: *anyopaque, namespace: []const u8, expected: ?u64, floor: u64) !bool {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.compareAndSwapManifestGcFloor(namespace, expected, floor);
    }

    fn erasedGetEnrichmentHeadVersion(ptr: *anyopaque, namespace: []const u8) !?u64 {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.getEnrichmentHeadVersion(namespace);
    }

    fn erasedCompareAndSwapEnrichmentHeadVersion(ptr: *anyopaque, namespace: []const u8, expected: ?u64, head_version: u64) !bool {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.compareAndSwapEnrichmentHeadVersion(namespace, expected, head_version);
    }

    fn erasedGetEnrichmentStage(ptr: *anyopaque, namespace: []const u8) !?u64 {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.getEnrichmentStage(namespace);
    }

    fn erasedCompareAndSwapEnrichmentStage(ptr: *anyopaque, namespace: []const u8, expected: ?u64, stage: u64) !bool {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.compareAndSwapEnrichmentStage(namespace, expected, stage);
    }

    fn erasedGetEnrichmentDocOffset(ptr: *anyopaque, namespace: []const u8) !?u64 {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.getEnrichmentDocOffset(namespace);
    }

    fn erasedCompareAndSwapEnrichmentDocOffset(ptr: *anyopaque, namespace: []const u8, expected: ?u64, doc_offset: u64) !bool {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.compareAndSwapEnrichmentDocOffset(namespace, expected, doc_offset);
    }

    fn erasedGetEnrichmentStageHeadVersion(ptr: *anyopaque, namespace: []const u8, stage_id: u8) !?u64 {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.getEnrichmentStageHeadVersion(namespace, @enumFromInt(stage_id));
    }

    fn erasedCompareAndSwapEnrichmentStageHeadVersion(
        ptr: *anyopaque,
        namespace: []const u8,
        stage_id: u8,
        expected: ?u64,
        head_version: u64,
    ) !bool {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.compareAndSwapEnrichmentStageHeadVersion(namespace, @enumFromInt(stage_id), expected, head_version);
    }

    fn erasedGetEnrichmentStageDocOffset(ptr: *anyopaque, namespace: []const u8, stage_id: u8) !?u64 {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.getEnrichmentStageDocOffset(namespace, @enumFromInt(stage_id));
    }

    fn erasedCompareAndSwapEnrichmentStageDocOffset(
        ptr: *anyopaque,
        namespace: []const u8,
        stage_id: u8,
        expected: ?u64,
        doc_offset: u64,
    ) !bool {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.compareAndSwapEnrichmentStageDocOffset(namespace, @enumFromInt(stage_id), expected, doc_offset);
    }

    fn erasedGetEnrichmentStageHeadDocOffset(
        ptr: *anyopaque,
        namespace: []const u8,
        stage_id: u8,
        head_version: u64,
    ) !?u64 {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.getEnrichmentStageHeadDocOffset(namespace, @enumFromInt(stage_id), head_version);
    }

    fn erasedCompareAndSwapEnrichmentStageHeadDocOffset(
        ptr: *anyopaque,
        namespace: []const u8,
        stage_id: u8,
        head_version: u64,
        expected: ?u64,
        doc_offset: u64,
    ) !bool {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.compareAndSwapEnrichmentStageHeadDocOffset(
            namespace,
            @enumFromInt(stage_id),
            head_version,
            expected,
            doc_offset,
        );
    }

    fn erasedDeleteEnrichmentStageHeadDocOffset(
        ptr: *anyopaque,
        namespace: []const u8,
        stage_id: u8,
        head_version: u64,
    ) !void {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.deleteEnrichmentStageHeadDocOffset(namespace, @enumFromInt(stage_id), head_version);
    }

    fn erasedGetEnrichmentStageProgress(
        ptr: *anyopaque,
        namespace: []const u8,
        stage_id: u8,
    ) !?progress_store.EnrichmentStageProgress {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.getEnrichmentStageProgress(namespace, @enumFromInt(stage_id));
    }

    fn erasedCompareAndSwapEnrichmentStageProgress(
        ptr: *anyopaque,
        namespace: []const u8,
        stage_id: u8,
        expected: ?progress_store.EnrichmentStageProgress,
        desired: progress_store.EnrichmentStageProgress,
    ) !bool {
        const self: *FsProgressStore = @ptrCast(@alignCast(ptr));
        return try self.compareAndSwapEnrichmentStageProgress(
            namespace,
            @enumFromInt(stage_id),
            expected,
            desired,
        );
    }
};

fn stageProgressOptionalEql(
    lhs: ?progress_store.EnrichmentStageProgress,
    rhs: ?progress_store.EnrichmentStageProgress,
) bool {
    if (lhs == null or rhs == null) return lhs == null and rhs == null;
    return lhs.?.eql(rhs.?);
}

fn threadedIo() std.Io.Threaded {
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    platform_sync.lockYielding(mutex);
}

fn readFileAlloc(alloc: Allocator, path: []const u8) ![]u8 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    return try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(std.math.maxInt(usize)));
}

fn ensureParentDir(path: []const u8) !void {
    const parent = std.fs.path.dirname(path) orelse return;
    var io_impl = threadedIo();
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), parent);
}

fn writeFileAtomically(path: []const u8, contents: []const u8) !void {
    const tmp_path = try std.fmt.allocPrint(std.heap.page_allocator, "{s}.tmp-{d}", .{ path, test_nonce.fetchAdd(1, .monotonic) });
    defer std.heap.page_allocator.free(tmp_path);

    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();

    {
        var file = try std.Io.Dir.createFileAbsolute(io, tmp_path, .{ .truncate = true });
        defer file.close(io);

        var buf: [4096]u8 = undefined;
        var writer = file.writer(io, &buf);
        try writer.interface.writeAll(contents);
        try writer.end();
        try file.sync(io);
    }

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
    if (std.fs.path.dirname(path)) |parent| try fs_paths.syncDirPortable(io, parent);
}

fn pathForAlloc(alloc: Allocator, root_dir: []const u8, namespace: []const u8, kind: FsProgressStore.Kind) ![]u8 {
    return switch (kind) {
        .head => try headPathAlloc(alloc, root_dir, namespace),
        .gc_watermark => try std.fs.path.join(alloc, &.{ root_dir, namespace, "GC_WATERMARK" }),
        .manifest_gc_floor => try std.fs.path.join(alloc, &.{ root_dir, namespace, "MANIFEST_GC_FLOOR" }),
        .enrichment_head_version => try std.fs.path.join(alloc, &.{ root_dir, namespace, "ENRICHMENT_HEAD_VERSION" }),
        .enrichment_stage => try std.fs.path.join(alloc, &.{ root_dir, namespace, "ENRICHMENT_STAGE" }),
        .enrichment_doc_offset => try std.fs.path.join(alloc, &.{ root_dir, namespace, "ENRICHMENT_DOC_OFFSET" }),
    };
}

fn stageLeaf(stage: catalog_types.EnrichmentStage) []const u8 {
    return switch (stage) {
        .lexical_sparse => "LEXICAL_SPARSE",
        .chunk_preview => "CHUNK_PREVIEW",
        .chunk_embeddings => "CHUNK_EMBEDDINGS",
        .rerank_terms => "RERANK_TERMS",
    };
}

fn stagePathAlloc(alloc: Allocator, root_dir: []const u8, namespace: []const u8, stage: catalog_types.EnrichmentStage, leaf: []const u8) ![]u8 {
    return try std.fs.path.join(alloc, &.{ root_dir, namespace, "ENRICHMENT", stageLeaf(stage), leaf });
}

fn stageHeadOffsetPathAlloc(
    alloc: Allocator,
    root_dir: []const u8,
    namespace: []const u8,
    stage: catalog_types.EnrichmentStage,
    head_version: u64,
) ![]u8 {
    const head_leaf = try std.fmt.allocPrint(alloc, "{d}", .{head_version});
    defer alloc.free(head_leaf);
    return try std.fs.path.join(alloc, &.{ root_dir, namespace, "ENRICHMENT", stageLeaf(stage), "DOC_OFFSETS", head_leaf });
}

fn headPathAlloc(alloc: Allocator, root_dir: []const u8, namespace: []const u8) ![]u8 {
    return try std.fs.path.join(alloc, &.{ root_dir, namespace, "HEAD" });
}

fn openNamespaceLock(
    alloc: Allocator,
    root_dir: []const u8,
    namespace: []const u8,
    io: std.Io,
) !std.Io.File {
    const path = try std.fs.path.join(alloc, &.{ root_dir, namespace, ".progress.lock" });
    defer alloc.free(path);
    try ensureParentDir(path);
    return try fs_paths.createFilePortable(io, path, .{ .truncate = false, .read = true });
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
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-progress-{s}-{d}-{d}\x00", .{
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

test "serverless manifest read pins persist across filesystem owners and use monotonic CAS" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "manifest-read-pin");
    defer cleanupTmp(path);
    {
        var fs = try FsProgressStore.init(alloc, std.mem.span(path));
        var store = fs.progressStore();
        defer store.deinit();
        try std.testing.expectEqual(@as(?u64, null), try store.getManifestReadDeadline("docs", 1));
        try std.testing.expect(try store.compareAndSwapManifestReadDeadline("docs", 1, null, 100));
    }
    var fs = try FsProgressStore.init(alloc, std.mem.span(path));
    var store = fs.progressStore();
    defer store.deinit();
    try std.testing.expectEqual(@as(?u64, 100), try store.getManifestReadDeadline("docs", 1));
    try std.testing.expectEqual(@as(?u64, null), try store.getManifestReadDeadline("docs", 2));
    try std.testing.expect(!try store.compareAndSwapManifestReadDeadline("docs", 1, null, 200));
    try std.testing.expect(!try store.compareAndSwapManifestReadDeadline("docs", 1, 100, 99));
    try std.testing.expect(try store.compareAndSwapManifestReadDeadline("docs", 1, 100, 200));
    try std.testing.expectError(error.ManifestGcFloorNotCommitted, store.pruneManifestReadDeadlines("docs", 2, 201, .none));
    try std.testing.expect(try store.compareAndSwapManifestGcFloor("docs", null, 2));
    try store.pruneManifestReadDeadlines("docs", 2, 200, .none);
    try std.testing.expectEqual(@as(?u64, 200), try store.getManifestReadDeadline("docs", 1));
    try store.pruneManifestReadDeadlines("docs", 2, 201, .none);
    try std.testing.expectEqual(@as(?u64, null), try store.getManifestReadDeadline("docs", 1));
}

test "serverless enrichment stage cursor filesystem CAS preserves key across heads and permits fenced wrap" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "enrichment-cursor");
    defer cleanupTmp(path);
    var impl = try FsProgressStore.init(std.testing.allocator, std.mem.span(path));
    var store = impl.progressStore();
    defer store.deinit();
    try progress_store.testEnrichmentCursorCompareAndSwap(&store);
}

test "serverless manifest GC floor persists across filesystem owners and rejects rollback" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "manifest-gc-floor");
    defer cleanupTmp(path);
    {
        var fs = try FsProgressStore.init(alloc, std.mem.span(path));
        var store = fs.progressStore();
        defer store.deinit();
        try std.testing.expectEqual(@as(?u64, null), try store.getManifestGcFloor("docs"));
        try std.testing.expect(try store.compareAndSwapManifestGcFloor("docs", null, 2));
    }
    var fs = try FsProgressStore.init(alloc, std.mem.span(path));
    var store = fs.progressStore();
    defer store.deinit();
    try std.testing.expectEqual(@as(?u64, 2), try store.getManifestGcFloor("docs"));
    try std.testing.expect(!try store.compareAndSwapManifestGcFloor("docs", null, 3));
    try std.testing.expect(!try store.compareAndSwapManifestGcFloor("docs", 2, 1));
    try std.testing.expect(try store.compareAndSwapManifestGcFloor("docs", 2, 3));
    try std.testing.expectEqual(@as(?u64, 3), try store.getManifestGcFloor("docs"));
}

test "serverless fs progress store manages head and gc watermark with CAS" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "cas");
    defer cleanupTmp(path);

    var fs = try FsProgressStore.init(std.testing.allocator, std.mem.span(path));
    defer fs.deinit();

    try std.testing.expect(try fs.compareAndSwapHead("docs", null, 1));
    try std.testing.expectEqual(@as(u64, 1), try fs.getHead("docs"));
    try std.testing.expect(!(try fs.compareAndSwapHead("docs", null, 2)));
    try std.testing.expect(try fs.compareAndSwapHead("docs", 1, 2));

    try std.testing.expectEqual(@as(?u64, null), try fs.getGcWatermark("docs"));
    try std.testing.expect(try fs.compareAndSwapGcWatermark("docs", null, 10));
    try std.testing.expectEqual(@as(?u64, 10), try fs.getGcWatermark("docs"));
    try std.testing.expect(!(try fs.compareAndSwapGcWatermark("docs", null, 11)));
    try std.testing.expect(!(try fs.compareAndSwapGcWatermark("docs", 10, 9)));
    try std.testing.expect(try fs.compareAndSwapGcWatermark("docs", 10, 12));
    try std.testing.expectEqual(@as(?u64, 12), try fs.getGcWatermark("docs"));

    try std.testing.expectEqual(@as(?u64, null), try fs.getEnrichmentHeadVersion("docs"));
    try std.testing.expect(try fs.compareAndSwapEnrichmentHeadVersion("docs", null, 3));
    try std.testing.expectEqual(@as(?u64, 3), try fs.getEnrichmentHeadVersion("docs"));
    try std.testing.expectEqual(@as(?u64, null), try fs.getEnrichmentStage("docs"));
    try std.testing.expect(try fs.compareAndSwapEnrichmentStage("docs", null, 1));
    try std.testing.expectEqual(@as(?u64, 1), try fs.getEnrichmentStage("docs"));
    try std.testing.expect(try fs.compareAndSwapEnrichmentDocOffset("docs", null, 42));
    try std.testing.expectEqual(@as(?u64, 42), try fs.getEnrichmentDocOffset("docs"));
    try std.testing.expect(try fs.compareAndSwapEnrichmentStageHeadVersion("docs", .chunk_embeddings, null, 7));
    try std.testing.expectEqual(@as(?u64, 7), try fs.getEnrichmentStageHeadVersion("docs", .chunk_embeddings));
    try std.testing.expect(try fs.compareAndSwapEnrichmentStageDocOffset("docs", .chunk_embeddings, null, 99));
    try std.testing.expectEqual(@as(?u64, 99), try fs.getEnrichmentStageDocOffset("docs", .chunk_embeddings));
    try std.testing.expect(try fs.compareAndSwapEnrichmentStageHeadDocOffset("docs", .chunk_embeddings, 7, null, 5));
    try std.testing.expectEqual(@as(?u64, 5), try fs.getEnrichmentStageHeadDocOffset("docs", .chunk_embeddings, 7));
    try std.testing.expectEqual(@as(?u64, null), try fs.getEnrichmentStageHeadDocOffset("docs", .chunk_embeddings, 8));
}

test "serverless enrichment progress isolates overlapping published head offsets" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "enrichment-head-offsets");
    defer cleanupTmp(path);

    var fs = try FsProgressStore.init(std.testing.allocator, std.mem.span(path));
    // Seed the legacy layout to prove migration preserves current progress.
    try std.testing.expect(try fs.compareAndSwapEnrichmentStageDocOffset("docs", .lexical_sparse, null, 4));
    var store = fs.progressStore();
    defer store.deinit();
    try std.testing.expect(try store.compareAndSwapEnrichmentStageHeadVersion("docs", .lexical_sparse, null, 1));
    try std.testing.expectEqual(@as(?u64, 4), try store.getEnrichmentStageDocOffset("docs", .lexical_sparse));
    try std.testing.expect(try store.compareAndSwapEnrichmentStageDocOffset("docs", .lexical_sparse, 4, 5));

    // Initialize the future head before publication. An old worker may still
    // advance head 1 after the head CAS, but it cannot alter head 2's offset.
    try std.testing.expect(try store.compareAndSwapEnrichmentStageHeadDocOffset("docs", .lexical_sparse, 2, null, 0));
    try std.testing.expect(try store.compareAndSwapEnrichmentStageHeadVersion("docs", .lexical_sparse, 1, 2));
    try std.testing.expect(!(try store.compareAndSwapEnrichmentStageHeadVersion("docs", .lexical_sparse, 2, 1)));
    try std.testing.expect(try store.compareAndSwapEnrichmentStageHeadDocOffset("docs", .lexical_sparse, 1, 5, 6));
    try std.testing.expectEqual(@as(?u64, 0), try store.getEnrichmentStageDocOffset("docs", .lexical_sparse));
    try store.deleteEnrichmentStageHeadDocOffset("docs", .lexical_sparse, 1);
    try std.testing.expectEqual(@as(?u64, null), try store.getEnrichmentStageHeadDocOffset("docs", .lexical_sparse, 1));
    try std.testing.expect(!(try store.compareAndSwapEnrichmentStageHeadDocOffset("docs", .lexical_sparse, 1, null, 0)));

    const first = progress_store.EnrichmentStageProgress{ .head_version = 2, .doc_offset = 4 };
    try std.testing.expect(try store.compareAndSwapEnrichmentStageProgress("docs", .chunk_preview, null, first));
    const next = progress_store.EnrichmentStageProgress{ .head_version = 3, .doc_offset = 0 };
    try std.testing.expect(try store.compareAndSwapEnrichmentStageProgress("docs", .chunk_preview, first, next));
    try std.testing.expect(!(try store.compareAndSwapEnrichmentStageProgress(
        "docs",
        .chunk_preview,
        first,
        .{ .head_version = 2, .doc_offset = 5 },
    )));
    try std.testing.expectEqual(@as(?progress_store.EnrichmentStageProgress, next), try store.getEnrichmentStageProgress("docs", .chunk_preview));
}

test "serverless fs progress store allows exactly one cross-instance head CAS winner" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "cas-race");
    defer cleanupTmp(path);

    var store_a = try FsProgressStore.init(alloc, std.mem.span(path));
    defer store_a.deinit();
    var store_b = try FsProgressStore.init(alloc, std.mem.span(path));
    defer store_b.deinit();
    try std.testing.expect(try store_a.compareAndSwapHead("docs", null, 1));

    const RaceState = struct {
        io: std.Io,
        stores: [2]*FsProgressStore,
        ready_count: std.atomic.Value(u32) = .init(0),
        ready: std.Io.Event = .unset,
        start: std.Io.Event = .unset,
        winner_count: std.atomic.Value(u32) = .init(0),
        error_count: std.atomic.Value(u32) = .init(0),

        fn race(self: *@This(), index: usize) void {
            if (self.ready_count.fetchAdd(1, .release) == 1) self.ready.set(self.io);
            self.start.waitUncancelable(self.io);
            const published = self.stores[index].compareAndSwapHead(
                "docs",
                1,
                2 + @as(u64, @intCast(index)),
            ) catch {
                _ = self.error_count.fetchAdd(1, .monotonic);
                return;
            };
            if (published) _ = self.winner_count.fetchAdd(1, .monotonic);
        }
    };

    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    var state = RaceState{ .io = io, .stores = .{ &store_a, &store_b } };
    var group: std.Io.Group = .init;
    errdefer group.cancel(io);
    group.async(io, RaceState.race, .{ &state, 0 });
    group.async(io, RaceState.race, .{ &state, 1 });
    state.ready.waitUncancelable(io);
    state.start.set(io);
    try group.await(io);

    try std.testing.expectEqual(@as(u32, 0), state.error_count.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 1), state.winner_count.load(.monotonic));
    const final_head = try store_a.getHead("docs");
    try std.testing.expect(final_head == 2 or final_head == 3);
}

test "serverless fs progress store preserves cross-instance GC watermark CAS" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "gc-cas-cross-instance");
    defer cleanupTmp(path);

    var store_a = try FsProgressStore.init(alloc, std.mem.span(path));
    defer store_a.deinit();
    var store_b = try FsProgressStore.init(alloc, std.mem.span(path));
    defer store_b.deinit();
    try std.testing.expect(try store_a.compareAndSwapGcWatermark("docs", null, 10));

    const Race = struct {
        io: std.Io,
        stores: [2]*FsProgressStore,
        ready_count: std.atomic.Value(u32) = .init(0),
        ready: std.Io.Event = .unset,
        start: std.Io.Event = .unset,
        winners: std.atomic.Value(u32) = .init(0),
        errors: std.atomic.Value(u32) = .init(0),

        fn run(self: *@This(), index: usize) void {
            if (self.ready_count.fetchAdd(1, .release) == 1) self.ready.set(self.io);
            self.start.waitUncancelable(self.io);
            const advanced = self.stores[index].compareAndSwapGcWatermark(
                "docs",
                10,
                20 + @as(u64, @intCast(index)),
            ) catch {
                _ = self.errors.fetchAdd(1, .monotonic);
                return;
            };
            if (advanced) _ = self.winners.fetchAdd(1, .monotonic);
        }
    };

    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    var race = Race{ .io = io, .stores = .{ &store_a, &store_b } };
    var group: std.Io.Group = .init;
    errdefer group.cancel(io);
    group.async(io, Race.run, .{ &race, 0 });
    group.async(io, Race.run, .{ &race, 1 });
    race.ready.waitUncancelable(io);
    race.start.set(io);
    try group.await(io);

    try std.testing.expectEqual(@as(u32, 0), race.errors.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 1), race.winners.load(.monotonic));
    const final = (try store_a.getGcWatermark("docs")).?;
    try std.testing.expect(final == 20 or final == 21);
}

test "serverless fs publication fencing survives bootstrap takeover and reopen" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "fenced-bootstrap");
    defer cleanupTmp(path);
    var first = try FsProgressStore.init(alloc, std.mem.span(path));
    defer first.deinit();
    var second = try FsProgressStore.init(alloc, std.mem.span(path));
    defer second.deinit();
    var a = first.progressStore();
    var b = second.progressStore();
    const pa = try a.workLeaseProvider();
    const pb = try b.workLeaseProvider();
    const stale = (try pa.acquireBootstrap("docs", "one", 100, 10)).?;
    try std.testing.expectError(error.FileNotFound, b.getHead("docs"));
    try std.testing.expect((try pb.acquire("docs", "two", 109, 10)) == null);
    const current = (try pb.acquire("docs", "two", 110, 10)).?;
    try std.testing.expect(current.fencing_token > stale.fencing_token);
    try std.testing.expectError(error.WorkLeaseLost, a.compareAndSwapHeadFenced("docs", null, 1, .{ .owner_id = "one", .fencing_token = stale.fencing_token }));
    try std.testing.expect(try b.compareAndSwapHeadFenced("docs", null, 1, .{ .owner_id = "two", .fencing_token = current.fencing_token }));
    try std.testing.expect(try pb.release("docs", "two", current.fencing_token));
    var reopened = try FsProgressStore.init(alloc, std.mem.span(path));
    defer reopened.deinit();
    var c = reopened.progressStore();
    try std.testing.expectEqual(@as(u64, 1), try c.getHead("docs"));
    const pc = try c.workLeaseProvider();
    const next = (try pc.acquire("docs", "three", 111, 10)).?;
    try std.testing.expect(next.fencing_token > current.fencing_token);
    try std.testing.expectError(error.WorkLeaseLost, pb.renew("docs", "two", current.fencing_token, 112, 10));
    try std.testing.expect(!try pa.release("docs", "one", stale.fencing_token));
    try std.testing.expect(try pc.release("docs", "three", next.fencing_token));
}

test "serverless fs enrichment progress CAS is atomic across store instances" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "stage-cas-cross-instance");
    defer cleanupTmp(path);

    var store_a = try FsProgressStore.init(alloc, std.mem.span(path));
    defer store_a.deinit();
    var store_b = try FsProgressStore.init(alloc, std.mem.span(path));
    defer store_b.deinit();
    const initial = progress_store.EnrichmentStageProgress{ .head_version = 1, .doc_offset = 1 };
    try std.testing.expect(try store_a.compareAndSwapEnrichmentStageProgress("docs", .lexical_sparse, null, initial));

    const Race = struct {
        io: std.Io,
        stores: [2]*FsProgressStore,
        ready_count: std.atomic.Value(u32) = .init(0),
        ready: std.Io.Event = .unset,
        start: std.Io.Event = .unset,
        winners: std.atomic.Value(u32) = .init(0),
        errors: std.atomic.Value(u32) = .init(0),

        fn run(self: *@This(), index: usize) void {
            if (self.ready_count.fetchAdd(1, .release) == 1) self.ready.set(self.io);
            self.start.waitUncancelable(self.io);
            const won = self.stores[index].compareAndSwapEnrichmentStageProgress(
                "docs",
                .lexical_sparse,
                initial,
                .{ .head_version = 1, .doc_offset = 2 + @as(u64, @intCast(index)) },
            ) catch {
                _ = self.errors.fetchAdd(1, .monotonic);
                return;
            };
            if (won) _ = self.winners.fetchAdd(1, .monotonic);
        }
    };

    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    var race = Race{ .io = io, .stores = .{ &store_a, &store_b } };
    var group: std.Io.Group = .init;
    errdefer group.cancel(io);
    group.async(io, Race.run, .{ &race, 0 });
    group.async(io, Race.run, .{ &race, 1 });
    race.ready.waitUncancelable(io);
    race.start.set(io);
    try group.await(io);

    try std.testing.expectEqual(@as(u32, 0), race.errors.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 1), race.winners.load(.monotonic));
    const final = (try store_a.getEnrichmentStageProgress("docs", .lexical_sparse)).?;
    try std.testing.expectEqual(@as(u64, 1), final.head_version);
    try std.testing.expect(final.doc_offset == 2 or final.doc_offset == 3);
}
