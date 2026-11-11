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

//! Preserve artifact-read provenance without changing backend capabilities.
//! Source readers do not use this adapter: their missing objects must remain
//! source failures rather than invalidating resumable derived work.
const std = @import("std");
const stores = @import("../serverless/artifacts/store.zig");
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
const local = @import("antfly_local_sources");
const Cache = local.serverless_query_lake_decoded_cache.Cache;
const residency = @import("antfly_platform").allocator.reclamation;
pub const Reads = struct {
    backend: stores.ArtifactStore,
    vtable: stores.ArtifactStore.VTable,
    cause_code: std.atomic.Value(u16) = .init(0),
    written: ?Cache = null,
    quota: ?*residency.Quota = null,
    reclaimer: residency.Reclaimer = undefined,
    const Written = struct { bytes: []const u8 };
    pub fn enableWriteCache(self: *Reads, a: A, maximum: usize) void {
        std.debug.assert(self.written == null);
        // Only a synchronized, refundable owner may retain optional bytes.
        // Opaque or monotonic allocators keep ordinary required-read behavior.
        const quota = residency.Quota.owner(a) orelse return;
        if (maximum == 0) return;
        self.written = .{ .a = a, .max_bytes = maximum, .max_entries = 512 };
        self.quota = quota;
        self.reclaimer = .{ .ptr = self, .reclaim = reclaim };
        quota.residency.register(&self.reclaimer);
    }
    fn reclaim(raw: *anyopaque) bool {
        const self: *Reads = @ptrCast(@alignCast(raw));
        const quota = self.quota orelse return false;
        if (!residency.Guard.allowed() or !quota.mutex.tryLock()) return false;
        defer quota.mutex.unlock();
        residency.Guard.enter(&quota.mutex);
        defer residency.Guard.leave();
        residency.Guard.beginReclaim();
        defer residency.Guard.endReclaim();
        const before = quota.live_bytes;
        const cache = &self.written.?;
        _ = cache.reclaim(256 * 1024);
        if (cache.mutex.tryLock()) {
            defer cache.mutex.unlock();
            if (cache.entries.count() == 0) {
                cache.entries.deinit(cache.a);
                cache.entries = .empty;
            }
        }
        return quota.live_bytes < before;
    }
    pub fn close(self: *Reads) void {
        if (self.quota) |quota| quota.residency.unregister(&self.reclaimer);
        self.quota = null;
        if (self.written) |*cache| cache.deinit();
        self.written = null;
    }
    fn key(id: []const u8) [32]u8 {
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(id, &digest, .{});
        return digest;
    }
    fn remember(self: *Reads, metadata: stores.ArtifactMetadata, bytes: []const u8) void {
        const cache = if (self.written) |*value| value else return;
        // A bounded attempt-local hint, never a new requirement on publication.
        // No credential, reader context or old-generation authority is retained.
        if (bytes.len > 256 * 1024 or bytes.len > cache.max_bytes or metadata.byte_len != bytes.len) return;
        stores.validateSha256ArtifactIdentity(metadata.artifact_id, metadata.checksum) catch return;
        var checksum: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &checksum, .{});
        if (!std.mem.eql(u8, &std.fmt.bytesToHex(checksum, .lower), metadata.checksum)) return;
        const lease = cache.create(bytes.len * 2 + 64 * 1024) catch return;
        defer lease.release();
        const a = lease.item.arena.allocator();
        const value = a.create(Written) catch return;
        value.* = .{ .bytes = a.dupe(u8, bytes) catch return };
        lease.item.payload = .{ .extension = value };
        cache.publish(key(metadata.artifact_id), lease);
    }
    const Shape = struct { offset: u64 = 0, length: ?usize = null, total: ?u64 = null, maximum: ?usize = null, checksum: ?[]const u8 = null };
    fn copy(self: *Reads, a: A, id: []const u8, shape: Shape, token: Cancellation) !?[]u8 {
        try token.check();
        const cache = if (self.written) |*value| value else return null;
        const lease = cache.lookup(key(id)) orelse return null;
        var released = false;
        defer if (!released) lease.release();
        const value: *const Written = @ptrCast(@alignCast(lease.item.payload.extension));
        if (shape.total) |total| if (value.bytes.len != total) return self.failure(error.ArtifactIntegrityMismatch);
        if (shape.maximum) |maximum| if (value.bytes.len > maximum) return self.failure(error.ArtifactIntegrityMismatch);
        if (shape.checksum) |expected| stores.validateSha256ArtifactIdentity(id, expected) catch |err| return self.failure(err);
        const length = shape.length orelse value.bytes.len;
        if (shape.offset > value.bytes.len or length > value.bytes.len - shape.offset) return self.failure(error.ArtifactIntegrityMismatch);
        const bytes = a.dupe(u8, value.bytes[@intCast(shape.offset)..][0..length]) catch |err| {
            if (err != error.OutOfMemory) return err;
            // The required output may need the same quota as this pinned hint.
            // Drop its lease before retrying through the ordinary backend.
            lease.release();
            released = true;
            cache.discard(key(id));
            return null;
        };
        errdefer a.free(bytes);
        try token.check();
        return bytes;
    }

    pub fn cause(self: *const Reads) ?anyerror {
        const code = self.cause_code.load(.acquire);
        return if (code == 0) null else @errorFromInt(code);
    }
    pub fn init(backend: stores.ArtifactStore) Reads {
        const vtable: stores.ArtifactStore.VTable = .{
            .optional_read_view = optional_read_view,
            .object_store = if (backend.vtable.object_store != null) object_store else null,
            .retention_witness = if (backend.vtable.retention_witness != null) retention_witness else null,
            .retained_closure = if (backend.vtable.retained_closure != null) retained_closure else null,
            .remember_closure = if (backend.vtable.remember_closure != null) remember_closure else null,
            .inherit_closure = if (backend.vtable.inherit_closure != null) inherit_closure else null,
            .seal_closures = if (backend.vtable.seal_closures != null) seal_closures else null,
            .deinit = deinit,
            .put = put,
            .put_with_cancellation = if (backend.vtable.put_with_cancellation != null) put_with_cancellation else null,
            .get_alloc = get_alloc,
            .get_alloc_with_cancellation = if (backend.vtable.get_alloc_with_cancellation != null) get_alloc_with_cancellation else null,
            .get_bounded_alloc_with_cancellation = if (backend.vtable.get_bounded_alloc_with_cancellation != null) get_bounded_alloc_with_cancellation else null,
            .get_range_alloc = get_range_alloc,
            .get_range_alloc_with_cancellation = if (backend.vtable.get_range_alloc_with_cancellation != null) get_range_alloc_with_cancellation else null,
            .get_verified_range_alloc_with_cancellation = if (backend.vtable.get_verified_range_alloc_with_cancellation != null) get_verified_range_alloc_with_cancellation else null,
            .get_verified_range_alloc_with_budget = if (backend.vtable.get_verified_range_alloc_with_budget != null) get_verified_range_alloc_with_budget else null,
            .stat = stat,
            .stat_with_cancellation = if (backend.vtable.stat_with_cancellation != null) stat_with_cancellation else null,
            .verify_content = if (backend.vtable.verify_content != null) verify_content else null,
            .delete = delete,
            .delete_with_cancellation = if (backend.vtable.delete_with_cancellation != null) delete_with_cancellation else null,
            .put_scoped = if (backend.vtable.put_scoped != null) put_scoped else null,
            .put_scoped_many = if (backend.vtable.put_scoped_many != null) put_scoped_many else null,
            .visit_scoped_uploads = if (backend.vtable.visit_scoped_uploads != null) visit_scoped_uploads else null,
            .reclaim_retired_scoped_inventory = if (backend.vtable.reclaim_retired_scoped_inventory != null) reclaim_retired_scoped_inventory else null,
            .cleanup_retired_scoped_temporaries = if (backend.vtable.cleanup_retired_scoped_temporaries != null) cleanup_retired_scoped_temporaries else null,
        };
        return .{ .backend = backend, .vtable = vtable };
    }
    /// Both this adapter and its borrowed backend outlive the returned handle.
    pub fn store(self: *Reads) stores.ArtifactStore {
        var handle = self.backend;
        handle.ptr = self;
        handle.vtable = &self.vtable;
        return handle;
    }
    fn optional_read_view(raw: *anyopaque) stores.ArtifactStore {
        const self: *Reads = @ptrCast(@alignCast(raw));
        return self.backend.optionalReadView();
    }
    fn object_store(raw: *anyopaque) *@import("../serverless/artifacts/object_store.zig").ObjectStore {
        const self: *Reads = @ptrCast(@alignCast(raw));
        return self.backend.providerStore().?;
    }
    fn retention_witness(raw: *anyopaque, scope: stores.UploadScope, token: Cancellation) !?stores.RetentionWitness {
        const self: *Reads = @ptrCast(@alignCast(raw));
        return self.backend.retentionWitness(scope, token);
    }
    fn retained_closure(raw: *anyopaque, id: []const u8, token: Cancellation) !bool {
        const self: *Reads = @ptrCast(@alignCast(raw));
        return self.backend.retainedClosure(id, token);
    }
    fn remember_closure(raw: *anyopaque, id: []const u8, witnesses: []const stores.RetentionWitness, token: Cancellation) !void {
        const self: *Reads = @ptrCast(@alignCast(raw));
        return self.backend.rememberClosure(id, witnesses, token);
    }
    fn inherit_closure(raw: *anyopaque, id: []const u8, previous: ?[]const u8, scope: stores.UploadScope, token: Cancellation) !void {
        const self: *Reads = @ptrCast(@alignCast(raw));
        return self.backend.vtable.inherit_closure.?(self.backend.ptr, id, previous, scope, token);
    }
    fn seal_closures(raw: *anyopaque, token: Cancellation) !void {
        const self: *Reads = @ptrCast(@alignCast(raw));
        return self.backend.sealClosures(token);
    }
    fn delete_with_cancellation(raw: *anyopaque, id: []const u8, token: Cancellation) !void {
        const self: *Reads = @ptrCast(@alignCast(raw));
        // A failed response does not prove that the remote mutation failed.
        defer if (self.written) |*cache| cache.discard(key(id));
        try self.backend.deleteWithCancellation(id, token);
    }
    fn failure(self: *Reads, err: anyerror) anyerror {
        switch (err) {
            error.FileNotFound, error.NotFound, error.CloudGenerationMapUnavailable, error.ArtifactIntegrityMismatch => {
                self.cause_code.store(@intFromError(err), .release);
                return error.LakeIndexArtifactUnavailable;
            },
            else => return err,
        }
    }
    fn deinit(a: A, raw: *anyopaque) void {
        const self: *Reads = @ptrCast(@alignCast(raw));
        _ = a;
        _ = self;
    }
    fn put(raw: *anyopaque, a: A, bytes: []const u8) !stores.ArtifactMetadata {
        const self: *Reads = @ptrCast(@alignCast(raw));
        const metadata = try self.backend.vtable.put(self.backend.ptr, a, bytes);
        self.remember(metadata, bytes);
        return metadata;
    }
    fn put_with_cancellation(raw: *anyopaque, a: A, bytes: []const u8, cancellation: Cancellation) !stores.ArtifactMetadata {
        const self: *Reads = @ptrCast(@alignCast(raw));
        const metadata = try self.backend.vtable.put_with_cancellation.?(self.backend.ptr, a, bytes, cancellation);
        self.remember(metadata, bytes);
        return metadata;
    }
    fn get_alloc(raw: *anyopaque, a: A, id: []const u8) ![]u8 {
        const self: *Reads = @ptrCast(@alignCast(raw));
        if (try self.copy(a, id, .{}, .none)) |value| return value;
        return self.backend.vtable.get_alloc(self.backend.ptr, a, id) catch |err| self.failure(err);
    }
    fn get_alloc_with_cancellation(raw: *anyopaque, a: A, id: []const u8, cancellation: Cancellation) ![]u8 {
        const self: *Reads = @ptrCast(@alignCast(raw));
        if (try self.copy(a, id, .{}, cancellation)) |value| return value;
        return self.backend.vtable.get_alloc_with_cancellation.?(self.backend.ptr, a, id, cancellation) catch |err| self.failure(err);
    }
    fn get_bounded_alloc_with_cancellation(raw: *anyopaque, a: A, id: []const u8, len: usize, cancellation: Cancellation) ![]u8 {
        const self: *Reads = @ptrCast(@alignCast(raw));
        if (try self.copy(a, id, .{ .maximum = len }, cancellation)) |value| return value;
        return self.backend.vtable.get_bounded_alloc_with_cancellation.?(self.backend.ptr, a, id, len, cancellation) catch |err| self.failure(err);
    }
    fn get_range_alloc(raw: *anyopaque, a: A, id: []const u8, offset: u64, len: usize) ![]u8 {
        const self: *Reads = @ptrCast(@alignCast(raw));
        if (try self.copy(a, id, .{ .offset = offset, .length = len }, .none)) |value| return value;
        return self.backend.vtable.get_range_alloc(self.backend.ptr, a, id, offset, len) catch |err| self.failure(err);
    }
    fn get_range_alloc_with_cancellation(raw: *anyopaque, a: A, id: []const u8, offset: u64, len: usize, cancellation: Cancellation) ![]u8 {
        const self: *Reads = @ptrCast(@alignCast(raw));
        if (try self.copy(a, id, .{ .offset = offset, .length = len }, cancellation)) |value| return value;
        return self.backend.vtable.get_range_alloc_with_cancellation.?(self.backend.ptr, a, id, offset, len, cancellation) catch |err| self.failure(err);
    }
    fn get_verified_range_alloc_with_cancellation(raw: *anyopaque, a: A, id: []const u8, bytes: u64, checksum: []const u8, offset: u64, len: usize, cancellation: Cancellation) ![]u8 {
        const self: *Reads = @ptrCast(@alignCast(raw));
        if (try self.copy(a, id, .{ .offset = offset, .length = len, .total = bytes, .checksum = checksum }, cancellation)) |value| return value;
        return self.backend.vtable.get_verified_range_alloc_with_cancellation.?(self.backend.ptr, a, id, bytes, checksum, offset, len, cancellation) catch |err| self.failure(err);
    }
    fn get_verified_range_alloc_with_budget(raw: *anyopaque, a: A, id: []const u8, bytes: u64, checksum: []const u8, offset: u64, len: usize, cancellation: Cancellation, remaining: *u64) ![]u8 {
        const self: *Reads = @ptrCast(@alignCast(raw));
        if (try self.copy(a, id, .{ .offset = offset, .length = len, .total = bytes, .checksum = checksum }, cancellation)) |value| return value;
        return self.backend.vtable.get_verified_range_alloc_with_budget.?(self.backend.ptr, a, id, bytes, checksum, offset, len, cancellation, remaining) catch |err| self.failure(err);
    }
    fn stat(raw: *anyopaque, a: A, id: []const u8) !stores.ArtifactMetadata {
        const self: *Reads = @ptrCast(@alignCast(raw));
        return self.backend.vtable.stat(self.backend.ptr, a, id) catch |err| self.failure(err);
    }
    fn stat_with_cancellation(raw: *anyopaque, a: A, id: []const u8, cancellation: Cancellation) !stores.ArtifactMetadata {
        const self: *Reads = @ptrCast(@alignCast(raw));
        return self.backend.vtable.stat_with_cancellation.?(self.backend.ptr, a, id, cancellation) catch |err| self.failure(err);
    }
    fn verify_content(raw: *anyopaque, a: A, id: []const u8, bytes: u64, checksum: []const u8, cancellation: Cancellation) !void {
        const self: *Reads = @ptrCast(@alignCast(raw));
        return self.backend.vtable.verify_content.?(self.backend.ptr, a, id, bytes, checksum, cancellation) catch |err| self.failure(err);
    }
    fn delete(raw: *anyopaque, id: []const u8) !void {
        const self: *Reads = @ptrCast(@alignCast(raw));
        defer if (self.written) |*cache| cache.discard(key(id));
        try self.backend.vtable.delete(self.backend.ptr, id);
    }
    fn put_scoped(raw: *anyopaque, a: A, scope: stores.UploadScope, bytes: []const u8, cancellation: Cancellation) !stores.ArtifactMetadata {
        const self: *Reads = @ptrCast(@alignCast(raw));
        const metadata = try self.backend.vtable.put_scoped.?(self.backend.ptr, a, scope, bytes, cancellation);
        self.remember(metadata, bytes);
        return metadata;
    }
    fn put_scoped_many(raw: *anyopaque, a: A, scope: stores.UploadScope, contents: []const []const u8, token: Cancellation) ![]stores.ArtifactMetadata {
        const self: *Reads = @ptrCast(@alignCast(raw));
        const metadata = try self.backend.vtable.put_scoped_many.?(self.backend.ptr, a, scope, contents, token);
        if (metadata.len == contents.len) for (metadata, contents) |item, bytes| self.remember(item, bytes);
        return metadata;
    }
    fn visit_scoped_uploads(raw: *anyopaque, domain: [32]u8, visitor: stores.ScopedUploadVisitor, cancellation: Cancellation) !void {
        const self: *Reads = @ptrCast(@alignCast(raw));
        return self.backend.vtable.visit_scoped_uploads.?(self.backend.ptr, domain, visitor, cancellation);
    }
    fn reclaim_retired_scoped_inventory(raw: *anyopaque, domain: [32]u8, floor: u64, cutoff: u64, cancellation: Cancellation) !void {
        const self: *Reads = @ptrCast(@alignCast(raw));
        return self.backend.vtable.reclaim_retired_scoped_inventory.?(self.backend.ptr, domain, floor, cutoff, cancellation);
    }
    fn cleanup_retired_scoped_temporaries(raw: *anyopaque, domain: [32]u8, floor: u64, cutoff: u64, cancellation: Cancellation) !void {
        const self: *Reads = @ptrCast(@alignCast(raw));
        return self.backend.vtable.cleanup_retired_scoped_temporaries.?(self.backend.ptr, domain, floor, cutoff, cancellation);
    }
};

test "external lake artifact recovery preserves transient failures and optional capabilities" {
    const Backend = struct {
        fn read(_: *anyopaque, _: A, id: []const u8) ![]u8 {
            if (std.mem.eql(u8, id, "missing")) return error.FileNotFound;
            if (std.mem.eql(u8, id, "not-found")) return error.NotFound;
            if (std.mem.eql(u8, id, "corrupt")) return error.ArtifactIntegrityMismatch;
            if (std.mem.eql(u8, id, "timeout")) return error.Timeout;
            if (std.mem.eql(u8, id, "denied")) return error.AccessDenied;
            return error.Canceled;
        }
    };
    const vtable: stores.ArtifactStore.VTable = .{ .deinit = undefined, .put = undefined, .get_alloc = Backend.read, .get_range_alloc = undefined, .stat = undefined, .delete = undefined };
    var reads = Reads.init(.{ .allocator = std.testing.allocator, .ptr = undefined, .vtable = &vtable });
    var handle = reads.store();
    try std.testing.expect(handle.vtable.get_bounded_alloc_with_cancellation == null);
    try std.testing.expect(handle.vtable.get_verified_range_alloc_with_budget == null);
    for ([_][]const u8{ "missing", "not-found", "corrupt" }, [_]anyerror{ error.FileNotFound, error.NotFound, error.ArtifactIntegrityMismatch }) |id, expected| {
        try std.testing.expectError(error.LakeIndexArtifactUnavailable, handle.getAlloc(id));
        try std.testing.expectEqual(expected, reads.cause().?);
    }
    try std.testing.expectError(error.Timeout, handle.getAlloc("timeout"));
    try std.testing.expectError(error.AccessDenied, handle.getAlloc("denied"));
    try std.testing.expectError(error.Canceled, handle.getAlloc("canceled"));
    // The same unwrapped read error retains source provenance.
    try std.testing.expectError(error.FileNotFound, Backend.read(undefined, std.testing.allocator, "missing"));
}

test "external lake publication write cache preserves required reads reclamation cancellation and deletion" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("publication-write-cache");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var backend = fs.artifactStore();
    backend.upload_scope = try stores.UploadScope.forPublication(@splat(7), 1, std.testing.io);
    var quota = residency.Quota.init(a, 2 * 1024 * 1024);
    defer quota.deinit();
    var adapter = Reads.init(backend);
    adapter.enableWriteCache(quota.allocator(), 1024 * 1024);
    defer adapter.close();
    var handle = adapter.store();
    var metadata = try handle.putWithCancellation("immutable publication metadata", .none);
    defer metadata.deinit(a);
    try std.testing.expect(adapter.written.?.entries.count() != 0);
    const before = adapter.written.?.hits;
    const bytes = try handle.getVerifiedAllocWithCancellation(metadata.artifact_id, @intCast(metadata.byte_len), metadata.checksum, .none);
    defer a.free(bytes);
    try std.testing.expectEqualStrings("immutable publication metadata", bytes);
    try std.testing.expect(adapter.written.?.hits > before);
    var budget: u64 = 1;
    try std.testing.expectError(error.ArtifactReadBudgetExceeded, handle.getVerifiedRangeAllocWithBudget(a, metadata.artifact_id, metadata.byte_len, metadata.checksum, 0, 2, .none, &budget));
    const cancelled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, handle.getAllocWithCancellation(metadata.artifact_id, Cancellation.fromAtomic(&cancelled)));
    // All optional storage, including empty hash-table capacity, must yield to
    // a required allocation that needs the complete working-set allowance.
    const required = try quota.allocator().alloc(u8, 2 * 1024 * 1024);
    quota.allocator().free(required);
    try std.testing.expectEqual(@as(usize, 0), quota.live_bytes);
    const fallback = try handle.getAlloc(metadata.artifact_id);
    defer a.free(fallback);
    try std.testing.expectEqualStrings(bytes, fallback);
    var second = try handle.putWithCancellation("delete residency too", .none);
    defer second.deinit(a);
    try handle.delete(second.artifact_id);
    try std.testing.expectError(error.LakeIndexArtifactUnavailable, handle.getAlloc(second.artifact_id));
}

test "external lake publication write cache invalidates ambiguous deletes" {
    const Fs = @import("../serverless/artifacts/fs_store.zig").FsStore;
    const Backend = struct {
        fn remove(raw: *anyopaque, id: []const u8) !void {
            const fs: *Fs = @ptrCast(@alignCast(raw));
            var original = fs.artifactStore();
            try original.delete(id);
            return error.Timeout;
        }
        fn removeWithCancellation(raw: *anyopaque, id: []const u8, token: Cancellation) !void {
            const fs: *Fs = @ptrCast(@alignCast(raw));
            var original = fs.artifactStore();
            try original.deleteWithCancellation(id, token);
            return error.Canceled;
        }
    };
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("publication-cache-ambiguous-delete");
    defer directory.cleanup();
    var fs = try Fs.init(a, directory.path());
    defer fs.deinit();
    var backend = fs.artifactStore();
    var vtable = backend.vtable.*;
    vtable.delete = Backend.remove;
    vtable.delete_with_cancellation = Backend.removeWithCancellation;
    backend.vtable = &vtable;
    var quota = residency.Quota.init(a, 1024 * 1024);
    defer quota.deinit();
    for ([_][]const u8{ "ambiguous cancellation", "ambiguous timeout" }, 0..) |content, index| {
        // The public API prefers cancellation-aware deletion when present;
        // exercise the ordinary callback through a backend without that hook.
        if (index == 1) vtable.delete_with_cancellation = null;
        var adapter = Reads.init(backend);
        adapter.enableWriteCache(quota.allocator(), 512 * 1024);
        defer adapter.close();
        var handle = adapter.store();
        var metadata = try handle.putWithCancellation(content, .none);
        defer metadata.deinit(a);
        try std.testing.expect(adapter.written.?.entries.contains(Reads.key(metadata.artifact_id)));
        if (index == 0) {
            try std.testing.expectError(error.Canceled, handle.deleteWithCancellation(metadata.artifact_id, .none));
        } else {
            try std.testing.expectError(error.Timeout, handle.delete(metadata.artifact_id));
        }
        // The remote mutation committed despite its failed response. A later
        // reader must discover that absence through the required backend path.
        try std.testing.expect(!adapter.written.?.entries.contains(Reads.key(metadata.artifact_id)));
        try std.testing.expectError(error.LakeIndexArtifactUnavailable, handle.getAlloc(metadata.artifact_id));
    }
}

test "external lake publication write cache yields its pin when required output uses the same quota" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("publication-cache-required-output");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var backend = fs.artifactStore();
    backend.upload_scope = try stores.UploadScope.forPublication(@splat(6), 1, std.testing.io);
    var quota = residency.Quota.init(a, 256 * 1024);
    defer quota.deinit();
    var adapter = Reads.init(backend);
    adapter.enableWriteCache(quota.allocator(), 256 * 1024);
    defer adapter.close();
    var handle = adapter.store();
    const content = try a.alloc(u8, 128 * 1024);
    defer a.free(content);
    @memset(content, 42);
    var metadata = try handle.putWithCancellation(content, .none);
    defer metadata.deinit(a);
    try std.testing.expect(adapter.written.?.entries.count() != 0);
    // The public range boundary charges requested bytes. The backend's extra
    // budget is only for cold full-content authentication, not another range.
    try backend.verifyContentWithCancellationUsingAllocator(a, metadata.artifact_id, metadata.byte_len, metadata.checksum, .none);
    var remaining: u64 = content.len;
    const required = try handle.getVerifiedRangeAllocWithBudget(quota.allocator(), metadata.artifact_id, metadata.byte_len, metadata.checksum, 0, content.len, .none, &remaining);
    try std.testing.expectEqual(@as(u64, 0), remaining);
    defer quota.allocator().free(required);
    try std.testing.expectEqualSlices(u8, content, required);
    try std.testing.expectEqual(@as(usize, 0), adapter.written.?.entries.count());
}

test "external lake artifact recovery preserves batch capability and partial failure semantics" {
    const Backend = struct {
        var calls: usize = 0;
        fn batch(_: *anyopaque, _: A, _: stores.UploadScope, _: []const []const u8, token: Cancellation) ![]stores.ArtifactMetadata {
            try token.check();
            calls += 1;
            return error.InjectedBatchFailure;
        }
    };
    var vtable: stores.ArtifactStore.VTable = .{ .deinit = undefined, .put = undefined, .get_alloc = undefined, .get_range_alloc = undefined, .stat = undefined, .delete = undefined, .put_scoped_many = Backend.batch };
    var adapter = Reads.init(.{ .allocator = std.testing.allocator, .ptr = undefined, .vtable = &vtable });
    var handle = adapter.store();
    try std.testing.expect(handle.vtable.put_scoped_many != null);
    const scope = try stores.UploadScope.forPublication(@splat(8), 1, std.testing.io);
    try std.testing.expectError(error.InjectedBatchFailure, handle.putScopedMany(scope, &.{"one"}, .none));
    try std.testing.expectEqual(@as(usize, 1), Backend.calls);
    vtable.put_scoped_many = null;
    var serial = Reads.init(.{ .allocator = std.testing.allocator, .ptr = undefined, .vtable = &vtable });
    try std.testing.expect(serial.store().vtable.put_scoped_many == null);
}

test "external lake optional opening reads preserve scope without promoting canonical loss to recovery" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("optional-canonical-recovery");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var reads = Reads.init(fs.artifactStore());
    defer reads.close();
    var required = reads.store();
    required.upload_scope = try stores.UploadScope.forPublication(@splat(17), 1, std.testing.io);
    required.namespace_identity = @splat(19);
    required.point_read_blocks = true;
    const hint = required.optionalReadView();
    try std.testing.expectEqualDeep(required.upload_scope, hint.upload_scope);
    try std.testing.expectEqualDeep(required.namespace_identity, hint.namespace_identity);
    try std.testing.expect(hint.point_read_blocks);
    const missing = try required.upload_scope.?.artifactId("0000000000000000000000000000000000000000000000000000000000000000");
    try std.testing.expectError(error.FileNotFound, hint.vtable.get_alloc(hint.ptr, a, &missing));
    try std.testing.expect(reads.cause() == null);
    try std.testing.expectError(error.LakeIndexArtifactUnavailable, required.vtable.get_alloc(required.ptr, a, &missing));
    try std.testing.expectEqual(error.FileNotFound, reads.cause().?);
    // A provider/authorization wrapper which has not opted in remains in
    // force. Hint reads may skip recovery classification, never permission.
    const Protected = struct {
        fn denied(_: *anyopaque, _: A, _: []const u8) ![]u8 {
            return error.Forbidden;
        }
    };
    var backend = fs.artifactStore();
    var vtable = backend.vtable.*;
    vtable.get_alloc = Protected.denied;
    backend.vtable = &vtable;
    var protected_reads = Reads.init(backend);
    defer protected_reads.close();
    const protected_hint = protected_reads.store().optionalReadView();
    try std.testing.expectError(error.Forbidden, protected_hint.vtable.get_alloc(protected_hint.ptr, a, &missing));
    try std.testing.expect(protected_reads.cause() == null);
}
