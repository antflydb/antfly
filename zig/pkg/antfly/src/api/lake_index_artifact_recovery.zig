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
pub const Reads = struct {
    backend: stores.ArtifactStore,
    vtable: stores.ArtifactStore.VTable,
    cause_code: std.atomic.Value(u16) = .init(0),
    pub fn cause(self: *const Reads) ?anyerror {
        const code = self.cause_code.load(.acquire);
        return if (code == 0) null else @errorFromInt(code);
    }
    pub fn init(backend: stores.ArtifactStore) Reads {
        const vtable: stores.ArtifactStore.VTable = .{
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
        return self.backend.deleteWithCancellation(id, token);
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
        return self.backend.vtable.put(self.backend.ptr, a, bytes);
    }
    fn put_with_cancellation(raw: *anyopaque, a: A, bytes: []const u8, cancellation: Cancellation) !stores.ArtifactMetadata {
        const self: *Reads = @ptrCast(@alignCast(raw));
        return self.backend.vtable.put_with_cancellation.?(self.backend.ptr, a, bytes, cancellation);
    }
    fn get_alloc(raw: *anyopaque, a: A, id: []const u8) ![]u8 {
        const self: *Reads = @ptrCast(@alignCast(raw));
        return self.backend.vtable.get_alloc(self.backend.ptr, a, id) catch |err| self.failure(err);
    }
    fn get_alloc_with_cancellation(raw: *anyopaque, a: A, id: []const u8, cancellation: Cancellation) ![]u8 {
        const self: *Reads = @ptrCast(@alignCast(raw));
        return self.backend.vtable.get_alloc_with_cancellation.?(self.backend.ptr, a, id, cancellation) catch |err| self.failure(err);
    }
    fn get_bounded_alloc_with_cancellation(raw: *anyopaque, a: A, id: []const u8, len: usize, cancellation: Cancellation) ![]u8 {
        const self: *Reads = @ptrCast(@alignCast(raw));
        return self.backend.vtable.get_bounded_alloc_with_cancellation.?(self.backend.ptr, a, id, len, cancellation) catch |err| self.failure(err);
    }
    fn get_range_alloc(raw: *anyopaque, a: A, id: []const u8, offset: u64, len: usize) ![]u8 {
        const self: *Reads = @ptrCast(@alignCast(raw));
        return self.backend.vtable.get_range_alloc(self.backend.ptr, a, id, offset, len) catch |err| self.failure(err);
    }
    fn get_range_alloc_with_cancellation(raw: *anyopaque, a: A, id: []const u8, offset: u64, len: usize, cancellation: Cancellation) ![]u8 {
        const self: *Reads = @ptrCast(@alignCast(raw));
        return self.backend.vtable.get_range_alloc_with_cancellation.?(self.backend.ptr, a, id, offset, len, cancellation) catch |err| self.failure(err);
    }
    fn get_verified_range_alloc_with_cancellation(raw: *anyopaque, a: A, id: []const u8, bytes: u64, checksum: []const u8, offset: u64, len: usize, cancellation: Cancellation) ![]u8 {
        const self: *Reads = @ptrCast(@alignCast(raw));
        return self.backend.vtable.get_verified_range_alloc_with_cancellation.?(self.backend.ptr, a, id, bytes, checksum, offset, len, cancellation) catch |err| self.failure(err);
    }
    fn get_verified_range_alloc_with_budget(raw: *anyopaque, a: A, id: []const u8, bytes: u64, checksum: []const u8, offset: u64, len: usize, cancellation: Cancellation, remaining: *u64) ![]u8 {
        const self: *Reads = @ptrCast(@alignCast(raw));
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
        return self.backend.vtable.delete(self.backend.ptr, id);
    }
    fn put_scoped(raw: *anyopaque, a: A, scope: stores.UploadScope, bytes: []const u8, cancellation: Cancellation) !stores.ArtifactMetadata {
        const self: *Reads = @ptrCast(@alignCast(raw));
        return self.backend.vtable.put_scoped.?(self.backend.ptr, a, scope, bytes, cancellation);
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
