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
const Allocator = std.mem.Allocator;
const manifest_types = @import("types.zig");
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;

pub const PublishResult = struct {
    published: bool,
    current_head: ?u64,
};

pub const ManifestStore = struct {
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        deinit: *const fn (Allocator, *anyopaque) void,
        put: *const fn (*anyopaque, manifest_types.Manifest) anyerror!void,
        get_alloc: *const fn (*anyopaque, Allocator, []const u8, u64) anyerror!manifest_types.Manifest,
        set_head: *const fn (*anyopaque, []const u8, u64) anyerror!void,
        get_head: *const fn (*anyopaque, []const u8) anyerror!u64,
        compare_and_swap_head: *const fn (*anyopaque, []const u8, ?u64, u64) anyerror!bool,
        list_versions_alloc: *const fn (*anyopaque, Allocator, []const u8) anyerror![]u64,
        delete_version: *const fn (*anyopaque, []const u8, u64) anyerror!void,
        delete_retired_candidate: ?*const fn (*anyopaque, []const u8, u64, u64) anyerror!bool = null,
        cleanup_retired_temporaries: ?*const fn (*anyopaque, []const u8, u64, CancellationToken) anyerror!void = null,
    };

    pub fn deinit(self: *ManifestStore) void {
        self.vtable.deinit(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn put(self: *ManifestStore, manifest: manifest_types.Manifest) !void {
        try self.vtable.put(self.ptr, manifest);
    }

    pub fn getAlloc(self: *ManifestStore, namespace: []const u8, version: u64) !manifest_types.Manifest {
        return try self.vtable.get_alloc(self.ptr, self.allocator, namespace, version);
    }

    pub fn setHead(self: *ManifestStore, namespace: []const u8, version: u64) !void {
        try self.vtable.set_head(self.ptr, namespace, version);
    }

    pub fn getHead(self: *ManifestStore, namespace: []const u8) !u64 {
        return try self.vtable.get_head(self.ptr, namespace);
    }

    pub fn compareAndSwapHead(self: *ManifestStore, namespace: []const u8, expected: ?u64, version: u64) !bool {
        return try self.vtable.compare_and_swap_head(self.ptr, namespace, expected, version);
    }

    pub fn listVersionsAlloc(self: *ManifestStore, namespace: []const u8) ![]u64 {
        return try self.vtable.list_versions_alloc(self.ptr, self.allocator, namespace);
    }

    pub fn deleteVersion(self: *ManifestStore, namespace: []const u8, version: u64) !void {
        try self.vtable.delete_version(self.ptr, namespace, version);
    }

    /// The caller must have acquired the namespace publication barrier at
    /// `cutoff`. Candidate versions can be reused, so validation and removal
    /// must be atomic against immutable creation, never GET followed by DELETE.
    pub fn deleteRetiredCandidate(self: *ManifestStore, namespace: []const u8, version: u64, cutoff: u64) !bool {
        if (cutoff == 0) return error.InvalidPublicationFence;
        const remove = self.vtable.delete_retired_candidate orelse return error.ConditionalManifestDeletionUnsupported;
        return remove(self.ptr, namespace, version, cutoff);
    }

    /// Filesystem implementations retire staging files only after the namespace
    /// publication barrier has fenced their owning token out of HEAD.
    pub fn cleanupRetiredTemporaries(self: *ManifestStore, namespace: []const u8, cutoff: u64, cancellation: CancellationToken) !void {
        try cancellation.check();
        if (cutoff == 0) return error.InvalidPublicationFence;
        if (self.vtable.cleanup_retired_temporaries) |cleanup| try cleanup(self.ptr, namespace, cutoff, cancellation);
    }

    pub fn publish(self: *ManifestStore, manifest: manifest_types.Manifest, expected_head: ?u64) !PublishResult {
        try self.put(manifest);
        const published = try self.compareAndSwapHead(manifest.namespace, expected_head, manifest.version);
        if (published) {
            return .{
                .published = true,
                .current_head = manifest.version,
            };
        }

        const current_head = self.getHead(manifest.namespace) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        return .{
            .published = false,
            .current_head = current_head,
        };
    }
};

/// Shared regression for two collectors with the same retired snapshot: the
/// second must not delete a replacement occupying the first one's freed slot.
pub fn testRetiredCandidateRecreation(store: *ManifestStore) !void {
    var unsupported_vtable = store.vtable.*;
    unsupported_vtable.delete_retired_candidate = null;
    var unsupported = store.*;
    unsupported.vtable = &unsupported_vtable;
    try std.testing.expectError(error.ConditionalManifestDeletionUnsupported, unsupported.deleteRetiredCandidate("bootstrap", 1, 2));
    for ([_]bool{ false, true }) |has_head| {
        const namespace = if (has_head) "normal" else "bootstrap";
        var candidate: manifest_types.Manifest = .{
            .namespace = @constCast(namespace),
            .version = 1,
            .built_at_ns = 1,
            .wal_start_lsn = 0,
            .wal_end_lsn = 0,
            .stats = .{},
            .artifacts = @constCast(&.{}),
            .publication_fencing_token = 1,
        };
        if (has_head) {
            try store.put(candidate);
            try store.setHead(namespace, 1);
            candidate.version = 2;
        }
        try store.put(candidate);
        try std.testing.expect(try store.deleteRetiredCandidate(namespace, candidate.version, 2));
        try std.testing.expect(!try store.deleteRetiredCandidate(namespace, candidate.version, 2));
        candidate.publication_fencing_token = 3;
        try store.put(candidate);
        try std.testing.expect(!try store.deleteRetiredCandidate(namespace, candidate.version, 2));
        try std.testing.expect(!try store.deleteRetiredCandidate(namespace, candidate.version, 3));
        try std.testing.expect(try store.compareAndSwapHead(namespace, if (has_head) 1 else null, candidate.version));
        var published = try store.getAlloc(namespace, candidate.version);
        defer published.deinit(store.allocator);
        try std.testing.expectEqual(@as(u64, 3), published.publication_fencing_token);
        try std.testing.expectError(error.CannotDeleteHead, store.deleteRetiredCandidate(namespace, candidate.version, 4));
    }
}
