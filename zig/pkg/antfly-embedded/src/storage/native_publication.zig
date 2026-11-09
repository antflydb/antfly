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

//! Borrowed, synchronous participation in an externally durable index WAL
//! publication. The host owns admission, rollback, durability and uncertainty.
//! A participant may stage operations, but cannot commit or activate memory.
const std = @import("std");
const abi = @import("../runtime_callback_abi.zig");

/// A private capability for final primary batches sharing one physical store.
/// Each ordinal arrives once. A caller must cancel on any preparation failure,
/// join all producers, and only then destroy the group. Auxiliary transactions
/// must never receive this capability.
pub const AtomicMember = struct {
    owner: *anyopaque,
    store: *anyopaque,
    ordinal: usize,
};

pub const AtomicGroup = struct {
    ptr: *anyopaque,
    store: *anyopaque,
    count: usize,
    cancel_fn: *const fn (*anyopaque) void,
    destroy_fn: *const fn (*anyopaque) void,
    /// Read only after all producers have joined. Captured under the physical
    /// store mutex, before rollback or another publication can erase it.
    replay_refusal_fn: ?*const fn (*anyopaque) ?@import("lite/replay_admission.zig").WaitHint = null,

    pub fn replayRefusal(self: AtomicGroup) ?@import("lite/replay_admission.zig").WaitHint {
        return if (self.replay_refusal_fn) |read| read(self.ptr) else null;
    }

    pub fn member(self: AtomicGroup, ordinal: usize) AtomicMember {
        std.debug.assert(ordinal < self.count);
        return .{ .owner = self.ptr, .store = self.store, .ordinal = ordinal };
    }
    pub fn cancel(self: AtomicGroup) void {
        self.cancel_fn(self.ptr);
    }
    pub fn deinit(self: *AtomicGroup) void {
        self.destroy_fn(self.ptr);
        self.* = undefined;
    }
};

/// All scoped publication paths use the same component and root validation.
pub fn requireDestination(prefix: []const u8, path: []const u8) !void {
    if (path.len == 0 or std.mem.indexOfScalar(u8, path, 0) != null) return error.InvalidNativeIndexPath;
    if (prefix.len != 0 and (!std.mem.startsWith(u8, path, prefix) or (path.len != prefix.len and path[prefix.len] != '/'))) return error.InvalidNativeIndexPath;
    if (path.len == prefix.len) return;
    const start = if (prefix.len == 0) @as(usize, @intFromBool(path[0] == '/')) else prefix.len + 1;
    var parts = std.mem.splitScalar(u8, path[start..], '/');
    while (parts.next()) |part| if (part.len == 0 or std.mem.eql(u8, part, ".") or std.mem.eql(u8, part, "..")) return error.InvalidNativeIndexPath;
}

pub const Scope = struct {
    inode: u64,
    publication_store: ?*anyopaque = null,
    file_namespace: [32]u8 = @splat(0),
    prefix: []u8,
    pub fn deinit(self: *Scope, alloc: std.mem.Allocator) void {
        alloc.free(self.prefix);
        self.* = undefined;
    }
};

/// Pointer-free identity of a fully loaded native index snapshot. Physical
/// cleanup preserves the semantic generation. The host still independently
/// validates the full namespace path, inode and file namespace at publication.
pub const NamespaceIdentity = struct {
    inode: u64,
    file_namespace: [32]u8,
    root: [32]u8,
    generation: ?[16]u8,
    pub fn eql(self: NamespaceIdentity, other: NamespaceIdentity) bool {
        return std.meta.eql(self, other);
    }

    /// A native vacuum replaces the inode while preserving each unchanged
    /// namespace's authenticated semantic generation. Retained segment sources
    /// still own their original extents. This permits rebasing a *loaded*
    /// snapshot; a prepared publication must retain and validate its exact inode.
    /// An absent generation cannot establish unchanged namespace contents.
    pub fn sameRetainedState(self: NamespaceIdentity, other: NamespaceIdentity) bool {
        if (self.generation == null or other.generation == null) return false;
        var rebased = self;
        rebased.inode = other.inode;
        return rebased.eql(other);
    }
};

/// The finite metadata inputs needed after releasing a broad native snapshot.
pub const PreparationRead = struct {
    namespace: @import("backend_types.zig").Namespace,
    key: []const u8,
};

/// Owned by a prepared namespace transaction until publication completes.
/// All keys and values are prepared before writer admission; external extent
/// references are forbidden here and use the artifact capability instead.
pub const NamespacePlan = struct {
    inode: u64,
    file_namespace: [32]u8,
    prefix: []const u8,
    generation_key: []const u8,
    expected_generation: ?[16]u8,
    mutations: []const @import("native_catalog_mutation.zig").CatalogMutation,
};

/// Consumes the originating transaction. No snapshots or runtime mutexes remain
/// held. The owner must retain this handle until its participant has returned.
pub const Prepared = struct {
    before: ?NamespaceIdentity = null,
    after: ?NamespaceIdentity = null,
    participant: Participant,
    release_fn: *const fn (*anyopaque) void,
    pub fn deinit(self: *Prepared) void {
        self.release_fn(self.participant.ptr);
        self.* = undefined;
    }
};

pub const View = struct {
    ptr: *anyopaque,
    link_fn: *const fn (*anyopaque, u64, []const u8, []const u8, usize) anyerror!void,

    put_fn: *const fn (*anyopaque, u64, []const u8, []const u8) anyerror!void,
    get_metadata_fn: *const fn (*anyopaque, std.mem.Allocator, Scope, []const u8) anyerror![]u8,
    put_metadata_fn: *const fn (*anyopaque, Scope, []const u8, []const u8) anyerror!void,
    namespace_fn: ?*const fn (*anyopaque, NamespacePlan) anyerror!void = null,
    require_absent_fn: ?*const fn (*anyopaque, u64, []const u8) anyerror!void = null,
    dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    const Boundary = abi.Boundary(struct {
        namespace: *const fn (*anyopaque, NamespacePlan) anyerror!void,
        require_absent: *const fn (*anyopaque, u64, []const u8) anyerror!void,
        link: *const fn (*anyopaque, u64, []const u8, []const u8, usize) anyerror!void,
        put: *const fn (*anyopaque, u64, []const u8, []const u8) anyerror!void,
        get_metadata: *const fn (*anyopaque, std.mem.Allocator, Scope, []const u8) anyerror![]u8,
        put_metadata: *const fn (*anyopaque, Scope, []const u8, []const u8) anyerror!void,
    });

    pub fn requireAbsent(self: View, inode: u64, path: []const u8) !void {
        const operation = self.require_absent_fn orelse return error.Unsupported;
        return Boundary.call("require_absent", self.dispatch, operation, .{ self.ptr, inode, path });
    }

    pub fn publishNamespace(self: View, plan: NamespacePlan) !void {
        const operation = self.namespace_fn orelse return error.Unsupported;
        return Boundary.call("namespace", self.dispatch, operation, .{ self.ptr, plan });
    }

    pub fn getMetadata(self: View, alloc: std.mem.Allocator, scope: Scope, key: []const u8) ![]u8 {
        return Boundary.call("get_metadata", self.dispatch, self.get_metadata_fn, .{ self.ptr, alloc, scope, key });
    }
    pub fn putMetadata(self: View, scope: Scope, key: []const u8, value: []const u8) !void {
        return Boundary.call("put_metadata", self.dispatch, self.put_metadata_fn, .{ self.ptr, scope, key, value });
    }

    pub fn put(self: View, inode: u64, path: []const u8, bytes: []const u8) !void {
        return Boundary.call("put", self.dispatch, self.put_fn, .{ self.ptr, inode, path, bytes });
    }

    pub fn link(self: View, inode: u64, construction: []const u8, path: []const u8, length: usize) !void {
        return Boundary.call("link", self.dispatch, self.link_fn, .{ self.ptr, inode, construction, path, length });
    }
};

pub const Participant = struct {
    ptr: *anyopaque,
    stage: *const fn (*anyopaque, View) anyerror!void,
    validate_fn: ?*const fn (*anyopaque, View) anyerror!void = null,
    dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    const Boundary = abi.Boundary(struct {
        stage: *const fn (*anyopaque, View) anyerror!void,
        validate: *const fn (*anyopaque, View) anyerror!void,
    });
    /// Commit hosts invoke this before applying primary postimages. Stage then
    /// sees those postimages in the same native transaction. Readset witnesses
    /// therefore never conflict with their own primary mutations.
    pub fn validate(self: Participant, view: View) !void {
        if (self.validate_fn) |operation| try Boundary.call("validate", self.dispatch, operation, .{ self.ptr, view });
    }
    pub fn apply(self: Participant, view: View) !void {
        return Boundary.call("stage", self.dispatch, self.stage, .{ self.ptr, view });
    }
};

/// Synchronous owner of a primary publication. The index supplies its fully
/// prepared participant; the caller stages it with primary effects and commits
/// once. A failed commit must preserve the host's uncertainty classification.
pub const Committer = struct {
    ptr: *anyopaque,
    commit_fn: *const fn (*anyopaque, Participant) anyerror!void,
    dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    const Boundary = abi.Boundary(struct { commit: *const fn (*anyopaque, Participant) anyerror!void });
    pub fn commit(self: Committer, participant: Participant) !void {
        return Boundary.call("commit", self.dispatch, self.commit_fn, .{ self.ptr, participant });
    }
};

/// Restricted transaction adapter for projection progress and revocation rules.
/// Returned slices remain owned until this synchronous participant returns.
pub const MetadataTxn = struct {
    alloc: std.mem.Allocator,
    view: View,
    scope: Scope,
    reads: std.ArrayListUnmanaged([]u8) = .empty,
    pub fn deinit(self: *MetadataTxn) void {
        for (self.reads.items) |raw| self.alloc.free(raw);
        self.reads.deinit(self.alloc);
    }
    pub fn get(self: *MetadataTxn, key: []const u8) ![]const u8 {
        const raw = try self.view.getMetadata(self.alloc, self.scope, key);
        errdefer self.alloc.free(raw);
        try self.reads.append(self.alloc, raw);
        return raw;
    }
    pub fn put(self: *MetadataTxn, key: []const u8, value: []const u8) !void {
        try self.view.putMetadata(self.scope, key, value);
    }
};

test "loaded native namespace rebase preserves semantic and file namespace fences" {
    const identity: NamespaceIdentity = .{ .inode = 1, .file_namespace = @splat(3), .root = @splat(7), .generation = @splat(11) };
    var replacement = identity;
    replacement.inode = 2;
    try std.testing.expect(!identity.eql(replacement));
    try std.testing.expect(identity.sameRetainedState(replacement));
    replacement.generation = @splat(12);
    try std.testing.expect(!identity.sameRetainedState(replacement));
    replacement = identity;
    replacement.file_namespace = @splat(4);
    try std.testing.expect(!identity.sameRetainedState(replacement));
    replacement = identity;
    replacement.root = @splat(8);
    try std.testing.expect(!identity.sameRetainedState(replacement));
    replacement = identity;
    replacement.generation = null;
    try std.testing.expect(!replacement.sameRetainedState(replacement));
}
