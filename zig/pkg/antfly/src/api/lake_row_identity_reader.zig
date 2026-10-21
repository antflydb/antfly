// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! One pinned resolver for native candidate hydration and reverse membership.
//! Authority and cloud clients belong to this query owner, never a shared cache.
const std = @import("std");
const local = @import("antfly_local_sources");
const identity = @import("lake_row_identity.zig");
const navigation = @import("lake_source_navigation.zig");
const Store = @import("lake_index_store.zig").Store;
const Lease = @import("lake_index_reader_lease.zig").Handle;
const Source = local.serverless_query_lake_serving.ServingSource;
const RowRef = local.storage_rowsource_types.RowRef;
const A = std.mem.Allocator;
const Owner = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    store: Store,
    artifacts: @import("../serverless/artifacts/store.zig").ArtifactStore,
    lease: *Lease,
    reader: identity.Reader,
    navigation: navigation.Resolver,
    pages: @import("../serverless/graph_segment/page_store.zig").PageStore,
    cache: @import("../serverless/graph_segment/page_tree.zig").Cache = undefined,
    reads: u64 = 256 * 1024 * 1024,
    writes: u64 = 0,
    cached: @import("lake_index_aggregate_artifact.zig").CachedRead,
    mutex: std.atomic.Mutex = .unlocked,
    identity_version: u8,
    index_identity_version: u8,
    fn lock(self: *Owner) !void {
        while (!self.mutex.tryLock()) {
            try self.cached.context.ensureActive();
            if (self.cached.context.io) |io| try io.sleep(.zero, .awake) else @import("antfly_platform").time.yieldBriefly();
        }
        self.cached.context.ensureActive() catch |err| {
            self.mutex.unlock();
            return err;
        };
    }
    fn fromIndex(raw: *anyopaque, a: A, ref: RowRef) !RowRef {
        const self: *Owner = @ptrCast(@alignCast(raw));
        try self.lock();
        defer self.mutex.unlock();
        const space = self.reader.root.indexSpace();
        if (ref != .external or !std.mem.eql(u8, ref.external.source_id, space.source) or !std.mem.eql(u8, ref.external.snapshot_id, space.snapshot)) return error.ExternalLakeSnapshotMismatch;
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const next = (try self.reader.fromIndex(scratch.allocator(), ref.external.file_id, ref.external.row_group_ordinal, ref.external.row_ordinal)) orelse return error.InvalidLogicalRowIdentity;
        var resolved = try self.reader.reference(a, self.reader.root.current, next);
        resolved.external.identity_version = self.identity_version;
        return resolved;
    }
    fn toIndex(raw: *anyopaque, a: A, ref: RowRef) !RowRef {
        const self: *Owner = @ptrCast(@alignCast(raw));
        try self.lock();
        defer self.mutex.unlock();
        const space = self.reader.root.current;
        if (ref != .external or !std.mem.eql(u8, ref.external.source_id, space.source) or !std.mem.eql(u8, ref.external.snapshot_id, space.snapshot)) return error.ExternalLakeSnapshotMismatch;
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const previous = (try self.reader.toIndex(scratch.allocator(), ref.external.file_id, ref.external.row_group_ordinal, ref.external.row_ordinal)) orelse return error.InvalidLogicalRowIdentity;
        var resolved = try self.reader.reference(a, self.reader.root.indexSpace(), previous);
        resolved.external.identity_version = self.index_identity_version;
        return resolved;
    }
    fn tieRank(raw: *anyopaque, a: A, ref: RowRef) !u64 {
        const self: *Owner = @ptrCast(@alignCast(raw));
        try self.lock();
        defer self.mutex.unlock();
        const space = self.reader.root.indexSpace();
        if (ref != .external or !std.mem.eql(u8, ref.external.source_id, space.source) or !std.mem.eql(u8, ref.external.snapshot_id, space.snapshot)) return error.ExternalLakeSnapshotMismatch;
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const current = (try self.reader.fromIndex(scratch.allocator(), ref.external.file_id, ref.external.row_group_ordinal, ref.external.row_ordinal)) orelse return error.InvalidLogicalRowIdentity;
        return current.tie_rank;
    }
    fn file(raw: *anyopaque, a: A, digest_: []const u8) !?[]const u8 {
        const self: *Owner = @ptrCast(@alignCast(raw));
        return self.navigation.file(a, digest_, false);
    }
    fn digest(raw: *anyopaque, a: A, file_: []const u8) ![]const u8 {
        const self: *Owner = @ptrCast(@alignCast(raw));
        return self.navigation.digest(a, file_);
    }
    fn readPage(raw: *anyopaque, a: A, store: *@import("../serverless/artifacts/store.zig").ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, offset: u64, length: usize, _: [32]u8, token: @import("antfly_cancellation").CancellationToken, budget: *u64) ![]u8 {
        const self: *Owner = @ptrCast(@alignCast(raw));
        try self.cached.context.ensureActive();
        if (offset != 0 or length != ref.byte_len) return error.InvalidLogicalRowIdentity;
        try @import("../serverless/artifacts/store.zig").chargeReadBudget(budget, length);
        return @import("lake_index_aggregate_artifact.zig").readArtifact(a, store.*, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, token, self.cached);
    }
    fn close(raw: *anyopaque) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        const a = self.a;
        self.cache.deinit();
        self.navigation.deinit();
        self.store.deinit();
        self.lease.deinit();
        self.arena.deinit();
        a.destroy(self);
    }
};

pub fn validate(a: A, root: identity.Root, publication: local.metadata_lake_index_catalog.Publication, domain: identity.logical.Digest, source: *Source) !void {
    try root.validate(a);
    if (!std.mem.eql(u8, &root.domain, &domain) or !std.mem.eql(u8, &root.cut, &publication.signature.source) or !std.mem.eql(u8, &root.values_recipe, &publication.signature.desired) or !std.mem.eql(u8, &root.artifacts, &try identity.declarationDigest(a, publication.declarations)) or !std.mem.eql(u8, root.current.source, source.inventory.source_id) or !std.mem.eql(u8, root.current.snapshot, source.inventory.snapshot_id) or root.current.file_count != source.fileCount()) return error.InvalidLogicalRowIdentity;
    const index = root.indexSpace();
    for (publication.declarations) |declaration| {
        if (!std.mem.eql(u8, declaration.binding.source_id, index.source) or !std.mem.eql(u8, declaration.binding.snapshot_id, index.snapshot)) return error.InvalidLogicalRowIdentity;
    }
}

pub fn install(source: *Source, store: *Store, publication: local.metadata_lake_index_catalog.Publication, root: identity.Root, cached: @import("lake_index_aggregate_artifact.zig").CachedRead, lease: *Lease) !void {
    if (root.indexes == null) return;
    const a = source.alloc;
    const owner = try a.create(Owner);
    errdefer a.destroy(owner);
    owner.* = .{ .a = a, .arena = .init(a), .store = undefined, .artifacts = undefined, .lease = undefined, .reader = undefined, .navigation = undefined, .pages = undefined, .cached = cached, .identity_version = source.inventory.row_identity_version, .index_identity_version = 1 };
    errdefer owner.arena.deinit();
    owner.lease = try lease.retain();
    errdefer owner.lease.deinit();
    owner.store = try store.reopenReadOnly(a);
    errdefer owner.store.deinit();
    const context = owner.lease.readContext();
    const token: @import("antfly_cancellation").CancellationToken = if (context.cancellation) |value| .{ .ptr = value.ptr, .is_cancelled_fn = value.is_cancelled_fn } else .none;
    owner.store.implementation.cloud_io = context.io;
    try owner.store.bindPublication(publication, token);
    owner.artifacts = owner.store.artifactStore();
    owner.cached.context = context;
    const oa = owner.arena.allocator();
    const encoded = try std.json.Stringify.valueAlloc(oa, root, .{});
    const owned = try std.json.parseFromSliceLeaky(identity.Root, oa, encoded, .{ .allocate = .alloc_always });
    const index = owned.indexSpace();
    const nav = try navigation.load(oa, owner.artifacts, index.navigation orelse return error.InvalidLogicalRowIdentity, token, owner.cached);
    if (!std.mem.eql(u8, &nav.domain, &root.domain) or !std.mem.eql(u8, nav.source, index.source) or !std.mem.eql(u8, nav.snapshot, index.snapshot) or nav.count != index.file_count) return error.InvalidLogicalRowIdentity;
    owner.navigation = .{ .root = nav, .store = owner.artifacts, .cached = owner.cached, .token = token };
    owner.index_identity_version = nav.public_identity_version;
    owner.pages = .{ .domain = root.domain, .artifacts = &owner.artifacts, .cancellation = token, .remaining_read_bytes = &owner.reads, .remaining_write_bytes = &owner.writes, .read_cache = .{ .ptr = owner, .read = Owner.readPage } };
    owner.cache = .{ .alloc = a, .underlying = owner.pages.store(), .max_bytes = 2 * 1024 * 1024 };
    owner.reader = .{ .root = owned, .pages = &owner.pages, .cache = &owner.cache };
    if (source.row_resolver) |previous| previous.close(previous.ptr);
    source.row_resolver = .{ .ptr = owner, .source = index.source, .snapshot = index.snapshot, .current_source = owned.current.source, .current_snapshot = owned.current.snapshot, .rows = owned.current.rows, .from_index = Owner.fromIndex, .to_index = Owner.toIndex, .file = Owner.file, .digest = Owner.digest, .tie_rank = Owner.tieRank, .close = Owner.close };
}
