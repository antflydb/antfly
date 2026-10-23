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

//! Query-owned, publication-pinned lazy provider selectors. Independent client
//! and reader ownership outlive short SQL index cursors sharing this source.
const std = @import("std");
const local = @import("antfly_local_sources");
const nav = @import("lake_source_navigation.zig");
const Store = @import("lake_index_store.zig").Store;
const Lease = @import("lake_index_reader_lease.zig").Handle;
const Source = local.serverless_query_lake_serving.ServingSource;
const Context = local.serverless_query_lake_read_context.Context;
const A = std.mem.Allocator;
const Owner = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    store: Store,
    lease: *Lease,
    resolver: nav.Resolver,
    fn read(raw: *anyopaque, a: A, file: local.serverless_external_source_types.FileEntry, context: Context) !Source.FileVersions {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try context.ensureActive();
        return self.resolver.selectors(a, file);
    }
    fn close(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const a = self.a;
        self.resolver.deinit();
        self.store.deinit();
        self.lease.deinit();
        self.arena.deinit();
        a.destroy(self);
    }
};
pub fn install(source: *Source, store: *Store, publication: local.metadata_lake_index_catalog.Publication, root: nav.Root, cached: @import("lake_index_aggregate_artifact.zig").CachedRead, lease: *Lease) !void {
    const a = source.alloc;
    const owner = try a.create(Owner);
    errdefer a.destroy(owner);
    owner.a = a;
    owner.arena = .init(a);
    errdefer owner.arena.deinit();
    owner.lease = try lease.retain();
    errdefer owner.lease.deinit();
    owner.store = try store.reopenReadOnly(a);
    errdefer owner.store.deinit();
    const context = owner.lease.readContext();
    const token: @import("antfly_cancellation").CancellationToken = if (context.cancellation) |value| .{ .ptr = value.ptr, .is_cancelled_fn = value.is_cancelled_fn } else .none;
    owner.store.implementation.cloud_io = context.io;
    try owner.store.bindPublication(publication, token);
    const oa = owner.arena.allocator();
    const owned_root = try std.json.parseFromSliceLeaky(nav.Root, oa, try std.json.Stringify.valueAlloc(oa, root, .{}), .{ .allocate = .alloc_always });
    var reads = cached;
    reads.context = context;
    owner.resolver = .{ .root = owned_root, .store = owner.store.artifactStore(), .cached = reads, .token = token };
    if (source.verified_selector) |prior| prior.close(prior.ptr);
    if (source.verified_inventory_lease) |prior| prior.release();
    source.verified_inventory_lease = null;
    source.verified_files = null;
    source.verified_selector = .{ .ptr = owner, .read = Owner.read, .close = Owner.close };
}
