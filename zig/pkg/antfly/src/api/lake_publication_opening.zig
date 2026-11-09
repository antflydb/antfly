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

//! Publication-scoped, optional immutable opening bundles. All admission still
//! uses current source/credential/reader authority. Bundles only seed the same
//! content-verified byte cache as ordinary demand reads, including after restart.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.metadata_lake_index_catalog;
const stores = @import("../serverless/artifacts/store.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const packs = @import("lake_index_text_opening_pack.zig");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const pages = @import("../serverless/graph_segment/page_store.zig");
const Store = @import("lake_index_store.zig").Store;
const Cache = local.serverless_query_lake_serving_cache.Cache;
const Context = local.serverless_query_lake_read_context.Context;
const Token = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
pub const max_bundles = 4;
pub const max_pages = 64;
const read_budget = 32 * 1024 * 1024;
const decode_budget = 64 * 1024 * 1024;
const parallel_budget = 32 * 1024 * 1024;

const Navigation = struct { domain: [32]u8, ref: tree.Ref, text: bool = false, depth: u8 = 0 };
const Producer = struct {
    a: A,
    store: *stores.ArtifactStore,
    collector: packs.Collector,
    cache: Cache,
    cached: artifacts.CachedRead = undefined,
    queue: [max_pages]Navigation = undefined,
    count: usize = 0,
    position: usize = 0,
    segments: usize = 0,
    remaining: u64 = read_budget,
    token: Token,

    fn capture(raw: *anyopaque, identity: []const u8, digest: [32]u8, bytes: []const u8) !void {
        const self: *Producer = @ptrCast(@alignCast(raw));
        if (bytes.len > self.remaining) return error.OpeningBundleBudgetExceeded;
        self.remaining -= bytes.len;
        try self.collector.addIdentity(identity, digest, bytes);
    }
    fn add(self: *Producer, domain: [32]u8, ref: ?tree.Ref, text: bool, depth: u8) void {
        const value = ref orelse return;
        if (self.count == self.queue.len or depth > 3) return;
        for (self.queue[0..self.count]) |prior| if (std.mem.eql(u8, &prior.domain, &domain) and prior.ref.eql(value)) return;
        self.queue[self.count] = .{ .domain = domain, .ref = value, .text = text, .depth = depth };
        self.count += 1;
    }
    fn read(self: *Producer, a: A, ref: artifacts.ChunkRef) !void {
        if (ref.byte_len > packs.max_decoded_bytes or self.collector.full()) return;
        const bytes = try artifacts.readArtifact(a, self.store.*, ref, self.token, self.cached);
        a.free(bytes);
    }
    fn collect(self: *Producer, publication: catalog.Publication, declarations: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact) !void {
        var arena = std.heap.ArenaAllocator.init(local.sql_memory_budget.reclaimingAllocator(self.a));
        defer arena.deinit();
        const a = arena.allocator();
        try self.read(a, .{ .artifact_id = publication.inventory.artifact_id, .checksum = publication.inventory.checksum, .byte_len = publication.inventory.byte_len });
        if (publication.directory) |ref| try self.read(a, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len });
        if (publication.source_navigation) |ref| if (ref.byte_len <= packs.max_decoded_bytes) {
            const root = try @import("lake_source_navigation.zig").load(a, self.store.*, ref, self.token, self.cached);
            self.add(root.domain, root.files, false, 0);
            self.add(root.domain, root.public, false, 0);
            self.add(root.domain, root.private, false, 0);
        };
        if (publication.row_identity) |ref| if (ref.byte_len <= packs.max_decoded_bytes) {
            const root = try @import("lake_row_identity.zig").load(a, self.store.*, ref, self.token, self.cached);
            var dependencies = @import("lake_row_identity.zig").dependencies(root);
            while (dependencies.next()) |page| self.add(root.domain, page, false, 0);
        };
        for (declarations, 0..) |declaration, i| {
            if (i == max_pages or self.collector.full()) break;
            try self.token.check();
            try self.cached.context.ensureActive();
            var scratch = std.heap.ArenaAllocator.init(local.sql_memory_budget.reclaimingAllocator(self.a));
            defer scratch.deinit();
            const sa = scratch.allocator();
            const ref = declaration.artifact;
            if (ref.byte_len > packs.max_decoded_bytes) continue;
            if (ref.kind == .text_segment and ref.metadata_version == @import("lake_index_native_text.zig").metadata_version) {
                const root = (try @import("lake_index_native_text.zig").loadDirectory(sa, self.store.*, ref, self.token, self.cached)).root;
                inline for (.{ root.term_statistics, root.term_directory.branches, root.term_directory.leaves, root.directories.routing, root.directories.ordered, root.directories.files, root.serving_routes, root.segment_files, root.source_routes, root.tie_bounds }) |page| self.add(root.domain, page, false, 0);
                self.add(root.domain, root.directories.segments, root.seekable, 0);
                for (root.open_packs) |pack| try self.read(sa, pack);
            } else if (ref.kind == .ordered_row_index) {
                const root = try @import("lake_index_ordered_rows.zig").loadRootLazy(sa, self.store.*, ref, self.token, self.cached);
                inline for (.{ root.page, root.reverse, root.covering, root.ties, root.predicates }) |page| self.add(root.domain, page, false, 0);
                if (root.file_dictionary) |dictionary| inline for (.{ dictionary.slots, dictionary.names, dictionary.public }) |page| self.add(root.domain, page, false, 0);
            }
        }
        while (self.position < self.count and !self.collector.full()) : (self.position += 1) {
            var scratch = std.heap.ArenaAllocator.init(local.sql_memory_budget.reclaimingAllocator(self.a));
            defer scratch.deinit();
            const sa = scratch.allocator();
            const selected = self.queue[self.position];
            var remaining: u64 = tree.max_page_bytes;
            var writes: u64 = 0;
            var store: pages.PageStore = .{ .domain = selected.domain, .artifacts = self.store, .cancellation = self.token, .remaining_read_bytes = &remaining, .remaining_write_bytes = &writes, .read_cache = @import("lake_index_navigation_pages.zig").cacheBinding(&self.cached) };
            var visitor = Visitor{ .producer = self, .navigation = selected, .a = sa };
            try tree.walkPage(sa, store.store(), selected.ref, &visitor);
        }
    }
    const Visitor = struct {
        producer: *Producer,
        navigation: Navigation,
        a: A,
        children: usize = 0,
        pub fn child(self: *Visitor, ref: tree.Ref) !void {
            if (self.children == 8) return;
            self.children += 1;
            self.producer.add(self.navigation.domain, ref, self.navigation.text, self.navigation.depth + 1);
        }
        pub fn record(self: *Visitor, _: []const u8, value: []const u8) !void {
            if (!self.navigation.text or self.producer.segments == 16 or self.producer.collector.full()) return;
            self.producer.segments += 1;
            const route = try std.json.parseFromSliceLeaky(struct { route: @import("lake_index_native_text.zig").SegmentRouting }, self.a, value, .{ .ignore_unknown_fields = true });
            _ = try @import("lake_index_seekable_text.zig").warmOpening(self.a, .{ .store = self.producer.store.*, .cache = self.producer.cached, .context = self.producer.cached.context, .cancellation = self.producer.token }, route.route.ref, 1024 * 1024);
        }
    };
};

/// Every original artifact stays independently rooted. The new bundles contain
/// copies, so legacy/missing/corrupt cache seeds never weaken serving authority.
pub fn publish(a: A, store: *stores.ArtifactStore, publication: catalog.Publication, declarations: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact, context: Context, token: Token) ![]const catalog.OpeningBundle {
    if (publication.reader_protocol < 98) return &.{};
    var arena = std.heap.ArenaAllocator.init(local.sql_memory_budget.reclaimingAllocator(a));
    defer arena.deinit();
    const sa = arena.allocator();
    var producer: Producer = .{ .a = sa, .store = store, .collector = .{ .a = sa, .out = a, .store = store, .cancellation = token, .version = 2, .max_packs = max_bundles }, .cache = Cache.initWithMemoryLimit(sa, 0), .token = token };
    defer producer.collector.deinit();
    defer producer.cache.deinit();
    producer.cached = .{ .cache = &producer.cache, .scope = publication.signature.store, .context = context, .capture = .{ .ptr = &producer, .record = Producer.capture } };
    producer.collect(publication, declarations) catch |err| switch (err) {
        error.OpeningBundleBudgetExceeded => {},
        else => return err,
    };
    try producer.collector.flush();
    const result = try a.alloc(catalog.OpeningBundle, producer.collector.refs.items.len);
    for (producer.collector.refs.items, result) |pack, *out| out.* = .{ .artifact = .{ .kind = .doc_values, .metadata_version = 1, .artifact_id = pack.artifact.artifact_id, .checksum = pack.artifact.checksum, .byte_len = pack.artifact.byte_len }, .decoded_bytes = @intCast(pack.decoded_bytes) };
    return result;
}

const Worker = struct {
    backing: A,
    store: *const Store,
    publication: catalog.Publication,
    bundle: catalog.OpeningBundle,
    domain: [32]u8,
    cached: artifacts.CachedRead,
    token: Token,
    fn run(self: *Worker) anyerror!void {
        const guard: artifacts.ReadCancellation = .{ .parent = self.token, .context = self.cached.context };
        const token = guard.token();
        try guard.check();
        var arena = std.heap.ArenaAllocator.init(self.backing);
        defer arena.deinit();
        const a = arena.allocator();
        // Clients and cloud-generation readers have independent ownership;
        // no request-arena allocator or mutable provider map crosses workers.
        var store = try self.store.reopenReadOnly(a);
        defer store.deinit();
        store.implementation.cloud_io = self.cached.context.io;
        try store.bindPublication(self.publication, token);
        packs.warmPublication(a, store.artifactStore(), .{ .artifact = .{ .artifact_id = self.bundle.artifact.artifact_id, .checksum = self.bundle.artifact.checksum, .byte_len = self.bundle.artifact.byte_len }, .decoded_bytes = self.bundle.decoded_bytes }, self.domain, self.cached, token) catch |err| switch (err) {
            // Immutable optional cache material cannot be repaired in place.
            // Remember its absence for this publication; demand remains strict.
            error.FileNotFound, error.NotFound, error.ObjectNotFound, error.ArtifactIntegrityMismatch, error.InvalidNativeLakeTextOpeningPack => return,
            else => return err,
        };
    }
};
const Loader = struct {
    store: *const Store,
    publication: catalog.Publication,
    cached: artifacts.CachedRead,
    token: Token,
    domain: [32]u8,
    fn load(raw: *anyopaque, item: *local.serverless_query_lake_decoded_cache.Item) !void {
        const self: *Loader = @ptrCast(@alignCast(raw));
        const backing = local.sql_memory_budget.reclaimingAllocator(item.arena.allocator());
        var position: usize = 0;
        while (position < self.publication.opening_bundles.len) {
            var work: [max_bundles]Worker = undefined;
            var tasks: [max_bundles]?local.sql_parallel_scheduler.Task(anyerror!void) = @splat(null);
            defer if (self.cached.context.io) |io| for (&tasks) |*task| if (task.*) |*owned| owned.cancel(io) catch {};
            var width: usize = 0;
            var bytes: u64 = 0;
            while (position + width < self.publication.opening_bundles.len and width < work.len) {
                const bundle = self.publication.opening_bundles[position + width];
                // Include provider buffers, decoding, member admission and
                // independent client setup rather than counting wire bytes only.
                const cost = 2 * (bundle.artifact.byte_len + bundle.decoded_bytes) + 2 * 1024 * 1024;
                if (width != 0 and bytes + cost > parallel_budget) break;
                bytes += cost;
                work[width] = .{ .backing = backing, .store = self.store, .publication = self.publication, .bundle = bundle, .domain = self.domain, .cached = self.cached, .token = self.token };
                if (self.cached.context.io) |io| {
                    tasks[width] = local.sql_parallel_scheduler.global().submitIo(io, @intCast(cost), Worker.run, .{&work[width]}) orelse return error.NavigationWarmupInProgress;
                } else try work[width].run();
                width += 1;
            }
            for (tasks[0..width]) |*task| if (task.*) |*owned| {
                const result = owned.await(self.cached.context.io.?);
                task.* = null;
                try result;
            };
            position += width;
        }
        const marker = try item.arena.allocator().create(u8);
        marker.* = 1;
        item.payload = .{ .extension = marker };
    }
};

/// Singleflight covers fetch, validation and admission, not just the pack GET.
/// Only a data-only marker survives; no authority, client or request is cached.
pub fn warm(store: *const Store, publication: catalog.Publication, cached: artifacts.CachedRead, token: Token) !void {
    if (publication.opening_bundles.len == 0) return;
    const started = @import("antfly_platform").time.monotonicNs();
    defer if (cached.context.trace) |trace| trace.add("opening_nanoseconds", @import("antfly_platform").time.monotonicNs() -| started);
    try publication.validate();
    try cached.context.ensureActive();
    const scope = (try stores.uploadScopeFromArtifactId(publication.inventory.artifact_id)) orelse return error.InvalidLakeIndexCatalog;
    const key = @import("lake_wal_image.zig").digest(.{ .kind = "publication-opening-admission-v1", .store = store.identity, .scope = cached.scope, .namespace = publication.namespace, .generation = publication.generation, .bundles = publication.opening_bundles });
    var loader: Loader = .{ .store = store, .publication = publication, .cached = cached, .token = token, .domain = scope.domain };
    const held = cached.cache.decoded.acquire(key, decode_budget, cached.context, .{ .ptr = &loader, .load = Loader.load }) catch |err| {
        try cached.context.ensureActive();
        try token.check();
        if (err == error.OutOfMemory or err == error.Canceled or err == error.DeadlineExceeded) return err;
        // Optional acceleration must not suppress authoritative source errors
        // or make a valid publication unavailable under cache/worker pressure.
        return;
    };
    held.release();
}
