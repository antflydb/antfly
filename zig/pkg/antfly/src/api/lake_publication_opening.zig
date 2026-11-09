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

const Navigation = struct { domain: [32]u8, ref: tree.Ref, text: bool = false, depth: u8 = 0, small: bool = false };
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
    small: [16]artifacts.ChunkRef = undefined,
    small_count: usize = 0,
    locators: std.ArrayList(catalog.ProviderLocator) = .empty,
    locator_attempts: usize = 0,
    remaining: u64 = read_budget,
    token: Token,

    fn recordLocator(raw: *anyopaque, ref: artifacts.ChunkRef) !void {
        const self: *Producer = @ptrCast(@alignCast(raw));
        if (self.locators.items.len == 128 or self.locator_attempts == 256) return;
        for (self.locators.items) |value| if (std.mem.eql(u8, value.id, ref.artifact_id)) return;
        const object = self.store.providerStore() orelse return;
        const writer = object.cloud_writer orelse return;
        self.locator_attempts += 1;
        const found = writer.lookup(self.a, ref.artifact_id, self.token) catch |err| switch (err) {
            error.CloudGenerationMissing => return,
            else => return err,
        };
        var record = found orelse return;
        defer record.deinit(self.a);
        if (record.bytes != ref.byte_len) return error.InvalidCloudGenerationMap;
        try self.locators.append(self.a, .{ .id = try self.a.dupe(u8, record.id), .bytes = record.bytes, .generation = try self.a.dupe(u8, record.generation), .retention_until_ms = record.retention_until_ms });
    }
    fn capture(raw: *anyopaque, identity: []const u8, digest: [32]u8, bytes: []const u8) !void {
        const self: *Producer = @ptrCast(@alignCast(raw));
        if (bytes.len > self.remaining) return error.OpeningBundleBudgetExceeded;
        self.remaining -= bytes.len;
        // Physical object captures also cover child navigation pages reached
        // through the page store. Synthetic range identities have no map entry.
        if (identity.len == 175) if ((stores.uploadScopeFromArtifactId(identity) catch null) != null) try recordLocator(self, .{ .artifact_id = identity, .checksum = "", .byte_len = bytes.len });
        try self.collector.addIdentity(identity, digest, bytes);
    }
    fn add(self: *Producer, domain: [32]u8, ref: ?tree.Ref, text: bool, depth: u8, small: bool) void {
        const value = ref orelse return;
        if (self.count == self.queue.len or depth > 3) return;
        for (self.queue[0..self.count]) |*prior| if (std.mem.eql(u8, &prior.domain, &domain) and prior.ref.eql(value)) {
            prior.text = prior.text or text;
            prior.small = prior.small or small;
            return;
        };
        self.queue[self.count] = .{ .domain = domain, .ref = value, .text = text, .depth = depth, .small = small };
        self.count += 1;
    }
    fn read(self: *Producer, a: A, ref: artifacts.ChunkRef) !void {
        if (ref.byte_len > packs.max_decoded_bytes or self.collector.full()) return;
        try recordLocator(self, ref);
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
            self.add(root.domain, root.files, false, 0, false);
            self.add(root.domain, root.public, false, 0, false);
            self.add(root.domain, root.private, false, 0, false);
        };
        if (publication.row_identity) |ref| if (ref.byte_len <= packs.max_decoded_bytes) {
            const root = try @import("lake_row_identity.zig").load(a, self.store.*, ref, self.token, self.cached);
            var dependencies = @import("lake_row_identity.zig").dependencies(root);
            while (dependencies.next()) |page| self.add(root.domain, page, false, 0, false);
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
                inline for (.{ root.term_statistics, root.term_directory.branches, root.term_directory.leaves, root.directories.routing, root.directories.ordered, root.directories.files, root.serving_routes, root.segment_files, root.source_routes, root.tie_bounds }) |page| self.add(root.domain, page, false, 0, false);
                self.add(root.domain, root.directories.segments, root.seekable, 0, root.serving_bytes != 0 and root.serving_bytes <= 256 * 1024 and root.segment_count <= 16);
                for (root.open_packs) |pack| try self.read(sa, pack);
            } else if (ref.kind == .ordered_row_index) {
                const root = try @import("lake_index_ordered_rows.zig").loadRootLazy(sa, self.store.*, ref, self.token, self.cached);
                inline for (.{ root.page, root.reverse, root.covering, root.ties, root.predicates }) |page| self.add(root.domain, page, false, 0, false);
                if (root.file_dictionary) |dictionary| inline for (.{ dictionary.slots, dictionary.names, dictionary.public }) |page| self.add(root.domain, page, false, 0, false);
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
        for (self.small[0..self.small_count]) |ref| {
            if (self.collector.full()) break;
            var scratch = std.heap.ArenaAllocator.init(local.sql_memory_budget.reclaimingAllocator(self.a));
            defer scratch.deinit();
            _ = try @import("lake_index_seekable_text.zig").warmSmall(scratch.allocator(), .{ .store = self.store.*, .cache = self.cached, .context = self.cached.context, .cancellation = self.token, .locator = .{ .ptr = self, .record = Producer.recordLocator } }, ref, 512 * 1024);
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
            self.producer.add(self.navigation.domain, ref, self.navigation.text, self.navigation.depth + 1, self.navigation.small);
        }
        pub fn record(self: *Visitor, _: []const u8, value: []const u8) !void {
            if (!self.navigation.text or self.producer.segments == 16 or self.producer.collector.full()) return;
            self.producer.segments += 1;
            const route = try std.json.parseFromSliceLeaky(struct { route: @import("lake_index_native_text.zig").SegmentRouting }, self.a, value, .{ .ignore_unknown_fields = true });
            _ = try @import("lake_index_seekable_text.zig").warmOpening(self.a, .{ .store = self.producer.store.*, .cache = self.producer.cached, .context = self.producer.cached.context, .cancellation = self.producer.token, .locator = .{ .ptr = self.producer, .record = Producer.recordLocator } }, route.route.ref, 1024 * 1024);
            if (self.navigation.small and route.route.bytes <= 256 * 1024 and self.producer.small_count < self.producer.small.len) {
                for (self.producer.small[0..self.producer.small_count]) |prior| if (std.mem.eql(u8, prior.artifact_id, route.route.ref.artifact_id)) return;
                const alloc = self.producer.a;
                self.producer.small[self.producer.small_count] = .{ .artifact_id = try alloc.dupe(u8, route.route.ref.artifact_id), .checksum = try alloc.dupe(u8, route.route.ref.checksum), .byte_len = route.route.ref.byte_len };
                self.producer.small_count += 1;
            }
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
    for (producer.collector.refs.items, result) |pack, *out| {
        out.* = .{ .artifact = .{ .kind = .doc_values, .metadata_version = 1, .artifact_id = pack.artifact.artifact_id, .checksum = pack.artifact.checksum, .byte_len = pack.artifact.byte_len }, .decoded_bytes = @intCast(pack.decoded_bytes) };
        if (store.providerStore()) |object| if (object.cloud_writer) |writer| if (try writer.lookup(sa, pack.artifact.artifact_id, token)) |captured| {
            var record = captured;
            defer record.deinit(sa);
            if (record.bytes != pack.artifact.byte_len) return error.InvalidCloudGenerationMap;
            out.provider_generation = try a.dupe(u8, record.generation);
        };
    }
    if (publication.reader_protocol >= 99 and result.len != 0 and producer.locators.items.len != 0) {
        const locators = try a.alloc(catalog.ProviderLocator, producer.locators.items.len);
        var copied: usize = 0;
        errdefer {
            for (locators[0..copied]) |value| {
                a.free(value.id);
                a.free(value.generation);
            }
            a.free(locators);
        }
        for (producer.locators.items, locators) |value, *out| {
            const id = try a.dupe(u8, value.id);
            errdefer a.free(id);
            out.* = .{ .id = id, .bytes = value.bytes, .generation = try a.dupe(u8, value.generation), .retention_until_ms = value.retention_until_ms };
            copied += 1;
        }
        result[0].provider_locators = locators;
    }
    return result;
}

const Worker = struct {
    backing: A,
    store: *const Store,
    publication: catalog.Publication,
    bundle: ?catalog.OpeningBundle,
    stopped: std.atomic.Value(bool) = .init(false),
    domain: [32]u8,
    cached: artifacts.CachedRead,
    token: Token,
    fn canceled(raw: *const anyopaque) bool {
        const self: *const Worker = @ptrCast(@alignCast(raw));
        if (self.stopped.load(.acquire)) return true;
        return if (self.token.ptr) |ptr| if (self.token.is_cancelled_fn) |callback| callback(ptr) else false else false;
    }
    fn check(raw: *const anyopaque) anyerror!void {
        const self: *const Worker = @ptrCast(@alignCast(raw));
        if (self.stopped.load(.acquire)) return error.Canceled;
        try self.token.check();
    }
    fn run(self: *Worker) anyerror!void {
        const guard: artifacts.ReadCancellation = .{ .parent = .{ .ptr = self, .is_cancelled_fn = canceled, .check_fn = check }, .context = self.cached.context };
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
        // The parent already admitted publication/reader/retention authority.
        // A seed worker always reads and hashes the entire optional bundle;
        // it never uses certificate shortcuts or needs another provider clock.
        try store.bindGenerationMap(self.publication.cloud_generations, token);
        if (store.cloud_binding) |binding| try binding.reader.bindPublicationLocators(self.publication.opening_bundles);
        const bundle = self.bundle orelse {
            // Overlap map-root navigation with direct seed GETs. Otherwise
            // skipping the map here merely moves its cold miss into ranking.
            if (store.cloud_binding) |binding| if (binding.reader.shared != null) if (binding.reader.root) |root| {
                _ = try binding.reader.read(a, root, 0, token);
            };
            return;
        };
        if (bundle.provider_generation) |generation| if (store.cloud_binding) |binding| {
            try binding.reader.bindOpeningHint(.{ .id = bundle.artifact.artifact_id, .bytes = bundle.artifact.byte_len, .generation = generation });
        };
        packs.warmPublication(a, store.artifactStore(), .{ .artifact = .{ .artifact_id = bundle.artifact.artifact_id, .checksum = bundle.artifact.checksum, .byte_len = bundle.artifact.byte_len }, .decoded_bytes = bundle.decoded_bytes }, self.domain, self.cached, token) catch |err| switch (err) {
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
        var root_work: Worker = .{ .backing = backing, .store = self.store, .publication = self.publication, .bundle = null, .domain = self.domain, .cached = self.cached, .token = self.token };
        var root_task: ?local.sql_parallel_scheduler.Task(anyerror!void) = null;
        defer if (root_task) |*active| {
            root_work.stopped.store(true, .release);
            active.cancel(self.cached.context.io.?) catch {};
        };
        const has_hint = for (self.publication.opening_bundles) |bundle| {
            if (bundle.provider_generation != null) break true;
        } else false;
        const has_locators = for (self.publication.opening_bundles) |bundle| {
            if (bundle.provider_locators.len != 0) break true;
        } else false;
        const root_cost = 8 * 1024 * 1024;
        // Current bundles supply authenticated direct locators. Speculating on
        // their entire map adds contention and an unnecessary joined teardown.
        // Missing locators retain the normal authoritative demand fallback.
        if (has_hint and !has_locators and self.publication.cloud_generations != null) if (self.cached.context.io) |io| {
            root_task = local.sql_parallel_scheduler.global().submitIo(io, root_cost, Worker.run, .{&root_work});
        };
        const wave_budget = parallel_budget - @as(usize, if (root_task != null) root_cost else 0);
        var position: usize = 0;
        while (position < self.publication.opening_bundles.len) {
            var work: [max_bundles]Worker = undefined;
            var tasks: [max_bundles]?local.sql_parallel_scheduler.Task(anyerror!void) = @splat(null);
            defer if (self.cached.context.io) |io| for (&tasks, &work) |*task, *worker| if (task.*) |*owned| {
                worker.stopped.store(true, .release);
                owned.cancel(io) catch {};
            };
            var width: usize = 0;
            var bytes: u64 = 0;
            while (position + width < self.publication.opening_bundles.len and width < work.len) {
                const bundle = self.publication.opening_bundles[position + width];
                // Include provider buffers, decoding, member admission and
                // independent client setup rather than counting wire bytes only.
                const cost = 2 * (bundle.artifact.byte_len + bundle.decoded_bytes) + 2 * 1024 * 1024;
                if (width != 0 and bytes + cost > wave_budget) break;
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
        // Root warming is optional: do not extend a ready seed wave waiting
        // for unrelated speculation. Its stop token interrupts provider I/O.
        if (root_task) |*active| {
            root_work.stopped.store(true, .release);
            active.cancel(self.cached.context.io.?) catch {};
            root_task = null;
        }
        try self.cached.context.ensureActive();
        try self.token.check();
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
