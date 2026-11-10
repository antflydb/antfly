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

//! Bounded startup warming. Authenticated immutable navigation and search blocks
//! enter process-accounted caches; reader capabilities remain query-owned.
const std = @import("std");
const local = @import("antfly_local_sources");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const stores = @import("../serverless/artifacts/store.zig");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const page_store = @import("../serverless/graph_segment/page_store.zig");
const A = std.mem.Allocator;
const Token = @import("antfly_cancellation").CancellationToken;
pub const max_pages = 64;
const page_workspace = 4 * 1024 * 1024;
pub const Navigation = struct { domain: [32]u8, ref: tree.Ref, text_segments: bool = false, depth: u8 = 0 };
const Progress = struct {
    key: [32]u8 = @splat(0),
    pending: [max_pages]Navigation = undefined,
    count: usize = 0,
    position: usize = 0,
    working_set: WorkingSet = .{},
};
pub const Stats = struct { attempts: u64 = 0, failures: u64 = 0, pages: u64 = 0, nanoseconds: u64 = 0 };
pub const Cache = struct {
    attempts: std.atomic.Value(u64) = .init(0),
    failures: std.atomic.Value(u64) = .init(0),
    pages: std.atomic.Value(u64) = .init(0),
    nanoseconds: std.atomic.Value(u64) = .init(0),
    recovery_active: [4]?[32]u8 = @splat(null),
    recovery_coalesced: std.atomic.Value(u64) = .init(0),
    active: [4]?[32]u8 = @splat(null),
    // Retry history belongs to publications, independently of reusable lanes.
    failed: [256]?struct { key: [32]u8, retry_ns: u64 } = @splat(null),
    failed_next: usize = 0,
    mutex: std.atomic.Mutex = .unlocked,
    keys: [256]?[32]u8 = @splat(null),
    next: usize = 0,
    plans: [4]Progress = @splat(.{}),
    plan_next: usize = 0,
    residency: [256]u64 = @splat(0),
    checked_ns: [256]u64 = @splat(0),
    pub const Recovery = struct {
        owner: *Cache,
        slot: usize,
        pub fn deinit(self: Recovery) void {
            @import("antfly_platform").sync.lockYielding(&self.owner.mutex);
            defer self.owner.mutex.unlock();
            self.owner.recovery_active[self.slot] = null;
        }
    };
    pub fn claimRecovery(self: *Cache, key: [32]u8, io: std.Io, context: local.api_operation.RequestContext) !Recovery {
        var waited = false;
        while (true) {
            try context.ensureActive();
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            var free: ?usize = null;
            var duplicate = false;
            for (self.recovery_active, 0..) |active, i| {
                if (active) |value| {
                    if (std.mem.eql(u8, &value, &key)) duplicate = true;
                } else if (free == null) free = i;
            }
            if (!duplicate) if (free) |slot| {
                self.recovery_active[slot] = key;
                self.mutex.unlock();
                return .{ .owner = self, .slot = slot };
            };
            self.mutex.unlock();
            if (!waited) {
                _ = self.recovery_coalesced.fetchAdd(1, .monotonic);
                waited = true;
            }
            try io.sleep(.fromMilliseconds(5), .awake);
        }
    }
    pub fn snapshot(self: *const Cache) Stats {
        return .{ .attempts = self.attempts.load(.monotonic), .failures = self.failures.load(.monotonic), .pages = self.pages.load(.monotonic), .nanoseconds = self.nanoseconds.load(.monotonic) };
    }
    fn begin(self: *Cache, key: [32]u8) !?usize {
        return self.beginResident(key, @import("antfly_platform").time.monotonicNs(), 0);
    }
    fn beginAt(self: *Cache, key: [32]u8, now_ns: u64) !?usize {
        return self.beginResident(key, now_ns, 0);
    }
    fn beginResident(self: *Cache, key: [32]u8, now_ns: u64, epoch: u64) !?usize {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        for (self.keys, 0..) |item, i| if (item) |old| if (std.mem.eql(u8, &old, &key)) {
            if (self.residency[i] == epoch or now_ns -| self.checked_ns[i] < 5 * std.time.ns_per_s) return null;
        };
        for (self.failed) |item| if (item) |failure| if (std.mem.eql(u8, &failure.key, &key) and now_ns < failure.retry_ns) return null;
        for (self.active) |item| if (item) |old| if (std.mem.eql(u8, &old, &key)) return error.NavigationWarmupInProgress;
        // Prefer a retained continuation, then rotate idle lanes so another
        // publication cannot repeatedly overwrite the only resumable plan.
        // Active plans are mutated only by their admitted owner. Taking a
        // by-value snapshot would race with those writes despite this mutex.
        for (&self.plans, 0..) |*plan, index| if (self.active[index] == null and std.mem.eql(u8, &plan.key, &key)) {
            self.active[index] = key;
            return index;
        };
        for (0..self.active.len) |offset| {
            const index = (self.plan_next + offset) % self.active.len;
            if (self.active[index] == null) {
                self.active[index] = key;
                self.plan_next = (index + 1) % self.active.len;
                return index;
            }
        }
        return error.NavigationWarmupInProgress;
    }
    fn finish(self: *Cache, slot: usize, key: [32]u8, completed: bool) void {
        self.finishAt(slot, key, completed, @import("antfly_platform").time.monotonicNs());
    }
    fn finishAt(self: *Cache, slot: usize, key: [32]u8, completed: bool, now_ns: u64) void {
        return self.finishResident(slot, key, completed, now_ns, 0);
    }
    fn finishResident(self: *Cache, slot: usize, key: [32]u8, completed: bool, now_ns: u64, epoch: u64) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        self.active[slot] = null;
        if (!completed) {
            const retry = now_ns +| 5 * std.time.ns_per_s;
            for (&self.failed) |*item| if (item.*) |*failure| {
                if (std.mem.eql(u8, &failure.key, &key)) {
                    failure.retry_ns = retry;
                    return;
                }
            };
            self.failed[self.failed_next] = .{ .key = key, .retry_ns = retry };
            self.failed_next = (self.failed_next + 1) % self.failed.len;
            return;
        }
        for (&self.failed) |*item| if (item.*) |failure| {
            if (std.mem.eql(u8, &failure.key, &key)) item.* = null;
        };
        for (self.keys, 0..) |item, i| if (item) |old| if (std.mem.eql(u8, &old, &key)) {
            self.residency[i] = epoch;
            self.checked_ns[i] = now_ns;
            return;
        };
        self.keys[self.next] = key;
        self.residency[self.next] = epoch;
        self.checked_ns[self.next] = now_ns;
        self.next = (self.next + 1) % self.keys.len;
    }

    pub fn contains(self: *Cache, key: [32]u8) bool {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        for (self.keys) |item| if (item) |old| if (std.mem.eql(u8, &old, &key)) return true;
        return false;
    }
    pub fn remember(self: *Cache, key: [32]u8) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        self.keys[self.next] = key;
        self.next = (self.next + 1) % self.keys.len;
    }
};
const WorkingSet = struct {
    remaining_bytes: std.atomic.Value(usize) = .init(16 * 1024 * 1024),
    remaining_segments: std.atomic.Value(usize) = .init(16),
    fn reserve(self: *WorkingSet) bool {
        var count = self.remaining_segments.load(.acquire);
        while (count != 0) {
            count = self.remaining_segments.cmpxchgWeak(count, count - 1, .acq_rel, .acquire) orelse break;
        }
        if (count == 0) return false;
        const bytes: usize = 1024 * 1024;
        var remaining = self.remaining_bytes.load(.acquire);
        while (remaining >= bytes) {
            remaining = self.remaining_bytes.cmpxchgWeak(remaining, remaining - bytes, .acq_rel, .acquire) orelse return true;
        }
        return false;
    }
};
const PageWarm = struct {
    navigation: Navigation,
    store: stores.ArtifactStore,
    cached: artifacts.CachedRead,
    token: Token,
    children: [8]?Navigation = @splat(null),
    count: usize = 0,
    working_set: *WorkingSet,
    scratch: A = undefined,
    pub fn child(self: *PageWarm, ref: tree.Ref) !void {
        // Warm at most three additional levels. A large fan-out cannot
        // turn a startup hint into a scan of the archive or all leaf pages.
        if (self.navigation.depth == 3 or self.count == self.children.len) return;
        self.children[self.count] = .{ .domain = self.navigation.domain, .ref = ref, .text_segments = self.navigation.text_segments, .depth = self.navigation.depth + 1 };
        self.count += 1;
    }
    pub fn record(self: *PageWarm, _: []const u8, value: []const u8) !void {
        if (!self.navigation.text_segments or !self.working_set.reserve()) return;
        var arena = std.heap.ArenaAllocator.init(self.scratch);
        defer arena.deinit();
        const a = arena.allocator();
        const descriptor = try std.json.parseFromSliceLeaky(struct { route: @import("lake_index_native_text.zig").SegmentRouting }, a, value, .{ .ignore_unknown_fields = true });
        _ = try @import("lake_index_seekable_text.zig").warmBounded(a, .{ .store = self.store, .cache = self.cached, .context = self.cached.context, .cancellation = self.token }, descriptor.route.ref, 1024 * 1024);
    }

    fn run(self: *PageWarm) anyerror!void {
        try self.cached.context.ensureActive();
        var memory: local.sql_memory_budget = .{ .backing = std.heap.page_allocator, .limit = page_workspace };
        self.scratch = memory.allocator();
        var reads: u64 = tree.max_page_bytes;
        var writes: u64 = 0;
        var pages: page_store.PageStore = .{ .domain = self.navigation.domain, .artifacts = &self.store, .cancellation = self.token, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .read_cache = @import("lake_index_navigation_pages.zig").cacheBinding(&self.cached) };
        try tree.walkPage(memory.allocator(), pages.store(), self.navigation.ref, self);
        try self.cached.context.ensureActive();
    }
};
const Guard = struct {
    authority: @import("lake_index_reader_lease.zig").Authority,
    table: u64,
    generation: u64,
    epoch: ?u64,
    fn check(self: Guard) !void {
        var arena = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.processAllocator(std.heap.page_allocator));
        defer arena.deinit();
        const state = try self.authority.readState(arena.allocator(), self.table);
        if (state.dropped or state.current != self.generation) return error.Canceled;
        if (self.epoch) |epoch| if (state.source_owner == null or state.source_owner.?.epoch != epoch) return error.Canceled;
    }
};
/// Four admitted I/O lanes, at most 64 authenticated pages and three child
/// levels, plus 16 segments/16 MiB of immutable working blocks. Every worker joins before its operation-local reader lease ends.
pub fn warmRoots(store: stores.ArtifactStore, cached: artifacts.CachedRead, roots: []const Navigation, token: Token) !usize {
    var progress: Progress = .{};
    return warmRootsGuarded(store, cached, roots, token, null, &progress, null);
}
fn warmRootsGuarded(store: stores.ArtifactStore, cached: artifacts.CachedRead, roots: []const Navigation, token: Token, guard: ?Guard, progress: *Progress, completed_pages: ?*std.atomic.Value(u64)) !usize {
    if (progress.count == 0 or progress.position >= progress.count) {
        // The traversal and its working-block quota form one resumable sweep.
        // A retry cannot turn each completed wave into another 16 MiB warmup.
        progress.working_set = .{};
        progress.count = @min(roots.len, progress.pending.len);
        progress.position = 0;
        @memcpy(progress.pending[0..progress.count], roots[0..progress.count]);
    }
    const pending = &progress.pending;
    var count = progress.count;
    var position = progress.position;
    const initial_position = position;
    while (position < count) {
        if (guard) |fence| try fence.check();
        try cached.context.ensureActive();
        try token.check();
        const width = @min(4, count - position);
        var work: [4]PageWarm = undefined;
        var tasks: [4]?local.sql_parallel_scheduler.Task(anyerror!void) = @splat(null);
        defer if (cached.context.io) |io| for (&tasks) |*task| if (task.*) |*owned| {
            owned.cancel(io) catch {};
        };
        for (work[0..width], 0..) |*item, slot| {
            item.* = .{ .navigation = pending[position + slot], .store = store, .cached = cached, .token = token, .working_set = &progress.working_set };
            if (cached.context.io) |io| tasks[slot] = local.sql_parallel_scheduler.global().submitIo(io, page_workspace, PageWarm.run, .{item});
            if (tasks[slot] == null) {
                // Saturated I/O admission must yield to foreground queries.
                // The deferred task drain and retry history retain safe bounds.
                if (cached.context.io != null) return error.NavigationWarmupInProgress;
                try item.run();
            }
        }
        for (tasks[0..width], 0..) |task, slot| if (task) |value| {
            var owned = value;
            tasks[slot] = null;
            try owned.await(cached.context.io.?);
        };
        for (work[0..width]) |item| {
            for (item.children[0..item.count]) |child| {
                if (count == pending.len) break;
                const candidate = child.?;
                const duplicate = for (pending[0..count]) |old| {
                    if (std.mem.eql(u8, &old.domain, &candidate.domain) and tree.Ref.eql(old.ref, candidate.ref)) break true;
                } else false;
                if (!duplicate) {
                    pending[count] = candidate;
                    count += 1;
                }
            }
        }
        position += width;
        progress.count = count;
        progress.position = position;
        // Count completed work even when a later wave times out. Resuming the
        // sweep must neither lose earlier pages nor count them a second time.
        if (completed_pages) |counter| _ = counter.fetchAdd(width, .monotonic);
    }
    return position - initial_position;
}
fn residencyEpoch(cache: *local.serverless_query_lake_serving_cache.Cache) u64 {
    // Cache eviction is a hint to retry bounded warming, never read authority.
    return cache.navigation.eviction_epoch.load(.monotonic) +% cache.decoded.eviction_epoch.load(.monotonic) +% cache.text_pages.eviction_epoch.load(.monotonic) +% cache.snapshot().evictions;
}
pub fn warmPublication(a: A, server: *@import("http_server.zig").ApiHttpServer, table: local.common_topology_records.TableRecord) !void {
    return warmPublicationWithin(a, server, table, .{});
}
pub fn warmPublicationWithin(a: A, server: *@import("http_server.zig").ApiHttpServer, table: local.common_topology_records.TableRecord, caller: local.api_operation.RequestContext) !void {
    try @import("lake_index_text_query.zig").refreshOwnedCut(a, server, table, caller);
    return warmNavigationWithin(a, server, table, caller);
}
/// Optional navigation warming never owns required serving-cut recovery.
pub fn warmNavigationWithin(a: A, server: *@import("http_server.zig").ApiHttpServer, table: local.common_topology_records.TableRecord, caller: local.api_operation.RequestContext) !void {
    if (server.cfg.node_config) |config| if (!config.lake_indexes.navigation_warming.enabled) return;
    // Initialization may reconcile before lifecycle callbacks are installed.
    // Do not memoize that absence as a completed warmup for this generation.
    if (server.source.lakeIndexLifecycleAuthority(caller) == null) return;
    var state = try local.metadata_lake_index_catalog.parse(a, table.lake_index_catalog_json);
    defer state.deinit();
    const publication = state.value.published orelse return;
    if ((publication.reader_protocol < local.metadata_lake_index_catalog.minimum_reader_protocol or publication.reader_protocol > local.metadata_lake_index_catalog.native_reader_protocol)) return;
    var store = try @import("lake_index_store.zig").Store.openNativeWithIo(a, server.cfg.node_config, server.cfg.secret_store, true, server.cfg.deployment_mode, server.cfg.native_lake_artifact_base_dir, server.embedding_provider_runtime.io);
    defer store.deinit();
    if (!std.mem.eql(u8, &publication.signature.store, &store.identity)) return error.ExternalLakeIndexStoreChanged;
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update(&store.cache_identity);
    var table_bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &table_bytes, table.table_id, .little);
    hash.update(&table_bytes);
    // Progress/checkpoint updates do not create new serving metadata. Warm
    // once per immutable publication rather than once per catalog revision.
    const publication_json = try std.json.Stringify.valueAlloc(a, publication, .{});
    defer a.free(publication_json);
    hash.update(publication_json);
    var key: [32]u8 = undefined;
    hash.final(&key);
    const slot = (try server.lake_navigation_warming.beginResident(key, @import("antfly_platform").time.monotonicNs(), residencyEpoch(&server.lake_read_cache))) orelse return;
    const progress = &server.lake_navigation_warming.plans[slot];
    if (!std.mem.eql(u8, &progress.key, &key)) progress.* = .{ .key = key };
    // Warming is best effort: readiness waits for a bounded attempt, not for
    // every page to become cached. Generation/read authority is checked again
    // by ordinary readiness and every query, independently of this hint.
    var completed = false;
    defer server.lake_navigation_warming.finishResident(slot, key, completed, @import("antfly_platform").time.monotonicNs(), residencyEpoch(&server.lake_read_cache));
    const started = @import("antfly_platform").time.monotonicNs();
    _ = server.lake_navigation_warming.attempts.fetchAdd(1, .monotonic);
    defer _ = server.lake_navigation_warming.nanoseconds.fetchAdd(@import("antfly_platform").time.monotonicNs() -| started, .monotonic);
    errdefer _ = server.lake_navigation_warming.failures.fetchAdd(1, .monotonic);
    const Stop = struct {
        owner: *@import("http_server.zig").ApiHttpServer,
        context: local.api_operation.RequestContext,
        fn canceled(raw: *const anyopaque) bool {
            const stop: *const @This() = @ptrCast(@alignCast(raw));
            if (stop.owner.index_installation_closing.load(.acquire)) return true;
            return stop.context.isCancellationRequested();
        }
        fn check(raw: *const anyopaque) !void {
            const stop: *const @This() = @ptrCast(@alignCast(raw));
            try stop.context.ensureActive();
            if (stop.owner.index_installation_closing.load(.acquire)) return error.Canceled;
        }
    };
    var stop: Stop = .{ .owner = server, .context = try caller.platformDeadline() };
    stop.context.deadline_ns = @min(stop.context.deadline_ns orelse std.math.maxInt(u64), started +| 2 * std.time.ns_per_s);
    const token: Token = .{ .ptr = &stop, .is_cancelled_fn = Stop.canceled, .check_fn = Stop.check };
    var request = stop.context;
    request.cancellation = .{ .ptr = &stop, .is_cancelled_fn = Stop.canceled, .check_fn = Stop.check };
    const authority = server.source.lakeIndexLifecycleAuthority(request) orelse return;
    const normalized = try request.platformDeadline();
    var context: local.serverless_query_lake_read_context.Context = .{ .io = server.embedding_provider_runtime.io, .deadline_ns = normalized.deadline_ns, .cancellation = local.storage_object_storage.CancellationToken.fromCallback(token.ptr, token.is_cancelled_fn) };
    const lease = try server.lake_reader_leases.acquire(server.embedding_provider_runtime.io, authority, table.table_id, publication.generation, context);
    defer lease.deinit();
    context = lease.readContext();
    try server.prepareLakeCache();
    store.implementation.cloud_io = context.io;
    try store.bindPublication(publication, token);
    const cached: artifacts.CachedRead = .{ .cache = &server.lake_read_cache, .scope = store.cache_identity, .context = context };
    // Published metadata can hold only the directory reference. Read the small
    // authenticated declaration document; never expand maintenance contributions.
    const declarations = if (publication.directory != null and publication.declarations.len == 0) blk: {
        const ref = publication.directory.?;
        const document = try @import("lake_index_directory.zig").loadDocument(state.arena.allocator(), store.artifactStore(), .{ .kind = .external_base_source, .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, token, cached);
        if (document.declarations.len != ref.count) return error.InvalidLakeIndexCatalog;
        break :blk document.declarations;
    } else publication.declarations;
    var roots: [max_pages]Navigation = undefined;
    var count: usize = 0;
    if (publication.row_identity) |ref| {
        const identity = try @import("lake_row_identity.zig").load(state.arena.allocator(), store.artifactStore(), ref, token, cached);
        var identity_pages = @import("lake_row_identity.zig").dependencies(identity);
        identity_roots: while (identity_pages.next()) |page| {
            for (roots[0..count]) |prior| if (std.mem.eql(u8, &prior.domain, &identity.domain) and prior.ref.eql(page)) continue :identity_roots;
            if (count == roots.len) break;
            roots[count] = .{ .domain = identity.domain, .ref = page };
            count += 1;
        }
    }
    for (declarations) |declaration| {
        try context.ensureActive();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const sa = arena.allocator();
        var domain: [32]u8 = undefined;
        var selected: [11]?tree.Ref = @splat(null);
        var warm_text_segments = false;
        if (declaration.artifact.kind == .text_segment and declaration.artifact.metadata_version == @import("lake_index_native_text.zig").metadata_version) {
            const metadata = try @import("lake_index_decoded_metadata.zig").acquire(@import("lake_index_native_text.zig").Directory, cached, store.artifactStore(), declaration.artifact, token, @import("lake_index_native_text.zig").loadDirectory);
            defer metadata.release();
            const root = metadata.value.root;
            domain = root.domain;
            warm_text_segments = root.seekable;
            selected = .{ root.term_statistics, root.term_directory.branches, root.term_directory.leaves, root.directories.routing, root.directories.ordered, root.directories.segments, root.directories.files, root.serving_routes, root.segment_files, root.source_routes, root.tie_bounds };
        } else if (declaration.artifact.kind == .ordered_row_index) {
            const root = try @import("lake_index_ordered_rows.zig").loadRootLazy(sa, store.artifactStore(), declaration.artifact, token, cached);
            domain = root.domain;
            selected[0..4].* = .{ root.page, root.reverse, root.ties, root.predicates };
            if (root.file_dictionary) |dictionary| selected[4..7].* = .{ dictionary.slots, dictionary.names, dictionary.public };
        } else continue;
        for (selected, 0..) |ref, selected_index| if (ref) |page| {
            if (count == roots.len) break;
            roots[count] = .{ .domain = domain, .ref = page, .text_segments = warm_text_segments and selected_index == 5 };
            count += 1;
        };
        if (count == roots.len) break;
    }
    const native_state = try authority.readState(a, table.table_id);
    _ = try warmRootsGuarded(store.artifactStore(), cached, roots[0..count], token, .{ .authority = authority, .table = table.table_id, .generation = publication.generation, .epoch = if (native_state.source_owner) |owner| owner.epoch else null }, progress, &server.lake_navigation_warming.pages);
    completed = true;
}

test "external lake navigation warming coalesces readiness and bounds publication concurrency" {
    var cache: Cache = .{};
    const key: [32]u8 = @splat(1);
    const slot = (try cache.begin(key)).?;
    try std.testing.expectError(error.NavigationWarmupInProgress, cache.begin(key));
    cache.finish(slot, key, true);
    try std.testing.expect(try cache.begin(key) == null);
    for (2..6) |value| _ = (try cache.begin(@splat(@intCast(value)))).?;
    try std.testing.expectError(error.NavigationWarmupInProgress, cache.begin(@splat(6)));
    try std.testing.expect(cache.contains(key));
}

test "external lake navigation warming retries failed hints after bounded backoff" {
    var cache: Cache = .{};
    const key: [32]u8 = @splat(1);
    const slot = (try cache.beginAt(key, 1)).?;
    cache.finishAt(slot, key, false, 2);
    try std.testing.expect(!cache.contains(key));
    try std.testing.expect(try cache.beginAt(key, 3) == null);
    const other: [32]u8 = @splat(2);
    const reused = (try cache.beginAt(other, 3)).?;
    cache.finishAt(reused, other, false, 4);
    // Reusing an execution slot cannot erase another publication's backoff.
    try std.testing.expect(try cache.beginAt(key, 5) == null);
    const retried = (try cache.beginAt(key, 2 + 5 * std.time.ns_per_s)).?;
    cache.finishAt(retried, key, true, 3 + 5 * std.time.ns_per_s);
    try std.testing.expect(cache.contains(key));
    try std.testing.expect(try cache.beginAt(other, 3 + 5 * std.time.ns_per_s) == null);
    try std.testing.expect(try cache.beginAt(key, 10 * std.time.ns_per_s) == null);
}

test "external lake navigation warming recovery waits cancel independently and residency eviction permits retry" {
    var cache: Cache = .{};
    const leader = try cache.claimRecovery(@splat(1), std.testing.io, .{});
    {
        defer leader.deinit();
        try std.testing.expectError(error.DeadlineExceeded, cache.claimRecovery(@splat(1), std.testing.io, .{ .deadline_ns = 0 }));
        const other = try cache.claimRecovery(@splat(2), std.testing.io, .{});
        other.deinit();
    }
    const retry = try cache.claimRecovery(@splat(1), std.testing.io, .{});
    retry.deinit();
    const slot = (try cache.beginResident(@splat(3), 1, 1)).?;
    cache.finishResident(slot, @splat(3), true, 2, 1);
    try std.testing.expect(try cache.beginResident(@splat(3), 3, 1) == null);
    try std.testing.expect(try cache.beginResident(@splat(3), 3, 2) == null);
    const rewarmed = (try cache.beginResident(@splat(3), 5 * std.time.ns_per_s + 3, 2)).?;
    cache.finishResident(rewarmed, @splat(3), true, 5 * std.time.ns_per_s + 4, 2);
    try std.testing.expect(try cache.beginResident(@splat(3), 10 * std.time.ns_per_s, 2) == null);
}

test "external lake navigation warming canceled recovery waiter leaves leader and capacity intact" {
    var cache: Cache = .{};
    const io = std.testing.io;
    const leader = try cache.claimRecovery(@splat(1), io, .{});
    var held = true;
    defer if (held) leader.deinit();
    const Waiter = struct {
        cache: *Cache,
        canceled: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) anyerror!void {
            const lease = try self.cache.claimRecovery(@splat(1), std.testing.io, .{ .cancellation = Token.fromAtomic(&self.canceled) });
            defer lease.deinit();
        }
    };
    var waiter: Waiter = .{ .cache = &cache };
    var future = try io.concurrent(Waiter.run, .{&waiter});
    defer {
        if (future.any_future != null) future.cancel(io) catch {};
    }
    var attempts: usize = 0;
    while (cache.recovery_coalesced.load(.acquire) == 0 and attempts < 5000) : (attempts += 1) try io.sleep(.fromMilliseconds(1), .awake);
    try std.testing.expect(cache.recovery_coalesced.load(.acquire) != 0);
    waiter.canceled.store(true, .release);
    try std.testing.expectError(error.Canceled, future.await(io));
    try std.testing.expectError(error.DeadlineExceeded, cache.claimRecovery(@splat(1), io, .{ .deadline_ns = 0 }));
    const other = try cache.claimRecovery(@splat(2), io, .{});
    other.deinit();
    leader.deinit();
    held = false;
    const next = try cache.claimRecovery(@splat(1), io, .{});
    next.deinit();
}

test "external lake navigation warming resumes completed waves without retaining request capabilities" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("resumable-navigation-warming");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(6), 1, std.testing.io);
    var reads: u64 = 1024 * 1024;
    var writes: u64 = 1024 * 1024;
    var pages: page_store.PageStore = .{ .domain = @splat(6), .attempt = store.upload_scope.?.attempt, .artifacts = &store, .cancellation = .none, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const root = (try tree.apply(a, pages.store(), null, &.{.{ .key = "key", .value = "value" }})).?;
    const roots: [8]Navigation = @splat(.{ .domain = @splat(6), .ref = root });
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    const cached: artifacts.CachedRead = .{ .cache = &cache, .scope = @splat(1), .context = .{} };
    var progress: Progress = .{};
    const Stop = struct {
        fn canceled(raw: *const anyopaque) bool {
            const plan: *const Progress = @ptrCast(@alignCast(raw));
            return plan.position >= 4;
        }
    };
    try std.testing.expectError(error.Canceled, warmRootsGuarded(store, cached, &roots, .{ .ptr = &progress, .is_cancelled_fn = Stop.canceled }, null, &progress, null));
    try std.testing.expectEqual(@as(usize, 4), progress.position);
    try std.testing.expectEqual(@as(usize, 4), try warmRootsGuarded(store, cached, &roots, .none, null, &progress, null));
    try std.testing.expectEqual(@as(usize, 8), progress.position);
    var plans: Cache = .{};
    const slot = (try plans.beginAt(@splat(1), 1)).?;
    plans.plans[slot] = progress;
    plans.plans[slot].key = @splat(1);
    plans.finishAt(slot, @splat(1), false, 2);
    const other = (try plans.beginAt(@splat(2), 3)).?;
    try std.testing.expect(other != slot);
    plans.finishAt(other, @splat(2), true, 4);
    const resumed = (try plans.beginAt(@splat(1), 5 * std.time.ns_per_s + 3)).?;
    try std.testing.expectEqual(slot, resumed);
    try std.testing.expectEqual(@as(usize, 8), plans.plans[resumed].position);
    plans.finishAt(resumed, @splat(1), true, 5 * std.time.ns_per_s + 4);
}

test "external lake navigation warming keeps cumulative quotas and completed-page metrics across resumes" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("resumable-navigation-quota");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(6), 1, std.testing.io);
    const bytes = (try local.storage_db_document_mapper.buildTextSegmentFromDocuments(a, &.{.{ .key = "one", .value = "{\"body\":\"alpha beta\"}" }}, .{}, null)).?;
    defer a.free(bytes);
    const ref = try @import("lake_index_seekable_text.zig").publish(a, a, &store, bytes, .none);
    defer a.free(ref.artifact_id);
    defer a.free(ref.checksum);
    const route: @import("lake_index_native_text.zig").SegmentRouting = .{ .ref = ref, .summary = null, .bytes = bytes.len, .doc_count = 1, .fields = &.{} };
    const value = try std.json.Stringify.valueAlloc(a, .{ .route = route }, .{});
    defer a.free(value);
    var read_bytes: u64 = 1024 * 1024;
    var write_bytes: u64 = 1024 * 1024;
    var pages: page_store.PageStore = .{ .domain = @splat(6), .attempt = store.upload_scope.?.attempt, .artifacts = &store, .cancellation = .none, .remaining_read_bytes = &read_bytes, .remaining_write_bytes = &write_bytes };
    const root = (try tree.apply(a, pages.store(), null, &.{.{ .key = "segment", .value = value }})).?;
    const roots: [24]Navigation = @splat(.{ .domain = @splat(6), .ref = root, .text_segments = true });
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    const cached: artifacts.CachedRead = .{ .cache = &cache, .scope = @splat(1), .context = .{} };
    var progress: Progress = .{};
    var completed = std.atomic.Value(u64).init(0);
    const Stop = struct {
        progress: *Progress,
        after: usize,
        fn canceled(raw: *const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            return self.progress.position >= self.after;
        }
    };
    for (1..6) |wave| {
        var stop: Stop = .{ .progress = &progress, .after = wave * 4 };
        try std.testing.expectError(error.Canceled, warmRootsGuarded(store, cached, &roots, .{ .ptr = &stop, .is_cancelled_fn = Stop.canceled }, null, &progress, &completed));
        try std.testing.expectEqual(wave * 4, progress.position);
        try std.testing.expectEqual(@as(u64, @intCast(wave * 4)), completed.load(.acquire));
        const remaining: usize = 16 - @min(16, wave * 4);
        try std.testing.expectEqual(remaining, progress.working_set.remaining_segments.load(.acquire));
        try std.testing.expectEqual(remaining * 1024 * 1024, progress.working_set.remaining_bytes.load(.acquire));
    }
    try std.testing.expectEqual(@as(usize, 4), try warmRootsGuarded(store, cached, &roots, .none, null, &progress, &completed));
    try std.testing.expectEqual(@as(u64, 24), completed.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), progress.working_set.remaining_segments.load(.acquire));
    // A completed sweep may be restarted after eviction, with a fresh quota.
    var next: Stop = .{ .progress = &progress, .after = 4 };
    try std.testing.expectError(error.Canceled, warmRootsGuarded(store, cached, &roots, .{ .ptr = &next, .is_cancelled_fn = Stop.canceled }, null, &progress, &completed));
    try std.testing.expectEqual(@as(usize, 12), progress.working_set.remaining_segments.load(.acquire));
    try std.testing.expectEqual(@as(u64, 28), completed.load(.acquire));
}

test "external lake navigation warming admits idle plans while another owner mutates its active continuation" {
    const io = std.testing.io;
    var cache: Cache = .{};
    const busy = (try cache.beginAt(@splat(1), 1)).?;
    cache.plans[busy].key = @splat(1);
    const Writer = struct {
        plan: *Progress,
        started: std.Io.Event = .unset,
        stop: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) anyerror!void {
            self.started.set(std.testing.io);
            while (!self.stop.load(.acquire)) {
                self.plan.position +%= 1;
                self.plan.pending[0].domain = @splat(@truncate(self.plan.position));
                try std.testing.io.sleep(.fromMilliseconds(1), .awake);
            }
        }
    };
    var writer: Writer = .{ .plan = &cache.plans[busy] };
    var task = try io.concurrent(Writer.run, .{&writer});
    defer {
        writer.stop.store(true, .release);
        if (task.any_future != null) task.cancel(io) catch {};
        cache.finishAt(busy, @splat(1), false, 1);
    }
    try writer.started.wait(io);
    for (0..100) |i| {
        const now = 1 + i * (5 * std.time.ns_per_s + 1);
        const idle = (try cache.beginAt(@splat(2), now)).?;
        try std.testing.expect(idle != busy);
        cache.finishAt(idle, @splat(2), false, now);
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    writer.stop.store(true, .release);
    try task.await(io);
    try std.testing.expect(cache.plans[busy].position != 0);
    try std.testing.expectEqual(@as([32]u8, @splat(1)), cache.plans[busy].key);
}
