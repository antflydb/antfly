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

//! Bounded startup planning warmup. Only immutable navigation enters shared
//! caches; reader capabilities, posting data and source rows remain query-owned.
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
pub const Navigation = struct { domain: [32]u8, ref: tree.Ref };
pub const Stats = struct { attempts: u64 = 0, failures: u64 = 0, pages: u64 = 0, nanoseconds: u64 = 0 };
pub const Cache = struct {
    attempts: std.atomic.Value(u64) = .init(0),
    failures: std.atomic.Value(u64) = .init(0),
    pages: std.atomic.Value(u64) = .init(0),
    nanoseconds: std.atomic.Value(u64) = .init(0),
    active: [4]?[32]u8 = @splat(null),
    mutex: std.atomic.Mutex = .unlocked,
    keys: [256]?[32]u8 = @splat(null),
    next: usize = 0,
    pub fn snapshot(self: *const Cache) Stats {
        return .{ .attempts = self.attempts.load(.monotonic), .failures = self.failures.load(.monotonic), .pages = self.pages.load(.monotonic), .nanoseconds = self.nanoseconds.load(.monotonic) };
    }
    fn begin(self: *Cache, key: [32]u8) !?usize {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        for (self.keys) |item| if (item) |old| if (std.mem.eql(u8, &old, &key)) return null;
        for (self.active) |item| if (item) |old| if (std.mem.eql(u8, &old, &key)) return error.NavigationWarmupInProgress;
        for (&self.active, 0..) |*item, index| if (item.* == null) {
            item.* = key;
            return index;
        };
        return error.NavigationWarmupInProgress;
    }
    fn finish(self: *Cache, slot: usize, key: [32]u8) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        self.active[slot] = null;
        self.keys[self.next] = key;
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
const PageWarm = struct {
    navigation: Navigation,
    store: stores.ArtifactStore,
    cached: artifacts.CachedRead,
    token: Token,
    children: [8]?Navigation = @splat(null),
    count: usize = 0,
    pub fn child(self: *PageWarm, ref: tree.Ref) !void {
        // Warm at most one additional interior level. A large fan-out cannot
        // turn a startup hint into a scan of the archive or all leaf pages.
        if (ref.height == 0 or self.count == self.children.len) return;
        self.children[self.count] = .{ .domain = self.navigation.domain, .ref = ref };
        self.count += 1;
    }
    pub fn record(_: *PageWarm, _: []const u8, _: []const u8) !void {}
    fn read(raw: *anyopaque, a: A, store: *stores.ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, _: u64, length: usize, _: [32]u8, token: Token, budget: *u64) ![]u8 {
        const self: *PageWarm = @ptrCast(@alignCast(raw));
        try self.cached.context.ensureActive();
        try stores.chargeReadBudget(budget, length);
        return artifacts.readArtifact(a, store.*, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, token, self.cached);
    }
    fn run(self: *PageWarm) anyerror!void {
        try self.cached.context.ensureActive();
        var memory: local.sql_memory_budget = .{ .backing = std.heap.page_allocator, .limit = page_workspace };
        var reads: u64 = tree.max_page_bytes;
        var writes: u64 = 0;
        var pages: page_store.PageStore = .{ .domain = self.navigation.domain, .artifacts = &self.store, .cancellation = self.token, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .read_cache = .{ .ptr = self, .read = read } };
        try tree.walkPage(memory.allocator(), pages.store(), self.navigation.ref, self);
        try self.cached.context.ensureActive();
    }
};
/// Four admitted I/O lanes, at most 64 authenticated pages and two interior
/// levels. Every worker joins before its operation-local reader lease ends.
pub fn warmRoots(store: stores.ArtifactStore, cached: artifacts.CachedRead, roots: []const Navigation, token: Token) !usize {
    var pending: [max_pages]Navigation = undefined;
    var count: usize = @min(roots.len, pending.len);
    @memcpy(pending[0..count], roots[0..count]);
    const initial = count;
    var position: usize = 0;
    while (position < count) {
        try cached.context.ensureActive();
        try token.check();
        const width = @min(4, count - position);
        var work: [4]PageWarm = undefined;
        var tasks: [4]?local.sql_parallel_scheduler.Task(anyerror!void) = @splat(null);
        defer if (cached.context.io) |io| for (&tasks) |*task| if (task.*) |*owned| {
            owned.cancel(io) catch {};
        };
        for (work[0..width], 0..) |*item, slot| {
            item.* = .{ .navigation = pending[position + slot], .store = store, .cached = cached, .token = token };
            if (cached.context.io) |io| tasks[slot] = local.sql_parallel_scheduler.global().submitIo(io, page_workspace, PageWarm.run, .{item});
            if (tasks[slot] == null) try item.run();
        }
        for (tasks[0..width], 0..) |task, slot| if (task) |value| {
            var owned = value;
            tasks[slot] = null;
            try owned.await(cached.context.io.?);
        };
        for (work[0..width], position..) |item, index| if (index < initial) {
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
        };
        position += width;
    }
    return position;
}
pub fn warmPublication(a: A, server: *@import("http_server.zig").ApiHttpServer, table: local.common_topology_records.TableRecord) !void {
    return warmPublicationWithin(a, server, table, .{});
}
pub fn warmPublicationWithin(a: A, server: *@import("http_server.zig").ApiHttpServer, table: local.common_topology_records.TableRecord, caller: local.api_operation.RequestContext) !void {
    if (server.cfg.node_config) |config| if (!config.lake_indexes.navigation_warming.enabled) return;
    var state = try local.metadata_lake_index_catalog.parse(a, table.lake_index_catalog_json);
    defer state.deinit();
    const publication = state.value.published orelse return;
    if (publication.reader_protocol != local.metadata_lake_index_catalog.native_reader_protocol) return;
    var store = try @import("lake_index_store.zig").Store.openNative(a, server.cfg.node_config, server.cfg.secret_store, true, server.cfg.deployment_mode, server.cfg.native_lake_artifact_base_dir);
    defer store.deinit();
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update(&store.identity);
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
    const slot = (try server.lake_navigation_warming.begin(key)) orelse return;
    // Warming is best effort: readiness waits for a bounded attempt, not for
    // every page to become cached. Generation/read authority is checked again
    // by ordinary readiness and every query, independently of this hint.
    defer server.lake_navigation_warming.finish(slot, key);
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
            stop.context.ensureActive() catch return true;
            return false;
        }
    };
    var stop: Stop = .{ .owner = server, .context = try caller.platformDeadline() };
    stop.context.deadline_ns = @min(stop.context.deadline_ns orelse std.math.maxInt(u64), started +| 2 * std.time.ns_per_s);
    const token: Token = .{ .ptr = &stop, .is_cancelled_fn = Stop.canceled };
    var request = stop.context;
    request.cancellation = .{ .ptr = &stop, .is_cancelled_fn = Stop.canceled };
    const authority = server.source.lakeIndexLifecycleAuthority(request) orelse return;
    const normalized = try request.platformDeadline();
    var context: local.serverless_query_lake_read_context.Context = .{ .io = server.embedding_provider_runtime.io, .deadline_ns = normalized.deadline_ns, .cancellation = local.storage_object_storage.CancellationToken.fromCallback(token.ptr, token.is_cancelled_fn) };
    const lease = try server.lake_reader_leases.acquire(server.embedding_provider_runtime.io, authority, table.table_id, publication.generation, context);
    defer lease.deinit();
    context = lease.readContext();
    try server.prepareLakeCache();
    const cached: artifacts.CachedRead = .{ .cache = &server.lake_read_cache, .scope = store.identity, .context = context };
    var roots: [max_pages]Navigation = undefined;
    var count: usize = 0;
    for (publication.declarations) |declaration| {
        try context.ensureActive();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const sa = arena.allocator();
        var domain: [32]u8 = undefined;
        var selected: [8]?tree.Ref = @splat(null);
        if (declaration.artifact.kind == .text_segment and declaration.artifact.metadata_version == @import("lake_index_native_text.zig").metadata_version) {
            const metadata = try @import("lake_index_decoded_metadata.zig").acquire(@import("lake_index_native_text.zig").Directory, cached, store.artifactStore(), declaration.artifact, token, @import("lake_index_native_text.zig").loadDirectory);
            defer metadata.release();
            const root = metadata.value.root;
            domain = root.domain;
            selected = .{ root.term_statistics, root.term_directory.branches, root.term_directory.leaves, root.directories.routing, root.directories.segments, root.directories.files, root.serving_routes, root.segment_files };
        } else if (declaration.artifact.kind == .ordered_row_index) {
            const root = try @import("lake_index_ordered_rows.zig").loadRoot(sa, store.artifactStore(), declaration.artifact, token, cached);
            domain = root.domain;
            selected[0..4].* = .{ root.page, root.reverse, root.ties, root.predicates };
        } else continue;
        for (selected) |ref| if (ref) |page| {
            if (count == roots.len) break;
            roots[count] = .{ .domain = domain, .ref = page };
            count += 1;
        };
        if (count == roots.len) break;
    }
    const pages = try warmRoots(store.artifactStore(), cached, roots[0..count], token);
    _ = server.lake_navigation_warming.pages.fetchAdd(pages, .monotonic);
}

test "external lake navigation warming coalesces readiness and bounds publication concurrency" {
    var cache: Cache = .{};
    const key: [32]u8 = @splat(1);
    const slot = (try cache.begin(key)).?;
    try std.testing.expectError(error.NavigationWarmupInProgress, cache.begin(key));
    cache.finish(slot, key);
    try std.testing.expect(try cache.begin(key) == null);
    for (2..6) |value| _ = (try cache.begin(@splat(@intCast(value)))).?;
    try std.testing.expectError(error.NavigationWarmupInProgress, cache.begin(@splat(6)));
    try std.testing.expect(cache.contains(key));
}
