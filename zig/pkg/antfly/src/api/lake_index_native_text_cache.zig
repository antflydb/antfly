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

//! Bounded process-owned native corpora. Current authorization, source proof
//! and publication leases belong to each query, never to this payload cache.
const std = @import("std");
const local = @import("antfly_local_sources");
const corpus = @import("lake_index_native_text.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const stores = @import("../serverless/artifacts/store.zig");
const A = std.mem.Allocator;
const Context = local.serverless_query_lake_read_context.Context;
const Cancellation = @import("antfly_cancellation").CancellationToken;
const platform = @import("antfly_platform");
pub const Cache = struct {
    mutex: std.atomic.Mutex = .unlocked,
    entries: std.AutoHashMapUnmanaged([32]u8, *Entry) = .empty,
    max_entries: usize = 64,
    max_bytes: u64 = 512 * 1024 * 1024,
    bytes: u64 = 0,
    reservations: std.AutoHashMapUnmanaged([32]u8, Reservation) = .empty,
    tick: u64 = 0,
    closing: bool = false,
    resource_manager: ?*local.storage_resource_manager.ResourceManager = null,
    // Includes schema arenas, cold segment payloads, decoded native metadata
    // and statistics across every corpus, rather than only encoded file size.
    heap_budget: local.sql_memory_budget = .{ .backing = @import("antfly_platform").allocator.processAllocator(std.heap.smp_allocator), .limit = 256 * 1024 * 1024 },

    const Charge = struct { key: [32]u8, bytes: u64 };
    const Reservation = struct { references: usize, bytes: u64 };
    fn charge(compatible: [32]u8, id: []const u8, bytes: u64) Charge {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("native-lake-physical-text-reservation-v1");
        hash.update(&compatible);
        hash.update(id);
        var key: [32]u8 = undefined;
        hash.final(&key);
        return .{ .key = key, .bytes = bytes };
    }
    fn releaseChargesLocked(self: *Cache, charges: []const Charge) void {
        for (charges) |value| {
            const entry = self.reservations.getPtr(value.key).?;
            std.debug.assert(entry.references != 0 and entry.bytes == value.bytes);
            entry.references -= 1;
            if (entry.references == 0) {
                self.bytes -= value.bytes;
                _ = self.reservations.remove(value.key);
            }
        }
    }

    pub fn attachResourceManager(self: *Cache, manager: *local.storage_resource_manager.ResourceManager) void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.resource_manager) |current| {
            std.debug.assert(current == manager);
            return;
        }
        std.debug.assert(self.entries.count() == 0);
        self.resource_manager = manager;
    }

    pub fn acquire(self: *Cache, io: std.Io, store: stores.ArtifactStore, root_ref: local.serverless_manifest_artifact_ref.ArtifactRef, root: corpus.Root, schema_json: []const u8, cached: artifacts.CachedRead, context: Context, cancellation: Cancellation) !local.storage_db_query_search_exec.PinnedTextSource {
        while (true) {
            return self.acquireOnce(io, store, root_ref, root, schema_json, cached, context, cancellation) catch |err| {
                if (err == error.NativeLakeTextCacheRetry) continue;
                if (err == error.NativeLakeTextCacheBusy) self.evictIdle();
                return err;
            };
        }
    }
    fn acquireOnce(self: *Cache, io: std.Io, store: stores.ArtifactStore, root_ref: local.serverless_manifest_artifact_ref.ArtifactRef, root: corpus.Root, schema_json: []const u8, cached: artifacts.CachedRead, context: Context, cancellation: Cancellation) !local.storage_db_query_search_exec.PinnedTextSource {
        try context.ensureActive();
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("native-lake-text-snapshot-cache-v1");
        hash.update(&cached.scope);
        hash.update(root_ref.artifact_id);
        hash.update(schema_json);
        var key: [32]u8 = undefined;
        hash.final(&key);
        platform.sync.lockYielding(&self.mutex);
        if (!self.closing) if (self.entries.get(key)) |entry| {
            if (entry.state.load(.acquire) != .failed) {
                self.tick +|= 1;
                entry.used = self.tick;
                _ = entry.references.fetchAdd(1, .monotonic);
                self.mutex.unlock();
                errdefer entry.release();
                try entry.awaitReady(io, context);
                return try entry.source(store, cached, context, cancellation);
            }
        };
        self.mutex.unlock();
        try root.validate();
        var bytes: u64 = root_ref.byte_len;
        for (root.segments) |segment| bytes = std.math.add(u64, bytes, segment.byte_len) catch return error.NativeLakeTextCorpusTooLarge;
        if (bytes > self.max_bytes or self.max_entries == 0) return error.NativeLakeTextCorpusTooLarge;
        var compatible_hash = std.crypto.hash.Blake3.init(.{});
        compatible_hash.update(&cached.scope);
        compatible_hash.update(&root.domain);
        compatible_hash.update(schema_json);
        compatible_hash.update(root_ref.name);
        compatible_hash.update(root.config_json);
        var compatible: [32]u8 = undefined;
        compatible_hash.final(&compatible);
        const charges = try std.heap.page_allocator.alloc(Charge, root.segments.len + 1);
        var transferred = false;
        defer if (!transferred) std.heap.page_allocator.free(charges);
        charges[0] = charge(compatible, root_ref.artifact_id, root_ref.byte_len);
        for (root.segments, charges[1..]) |segment, *value| value.* = charge(compatible, segment.artifact_id, segment.byte_len);
        platform.sync.lockYielding(&self.mutex);
        if (self.closing) {
            self.mutex.unlock();
            return error.Canceled;
        }
        self.tick +|= 1;
        if (self.entries.get(key)) |entry| {
            if (entry.state.load(.acquire) != .failed) {
                entry.used = self.tick;
                _ = entry.references.fetchAdd(1, .monotonic);
                self.mutex.unlock();
                errdefer entry.release();
                try entry.awaitReady(io, context);
                return try entry.source(store, cached, context, cancellation);
            }
            if (entry.references.load(.acquire) != 1) {
                self.mutex.unlock();
                try io.sleep(.fromMilliseconds(1), .awake);
                return error.NativeLakeTextCacheRetry;
            }
            _ = self.entries.remove(key);
            self.mutex.unlock();
            entry.release();
            return error.NativeLakeTextCacheRetry;
        }
        // Serialize compatible cold builders so a reserved segment always
        // becomes a shared physical reader before another generation uses it.
        var pending = self.entries.valueIterator();
        while (pending.next()) |candidate| {
            const prior = candidate.*;
            if (!std.mem.eql(u8, &prior.compatible, &compatible)) continue;
            if (prior.state.load(.acquire) == .failed) {
                if (prior.references.load(.acquire) == 1) {
                    _ = self.entries.remove(prior.key);
                    self.mutex.unlock();
                    prior.release();
                } else {
                    self.mutex.unlock();
                    try io.sleep(.fromMilliseconds(1), .awake);
                }
                return error.NativeLakeTextCacheRetry;
            }
            if (prior.state.load(.acquire) != .loading) continue;
            _ = prior.references.fetchAdd(1, .monotonic);
            self.mutex.unlock();
            defer prior.release();
            prior.awaitReady(io, context) catch {
                try context.ensureActive();
            };
            return error.NativeLakeTextCacheRetry;
        }
        var additional: u64 = 0;
        for (charges) |value| {
            if (self.reservations.get(value.key)) |prior| {
                if (prior.bytes != value.bytes) {
                    self.mutex.unlock();
                    return error.InvalidNativeLakeTextRoot;
                }
            } else additional +|= value.bytes;
        }
        if (self.entries.count() >= @min(self.max_entries, 64) or additional > self.max_bytes -| self.bytes) {
            var victim: ?*Entry = null;
            var iterator = self.entries.valueIterator();
            while (iterator.next()) |candidate| {
                const entry = candidate.*;
                if (entry.references.load(.acquire) != 1 or entry.state.load(.acquire) == .loading) continue;
                if (victim == null or entry.used < victim.?.used) victim = entry;
            }
            const old = victim orelse {
                self.mutex.unlock();
                return error.NativeLakeTextCacheBusy;
            };
            _ = self.entries.remove(old.key);
            self.mutex.unlock();
            // Destruction returns physical reservations before retrying. A
            // publication still used by a reader can never be evicted here.
            old.release();
            return error.NativeLakeTextCacheRetry;
        }
        var base: ?*Entry = null;
        var best_overlap: usize = 0;
        var candidates = self.entries.valueIterator();
        while (candidates.next()) |candidate| {
            const prior = candidate.*;
            if (prior.state.load(.acquire) != .ready or !std.mem.eql(u8, &prior.compatible, &compatible)) continue;
            var overlap: usize = 0;
            for (root.segments) |segment| if (prior.segments.contains(segment.artifact_id)) {
                overlap += 1;
            };
            if (overlap > best_overlap) {
                base = prior;
                best_overlap = overlap;
            }
        }
        var peers: [64]*Entry = undefined;
        var peer_count: usize = 0;
        var peer_iterator = self.entries.valueIterator();
        while (peer_iterator.next()) |candidate| {
            const prior = candidate.*;
            if (prior.state.load(.acquire) == .ready and std.mem.eql(u8, &prior.compatible, &compatible)) {
                peers[peer_count] = prior;
                peer_count += 1;
            }
        }
        const entry = std.heap.page_allocator.create(Entry) catch |err| {
            self.mutex.unlock();
            return err;
        };
        self.entries.ensureUnusedCapacity(std.heap.page_allocator, 1) catch |err| {
            self.mutex.unlock();
            std.heap.page_allocator.destroy(entry);
            return err;
        };
        self.reservations.ensureUnusedCapacity(std.heap.page_allocator, @intCast(charges.len)) catch |err| {
            self.mutex.unlock();
            std.heap.page_allocator.destroy(entry);
            return err;
        };
        for (charges) |value| {
            const reservation = self.reservations.getOrPutAssumeCapacity(value.key);
            if (!reservation.found_existing) {
                reservation.value_ptr.* = .{ .references = 0, .bytes = value.bytes };
                self.bytes += value.bytes;
            }
            reservation.value_ptr.references += 1;
        }
        entry.* = .{ .key = key, .compatible = compatible, .owner = self, .charges = charges, .used = self.tick, .arena = .init(self.heap_budget.allocator()), .resource_manager = self.resource_manager, .allocator = self.heap_budget.allocator() };
        self.entries.putAssumeCapacity(key, entry);
        transferred = true;
        for (peers[0..peer_count]) |prior| _ = prior.references.fetchAdd(1, .monotonic);
        self.mutex.unlock();
        defer for (peers[0..peer_count]) |prior| prior.release();
        errdefer entry.release();
        entry.build(io, store, root, root_ref.name, schema_json, cached, cancellation, base, peers[0..peer_count]) catch |err| {
            const failure = if (err == error.OutOfMemory) error.NativeLakeTextCacheBusy else err;
            entry.failure = failure;
            entry.state.store(.failed, .release);
            entry.ready.set(io);
            if (err == error.OutOfMemory) self.evictIdle();
            return failure;
        };
        entry.state.store(.ready, .release);
        entry.ready.set(io);
        try context.ensureActive();
        return try entry.source(store, cached, context, cancellation);
    }
    pub fn deinit(self: *Cache) void {
        platform.sync.lockYielding(&self.mutex);
        self.closing = true;
        var entries = self.entries;
        self.entries = .empty;
        self.mutex.unlock();
        var iterator = entries.valueIterator();
        while (iterator.next()) |entry| {
            // The server drains requests before destroying its payload cache.
            std.debug.assert(entry.*.references.load(.acquire) == 1);
            entry.*.release();
        }
        entries.deinit(std.heap.page_allocator);
        std.debug.assert(self.bytes == 0 and self.reservations.count() == 0);
        self.reservations.deinit(std.heap.page_allocator);
        std.debug.assert(self.heap_budget.live == 0);
    }
    fn evictIdle(self: *Cache) void {
        var retired: [64]*Entry = undefined;
        var count: usize = 0;
        platform.sync.lockYielding(&self.mutex);
        var iterator = self.entries.valueIterator();
        while (iterator.next()) |entry| {
            if (entry.*.references.load(.acquire) != 1 or entry.*.state.load(.acquire) == .loading) continue;
            retired[count] = entry.*;
            count += 1;
        }
        for (retired[0..count]) |entry| {
            _ = self.entries.remove(entry.key);
        }
        self.mutex.unlock();
        for (retired[0..count]) |entry| entry.release();
    }
};
const Entry = struct {
    const Location = struct { id: u64, ordinal: usize };
    key: [32]u8,
    compatible: [32]u8,
    segments: std.StringHashMapUnmanaged(Location) = .empty,
    owner: *Cache,
    charges: []Cache.Charge,
    used: u64,
    references: std.atomic.Value(usize) = .init(2), // cache and first query
    state: std.atomic.Value(enum(u8) { loading, ready, failed }) = .init(.loading),
    ready: std.Io.Event = .unset,
    failure: ?anyerror = null,
    arena: std.heap.ArenaAllocator,
    writer: ?local.index.IndexWriter = null,
    seekable: bool = false,
    analysis: local.introducer.TextAnalysisConfig = .{},
    selected_field: ?[]const u8 = null,
    schema: ?local.storage_schema.TableSchema = null,
    name: []const u8 = "",
    allocator: A,
    resource_manager: ?*local.storage_resource_manager.ResourceManager = null,
    fn build(self: *Entry, io: std.Io, store: stores.ArtifactStore, root: corpus.Root, name: []const u8, schema_json: []const u8, cached: artifacts.CachedRead, cancellation: Cancellation, base: ?*Entry, peers: []const *Entry) !void {
        self.seekable = root.seekable;
        const a = self.arena.allocator();
        var schema = try local.schema_mod.parseValidatedTableSchema(a, schema_json);
        defer schema.deinit(a);
        self.schema = try local.schema_mod.deriveRuntimeTableSchema(a, schema);
        self.analysis = try local.storage_db_catalog_index_manager.parseTextAnalysisForIndexConfig(a, root.config_json, self.schema);
        const config = try std.json.parseFromSliceLeaky(std.json.Value, a, root.config_json, .{ .allocate = .alloc_always });
        if (config == .object) if (config.object.get("field")) |field| {
            if (field == .string) self.selected_field = field.string;
        };
        self.name = try a.dupe(u8, name);
        self.writer = if (base) |prior| try prior.writer.?.forkImmutable() else try local.index.IndexWriter.init(self.allocator);
        var removed: std.ArrayList(u64) = .empty;
        defer removed.deinit(self.allocator);
        var additions: std.ArrayList(artifacts.ChunkRef) = .empty;
        defer additions.deinit(self.allocator);
        var ids: std.ArrayList(u64) = .empty;
        defer ids.deinit(self.allocator);
        var shared: std.ArrayList(local.index.IndexWriter.ImmutableSegment) = .empty;
        defer shared.deinit(self.allocator);
        defer for (shared.items) |item| item.snapshot.release();
        var next_id = self.writer.?.next_segment_id;
        for (root.segments, 0..) |segment, ordinal| {
            const existing = if (base) |prior| prior.segments.get(segment.artifact_id) else null;
            const id = if (existing) |location| location.id else next_id;
            if (existing == null) {
                next_id += 1;
                var reused = false;
                for (peers) |peer| if (peer.segments.get(segment.artifact_id)) |location| {
                    const snapshot = peer.writer.?.acquireSnapshot();
                    errdefer snapshot.release();
                    try shared.append(self.allocator, .{ .snapshot = snapshot, .ordinal = location.ordinal, .target_id = id });
                    reused = true;
                    break;
                };
                if (!reused) {
                    try additions.append(self.allocator, segment);
                    try ids.append(self.allocator, id);
                }
            }
            try self.segments.put(a, try a.dupe(u8, segment.artifact_id), .{ .id = id, .ordinal = ordinal });
        }
        if (base) |prior| {
            var iterator = prior.segments.iterator();
            while (iterator.next()) |segment| if (!self.segments.contains(segment.key_ptr.*)) try removed.append(self.allocator, segment.value_ptr.id);
        }
        const replacements = try self.allocator.alloc(local.index.ReplacementSegmentData, additions.items.len);
        defer self.allocator.free(replacements);
        var loaded: usize = 0;
        defer for (replacements[0..loaded]) |*replacement| deinitReplacement(replacement, self.allocator);
        var loader: corpus.CachedSegments = .{ .store = store, .cache = cached, .seekable = root.seekable, .query_owned = true, .resource_manager = self.resource_manager };
        var read_bytes: u64 = 512 * 1024 * 1024;
        var position: usize = 0;
        while (position < additions.items.len) {
            const end = @min(position + 4, additions.items.len);
            var tasks: [4]?local.sql_parallel_scheduler.Task(anyerror!local.index.ReplacementSegmentData) = @splat(null);
            defer for (&tasks) |*task| if (task.*) |*pending| if (pending.future != null) {
                var replacement = pending.cancel(io) catch continue;
                deinitReplacement(&replacement, self.allocator);
            };
            for (position..end) |ordinal| {
                const ref = additions.items[ordinal];
                try cancellation.check();
                try stores.chargeReadBudget(&read_bytes, ref.byte_len);
                tasks[ordinal - position] = local.sql_parallel_scheduler.global().submit(io, @intCast(ref.byte_len), loadSegment, .{ &loader, self.allocator, ref, cancellation, ids.items[ordinal] });
                if (tasks[ordinal - position] == null) {
                    replacements[loaded] = try loadSegment(&loader, self.allocator, ref, cancellation, ids.items[ordinal]);
                    loaded += 1;
                }
            }
            for (position..end) |ordinal| if (tasks[ordinal - position]) |*pending| {
                replacements[loaded] = try pending.await(io);
                loaded += 1;
            };
            position = end;
        }
        try cached.context.ensureActive();
        try self.writer.?.shareImmutableSegments(shared.items);
        if (removed.items.len != 0 or replacements.len != 0) try self.writer.?.replaceSegmentsManyData(removed.items, replacements);
        loaded = 0; // writer owns every replacement after atomic publication
        for (replacements) |replacement| {
            if (replacement.data == .native and replacement.data.native == .ranges) {
                const range = replacement.data.native.ranges;
                if (range.seal_read_context) |seal| seal(range.ptr);
            }
        }
        const ordered_ids = try self.allocator.alloc(u64, root.segments.len);
        defer self.allocator.free(ordered_ids);
        for (root.segments, ordered_ids) |segment, *id| id.* = self.segments.get(segment.artifact_id).?.id;
        try self.writer.?.orderImmutableSegments(ordered_ids);
        if (self.resource_manager) |manager| self.writer.?.attachResourceManager(manager);
    }
    fn loadSegment(loader: *corpus.CachedSegments, a: A, ref: artifacts.ChunkRef, cancellation: Cancellation, id: u64) anyerror!local.index.ReplacementSegmentData {
        const mapped = loader.loader();
        var data = try mapped.load(mapped.ptr, a, ref, cancellation);
        errdefer data.deinit(a);
        // Directory loading alone leaves footer/field metadata I/O until the
        // writer's serial publication loop. Admit those required reads in the
        // same bounded jobs, then transfer each reader without reopening it.
        var reader = try data.initReader(a);
        errdefer reader.deinit();
        try cancellation.check();
        return .{ .id = id, .data = data, .prepared_reader = reader };
    }
    fn deinitReplacement(replacement: *local.index.ReplacementSegmentData, a: A) void {
        if (replacement.prepared_reader) |*reader| reader.deinit();
        replacement.data.deinit(a);
    }
    fn awaitReady(self: *Entry, io: std.Io, context: Context) !void {
        while (self.state.load(.acquire) == .loading) {
            try context.ensureActive();
            self.ready.waitTimeout(io, .{ .duration = .{ .clock = .awake, .raw = .fromMilliseconds(10) } }) catch |err| switch (err) {
                error.Timeout => continue,
                else => return err,
            };
        }
        try context.ensureActive();
        if (self.failure) |failure| return failure;
    }
    const QueryLease = struct {
        entry: *Entry,
        read: @import("lake_index_seekable_text.zig").Read,
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const entry = self.entry;
            entry.allocator.destroy(self);
            entry.release();
        }
    };
    fn source(self: *Entry, store: stores.ArtifactStore, cached: artifacts.CachedRead, context: Context, cancellation: Cancellation) !local.storage_db_query_search_exec.PinnedTextSource {
        if (!self.seekable) return .{ .snapshot = self.writer.?.acquireSnapshot(), .name = self.name, .text_analysis = self.analysis, .runtime_schema = self.schema, .selected_field = self.selected_field, .owner = self, .release_owner = releaseSource };
        const lease = self.allocator.create(QueryLease) catch return error.NativeLakeTextCacheBusy;
        errdefer self.allocator.destroy(lease);
        lease.* = .{ .entry = self, .read = .{ .store = store, .cache = cached, .context = context, .cancellation = cancellation, .resource_manager = self.resource_manager } };
        return .{ .read_context = &lease.read, .snapshot = self.writer.?.acquireSnapshotWithReadContext(&lease.read) catch |err| return if (err == error.OutOfMemory) error.NativeLakeTextCacheBusy else err, .name = self.name, .text_analysis = self.analysis, .runtime_schema = self.schema, .selected_field = self.selected_field, .owner = lease, .release_owner = QueryLease.release };
    }
    fn releaseSource(raw: *anyopaque) void {
        const self: *Entry = @ptrCast(@alignCast(raw));
        self.release();
    }
    fn release(self: *Entry) void {
        if (self.references.fetchSub(1, .acq_rel) != 1) return;
        if (self.writer) |*writer| writer.deinit();
        self.arena.deinit();
        platform.sync.lockYielding(&self.owner.mutex);
        self.owner.releaseChargesLocked(self.charges);
        self.owner.mutex.unlock();
        std.heap.page_allocator.free(self.charges);
        std.heap.page_allocator.destroy(self);
    }
};
