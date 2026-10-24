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
    // Seekable packs stay behind bounded block readers. Their total remote
    // footprint is independent of the decoded heap budget below.
    max_seekable_bytes: ?u64 = null,
    bytes: u64 = 0,
    reservations: std.AutoHashMapUnmanaged([32]u8, Reservation) = .empty,
    tick: u64 = 0,
    admissions: std.AutoHashMapUnmanaged([32]u8, *Admission) = .empty,
    inventory_budget: local.sql_memory_budget = .{ .backing = std.heap.page_allocator, .limit = 64 * 1024 * 1024 },
    inventory_builds: std.atomic.Value(u64) = .init(0),
    closing: bool = false,
    resource_manager: ?*local.storage_resource_manager.ResourceManager = null,
    // Includes schema arenas, cold segment payloads, decoded native metadata
    // and statistics across every corpus, rather than only encoded file size.
    heap_budget: local.sql_memory_budget = .{ .backing = @import("antfly_platform").allocator.processAllocator(std.heap.smp_allocator), .limit = 256 * 1024 * 1024 },

    const Admission = struct {
        references: usize = 1,
        finished: std.atomic.Value(bool) = .init(false),
        ready: std.Io.Event = .unset,
    };
    fn releaseAdmission(self: *Cache, flight: *Admission) void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        flight.references -= 1;
        if (flight.references == 0) std.heap.page_allocator.destroy(flight);
    }
    fn finishAdmission(self: *Cache, key: [32]u8, flight: *Admission, io: std.Io) void {
        platform.sync.lockYielding(&self.mutex);
        _ = self.admissions.remove(key);
        flight.finished.store(true, .release);
        flight.ready.set(io);
        self.mutex.unlock();
        self.releaseAdmission(flight);
    }
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
        return self.acquireSelected(io, store, root_ref, root, schema_json, cached, context, cancellation, null);
    }
    pub fn acquireSelected(self: *Cache, io: std.Io, store: stores.ArtifactStore, root_ref: local.serverless_manifest_artifact_ref.ArtifactRef, root: corpus.Root, schema_json: []const u8, cached: artifacts.CachedRead, context: Context, cancellation: Cancellation, selection: ?[]const [32]u8) !local.storage_db_query_search_exec.PinnedTextSource {
        while (true) {
            return self.acquireOnce(io, store, root_ref, root, schema_json, cached, context, cancellation, selection) catch |err| {
                if (err == error.NativeLakeTextCacheRetry) continue;
                if (err == error.NativeLakeTextCacheBusy) self.evictIdle();
                return err;
            };
        }
    }
    fn acquireOnce(self: *Cache, io: std.Io, store: stores.ArtifactStore, root_ref: local.serverless_manifest_artifact_ref.ArtifactRef, directory: corpus.Root, schema_json: []const u8, cached: artifacts.CachedRead, context: Context, cancellation: Cancellation, selection: ?[]const [32]u8) !local.storage_db_query_search_exec.PinnedTextSource {
        try context.ensureActive();
        try cancellation.check();
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("native-lake-text-snapshot-cache-v1");
        hash.update(&cached.scope);
        hash.update(root_ref.artifact_id);
        hash.update(schema_json);
        hash.update(if (selection != null) "selected-files-v1" else "full-corpus-v1");
        if (selection) |files| for (files) |file| hash.update(&file);
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
                entry.awaitReady(io, context) catch |err| {
                    if (entry.state.load(.acquire) != .failed) return err;
                    try context.ensureActive();
                    try cancellation.check();
                    return error.NativeLakeTextCacheRetry;
                };
                return try entry.source(store, cached, context, cancellation);
            }
        };
        if (self.closing) {
            self.mutex.unlock();
            return error.Canceled;
        }
        if (self.admissions.get(key)) |flight| {
            flight.references += 1;
            self.mutex.unlock();
            defer self.releaseAdmission(flight);
            while (!flight.finished.load(.acquire)) {
                try context.ensureActive();
                try cancellation.check();
                flight.ready.waitTimeout(io, .{ .duration = .{ .clock = .awake, .raw = .fromMilliseconds(10) } }) catch |err| switch (err) {
                    error.Timeout => continue,
                    else => return err,
                };
            }
            try context.ensureActive();
            try cancellation.check();
            // A leader owns only its own authority. Retry the resident entry
            // (or a failed admission) under this waiter's capability.
            return error.NativeLakeTextCacheRetry;
        }
        if (self.admissions.count() >= 16) {
            self.mutex.unlock();
            return error.NativeLakeTextCacheBusy;
        }
        const flight = std.heap.page_allocator.create(Admission) catch |err| {
            self.mutex.unlock();
            return err;
        };
        flight.* = .{};
        self.admissions.put(std.heap.page_allocator, key, flight) catch |err| {
            self.mutex.unlock();
            std.heap.page_allocator.destroy(flight);
            return err;
        };
        self.mutex.unlock();
        defer self.finishAdmission(key, flight, io);
        _ = self.inventory_builds.fetchAdd(1, .monotonic);
        // Directory decoding finishes before acquiring child cache flights.
        // A large manifest can safely request exclusive decode admission.
        var metadata_budget: local.sql_memory_budget = .{ .backing = self.inventory_budget.allocator(), .limit = self.inventory_budget.limit };
        var metadata_arena = std.heap.ArenaAllocator.init(metadata_budget.allocator());
        defer metadata_arena.deinit();
        const inventory = inventory: {
            const started = platform.time.monotonicNs();
            defer if (cached.metrics) |metrics| metrics.record(.text_inventory, started);
            break :inventory (if (selection) |files|
                corpus.admitSelectedServingInventory(metadata_arena.allocator(), self.inventory_budget.allocator(), directory, store, cached, cancellation, files)
            else
                corpus.admitServingInventory(metadata_arena.allocator(), self.inventory_budget.allocator(), directory, store, cached, cancellation)) catch |err| {
                if (err == error.OutOfMemory and (self.inventory_budget.isExhausted() or metadata_budget.isExhausted())) return error.NativeLakeTextCacheBusy;
                return err;
            };
        };
        defer inventory.deinit();
        const root = inventory.root;
        var bytes: u64 = root_ref.byte_len;
        for (root.segments) |segment| bytes = std.math.add(u64, bytes, segment.byte_len) catch return error.NativeLakeTextCorpusTooLarge;
        if (bytes > (if (root.seekable) self.max_seekable_bytes orelse self.max_bytes else self.max_bytes) or self.max_entries == 0) return error.NativeLakeTextCorpusTooLarge;
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
                entry.awaitReady(io, context) catch |err| {
                    if (entry.state.load(.acquire) != .failed) return err;
                    try context.ensureActive();
                    try cancellation.check();
                    return error.NativeLakeTextCacheRetry;
                };
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
        if (self.entries.count() >= @min(self.max_entries, 64) or additional > (self.max_seekable_bytes orelse self.max_bytes) -| self.bytes) {
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
        std.debug.assert(self.admissions.count() == 0 and self.inventory_budget.live == 0);
        self.admissions.deinit(std.heap.page_allocator);
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
    masked: bool = false,
    statistics_version: u8 = 0,
    term_statistics: ?corpus.statistics.Ref = null,
    segment_summaries: []const ?corpus.statistics.Ref = &.{},
    term_directory: corpus.term_directory.Roots = .{},
    statistics_identities: []const [32]u8 = &.{},
    statistics_ordinals: []const corpus.statistics.IdentityOrdinal = &.{},
    statistics_routing: []const local.index.IndexSnapshot.TextCandidate = &.{},
    domain: [32]u8 = @splat(0),
    descriptors: @import("lake_index_seekable_text.zig").Descriptors = .empty,
    identities: @import("lake_index_text_predicate.zig").Identities = undefined,
    analysis: local.introducer.TextAnalysisConfig = .{},
    selected_field: ?[]const u8 = null,
    stored_projection_fields: []const []const u8 = &.{},
    schema: ?local.storage_schema.TableSchema = null,
    name: []const u8 = "",
    allocator: A,
    resource_manager: ?*local.storage_resource_manager.ResourceManager = null,
    fn build(self: *Entry, io: std.Io, store: stores.ArtifactStore, root: corpus.Root, name: []const u8, schema_json: []const u8, cached: artifacts.CachedRead, cancellation: Cancellation, base: ?*Entry, peers: []const *Entry) !void {
        const started = platform.time.monotonicNs();
        defer if (cached.metrics) |metrics| metrics.record(.text_opening, started);
        self.seekable = root.seekable;
        self.statistics_version = root.statistics_version;
        self.term_statistics = root.term_statistics;
        self.term_directory = root.term_directory;
        self.domain = root.domain;
        const a = self.arena.allocator();
        if (root.statistics_version >= 1) {
            const identities_ = try a.alloc([32]u8, root.segments.len);
            for (identities_, root.segments) |*id, segment| id.* = corpus.term_directory.identity(segment.artifact_id);
            self.statistics_identities = identities_;
            const refs = try a.alloc(?corpus.statistics.Ref, root.segments.len);
            var position_: usize = 0;
            for (root.file_groups) |group| {
                if (group.summaries.len != group.segments.len or group.summaries.len > refs.len - position_) return error.InvalidNativeLakeTextCorpus;
                @memcpy(refs[position_..][0..group.summaries.len], group.summaries);
                position_ += group.summaries.len;
            }
            if (position_ != refs.len) return error.InvalidNativeLakeTextCorpus;
            self.segment_summaries = refs;
        }
        if (root.stored_projection) {
            const fields = try a.alloc([]const u8, root.binding.column_bindings.len);
            for (fields, root.binding.column_bindings) |*field, path| field.* = try a.dupe(u8, path);
            self.stored_projection_fields = fields;
        }
        var schema = try local.schema_mod.parseValidatedTableSchema(a, schema_json);
        defer schema.deinit(a);
        self.schema = try local.schema_mod.deriveRuntimeTableSchema(a, schema);
        self.analysis = try local.storage_db_catalog_index_manager.parseTextAnalysisForIndexConfig(a, root.config_json, self.schema);
        const config = try std.json.parseFromSliceLeaky(std.json.Value, a, root.config_json, .{ .allocate = .alloc_always });
        if (config == .object) if (config.object.get("field")) |field| {
            if (field == .string) self.selected_field = field.string;
        };
        self.name = try a.dupe(u8, name);
        const masked = root.physical_documents != null;
        self.masked = masked;
        const predecessor = if (masked or (if (base) |prior| prior.masked else false)) null else base;
        var live_by_id: std.AutoHashMapUnmanaged(u64, []const corpus.text_directory.Interval) = .empty;
        defer live_by_id.deinit(self.allocator);
        self.writer = if (predecessor) |prior| try prior.writer.?.forkImmutable() else try local.index.IndexWriter.init(self.allocator);
        var removed: std.ArrayList(u64) = .empty;
        defer removed.deinit(self.allocator);
        var additions: std.ArrayList(artifacts.ChunkRef) = .empty;
        defer additions.deinit(self.allocator);
        var admission_by_id: std.StringHashMapUnmanaged(local.segment.SegmentReader.Admission) = .empty;
        defer admission_by_id.deinit(self.allocator);
        const Route = struct { segment: corpus.SegmentRouting, manifest: local.serverless_manifest_artifact_ref.ArtifactRef, ordinal: usize };
        var routing_by_id: std.StringHashMapUnmanaged(Route) = .empty;
        defer routing_by_id.deinit(self.allocator);
        if (root.manifests.len == root.file_groups.len) for (root.file_groups, root.manifests) |group, manifest| {
            const ref: local.serverless_manifest_artifact_ref.ArtifactRef = .{ .kind = .text_segment, .metadata_version = corpus.metadata_version, .artifact_id = try a.dupe(u8, manifest.artifact_id), .checksum = try a.dupe(u8, manifest.checksum), .byte_len = manifest.byte_len };
            for (group.segments, 0..) |segment, ordinal| try self.descriptors.put(a, try a.dupe(u8, segment.artifact_id), .{ .manifest = ref, .ordinal = ordinal });
        };
        if (root.routing.len != 0) for (root.routing, root.manifests) |routes, manifest| {
            for (routes, 0..) |route, ordinal| try routing_by_id.put(self.allocator, route.ref.artifact_id, .{ .segment = route, .manifest = .{ .kind = .text_segment, .metadata_version = corpus.metadata_version, .artifact_id = manifest.artifact_id, .checksum = manifest.checksum, .byte_len = manifest.byte_len }, .ordinal = ordinal });
        };
        // Exact per-segment descriptors belong to this generation, including
        // when its immutable physical reader is shared with another corpus.
        for (root.routing) |routes| for (routes) |route| if (route.descriptor) |descriptor| {
            const ref: local.serverless_manifest_artifact_ref.ArtifactRef = .{ .kind = .text_segment, .metadata_version = corpus.metadata_version, .artifact_id = try a.dupe(u8, descriptor.artifact_id), .checksum = try a.dupe(u8, descriptor.checksum), .byte_len = descriptor.byte_len };
            try self.descriptors.put(a, try a.dupe(u8, route.ref.artifact_id), .{ .manifest = ref, .ordinal = 0 });
            try routing_by_id.put(self.allocator, route.ref.artifact_id, .{ .segment = route, .manifest = ref, .ordinal = 0 });
        };
        for (root.file_groups) |group| if (group.masked and root.routing.len == 0) {
            const routes = try @import("lake_index_serving_inventory.zig").compactRoutes(a, group);
            for (routes) |route| {
                const descriptor = route.descriptor orelse return error.InvalidNativeLakeTextCorpus;
                try routing_by_id.put(self.allocator, route.ref.artifact_id, .{ .segment = route, .manifest = .{ .kind = .text_segment, .metadata_version = corpus.metadata_version, .artifact_id = descriptor.artifact_id, .checksum = descriptor.checksum, .byte_len = descriptor.byte_len }, .ordinal = 0 });
            }
        };
        for (root.file_groups) |group| if (group.admissions.len != 0) {
            for (group.segments, group.admissions) |ref, metadata| try admission_by_id.put(self.allocator, ref.artifact_id, metadata);
        };
        var ids: std.ArrayList(u64) = .empty;
        defer ids.deinit(self.allocator);
        var shared: std.ArrayList(local.index.IndexWriter.ImmutableSegment) = .empty;
        defer shared.deinit(self.allocator);
        defer for (shared.items) |item| item.snapshot.release();
        var next_id = self.writer.?.next_segment_id;
        for (root.segments, 0..) |segment, ordinal| {
            const existing = if (predecessor) |prior| prior.segments.get(segment.artifact_id) else null;
            const id = if (existing) |location| location.id else next_id;
            if (existing == null) {
                next_id += 1;
                var reused = false;
                if (!masked) for (peers) |peer| if (!peer.masked) if (peer.segments.get(segment.artifact_id)) |location| {
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
            if (routing_by_id.get(segment.artifact_id)) |route| if (route.segment.live.len != 0 and !(route.segment.live.len == 1 and route.segment.live[0].lower == 0 and route.segment.live[0].count == route.segment.doc_count)) try live_by_id.put(self.allocator, id, route.segment.live);
        }
        if (predecessor) |prior| {
            var iterator = prior.segments.iterator();
            while (iterator.next()) |segment| if (!self.segments.contains(segment.key_ptr.*)) try removed.append(self.allocator, segment.value_ptr.id);
        }
        const replacements = try self.allocator.alloc(local.index.ReplacementSegmentData, additions.items.len);
        defer self.allocator.free(replacements);
        var loaded: usize = 0;
        defer for (replacements[0..loaded]) |*replacement| deinitReplacement(replacement, self.allocator);
        // Transport cohorts populate original content-addressed cache entries.
        // Bound optional warming independently of archive size; ordinary required
        // reads remain authoritative if residency is denied or the bound is hit.
        if (additions.items.len != 0) {
            var cold = std.StringHashMap(void).init(self.allocator);
            defer cold.deinit();
            for (additions.items) |ref| try cold.put(ref.artifact_id, {});
            var opening_bytes: usize = 16 * 1024 * 1024;
            for (root.file_groups) |group| {
                const needed = for (group.segments) |ref| {
                    if (cold.contains(ref.artifact_id)) break true;
                } else false;
                if (!needed) continue;
                for (group.opening_packs) |pack| {
                    if (pack.decoded_bytes > opening_bytes) continue;
                    opening_bytes -= pack.decoded_bytes;
                    @import("lake_index_text_opening_pack.zig").warm(self.allocator, store, pack, root.domain, cached, cancellation) catch |err| switch (err) {
                        error.OutOfMemory, error.CacheBudgetExceeded => {},
                        else => return err,
                    };
                }
            }
        }
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
                if (routing_by_id.get(ref.artifact_id)) |route| {
                    const seekable = @import("lake_index_seekable_text.zig");
                    var data = try seekable.loadRouted(self.allocator, .{ .store = store, .cache = cached, .context = cached.context, .cancellation = cancellation, .resource_manager = self.resource_manager }, ref, route.segment.bytes, route.manifest, route.ordinal);
                    errdefer data.deinit(self.allocator);
                    const reader = try local.segment.SegmentReader.initRouting(self.allocator, route.segment.doc_count, route.segment.fields, seekable.routedReader);
                    replacements[loaded] = .{ .id = ids.items[ordinal], .data = data, .prepared_reader = reader };
                    loaded += 1;
                    continue;
                }
                // Descriptor admission performs no artifact reads. Do not
                // charge unvisited directory payloads or queue an I/O-sized
                // CPU job for these small, bounded metadata copies.
                if (admission_by_id.get(ref.artifact_id)) |metadata| {
                    replacements[loaded] = try loadSegment(&loader, self.allocator, ref, cancellation, ids.items[ordinal], metadata);
                    loaded += 1;
                    continue;
                }
                try stores.chargeReadBudget(&read_bytes, ref.byte_len);
                tasks[ordinal - position] = local.sql_parallel_scheduler.global().submit(io, @intCast(ref.byte_len), loadSegment, .{ &loader, self.allocator, ref, cancellation, ids.items[ordinal], admission_by_id.get(ref.artifact_id) });
                if (tasks[ordinal - position] == null) {
                    replacements[loaded] = try loadSegment(&loader, self.allocator, ref, cancellation, ids.items[ordinal], admission_by_id.get(ref.artifact_id));
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
        if (!masked and live_by_id.count() != 0) return error.InvalidNativeLakeTextCorpus;
        const masks = try self.allocator.alloc(local.encoding_roaring.RoaringBitmap, if (masked) replacements.len else 0);
        defer self.allocator.free(masks);
        for (masks) |*mask| mask.* = .init(self.allocator);
        defer for (masks) |*mask| mask.deinit();
        for (replacements, 0..) |*replacement, ordinal| if (live_by_id.get(replacement.id)) |live| {
            const mask = &masks[ordinal];
            var previous: u32 = 0;
            const count = (replacement.prepared_reader orelse return error.InvalidNativeLakeTextCorpus).doc_count;
            for (live) |interval| {
                const end = try std.math.add(u32, interval.lower, interval.count);
                if (interval.count == 0 or interval.lower < previous or end > count) return error.InvalidNativeLakeTextCorpus;
                try mask.addRange(previous, interval.lower);
                previous = end;
            }
            try mask.addRange(previous, count);
            replacement.deleted = mask;
        };
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
        // A formerly selected subset can become the complete corpus after
        // file removal. Both directions need private, generation-owned scoring
        // state even when ordering and physical readers remain unchanged.
        const physical = self.writer.?.acquireSnapshot();
        defer physical.release();
        var documents: u64 = 0;
        const field_count = if (root.collection) |totals| totals.fields.len else physical.global_total_field_len.count();
        const fields = try a.alloc(local.index.IndexWriter.CollectionField, field_count);
        if (root.collection) |totals| {
            documents = totals.documents;
            for (fields, totals.fields) |*field, total| field.* = .{ .name = total.name, .total_length = total.total_length };
        } else {
            if (root.partial_inventory) return error.InvalidNativeLakeTextCorpus;
            for (physical.segments) |segment| documents += segment.reader.doc_count;
            var totals = physical.global_total_field_len.iterator();
            for (fields) |*field| {
                const total = totals.next().?;
                field.* = .{ .name = total.key_ptr.*, .total_length = total.value_ptr.* };
            }
        }
        if (documents > std.math.maxInt(u32)) return error.InvalidNativeLakeTextCorpus;
        if (masked) try self.writer.?.sealVerifiedMaskedCollection(@intCast(documents), fields) else try self.writer.?.sealImmutableCollection(@intCast(documents), fields);
        const snapshot = self.writer.?.acquireSnapshot();
        defer snapshot.release();
        self.identities = try @import("lake_index_text_predicate.zig").Identities.init(a, root, snapshot);
        self.identities.segment_ids = self.statistics_identities;
        if (root.statistics_version >= 1) {
            const ordinals = try a.alloc(corpus.statistics.IdentityOrdinal, root.segments.len);
            var offset: u32 = 0;
            for (ordinals, self.statistics_identities, snapshot.segments, 0..) |*ordinal, id, segment, i| {
                ordinal.* = .{ .identity = id, .segment_idx = i, .doc_offset = offset };
                offset = try std.math.add(u32, offset, segment.reader.doc_count);
            }
            std.mem.sort(corpus.statistics.IdentityOrdinal, ordinals, {}, corpus.statistics.identityLess);
            self.statistics_ordinals = ordinals;
            const routing = try a.alloc(local.index.IndexSnapshot.TextCandidate, ordinals.len);
            for (routing, ordinals) |*candidate, ordinal| candidate.* = .{ .segment_idx = ordinal.segment_idx, .doc_offset = ordinal.doc_offset };
            self.statistics_routing = routing;
        }
        if (self.resource_manager) |manager| self.writer.?.attachResourceManager(manager);
    }
    fn loadSegment(loader: *corpus.CachedSegments, a: A, ref: artifacts.ChunkRef, cancellation: Cancellation, id: u64, metadata: ?local.segment.SegmentReader.Admission) anyerror!local.index.ReplacementSegmentData {
        if (metadata) |admitted| {
            var data = try @import("lake_index_seekable_text.zig").loadAdmitted(a, .{ .store = loader.store, .cache = loader.cache, .context = loader.cache.context, .cancellation = cancellation, .resource_manager = loader.resource_manager }, ref, admitted);
            errdefer data.deinit(a);
            var reader = try local.segment.SegmentReader.initAuthenticated(a, data.native, admitted);
            errdefer reader.deinit();
            try cancellation.check();
            return .{ .id = id, .data = data, .prepared_reader = reader };
        }
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
        statistics: corpus.statistics.Reader = undefined,
        fn checkRead(raw: *anyopaque) !void {
            const read: *const @import("lake_index_seekable_text.zig").Read = @ptrCast(@alignCast(raw));
            try read.context.ensureActive();
            try read.cancellation.check();
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const entry = self.entry;
            self.statistics.deinit();
            entry.allocator.destroy(self);
            entry.release();
        }
    };
    fn source(self: *Entry, store: stores.ArtifactStore, cached: artifacts.CachedRead, context: Context, cancellation: Cancellation) !local.storage_db_query_search_exec.PinnedTextSource {
        if (!self.seekable) return .{ .snapshot = self.writer.?.acquireSnapshot(), .name = self.name, .text_analysis = self.analysis, .runtime_schema = self.schema, .selected_field = self.selected_field, .stored_projection_fields = self.stored_projection_fields, .provider_metadata = &self.identities, .logical_tie_rank = self.identities.tie_rank, .owner = self, .release_owner = releaseSource };
        const lease = self.allocator.create(QueryLease) catch return error.NativeLakeTextCacheBusy;
        errdefer self.allocator.destroy(lease);
        lease.* = .{ .entry = self, .statistics = .{ .domain = self.domain, .global = null, .segments = &.{}, .read = undefined }, .read = .{ .store = store, .cache = cached, .context = context, .cancellation = cancellation, .resource_manager = self.resource_manager, .descriptors = if (self.descriptors.count() != 0) &self.descriptors else null } };
        const snapshot = self.writer.?.acquireSnapshotWithReadCapability(.{ .ptr = &lease.read, .check = QueryLease.checkRead, .io = context.io }) catch |err| return if (err == error.OutOfMemory) error.NativeLakeTextCacheBusy else err;
        if (self.statistics_version >= 1) {
            lease.statistics = .{ .read = lease.read, .domain = self.domain, .global = self.term_statistics, .segments = self.segment_summaries, .directory = self.term_directory, .identities = self.statistics_identities, .ordinals = self.statistics_ordinals, .routing = self.statistics_routing, .allocator = self.allocator, .partial_inventory = self.identities.partial_inventory };
            snapshot.text_statistics = lease.statistics.interface();
        }
        return .{ .read_context = &lease.read, .snapshot = snapshot, .name = self.name, .text_analysis = self.analysis, .runtime_schema = self.schema, .selected_field = self.selected_field, .stored_projection_fields = self.stored_projection_fields, .provider_metadata = &self.identities, .logical_tie_rank = self.identities.tie_rank, .owner = lease, .release_owner = QueryLease.release };
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
