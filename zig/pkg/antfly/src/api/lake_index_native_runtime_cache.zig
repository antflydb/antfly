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

//! Bounded reusable native vector runtimes. Each lane is exclusively borrowed
//! by one execution: mutable scratch and callback contexts never cross requests.
//! Fresh publication/source/credential proofs are supplied before admission.
const std = @import("std");
const local = @import("antfly_local_sources");
const server_api = @import("http_server.zig");
const Store = @import("lake_index_store.zig").Store;
const files = @import("lake_index_native_files.zig");
const dense = @import("lake_index_native_dense.zig");
const sparse = @import("lake_index_native_sparse.zig");
const A = std.mem.Allocator;
const Context = local.serverless_query_lake_read_context.Context;
const Manager = local.storage_db_catalog_index_manager.IndexManager;
pub const Cache = struct {
    mutex: std.atomic.Mutex = .unlocked,
    entries: [64]?*Entry = @splat(null),
    tick: u64 = 0,
    heap: local.sql_memory_budget = .{ .backing = @import("antfly_platform").allocator.processAllocator(std.heap.smp_allocator), .limit = 512 * 1024 * 1024 },
    managed: ?local.storage_resource_manager.BudgetedAllocator = null,
    closing: bool = false,
    pub fn attach(self: *Cache, manager: *local.storage_resource_manager.ResourceManager) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.managed != null) return;
        self.managed = local.storage_resource_manager.BudgetedAllocator.init(manager, .dense_search_working_set, @import("antfly_platform").allocator.processAllocator(std.heap.smp_allocator), 1);
        self.heap.backing = self.managed.?.allocator();
    }
    pub fn acquire(self: *Cache, server: *server_api.ApiHttpServer, declaration: local.serverless_segment_sidecar_manifest.DeclaredArtifact, domain: [32]u8, scope: [32]u8, context: Context) !*Entry {
        try context.ensureActive();
        @import("antfly_platform").sync.lockYielding(&self.heap.mutex);
        const pressure = self.heap.live > self.heap.limit / 2;
        self.heap.mutex.unlock();
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("native-lake-runtime-cache-v1");
        hash.update(&scope);
        hash.update(&domain);
        hash.update(declaration.artifact.artifact_id);
        hash.update(declaration.name);
        var key: [32]u8 = undefined;
        hash.final(&key);
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        if (self.closing) {
            self.mutex.unlock();
            return error.Canceled;
        }
        self.tick +|= 1;
        var available: ?usize = null;
        for (self.entries, 0..) |optional, slot| {
            const entry = optional orelse {
                if (available == null) available = slot;
                continue;
            };
            if (entry.busy) continue;
            if (std.mem.eql(u8, &entry.key, &key)) {
                entry.busy = true;
                entry.used = self.tick;
                entry.bind(context);
                self.mutex.unlock();
                return entry;
            }
            if (available == null or (self.entries[available.?] != null and entry.used < self.entries[available.?].?.used)) available = slot;
        }
        const slot = available orelse {
            self.mutex.unlock();
            return error.NativeLakeRuntimeCacheBusy;
        };
        const entry = std.heap.page_allocator.create(Entry) catch |err| {
            self.mutex.unlock();
            return err;
        };
        entry.* = .{ .cache = self, .key = key, .used = self.tick, .arena = .init(self.heap.allocator()), .budget = .{ .backing = self.heap.allocator(), .limit = 256 * 1024 * 1024 }, .store = undefined };
        const retired = self.entries[slot];
        // Reserve the lane before opening: concurrent admissions cannot exceed
        // the descriptor limit or borrow a partially initialized runtime.
        self.entries[slot] = entry;
        self.mutex.unlock();
        if (retired) |previous| previous.destroy();
        if (pressure) self.evictIdle();
        errdefer {
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            self.entries[slot] = null;
            self.mutex.unlock();
            entry.destroy();
        }
        try entry.open(server, declaration, domain, scope, context);
        return entry;
    }
    fn evictIdle(self: *Cache) void {
        var removed: [64]*Entry = undefined;
        var count: usize = 0;
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        for (&self.entries) |*slot| if (slot.*) |entry| {
            if (!entry.busy) {
                removed[count] = entry;
                count += 1;
                slot.* = null;
            }
        };
        self.mutex.unlock();
        for (removed[0..count]) |entry| entry.destroy();
    }
    pub fn deinit(self: *Cache) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        self.closing = true;
        const entries = self.entries;
        self.entries = @splat(null);
        self.mutex.unlock();
        for (entries) |optional| if (optional) |entry| {
            std.debug.assert(!entry.busy);
            entry.destroy();
        };
        std.debug.assert(self.heap.live == 0);
        if (self.managed) |*budget| budget.deinit();
    }
};
pub const Entry = struct {
    cache: *Cache,
    key: [32]u8,
    busy: bool = true,
    used: u64 = 0,
    arena: std.heap.ArenaAllocator,
    budget: local.sql_memory_budget,
    store: Store,
    store_open: bool = false,
    reader: ?*files.Reader = null,
    dense_metadata: ?@import("lake_index_decoded_metadata.zig").Owned(dense.Root) = null,
    sparse_metadata: ?@import("lake_index_decoded_metadata.zig").Owned(sparse.Root) = null,
    dense_entry: ?*Manager.DenseIndex = null,
    sparse_entry: ?*Manager.SparseIndex = null,
    vectors: ?*local.storage_lsm_backend.Backend = null,
    loader: ?*dense.VectorLoader = null,
    fn bind(self: *Entry, context: Context) void {
        const reader = self.reader.?;
        reader.context = context;
        if (reader.cache) |*cached| cached.context = context;
        if (self.loader) |loader| loader.context = context;
    }
    pub fn release(self: *Entry) void {
        // Never retain a pointer into an expired request or reader handle.
        self.bind(.{});
        @import("antfly_platform").sync.lockYielding(&self.cache.mutex);
        defer self.cache.mutex.unlock();
        std.debug.assert(self.busy);
        self.busy = false;
    }
    fn destroy(self: *Entry) void {
        if (self.dense_entry) |entry| entry.index.close();
        if (self.sparse_entry) |entry| entry.index.close();
        if (self.vectors) |vectors| vectors.close();
        if (self.reader) |reader| reader.deinit();
        if (self.dense_metadata) |owned| owned.release();
        if (self.sparse_metadata) |owned| owned.release();
        std.debug.assert(self.budget.live == 0);
        if (self.store_open) self.store.deinit();
        self.arena.deinit();
        std.heap.page_allocator.destroy(self);
    }
    fn open(self: *Entry, server: *server_api.ApiHttpServer, declaration: local.serverless_segment_sidecar_manifest.DeclaredArtifact, domain: [32]u8, scope: [32]u8, context: Context) !void {
        const a = self.arena.allocator();
        self.store = try Store.openNative(a, server.cfg.node_config, server.cfg.secret_store, true, server.cfg.deployment_mode, server.cfg.native_lake_artifact_base_dir);
        self.store_open = true;
        if (!std.mem.eql(u8, &self.store.identity, &scope)) return error.ExternalLakeIndexStoreChanged;
        const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = &server.lake_read_cache, .scope = scope, .context = context };
        const name = try a.dupe(u8, declaration.name);
        const path = try std.fmt.allocPrintSentinel(a, "/native-lake/{s}", .{declaration.artifact.artifact_id}, 0);
        const mutex = try a.create(std.atomic.Mutex);
        mutex.* = .unlocked;
        const reader = try a.create(files.Reader);
        if (declaration.artifact.kind == .vector_segment) {
            self.dense_metadata = try @import("lake_index_decoded_metadata.zig").acquire(dense.Root, cached, self.store.artifactStore(), declaration.artifact, .none, dense.loadRoot);
            const root = self.dense_metadata.?.value.*;
            if (!std.mem.eql(u8, &root.generation.domain, &domain) or !@import("../serverless/build/lake_rebuild.zig").bindingsEqual(root.binding, declaration.binding)) return error.InvalidNativeLakeDenseRoot;
            reader.* = .{ .root = root.generation, .prefix = path, .store = self.store.artifactStore(), .context = context, .cache = cached, .scratch = self.budget.allocator(), .max_block_bytes = 8 * 1024 * 1024 };
            self.reader = reader;
            const vectors = try a.create(local.storage_lsm_backend.Backend);
            vectors.* = try local.storage_lsm_backend.Backend.open(self.budget.allocator(), try std.fmt.allocPrint(a, "{s}/vectors", .{path}), .{ .storage = reader.storage(), .backend = .{ .read_only = true, .create_if_missing = false } });
            self.vectors = vectors;
            const index = try a.create(local.storage_hbc_adapter.HBCIndex);
            var config: local.storage_hbc_adapter.HBCConfig = .{ .dims = root.dims };
            config.metric = std.meta.stringToEnum(@TypeOf(config.metric), root.metric) orelse return error.InvalidNativeLakeDenseRoot;
            index.* = try local.storage_hbc_adapter.HBCIndex.openWithLsmOptions(self.budget.allocator(), path, config, .{ .storage = reader.storage(), .backend_options = .{ .backend = .{ .read_only = true, .create_if_missing = false } } });
            errdefer index.close();
            index.setIo(server.embedding_provider_runtime.io);
            const loader = try a.create(dense.VectorLoader);
            loader.* = .{ .backend = vectors, .dims = root.dims, .context = context };
            self.loader = loader;
            index.setExternalVectorLoader(loader, dense.VectorLoader.load);
            index.setExternalVectorScratchLoader(loader, dense.VectorLoader.loadInto);
            index.setExternalVectorBatchScratchLoader(loader, dense.VectorLoader.loadMany);
            try index.activateExperimentalPostingReads(0);
            const entry = try a.create(Manager.DenseIndex);
            entry.* = .{ .apply_mutex = mutex, .config = .{ .name = name, .kind = .dense_vector, .config_json = root.config_json }, .field_name = try a.dupe(u8, root.binding.column_bindings[0]), .dims = root.dims, .metric = config.metric, .external = true, .chunk_name = null, .embedding_name = null, .native_physical_v2 = true, .index = index };
            self.dense_entry = entry;
        } else {
            self.sparse_metadata = try @import("lake_index_decoded_metadata.zig").acquire(sparse.Root, cached, self.store.artifactStore(), declaration.artifact, .none, sparse.loadRoot);
            const root = self.sparse_metadata.?.value.*;
            if (!std.mem.eql(u8, &root.generation.domain, &domain) or !@import("../serverless/build/lake_rebuild.zig").bindingsEqual(root.binding, declaration.binding)) return error.InvalidNativeLakeSparseRoot;
            reader.* = .{ .root = root.generation, .prefix = path, .store = self.store.artifactStore(), .context = context, .cache = cached, .scratch = self.budget.allocator(), .max_block_bytes = 8 * 1024 * 1024 };
            self.reader = reader;
            const entry = try a.create(Manager.SparseIndex);
            entry.* = .{ .apply_mutex = mutex, .config = .{ .name = name, .kind = .sparse_vector, .config_json = root.config_json }, .field_name = try a.dupe(u8, root.binding.column_bindings[0]), .external = true, .chunk_name = null, .embedding_name = null, .rebuild_root_path = path, .index = try local.sparse_sparse.SparseIndex.open(self.budget.allocator(), path, .{ .lsm_storage = reader.storage(), .lsm_options = .{ .backend = .{ .read_only = true, .create_if_missing = false } } }) };
            self.sparse_entry = entry;
        }
    }
};
