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

//! Shared local handles and helpers. Server lifetimes are opaque callbacks.
pub const antfly = @import("../capi_embedded_root.zig");
pub const std = @import("std");
pub const builtin = @import("builtin");
pub const local_write = antfly.local_write;
pub const capi = @import("types.zig");
pub const kernel_owner_abi = @import("kernel_owner_abi");
pub const local_query_client = @import("local_query_client");
pub const capi_build_options = @import("capi_build_options");
pub const db_mod = antfly.db;
pub const read_consistency = @import("../storage/read_consistency.zig");
pub const transactions_mod = antfly.transactions;
pub const aggregations_mod = db_mod.aggregations;
pub const search_agg_mod = antfly.aggregation;
pub const geo_mod = antfly.geo;
pub const lite_backend = antfly.lite.backend;
pub const batch_api = antfly.public_api.batch;
pub const query_api = antfly.public_api.query;
pub const tables_api = antfly.public_api.tables;
pub const table_reads_api = antfly.local_query_contract;
pub const inference_provider = antfly.inference_provider;
pub const managed_embedder = antfly.managed_embedder;
pub const Allocator = std.mem.Allocator;

/// Version 2 unified the naming (antfly_db_* takes a handle, antfly_* is
/// library-level, antfly_lite_* is the .aflite format) and merged the Lite
/// open options into antfly_open_options.
pub const abi_version: u32 = 2;
pub const Handle = struct {
    alloc: std.mem.Allocator,
    db: db_mod.DB,
    open_mode: db_mod.OpenOptions.OpenMode = .writer,
    readable_lease_hook: ?ReadableLeaseHook = null,
    owned_lite_backend: ?lite_backend.Handle = null,
    lite_profile: ?lite_backend.Profile = null,
    lite_inference_status: ?lite_backend.InferenceStatus = null,
    storage_owner_path: ?[]u8 = null,
    storage_owner_managed_config: local_write.OwnerManagedConfig = .{},
    storage_owner_target_observer: kernel_owner_abi.TargetObserver = .{},
    storage_owner_table_name: ?[]u8 = null,
    row_policy_authority_secret: ?[]u8 = null,
    row_policy_authority_issuer: ?[]u8 = null,
    storage_owner_group_id: u64 = 0,
    storage_owner_root_generation: u64 = 0,
    storage_owner_context: ?*anyopaque = null,
    storage_owner_transaction_recovery: ?*anyopaque = null,
    storage_owner_runtime_hooks: ?*anyopaque = null,
    server_cleanup: ?*const fn (*Handle) void = null,
    server_context_release: ?*const fn (*anyopaque) void = null,
    // Present only for a Lite handle opened with the local-runtime-configured
    // flag on a build that both advertises and actually links the local
    // inference runtime (see capi_build_options.inference_enabled and
    // pkg/antfly/build/runtime.zig's addCapiInferenceVariantUnits). Default
    // libantfly and Lite handles opened without the flag leave these null,
    // which keeps today's behavior unchanged.
    lite_inference_lifetime: ?inference_provider.EmbeddedInferenceProviderLifetime = null,
    lite_inference_io: ?*std.Io.Threaded = null,
    // Mirrors `LiteResolvedOpenOptions.generated_enrichment_replay` from this
    // handle's open call. `refreshLiteManagedEmbeddingRuntime` must not lose
    // this caller intent across its own reconfigure passes -- see its doc
    // comment.
    lite_generated_enrichment_replay: bool = false,
    // Serialized threading mode (see CAPI.md "Thread Safety"): every export
    // enters through `enterHandle`, which takes `api_lock` shared for reads
    // and data writes and exclusively for schema/admin changes. Data writes
    // and maintenance additionally serialize on their own mutexes so they
    // queue instead of failing with ANTFLY_BUSY, while reads keep running
    // against pinned snapshots. Callers hold a HandleRegistry id rather than
    // this pointer, so close can drain and free safely (see HandleRegistry).
    api_lock: std.Io.RwLock = .init,
    write_mutex: std.Io.Mutex = .init,
    maintenance_mutex: std.Io.Mutex = .init,

    pub fn liteAntflyProvider(self: *Handle) ?managed_embedder.AntflyProvider {
        const lifetime = if (self.lite_inference_lifetime) |*value| value else return null;
        return inference_provider.inferenceBoundaryProvider(lifetime);
    }

    pub fn prepareSearchRequest(self: *Handle, req: db_mod.types.SearchRequest) !void {
        const hook = self.readable_lease_hook orelse return;
        try hook.featureReads().prepareSearch(hook.group_id, req);
    }

    pub fn prepareDenseSearchRequest(
        self: *Handle,
        index_name: []const u8,
        vector: []const f32,
        k: u32,
        limit: u32,
        offset: u32,
    ) !void {
        try self.prepareSearchRequest(.{
            .index_name = index_name,
            .query = .{ .dense_knn = .{
                .vector = vector,
                .k = k,
            } },
            .limit = limit,
            .offset = offset,
            .include_stored = false,
        });
    }

    pub fn prepareLookupRequest(self: *Handle, key: []const u8, opts: db_mod.types.LookupOptions) !void {
        const hook = self.readable_lease_hook orelse return;
        try hook.featureReads().prepareLookup(hook.group_id, key, opts);
    }

    pub fn prepareScanRequest(
        self: *Handle,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_mod.types.ScanOptions,
    ) !void {
        const hook = self.readable_lease_hook orelse return;
        try hook.featureReads().prepareScan(hook.group_id, from_key, to_key, opts);
    }
};

pub fn stopLiteEmbeddedInference(handle: *Handle) void {
    if (handle.lite_inference_lifetime) |*lifetime| {
        lifetime.quiesce();
        inference_provider.destroyEmbeddedInferenceNode(lifetime.handle, lifetime.resource_owner);
        handle.lite_inference_lifetime = null;
    }
    if (handle.lite_inference_io) |io_impl| {
        io_impl.deinit();
        handle.alloc.destroy(io_impl);
        handle.lite_inference_io = null;
    }
}

pub fn closeHandle(handle: *Handle) void {
    const storage_owner_context = handle.storage_owner_context;
    const server_context_release = handle.server_context_release;
    if (handle.owned_lite_backend != null and liteOpenModeCanWrite(handle.open_mode)) {
        handle.db.sync(true) catch {};
        handle.db.syncIndexes(true) catch {};
    }
    handle.db.close();
    stopLiteEmbeddedInference(handle);
    if (handle.server_cleanup) |cleanup| cleanup(handle);
    if (handle.owned_lite_backend) |*backend| {
        backend.deinit();
    }
    if (handle.storage_owner_path) |path| handle.alloc.free(path);
    if (handle.storage_owner_table_name) |table_name| handle.alloc.free(table_name);
    if (handle.row_policy_authority_secret) |secret| {
        @memset(secret, 0);
        handle.alloc.free(secret);
    }
    if (handle.row_policy_authority_issuer) |issuer| handle.alloc.free(issuer);
    handle.alloc.destroy(handle);
    if (storage_owner_context) |context| if (server_context_release) |release| release(context);
}

pub fn liteOpenModeCanWrite(open_mode: db_mod.OpenOptions.OpenMode) bool {
    return switch (open_mode) {
        .writer, .writer_no_replay => true,
        else => false,
    };
}

pub fn currentIdentityReadGenerationForHandle(handle: *Handle, requested: ?u64) !u64 {
    return try handle.db.currentIdentityReadGenerationForRequest(requested);
}

pub fn stampSearchRequestIdentityGeneration(handle: *Handle, req: *db_mod.types.SearchRequest) !void {
    req.identity_read_generation = try currentIdentityReadGenerationForHandle(handle, req.identity_read_generation);
}

pub const ReadableLeaseHookFn = *const fn (
    ctx: ?*anyopaque,
    group_id: u64,
    request_ctx_ptr: ?[*]const u8,
    request_ctx_len: usize,
) callconv(.c) capi.ErrorCode;

pub const ReadableLeaseHook = struct {
    group_id: u64,
    callback_ctx: ?*anyopaque,
    callback: ReadableLeaseHookFn,

    pub fn requester(self: *const ReadableLeaseHook) read_consistency.ReadSafetyBarrier {
        return .{
            .ptr = @constCast(self),
            .vtable = &.{
                .wait_read_safe = waitReadSafe,
            },
        };
    }

    pub fn featureReads(self: *const ReadableLeaseHook) read_consistency.FeatureReads {
        return read_consistency.FeatureReads.init(self.requester());
    }

    pub fn waitReadSafe(ptr: *anyopaque, group_id: u64, request_ctx: []const u8) !void {
        const self: *ReadableLeaseHook = @ptrCast(@alignCast(ptr));
        const code = self.callback(
            self.callback_ctx,
            group_id,
            if (request_ctx.len > 0) request_ctx.ptr else null,
            request_ctx.len,
        );
        switch (code) {
            .ok => {},
            .invalid_argument => return error.InvalidArgument,
            .not_found => return error.NotFound,
            .version_conflict => return error.VersionConflict,
            .intent_conflict => return error.IntentConflict,
            .txn_not_found => return error.TxnNotFound,
            .busy => return error.WouldBlock,
            .outcome_unknown => return error.DurabilityOutcomeUnknown,
            .unsupported => return error.UnsupportedOperation,
            .stalled => return error.Stalled,
            .cancelled => return error.Canceled,
            .internal => return error.Internal,
        }
    }
};

/// Resolves a caller's handle id without entering it. Exports go through
/// `enterHandle` instead; this is for close and for internal callers (the
/// storage-owner ABI, tests) that do not race close.
pub fn asHandle(ptr: ?*anyopaque) ?*Handle {
    const id = handle_registry.decode(ptr) orelse return null;
    const slot = handle_registry.slotFor(id.index) orelse return null;
    if (slot.state.load(.acquire) >> 1 != id.generation) return null;
    return slot.handle.load(.acquire);
}

/// Handles given to callers are ids naming a registry slot plus a
/// generation, not `*Handle` pointers. Slots live in chunks that are never
/// freed, so any handle value a caller passes, including one for a handle
/// closed on another thread a moment ago, dereferences valid memory:
/// entering a stale or closing generation fails with
/// ANTFLY_INVALID_ARGUMENT instead of touching a freed Handle, closing one
/// is a no-op, and a reused slot never matches an old id.
///
/// Handle values must also be safe for bindings to hold in pointer-typed
/// fields: Go's garbage collector throws on an `unsafe.Pointer` that lands
/// in its heap arenas without naming a live object, and on values below
/// 4096. So on 64-bit POSIX targets each id is encoded as an address inside
/// a PROT_NONE reservation this process owns and never touches: a real,
/// unique address that no allocator can ever return.
pub fn HandleRegistryOf(comptime T: type) type {
    return struct {
        const Self = @This();
        const chunk_len = 256;
        const index_bits = 20;
        const max_slots = 1 << index_bits;
        const max_chunks = max_slots / chunk_len;
        /// Handle values are 8-byte aligned offsets into the reservation.
        const stride_shift = 3;
        const reserve_address_space = @bitSizeOf(usize) == 64 and switch (builtin.os.tag) {
            .linux, .macos, .freebsd, .netbsd, .openbsd, .dragonfly, .ios => true,
            else => false,
        };

        pub const Slot = struct {
            /// `generation << 1 | closing`. The slot is open for `generation`
            /// exactly when the closing bit is clear.
            state: @import("antfly_platform").atomic.Value(u64) = .init(0),
            /// Calls that have entered, or are trying to, for any generation.
            active: std.atomic.Value(u32) = .init(0),
            handle: std.atomic.Value(?*T) = .init(null),
            next_free: u32 = 0,
        };

        const Id = struct {
            index: u32,
            generation: u64,
        };

        chunks: [max_chunks]std.atomic.Value(?*[chunk_len]Slot) = @splat(.init(null)),
        mutex: std.atomic.Mutex = .unlocked,
        slot_count: u32 = 0,
        free_head: ?u32 = null,
        /// Start of the id reservation (0 until the first registration, or on
        /// targets that use plain integer ids) and the generation width it fits.
        base: std.atomic.Value(usize) = .init(0),
        generation_bits: u6 = 0,

        pub fn generationMask(self: *const Self) u64 {
            return (@as(u64, 1) << self.generation_bits) - 1;
        }

        /// Reserves the id address space on first use. Called with `mutex` held.
        pub fn ensureIdSpace(self: *Self) !void {
            if (self.generation_bits != 0) return;
            if (comptime !reserve_address_space) {
                self.generation_bits = @bitSizeOf(usize) - index_bits - 1;
                return;
            }
            // Prefer 17 generation bits (1 TiB of address space, no memory);
            // step down if the platform limits reservations.
            for ([_]u6{ 17, 13, 9 }) |bits| {
                const len = @as(usize, 1) << (index_bits + bits + stride_shift);
                const region = std.posix.mmap(null, len, .{}, .{ .TYPE = .PRIVATE, .ANONYMOUS = true, .NORESERVE = true }, -1, 0) catch continue;
                self.generation_bits = bits;
                self.base.store(@intFromPtr(region.ptr), .release);
                return;
            }
            return error.OutOfMemory;
        }

        pub fn encode(self: *const Self, id: Id) *anyopaque {
            const offset = (id.generation << index_bits | id.index);
            if (comptime !reserve_address_space) {
                // Plain ids; index + 1 keeps the value non-null.
                return @ptrFromInt(@as(usize, @intCast(offset + 1)));
            }
            return @ptrFromInt(self.base.load(.acquire) + (@as(usize, @intCast(offset)) << stride_shift));
        }

        pub fn decode(self: *const Self, ptr: ?*anyopaque) ?Id {
            const raw = @intFromPtr(ptr orelse return null);
            const offset: u64 = if (comptime !reserve_address_space) blk: {
                break :blk @as(u64, raw) - 1;
            } else blk: {
                const base = self.base.load(.acquire);
                if (base == 0 or raw < base) return null;
                const delta = raw - base;
                if (delta & ((1 << stride_shift) - 1) != 0) return null;
                const offset = delta >> stride_shift;
                if (offset >> index_bits > self.generationMask()) return null;
                break :blk offset;
            };
            return .{
                .index = @intCast(offset & (max_slots - 1)),
                .generation = offset >> index_bits,
            };
        }

        pub fn slotFor(self: *Self, index: u32) ?*Slot {
            const chunk_index = index / chunk_len;
            if (chunk_index >= max_chunks) return null;
            const chunk = self.chunks[chunk_index].load(.acquire) orelse return null;
            return &chunk[index % chunk_len];
        }

        /// Publishes `handle` and returns the id callers hold.
        pub fn register(self: *Self, handle: *T) !*anyopaque {
            antfly.platform_sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            try self.ensureIdSpace();
            const index = if (self.free_head) |free| blk: {
                self.free_head = if (self.slotFor(free).?.next_free == 0) null else self.slotFor(free).?.next_free - 1;
                break :blk free;
            } else blk: {
                const index = self.slot_count;
                const chunk_index = index / chunk_len;
                if (chunk_index >= max_chunks) return error.OutOfMemory;
                if (self.chunks[chunk_index].load(.acquire) == null) {
                    const chunk = try std.heap.page_allocator.create([chunk_len]Slot);
                    chunk.* = @splat(.{});
                    self.chunks[chunk_index].store(chunk, .release);
                }
                self.slot_count += 1;
                break :blk index;
            };
            const slot = self.slotFor(index).?;
            slot.handle.store(handle, .release);
            const generation = slot.state.load(.acquire) >> 1;
            return self.encode(.{ .index = index, .generation = generation });
        }

        /// Claims the slot for close. Returns the handle to free, or null when
        /// the id is stale or another close already claimed it. Waits for every
        /// call that entered (or is backing out) to leave first.
        pub fn beginClose(self: *Self, ptr: ?*anyopaque) ?struct { *T, Id } {
            const id = self.decode(ptr) orelse return null;
            const slot = self.slotFor(id.index) orelse return null;
            if (slot.state.cmpxchgStrong(id.generation << 1, id.generation << 1 | 1, .seq_cst, .seq_cst) != null) return null;
            // A call that counted itself before the closing bit was set may still
            // be queued behind a handle lock, so poll rather than taking the lock:
            // yield first, then back off so a long search does not spin a core.
            var spins: u32 = 0;
            while (slot.active.load(.seq_cst) != 0) : (spins +|= 1) {
                if (spins < 64) {
                    @import("antfly_platform").time.yieldNow();
                } else {
                    handleLockIo().sleep(.fromMicroseconds(500), .awake) catch {};
                }
            }
            const handle = slot.handle.swap(null, .acq_rel) orelse return null;
            return .{ handle, id };
        }

        /// Counts a call into the handle `ptr` names, so `beginClose` waits
        /// for it. Returns null for a stale, closing, or foreign id. Pair
        /// with `leave(slot)`.
        pub fn enter(self: *Self, ptr: ?*anyopaque) ?struct { *T, *Slot } {
            const id = self.decode(ptr) orelse return null;
            const slot = self.slotFor(id.index) orelse return null;
            // Count the call before checking the slot state so close either
            // sees this call and waits for it, or this call sees closing (or a
            // newer generation) and backs out. Each side stores then loads the
            // other's variable, so both need seq_cst: weaker orderings let both
            // loads miss both stores. The slot itself is never freed, so this
            // is safe even if the handle was closed before we got here.
            _ = slot.active.fetchAdd(1, .seq_cst);
            if (slot.state.load(.seq_cst) != id.generation << 1) {
                _ = slot.active.fetchSub(1, .release);
                return null;
            }
            const handle = slot.handle.load(.acquire) orelse {
                _ = slot.active.fetchSub(1, .release);
                return null;
            };
            return .{ handle, slot };
        }

        pub fn leave(slot: *Slot) void {
            _ = slot.active.fetchSub(1, .release);
        }

        /// Releases a slot claimed by `beginClose` after its handle is freed.
        pub fn finishClose(self: *Self, id: Id) void {
            const slot = self.slotFor(id.index).?;
            antfly.platform_sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            if (id.generation >= self.generationMask()) {
                // The slot has used every generation an id can encode. Wrapping
                // would let an old id match a future handle, so retire it: the
                // state stays claimed (closing bit set), which no id can enter or
                // close, and the slot never returns to the free list.
                return;
            }
            slot.state.store((id.generation + 1) << 1, .release);
            slot.next_free = if (self.free_head) |free| free + 1 else 0;
            self.free_head = id.index;
        }
    };
}

pub const HandleRegistry = HandleRegistryOf(Handle);

pub var handle_registry: HandleRegistry = .{};

/// Closes a handle id: rejects new calls, drains entered ones, frees it.
/// Safe for stale ids and concurrent or repeated closes.
pub fn closeHandleId(ptr: ?*anyopaque) void {
    const handle, const id = handle_registry.beginClose(ptr) orelse return;
    closeHandle(handle);
    handle_registry.finishClose(id);
}

/// The handle locks are called from arbitrary foreign threads, so they use
/// the process-wide threaded Io, whose waits block the calling OS thread.
pub fn handleLockIo() std.Io {
    return std.Options.debug_io;
}

pub fn dupBytes(bytes: []const u8) !capi.Buffer {
    if (bytes.len == 0) return .{};
    const out = try std.heap.c_allocator.alloc(u8, bytes.len);
    @memcpy(out, bytes);
    return .{
        .ptr = out.ptr,
        .len = out.len,
    };
}

pub const JsonSearchAggregationRequest = struct {
    name: []const u8,
    type: []const u8,
    field: []const u8,
    size: i64 = 0,
    interval: f64 = 0,
    calendar_interval: []const u8 = "",
    fixed_interval: []const u8 = "",
    min_doc_count: i64 = 0,
    significance_algorithm: []const u8 = "",
    background_query_type: []const u8 = "",
    background_field: []const u8 = "",
    background_text: []const u8 = "",
    bucket_path: []const u8 = "",
    sort_order: []const u8 = "",
    from: i64 = 0,
    window: i64 = 0,
    gap_policy: []const u8 = "",
    term_prefix: []const u8 = "",
    term_pattern: []const u8 = "",
    ranges: []const JsonNumericRangeRequest = &.{},
    date_ranges: []const JsonDateRangeRequest = &.{},
    distance_ranges: []const JsonDistanceRangeRequest = &.{},
    center_lat: f64 = 0,
    center_lon: f64 = 0,
    distance_unit: []const u8 = "",
    geohash_precision: u8 = 0,
    aggregations: []const JsonSearchAggregationRequest = &.{},
};

pub const JsonNumericRangeRequest = struct {
    name: []const u8 = "",
    start: ?f64 = null,
    end: ?f64 = null,
};

pub const JsonDateRangeRequest = struct {
    name: []const u8 = "",
    start: ?[]const u8 = null,
    end: ?[]const u8 = null,
};

pub const JsonDistanceRangeRequest = struct {
    name: []const u8 = "",
    from: ?f64 = null,
    to: ?f64 = null,
};

pub const JsonSearchAggregationBucket = struct {
    key_json: []const u8,
    count: i64,
    score: ?f64 = null,
    bg_count: ?i64 = null,
    aggregations: []JsonSearchAggregationResult = &.{},

    pub fn deinit(self: *JsonSearchAggregationBucket, alloc: Allocator) void {
        alloc.free(self.key_json);
        for (self.aggregations) |*agg| agg.deinit(alloc);
        if (self.aggregations.len > 0) alloc.free(self.aggregations);
        self.* = undefined;
    }
};

pub const JsonSearchAggregationResult = struct {
    name: []const u8,
    field: []const u8,
    type: []const u8,
    value_json: ?[]const u8 = null,
    metadata_json: ?[]const u8 = null,
    buckets: []JsonSearchAggregationBucket = &.{},

    pub fn deinit(self: *JsonSearchAggregationResult, alloc: Allocator) void {
        if (self.value_json) |value_json| alloc.free(value_json);
        if (self.metadata_json) |metadata_json| alloc.free(metadata_json);
        for (self.buckets) |*bucket| bucket.deinit(alloc);
        if (self.buckets.len > 0) alloc.free(self.buckets);
        self.* = undefined;
    }
};

pub fn freeAggregationRequests(alloc: Allocator, requests: []const aggregations_mod.SearchAggregationRequest) void {
    for (requests) |request| {
        if (request.ranges.len > 0) alloc.free(request.ranges);
        if (request.date_ranges.len > 0) alloc.free(request.date_ranges);
        if (request.distance_ranges.len > 0) alloc.free(request.distance_ranges);
        freeAggregationRequests(alloc, request.aggregations);
    }
    if (requests.len > 0) alloc.free(requests);
}

pub fn freeRawBuffer(ptr: ?[*]u8, len: usize) void {
    if (ptr == null or len == 0) return;
    std.heap.c_allocator.free(ptr.?[0..len]);
}

pub fn computeSearchAggregations(
    alloc: Allocator,
    requests: []const JsonSearchAggregationRequest,
    result: db_mod.types.SearchResult,
) anyerror![]JsonSearchAggregationResult {
    var out = try alloc.alloc(JsonSearchAggregationResult, requests.len);
    errdefer alloc.free(out);

    for (requests, 0..) |request, i| {
        out[i] = try computeSingleAggregation(alloc, request, result.hits);
    }
    return out;
}

pub fn computeSingleAggregation(
    alloc: Allocator,
    request: JsonSearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
) anyerror!JsonSearchAggregationResult {
    if (std.mem.eql(u8, request.type, "count")) {
        return .{
            .name = request.name,
            .field = request.field,
            .type = request.type,
            .value_json = try std.fmt.allocPrint(alloc, "{d}", .{hits.len}),
        };
    }
    if (std.mem.eql(u8, request.type, "sum")) return try computeNumericMetricAggregation(alloc, request, hits, .sum);
    if (std.mem.eql(u8, request.type, "min")) return try computeNumericMetricAggregation(alloc, request, hits, .min);
    if (std.mem.eql(u8, request.type, "max")) return try computeNumericMetricAggregation(alloc, request, hits, .max);
    if (std.mem.eql(u8, request.type, "avg")) return try computeNumericMetricAggregation(alloc, request, hits, .avg);
    if (std.mem.eql(u8, request.type, "stats")) return try computeNumericMetricAggregation(alloc, request, hits, .stats);
    if (std.mem.eql(u8, request.type, "cardinality")) return try computeCardinalityAggregation(alloc, request, hits);
    if (std.mem.eql(u8, request.type, "terms")) return try computeTermsAggregation(alloc, request, hits);
    if (std.mem.eql(u8, request.type, "histogram")) return try computeHistogramAggregation(alloc, request, hits);
    if (std.mem.eql(u8, request.type, "date_histogram")) return try computeDateHistogramAggregation(alloc, request, hits);
    if (std.mem.eql(u8, request.type, "range")) return try computeRangeAggregation(alloc, request, hits);
    return error.UnsupportedAggregation;
}

pub const NumericMetricKind = enum { sum, min, max, avg, stats };

pub fn computeNumericMetricAggregation(
    alloc: Allocator,
    request: JsonSearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
    kind: NumericMetricKind,
) anyerror!JsonSearchAggregationResult {
    if (request.field.len == 0) return error.InvalidAggregation;

    var sum: f64 = 0;
    var sum_squares: f64 = 0;
    var count: i64 = 0;
    var min_value: f64 = std.math.inf(f64);
    var max_value: f64 = -std.math.inf(f64);

    for (hits) |hit| {
        const stored = hit.stored_data orelse continue;
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, stored, .{}) catch continue;
        defer parsed.deinit();
        const value = extractValueAtPath(parsed.value, request.field) orelse continue;
        accumulateNumericJsonValue(value, &sum, &sum_squares, &count, &min_value, &max_value);
    }

    const value_json = switch (kind) {
        .sum => try std.fmt.allocPrint(alloc, "{d}", .{sum}),
        .min => if (count == 0) try alloc.dupe(u8, "null") else try std.fmt.allocPrint(alloc, "{d}", .{min_value}),
        .max => if (count == 0) try alloc.dupe(u8, "null") else try std.fmt.allocPrint(alloc, "{d}", .{max_value}),
        .avg => if (count == 0)
            try alloc.dupe(u8, "{\"count\":0,\"sum\":0,\"avg\":0}")
        else
            try std.fmt.allocPrint(alloc, "{{\"count\":{d},\"sum\":{d},\"avg\":{d}}}", .{ count, sum, sum / @as(f64, @floatFromInt(count)) }),
        .stats => blk: {
            if (count == 0) break :blk try alloc.dupe(u8, "{\"count\":0,\"sum\":0,\"avg\":0,\"min\":null,\"max\":null,\"sum_squares\":0,\"variance\":0,\"std_dev\":0}");
            const avg = sum / @as(f64, @floatFromInt(count));
            const variance = (sum_squares / @as(f64, @floatFromInt(count))) - (avg * avg);
            const non_negative_variance = if (variance < 0) 0 else variance;
            break :blk try std.fmt.allocPrint(
                alloc,
                "{{\"count\":{d},\"sum\":{d},\"avg\":{d},\"min\":{d},\"max\":{d},\"sum_squares\":{d},\"variance\":{d},\"std_dev\":{d}}}",
                .{ count, sum, avg, min_value, max_value, sum_squares, non_negative_variance, @sqrt(non_negative_variance) },
            );
        },
    };
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .value_json = value_json,
    };
}

pub fn computeCardinalityAggregation(
    alloc: Allocator,
    request: JsonSearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
) anyerror!JsonSearchAggregationResult {
    if (request.field.len == 0) return error.InvalidAggregation;

    var seen = std.StringHashMap(void).init(alloc);
    defer {
        var it = seen.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        seen.deinit();
    }

    for (hits) |hit| {
        const stored = hit.stored_data orelse continue;
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, stored, .{}) catch continue;
        defer parsed.deinit();
        const value = extractValueAtPath(parsed.value, request.field) orelse continue;
        try collectCardinalityValues(alloc, &seen, value);
    }

    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .value_json = try std.fmt.allocPrint(alloc, "{{\"value\":{d}}}", .{seen.count()}),
    };
}

pub fn computeTermsAggregation(
    alloc: Allocator,
    request: JsonSearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
) anyerror!JsonSearchAggregationResult {
    if (request.field.len == 0) return error.InvalidAggregation;

    var counts = std.StringHashMap(i64).init(alloc);
    defer {
        var it = counts.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        counts.deinit();
    }
    var grouped = std.StringHashMap(std.ArrayListUnmanaged(db_mod.types.SearchHit)).init(alloc);
    defer {
        var it = grouped.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit(alloc);
        grouped.deinit();
    }

    if (request.term_pattern.len > 0) return error.UnsupportedAggregation;

    for (hits) |hit| {
        const stored = hit.stored_data orelse continue;
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, stored, .{}) catch continue;
        defer parsed.deinit();
        const value = extractValueAtPath(parsed.value, request.field) orelse continue;
        try appendTermAggregationValuesZig(alloc, &counts, &grouped, hit, value);
    }

    var entries = std.ArrayList(struct { key: []const u8, count: i64 }).empty;
    defer entries.deinit(alloc);
    var it = counts.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const count = entry.value_ptr.*;
        if (request.term_prefix.len > 0 and !std.mem.startsWith(u8, key, request.term_prefix)) continue;
        if (request.min_doc_count > 0 and count < request.min_doc_count) continue;
        try entries.append(alloc, .{ .key = key, .count = count });
    }
    std.mem.sort(@TypeOf(entries.items[0]), entries.items, {}, struct {
        pub fn lessThan(_: void, lhs: @TypeOf(entries.items[0]), rhs: @TypeOf(entries.items[0])) bool {
            if (lhs.count == rhs.count) return std.mem.order(u8, lhs.key, rhs.key) == .lt;
            return lhs.count > rhs.count;
        }
    }.lessThan);

    const limit: usize = if (request.size > 0 and @as(usize, @intCast(request.size)) < entries.items.len) @intCast(request.size) else entries.items.len;
    var buckets = try alloc.alloc(JsonSearchAggregationBucket, limit);
    errdefer {
        for (buckets) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (entries.items[0..limit], 0..) |entry, idx| {
        const grouped_hits = grouped.get(entry.key).?.items;
        const nested = blk: {
            if (request.aggregations.len == 0) break :blk try alloc.alloc(JsonSearchAggregationResult, 0);
            break :blk try computeSearchAggregations(alloc, request.aggregations, .{
                .alloc = alloc,
                .hits = grouped_hits,
                .total_hits = @intCast(grouped_hits.len),
            });
        };
        buckets[idx] = .{
            .key_json = try std.fmt.allocPrint(alloc, "\"{s}\"", .{entry.key}),
            .count = entry.count,
            .aggregations = nested,
        };
    }
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

pub fn computeHistogramAggregation(
    alloc: Allocator,
    request: JsonSearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
) anyerror!JsonSearchAggregationResult {
    if (request.field.len == 0 or request.interval <= 0) return error.InvalidAggregation;

    var bucket_counts = std.AutoHashMap(i64, i64).init(alloc);
    defer bucket_counts.deinit();
    var grouped = std.AutoHashMap(i64, std.ArrayListUnmanaged(db_mod.types.SearchHit)).init(alloc);
    defer {
        var it_grouped = grouped.iterator();
        while (it_grouped.next()) |entry| entry.value_ptr.deinit(alloc);
        grouped.deinit();
    }

    for (hits) |hit| {
        const stored = hit.stored_data orelse continue;
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, stored, .{}) catch continue;
        defer parsed.deinit();
        const value = extractValueAtPath(parsed.value, request.field) orelse continue;
        if (jsonValueToF64(value)) |numeric| {
            const bucket_index = @as(i64, @intFromFloat(@floor(numeric / request.interval)));
            const entry = try bucket_counts.getOrPut(bucket_index);
            if (entry.found_existing) entry.value_ptr.* += 1 else entry.value_ptr.* = 1;
            const grouped_entry = try grouped.getOrPut(bucket_index);
            if (!grouped_entry.found_existing) grouped_entry.value_ptr.* = .empty;
            try grouped_entry.value_ptr.append(alloc, hit);
        }
    }

    var present_keys = try alloc.alloc(i64, bucket_counts.count());
    defer if (present_keys.len > 0) alloc.free(present_keys);
    var iter = bucket_counts.iterator();
    var present_count: usize = 0;
    while (iter.next()) |entry| {
        if (request.min_doc_count > 0 and entry.value_ptr.* < request.min_doc_count) continue;
        present_keys[present_count] = entry.key_ptr.*;
        present_count += 1;
    }
    std.mem.sort(i64, present_keys[0..present_count], {}, struct {
        pub fn lessThan(_: void, lhs: i64, rhs: i64) bool {
            return lhs < rhs;
        }
    }.lessThan);

    const keys = if (request.min_doc_count == 0 and present_count > 0)
        try fillHistogramBucketKeys(alloc, present_keys[0], present_keys[present_count - 1])
    else
        try alloc.dupe(i64, present_keys[0..present_count]);
    defer if (keys.len > 0) alloc.free(keys);

    var buckets = try alloc.alloc(JsonSearchAggregationBucket, keys.len);
    errdefer {
        for (buckets[0..keys.len]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (keys, 0..) |bucket_index, i| {
        const nested = blk: {
            if (request.aggregations.len == 0) break :blk try alloc.alloc(JsonSearchAggregationResult, 0);
            if (grouped.get(bucket_index)) |list| {
                break :blk try computeSearchAggregations(alloc, request.aggregations, .{
                    .alloc = alloc,
                    .hits = list.items,
                    .total_hits = @intCast(list.items.len),
                });
            }
            break :blk try alloc.alloc(JsonSearchAggregationResult, 0);
        };
        buckets[i] = .{
            .key_json = try std.fmt.allocPrint(alloc, "{d}", .{@as(f64, @floatFromInt(bucket_index)) * request.interval}),
            .count = bucket_counts.get(bucket_index) orelse 0,
            .aggregations = nested,
        };
    }
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

pub fn computeDateHistogramAggregation(
    alloc: Allocator,
    request: JsonSearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
) anyerror!JsonSearchAggregationResult {
    if (request.field.len == 0) return error.InvalidAggregation;
    const interval = try parseDateInterval(request);
    var agg = search_agg_mod.DateHistogramAgg.init(alloc, interval);
    defer agg.deinit();
    var grouped = std.AutoHashMap(u64, std.ArrayListUnmanaged(db_mod.types.SearchHit)).init(alloc);
    defer {
        var it_grouped = grouped.iterator();
        while (it_grouped.next()) |entry| entry.value_ptr.deinit(alloc);
        grouped.deinit();
    }

    for (hits) |hit| {
        const stored = hit.stored_data orelse continue;
        const value = extractTimestampFieldFromStoredJson(alloc, stored, request.field) catch null;
        if (value) |ns| {
            try agg.collect(ns);
            const bucket_key = search_agg_mod.truncateToInterval(ns, interval);
            const entry = try grouped.getOrPut(bucket_key);
            if (!entry.found_existing) entry.value_ptr.* = .empty;
            try entry.value_ptr.append(alloc, hit);
        }
    }

    const present_keys = try agg.sortedKeys(alloc);
    defer if (present_keys.len > 0) alloc.free(present_keys);

    var kept: usize = 0;
    for (present_keys) |key| {
        const count = agg.getCount(key);
        if (request.min_doc_count > 0 and count < @as(u64, @intCast(request.min_doc_count))) continue;
        kept += 1;
    }

    const keys = if (request.min_doc_count == 0 and kept > 0)
        try fillDateHistogramBucketKeys(alloc, present_keys[0], present_keys[present_keys.len - 1], interval)
    else blk: {
        var filtered = try alloc.alloc(u64, kept);
        var idx: usize = 0;
        for (present_keys) |key| {
            const count = agg.getCount(key);
            if (request.min_doc_count > 0 and count < @as(u64, @intCast(request.min_doc_count))) continue;
            filtered[idx] = key;
            idx += 1;
        }
        break :blk filtered;
    };
    defer if (keys.len > 0) alloc.free(keys);

    var buckets = try alloc.alloc(JsonSearchAggregationBucket, keys.len);
    errdefer {
        for (buckets[0..keys.len]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (keys, 0..) |key, idx| {
        const formatted = try formatRfc3339Bucket(alloc, key);
        defer alloc.free(formatted);
        const nested = blk: {
            if (request.aggregations.len == 0) break :blk try alloc.alloc(JsonSearchAggregationResult, 0);
            if (grouped.get(key)) |list| {
                break :blk try computeSearchAggregations(alloc, request.aggregations, .{
                    .alloc = alloc,
                    .hits = list.items,
                    .total_hits = @intCast(list.items.len),
                });
            }
            break :blk try alloc.alloc(JsonSearchAggregationResult, 0);
        };
        buckets[idx] = .{
            .key_json = try std.fmt.allocPrint(alloc, "\"{s}\"", .{formatted}),
            .count = @intCast(agg.getCount(key)),
            .aggregations = nested,
        };
    }

    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

pub fn computeRangeAggregation(
    alloc: Allocator,
    request: JsonSearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
) anyerror!JsonSearchAggregationResult {
    if (request.field.len == 0) return error.InvalidAggregation;
    const has_numeric = request.ranges.len > 0;
    const has_date = request.date_ranges.len > 0;
    const has_distance = request.distance_ranges.len > 0;
    if ((@intFromBool(has_numeric) + @intFromBool(has_date) + @intFromBool(has_distance)) != 1) return error.InvalidAggregation;

    if (has_numeric) {
        var buckets = try alloc.alloc(JsonSearchAggregationBucket, request.ranges.len);
        errdefer {
            for (buckets) |*bucket| bucket.deinit(alloc);
            alloc.free(buckets);
        }
        for (request.ranges, 0..) |range_spec, idx| {
            var count: i64 = 0;
            var matched = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
            defer matched.deinit(alloc);
            for (hits) |hit| {
                const stored = hit.stored_data orelse continue;
                const value = extractNumericFieldFromStoredJson(alloc, stored, request.field) catch null;
                if (value) |numeric| {
                    if (matchesNumericRangeValue(numeric, range_spec)) {
                        count += 1;
                        try matched.append(alloc, hit);
                    }
                }
            }
            const nested = if (request.aggregations.len > 0) try computeSearchAggregations(alloc, request.aggregations, .{
                .alloc = alloc,
                .hits = matched.items,
                .total_hits = @intCast(matched.items.len),
            }) else try alloc.alloc(JsonSearchAggregationResult, 0);
            buckets[idx] = .{
                .key_json = try std.fmt.allocPrint(alloc, "\"{s}\"", .{range_spec.name}),
                .count = count,
                .aggregations = nested,
            };
        }
        return .{
            .name = request.name,
            .field = request.field,
            .type = request.type,
            .buckets = buckets,
        };
    }

    if (has_date) {
        var buckets = try alloc.alloc(JsonSearchAggregationBucket, request.date_ranges.len);
        errdefer {
            for (buckets) |*bucket| bucket.deinit(alloc);
            alloc.free(buckets);
        }
        for (request.date_ranges, 0..) |range_spec, idx| {
            var count: i64 = 0;
            var matched = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
            defer matched.deinit(alloc);
            const start_ns = if (range_spec.start) |start| try parseRfc3339ToNs(start) else null;
            const end_ns = if (range_spec.end) |end| try parseRfc3339ToNs(end) else null;
            for (hits) |hit| {
                const stored = hit.stored_data orelse continue;
                const value = extractTimestampFieldFromStoredJson(alloc, stored, request.field) catch null;
                if (value) |timestamp| {
                    if (matchesDateRangeValue(timestamp, start_ns, end_ns)) {
                        count += 1;
                        try matched.append(alloc, hit);
                    }
                }
            }
            const nested = if (request.aggregations.len > 0) try computeSearchAggregations(alloc, request.aggregations, .{
                .alloc = alloc,
                .hits = matched.items,
                .total_hits = @intCast(matched.items.len),
            }) else try alloc.alloc(JsonSearchAggregationResult, 0);
            buckets[idx] = .{
                .key_json = try std.fmt.allocPrint(alloc, "\"{s}\"", .{range_spec.name}),
                .count = count,
                .aggregations = nested,
            };
        }
        return .{
            .name = request.name,
            .field = request.field,
            .type = request.type,
            .buckets = buckets,
        };
    }

    var bands = try alloc.alloc(search_agg_mod.GeoDistanceRange, request.distance_ranges.len);
    defer alloc.free(bands);
    for (request.distance_ranges, 0..) |range_spec, idx| {
        bands[idx] = .{
            .from = if (range_spec.from) |from| try distanceToMeters(from, request.distance_unit) else null,
            .to = if (range_spec.to) |to| try distanceToMeters(to, request.distance_unit) else null,
        };
    }

    var agg = try search_agg_mod.GeoDistanceAgg.init(alloc, .{
        .lat = request.center_lat,
        .lon = request.center_lon,
    }, bands);
    defer agg.deinit();

    for (hits) |hit| {
        const stored = hit.stored_data orelse continue;
        const point = extractGeoPointFieldFromStoredJson(alloc, stored, request.field) catch null;
        if (point) |geo_point| {
            agg.collect(geo_point);
        }
    }

    var buckets = try alloc.alloc(JsonSearchAggregationBucket, request.distance_ranges.len);
    errdefer {
        for (buckets) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (request.distance_ranges, 0..) |range_spec, idx| {
        var matched = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
        defer matched.deinit(alloc);
        const from_meters = if (range_spec.from) |from| try distanceToMeters(from, request.distance_unit) else null;
        const to_meters = if (range_spec.to) |to| try distanceToMeters(to, request.distance_unit) else null;
        for (hits) |hit| {
            const stored = hit.stored_data orelse continue;
            const point = extractGeoPointFieldFromStoredJson(alloc, stored, request.field) catch null;
            if (point) |geo_point| {
                const dist = geo_mod.haversineDistance(.{ .lat = request.center_lat, .lon = request.center_lon }, geo_point);
                if (matchesGeoDistanceValue(dist, from_meters, to_meters)) try matched.append(alloc, hit);
            }
        }
        const nested = if (request.aggregations.len > 0) try computeSearchAggregations(alloc, request.aggregations, .{
            .alloc = alloc,
            .hits = matched.items,
            .total_hits = @intCast(matched.items.len),
        }) else try alloc.alloc(JsonSearchAggregationResult, 0);
        buckets[idx] = .{
            .key_json = try std.fmt.allocPrint(alloc, "\"{s}\"", .{range_spec.name}),
            .count = @intCast(agg.bands[idx].count),
            .aggregations = nested,
        };
    }
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

pub fn matchesNumericRangeValue(value: f64, range_spec: JsonNumericRangeRequest) bool {
    if (range_spec.start) |start| {
        if (value < start) return false;
    }
    if (range_spec.end) |end| {
        if (value >= end) return false;
    }
    return true;
}

pub fn matchesDateRangeValue(value: u64, start_ns: ?u64, end_ns: ?u64) bool {
    if (start_ns) |start| {
        if (value < start) return false;
    }
    if (end_ns) |end| {
        if (value >= end) return false;
    }
    return true;
}

pub fn matchesGeoDistanceValue(value_meters: f64, from_meters: ?f64, to_meters: ?f64) bool {
    if (from_meters) |from| {
        if (value_meters < from) return false;
    }
    if (to_meters) |to| {
        if (value_meters >= to) return false;
    }
    return true;
}

pub fn accumulateNumericJsonValue(
    value: std.json.Value,
    sum: *f64,
    sum_squares: *f64,
    count: *i64,
    min_value: *f64,
    max_value: *f64,
) void {
    switch (value) {
        .array => |arr| for (arr.items) |item| {
            accumulateNumericJsonValue(item, sum, sum_squares, count, min_value, max_value);
        },
        else => if (jsonValueToF64(value)) |numeric| {
            sum.* += numeric;
            sum_squares.* += numeric * numeric;
            count.* += 1;
            if (numeric < min_value.*) min_value.* = numeric;
            if (numeric > max_value.*) max_value.* = numeric;
        },
    }
}

pub fn collectCardinalityValues(alloc: Allocator, seen: *std.StringHashMap(void), value: std.json.Value) !void {
    switch (value) {
        .array => |arr| {
            for (arr.items) |item| try collectCardinalityValues(alloc, seen, item);
        },
        else => {
            const key = try stringifyJsonValueCompact(alloc, value);
            errdefer alloc.free(key);
            const entry = try seen.getOrPut(key);
            if (entry.found_existing) {
                alloc.free(key);
            } else {
                entry.key_ptr.* = key;
                entry.value_ptr.* = {};
            }
        },
    }
}

pub fn appendTermAggregationValuesZig(
    alloc: Allocator,
    counts: *std.StringHashMap(i64),
    grouped: *std.StringHashMap(std.ArrayListUnmanaged(db_mod.types.SearchHit)),
    hit: db_mod.types.SearchHit,
    value: std.json.Value,
) !void {
    switch (value) {
        .array => |arr| {
            for (arr.items) |item| try appendTermAggregationValuesZig(alloc, counts, grouped, hit, item);
        },
        else => {
            const key = try jsonValueToTermKey(alloc, value);
            defer alloc.free(key);

            const count_entry = try counts.getOrPut(key);
            if (count_entry.found_existing) {
                count_entry.value_ptr.* += 1;
            } else {
                count_entry.key_ptr.* = try alloc.dupe(u8, key);
                count_entry.value_ptr.* = 1;
            }

            const group_entry = try grouped.getOrPut(count_entry.key_ptr.*);
            if (!group_entry.found_existing) group_entry.value_ptr.* = .empty;
            try group_entry.value_ptr.append(alloc, hit);
        },
    }
}

pub fn jsonValueToTermKey(alloc: Allocator, value: std.json.Value) ![]u8 {
    return switch (value) {
        .string => try alloc.dupe(u8, value.string),
        .bool => if (value.bool) try alloc.dupe(u8, "true") else try alloc.dupe(u8, "false"),
        .integer => try std.fmt.allocPrint(alloc, "{d}", .{value.integer}),
        .float => try std.fmt.allocPrint(alloc, "{d}", .{value.float}),
        .number_string => try alloc.dupe(u8, value.number_string),
        .null => try alloc.dupe(u8, "null"),
        else => try stringifyJsonValueCompact(alloc, value),
    };
}

pub fn stringifyJsonValueCompact(alloc: Allocator, value: std.json.Value) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
}

pub fn distanceToMeters(value: f64, unit: []const u8) !f64 {
    if (unit.len == 0 or std.mem.eql(u8, unit, "m") or std.mem.eql(u8, unit, "meter") or std.mem.eql(u8, unit, "meters")) {
        return value;
    }
    if (std.mem.eql(u8, unit, "km") or std.mem.eql(u8, unit, "kilometer") or std.mem.eql(u8, unit, "kilometers")) {
        return value * 1000.0;
    }
    if (std.mem.eql(u8, unit, "mi") or std.mem.eql(u8, unit, "mile") or std.mem.eql(u8, unit, "miles")) {
        return value * 1609.344;
    }
    if (std.mem.eql(u8, unit, "ft") or std.mem.eql(u8, unit, "foot") or std.mem.eql(u8, unit, "feet")) {
        return value * 0.3048;
    }
    return error.UnsupportedAggregation;
}

pub fn extractGeoPointFieldFromStoredJson(alloc: Allocator, raw_json: []const u8, field_path: []const u8) !?geo_mod.GeoPoint {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw_json, .{});
    defer parsed.deinit();

    const value = extractValueAtPath(parsed.value, field_path) orelse return null;
    return switch (value) {
        .object => |obj| blk: {
            const lat_value = obj.get("lat") orelse break :blk null;
            const lon_value = obj.get("lon") orelse break :blk null;
            const lat = jsonValueToF64(lat_value) orelse break :blk null;
            const lon = jsonValueToF64(lon_value) orelse break :blk null;
            break :blk .{ .lat = lat, .lon = lon };
        },
        else => null,
    };
}

pub fn jsonValueToF64(value: std.json.Value) ?f64 {
    return switch (value) {
        .integer => @floatFromInt(value.integer),
        .float => value.float,
        .number_string => std.fmt.parseFloat(f64, value.number_string) catch null,
        else => null,
    };
}

pub fn fillHistogramBucketKeys(alloc: Allocator, first_key: i64, last_key: i64) ![]i64 {
    if (last_key < first_key) return &.{};
    const len: usize = @intCast(last_key - first_key + 1);
    const keys = try alloc.alloc(i64, len);
    for (keys, 0..) |*slot, idx| {
        slot.* = first_key + @as(i64, @intCast(idx));
    }
    return keys;
}

pub fn fillDateHistogramBucketKeys(
    alloc: Allocator,
    first_key: u64,
    last_key: u64,
    interval: search_agg_mod.DateInterval,
) ![]u64 {
    var keys: std.ArrayList(u64) = .empty;
    errdefer keys.deinit(alloc);

    var current = first_key;
    while (current <= last_key) {
        try keys.append(alloc, current);
        const next = try nextDateHistogramBucketKey(current, interval);
        if (next <= current) break;
        current = next;
    }
    return keys.toOwnedSlice(alloc);
}

pub fn nextDateHistogramBucketKey(current: u64, interval: search_agg_mod.DateInterval) !u64 {
    return switch (interval) {
        .minute => current + 60 * std.time.ns_per_s,
        .hour => current + std.time.ns_per_hour,
        .day => current + std.time.ns_per_day,
        .week => current + 7 * std.time.ns_per_day,
        .month => try addCalendarMonths(current, 1),
        .year => try addCalendarYears(current, 1),
    };
}

pub fn addCalendarMonths(current: u64, delta_months: i64) !u64 {
    const total_seconds: u64 = @intCast(@divFloor(current, std.time.ns_per_s));
    const days: i64 = @intCast(@divFloor(total_seconds, 86_400));
    const civil = civilFromDays(days);
    const month_index = (civil.year * 12 + (civil.month - 1)) + delta_months;
    var year = @divFloor(month_index, 12);
    var month = @mod(month_index, 12) + 1;
    if (month <= 0) {
        month += 12;
        year -= 1;
    }
    return civilDateToBucketNs(year, month, 1);
}

pub fn addCalendarYears(current: u64, delta_years: i64) !u64 {
    const total_seconds: u64 = @intCast(@divFloor(current, std.time.ns_per_s));
    const days: i64 = @intCast(@divFloor(total_seconds, 86_400));
    const civil = civilFromDays(days);
    return civilDateToBucketNs(civil.year + delta_years, 1, 1);
}

pub fn civilDateToBucketNs(year: i64, month: i64, day: i64) !u64 {
    const days = daysFromCivil(year, month, day);
    if (days < 0) return error.InvalidAggregation;
    return @as(u64, @intCast(days)) * std.time.ns_per_day;
}

pub fn extractNumericFieldFromStoredJson(alloc: Allocator, raw_json: []const u8, field_path: []const u8) !?f64 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw_json, .{});
    defer parsed.deinit();

    const value = extractValueAtPath(parsed.value, field_path) orelse return null;
    return switch (value) {
        .integer => @floatFromInt(value.integer),
        .float => value.float,
        .number_string => std.fmt.parseFloat(f64, value.number_string) catch null,
        else => null,
    };
}

pub fn extractTimestampFieldFromStoredJson(alloc: Allocator, raw_json: []const u8, field_path: []const u8) !?u64 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw_json, .{});
    defer parsed.deinit();

    const value = extractValueAtPath(parsed.value, field_path) orelse return null;
    return switch (value) {
        .integer => @intCast(value.integer),
        .float => @intFromFloat(value.float),
        .number_string => std.fmt.parseInt(u64, value.number_string, 10) catch null,
        .string => try parseRfc3339ToNs(value.string),
        else => null,
    };
}

pub fn parseDateInterval(request: JsonSearchAggregationRequest) !search_agg_mod.DateInterval {
    const value = if (request.calendar_interval.len > 0) request.calendar_interval else request.fixed_interval;
    if (std.mem.eql(u8, value, "minute") or std.mem.eql(u8, value, "1m")) return .minute;
    if (std.mem.eql(u8, value, "hour") or std.mem.eql(u8, value, "1h")) return .hour;
    if (std.mem.eql(u8, value, "day") or std.mem.eql(u8, value, "1d")) return .day;
    if (std.mem.eql(u8, value, "week") or std.mem.eql(u8, value, "1w")) return .week;
    if (std.mem.eql(u8, value, "month")) return .month;
    if (std.mem.eql(u8, value, "year")) return .year;
    return error.UnsupportedAggregation;
}

pub fn parseRfc3339ToNs(text: []const u8) !?u64 {
    if (text.len < 20) return null;
    if (text[4] != '-' or text[7] != '-' or text[10] != 'T' or text[13] != ':' or text[16] != ':') return null;

    const year = std.fmt.parseInt(i64, text[0..4], 10) catch return null;
    const month = std.fmt.parseInt(i64, text[5..7], 10) catch return null;
    const day = std.fmt.parseInt(i64, text[8..10], 10) catch return null;
    const hour = std.fmt.parseInt(i64, text[11..13], 10) catch return null;
    const minute = std.fmt.parseInt(i64, text[14..16], 10) catch return null;
    const second = std.fmt.parseInt(i64, text[17..19], 10) catch return null;

    var idx: usize = 19;
    var nanos: u64 = 0;
    if (idx < text.len and text[idx] == '.') {
        idx += 1;
        const frac_start = idx;
        while (idx < text.len and text[idx] >= '0' and text[idx] <= '9') : (idx += 1) {}
        const frac = text[frac_start..idx];
        if (frac.len == 0 or frac.len > 9) return null;
        var frac_ns = std.fmt.parseInt(u64, frac, 10) catch return null;
        var scale: usize = frac.len;
        while (scale < 9) : (scale += 1) frac_ns *= 10;
        nanos = frac_ns;
    }
    if (idx >= text.len or text[idx] != 'Z' or idx + 1 != text.len) return null;

    const days = daysFromCivil(year, month, day);
    if (days < 0) return null;
    const secs = days * 86_400 + hour * 3_600 + minute * 60 + second;
    if (secs < 0) return null;
    return @as(u64, @intCast(secs)) * std.time.ns_per_s + nanos;
}

pub fn daysFromCivil(year: i64, month: i64, day: i64) i64 {
    var y = year;
    y -= if (month <= 2) @as(i64, 1) else @as(i64, 0);
    const era = @divFloor(if (y >= 0) y else y - 399, 400);
    const yoe = y - era * 400;
    const mp = month + (if (month > 2) @as(i64, -3) else @as(i64, 9));
    const doy = @divFloor(153 * mp + 2, 5) + day - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146_097 + doe - 719_468;
}

pub fn formatRfc3339Bucket(alloc: Allocator, ns: u64) ![]const u8 {
    const total_seconds: u64 = @intCast(@divFloor(ns, std.time.ns_per_s));
    const days: i64 = @intCast(@divFloor(total_seconds, 86_400));
    const secs_of_day: u64 = total_seconds % 86_400;
    const civil = civilFromDays(days);
    const hour: u64 = secs_of_day / 3_600;
    const minute: u64 = (secs_of_day % 3_600) / 60;
    const second: u64 = secs_of_day % 60;
    return try std.fmt.allocPrint(alloc, "{:0>4}-{:0>2}-{:0>2}T{:0>2}:{:0>2}:{:0>2}Z", .{
        @as(u64, @intCast(civil.year)),
        @as(u64, @intCast(civil.month)),
        @as(u64, @intCast(civil.day)),
        hour,
        minute,
        second,
    });
}

pub fn civilFromDays(days_since_epoch: i64) struct { year: i64, month: i64, day: i64 } {
    const z = days_since_epoch + 719_468;
    const era = @divFloor(if (z >= 0) z else z - 146_096, 146_097);
    const doe = z - era * 146_097;
    const yoe = @divFloor(doe - @divFloor(doe, 1_460) + @divFloor(doe, 36_524) - @divFloor(doe, 146_096), 365);
    var y = yoe + era * 400;
    const doy = doe - (365 * yoe + @divFloor(yoe, 4) - @divFloor(yoe, 100));
    const mp = @divFloor(5 * doy + 2, 153);
    const d = doy - @divFloor(153 * mp + 2, 5) + 1;
    const m = mp + (if (mp < 10) @as(i64, 3) else @as(i64, -9));
    y += if (m <= 2) @as(i64, 1) else @as(i64, 0);
    return .{ .year = y, .month = m, .day = d };
}

pub fn extractValueAtPath(root: std.json.Value, field_path: []const u8) ?std.json.Value {
    var current = root;
    var parts = std.mem.splitScalar(u8, field_path, '.');
    while (parts.next()) |part| {
        switch (current) {
            .object => |obj| {
                current = obj.get(part) orelse return null;
            },
            else => return null,
        }
    }
    return current;
}
