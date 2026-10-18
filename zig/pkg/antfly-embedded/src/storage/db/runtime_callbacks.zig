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

//! Callback contracts injected by distributed control into storage-owned
//! resolution and promotion workers. Implementations live on either side of
//! the compiled storage boundary; this module must remain implementation-free.

const std = @import("std");
const named = @import("../named_callback.zig");

pub const CandidateSource = struct {
    const NativeBoundary = named.Boundary(VTable);
    // Reverse callbacks are received in the source compilation unit, so both
    // directions translate their owning error domain before it is inspected.
    const IterationMethods = struct {
        scan: *const fn (CandidateSource, std.mem.Allocator, []const u8, []const u8, ScanOptions, Consumer) anyerror!void,
        nearest: *const fn (CandidateSource, std.mem.Allocator, []const u8, NearestQuery, Consumer) anyerror!void,
        get_many: *const fn (CandidateSource, std.mem.Allocator, []const u8, []const []const u8, Consumer) anyerror!void,
    };
    const IterationBoundary = named.Boundary(IterationMethods);

    const Consumer = struct {
        context: *anyopaque,
        callback: Consume,
        dispatch: named.Dispatch = ConsumerBoundary.local_dispatch,
        const ConsumerBoundary = named.Boundary(struct { consume: Consume });
        fn receive(raw: *anyopaque, key: []const u8, value: []const u8) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return ConsumerBoundary.call("consume", self.dispatch, self.callback, .{ self.context, key, value });
        }
    };
    fn sourceScan(source: CandidateSource, alloc: std.mem.Allocator, table: []const u8, prefix: []const u8, opts: ScanOptions, consumer: Consumer) anyerror!void {
        var receiving = consumer;
        return source.vtable.scan_prefix.?(source.ptr, alloc, table, prefix, opts, &receiving, Consumer.receive);
    }
    fn sourceNearest(source: CandidateSource, alloc: std.mem.Allocator, table: []const u8, query: NearestQuery, consumer: Consumer) anyerror!void {
        var receiving = consumer;
        return source.vtable.nearest.?(source.ptr, alloc, table, query, &receiving, Consumer.receive);
    }
    fn sourceGetMany(source: CandidateSource, alloc: std.mem.Allocator, table: []const u8, keys: []const []const u8, consumer: Consumer) anyerror!void {
        var receiving = consumer;
        return source.vtable.get_many.?(source.ptr, alloc, table, keys, &receiving, Consumer.receive);
    }

    callback_dispatch: named.Dispatch = NativeBoundary.local_dispatch,
    iteration_dispatch: named.Dispatch = IterationBoundary.local_dispatch,
    iteration_methods: IterationMethods = .{ .scan = sourceScan, .nearest = sourceNearest, .get_many = sourceGetMany },
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const Consume = *const fn (ctx: *anyopaque, entity_key: []const u8, value: []const u8) anyerror!void;
    pub const NearestQuery = struct {
        index_name: []const u8,
        embedding: []const f32,
        k: usize,
        /// Candidate record fields consumed by comparisons against the mention vector.
        /// Empty means ANN blocking only: return stored JSON without vector hydration.
        vector_fields: []const []const u8 = &.{},
        cache_vectors: bool = false,
    };
    pub const ScanOptions = struct {
        limit: usize = 0,
    };

    pub const Batch = struct {
        source: CandidateSource,
        release: ?*const fn (*anyopaque, std.mem.Allocator) void = null,
        pub fn deinit(self: Batch, alloc: std.mem.Allocator) void {
            if (self.release) |f| f(self.source.ptr, alloc);
        }
    };
    pub fn beginBatch(self: CandidateSource, alloc: std.mem.Allocator, tables: []const []const u8) !Batch {
        if (self.vtable.begin_batch) |f| return NativeBoundary.call("begin_batch", self.callback_dispatch, f, .{ self.ptr, alloc, tables });
        return .{ .source = self };
    }
    pub fn boundTable(self: CandidateSource, table: []const u8) !?[]const u8 {
        if (self.vtable.bound_table) |f| return NativeBoundary.call("bound_table", self.callback_dispatch, f, .{ self.ptr, table });
        return null;
    }
    /// Revisions must cover the complete candidate universe (including newly
    /// inserted records, table rebinding, index rebuilds and ANN visibility),
    /// and the selected embedding producer. Null disables cross-run reuse.
    pub const Revisions = struct {
        candidates: ?[32]u8 = null,
        embedding: ?[32]u8 = null,
    };
    pub const CacheBatch = struct {
        pub const Write = struct { key: []const u8, value: []const u8 };
        ptr: *anyopaque,
        writes: []const Write,
        deletes: []const []const u8,
        finish: *const fn (*anyopaque, bool) void,
    };
    pub const VTable = struct {
        get_description: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, []const u8, []const u8) anyerror!?[]u8 = null,
        adjudicate: ?*const fn (*anyopaque, std.mem.Allocator, @import("antfly_resolver").adjudication.Request) anyerror![]u8 = null,
        /// Root-source caches join the resolution transaction. Other physical
        /// stores flush one optional shared-cache batch after resolution commit.
        embedding_cache_prepare: ?*const fn (*anyopaque, std.mem.Allocator, *anyopaque) anyerror!?CacheBatch = null,
        embedding_cache_flush: ?*const fn (*anyopaque) anyerror!void = null,
        embedding_cache_get: ?*const fn (*anyopaque, std.mem.Allocator, [32]u8) anyerror!?[]const f32 = null,
        embedding_cache_put: ?*const fn (*anyopaque, [32]u8, []const f32) anyerror!void = null,
        load_vector: ?*const fn (*anyopaque, std.mem.Allocator, @import("antfly_resolver").VectorReference) anyerror!?[]const f32 = null,
        revisions: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, []const u8, []const u8) anyerror!Revisions = null,
        /// Bulk point candidates. Missing keys are omitted. Values are borrowed
        /// during consume; this optional capability must preserve point-read semantics.
        get_many: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, []const []const u8, *anyopaque, Consume) anyerror!void = null,
        /// A work-unit-owned binding; never shared between background workers.
        begin_batch: ?*const fn (*anyopaque, std.mem.Allocator, []const []const u8) anyerror!Batch = null,
        /// Borrowed immutable destination, retained through batch deinit.
        bound_table: ?*const fn (*anyopaque, []const u8) anyerror!?[]const u8 = null,
        /// Optional table-scoped embedding configuration for mention blocking.
        embed_mention: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, []const u8, []const u8, u32) anyerror![]f32 = null,
        /// Fetch the entity doc for `key` in `table` (owned bytes or null).
        get: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator, table: []const u8, key: []const u8) anyerror!?[]u8,
        /// Scan `table` for entities whose key starts with `prefix`.
        scan_prefix: ?*const fn (ptr: *anyopaque, allocator: std.mem.Allocator, table: []const u8, prefix: []const u8, opts: ScanOptions, ctx: *anyopaque, consume: Consume) anyerror!void = null,
        /// The nearest entities in `table` for a named dense index.
        nearest: ?*const fn (ptr: *anyopaque, allocator: std.mem.Allocator, table: []const u8, query: NearestQuery, ctx: *anyopaque, consume: Consume) anyerror!void = null,
    };

    pub fn getDescription(self: CandidateSource, a: std.mem.Allocator, table: []const u8, key: []const u8, artifact: []const u8) !?[]u8 {
        const method = self.vtable.get_description orelse return null;
        return NativeBoundary.call("get_description", self.callback_dispatch, method, .{ self.ptr, a, table, key, artifact });
    }

    pub fn adjudicate(self: CandidateSource, a: std.mem.Allocator, request: @import("antfly_resolver").adjudication.Request) ![]u8 {
        const method = self.vtable.adjudicate orelse return error.MissingAdjudicator;
        return NativeBoundary.call("adjudicate", self.callback_dispatch, method, .{ self.ptr, a, request });
    }

    pub fn get(self: CandidateSource, allocator: std.mem.Allocator, table: []const u8, key: []const u8) anyerror!?[]u8 {
        return NativeBoundary.call("get", self.callback_dispatch, self.vtable.get, .{ self.ptr, allocator, table, key });
    }
    pub fn scanPrefix(self: CandidateSource, allocator: std.mem.Allocator, table: []const u8, prefix: []const u8, opts: ScanOptions, ctx: *anyopaque, consume: Consume) anyerror!void {
        if (self.vtable.scan_prefix == null) return error.ScanUnsupported;
        return IterationBoundary.call("scan", self.iteration_dispatch, self.iteration_methods.scan, .{ self, allocator, table, prefix, opts, Consumer{ .context = ctx, .callback = consume } });
    }
    pub fn nearest(self: CandidateSource, allocator: std.mem.Allocator, table: []const u8, query: NearestQuery, ctx: *anyopaque, consume: Consume) anyerror!void {
        if (self.vtable.nearest == null) return error.NearestUnsupported;
        return IterationBoundary.call("nearest", self.iteration_dispatch, self.iteration_methods.nearest, .{ self, allocator, table, query, Consumer{ .context = ctx, .callback = consume } });
    }
    pub fn getMany(self: CandidateSource, alloc: std.mem.Allocator, table: []const u8, keys: []const []const u8, ctx: *anyopaque, consume: Consume) anyerror!void {
        if (self.vtable.get_many == null) return error.ScanUnsupported;
        return IterationBoundary.call("get_many", self.iteration_dispatch, self.iteration_methods.get_many, .{ self, alloc, table, keys, Consumer{ .context = ctx, .callback = consume } });
    }
    pub fn prepareCache(self: CandidateSource, alloc: std.mem.Allocator, store: *anyopaque) anyerror!?CacheBatch {
        const method = self.vtable.embedding_cache_prepare orelse return null;
        return NativeBoundary.call("embedding_cache_prepare", self.callback_dispatch, method, .{ self.ptr, alloc, store });
    }
    pub fn flushCache(self: CandidateSource) anyerror!void {
        const method = self.vtable.embedding_cache_flush orelse return;
        return NativeBoundary.call("embedding_cache_flush", self.callback_dispatch, method, .{self.ptr});
    }
    pub fn getCache(self: CandidateSource, alloc: std.mem.Allocator, digest: [32]u8) anyerror!?[]const f32 {
        const method = self.vtable.embedding_cache_get orelse return null;
        return NativeBoundary.call("embedding_cache_get", self.callback_dispatch, method, .{ self.ptr, alloc, digest });
    }
    pub fn putCache(self: CandidateSource, digest: [32]u8, vector: []const f32) anyerror!void {
        const method = self.vtable.embedding_cache_put orelse return;
        return NativeBoundary.call("embedding_cache_put", self.callback_dispatch, method, .{ self.ptr, digest, vector });
    }
    pub fn loadVector(self: CandidateSource, alloc: std.mem.Allocator, reference: @import("antfly_resolver").VectorReference) anyerror!?[]const f32 {
        const method = self.vtable.load_vector orelse return null;
        return NativeBoundary.call("load_vector", self.callback_dispatch, method, .{ self.ptr, alloc, reference });
    }
    pub fn readRevisions(self: CandidateSource, alloc: std.mem.Allocator, table: []const u8, index: []const u8, embedding: []const u8) anyerror!Revisions {
        const method = self.vtable.revisions orelse return .{};
        return NativeBoundary.call("revisions", self.callback_dispatch, method, .{ self.ptr, alloc, table, index, embedding });
    }
    pub fn embedMention(self: CandidateSource, alloc: std.mem.Allocator, table: []const u8, embedding: []const u8, text: []const u8, dims: u32) anyerror![]f32 {
        const method = self.vtable.embed_mention orelse return error.NearestUnsupported;
        return NativeBoundary.call("embed_mention", self.callback_dispatch, method, .{ self.ptr, alloc, table, embedding, text, dims });
    }
    /// A receiving view is synchronous. Its address and source must remain
    /// live until all stage callbacks complete; retained batches keep originals.
    pub fn receive(self: CandidateSource) Receiver {
        return .{ .source = self };
    }
    pub const Receiver = struct {
        source: CandidateSource,
        vtable: VTable = undefined,
        pub fn sourceView(self: *Receiver) CandidateSource {
            self.vtable = .{ .get = Receiver.get };
            if (self.source.vtable.get_description != null) self.vtable.get_description = Receiver.get_description;
            if (self.source.vtable.adjudicate != null) self.vtable.adjudicate = Receiver.adjudicate;
            if (self.source.vtable.bound_table != null) self.vtable.bound_table = bound_table;
            if (self.source.vtable.embed_mention != null) self.vtable.embed_mention = embed_mention;
            if (self.source.vtable.revisions != null) self.vtable.revisions = revisions;
            if (self.source.vtable.embedding_cache_get != null) self.vtable.embedding_cache_get = embedding_cache_get;
            if (self.source.vtable.embedding_cache_put != null) self.vtable.embedding_cache_put = embedding_cache_put;
            if (self.source.vtable.load_vector != null) self.vtable.load_vector = load_vector;
            if (self.source.vtable.scan_prefix != null) self.vtable.scan_prefix = scan_prefix;
            if (self.source.vtable.nearest != null) self.vtable.nearest = Receiver.nearest;
            if (self.source.vtable.get_many != null) self.vtable.get_many = get_many;
            return .{ .ptr = self, .vtable = &self.vtable };
        }
        fn original(raw: *anyopaque) CandidateSource {
            const self: *Receiver = @ptrCast(@alignCast(raw));
            return self.source;
        }
        fn get_description(raw: *anyopaque, a: std.mem.Allocator, table: []const u8, key: []const u8, artifact: []const u8) anyerror!?[]u8 {
            return original(raw).getDescription(a, table, key, artifact);
        }
        fn adjudicate(raw: *anyopaque, a: std.mem.Allocator, request: @import("antfly_resolver").adjudication.Request) anyerror![]u8 {
            return original(raw).adjudicate(a, request);
        }
        fn get(raw: *anyopaque, alloc: std.mem.Allocator, table: []const u8, key: []const u8) anyerror!?[]u8 {
            return original(raw).get(alloc, table, key);
        }
        fn bound_table(raw: *anyopaque, table: []const u8) anyerror!?[]const u8 {
            return original(raw).boundTable(table);
        }
        fn embed_mention(raw: *anyopaque, alloc: std.mem.Allocator, table: []const u8, embedding: []const u8, text: []const u8, dims: u32) anyerror![]f32 {
            return original(raw).embedMention(alloc, table, embedding, text, dims);
        }
        fn revisions(raw: *anyopaque, alloc: std.mem.Allocator, table: []const u8, index: []const u8, embedding: []const u8) anyerror!Revisions {
            return original(raw).readRevisions(alloc, table, index, embedding);
        }
        fn embedding_cache_get(raw: *anyopaque, alloc: std.mem.Allocator, digest: [32]u8) anyerror!?[]const f32 {
            return original(raw).getCache(alloc, digest);
        }
        fn embedding_cache_put(raw: *anyopaque, digest: [32]u8, vector: []const f32) anyerror!void {
            return original(raw).putCache(digest, vector);
        }
        fn load_vector(raw: *anyopaque, alloc: std.mem.Allocator, reference: @import("antfly_resolver").VectorReference) anyerror!?[]const f32 {
            return original(raw).loadVector(alloc, reference);
        }
        fn scan_prefix(raw: *anyopaque, alloc: std.mem.Allocator, table: []const u8, prefix: []const u8, opts: ScanOptions, ctx: *anyopaque, consume: Consume) anyerror!void {
            return original(raw).scanPrefix(alloc, table, prefix, opts, ctx, consume);
        }
        fn nearest(raw: *anyopaque, alloc: std.mem.Allocator, table: []const u8, query: NearestQuery, ctx: *anyopaque, consume: Consume) anyerror!void {
            return original(raw).nearest(alloc, table, query, ctx, consume);
        }
        fn get_many(raw: *anyopaque, alloc: std.mem.Allocator, table: []const u8, keys: []const []const u8, ctx: *anyopaque, consume: Consume) anyerror!void {
            return original(raw).getMany(alloc, table, keys, ctx, consume);
        }
    };
};

pub const EntityUpsert = struct {
    table: []const u8,
    storage_table: ?[]const u8 = null,
    key: []const u8,
    doc_json: []const u8 = "",
    /// Remove a prior pinned physical copy during an atomic destination move.
    delete: bool = false,
};

pub const MissingSinkPolicy = enum {
    wait,
    disabled,
};

pub const EntitySink = struct {
    const NativeBoundary = named.Boundary(VTable);
    callback_dispatch: named.Dispatch = NativeBoundary.local_dispatch,
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Upsert the entity at `key` in `table` with the canonical document
        /// `doc_json`. Must be idempotent under replay.
        upsert: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator, table: []const u8, key: []const u8, doc_json: []const u8) anyerror!void,
        /// Upsert all entities resolved from one document atomically (a single
        /// multi-participant transaction), so a document never lands a partial
        /// set of its entities. Optional: when absent, `upsertBatch` falls back
        /// to per-entity upserts.
        upsert_batch: ?*const fn (ptr: *anyopaque, allocator: std.mem.Allocator, entries: []const EntityUpsert) anyerror!void = null,
    };

    pub fn upsert(self: EntitySink, allocator: std.mem.Allocator, table: []const u8, key: []const u8, doc_json: []const u8) anyerror!void {
        return NativeBoundary.call("upsert", self.callback_dispatch, self.vtable.upsert, .{ self.ptr, allocator, table, key, doc_json });
    }

    pub fn upsertBatch(self: EntitySink, allocator: std.mem.Allocator, entries: []const EntityUpsert) anyerror!void {
        if (self.vtable.upsert_batch) |f| return NativeBoundary.call("upsert_batch", self.callback_dispatch, f, .{ self.ptr, allocator, entries });
        for (entries) |e| {
            if (e.storage_table != null or e.delete) return error.EntityPromotionAtomicCommitUnavailable;
        }
        for (entries) |e| {
            try self.upsert(allocator, e.table, e.key, e.doc_json);
        }
    }
};

pub const PromotionOwner = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        is_local_owner: *const fn (ptr: *anyopaque) bool,
    };

    pub fn isLocalOwner(self: PromotionOwner) bool {
        return self.vtable.is_local_owner(self.ptr);
    }
};

pub const DenseNativeMigrationPolicySource = struct {
    ptr: *const anyopaque,
    authority_permitted: *const fn (ptr: *const anyopaque) bool,

    pub fn authorityPermitted(self: @This()) bool {
        return self.authority_permitted(self.ptr);
    }
};

test "candidate and promotion callbacks preserve source errors and batch leases" {
    const native = @import("antfly_runtime_abi").native_abi;
    const status = @import("antfly_runtime_abi").error_abi;
    const Fixture = struct {
        visits: usize = 0,
        releases: usize = 0,
        fn get(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8) anyerror!?[]u8 {
            return error.TestWindowFailed;
        }
        fn cacheGet(_: *anyopaque, _: std.mem.Allocator, _: [32]u8) anyerror!?[]const f32 {
            return error.TestWindowFailed;
        }
        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: CandidateSource.ScanOptions, ctx: *anyopaque, consume_fn: CandidateSource.Consume) anyerror!void {
            return consume_fn(ctx, "candidate", "{}");
        }
        fn consume(raw: *anyopaque, _: []const u8, _: []const u8) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.visits += 1;
            return error.CandidateLimitReached;
        }
        fn release(raw: *anyopaque, _: std.mem.Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.releases += 1;
        }
        fn begin(raw: *anyopaque, _: std.mem.Allocator, _: []const []const u8) anyerror!CandidateSource.Batch {
            return .{ .source = source(raw), .release = release };
        }
        fn source(raw: *anyopaque) CandidateSource {
            return .{ .ptr = raw, .vtable = &.{ .get = get, .begin_batch = begin, .embedding_cache_get = cacheGet, .scan_prefix = scan }, .callback_dispatch = dispatch };
        }
        fn dispatch(contract: *const native.CallContract, callback: *const anyopaque, args: *const anyopaque, output: *anyopaque) callconv(.c) status.Status {
            return CandidateSource.NativeBoundary.local_dispatch(contract, callback, args, output);
        }
        fn reverseDispatch(contract: *const native.CallContract, callback: *const anyopaque, args: *const anyopaque, output: *anyopaque) callconv(.c) status.Status {
            return CandidateSource.Consumer.ConsumerBoundary.local_dispatch(contract, callback, args, output);
        }
        fn upsert(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8) anyerror!void {
            return error.TestWindowFailed;
        }
        fn sinkDispatch(contract: *const native.CallContract, callback: *const anyopaque, args: *const anyopaque, output: *anyopaque) callconv(.c) status.Status {
            return EntitySink.NativeBoundary.local_dispatch(contract, callback, args, output);
        }
    };
    var fixture: Fixture = .{};
    const source = Fixture.source(&fixture);
    const batch = try source.beginBatch(std.testing.allocator, &.{"entities"});
    try std.testing.expect(batch.source.ptr == source.ptr);
    try std.testing.expect(batch.source.callback_dispatch == source.callback_dispatch);
    var receiver = batch.source.receive();
    const received = receiver.sourceView();
    try std.testing.expectError(error.TestWindowFailed, received.get(std.testing.allocator, "entities", "key"));
    try std.testing.expectError(error.TestWindowFailed, received.vtable.embedding_cache_get.?(received.ptr, std.testing.allocator, @splat(0)));
    const reverse = CandidateSource.Consumer{ .context = &fixture, .callback = Fixture.consume, .dispatch = Fixture.reverseDispatch };
    try std.testing.expectError(error.CandidateLimitReached, CandidateSource.sourceScan(batch.source, std.testing.allocator, "entities", "", .{}, reverse));
    try std.testing.expectEqual(@as(usize, 1), fixture.visits);
    batch.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), fixture.releases);
    const sink: EntitySink = .{ .ptr = &fixture, .vtable = &.{ .upsert = Fixture.upsert }, .callback_dispatch = Fixture.sinkDispatch };
    try std.testing.expectError(error.TestWindowFailed, sink.upsertBatch(std.testing.allocator, &.{.{ .table = "entities", .key = "key" }}));
}
