// Copyright 2026 Antfly, Inc.
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

//! Entity-resolution stage core (see zig/RESOLUTION.md).
//!
//! This is the transform the managed resolution worker runs per changed
//! extraction artifact: given the shard's configured resolvers and the bytes of
//! a changed extraction artifact, find the resolver that consumes it, build its
//! engine from the durable catalog config, resolve the mentions, and produce
//! the resolution artifact bytes. The worker that wraps this owns the store I/O
//! (reading the extraction artifact, persisting the resolution artifact through
//! a DerivedBatch) and candidate blocking; keeping the transform separate makes
//! it pure and unit-testable.

const std = @import("std");
const resolver_lib = @import("antfly_resolver");
const resolver_catalog = @import("catalog/resolver_catalog.zig");
const internal_keys = @import("../internal_keys.zig");
const derived_types = @import("derived/derived_types.zig");
const change_journal_mod = @import("derived/change_journal.zig");
const replay_source_mod = @import("derived/replay_source.zig");
const enrichment_state = @import("enrichment/enrichment_state.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const backend_erased = @import("../backend_erased.zig");
const background_runtime_mod = @import("../background_runtime.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;

pub const ResolverConfig = resolver_catalog.ResolverConfig;

/// Appends a derived batch to the replay log, returning its sequence. Matches
/// the enrichment runtime's writer so the db wires the same callback.
pub const DerivedRecordWriter = *const fn (ptr: *anyopaque, batch: derived_types.DerivedBatch) anyerror!u64;

pub const ResolutionOutput = struct {
    /// Name of the resolution artifact to write (borrows the matched config).
    resolution_artifact: []const u8,
    /// Serialized resolution artifact; owned by the caller.
    bytes: []u8,
};

/// Returns the resolver whose `source_artifact` matches `artifact_name`, or
/// null. The first match wins; V1 expects one resolver per source artifact.
pub fn resolverForArtifact(
    resolvers: []const ResolverConfig,
    artifact_name: []const u8,
) ?*const ResolverConfig {
    for (resolvers) |*cfg| {
        if (std.mem.eql(u8, cfg.source_artifact, artifact_name)) return cfg;
    }
    return null;
}

/// Resolve a changed extraction artifact into resolution artifact bytes.
/// Returns null when no configured resolver consumes `artifact_name`.
/// `candidates` supplies blocking candidates per entity (empty = deterministic
/// minting only); the worker fills this from the entity table.
pub fn resolveExtraction(
    gpa: std.mem.Allocator,
    resolvers: []const ResolverConfig,
    artifact_name: []const u8,
    extraction_bytes: []const u8,
    candidates: []const []const resolver_lib.Candidate,
) !?ResolutionOutput {
    const cfg = resolverForArtifact(resolvers, artifact_name) orelse return null;

    var resolver = try resolver_lib.Resolver.initFromParts(
        gpa,
        cfg.table,
        cfg.key_template,
        cfg.type_must_match,
        cfg.scorer_json,
    );
    defer resolver.deinit();

    var parsed = try resolver_lib.parseExtractionEntities(gpa, extraction_bytes);
    defer parsed.deinit();

    var resolution = try resolver.resolve(gpa, cfg.config_generation, parsed.entities, candidates);
    defer resolution.deinit();

    const bytes = try resolution.toJson(gpa);
    return .{ .resolution_artifact = cfg.resolution_artifact, .bytes = bytes };
}

pub const ProcessOutcome = struct {
    result: resolver_lib.RunResult,
    /// The resolution artifact key the stage acted on; owned by the caller.
    resolution_key: []u8,
};

/// Process a changed extraction (asset) artifact key: look up the resolver that
/// consumes it and run the resolution stage to idempotently (re)persist the
/// resolution artifact through `store`. Returns null when `changed_key` is not
/// an asset artifact or no configured resolver consumes it. `provider` supplies
/// blocking candidates (null = deterministic minting only). The returned
/// `resolution_key` is owned by the caller, which journals it (on written /
/// cleared) via a `DerivedBatch` so downstream stages (graph materializer) wake.
pub fn processChangedExtraction(
    gpa: std.mem.Allocator,
    resolvers: []const ResolverConfig,
    store: resolver_lib.ArtifactStore,
    provider: ?resolver_lib.CandidateProvider,
    changed_key: []const u8,
) !?ProcessOutcome {
    const parsed = (try internal_keys.parseAssetArtifactKeyAlloc(gpa, changed_key)) orelse return null;
    defer gpa.free(parsed.doc_key);
    defer gpa.free(parsed.artifact_name);

    const cfg = resolverForArtifact(resolvers, parsed.artifact_name) orelse return null;

    var resolver = try resolver_lib.Resolver.initFromParts(
        gpa,
        cfg.table,
        cfg.key_template,
        cfg.type_must_match,
        cfg.scorer_json,
    );
    defer resolver.deinit();

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(gpa, parsed.doc_key, cfg.resolution_artifact);
    errdefer gpa.free(resolution_key);

    const stage = resolver_lib.ResolutionStage{ .resolver = &resolver, .config_generation = cfg.config_generation };
    const result = try stage.run(gpa, store, provider, changed_key, resolution_key);
    return .{ .result = result, .resolution_key = resolution_key };
}

/// Process every changed artifact key in a replay record: resolve each
/// extraction artifact and journal the resolution keys that actually changed
/// (written/cleared) in a single `DerivedBatch`, so downstream stages wake.
pub fn processRecordKeys(
    gpa: std.mem.Allocator,
    resolvers: []const ResolverConfig,
    store: resolver_lib.ArtifactStore,
    provider: ?resolver_lib.CandidateProvider,
    changed_artifact_keys: []const []const u8,
    write_ctx: *anyopaque,
    write_fn: DerivedRecordWriter,
) !void {
    var journal_keys = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (journal_keys.items) |k| gpa.free(@constCast(k));
        journal_keys.deinit(gpa);
    }

    for (changed_artifact_keys) |key| {
        const outcome = (try processChangedExtraction(gpa, resolvers, store, provider, key)) orelse continue;
        switch (outcome.result) {
            .written, .cleared => try journal_keys.append(gpa, outcome.resolution_key),
            else => gpa.free(outcome.resolution_key),
        }
    }

    if (journal_keys.items.len > 0) {
        _ = try write_fn(write_ctx, .{ .changed_artifact_keys = journal_keys.items });
    }
}

pub const default_max_records_per_window: usize = 1024;

/// Iterate replay records matching the resolution hint from `from_sequence`,
/// resolve each record's changed extraction artifacts, and journal the
/// resolution writes via `write_fn`. Returns the highest sequence processed (or
/// `from_sequence -| 1` if none), which the caller persists as applied. Pure of
/// the runtime's threading/state so it is unit-testable.
pub fn catchUpWindow(
    gpa: Allocator,
    replay_source: replay_source_mod.Source,
    resolvers: []const ResolverConfig,
    store: resolver_lib.ArtifactStore,
    provider: ?resolver_lib.CandidateProvider,
    from_sequence: u64,
    max_records: usize,
    write_ctx: *anyopaque,
    write_fn: DerivedRecordWriter,
) !u64 {
    const Ctx = struct {
        gpa: Allocator,
        resolvers: []const ResolverConfig,
        store: resolver_lib.ArtifactStore,
        provider: ?resolver_lib.CandidateProvider,
        write_ctx: *anyopaque,
        write_fn: DerivedRecordWriter,
        max_seen: u64,

        fn consume(ptr: *anyopaque, sequence: u64, payload: []const u8) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var decoded = try change_journal_mod.decodeRecord(self.gpa, payload);
            defer decoded.deinit();
            try processRecordKeys(
                self.gpa,
                self.resolvers,
                self.store,
                self.provider,
                decoded.record.changed_artifact_keys,
                self.write_ctx,
                self.write_fn,
            );
            if (sequence > self.max_seen) self.max_seen = sequence;
        }
    };

    var ctx = Ctx{
        .gpa = gpa,
        .resolvers = resolvers,
        .store = store,
        .provider = provider,
        .write_ctx = write_ctx,
        .write_fn = write_fn,
        .max_seen = from_sequence -| 1,
    };
    _ = try replay_source.forEachMatchingRecord(gpa, from_sequence, .resolution, max_records, &ctx, Ctx.consume);
    return ctx.max_seen;
}

const RuntimeStoreHandle = struct {
    store: backend_erased.Store,
    owned: bool,

    fn deinit(self: *RuntimeStoreHandle) void {
        if (self.owned) self.store.deinit();
    }
};

fn initRuntimeStore(alloc: Allocator, store: anytype) !RuntimeStoreHandle {
    const T = @TypeOf(store);
    if (T == backend_erased.Store) return .{ .store = store, .owned = false };
    if (T == *backend_erased.Store) return .{ .store = store.*, .owned = false };
    switch (@typeInfo(T)) {
        .pointer => |ptr| {
            if (@hasDecl(ptr.child, "backendStore")) {
                return .{ .store = try backend_erased.storeFrom(alloc, store.backendStore()), .owned = true };
            }
        },
        else => {
            if (@hasDecl(T, "backendStore")) {
                return .{ .store = try backend_erased.storeFrom(alloc, store.backendStore()), .owned = true };
            }
        },
    }
    return .{ .store = try backend_erased.storeFrom(alloc, store), .owned = true };
}

/// Managed worker that catches the resolution stage up on changed extraction
/// artifacts. Mirrors `EnrichmentRuntime`'s lifecycle: it wraps the shard store
/// into the erased store and runs a background loop on the `backend_runtime` io
/// that drains applied -> target. Crash recovery is ordinary replay --
/// `applied_sequence` is persisted only after a window's resolution writes are
/// durable, and the stage is idempotent.
pub const ResolutionRuntime = struct {
    alloc: Allocator,
    store_handle: RuntimeStoreHandle,
    replay_source: replay_source_mod.Source,
    index_manager: *index_manager_mod.IndexManager,
    write_ctx: *anyopaque,
    write_fn: DerivedRecordWriter,
    io_impl: ?*background_runtime_mod.IoImpl,
    applied_sequence: u64,
    target_sequence: std.atomic.Value(u64),
    shutdown_flag: std.atomic.Value(bool),
    future: ?Io.Future(void),

    const scope_name = "resolution";

    pub fn init(
        alloc: Allocator,
        store: anytype,
        replay_source: replay_source_mod.Source,
        index_manager: *index_manager_mod.IndexManager,
        write_ctx: *anyopaque,
        write_fn: DerivedRecordWriter,
        backend_runtime: *background_runtime_mod.BackendRuntime,
    ) !ResolutionRuntime {
        var store_handle = try initRuntimeStore(alloc, store);
        errdefer store_handle.deinit();
        const applied = try enrichment_state.loadAppliedSequence(alloc, store_handle.store, scope_name);
        return .{
            .alloc = alloc,
            .store_handle = store_handle,
            .replay_source = replay_source,
            .index_manager = index_manager,
            .write_ctx = write_ctx,
            .write_fn = write_fn,
            .io_impl = backend_runtime.io_impl,
            .applied_sequence = applied,
            .target_sequence = .init(applied),
            .shutdown_flag = .init(false),
            .future = null,
        };
    }

    pub fn deinit(self: *ResolutionRuntime) void {
        self.stop();
        self.store_handle.deinit();
        self.* = undefined;
    }

    /// Raise the catch-up target; the worker loop drains toward it.
    pub fn notifySequence(self: *ResolutionRuntime, sequence: u64) void {
        var cur = self.target_sequence.load(.monotonic);
        while (sequence > cur) {
            cur = self.target_sequence.cmpxchgWeak(cur, sequence, .monotonic, .monotonic) orelse break;
        }
    }

    pub fn start(self: *ResolutionRuntime) !void {
        const io_impl = self.io_impl orelse return error.MissingBackendRuntimeIo;
        self.future = try io_impl.io().concurrent(workerMain, .{self});
    }

    pub fn stop(self: *ResolutionRuntime) void {
        self.shutdown_flag.store(true, .release);
        if (self.future) |*future| {
            if (self.io_impl) |io_impl| {
                _ = future.await(io_impl.io());
            }
            self.future = null;
        }
    }

    /// One catch-up pass: drain applied -> target. Idempotent and safe to retry
    /// (the stage skips unchanged resolutions).
    pub fn catchUp(self: *ResolutionRuntime) !void {
        const target = self.target_sequence.load(.acquire);
        if (self.applied_sequence >= target) return;

        const resolvers = try self.index_manager.listResolvers(self.alloc);
        defer {
            for (resolvers) |*cfg| cfg.deinit(self.alloc);
            self.alloc.free(resolvers);
        }
        if (resolvers.len == 0) {
            // No resolver configured; advance so the hint does not rescan.
            try enrichment_state.saveAppliedSequence(self.store_handle.store, scope_name, target);
            self.applied_sequence = target;
            return;
        }

        var das = DbArtifactStore(backend_erased.Store){ .store = &self.store_handle.store };
        const max_seen = try catchUpWindow(
            self.alloc,
            self.replay_source,
            resolvers,
            das.artifactStore(),
            null,
            self.applied_sequence + 1,
            default_max_records_per_window,
            self.write_ctx,
            self.write_fn,
        );
        if (max_seen > self.applied_sequence) {
            try enrichment_state.saveAppliedSequence(self.store_handle.store, scope_name, max_seen);
            self.applied_sequence = max_seen;
        }
    }

    fn workerMain(self: *ResolutionRuntime) void {
        const io = (self.io_impl orelse return).io();
        while (!self.shutdown_flag.load(.acquire)) {
            if (self.applied_sequence < self.target_sequence.load(.acquire)) {
                self.catchUp() catch |err| {
                    std.log.warn("resolution catch-up failed: {s}", .{@errorName(err)});
                    io.sleep(Io.Duration.fromMilliseconds(50), .awake) catch {};
                };
            } else {
                io.sleep(Io.Duration.fromMilliseconds(50), .awake) catch {};
            }
        }
        self.catchUp() catch {};
    }
};

const testing = std.testing;

/// Adapts any shard store (the erased backend store: `beginRead/get`,
/// `beginWrite/put/delete/commit/abort`) to the resolver's `ArtifactStore`
/// seam. Generic over the store type so it works with the production erased
/// store and is unit-testable with a fake. The worker holds one of these and
/// passes `artifactStore()` to `processChangedExtraction`.
pub fn DbArtifactStore(comptime Store: type) type {
    return struct {
        store: *Store,

        const Self = @This();

        pub fn artifactStore(self: *Self) resolver_lib.ArtifactStore {
            return .{ .ptr = self, .vtable = &vtable };
        }

        const vtable = resolver_lib.ArtifactStore.VTable{ .get = getFn, .put = putFn, .delete = deleteFn };

        fn getFn(ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8) anyerror!?[]u8 {
            const self: *Self = @ptrCast(@alignCast(ptr));
            var txn = try self.store.beginRead();
            defer txn.abort();
            const raw = txn.get(key) catch |err| {
                if (err == error.NotFound) return null;
                return err;
            };
            return try allocator.dupe(u8, raw);
        }

        fn putFn(ptr: *anyopaque, key: []const u8, value: []const u8) anyerror!void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            var txn = try self.store.beginWrite();
            errdefer txn.abort();
            try txn.put(key, value);
            try txn.commit();
        }

        fn deleteFn(ptr: *anyopaque, key: []const u8) anyerror!void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            var txn = try self.store.beginWrite();
            errdefer txn.abort();
            txn.delete(key) catch |err| {
                if (err != error.NotFound) return err;
            };
            try txn.commit();
        }
    };
}

const test_extraction =
    \\{ "entities": [
    \\    { "id": "e0", "label": "Person", "text": "Ada Lovelace" },
    \\    { "id": "e1", "label": "Org", "text": "Antfly" }
    \\  ] }
;

test "resolveExtraction produces a resolution artifact for the matching resolver" {
    const alloc = testing.allocator;
    const resolvers = [_]ResolverConfig{.{
        .name = "knowledge_graph",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
        .config_generation = 2,
    }};

    const out = (try resolveExtraction(alloc, &resolvers, "relations_v1", test_extraction, &.{})).?;
    defer alloc.free(out.bytes);
    try testing.expectEqualStrings("resolution_v1", out.resolution_artifact);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, out.bytes, .{});
    defer parsed.deinit();
    try testing.expectEqual(@as(i64, 2), parsed.value.object.get("config_generation").?.integer);
    const entities = parsed.value.object.get("entities").?.array.items;
    try testing.expectEqual(@as(usize, 2), entities.len);
    try testing.expectEqualStrings(
        "person/ada_lovelace",
        entities[0].object.get("doc_ref").?.object.get("key").?.string,
    );
    try testing.expectEqualStrings(
        "org/antfly",
        entities[1].object.get("doc_ref").?.object.get("key").?.string,
    );
}

/// In-memory ArtifactStore for tests.
const MapStore = struct {
    alloc: std.mem.Allocator,
    map: std.StringHashMapUnmanaged([]u8) = .empty,

    fn deinit(self: *MapStore) void {
        var it = self.map.iterator();
        while (it.next()) |e| {
            self.alloc.free(e.key_ptr.*);
            self.alloc.free(e.value_ptr.*);
        }
        self.map.deinit(self.alloc);
    }

    fn store(self: *MapStore) resolver_lib.ArtifactStore {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = resolver_lib.ArtifactStore.VTable{ .get = get, .put = put, .delete = delete };

    fn get(ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8) anyerror!?[]u8 {
        const self: *MapStore = @ptrCast(@alignCast(ptr));
        const v = self.map.get(key) orelse return null;
        return try allocator.dupe(u8, v);
    }
    fn put(ptr: *anyopaque, key: []const u8, value: []const u8) anyerror!void {
        const self: *MapStore = @ptrCast(@alignCast(ptr));
        const owned_value = try self.alloc.dupe(u8, value);
        errdefer self.alloc.free(owned_value);
        const gop = try self.map.getOrPut(self.alloc, key);
        if (gop.found_existing) {
            self.alloc.free(gop.value_ptr.*);
        } else {
            gop.key_ptr.* = try self.alloc.dupe(u8, key);
        }
        gop.value_ptr.* = owned_value;
    }
    fn delete(ptr: *anyopaque, key: []const u8) anyerror!void {
        const self: *MapStore = @ptrCast(@alignCast(ptr));
        if (self.map.fetchRemove(key)) |kv| {
            self.alloc.free(kv.key);
            self.alloc.free(kv.value);
        }
    }
};

test "processChangedExtraction resolves and persists the resolution artifact" {
    const alloc = testing.allocator;
    const resolvers = [_]ResolverConfig{.{
        .name = "knowledge_graph",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ lower _entity.label }}/{{ slug _entity.text }}",
        .config_generation = 4,
    }};

    var map = MapStore{ .alloc = alloc };
    defer map.deinit();

    const extraction_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a1", "asset", "relations_v1");
    defer alloc.free(extraction_key);
    try map.store().put(extraction_key, test_extraction);

    {
        const outcome = (try processChangedExtraction(alloc, &resolvers, map.store(), null, extraction_key)).?;
        defer alloc.free(outcome.resolution_key);
        try testing.expectEqual(resolver_lib.RunResult.written, outcome.result);
    }
    // Replay is idempotent.
    {
        const outcome = (try processChangedExtraction(alloc, &resolvers, map.store(), null, extraction_key)).?;
        defer alloc.free(outcome.resolution_key);
        try testing.expectEqual(resolver_lib.RunResult.unchanged, outcome.result);
    }

    // The resolution artifact landed at the expected key with the resolved entities.
    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:a1", "resolution_v1");
    defer alloc.free(resolution_key);
    const stored = (try map.store().get(alloc, resolution_key)).?;
    defer alloc.free(stored);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, stored, .{});
    defer parsed.deinit();
    try testing.expectEqual(@as(i64, 4), parsed.value.object.get("config_generation").?.integer);
    const entities = parsed.value.object.get("entities").?.array.items;
    try testing.expectEqualStrings("person/ada_lovelace", entities[0].object.get("doc_ref").?.object.get("key").?.string);

    // A non-asset key is ignored.
    try testing.expect((try processChangedExtraction(alloc, &resolvers, map.store(), null, "not-an-artifact-key")) == null);
}

test "resolveExtraction returns null when no resolver consumes the artifact" {
    const alloc = testing.allocator;
    const resolvers = [_]ResolverConfig{.{
        .name = "knowledge_graph",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ slug _entity.text }}",
    }};
    try testing.expect((try resolveExtraction(alloc, &resolvers, "other_artifact", test_extraction, &.{})) == null);
    try testing.expect(resolverForArtifact(&resolvers, "relations_v1") != null);
    try testing.expect(resolverForArtifact(&resolvers, "nope") == null);
}

/// In-memory store with the erased-store txn shape, for testing DbArtifactStore.
const FakeStore = struct {
    alloc: std.mem.Allocator,
    map: std.StringHashMapUnmanaged([]u8) = .empty,

    fn deinit(self: *FakeStore) void {
        var it = self.map.iterator();
        while (it.next()) |e| {
            self.alloc.free(e.key_ptr.*);
            self.alloc.free(e.value_ptr.*);
        }
        self.map.deinit(self.alloc);
    }

    fn beginRead(self: *FakeStore) !ReadTxn {
        return .{ .s = self };
    }
    fn beginWrite(self: *FakeStore) !WriteTxn {
        return .{ .s = self };
    }

    const ReadTxn = struct {
        s: *FakeStore,
        fn abort(self: *ReadTxn) void {
            _ = self;
        }
        fn get(self: *ReadTxn, key: []const u8) ![]const u8 {
            return self.s.map.get(key) orelse error.NotFound;
        }
    };

    const WriteTxn = struct {
        s: *FakeStore,
        fn abort(self: *WriteTxn) void {
            _ = self;
        }
        fn commit(self: *WriteTxn) !void {
            _ = self;
        }
        fn put(self: *WriteTxn, key: []const u8, value: []const u8) !void {
            const owned_value = try self.s.alloc.dupe(u8, value);
            errdefer self.s.alloc.free(owned_value);
            const gop = try self.s.map.getOrPut(self.s.alloc, key);
            if (gop.found_existing) {
                self.s.alloc.free(gop.value_ptr.*);
            } else {
                gop.key_ptr.* = try self.s.alloc.dupe(u8, key);
            }
            gop.value_ptr.* = owned_value;
        }
        fn delete(self: *WriteTxn, key: []const u8) !void {
            if (self.s.map.fetchRemove(key)) |kv| {
                self.s.alloc.free(kv.key);
                self.s.alloc.free(kv.value);
            } else return error.NotFound;
        }
    };
};

test "DbArtifactStore adapts a shard store to the ArtifactStore seam" {
    const alloc = testing.allocator;
    var fake = FakeStore{ .alloc = alloc };
    defer fake.deinit();

    var das = DbArtifactStore(FakeStore){ .store = &fake };
    const store = das.artifactStore();

    try testing.expect((try store.get(alloc, "k")) == null);
    try store.put("k", "v1");
    {
        const got = (try store.get(alloc, "k")).?;
        defer alloc.free(got);
        try testing.expectEqualStrings("v1", got);
    }
    try store.put("k", "v2");
    {
        const got = (try store.get(alloc, "k")).?;
        defer alloc.free(got);
        try testing.expectEqualStrings("v2", got);
    }
    try store.delete("k");
    try testing.expect((try store.get(alloc, "k")) == null);
    try store.delete("k"); // delete of a missing key is a no-op
}

test "processChangedExtraction runs over a DbArtifactStore" {
    const alloc = testing.allocator;
    const resolvers = [_]ResolverConfig{.{
        .name = "kg",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ slug _entity.text }}",
        .config_generation = 1,
    }};

    var fake = FakeStore{ .alloc = alloc };
    defer fake.deinit();
    var das = DbArtifactStore(FakeStore){ .store = &fake };
    const store = das.artifactStore();

    const extraction_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:z", "asset", "relations_v1");
    defer alloc.free(extraction_key);
    try store.put(extraction_key, test_extraction);

    {
        const outcome = (try processChangedExtraction(alloc, &resolvers, store, null, extraction_key)).?;
        defer alloc.free(outcome.resolution_key);
        try testing.expectEqual(resolver_lib.RunResult.written, outcome.result);
    }

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:z", "resolution_v1");
    defer alloc.free(resolution_key);
    const stored = (try store.get(alloc, resolution_key)).?;
    defer alloc.free(stored);
    try testing.expect(std.mem.indexOf(u8, stored, "ada_lovelace") != null);
}

/// Capturing DerivedRecordWriter for tests.
const CaptureWriter = struct {
    alloc: std.mem.Allocator,
    keys: std.ArrayListUnmanaged([]u8) = .empty,
    calls: u64 = 0,

    fn deinit(self: *CaptureWriter) void {
        for (self.keys.items) |k| self.alloc.free(k);
        self.keys.deinit(self.alloc);
    }

    fn writeFn(ptr: *anyopaque, batch: derived_types.DerivedBatch) anyerror!u64 {
        const self: *CaptureWriter = @ptrCast(@alignCast(ptr));
        for (batch.changed_artifact_keys) |k| {
            try self.keys.append(self.alloc, try self.alloc.dupe(u8, k));
        }
        self.calls += 1;
        return self.calls;
    }
};

test "processRecordKeys resolves changed asset keys and journals resolution keys" {
    const alloc = testing.allocator;
    const resolvers = [_]ResolverConfig{.{
        .name = "kg",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ slug _entity.text }}",
        .config_generation = 1,
    }};

    var fake = FakeStore{ .alloc = alloc };
    defer fake.deinit();
    var das = DbArtifactStore(FakeStore){ .store = &fake };
    const store = das.artifactStore();

    const extraction_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:r", "asset", "relations_v1");
    defer alloc.free(extraction_key);
    try store.put(extraction_key, test_extraction);

    var writer = CaptureWriter{ .alloc = alloc };
    defer writer.deinit();

    // A record carrying the extraction key plus a non-asset key (ignored).
    const changed = [_][]const u8{ extraction_key, "not-an-artifact" };
    try processRecordKeys(alloc, &resolvers, store, null, &changed, &writer, CaptureWriter.writeFn);

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:r", "resolution_v1");
    defer alloc.free(resolution_key);
    try testing.expectEqual(@as(usize, 1), writer.keys.items.len);
    try testing.expectEqualSlices(u8, resolution_key, writer.keys.items[0]);
    try testing.expectEqual(@as(u64, 1), writer.calls);

    // Idempotent replay: recomputed bytes match, so nothing is journaled.
    writer.calls = 0;
    try processRecordKeys(alloc, &resolvers, store, null, &changed, &writer, CaptureWriter.writeFn);
    try testing.expectEqual(@as(usize, 1), writer.keys.items.len);
    try testing.expectEqual(@as(u64, 0), writer.calls);
}

/// Minimal replay Source for tests: replays a fixed list of encoded records.
const FakeSource = struct {
    const Rec = struct { sequence: u64, payload: []const u8 };
    records: []const Rec,

    fn source(self: *FakeSource) replay_source_mod.Source {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = replay_source_mod.Source.VTable{
        .open_matching_cursor = openCursor,
        .for_each_matching_record = forEach,
        .latest_matching_sequence = latest,
        .collect_enrichment_document_groups = collectGroups,
        .is_sequence_visible = isVisible,
    };

    fn forEach(
        ptr: *anyopaque,
        alloc: Allocator,
        from_sequence: u64,
        hint: replay_source_mod.TargetHint,
        max_matched_entries: usize,
        ctx: *anyopaque,
        consume: *const fn (ctx: *anyopaque, sequence: u64, payload: []const u8) anyerror!void,
    ) anyerror!replay_source_mod.MatchingRecordStats {
        _ = alloc;
        _ = hint;
        const self: *FakeSource = @ptrCast(@alignCast(ptr));
        var matched: usize = 0;
        var last: u64 = 0;
        for (self.records) |rec| {
            if (rec.sequence < from_sequence) continue;
            if (max_matched_entries != 0 and matched >= max_matched_entries) break;
            try consume(ctx, rec.sequence, rec.payload);
            matched += 1;
            last = rec.sequence;
        }
        return .{ .matched_entries = matched, .last_sequence = last };
    }

    fn openCursor(_: *anyopaque, _: Allocator, _: u64, _: replay_source_mod.TargetHint) anyerror!replay_source_mod.MatchingCursor {
        return error.Unsupported;
    }
    fn latest(_: *anyopaque, _: Allocator, _: u64, _: replay_source_mod.TargetHint) anyerror!u64 {
        return error.Unsupported;
    }
    fn collectGroups(_: *anyopaque, _: Allocator, _: u64) anyerror![]replay_source_mod.PendingDocumentGroup {
        return error.Unsupported;
    }
    fn isVisible(_: *anyopaque, _: u64) anyerror!bool {
        return error.Unsupported;
    }
};

test "catchUpWindow resolves matching records and journals resolution writes" {
    const alloc = testing.allocator;
    const resolvers = [_]ResolverConfig{.{
        .name = "kg",
        .table = "entities",
        .source_artifact = "relations_v1",
        .resolution_artifact = "resolution_v1",
        .key_template = "{{ slug _entity.text }}",
        .config_generation = 1,
    }};

    var fake = FakeStore{ .alloc = alloc };
    defer fake.deinit();
    var das = DbArtifactStore(FakeStore){ .store = &fake };
    const store = das.artifactStore();

    const extraction_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:w", "asset", "relations_v1");
    defer alloc.free(extraction_key);
    try store.put(extraction_key, test_extraction);

    const payload = try change_journal_mod.encodeRecord(alloc, .{
        .sequence = 7,
        .changed_artifact_keys = &.{extraction_key},
        .target_hints = &.{.resolution},
    });
    defer alloc.free(payload);

    var fake_source = FakeSource{ .records = &.{.{ .sequence = 7, .payload = payload }} };
    var writer = CaptureWriter{ .alloc = alloc };
    defer writer.deinit();

    const max_seen = try catchUpWindow(
        alloc,
        fake_source.source(),
        &resolvers,
        store,
        null,
        1,
        0,
        &writer,
        CaptureWriter.writeFn,
    );
    try testing.expectEqual(@as(u64, 7), max_seen);

    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:w", "resolution_v1");
    defer alloc.free(resolution_key);
    try testing.expect(fake.map.contains(resolution_key));
    try testing.expectEqual(@as(usize, 1), writer.keys.items.len);
    try testing.expectEqualSlices(u8, resolution_key, writer.keys.items[0]);
}

test "catchUpWindow with no matching records returns from-1" {
    const alloc = testing.allocator;
    const resolvers = [_]ResolverConfig{};
    var fake = FakeStore{ .alloc = alloc };
    defer fake.deinit();
    var das = DbArtifactStore(FakeStore){ .store = &fake };
    var fake_source = FakeSource{ .records = &.{} };
    var writer = CaptureWriter{ .alloc = alloc };
    defer writer.deinit();

    const max_seen = try catchUpWindow(alloc, fake_source.source(), &resolvers, das.artifactStore(), null, 5, 0, &writer, CaptureWriter.writeFn);
    try testing.expectEqual(@as(u64, 4), max_seen);
    try testing.expectEqual(@as(u64, 0), writer.calls);
}

test "ResolutionRuntime compiles end-to-end" {
    testing.refAllDecls(ResolutionRuntime);
}
