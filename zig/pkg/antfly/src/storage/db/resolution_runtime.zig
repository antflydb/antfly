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
