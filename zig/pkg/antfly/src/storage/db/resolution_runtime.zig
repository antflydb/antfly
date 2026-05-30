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

pub const ResolverConfig = resolver_catalog.ResolverConfig;

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

/// Process a changed extraction (asset) artifact key: look up the resolver that
/// consumes it and run the resolution stage to idempotently (re)persist the
/// resolution artifact through `store`. Returns null when `changed_key` is not
/// an asset artifact or no configured resolver consumes it. `provider` supplies
/// blocking candidates (null = deterministic minting only).
///
/// The db worker supplies a `store` backed by the shard primary store and,
/// based on the `RunResult`, journals the resolution artifact key via a
/// `DerivedBatch` so downstream stages (graph materializer) wake.
pub fn processChangedExtraction(
    gpa: std.mem.Allocator,
    resolvers: []const ResolverConfig,
    store: resolver_lib.ArtifactStore,
    provider: ?resolver_lib.CandidateProvider,
    changed_key: []const u8,
) !?resolver_lib.RunResult {
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
    defer gpa.free(resolution_key);

    const stage = resolver_lib.ResolutionStage{ .resolver = &resolver, .config_generation = cfg.config_generation };
    return try stage.run(gpa, store, provider, changed_key, resolution_key);
}

const testing = std.testing;

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

    const result = (try processChangedExtraction(alloc, &resolvers, map.store(), null, extraction_key)).?;
    try testing.expectEqual(resolver_lib.RunResult.written, result);

    // Replay is idempotent.
    try testing.expectEqual(
        resolver_lib.RunResult.unchanged,
        (try processChangedExtraction(alloc, &resolvers, map.store(), null, extraction_key)).?,
    );

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
