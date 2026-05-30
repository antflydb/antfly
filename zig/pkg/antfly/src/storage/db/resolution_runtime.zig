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
