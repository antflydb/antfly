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

//! Persisted catalog of entity-resolution configs (see zig/RESOLUTION.md).
//!
//! A resolver consumes a source extraction artifact for a document, resolves
//! its mentions to canonical entity `DocRef`s, and writes a resolution
//! artifact. The durable config mirrors the enrichment catalog: it is stored
//! per shard and replayed at the recorded `config_generation` so resolution
//! decisions stay stable (the replay-stability invariant).
//!
//! `scorer_json` is the optional matcher scorer config (parsed by
//! `lib/matcher`); an empty `scorer_json` means a purely deterministic resolver
//! that always mints canonical keys from `key_template`.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const ResolverConfig = struct {
    /// Catalog name of the resolver.
    name: []const u8,
    /// Entity table that canonical entities live in.
    table: []const u8,
    /// Extraction artifact this resolver consumes (asset artifact name).
    source_artifact: []const u8,
    /// Resolution artifact this resolver writes.
    resolution_artifact: []const u8,
    /// Template that renders a canonical entity key from a mention.
    key_template: []const u8,
    /// Require the candidate entity label to match the mention label.
    type_must_match: bool = true,
    /// Optional matcher scorer config; empty means deterministic minting only.
    scorer_json: []const u8 = "",
    /// Candidate blocking strategy: "" (none, deterministic mint) or
    /// "exact_key" (look up the rendered canonical key as a candidate so the
    /// scorer can link to an existing entity). ANN/prefix blocking over the
    /// entity table is a phase-2 extension.
    candidate_search: []const u8 = "",
    /// Bumped to force a versioned re-resolution pass.
    config_generation: u64 = 0,

    pub fn clone(alloc: Allocator, cfg: ResolverConfig) !ResolverConfig {
        return .{
            .name = try alloc.dupe(u8, cfg.name),
            .table = try alloc.dupe(u8, cfg.table),
            .source_artifact = try alloc.dupe(u8, cfg.source_artifact),
            .resolution_artifact = try alloc.dupe(u8, cfg.resolution_artifact),
            .key_template = try alloc.dupe(u8, cfg.key_template),
            .type_must_match = cfg.type_must_match,
            .scorer_json = if (cfg.scorer_json.len > 0) try alloc.dupe(u8, cfg.scorer_json) else "",
            .candidate_search = if (cfg.candidate_search.len > 0) try alloc.dupe(u8, cfg.candidate_search) else "",
            .config_generation = cfg.config_generation,
        };
    }

    pub fn deinit(self: *ResolverConfig, alloc: Allocator) void {
        alloc.free(@constCast(self.name));
        alloc.free(@constCast(self.table));
        alloc.free(@constCast(self.source_artifact));
        alloc.free(@constCast(self.resolution_artifact));
        alloc.free(@constCast(self.key_template));
        if (self.scorer_json.len > 0) alloc.free(@constCast(self.scorer_json));
        if (self.candidate_search.len > 0) alloc.free(@constCast(self.candidate_search));
        self.* = undefined;
    }
};

pub fn serializeCatalog(alloc: Allocator, resolvers: []const ResolverConfig) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, resolvers, .{});
}

pub fn deserializeCatalog(alloc: Allocator, data: []const u8) ![]ResolverConfig {
    const parsed = try std.json.parseFromSlice([]ResolverConfig, alloc, data, .{
        .allocate = .alloc_always,
    });
    defer parsed.deinit();

    const out = try alloc.alloc(ResolverConfig, parsed.value.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*cfg| cfg.deinit(alloc);
        alloc.free(out);
    }
    for (parsed.value, 0..) |cfg, i| {
        out[i] = try ResolverConfig.clone(alloc, cfg);
        initialized += 1;
    }
    return out;
}

test "resolver catalog round trip" {
    const alloc = std.testing.allocator;

    const encoded = try serializeCatalog(alloc, &.{
        .{
            .name = "knowledge_graph",
            .table = "entities",
            .source_artifact = "relations_v1",
            .resolution_artifact = "resolution_v1",
            .key_template = "{{ lower _entity.label }}/{{ slug _entity.canonical_text }}",
            .scorer_json = "{\"comparisons\":[],\"combine\":{\"bias\":-3.0},\"decision\":{\"match\":0.9}}",
            .config_generation = 7,
        },
        .{
            .name = "people_only",
            .table = "entities",
            .source_artifact = "relations_v1",
            .resolution_artifact = "people_resolution_v1",
            .key_template = "{{ slug _entity.text }}",
            .type_must_match = false,
        },
    });
    defer alloc.free(encoded);

    const decoded = try deserializeCatalog(alloc, encoded);
    defer {
        for (decoded) |*cfg| cfg.deinit(alloc);
        alloc.free(decoded);
    }

    try std.testing.expectEqual(@as(usize, 2), decoded.len);
    try std.testing.expectEqualStrings("knowledge_graph", decoded[0].name);
    try std.testing.expectEqualStrings("entities", decoded[0].table);
    try std.testing.expectEqualStrings("relations_v1", decoded[0].source_artifact);
    try std.testing.expectEqualStrings("resolution_v1", decoded[0].resolution_artifact);
    try std.testing.expectEqualStrings("{{ lower _entity.label }}/{{ slug _entity.canonical_text }}", decoded[0].key_template);
    try std.testing.expect(decoded[0].type_must_match);
    try std.testing.expectEqual(@as(u64, 7), decoded[0].config_generation);
    try std.testing.expect(decoded[0].scorer_json.len > 0);

    try std.testing.expectEqualStrings("people_only", decoded[1].name);
    try std.testing.expect(!decoded[1].type_must_match);
    try std.testing.expectEqual(@as(usize, 0), decoded[1].scorer_json.len);
    try std.testing.expectEqual(@as(u64, 0), decoded[1].config_generation);
}

test "resolver catalog round trip preserves order and empty list" {
    const alloc = std.testing.allocator;

    const empty = try serializeCatalog(alloc, &.{});
    defer alloc.free(empty);
    const decoded_empty = try deserializeCatalog(alloc, empty);
    defer alloc.free(decoded_empty);
    try std.testing.expectEqual(@as(usize, 0), decoded_empty.len);
}
