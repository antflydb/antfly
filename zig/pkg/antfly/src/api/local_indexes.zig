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

pub const std = @import("std");
pub const db_mod = @import("../storage/db/selected_root.zig").db;
pub const managed_embedder = @import("../inference/managed_embedder.zig");
pub const document_content_hash = @import("../storage/db/document_content_hash.zig");
pub const enrichment_config_validation = @import("../storage/db/enrichment/config_validation.zig");
pub fn sortArtifactEnrichmentsByDependency(configs: []db_mod.types.EnrichmentConfig) void {
    std.mem.sort(db_mod.types.EnrichmentConfig, configs, {}, artifactEnrichmentLessThan);
}

pub fn collectArtifactEnrichmentsFromValueWithOptions(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    embedding_options: managed_embedder.InitOptions,
    out: *std.ArrayListUnmanaged(db_mod.types.EnrichmentConfig),
) !void {
    switch (value) {
        .object => |object| {
            const embedding_producer_json = blk: {
                const type_value = object.get("type") orelse break :blk null;
                if (type_value != .string or !std.mem.eql(u8, type_value.string, "embeddings")) break :blk null;
                if (object.get("embedder") == null) break :blk null;
                break :blk managed_embedder.embeddingCatalogSemanticProducerJsonAllocWithOptions(alloc, value, embedding_options) catch |err| switch (err) {
                    error.OutOfMemory => return err,
                    else => return error.InvalidEnrichmentConfig,
                };
            };
            defer if (embedding_producer_json) |raw| alloc.free(raw);
            if (object.get("enrichments")) |enrichments| {
                if (enrichments != .array) return error.InvalidEnrichmentConfig;
                for (enrichments.array.items) |item| {
                    if (item != .object) return error.InvalidEnrichmentConfig;
                    const parsed = std.json.parseFromValue(db_mod.types.EnrichmentConfig, alloc, item, .{
                        .allocate = .alloc_always,
                        .ignore_unknown_fields = true,
                    }) catch |err| switch (err) {
                        error.OutOfMemory => return err,
                        else => return error.InvalidEnrichmentConfig,
                    };
                    defer parsed.deinit();
                    var owned = try db_mod.types.EnrichmentConfig.clone(alloc, parsed.value);
                    errdefer owned.deinit(alloc);
                    if (item.object.get("chunker")) |chunker| {
                        const legacy_chunker = item.object.get("chunker_json");
                        if (owned.kind != .chunk or chunker != .object or (legacy_chunker != null and legacy_chunker.? != .null))
                            return error.InvalidEnrichmentConfig;
                        owned.chunker_json = try document_content_hash.canonicalJsonValueAlloc(alloc, chunker);
                    }
                    if (item.object.get("producer")) |producer| {
                        const legacy_producer = item.object.get("producer_json");
                        const transcriber = item.object.get("transcriber");
                        if (producer != .object or (legacy_producer != null and legacy_producer.? != .null) or (transcriber != null and transcriber.? != .null))
                            return error.InvalidEnrichmentConfig;
                        owned.producer_json = try document_content_hash.canonicalJsonValueAlloc(alloc, producer);
                    }
                    if (item.object.get("transcriber")) |transcriber| {
                        // The typed shorthand replaces producer_json rather
                        // than layering on it, and only an asset stream can
                        // hold transcripts.
                        if (owned.kind != .asset or owned.producer_json.len > 0) return error.InvalidEnrichmentConfig;
                        owned.producer_json = enrichment_config_validation.transcriberShorthandProducerJsonAlloc(alloc, transcriber) catch |err| switch (err) {
                            error.OutOfMemory => return err,
                            else => return error.InvalidEnrichmentConfig,
                        };
                        if (owned.content_type.len == 0) owned.content_type = try alloc.dupe(u8, "application/json");
                    }
                    if (owned.kind == .embedding) {
                        if (embedding_producer_json) |raw| {
                            if (owned.producer_json.len > 0) alloc.free(owned.producer_json);
                            owned.producer_json = try alloc.dupe(u8, raw);
                        }
                    }
                    try out.append(alloc, owned);
                }
            }
            var it = object.iterator();
            while (it.next()) |entry| {
                if (std.mem.eql(u8, entry.key_ptr.*, "enrichments")) continue;
                try collectArtifactEnrichmentsFromValueWithOptions(alloc, entry.value_ptr.*, embedding_options, out);
            }
        },
        .array => |array| {
            for (array.items) |item| try collectArtifactEnrichmentsFromValueWithOptions(alloc, item, embedding_options, out);
        },
        else => {},
    }
}

pub fn artifactEnrichmentLessThan(_: void, lhs: db_mod.types.EnrichmentConfig, rhs: db_mod.types.EnrichmentConfig) bool {
    const lhs_rank = artifactEnrichmentKindRank(lhs.kind);
    const rhs_rank = artifactEnrichmentKindRank(rhs.kind);
    if (lhs_rank != rhs_rank) return lhs_rank < rhs_rank;
    return std.mem.lessThan(u8, lhs.name, rhs.name);
}

pub fn artifactEnrichmentKindRank(kind: db_mod.types.EnrichmentKind) u8 {
    return switch (kind) {
        .asset => 0,
        .chunk => 1,
        .embedding => 2,
    };
}

pub const tables_api = @import("local_tables.zig");
pub fn collectArtifactEnrichmentsFromTableIndexesJsonWithOptions(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
    embedding_options: managed_embedder.InitOptions,
) ![]db_mod.types.EnrichmentConfig {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexesJsonSource(indexes_json), .{});
    defer parsed.deinit();

    var out = std.ArrayListUnmanaged(db_mod.types.EnrichmentConfig).empty;
    errdefer {
        for (out.items) |*cfg| cfg.deinit(alloc);
        out.deinit(alloc);
    }
    try collectArtifactEnrichmentsFromValueWithOptions(alloc, parsed.value, embedding_options, &out);
    return try out.toOwnedSlice(alloc);
}

pub fn validateArtifactEnrichmentConfigs(
    alloc: std.mem.Allocator,
    configs: []const db_mod.types.EnrichmentConfig,
) !void {
    try validateArtifactEnrichmentConfigDefinitions(alloc, configs);
    for (configs) |cfg| {
        switch (cfg.kind) {
            .chunk => {
                if (cfg.source_artifact_name.len > 0 and findArtifactEnrichmentConfig(configs, .asset, cfg.source_artifact_name) == null) {
                    return error.InvalidEnrichmentConfig;
                }
            },
            .embedding => {
                if (cfg.source_artifact_name.len > 0 and findArtifactEnrichmentConfig(configs, .chunk, cfg.source_artifact_name) == null) {
                    return error.InvalidEnrichmentConfig;
                }
            },
            .asset => {
                // Walk the asset-consumes-asset chain: every upstream must
                // resolve to an admitted asset, and the chain must terminate
                // without revisiting this config (self-reference is the
                // one-hop cycle). A cycle that excludes `cfg` is caught when
                // its own members are validated.
                var hops: usize = 0;
                var current: []const u8 = cfg.source_artifact_name;
                while (current.len > 0) {
                    if (std.mem.eql(u8, current, cfg.name)) return error.InvalidEnrichmentConfig;
                    const upstream = findArtifactEnrichmentConfig(configs, .asset, current) orelse
                        return error.InvalidEnrichmentConfig;
                    current = upstream.source_artifact_name;
                    hops += 1;
                    if (hops > configs.len) return error.InvalidEnrichmentConfig;
                }
            },
        }
    }
}

pub fn validateArtifactEnrichmentConfigDefinitions(
    alloc: std.mem.Allocator,
    configs: []const db_mod.types.EnrichmentConfig,
) !void {
    for (configs, 0..) |cfg, i| {
        try enrichment_config_validation.validatePublicConfig(alloc, cfg);
        for (configs[0..i]) |prior| {
            if (!std.mem.eql(u8, prior.name, cfg.name)) continue;
            if (!try artifactEnrichmentConfigsEqual(alloc, prior, cfg)) return error.ConflictingEnrichmentConfig;
        }
        if (cfg.full_text_index and cfg.kind == .embedding) return error.InvalidEnrichmentConfig;
    }
}

pub fn findArtifactEnrichmentConfig(
    configs: []const db_mod.types.EnrichmentConfig,
    kind: db_mod.types.EnrichmentKind,
    name: []const u8,
) ?db_mod.types.EnrichmentConfig {
    for (configs) |cfg| {
        if (cfg.kind == kind and std.mem.eql(u8, cfg.name, name)) return cfg;
    }
    return null;
}

pub fn artifactEnrichmentConfigsEqual(
    alloc: std.mem.Allocator,
    a: db_mod.types.EnrichmentConfig,
    b: db_mod.types.EnrichmentConfig,
) !bool {
    return a.kind == b.kind and
        std.mem.eql(u8, a.name, b.name) and
        std.mem.eql(u8, a.field, b.field) and
        std.mem.eql(u8, a.template, b.template) and
        std.mem.eql(u8, a.source_artifact_name, b.source_artifact_name) and
        a.expected_dims == b.expected_dims and
        std.mem.eql(u8, a.vector_space, b.vector_space) and
        a.chunk_size == b.chunk_size and
        a.chunk_overlap == b.chunk_overlap and
        try enrichment_config_validation.producerJsonValuesEqual(alloc, a.chunker_json, b.chunker_json) and
        a.full_text_index == b.full_text_index and
        std.mem.eql(u8, a.content_type, b.content_type) and
        try enrichment_config_validation.producerJsonValuesEqual(alloc, a.producer_json, b.producer_json) and
        neighborContextConfigsEqual(a.neighbor_context, b.neighbor_context) and
        std.meta.eql(a.execution, b.execution);
}

pub fn neighborContextConfigsEqual(
    a: ?db_mod.types.EnrichmentNeighborContextConfig,
    b: ?db_mod.types.EnrichmentNeighborContextConfig,
) bool {
    const lhs = a orelse return b == null;
    const rhs = b orelse return false;
    if (!std.mem.eql(u8, lhs.graph_index, rhs.graph_index) or
        lhs.direction != rhs.direction or
        lhs.limit != rhs.limit or
        lhs.edge_types.len != rhs.edge_types.len) return false;
    for (lhs.edge_types, rhs.edge_types) |lhs_type, rhs_type| {
        if (!std.mem.eql(u8, lhs_type, rhs_type)) return false;
    }
    return true;
}

pub fn indexesJsonSource(indexes_json: []const u8) []const u8 {
    return if (indexes_json.len > 0) indexes_json else tables_api.default_indexes_json;
}

pub fn isReservedIndexMetadataEntry(name: []const u8) bool {
    return std.mem.eql(u8, name, "resolvers") or std.mem.eql(u8, name, "enrichments");
}
