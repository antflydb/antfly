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

const std = @import("std");
const ant_json = @import("antfly-json");
const metadata_api = @import("../metadata/api.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const db_mod = @import("../storage/db/mod.zig");
const tables_api = @import("tables.zig");
const runtime_status = @import("runtime_status.zig");
const coverage_policy_mod = @import("coverage_policy.zig");
const json_helpers = @import("json_helpers.zig");
const managed_embedder = @import("../inference/managed_embedder.zig");
const internal_keys = @import("../storage/internal_keys.zig");
const indexes_openapi = @import("antfly_indexes_openapi");
const chunking_openapi = @import("antfly_chunking_openapi");
const chunking_api_openapi = @import("antfly_chunking_api_openapi");
const enrichment_config_validation = @import("../storage/db/enrichment/config_validation.zig");
const public_index_contract = @import("public_index_contract.zig");
const index_repair_status = @import("../common/index_repair_status.zig");

pub fn parseCreateIndexRequest(alloc: std.mem.Allocator, body: []const u8) ![]u8 {
    if (body.len == 0) return error.InvalidCreateIndexRequest;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    switch (parsed.value) {
        .object => {},
        else => return error.InvalidCreateIndexRequest,
    }
    coverage_policy_mod.validateIndexConfig(parsed.value) catch return error.InvalidCreateIndexRequest;
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(parsed.value, .{})});
}

pub fn addIndexToTableIndexesJson(
    alloc: std.mem.Allocator,
    current_indexes_json: []const u8,
    index_name: []const u8,
    index_json: []const u8,
) ![]u8 {
    var current = try std.json.parseFromSlice(std.json.Value, alloc, current_indexes_json, .{});
    defer current.deinit();
    var config = try std.json.parseFromSlice(std.json.Value, alloc, index_json, .{});
    defer config.deinit();

    const root = switch (current.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };
    if (config.value != .object) return error.InvalidCreateIndexRequest;
    const stored_config = storedIndexConfigForMutationAlloc(
        alloc,
        index_name,
        root.get(index_name),
        config.value,
    ) catch return error.InvalidCreateIndexRequest;
    defer alloc.free(stored_config);

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');

    var first = true;
    var it = root.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, index_name)) continue;
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonString(alloc, &out, entry.key_ptr.*);
        try out.append(alloc, ':');
        const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(entry.value_ptr.*, .{})});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }

    if (!first) try out.append(alloc, ',');
    try appendJsonString(alloc, &out, index_name);
    try out.append(alloc, ':');
    try out.appendSlice(alloc, stored_config);
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn storedIndexConfigForMutationAlloc(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    existing: ?std.json.Value,
    requested: std.json.Value,
) ![]u8 {
    try coverage_policy_mod.validateStoredIndexConfig(requested);
    if (coverage_policy_mod.incarnation(requested) != null) {
        return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(requested, .{})});
    }
    if (existing) |current| {
        if (coverage_policy_mod.incarnation(current)) |current_incarnation| {
            const exact_match = try equivalentIndexConfigValues(alloc, index_name, current, requested);
            const output_match = if (exact_match)
                true
            else
                equivalentDerivedOutputConfig(alloc, index_name, current, requested);
            if (output_match) {
                return try coverage_policy_mod.withIncarnationAlloc(alloc, requested, current_incarnation);
            }
        }
    }
    return try coverage_policy_mod.withFreshIncarnationAlloc(alloc, requested);
}

fn equivalentDerivedOutputConfig(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    lhs: std.json.Value,
    rhs: std.json.Value,
) bool {
    const lhs_fingerprint = expectedCoverageConfigHash(alloc, index_name, lhs) catch return false;
    const rhs_fingerprint = expectedCoverageConfigHash(alloc, index_name, rhs) catch return false;
    return lhs_fingerprint == rhs_fingerprint;
}

fn equivalentIndexConfigValues(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    lhs: std.json.Value,
    rhs: std.json.Value,
) !bool {
    const lhs_json = try canonicalIndexConfigJson(alloc, index_name, lhs);
    defer alloc.free(lhs_json);
    const rhs_json = try canonicalIndexConfigJson(alloc, index_name, rhs);
    defer alloc.free(rhs_json);
    var lhs_parsed = try std.json.parseFromSlice(std.json.Value, alloc, lhs_json, .{});
    defer lhs_parsed.deinit();
    var rhs_parsed = try std.json.parseFromSlice(std.json.Value, alloc, rhs_json, .{});
    defer rhs_parsed.deinit();
    return json_helpers.jsonValuesEqual(lhs_parsed.value, rhs_parsed.value);
}

pub fn storedIndexConfigJsonAlloc(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
    index_name: []const u8,
) !?[]u8 {
    var lookup = (try lookupSingleIndexConfig(alloc, indexes_json, index_name)) orelse return null;
    defer lookup.deinit();
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(lookup.config, .{})});
}

pub fn removeIndexFromTableIndexesJson(
    alloc: std.mem.Allocator,
    current_indexes_json: []const u8,
    index_name: []const u8,
) !?[]u8 {
    var current = try std.json.parseFromSlice(std.json.Value, alloc, current_indexes_json, .{});
    defer current.deinit();

    const root = switch (current.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };
    if (!root.contains(index_name)) return null;

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');

    var first = true;
    var it = root.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, index_name)) continue;
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonString(alloc, &out, entry.key_ptr.*);
        try out.append(alloc, ':');
        const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(entry.value_ptr.*, .{})});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }

    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn addEnrichmentToTableIndexesJson(
    alloc: std.mem.Allocator,
    current_indexes_json: []const u8,
    enrichment_name: []const u8,
    enrichment_json: []const u8,
) ![]u8 {
    try validateEnrichmentConfigName(alloc, enrichment_name, enrichment_json);
    var current = try std.json.parseFromSlice(std.json.Value, alloc, current_indexes_json, .{});
    defer current.deinit();
    var config = std.json.parseFromSlice(std.json.Value, alloc, enrichment_json, .{}) catch return error.InvalidExtensionEnrichment;
    defer config.deinit();

    const root = switch (current.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };
    if (config.value != .object) return error.InvalidExtensionEnrichment;

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');

    var first = true;
    var wrote_enrichments = false;
    var it = root.iterator();
    while (it.next()) |entry| {
        if (!first) try out.append(alloc, ',');
        first = false;

        if (std.mem.eql(u8, entry.key_ptr.*, "enrichments")) {
            wrote_enrichments = true;
            try appendJsonString(alloc, &out, "enrichments");
            try out.append(alloc, ':');
            try appendEnrichmentArrayWithReplacement(alloc, &out, entry.value_ptr.*, enrichment_name, config.value);
            continue;
        }

        try appendJsonString(alloc, &out, entry.key_ptr.*);
        try out.append(alloc, ':');
        try appendJsonValue(alloc, &out, entry.value_ptr.*);
    }

    if (!wrote_enrichments) {
        if (!first) try out.append(alloc, ',');
        try appendJsonString(alloc, &out, "enrichments");
        try out.appendSlice(alloc, ":[");
        try appendJsonValue(alloc, &out, config.value);
        try out.append(alloc, ']');
    }

    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn removeEnrichmentFromTableIndexesJson(
    alloc: std.mem.Allocator,
    current_indexes_json: []const u8,
    enrichment_name: []const u8,
) !?[]u8 {
    var current = try std.json.parseFromSlice(std.json.Value, alloc, current_indexes_json, .{});
    defer current.deinit();

    const root = switch (current.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };
    const current_enrichments = root.get("enrichments") orelse return null;
    if (current_enrichments != .array) return error.InvalidTableIndexMetadata;

    var removed = false;
    var remaining: usize = 0;
    for (current_enrichments.array.items) |item| {
        if (enrichmentValueNameEquals(item, enrichment_name)) {
            removed = true;
        } else {
            remaining += 1;
        }
    }
    if (!removed) return null;

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');

    var first = true;
    var it = root.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "enrichments") and remaining == 0) continue;
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonString(alloc, &out, entry.key_ptr.*);
        try out.append(alloc, ':');
        if (std.mem.eql(u8, entry.key_ptr.*, "enrichments")) {
            try appendEnrichmentArrayWithoutName(alloc, &out, entry.value_ptr.*, enrichment_name);
        } else {
            try appendJsonValue(alloc, &out, entry.value_ptr.*);
        }
    }

    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn collectArtifactEnrichmentsFromTableIndexesJson(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
) ![]db_mod.types.EnrichmentConfig {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexesJsonSource(indexes_json), .{});
    defer parsed.deinit();

    var out = std.ArrayListUnmanaged(db_mod.types.EnrichmentConfig).empty;
    errdefer {
        for (out.items) |*cfg| cfg.deinit(alloc);
        out.deinit(alloc);
    }
    try collectArtifactEnrichmentsFromValue(alloc, parsed.value, &out);
    return try out.toOwnedSlice(alloc);
}

pub fn encodeArtifactEnrichmentList(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    indexes_json: []const u8,
) ![]u8 {
    var enrichments = try collectArtifactEnrichmentsFromTableIndexesJson(alloc, indexes_json);
    var unique_len: usize = 0;
    defer {
        for (enrichments[0..unique_len]) |*cfg| cfg.deinit(alloc);
        alloc.free(enrichments);
    }

    sortArtifactEnrichmentsByDependency(enrichments);
    for (enrichments, 0..) |*cfg, i| {
        var duplicate = false;
        for (enrichments[0..unique_len]) |prior| {
            if (std.mem.eql(u8, prior.name, cfg.name)) {
                duplicate = true;
                break;
            }
        }
        if (duplicate) {
            cfg.deinit(alloc);
            continue;
        }
        if (unique_len != i) {
            enrichments[unique_len] = cfg.*;
            cfg.* = undefined;
        }
        unique_len += 1;
    }

    const response = struct {
        table_name: []const u8,
        artifacts: []const db_mod.types.EnrichmentConfig,
    }{
        .table_name = table_name,
        .artifacts = enrichments[0..unique_len],
    };
    return try std.json.Stringify.valueAlloc(alloc, response, .{});
}

pub fn validateArtifactEnrichmentsForTableIndexesJson(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
) !void {
    const enrichments = try collectArtifactEnrichmentsFromTableIndexesJson(alloc, indexes_json);
    defer db_mod.types.freeEnrichmentConfigs(alloc, enrichments);
    try validateArtifactEnrichmentConfigs(alloc, enrichments);
}

pub fn validateArtifactEnrichmentConfigs(
    alloc: std.mem.Allocator,
    configs: []const db_mod.types.EnrichmentConfig,
) !void {
    for (configs, 0..) |cfg, i| {
        try enrichment_config_validation.validatePublicConfig(alloc, cfg);
        for (configs[0..i]) |prior| {
            if (!std.mem.eql(u8, prior.name, cfg.name)) continue;
            if (!artifactEnrichmentConfigsEqual(prior, cfg)) return error.ConflictingEnrichmentConfig;
        }
        if (cfg.full_text_index and cfg.kind == .embedding) return error.InvalidEnrichmentConfig;
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
            .asset => {},
        }
    }
}

pub fn sortArtifactEnrichmentsByDependency(configs: []db_mod.types.EnrichmentConfig) void {
    std.mem.sort(db_mod.types.EnrichmentConfig, configs, {}, artifactEnrichmentLessThan);
}

pub fn collectArtifactEnrichmentsFromValue(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    out: *std.ArrayListUnmanaged(db_mod.types.EnrichmentConfig),
) !void {
    switch (value) {
        .object => |object| {
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
                    try out.append(alloc, owned);
                }
            }
            var it = object.iterator();
            while (it.next()) |entry| {
                if (std.mem.eql(u8, entry.key_ptr.*, "enrichments")) continue;
                try collectArtifactEnrichmentsFromValue(alloc, entry.value_ptr.*, out);
            }
        },
        .array => |array| {
            for (array.items) |item| try collectArtifactEnrichmentsFromValue(alloc, item, out);
        },
        else => {},
    }
}

fn findArtifactEnrichmentConfig(
    configs: []const db_mod.types.EnrichmentConfig,
    kind: db_mod.types.EnrichmentKind,
    name: []const u8,
) ?db_mod.types.EnrichmentConfig {
    for (configs) |cfg| {
        if (cfg.kind == kind and std.mem.eql(u8, cfg.name, name)) return cfg;
    }
    return null;
}

fn artifactEnrichmentConfigsEqual(a: db_mod.types.EnrichmentConfig, b: db_mod.types.EnrichmentConfig) bool {
    return a.kind == b.kind and
        std.mem.eql(u8, a.name, b.name) and
        std.mem.eql(u8, a.field, b.field) and
        std.mem.eql(u8, a.template, b.template) and
        std.mem.eql(u8, a.source_artifact_name, b.source_artifact_name) and
        a.expected_dims == b.expected_dims and
        a.chunk_size == b.chunk_size and
        a.chunk_overlap == b.chunk_overlap and
        std.mem.eql(u8, a.chunker_json, b.chunker_json) and
        a.full_text_index == b.full_text_index and
        std.mem.eql(u8, a.content_type, b.content_type) and
        std.mem.eql(u8, a.producer_json, b.producer_json) and
        std.meta.eql(a.execution, b.execution);
}

fn artifactEnrichmentLessThan(_: void, lhs: db_mod.types.EnrichmentConfig, rhs: db_mod.types.EnrichmentConfig) bool {
    const lhs_rank = artifactEnrichmentKindRank(lhs.kind);
    const rhs_rank = artifactEnrichmentKindRank(rhs.kind);
    if (lhs_rank != rhs_rank) return lhs_rank < rhs_rank;
    return std.mem.lessThan(u8, lhs.name, rhs.name);
}

fn artifactEnrichmentKindRank(kind: db_mod.types.EnrichmentKind) u8 {
    return switch (kind) {
        .asset => 0,
        .chunk => 1,
        .embedding => 2,
    };
}

pub fn encodeIndexList(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table_name: []const u8,
    local_statuses: ?*const runtime_status.LocalTableRuntimeStatuses,
) !?[]u8 {
    const table = tables_api.findTableByName(snapshot, table_name) orelse return null;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexesJsonSource(table.indexes_json), .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };
    const expected_group_ids = try expectedTableGroupIds(alloc, snapshot, table.table_id);
    defer if (expected_group_ids.len > 0) alloc.free(expected_group_ids);
    var status_lookup = try RuntimeStatusLookup.init(alloc, expected_group_ids, local_statuses);
    defer status_lookup.deinit();

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '[');
    var first = true;
    var it = object.iterator();
    while (it.next()) |entry| {
        if (isReservedIndexMetadataEntry(entry.key_ptr.*)) continue;
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendIndexStatus(alloc, &out, entry.key_ptr.*, entry.value_ptr.*, expected_group_ids, local_statuses, &status_lookup);
    }
    try out.append(alloc, ']');
    return try out.toOwnedSlice(alloc);
}

pub fn encodeSingleIndex(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table_name: []const u8,
    index_name: []const u8,
    local_statuses: ?*const runtime_status.LocalTableRuntimeStatuses,
) !?[]u8 {
    const table = tables_api.findTableByName(snapshot, table_name) orelse return null;
    const expected_group_ids = try expectedTableGroupIds(alloc, snapshot, table.table_id);
    defer if (expected_group_ids.len > 0) alloc.free(expected_group_ids);
    return try encodeSingleIndexForTableWithTopology(alloc, table, index_name, expected_group_ids, local_statuses);
}

pub const IndexRuntimeIdentity = struct {
    coverage_generation: u64,
    coverage_config_hash: u64,
};

/// Returns the runtime identity carried by an incarnation-scoped index.
/// Non-embedding indexes have no catalog incarnation and therefore use the
/// existing name-scoped status selection.
pub fn indexRuntimeIdentity(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    config: std.json.Value,
) !?IndexRuntimeIdentity {
    const coverage_generation = coverage_policy_mod.incarnation(config) orelse return null;
    return .{
        .coverage_generation = coverage_generation,
        .coverage_config_hash = try expectedCoverageConfigHash(alloc, index_name, config),
    };
}

/// Encodes an already-resolved index lookup while retaining the table's shard
/// topology. The public GET path uses this to reject missing indexes before
/// consulting runtime owners and to avoid parsing the catalog config twice.
pub fn encodeSingleIndexLookupForTable(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table: *const metadata_table_manager.TableRecord,
    index_name: []const u8,
    config: std.json.Value,
    runtime_identity: ?IndexRuntimeIdentity,
    local_statuses: ?*const runtime_status.LocalTableRuntimeStatuses,
) ![]u8 {
    const expected_group_ids = try expectedTableGroupIds(alloc, snapshot, table.table_id);
    defer if (expected_group_ids.len > 0) alloc.free(expected_group_ids);
    return try encodeSingleIndexLookupWithTopology(alloc, index_name, config, runtime_identity, expected_group_ids, local_statuses);
}

pub fn encodeSingleIndexForTable(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    index_name: []const u8,
    local_statuses: ?*const runtime_status.LocalTableRuntimeStatuses,
) !?[]u8 {
    return try encodeSingleIndexForTableWithTopology(alloc, table, index_name, &.{}, local_statuses);
}

fn encodeSingleIndexForTableWithTopology(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    index_name: []const u8,
    expected_group_ids: []const u64,
    local_statuses: ?*const runtime_status.LocalTableRuntimeStatuses,
) !?[]u8 {
    var lookup = (try lookupSingleIndexConfig(alloc, table.indexes_json, index_name)) orelse return null;
    defer lookup.deinit();
    const runtime_identity = try indexRuntimeIdentity(alloc, index_name, lookup.config);
    return try encodeSingleIndexLookupWithTopology(alloc, index_name, lookup.config, runtime_identity, expected_group_ids, local_statuses);
}

pub fn encodeSingleIndexLookup(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    config: std.json.Value,
    local_statuses: ?*const runtime_status.LocalTableRuntimeStatuses,
) ![]u8 {
    const runtime_identity = try indexRuntimeIdentity(alloc, index_name, config);
    return try encodeSingleIndexLookupWithTopology(alloc, index_name, config, runtime_identity, &.{}, local_statuses);
}

fn encodeSingleIndexLookupWithTopology(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    config: std.json.Value,
    runtime_identity: ?IndexRuntimeIdentity,
    expected_group_ids: []const u64,
    local_statuses: ?*const runtime_status.LocalTableRuntimeStatuses,
) ![]u8 {
    if (config != .object) return error.InvalidTableIndexMetadata;
    var status_lookup = try RuntimeStatusLookup.init(alloc, expected_group_ids, local_statuses);
    defer status_lookup.deinit();

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try appendIndexStatusWithIdentity(alloc, &out, index_name, config, runtime_identity, expected_group_ids, local_statuses, &status_lookup);
    return try out.toOwnedSlice(alloc);
}

pub fn encodeIndexConfigMap(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexesJsonSource(indexes_json), .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    var it = object.iterator();
    while (it.next()) |entry| {
        if (isReservedIndexMetadataEntry(entry.key_ptr.*)) continue;
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonString(alloc, &out, entry.key_ptr.*);
        try out.append(alloc, ':');
        try appendIndexConfig(alloc, &out, entry.key_ptr.*, entry.value_ptr.*);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn encodeSingleIndexConfig(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
    index_name: []const u8,
) !?[]u8 {
    var lookup = (try lookupSingleIndexConfig(alloc, indexes_json, index_name)) orelse return null;
    defer lookup.deinit();

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try appendIndexConfig(alloc, &out, index_name, lookup.config);
    return try out.toOwnedSlice(alloc);
}

/// Encode the effective index configuration returned by create. The index
/// identity is path-owned and public responses must never reflect inline
/// provider credentials from the stored catalog document.
pub fn encodeCreatedIndexConfig(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    index_json: []const u8,
) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, index_json, .{});
    defer parsed.deinit();

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try appendPublicIndexConfig(alloc, &out, index_name, parsed.value, false);
    return try out.toOwnedSlice(alloc);
}

pub fn hasIndexConfig(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
    index_name: []const u8,
) !bool {
    var lookup = (try lookupSingleIndexConfig(alloc, indexes_json, index_name)) orelse return false;
    defer lookup.deinit();
    return true;
}

pub const SingleIndexConfigLookup = struct {
    parsed: std.json.Parsed(std.json.Value),
    config: std.json.Value,

    pub fn deinit(self: *SingleIndexConfigLookup) void {
        self.parsed.deinit();
        self.* = undefined;
    }
};

pub fn lookupSingleIndexConfig(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
    index_name: []const u8,
) !?SingleIndexConfigLookup {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexesJsonSource(indexes_json), .{});
    errdefer parsed.deinit();

    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };
    const config = object.get(index_name) orelse {
        parsed.deinit();
        return null;
    };
    return .{
        .parsed = parsed,
        .config = config,
    };
}

pub fn equivalentIndexConfigJson(
    alloc: std.mem.Allocator,
    lhs_json: []const u8,
    rhs_json: []const u8,
) !bool {
    if (std.mem.eql(u8, lhs_json, rhs_json)) return true;

    var lhs = try std.json.parseFromSlice(std.json.Value, alloc, lhs_json, .{});
    defer lhs.deinit();
    var rhs = try std.json.parseFromSlice(std.json.Value, alloc, rhs_json, .{});
    defer rhs.deinit();

    const lhs_object = switch (lhs.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };
    const rhs_object = switch (rhs.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };
    if (lhs_object.count() != rhs_object.count()) return false;

    var it = lhs_object.iterator();
    while (it.next()) |entry| {
        const rhs_value = rhs_object.get(entry.key_ptr.*) orelse return false;
        if (!try equivalentIndexConfigValues(alloc, entry.key_ptr.*, entry.value_ptr.*, rhs_value)) return false;
    }
    return true;
}

const ApiIndexType = public_index_contract.Kind;

fn indexesJsonSource(indexes_json: []const u8) []const u8 {
    return if (indexes_json.len > 0) indexes_json else tables_api.default_indexes_json;
}

fn isReservedIndexMetadataEntry(name: []const u8) bool {
    return std.mem.eql(u8, name, "resolvers") or std.mem.eql(u8, name, "enrichments");
}

pub fn expectedTableGroupIds(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table_id: u64,
) ![]u64 {
    var count: usize = 0;
    for (snapshot.ranges) |range| {
        if (range.table_id == table_id) count += 1;
    }
    if (count == 0) return &.{};

    const group_ids = try alloc.alloc(u64, count);
    var i: usize = 0;
    for (snapshot.ranges) |range| {
        if (range.table_id != table_id) continue;
        group_ids[i] = range.group_id;
        i += 1;
    }
    return group_ids;
}

const RuntimeStatusLookup = struct {
    const IndexMap = std.StringHashMapUnmanaged(*const db_mod.types.DBIndexStats);

    alloc: std.mem.Allocator,
    expected_group_indexes: std.AutoHashMapUnmanaged(u64, usize) = .empty,
    runtime_indexes: []IndexMap = &.{},

    fn init(
        alloc: std.mem.Allocator,
        expected_group_ids: []const u64,
        local_statuses: ?*const runtime_status.LocalTableRuntimeStatuses,
    ) !RuntimeStatusLookup {
        var lookup = RuntimeStatusLookup{ .alloc = alloc };
        errdefer lookup.deinit();
        try lookup.expected_group_indexes.ensureTotalCapacity(alloc, @intCast(expected_group_ids.len));
        for (expected_group_ids, 0..) |group_id, i| {
            lookup.expected_group_indexes.putAssumeCapacity(group_id, i);
        }

        const statuses = if (local_statuses) |runtime| runtime.items else &.{};
        if (statuses.len == 0) return lookup;
        lookup.runtime_indexes = try alloc.alloc(IndexMap, statuses.len);
        @memset(lookup.runtime_indexes, .empty);
        for (statuses, 0..) |*status, runtime_index| {
            const map = &lookup.runtime_indexes[runtime_index];
            try map.ensureTotalCapacity(alloc, @intCast(status.stats.indexes.len));
            for (status.stats.indexes) |*item| map.putAssumeCapacity(item.name, item);
        }
        return lookup;
    }

    fn deinit(self: *RuntimeStatusLookup) void {
        for (self.runtime_indexes) |*map| map.deinit(self.alloc);
        if (self.runtime_indexes.len > 0) self.alloc.free(self.runtime_indexes);
        self.expected_group_indexes.deinit(self.alloc);
        self.* = undefined;
    }

    fn expectedGroupIndex(self: *const RuntimeStatusLookup, expected_group_ids: []const u64, group_id: u64) ?usize {
        if (expected_group_ids.len == 0) return null;
        return self.expected_group_indexes.get(group_id);
    }

    fn findIndex(self: *const RuntimeStatusLookup, runtime_index: usize, index_name: []const u8) ?db_mod.types.DBIndexStats {
        if (runtime_index >= self.runtime_indexes.len) return null;
        return (self.runtime_indexes[runtime_index].get(index_name) orelse return null).*;
    }
};

fn appendIndexStatus(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    index_name: []const u8,
    config: std.json.Value,
    expected_group_ids: []const u64,
    local_statuses: ?*const runtime_status.LocalTableRuntimeStatuses,
    status_lookup: *const RuntimeStatusLookup,
) !void {
    const runtime_identity = try indexRuntimeIdentity(alloc, index_name, config);
    return try appendIndexStatusWithIdentity(
        alloc,
        out,
        index_name,
        config,
        runtime_identity,
        expected_group_ids,
        local_statuses,
        status_lookup,
    );
}

fn appendIndexStatusWithIdentity(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    index_name: []const u8,
    config: std.json.Value,
    runtime_identity: ?IndexRuntimeIdentity,
    expected_group_ids: []const u64,
    local_statuses: ?*const runtime_status.LocalTableRuntimeStatuses,
    status_lookup: *const RuntimeStatusLookup,
) !void {
    const index_type = inferIndexType(index_name, config) orelse return error.InvalidTableIndexMetadata;
    const embeddings_coverage_policy = if (index_type == .embeddings)
        embeddingsCoveragePolicy(config)
    else
        .strict;
    const embeddings_sparse = if (index_type == .embeddings)
        embeddingsIsSparse(config)
    else
        false;
    const graph_source_status = if (index_type == .graph)
        graphSourceStatus(config)
    else
        null;
    const coverage_generation = if (runtime_identity) |identity| identity.coverage_generation else 0;
    const coverage_config_hash = if (runtime_identity) |identity| identity.coverage_config_hash else 0;
    try out.appendSlice(alloc, "{\"config\":");
    try appendIndexConfig(alloc, out, index_name, config);
    try out.appendSlice(alloc, ",\"status\":");
    try appendIndexRuntimeStatus(alloc, out, index_name, index_type, embeddings_coverage_policy, embeddings_sparse, coverage_generation, coverage_config_hash, graph_source_status, expected_group_ids, local_statuses, status_lookup, false);
    try out.appendSlice(alloc, ",\"shard_status\":");
    try appendIndexRuntimeStatus(alloc, out, index_name, index_type, embeddings_coverage_policy, embeddings_sparse, coverage_generation, coverage_config_hash, graph_source_status, expected_group_ids, local_statuses, status_lookup, true);
    try out.append(alloc, '}');
}

fn appendIndexConfig(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    index_name: []const u8,
    config: std.json.Value,
) !void {
    return appendPublicIndexConfig(alloc, out, index_name, config, true);
}

fn appendPublicIndexConfig(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    index_name: []const u8,
    config: std.json.Value,
    canonicalize_enrichments: bool,
) !void {
    if (config != .object) return error.InvalidTableIndexMetadata;
    const index_type = inferIndexType(index_name, config) orelse return error.InvalidTableIndexMetadata;

    try out.append(alloc, '{');
    try appendJsonString(alloc, out, "name");
    try out.append(alloc, ':');
    try appendJsonString(alloc, out, index_name);
    if (config.object.get("type") == null) {
        try out.append(alloc, ',');
        try appendJsonString(alloc, out, "type");
        try out.append(alloc, ':');
        try appendJsonString(alloc, out, switch (index_type) {
            .full_text => "full_text",
            .embeddings => "embeddings",
            .graph => "graph",
            .algebraic => "algebraic",
        });
    }

    var it = config.object.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "name") or
            std.mem.eql(u8, entry.key_ptr.*, coverage_policy_mod.incarnation_field)) continue;
        if (!public_index_contract.isAllowedConfigField(index_type, entry.key_ptr.*)) continue;
        if (public_index_contract.isWriteOnlyConfigField(entry.key_ptr.*)) continue;
        if (isSensitivePublicConfigField(entry.key_ptr.*)) continue;
        if (isSensitivePublicConfigValue(entry.key_ptr.*, entry.value_ptr.*)) continue;
        if (!public_index_contract.rootFieldValueMatches(index_type, entry.key_ptr.*, entry.value_ptr.*)) continue;
        const object_shape = public_index_contract.createdObjectShapeForRootField(index_type, entry.key_ptr.*);
        if (!public_index_contract.createdValueMatchesShape(object_shape, entry.value_ptr.*)) continue;
        try out.append(alloc, ',');
        try appendJsonString(alloc, out, entry.key_ptr.*);
        try out.append(alloc, ':');
        if (canonicalize_enrichments and std.mem.eql(u8, entry.key_ptr.*, "enrichments")) {
            try appendCanonicalIndexEnrichments(alloc, out, entry.value_ptr.*);
        } else {
            try appendPublicConfigValue(
                alloc,
                out,
                entry.value_ptr.*,
                entry.key_ptr.*,
                object_shape,
            );
        }
    }
    try out.append(alloc, '}');
}

fn isSensitivePublicConfigField(field: []const u8) bool {
    if (std.ascii.eqlIgnoreCase(field, "authorization") or
        std.ascii.eqlIgnoreCase(field, "proxy-authorization") or
        std.ascii.eqlIgnoreCase(field, "cookie") or
        std.ascii.eqlIgnoreCase(field, "set-cookie") or
        std.ascii.eqlIgnoreCase(field, "credentials_path") or
        std.ascii.eqlIgnoreCase(field, "private_key") or
        std.ascii.eqlIgnoreCase(field, "secret")) return true;
    return normalizedCredentialNameIsSensitive(field);
}

fn normalizedCredentialNameIsSensitive(name: []const u8) bool {
    var normalized_buffer: [128]u8 = undefined;
    if (name.len > normalized_buffer.len) return true;
    var normalized_len: usize = 0;
    for (name) |byte| {
        if (!std.ascii.isAlphanumeric(byte)) continue;
        normalized_buffer[normalized_len] = std.ascii.toLower(byte);
        normalized_len += 1;
    }
    const normalized = normalized_buffer[0..normalized_len];
    if (normalized.len == 0) return true;
    const exact_sensitive = [_][]const u8{
        "auth", "code", "cookie", "credentials", "key", "password", "secret", "sig", "token", "xauth",
    };
    for (exact_sensitive) |candidate| {
        if (std.mem.eql(u8, normalized, candidate)) return true;
    }
    const sensitive_components = [_][]const u8{
        "apikey",
        "accesskey",
        "secretkey",
        "privatekey",
        "subscriptionkey",
        "authtoken",
        "authkey",
    };
    for (sensitive_components) |component| {
        if (std.mem.indexOf(u8, normalized, component) != null) return true;
    }
    const sensitive_suffixes = [_][]const u8{
        "password", "passwd", "secret", "token", "credential", "signature", "authorization",
    };
    for (sensitive_suffixes) |suffix| {
        if (std.mem.endsWith(u8, normalized, suffix)) return true;
    }
    return false;
}

fn isSensitivePublicConfigValue(field: []const u8, value: std.json.Value) bool {
    if (value != .string) return false;
    // Secret-store references are implementation details and can disclose
    // credential inventory even when they do not contain the secret value.
    if (std.mem.indexOf(u8, value.string, "${secret:") != null) return true;
    if (!isPublicProviderUrlField(field)) return false;
    return urlContainsCredentials(value.string);
}

fn isPublicProviderUrlField(field: []const u8) bool {
    return std.ascii.eqlIgnoreCase(field, "url") or
        std.ascii.eqlIgnoreCase(field, "api_url") or
        std.ascii.eqlIgnoreCase(field, "base_url") or
        std.ascii.eqlIgnoreCase(field, "endpoint") or
        std.ascii.eqlIgnoreCase(field, "endpoint_url");
}

fn urlContainsCredentials(url: []const u8) bool {
    const authority_start = if (std.mem.indexOf(u8, url, "://")) |scheme_end|
        scheme_end + 3
    else if (std.mem.startsWith(u8, url, "//"))
        @as(usize, 2)
    else
        null;
    if (authority_start) |start| {
        var authority_end = url.len;
        for (url[start..], start..) |byte, index| {
            if (byte == '/' or byte == '?' or byte == '#') {
                authority_end = index;
                break;
            }
        }
        if (std.mem.indexOfScalar(u8, url[start..authority_end], '@') != null) return true;
    }

    const fragment_start = std.mem.indexOfScalar(u8, url, '#');
    if (std.mem.indexOfScalar(u8, url, '?')) |query_marker| {
        if (fragment_start == null or query_marker < fragment_start.?) {
            const query_end = fragment_start orelse url.len;
            if (urlParameterListContainsCredentials(url[query_marker + 1 .. query_end])) return true;
        }
    }
    if (fragment_start) |marker| {
        return urlParameterListContainsCredentials(url[marker + 1 ..]);
    }
    return false;
}

fn urlParameterListContainsCredentials(encoded_parameters: []const u8) bool {
    var parameters = std.mem.tokenizeAny(u8, encoded_parameters, "&;");
    while (parameters.next()) |parameter| {
        const key = parameter[0 .. std.mem.indexOfScalar(u8, parameter, '=') orelse parameter.len];
        if (urlQueryKeyIsSensitive(key)) return true;
    }
    return false;
}

fn urlQueryKeyIsSensitive(encoded: []const u8) bool {
    var decoded_buffer: [128]u8 = undefined;
    if (encoded.len > decoded_buffer.len) return true;
    var decoded_len: usize = 0;
    var index: usize = 0;
    while (index < encoded.len) {
        if (encoded[index] == '%') {
            if (index + 2 >= encoded.len) return true;
            const high = std.fmt.charToDigit(encoded[index + 1], 16) catch return true;
            const low = std.fmt.charToDigit(encoded[index + 2], 16) catch return true;
            decoded_buffer[decoded_len] = @intCast((high << 4) | low);
            decoded_len += 1;
            index += 3;
            continue;
        }
        decoded_buffer[decoded_len] = if (encoded[index] == '+') ' ' else encoded[index];
        decoded_len += 1;
        index += 1;
    }
    const key = std.mem.trim(u8, decoded_buffer[0..decoded_len], &std.ascii.whitespace);
    return isSensitivePublicConfigField(key);
}

fn appendPublicConfigValue(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    value: std.json.Value,
    field_name: ?[]const u8,
    object_shape: public_index_contract.CreatedObjectShape,
) !void {
    if (field_name) |name| {
        if (std.ascii.endsWithIgnoreCase(name, "_json") and value == .string) {
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, value.string, .{}) catch {
                // Opaque producer documents can contain provider credentials.
                // An invalid document should already have failed validation;
                // fail closed here instead of reflecting it to a client.
                return appendJsonString(alloc, out, "[redacted]");
            };
            defer parsed.deinit();
            var nested = std.ArrayListUnmanaged(u8).empty;
            defer nested.deinit(alloc);
            try appendPublicConfigValue(alloc, &nested, parsed.value, null, .unrestricted);
            return appendJsonString(alloc, out, nested.items);
        }
    }

    switch (value) {
        .object => |object| {
            try out.append(alloc, '{');
            var first = true;
            var it = object.iterator();
            while (it.next()) |entry| {
                if (!public_index_contract.isAllowedCreatedObjectField(object_shape, entry.key_ptr.*)) continue;
                // Asset producers are intentionally opaque and may gain new
                // provider-specific credential fields at any time. A
                // deny-list cannot safely project them into a public response,
                // so preserve the table-status invariant and omit the entire
                // write-only document.
                if (public_index_contract.isWriteOnlyConfigField(entry.key_ptr.*)) continue;
                if (isSensitivePublicConfigField(entry.key_ptr.*)) continue;
                if (isSensitivePublicConfigValue(entry.key_ptr.*, entry.value_ptr.*)) continue;
                if (!public_index_contract.createdFieldValueMatches(object_shape, entry.key_ptr.*, entry.value_ptr.*)) continue;
                const child_shape = public_index_contract.createdObjectShapeForChild(object_shape, entry.key_ptr.*);
                if (!public_index_contract.createdValueMatchesShape(child_shape, entry.value_ptr.*)) continue;
                if (!first) try out.append(alloc, ',');
                first = false;
                try appendJsonString(alloc, out, entry.key_ptr.*);
                try out.append(alloc, ':');
                try appendPublicConfigValue(
                    alloc,
                    out,
                    entry.value_ptr.*,
                    entry.key_ptr.*,
                    child_shape,
                );
            }
            try out.append(alloc, '}');
        },
        .array => |array| {
            try out.append(alloc, '[');
            const item_shape = public_index_contract.createdObjectShapeForArrayItem(object_shape);
            var first = true;
            for (array.items) |item| {
                if (!public_index_contract.createdValueMatchesShape(item_shape, item)) continue;
                if (!first) try out.append(alloc, ',');
                first = false;
                try appendPublicConfigValue(alloc, out, item, null, item_shape);
            }
            try out.append(alloc, ']');
        },
        else => try appendJsonValue(alloc, out, value),
    }
}

fn appendCanonicalIndexEnrichments(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    value: std.json.Value,
) !void {
    if (value != .array) {
        const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
        return;
    }
    try out.append(alloc, '[');
    for (value.array.items, 0..) |item, i| {
        if (i > 0) try out.append(alloc, ',');
        switch (item) {
            .string => |name| try appendJsonString(alloc, out, name),
            .object => |object| {
                const name = object.get("name") orelse return error.InvalidTableIndexMetadata;
                if (name != .string) return error.InvalidTableIndexMetadata;
                try appendJsonString(alloc, out, name.string);
            },
            else => return error.InvalidTableIndexMetadata,
        }
    }
    try out.append(alloc, ']');
}

fn canonicalIndexConfigJson(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    config: std.json.Value,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try appendIndexConfig(alloc, &out, index_name, config);
    return try out.toOwnedSlice(alloc);
}

const EmbeddingsCoveragePolicy = coverage_policy_mod.Policy;

fn expectedCoverageConfigHash(alloc: std.mem.Allocator, index_name: []const u8, config: std.json.Value) !u64 {
    const stored = try managed_embedder.translateEmbeddingsIndexConfigJson(alloc, index_name, config);
    defer alloc.free(stored);
    return try internal_keys.derivedCoverageConfigFingerprint(alloc, stored);
}

fn embeddingsCoveragePolicyName(policy: EmbeddingsCoveragePolicy) []const u8 {
    return switch (policy) {
        .strict => "strict",
        .partial => "partial",
        .best_effort => "best_effort",
        .external => "external",
    };
}

fn embeddingsCoveragePolicyAllowsSkips(policy: EmbeddingsCoveragePolicy) bool {
    return policy == .partial or policy == .best_effort or policy == .external;
}

fn embeddingsCoveragePolicyRequiresTableCoverage(policy: EmbeddingsCoveragePolicy) bool {
    return policy == .strict;
}

fn embeddingsCoveragePolicy(config: std.json.Value) EmbeddingsCoveragePolicy {
    if (config != .object) return .strict;
    if (config.object.get("external")) |external| {
        switch (external) {
            .bool => |value| if (value) return .external,
            else => {},
        }
    }
    if (config.object.get("coverage_policy")) |policy| {
        return coverage_policy_mod.parse(policy) catch .strict;
    }
    return .strict;
}

fn embeddingsIsSparse(config: std.json.Value) bool {
    if (config != .object) return false;
    const sparse = config.object.get("sparse") orelse return false;
    return switch (sparse) {
        .bool => |value| value,
        else => false,
    };
}

const GraphSourceStatus = struct {
    artifact: []const u8,
    path: []const u8 = "",
    format: []const u8 = "extraction_relation",
};

fn graphSourceStatus(config: std.json.Value) ?GraphSourceStatus {
    if (config != .object) return null;
    const source = config.object.get("source") orelse return null;
    if (source != .object) return null;
    const kind = source.object.get("kind") orelse return null;
    if (kind != .string or !std.mem.eql(u8, kind.string, "artifact")) return null;
    const artifact = source.object.get("artifact") orelse return null;
    if (artifact != .string or artifact.string.len == 0) return null;
    return .{
        .artifact = artifact.string,
        .path = if (source.object.get("path")) |value| switch (value) {
            .string => value.string,
            else => "",
        } else "",
        .format = if (source.object.get("format")) |value| switch (value) {
            .string => value.string,
            else => "extraction_relation",
        } else "extraction_relation",
    };
}

fn indexTypeName(index_type: ApiIndexType) []const u8 {
    return switch (index_type) {
        .full_text => "full_text",
        .embeddings => "embeddings",
        .graph => "graph",
        .algebraic => "algebraic",
    };
}

fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const escaped = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(escaped);
    try out.appendSlice(alloc, escaped);
}

fn appendJsonValue(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: std.json.Value) !void {
    const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
}

fn validateEnrichmentConfigName(alloc: std.mem.Allocator, enrichment_name: []const u8, enrichment_json: []const u8) !void {
    var parsed = std.json.parseFromSlice(db_mod.types.EnrichmentConfig, alloc, enrichment_json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    }) catch return error.InvalidExtensionEnrichment;
    defer parsed.deinit();
    if (!std.mem.eql(u8, parsed.value.name, enrichment_name)) return error.InvalidExtensionEnrichment;
}

fn enrichmentValueNameEquals(value: std.json.Value, enrichment_name: []const u8) bool {
    if (value != .object) return false;
    const name_value = value.object.get("name") orelse return false;
    return name_value == .string and std.mem.eql(u8, name_value.string, enrichment_name);
}

fn appendEnrichmentArrayWithReplacement(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    value: std.json.Value,
    enrichment_name: []const u8,
    replacement: std.json.Value,
) !void {
    if (value != .array) return error.InvalidTableIndexMetadata;
    try out.append(alloc, '[');
    var first = true;
    for (value.array.items) |item| {
        if (enrichmentValueNameEquals(item, enrichment_name)) continue;
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonValue(alloc, out, item);
    }
    if (!first) try out.append(alloc, ',');
    try appendJsonValue(alloc, out, replacement);
    try out.append(alloc, ']');
}

fn appendEnrichmentArrayWithoutName(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    value: std.json.Value,
    enrichment_name: []const u8,
) !void {
    if (value != .array) return error.InvalidTableIndexMetadata;
    try out.append(alloc, '[');
    var first = true;
    for (value.array.items) |item| {
        if (enrichmentValueNameEquals(item, enrichment_name)) continue;
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonValue(alloc, out, item);
    }
    try out.append(alloc, ']');
}

fn appendAlgebraicIndexStatsFields(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    item: anytype,
) !void {
    var stats = indexes_openapi.AlgebraicIndexStats{
        .index_type = .algebraic,
        .healthy = item.algebraic_parse_error_count == 0,
        .parse_error_count = saturatingI64(item.algebraic_parse_error_count),
        .schema_version = saturatingI64(item.algebraic_schema_version),
        .capability_lifecycle_status = item.algebraic_capability_lifecycle_status orelse "current",
        .planner_selected = saturatingI64(item.algebraic_planner_selected),
        .planner_fallback_count = saturatingI64(item.algebraic_planner_fallback_count),
        .planner_last_decision = item.algebraic_planner_last_decision,
        .planner_last_fallback_reason = item.algebraic_planner_last_fallback_reason,
        .planner_last_estimated_scan_rows = if (item.algebraic_planner_last_estimated_scan_rows) |value| saturatingI64(value) else null,
        .planner_last_estimated_result_buckets = if (item.algebraic_planner_last_estimated_result_buckets) |value| saturatingI64(value) else null,
        .planner_lifecycle_ready = item.algebraic_planner_lifecycle_ready,
        .planner_lifecycle_blocking_reason = item.algebraic_planner_lifecycle_blocking_reason,
        .adaptive_progress_count = saturatingI64(item.algebraic_adaptive_progress_count),
        .recommendation_count = saturatingI64(item.algebraic_recommendation_count),
        .adaptive_backfilling_count = saturatingI64(item.algebraic_adaptive_backfilling_count),
        .adaptive_ready_count = saturatingI64(item.algebraic_adaptive_ready_count),
        .adaptive_stale_count = saturatingI64(item.algebraic_adaptive_stale_count),
        .adaptive_cleanup_recommended_count = saturatingI64(item.algebraic_adaptive_dematerialize_recommended_count),
        .last_error_reason = item.algebraic_last_error_reason,
    };
    if (item.algebraic_active_progress) |progress_status| {
        stats.active_progress_lifecycle = progress_status.lifecycle;
        stats.active_progress_rows_processed = saturatingI64(progress_status.rows_processed);
        stats.active_progress_target_rows = saturatingI64(progress_status.target_rows);
    }

    const encoded = try std.json.Stringify.valueAlloc(alloc, stats, .{ .emit_null_optional_fields = false });
    defer alloc.free(encoded);
    if (encoded.len <= 2) return;
    try out.append(alloc, ',');
    try out.appendSlice(alloc, encoded[1 .. encoded.len - 1]);
}

fn saturatingI64(value: u64) i64 {
    return std.math.cast(i64, value) orelse std.math.maxInt(i64);
}

fn appendIndexRuntimeStatus(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    index_name: []const u8,
    index_type: ApiIndexType,
    embeddings_coverage_policy: EmbeddingsCoveragePolicy,
    embeddings_sparse: bool,
    coverage_generation: u64,
    coverage_config_hash: u64,
    graph_source_status: ?GraphSourceStatus,
    expected_group_ids: []const u64,
    local_statuses: ?*const runtime_status.LocalTableRuntimeStatuses,
    status_lookup: *const RuntimeStatusLookup,
    shard_view: bool,
) !void {
    if (shard_view) {
        try out.append(alloc, '{');
        var emitted = false;
        var emitted_expected: []bool = &.{};
        if (expected_group_ids.len > 0) {
            emitted_expected = try alloc.alloc(bool, expected_group_ids.len);
            @memset(emitted_expected, false);
        }
        defer if (emitted_expected.len > 0) alloc.free(emitted_expected);

        if (local_statuses) |runtime| {
            for (runtime.items, 0..) |item_runtime, runtime_index| {
                const expected_index = if (expected_group_ids.len > 0)
                    status_lookup.expectedGroupIndex(expected_group_ids, item_runtime.group_id) orelse continue
                else
                    null;
                const item = status_lookup.findIndex(runtime_index, index_name) orelse continue;
                if (expected_index) |i| emitted_expected[i] = true;
                if (emitted) try out.append(alloc, ',');
                emitted = true;
                const key = if (item_runtime.group_id != 0)
                    try std.fmt.allocPrint(alloc, "{d}", .{item_runtime.group_id})
                else
                    try alloc.dupe(u8, "local");
                defer alloc.free(key);
                try appendJsonString(alloc, out, key);
                try out.append(alloc, ':');
                try appendSingleIndexRuntimeStatus(alloc, out, index_type, item, item_runtime.stats.source_doc_count, embeddings_coverage_policy, embeddings_sparse, coverage_generation, coverage_config_hash, graph_source_status, item_runtime.stats.async_indexing, if (index_type == .embeddings) item_runtime.stats.enrichment else null, item_runtime.stats.resolution, item_runtime.stats.promotion, item_runtime.stats.resolver_replay, item_runtime.metadata, runtime_status.statusHasRuntimeFacts(item_runtime));
            }
        }
        if (expected_group_ids.len > 0) {
            const missing = missingAggregateIndexStatus(1);
            for (expected_group_ids, 0..) |group_id, i| {
                if (emitted_expected[i]) continue;
                if (emitted) try out.append(alloc, ',');
                emitted = true;
                const key = try std.fmt.allocPrint(alloc, "{d}", .{group_id});
                defer alloc.free(key);
                try appendJsonString(alloc, out, key);
                try out.append(alloc, ':');
                try appendSingleIndexRuntimeStatus(alloc, out, index_type, missing, 0, embeddings_coverage_policy, embeddings_sparse, coverage_generation, coverage_config_hash, graph_source_status, .{}, null, null, null, .{}, .{
                    .source = .synthetic_config,
                    .freshness = .missing,
                }, false);
            }
        }
        try out.append(alloc, '}');
        return;
    }

    const aggregate = if (local_statuses) |runtime|
        aggregateIndexStatusIndexed(runtime.items, index_name, expected_group_ids, coverage_generation, coverage_config_hash, status_lookup) orelse
            if (expected_group_ids.len > 0) missingAggregateIndexStatus(expected_group_ids.len) else null
    else if (expected_group_ids.len > 0)
        missingAggregateIndexStatus(expected_group_ids.len)
    else
        null;
    const item = aggregate orelse {
        try appendMinimalIndexRuntimeStatus(alloc, out, index_type);
        return;
    };
    try appendSingleIndexRuntimeStatus(alloc, out, index_type, item, item.table_doc_count, embeddings_coverage_policy, embeddings_sparse, coverage_generation, coverage_config_hash, graph_source_status, item.async_indexing, if (index_type == .embeddings) item.enrichment else null, item.resolution, item.promotion, item.resolver_replay, null, item.runtime_present);
}

fn appendMinimalIndexRuntimeStatus(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    index_type: ApiIndexType,
) !void {
    try out.appendSlice(alloc, "{\"index_type\":");
    try appendJsonString(alloc, out, indexTypeName(index_type));
    try out.append(alloc, '}');
}

/// Collapse the durable repair state machine into the small vocabulary that is
/// useful to an application or an operator scanning ordinary index status.
/// Detailed phases, identities, retry deadlines, and sequence counters remain
/// available through internal diagnostics.
fn publicIndexRepairState(item: anytype) ?[]const u8 {
    const T = @TypeOf(item);
    if (@hasField(T, "repair_state")) return item.repair_state;
    if (@hasField(T, "index_repair_status")) {
        if (item.index_repair_status) |status| return @tagName(status);
    }
    if (!@hasField(T, "index_repair_id")) return null;
    const status = index_repair_status.summarize(
        item.index_repair_id != null,
        item.index_repair_automation,
        item.index_repair_phase,
        item.index_repair_wait_reason,
    ) orelse return null;
    return @tagName(status);
}

fn repairStateRank(state: []const u8) u8 {
    // Aggregate toward the least-progressing shard so a partially stalled
    // distributed index is not presented as uniformly rebuilding.
    if (std.mem.eql(u8, state, "failed")) return 5;
    if (std.mem.eql(u8, state, "paused")) return 4;
    if (std.mem.eql(u8, state, "waiting")) return 3;
    if (std.mem.eql(u8, state, "rebuilding")) return 1;
    return 0;
}

test "index repair aggregation exposes a waiting shard over rebuilding shards" {
    try std.testing.expect(repairStateRank("waiting") > repairStateRank("rebuilding"));
    try std.testing.expect(repairStateRank("failed") > repairStateRank("waiting"));
}

const AggregatedIndexStatus = struct {
    kind: ?db_mod.types.IndexKind = null,
    load_error: ?[]const u8 = null,
    repair_state: ?[]const u8 = null,
    repair_active_generation_serviceable: bool = true,
    backfill_active: bool = false,
    backfill_progress: f64 = 0.0,
    enrichment_failed: bool = false,
    repair_degraded: bool = false,
    repair_issue_count: u64 = 0,
    repair_summary_ready: bool = true,
    repair_issue_count_estimated: bool = false,
    table_doc_count: u64 = 0,
    doc_count: u64 = 0,
    term_count: u64 = 0,
    edge_count: u64 = 0,
    node_count: u64 = 0,
    root_node: u64 = 0,
    coverage_produced_count: u64 = 0,
    coverage_skipped_count: u64 = 0,
    coverage_terminal_failed_count: u64 = 0,
    coverage_generation: u64 = 0,
    coverage_config_hash: u64 = 0,
    coverage_identity_ready: bool = false,
    coverage_summary_ready: bool = true,
    coverage_config_mismatch_count: u64 = 0,
    replay_applied_sequence: u64 = 0,
    replay_target_sequence: u64 = 0,
    replay_catch_up_required: bool = false,
    catch_up_active: bool = false,
    catch_up_phase: db_mod.types.DenseCatchUpStats.Phase = .idle,
    catch_up_applied_sequence: u64 = 0,
    catch_up_target_sequence: u64 = 0,
    text_merge: db_mod.types.TextMergeStats = .{},
    hbc_cache: db_mod.types.HbcCacheStats = .{},
    hbc_posting: db_mod.types.HbcPostingStats = .{},
    async_indexing: db_mod.types.AsyncIndexingStats = .{},
    enrichment: db_mod.types.EnrichmentStats = .{},
    enrichment_observation_count: u64 = 0,
    resolution: db_mod.types.ReplayStageStats = .{},
    promotion: db_mod.types.ReplayStageStats = .{},
    resolver_replay: db_mod.types.ResolverReplayDiagnostics = .{},
    expected_group_count: u64 = 0,
    reported_group_count: u64 = 0,
    fresh_group_count: u64 = 0,
    stale_group_count: u64 = 0,
    missing_group_count: u64 = 0,
    remote_unknown_group_count: u64 = 0,
    unknown_group_count: u64 = 0,
    runtime_present: bool = false,
    runtime_fresh: bool = false,
    algebraic_parse_error_count: u64 = 0,
    algebraic_planner_selected: u64 = 0,
    algebraic_planner_fallback_count: u64 = 0,
    algebraic_planner_last_decision: ?[]const u8 = null,
    algebraic_planner_last_fallback_reason: ?[]const u8 = null,
    algebraic_planner_last_estimated_scan_rows: ?u64 = null,
    algebraic_planner_last_estimated_result_buckets: ?u64 = null,
    algebraic_planner_lifecycle_ready: bool = true,
    algebraic_planner_lifecycle_blocking_reason: ?[]const u8 = null,
    algebraic_graph_traversal_attempt_count: u64 = 0,
    algebraic_graph_traversal_proven_count: u64 = 0,
    algebraic_graph_traversal_rejected_count: u64 = 0,
    algebraic_graph_traversal_fallback_count: u64 = 0,
    algebraic_graph_traversal_result_node_count: u64 = 0,
    algebraic_recommendation_count: u64 = 0,
    algebraic_adaptive_progress_count: u64 = 0,
    algebraic_adaptive_backfilling_count: u64 = 0,
    algebraic_adaptive_ready_count: u64 = 0,
    algebraic_adaptive_stale_count: u64 = 0,
    algebraic_adaptive_dematerialize_recommended_count: u64 = 0,
    algebraic_last_error_reason: ?[]const u8 = null,
    algebraic_schema_version: u32 = 0,
    algebraic_capability_lifecycle_status: ?[]const u8 = null,
    algebraic_active_progress: ?db_mod.types.AlgebraicProgressStatus = null,
};

fn expectedGroupIndex(expected_group_ids: []const u64, group_id: u64) ?usize {
    for (expected_group_ids, 0..) |expected, i| {
        if (expected == group_id) return i;
    }
    return null;
}

fn expectedGroupAllowsStatus(expected_group_ids: []const u64, group_id: u64) bool {
    if (expected_group_ids.len == 0) return true;
    return expectedGroupIndex(expected_group_ids, group_id) != null;
}

fn missingAggregateIndexStatus(expected_group_count: usize) AggregatedIndexStatus {
    return .{
        .backfill_active = true,
        .replay_catch_up_required = true,
        .expected_group_count = @intCast(expected_group_count),
        .missing_group_count = @intCast(expected_group_count),
    };
}

fn statusFreshnessCountsAsFresh(metadata: runtime_status.RuntimeStatusMetadata) bool {
    return metadata.freshness == .fresh;
}

fn statusFreshnessName(freshness: runtime_status.RuntimeStatusFreshness) []const u8 {
    return @tagName(freshness);
}

fn statusSourceName(source: runtime_status.RuntimeStatusSource) []const u8 {
    return @tagName(source);
}

fn statusFreshnessCountsAsRemoteUnknown(metadata: runtime_status.RuntimeStatusMetadata) bool {
    return metadata.freshness == .remote_unknown;
}

fn aggregateIndexStatus(
    runtimes: []const runtime_status.LocalTableRuntimeStatus,
    index_name: []const u8,
    expected_group_ids: []const u64,
    coverage_config_hash: u64,
) ?AggregatedIndexStatus {
    return aggregateIndexStatusIndexed(runtimes, index_name, expected_group_ids, 0, coverage_config_hash, null);
}

fn aggregateIndexStatusIndexed(
    runtimes: []const runtime_status.LocalTableRuntimeStatus,
    index_name: []const u8,
    expected_group_ids: []const u64,
    coverage_generation: u64,
    coverage_config_hash: u64,
    status_lookup: ?*const RuntimeStatusLookup,
) ?AggregatedIndexStatus {
    var aggregate: AggregatedIndexStatus = .{ .coverage_config_hash = coverage_config_hash };
    var found = false;
    var materialization_count: usize = 0;
    var active_count: usize = 0;
    var active_progress_sum: f64 = 0.0;

    for (runtimes, 0..) |runtime, runtime_index| {
        const expected = if (status_lookup) |lookup|
            expected_group_ids.len == 0 or lookup.expectedGroupIndex(expected_group_ids, runtime.group_id) != null
        else
            expectedGroupAllowsStatus(expected_group_ids, runtime.group_id);
        if (!expected) continue;
        const item = if (status_lookup) |lookup|
            lookup.findIndex(runtime_index, index_name) orelse continue
        else
            findIndexStatus(runtime.stats.indexes, index_name) orelse continue;
        found = true;
        if (aggregate.kind == null) aggregate.kind = item.kind;
        const runtime_present = runtime_status.statusHasRuntimeFacts(runtime);
        if (!runtime_present) continue;
        aggregate.reported_group_count += 1;
        aggregate.runtime_present = true;
        // Integrity failures are current index-scoped facts even when the
        // surrounding runtime snapshot is stale or failed. Capture them before
        // gating materialization counters on freshness.
        if (item.load_error != null and aggregate.load_error == null) aggregate.load_error = item.load_error;
        if (statusFreshnessCountsAsFresh(runtime.metadata)) {
            aggregate.fresh_group_count += 1;
            aggregate.runtime_fresh = true;
        } else if (statusFreshnessCountsAsRemoteUnknown(runtime.metadata)) {
            aggregate.remote_unknown_group_count += 1;
        } else if (runtime.metadata.freshness == .unknown) {
            aggregate.unknown_group_count += 1;
        } else {
            aggregate.stale_group_count += 1;
        }
        const observation_current = statusFreshnessCountsAsFresh(runtime.metadata) and
            coverageIdentityMatches(item, coverage_generation, coverage_config_hash);
        // Coverage is projected only from current observations. Stale groups
        // remain visible in diagnostics but cannot contribute cardinality or
        // outcomes to a complete aggregate.
        if (statusFreshnessCountsAsFresh(runtime.metadata)) {
            aggregate.table_doc_count +|= runtime.stats.source_doc_count;
            if (!observation_current) {
                aggregate.coverage_config_mismatch_count += 1;
            } else if (!item.coverage_summary_ready) {
                aggregate.coverage_summary_ready = false;
            } else {
                aggregate.coverage_produced_count +|= item.coverage_produced_count;
                aggregate.coverage_skipped_count +|= item.coverage_skipped_count;
                aggregate.coverage_terminal_failed_count +|= item.coverage_terminal_failed_count;
            }
        }
        // Materialization and replay facts are scoped to the index
        // incarnation. Retaining an old runtime observation is useful for
        // diagnostics, but publishing its counts as the new index would make
        // delete/recreate and schema replacement appear query-ready before
        // repair has produced the replacement artifact.
        if (!observation_current) {
            aggregate.backfill_active = true;
            aggregate.replay_catch_up_required = true;
            continue;
        }
        materialization_count += 1;
        if (publicIndexRepairState(item)) |state| {
            aggregate.repair_active_generation_serviceable =
                aggregate.repair_active_generation_serviceable and
                item.index_repair_active_generation_serviceable;
            if (aggregate.repair_state == null or repairStateRank(state) > repairStateRank(aggregate.repair_state.?)) {
                aggregate.repair_state = state;
            }
        }
        aggregate.doc_count += item.doc_count;
        aggregate.term_count += item.term_count;
        aggregate.edge_count += item.edge_count;
        aggregate.node_count += item.node_count;
        aggregate.root_node = if (materialization_count == 1) item.root_node else 0;
        aggregate.replay_applied_sequence += item.replay_applied_sequence;
        aggregate.replay_target_sequence += item.replay_target_sequence;
        if (item.replay_catch_up_required) aggregate.replay_catch_up_required = true;
        if (item.enrichment_failed) aggregate.enrichment_failed = true;
        if (item.repair_degraded) aggregate.repair_degraded = true;
        aggregate.repair_issue_count += item.repair_issue_count;
        if (!item.repair_summary_ready) aggregate.repair_summary_ready = false;
        if (item.repair_issue_count_estimated) aggregate.repair_issue_count_estimated = true;
        aggregate.catch_up_applied_sequence += item.catch_up_applied_sequence;
        aggregate.catch_up_target_sequence += item.catch_up_target_sequence;
        if (item.catch_up_active) aggregate.catch_up_active = true;
        if (@intFromEnum(item.catch_up_phase) > @intFromEnum(aggregate.catch_up_phase)) aggregate.catch_up_phase = item.catch_up_phase;
        aggregateTextMergeStats(&aggregate.text_merge, item.text_merge);
        aggregateHbcCacheStats(&aggregate.hbc_cache, item.hbc_cache);
        aggregateHbcPostingStats(&aggregate.hbc_posting, item.hbc_posting);
        aggregate.algebraic_parse_error_count += item.algebraic_parse_error_count;
        aggregate.algebraic_planner_selected += item.algebraic_planner_selected;
        aggregate.algebraic_planner_fallback_count += item.algebraic_planner_fallback_count;
        if (item.algebraic_planner_last_decision != null) aggregate.algebraic_planner_last_decision = item.algebraic_planner_last_decision;
        if (item.algebraic_planner_last_fallback_reason != null) aggregate.algebraic_planner_last_fallback_reason = item.algebraic_planner_last_fallback_reason;
        if (item.algebraic_planner_last_estimated_scan_rows != null) aggregate.algebraic_planner_last_estimated_scan_rows = item.algebraic_planner_last_estimated_scan_rows;
        if (item.algebraic_planner_last_estimated_result_buckets != null) aggregate.algebraic_planner_last_estimated_result_buckets = item.algebraic_planner_last_estimated_result_buckets;
        if (!item.algebraic_planner_lifecycle_ready) aggregate.algebraic_planner_lifecycle_ready = false;
        if (item.algebraic_planner_lifecycle_blocking_reason != null) aggregate.algebraic_planner_lifecycle_blocking_reason = item.algebraic_planner_lifecycle_blocking_reason;
        aggregate.algebraic_graph_traversal_attempt_count += item.algebraic_graph_traversal_attempt_count;
        aggregate.algebraic_graph_traversal_proven_count += item.algebraic_graph_traversal_proven_count;
        aggregate.algebraic_graph_traversal_rejected_count += item.algebraic_graph_traversal_rejected_count;
        aggregate.algebraic_graph_traversal_fallback_count += item.algebraic_graph_traversal_fallback_count;
        aggregate.algebraic_graph_traversal_result_node_count += item.algebraic_graph_traversal_result_node_count;
        aggregate.algebraic_recommendation_count += item.algebraic_recommendation_count;
        aggregate.algebraic_adaptive_progress_count += item.algebraic_adaptive_progress_count;
        aggregate.algebraic_adaptive_backfilling_count += item.algebraic_adaptive_backfilling_count;
        aggregate.algebraic_adaptive_ready_count += item.algebraic_adaptive_ready_count;
        aggregate.algebraic_adaptive_stale_count += item.algebraic_adaptive_stale_count;
        aggregate.algebraic_adaptive_dematerialize_recommended_count += item.algebraic_adaptive_dematerialize_recommended_count;
        if (item.algebraic_last_error_reason != null) aggregate.algebraic_last_error_reason = item.algebraic_last_error_reason;
        aggregate.algebraic_schema_version = @max(aggregate.algebraic_schema_version, item.algebraic_schema_version);
        if (item.algebraic_capability_lifecycle_status) |status| {
            if (aggregate.algebraic_capability_lifecycle_status == null or algebraicCapabilityLifecycleRanksHigher(status, aggregate.algebraic_capability_lifecycle_status.?)) {
                aggregate.algebraic_capability_lifecycle_status = status;
            }
        }
        if (item.algebraic_active_progress) |progress| {
            if (aggregate.algebraic_active_progress == null or algebraicProgressSummaryRanksHigher(progress, aggregate.algebraic_active_progress.?)) {
                aggregate.algebraic_active_progress = progress;
            }
        }
        db_mod.types.accumulateAsyncIndexingStats(&aggregate.async_indexing, runtime.stats.async_indexing);
        aggregateEnrichmentStats(
            &aggregate.enrichment,
            runtime.stats.enrichment,
            aggregate.enrichment_observation_count == 0,
        );
        aggregate.enrichment_observation_count +|= 1;
        aggregateReplayStageStats(&aggregate.resolution, runtime.stats.resolution);
        aggregateReplayStageStats(&aggregate.promotion, runtime.stats.promotion);
        aggregateResolverReplayDiagnostics(&aggregate.resolver_replay, runtime.stats.resolver_replay);
        if (item.backfill_active) {
            aggregate.backfill_active = true;
            active_count += 1;
            active_progress_sum += item.backfill_progress;
        }
    }

    aggregate.expected_group_count = if (expected_group_ids.len > 0)
        @intCast(expected_group_ids.len)
    else
        aggregate.reported_group_count;
    if (aggregate.fresh_group_count > 0 and aggregate.coverage_config_mismatch_count == 0) {
        aggregate.coverage_generation = coverage_generation;
        aggregate.coverage_config_hash = coverage_config_hash;
        aggregate.coverage_identity_ready = coverage_generation != 0;
    }
    aggregate.missing_group_count = aggregate.expected_group_count -| aggregate.reported_group_count;
    if (aggregate.expected_group_count != aggregate.fresh_group_count) {
        aggregate.enrichment.projection_checkpoint_identity_consistent = false;
        aggregate.enrichment.projection_checkpoint_generation = 0;
        aggregate.enrichment.projection_checkpoint_config_hash = 0;
    }
    if (aggregate.missing_group_count > 0 or aggregate.stale_group_count > 0 or aggregate.remote_unknown_group_count > 0) {
        aggregate.backfill_active = true;
        aggregate.replay_catch_up_required = true;
        if (active_count == 0) aggregate.backfill_progress = 0.0;
    }
    if (!found and expected_group_ids.len == 0) return null;
    if (active_count > 0) aggregate.backfill_progress = active_progress_sum / @as(f64, @floatFromInt(active_count));
    normalizeReadyFullTextAggregate(&aggregate);
    normalizeReadyEmbeddingsAggregate(&aggregate);
    return aggregate;
}

fn coverageIdentityMatches(item: anytype, expected_generation: u64, expected_config_hash: u64) bool {
    if (@hasField(@TypeOf(item), "coverage_config_hash") and item.coverage_config_hash != expected_config_hash) return false;
    if (expected_generation != 0) {
        if (!@hasField(@TypeOf(item), "coverage_identity_ready") or !item.coverage_identity_ready) return false;
        if (!@hasField(@TypeOf(item), "coverage_generation") or item.coverage_generation != expected_generation) return false;
    }
    return true;
}

fn normalizeReadyFullTextAggregate(aggregate: *AggregatedIndexStatus) void {
    const kind = aggregate.kind orelse return;
    if (kind != .full_text) return;
    if (aggregate.reported_group_count == 0 or aggregate.missing_group_count > 0 or aggregate.stale_group_count > 0 or aggregate.remote_unknown_group_count > 0) return;
    if (aggregate.replay_target_sequence == 0 or aggregate.replay_applied_sequence < aggregate.replay_target_sequence) return;
    if (aggregate.table_doc_count == 0 or aggregate.doc_count < aggregate.table_doc_count) return;

    aggregate.replay_catch_up_required = false;
    aggregate.catch_up_active = false;
    aggregate.backfill_active = false;
    aggregate.backfill_progress = 1.0;
}

fn normalizeReadyEmbeddingsAggregate(aggregate: *AggregatedIndexStatus) void {
    const kind = aggregate.kind orelse return;
    if (kind != .dense_vector and kind != .sparse_vector) return;
    if (aggregate.reported_group_count == 0 or
        aggregate.missing_group_count > 0 or
        aggregate.stale_group_count > 0 or
        aggregate.unknown_group_count > 0 or
        aggregate.remote_unknown_group_count > 0 or
        aggregate.expected_group_count != aggregate.fresh_group_count) return;
    if (aggregate.load_error != null or aggregate.repair_degraded or aggregate.enrichment_failed) return;
    const enrichment_blocked = aggregate.enrichment.enabled and (aggregate.enrichment.retrying or aggregate.enrichment.worker_failed);
    if (enrichment_blocked) return;
    const complete_materialization = aggregate.coverage_identity_ready and
        aggregate.coverage_summary_ready and
        aggregate.coverage_config_mismatch_count == 0 and
        aggregate.coverage_terminal_failed_count == 0 and
        coverageAllSourcesTerminal(
            aggregate.table_doc_count,
            aggregate.coverage_produced_count,
            aggregate.coverage_skipped_count,
            aggregate.coverage_terminal_failed_count,
        );
    if (aggregate.catch_up_active or
        (aggregate.catch_up_target_sequence > aggregate.catch_up_applied_sequence and !complete_materialization)) return;
    if (!complete_materialization) return;
    if (!embeddingsArtifactPublishComplete(aggregate.*, kind == .sparse_vector, aggregate.coverage_produced_count)) return;

    aggregate.replay_applied_sequence = @max(aggregate.replay_applied_sequence, aggregate.replay_target_sequence);
    aggregate.catch_up_applied_sequence = @max(aggregate.catch_up_applied_sequence, aggregate.catch_up_target_sequence);
    aggregate.replay_catch_up_required = false;
    aggregate.catch_up_active = false;
    aggregate.catch_up_phase = .idle;
    aggregate.backfill_active = false;
    aggregate.backfill_progress = 1.0;
}

fn algebraicProgressSummaryRanksHigher(
    progress: db_mod.types.AlgebraicProgressStatus,
    selected: db_mod.types.AlgebraicProgressStatus,
) bool {
    if (std.mem.eql(u8, progress.lifecycle, "backfilling") and !std.mem.eql(u8, selected.lifecycle, "backfilling")) return true;
    if (!std.mem.eql(u8, progress.lifecycle, selected.lifecycle)) return false;
    if (progress.target_sequence != selected.target_sequence) return progress.target_sequence > selected.target_sequence;
    return progress.rows_processed > selected.rows_processed;
}

fn algebraicCapabilityLifecycleRanksHigher(status: []const u8, selected: []const u8) bool {
    return algebraicCapabilityLifecycleRank(status) > algebraicCapabilityLifecycleRank(selected);
}

fn algebraicCapabilityLifecycleRank(status: []const u8) u8 {
    if (std.mem.eql(u8, status, "rebuild_required")) return 30;
    if (std.mem.eql(u8, status, "stale")) return 20;
    if (std.mem.eql(u8, status, "backfilling")) return 10;
    if (std.mem.eql(u8, status, "current")) return 0;
    return 5;
}

fn aggregateResolverReplayDiagnostics(dst: *db_mod.types.ResolverReplayDiagnostics, src: db_mod.types.ResolverReplayDiagnostics) void {
    dst.resolver_count += src.resolver_count;
    dst.resolution_runtime_present = dst.resolution_runtime_present or src.resolution_runtime_present;
    dst.resolution_worker_started = dst.resolution_worker_started or src.resolution_worker_started;
    dst.promotion_runtime_present = dst.promotion_runtime_present or src.promotion_runtime_present;
    dst.promotion_worker_started = dst.promotion_worker_started or src.promotion_worker_started;
    if (dst.resolvers.len == 0 and src.resolvers.len > 0) dst.resolvers = src.resolvers;
}

fn aggregateReplayStageStats(dst: *db_mod.types.ReplayStageStats, src: db_mod.types.ReplayStageStats) void {
    dst.enabled = dst.enabled or src.enabled;
    dst.target_sequence += src.target_sequence;
    dst.applied_sequence += src.applied_sequence;
    dst.catch_up_required = dst.catch_up_required or src.catch_up_required;
    dst.blocked = dst.blocked or src.blocked;
    if (dst.blocked_reason.len == 0 and src.blocked_reason.len > 0) dst.blocked_reason = src.blocked_reason;
    dst.error_count += src.error_count;
}

fn aggregateEnrichmentStats(
    dst: *db_mod.types.EnrichmentStats,
    src: db_mod.types.EnrichmentStats,
    first_observation: bool,
) void {
    dst.enabled = dst.enabled or src.enabled;
    dst.lease_owned = dst.lease_owned and src.lease_owned;
    dst.has_lease = dst.has_lease or src.has_lease;
    dst.acquisition_count +|= src.acquisition_count;
    dst.lease_acquire_failures +|= src.lease_acquire_failures;
    dst.lost_leases +|= src.lost_leases;
    dst.last_acquired_ms = @max(dst.last_acquired_ms, src.last_acquired_ms);
    dst.target_sequence +|= src.target_sequence;
    dst.applied_sequence +|= src.applied_sequence;
    if (first_observation) {
        dst.projection_checkpoint_status = src.projection_checkpoint_status;
        dst.projection_checkpoint_generation = src.projection_checkpoint_generation;
        dst.projection_checkpoint_config_hash = src.projection_checkpoint_config_hash;
        dst.projection_checkpoint_identity_consistent = src.projection_checkpoint_identity_consistent;
        if (!dst.projection_checkpoint_identity_consistent) {
            dst.projection_checkpoint_generation = 0;
            dst.projection_checkpoint_config_hash = 0;
        }
    } else {
        if (projectionCheckpointStatusRank(src.projection_checkpoint_status) >
            projectionCheckpointStatusRank(dst.projection_checkpoint_status))
        {
            dst.projection_checkpoint_status = src.projection_checkpoint_status;
        }
        if (dst.projection_checkpoint_generation != src.projection_checkpoint_generation or
            dst.projection_checkpoint_config_hash != src.projection_checkpoint_config_hash or
            !src.projection_checkpoint_identity_consistent)
        {
            dst.projection_checkpoint_identity_consistent = false;
            dst.projection_checkpoint_generation = 0;
            dst.projection_checkpoint_config_hash = 0;
        }
    }
    dst.projection_checkpoint_applied_sequence +|= src.projection_checkpoint_applied_sequence;
    dst.checkpoint_replay_tail_sequence_count +|= src.checkpoint_replay_tail_sequence_count;
    dst.processed_requests +|= src.processed_requests;
    dst.error_count +|= src.error_count;
    dst.retryable_error_count +|= src.retryable_error_count;
    dst.fatal_error_count +|= src.fatal_error_count;
    dst.consecutive_retry_count = @max(dst.consecutive_retry_count, src.consecutive_retry_count);
    // This field answers when the next shard becomes eligible, so aggregate
    // the earliest retrying observation. A later "all shards eligible" time is
    // a different gauge and must not delay the operator-visible next action.
    if (src.retrying and (!dst.retrying or src.next_retry_at_ms < dst.next_retry_at_ms)) {
        dst.next_retry_at_ms = src.next_retry_at_ms;
    }
    dst.retrying = dst.retrying or src.retrying;
    dst.worker_failed = dst.worker_failed or src.worker_failed;
    dst.worker_started = dst.worker_started or src.worker_started;
    dst.stalled = dst.stalled or src.stalled;
    dst.skip_by_hash_count +|= src.skip_by_hash_count;
    dst.skipped_source_count +|= src.skipped_source_count;
    dst.codec_decode_failures +|= src.codec_decode_failures;
    dst.embed_batches_started +|= src.embed_batches_started;
    dst.embed_batches_completed +|= src.embed_batches_completed;
    dst.embed_items_started +|= src.embed_items_started;
    dst.embed_items_completed +|= src.embed_items_completed;
    dst.active_embed_batch_items +|= src.active_embed_batch_items;
    dst.active_embed_batch_bytes +|= src.active_embed_batch_bytes;
    dst.active_embed_batch_max_bytes = @max(dst.active_embed_batch_max_bytes, src.active_embed_batch_max_bytes);
    if (src.active_embed_batch_started_ms != 0 and
        (dst.active_embed_batch_started_ms == 0 or src.active_embed_batch_started_ms < dst.active_embed_batch_started_ms))
    {
        dst.active_embed_batch_started_ms = src.active_embed_batch_started_ms;
    }
    if (src.last_embed_batch_completed_ms > dst.last_embed_batch_completed_ms or
        (src.last_embed_batch_completed_ms == dst.last_embed_batch_completed_ms and
            (src.last_embed_batch_ns > dst.last_embed_batch_ns or
                (src.last_embed_batch_ns == dst.last_embed_batch_ns and
                    (src.last_embed_batch_bytes > dst.last_embed_batch_bytes or
                        (src.last_embed_batch_bytes == dst.last_embed_batch_bytes and
                            src.last_embed_batch_items > dst.last_embed_batch_items))))))
    {
        dst.last_embed_batch_items = src.last_embed_batch_items;
        dst.last_embed_batch_bytes = src.last_embed_batch_bytes;
        dst.last_embed_batch_max_bytes = src.last_embed_batch_max_bytes;
        dst.last_embed_batch_completed_ms = src.last_embed_batch_completed_ms;
        dst.last_embed_batch_ns = src.last_embed_batch_ns;
    }
    dst.total_embed_ns +|= src.total_embed_ns;
    dst.dense_artifact_bytes_written +|= src.dense_artifact_bytes_written;
    dst.sparse_artifact_bytes_written +|= src.sparse_artifact_bytes_written;
    dst.chunk_artifact_bytes_written +|= src.chunk_artifact_bytes_written;
    dst.artifact_bytes_written +|= src.artifact_bytes_written;
}

fn projectionCheckpointStatusRank(status: []const u8) u8 {
    if (std.mem.eql(u8, status, "repair_required") or std.mem.eql(u8, status, "failed")) return 50;
    if (std.mem.eql(u8, status, "degraded")) return 40;
    if (std.mem.eql(u8, status, "retrying")) return 30;
    if (std.mem.eql(u8, status, "rebuilding")) return 20;
    if (std.mem.eql(u8, status, "clean")) return 0;
    return 10;
}

fn aggregateTextMergeStats(dst: *db_mod.types.TextMergeStats, src: db_mod.types.TextMergeStats) void {
    dst.active_indexes += src.active_indexes;
    dst.active_segments += src.active_segments;
    dst.max_active_segments_per_index = @max(dst.max_active_segments_per_index, src.max_active_segments_per_index);
    dst.pending_indexes += src.pending_indexes;
    dst.pending_segments += src.pending_segments;
    dst.pending_bytes += src.pending_bytes;
    dst.pending_heap_bytes += src.pending_heap_bytes;
    dst.pending_mmap_bytes += src.pending_mmap_bytes;
    dst.in_flight_merges += src.in_flight_merges;
    dst.in_flight_segments += src.in_flight_segments;
    dst.completed_merges += src.completed_merges;
    dst.skipped_stale_merges += src.skipped_stale_merges;
    dst.failed_merges += src.failed_merges;
    dst.merge_input_segments_total += src.merge_input_segments_total;
    dst.merge_input_bytes_total += src.merge_input_bytes_total;
    dst.merge_output_segments_total += src.merge_output_segments_total;
    dst.merge_output_bytes_total += src.merge_output_bytes_total;
    dst.last_merge_input_segments = @max(dst.last_merge_input_segments, src.last_merge_input_segments);
    dst.last_merge_input_bytes = @max(dst.last_merge_input_bytes, src.last_merge_input_bytes);
    dst.last_merge_output_segments = @max(dst.last_merge_output_segments, src.last_merge_output_segments);
    dst.last_merge_output_bytes = @max(dst.last_merge_output_bytes, src.last_merge_output_bytes);
    dst.quarantined_merges += src.quarantined_merges;
    dst.quarantined_segments += src.quarantined_segments;
    dst.deferred_for_pressure += src.deferred_for_pressure;
    if (dst.last_merge_error.len == 0 and src.last_merge_error.len > 0) dst.last_merge_error = src.last_merge_error;
    if (src.retry_after_ns > 0 and (dst.retry_after_ns == 0 or src.retry_after_ns < dst.retry_after_ns)) dst.retry_after_ns = src.retry_after_ns;
}

fn aggregateHbcCacheKindStats(dst: *db_mod.types.HbcCacheKindStats, src: db_mod.types.HbcCacheKindStats) void {
    dst.used_bytes += src.used_bytes;
    dst.peak_bytes += src.peak_bytes;
    dst.insertions += src.insertions;
    dst.admission_skips += src.admission_skips;
    dst.evictions += src.evictions;
}

fn aggregateHbcCacheStats(dst: *db_mod.types.HbcCacheStats, src: db_mod.types.HbcCacheStats) void {
    dst.total_bytes += src.total_bytes;
    dst.accounted_bytes += src.accounted_bytes;
    aggregateHbcCacheKindStats(&dst.node, src.node);
    aggregateHbcCacheKindStats(&dst.quantized, src.quantized);
    aggregateHbcCacheKindStats(&dst.vector, src.vector);
    aggregateHbcCacheKindStats(&dst.metadata, src.metadata);
}

fn aggregateHbcPostingStats(dst: *db_mod.types.HbcPostingStats, src: db_mod.types.HbcPostingStats) void {
    dst.scanned_nodes += src.scanned_nodes;
    dst.scanned_postings += src.scanned_postings;
    dst.dirty_postings += src.dirty_postings;
    dst.centroid_dirty_postings += src.centroid_dirty_postings;
    dst.payload_dirty_postings += src.payload_dirty_postings;
    dst.max_centroid_version_lag = @max(dst.max_centroid_version_lag, src.max_centroid_version_lag);
    dst.max_payload_version_lag = @max(dst.max_payload_version_lag, src.max_payload_version_lag);
    dst.max_mutation_version = @max(dst.max_mutation_version, src.max_mutation_version);
    dst.skipped_missing += src.skipped_missing;
    dst.maintenance_scanned_nodes += src.maintenance_scanned_nodes;
    dst.maintenance_scanned_postings += src.maintenance_scanned_postings;
    dst.maintenance_dirty_postings += src.maintenance_dirty_postings;
    dst.maintenance_repaired_postings += src.maintenance_repaired_postings;
    dst.maintenance_centroid_refreshed += src.maintenance_centroid_refreshed;
    dst.maintenance_payload_refreshed += src.maintenance_payload_refreshed;
    dst.maintenance_ancestor_refresh_roots += src.maintenance_ancestor_refresh_roots;
    dst.maintenance_split_postings += src.maintenance_split_postings;
    dst.maintenance_merged_postings += src.maintenance_merged_postings;
    dst.maintenance_boundary_reassigned_vectors += src.maintenance_boundary_reassigned_vectors;
    dst.lazy_centroid_deferrals += src.lazy_centroid_deferrals;
    dst.lazy_payload_deferrals += src.lazy_payload_deferrals;
    dst.lazy_ancestor_deferrals += src.lazy_ancestor_deferrals;
}

const EmbeddingsRuntimeView = struct {
    backfill_active: bool,
    backfill_progress: f64,
    coverage_degraded: bool,
    replay_applied_sequence: u64,
    replay_target_sequence: u64,
    replay_catch_up_required: bool,
};

const CoverageEvaluation = struct {
    covered: u64,
    settled: u64,
    uncovered: ?u64,
    pending: ?u64,
    complete: bool,
    healthy: bool,
    degraded: bool,
    source_visible: bool,
    counters_valid: bool,
};

const CoverageIncompleteReason = enum {
    runtime_unavailable,
    missing_group,
    unknown_group,
    remote_unknown_group,
    stale_group,
    summary_unavailable,
    config_mismatch,
    counter_mismatch,
};

fn coverageOutcomeTotal(produced: u64, skipped: u64, terminal_failed: u64) ?u64 {
    const produced_and_skipped = std.math.add(u64, produced, skipped) catch return null;
    return std.math.add(u64, produced_and_skipped, terminal_failed) catch null;
}

fn coverageCountersValid(source_total: u64, produced: u64, skipped: u64, terminal_failed: u64) bool {
    return db_mod.types.evaluateDerivedCoverageHealth(source_total, produced, skipped, terminal_failed, true, true).counters_valid;
}

fn coverageAllSourcesTerminal(source_total: u64, produced: u64, skipped: u64, terminal_failed: u64) bool {
    return db_mod.types.evaluateDerivedCoverageHealth(source_total, produced, skipped, terminal_failed, true, true).all_sources_terminal;
}

fn coverageReplayCurrent(applied_sequence: u64, target_sequence: u64, catch_up_required: bool) bool {
    return !catch_up_required and applied_sequence >= target_sequence;
}

fn evaluateCoverage(
    policy: EmbeddingsCoveragePolicy,
    source_total: u64,
    produced: u64,
    skipped: u64,
    terminal_failed: u64,
    observation_complete: bool,
    replay_current: bool,
) CoverageEvaluation {
    const assessment = db_mod.types.evaluateDerivedCoverageAssessment(
        switch (policy) {
            .strict, .external => .strict,
            .partial => .partial,
            .best_effort => .best_effort,
        },
        source_total,
        produced,
        skipped,
        terminal_failed,
        observation_complete,
        replay_current,
    );
    return .{
        .covered = assessment.covered,
        .settled = assessment.health.settled,
        .uncovered = if (observation_complete and assessment.health.counters_valid) source_total -| assessment.covered else null,
        .pending = assessment.health.pending,
        .complete = assessment.complete,
        .healthy = assessment.healthy,
        .degraded = assessment.degraded,
        .source_visible = source_total == 0 or assessment.covered > 0,
        .counters_valid = assessment.health.counters_valid,
    };
}

test "derived coverage evaluation is policy exact and observation gated" {
    const strict = evaluateCoverage(.strict, 3, 1, 1, 1, true, true);
    try std.testing.expectEqual(@as(u64, 1), strict.covered);
    try std.testing.expectEqual(@as(u64, 3), strict.settled);
    try std.testing.expectEqual(@as(?u64, 2), strict.uncovered);
    try std.testing.expectEqual(@as(?u64, 0), strict.pending);
    try std.testing.expect(!strict.complete);
    try std.testing.expect(strict.degraded);

    const partial = evaluateCoverage(.partial, 3, 1, 2, 0, true, true);
    try std.testing.expectEqual(@as(u64, 3), partial.covered);
    try std.testing.expect(partial.complete);
    try std.testing.expect(partial.healthy);

    const replay_pending = evaluateCoverage(.partial, 3, 1, 2, 0, true, false);
    try std.testing.expect(!replay_pending.complete);
    try std.testing.expect(!replay_pending.healthy);

    const best_effort = evaluateCoverage(.best_effort, 3, 1, 1, 1, true, true);
    try std.testing.expect(best_effort.complete);
    try std.testing.expect(!best_effort.healthy);
    try std.testing.expect(best_effort.degraded);

    const incomplete_observation = evaluateCoverage(.partial, 3, 1, 2, 0, false, true);
    try std.testing.expectEqual(@as(?u64, null), incomplete_observation.pending);
    try std.testing.expect(!incomplete_observation.complete);
    try std.testing.expect(!incomplete_observation.healthy);

    const excess_outcomes = evaluateCoverage(.partial, 2, 2, 1, 0, true, true);
    try std.testing.expectEqual(@as(u64, 3), excess_outcomes.covered);
    try std.testing.expectEqual(@as(?u64, null), excess_outcomes.pending);
    try std.testing.expect(!excess_outcomes.counters_valid);
    try std.testing.expect(!excess_outcomes.complete);

    const external_partial = evaluateCoverage(.external, 3, 1, 0, 0, true, true);
    try std.testing.expectEqual(@as(u64, 1), external_partial.covered);
    try std.testing.expectEqual(@as(?u64, 2), external_partial.pending);
    try std.testing.expect(!external_partial.complete);

    const external_complete = evaluateCoverage(.external, 3, 3, 0, 0, true, true);
    try std.testing.expect(external_complete.complete);
    try std.testing.expect(external_complete.healthy);
}

test "settled terminal enrichment debt is degraded rather than rebuilding" {
    const item = AggregatedIndexStatus{
        .coverage_produced_count = 2,
        .coverage_terminal_failed_count = 1,
        .coverage_generation = 42,
        .coverage_config_hash = 99,
        .coverage_identity_ready = true,
        .coverage_summary_ready = true,
        .backfill_active = true,
        .replay_applied_sequence = 5,
        .replay_target_sequence = 5,
    };
    const view = embeddingsRuntimeView(item, 3, .strict, false, 42, 99, .{
        .enabled = true,
        .applied_sequence = 5,
        .target_sequence = 5,
    }, true);
    try std.testing.expect(!view.backfill_active);
    try std.testing.expectEqual(@as(f64, 1.0), view.backfill_progress);
}

test "derived coverage aggregation rejects mixed config observations" {
    var indexes_a = [_]db_mod.types.DBIndexStats{.{
        .name = "visual",
        .kind = .dense_vector,
        .load_error = "IncompleteBulkPublish",
        .coverage_produced_count = 1,
        .coverage_config_hash = 41,
    }};
    var indexes_b = [_]db_mod.types.DBIndexStats{.{
        .name = "visual",
        .kind = .dense_vector,
        .coverage_produced_count = 99,
        .coverage_config_hash = 42,
    }};
    const runtimes = [_]runtime_status.LocalTableRuntimeStatus{
        .{ .group_id = 1, .metadata = .{ .source = .remote_store, .freshness = .fresh }, .stats = .{ .source_doc_count = 1, .index_count = 1, .indexes = indexes_a[0..] } },
        .{ .group_id = 2, .metadata = .{ .source = .remote_store, .freshness = .fresh }, .stats = .{ .source_doc_count = 1, .index_count = 1, .indexes = indexes_b[0..] } },
    };

    const aggregate = aggregateIndexStatus(&runtimes, "visual", &.{ 1, 2 }, 41) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u64, 2), aggregate.table_doc_count);
    try std.testing.expectEqual(@as(u64, 1), aggregate.coverage_produced_count);
    try std.testing.expectEqual(@as(u64, 1), aggregate.coverage_config_mismatch_count);
    try std.testing.expect(aggregateRuntimeCoverageIncomplete(aggregate, 0, 41));

    var reasons = std.ArrayListUnmanaged(u8).empty;
    defer reasons.deinit(std.testing.allocator);
    try appendCoverageIncompleteReasons(std.testing.allocator, &reasons, aggregate, 0, 41, true, null, 2);
    try std.testing.expectEqualStrings("[\"config_mismatch\"]", reasons.items);

    var missing_status = std.ArrayListUnmanaged(u8).empty;
    defer missing_status.deinit(std.testing.allocator);
    try appendSingleIndexRuntimeStatus(
        std.testing.allocator,
        &missing_status,
        .embeddings,
        missingAggregateIndexStatus(1),
        0,
        .partial,
        false,
        0,
        41,
        null,
        .{},
        null,
        null,
        null,
        .{},
        null,
        false,
    );
    try std.testing.expect(std.mem.indexOf(u8, missing_status.items, "\"observation_incomplete_reasons\":[\"runtime_unavailable\",\"missing_group\"]") != null);
    try std.testing.expect(std.mem.indexOf(u8, missing_status.items, "\"pending\":null") != null);
}

test "rebuild quarantine remains an explicit failed public index status" {
    const item = db_mod.types.DBIndexStats{
        .name = "search_idx",
        .kind = .full_text,
        .load_error = "InvalidRebuildState",
    };
    const runtimes = [_]runtime_status.LocalTableRuntimeStatus{.{
        .group_id = 1,
        .metadata = .{ .source = .rebuild_state_quarantine, .freshness = .failed },
        .stats = .{ .index_count = 1, .indexes = @constCast((&[_]db_mod.types.DBIndexStats{item})[0..]) },
    }};
    const aggregate = aggregateIndexStatus(&runtimes, "search_idx", &.{1}, 0) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("InvalidRebuildState", aggregate.load_error.?);

    var encoded = std.ArrayListUnmanaged(u8).empty;
    defer encoded.deinit(std.testing.allocator);
    try appendSingleIndexRuntimeStatus(
        std.testing.allocator,
        &encoded,
        .full_text,
        item,
        0,
        .strict,
        false,
        0,
        0,
        null,
        .{},
        null,
        null,
        null,
        .{},
        runtimes[0].metadata,
        true,
    );
    try std.testing.expect(std.mem.indexOf(u8, encoded.items, "\"backfill_state\":\"failed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.items, "\"error\":\"load failed: InvalidRebuildState\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.items, "\"runtime_source\":\"rebuild_state_quarantine\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.items, "\"runtime_freshness\":\"failed\"") != null);
}

test "index status exposes compact repair state without internal diagnostics" {
    const item = db_mod.types.DBIndexStats{
        .name = "visual",
        .kind = .dense_vector,
        .index_repair_id = 1,
        .index_repair_phase = "building",
        .index_repair_automation = "enabled",
        .index_repair_wait_reason = "none",
        .index_repair_attempts = 7,
        .index_repair_applied_sequence = 41,
        .index_repair_target_sequence = 99,
    };
    try std.testing.expectEqualStrings("rebuilding", publicIndexRepairState(item).?);

    var encoded = std.ArrayListUnmanaged(u8).empty;
    defer encoded.deinit(std.testing.allocator);
    try appendSingleIndexRuntimeStatus(
        std.testing.allocator,
        &encoded,
        .embeddings,
        item,
        0,
        .external,
        false,
        0,
        0,
        null,
        .{},
        null,
        null,
        null,
        .{},
        null,
        true,
    );
    try std.testing.expect(std.mem.indexOf(u8, encoded.items, "\"repair\":{\"state\":\"rebuilding\",\"action_required\":false}") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.items, "\"rebuilding\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.items, "\"backfill_state\":\"running\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.items, "load failed") == null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.items, "index_repair_id") == null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.items, "index_repair_phase") == null);

    var paused = item;
    paused.index_repair_automation = "paused";
    try std.testing.expectEqualStrings("paused", publicIndexRepairState(paused).?);

    var failed = item;
    failed.index_repair_phase = "terminal";
    try std.testing.expectEqualStrings("failed", publicIndexRepairState(failed).?);

    var corrupt = failed;
    corrupt.index_repair_id = null;
    try std.testing.expectEqualStrings("failed", publicIndexRepairState(corrupt).?);

    var waiting = item;
    waiting.index_repair_wait_reason = "backoff";
    try std.testing.expectEqualStrings("waiting", publicIndexRepairState(waiting).?);
}

test "complete partial embeddings coverage is ready after active generation proof" {
    const item = db_mod.types.DBIndexStats{
        .name = "thumbnail",
        .kind = .dense_vector,
        .doc_count = 1,
        .node_count = 1,
        .root_node = 1,
        .coverage_produced_count = 1,
        .coverage_skipped_count = 1,
        .coverage_generation = 42,
        .coverage_config_hash = 99,
        .coverage_identity_ready = true,
        .replay_applied_sequence = 7,
        .replay_target_sequence = 7,
        .backfill_active = true,
        .backfill_progress = 1.0,
        .repair_degraded = true,
        .index_repair_status = .rebuilding,
        .index_repair_active_generation_serviceable = true,
    };
    var encoded = std.ArrayListUnmanaged(u8).empty;
    defer encoded.deinit(std.testing.allocator);
    try appendSingleIndexRuntimeStatus(
        std.testing.allocator,
        &encoded,
        .embeddings,
        item,
        2,
        .partial,
        false,
        42,
        99,
        null,
        .{},
        .{
            .enabled = true,
            .target_sequence = 7,
            .applied_sequence = 7,
            .skipped_source_count = 1,
        },
        null,
        null,
        .{},
        null,
        true,
    );
    try ant_json.testing.expectSubsetJsonText(
        std.testing.allocator,
        "{\"rebuilding\":false,\"backfill_active\":false,\"backfill_state\":\"ready\",\"coverage\":{\"policy\":\"partial\",\"complete\":true,\"healthy\":true,\"pending\":0}}",
        encoded.items,
    );
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, encoded.items, .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.value.object.get("repair") == null);
}

test "derived coverage aggregation rejects stale index incarnations" {
    var indexes = [_]db_mod.types.DBIndexStats{.{
        .name = "visual",
        .kind = .dense_vector,
        .doc_count = 7,
        .term_count = 11,
        .node_count = 9,
        .root_node = 3,
        .coverage_produced_count = 1,
        .coverage_generation = 41,
        .coverage_config_hash = 99,
        .coverage_identity_ready = true,
        .replay_applied_sequence = 8,
        .replay_target_sequence = 8,
        .projection_checkpoint_status = "clean",
        .projection_checkpoint_applied_sequence = 8,
        .projection_checkpoint_generation = 41,
        .projection_checkpoint_config_hash = 99,
        .checkpoint_replay_tail_sequence_count = 3,
        .repair_degraded = true,
        .repair_issue_count = 5,
        .repair_summary_ready = true,
        .repair_scan_issue_count = 4,
        .hbc_cache = .{
            .total_bytes = 1024,
            .accounted_bytes = 768,
            .node = .{ .used_bytes = 256, .insertions = 9 },
        },
        .hbc_posting = .{
            .scanned_nodes = 7,
            .dirty_postings = 2,
            .maintenance_repaired_postings = 1,
        },
    }};
    const runtimes = [_]runtime_status.LocalTableRuntimeStatus{.{
        .group_id = 1,
        .metadata = .{ .source = .remote_store, .freshness = .fresh },
        .stats = .{ .source_doc_count = 1, .index_count = 1, .indexes = indexes[0..] },
    }};

    const aggregate = aggregateIndexStatusIndexed(&runtimes, "visual", &.{1}, 42, 99, null) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u64, 1), aggregate.table_doc_count);
    try std.testing.expectEqual(@as(u64, 0), aggregate.doc_count);
    try std.testing.expectEqual(@as(u64, 0), aggregate.node_count);
    try std.testing.expectEqual(@as(u64, 0), aggregate.root_node);
    try std.testing.expectEqual(@as(u64, 0), aggregate.coverage_produced_count);
    try std.testing.expectEqual(@as(u64, 1), aggregate.coverage_config_mismatch_count);
    try std.testing.expectEqual(@as(u64, 0), aggregate.replay_applied_sequence);
    try std.testing.expect(aggregate.replay_catch_up_required);
    try std.testing.expect(aggregate.backfill_active);
    try std.testing.expect(aggregateRuntimeCoverageIncomplete(aggregate, 42, 99));

    var shard_status = std.ArrayListUnmanaged(u8).empty;
    defer shard_status.deinit(std.testing.allocator);
    try appendSingleIndexRuntimeStatus(
        std.testing.allocator,
        &shard_status,
        .embeddings,
        indexes[0],
        1,
        .strict,
        false,
        42,
        99,
        null,
        .{},
        .{
            .enabled = true,
            .target_sequence = 8,
            .applied_sequence = 7,
            .processed_requests = 6,
            .worker_failed = true,
        },
        null,
        null,
        .{},
        runtimes[0].metadata,
        true,
    );
    try std.testing.expect(std.mem.indexOf(u8, shard_status.items, "\"total_indexed\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, shard_status.items, "\"query_visible_doc_count\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, shard_status.items, "\"published_node_count\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, shard_status.items, "\"dense_replay_applied_sequence\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, shard_status.items, "\"dense_publish_pending\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, shard_status.items, "\"observation_incomplete_reasons\":[\"config_mismatch\"]") != null);
    try std.testing.expect(std.mem.indexOf(u8, shard_status.items, "\"config_mismatch_group_count\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, shard_status.items, "\"produced\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, shard_status.items, "\"projection_checkpoint_status\":\"rebuilding\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, shard_status.items, "\"projection_checkpoint_applied_sequence\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, shard_status.items, "\"projection_checkpoint_generation\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, shard_status.items, "\"projection_checkpoint_config_fingerprint\":\"0000000000000000\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, shard_status.items, "\"checkpoint_replay_tail_sequence_count\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, shard_status.items, "\"repair_degraded\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, shard_status.items, "\"repair_issue_count\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, shard_status.items, "\"repair_summary_ready\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, shard_status.items, "\"repair_issue_count_estimated\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, shard_status.items, "\"repair_scan_issue_count\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, shard_status.items, "\"hbc_cache\":{\"total_bytes\":0,\"accounted_bytes\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, shard_status.items, "\"hbc_posting\":{\"scanned_nodes\":0,\"scanned_postings\":0,\"dirty_postings\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, shard_status.items, "\"enrichment_runtime\":{\"enabled\":false,\"target_sequence\":0,\"applied_sequence\":0") != null);
}

test "derived coverage rejects unknown freshness for aggregate and shard views" {
    var indexes = [_]db_mod.types.DBIndexStats{.{
        .name = "visual",
        .kind = .dense_vector,
        .coverage_produced_count = 1,
        .coverage_config_hash = 41,
    }};
    const runtimes = [_]runtime_status.LocalTableRuntimeStatus{.{
        .group_id = 1,
        .metadata = .{ .source = .remote_store, .freshness = .unknown },
        .stats = .{ .source_doc_count = 1, .index_count = 1, .indexes = indexes[0..] },
    }};

    const aggregate = aggregateIndexStatus(&runtimes, "visual", &.{1}, 41) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u64, 0), aggregate.fresh_group_count);
    try std.testing.expectEqual(@as(u64, 1), aggregate.unknown_group_count);
    try std.testing.expectEqual(@as(u64, 0), aggregate.table_doc_count);
    try std.testing.expect(aggregateRuntimeCoverageIncomplete(aggregate, 0, 41));

    var shard_status = std.ArrayListUnmanaged(u8).empty;
    defer shard_status.deinit(std.testing.allocator);
    try appendSingleIndexRuntimeStatus(
        std.testing.allocator,
        &shard_status,
        .embeddings,
        indexes[0],
        1,
        .partial,
        false,
        0,
        41,
        null,
        .{},
        null,
        null,
        null,
        .{},
        runtimes[0].metadata,
        true,
    );
    try std.testing.expect(std.mem.indexOf(u8, shard_status.items, "\"observation_complete\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, shard_status.items, "\"observation_incomplete_reasons\":[\"unknown_group\"]") != null);
    try std.testing.expect(std.mem.indexOf(u8, shard_status.items, "\"pending\":null") != null);
}

test "derived coverage semantic fingerprint ignores execution policy" {
    var first = try std.json.parseFromSlice(
        std.json.Value,
        std.testing.allocator,
        "{\"type\":\"embeddings\",\"external\":true,\"dimension\":3,\"execution\":{\"embedding\":{\"batch_items\":16}}}",
        .{},
    );
    defer first.deinit();
    var second = try std.json.parseFromSlice(
        std.json.Value,
        std.testing.allocator,
        "{\"execution\":{\"embedding\":{\"batch_items\":1024}},\"dimension\":3,\"external\":true,\"type\":\"embeddings\"}",
        .{},
    );
    defer second.deinit();

    try std.testing.expectEqual(
        try expectedCoverageConfigHash(std.testing.allocator, "external_idx", first.value),
        try expectedCoverageConfigHash(std.testing.allocator, "external_idx", second.value),
    );
}

fn embeddingsRuntimeView(item: anytype, table_doc_count: u64, coverage_policy: EmbeddingsCoveragePolicy, sparse: bool, coverage_generation: u64, coverage_config_hash: u64, enrichment: ?db_mod.types.EnrichmentStats, runtime_present: bool) EmbeddingsRuntimeView {
    const observation_current = runtime_present and
        coverageIdentityMatches(item, coverage_generation, coverage_config_hash);
    var view: EmbeddingsRuntimeView = .{
        .backfill_active = if (observation_current) item.backfill_active else true,
        .backfill_progress = if (observation_current) item.backfill_progress else 0.0,
        .coverage_degraded = false,
        .replay_applied_sequence = if (observation_current) item.replay_applied_sequence else 0,
        .replay_target_sequence = if (observation_current) item.replay_target_sequence else 0,
        .replay_catch_up_required = if (observation_current) item.replay_catch_up_required else true,
    };
    const produced_count = if (observation_current and @hasField(@TypeOf(item), "coverage_produced_count")) item.coverage_produced_count else 0;
    const skipped_count = if (observation_current and @hasField(@TypeOf(item), "coverage_skipped_count")) item.coverage_skipped_count else 0;
    const terminal_failed_count = if (observation_current and @hasField(@TypeOf(item), "coverage_terminal_failed_count")) item.coverage_terminal_failed_count else 0;
    const replay_current = coverageReplayCurrent(view.replay_applied_sequence, view.replay_target_sequence, view.replay_catch_up_required);
    const coverage_incomplete = !observation_current or
        aggregateRuntimeCoverageIncomplete(item, coverage_generation, coverage_config_hash) or
        !coverageCountersValid(table_doc_count, produced_count, skipped_count, terminal_failed_count);
    const coverage = evaluateCoverage(
        coverage_policy,
        table_doc_count,
        produced_count,
        skipped_count,
        terminal_failed_count,
        !coverage_incomplete,
        replay_current,
    );
    view.coverage_degraded = coverage.degraded;
    const require_table_coverage = embeddingsCoveragePolicyRequiresTableCoverage(coverage_policy);
    const source_coverage_visible = coverage.source_visible;
    const dense_coverage_complete = coverage.complete;
    const materialization_complete = !coverage_incomplete and
        replay_current and
        terminal_failed_count == 0 and
        coverageAllSourcesTerminal(table_doc_count, produced_count, skipped_count, terminal_failed_count) and
        embeddingsArtifactPublishComplete(item, sparse, produced_count);
    // External indexes are query-ready once their published artifacts and
    // replay are current. Their coverage remains independently observable and
    // may be incomplete because callers are not required to supply a vector
    // for every source document.
    const readiness_complete = if (coverage_policy == .external)
        !coverage_incomplete and replay_current
    else
        materialization_complete;
    const all_sources_settled = !coverage_incomplete and replay_current and
        coverageAllSourcesTerminal(table_doc_count, produced_count, skipped_count, terminal_failed_count);
    if (if (observation_current) enrichment else null) |stats| {
        const index_applied_sequence = view.replay_applied_sequence;
        const index_target_sequence = view.replay_target_sequence;
        view.replay_target_sequence = @max(index_target_sequence, stats.target_sequence);
        view.replay_applied_sequence = if (index_target_sequence == 0)
            stats.applied_sequence
        else if (stats.target_sequence == 0)
            index_applied_sequence
        else
            @min(index_applied_sequence, stats.applied_sequence);
        if (view.replay_applied_sequence < view.replay_target_sequence) {
            view.replay_catch_up_required = true;
            view.backfill_active = true;
            view.backfill_progress = @min(
                1.0,
                @as(f64, @floatFromInt(view.replay_applied_sequence)) /
                    @as(f64, @floatFromInt(view.replay_target_sequence)),
            );
        } else if (stats.retrying) {
            view.backfill_active = true;
            if (view.backfill_progress >= 1.0) view.backfill_progress = 0.999;
        }
    }
    const enrichment_pending = if (if (observation_current) enrichment else null) |stats|
        stats.enabled and (stats.worker_failed or stats.retrying or stats.applied_sequence < stats.target_sequence)
    else
        false;
    if (readiness_complete and !enrichment_pending) {
        view.replay_applied_sequence = @max(view.replay_applied_sequence, view.replay_target_sequence);
        view.replay_catch_up_required = false;
        view.backfill_active = false;
        view.backfill_progress = 1.0;
        return view;
    }
    if (all_sources_settled and !enrichment_pending) {
        view.replay_applied_sequence = @max(view.replay_applied_sequence, view.replay_target_sequence);
        view.replay_catch_up_required = false;
        view.backfill_active = false;
        view.backfill_progress = 1.0;
        return view;
    }
    const replay_ready = view.replay_target_sequence > 0 and
        view.replay_target_sequence <= view.replay_applied_sequence and
        !(if (enrichment) |stats| stats.retrying or stats.worker_failed else false);
    const artifact_visible = observation_current and embeddingsArtifactPublishComplete(item, sparse, produced_count);
    if (replay_ready and !artifact_visible and view.replay_target_sequence > 0) {
        view.backfill_active = true;
        view.backfill_progress = 0.0;
        return view;
    }
    if (readiness_complete and !enrichment_pending) {
        view.backfill_active = false;
        view.backfill_progress = 1.0;
    } else if (!coverage_incomplete and replay_ready and source_coverage_visible and (!require_table_coverage or table_doc_count == 0) and !enrichment_pending) {
        view.backfill_active = false;
        view.backfill_progress = 1.0;
    } else if (table_doc_count > 0 and !dense_coverage_complete) {
        view.backfill_active = true;
        const terminal_outcomes = coverageOutcomeTotal(produced_count, skipped_count, terminal_failed_count) orelse 0;
        view.backfill_progress = @min(
            1.0,
            @as(f64, @floatFromInt(terminal_outcomes)) /
                @as(f64, @floatFromInt(table_doc_count)),
        );
    }
    return view;
}

fn aggregateRuntimeCoverageIncomplete(item: anytype, expected_generation: u64, expected_config_hash: u64) bool {
    const Item = @TypeOf(item);
    if (@hasField(Item, "missing_group_count") and item.missing_group_count > 0) return true;
    if (@hasField(Item, "remote_unknown_group_count") and item.remote_unknown_group_count > 0) return true;
    if (@hasField(Item, "unknown_group_count") and item.unknown_group_count > 0) return true;
    if (@hasField(Item, "stale_group_count") and item.stale_group_count > 0) return true;
    if (@hasField(Item, "expected_group_count") and @hasField(Item, "fresh_group_count") and
        item.expected_group_count != item.fresh_group_count) return true;
    if (@hasField(Item, "coverage_summary_ready") and !item.coverage_summary_ready) return true;
    if (@hasField(Item, "coverage_config_mismatch_count") and item.coverage_config_mismatch_count > 0) return true;
    if (!coverageIdentityMatches(item, expected_generation, expected_config_hash)) return true;
    return false;
}

fn appendCoverageIncompleteReasons(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    item: anytype,
    expected_generation: u64,
    expected_config_hash: u64,
    runtime_present: bool,
    metadata: ?runtime_status.RuntimeStatusMetadata,
    source_total: u64,
) !void {
    const Item = @TypeOf(item);
    const observation_current = runtime_present and (metadata == null or metadata.?.freshness == .fresh);
    var reasons = std.EnumSet(CoverageIncompleteReason).initEmpty();

    if (!runtime_present) reasons.insert(.runtime_unavailable);
    if (@hasField(Item, "missing_group_count") and item.missing_group_count > 0)
        reasons.insert(.missing_group);
    if ((@hasField(Item, "unknown_group_count") and item.unknown_group_count > 0) or
        (metadata != null and metadata.?.freshness == .unknown))
        reasons.insert(.unknown_group);
    if (@hasField(Item, "remote_unknown_group_count") and item.remote_unknown_group_count > 0)
        reasons.insert(.remote_unknown_group);
    if (metadata != null and metadata.?.freshness == .remote_unknown)
        reasons.insert(.remote_unknown_group);
    if (@hasField(Item, "stale_group_count") and item.stale_group_count > 0)
        reasons.insert(.stale_group);
    if (metadata != null and metadata.?.freshness != .fresh and metadata.?.freshness != .unknown and metadata.?.freshness != .remote_unknown)
        reasons.insert(.stale_group);
    if (@hasField(Item, "coverage_summary_ready") and !item.coverage_summary_ready)
        reasons.insert(.summary_unavailable);
    const config_mismatch = (@hasField(Item, "coverage_config_mismatch_count") and item.coverage_config_mismatch_count > 0) or
        (observation_current and !coverageIdentityMatches(item, expected_generation, expected_config_hash));
    if (config_mismatch) reasons.insert(.config_mismatch);
    const summary_ready = if (@hasField(Item, "coverage_summary_ready")) item.coverage_summary_ready else false;
    const produced = if (@hasField(Item, "coverage_produced_count")) item.coverage_produced_count else 0;
    const skipped = if (@hasField(Item, "coverage_skipped_count")) item.coverage_skipped_count else 0;
    const terminal_failed = if (@hasField(Item, "coverage_terminal_failed_count")) item.coverage_terminal_failed_count else 0;
    if (observation_current and !config_mismatch and summary_ready and !coverageCountersValid(source_total, produced, skipped, terminal_failed))
        reasons.insert(.counter_mismatch);

    try out.append(alloc, '[');
    var emitted = false;
    for (std.meta.tags(CoverageIncompleteReason)) |reason| {
        if (!reasons.contains(reason)) continue;
        if (emitted) try out.append(alloc, ',');
        emitted = true;
        try appendJsonString(alloc, out, @tagName(reason));
    }
    try out.append(alloc, ']');
}

test "derived coverage reasons expose counter mismatch" {
    const aggregate = AggregatedIndexStatus{
        .coverage_config_hash = 41,
        .coverage_produced_count = 2,
        .coverage_skipped_count = 1,
        .coverage_summary_ready = true,
    };
    var reasons = std.ArrayListUnmanaged(u8).empty;
    defer reasons.deinit(std.testing.allocator);
    try appendCoverageIncompleteReasons(std.testing.allocator, &reasons, aggregate, 0, 41, true, null, 2);
    try std.testing.expectEqualStrings("[\"counter_mismatch\"]", reasons.items);
}

test "derived coverage reasons deduplicate overlapping freshness signals" {
    const aggregate = AggregatedIndexStatus{
        .coverage_config_hash = 41,
        .coverage_summary_ready = true,
        .remote_unknown_group_count = 1,
    };
    var reasons = std.ArrayListUnmanaged(u8).empty;
    defer reasons.deinit(std.testing.allocator);
    try appendCoverageIncompleteReasons(std.testing.allocator, &reasons, aggregate, 0, 41, true, .{
        .freshness = .remote_unknown,
    }, 0);
    try std.testing.expectEqualStrings("[\"remote_unknown_group\"]", reasons.items);
}

fn appendCoverageFingerprint(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), fingerprint: u64) !void {
    var buf: [16]u8 = undefined;
    const encoded = std.fmt.bufPrint(&buf, "{x:0>16}", .{fingerprint}) catch unreachable;
    try appendJsonString(alloc, out, encoded);
}

fn embeddingsArtifactVisible(item: anytype, sparse: bool) bool {
    if (sparse) return item.doc_count > 0;
    return item.doc_count > 0 and (item.node_count > 0 or item.root_node > 0);
}

fn embeddingsArtifactPublishComplete(item: anytype, sparse: bool, expected_doc_count: u64) bool {
    if (expected_doc_count == 0) return item.doc_count == 0;
    return item.doc_count >= expected_doc_count and embeddingsArtifactVisible(item, sparse);
}

fn backfillState(index_type: ApiIndexType, active: bool, enrichment_degraded: bool, replay_applied_sequence: u64, replay_target_sequence: u64, enrichment: ?db_mod.types.EnrichmentStats) []const u8 {
    if (index_type == .embeddings) {
        _ = replay_applied_sequence;
        _ = replay_target_sequence;
        if (enrichment_degraded) return "degraded";
        if (active) {
            if (enrichment) |stats| {
                if (stats.worker_failed) return "failed";
                if (stats.retrying) return "retrying";
            }
            return "running";
        }
        if (enrichment) |stats| {
            if (stats.worker_failed) return "failed";
        }
        return "ready";
    }
    return if (active) "running" else "ready";
}

fn appendEnrichmentRuntimeStatus(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), stats: db_mod.types.EnrichmentStats) !void {
    try out.append(alloc, '{');
    try out.appendSlice(alloc, "\"enabled\":");
    try out.appendSlice(alloc, if (stats.enabled) "true" else "false");
    try out.appendSlice(alloc, ",\"target_sequence\":");
    try appendIntValue(alloc, out, stats.target_sequence);
    try out.appendSlice(alloc, ",\"applied_sequence\":");
    try appendIntValue(alloc, out, stats.applied_sequence);
    try out.appendSlice(alloc, ",\"pending_sequence_count\":");
    try appendIntValue(alloc, out, stats.target_sequence -| stats.applied_sequence);
    try out.appendSlice(alloc, ",\"projection_checkpoint_status\":");
    try appendJsonString(alloc, out, stats.projection_checkpoint_status);
    try out.appendSlice(alloc, ",\"projection_checkpoint_applied_sequence\":");
    try appendIntValue(alloc, out, stats.projection_checkpoint_applied_sequence);
    try out.appendSlice(alloc, ",\"projection_checkpoint_generation\":");
    try appendIntValue(alloc, out, stats.projection_checkpoint_generation);
    try out.appendSlice(alloc, ",\"projection_checkpoint_config_fingerprint\":");
    try appendCoverageFingerprint(alloc, out, stats.projection_checkpoint_config_hash);
    try out.appendSlice(alloc, ",\"projection_checkpoint_identity_consistent\":");
    try out.appendSlice(alloc, if (stats.projection_checkpoint_identity_consistent) "true" else "false");
    try out.appendSlice(alloc, ",\"checkpoint_replay_tail_sequence_count\":");
    try appendIntValue(alloc, out, stats.checkpoint_replay_tail_sequence_count);
    try out.appendSlice(alloc, ",\"processed_requests\":");
    try appendIntValue(alloc, out, stats.processed_requests);
    try out.appendSlice(alloc, ",\"error_count\":");
    try appendIntValue(alloc, out, stats.error_count);
    try out.appendSlice(alloc, ",\"retryable_error_count\":");
    try appendIntValue(alloc, out, stats.retryable_error_count);
    try out.appendSlice(alloc, ",\"fatal_error_count\":");
    try appendIntValue(alloc, out, stats.fatal_error_count);
    try out.appendSlice(alloc, ",\"consecutive_retry_count\":");
    try appendIntValue(alloc, out, stats.consecutive_retry_count);
    try out.appendSlice(alloc, ",\"next_retry_at_ms\":");
    try appendIntValue(alloc, out, stats.next_retry_at_ms);
    try out.appendSlice(alloc, ",\"retrying\":");
    try out.appendSlice(alloc, if (stats.retrying) "true" else "false");
    try out.appendSlice(alloc, ",\"worker_failed\":");
    try out.appendSlice(alloc, if (stats.worker_failed) "true" else "false");
    try out.appendSlice(alloc, ",\"worker_started\":");
    try out.appendSlice(alloc, if (stats.worker_started) "true" else "false");
    try out.appendSlice(alloc, ",\"stalled\":");
    try out.appendSlice(alloc, if (stats.stalled) "true" else "false");
    try out.appendSlice(alloc, ",\"skip_by_hash_count\":");
    try appendIntValue(alloc, out, stats.skip_by_hash_count);
    try out.appendSlice(alloc, ",\"skipped_source_count\":");
    try appendIntValue(alloc, out, stats.skipped_source_count);
    try out.appendSlice(alloc, ",\"codec_decode_failures\":");
    try appendIntValue(alloc, out, stats.codec_decode_failures);
    try out.appendSlice(alloc, ",\"embed_batches_started\":");
    try appendIntValue(alloc, out, stats.embed_batches_started);
    try out.appendSlice(alloc, ",\"embed_batches_completed\":");
    try appendIntValue(alloc, out, stats.embed_batches_completed);
    try out.appendSlice(alloc, ",\"embed_items_started\":");
    try appendIntValue(alloc, out, stats.embed_items_started);
    try out.appendSlice(alloc, ",\"embed_items_completed\":");
    try appendIntValue(alloc, out, stats.embed_items_completed);
    try out.appendSlice(alloc, ",\"active_embed_batch_items\":");
    try appendIntValue(alloc, out, stats.active_embed_batch_items);
    try out.appendSlice(alloc, ",\"active_embed_batch_bytes\":");
    try appendIntValue(alloc, out, stats.active_embed_batch_bytes);
    try out.appendSlice(alloc, ",\"active_embed_batch_max_bytes\":");
    try appendIntValue(alloc, out, stats.active_embed_batch_max_bytes);
    try out.appendSlice(alloc, ",\"active_embed_batch_started_ms\":");
    try appendIntValue(alloc, out, stats.active_embed_batch_started_ms);
    try out.appendSlice(alloc, ",\"last_embed_batch_items\":");
    try appendIntValue(alloc, out, stats.last_embed_batch_items);
    try out.appendSlice(alloc, ",\"last_embed_batch_bytes\":");
    try appendIntValue(alloc, out, stats.last_embed_batch_bytes);
    try out.appendSlice(alloc, ",\"last_embed_batch_max_bytes\":");
    try appendIntValue(alloc, out, stats.last_embed_batch_max_bytes);
    try out.appendSlice(alloc, ",\"last_embed_batch_completed_ms\":");
    try appendIntValue(alloc, out, stats.last_embed_batch_completed_ms);
    try out.appendSlice(alloc, ",\"last_embed_batch_ns\":");
    try appendIntValue(alloc, out, stats.last_embed_batch_ns);
    try out.appendSlice(alloc, ",\"total_embed_ns\":");
    try appendIntValue(alloc, out, stats.total_embed_ns);
    try out.append(alloc, '}');
}

test "enrichment index status encodes worker lifecycle diagnostics" {
    var encoded = std.ArrayListUnmanaged(u8).empty;
    defer encoded.deinit(std.testing.allocator);

    try appendEnrichmentRuntimeStatus(std.testing.allocator, &encoded, .{
        .enabled = true,
        .target_sequence = 5,
        .applied_sequence = 1,
        .worker_started = false,
        .stalled = true,
    });

    try std.testing.expect(std.mem.indexOf(u8, encoded.items, "\"worker_started\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded.items, "\"stalled\":true") != null);
    var parsed = try std.json.parseFromSlice(indexes_openapi.EnrichmentRuntimeStatus, std.testing.allocator, encoded.items, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(false, parsed.value.worker_started);
    try std.testing.expectEqual(true, parsed.value.stalled);
}

test "enrichment aggregation preserves telemetry and fences mixed checkpoint identity" {
    var aggregate: db_mod.types.EnrichmentStats = .{};
    aggregateEnrichmentStats(&aggregate, .{
        .enabled = true,
        .target_sequence = 11,
        .applied_sequence = 7,
        .projection_checkpoint_status = "clean",
        .projection_checkpoint_applied_sequence = 7,
        .projection_checkpoint_generation = 41,
        .projection_checkpoint_config_hash = std.math.maxInt(u64) - 7,
        .processed_requests = std.math.maxInt(u64) - 1,
        .consecutive_retry_count = 2,
        .next_retry_at_ms = 5000,
        .retrying = true,
        .active_embed_batch_items = 3,
        .active_embed_batch_started_ms = 200,
        .last_embed_batch_items = 4,
        .last_embed_batch_bytes = 100,
        .last_embed_batch_completed_ms = 2000,
        .last_embed_batch_ns = 900,
    }, true);
    aggregateEnrichmentStats(&aggregate, .{
        .enabled = true,
        .target_sequence = 13,
        .applied_sequence = 9,
        .projection_checkpoint_status = "repair_required",
        .projection_checkpoint_applied_sequence = 9,
        .projection_checkpoint_generation = 42,
        .projection_checkpoint_config_hash = 99,
        .processed_requests = 10,
        .consecutive_retry_count = 4,
        .next_retry_at_ms = 2000,
        .retrying = true,
        .active_embed_batch_items = 5,
        .active_embed_batch_started_ms = 100,
        .last_embed_batch_items = 8,
        .last_embed_batch_bytes = 200,
        .last_embed_batch_completed_ms = 1000,
        .last_embed_batch_ns = 9010,
    }, false);

    try std.testing.expectEqual(std.math.maxInt(u64), aggregate.processed_requests);
    try std.testing.expectEqual(@as(u64, 24), aggregate.target_sequence);
    try std.testing.expectEqual(@as(u64, 16), aggregate.applied_sequence);
    try std.testing.expectEqual(@as(u32, 4), aggregate.consecutive_retry_count);
    try std.testing.expectEqual(@as(u64, 2000), aggregate.next_retry_at_ms);
    try std.testing.expect(aggregate.retrying);
    try std.testing.expectEqual(@as(u64, 16), aggregate.projection_checkpoint_applied_sequence);
    try std.testing.expectEqualStrings("repair_required", aggregate.projection_checkpoint_status);
    try std.testing.expect(!aggregate.projection_checkpoint_identity_consistent);
    try std.testing.expectEqual(@as(u64, 0), aggregate.projection_checkpoint_generation);
    try std.testing.expectEqual(@as(u64, 0), aggregate.projection_checkpoint_config_hash);
    try std.testing.expectEqual(@as(u64, 8), aggregate.active_embed_batch_items);
    try std.testing.expectEqual(@as(u64, 100), aggregate.active_embed_batch_started_ms);
    try std.testing.expectEqual(@as(u64, 4), aggregate.last_embed_batch_items);
    try std.testing.expectEqual(@as(u64, 2000), aggregate.last_embed_batch_completed_ms);
    try std.testing.expectEqual(@as(u64, 900), aggregate.last_embed_batch_ns);

    var encoded = std.ArrayListUnmanaged(u8).empty;
    defer encoded.deinit(std.testing.allocator);
    try appendEnrichmentRuntimeStatus(std.testing.allocator, &encoded, .{
        .projection_checkpoint_config_hash = std.math.maxInt(u64) - 7,
    });
    var parsed = try std.json.parseFromSlice(indexes_openapi.EnrichmentRuntimeStatus, std.testing.allocator, encoded.items, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("fffffffffffffff8", parsed.value.projection_checkpoint_config_fingerprint);
    try std.testing.expect(parsed.value.projection_checkpoint_identity_consistent);
}

fn appendSingleIndexRuntimeStatus(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    index_type: ApiIndexType,
    item: anytype,
    table_doc_count: u64,
    embeddings_coverage_policy: EmbeddingsCoveragePolicy,
    embeddings_sparse: bool,
    coverage_generation: u64,
    coverage_config_hash: u64,
    graph_source_status: ?GraphSourceStatus,
    async_indexing: db_mod.types.AsyncIndexingStats,
    enrichment: ?db_mod.types.EnrichmentStats,
    resolution: ?db_mod.types.ReplayStageStats,
    promotion: ?db_mod.types.ReplayStageStats,
    resolver_replay: db_mod.types.ResolverReplayDiagnostics,
    metadata: ?runtime_status.RuntimeStatusMetadata,
    runtime_present: bool,
) !void {
    const coverage_runtime_present = runtime_present and (metadata == null or metadata.?.freshness == .fresh);
    const embeddings_materialization_current = index_type != .embeddings or
        (coverage_runtime_present and coverageIdentityMatches(item, coverage_generation, coverage_config_hash));
    const visible_doc_count = if (embeddings_materialization_current) item.doc_count else 0;
    const visible_term_count = if (embeddings_materialization_current) item.term_count else 0;
    const visible_edge_count = if (embeddings_materialization_current) item.edge_count else 0;
    const visible_node_count = if (embeddings_materialization_current) item.node_count else 0;
    const visible_root_node = if (embeddings_materialization_current) item.root_node else 0;
    const embeddings_view = if (index_type == .embeddings)
        embeddingsRuntimeView(item, table_doc_count, embeddings_coverage_policy, embeddings_sparse, coverage_generation, coverage_config_hash, enrichment, coverage_runtime_present)
    else
        null;
    const visible_enrichment = if (embeddings_materialization_current) enrichment else null;
    var backfill_active = if (embeddings_view) |view| view.backfill_active else item.backfill_active;
    var backfill_progress = if (embeddings_view) |view| view.backfill_progress else item.backfill_progress;
    // Replay watermarks describe the managed index worker's real ledger.
    // Coverage and enrichment are separate stages with separate sequence
    // domains; they may keep readiness/backfill active, but must not rewrite a
    // converged index watermark into synthetic replay debt. Older snapshots
    // that contain no index replay facts can still derive a compatibility
    // view from enrichment status.
    const visible_item_replay_applied_sequence = if (embeddings_materialization_current) item.replay_applied_sequence else 0;
    const visible_item_replay_target_sequence = if (embeddings_materialization_current) item.replay_target_sequence else 0;
    const visible_item_replay_catch_up_required = if (embeddings_materialization_current) item.replay_catch_up_required else index_type == .embeddings;
    const index_replay_present = visible_item_replay_applied_sequence != 0 or
        visible_item_replay_target_sequence != 0 or visible_item_replay_catch_up_required;
    var replay_applied_sequence = if (index_replay_present or embeddings_view == null)
        visible_item_replay_applied_sequence
    else
        embeddings_view.?.replay_applied_sequence;
    var replay_target_sequence = if (index_replay_present or embeddings_view == null)
        visible_item_replay_target_sequence
    else
        embeddings_view.?.replay_target_sequence;
    var replay_catch_up_required = if (index_replay_present or embeddings_view == null)
        visible_item_replay_catch_up_required
    else
        embeddings_view.?.replay_catch_up_required;
    const dense_catch_up = async_indexing.dense_catch_up;
    var catch_up_active = if (embeddings_materialization_current) item.catch_up_active else false;
    var catch_up_phase = if (embeddings_materialization_current) item.catch_up_phase else .idle;
    var catch_up_applied_sequence = if (embeddings_materialization_current) item.catch_up_applied_sequence else 0;
    var catch_up_target_sequence = if (embeddings_materialization_current) item.catch_up_target_sequence else 0;
    if (index_type == .embeddings) {
        if (embeddings_materialization_current and dense_catch_up.active) {
            catch_up_active = true;
            catch_up_phase = dense_catch_up.phase;
            catch_up_applied_sequence = @max(catch_up_applied_sequence, dense_catch_up.current_sequence);
            catch_up_target_sequence = @max(catch_up_target_sequence, dense_catch_up.current_target_sequence);
        } else if (embeddings_view) |view| {
            if (!view.backfill_active) {
                catch_up_active = false;
                catch_up_phase = .idle;
                catch_up_applied_sequence = @max(catch_up_applied_sequence, catch_up_target_sequence);
            }
        }
    }
    if (!index_replay_present and (catch_up_active or catch_up_target_sequence > catch_up_applied_sequence)) {
        replay_catch_up_required = true;
        backfill_active = true;
        replay_target_sequence = @max(replay_target_sequence, catch_up_target_sequence);
        if (catch_up_applied_sequence != 0) {
            replay_applied_sequence = if (replay_applied_sequence == 0)
                catch_up_applied_sequence
            else
                @min(replay_applied_sequence, catch_up_applied_sequence);
        }
        if (replay_target_sequence > 0) {
            backfill_progress = @min(
                0.999,
                @as(f64, @floatFromInt(replay_applied_sequence)) /
                    @as(f64, @floatFromInt(replay_target_sequence)),
            );
        }
    }
    if (catch_up_active and catch_up_phase == .idle and replay_catch_up_required) catch_up_phase = .replay;

    const active_generation_serviceable = if (@hasField(@TypeOf(item), "index_repair_active_generation_serviceable"))
        item.index_repair_active_generation_serviceable
    else if (@hasField(@TypeOf(item), "repair_active_generation_serviceable"))
        item.repair_active_generation_serviceable
    else
        false;
    const repair_state = if (embeddings_materialization_current and !active_generation_serviceable)
        publicIndexRepairState(item)
    else
        null;
    // A load failure with no durable automatic repair remains a terminal
    // operator-visible error. Once a repair intent exists, the compact repair
    // state is authoritative; exposing the quarantined root's raw load error
    // at the same time would incorrectly tell clients to drop/recreate.
    const raw_load_error: ?[]const u8 = if (@hasField(@TypeOf(item), "load_error")) item.load_error else null;
    const load_error: ?[]const u8 = if (repair_state == null) raw_load_error else null;
    const repair_blocks_readiness = repair_state != null;
    if (load_error != null) {
        backfill_active = false;
        catch_up_active = false;
        replay_catch_up_required = false;
        catch_up_phase = .idle;
    } else if (repair_blocks_readiness) {
        const state = repair_state.?;
        backfill_active = std.mem.eql(u8, state, "rebuilding") or std.mem.eql(u8, state, "waiting");
    }

    try out.append(alloc, '{');
    if (index_type != .algebraic) {
        try appendJsonString(alloc, out, "index_type");
        try out.append(alloc, ':');
        try appendJsonString(alloc, out, indexTypeName(index_type));
        try out.append(alloc, ',');
    }
    try out.appendSlice(alloc, "\"rebuilding\":");
    try out.appendSlice(alloc, if (backfill_active) "true" else "false");
    switch (index_type) {
        .full_text, .embeddings, .algebraic => {
            try out.appendSlice(alloc, ",\"total_indexed\":");
            try appendIntValue(alloc, out, visible_doc_count);
        },
        .graph => {
            try out.appendSlice(alloc, ",\"total_edges\":");
            try appendIntValue(alloc, out, visible_edge_count);
        },
    }
    if (index_type == .embeddings) {
        try out.appendSlice(alloc, ",\"total_terms\":");
        try appendIntValue(alloc, out, visible_term_count);
        try out.appendSlice(alloc, ",\"total_nodes\":");
        try appendIntValue(alloc, out, visible_node_count);
    }
    try out.append(alloc, ',');
    try appendJsonString(alloc, out, "backfill_active");
    try out.appendSlice(alloc, if (backfill_active) ":true" else ":false");
    try out.appendSlice(alloc, ",\"backfill_progress\":");
    const progress = try std.fmt.allocPrint(alloc, "{d:.3}", .{backfill_progress});
    defer alloc.free(progress);
    try out.appendSlice(alloc, progress);
    try out.appendSlice(alloc, ",\"backfill_state\":");
    if (load_error != null or (repair_state != null and
        (std.mem.eql(u8, repair_state.?, "paused") or std.mem.eql(u8, repair_state.?, "failed"))))
    {
        try appendJsonString(alloc, out, "failed");
    } else if (repair_blocks_readiness and repair_state != null and std.mem.eql(u8, repair_state.?, "waiting")) {
        try appendJsonString(alloc, out, "retrying");
    } else {
        const enrichment_degraded = (embeddings_materialization_current and item.enrichment_failed) or
            (if (embeddings_view) |view| view.coverage_degraded else false);
        try appendJsonString(alloc, out, backfillState(index_type, backfill_active, enrichment_degraded, replay_applied_sequence, replay_target_sequence, visible_enrichment));
    }
    if (load_error) |err_name| {
        const msg = try std.fmt.allocPrint(alloc, "load failed: {s}", .{err_name});
        defer alloc.free(msg);
        try out.appendSlice(alloc, ",\"error\":");
        try appendJsonString(alloc, out, msg);
    }
    if (repair_state) |state| {
        try out.appendSlice(alloc, ",\"repair\":{\"state\":");
        try appendJsonString(alloc, out, state);
        try out.appendSlice(alloc, ",\"action_required\":");
        try out.appendSlice(alloc, if (std.mem.eql(u8, state, "paused") or std.mem.eql(u8, state, "failed")) "true" else "false");
        try out.append(alloc, '}');
    }
    try out.appendSlice(alloc, ",\"doc_count\":");
    try appendIntValue(alloc, out, visible_doc_count);
    try out.appendSlice(alloc, ",\"term_count\":");
    try appendIntValue(alloc, out, visible_term_count);
    try out.appendSlice(alloc, ",\"edge_count\":");
    try appendIntValue(alloc, out, visible_edge_count);
    try out.appendSlice(alloc, ",\"node_count\":");
    try appendIntValue(alloc, out, visible_node_count);
    if (index_type == .embeddings) {
        const skipped_count = if (embeddings_materialization_current and @hasField(@TypeOf(item), "coverage_skipped_count")) item.coverage_skipped_count else 0;
        const terminal_failed_count = if (embeddings_materialization_current and @hasField(@TypeOf(item), "coverage_terminal_failed_count")) item.coverage_terminal_failed_count else 0;
        const produced_count = if (embeddings_materialization_current and @hasField(@TypeOf(item), "coverage_produced_count")) item.coverage_produced_count else 0;
        const counters_valid = coverageCountersValid(table_doc_count, produced_count, skipped_count, terminal_failed_count);
        const replay_current = coverageReplayCurrent(replay_applied_sequence, replay_target_sequence, replay_catch_up_required);
        const observation_complete = coverage_runtime_present and
            !aggregateRuntimeCoverageIncomplete(item, coverage_generation, coverage_config_hash) and
            counters_valid;
        const coverage = evaluateCoverage(
            embeddings_coverage_policy,
            table_doc_count,
            produced_count,
            skipped_count,
            terminal_failed_count,
            observation_complete,
            replay_current,
        );
        const coverage_complete = coverage.complete;
        const artifact_publish_pending = !embeddings_materialization_current or
            (replay_target_sequence > 0 and
                !embeddingsArtifactPublishComplete(item, embeddings_sparse, produced_count) and
                !coverage_complete);
        try out.appendSlice(alloc, ",\"query_visible_doc_count\":");
        try appendIntValue(alloc, out, visible_doc_count);
        try out.appendSlice(alloc, ",\"published_doc_count\":");
        try appendIntValue(alloc, out, visible_doc_count);
        try out.appendSlice(alloc, ",\"published_node_count\":");
        try appendIntValue(alloc, out, visible_node_count);
        try out.appendSlice(alloc, ",\"root_node\":");
        try appendIntValue(alloc, out, visible_root_node);
        try out.appendSlice(alloc, ",\"published_root_node\":");
        try appendIntValue(alloc, out, visible_root_node);
        try out.appendSlice(alloc, ",\"dense_replay_applied_sequence\":");
        try appendIntValue(alloc, out, replay_applied_sequence);
        try out.appendSlice(alloc, ",\"dense_replay_target_sequence\":");
        try appendIntValue(alloc, out, replay_target_sequence);
        try out.appendSlice(alloc, ",\"dense_publish_pending\":");
        try out.appendSlice(alloc, if (catch_up_active or replay_catch_up_required or artifact_publish_pending) "true" else "false");
        try out.appendSlice(alloc, ",\"coverage\":{");
        try appendJsonString(alloc, out, "policy");
        try out.append(alloc, ':');
        try appendJsonString(alloc, out, embeddingsCoveragePolicyName(embeddings_coverage_policy));
        try out.appendSlice(alloc, ",\"observation_complete\":");
        try out.appendSlice(alloc, if (observation_complete) "true" else "false");
        try out.appendSlice(alloc, ",\"observation_incomplete_reasons\":");
        try appendCoverageIncompleteReasons(alloc, out, item, coverage_generation, coverage_config_hash, runtime_present, metadata, table_doc_count);
        try out.appendSlice(alloc, ",\"config_fingerprint\":");
        try appendCoverageFingerprint(alloc, out, coverage_config_hash);
        try out.appendSlice(alloc, ",\"summary_ready\":");
        const coverage_summary_ready = embeddings_materialization_current and if (@hasField(@TypeOf(item), "coverage_summary_ready")) item.coverage_summary_ready else false;
        try out.appendSlice(alloc, if (coverage_summary_ready) "true" else "false");
        try out.appendSlice(alloc, ",\"config_mismatch_group_count\":");
        const config_mismatch_group_count = if (@hasField(@TypeOf(item), "coverage_config_mismatch_count")) item.coverage_config_mismatch_count else @intFromBool(coverage_runtime_present and !coverageIdentityMatches(item, coverage_generation, coverage_config_hash));
        try appendIntValue(alloc, out, config_mismatch_group_count);
        try out.appendSlice(alloc, ",\"source_total\":");
        try appendIntValue(alloc, out, table_doc_count);
        try out.appendSlice(alloc, ",\"produced\":");
        try appendIntValue(alloc, out, produced_count);
        try out.appendSlice(alloc, ",\"skipped\":");
        try appendIntValue(alloc, out, skipped_count);
        try out.appendSlice(alloc, ",\"terminal_failed\":");
        try appendIntValue(alloc, out, terminal_failed_count);
        try out.appendSlice(alloc, ",\"covered\":");
        try appendIntValue(alloc, out, coverage.covered);
        try out.appendSlice(alloc, ",\"settled\":");
        try appendIntValue(alloc, out, coverage.settled);
        try out.appendSlice(alloc, ",\"uncovered\":");
        if (coverage.uncovered) |uncovered| {
            try appendIntValue(alloc, out, uncovered);
        } else {
            try out.appendSlice(alloc, "null");
        }
        try out.appendSlice(alloc, ",\"pending\":");
        if (coverage.pending) |pending| {
            try appendIntValue(alloc, out, pending);
        } else {
            try out.appendSlice(alloc, "null");
        }
        try out.appendSlice(alloc, ",\"complete\":");
        try out.appendSlice(alloc, if (coverage_complete) "true" else "false");
        try out.appendSlice(alloc, ",\"healthy\":");
        try out.appendSlice(alloc, if (coverage.healthy) "true" else "false");
        try out.appendSlice(alloc, ",\"degraded\":");
        try out.appendSlice(alloc, if (coverage.degraded) "true" else "false");
        try out.append(alloc, '}');
    }
    if (index_type == .graph) {
        try out.appendSlice(alloc, ",\"algebraic_graph\":{\"traversal\":{\"attempted\":");
        try appendIntValue(alloc, out, item.algebraic_graph_traversal_attempt_count);
        try out.appendSlice(alloc, ",\"proven\":");
        try appendIntValue(alloc, out, item.algebraic_graph_traversal_proven_count);
        try out.appendSlice(alloc, ",\"rejected\":");
        try appendIntValue(alloc, out, item.algebraic_graph_traversal_rejected_count);
        try out.appendSlice(alloc, ",\"fallback\":");
        try appendIntValue(alloc, out, item.algebraic_graph_traversal_fallback_count);
        try out.appendSlice(alloc, ",\"result_nodes\":");
        try appendIntValue(alloc, out, item.algebraic_graph_traversal_result_node_count);
        try out.appendSlice(alloc, "}}");
        if (graph_source_status) |source| {
            try out.appendSlice(alloc, ",\"source_artifact\":{");
            try appendJsonString(alloc, out, "name");
            try out.append(alloc, ':');
            try appendJsonString(alloc, out, source.artifact);
            try out.append(alloc, ',');
            try appendJsonString(alloc, out, "path");
            try out.append(alloc, ':');
            try appendJsonString(alloc, out, source.path);
            try out.append(alloc, ',');
            try appendJsonString(alloc, out, "format");
            try out.append(alloc, ':');
            try appendJsonString(alloc, out, source.format);
            try out.appendSlice(alloc, ",\"materialization_pending\":");
            try out.appendSlice(alloc, if (catch_up_active or replay_catch_up_required) "true" else "false");
            try out.append(alloc, '}');
        }
    }
    if (index_type == .algebraic) try appendAlgebraicIndexStatsFields(alloc, out, item);
    try out.appendSlice(alloc, ",\"replay_applied_sequence\":");
    try appendIntValue(alloc, out, replay_applied_sequence);
    try out.appendSlice(alloc, ",\"replay_target_sequence\":");
    try appendIntValue(alloc, out, replay_target_sequence);
    try out.appendSlice(alloc, ",\"replay_catch_up_required\":");
    try out.appendSlice(alloc, if (replay_catch_up_required) "true" else "false");
    if (@hasField(@TypeOf(item), "projection_checkpoint_status")) {
        try out.appendSlice(alloc, ",\"projection_checkpoint_status\":");
        try appendJsonString(alloc, out, if (embeddings_materialization_current) item.projection_checkpoint_status else "rebuilding");
        try out.appendSlice(alloc, ",\"projection_checkpoint_applied_sequence\":");
        try appendIntValue(alloc, out, if (embeddings_materialization_current) item.projection_checkpoint_applied_sequence else 0);
        try out.appendSlice(alloc, ",\"projection_checkpoint_generation\":");
        try appendIntValue(alloc, out, if (embeddings_materialization_current) item.projection_checkpoint_generation else 0);
        try out.appendSlice(alloc, ",\"projection_checkpoint_config_fingerprint\":");
        try appendCoverageFingerprint(alloc, out, if (embeddings_materialization_current) item.projection_checkpoint_config_hash else 0);
        try out.appendSlice(alloc, ",\"checkpoint_replay_tail_sequence_count\":");
        try appendIntValue(alloc, out, if (embeddings_materialization_current) item.checkpoint_replay_tail_sequence_count else 0);
    }
    if (@hasField(@TypeOf(item), "repair_degraded")) {
        try out.appendSlice(alloc, ",\"repair_degraded\":");
        try out.appendSlice(alloc, if (!embeddings_materialization_current or item.repair_degraded) "true" else "false");
    }
    if (@hasField(@TypeOf(item), "repair_issue_count")) {
        try out.appendSlice(alloc, ",\"repair_issue_count\":");
        try appendIntValue(alloc, out, if (embeddings_materialization_current) item.repair_issue_count else 0);
    }
    if (@hasField(@TypeOf(item), "repair_summary_ready")) {
        try out.appendSlice(alloc, ",\"repair_summary_ready\":");
        try out.appendSlice(alloc, if (embeddings_materialization_current and item.repair_summary_ready) "true" else "false");
    }
    if (@hasField(@TypeOf(item), "repair_issue_count_estimated")) {
        try out.appendSlice(alloc, ",\"repair_issue_count_estimated\":");
        try out.appendSlice(alloc, if (!embeddings_materialization_current or item.repair_issue_count_estimated) "true" else "false");
    }
    if (@hasField(@TypeOf(item), "repair_scan_issue_count")) {
        try out.appendSlice(alloc, ",\"repair_scan_issue_count\":");
        try appendIntValue(alloc, out, if (embeddings_materialization_current) item.repair_scan_issue_count else 0);
    }
    try out.appendSlice(alloc, ",\"runtime_present\":");
    try out.appendSlice(alloc, if (runtime_present) "true" else "false");
    const runtime_fresh = if (@hasField(@TypeOf(item), "runtime_fresh"))
        item.runtime_fresh
    else if (metadata) |md|
        runtime_present and md.freshness == .fresh
    else
        false;
    try out.appendSlice(alloc, ",\"runtime_fresh\":");
    try out.appendSlice(alloc, if (runtime_fresh) "true" else "false");
    if (resolution) |stats| try appendReplayStageStatus(alloc, out, "resolution", stats);
    if (promotion) |stats| try appendReplayStageStatus(alloc, out, "promotion", stats);
    if (index_type == .graph) try appendResolverReplayDiagnosticsStatus(alloc, out, resolver_replay);
    if (metadata) |md| {
        try out.appendSlice(alloc, ",\"runtime_source\":");
        try appendJsonString(alloc, out, statusSourceName(md.source));
        try out.appendSlice(alloc, ",\"runtime_freshness\":");
        try appendJsonString(alloc, out, statusFreshnessName(md.freshness));
    }
    try out.appendSlice(alloc, ",\"catch_up_active\":");
    try out.appendSlice(alloc, if (catch_up_active) "true" else "false");
    try out.appendSlice(alloc, ",\"catch_up_phase\":\"");
    try out.appendSlice(alloc, @tagName(catch_up_phase));
    try out.append(alloc, '"');
    try out.appendSlice(alloc, ",\"catch_up_applied_sequence\":");
    try appendIntValue(alloc, out, catch_up_applied_sequence);
    try out.appendSlice(alloc, ",\"catch_up_target_sequence\":");
    try appendIntValue(alloc, out, catch_up_target_sequence);
    if (@hasField(@TypeOf(item), "expected_group_count")) {
        try out.appendSlice(alloc, ",\"expected_groups\":");
        try appendIntValue(alloc, out, item.expected_group_count);
        try out.appendSlice(alloc, ",\"reported_groups\":");
        try appendIntValue(alloc, out, item.reported_group_count);
        try out.appendSlice(alloc, ",\"fresh_groups\":");
        try appendIntValue(alloc, out, item.fresh_group_count);
        try out.appendSlice(alloc, ",\"stale_groups\":");
        try appendIntValue(alloc, out, item.stale_group_count);
        try out.appendSlice(alloc, ",\"missing_groups\":");
        try appendIntValue(alloc, out, item.missing_group_count);
        try out.appendSlice(alloc, ",\"unknown_remote_groups\":");
        try appendIntValue(alloc, out, item.remote_unknown_group_count);
    }
    if (index_type == .full_text) {
        try out.appendSlice(alloc, ",\"text_merge\":");
        try appendTextMergeStatus(alloc, out, item.text_merge);
    }
    if (index_type == .embeddings) {
        try out.appendSlice(alloc, ",\"hbc_cache\":");
        try appendHbcCacheStatus(alloc, out, if (embeddings_materialization_current) item.hbc_cache else .{});
        try out.appendSlice(alloc, ",\"hbc_posting\":");
        try appendHbcPostingStatus(alloc, out, if (embeddings_materialization_current) item.hbc_posting else .{});
        try out.appendSlice(alloc, ",\"enrichment_runtime\":");
        try appendEnrichmentRuntimeStatus(alloc, out, visible_enrichment orelse .{});
    }
    try out.appendSlice(alloc, ",\"async_indexing\":");
    try appendAsyncIndexingStatus(alloc, out, async_indexing);
    try out.append(alloc, '}');
}

fn appendResolverReplayDiagnosticsStatus(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), stats: db_mod.types.ResolverReplayDiagnostics) !void {
    try out.appendSlice(alloc, ",\"resolver_replay\":{");
    try out.appendSlice(alloc, "\"resolver_count\":");
    try appendIntValue(alloc, out, stats.resolver_count);
    try out.appendSlice(alloc, ",\"resolution_runtime_present\":");
    try out.appendSlice(alloc, if (stats.resolution_runtime_present) "true" else "false");
    try out.appendSlice(alloc, ",\"resolution_worker_started\":");
    try out.appendSlice(alloc, if (stats.resolution_worker_started) "true" else "false");
    try out.appendSlice(alloc, ",\"promotion_runtime_present\":");
    try out.appendSlice(alloc, if (stats.promotion_runtime_present) "true" else "false");
    try out.appendSlice(alloc, ",\"promotion_worker_started\":");
    try out.appendSlice(alloc, if (stats.promotion_worker_started) "true" else "false");
    try out.appendSlice(alloc, ",\"resolvers\":[");
    for (stats.resolvers, 0..) |resolver, i| {
        if (i > 0) try out.appendSlice(alloc, ",");
        try out.appendSlice(alloc, "{");
        try appendJsonString(alloc, out, "name");
        try out.appendSlice(alloc, ":");
        try appendJsonString(alloc, out, resolver.name);
        try out.appendSlice(alloc, ",");
        try appendJsonString(alloc, out, "table");
        try out.appendSlice(alloc, ":");
        try appendJsonString(alloc, out, resolver.table);
        try out.appendSlice(alloc, ",");
        try appendJsonString(alloc, out, "source_artifact");
        try out.appendSlice(alloc, ":");
        try appendJsonString(alloc, out, resolver.source_artifact);
        try out.appendSlice(alloc, ",");
        try appendJsonString(alloc, out, "resolution_artifact");
        try out.appendSlice(alloc, ":");
        try appendJsonString(alloc, out, resolver.resolution_artifact);
        try out.appendSlice(alloc, "}");
    }
    try out.appendSlice(alloc, "]}");
}

fn appendReplayStageStatus(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), name: []const u8, stats: db_mod.types.ReplayStageStats) !void {
    try out.append(alloc, ',');
    try appendJsonString(alloc, out, name);
    try out.appendSlice(alloc, ":{");
    try out.appendSlice(alloc, "\"enabled\":");
    try out.appendSlice(alloc, if (stats.enabled) "true" else "false");
    try out.appendSlice(alloc, ",\"target_sequence\":");
    try appendIntValue(alloc, out, stats.target_sequence);
    try out.appendSlice(alloc, ",\"applied_sequence\":");
    try appendIntValue(alloc, out, stats.applied_sequence);
    try out.appendSlice(alloc, ",\"catch_up_required\":");
    try out.appendSlice(alloc, if (stats.catch_up_required) "true" else "false");
    try out.appendSlice(alloc, ",\"blocked\":");
    try out.appendSlice(alloc, if (stats.blocked) "true" else "false");
    try out.appendSlice(alloc, ",\"blocked_reason\":");
    try appendJsonString(alloc, out, stats.blocked_reason);
    try out.appendSlice(alloc, ",\"error_count\":");
    try appendIntValue(alloc, out, stats.error_count);
    try out.append(alloc, '}');
}

fn appendDbMutexStatus(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), stats: db_mod.types.DBMutexStats) !void {
    try out.append(alloc, '{');
    try out.appendSlice(alloc, "\"lock_calls\":");
    try appendIntValue(alloc, out, stats.lock_calls);
    try out.appendSlice(alloc, ",\"contended_calls\":");
    try appendIntValue(alloc, out, stats.contended_calls);
    try out.appendSlice(alloc, ",\"max_waiters\":");
    try appendIntValue(alloc, out, stats.max_waiters);
    try out.appendSlice(alloc, ",\"spin_loops\":");
    try appendIntValue(alloc, out, stats.spin_loops);
    try out.appendSlice(alloc, ",\"yield_loops\":");
    try appendIntValue(alloc, out, stats.yield_loops);
    try out.appendSlice(alloc, ",\"sleep_loops\":");
    try appendIntValue(alloc, out, stats.sleep_loops);
    try out.appendSlice(alloc, ",\"wait_ns\":");
    try appendIntValue(alloc, out, stats.wait_ns);
    try out.appendSlice(alloc, ",\"max_wait_ns\":");
    try appendIntValue(alloc, out, stats.max_wait_ns);
    try out.appendSlice(alloc, ",\"hold_ns\":");
    try appendIntValue(alloc, out, stats.hold_ns);
    try out.appendSlice(alloc, ",\"max_hold_ns\":");
    try appendIntValue(alloc, out, stats.max_hold_ns);
    try out.append(alloc, '}');
}

fn appendAppliedSequenceStatus(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), stats: db_mod.types.AppliedSequenceStats) !void {
    try out.append(alloc, '{');
    try out.appendSlice(alloc, "\"note_calls\":");
    try appendIntValue(alloc, out, stats.note_calls);
    try out.appendSlice(alloc, ",\"forced_flush_calls\":");
    try appendIntValue(alloc, out, stats.forced_flush_calls);
    try out.appendSlice(alloc, ",\"skipped_flush_calls\":");
    try appendIntValue(alloc, out, stats.skipped_flush_calls);
    try out.appendSlice(alloc, ",\"flush_calls\":");
    try appendIntValue(alloc, out, stats.flush_calls);
    try out.appendSlice(alloc, ",\"flushed_indexes\":");
    try appendIntValue(alloc, out, stats.flushed_indexes);
    try out.appendSlice(alloc, ",\"sync_ns\":");
    try appendIntValue(alloc, out, stats.sync_ns);
    try out.appendSlice(alloc, ",\"save_ns\":");
    try appendIntValue(alloc, out, stats.save_ns);
    try out.appendSlice(alloc, ",\"flush_ns\":");
    try appendIntValue(alloc, out, stats.flush_ns);
    try out.appendSlice(alloc, ",\"max_flush_ns\":");
    try appendIntValue(alloc, out, stats.max_flush_ns);
    try out.append(alloc, '}');
}

fn appendDenseCatchUpStatus(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), stats: db_mod.types.DenseCatchUpStats) !void {
    try out.append(alloc, '{');
    try out.appendSlice(alloc, "\"begin_calls\":");
    try appendIntValue(alloc, out, stats.begin_calls);
    try out.appendSlice(alloc, ",\"finish_calls\":");
    try appendIntValue(alloc, out, stats.finish_calls);
    try out.appendSlice(alloc, ",\"abort_calls\":");
    try appendIntValue(alloc, out, stats.abort_calls);
    try out.appendSlice(alloc, ",\"active\":");
    try out.appendSlice(alloc, if (stats.active) "true" else "false");
    try out.appendSlice(alloc, ",\"phase\":\"");
    try out.appendSlice(alloc, @tagName(stats.phase));
    try out.append(alloc, '"');
    try out.appendSlice(alloc, ",\"current_sequence\":");
    try appendIntValue(alloc, out, stats.current_sequence);
    try out.appendSlice(alloc, ",\"current_target_sequence\":");
    try appendIntValue(alloc, out, stats.current_target_sequence);
    try out.appendSlice(alloc, ",\"current_scanned_entries\":");
    try appendIntValue(alloc, out, stats.current_scanned_entries);
    try out.appendSlice(alloc, ",\"current_applied_entries\":");
    try appendIntValue(alloc, out, stats.current_applied_entries);
    try out.appendSlice(alloc, ",\"replay_scan_batches\":");
    try appendIntValue(alloc, out, stats.replay_scan_batches);
    try out.appendSlice(alloc, ",\"replay_hint_filter_skips\":");
    try appendIntValue(alloc, out, stats.replay_hint_filter_skips);
    try out.appendSlice(alloc, ",\"progress_updates\":");
    try appendIntValue(alloc, out, stats.progress_updates);
    try out.appendSlice(alloc, ",\"bulk_finish_windows\":");
    try appendIntValue(alloc, out, stats.bulk_finish_windows);
    try out.appendSlice(alloc, ",\"bulk_finish_split_steps\":");
    try appendIntValue(alloc, out, stats.bulk_finish_split_steps);
    try out.appendSlice(alloc, ",\"bulk_finish_deferred_leaf_splits\":");
    try appendIntValue(alloc, out, stats.bulk_finish_deferred_leaf_splits);
    try out.appendSlice(alloc, ",\"bulk_finish_current_window\":");
    try appendIntValue(alloc, out, stats.bulk_finish_current_window);
    try out.appendSlice(alloc, ",\"bulk_finish_current_window_split_steps\":");
    try appendIntValue(alloc, out, stats.bulk_finish_current_window_split_steps);
    try out.appendSlice(alloc, ",\"bulk_finish_current_window_ns\":");
    try appendIntValue(alloc, out, stats.bulk_finish_current_window_ns);
    try out.appendSlice(alloc, ",\"bulk_finish_max_window_ns\":");
    try appendIntValue(alloc, out, stats.bulk_finish_max_window_ns);
    try out.appendSlice(alloc, ",\"finish_ns\":");
    try appendIntValue(alloc, out, stats.finish_ns);
    try out.appendSlice(alloc, ",\"max_finish_ns\":");
    try appendIntValue(alloc, out, stats.max_finish_ns);
    try out.appendSlice(alloc, ",\"finalize_ns\":");
    try appendIntValue(alloc, out, stats.finalize_ns);
    try out.appendSlice(alloc, ",\"max_finalize_ns\":");
    try appendIntValue(alloc, out, stats.max_finalize_ns);
    try out.appendSlice(alloc, ",\"maintenance_calls\":");
    try appendIntValue(alloc, out, stats.maintenance_calls);
    try out.appendSlice(alloc, ",\"maintenance_steps\":");
    try appendIntValue(alloc, out, stats.maintenance_steps);
    try out.appendSlice(alloc, ",\"maintenance_ns\":");
    try appendIntValue(alloc, out, stats.maintenance_ns);
    try out.appendSlice(alloc, ",\"max_maintenance_ns\":");
    try appendIntValue(alloc, out, stats.max_maintenance_ns);
    try out.appendSlice(alloc, ",\"manifest_writes\":");
    try appendIntValue(alloc, out, stats.manifest_writes);
    try out.appendSlice(alloc, ",\"manifest_ns\":");
    try appendIntValue(alloc, out, stats.manifest_ns);
    try out.appendSlice(alloc, ",\"write_pressure_compactions\":");
    try appendIntValue(alloc, out, stats.write_pressure_compactions);
    try out.appendSlice(alloc, ",\"write_pressure_ns\":");
    try appendIntValue(alloc, out, stats.write_pressure_ns);
    try out.append(alloc, '}');
}

fn appendAsyncIndexingStatus(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), stats: db_mod.types.AsyncIndexingStats) !void {
    try out.append(alloc, '{');
    try out.appendSlice(alloc, "\"apply_mutex\":");
    try appendDbMutexStatus(alloc, out, stats.apply_mutex);
    try out.appendSlice(alloc, ",\"applied_sequence_mutex\":");
    try appendDbMutexStatus(alloc, out, stats.applied_sequence_mutex);
    try out.appendSlice(alloc, ",\"dense_finish_mutex\":");
    try appendDbMutexStatus(alloc, out, stats.dense_finish_mutex);
    try out.appendSlice(alloc, ",\"applied_sequence\":");
    try appendAppliedSequenceStatus(alloc, out, stats.applied_sequence);
    try out.appendSlice(alloc, ",\"startup\":");
    try appendStartupCatchUpStatus(alloc, out, stats.startup);
    try out.appendSlice(alloc, ",\"dense_catch_up\":");
    try appendDenseCatchUpStatus(alloc, out, stats.dense_catch_up);
    try out.append(alloc, '}');
}

fn appendStartupCatchUpStatus(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), stats: db_mod.types.StartupCatchUpStats) !void {
    try out.append(alloc, '{');
    try out.appendSlice(alloc, "\"active\":");
    try out.appendSlice(alloc, if (stats.active) "true" else "false");
    try out.appendSlice(alloc, ",\"phase\":");
    try appendJsonString(alloc, out, switch (stats.phase) {
        .idle => "idle",
        .opening_db => "opening_db",
        .artifact_rebuild => "artifact_rebuild",
        .startup_catch_up => "startup_catch_up",
    });
    try out.appendSlice(alloc, ",\"wal_retained_segments\":");
    try appendIntValue(alloc, out, stats.wal_retained_segments);
    try out.appendSlice(alloc, ",\"wal_retained_bytes\":");
    try appendIntValue(alloc, out, stats.wal_retained_bytes);
    try out.appendSlice(alloc, ",\"wal_checkpoint_oldest_retained_segment\":");
    try appendIntValue(alloc, out, stats.wal_checkpoint_oldest_retained_segment);
    try out.appendSlice(alloc, ",\"wal_checkpoint_covered_through_segment\":");
    try appendIntValue(alloc, out, stats.wal_checkpoint_covered_through_segment);
    try out.appendSlice(alloc, ",\"wal_checkpoint_current_segment\":");
    try appendIntValue(alloc, out, stats.wal_checkpoint_current_segment);
    try out.appendSlice(alloc, ",\"wal_checkpoint_lag_segments\":");
    try appendIntValue(alloc, out, stats.wal_checkpoint_lag_segments);
    try out.appendSlice(alloc, ",\"wal_replay_retained_segments\":");
    try appendIntValue(alloc, out, stats.wal_replay_retained_segments);
    try out.appendSlice(alloc, ",\"wal_replay_retained_bytes\":");
    try appendIntValue(alloc, out, stats.wal_replay_retained_bytes);
    try out.appendSlice(alloc, ",\"wal_replay_current_segment\":");
    try appendIntValue(alloc, out, stats.wal_replay_current_segment);
    try out.appendSlice(alloc, ",\"configured_indexes\":");
    try appendIntValue(alloc, out, stats.configured_indexes);
    try out.appendSlice(alloc, ",\"configured_dense_indexes\":");
    try appendIntValue(alloc, out, stats.configured_dense_indexes);
    try out.appendSlice(alloc, ",\"configured_sparse_indexes\":");
    try appendIntValue(alloc, out, stats.configured_sparse_indexes);
    try out.appendSlice(alloc, ",\"configured_full_text_indexes\":");
    try appendIntValue(alloc, out, stats.configured_full_text_indexes);
    try out.appendSlice(alloc, ",\"configured_graph_indexes\":");
    try appendIntValue(alloc, out, stats.configured_graph_indexes);
    try out.appendSlice(alloc, ",\"opened_indexes\":");
    try appendIntValue(alloc, out, stats.opened_indexes);
    try out.appendSlice(alloc, ",\"db_open_ns\":");
    try appendIntValue(alloc, out, stats.db_open_ns);
    try out.appendSlice(alloc, ",\"load_indexes_ns\":");
    try appendIntValue(alloc, out, stats.load_indexes_ns);
    try out.appendSlice(alloc, ",\"lsm_open_stores\":");
    try appendIntValue(alloc, out, stats.lsm_open_stores);
    try out.appendSlice(alloc, ",\"lsm_open_completed\":");
    try appendIntValue(alloc, out, stats.lsm_open_completed);
    try out.appendSlice(alloc, ",\"lsm_open_failed\":");
    try appendIntValue(alloc, out, stats.lsm_open_failed);
    try out.appendSlice(alloc, ",\"lsm_open_total_ns\":");
    try appendIntValue(alloc, out, stats.lsm_open_total_ns);
    try out.appendSlice(alloc, ",\"lsm_open_initializing_storage_ns\":");
    try appendIntValue(alloc, out, stats.lsm_open_initializing_storage_ns);
    try out.appendSlice(alloc, ",\"lsm_open_recovered_temp_cleanup_ns\":");
    try appendIntValue(alloc, out, stats.lsm_open_recovered_temp_cleanup_ns);
    try out.appendSlice(alloc, ",\"lsm_open_manifest_ns\":");
    try appendIntValue(alloc, out, stats.lsm_open_manifest_ns);
    try out.appendSlice(alloc, ",\"lsm_open_ensuring_dirs_ns\":");
    try appendIntValue(alloc, out, stats.lsm_open_ensuring_dirs_ns);
    try out.appendSlice(alloc, ",\"lsm_open_wal_replay_ns\":");
    try appendIntValue(alloc, out, stats.lsm_open_wal_replay_ns);
    try out.appendSlice(alloc, ",\"lsm_open_mounting_runs_ns\":");
    try appendIntValue(alloc, out, stats.lsm_open_mounting_runs_ns);
    try out.appendSlice(alloc, ",\"lsm_open_loaded_runs\":");
    try appendIntValue(alloc, out, stats.lsm_open_loaded_runs);
    try out.appendSlice(alloc, ",\"lsm_open_obsolete_paths\":");
    try appendIntValue(alloc, out, stats.lsm_open_obsolete_paths);
    try out.appendSlice(alloc, ",\"lsm_open_mutable_entries_after_replay\":");
    try appendIntValue(alloc, out, stats.lsm_open_mutable_entries_after_replay);
    try out.appendSlice(alloc, ",\"lsm_open_immutable_memtables_after_replay\":");
    try appendIntValue(alloc, out, stats.lsm_open_immutable_memtables_after_replay);
    try out.appendSlice(alloc, ",\"lsm_open_recovered_temp_files_deleted\":");
    try appendIntValue(alloc, out, stats.lsm_open_recovered_temp_files_deleted);
    try out.appendSlice(alloc, ",\"lsm_open_recovered_temp_bytes_deleted\":");
    try appendIntValue(alloc, out, stats.lsm_open_recovered_temp_bytes_deleted);
    try out.appendSlice(alloc, ",\"wal_replay_records\":");
    try appendIntValue(alloc, out, stats.wal_replay_records);
    try out.appendSlice(alloc, ",\"wal_replay_entries\":");
    try appendIntValue(alloc, out, stats.wal_replay_entries);
    try out.appendSlice(alloc, ",\"wal_replay_bytes\":");
    try appendIntValue(alloc, out, stats.wal_replay_bytes);
    try out.appendSlice(alloc, ",\"wal_replay_ns\":");
    try appendIntValue(alloc, out, stats.wal_replay_ns);
    try out.appendSlice(alloc, ",\"wal_replay_truncated_tail_bytes\":");
    try appendIntValue(alloc, out, stats.wal_replay_truncated_tail_bytes);
    try out.append(alloc, '}');
}

fn appendHbcCacheKindStatus(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), stats: db_mod.types.HbcCacheKindStats) !void {
    try out.append(alloc, '{');
    try out.appendSlice(alloc, "\"used_bytes\":");
    try appendIntValue(alloc, out, stats.used_bytes);
    try out.appendSlice(alloc, ",\"peak_bytes\":");
    try appendIntValue(alloc, out, stats.peak_bytes);
    try out.appendSlice(alloc, ",\"insertions\":");
    try appendIntValue(alloc, out, stats.insertions);
    try out.appendSlice(alloc, ",\"admission_skips\":");
    try appendIntValue(alloc, out, stats.admission_skips);
    try out.appendSlice(alloc, ",\"evictions\":");
    try appendIntValue(alloc, out, stats.evictions);
    try out.append(alloc, '}');
}

fn appendHbcCacheStatus(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), stats: db_mod.types.HbcCacheStats) !void {
    try out.append(alloc, '{');
    try out.appendSlice(alloc, "\"total_bytes\":");
    try appendIntValue(alloc, out, stats.total_bytes);
    try out.appendSlice(alloc, ",\"accounted_bytes\":");
    try appendIntValue(alloc, out, stats.accounted_bytes);
    try out.appendSlice(alloc, ",\"node\":");
    try appendHbcCacheKindStatus(alloc, out, stats.node);
    try out.appendSlice(alloc, ",\"quantized\":");
    try appendHbcCacheKindStatus(alloc, out, stats.quantized);
    try out.appendSlice(alloc, ",\"vector\":");
    try appendHbcCacheKindStatus(alloc, out, stats.vector);
    try out.appendSlice(alloc, ",\"metadata\":");
    try appendHbcCacheKindStatus(alloc, out, stats.metadata);
    try out.append(alloc, '}');
}

fn appendHbcPostingStatus(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), stats: db_mod.types.HbcPostingStats) !void {
    try out.append(alloc, '{');
    try out.appendSlice(alloc, "\"scanned_nodes\":");
    try appendIntValue(alloc, out, stats.scanned_nodes);
    try out.appendSlice(alloc, ",\"scanned_postings\":");
    try appendIntValue(alloc, out, stats.scanned_postings);
    try out.appendSlice(alloc, ",\"dirty_postings\":");
    try appendIntValue(alloc, out, stats.dirty_postings);
    try out.appendSlice(alloc, ",\"centroid_dirty_postings\":");
    try appendIntValue(alloc, out, stats.centroid_dirty_postings);
    try out.appendSlice(alloc, ",\"payload_dirty_postings\":");
    try appendIntValue(alloc, out, stats.payload_dirty_postings);
    try out.appendSlice(alloc, ",\"max_centroid_version_lag\":");
    try appendIntValue(alloc, out, stats.max_centroid_version_lag);
    try out.appendSlice(alloc, ",\"max_payload_version_lag\":");
    try appendIntValue(alloc, out, stats.max_payload_version_lag);
    try out.appendSlice(alloc, ",\"max_mutation_version\":");
    try appendIntValue(alloc, out, stats.max_mutation_version);
    try out.appendSlice(alloc, ",\"skipped_missing\":");
    try appendIntValue(alloc, out, stats.skipped_missing);
    try out.appendSlice(alloc, ",\"maintenance_scanned_nodes\":");
    try appendIntValue(alloc, out, stats.maintenance_scanned_nodes);
    try out.appendSlice(alloc, ",\"maintenance_scanned_postings\":");
    try appendIntValue(alloc, out, stats.maintenance_scanned_postings);
    try out.appendSlice(alloc, ",\"maintenance_dirty_postings\":");
    try appendIntValue(alloc, out, stats.maintenance_dirty_postings);
    try out.appendSlice(alloc, ",\"maintenance_repaired_postings\":");
    try appendIntValue(alloc, out, stats.maintenance_repaired_postings);
    try out.appendSlice(alloc, ",\"maintenance_centroid_refreshed\":");
    try appendIntValue(alloc, out, stats.maintenance_centroid_refreshed);
    try out.appendSlice(alloc, ",\"maintenance_payload_refreshed\":");
    try appendIntValue(alloc, out, stats.maintenance_payload_refreshed);
    try out.appendSlice(alloc, ",\"maintenance_ancestor_refresh_roots\":");
    try appendIntValue(alloc, out, stats.maintenance_ancestor_refresh_roots);
    try out.appendSlice(alloc, ",\"maintenance_split_postings\":");
    try appendIntValue(alloc, out, stats.maintenance_split_postings);
    try out.appendSlice(alloc, ",\"maintenance_merged_postings\":");
    try appendIntValue(alloc, out, stats.maintenance_merged_postings);
    try out.appendSlice(alloc, ",\"maintenance_boundary_reassigned_vectors\":");
    try appendIntValue(alloc, out, stats.maintenance_boundary_reassigned_vectors);
    try out.appendSlice(alloc, ",\"lazy_centroid_deferrals\":");
    try appendIntValue(alloc, out, stats.lazy_centroid_deferrals);
    try out.appendSlice(alloc, ",\"lazy_payload_deferrals\":");
    try appendIntValue(alloc, out, stats.lazy_payload_deferrals);
    try out.appendSlice(alloc, ",\"lazy_ancestor_deferrals\":");
    try appendIntValue(alloc, out, stats.lazy_ancestor_deferrals);
    try out.append(alloc, '}');
}

fn appendTextMergeStatus(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), stats: db_mod.types.TextMergeStats) !void {
    try out.append(alloc, '{');
    try out.appendSlice(alloc, "\"active_segments\":");
    try appendIntValue(alloc, out, stats.active_segments);
    try out.appendSlice(alloc, ",\"max_active_segments_per_index\":");
    try appendIntValue(alloc, out, stats.max_active_segments_per_index);
    try out.appendSlice(alloc, ",\"pending_segments\":");
    try appendIntValue(alloc, out, stats.pending_segments);
    try out.appendSlice(alloc, ",\"pending_bytes\":");
    try appendIntValue(alloc, out, stats.pending_bytes);
    try out.appendSlice(alloc, ",\"pending_heap_bytes\":");
    try appendIntValue(alloc, out, stats.pending_heap_bytes);
    try out.appendSlice(alloc, ",\"pending_mmap_bytes\":");
    try appendIntValue(alloc, out, stats.pending_mmap_bytes);
    try out.appendSlice(alloc, ",\"in_flight_merges\":");
    try appendIntValue(alloc, out, stats.in_flight_merges);
    try out.appendSlice(alloc, ",\"failed_merges\":");
    try appendIntValue(alloc, out, stats.failed_merges);
    try out.appendSlice(alloc, ",\"merge_input_segments_total\":");
    try appendIntValue(alloc, out, stats.merge_input_segments_total);
    try out.appendSlice(alloc, ",\"merge_input_bytes_total\":");
    try appendIntValue(alloc, out, stats.merge_input_bytes_total);
    try out.appendSlice(alloc, ",\"merge_output_segments_total\":");
    try appendIntValue(alloc, out, stats.merge_output_segments_total);
    try out.appendSlice(alloc, ",\"merge_output_bytes_total\":");
    try appendIntValue(alloc, out, stats.merge_output_bytes_total);
    try out.appendSlice(alloc, ",\"last_merge_input_segments\":");
    try appendIntValue(alloc, out, stats.last_merge_input_segments);
    try out.appendSlice(alloc, ",\"last_merge_input_bytes\":");
    try appendIntValue(alloc, out, stats.last_merge_input_bytes);
    try out.appendSlice(alloc, ",\"last_merge_output_segments\":");
    try appendIntValue(alloc, out, stats.last_merge_output_segments);
    try out.appendSlice(alloc, ",\"last_merge_output_bytes\":");
    try appendIntValue(alloc, out, stats.last_merge_output_bytes);
    try out.appendSlice(alloc, ",\"quarantined_merges\":");
    try appendIntValue(alloc, out, stats.quarantined_merges);
    try out.appendSlice(alloc, ",\"quarantined_segments\":");
    try appendIntValue(alloc, out, stats.quarantined_segments);
    try out.appendSlice(alloc, ",\"retry_after_ns\":");
    try appendIntValue(alloc, out, stats.retry_after_ns);
    try out.appendSlice(alloc, ",\"deferred_for_pressure\":");
    try appendIntValue(alloc, out, stats.deferred_for_pressure);
    try out.appendSlice(alloc, ",\"last_merge_error\":");
    try appendJsonString(alloc, out, stats.last_merge_error);
    try out.append(alloc, '}');
}

pub fn inferIndexType(index_name: []const u8, config: std.json.Value) ?ApiIndexType {
    if (config != .object) return null;
    if (config.object.get("type")) |type_value| {
        if (type_value != .string) return null;
        if (std.mem.eql(u8, type_value.string, "full_text")) return .full_text;
        if (std.mem.eql(u8, type_value.string, "embeddings")) return .embeddings;
        if (std.mem.eql(u8, type_value.string, "graph")) return .graph;
        if (std.mem.eql(u8, type_value.string, "algebraic")) return .algebraic;
        return null;
    }
    if (config.object.get("dimension") != null or
        config.object.get("sparse") != null or
        config.object.get("chunker") != null or
        config.object.get("embedder") != null or
        config.object.get("generator") != null)
    {
        return .embeddings;
    }
    if (config.object.get("edge_type_configs") != null or
        config.object.get("store_reverse_edges") != null)
    {
        return .graph;
    }
    if (std.mem.eql(u8, index_name, tables_api.default_full_text_index_name)) return .full_text;
    if (std.mem.startsWith(u8, index_name, "full_text_index_v")) return .full_text;
    if (std.mem.eql(u8, index_name, "default")) return .full_text;
    return null;
}

fn appendIntValue(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: u64) !void {
    const encoded = try std.fmt.allocPrint(alloc, "{d}", .{value});
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
}

fn findIndexStatus(indexes: []const db_mod.types.DBIndexStats, index_name: []const u8) ?db_mod.types.DBIndexStats {
    for (indexes) |item| {
        if (std.mem.eql(u8, item.name, index_name)) return item;
    }
    return null;
}

test "index encoders expose metadata-backed configs" {
    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"search_idx\":{\"type\":\"full_text\"},\"embed_idx\":{\"type\":\"embeddings\",\"dimension\":384}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded_list = (try encodeIndexList(std.testing.allocator, &snapshot, "docs", null)).?;
    defer std.testing.allocator.free(encoded_list);
    try std.testing.expect(std.mem.indexOf(u8, encoded_list, "\"type\":\"full_text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded_list, "\"dimension\":384") != null);

    const encoded_single = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "embed_idx", null)).?;
    defer std.testing.allocator.free(encoded_single);
    try std.testing.expect(std.mem.indexOf(u8, encoded_single, "\"name\":\"embed_idx\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded_single, "\"type\":\"embeddings\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded_single, "\"status\":{\"index_type\":\"embeddings\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded_single, "\"runtime_present\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded_single, "\"shard_status\":{}") != null);
}

test "index config map encoder injects canonical name and type" {
    const encoded = try encodeIndexConfigMap(std.testing.allocator, "{\"full_text_index_v0\":{},\"embed_idx\":{\"type\":\"embeddings\",\"dimension\":384}}");
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"full_text_index_v0\":{\"name\":\"full_text_index_v0\",\"type\":\"full_text\"}") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"embed_idx\":{\"name\":\"embed_idx\",\"type\":\"embeddings\",\"dimension\":384}") != null);
}

test "public index config encoders redact coverage incarnation" {
    const indexes_json =
        \\{"embed_idx":{"type":"embeddings","dimension":384,"_coverage_incarnation":42}}
    ;
    const encoded_map = try encodeIndexConfigMap(std.testing.allocator, indexes_json);
    defer std.testing.allocator.free(encoded_map);
    try ant_json.testing.expectEqualJsonText(
        std.testing.allocator,
        "{\"embed_idx\":{\"name\":\"embed_idx\",\"type\":\"embeddings\",\"dimension\":384}}",
        encoded_map,
    );

    const encoded_single = (try encodeSingleIndexConfig(std.testing.allocator, indexes_json, "embed_idx")).?;
    defer std.testing.allocator.free(encoded_single);
    try ant_json.testing.expectEqualJsonText(
        std.testing.allocator,
        "{\"name\":\"embed_idx\",\"type\":\"embeddings\",\"dimension\":384}",
        encoded_single,
    );
}

fn expectCreatedObjectAllowlistCovers(
    comptime T: type,
    shape: public_index_contract.CreatedObjectShape,
) !void {
    inline for (@typeInfo(T).@"struct".fields) |field| {
        try std.testing.expect(public_index_contract.isAllowedCreatedObjectField(shape, field.name));
    }
}

test "created nested response allowlists cover generated schemas" {
    try expectCreatedObjectAllowlistCovers(indexes_openapi.CreatedProviderConfig, .provider);
    try expectCreatedObjectAllowlistCovers(indexes_openapi.CreatedEnrichmentConfig, .enrichment);
    try expectCreatedObjectAllowlistCovers(indexes_openapi.GraphArtifactSourceConfig, .graph_source);
    try expectCreatedObjectAllowlistCovers(indexes_openapi.CreatedGraphArtifactProducerConfig, .graph_artifact);
    try expectCreatedObjectAllowlistCovers(indexes_openapi.GraphArtifactNodeMappingConfig, .graph_nodes);
    try expectCreatedObjectAllowlistCovers(indexes_openapi.GraphArtifactEdgeMappingConfig, .graph_edge);
    try expectCreatedObjectAllowlistCovers(indexes_openapi.GraphArtifactContextConfig, .graph_context);
    try expectCreatedObjectAllowlistCovers(indexes_openapi.GraphAlgebraicPlanningConfig, .graph_algebraic_planning);
    try expectCreatedObjectAllowlistCovers(indexes_openapi.GraphBoundedTraversalConfig, .graph_bounded_traversal);
    try expectCreatedObjectAllowlistCovers(indexes_openapi.EdgeTypeConfig, .edge_type);
    try expectCreatedObjectAllowlistCovers(indexes_openapi.GraphResolverConfig, .graph_resolver);
    try expectCreatedObjectAllowlistCovers(chunking_openapi.ChunkerConfig, .chunker);
    try expectCreatedObjectAllowlistCovers(chunking_api_openapi.TextChunkOptions, .chunker_text);
    try expectCreatedObjectAllowlistCovers(chunking_api_openapi.AudioChunkOptions, .chunker_audio);
    try expectCreatedObjectAllowlistCovers(indexes_openapi.IndexExecutionConfig, .index_execution);
    try expectCreatedObjectAllowlistCovers(indexes_openapi.ExecutionPolicy, .execution_policy);

    inline for (@typeInfo(indexes_openapi.GraphArtifactProducerConfig).@"struct".fields) |field| {
        try std.testing.expect(public_index_contract.isAllowedGraphArtifactRequestField(field.name));
    }
    inline for (@typeInfo(indexes_openapi.EnrichmentConfig).@"struct".fields) |field| {
        try std.testing.expect(public_index_contract.isAllowedEnrichmentRequestField(field.name));
    }

    inline for (.{
        public_index_contract.CreatedObjectShape.provider,
        .enrichment,
        .graph_source,
        .graph_artifact,
        .graph_nodes,
        .graph_edge,
        .graph_context,
        .graph_algebraic_planning,
        .graph_bounded_traversal,
        .edge_type,
        .graph_resolver,
        .chunker,
        .chunker_text,
        .chunker_audio,
        .index_execution,
        .execution_policy,
    }) |shape| {
        try std.testing.expect(!public_index_contract.isAllowedCreatedObjectField(shape, "client_value"));
        try std.testing.expect(!public_index_contract.isAllowedCreatedObjectField(shape, "settings"));
    }
}

test "public index config encoders redact nested credentials" {
    const indexes_json =
        \\{"embed_idx":{"type":"embeddings","dimension":384,"source_artifact_name":{"client_value":"private-root-value"},"embedder":{"provider":"openai","model":"text-embedding-3-small","models":{"client_value":"private-models-value"},"api_key":"sk-private","secret_key":"private-secret-key","url":"https://alice:private-password@example.com/v1","endpoint":"https://example.com/v1?auth=private-endpoint-auth","base_url":"https://example.com/oauth/callback#access_token=private-fragment-token","client_value":"private-unknown-field","settings":{"opaque":"private-nested-value"}},"summarizer":{"provider":"gemini","model":"gemini-2.5-flash","api_key":"${secret:gemini_key}","api_url":"https://example.com/v1?access%5Fkey%5Fid=private-url-key"},"chunker":{"provider":"antfly","model":"fixed","api_url":"https://chunker.example.com","store_chunks":true,"max_chunks":50,"threshold":0.75,"client_value":"private-chunker-value","text":{"target_tokens":500,"overlap_tokens":50,"separator":"---","client_value":"private-text-value"},"audio":{"window_duration_ms":30000,"overlap_duration_ms":500,"client_value":"private-audio-value"},"full_text_index":{"analyzer":"standard"}},"execution":{"embedding":{"batch_items":32,"batch_bytes":{"client_value":"private-batch-value"},"client_value":"private-execution-value"},"chunking":"private-malformed-policy","settings":{"opaque":"private-execution-settings"}},"enrichments":[{"name":"asset_v1","kind":"asset","content_type":{"client_value":"private-content-type"},"client_value":"private-enrichment-value","producer_json":"{\"provider\":\"s3\",\"secret_access_key\":\"unknown-provider-secret\",\"access_token\":\"private-token\",\"headers\":{\"Authorization\":\"Bearer private-auth\",\"X-Auth\":\"future-private-header\",\"Accept\":\"text/html\"},\"format\":\"html\"}","execution":{"batch_items":4,"settings":{"opaque":"private-policy-settings"}}}]}}
    ;

    const encoded_single = (try encodeSingleIndexConfig(std.testing.allocator, indexes_json, "embed_idx")).?;
    defer std.testing.allocator.free(encoded_single);
    try ant_json.testing.expectEqualJsonText(
        std.testing.allocator,
        "{\"name\":\"embed_idx\",\"type\":\"embeddings\",\"dimension\":384,\"embedder\":{\"provider\":\"openai\",\"model\":\"text-embedding-3-small\"},\"summarizer\":{\"provider\":\"gemini\",\"model\":\"gemini-2.5-flash\"},\"chunker\":{\"provider\":\"antfly\",\"model\":\"fixed\",\"api_url\":\"https://chunker.example.com\",\"store_chunks\":true,\"max_chunks\":50,\"threshold\":0.75,\"text\":{\"target_tokens\":500,\"overlap_tokens\":50,\"separator\":\"---\"},\"audio\":{\"window_duration_ms\":30000,\"overlap_duration_ms\":500},\"full_text_index\":{\"analyzer\":\"standard\"}},\"execution\":{\"embedding\":{\"batch_items\":32}},\"enrichments\":[\"asset_v1\"]}",
        encoded_single,
    );

    const created = try encodeCreatedIndexConfig(std.testing.allocator, "embed_idx", indexes_json[13 .. indexes_json.len - 1]);
    defer std.testing.allocator.free(created);
    try ant_json.testing.expectEqualJsonText(
        std.testing.allocator,
        "{\"name\":\"embed_idx\",\"type\":\"embeddings\",\"dimension\":384,\"embedder\":{\"provider\":\"openai\",\"model\":\"text-embedding-3-small\"},\"summarizer\":{\"provider\":\"gemini\",\"model\":\"gemini-2.5-flash\"},\"chunker\":{\"provider\":\"antfly\",\"model\":\"fixed\",\"api_url\":\"https://chunker.example.com\",\"store_chunks\":true,\"max_chunks\":50,\"threshold\":0.75,\"text\":{\"target_tokens\":500,\"overlap_tokens\":50,\"separator\":\"---\"},\"audio\":{\"window_duration_ms\":30000,\"overlap_duration_ms\":500},\"full_text_index\":{\"analyzer\":\"standard\"}},\"execution\":{\"embedding\":{\"batch_items\":32}},\"enrichments\":[{\"name\":\"asset_v1\",\"kind\":\"asset\",\"execution\":{\"batch_items\":4}}]}",
        created,
    );
}

test "public index config encoders retain credential-free provider urls" {
    const config =
        \\{"type":"embeddings","dimension":384,"embedder":{"provider":"openai","model":"text-embedding-3-small","url":"https://api.example.com/v1?api-version=2026-08-01#documentation","endpoint":"https://gateway.example.com/v1?region=us-west-2"}}
    ;
    const created = try encodeCreatedIndexConfig(std.testing.allocator, "embed_idx", config);
    defer std.testing.allocator.free(created);
    try ant_json.testing.expectEqualJsonText(
        std.testing.allocator,
        "{\"name\":\"embed_idx\",\"type\":\"embeddings\",\"dimension\":384,\"embedder\":{\"provider\":\"openai\",\"model\":\"text-embedding-3-small\",\"url\":\"https://api.example.com/v1?api-version=2026-08-01#documentation\"}}",
        created,
    );
}

test "public index config encoders omit root write-only producer documents" {
    const config =
        \\{"type":"embeddings","external":true,"dimension":384,"producer_json":"{\"secret_access_key\":\"private\"}","future_provider_secret":"private-too"}
    ;
    const created = try encodeCreatedIndexConfig(std.testing.allocator, "embed_idx", config);
    defer std.testing.allocator.free(created);
    try ant_json.testing.expectEqualJsonText(
        std.testing.allocator,
        "{\"name\":\"embed_idx\",\"type\":\"embeddings\",\"external\":true,\"dimension\":384}",
        created,
    );
}

test "created graph index response projects closed nested schemas" {
    const config =
        \\{"type":"graph","template":{"client_value":"private-root-value"},"source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":{"client_value":"private-format-value"},"client_value":"private-source-value","settings":{"opaque":"private-source-settings"}},"artifact":{"name":"relations_v1","kind":"asset","field":"relations","template":"{{ body }}","content_type":{"client_value":"private-content-type"},"producer_json":{"provider":"private","api_key":"private-key"},"execution":{"batch_items":8,"settings":{"opaque":"private-execution-settings"}},"client_value":"private-artifact-value","settings":{"opaque":"private-artifact-settings"}},"nodes":{"model":"document","source":"{{ _doc.key }}","target":"{{ _item.target.text }}","client_value":"private-node-value"},"edge":{"type":"{{ _item.predicate }}","weight":0.75,"metadata":{"source":"extractor","api_key":"private-metadata-key","nested":{"label":"public","authorization":"private-authorization"}},"client_value":"private-edge-mapping-value"},"context":{"doc_fields":["title","body"],"client_value":"private-context-value"},"algebraic_planning":{"bounded_traversal":{"law":"provenance_semiring","enabled":true,"client_value":"private-bounded-value"},"client_value":"private-planning-value"},"edge_types":[{"name":"mentions","field":"relations","topology":"graph","max_weight":0.9,"min_weight":0,"allow_self_loops":false,"required_metadata":["source","confidence"],"client_value":"private-edge-value"},{"name":"malformed","required_metadata":{"client_value":"private-metadata-value"}},{"name":{"client_value":"private-required-value"}}],"resolvers":["private-malformed-resolver",{"name":"kg","table":"entities","source_artifact":"relations_v1","resolution_artifact":"resolution_v1","key_template":"{{label}}","candidate_search":"prefix","candidate_limit":{"client_value":"private-limit-value"},"client_value":"private-resolver-value","settings":{"opaque":"private-resolver-settings"}}]}
    ;
    const expected =
        \\{"name":"relations_graph","type":"graph","source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]"},"artifact":{"name":"relations_v1","kind":"asset","field":"relations","template":"{{ body }}","execution":{"batch_items":8}},"nodes":{"model":"document","source":"{{ _doc.key }}","target":"{{ _item.target.text }}"},"edge":{"type":"{{ _item.predicate }}","weight":0.75,"metadata":{"source":"extractor","nested":{"label":"public"}}},"context":{"doc_fields":["title","body"]},"algebraic_planning":{"bounded_traversal":{"law":"provenance_semiring"}},"edge_types":[{"name":"mentions","field":"relations","topology":"graph","max_weight":0.9,"min_weight":0,"allow_self_loops":false,"required_metadata":["source","confidence"]},{"name":"malformed"}],"resolvers":[{"name":"kg","table":"entities","source_artifact":"relations_v1","resolution_artifact":"resolution_v1","key_template":"{{label}}","candidate_search":"prefix"}]}
    ;
    const created = try encodeCreatedIndexConfig(std.testing.allocator, "relations_graph", config);
    defer std.testing.allocator.free(created);
    try ant_json.testing.expectEqualJsonText(std.testing.allocator, expected, created);

    const indexes_json = "{\"relations_graph\":" ++ config ++ "}";
    const stored = (try encodeSingleIndexConfig(std.testing.allocator, indexes_json, "relations_graph")).?;
    defer std.testing.allocator.free(stored);
    try ant_json.testing.expectEqualJsonText(std.testing.allocator, expected, stored);
}

test "identical index mutation retries preserve coverage incarnation" {
    const alloc = std.testing.allocator;
    const requested = "{\"type\":\"embeddings\",\"external\":true,\"dimension\":384}";
    const first = try addIndexToTableIndexesJson(alloc, tables_api.default_indexes_json, "embed_idx", requested);
    defer alloc.free(first);
    const retried = try addIndexToTableIndexesJson(
        alloc,
        first,
        "embed_idx",
        "{\"dimension\":384,\"external\":true,\"type\":\"embeddings\"}",
    );
    defer alloc.free(retried);

    var first_lookup = (try lookupSingleIndexConfig(alloc, first, "embed_idx")).?;
    defer first_lookup.deinit();
    var retried_lookup = (try lookupSingleIndexConfig(alloc, retried, "embed_idx")).?;
    defer retried_lookup.deinit();
    const first_incarnation = coverage_policy_mod.incarnation(first_lookup.config) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(first_incarnation, coverage_policy_mod.incarnation(retried_lookup.config).?);

    const tuned = try addIndexToTableIndexesJson(
        alloc,
        retried,
        "embed_idx",
        "{\"type\":\"embeddings\",\"external\":true,\"dimension\":384,\"execution\":{\"embedding\":{\"batch_items\":64}}}",
    );
    defer alloc.free(tuned);
    var tuned_lookup = (try lookupSingleIndexConfig(alloc, tuned, "embed_idx")).?;
    defer tuned_lookup.deinit();
    try std.testing.expectEqual(first_incarnation, coverage_policy_mod.incarnation(tuned_lookup.config).?);
    try std.testing.expect(tuned_lookup.config.object.get("execution") != null);

    const changed = try addIndexToTableIndexesJson(
        alloc,
        tuned,
        "embed_idx",
        "{\"type\":\"embeddings\",\"external\":true,\"dimension\":768}",
    );
    defer alloc.free(changed);
    var changed_lookup = (try lookupSingleIndexConfig(alloc, changed, "embed_idx")).?;
    defer changed_lookup.deinit();
    try std.testing.expect(first_incarnation != coverage_policy_mod.incarnation(changed_lookup.config).?);
}

test "index status encoder projects inline enrichment configs as names" {
    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json =
            \\{"document_text":{"type":"full_text","enrichments":[{"name":"document_units_v1","kind":"asset"},{"name":"document_chunks_v1","kind":"chunk"}]},"document_vectors":{"type":"embeddings","enrichments":[{"name":"document_chunk_dense_v1","kind":"embedding"}]}}
            ,
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded_list = (try encodeIndexList(std.testing.allocator, &snapshot, "docs", null)).?;
    defer std.testing.allocator.free(encoded_list);
    try std.testing.expect(std.mem.indexOf(u8, encoded_list, "\"enrichments\":[\"document_units_v1\",\"document_chunks_v1\"]") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded_list, "\"enrichments\":[\"document_chunk_dense_v1\"]") != null);
}

test "single index config encoder isolates requested index" {
    const encoded = (try encodeSingleIndexConfig(
        std.testing.allocator,
        "{\"full_text_index_v0\":{},\"semantic_chunked_idx\":{\"field\":\"body\",\"dimension\":3}}",
        "full_text_index_v0",
    )).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expectEqualStrings(
        "{\"name\":\"full_text_index_v0\",\"type\":\"full_text\"}",
        encoded,
    );
}

test "single index config encoder infers shorthand embeddings type" {
    const encoded = (try encodeSingleIndexConfig(
        std.testing.allocator,
        "{\"semantic_chunked_idx\":{\"field\":\"body\",\"dimension\":3,\"chunker\":{\"provider\":\"antfly\"}}}",
        "semantic_chunked_idx",
    )).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"name\":\"semantic_chunked_idx\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"type\":\"embeddings\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"dimension\":3") != null);
}

test "single index helpers use default index metadata when indexes_json is empty" {
    const snapshot = metadata_api.AdminSnapshot{
        .status = .{ .metadata_group_id = 0, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded_status = (try encodeSingleIndex(
        std.testing.allocator,
        &snapshot,
        "docs",
        "full_text_index_v0",
        null,
    )).?;
    defer std.testing.allocator.free(encoded_status);
    try std.testing.expect(std.mem.indexOf(u8, encoded_status, "\"name\":\"full_text_index_v0\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded_status, "\"type\":\"full_text\"") != null);

    const encoded_list = (try encodeIndexList(std.testing.allocator, &snapshot, "docs", null)).?;
    defer std.testing.allocator.free(encoded_list);
    try std.testing.expect(std.mem.indexOf(u8, encoded_list, "\"name\":\"full_text_index_v0\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded_list, "\"type\":\"full_text\"") != null);

    const encoded_config = (try encodeSingleIndexConfig(
        std.testing.allocator,
        "",
        "full_text_index_v0",
    )).?;
    defer std.testing.allocator.free(encoded_config);
    try std.testing.expectEqualStrings(
        "{\"name\":\"full_text_index_v0\",\"type\":\"full_text\"}",
        encoded_config,
    );
}

test "index metadata helpers add and remove entries" {
    const added = try addIndexToTableIndexesJson(std.testing.allocator, "{\"default\":{\"type\":\"full_text\"}}", "embed_idx", "{\"type\":\"embeddings\",\"dimension\":384}");
    defer std.testing.allocator.free(added);
    try std.testing.expect(std.mem.indexOf(u8, added, "\"embed_idx\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, added, "\"dimension\":384") != null);

    const removed = (try removeIndexFromTableIndexesJson(std.testing.allocator, added, "default")).?;
    defer std.testing.allocator.free(removed);
    try std.testing.expect(std.mem.indexOf(u8, removed, "\"default\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, removed, "\"embed_idx\"") != null);
}

test "index metadata helpers add replace and remove enrichments" {
    const added = try addEnrichmentToTableIndexesJson(
        std.testing.allocator,
        "{\"default\":{\"type\":\"full_text\"}}",
        "memory_embed",
        "{\"name\":\"memory_embed\",\"kind\":\"embedding\",\"field\":\"body\",\"expected_dims\":384}",
    );
    defer std.testing.allocator.free(added);
    try std.testing.expect(std.mem.indexOf(u8, added, "\"enrichments\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, added, "\"memory_embed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, added, "\"default\"") != null);

    const replaced = try addEnrichmentToTableIndexesJson(
        std.testing.allocator,
        added,
        "memory_embed",
        "{\"name\":\"memory_embed\",\"kind\":\"embedding\",\"field\":\"summary\",\"expected_dims\":384}",
    );
    defer std.testing.allocator.free(replaced);
    try std.testing.expect(std.mem.indexOf(u8, replaced, "\"summary\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, replaced, "\"body\"") == null);

    const removed = (try removeEnrichmentFromTableIndexesJson(std.testing.allocator, replaced, "memory_embed")).?;
    defer std.testing.allocator.free(removed);
    try std.testing.expect(std.mem.indexOf(u8, removed, "\"memory_embed\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, removed, "\"default\"") != null);
}

test "index metadata validates artifact enrichment graph" {
    try std.testing.expectError(
        error.InvalidEnrichmentConfig,
        validateArtifactEnrichmentsForTableIndexesJson(
            std.testing.allocator,
            "{\"enrichments\":[{\"name\":\"chunks\",\"kind\":\"chunk\",\"chunk_size\":512}]}",
        ),
    );
    try std.testing.expectError(
        error.InvalidEnrichmentConfig,
        validateArtifactEnrichmentsForTableIndexesJson(
            std.testing.allocator,
            "{\"enrichments\":[{\"name\":\"chunks\",\"kind\":\"chunk\",\"field\":\"text\",\"source_artifact_name\":\"units\",\"chunk_size\":512}]}",
        ),
    );
    try validateArtifactEnrichmentsForTableIndexesJson(
        std.testing.allocator,
        "{\"enrichments\":[{\"name\":\"chunks\",\"kind\":\"chunk\",\"field\":\"text\",\"source_artifact_name\":\"units\",\"chunk_size\":512},{\"name\":\"units\",\"kind\":\"asset\",\"field\":\"url\"}]}",
    );
}

test "index metadata rejects artifact enrichment deletion with dependents" {
    const indexes_json = "{\"enrichments\":[{\"name\":\"units\",\"kind\":\"asset\",\"field\":\"url\"},{\"name\":\"chunks\",\"kind\":\"chunk\",\"field\":\"text\",\"source_artifact_name\":\"units\",\"chunk_size\":512}]}";
    const removed = (try removeEnrichmentFromTableIndexesJson(std.testing.allocator, indexes_json, "units")).?;
    defer std.testing.allocator.free(removed);
    try std.testing.expectError(
        error.InvalidEnrichmentConfig,
        validateArtifactEnrichmentsForTableIndexesJson(std.testing.allocator, removed),
    );
}

test "index encoders expose local shard runtime status" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "search_idx"),
        .kind = .full_text,
        .doc_count = 12,
        .term_count = 34,
        .backfill_active = true,
        .backfill_progress = 0.5,
        .replay_applied_sequence = 3,
        .replay_target_sequence = 5,
        .replay_catch_up_required = true,
        .catch_up_active = true,
        .catch_up_applied_sequence = 3,
        .catch_up_target_sequence = 5,
        .text_merge = .{
            .pending_segments = 3,
            .quarantined_segments = 2,
            .last_merge_error = "InvalidChunk",
            .deferred_for_pressure = 1,
        },
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 12,
            .index_count = 1,
            .indexes = indexes,
            .async_indexing = .{
                .apply_mutex = .{
                    .lock_calls = 11,
                    .contended_calls = 3,
                },
                .applied_sequence = .{
                    .flush_calls = 5,
                },
                .startup = .{
                    .active = true,
                    .phase = .opening_db,
                    .wal_retained_segments = 4,
                    .wal_retained_bytes = 99,
                    .wal_checkpoint_oldest_retained_segment = 2,
                    .wal_checkpoint_covered_through_segment = 3,
                    .wal_checkpoint_current_segment = 5,
                    .wal_checkpoint_lag_segments = 2,
                    .wal_replay_retained_segments = 1,
                    .wal_replay_retained_bytes = 44,
                    .wal_replay_current_segment = 6,
                    .lsm_open_stores = 2,
                    .lsm_open_recovered_temp_cleanup_ns = 77,
                    .lsm_open_wal_replay_ns = 123,
                    .lsm_open_loaded_runs = 6,
                    .lsm_open_recovered_temp_files_deleted = 4,
                    .lsm_open_recovered_temp_bytes_deleted = 2048,
                    .wal_replay_bytes = 456,
                    .wal_replay_truncated_tail_bytes = 7,
                },
                .dense_catch_up = .{
                    .active = true,
                    .current_sequence = 41,
                    .current_target_sequence = 77,
                    .current_scanned_entries = 1024,
                    .current_applied_entries = 768,
                    .progress_updates = 9,
                },
            },
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"search_idx\":{\"type\":\"full_text\"}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "search_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"rebuilding\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"total_indexed\":12") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_progress\":0.500") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_applied_sequence\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_target_sequence\":5") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_catch_up_required\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"catch_up_active\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"catch_up_applied_sequence\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"catch_up_target_sequence\":5") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"text_merge\":{") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"quarantined_segments\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"last_merge_error\":\"InvalidChunk\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"async_indexing\":{") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"lock_calls\":11") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"flush_calls\":5") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"startup\":{\"active\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"phase\":\"opening_db\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"wal_retained_segments\":4") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"wal_retained_bytes\":99") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"wal_checkpoint_oldest_retained_segment\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"wal_checkpoint_covered_through_segment\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"wal_checkpoint_current_segment\":5") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"wal_checkpoint_lag_segments\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"wal_replay_retained_segments\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"wal_replay_retained_bytes\":44") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"wal_replay_current_segment\":6") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"lsm_open_stores\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"lsm_open_recovered_temp_cleanup_ns\":77") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"lsm_open_wal_replay_ns\":123") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"lsm_open_loaded_runs\":6") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"lsm_open_recovered_temp_files_deleted\":4") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"lsm_open_recovered_temp_bytes_deleted\":2048") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"wal_replay_bytes\":456") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"wal_replay_truncated_tail_bytes\":7") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"active\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"current_sequence\":41") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"current_target_sequence\":77") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"current_scanned_entries\":1024") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"current_applied_entries\":768") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"progress_updates\":9") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"shard_status\":{\"7\":{") != null);
}

test "index encoders expose algebraic graph traversal health" {
    const alloc = std.testing.allocator;
    const indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1);
    defer alloc.free(indexes);
    indexes[0] = .{
        .name = try alloc.dupe(u8, "graph_idx"),
        .kind = .graph,
        .edge_count = 12,
        .node_count = 7,
        .doc_count = 7,
        .algebraic_graph_traversal_attempt_count = 3,
        .algebraic_graph_traversal_proven_count = 2,
        .algebraic_graph_traversal_rejected_count = 1,
        .algebraic_graph_traversal_fallback_count = 4,
        .algebraic_graph_traversal_result_node_count = 9,
    };
    defer alloc.free(indexes[0].name);

    const local_items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer alloc.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 7,
            .index_count = 1,
            .indexes = indexes,
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"graph_idx\":{\"type\":\"graph\",\"algebraic_planning\":{\"bounded_traversal\":{\"law\":\"provenance_semiring\"}}}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(alloc, &snapshot, "docs", "graph_idx", &local_status)).?;
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"total_edges\":12") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"algebraic_graph\":{\"traversal\":{\"attempted\":3,\"proven\":2,\"rejected\":1,\"fallback\":4,\"result_nodes\":9}}") != null);
}

test "index encoders expose graph artifact source materialization status" {
    const alloc = std.testing.allocator;
    const indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1);
    defer alloc.free(indexes);
    indexes[0] = .{
        .name = try alloc.dupe(u8, "relations_graph"),
        .kind = .graph,
        .edge_count = 4,
        .replay_applied_sequence = 3,
        .replay_target_sequence = 5,
        .replay_catch_up_required = true,
    };
    defer alloc.free(indexes[0].name);

    const local_items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer alloc.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 2,
            .index_count = 1,
            .indexes = indexes,
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"relations_graph\":{\"type\":\"graph\",\"source\":{\"kind\":\"artifact\",\"artifact\":\"relations_v1\",\"path\":\"$.relations[*]\",\"format\":\"extraction_relation\"}}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(alloc, &snapshot, "docs", "relations_graph", &local_status)).?;
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"source_artifact\":{\"name\":\"relations_v1\",\"path\":\"$.relations[*]\",\"format\":\"extraction_relation\",\"materialization_pending\":true}") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"shard_status\":{\"7\":{") != null);
}

test "index encoders expose compact algebraic public status" {
    const alloc = std.testing.allocator;
    const indexes = try alloc.alloc(db_mod.types.DBIndexStats, 1);
    defer alloc.free(indexes);
    indexes[0] = .{
        .name = try alloc.dupe(u8, "alg"),
        .kind = .algebraic,
        .algebraic_parse_error_count = 2,
        .algebraic_last_error_reason = try alloc.dupe(u8, "invalid_json"),
        .algebraic_schema_version = 42,
        .algebraic_capability_lifecycle_status = try alloc.dupe(u8, "stale"),
        .algebraic_planner_selected = 3,
        .algebraic_planner_fallback_count = 1,
        .algebraic_planner_last_decision = try alloc.dupe(u8, "fallback"),
        .algebraic_planner_last_fallback_reason = try alloc.dupe(u8, "schema_lifecycle_not_ready"),
        .algebraic_planner_last_estimated_scan_rows = 61,
        .algebraic_planner_last_estimated_result_buckets = 8,
        .algebraic_planner_lifecycle_ready = false,
        .algebraic_planner_lifecycle_blocking_reason = try alloc.dupe(u8, "capability_lifecycle_not_ready"),
        .algebraic_recommendation_count = 4,
        .algebraic_adaptive_progress_count = 2,
        .algebraic_adaptive_backfilling_count = 1,
        .algebraic_adaptive_ready_count = 1,
        .algebraic_adaptive_stale_count = 0,
        .algebraic_adaptive_dematerialize_recommended_count = 1,
        .algebraic_active_progress = .{
            .recommendation = try alloc.dupe(u8, "recommendation:v2"),
            .materialization_id = try alloc.dupe(u8, "adaptive:v2"),
            .lifecycle = try alloc.dupe(u8, "backfilling"),
            .target_sequence = 50,
            .applied_sequence = 25,
            .rows_processed = 30,
            .target_rows = 60,
        },
    };
    defer {
        alloc.free(indexes[0].name);
        alloc.free(indexes[0].algebraic_last_error_reason.?);
        alloc.free(indexes[0].algebraic_capability_lifecycle_status.?);
        alloc.free(indexes[0].algebraic_planner_last_decision.?);
        alloc.free(indexes[0].algebraic_planner_last_fallback_reason.?);
        alloc.free(indexes[0].algebraic_planner_lifecycle_blocking_reason.?);
        alloc.free(indexes[0].algebraic_active_progress.?.recommendation);
        alloc.free(indexes[0].algebraic_active_progress.?.materialization_id);
        alloc.free(indexes[0].algebraic_active_progress.?.lifecycle);
    }

    const local_items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer alloc.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .stats = .{
            .index_count = 1,
            .indexes = indexes,
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"alg\":{\"type\":\"algebraic\"}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(alloc, &snapshot, "docs", "alg", &local_status)).?;
    defer alloc.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"index_type\":\"algebraic\"") != null);
    // `index_type` is emitted once in the aggregate `status` and once per group
    // in `shard_status`, consistent with how every other index kind reports its
    // type in both views. This fixture has a single group, so it appears twice.
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, encoded, "\"index_type\""));
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"healthy\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"parse_error_count\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"schema_version\":42") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"capability_lifecycle_status\":\"stale\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"planner_selected\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"planner_fallback_count\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"planner_last_decision\":\"fallback\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"planner_last_fallback_reason\":\"schema_lifecycle_not_ready\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"planner_last_estimated_scan_rows\":61") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"planner_last_estimated_result_buckets\":8") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"planner_lifecycle_ready\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"planner_lifecycle_blocking_reason\":\"capability_lifecycle_not_ready\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"recommendation_count\":4") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"adaptive_progress_count\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"adaptive_backfilling_count\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"adaptive_ready_count\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"adaptive_cleanup_recommended_count\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"last_error_reason\":\"invalid_json\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"active_progress_lifecycle\":\"backfilling\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"active_progress_rows_processed\":30") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"active_progress_target_rows\":60") != null);
}

test "index status aggregation preserves most severe algebraic capability lifecycle" {
    var rebuild_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = "alg",
        .kind = .algebraic,
        .doc_count = 1,
        .algebraic_capability_lifecycle_status = "rebuild_required",
    }};
    var current_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = "alg",
        .kind = .algebraic,
        .doc_count = 1,
        .algebraic_capability_lifecycle_status = "current",
    }};
    var stale_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = "alg",
        .kind = .algebraic,
        .doc_count = 1,
        .algebraic_capability_lifecycle_status = "stale",
    }};
    const runtimes = [_]runtime_status.LocalTableRuntimeStatus{
        .{
            .group_id = 1,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
            .stats = .{ .index_count = 1, .indexes = rebuild_indexes[0..] },
        },
        .{
            .group_id = 2,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
            .stats = .{ .index_count = 1, .indexes = current_indexes[0..] },
        },
        .{
            .group_id = 3,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
            .stats = .{ .index_count = 1, .indexes = stale_indexes[0..] },
        },
    };

    const aggregate = aggregateIndexStatus(runtimes[0..], "alg", &.{}, 0) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("rebuild_required", aggregate.algebraic_capability_lifecycle_status.?);
}

test "index status aggregation reports selected algebraic progress summary shard" {
    const low_progress = [_]db_mod.types.AlgebraicProgressStatus{.{
        .recommendation = "recommendation:low-progress",
        .materialization_id = "adaptive:low-progress",
        .lifecycle = "backfilling",
        .target_sequence = 10,
        .applied_sequence = 2,
        .rows_processed = 4,
        .target_rows = 20,
    }};
    const high_progress = [_]db_mod.types.AlgebraicProgressStatus{.{
        .recommendation = "recommendation:high-progress",
        .materialization_id = "adaptive:high-progress",
        .lifecycle = "backfilling",
        .target_sequence = 50,
        .applied_sequence = 10,
        .rows_processed = 30,
        .target_rows = 60,
    }};
    var low_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = "alg",
        .kind = .algebraic,
        .doc_count = 1,
        .algebraic_active_progress = low_progress[0],
    }};
    var high_indexes = [_]db_mod.types.DBIndexStats{.{
        .name = "alg",
        .kind = .algebraic,
        .doc_count = 1,
        .algebraic_active_progress = high_progress[0],
    }};
    const runtimes = [_]runtime_status.LocalTableRuntimeStatus{
        .{
            .group_id = 1,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
            .stats = .{ .index_count = 1, .indexes = low_indexes[0..] },
        },
        .{
            .group_id = 2,
            .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
            .stats = .{ .index_count = 1, .indexes = high_indexes[0..] },
        },
    };

    const aggregate = aggregateIndexStatus(runtimes[0..], "alg", &.{}, 0) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("recommendation:high-progress", aggregate.algebraic_active_progress.?.recommendation);
}

test "index encoders aggregate replay debt across local shards" {
    const shard_a_indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(shard_a_indexes);
    shard_a_indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "search_idx"),
        .kind = .full_text,
        .doc_count = 4,
        .term_count = 10,
        .backfill_active = true,
        .backfill_progress = 0.4,
        .replay_applied_sequence = 2,
        .replay_target_sequence = 5,
        .replay_catch_up_required = true,
    };
    defer std.testing.allocator.free(shard_a_indexes[0].name);

    const shard_b_indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(shard_b_indexes);
    shard_b_indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "search_idx"),
        .kind = .full_text,
        .doc_count = 6,
        .term_count = 14,
        .replay_applied_sequence = 5,
        .replay_target_sequence = 5,
        .replay_catch_up_required = false,
    };
    defer std.testing.allocator.free(shard_b_indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 2);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 4,
            .index_count = 1,
            .indexes = shard_a_indexes,
        },
    };
    local_items[1] = .{
        .group_id = 8,
        .stats = .{
            .doc_count = 6,
            .index_count = 1,
            .indexes = shard_b_indexes,
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"search_idx\":{\"type\":\"full_text\"}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "search_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_applied_sequence\":7") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_target_sequence\":10") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_catch_up_required\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_progress\":0.400") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"shard_status\":{\"7\":{") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"8\":{") != null);
}

test "full text aggregate clears stale completed replay backfill flag" {
    var indexes = [_]db_mod.types.DBIndexStats{.{
        .name = try std.testing.allocator.dupe(u8, "full_text_index_v1"),
        .kind = .full_text,
        .doc_count = 1000,
        .term_count = 0,
        .backfill_active = true,
        .backfill_progress = 0.0,
        .replay_applied_sequence = 1,
        .replay_target_sequence = 1,
        .replay_catch_up_required = true,
        .catch_up_applied_sequence = 1,
        .catch_up_target_sequence = 1,
    }};
    defer std.testing.allocator.free(indexes[0].name);

    const runtimes = [_]runtime_status.LocalTableRuntimeStatus{.{
        .group_id = 7,
        .metadata = .{ .source = .cached_snapshot, .freshness = .stale },
        .stats = .{
            .doc_count = 1000,
            .index_count = 1,
            .indexes = indexes[0..],
        },
    }};

    const aggregate = aggregateIndexStatus(runtimes[0..], "full_text_index_v1", &.{7}, 0) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u64, 1), aggregate.stale_group_count);
    try std.testing.expectEqual(@as(u64, 1), aggregate.replay_applied_sequence);
    try std.testing.expectEqual(@as(u64, 1), aggregate.replay_target_sequence);
    try std.testing.expect(!aggregate.replay_catch_up_required);
    try std.testing.expect(!aggregate.backfill_active);
    try std.testing.expectEqual(@as(f64, 1.0), aggregate.backfill_progress);
}

test "index status keeps generic catch-up lag pending when replay sequence is equal" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "search_idx"),
        .kind = .full_text,
        .doc_count = 42,
        .term_count = 128,
        .replay_applied_sequence = 100,
        .replay_target_sequence = 100,
        .replay_catch_up_required = false,
        .catch_up_active = false,
        .catch_up_applied_sequence = 40,
        .catch_up_target_sequence = 100,
        .backfill_active = false,
        .backfill_progress = 1.0,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .stats = .{ .doc_count = 42, .index_count = 1, .indexes = indexes },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"search_idx\":{\"type\":\"full_text\"}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "search_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"rebuilding\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_applied_sequence\":40") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_target_sequence\":100") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_catch_up_required\":true") != null);
}

test "index encoders aggregate preserved synthetic shard counters" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
        .doc_count = 1_000_000,
        .node_count = 8_837,
        .root_node = 1,
        .replay_applied_sequence = 10_002,
        .replay_target_sequence = 10_002,
        .catch_up_applied_sequence = 10_002,
        .catch_up_target_sequence = 10_002,
        .backfill_progress = 1.0,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .metadata = .{
            .source = .synthetic_config,
            .freshness = .stale,
        },
        .stats = .{
            .doc_count = 1_000_000,
            .index_count = 1,
            .indexes = indexes,
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"dense_idx\":{\"type\":\"embeddings\",\"dimension\":1024,\"external\":true}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "dense_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"status\":{\"rebuilding\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"total_indexed\":1000000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"query_visible_doc_count\":1000000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"published_node_count\":8837") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"runtime_present\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"shard_status\":{\"7\":{") != null);
}

test "single embeddings index encoder fences stale runtime materialization" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 3,
        .node_count = 1,
        .root_node = 1,
        .backfill_active = true,
        .backfill_progress = 0.667,
        .replay_applied_sequence = 2,
        .replay_target_sequence = 3,
        .replay_catch_up_required = true,
        .catch_up_applied_sequence = 2,
        .catch_up_target_sequence = 3,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .metadata = .{
            .source = .synthetic_config,
            .freshness = .stale,
        },
        .stats = .{
            .doc_count = 3,
            .index_count = 1,
            .indexes = indexes,
            .enrichment = .{
                .enabled = true,
                .target_sequence = 3,
                .applied_sequence = 2,
                .retryable_error_count = 8,
            },
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "semantic_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"rebuilding\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_progress\":0.000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_state\":\"running\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"query_visible_doc_count\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"runtime_fresh\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"stale_groups\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"pending_sequence_count\":0") != null);
}

test "index encoders report missing and stale topology groups without probing databases" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "search_idx"),
        .kind = .full_text,
        .doc_count = 4,
        .term_count = 10,
        .replay_applied_sequence = 5,
        .replay_target_sequence = 5,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .metadata = .{ .freshness = .stale },
        .stats = .{
            .doc_count = 4,
            .index_count = 1,
            .indexes = indexes,
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"search_idx\":{\"type\":\"full_text\"}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
            .{ .table_id = 7, .group_id = 7, .start_key = "" },
            .{ .table_id = 7, .group_id = 8, .start_key = "m" },
        })[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "search_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"expected_groups\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"reported_groups\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"fresh_groups\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"stale_groups\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"missing_groups\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"unknown_remote_groups\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"rebuilding\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_catch_up_required\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"shard_status\":{\"7\":{") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"8\":{") != null);
}

test "single embeddings index encoder exposes replay and enrichment runtime state" {
    const config_json = "{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3,\"embedder\":{\"provider\":\"antfly\",\"model\":\"antflydb/clipclap\"},\"_coverage_incarnation\":42}";
    var parsed_config = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, config_json, .{});
    defer parsed_config.deinit();
    const config_hash = try expectedCoverageConfigHash(std.testing.allocator, "semantic_idx", parsed_config.value);

    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 1,
        .node_count = 1,
        .backfill_active = true,
        .backfill_progress = 0.2,
        .coverage_produced_count = 1,
        .coverage_generation = 42,
        .coverage_config_hash = config_hash,
        .coverage_identity_ready = true,
        .replay_applied_sequence = 1,
        .replay_target_sequence = 5,
        .replay_catch_up_required = true,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
        .stats = .{
            .doc_count = 1,
            .source_doc_count = 1,
            .index_count = 1,
            .indexes = indexes,
            .enrichment = .{
                .enabled = true,
                .target_sequence = 5,
                .applied_sequence = 1,
                .processed_requests = 1,
                .error_count = 2,
                .retryable_error_count = 2,
                .retrying = true,
                .skip_by_hash_count = 1,
            },
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3,\"embedder\":{\"provider\":\"antfly\",\"model\":\"antflydb/clipclap\"},\"_coverage_incarnation\":42}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "semantic_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":true") != null);
    // Coverage is complete even though replay is retrying. Keep those two
    // dimensions separate instead of presenting replay lag as missing rows.
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_progress\":1.000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_state\":\"retrying\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_applied_sequence\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_target_sequence\":5") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"enrichment_runtime\":{") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"target_sequence\":5") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"applied_sequence\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"pending_sequence_count\":4") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"retryable_error_count\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"retrying\":true") != null);
}

test "single embeddings index encoder synthesizes replay state from enrichment runtime" {
    const config_json = "{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3,\"embedder\":{\"provider\":\"antfly\",\"model\":\"antflydb/clipclap\"},\"_coverage_incarnation\":42}";
    var parsed_config = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, config_json, .{});
    defer parsed_config.deinit();
    const config_hash = try expectedCoverageConfigHash(std.testing.allocator, "semantic_idx", parsed_config.value);

    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 1,
        .node_count = 1,
        .coverage_produced_count = 1,
        .coverage_generation = 42,
        .coverage_config_hash = config_hash,
        .coverage_identity_ready = true,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
        .stats = .{
            .doc_count = 1,
            .source_doc_count = 1,
            .index_count = 1,
            .indexes = indexes,
            .enrichment = .{
                .enabled = true,
                .target_sequence = 5,
                .applied_sequence = 1,
                .processed_requests = 1,
                .error_count = 2,
                .retryable_error_count = 2,
                .retrying = true,
                .skip_by_hash_count = 1,
            },
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3,\"embedder\":{\"provider\":\"antfly\",\"model\":\"antflydb/clipclap\"},\"_coverage_incarnation\":42}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "semantic_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_progress\":0.200") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_state\":\"retrying\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_applied_sequence\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_target_sequence\":5") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_catch_up_required\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"target_sequence\":5") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"applied_sequence\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"pending_sequence_count\":4") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"retryable_error_count\":2") != null);
}

test "single embeddings index encoder scopes isolated enrichment failure to one index" {
    const alloc = std.testing.allocator;
    const visual_config_json = "{\"type\":\"embeddings\",\"field\":\"image\",\"dimension\":3,\"embedder\":{\"provider\":\"antfly\",\"model\":\"antflydb/clipclap\"},\"_coverage_incarnation\":42}";
    var parsed_visual_config = try std.json.parseFromSlice(std.json.Value, alloc, visual_config_json, .{});
    defer parsed_visual_config.deinit();
    const visual_config_hash = try expectedCoverageConfigHash(alloc, "visual_idx", parsed_visual_config.value);
    const semantic_config_json = "{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3,\"embedder\":{\"provider\":\"antfly\",\"model\":\"antflydb/clipclap\"},\"_coverage_incarnation\":42}";
    var parsed_semantic_config = try std.json.parseFromSlice(std.json.Value, alloc, semantic_config_json, .{});
    defer parsed_semantic_config.deinit();
    const semantic_config_hash = try expectedCoverageConfigHash(alloc, "semantic_idx", parsed_semantic_config.value);

    const indexes = try alloc.alloc(db_mod.types.DBIndexStats, 2);
    defer alloc.free(indexes);
    indexes[0] = .{
        .name = try alloc.dupe(u8, "visual_idx"),
        .kind = .dense_vector,
        .doc_count = 0,
        .node_count = 0,
        .coverage_generation = 42,
        .coverage_config_hash = visual_config_hash,
        .coverage_identity_ready = true,
        .enrichment_failed = true,
    };
    indexes[1] = .{
        .name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 1,
        .node_count = 1,
        .coverage_produced_count = 1,
        .coverage_generation = 42,
        .coverage_config_hash = semantic_config_hash,
        .coverage_identity_ready = true,
    };
    defer alloc.free(indexes[0].name);
    defer alloc.free(indexes[1].name);

    const local_items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer alloc.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
        .stats = .{
            .doc_count = 1,
            .source_doc_count = 1,
            .index_count = 2,
            .indexes = indexes,
            .enrichment = .{
                .enabled = true,
                .target_sequence = 1,
                .applied_sequence = 1,
                .processed_requests = 1,
                .error_count = 1,
                .retryable_error_count = 0,
                .worker_failed = false,
            },
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json =
            \\{"visual_idx":{"type":"embeddings","field":"image","dimension":3,"embedder":{"provider":"antfly","model":"antflydb/clipclap"},"_coverage_incarnation":42},"semantic_idx":{"type":"embeddings","field":"body","dimension":3,"embedder":{"provider":"antfly","model":"antflydb/clipclap"},"_coverage_incarnation":42}}
            ,
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const failed_encoded = (try encodeSingleIndex(alloc, &snapshot, "docs", "visual_idx", &local_status)).?;
    defer alloc.free(failed_encoded);
    try std.testing.expect(std.mem.indexOf(u8, failed_encoded, "\"backfill_state\":\"degraded\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, failed_encoded, "\"worker_failed\":false") != null);

    const healthy_encoded = (try encodeSingleIndex(alloc, &snapshot, "docs", "semantic_idx", &local_status)).?;
    defer alloc.free(healthy_encoded);
    try std.testing.expect(std.mem.indexOf(u8, healthy_encoded, "\"backfill_state\":\"ready\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, healthy_encoded, "\"backfill_state\":\"failed\"") == null);
}

test "single embeddings index encoder keeps published visibility separate from replay debt" {
    const config_json = "{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3,\"embedder\":{\"provider\":\"antfly\",\"model\":\"antflydb/clipclap\"},\"_coverage_incarnation\":42}";
    var parsed_config = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, config_json, .{});
    defer parsed_config.deinit();
    const config_hash = try expectedCoverageConfigHash(std.testing.allocator, "semantic_idx", parsed_config.value);

    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 3,
        .node_count = 1,
        .coverage_produced_count = 3,
        .coverage_generation = 42,
        .coverage_config_hash = config_hash,
        .coverage_identity_ready = true,
        .replay_applied_sequence = 0,
        .replay_target_sequence = 3,
        .replay_catch_up_required = true,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
        .stats = .{
            .doc_count = 3,
            .source_doc_count = 3,
            .index_count = 1,
            .indexes = indexes,
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3,\"embedder\":{\"provider\":\"antfly\",\"model\":\"antflydb/clipclap\"},\"_coverage_incarnation\":42}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "semantic_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"rebuilding\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_progress\":1.000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_state\":\"running\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"query_visible_doc_count\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_applied_sequence\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_target_sequence\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_catch_up_required\":true") != null);
}

test "single embeddings index encoder keeps backfill active while enrichment replay lags" {
    const config_json = "{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3,\"embedder\":{\"provider\":\"antfly\",\"model\":\"antflydb/clipclap\"},\"_coverage_incarnation\":42}";
    var parsed_config = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, config_json, .{});
    defer parsed_config.deinit();
    const config_hash = try expectedCoverageConfigHash(std.testing.allocator, "semantic_idx", parsed_config.value);

    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 3,
        .node_count = 1,
        .coverage_produced_count = 3,
        .coverage_generation = 42,
        .coverage_config_hash = config_hash,
        .coverage_identity_ready = true,
        .replay_applied_sequence = 3,
        .replay_target_sequence = 3,
        .replay_catch_up_required = false,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
        .stats = .{
            .doc_count = 3,
            .source_doc_count = 3,
            .index_count = 1,
            .indexes = indexes,
            .enrichment = .{
                .enabled = true,
                .target_sequence = 5,
                .applied_sequence = 3,
                .retrying = true,
            },
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3,\"embedder\":{\"provider\":\"antfly\",\"model\":\"antflydb/clipclap\"},\"_coverage_incarnation\":42}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "semantic_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"rebuilding\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_progress\":0.600") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_applied_sequence\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_target_sequence\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_catch_up_required\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_state\":\"retrying\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"retrying\":true") != null);
}

test "single embeddings index encoder keeps retrying coverage gaps catch-up coherent" {
    const config_json = "{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3,\"embedder\":{\"provider\":\"antfly\",\"model\":\"antflydb/clipclap\"},\"_coverage_incarnation\":42}";
    var parsed_config = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, config_json, .{});
    defer parsed_config.deinit();
    const config_hash = try expectedCoverageConfigHash(std.testing.allocator, "semantic_idx", parsed_config.value);

    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 1,
        .node_count = 1,
        .coverage_produced_count = 1,
        .coverage_generation = 42,
        .coverage_config_hash = config_hash,
        .coverage_identity_ready = true,
        .replay_applied_sequence = 34,
        .replay_target_sequence = 34,
        .replay_catch_up_required = false,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
        .stats = .{
            .source_doc_count = 3,
            .doc_count = 3,
            .index_count = 1,
            .indexes = indexes,
            .enrichment = .{
                .enabled = true,
                .target_sequence = 34,
                .applied_sequence = 34,
                .retrying = true,
                .retryable_error_count = 1,
            },
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"semantic_idx\":" ++ config_json ++ "}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "semantic_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_progress\":0.333") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_applied_sequence\":34") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_target_sequence\":34") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_catch_up_required\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_state\":\"retrying\"") != null);
}

test "managed embeddings skipped terminal sources complete backfill without fabricating replay debt" {
    const config_json = "{\"type\":\"embeddings\",\"field\":\"semantic_content\",\"dimension\":3,\"embedder\":{\"provider\":\"antfly\",\"model\":\"antflydb/clipclap\"},\"_coverage_incarnation\":42}";
    var parsed_config = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, config_json, .{});
    defer parsed_config.deinit();
    const config_hash = try expectedCoverageConfigHash(std.testing.allocator, "semantic_idx", parsed_config.value);

    var indexes = [_]db_mod.types.DBIndexStats{.{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .doc_count = 12,
        .node_count = 1,
        .root_node = 1,
        .coverage_produced_count = 12,
        .coverage_skipped_count = 4,
        .coverage_generation = 42,
        .coverage_config_hash = config_hash,
        .coverage_identity_ready = true,
        .replay_applied_sequence = 20,
        .replay_target_sequence = 20,
        .replay_catch_up_required = false,
        .catch_up_active = false,
        .catch_up_phase = .idle,
        .catch_up_applied_sequence = 20,
        .catch_up_target_sequence = 20,
    }};
    var items = [_]runtime_status.LocalTableRuntimeStatus{.{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
        .stats = .{
            .source_doc_count = 16,
            .doc_count = 16,
            .index_count = 1,
            .indexes = indexes[0..],
            .enrichment = .{
                .enabled = true,
                .target_sequence = 20,
                .applied_sequence = 20,
                .processed_requests = 16,
                .skipped_source_count = 4,
            },
        },
    }};
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = items[0..] };
    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"semantic_idx\":" ++ config_json ++ "}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "semantic_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"rebuilding\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_progress\":1.000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_state\":\"degraded\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_applied_sequence\":20") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_target_sequence\":20") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_catch_up_required\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"policy\":\"strict\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"source_total\":16,\"produced\":12,\"skipped\":4") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"settled\":16,\"uncovered\":4,\"pending\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"complete\":false") != null);
}

test "external embeddings index readiness does not require table doc coverage" {
    const config_json = "{\"type\":\"embeddings\",\"dimension\":1536,\"external\":true,\"_coverage_incarnation\":42}";
    var parsed_config = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, config_json, .{});
    defer parsed_config.deinit();
    const config_hash = try expectedCoverageConfigHash(std.testing.allocator, "vec", parsed_config.value);
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "vec"),
        .kind = .dense_vector,
        .doc_count = 50_000,
        .node_count = 24,
        .coverage_produced_count = 50_000,
        .coverage_generation = 42,
        .coverage_config_hash = config_hash,
        .coverage_identity_ready = true,
        .replay_applied_sequence = 502,
        .replay_target_sequence = 502,
        .replay_catch_up_required = false,
        .backfill_active = true,
        .backfill_progress = 1.0,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
        .stats = .{
            .source_doc_count = 50_001,
            .doc_count = 50_001,
            .index_count = 1,
            .indexes = indexes,
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"vec\":" ++ config_json ++ "}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "vec", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"rebuilding\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_state\":\"ready\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"total_indexed\":50000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"query_visible_doc_count\":50000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"published_doc_count\":50000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"coverage\":{\"policy\":\"external\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"pending\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"complete\":false") != null);
}

test "embeddings index status reports dense catch-up phase separately from published visibility" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
        .doc_count = 25_000,
        .node_count = 128,
        .root_node = 9,
        .replay_applied_sequence = 700,
        .replay_target_sequence = 701,
        .replay_catch_up_required = true,
        .catch_up_active = true,
        .catch_up_phase = .idle,
        .catch_up_applied_sequence = 700,
        .catch_up_target_sequence = 701,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 25_000,
            .index_count = 1,
            .indexes = indexes,
            .async_indexing = .{
                .dense_catch_up = .{
                    .active = true,
                    .phase = .replay,
                    .current_sequence = 700,
                    .current_target_sequence = 701,
                },
            },
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"dense_idx\":{\"type\":\"embeddings\",\"dimension\":384,\"external\":true}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "dense_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"doc_count\":25000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"query_visible_doc_count\":25000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"published_doc_count\":25000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"dense_replay_applied_sequence\":700") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"dense_replay_target_sequence\":701") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"dense_publish_pending\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_catch_up_required\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"catch_up_phase\":\"replay\"") != null);
}

test "embeddings index status ignores inactive stale catch-up progress once dense coverage is visible" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
        .doc_count = 217_500,
        .node_count = 3_300,
        .root_node = 1,
        .replay_applied_sequence = 325,
        .replay_target_sequence = 325,
        .replay_catch_up_required = false,
        .catch_up_active = false,
        .catch_up_applied_sequence = 77,
        .catch_up_target_sequence = 325,
        .backfill_active = false,
        .backfill_progress = 1.0,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
        .stats = .{
            .doc_count = 217_500,
            .index_count = 1,
            .indexes = indexes,
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"dense_idx\":{\"type\":\"embeddings\",\"dimension\":512,\"external\":true}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "dense_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"rebuilding\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"dense_publish_pending\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_applied_sequence\":325") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_target_sequence\":325") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_catch_up_required\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"catch_up_applied_sequence\":325") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"catch_up_phase\":\"idle\"") != null);
}

test "managed embeddings readiness ignores inactive stale catch-up after rate-limit recovery" {
    const config_json = "{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3,\"embedder\":{\"provider\":\"antfly\",\"model\":\"antflydb/clipclap\"},\"_coverage_incarnation\":42}";
    var parsed_config = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, config_json, .{});
    defer parsed_config.deinit();
    const config_hash = try expectedCoverageConfigHash(std.testing.allocator, "semantic_idx", parsed_config.value);
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 3,
        .node_count = 1,
        .root_node = 1,
        .coverage_produced_count = 3,
        .coverage_generation = 42,
        .coverage_config_hash = config_hash,
        .coverage_identity_ready = true,
        .replay_applied_sequence = 4,
        .replay_target_sequence = 8,
        .replay_catch_up_required = true,
        .catch_up_active = false,
        .catch_up_phase = .idle,
        .catch_up_applied_sequence = 4,
        .catch_up_target_sequence = 8,
        .backfill_active = true,
        .backfill_progress = 0.5,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
        .stats = .{
            .source_doc_count = 3,
            .doc_count = 3,
            .index_count = 1,
            .indexes = indexes,
            .enrichment = .{
                .enabled = true,
                .target_sequence = 6,
                .applied_sequence = 6,
                .retryable_error_count = 10,
            },
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"semantic_idx\":" ++ config_json ++ "}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "semantic_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"rebuilding\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_progress\":1.000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_state\":\"ready\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_applied_sequence\":8") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_target_sequence\":8") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_catch_up_required\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"catch_up_active\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"catch_up_applied_sequence\":8") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"catch_up_phase\":\"idle\"") != null);
}

test "partial coverage embeddings readiness counts skipped source units" {
    const config_json = "{\"type\":\"embeddings\",\"coverage_policy\":\"partial\",\"template\":\"{{#if image_url}}{{remoteMedia url=image_url}}{{/if}}\",\"dimension\":512,\"embedder\":{\"provider\":\"antfly\",\"model\":\"antflydb/clipclap\"},\"_coverage_incarnation\":42}";
    var parsed_config = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, config_json, .{});
    defer parsed_config.deinit();
    const config_hash = try expectedCoverageConfigHash(std.testing.allocator, "visual_idx", parsed_config.value);
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "visual_idx"),
        .kind = .dense_vector,
        .doc_count = 1,
        .node_count = 1,
        .root_node = 1,
        .coverage_produced_count = 1,
        .coverage_skipped_count = 1,
        .coverage_generation = 42,
        .coverage_config_hash = config_hash,
        .coverage_identity_ready = true,
        .replay_applied_sequence = 2,
        .replay_target_sequence = 2,
        .replay_catch_up_required = false,
        .backfill_active = false,
        .backfill_progress = 1.0,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
        .stats = .{
            .source_doc_count = 2,
            .doc_count = 2,
            .index_count = 1,
            .indexes = indexes,
            .enrichment = .{
                .enabled = true,
                .target_sequence = 2,
                .applied_sequence = 2,
                .skipped_source_count = 1,
            },
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"visual_idx\":" ++ config_json ++ "}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "visual_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"rebuilding\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_catch_up_required\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"coverage\":{\"policy\":\"partial\",\"observation_complete\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"observation_incomplete_reasons\":[]") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"config_fingerprint\":\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"summary_ready\":true,\"config_mismatch_group_count\":0,\"source_total\":2,\"produced\":1,\"skipped\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"pending\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"complete\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"skipped_source_count\":1") != null);
}

test "partial coverage embeddings readiness does not mask pending enrichment" {
    const config_json = "{\"type\":\"embeddings\",\"coverage_policy\":\"partial\",\"template\":\"{{#if image_url}}{{remoteMedia url=image_url}}{{/if}}\",\"dimension\":512,\"embedder\":{\"provider\":\"antfly\",\"model\":\"antflydb/clipclap\"},\"_coverage_incarnation\":42}";
    var parsed_config = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, config_json, .{});
    defer parsed_config.deinit();
    const config_hash = try expectedCoverageConfigHash(std.testing.allocator, "visual_idx", parsed_config.value);
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "visual_idx"),
        .kind = .dense_vector,
        .doc_count = 1,
        .node_count = 1,
        .root_node = 1,
        .coverage_produced_count = 1,
        .coverage_skipped_count = 1,
        .coverage_generation = 42,
        .coverage_config_hash = config_hash,
        .coverage_identity_ready = true,
        .replay_applied_sequence = 1,
        .replay_target_sequence = 3,
        .replay_catch_up_required = true,
        .backfill_active = true,
        .backfill_progress = 0.333,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
        .stats = .{
            .source_doc_count = 2,
            .doc_count = 2,
            .index_count = 1,
            .indexes = indexes,
            .enrichment = .{
                .enabled = true,
                .target_sequence = 3,
                .applied_sequence = 1,
                .skipped_source_count = 1,
            },
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"visual_idx\":" ++ config_json ++ "}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "visual_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"rebuilding\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_state\":\"running\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_catch_up_required\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"coverage\":{\"policy\":\"partial\",\"observation_complete\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"summary_ready\":true,\"config_mismatch_group_count\":0,\"source_total\":2,\"produced\":1,\"skipped\":1") != null);
}

test "managed embeddings readiness prefers replay completion once docs are indexed" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 1,
        .node_count = 1,
        .replay_applied_sequence = 2,
        .replay_target_sequence = 2,
        .replay_catch_up_required = false,
        .backfill_active = true,
        .backfill_progress = 0.5,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 2,
            .index_count = 1,
            .indexes = indexes,
            .enrichment = .{
                .enabled = true,
                .processed_requests = 2,
            },
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "semantic_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"rebuilding\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_progress\":1.000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_state\":\"ready\"") != null);
}

test "managed embeddings readiness does not require table doc count once replay is complete" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 1,
        .node_count = 1,
        .replay_applied_sequence = 2,
        .replay_target_sequence = 2,
        .replay_catch_up_required = false,
        .backfill_active = true,
        .backfill_progress = 0.5,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .stats = .{
            .doc_count = 0,
            .index_count = 1,
            .indexes = indexes,
            .enrichment = .{
                .enabled = true,
            },
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "semantic_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"rebuilding\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_progress\":1.000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_state\":\"ready\"") != null);
}

test "embeddings index replay completion without artifact visibility is not ready" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "vec"),
        .kind = .dense_vector,
        .doc_count = 0,
        .node_count = 0,
        .root_node = 0,
        .replay_applied_sequence = 4000,
        .replay_target_sequence = 4000,
        .replay_catch_up_required = false,
        .backfill_active = false,
        .backfill_progress = 1.0,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .metadata = .{
            .source = .background_refresh,
            .freshness = .fresh,
        },
        .stats = .{
            .doc_count = 12,
            .index_count = 1,
            .indexes = indexes,
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"vec\":{\"type\":\"embeddings\",\"dimension\":512,\"external\":true}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "vec", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"rebuilding\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_state\":\"running\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"dense_publish_pending\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_applied_sequence\":4000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_target_sequence\":4000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_catch_up_required\":false") != null);
}

test "empty embeddings index status is ready without dense artifact visibility" {
    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 0,
        .node_count = 0,
        .root_node = 0,
        .replay_applied_sequence = 1,
        .replay_target_sequence = 1,
        .replay_catch_up_required = false,
        .backfill_active = true,
        .backfill_progress = 0.0,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .metadata = .{
            .source = .background_refresh,
            .freshness = .fresh,
        },
        .stats = .{
            .doc_count = 0,
            .index_count = 1,
            .indexes = indexes,
            .enrichment = .{
                .enabled = true,
                .applied_sequence = 1,
                .target_sequence = 1,
            },
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "semantic_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"rebuilding\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_state\":\"ready\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"dense_publish_pending\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_applied_sequence\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_target_sequence\":1") != null);
}

test "single embeddings index encoder keeps partial backfill active while indexed docs lag table docs" {
    const config_json = "{\"type\":\"embeddings\",\"coverage_policy\":\"partial\",\"field\":\"body\",\"dimension\":3,\"embedder\":{\"provider\":\"antfly\",\"model\":\"antflydb/clipclap\"},\"_coverage_incarnation\":42}";
    var parsed_config = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, config_json, .{});
    defer parsed_config.deinit();
    const config_hash = try expectedCoverageConfigHash(std.testing.allocator, "semantic_idx", parsed_config.value);

    const indexes = try std.testing.allocator.alloc(db_mod.types.DBIndexStats, 1);
    defer std.testing.allocator.free(indexes);
    indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .doc_count = 0,
        .node_count = 1,
        .coverage_generation = 42,
        .coverage_config_hash = config_hash,
        .coverage_identity_ready = true,
        .replay_applied_sequence = 1,
        .replay_target_sequence = 1,
    };
    defer std.testing.allocator.free(indexes[0].name);

    const local_items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    defer std.testing.allocator.free(local_items);
    local_items[0] = .{
        .group_id = 7,
        .metadata = .{ .source = .live_writer_publish, .freshness = .fresh },
        .stats = .{
            .doc_count = 3,
            .source_doc_count = 3,
            .index_count = 1,
            .indexes = indexes,
            .enrichment = .{
                .enabled = true,
            },
        },
    };
    var local_status = runtime_status.LocalTableRuntimeStatuses{ .items = local_items };

    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"coverage_policy\":\"partial\",\"field\":\"body\",\"dimension\":3,\"embedder\":{\"provider\":\"antfly\",\"model\":\"antflydb/clipclap\"},\"_coverage_incarnation\":42}}",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleIndex(std.testing.allocator, &snapshot, "docs", "semantic_idx", &local_status)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_active\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_progress\":0.000") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"backfill_state\":\"running\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_applied_sequence\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_target_sequence\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replay_catch_up_required\":false") != null);
}
