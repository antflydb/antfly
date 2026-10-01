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

//! One local catalog operation per retry of an already committed admission.
//! This is never an autonomous reconciler of an obsolete catalog receipt.
const std = @import("std");
const inventory = @import("artifact_inventory.zig");
const indexes = @import("catalog/index_manager.zig");
const enrichments = @import("catalog/enrichment_catalog.zig");
const resolvers = @import("catalog/resolver_catalog.zig");
const types = @import("types.zig");

pub fn validateDesired(alloc: std.mem.Allocator, command: inventory.Command) !void {
    try command.validate();
    const desired = if (command.catalogs.indexes.len != 0) try indexes.deserializeCatalog(alloc, command.catalogs.indexes) else try alloc.alloc(types.IndexConfig, 0);
    defer types.freeIndexConfigs(alloc, desired);
    const producers = try desiredProducers(alloc, command.catalogs);
    defer freeProducers(alloc, producers);
    for (producers) |config| if (command.binding.effect_protocol != 15 or !indexes.onlineBaseEmbeddingProducer(config)) return error.OnlineMergeArtifactTailsUnsupported;
    for (desired) |config| {
        if (config.kind == .full_text) continue;
        if (@import("online_vector_artifacts.zig").isEnabled() and try indexes.onlineDirectVectorConfig(alloc, config)) continue;
        if (command.binding.effect_protocol == 15 and try indexes.onlineBaseGeneratedVectorConfig(alloc, config, producers)) continue;
        return error.OnlineMergeArtifactTailsUnsupported;
    }
    if (command.catalogs.resolvers.len != 0) {
        const configs = try resolvers.deserializeCatalog(alloc, command.catalogs.resolvers);
        defer {
            for (configs) |*config| config.deinit(alloc);
            alloc.free(configs);
        }
        if (configs.len != 0) return error.OnlineMergeArtifactTailsUnsupported;
    }
}

pub fn hasDirectVectors(alloc: std.mem.Allocator, catalogs: inventory.Catalogs) !bool {
    if (catalogs.indexes.len == 0) return false;
    const configs = try indexes.deserializeCatalog(alloc, catalogs.indexes);
    defer types.freeIndexConfigs(alloc, configs);
    for (configs) |config| if (config.kind == .dense_vector or config.kind == .sparse_vector) {
        if (!try indexes.onlineDirectVectorConfig(alloc, config)) return error.OnlineMergeArtifactTailsUnsupported;
        return true;
    };
    return false;
}

pub fn step(db: anytype, command: inventory.Command, context: @import("artifact_reconcile_intent.zig").Context) !bool {
    const alloc = db.alloc;
    try validateDesired(alloc, command);
    const desired = if (command.catalogs.indexes.len != 0) try indexes.deserializeCatalog(alloc, command.catalogs.indexes) else try alloc.alloc(types.IndexConfig, 0);
    defer types.freeIndexConfigs(alloc, desired);
    const current = try db.listIndexes(alloc);
    defer types.freeIndexConfigs(alloc, current);
    if (try db.core.index_manager.firstFailedIndexLoadName(alloc)) |name| {
        defer alloc.free(name);
        const needed = for (current) |actual| {
            if (!std.mem.eql(u8, actual.name, name)) continue;
            const exact = for (desired) |config| {
                if (equivalent(actual, config)) break true;
            } else false;
            break exact;
        } else false;
        if (needed) _ = try db.retryQuarantinedIndexLoad(name, true) else _ = try db.reconcileArtifactDeleteIndex(name, context);
        return false;
    }
    // Remove dependent indexes first, then their producers and resolvers.
    for (current) |config| {
        const matches = for (desired) |candidate| {
            if (equivalent(config, candidate)) break true;
        } else false;
        if (!matches) {
            _ = try db.reconcileArtifactDeleteIndex(config.name, context);
            return false;
        }
    }
    if (try db.advanceGeneratedArtifactCleanupPage(null) != .idle) return false;
    const current_resolvers = try db.listResolvers(alloc);
    defer {
        for (current_resolvers) |*config| config.deinit(alloc);
        if (current_resolvers.len != 0) alloc.free(current_resolvers);
    }
    if (current_resolvers.len != 0) {
        _ = try db.reconcileArtifactDeleteResolver(current_resolvers[0].name, context);
        return false;
    }
    const current_enrichments = try db.listEnrichments(alloc);
    defer types.freeEnrichmentConfigs(alloc, current_enrichments);
    const producers = try desiredProducers(alloc, command.catalogs);
    defer freeProducers(alloc, producers);
    for (current_enrichments) |config| {
        const wanted = for (producers) |producer| {
            if (std.mem.eql(u8, producer.name, config.name)) break true;
        } else false;
        if (!wanted) {
            _ = db.reconcileArtifactDeleteEnrichment(config.kind, config.name, context) catch |err| switch (err) {
                error.EnrichmentInUse => return error.ArtifactCatalogDrift,
                else => return err,
            };
            return false;
        }
    }
    for (producers) |producer| {
        var public = try indexes.enrichmentToPublic(alloc, producer);
        defer public.deinit(alloc);
        const wanted_bytes = try std.json.Stringify.valueAlloc(alloc, public, .{});
        defer alloc.free(wanted_bytes);
        const present = for (current_enrichments) |existing| {
            if (!std.mem.eql(u8, existing.name, public.name)) continue;
            const actual_bytes = try std.json.Stringify.valueAlloc(alloc, existing, .{});
            defer alloc.free(actual_bytes);
            break std.mem.eql(u8, wanted_bytes, actual_bytes);
        } else false;
        if (!present) {
            try db.reconcileArtifactAddEnrichment(public, context);
            return false;
        }
    }
    for (desired) |config| {
        const present = for (current) |existing| {
            if (std.mem.eql(u8, existing.name, config.name)) break true;
        } else false;
        if (!present) {
            _ = try db.reconcileArtifactAddIndex(config, context);
            return false;
        }
    }
    try db.alignOrderedArtifactCatalog(command, desired, context);
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    return db.artifactMaterializationsReady(&read, command.catalogs);
}

fn desiredProducers(alloc: std.mem.Allocator, catalogs: inventory.Catalogs) ![]enrichments.EnrichmentConfig {
    return if (catalogs.enrichments.len == 0) try alloc.alloc(enrichments.EnrichmentConfig, 0) else try enrichments.deserializeCatalog(alloc, catalogs.enrichments);
}

fn freeProducers(alloc: std.mem.Allocator, configs: []enrichments.EnrichmentConfig) void {
    for (configs) |*config| config.deinit(alloc);
    alloc.free(configs);
}

fn equivalent(a: types.IndexConfig, b: types.IndexConfig) bool {
    const generation = @import("../internal_keys.zig").derivedCoverageGenerationForConfig;
    return std.mem.eql(u8, a.name, b.name) and a.kind == b.kind and generation(a.coverage_generation, a.config_json) == generation(b.coverage_generation, b.config_json) and std.mem.eql(u8, a.config_json, b.config_json);
}

test "ordered artifact inventory generated producer definitions require protocol fifteen and base input" {
    const alloc = std.testing.allocator;
    const catalogs: inventory.Catalogs = .{ .enrichments = "[{\"name\":\"model\",\"kind\":\"embedding\",\"source_field\":\"body\",\"expected_dims\":2}]" };
    var command: inventory.Command = .{ .namespace = @splat(1), .catalogs = catalogs, .binding = .{ .epoch = 1, .digest = catalogs.digest(), .semantic_digest = try catalogs.semanticDigest(alloc) } };
    try std.testing.expectError(error.OnlineMergeArtifactTailsUnsupported, validateDesired(alloc, command));
    command.binding.effect_protocol = 15;
    try validateDesired(alloc, command);
    command.catalogs.enrichments = "[{\"name\":\"model\",\"kind\":\"embedding\",\"source_field\":\"body\",\"source_artifact_name\":\"chunks\",\"expected_dims\":2}]";
    command.binding.digest = command.catalogs.digest();
    command.binding.semantic_digest = try command.catalogs.semanticDigest(alloc);
    try std.testing.expectError(error.OnlineMergeArtifactTailsUnsupported, validateDesired(alloc, command));
}
