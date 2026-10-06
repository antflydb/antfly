// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Queryability follows current complete source/credential/store proof. Uploaded
//! text/vector declarations stay pending until their native consumers exist.
const std = @import("std");
const local = @import("antfly_local_sources");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const recipes = @import("lake_index_native_aggregates.zig");
const A = std.mem.Allocator;

pub fn names(a: A, server: *@import("http_server.zig").ApiHttpServer, table: local.common_topology_records.TableRecord, request: local.api_operation.RequestContext) ![]const []const u8 {
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const sa = scratch.allocator();
    var state = try local.metadata_lake_index_catalog.parse(sa, table.lake_index_catalog_json);
    defer state.deinit();
    const publication = state.value.published orelse return &.{};
    const eligible = for (publication.declarations) |declaration| {
        if (declaration.artifact.kind == .algebraic_segment and artifacts.supportsMetadataVersion(declaration.artifact.metadata_version)) break true;
    } else false;
    if (!eligible and publication.directory == null) return &.{};
    const normalized = try request.platformDeadline();
    const context: local.serverless_query_lake_read_context.Context = .{ .io = server.embedding_provider_runtime.io, .deadline_ns = normalized.deadline_ns, .cancellation = local.storage_object_storage.CancellationToken.fromCallback(normalized.cancellation.ptr, normalized.cancellation.is_cancelled_fn) };
    var sql_table = try server.sql_schema_cache.resolve(server.embedding_provider_runtime.io, sa, table.schema_json, table.table_id, table.name);
    sql_table.external_indexes = .{ .catalog_json = table.lake_index_catalog_json, .indexes_json = table.indexes_json, .desired = local.metadata_lake_index_catalog.desiredFingerprint(table) };
    const options: @import("../serverless/configured_object_store_support.zig").BindingObjectStoreOpenOptions = .{ .node_config = server.cfg.node_config, .secret_store = server.cfg.secret_store };
    try server.prepareLakeCache();
    var source = try local.serverless_query_lake_serving.ServingSource.openCached(a, .{ .storage_mode = .relational, .external_base_source = sql_table.external_base_source }, options.lakeOptions(), context, &server.lake_read_cache);
    defer source.deinit();
    var store = try @import("lake_index_store.zig").Store.openNative(a, server.cfg.node_config, server.cfg.secret_store, true, server.cfg.deployment_mode, server.cfg.native_lake_artifact_base_dir);
    defer store.deinit();
    var selected = (try @import("lake_index_selection.zig").selectCached(sa, sql_table, &source, &store, context, .automatic, &server.lake_read_cache)) orelse return &.{};
    defer selected.deinit();
    const definitions = try std.json.parseFromSliceLeaky(std.json.Value, sa, table.indexes_json, .{});
    if (definitions != .object) return error.InvalidTableIndexMetadata;
    var result: std.ArrayList([]const u8) = .empty;
    errdefer {
        for (result.items) |name| a.free(name);
        result.deinit(a);
    }
    var iterator = definitions.object.iterator();
    while (iterator.next()) |entry| {
        const config = entry.value_ptr.*;
        if (config != .object) continue;
        const kind = config.object.get("type") orelse continue;
        if (kind != .string or !std.mem.eql(u8, kind.string, "algebraic")) continue;
        const mats = config.object.get("materializations") orelse continue;
        if (mats != .array or mats.array.items.len == 0) continue;
        const ready = for (mats.array.items) |mat| {
            const recipe = (try recipes.recipeFor(sa, sql_table, config, mat)) orelse break false;
            const name = mat.object.get("name") orelse break false;
            if (name != .string) break false;
            const identity = try recipes.recipeIdentity(sa, recipe);
            const declaration = for (selected.publication().declarations) |decl| {
                if (decl.artifact.kind == .algebraic_segment and artifacts.supportsMetadataVersion(decl.artifact.metadata_version) and std.mem.eql(u8, decl.binding.index_config_hash, identity) and try @import("lake_index_names.zig").matches(sa, decl.name, entry.key_ptr.*, name.string, decl.artifact.metadata_version)) break decl;
            } else break false;
            // A bounded root check proves the reader's contract, not merely a
            // successful upload. Descendant blocks are verified as SQL drains.
            const reader = artifacts.Reader.openWithCache(a, store.artifactStore(), declaration.artifact, recipe, normalized.cancellation, .{ .cache = &server.lake_read_cache, .scope = store.identity, .context = context }) catch |err| {
                try context.ensureActive();
                if (err == error.OutOfMemory) return err;
                break false;
            };
            const cursor = reader.cursor();
            cursor.close(cursor.ptr);
        } else true;
        if (ready) {
            const name = try a.dupe(u8, entry.key_ptr.*);
            result.append(a, name) catch |err| {
                a.free(name);
                return err;
            };
        }
    }
    try context.ensureActive();
    return result.toOwnedSlice(a);
}
