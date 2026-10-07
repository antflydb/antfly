// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Queryability follows current complete source/credential/store proof. Uploaded
//! Text and ordered-row roots require native format and current source proof;
//! vector declarations stay pending until their native consumers exist.
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
        if ((declaration.artifact.kind == .algebraic_segment and artifacts.supportsMetadataVersion(declaration.artifact.metadata_version)) or declaration.artifact.kind == .ordered_row_index) break true;
    } else false;
    if (!eligible and publication.directory == null) return &.{};
    const normalized = try request.platformDeadline();
    var context: local.serverless_query_lake_read_context.Context = .{ .io = server.embedding_provider_runtime.io, .deadline_ns = normalized.deadline_ns, .cancellation = local.storage_object_storage.CancellationToken.fromCallback(normalized.cancellation.ptr, normalized.cancellation.is_cancelled_fn) };
    var sql_table = try server.sql_schema_cache.resolve(server.embedding_provider_runtime.io, sa, table.schema_json, table.table_id, table.name);
    sql_table.external_indexes = .{ .catalog_json = table.lake_index_catalog_json, .indexes_json = table.indexes_json, .desired = local.metadata_lake_index_catalog.desiredFingerprint(table) };
    const options: @import("../serverless/configured_object_store_support.zig").BindingObjectStoreOpenOptions = .{ .node_config = server.cfg.node_config, .secret_store = server.cfg.secret_store };
    try server.prepareLakeCache();
    var source = try local.serverless_query_lake_serving.ServingSource.openCached(a, .{ .storage_mode = .relational, .external_base_source = sql_table.external_base_source }, options.lakeOptions(), context, &server.lake_read_cache);
    defer source.deinit();
    var store = try @import("lake_index_store.zig").Store.openNative(a, server.cfg.node_config, server.cfg.secret_store, true, server.cfg.deployment_mode, server.cfg.native_lake_artifact_base_dir);
    defer store.deinit();
    var reader_lease: ?*@import("lake_index_reader_lease.zig").Handle = null;
    defer if (reader_lease) |lease| lease.deinit();
    if (server.source.lakeIndexLifecycleAuthority(request)) |authority| {
        reader_lease = server.lake_reader_leases.acquire(server.embedding_provider_runtime.io, authority, table.table_id, publication.generation, context) catch |err| {
            try context.ensureActive();
            if (err == error.OutOfMemory or err == error.MetadataMutationOutcomeUnknown) return err;
            return &.{};
        };
        context = reader_lease.?.readContext();
    }
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
        if (kind != .string) continue;
        if (std.mem.eql(u8, kind.string, "embeddings") and (if (config.object.get("sparse")) |sparse| sparse == .bool and sparse.bool else false)) {
            const native = @import("lake_index_native_sparse.zig");
            const declaration = for (selected.publication().declarations) |decl| {
                if (decl.artifact.kind == .sparse_segment and decl.artifact.metadata_version == native.metadata_version and std.mem.eql(u8, decl.name, entry.key_ptr.*)) break decl;
            } else continue;
            const root = native.loadRoot(sa, store.artifactStore(), declaration.artifact, normalized.cancellation, .{ .cache = &server.lake_read_cache, .scope = store.identity, .context = context }) catch |err| {
                try context.ensureActive();
                if (err == error.OutOfMemory) return err;
                continue;
            };
            const domain = @import("lake_index_publication.zig").uploadDomainWithNamespace(table.table_id, store.identity, selected.publication().namespace);
            if (!std.mem.eql(u8, &root.generation.domain, &domain) or !@import("../serverless/build/lake_rebuild.zig").bindingsEqual(root.binding, declaration.binding)) continue;
            const name = try a.dupe(u8, entry.key_ptr.*);
            result.append(a, name) catch |err| {
                a.free(name);
                return err;
            };
            continue;
        }
        if (std.mem.eql(u8, kind.string, "embeddings")) {
            const native = @import("lake_index_native_dense.zig");
            const declaration = for (selected.publication().declarations) |decl| {
                if (decl.artifact.kind == .vector_segment and decl.artifact.metadata_version == native.metadata_version and std.mem.eql(u8, decl.name, entry.key_ptr.*)) break decl;
            } else continue;
            const root = native.loadRoot(sa, store.artifactStore(), declaration.artifact, normalized.cancellation, .{ .cache = &server.lake_read_cache, .scope = store.identity, .context = context }) catch |err| {
                try context.ensureActive();
                if (err == error.OutOfMemory) return err;
                continue;
            };
            const domain = @import("lake_index_publication.zig").uploadDomainWithNamespace(table.table_id, store.identity, selected.publication().namespace);
            if (!std.mem.eql(u8, &root.generation.domain, &domain) or !@import("../serverless/build/lake_rebuild.zig").bindingsEqual(root.binding, declaration.binding)) continue;
            const name = try a.dupe(u8, entry.key_ptr.*);
            result.append(a, name) catch |err| {
                a.free(name);
                return err;
            };
            continue;
        }
        if (std.mem.eql(u8, kind.string, "full_text")) {
            const native_text = @import("lake_index_native_text.zig");
            const declaration = for (selected.publication().declarations) |decl| {
                if (decl.artifact.kind == .text_segment and decl.artifact.metadata_version == native_text.metadata_version and std.mem.eql(u8, decl.name, entry.key_ptr.*)) break decl;
            } else continue;
            const root = native_text.loadRoot(sa, store.artifactStore(), declaration.artifact, normalized.cancellation, .{ .cache = &server.lake_read_cache, .scope = store.identity, .context = context }) catch |err| {
                try context.ensureActive();
                if (err == error.OutOfMemory) return err;
                continue;
            };
            const expected_domain = @import("lake_index_publication.zig").uploadDomainWithNamespace(table.table_id, store.identity, selected.publication().namespace);
            if (!std.mem.eql(u8, &root.domain, &expected_domain) or !@import("../serverless/build/lake_rebuild.zig").bindingsEqual(root.binding, declaration.binding)) continue;
            const name = try a.dupe(u8, entry.key_ptr.*);
            result.append(a, name) catch |err| {
                a.free(name);
                return err;
            };
            continue;
        }
        if (!std.mem.eql(u8, kind.string, "algebraic")) continue;
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
    var schema = try local.schema_mod.parseValidatedTableSchema(a, table.schema_json);
    defer schema.deinit(a);
    if (try schema.relationalIndexDefinitions(sa)) |indexes| {
        const runtime = try local.schema_mod.deriveRuntimeTableSchema(a, schema);
        defer local.storage_schema.freeSchema(a, runtime);
        var layout = try local.storage_db_algebraic_relational_row_codec.PhysicalLayout.init(a, runtime);
        defer layout.deinit();
        for (indexes) |index| {
            const logical_name = try @import("lake_index_native_rows.zig").logicalName(sa, index.name);
            const declaration = for (selected.publication().declarations) |decl| {
                if (decl.artifact.kind == .ordered_row_index and std.mem.eql(u8, decl.name, logical_name)) break decl;
            } else continue;
            var tuple = try local.storage_db_relational_index_keys.TuplePlan.init(a, runtime, &layout, index.keys);
            defer tuple.deinit();
            var predicate: ?local.storage_db_relational_index_predicate.Plan = if (index.where.len == 0) null else try local.storage_db_relational_index_predicate.Plan.init(a, runtime, &layout, index.where);
            defer if (predicate) |*plan| plan.deinit();
            const root = @import("lake_index_ordered_rows.zig").loadRoot(sa, store.artifactStore(), declaration.artifact, normalized.cancellation, .{ .cache = &server.lake_read_cache, .scope = store.identity, .context = context }) catch |err| {
                try context.ensureActive();
                if (err == error.OutOfMemory) return err;
                continue;
            };
            const expected = @import("lake_index_native_rows.zig").fingerprint(tuple, predicate, try @import("lake_index_native_rows.zig").coverColumns(sa, index));
            const domain = @import("lake_index_publication.zig").uploadDomainWithNamespace(table.table_id, store.identity, selected.publication().namespace);
            if (!std.mem.eql(u8, &root.fingerprint, &expected) or !std.mem.eql(u8, &root.domain, &domain)) continue;
            const name = try a.dupe(u8, index.name);
            result.append(a, name) catch |err| {
                a.free(name);
                return err;
            };
        }
    }
    try context.ensureActive();
    return result.toOwnedSlice(a);
}
