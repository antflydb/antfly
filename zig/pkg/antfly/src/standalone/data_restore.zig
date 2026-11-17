// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Restore one standalone Lite table as data under a caller-owned definition.
//! The input is only opened through storage/catalog readers. Its server state,
//! producer definitions, identities and jobs never become destination authority.
const std = @import("std");
const local = @import("antfly_local_sources");
const staging = local.storage_lite_restore_staging;
const portable = local.storage_portable_backup;
const codec = local.storage_backup_codec;
const backend_mod = local.storage_lite_backend;
const connection = local.storage_lite_connection;
const metadata = @import("../metadata/storage/raft_apply_store.zig");
const Value = std.json.Value;
const Allocator = std.mem.Allocator;

pub const Options = struct {
    max_input_bytes: u64 = 512 * 1024 * 1024,
    max_expanded_bytes: u64 = 1024 * 1024 * 1024,
    max_document_bytes: usize = 16 * 1024 * 1024,
    max_documents: u64 = 250000,
};

const Definition = struct {
    schema: Value,
    indexes: Value,
};

pub fn restoreJsonAlloc(a: Allocator, input: []const u8, output: []const u8, table_name: []const u8, definition_json: []const u8, options: Options) ![]u8 {
    if (input.len == 0 or output.len == 0 or table_name.len == 0 or table_name.len > 256 or definition_json.len > 1024 * 1024 or options.max_input_bytes == 0 or options.max_expanded_bytes == 0)
        return error.InvalidArguments;
    var definition = try std.json.parseFromSlice(Definition, a, definition_json, .{});
    defer definition.deinit();
    try validateDefinition(definition.value);
    const schema_json = try std.json.Stringify.valueAlloc(a, definition.value.schema, .{});
    defer a.free(schema_json);
    const indexes_json = try std.json.Stringify.valueAlloc(a, definition.value.indexes, .{});
    defer a.free(indexes_json);

    var io_impl = std.Io.Threaded.init(a, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var source = try std.Io.Dir.cwd().openFile(io, input, .{});
    defer source.close(io);
    if (!try source.tryLock(io, .shared)) return error.WriterLocked;
    defer source.unlock(io);
    const initial = try source.stat(io);
    if (initial.kind != .file or initial.size == 0 or initial.size > options.max_input_bytes) return error.BackupInputLimitExceeded;
    const source_digest = try fileDigest(io, source, initial.size);
    const physical = try backend_mod.checkFile(a, input);
    if (!physical.valid or physical.tail_bytes != 0 or physical.file_size != initial.size) return error.InvalidBackupRequest;

    var backend = try backend_mod.Handle.open(a, input, .{ .read_only = true });
    defer backend.deinit();
    if (!try backend.isStandaloneArtifact() and !backend.hasStandaloneRootAdoption()) return error.UnsupportedLiteInspectionLayout;
    var catalog = try metadata.RaftApplyStore.init(a, .{ .root_dir = "", .read_only = true, .borrowed_store = try backend.runtimeStoreForNamespace("system/metadata") });
    defer catalog.deinit();
    if (try catalog.standaloneRevision() == 0) return error.UnsupportedLiteInspectionLayout;
    const projection = try catalog.captureInspectionCatalog(a, local.common_group_ids.main_metadata_group_id, 4096, 16 * 1024 * 1024, .none);
    defer catalog.freeTables(a, projection.tables);
    defer catalog.freeRanges(a, projection.ranges);
    const table = selected: {
        // Resolve the primary logical bindings. A released snapshot may have an
        // older derived name-index version, which must not be rebuilt in source.
        var txn = try catalog.store.beginReadTxn();
        defer txn.abort();
        var selected: ?@TypeOf(projection.tables[0]) = null;
        for (projection.tables) |table| {
            var binding = try @import("../system_catalog/storage.zig").getByIdLimited(a, &txn, local.common_group_ids.main_metadata_group_id, .table, table.table_id, 1024 * 1024);
            defer if (binding) |*value| value.deinit();
            const name = if (binding) |value| bound: {
                if (!std.mem.eql(u8, value.value.storage_name, table.name)) return error.InvalidCatalogRecord;
                if (value.value.parent_id != local.system_catalog_domain.default_namespace_id) continue;
                if (value.value.native_service != null) return error.InvalidBackupRequest;
                break :bound value.value.name;
            } else table.name;
            if (!std.mem.eql(u8, name, table_name)) continue;
            if (selected != null) return error.UnsupportedLiteInspectionLayout;
            selected = table;
        }
        break :selected selected orelse return error.TableNotFound;
    };
    if (table.storage.engine != .local or table.storage_migration != null or table.lake_index_catalog_json.len != 0) return error.UnsupportedLiteInspectionLayout;
    if (!try jsonEqual(a, table.schema_json, schema_json) or !try emptyArray(a, table.replication_sources_json)) return error.InvalidBackupRequest;
    try requireIndexes(a, table.indexes_json, definition.value.indexes);
    var group: ?u64 = null;
    for (projection.ranges) |range| if (range.table_id == table.table_id) {
        if (group != null or range.start_key.len != 0) return error.UnsupportedLiteInspectionLayout;
        if (range.end_key) |end| if (end.len != 0) return error.UnsupportedLiteInspectionLayout;
        group = range.group_id;
    };
    const namespace = try std.fmt.allocPrint(a, "group-{d}/table-db", .{group orelse return error.UnsupportedLiteInspectionLayout});
    defer a.free(namespace);
    var store = try local.storage_docstore.DocStore.openRuntime(a, try backend.runtimeStoreForNamespace(namespace));
    defer store.close();

    var nonce: [16]u8 = undefined;
    try io.randomSecure(&nonce);
    const temporary = try std.fmt.allocPrint(a, "{s}.restore-{s}.aflite", .{ output, std.fmt.bytesToHex(nonce, .lower) });
    defer a.free(temporary);
    defer std.Io.Dir.cwd().deleteFile(io, temporary) catch {};
    const lock = try std.fmt.allocPrint(a, "{s}.lock", .{temporary});
    defer a.free(lock);
    defer std.Io.Dir.cwd().deleteFile(io, lock) catch {};
    const archive_path = try std.fmt.allocPrint(a, "{s}.afb", .{temporary});
    defer a.free(archive_path);
    defer std.Io.Dir.cwd().deleteFile(io, archive_path) catch {};
    var archive = try std.Io.Dir.cwd().createFile(io, archive_path, .{ .read = true, .exclusive = true, .permissions = .fromMode(0o600) });
    defer archive.close(io);
    var buffer: [64 * 1024]u8 = undefined;
    var writer = archive.writer(io, &buffer);
    var limited = staging.LimitedWriter.init(&writer.interface, options.max_expanded_bytes);
    portable.exportPortableToWriter(a, &store, &limited.interface) catch |err| {
        if (limited.exceeded) return error.BackupExpansionLimitExceeded;
        return err;
    };
    try writer.end();

    var documents: u64 = 0;
    var vectors: u64 = 0;
    {
        var target = try openDestination(a, temporary, true);
        defer target.close();
        try target.db.setSchemaJson(a, schema_json);
        var copy: Copy = .{ .a = a, .target = &target, .definition = definition.value, .options = options };
        defer copy.decoder.deinit(a);
        const inspection_options: portable.FileInspectionOptions = .{
            .metadata_first = true,
            .max_input_bytes = options.max_expanded_bytes,
            .max_logical_bytes = options.max_expanded_bytes,
            .max_metadata_bytes = 16 * 1024 * 1024,
            .max_document_bytes = options.max_document_bytes,
        };
        _ = try portable.inspectPortableFileBlocks(a, io, archive, inspection_options, &copy, Copy.visit);
        // Large archives can flush an artifact block before its document block.
        // Restore documents first, then validate/copy vectors against those keys.
        copy.vectors_only = true;
        _ = try portable.inspectPortableFileBlocks(a, io, archive, inspection_options, &copy, Copy.visit);
        // Indexes are built only from the caller's definition, after all
        // records validate. No source catalog is ever installed in this DB.
        var it = definition.value.indexes.object.iterator();
        while (it.next()) |entry| {
            if (std.mem.eql(u8, entry.key_ptr.*, "full_text_index_v0")) continue;
            const public_json = try std.json.Stringify.valueAlloc(a, entry.value_ptr.*, .{});
            defer a.free(public_json);
            const config = try local.api_table_index_config.parseIndexConfig(a, entry.key_ptr.*, public_json);
            defer a.free(config.name);
            defer a.free(config.config_json);
            try target.db.addIndex(config);
        }
        try staging.finalizeRestoredLiteDb(a, &target.db);
        documents = copy.documents;
        vectors = copy.vectors;
        // Assign the public name to the fresh standalone table. Its native
        // root identity was fixed before building any derived indexes.
        const identity = connection.embeddedRootIdentity();
        var target_catalog = try metadata.RaftApplyStore.init(a, .{ .root_dir = "", .borrowed_store = try target.backend.runtimeStoreForNamespace("system/metadata") });
        defer target_catalog.deinit();
        try target_catalog.updateStandaloneCatalog(local.common_group_ids.main_metadata_group_id, 0, .{
            .replace = true,
            .auxiliary_json = "{}",
            .tables = &.{.{ .table_id = identity.table_id, .name = table_name, .schema_json = schema_json, .indexes_json = indexes_json, .desired_replica_count = 1 }},
            .ranges = &.{.{ .table_id = identity.table_id, .group_id = identity.shard_id, .range_id = identity.range_id, .doc_identity_shard_id = identity.shard_id, .doc_identity_range_id = identity.range_id, .start_key = "" }},
        });
        try target.backend.markStandaloneArtifact();
    }
    // Do not publish an output if the source name or bytes changed while the
    // read-only catalog and portable readers were using it.
    var final_source = try std.Io.Dir.cwd().openFile(io, input, .{});
    defer final_source.close(io);
    if (!local.api_local_backups.localArtifactStatsEqual(initial, try final_source.stat(io)) or !std.mem.eql(u8, &source_digest, &try fileDigest(io, final_source, initial.size))) return error.SourceFileChanged;
    var result = try std.Io.Dir.cwd().openFile(io, temporary, .{});
    defer result.close(io);
    const result_stat = try result.stat(io);
    if (result_stat.size > options.max_expanded_bytes) return error.BackupExpansionLimitExceeded;
    try result.sync(io);
    const result_digest = try fileDigest(io, result, result_stat.size);
    const json = try std.json.Stringify.valueAlloc(a, .{
        .report_version = 1,
        .operation = "data_restore",
        .source = .{ .sha256 = &source_digest, .size_bytes = initial.size, .tables = projection.tables.len },
        .output = .{ .sha256 = &result_digest, .size_bytes = result_stat.size, .table = table_name },
        .inventory = .{ .documents = documents, .artifact_records = vectors, .shards = 1 },
    }, .{});
    errdefer a.free(json);
    try std.Io.Dir.hardLink(.cwd(), temporary, .cwd(), output, io, .{});
    staging.confirmPublishedFileDurability(io, output) catch return error.DurabilityOutcomeUnknown;
    return json;
}

fn openDestination(a: Allocator, path: []const u8, create: bool) !connection.Connection {
    var backend = if (create)
        try backend_mod.Handle.create(a, path, true)
    else
        try backend_mod.Handle.open(a, path, .{ .read_only = true });
    errdefer backend.deinit();
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    const identity = connection.embeddedRootIdentity();
    const namespace = try std.fmt.allocPrint(a, "group-{d}/table-db", .{identity.shard_id});
    defer a.free(namespace);
    var options: db_mod.OpenOptions = .{
        .open_mode = if (create) .writer else .query_readonly,
        .external_derived_checkpoints = false,
        .identity_namespace = identity,
    };
    // Construct directly in the standalone namespace. Adopting an embedded
    // root after index construction changes its durable root incarnation and
    // invalidates the vector publication proof on the first server open.
    try backend.configureDbOpenOptionsForNamespace(&options, namespace);
    var db = try db_mod.DB.open(a, namespace, options);
    errdefer db.close();
    if (create) try connection.provisionDefaultFullTextIndex(&db);
    return .{ .backend = backend, .db = db, .open_mode = options.open_mode };
}

fn validateDefinition(definition: Definition) !void {
    if (definition.schema != .object or definition.indexes != .object or definition.indexes.object.count() == 0 or definition.indexes.object.count() > 64) return error.InvalidBackupRequest;
    var it = definition.indexes.object.iterator();
    while (it.next()) |entry| {
        const index = entry.value_ptr.*;
        if (index != .object or entry.key_ptr.len == 0 or entry.key_ptr.len > 256) return error.InvalidBackupRequest;
        const name = index.object.get("name") orelse return error.InvalidBackupRequest;
        const kind = index.object.get("type") orelse return error.InvalidBackupRequest;
        if (name != .string or !std.mem.eql(u8, name.string, entry.key_ptr.*) or kind != .string) return error.InvalidBackupRequest;
        if (std.mem.eql(u8, kind.string, "full_text")) {
            if (index.object.count() != 2) return error.InvalidBackupRequest;
        } else if (std.mem.eql(u8, kind.string, "embeddings")) {
            if (index.object.count() != 5) return error.InvalidBackupRequest;
            const external = index.object.get("external") orelse return error.InvalidBackupRequest;
            const dims = index.object.get("dimension") orelse return error.InvalidBackupRequest;
            const metric = index.object.get("distance_metric") orelse return error.InvalidBackupRequest;
            if (external != .bool or !external.bool or dims != .integer or dims.integer <= 0 or dims.integer > 65535 or metric != .string or
                (!std.mem.eql(u8, metric.string, "cosine") and !std.mem.eql(u8, metric.string, "l2_squared") and !std.mem.eql(u8, metric.string, "inner_product"))) return error.InvalidBackupRequest;
        } else return error.InvalidBackupRequest;
    }
    const default_index = definition.indexes.object.get("full_text_index_v0") orelse return error.InvalidBackupRequest;
    if (!std.mem.eql(u8, default_index.object.get("type").?.string, "full_text")) return error.InvalidBackupRequest;
}

const Copy = struct {
    a: Allocator,
    target: *connection.Connection,
    definition: Definition,
    options: Options,
    decoder: portable.InspectionDocumentDecoder = .{},
    documents: u64 = 0,
    vectors: u64 = 0,
    vectors_only: bool = false,

    fn visit(self: *Copy, kind: codec.BlockType, payload: []const u8) !void {
        switch (kind) {
            .metadata_batch => {
                if (self.vectors_only) return;
                try self.decoder.metadataLimited(self.a, payload, 16 * 1024 * 1024);
                var reader = try codec.KeyValueBatchReader.init(payload);
                while (try reader.next()) |entry| {
                    if (std.mem.endsWith(u8, entry.key, ":enrichments") or std.mem.endsWith(u8, entry.key, ":resolvers")) {
                        if (!try emptyArray(self.a, entry.value)) return error.InvalidBackupRequest;
                    }
                }
            },
            .document_batch => {
                if (self.vectors_only) return;
                const entries = try codec.decodeDocumentBatchBorrowed(self.a, payload);
                defer self.a.free(entries);
                for (entries) |entry| {
                    if (self.documents == self.options.max_documents or entry.value_flags & codec.doc_value_flag_relational_row != 0) return error.BackupInputLimitExceeded;
                    const json = try self.decoder.documentJsonLimitedAlloc(self.a, entry, self.options.max_document_bytes);
                    defer self.a.free(json);
                    // Batch input has reserved write directives. Stored data
                    // must not be reinterpreted as a request to create edges
                    // or artifacts while rebuilding the destination.
                    var document = try std.json.parseFromSlice(Value, self.a, json, .{});
                    defer document.deinit();
                    if (document.value != .object or document.value.object.contains("_edges") or document.value.object.contains("_embeddings")) return error.InvalidBackupRequest;
                    const existing = try self.target.db.lookup(self.a, entry.key, .{});
                    if (existing) |value| {
                        var owned = value;
                        owned.deinit(self.a);
                        return error.InvalidBackupRequest;
                    }
                    try self.target.db.batch(.{ .writes = &.{.{ .key = entry.key, .value = json }}, .sync_level = .write });
                    self.documents += 1;
                }
            },
            .embedding_batch => {
                if (!self.vectors_only) return;
                const batch = try codec.decodeEmbeddingBatch(self.a, payload);
                defer {
                    self.a.free(batch.index_name);
                    for (batch.entries) |entry| {
                        self.a.free(entry.doc_key);
                        self.a.free(entry.vector);
                    }
                    self.a.free(batch.entries);
                }
                for (batch.entries) |entry| try self.copyVector(batch.index_name, entry.doc_key, entry.vector);
            },
            .artifact_batch => {
                if (!self.vectors_only) return;
                var reader = try codec.KeyValueBatchReader.init(payload);
                while (try reader.next()) |entry| {
                    var ref = (try local.storage_db_artifact_ids.decodeArtifactPublicIdAlloc(self.a, entry.key)) orelse return error.InvalidBackupRequest;
                    defer ref.deinit(self.a);
                    if (ref.kind != .embedding or ref.chunk_id != null or ref.window_index != null or ref.unit_id != null or ref.source != null or ref.revision != null or ref.derivation_revision != null) return error.InvalidBackupRequest;
                    const vector = try local.storage_db_enrichment_artifact_codec.decodeDenseEmbeddingAlloc(self.a, entry.value);
                    defer self.a.free(vector);
                    try self.copyVector(ref.name, ref.document_id, vector);
                }
            },
            // Framing and source identities are verified, never installed.
            .bundle_manifest, .cluster_manifest, .table_manifest, .shard_header, .shard_footer, .file_footer, .doc_identity_batch => {},
            else => return error.InvalidBackupRequest,
        }
    }

    fn copyVector(self: *Copy, name: []const u8, doc_key: []const u8, vector: []const f32) !void {
        const index = self.definition.indexes.object.get(name) orelse return error.InvalidBackupRequest;
        const dims = index.object.get("dimension") orelse return error.InvalidBackupRequest;
        if (vector.len != dims.integer) return error.InvalidBackupRequest;
        for (vector) |v| if (!std.math.isFinite(v)) return error.InvalidBackupRequest;
        var doc = (try self.target.db.lookup(self.a, doc_key, .{})) orelse return error.InvalidBackupRequest;
        doc.deinit(self.a);
        const key = try local.storage_internal_keys.embeddingArtifactKeyForDocumentAlloc(self.a, doc_key, name);
        defer self.a.free(key);
        const prior = self.target.db.core.store.get(self.a, key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (prior) |bytes| {
            self.a.free(bytes);
            return error.InvalidBackupRequest;
        }
        const value = try local.storage_db_enrichment_artifact_codec.encodeAuthoredDenseEmbeddingAlloc(self.a, vector);
        defer self.a.free(value);
        try self.target.db.core.store.putBatch(&.{.{ .key = key, .value = value }}, &.{});
        self.vectors += 1;
    }
};

fn emptyArray(a: Allocator, json: []const u8) !bool {
    if (json.len == 0) return true;
    var parsed = try std.json.parseFromSlice(Value, a, json, .{});
    defer parsed.deinit();
    return parsed.value == .array and parsed.value.array.items.len == 0;
}

fn jsonEqual(a: Allocator, left: []const u8, right: []const u8) !bool {
    var l = try std.json.parseFromSlice(Value, a, left, .{});
    defer l.deinit();
    var r = try std.json.parseFromSlice(Value, a, right, .{});
    defer r.deinit();
    return valueEqual(l.value, r.value);
}

fn valueEqual(left: Value, right: Value) bool {
    if (std.meta.activeTag(left) != std.meta.activeTag(right)) return false;
    return switch (left) {
        .null => true,
        .bool => |v| v == right.bool,
        .integer => |v| v == right.integer,
        .float => |v| v == right.float,
        .number_string => |v| std.mem.eql(u8, v, right.number_string),
        .string => |v| std.mem.eql(u8, v, right.string),
        .array => |v| blk: {
            if (v.items.len != right.array.items.len) break :blk false;
            for (v.items, right.array.items) |x, y| if (!valueEqual(x, y)) break :blk false;
            break :blk true;
        },
        .object => |v| blk: {
            if (v.count() != right.object.count()) break :blk false;
            var it = v.iterator();
            while (it.next()) |entry| if (!valueEqual(entry.value_ptr.*, right.object.get(entry.key_ptr.*) orelse break :blk false)) break :blk false;
            break :blk true;
        },
    };
}

fn requireIndexes(a: Allocator, json: []const u8, expected: Value) !void {
    var parsed = try std.json.parseFromSlice(Value, a, json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidBackupRequest;
    var it = parsed.value.object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .object) return error.InvalidBackupRequest;
        _ = entry.value_ptr.object.orderedRemove("_index_incarnation");
        _ = entry.value_ptr.object.orderedRemove("_coverage_incarnation");
    }
    if (!valueEqual(parsed.value, expected)) return error.InvalidBackupRequest;
}

fn fileDigest(io: std.Io, file: std.Io.File, size: u64) ![64]u8 {
    var h = std.crypto.hash.sha2.Sha256.init(.{});
    var buffer: [64 * 1024]u8 = undefined;
    var offset: u64 = 0;
    while (offset < size) {
        const n: usize = @intCast(@min(buffer.len, size - offset));
        if (try file.readPositionalAll(io, buffer[0..n], offset) != n) return error.SourceFileChanged;
        h.update(buffer[0..n]);
        offset += n;
    }
    return std.fmt.bytesToHex(h.finalResult(), .lower);
}

const test_schema = "{\"version\":0,\"default_type\":\"doc\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"additionalProperties\":true,\"properties\":{\"content\":{\"type\":\"string\",\"x-antfly-types\":[\"text\"]}}}}}}";
const test_indexes = "{\"full_text_index_v0\":{\"name\":\"full_text_index_v0\",\"type\":\"full_text\"},\"vectors\":{\"name\":\"vectors\",\"type\":\"embeddings\",\"external\":true,\"dimension\":2,\"distance_metric\":\"cosine\"}}";
const test_definition = "{\"schema\":" ++ test_schema ++ ",\"indexes\":" ++ test_indexes ++ "}";

fn createTestSource(a: Allocator, path: []const u8, producer: bool) !void {
    var source = try connection.Connection.create(a, path, true);
    defer source.close();
    try source.db.setSchemaJson(a, test_schema);
    const config = try local.api_table_index_config.parseIndexConfig(a, "vectors", "{\"name\":\"vectors\",\"type\":\"embeddings\",\"external\":true,\"dimension\":2,\"distance_metric\":\"cosine\"}");
    defer a.free(config.name);
    defer a.free(config.config_json);
    try source.db.addIndex(config);
    try source.db.batch(.{ .writes = &.{
        .{ .key = "one", .value = "{\"content\":\"first searchable record\",\"_embeddings\":{\"vectors\":[1,0]}}" },
        .{ .key = "two", .value = "{\"content\":\"keyword only record\"}" },
    }, .sync_level = .write });
    try source.db.sync(true);
    if (producer) try source.db.core.store.putBatch(&.{.{ .key = "\x00\x00__metadata__:enrichments", .value = "[{\"name\":\"hidden\",\"kind\":\"chunk\",\"source_field\":\"content\"}]" }}, &.{});
    const identity = connection.embeddedRootIdentity();
    const namespace = try std.fmt.allocPrint(a, "group-{d}/table-db", .{identity.shard_id});
    defer a.free(namespace);
    try source.backend.adoptEmbeddedRootAsNamespace(namespace);
    var catalog = try metadata.RaftApplyStore.init(a, .{ .root_dir = "", .borrowed_store = try source.backend.runtimeStoreForNamespace("system/metadata") });
    defer catalog.deinit();
    try catalog.updateStandaloneCatalog(local.common_group_ids.main_metadata_group_id, 0, .{
        .replace = true,
        .auxiliary_json = "{}",
        .import_catalog = .{ .revision = 1, .next_id = identity.table_id + 1, .resources = &.{.{ .kind = .table, .id = identity.table_id, .parent_id = local.system_catalog_domain.default_namespace_id, .name = "records", .storage_name = "physical-records" }} },
        .tables = &.{.{ .table_id = identity.table_id, .name = "physical-records", .schema_json = test_schema, .indexes_json = test_indexes, .desired_replica_count = 1 }},
        .ranges = &.{.{ .table_id = identity.table_id, .group_id = identity.shard_id, .range_id = identity.range_id, .doc_identity_shard_id = identity.shard_id, .doc_identity_range_id = identity.range_id, .start_key = "" }},
    });
    // This deliberately is not a runnable restore-job record. A full server
    // startup must not be used to inspect or admit this source.
    const jobs = try source.backend.runtimeStoreForNamespace("system/api-restore-jobs");
    var write = try jobs.beginWrite();
    errdefer write.abort();
    try write.put("source-only-job", "do not resume");
    try write.commit();
    try source.backend.markStandaloneArtifact();
}

test "data restore preserves documents and partial external vectors without source jobs" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const input = try std.fs.path.join(a, &.{ root, "source.aflite" });
    defer a.free(input);
    const output = try std.fs.path.join(a, &.{ root, "result.aflite" });
    defer a.free(output);
    try createTestSource(a, input, false);
    var source = try std.Io.Dir.cwd().openFile(io, input, .{});
    defer source.close(io);
    const before = try fileDigest(io, source, (try source.stat(io)).size);
    const report = try restoreJsonAlloc(a, input, output, "records", test_definition, .{});
    defer a.free(report);
    var parsed = try std.json.parseFromSlice(Value, a, report, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(i64, 2), parsed.value.object.get("inventory").?.object.get("documents").?.integer);
    try std.testing.expectEqual(@as(i64, 1), parsed.value.object.get("inventory").?.object.get("artifact_records").?.integer);
    try std.testing.expectEqualStrings(&before, parsed.value.object.get("source").?.object.get("sha256").?.string);
    try std.testing.expectEqualStrings(&before, &try fileDigest(io, source, (try source.stat(io)).size));
    var result = try openDestination(a, output, false);
    defer result.close();
    try std.testing.expect(result.db.core.index_manager.vectorBlockReadyForDenseIndex("vectors"));
    var one = (try result.db.lookup(a, "one", .{})) orelse return error.MissingDocument;
    defer one.deinit(a);
    try std.testing.expect(std.mem.indexOf(u8, one.json, "first searchable record") != null);
    const vector_key = try local.storage_internal_keys.embeddingArtifactKeyForDocumentAlloc(a, "one", "vectors");
    defer a.free(vector_key);
    const actual = try result.db.core.store.get(a, vector_key);
    defer a.free(actual);
    const expected = try local.storage_db_enrichment_artifact_codec.encodeAuthoredDenseEmbeddingAlloc(a, &.{ 1, 0 });
    defer a.free(expected);
    try std.testing.expectEqualSlices(u8, expected, actual);
    for ([_]struct { request: []const u8, expected: []const u8 }{
        .{ .request = "{\"full_text_search\":{\"match\":\"keyword\",\"field\":\"content\"},\"indexes\":[\"full_text_index_v0\"],\"limit\":10}", .expected = "\"two\"" },
        .{ .request = "{\"embeddings\":{\"vectors\":[1,0]},\"indexes\":[\"vectors\"],\"limit\":1}", .expected = "\"one\"" },
    }) |query| {
        var request = try local.api_query.parsePublicQueryRequest(a, null, "records", query.request);
        defer request.deinit(a);
        var hits = try result.db.search(a, request.req);
        defer hits.deinit();
        var response = try local.api_query.encodeQueryResponses(a, "records", request.req, .{}, hits);
        defer response.deinit(a);
        if (std.mem.indexOf(u8, response.json, query.expected) == null) std.debug.print("restore query expected {s}: {s}\n", .{ query.expected, response.json });
        try std.testing.expect(std.mem.indexOf(u8, response.json, query.expected) != null);
    }
    const jobs = try result.backend.runtimeStoreForNamespace("system/api-restore-jobs");
    var read = try jobs.beginRead();
    defer read.abort();
    try std.testing.expectError(error.NotFound, read.get("source-only-job"));
}

test "data restore rejects hidden producers and never replaces an existing output" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const input = try std.fs.path.join(a, &.{ root, "producer.aflite" });
    defer a.free(input);
    const output = try std.fs.path.join(a, &.{ root, "previous.aflite" });
    defer a.free(output);
    try createTestSource(a, input, true);
    var previous = try std.Io.Dir.cwd().createFile(io, output, .{ .read = true, .exclusive = true });
    defer previous.close(io);
    try previous.writePositionalAll(io, "previous generation", 0);
    try std.testing.expectError(error.InvalidBackupRequest, restoreJsonAlloc(a, input, output, "records", test_definition, .{}));
    var bytes: [19]u8 = undefined;
    try std.testing.expectEqual(@as(usize, bytes.len), try previous.readPositionalAll(io, &bytes, 0));
    try std.testing.expectEqualStrings("previous generation", &bytes);
    try std.testing.expectError(error.BackupInputLimitExceeded, restoreJsonAlloc(a, input, output, "records", test_definition, .{ .max_input_bytes = 1 }));
}

test "data restore fails closed on limits corruption and existing outputs" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const input = try std.fs.path.join(a, &.{ root, "source.aflite" });
    defer a.free(input);
    const output = try std.fs.path.join(a, &.{ root, "result.aflite" });
    defer a.free(output);
    try createTestSource(a, input, false);
    try std.testing.expectError(error.TableNotFound, restoreJsonAlloc(a, input, output, "absent", test_definition, .{}));
    try std.testing.expectError(error.BackupExpansionLimitExceeded, restoreJsonAlloc(a, input, output, "records", test_definition, .{ .max_expanded_bytes = 16 }));
    try std.testing.expectError(error.BackupInputLimitExceeded, restoreJsonAlloc(a, input, output, "records", test_definition, .{ .max_documents = 1 }));
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().openFile(io, output, .{}));
    {
        var previous = try std.Io.Dir.cwd().createFile(io, output, .{ .read = true, .exclusive = true });
        defer previous.close(io);
        try previous.writePositionalAll(io, "previous generation", 0);
        try std.testing.expectError(error.PathAlreadyExists, restoreJsonAlloc(a, input, output, "records", test_definition, .{}));
        var bytes: [19]u8 = undefined;
        try std.testing.expectEqual(@as(usize, bytes.len), try previous.readPositionalAll(io, &bytes, 0));
        try std.testing.expectEqualStrings("previous generation", &bytes);
    }
    try std.Io.Dir.cwd().deleteFile(io, output);
    {
        var source = try std.Io.Dir.cwd().openFile(io, input, .{ .mode = .read_write });
        defer source.close(io);
        try source.writePositionalAll(io, "trailing garbage", (try source.stat(io)).size);
    }
    try std.testing.expectError(error.InvalidBackupRequest, restoreJsonAlloc(a, input, output, "records", test_definition, .{}));
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().openFile(io, output, .{}));
}
