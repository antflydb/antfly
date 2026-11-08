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

//! Actual S3-compatible source, catalog, pin and artifact reopen qualification.
const std = @import("std");
const local = @import("antfly_local_sources");
const support = @import("lake_execution_qualification_support.zig");
const configured = @import("../serverless/configured_object_store_support.zig");
const publication = @import("lake_index_publication.zig");
const native = @import("lake_execution_native_client.zig");
const catalog = local.metadata_lake_index_catalog;
const lake_catalog = local.serverless_external_source_mod.lake_catalog;
const A = std.mem.Allocator;

test "external lake execution S3-compatible catalog pins reopen and retirement fence real object I/O" {
    if (!support.enabled("ANTFLY_LAKE_S3_INTEGRATION")) return error.SkipZigTest;
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-s3-native");
    defer directory.cleanup();
    const endpoint = try support.requiredEnv(a, "AWS_ENDPOINT_URL");
    defer a.free(endpoint);
    const access_key = try support.requiredEnv(a, "AWS_ACCESS_KEY_ID");
    defer a.free(access_key);
    const secret_key = try support.requiredEnv(a, "AWS_SECRET_ACCESS_KEY");
    defer a.free(secret_key);
    const bucket = try support.requiredEnv(a, "OBJECTSTORE_S3_TEST_BUCKET");
    defer a.free(bucket);
    const prefix = try std.fmt.allocPrint(a, "lake-qualification-{d}", .{@import("antfly_platform").time.monotonicNs()});
    defer a.free(prefix);
    const source_prefix = try std.fmt.allocPrint(a, "{s}/source", .{prefix});
    defer a.free(source_prefix);
    const artifact_prefix = try std.fmt.allocPrint(a, "{s}/artifacts", .{prefix});
    defer a.free(artifact_prefix);
    const uri = try std.fmt.allocPrint(a, "s3://{s}/{s}", .{ bucket, source_prefix });
    defer a.free(uri);
    const secret_path = try std.fs.path.join(a, &.{ directory.path(), "secrets.json" });
    defer a.free(secret_path);
    var secrets = try local.common_secrets.FileStore.init(a, secret_path);
    defer secrets.deinit();
    for ([_]struct { key: []const u8, value: []const u8 }{ .{ .key = "fixture.endpoint", .value = endpoint }, .{ .key = "fixture.access", .value = access_key }, .{ .key = "fixture.secret", .value = secret_key } }) |entry| {
        var stored = try secrets.put(a, entry.key, entry.value);
        stored.deinit(a);
    }
    const cfg_json = try std.json.Stringify.valueAlloc(a, .{ .deployment_mode = "standalone", .storage = .{ .engine = "local", .local = .{ .base_dir = directory.path() }, .artifacts = .{ .connection = "fixture", .bucket = bucket, .prefix = artifact_prefix } }, .connections = .{ .fixture = .{ .kind = "external_io", .capabilities = .{ "lake.read", "lake.write", "storage.primary" }, .external_io = .{ .protocol = "s3", .endpoint = "${secret:fixture.endpoint}", .addressing_style = "path", .buckets = .{bucket}, .prefix = prefix, .credentials = .{ .source = "static", .access_key_id = "${secret:fixture.access}", .secret_access_key = "${secret:fixture.secret}" } } } } }, .{});
    defer a.free(cfg_json);
    var config = try local.common_config.Config.parseFromSliceWithSecrets(a, cfg_json, &secrets);
    defer config.deinit();
    const binding_json = try std.json.Stringify.valueAlloc(a, .{ .version = 1, .storage_mode = "relational", .default_type = "row", .base_source = .{ .kind = "external", .table_id = "pinned", .format = "iceberg", .uri = uri, .schema_fingerprint = "auto", .write_policy = "iceberg_writer", .credential_ref = .{ .ref_id = "fixture", .scope = source_prefix }, .catalog = .{ .type = "managed" }, .row_identity = .{ .key_columns = .{"body"} } }, .document_schemas = .{ .row = .{ .schema = .{ .type = "object", .properties = .{ .body = .{ .type = "string" } }, .additionalProperties = false } } } }, .{});
    defer a.free(binding_json);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, binding_json)).?;
    defer binding.deinit(a);
    var options: configured.BindingObjectStoreOpenOptions = .{ .node_config = &config, .secret_store = &secrets, .read_only = false, .catalog_table_id = 9 };
    var opened = try configured.openBindingObjectStoreAlloc(a, binding.binding, options);
    defer opened.deinit();
    var client = opened.client;
    const data = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{}, &.{.{ .column_id = "body", .field_id = 1, .converted_type = 0, .values = &.{ "pinned first", "pinned second" } }});
    defer a.free(data);
    const data_key = try std.fmt.allocPrint(a, "{s}/part.parquet", .{source_prefix});
    defer a.free(data_key);
    const data_uri = try std.fmt.allocPrint(a, "s3://{s}/{s}", .{ bucket, data_key });
    defer a.free(data_uri);
    const manifest_key = try std.fmt.allocPrint(a, "{s}/metadata/data.avro", .{source_prefix});
    defer a.free(manifest_key);
    const manifest_uri = try std.fmt.allocPrint(a, "s3://{s}/{s}", .{ bucket, manifest_key });
    defer a.free(manifest_uri);
    const list_key = try std.fmt.allocPrint(a, "{s}/metadata/list.avro", .{source_prefix});
    defer a.free(list_key);
    const list_uri = try std.fmt.allocPrint(a, "s3://{s}/{s}", .{ bucket, list_key });
    defer a.free(list_uri);
    const iceberg = local.serverless_query_lake_iceberg_snapshot;
    const manifest = try iceberg.buildTestDataManifestAlloc(a, &.{.{ .path = data_uri, .rows = 2, .bytes = data.len }});
    defer a.free(manifest);
    const manifest_list = try iceberg.buildTestManifestListAlloc(a, manifest_uri, manifest.len, 1, 2);
    defer a.free(manifest_list);
    for ([_]struct { key: []const u8, bytes: []const u8 }{ .{ .key = data_key, .bytes = data }, .{ .key = manifest_key, .bytes = manifest }, .{ .key = list_key, .bytes = manifest_list } }) |object| {
        var put = try client.putObject(bucket, object.key, object.bytes, .{});
        put.deinit(a);
    }
    var managed: lake_catalog.managed.Managed = .{ .client = client, .bucket = bucket, .prefix = source_prefix, .source_uri = uri, .context = .{ .io = std.testing.io } };
    var created = try managed.create(a, "initialize", "{\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"body\",\"type\":\"string\",\"required\":false}]}}", 1);
    defer created.deinit(a);
    const updates = try std.json.Stringify.valueAlloc(a, .{ .requirements = [_]u32{}, .updates = .{ .{ .action = "add-snapshot", .snapshot = .{ .@"snapshot-id" = 7, .@"sequence-number" = 1, .@"timestamp-ms" = 2, .@"manifest-list" = list_uri, .summary = .{ .operation = "append" } } }, .{ .action = "set-snapshot-ref", .@"ref-name" = "main", .@"snapshot-id" = 7, .type = "branch" } } }, .{});
    defer a.free(updates);
    var pinned = try managed.commit(a, .{ .id = "snapshot-7", .expected_metadata_location = created.metadata_location, .body = updates, .timestamp_ms = 2 });
    defer pinned.deinit(a);
    var detected = try local.serverless_query_lake_schema.icebergSchema(a, pinned.metadata_json, null);
    defer detected.deinit();
    const fingerprint = try a.dupe(u8, detected.fingerprint);
    a.free(binding.schema_fingerprint);
    binding.schema_fingerprint = fingerprint;
    binding.binding.schema_fingerprint = binding.schema_fingerprint;
    var schema_value = try std.json.parseFromSlice(std.json.Value, a, binding_json, .{});
    defer schema_value.deinit();
    schema_value.value.object.getPtr("base_source").?.object.getPtr("schema_fingerprint").?.* = .{ .string = detected.fingerprint };
    const resolved_schema = try std.json.Stringify.valueAlloc(a, schema_value.value, .{});
    defer a.free(resolved_schema);
    options.read_only = true;
    var source = try local.serverless_query_lake_serving.ServingSource.openWithContext(a, .{ .storage_mode = .relational, .external_base_source = binding }, options.lakeOptions(), .{ .io = std.testing.io });
    defer source.deinit();
    try std.testing.expectEqualStrings("7", source.inventory.snapshot_id);
    try std.testing.expectEqual(@as(usize, 1), source.inventory.files.len);
    try std.testing.expectEqual(@as(u64, 2), source.inventory.files[0].row_count);
    var store = try @import("lake_index_store.zig").Store.openNative(a, &config, &secrets, false, .standalone, null);
    defer store.deinit();
    const table: local.common_topology_records.TableRecord = .{ .table_id = 9, .name = "pinned", .schema_json = resolved_schema, .indexes_json = "{\"body_text\":{\"type\":\"full_text\",\"field\":\"body\",\"store_source\":true}}" };
    const pending = try publication.beginWithLocator(a, std.testing.io, table, &source, store.identity, .{ .io = std.testing.io }, 100, 1000, store.locator);
    defer a.free(pending);
    var state: support.State = .{ .allocator = a, .table = table, .io = std.testing.io };
    defer state.deinit();
    try state.set(pending);
    // The new authoritative head points at an absent manifest. A worker must
    // reopen the admitted snapshot, not silently follow this later head.
    const later_updates = try std.json.Stringify.valueAlloc(a, .{ .requirements = [_]u32{}, .updates = .{ .{ .action = "add-snapshot", .snapshot = .{ .@"snapshot-id" = 8, .@"sequence-number" = 2, .@"timestamp-ms" = 3, .@"manifest-list" = "s3://missing-bucket/missing-list.avro", .summary = .{ .operation = "append" } } }, .{ .action = "set-snapshot-ref", .@"ref-name" = "main", .@"snapshot-id" = 8, .type = "branch" } } }, .{});
    defer a.free(later_updates);
    var later = try managed.commit(a, .{ .id = "snapshot-8", .expected_metadata_location = pinned.metadata_location, .body = later_updates, .timestamp_ms = 3 });
    defer later.deinit(a);
    var reopen = options;
    reopen.retained_catalog_metadata = source.catalog_metadata;
    var cause: ?anyerror = null;
    const result = try native.build(a, std.testing.io, state.table, reopen, try store.executionDescriptor(), .{ .io = std.testing.io }, .none, state.clock(), state.lease(), null, .{}, .{}, &cause);
    defer a.free(result);
    var ready = try catalog.parse(a, result);
    defer ready.deinit();
    try std.testing.expectEqualStrings("7", ready.value.published.?.base_source.external_iceberg.snapshot_id);
    try std.testing.expect(cause == null);
    var artifact_arena = std.heap.ArenaAllocator.init(a);
    defer artifact_arena.deinit();
    const directory_ref = ready.value.published.?.directory.?;
    const directory_doc = try @import("lake_index_directory.zig").loadDocument(artifact_arena.allocator(), store.artifactStore(), .{ .kind = .external_base_source, .artifact_id = directory_ref.artifact_id, .checksum = directory_ref.checksum, .byte_len = directory_ref.byte_len }, .none, null);
    try std.testing.expectEqual(@as(usize, 1), directory_doc.declarations.len);
    for (directory_doc.declarations) |declaration| {
        const artifact = try @import("lake_index_aggregate_artifact.zig").readArtifact(a, store.artifactStore(), .{ .artifact_id = declaration.artifact.artifact_id, .checksum = declaration.artifact.checksum, .byte_len = declaration.artifact.byte_len }, .none, null);
        a.free(artifact);
    }
    // A second worker can resume the durable checkpoint using fresh clients.
    const resumed = try native.build(a, std.testing.io, state.table, reopen, try store.executionDescriptor(), .{ .io = std.testing.io }, .none, state.clock(), state.lease(), null, .{}, .{}, &cause);
    defer a.free(resumed);
    var resumed_state = try catalog.parse(a, resumed);
    defer resumed_state.deinit();
    try std.testing.expectEqualDeep(ready.value.published.?.signature, resumed_state.value.published.?.signature);
    // A current catalog load remains mandatory even when metadata is retained.
    const head_key = try std.fmt.allocPrint(a, "{s}/metadata/antfly-catalog/head.json", .{source_prefix});
    defer a.free(head_key);
    var head = try client.getObject(bucket, head_key, .{});
    defer head.deinit(a);
    try client.deleteObject(bucket, head_key, .{});
    try std.testing.expectError(error.LakeTableNotFound, native.build(a, std.testing.io, state.table, reopen, try store.executionDescriptor(), .{ .io = std.testing.io }, .none, state.clock(), state.lease(), null, .{}, .{}, &cause));
    var restored = try client.putObject(bucket, head_key, head.body, .{});
    restored.deinit(a);
    // Retained metadata from another incarnation cannot bypass a real
    // authoritative catalog read, even when its snapshot is otherwise valid.
    var alien = try std.json.parseFromSlice(std.json.Value, a, pinned.metadata_json, .{ .allocate = .alloc_always });
    defer alien.deinit();
    alien.value.object.getPtr("table-uuid").?.* = .{ .string = "another-incarnation" };
    const alien_json = try std.json.Stringify.valueAlloc(a, alien.value, .{});
    defer a.free(alien_json);
    var wrong_incarnation = reopen;
    wrong_incarnation.retained_catalog_metadata = .{ .metadata_location = pinned.metadata_location, .metadata_json = alien_json };
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, native.build(a, std.testing.io, state.table, wrong_incarnation, try store.executionDescriptor(), .{ .io = std.testing.io }, .none, state.clock(), state.lease(), null, .{}, .{}, &cause));
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const root = try lake_catalog.metadata.parse(scratch.allocator(), pinned.metadata_json);
    const uuid = try lake_catalog.metadata.str(try lake_catalog.metadata.get(root, "table-uuid"));
    const pins = @import("../serverless/lake_snapshot_pins.zig");
    const pin_prefix = try pins.namespace(a, store.opened.prefix, binding.binding, uuid);
    defer a.free(pin_prefix);
    var pin_store: pins.Store = .{ .client = store.opened.client, .bucket = store.opened.bucket, .prefix = pin_prefix, .context = .{ .io = std.testing.io } };
    const after_deadline = @import("antfly_platform").time.realtimeNs() +| pins.duration_ns +| pins.grace_ns +| std.time.ns_per_min;
    try std.testing.expect(try pin_store.retire(a, "7", after_deadline));
    try std.testing.expectError(error.LakeSnapshotRetired, native.build(a, std.testing.io, state.table, reopen, try store.executionDescriptor(), .{ .io = std.testing.io }, .none, state.clock(), state.lease(), null, .{}, .{}, &cause));
    std.debug.print("LAKE_S3_QUALIFIED source=s3 artifacts=s3 catalog=managed pinned=7 head=8 missing_head=fenced incarnation=fenced retired=fenced\n", .{});
}
