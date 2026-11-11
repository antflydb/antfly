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

//! Exercise the complete boundary with independently reopened physical resources.
const std = @import("std");
const local = @import("antfly_local_sources");
const c = @import("lake_execution_native_contract.zig");
const publication = @import("lake_index_publication.zig");
const catalog = local.metadata_lake_index_catalog;
const A = std.mem.Allocator;
comptime {
    _ = @import("lake_index_native_state.zig");
    _ = @import("lake_row_fingerprints.zig");
    _ = @import("lake_row_input_recipes.zig");
    if (!@hasDecl(@import("lake_execution_options"), "external_test_provider")) @import("lake_execution_native_provider.zig").exportOperations();
}

/// Retrying one fence preserves logical declarations and valid owned artifacts.
/// Physical checkpoint roots may be repacked, so content-addressed directory
/// identifiers are not the publication idempotency key (generation + token is).
fn expectCandidate(a: A, expected: catalog.Publication, expected_declarations: []const local.serverless_segment_sidecar_manifest.DeclaredArtifact, actual: catalog.Publication, store: @import("../serverless/artifacts/store.zig").ArtifactStore) !void {
    try std.testing.expectEqualDeep(expected.signature, actual.signature);
    try std.testing.expectEqual(expected.generation, actual.generation);
    try std.testing.expectEqualDeep(expected.token, actual.token);
    try std.testing.expectEqual(expected.reader_protocol, actual.reader_protocol);
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const directories = @import("lake_index_directory.zig");
    const after = actual.directory.?;
    const current = try directories.loadDocument(scratch.allocator(), store, .{ .kind = .external_base_source, .artifact_id = after.artifact_id, .checksum = after.checksum, .byte_len = after.byte_len }, .none, null);
    try std.testing.expectEqual(expected_declarations.len, current.declarations.len);
    for (expected_declarations) |old| {
        const declaration = for (current.declarations) |value| {
            if (std.mem.eql(u8, old.name, value.name)) break value;
        } else return error.TestExpectedEqual;
        try std.testing.expectEqual(old.artifact.kind, declaration.artifact.kind);
        try std.testing.expectEqualDeep(old.binding, declaration.binding);
        const bytes = try @import("lake_index_aggregate_artifact.zig").readArtifact(a, store, .{ .artifact_id = declaration.artifact.artifact_id, .checksum = declaration.artifact.checksum, .byte_len = declaration.artifact.byte_len }, .none, null);
        a.free(bytes);
    }
}

test "external lake execution operation owns clients checkpoints cancellation and fenced results" {
    try testNativeExecution(false, false);
}

test "external lake execution builds native typed Parquet vector lists across pages" {
    try testNativeExecution(true, false);
}

test "external lake execution preserves DOUBLE array precision through native index builds" {
    try testNativeExecution(true, true);
}

fn testNativeExecution(typed_vectors: bool, double_vectors: bool) !void {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-execution-native");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    var input_arena = std.heap.ArenaAllocator.init(a);
    defer input_arena.deinit();
    const input_alloc = input_arena.allocator();
    var bodies: [231][]const u8 = undefined;
    var dense: [bodies.len][]const u8 = undefined;
    var sparse: [bodies.len][]const u8 = undefined;
    for (&bodies, &dense, &sparse, 0..) |*body, *vector, *terms, i| {
        body.* = try std.fmt.allocPrint(input_alloc, "value {d}", .{i});
        vector.* = try std.fmt.allocPrint(input_alloc, "[{d},1]", .{i});
        terms.* = "{\"1\":2}";
    }
    for (0..2) |file| {
        for (&bodies, 0..) |*body, i| body.* = try std.fmt.allocPrint(input_alloc, "file-{d}-value-{d}", .{ file, i });
        const data = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{}, &.{
            .{ .column_id = "body", .field_id = 1, .converted_type = 0, .values = &bodies },
            .{ .column_id = "dense", .field_id = 2, .converted_type = 0, .values = &dense },
            .{ .column_id = "sparse", .field_id = 3, .converted_type = 0, .values = &sparse },
        });
        defer a.free(data);
        var name: [32]u8 = undefined;
        const typed_data = if (double_vectors)
            (if (file == 0) @embedFile("testdata/lake_double_vectors_0.parquet") else @embedFile("testdata/lake_double_vectors_1.parquet"))
        else if (file == 0) @embedFile("testdata/lake_vectors_0.parquet") else @embedFile("testdata/lake_vectors_1.parquet");
        var put = try client.putObject("antfly", try std.fmt.bufPrint(&name, "part-{d}.parquet", .{file}), if (typed_vectors) typed_data else data, .{});
        put.deinit(a);
    }
    const schema = try std.fmt.allocPrint(a, "{{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"base_source\":{{\"kind\":\"external\",\"table_id\":\"lake\",\"format\":\"parquet\",\"uri\":\"file://{s}\",\"schema_fingerprint\":\"schema\",\"row_identity\":{{\"key_columns\":[\"body\"]}}}},\"relational_indexes\":[{{\"name\":\"body_idx\",\"keys\":[{{\"column\":\"body\"}}],\"include_columns\":[\"dense\"]}}],\"document_schemas\":{{\"row\":{{\"schema\":{{\"type\":\"object\",\"properties\":{{\"body\":{{\"type\":\"string\"}},\"dense\":{s},\"sparse\":{{\"type\":\"string\"}}}},\"additionalProperties\":false}}}}}}}}", .{ directory.path(), if (typed_vectors) "{\"type\":\"array\",\"items\":{\"type\":\"number\"}}" else "{\"type\":\"string\"}" });
    defer a.free(schema);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema)).?;
    defer binding.deinit(a);
    var source = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
    defer source.deinit();
    if (double_vectors) {
        var stream = try local.serverless_query_lake_stream.Stream.init(a, &source, &.{"dense"}, &.{}, .{ .io = std.testing.io }, .{});
        defer stream.deinit();
        const batch = (try stream.next()).?;
        const column = batch.findColumn("dense").?;
        try std.testing.expect(column.values == .json);
        var check_arena = std.heap.ArenaAllocator.init(a);
        defer check_arena.deinit();
        const value = try column.values.jsonAt(check_arena.allocator(), 0);
        try std.testing.expectEqual(@as(f64, 1e-8), try std.fmt.parseFloat(f64, value.array.items[0].number_string));
        try std.testing.expectEqual(@as(f64, 1.0000000000000002), try std.fmt.parseFloat(f64, value.array.items[1].number_string));
    }
    var store = try @import("lake_index_store.zig").Store.openNative(a, null, null, false, .standalone, directory.path());
    defer store.deinit();
    const table: local.common_topology_records.TableRecord = .{ .table_id = 4, .name = "lake", .schema_json = schema, .indexes_json = if (typed_vectors) "{\"body_text\":{\"type\":\"full_text\",\"field\":\"body\",\"store_source\":true},\"stats\":{\"type\":\"algebraic\",\"materializations\":[{\"name\":\"rows\",\"op\":\"count\"}]},\"dense\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2,\"lake_bulk_build\":\"lloyd\",\"quantization_bits\":4,\"quantization_refine\":\"all\"},\"sparse\":{\"type\":\"embeddings\",\"external\":true,\"sparse\":true}}" else "{\"body_text\":{\"type\":\"full_text\",\"field\":\"body\",\"store_source\":true},\"stats\":{\"type\":\"algebraic\",\"materializations\":[{\"name\":\"rows\",\"op\":\"count\"}]},\"dense\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2,\"lake_bulk_build\":\"lloyd\"},\"sparse\":{\"type\":\"embeddings\",\"external\":true,\"sparse\":true}}" };
    const pending_bytes = try publication.beginWithLocator(a, std.testing.io, table, &source, store.identity, .{}, 100, 1000, store.locator);
    defer a.free(pending_bytes);
    var pending = table;
    pending.lake_index_catalog_json = pending_bytes;
    const Authority = struct {
        table: local.common_topology_records.TableRecord,
        saved: ?[]u8 = null,
        clock: u64 = 101,
        checkpoints: usize = 0,
        discards: usize = 0,
        ambiguous_discard: bool = false,
        rejected: bool = false,
        expire_after_checkpoint: bool = false,
        exhaust_snapshot_budget: bool = false,
        mutex: std.Io.Mutex = .init,
        fn snapshot(raw: *anyopaque, allocator: A) !local.common_topology_records.TableRecord {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.mutex.lock(std.testing.io);
            defer self.mutex.unlock(std.testing.io);
            if (self.exhaust_snapshot_budget) {
                const impossible = try allocator.alloc(u8, 2 * 1024 * 1024 * 1024);
                defer allocator.free(impossible);
            }
            var result = self.table;
            result.lake_index_catalog_json = try allocator.dupe(u8, self.table.lake_index_catalog_json);
            return result;
        }
        fn now(raw: *const anyopaque) !u64 {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            return self.clock;
        }
        fn save(raw: *anyopaque, saved: catalog.Publication) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.mutex.lock(std.testing.io);
            defer self.mutex.unlock(std.testing.io);
            if (self.rejected) return error.CallerOnlyLakeCheckpointRejected;
            var current = try catalog.parse(std.testing.allocator, self.table.lake_index_catalog_json);
            defer current.deinit();
            const bytes = try catalog.encode(std.testing.allocator, try current.value.saveCheckpoint(saved, self.clock));
            if (self.saved) |old| std.testing.allocator.free(old);
            self.saved = bytes;
            self.table.lake_index_catalog_json = bytes;
            self.checkpoints += 1;
            if (self.expire_after_checkpoint) self.clock = 1100;
        }
        fn discard(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var current = try catalog.parse(std.testing.allocator, self.table.lake_index_catalog_json);
            defer current.deinit();
            const bytes = try catalog.encode(std.testing.allocator, try current.value.discardCheckpoint(current.value.pending.?.token, self.clock));
            if (self.saved) |old| std.testing.allocator.free(old);
            self.saved = bytes;
            self.table.lake_index_catalog_json = bytes;
            self.discards += 1;
            if (self.ambiguous_discard) return error.MetadataMutationOutcomeUnknown;
        }
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.rejected) return error.CallerOnlyLakeOperationCanceled;
        }
    };
    var authority: Authority = .{ .table = pending };
    defer if (authority.saved) |bytes| a.free(bytes);
    const lease: publication.Lease = .{ .ptr = &authority, .snapshot = Authority.snapshot, .save_checkpoint = Authority.save, .discard_checkpoint = Authority.discard };
    const clock: publication.Clock = .{ .ptr = &authority, .now_ms = Authority.now };
    var cause: ?anyerror = null;
    const native_client = @import("lake_execution_native_client.zig");
    if (@hasDecl(@import("lake_execution_options"), "external_test_provider")) {
        const get = @extern(*const fn () callconv(.c) *const c.Endpoint, .{ .name = "antfly_lake_execute_v3" });
        const endpoint = get();
        if (endpoint.version == c.endpoint_version and endpoint.layout_count == c.layouts.len) for (endpoint.layout.*, c.layouts, 0..) |actual, expected, index| if (!actual.matches(expected)) {
            std.debug.print("lake execution incompatible layout {d}: actual={any} expected={any}\n", .{ index, actual, expected });
        };
    }
    const published_bytes = try native_client.build(a, std.testing.io, pending, .{}, try store.executionDescriptor(), .{}, .none, clock, lease, null, .{}, .{}, &cause);
    defer a.free(published_bytes);
    try std.testing.expect(cause == null);
    var built = try catalog.parse(a, published_bytes);
    defer built.deinit();
    try std.testing.expect(built.value.published != null);
    var inventory_arena = std.heap.ArenaAllocator.init(a);
    defer inventory_arena.deinit();
    const ready = built.value.published.?.directory.?;
    const inventory = try @import("lake_index_directory.zig").loadDocument(inventory_arena.allocator(), store.artifactStore(), .{ .kind = .external_base_source, .artifact_id = ready.artifact_id, .checksum = ready.checksum, .byte_len = ready.byte_len }, .none, null);
    inline for (.{ .text_segment, .algebraic_segment, .ordered_row_index, .vector_segment, .sparse_segment }) |kind| {
        var found = false;
        for (inventory.declarations) |declaration| if (declaration.artifact.kind == kind) {
            found = true;
        };
        try std.testing.expect(found);
    }

    for (inventory.declarations) |declaration| if (declaration.artifact.kind == .vector_segment) {
        const native = @import("lake_index_native_dense.zig");
        const native_files = @import("lake_index_native_files.zig");
        const root = try native.loadRoot(inventory_arena.allocator(), store.artifactStore(), declaration.artifact, .none, null);
        var reader: native_files.Reader = .{ .root = root.generation, .prefix = "/published-bulk", .store = store.artifactStore(), .context = .{}, .scratch = a };
        defer reader.deinit();
        var vectors = try local.storage_lsm_backend.Backend.open(a, "/published-bulk/vectors", .{ .storage = reader.storage(), .backend = .{ .read_only = true, .create_if_missing = false } });
        defer vectors.close();
        var loader: native.VectorLoader = .{ .backend = &vectors, .dims = 2 };
        var hbc_config: local.storage_hbc_adapter.HBCConfig = .{ .dims = 2 };
        try native.applyStoredQuantization(a, &hbc_config, root);
        try std.testing.expectEqual(@as(u8, if (typed_vectors) 4 else 1), hbc_config.quantization_bits);
        var index = try local.storage_hbc_adapter.HBCIndex.openWithLsmOptions(a, "/published-bulk", hbc_config, .{ .storage = reader.storage(), .backend_options = .{ .backend = .{ .read_only = true, .create_if_missing = false } } });
        defer index.close();
        index.setIo(std.testing.io);
        index.setExternalVectorLoader(&loader, native.VectorLoader.load);
        try index.activateExperimentalPostingReads(0);
        try std.testing.expectEqual(@as(u64, bodies.len * 2), index.stats().active_count);
        try std.testing.expect((try index.verifyTreeLinks()).consistent());
        var hits = try index.search(&.{ 100, 1 }, 2);
        defer hits.deinit();
        try std.testing.expectEqual(@as(usize, 2), hits.items.items.len);
    };

    try std.testing.expect(authority.checkpoints != 0);
    var control = try catalog.parse(a, authority.table.lake_index_catalog_json);
    defer control.deinit();
    // The execution owner returns candidate bytes; it never publishes them.
    try std.testing.expect(control.value.published == null);
    try std.testing.expect(control.value.pending != null);
    try std.testing.expect(control.value.checkpoint != null);
    // Reopening the operation resumes durable attempt-private checkpoints.
    const resumed_bytes = try native_client.build(a, std.testing.io, authority.table, .{}, try store.executionDescriptor(), .{}, .none, clock, lease, null, .{}, .{}, &cause);
    defer a.free(resumed_bytes);
    var resumed = try catalog.parse(a, resumed_bytes);
    defer resumed.deinit();
    try expectCandidate(a, built.value.published.?, inventory.declarations, resumed.value.published.?, store.artifactStore());
    // The physical owner handles derived-read provenance and one bounded
    // rebuild. An ambiguous durable checkpoint discard is never replayed.
    var resumed_control = try catalog.parse(a, authority.table.lake_index_catalog_json);
    defer resumed_control.deinit();
    const checkpoint_directory = resumed_control.value.checkpoint.?.directory.?;
    var checkpoint_artifacts = store.artifactStore();
    checkpoint_artifacts.upload_scope = try @import("../serverless/artifacts/store.zig").uploadScopeFromArtifactId(checkpoint_directory.artifact_id);
    try checkpoint_artifacts.delete(checkpoint_directory.artifact_id);
    authority.ambiguous_discard = true;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, native_client.build(a, std.testing.io, authority.table, .{}, try store.executionDescriptor(), .{}, .none, clock, lease, null, .{}, .{}, &cause));
    try std.testing.expectEqual(@as(usize, 1), authority.discards);
    var discarded = try catalog.parse(a, authority.table.lake_index_catalog_json);
    defer discarded.deinit();
    try std.testing.expect(discarded.value.checkpoint == null);
    try std.testing.expect(discarded.value.pending != null);
    authority.ambiguous_discard = false;
    const recovered_bytes = try native_client.build(a, std.testing.io, authority.table, .{}, try store.executionDescriptor(), .{}, .none, clock, lease, null, .{}, .{}, &cause);
    defer a.free(recovered_bytes);
    var recovered = try catalog.parse(a, recovered_bytes);
    defer recovered.deinit();
    try expectCandidate(a, built.value.published.?, inventory.declarations, recovered.value.published.?, store.artifactStore());
    try std.testing.expectEqual(@as(usize, 1), authority.discards);

    try std.testing.expectError(error.LakeSidecarBuildLimitExceeded, native_client.build(a, std.testing.io, pending, .{}, try store.executionDescriptor(), .{}, .none, clock, lease, null, .{}, .{ .max_working_set_bytes = 1 }, &cause));
    authority.exhaust_snapshot_budget = true;
    try std.testing.expectError(error.LakeSidecarBuildLimitExceeded, native_client.build(a, std.testing.io, authority.table, .{}, try store.executionDescriptor(), .{}, .none, clock, lease, null, .{}, .{}, &cause));
    authority.exhaust_snapshot_budget = false;
    authority.clock = 1100;
    try std.testing.expectError(error.LakeIndexPublicationFenceChanged, native_client.build(a, std.testing.io, pending, .{}, try store.executionDescriptor(), .{}, .none, clock, lease, null, .{}, .{}, &cause));
    authority.clock = 101;
    authority.expire_after_checkpoint = true;
    try std.testing.expectError(error.LakeIndexPublicationFenceChanged, native_client.build(a, std.testing.io, pending, .{}, try store.executionDescriptor(), .{}, .none, clock, lease, null, .{}, .{}, &cause));
    authority.expire_after_checkpoint = false;
    authority.clock = 101;
    authority.rejected = true;
    try std.testing.expectError(error.CallerOnlyLakeOperationCanceled, native_client.build(a, std.testing.io, pending, .{}, try store.executionDescriptor(), .{ .checkpoint = .{ .ptr = &authority, .check = Authority.check } }, .none, clock, lease, null, .{}, .{}, &cause));
    try std.testing.expectError(error.CallerOnlyLakeCheckpointRejected, native_client.build(a, std.testing.io, pending, .{}, try store.executionDescriptor(), .{}, .none, clock, lease, null, .{}, .{}, &cause));
    authority.rejected = false;
    const retried_bytes = try native_client.build(a, std.testing.io, authority.table, .{}, try store.executionDescriptor(), .{}, .none, clock, lease, null, .{}, .{}, &cause);
    defer a.free(retried_bytes);
    var retried = try catalog.parse(a, retried_bytes);
    defer retried.deinit();
    try expectCandidate(a, built.value.published.?, inventory.declarations, retried.value.published.?, store.artifactStore());
    var changed_store = try store.executionDescriptor();
    changed_store.identity[0] ^= 1;
    try std.testing.expectError(error.ExternalLakeIndexStoreChanged, native_client.build(a, std.testing.io, pending, .{}, changed_store, .{}, .none, clock, lease, null, .{}, .{}, &cause));
    const added_data = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{}, &.{
        .{ .column_id = "body", .field_id = 1, .converted_type = 0, .values = &bodies },
        .{ .column_id = "dense", .field_id = 2, .converted_type = 0, .values = &dense },
        .{ .column_id = "sparse", .field_id = 3, .converted_type = 0, .values = &sparse },
    });
    defer a.free(added_data);
    var added = try client.putObject("antfly", "part2.parquet", added_data, .{});
    added.deinit(a);
    // A fresh owner must validate the source against the admitted signature,
    // including changes hidden from the API's already-open reader inventory.
    try std.testing.expectError(error.LakeIndexPublicationFenceChanged, native_client.build(a, std.testing.io, pending, .{}, try store.executionDescriptor(), .{}, .none, clock, lease, null, .{}, .{}, &cause));
}

test "external lake execution rejects incompatible contracts before interpreting resources" {
    const Stub = struct {
        fn invoke(_: *const c.native.CallContract, _: *const anyopaque, _: *anyopaque) callconv(.c) void {
            @panic("invalid contract must never invoke");
        }
    };
    var endpoint: c.Endpoint = .{ .contract = c.signature(), .invoke = Stub.invoke };
    try c.validate(&endpoint);
    endpoint.version += 1;
    try std.testing.expectError(error.InvalidArgument, c.validate(&endpoint));
    endpoint.version -= 1;
    var changed_layouts = c.layouts;
    changed_layouts[0].size += 1;
    endpoint.layout = &changed_layouts;
    try std.testing.expectError(error.InvalidArgument, c.validate(&endpoint));
}

test "external lake execution checks endpoint version and count before layout dereference" {
    const Stub = struct {
        fn invoke(_: *const c.native.CallContract, _: *const anyopaque, _: *anyopaque) callconv(.c) void {
            @panic("stale layout must never invoke");
        }
    };
    var endpoint: c.Endpoint = .{ .contract = c.signature(), .layout_count = 0, .layout = @ptrFromInt(@alignOf(c.native.TypeContract)), .invoke = Stub.invoke };
    try std.testing.expectError(error.InvalidArgument, c.validate(&endpoint));
    endpoint.layout_count = c.layouts.len;
    endpoint.version = c.endpoint_version - 1;
    try std.testing.expectError(error.InvalidArgument, c.validate(&endpoint));
}

test "external lake execution guards nested cancellation and progress callback layouts" {
    const Stub = struct {
        fn invoke(_: *const c.native.CallContract, _: *const anyopaque, _: *anyopaque) callconv(.c) void {
            @panic("incompatible cancellation must never invoke");
        }
    };
    const execution = @import("antfly_inference_execution_context");
    inline for (.{ @import("antfly_cancellation").CancellationToken, execution.ProgressSink, execution.Progress, execution.Phase }) |T| {
        const expected = c.native.TypeContract.of(T);
        var changed = c.layouts;
        var found = false;
        for (&changed) |*layout| if (layout.type_id == expected.type_id) {
            layout.layout_id ^= 1;
            found = true;
        };
        try std.testing.expect(found);
        const endpoint: c.Endpoint = .{ .contract = c.signature(), .layout = &changed, .invoke = Stub.invoke };
        try std.testing.expectError(error.InvalidArgument, c.validate(&endpoint));
    }
}

test "external lake execution source fixtures ignore product execution selection" {
    const options = @import("lake_execution_options");
    try std.testing.expect(!options.shared_lake_execution);
    try std.testing.expect(!options.provide_lake_execution);
}

test "external lake execution native directories admit paged generations above sixteen GiB" {
    const files = @import("lake_index_native_files.zig");
    const tree = @import("../serverless/graph_segment/page_tree.zig");
    const ref: tree.Ref = .{ .digest = @splat(1), .bytes = 128, .height = 2, .records = 131072 };
    const file: files.File = .{ .path = "posting-segments/segment-1.afps", .bytes = @as(u64, files.chunk_bytes) * ref.records, .chunk_root = ref };
    try file.validate(@splat(1));
    const root: files.Root = .{ .version = 2, .domain = @splat(1), .directory = .{ .digest = @splat(1), .bytes = 128, .height = 0, .records = 1 }, .file_count = 1, .chunk_count = ref.records };
    try root.validate();
}

test "external lake execution dense quantization reconstructs defaults and rejects invalid recipes" {
    const dense = @import("lake_index_native_dense.zig");
    var config: local.storage_hbc_adapter.HBCConfig = .{ .dims = 768 };
    try dense.applyQuantizationJson(std.testing.allocator, &config, "{}");
    try std.testing.expectEqual(@as(u8, 1), config.quantization_bits);
    try std.testing.expect(!config.use_random_ortho_trans);
    try dense.applyQuantizationJson(std.testing.allocator, &config, "{\"quantization_bits\":4,\"quantization_refine\":\"all\"}");
    try std.testing.expectEqual(@as(u8, 4), config.quantization_bits);
    try std.testing.expect(config.use_random_ortho_trans);
    try std.testing.expectEqual(.all, config.quantization_refine);
    for ([_][]const u8{ "{\"quantization_bits\":0}", "{\"quantization_bits\":9}", "{\"quantization_bits\":true}", "{\"quantization_refine\":\"invalid\"}", "{\"use_random_ortho_trans\":1}" }) |json| {
        try std.testing.expectError(error.InvalidIndexConfig, dense.applyQuantizationJson(std.testing.allocator, &config, json));
    }
}

test "external lake execution legacy quantization reopens actual codec and rebuilds changed options" {
    const dense = @import("lake_index_native_dense.zig");
    var config: local.storage_hbc_adapter.HBCConfig = .{ .dims = 768 };
    const root: dense.Root = .{ .version = 4, .binding = undefined, .config_json = "{\"quantization_bits\":4,\"quantization_refine\":\"all\"}", .dims = 768, .metric = "l2_squared", .generation = undefined };
    try dense.applyStoredQuantization(std.testing.allocator, &config, root);
    try std.testing.expectEqual(@as(u8, 1), config.quantization_bits);
    try std.testing.expect(!config.use_random_ortho_trans);
    try std.testing.expect(dense.canReuseQuantization(4, config));
    try dense.applyQuantizationJson(std.testing.allocator, &config, root.config_json);
    try std.testing.expect(!dense.canReuseQuantization(4, config));
    try std.testing.expect(dense.canReuseQuantization(5, config));
}
