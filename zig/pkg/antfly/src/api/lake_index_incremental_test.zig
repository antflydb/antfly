// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
const local = @import("antfly_local_sources");
const publication = @import("lake_index_publication.zig");
const catalog = local.metadata_lake_index_catalog;
const serving = local.serverless_query_lake_serving;

test "external lake incremental native publication appends replaces removes and reintroduces physical files" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-incremental-publication");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const first = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{}, &.{
        .{ .column_id = "body", .field_id = 1, .converted_type = 0, .values = &.{"needle"} },
        .{ .column_id = "dense", .field_id = 2, .converted_type = 0, .values = &.{"[1,0]"} },
        .{ .column_id = "sparse", .field_id = 3, .converted_type = 0, .values = &.{"{\"1\":2}"} },
    });
    defer a.free(first);
    const replacement = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{}, &.{
        .{ .column_id = "body", .field_id = 1, .converted_type = 0, .values = &.{"replacement needle"} },
        .{ .column_id = "dense", .field_id = 2, .converted_type = 0, .values = &.{"[0,1]"} },
        .{ .column_id = "sparse", .field_id = 3, .converted_type = 0, .values = &.{"{\"2\":3}"} },
    });
    defer a.free(replacement);
    var uploaded = try client.putObject("antfly", "part.parquet", first, .{});
    uploaded.deinit(a);
    const schema_json = try std.fmt.allocPrint(a,
        \\{{"version":1,"storage_mode":"relational","default_type":"row","base_source":{{"kind":"external","table_id":"lake","format":"parquet","uri":"file://{s}","schema_fingerprint":"schema"}},"document_schemas":{{"row":{{"schema":{{"type":"object","properties":{{"body":{{"type":"string"}},"dense":{{"type":"string"}},"sparse":{{"type":"string"}}}},"additionalProperties":false}}}}}}}}
    , .{directory.path()});
    defer a.free(schema_json);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema_json)).?;
    defer binding.deinit(a);
    const artifact_path = try std.fs.path.join(a, &.{ directory.path(), "artifacts" });
    defer a.free(artifact_path);
    var artifact_fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, artifact_path);
    defer artifact_fs.deinit();
    var store = artifact_fs.artifactStore();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const Clock = struct {
        fn now(_: *const anyopaque) !u64 {
            return 101;
        }
    };
    var clock: u8 = 0;
    var table: local.common_topology_records.TableRecord = .{ .table_id = 7, .name = "lake", .schema_json = schema_json, .indexes_json = "{\"body_text\":{\"type\":\"full_text\",\"field\":\"body\",\"store_source\":true},\"all_text\":{\"type\":\"full_text\"},\"dense\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2},\"sparse\":{\"type\":\"embeddings\",\"external\":true,\"sparse\":true}}" };
    var retained: std.StringHashMapUnmanaged(void) = .empty;
    defer retained.deinit(a);
    var read_cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer read_cache.deinit();
    var corpora: @import("lake_index_native_text_cache.zig").Cache = .{};
    defer corpora.deinit();
    var constrained_corpora: @import("lake_index_native_text_cache.zig").Cache = .{ .max_entries = 1 };
    defer constrained_corpora.deinit();
    var prior_snapshot: ?*local.index.IndexSnapshot = null;
    defer if (prior_snapshot) |snapshot| snapshot.release();
    for (0..5) |phase| {
        switch (phase) {
            1, 4 => {
                var result = try client.putObject("antfly", if (phase == 1) "part2.parquet" else "part.parquet", first, .{});
                result.deinit(a);
            },
            2 => {
                var result = try client.putObject("antfly", "part2.parquet", replacement, .{});
                result.deinit(a);
            },
            3 => try client.deleteObject("antfly", "part.parquet", .{}),
            else => {},
        }
        var source = try serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
        defer source.deinit();
        table.lake_index_catalog_json = try publication.begin(ca, std.testing.io, table, &source, @splat(7), .{}, 100, 20);
        table.lake_index_catalog_json = try publication.build(ca, &store, table, &source, @splat(7), .{ .io = std.testing.io }, .none, .{ .ptr = &clock, .now_ms = Clock.now });
        var parsed = try catalog.parse(a, table.lake_index_catalog_json);
        defer parsed.deinit();
        try @import("lake_index_directory.zig").hydrate(parsed.arena.allocator(), store, &parsed.value.published.?, .none, null);
        const expected: u64 = if (phase == 0 or phase == 3) 1 else 2;
        var reused: usize = 0;
        for (parsed.value.published.?.declarations) |declaration| switch (declaration.artifact.kind) {
            .text_segment => {
                const native = @import("lake_index_native_text.zig");
                const root = try native.loadRoot(ca, store, declaration.artifact, .none, null);
                try std.testing.expectEqual(expected, root.file_groups.len);
                var writer = try native.loadWriter(a, store, root, .none, null, null);
                defer writer.deinit();
                const snapshot = writer.acquireSnapshot();
                defer snapshot.release();
                try std.testing.expectEqual(expected, snapshot.liveDocCount());
                const doc = (try snapshot.storedDocDecompressed(a, 0)).?;
                defer a.free(doc.data);
                var stored = try std.json.parseFromSlice(std.json.Value, a, doc.data, .{});
                defer stored.deinit();
                if (std.mem.eql(u8, declaration.name, "body_text")) {
                    try std.testing.expect(stored.value.object.contains("body"));
                    try std.testing.expect(!stored.value.object.contains("dense"));
                    try std.testing.expect(!stored.value.object.contains("sparse"));
                } else try std.testing.expectEqual(@as(usize, 0), stored.value.object.count());
                var pooled = try corpora.acquire(std.testing.io, store, declaration.artifact, root, schema_json, .{ .cache = &read_cache, .scope = @splat(1), .context = .{} }, .{}, .none);
                defer pooled.deinit();
                try std.testing.expectEqual(expected, pooled.snapshot.liveDocCount());
                var warm = try corpora.acquire(std.testing.io, store, declaration.artifact, root, schema_json, .{ .cache = &read_cache, .scope = @splat(1), .context = .{} }, .{}, .none);
                defer warm.deinit();
                try std.testing.expect(warm.snapshot != pooled.snapshot);
                try std.testing.expect(pooled.provider_metadata != null);
                try std.testing.expect(warm.provider_metadata == pooled.provider_metadata);
                const identities: *const @import("lake_index_text_predicate.zig").Identities = @ptrCast(@alignCast(pooled.provider_metadata.?));
                try std.testing.expectEqual(expected, identities.files.count());
                for (warm.snapshot.segments, pooled.snapshot.segments) |left, right| try std.testing.expect(left.shared == right.shared);
                if (std.mem.eql(u8, declaration.name, "all_text")) {
                    var encoded_bytes = declaration.artifact.byte_len;
                    for (root.segments) |segment| encoded_bytes += segment.byte_len;
                    constrained_corpora.max_bytes = encoded_bytes;
                    var constrained = try constrained_corpora.acquire(std.testing.io, store, declaration.artifact, root, schema_json, .{ .cache = &read_cache, .scope = @splat(1), .context = .{} }, .{}, .none);
                    defer constrained.deinit();
                    try std.testing.expectEqual(expected, constrained.snapshot.liveDocCount());
                    try std.testing.expectEqual(@as(usize, 1), constrained_corpora.entries.count());
                    if (phase == 0 and root.seekable) {
                        var seekable_cache: @import("lake_index_native_text_cache.zig").Cache = .{ .max_bytes = 1, .max_seekable_bytes = encoded_bytes };
                        defer seekable_cache.deinit();
                        var seekable_pin = try seekable_cache.acquire(std.testing.io, store, declaration.artifact, root, schema_json, .{ .cache = &read_cache, .scope = @splat(1), .context = .{} }, .{}, .none);
                        defer seekable_pin.deinit();
                        try std.testing.expectEqual(expected, seekable_pin.snapshot.liveDocCount());
                        try std.testing.expectEqual(encoded_bytes, seekable_cache.bytes);
                    }
                    if (phase == 0) {
                        // Two active publication roots sharing all segments
                        // must fit in one corpus reservation plus two roots.
                        var shared_cache: @import("lake_index_native_text_cache.zig").Cache = .{ .max_bytes = encoded_bytes + declaration.artifact.byte_len };
                        defer shared_cache.deinit();
                        var one = try shared_cache.acquire(std.testing.io, store, declaration.artifact, root, schema_json, .{ .cache = &read_cache, .scope = @splat(1), .context = .{} }, .{}, .none);
                        defer one.deinit();
                        const scope = try @import("../serverless/artifacts/store.zig").UploadScope.forPublication(root.domain, 42, std.testing.io);
                        const root_bytes = try store.getAlloc(declaration.artifact.artifact_id);
                        defer a.free(root_bytes);
                        var alias = try store.putScoped(scope, root_bytes, .none);
                        defer alias.deinit(a);
                        var ref = declaration.artifact;
                        ref.artifact_id = alias.artifact_id;
                        ref.checksum = alias.checksum;
                        var two = try shared_cache.acquire(std.testing.io, store, ref, root, schema_json, .{ .cache = &read_cache, .scope = @splat(1), .context = .{} }, .{}, .none);
                        defer two.deinit();
                        try std.testing.expectEqual(@as(usize, 2), shared_cache.entries.count());
                        try std.testing.expectEqual(shared_cache.max_bytes, shared_cache.bytes);
                        for (one.snapshot.segments, two.snapshot.segments) |left, right| try std.testing.expect(left.shared == right.shared);
                        prior_snapshot = pooled.snapshot.retain();
                    }
                    if (phase == 1) {
                        var shared: usize = 0;
                        for (pooled.snapshot.segments) |current| for (prior_snapshot.?.segments) |old| {
                            if (current.shared == old.shared) {
                                shared += 1;
                            }
                        };
                        try std.testing.expectEqual(@as(usize, 1), shared);
                    }
                }

                for (root.segments) |segment| {
                    if (phase == 0) try retained.put(a, segment.artifact_id, {});
                    if (phase == 1 and retained.contains(segment.artifact_id)) reused += 1;
                }
            },
            .sparse_segment, .vector_segment => {
                const states = if (declaration.artifact.kind == .vector_segment)
                    (try @import("lake_index_native_dense.zig").loadRoot(ca, store, declaration.artifact, .none, null)).file_states
                else
                    (try @import("lake_index_native_sparse.zig").loadRoot(ca, store, declaration.artifact, .none, null)).file_states;
                const generation = if (declaration.artifact.kind == .vector_segment)
                    (try @import("lake_index_native_dense.zig").loadRoot(ca, store, declaration.artifact, .none, null)).generation
                else
                    (try @import("lake_index_native_sparse.zig").loadRoot(ca, store, declaration.artifact, .none, null)).generation;
                var reused_chunks: usize = 0;
                for (generation.files) |file| for (file.chunks) |ref| {
                    if (phase == 0) try retained.put(a, ref.artifact_id, {});
                    if (phase == 1 and retained.contains(ref.artifact_id)) reused_chunks += 1;
                };
                if (phase == 1) try std.testing.expect(reused_chunks != 0);
                var count: u64 = 0;
                for (states) |file| {
                    count += file.count;
                    for (file.docs) |ref| {
                        if (phase == 0) try retained.put(a, ref.artifact_id, {});
                        if (phase == 1 and retained.contains(ref.artifact_id)) reused += 1;
                    }
                }
                try std.testing.expectEqual(expected, count);
            },
            else => return error.TestUnexpectedResult,
        };
        if (phase == 1) try std.testing.expectEqual(@as(usize, 4), reused);
    }
}

test "external lake managed chunks publish independent vectors and replace their complete source membership" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("native-chunk-publication");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const first = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{}, &.{
        .{ .column_id = "body", .field_id = 1, .converted_type = 0, .values = &.{"alpha beta gamma delta"} },
        .{ .column_id = "dense", .field_id = 2, .converted_type = 0, .values = &.{"[1,0]"} },
        .{ .column_id = "sparse", .field_id = 3, .converted_type = 0, .values = &.{"{\"1\":2}"} },
    });
    defer a.free(first);
    const replacement = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{}, &.{
        .{ .column_id = "body", .field_id = 1, .converted_type = 0, .values = &.{"replacement epsilon"} },
        .{ .column_id = "dense", .field_id = 2, .converted_type = 0, .values = &.{"[0,1]"} },
        .{ .column_id = "sparse", .field_id = 3, .converted_type = 0, .values = &.{"{\"2\":3}"} },
    });
    defer a.free(replacement);
    var uploaded = try client.putObject("antfly", "part.parquet", first, .{});
    uploaded.deinit(a);
    const schema_json = try std.fmt.allocPrint(a,
        \\{{"version":1,"storage_mode":"relational","default_type":"row","base_source":{{"kind":"external","table_id":"lake","format":"parquet","uri":"file://{s}","schema_fingerprint":"schema"}},"document_schemas":{{"row":{{"schema":{{"type":"object","properties":{{"body":{{"type":"string"}},"dense":{{"type":"string"}},"sparse":{{"type":"string"}}}},"additionalProperties":false}}}}}}}}
    , .{directory.path()});
    defer a.free(schema_json);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema_json)).?;
    defer binding.deinit(a);
    const artifact_path = try std.fs.path.join(a, &.{ directory.path(), "artifacts" });
    defer a.free(artifact_path);
    var artifact_fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, artifact_path);
    defer artifact_fs.deinit();
    var store = artifact_fs.artifactStore();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const Clock = struct {
        fn now(_: *const anyopaque) !u64 {
            return 101;
        }
    };
    var clock: u8 = 0;
    var table: local.common_topology_records.TableRecord = .{ .table_id = 7, .name = "lake", .schema_json = schema_json, .indexes_json =
        \\{"semantic":{"type":"embeddings","field":"body","dimension":2,"embedder":{"provider":"antfly","model":"local-model"},"template":"{{body}} {{dense}}","chunker":{"provider":"mock","text":{"target_tokens":1,"overlap_tokens":0}}},"sparse_chunks":{"type":"embeddings","field":"body","sparse":true,"embedder":{"provider":"antfly","model":"local-model","sparse":true},"chunker":{"provider":"mock","text":{"target_tokens":1,"overlap_tokens":0}}}}
    };
    const Provider = struct {
        calls: usize = 0,
        sparse_calls: usize = 0,
        fn dense(raw: *anyopaque, alloc: std.mem.Allocator, _: []const u8, texts: []const []const u8) ![][]f32 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += texts.len;
            const vectors = try alloc.alloc([]f32, texts.len);
            for (vectors) |*vector| vector.* = try alloc.dupe(f32, &.{ 1, 0 });
            return vectors;
        }
        fn controlled(raw: *anyopaque, alloc: std.mem.Allocator, model: []const u8, texts: []const []const u8, context: local.inference_managed_embedder.EmbeddingRequestContext) ![][]f32 {
            try context.check();
            return dense(raw, alloc, model, texts);
        }
        fn sparse(raw: *anyopaque, alloc: std.mem.Allocator, _: []const u8, texts: []const []const u8) ![]local.storage_db_enrichment_embedder.SparseEmbedding {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.sparse_calls += texts.len;
            const vectors = try alloc.alloc(local.storage_db_enrichment_embedder.SparseEmbedding, texts.len);
            for (vectors) |*vector| vector.* = .{ .indices = try alloc.dupe(u32, &.{1}), .values = try alloc.dupe(f32, &.{2}) };
            return vectors;
        }
        fn sparseControlled(raw: *anyopaque, alloc: std.mem.Allocator, model: []const u8, texts: []const []const u8, context: local.inference_managed_embedder.EmbeddingRequestContext) ![]local.storage_db_enrichment_embedder.SparseEmbedding {
            try context.check();
            return sparse(raw, alloc, model, texts);
        }
    };
    var provider: Provider = .{};
    const options: local.inference_managed_embedder.InitOptions = .{ .io = std.testing.io, .antfly_provider = .{ .ptr = &provider, .embed_dense_texts = Provider.dense, .embed_dense_texts_with_context = Provider.controlled, .embed_sparse_texts = Provider.sparse, .embed_sparse_texts_with_context = Provider.sparseControlled } };
    for (0..2) |phase| {
        if (phase == 1) {
            var result = try client.putObject("antfly", "part.parquet", replacement, .{});
            result.deinit(a);
        }
        var source = try serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
        defer source.deinit();
        table.lake_index_catalog_json = try publication.begin(ca, std.testing.io, table, &source, @splat(7), .{}, 100, 20);
        const before = provider.calls;
        const sparse_before = provider.sparse_calls;
        table.lake_index_catalog_json = try publication.buildWithLeaseAndEmbedding(ca, &store, table, &source, @splat(7), .{ .io = std.testing.io }, .none, .{ .ptr = &clock, .now_ms = Clock.now }, null, options, null);
        var parsed = try catalog.parse(a, table.lake_index_catalog_json);
        defer parsed.deinit();
        try @import("lake_index_directory.zig").hydrate(parsed.arena.allocator(), store, &parsed.value.published.?, .none, null);
        const declarations = parsed.value.published.?.declarations;
        try std.testing.expectEqual(@as(usize, 2), declarations.len);
        for (declarations) |declaration| {
            const states = if (declaration.artifact.kind == .vector_segment)
                (try @import("lake_index_native_dense.zig").loadRoot(ca, store, declaration.artifact, .none, null)).file_states
            else
                (try @import("lake_index_native_sparse.zig").loadRoot(ca, store, declaration.artifact, .none, null)).file_states;
            try std.testing.expectEqual(@as(usize, 1), states.len);
            try std.testing.expectEqual(@as(u16, 108), states[0].key_width);
            const count = states[0].count;
            try std.testing.expect(count > 1);
            try std.testing.expectEqual(count, if (declaration.artifact.kind == .vector_segment) provider.calls - before else provider.sparse_calls - sparse_before);
        }
    }
}
