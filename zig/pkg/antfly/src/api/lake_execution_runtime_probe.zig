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

//! Bounded source/native whole-attempt probe; semantic checks precede metrics.
const std = @import("std");
const local = @import("antfly_local_sources");
const support = @import("lake_execution_qualification_support.zig");
const publication = @import("lake_index_publication.zig");
const catalog = local.metadata_lake_index_catalog;
const Store = @import("lake_index_store.zig").Store;
const A = std.mem.Allocator;
const Mode = enum { source, native };
const metrics = @import("lake_execution_probe_metrics.zig");
fn familyTimes(mode: Mode, counts: bool) [metrics.names.len]u64 {
    if (mode == .source) return if (counts) @import("root").families.counts() else @import("root").families.times();
    var result: [metrics.names.len]u64 = undefined;
    for (&result, 0..) |*value, index| value.* = if (counts) @extern(*const fn (u8) callconv(.c) u64, .{ .name = "antfly_lake_probe_family_calls" })(@intCast(index)) else @extern(*const fn (u8) callconv(.c) u64, .{ .name = "antfly_lake_probe_family_ns" })(@intCast(index));
    return result;
}
fn count(comptime name: [:0]const u8, fallback: usize, maximum: usize) !usize {
    const raw = std.c.getenv(name) orelse return fallback;
    const value = try std.fmt.parseInt(usize, std.mem.span(raw), 10);
    if (value == 0 or value > maximum) return error.InvalidProbeWorkload;
    return value;
}
fn bulkPhaseTimes(counts: bool) [metrics.names.len + metrics.bulk_names.len]u64 {
    var result: [metrics.names.len + metrics.bulk_names.len]u64 = undefined;
    const base = familyTimes(.native, counts);
    @memcpy(result[0..base.len], &base);
    for (result[base.len..], 0..) |*value, index| value.* = if (counts)
        @extern(*const fn (u8) callconv(.c) u64, .{ .name = "antfly_lake_probe_bulk_calls" })(@intCast(index))
    else
        @extern(*const fn (u8) callconv(.c) u64, .{ .name = "antfly_lake_probe_bulk_ns" })(@intCast(index));
    return result;
}

fn writeFile(a: A, client: *local.storage_object_storage.ObjectStorage, ordinal: usize, rows: usize) !void {
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const sa = scratch.allocator();
    const body = try sa.alloc([]const u8, rows);
    const dense = try sa.alloc([]const u8, rows);
    const sparse = try sa.alloc([]const u8, rows);
    for (body, dense, sparse, 0..) |*text, *vector, *features, index| {
        text.* = try std.fmt.allocPrint(sa, "needle document file-{d} row-{d}", .{ ordinal, index });
        vector.* = "[1,0]";
        features.* = "{\"1\":2}";
    }
    const bytes = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{}, &.{
        .{ .column_id = "body", .field_id = 1, .converted_type = 0, .values = body },
        .{ .column_id = "dense", .field_id = 2, .converted_type = 0, .values = dense },
        .{ .column_id = "sparse", .field_id = 3, .converted_type = 0, .values = sparse },
    });
    defer a.free(bytes);
    const key = try std.fmt.allocPrint(a, "part-{d}.parquet", .{ordinal});
    defer a.free(key);
    var result = try client.putObject("antfly", key, bytes, .{});
    result.deinit(a);
}

pub fn run(io: std.Io) !void {
    comptime std.debug.assert(!@import("builtin").is_test);
    if (std.c.getenv("ANTFLY_LAKE_BULK_PARQUET")) |uri| return runParquet(io, std.mem.span(uri));
    if (std.c.getenv("ANTFLY_LAKE_BULK_PARQUET_FILE") != null) return runParquet(io, "");
    const provider_mode = @extern(*const fn () callconv(.c) u8, .{ .name = "antfly_lake_probe_builtin_is_test" });
    const provider_is_test = provider_mode() != 0;
    try std.testing.expect(!provider_is_test);
    // Keep allocation stack-trace capture out of the timing. Real quota
    // owners still assert that every charged allocation is released.
    const a = std.heap.smp_allocator;
    const single_quota = support.enabled("ANTFLY_LAKE_PROBE_SINGLE_QUOTA");
    const rows = try count("ANTFLY_LAKE_PROBE_ROWS", 128, 4096);
    const rounds = try count("ANTFLY_LAKE_PROBE_ROUNDS", 1, 8);
    for (0..rounds) |round| {
        // Own one unique fixture beneath this worktree's existing Zig cache.
        var nonce: [16]u8 = undefined;
        io.random(&nonce);
        const relative = try std.fmt.allocPrint(a, ".zig-cache/tmp/lake-runtime-{s}", .{std.fmt.bytesToHex(nonce, .lower)});
        defer a.free(relative);
        const cwd = std.Io.Dir.cwd();
        try cwd.createDirPath(io, ".zig-cache/tmp");
        try cwd.createDir(io, relative, .default_dir);
        defer cwd.deleteTree(io, relative) catch {};
        var fixture = try cwd.openDir(io, relative, .{});
        defer fixture.close(io);
        var absolute: [std.fs.max_path_bytes]u8 = undefined;
        const absolute_len = try fixture.realPath(io, &absolute);
        const directory = try a.dupe(u8, absolute[0..absolute_len]);
        defer a.free(directory);
        const input = try std.fs.path.join(a, &.{ directory, "input" });
        defer a.free(input);
        var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, input);
        defer fs.deinit();
        var client = fs.client();
        try client.makeBucket("antfly");
        for (0..4) |file| try writeFile(a, &client, file, rows);
        const schema = try std.fmt.allocPrint(a,
            \\{{"version":1,"storage_mode":"relational","default_type":"row","base_source":{{"kind":"external","table_id":"probe","format":"parquet","uri":"file://{s}","schema_fingerprint":"schema","row_identity":{{"key_columns":["body"]}}}},"relational_indexes":[{{"name":"body_order","keys":[{{"column":"body"}}],"include_columns":["dense"]}}],"document_schemas":{{"row":{{"schema":{{"type":"object","properties":{{"body":{{"type":"string"}},"dense":{{"type":"string"}},"sparse":{{"type":"string"}}}},"additionalProperties":false}}}}}}}}
        , .{input});
        defer a.free(schema);
        var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema)).?;
        defer binding.deinit(a);
        var caches = [_]local.serverless_query_lake_serving_cache.Cache{ .init(a), .init(a) };
        defer for (&caches) |*cache| cache.deinit();
        var states: [2]support.State = undefined;
        var stores: [2]Store = undefined;
        var artifact_roots: [2][]u8 = undefined;
        var ids: [2]std.StringHashMapUnmanaged(void) = .{ .empty, .empty };
        for ([_]Mode{ .source, .native }, 0..) |mode, index| {
            const root = try std.fs.path.join(a, &.{ directory, @tagName(mode) });
            artifact_roots[index] = root;
            stores[index] = try Store.openNative(a, null, null, false, .standalone, root);
            states[index] = .{ .allocator = a, .io = io, .table = .{ .table_id = 17, .name = "probe", .schema_json = schema, .indexes_json = "{\"body_text\":{\"type\":\"full_text\",\"field\":\"body\",\"store_source\":true},\"stats\":{\"type\":\"algebraic\",\"materializations\":[{\"name\":\"rows\",\"op\":\"count\"}]},\"dense\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2},\"sparse\":{\"type\":\"embeddings\",\"external\":true,\"sparse\":true}}" } };
        }
        defer for (&stores, &states, &ids, &artifact_roots) |*store, *state, *previous, root| {
            store.deinit();
            a.free(root);
            state.deinit();
            var keys = previous.keyIterator();
            while (keys.next()) |key| a.free(key.*);
            previous.deinit(a);
        };
        for ([_][]const u8{ "cold", "checkpoint_resume", "incremental_append" }, 0..) |phase, phase_index| {
            if (phase_index == 2) try writeFile(a, &client, 4, rows);
            const order: [2]Mode = if (round % 2 == 0) .{ .source, .native } else .{ .native, .source };
            for (order) |mode| {
                const index = @backingInt(mode);
                const state = &states[index];
                const store = &stores[index];
                const before = caches[index].snapshot();
                const before_decoded = metrics.decodedHits(&caches[index]);
                const control_start = @import("antfly_platform").time.monotonicNs();
                const context: local.serverless_query_lake_read_context.Context = .{ .io = io };
                var source = try local.serverless_query_lake_serving.ServingSource.openCached(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{}, context, &caches[index]);
                defer source.deinit();
                try source.attachCache(&caches[index], binding.binding, context);
                const api_source_open_ns = @import("antfly_platform").time.monotonicNs() - control_start;
                if (phase_index != 1) {
                    const pending = try publication.beginWithLocator(a, io, state.table, &source, store.identity, context, 100, 1000, store.locator);
                    defer a.free(pending);
                    try state.set(pending);
                }
                const control_ns = @import("antfly_platform").time.monotonicNs() - control_start;
                var working = try @import("../serverless/build/lake_build_limits.zig").WorkingSetAllocator.init(a, .{});
                defer working.deinit();
                const wa = working.allocator();
                const checkpoint_start = state.checkpoints;
                @import("root").resetHeadroomQueries();
                @import("root").families.reset();
                @import("root").resetIoDispatchCalls();
                if (mode == .native) @extern(*const fn (u8) callconv(.c) void, .{ .name = "antfly_lake_probe_set_linear_io" })(@intFromBool(@import("root").ioDispatchIsLinear()));
                if (mode == .native) @extern(*const fn () callconv(.c) void, .{ .name = "antfly_lake_probe_reset" })();
                const started = @import("antfly_platform").time.monotonicNs();
                var cause: ?anyerror = null;
                var artifact_store = store.artifactStore();
                const result_allocator = if (mode == .native and single_quota) a else wa;
                const result = if (mode == .source)
                    try publication.buildWithReuse(wa, &artifact_store, state.table, &source, store.identity, context, .none, state.clock(), state.lease(), null, .{ .store = store, .table_id = state.table.table_id, .recipe = catalog.desiredFingerprint(state.table), .context = context }, .{})
                else
                    try @import("lake_execution_native_client.zig").build(result_allocator, io, state.table, .{}, try store.executionDescriptor(), context, .none, state.clock(), state.lease(), null, .{}, .{}, &cause);
                defer result_allocator.free(result);
                const execution_ns = @import("antfly_platform").time.monotonicNs() - started;
                try std.testing.expect(cause == null);
                var ready = try catalog.parse(a, result);
                defer ready.deinit();
                try std.testing.expect(ready.value.pending == null);
                try std.testing.expectEqual(@as(u64, if (phase_index == 2) 2 else 1), ready.value.published.?.generation);
                var scratch = std.heap.ArenaAllocator.init(a);
                defer scratch.deinit();
                try store.bindPublication(ready.value.published.?, .none);
                const ref = ready.value.published.?.directory.?;
                const document = try @import("lake_index_directory.zig").loadDocument(scratch.allocator(), store.artifactStore(), .{ .kind = .external_base_source, .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, .none, null);
                try std.testing.expectEqual(@as(usize, 5), document.declarations.len);
                var reused: usize = 0;
                var root_bytes: [metrics.names.len]u64 = @splat(0);
                for (document.declarations) |declaration| {
                    const family: usize = switch (declaration.artifact.kind) {
                        .algebraic_segment => 3,
                        .ordered_row_index => 4,
                        .text_segment => 5,
                        .sparse_segment => 6,
                        .vector_segment => 7,
                        else => return error.UnexpectedProbeArtifactKind,
                    };
                    root_bytes[family] +|= declaration.artifact.byte_len;
                    if (ids[index].contains(declaration.artifact.artifact_id)) reused += 1 else try ids[index].put(a, try a.dupe(u8, declaration.artifact.artifact_id), {});
                    const bytes = try @import("lake_index_aggregate_artifact.zig").readArtifact(a, store.artifactStore(), .{ .artifact_id = declaration.artifact.artifact_id, .checksum = declaration.artifact.checksum, .byte_len = declaration.artifact.byte_len }, .none, null);
                    a.free(bytes);
                }
                if (phase_index != 0) try state.set(result);
                const after = caches[index].snapshot();
                const observation = try std.json.Stringify.valueAlloc(a, .{ .parallel_jobs = if (mode == .native) @extern(*const fn () callconv(.c) u64, .{ .name = "antfly_lake_probe_parallel_jobs" })() else @import("root").families.parallel_jobs.load(.monotonic), .headroom_mode = @import("root").headroomMode(), .headroom_queries = @import("root").headroomQueries(), .family_names = metrics.names, .family_ns = familyTimes(mode, false), .family_calls = familyTimes(mode, true), .family_declaration_root_bytes = root_bytes, .api_decoded_hits = metrics.decodedHits(&caches[index]) - before_decoded, .provider_decoded_hits = if (mode == .native) @extern(*const fn () callconv(.c) u64, .{ .name = "antfly_lake_probe_decoded_hits" })() else 0, .io_dispatch = if (@import("root").ioDispatchIsLinear()) "linear" else "switch", .caller_io_dispatch_calls = @import("root").ioDispatchCalls(), .provider_io_dispatch_calls = if (mode == .native) @extern(*const fn () callconv(.c) u64, .{ .name = "antfly_lake_probe_io_calls" })() else 0, .source_optimize = @tagName(@import("builtin").mode), .provider_optimize = if (mode == .native) @extern(*const fn () callconv(.c) u8, .{ .name = "antfly_lake_probe_optimize" })() else 0, .source_is_test = @import("builtin").is_test, .provider_is_test = provider_is_test, .mode = @tagName(mode), .phase = phase, .round = round, .rows = rows * @as(usize, if (phase_index == 2) 5 else 4), .api_source_open_ns = api_source_open_ns, .api_admission_ns = control_ns - api_source_open_ns, .provider_store_open_ns = if (mode == .native) @extern(*const fn () callconv(.c) u64, .{ .name = "antfly_lake_probe_store_open_ns" })() else 0, .provider_source_open_ns = if (mode == .native) @extern(*const fn () callconv(.c) u64, .{ .name = "antfly_lake_probe_source_open_ns" })() else 0, .provider_cache_hits = if (mode == .native) @extern(*const fn () callconv(.c) u64, .{ .name = "antfly_lake_probe_cache_hits" })() else 0, .provider_cache_reads = if (mode == .native) @extern(*const fn () callconv(.c) u64, .{ .name = "antfly_lake_probe_cache_reads" })() else 0, .provider_cache_bytes = if (mode == .native) @extern(*const fn () callconv(.c) u64, .{ .name = "antfly_lake_probe_cache_bytes" })() else 0, .provider_physical_ns = if (mode == .native) @extern(*const fn () callconv(.c) u64, .{ .name = "antfly_lake_probe_physical_ns" })() else 0, .control_ns = control_ns, .execution_ns = execution_ns, .total_attempt_ns = control_ns + execution_ns, .working_quota_layers = @as(u8, if (mode == .native and !single_quota) 2 else 1), .provider_quota_peak_bytes = if (mode == .native) @extern(*const fn () callconv(.c) u64, .{ .name = "antfly_lake_probe_quota_peak" })() else 0, .peak_working_set_bytes = if (mode == .native and single_quota) @extern(*const fn () callconv(.c) u64, .{ .name = "antfly_lake_probe_quota_peak" })() else working.quota.peak_bytes, .candidate_bytes = result.len, .checkpoints = state.checkpoints - checkpoint_start, .reused_declaration_roots = reused, .declarations = document.declarations.len, .api_cache_hits = after.hits - before.hits, .api_cache_misses = after.misses - before.misses, .api_cache_provider_reads = after.provider_reads - before.provider_reads, .api_cache_provider_bytes = after.provider_bytes - before.provider_bytes }, .{});
                defer a.free(observation);
                std.debug.print("LAKE_RUNTIME {s}\n", .{observation});
            }
        }
    }
}

/// Exercises the real native operation against an immutable, authored Parquet
/// vector column, including identity construction and artifact publication.
fn runParquet(io: std.Io, uri: []const u8) !void {
    const a = std.heap.smp_allocator;
    const provider_test = @extern(*const fn () callconv(.c) u8, .{ .name = "antfly_lake_probe_builtin_is_test" })() != 0;
    const provider_mode = @extern(*const fn () callconv(.c) u8, .{ .name = "antfly_lake_probe_optimize" })();
    if (@import("builtin").is_test or provider_test or !std.mem.eql(u8, @tagName(@import("builtin").mode), "fast") or provider_mode != 1) return error.InvalidProbeWorkload;
    const mode_json = try std.json.Stringify.valueAlloc(a, .{ .caller = @tagName(@import("builtin").mode), .caller_value = @as(u8, 1), .provider = provider_mode, .provider_is_test = provider_test }, .{});
    defer a.free(mode_json);
    std.debug.print("PARQUET_BULK_BUILD_MODE {s}\n", .{mode_json});
    var remote_config = if (std.c.getenv("ANTFLY_LAKE_BULK_CONFIG")) |raw|
        try local.common_config.loadFromPathWithSecretsWithIo(a, io, std.mem.span(raw), null)
    else
        null;
    defer if (remote_config) |*config| config.deinit();
    const node_config: ?*const local.common_config.Config = if (remote_config) |*config| config else null;
    const source_options: @import("lake_execution_native_contract.zig").SourceOptions = .{ .node_config = node_config, .io = io };
    const dims = try count("ANTFLY_LAKE_BULK_DIMS", 768, 65536);
    const allowance = try count("ANTFLY_LAKE_BULK_INPUT_BYTES", 4 * 1024 * 1024 * 1024, std.math.maxInt(usize) / 2);
    const algorithm = if (std.c.getenv("ANTFLY_LAKE_BULK_ALGORITHM")) |raw| std.mem.span(raw) else "lloyd";
    if (!std.mem.eql(u8, algorithm, "lloyd") and !std.mem.eql(u8, algorithm, "emeans") and !std.mem.eql(u8, algorithm, "incremental")) return error.InvalidProbeWorkload;
    var nonce: [16]u8 = undefined;
    io.random(&nonce);
    const relative = try std.fmt.allocPrint(a, ".zig-cache/tmp/parquet-bulk-{s}", .{std.fmt.bytesToHex(nonce, .lower)});
    defer a.free(relative);
    const cwd = std.Io.Dir.cwd();
    try cwd.createDirPath(io, relative);
    defer cwd.deleteTree(io, relative) catch {};
    var dir = try cwd.openDir(io, relative, .{});
    defer dir.close(io);
    var absolute: [std.fs.max_path_bytes]u8 = undefined;
    const path_len = try dir.realPath(io, &absolute);
    const path = absolute[0..path_len];
    // Prepare a local raw file through the same streaming object codec used by
    // the filesystem adapter. Fixture upload is outside the operation timer.
    const source_uri = if (std.c.getenv("ANTFLY_LAKE_BULK_PARQUET_FILE")) |raw| prepare: {
        const input = try std.fmt.allocPrint(a, "{s}/input", .{path});
        defer a.free(input);
        var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, input);
        defer fs.deinit();
        var client = fs.client();
        var uploaded = try client.putFileWithIo(io, "antfly", "train.parquet", std.mem.span(raw), .{});
        uploaded.deinit(a);
        break :prepare try std.fmt.allocPrint(a, "file://{s}", .{input});
    } else try a.dupe(u8, uri);
    defer a.free(source_uri);
    const schema = try std.fmt.allocPrint(a,
        \\{{"version":1,"storage_mode":"relational","default_type":"row","base_source":{{"kind":"external","table_id":"parquet-bulk","format":"parquet","uri":"{s}"{s},"schema_fingerprint":"schema","row_identity":{{"key_columns":["id"]}}}},"document_schemas":{{"row":{{"schema":{{"type":"object","properties":{{"id":{{"type":"integer"}},"emb":{{"type":"array","items":{{"type":"number"}}}}}},"additionalProperties":false}}}}}}}}
    , .{ source_uri, if (node_config != null) ",\"credentials\":{\"ref\":\"bulk-source\"}" else "" });
    defer a.free(schema);
    const indexes = if (std.mem.eql(u8, algorithm, "incremental")) try std.fmt.allocPrint(a,
        \\{{"emb":{{"type":"embeddings","external":true,"dimension":{d},"distance_metric":"l2_squared"}}}}
    , .{dims}) else try std.fmt.allocPrint(a,
        \\{{"emb":{{"type":"embeddings","external":true,"dimension":{d},"distance_metric":"l2_squared","lake_bulk_build":"{s}","lake_bulk_build_max_input_bytes":{d}}}}}
    , .{ dims, algorithm, allowance });
    defer a.free(indexes);
    var state: support.State = .{
        .allocator = a,
        .io = io,
        .table = .{ .table_id = 17, .name = "parquet-bulk", .schema_json = schema, .indexes_json = indexes },
        .checkpoint_path = if (std.c.getenv("ANTFLY_LAKE_BULK_CATALOG")) |raw| std.mem.span(raw) else null,
        .interrupt_at_bulk_receipt = if (std.c.getenv("ANTFLY_LAKE_BULK_INTERRUPT")) |raw| try std.fmt.parseInt(usize, std.mem.span(raw), 10) else null,
    };
    defer state.deinit();
    var store = try Store.openNativeWithTransport(a, node_config, null, false, .standalone, path, .{ .io = io });
    defer store.deinit();
    const context: local.serverless_query_lake_read_context.Context = .{ .io = io };
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema)).?;
    defer binding.deinit(a);
    var source = try local.serverless_query_lake_serving.ServingSource.openWithContext(a, .{ .storage_mode = .relational, .external_base_source = binding }, source_options.lakeOptions(), context);
    defer source.deinit();
    const InputFile = struct { uri: []const u8, bytes: u64, version: []const u8, etag: []const u8 };
    const input_files = try a.alloc(InputFile, source.inventory.files.len);
    defer a.free(input_files);
    for (input_files, source.inventory.files) |*file, entry| file.* = .{ .uri = entry.object_uri, .bytes = entry.byte_len, .version = entry.version_id, .etag = entry.etag };
    const input_json = try std.json.Stringify.valueAlloc(a, .{ .files = input_files }, .{});
    defer a.free(input_json);
    // Pending publication and provider admission bind this inventory's source
    // signature. Qualification must check the full set, including vectorless files.
    std.debug.print("PARQUET_BULK_INVENTORY {s}\n", .{input_json});
    if (std.c.getenv("ANTFLY_LAKE_BULK_EXPECTED_INVENTORY")) |raw| {
        const expected_bytes = try cwd.readFileAlloc(io, std.mem.span(raw), a, .limited(16 * 1024 * 1024));
        defer a.free(expected_bytes);
        var expected = try std.json.parseFromSlice([]const InputFile, a, expected_bytes, .{});
        defer expected.deinit();
        if (expected.value.len != input_files.len) return error.ProbeSourceInventoryMismatch;
        const matched = try a.alloc(bool, expected.value.len);
        defer a.free(matched);
        @memset(matched, false);
        for (input_files) |actual| {
            const position = for (expected.value, 0..) |admitted, i| {
                if (!matched[i] and std.mem.eql(u8, actual.uri, admitted.uri)) break i;
            } else return error.ProbeSourceInventoryMismatch;
            const admitted = expected.value[position];
            if (actual.bytes != admitted.bytes) return error.ProbeSourceInventoryMismatch;
            if (actual.version.len != 0) {
                if (!std.mem.eql(u8, actual.version, admitted.version) or (actual.etag.len != 0 and !std.mem.eql(u8, actual.etag, admitted.etag))) return error.ProbeSourceInventoryMismatch;
            } else if (admitted.etag.len == 0 or !std.mem.eql(u8, actual.etag, admitted.etag)) return error.ProbeSourceInventoryMismatch;
            matched[position] = true;
        }
    }
    const pending = try publication.beginWithLocator(a, io, state.table, &source, store.identity, context, 100, 1000, store.locator);
    defer a.free(pending);
    const reopen_only = support.enabled("ANTFLY_LAKE_BULK_REOPEN_ONLY");
    if (support.enabled("ANTFLY_LAKE_BULK_RESUME") or reopen_only) {
        const saved = try cwd.readFileAlloc(io, state.checkpoint_path orelse return error.InvalidProbeWorkload, a, .limited(16 * 1024 * 1024));
        defer a.free(saved);
        try state.set(saved);
    } else try state.set(pending);
    var cause: ?anyerror = null;
    const started = @import("antfly_platform").time.monotonicNs();
    const result = if (reopen_only) try a.dupe(u8, state.table.lake_index_catalog_json) else @import("lake_execution_native_client.zig").build(a, io, state.table, source_options, try store.executionDescriptor(), context, .none, state.clock(), state.lease(), null, .{
        .identity_spill_bytes = if (std.c.getenv("ANTFLY_LAKE_BULK_IDENTITY_SPILL_BYTES")) |value| try std.fmt.parseUnsigned(u64, std.mem.span(value), 10) else 1024 * 1024 * 1024,
        .bulk_prebind_parents = if (std.c.getenv("ANTFLY_LAKE_BULK_PREBIND")) |flag| !std.mem.eql(u8, std.mem.span(flag), "0") else true,
        .bulk_compact_partitions = if (std.c.getenv("ANTFLY_LAKE_BULK_COMPACT")) |flag| !std.mem.eql(u8, std.mem.span(flag), "0") else false,
        .fuse_bulk_identity = if (std.c.getenv("ANTFLY_LAKE_BULK_FUSED")) |flag| !std.mem.eql(u8, std.mem.span(flag), "0") else true,
        .bulk_source_checkpoints = if (std.c.getenv("ANTFLY_LAKE_BULK_CHECKPOINT")) |flag| !std.mem.eql(u8, std.mem.span(flag), "0") else true,
    }, .{}, &cause) catch |err| {
        const failure = try std.json.Stringify.valueAlloc(a, .{ .error_name = @errorName(err), .cause = if (cause) |value| @errorName(value) else null, .elapsed_ns = @import("antfly_platform").time.monotonicNs() - started, .names = metrics.names ++ metrics.bulk_names, .ns = bulkPhaseTimes(false), .calls = bulkPhaseTimes(true) }, .{});
        defer a.free(failure);
        std.debug.print("PARQUET_BULK_FAILURE_PHASES {s}\n", .{failure});
        return err;
    };
    defer a.free(result);
    try state.set(result);
    const elapsed = if (reopen_only) 0 else @import("antfly_platform").time.monotonicNs() - started;
    var ready = try catalog.parse(a, result);
    defer ready.deinit();
    try store.bindPublication(ready.value.published.?, .none);
    const ref = ready.value.published.?.directory.?;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const document = try @import("lake_index_directory.zig").loadDocument(arena.allocator(), store.artifactStore(), .{ .kind = .external_base_source, .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, .none, null);
    const declaration = for (document.declarations) |declaration| {
        if (declaration.artifact.kind == .vector_segment) break declaration;
    } else return error.InvalidProbeWorkload;
    const dense = @import("lake_index_native_dense.zig");
    // Match pooled serving readers: immutable authenticated chunks have a
    // bounded memory cache plus an owned persistent disk tier. This is outside
    // construction timing and never changes the built publication.
    var serving_cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer serving_cache.deinit();
    if (node_config != null) {
        const cache_path = try std.fmt.allocPrint(a, "{s}/serving-cache", .{path});
        defer a.free(cache_path);
        try serving_cache.ensurePersistent(io, cache_path, .{ .max_total_bytes = allowance *| 2, .max_entries = @max(16384, allowance / (128 * 1024)) }, .{});
    }
    const cached: ?@import("lake_index_aggregate_artifact.zig").CachedRead = if (node_config != null) .{ .cache = &serving_cache, .scope = store.cache_identity, .context = context } else null;
    const root = try dense.loadRoot(arena.allocator(), store.artifactStore(), declaration.artifact, .none, cached);
    const files = @import("lake_index_native_files.zig");
    var reader: files.Reader = .{ .root = root.generation, .prefix = "/parquet-bulk", .store = store.artifactStore(), .context = context, .scratch = a, .cache = cached, .max_block_bytes = if (cached != null) 8 * 1024 * 1024 else 0 };
    defer reader.deinit();
    var vectors = try local.storage_lsm_backend.Backend.open(a, "/parquet-bulk/vectors", .{ .storage = reader.storage(), .backend = .{ .read_only = true, .create_if_missing = false } });
    defer vectors.close();
    var config: local.storage_hbc_adapter.HBCConfig = .{ .dims = @intCast(dims), .max_cached_nodes = 256, .max_cached_vectors = 0, .max_cached_metadata = 0 };
    try dense.applyStoredQuantization(a, &config, root);
    config.metric = std.meta.stringToEnum(@TypeOf(config.metric), root.metric) orelse return error.InvalidProbeWorkload;
    var index = try local.storage_hbc_adapter.HBCIndex.openWithLsmOptions(a, "/parquet-bulk", config, .{ .storage = reader.storage(), .backend_options = .{ .backend = .{ .read_only = true, .create_if_missing = false } } });
    defer index.close();
    index.setIo(io);
    var loader: dense.VectorLoader = .{ .backend = &vectors, .dims = @intCast(dims), .context = context };
    index.setExternalVectorLoader(&loader, dense.VectorLoader.load);
    index.setExternalVectorScratchLoader(&loader, dense.VectorLoader.loadInto);
    index.setExternalVectorBatchScratchLoader(&loader, dense.VectorLoader.loadMany);
    try index.activateExperimentalPostingReads(0);
    const query = try a.alloc(f32, dims);
    defer a.free(query);
    @memset(query, 1);
    var hits = try index.search(query, 10);
    defer hits.deinit();
    if (hits.getHits().len == 0) return error.InvalidProbeWorkload;
    std.debug.print("PARQUET_BULK algorithm={s} vectors={} dims={} total_ns={} reopened_hits={} checkpoints={} logical_identity={}\n", .{ algorithm, index.stats().active_count, dims, elapsed, hits.getHits().len, state.checkpoints, ready.value.published.?.row_identity != null });
    const phase_json = try std.json.Stringify.valueAlloc(a, .{ .names = metrics.names ++ metrics.bulk_names, .ns = bulkPhaseTimes(false), .calls = bulkPhaseTimes(true) }, .{});
    defer a.free(phase_json);
    std.debug.print("PARQUET_BULK_PHASES {s}\n", .{phase_json});
    if (std.c.getenv("ANTFLY_LAKE_BULK_RECALL_DIR")) |raw| try parquetRecall(a, io, &index, &source, store.artifactStore(), root, ready.value.published.?.row_identity, declaration.name, cached, std.mem.span(raw), dims);
    const read_stats = try std.json.Stringify.valueAlloc(a, serving_cache.snapshot(), .{});
    defer a.free(read_stats);
    std.debug.print("PARQUET_BULK_SERVING_CACHE {s}\n", .{read_stats});
}

fn parquetRecall(a: A, io: std.Io, index: *local.storage_hbc_adapter.HBCIndex, source: *local.serverless_query_lake_serving.ServingSource, store: @import("../serverless/artifacts/store.zig").ArtifactStore, root: @import("lake_index_native_dense.zig").Root, identity_ref: ?local.serverless_manifest_artifact_ref.ArtifactRef, index_name: []const u8, cached: ?@import("lake_index_aggregate_artifact.zig").CachedRead, path: []const u8, dims: usize) !void {
    // Truth uses one-based physical ordinals in lexically sorted object order.
    // Authenticated private file identities map hits back to those coordinates.
    const inventory = source.inventory;
    const native_state = @import("lake_index_native_state.zig");
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const entries = if (root.file_directory) |directory| try native_state.directoryFiles(arena.allocator(), store, directory, root.generation.domain, .none) else root.file_states;
    const identity = @import("lake_row_identity.zig");
    const tree = @import("../serverless/graph_segment/page_tree.zig");
    // Default fresh builds can index logical fragments. Never compare their
    // fragment ordinals directly with the physical Parquet oracle.
    const relocated = for (entries) |entry| {
        const physical = for (inventory.files) |file| {
            if (std.mem.eql(u8, file.file_id, entry.id)) break true;
        } else false;
        if (!physical) break true;
    } else false;
    var identity_root: ?identity.Root = if (relocated) try identity.load(arena.allocator(), store, identity_ref orelse return error.InvalidProbeWorkload, .none, cached) else null;
    if (identity_root) |*value| {
        value.indexes = value.spaceForIndex(index_name);
        value.reused = &.{};
    }
    var identity_store = store;
    var reads: u64 = 128 * 1024 * 1024;
    var writes: u64 = 0;
    var pages: @import("../serverless/graph_segment/page_store.zig").PageStore = .{
        .domain = if (identity_root) |value| value.domain else @splat(0),
        .artifacts = &identity_store,
        .remaining_read_bytes = &reads,
        .remaining_write_bytes = &writes,
    };
    var page_cache = struct {
        cached: ?@import("lake_index_aggregate_artifact.zig").CachedRead,
        fn read(raw: *anyopaque, alloc: A, artifacts: *@import("../serverless/artifacts/store.zig").ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, offset: u64, length: usize, digest_: [32]u8, token: @import("antfly_cancellation").CancellationToken, remaining: *u64) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try @import("../serverless/artifacts/store.zig").chargeReadBudget(remaining, length);
            return @import("lake_index_aggregate_artifact.zig").readPageRange(alloc, artifacts.*, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, offset, length, digest_, token, self.cached);
        }
    }{ .cached = cached };
    if (cached != null) pages.read_cache = .{ .ptr = &page_cache, .read = @TypeOf(page_cache).read };
    var identity_cache: tree.Cache = .{ .alloc = a, .underlying = pages.store(), .max_bytes = 2 * 1024 * 1024 };
    defer identity_cache.deinit();
    const group_rows = try arena.allocator().alloc([]u64, inventory.files.len);
    for (inventory.files, group_rows) |file, *rows| {
        var footer = try local.serverless_query_lake_schema.readFooter(a, source.scanner.reader(), file);
        defer footer.deinit(a);
        rows.* = try arena.allocator().alloc(u64, footer.row_groups.len);
        for (footer.row_groups, rows.*) |group, *count_| count_.* = group.row_count;
    }
    const order = try a.alloc(usize, inventory.files.len);
    defer a.free(order);
    for (order, 0..) |*ordinal, i| ordinal.* = i;
    std.mem.sort(usize, order, inventory.files, struct {
        fn less(files: []const local.serverless_external_source_types.FileEntry, left: usize, right: usize) bool {
            return std.mem.lessThan(u8, files[left].object_uri, files[right].object_uri);
        }
    }.less);
    const query_path = try std.fs.path.join(a, &.{ path, "queries.f32" });
    defer a.free(query_path);
    const truth_path = try std.fs.path.join(a, &.{ path, "truth.u64" });
    defer a.free(truth_path);
    const query_bytes = try std.Io.Dir.cwd().readFileAlloc(io, query_path, a, .limited(64 * 1024 * 1024));
    defer a.free(query_bytes);
    const truth_bytes = try std.Io.Dir.cwd().readFileAlloc(io, truth_path, a, .limited(8 * 1024 * 1024));
    defer a.free(truth_bytes);
    if (query_bytes.len == 0 or query_bytes.len % (dims * 4) != 0) return error.InvalidProbeWorkload;
    const nq = query_bytes.len / (dims * 4);
    if (nq > 1000 or truth_bytes.len != nq * 100 * 8) return error.InvalidProbeWorkload;
    const queries = try a.alloc(f32, query_bytes.len / 4);
    defer a.free(queries);
    for (queries, 0..) |*value, ordinal| value.* = @bitCast(std.mem.readInt(u32, query_bytes[ordinal * 4 ..][0..4], .little));
    var last_recall: f64 = 0;
    for ([_]usize{ 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072 }) |probes| {
        index.config.flat_centroid_probe_count = probes;
        var found: usize = 0;
        var scored: u64 = 0;
        const started = @import("antfly_platform").time.monotonicNs();
        for (0..nq) |qi| {
            var result = try index.searchProfiledRequest(.{ .query = queries[qi * dims ..][0..dims], .k = 100, .search_width = @intCast(probes), .epsilon = 7, .rerank_factor = 4, .load_metadata = true });
            defer result.results.deinit();
            var query_arena = std.heap.ArenaAllocator.init(a);
            defer query_arena.deinit();
            const qa = query_arena.allocator();
            const refs = try qa.alloc(local.storage_rowsource_types.RowRef, result.results.getHits().len);
            for (result.results.getHits(), refs) |hit, *ref_| {
                const key = hit.metadata orelse return error.InvalidProbeWorkload;
                const coordinates = try local.storage_rowsource_identity.parse(key);
                const file_state = for (entries) |entry| {
                    const digest = std.fmt.bytesToHex(&(entry.key_digest orelse entry.digest), .lower);
                    if (std.mem.eql(u8, &digest, coordinates.digest)) break entry;
                } else {
                    std.log.err("recall private key has no authenticated file: {s}", .{key});
                    return error.InvalidProbeWorkload;
                };
                ref_.* = .{ .external = .{
                    .source_id = if (identity_root) |value| value.indexSpace().source else inventory.source_id,
                    .snapshot_id = if (identity_root) |value| value.indexSpace().snapshot else inventory.snapshot_id,
                    .file_id = file_state.id,
                    .row_group_ordinal = coordinates.group,
                    .row_ordinal = coordinates.row,
                } };
            }
            if (identity_root) |value| {
                reads = 128 * 1024 * 1024;
                var logical_reader: identity.Reader = .{ .root = value, .pages = &pages, .cache = &identity_cache };
                const resolved = try logical_reader.translateMany(qa, refs, true);
                const slots = try qa.alloc(u32, resolved.len);
                for (resolved, slots) |row, *slot| slot.* = row.coordinate.file;
                const names = try logical_reader.filesMany(qa, value.current, slots);
                for (refs, resolved, names) |*ref_, row, name| {
                    ref_.external.file_id = name;
                    ref_.external.row_group_ordinal = row.coordinate.group;
                    ref_.external.row_ordinal = row.coordinate.row;
                }
            }
            for (refs) |ref_| {
                const coordinates = ref_.external;
                var ordinal: u64 = coordinates.row_ordinal + 1;
                var matched = false;
                for (order) |fi| {
                    const file = inventory.files[fi];
                    if (std.mem.eql(u8, file.file_id, coordinates.file_id)) {
                        if (coordinates.row_group_ordinal >= group_rows[fi].len or coordinates.row_ordinal >= group_rows[fi][coordinates.row_group_ordinal]) {
                            std.log.err("recall coordinate outside pinned footer file={s} group={} row={}", .{ file.file_id, coordinates.row_group_ordinal, coordinates.row_ordinal });
                            return error.InvalidProbeWorkload;
                        }
                        for (group_rows[fi][0..coordinates.row_group_ordinal]) |count_| ordinal += count_;
                        matched = true;
                        break;
                    }
                    for (group_rows[fi]) |count_| ordinal += count_;
                }
                if (!matched) return error.InvalidProbeWorkload;
                for (0..100) |rank| if (std.mem.readInt(u64, truth_bytes[(qi * 100 + rank) * 8 ..][0..8], .little) == ordinal) {
                    found += 1;
                    break;
                };
            }
            scored += result.profile.approx_vectors_scored;
        }
        last_recall = @as(f64, @floatFromInt(found)) / @as(f64, @floatFromInt(nq * 100));
        const report = try std.json.Stringify.valueAlloc(a, .{ .queries = nq, .probes = probes, .k = 100, .hits = found, .recall = last_recall, .scored = scored, .elapsed_ns = @import("antfly_platform").time.monotonicNs() - started }, .{});
        defer a.free(report);
        std.debug.print("PARQUET_BULK_RECALL {s}\n", .{report});
        if (probes >= 8192 and last_recall >= 0.95) break;
    }
    if (last_recall < 0.95) return error.InvalidProbeWorkload;
}
