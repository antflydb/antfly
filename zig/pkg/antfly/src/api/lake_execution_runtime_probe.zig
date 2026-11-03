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
