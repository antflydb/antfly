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
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-execution-native");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const data = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{}, &.{
        .{ .column_id = "body", .field_id = 1, .converted_type = 0, .values = &.{ "first value", "second value" } },
        .{ .column_id = "dense", .field_id = 2, .converted_type = 0, .values = &.{ "[1,0]", "[0,1]" } },
        .{ .column_id = "sparse", .field_id = 3, .converted_type = 0, .values = &.{ "{\"1\":2}", "{\"2\":3}" } },
    });
    defer a.free(data);
    var put = try client.putObject("antfly", "part.parquet", data, .{});
    put.deinit(a);
    const schema = try std.fmt.allocPrint(a, "{{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"base_source\":{{\"kind\":\"external\",\"table_id\":\"lake\",\"format\":\"parquet\",\"uri\":\"file://{s}\",\"schema_fingerprint\":\"schema\",\"row_identity\":{{\"key_columns\":[\"body\"]}}}},\"relational_indexes\":[{{\"name\":\"body_idx\",\"keys\":[{{\"column\":\"body\"}}],\"include_columns\":[\"dense\"]}}],\"document_schemas\":{{\"row\":{{\"schema\":{{\"type\":\"object\",\"properties\":{{\"body\":{{\"type\":\"string\"}},\"dense\":{{\"type\":\"string\"}},\"sparse\":{{\"type\":\"string\"}}}},\"additionalProperties\":false}}}}}}}}", .{directory.path()});
    defer a.free(schema);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema)).?;
    defer binding.deinit(a);
    var source = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
    defer source.deinit();
    var store = try @import("lake_index_store.zig").Store.openNative(a, null, null, false, .standalone, directory.path());
    defer store.deinit();
    const table: local.common_topology_records.TableRecord = .{ .table_id = 4, .name = "lake", .schema_json = schema, .indexes_json = "{\"body_text\":{\"type\":\"full_text\",\"field\":\"body\",\"store_source\":true},\"stats\":{\"type\":\"algebraic\",\"materializations\":[{\"name\":\"rows\",\"op\":\"count\"}]},\"dense\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2},\"sparse\":{\"type\":\"embeddings\",\"external\":true,\"sparse\":true}}" };
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
        const get = @extern(*const fn () callconv(.c) *const c.Endpoint, .{ .name = "antfly_lake_execute_v2" });
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
    var added = try client.putObject("antfly", "part2.parquet", data, .{});
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
