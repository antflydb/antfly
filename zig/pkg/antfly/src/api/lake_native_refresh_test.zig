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

const std = @import("std");
const local = @import("antfly_local_sources");
const publication = @import("lake_index_publication.zig");
const catalog = local.metadata_lake_index_catalog;
const identity = @import("lake_row_identity.zig");

test "external lake selective captured publication preserves untouched cohorts across physical rewrites" {
    try refreshScenario(false, false, false);
}
test "external lake selective object publication retains every live map during collection" {
    try refreshScenario(true, true, false);
}
test "external lake selective object publication preserves untouched cohorts without collection" {
    try refreshScenario(true, false, false);
}
test "external lake empty text refresh publishes nontext updates without rescanning" {
    try refreshScenario(false, false, true);
}
fn refreshScenario(object_backend: bool, collect: bool, comptime numeric_body: bool) !void {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-selective-publication");
    defer directory.cleanup();
    var fs = try local.storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const schema = try std.fmt.allocPrint(a,
        \\{{"version":1,"storage_mode":"relational","default_type":"row","base_source":{{"kind":"external","table_id":"lake","format":"parquet","uri":"file://{s}","schema_fingerprint":"schema","row_identity":{{"key_columns":["id"]}}}},"document_schemas":{{"row":{{"schema":{{"type":"object","properties":{{"id":{{"type":"integer"}},"body":{{"type":"{s}"}},"label":{{"type":"string"}},"payload":{{"type":"string"}}}},"additionalProperties":false}}}}}}}}
    , .{ directory.path(), if (numeric_body) "integer" else "string" });
    defer a.free(schema);
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, schema)).?;
    defer binding.deinit(a);
    const artifact_root = try std.fs.path.join(a, &.{ directory.path(), "native-artifacts" });
    defer a.free(artifact_root);
    var fs_artifacts = if (!object_backend) try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, artifact_root) else null;
    defer if (fs_artifacts) |*value| value.deinit();
    const artifact_uri = try std.fmt.allocPrint(a, "file://{s}", .{artifact_root});
    defer a.free(artifact_uri);
    var objects = if (object_backend) try @import("../serverless/artifacts/object_store.zig").ObjectStore.initFileUri(a, artifact_uri) else null;
    defer if (objects) |*value| value.deinit();
    var store = if (objects) |*value| value.artifactStore() else fs_artifacts.?.artifactStore();
    var table: local.common_topology_records.TableRecord = .{ .table_id = 4, .name = "lake", .schema_json = schema, .indexes_json =
        \\{"body_text":{"type":"full_text","field":"body","store_source":false},"all_text":{"type":"full_text","store_source":false},"stats":{"type":"algebraic","materializations":[{"name":"rows","op":"count"},{"name":"body_sum","op":"sum","measure":"id","group_by":["body"]}]}}
    };
    const Fixed = struct {
        now: u64 = 101,
        fn read(raw: *const anyopaque) !u64 {
            return @as(*const @This(), @ptrCast(@alignCast(raw))).now;
        }
    };
    var clock: Fixed = .{};
    var bytes: []u8 = &.{};
    defer a.free(bytes);
    var after_body: [64]u8 = undefined;
    const steps = [_]usize{ 0, 1, 2, 5, 3, 6 };
    for (steps, 0..) |step, iteration| {
        var ids: [257]i64 = undefined;
        var bodies: [257][]const u8 = undefined;
        var numeric_bodies: [257]i64 = undefined;
        var labels: [257][]const u8 = undefined;
        var payloads: [257][]const u8 = undefined;
        for (&ids, &bodies, &labels, &payloads, 0..) |*id, *body, *label, *payload, row| {
            const ordered = if (step == 0) row else (row * 73) % 257;
            const original = if (step == 2) 256 - ordered else ordered;
            id.* = 9007199254740993 + @as(i64, @intCast(original));
            numeric_bodies[row] = if (step == 6 and original == 17) 2 else 1;
            body.* = if ((step == 3 or step == 6) and original == 17) "changed word" else "needle same text";
            label.* = if (step == 5) "updated label" else "original label";
            payload.* = if (step == 6 and original == 17) "new payload" else "old payload";
        }
        for (0..3) |part| {
            const path = try std.fmt.allocPrint(a, "part-{d}-{d}.parquet", .{ iteration, part });
            defer a.free(path);
            if (iteration != 0) {
                const old = try std.fmt.allocPrint(a, "part-{d}-{d}.parquet", .{ iteration - 1, part });
                defer a.free(old);
                try client.deleteObject("antfly", old, .{});
            }
            const begin = part * 100;
            const end = @min(ids.len, begin + 100);
            const data = try local.serverless_query_lake_parquet_rowgroup.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, if (numeric_body) &.{ .{ .column_id = "id", .field_id = 1, .values = ids[begin..end] }, .{ .column_id = "body", .field_id = 2, .values = numeric_bodies[begin..end] } } else &.{.{ .column_id = "id", .field_id = 1, .values = ids[begin..end] }}, if (numeric_body) &.{
                .{ .column_id = "label", .field_id = 3, .converted_type = 0, .values = labels[begin..end] },
                .{ .column_id = "payload", .field_id = 4, .converted_type = 0, .values = payloads[begin..end] },
            } else &.{
                .{ .column_id = "body", .field_id = 2, .converted_type = 0, .values = bodies[begin..end] },
                .{ .column_id = "label", .field_id = 3, .converted_type = 0, .values = labels[begin..end] },
                .{ .column_id = "payload", .field_id = 4, .converted_type = 0, .values = payloads[begin..end] },
            });
            defer a.free(data);
            var put = try client.putObject("antfly", path, data, .{});
            put.deinit(a);
        }
        var source = try local.serverless_query_lake_serving.ServingSource.open(a, .{ .storage_mode = .relational, .external_base_source = binding }, .{});
        defer source.deinit();
        clock.now = 101 + iteration * 100;
        const pending = try publication.begin(a, std.testing.io, table, &source, @splat(4), .{}, clock.now - 1, 100000);
        defer a.free(pending);
        var attempt = table;
        attempt.lake_index_catalog_json = pending;
        const completed = try publication.build(a, &store, attempt, &source, @splat(4), .{ .io = std.testing.io }, .none, .{ .ptr = &clock, .now_ms = Fixed.read });
        a.free(bytes);
        bytes = completed;
        table.lake_index_catalog_json = completed;
        var parsed = try catalog.parse(a, completed);
        defer parsed.deinit();
        const published = parsed.value.published.?;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const root = try identity.load(arena.allocator(), store, published.row_identity.?, .none, null);
        if (step == 0 and !numeric_body) {
            var hydrated = published;
            try @import("lake_index_directory.zig").hydrateLazy(arena.allocator(), store, &hydrated, .none, null);
            const text = for (hydrated.declarations) |entry| {
                if (std.mem.eql(u8, entry.name, "body_text")) break entry;
            } else return error.TestUnexpectedResult;
            const corpus = @import("lake_index_native_text.zig");
            const text_root = try corpus.loadRoot(arena.allocator(), store, text.artifact, .none, null);
            var count: usize = 0;
            for (text_root.file_groups) |group| if (group.segments.len != 0) {
                try std.testing.expectEqual(@as(u8, 1), group.forward_version);
                try std.testing.expectEqual(group.segments.len, group.forwards.len);
                count += 1;
            };
            try std.testing.expect(count != 0);
        }
        const navigation = root.spaceForIndex("body_text").navigation.?;
        if (step == 3) after_body = navigation.checksum[0..64].*;
        if (step == 6 and !numeric_body) try std.testing.expectEqualSlices(u8, &after_body, navigation.checksum);
        if (collect) {
            const Gc = @import("lake_index_gc.zig");
            var marks = std.heap.ArenaAllocator.init(a);
            defer marks.deinit();
            var collector: Gc.Collector = .{ .a = marks.allocator(), .table = table.table_id, .authority = undefined, .store = store, .identity = @splat(4), .namespace = published.namespace, .context = .{ .io = std.testing.io }, .token = @splat(1), .audit_only = true, .remaining_reads = 512 * 1024 * 1024 };
            try collector.markPublication(published);
            const Sweep = struct {
                store: *@import("../serverless/artifacts/store.zig").ArtifactStore,
                collector: *Gc.Collector,
                current: u64,
                fn visit(raw: *anyopaque, scope: @import("../serverless/artifacts/store.zig").UploadScope, id: []const u8) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    if (scope.fencingToken() >= self.current or self.collector.marked.contains(id)) return;
                    self.store.delete(id) catch |err| switch (err) {
                        error.FileNotFound, error.ArtifactRetentionActive => {},
                        else => return err,
                    };
                }
            };
            var sweep: Sweep = .{ .store = &store, .collector = &collector, .current = published.generation };
            try store.visitScopedUploads(root.domain, .{ .ptr = &sweep, .visit = Sweep.visit }, .none);
        }
    }
}
