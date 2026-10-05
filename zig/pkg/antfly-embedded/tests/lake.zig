// Copyright 2026 Antfly, Inc.
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

//! Consumer regression: import the public package, never a private source owner.
const std = @import("std");
const embedded = @import("antfly-embedded");
const lake = embedded.lake;

test "public embedded lake opens a host-resolved source and scans a SQL cursor" {
    const alloc = std.testing.allocator;
    var memory = embedded.object_storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");
    const bytes = try lake.parquet.buildTestPlainI64ParquetObjectAlloc(alloc, &.{.{ .column_id = "amount", .values = &.{ 3, 5 }, .write_statistics = true }});
    defer alloc.free(bytes);
    var written = try client.putObject("bucket", "events/data.parquet", bytes, .{});
    written.deinit(alloc);
    const Host = struct {
        client: embedded.object_storage.ObjectStorage,
        opens: usize = 0,
        fn open(raw: *const anyopaque, allocator: std.mem.Allocator, binding: lake.host.Binding) !lake.host.OpenedObjectStore {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            try std.testing.expectEqualStrings("managed", binding.credential_ref.?.ref_id);
            self.opens += 1;
            return lake.host.OpenedObjectStore.initWithExistingClient(allocator, self.client, "bucket", "events");
        }
    };
    var host = Host{ .client = client };
    const binding: lake.host.Binding = .{ .table_id = "events", .format = .parquet, .source_uri = "s3://bucket/events", .schema_fingerprint = "v1", .credential_ref = .{ .ref_id = "managed" } };
    const table: lake.sql_catalog.Table = .{
        .id = 7,
        .physical_name = "events",
        .schema_version = 1,
        .columns = &.{.{ .name = "amount", .path = "amount", .type = .integer }},
        .external_base_source = .{ .binding = binding, .table_id = @constCast("events"), .source_uri = @constCast("s3://bucket/events"), .schema_fingerprint = @constCast("v1") },
    };
    const cursor = try lake.sql_cursor.open(alloc, table, .{ .fields = &.{"amount"}, .limit = 10 }, .{}, .{ .resolver = .{ .ptr = &host, .open_fn = Host.open } });
    defer cursor.close(cursor.ptr);
    try std.testing.expectEqual(@as(usize, 1), host.opens);
    // Opening completes synchronously. Subsequent reads and close own their
    // returned store and do not borrow the host options or invoke it again.
    const page = try cursor.next(cursor.ptr, alloc, 10);
    defer page.deinit();
    try std.testing.expectEqual(@as(usize, 2), page.rows.len);
    try std.testing.expectEqual(@as(i64, 3), page.rows[0].value.object.get("amount").?.integer);
    try std.testing.expectEqual(@as(i64, 5), page.rows[1].value.object.get("amount").?.integer);
    try std.testing.expectEqual(@as(usize, 1), host.opens);
}

test "public embedded SQL compiler uses the native package dependency graph" {
    var compiled = try lake.sql_compiler.compile(std.testing.allocator, "SELECT amount FROM events", .{});
    defer compiled.deinit();
}
