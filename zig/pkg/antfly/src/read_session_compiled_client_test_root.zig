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
const DB = local.storage_db_db.DB;
const TestDirectory = local.common_test_directory.TestDirectory;
const Allocator = std.mem.Allocator;
const types = local.storage_db_types;
pub const antfly_sources = @import("source_owner_physical.zig");

test "opaque read session crosses independently compiled production client and provider" {
    const Contract = local.storage_engine.session_contract;
    const Native = @import("antfly_runtime_abi").native_abi.TypeContract;
    const Bridge = struct {
        extern fn antfly_read_session_client_probe_v1(?*anyopaque, *const Native, *const Allocator, *Contract.ProbeResult) callconv(.c) u32;
    };
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("opaque-session-compiled-client");
    defer directory.cleanup();
    var database = try DB.open(alloc, directory.path(), .{});
    defer database.close();
    try database.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"label":{"type":"string"},"amount":{"type":"number","x-antfly-sql-type":"numeric"},"numbers":{"type":"sql_array","x-antfly-sql-type":"int64","nullable":true}},"additionalProperties":false}}}}
    );
    try database.batch(.{ .timestamp_ns = 100, .writes = &.{.{ .key = "a", .value = "{\"id\":9007199254740993,\"label\":\"retained across close\"}" }} });
    try database.batch(.{ .writes = &.{.{ .key = "typed-values", .value = "{\"amount\":9007199254740993.2500,\"numbers\":{\"dimensions\":[{\"length\":2,\"lower_bound\":-3}],\"values\":[\"9007199254740993\",null],\"sql_nulls\":[false,true]}}" }} });
    const layout = Native.of(DB);
    var receipt: Contract.ProbeResult = .{};
    const status = Bridge.antfly_read_session_client_probe_v1(&database, &layout, &alloc, &receipt);
    if (status != 0) std.debug.print("compiled session client failed: {s}\n", .{std.mem.sliceTo(&receipt.failure, 0)});
    try std.testing.expectEqual(@as(u32, 0), status);
    try std.testing.expectEqual(@as(u32, 1), receipt.schema_version);
    try std.testing.expectEqual(@as(i64, 9007199254740993), receipt.value);
    try std.testing.expectEqual(@as(u32, 1), receipt.owner_page);
    try std.testing.expectEqual(@as(u32, 1), receipt.canceled);
    try std.testing.expectEqual(@as(u32, 1), receipt.released);
    try std.testing.expectEqual(@as(u32, 4), receipt.guarded);
}

test "opaque read session relational snapshot pages retain exact values after close" {
    const provider = @import("read_session_provider");
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("opaque-relational-session");
    defer directory.cleanup();
    var database = try DB.open(alloc, directory.path(), .{});
    defer database.close();
    try database.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    );
    try database.batch(.{ .timestamp_ns = 100, .writes = &.{.{ .key = "a", .value = "{\"id\":9007199254740993}" }} });
    var fence = (try database.tryStatementReadFence()) orelse return error.Backpressured;
    const snapshot = provider.capture(&database) catch |err| {
        fence.release();
        return err;
    };
    fence.release();
    var cut_open = true;
    defer if (cut_open) provider.closeStatement(snapshot.handle) catch unreachable;
    try database.batch(.{ .timestamp_ns = 200, .writes = &.{.{ .key = "a", .value = "{\"id\":2}" }} });
    var cancellation = std.atomic.Value(bool).init(false);
    const handle = try provider.openRelational(&database, alloc, "", "", .{
        .relational_query_json = "{\"fields\":[\"id\"],\"schema_version\":1}",
        .cancellation = types.CancellationToken.fromAtomic(&cancellation),
    }, snapshot.handle);
    var session_open = true;
    defer if (session_open) provider.closeRelational(handle) catch unreachable;
    // Closing the captured statement leaves the independently forked scan valid.
    try provider.closeStatement(snapshot.handle);
    cut_open = false;
    try std.testing.expectError(error.SqlRangeTrackingRequired, provider.relationalProofs(handle, alloc));
    try std.testing.expectError(error.InvalidRelationalRowsBudget, provider.nextTyped(handle, alloc, null, .{ .rows = 0 }));
    var page = try provider.nextView(handle, alloc, 1);
    defer page.deinit();
    try std.testing.expect(page.owner_release != null);
    try std.testing.expectEqual(@as(usize, 1), page.rows.len);
    try std.testing.expectEqual(@as(i64, 9007199254740993), (try page.rows[0].cell("id")).value.integer);
    var retained = try page.retain(alloc);
    defer retained.deinit();
    cancellation.store(true, .release);
    try std.testing.expectError(error.Canceled, provider.nextView(handle, alloc, 1));
    try provider.closeRelational(handle);
    session_open = false;
    try std.testing.expectEqual(@as(i64, 9007199254740993), (try retained.rows[0].cell("id")).value.integer);
    try std.testing.expect(database.core.tryLockApplyExclusive());
    database.core.unlockApplyExclusive();
}

test "opaque read session document pages and batch normalization own their output" {
    const provider = @import("read_session_provider");
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("opaque-document-session");
    defer directory.cleanup();
    var database = try DB.open(alloc, directory.path(), .{});
    defer database.close();
    try database.setSchemaJson(alloc,
        \\{"version":1,"default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    );
    try database.batch(.{ .timestamp_ns = 100, .writes = &.{.{ .key = "a", .value = "{\"id\":7}" }} });
    const handle = try provider.openDocument(&database, alloc, "", "", .{ .relational_query_json = "{\"fields\":[\"id\"],\"schema_version\":1}" });
    var session_open = true;
    defer if (session_open) provider.closeDocument(handle) catch unreachable;
    try database.batch(.{ .timestamp_ns = 200, .writes = &.{.{ .key = "a", .value = "{\"id\":8}" }} });
    const normalized = try provider.normalizeDocument(handle, alloc, &.{.{ .key = "b", .value = "{\"id\":9007199254740993}" }});
    defer {
        for (normalized) |write| {
            alloc.free(write.key);
            alloc.free(write.value);
            for (write.json_null_fields) |field| alloc.free(field);
            if (write.json_null_fields.len != 0) alloc.free(write.json_null_fields);
        }
        alloc.free(normalized);
    }
    try std.testing.expectEqualStrings("b", normalized[0].key);
    try std.testing.expect(std.mem.indexOf(u8, normalized[0].value, "9007199254740993") != null);
    var page = try provider.nextDocument(handle, alloc, 1);
    defer page.deinit();
    try provider.closeDocument(handle);
    session_open = false;
    try std.testing.expect(page.owner_release != null);
    try std.testing.expectEqual(@as(usize, 1), page.rows.len);
    try std.testing.expectEqual(@as(i64, 7), (try page.rows[0].cell("id")).value.integer);
    try std.testing.expectEqualStrings("b", normalized[0].key);
}

test "opaque read session document reader crosses native owner and retires bounded windows" {
    const Contract = local.storage_engine.session_contract;
    const Native = @import("antfly_runtime_abi").native_abi.TypeContract;
    const Bridge = struct {
        extern fn antfly_read_document_client_probe_v1(?*anyopaque, *const Native, *const Allocator, *Contract.ProbeResult) callconv(.c) u32;
    };
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("opaque-document-reader-native");
    defer directory.cleanup();
    var database = try DB.open(alloc, directory.path(), .{});
    defer database.close();
    try database.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"document","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"padding":{"type":"string"},"v":{"type":"array","items":{"type":"number"}}},"additionalProperties":false}}}}
    );
    try database.addIndex(.{ .name = "document_vector", .kind = .dense_vector, .config_json = "{\"field\":\"v\",\"dims\":2}" });
    const layout = Native.of(DB);
    var receipt: Contract.ProbeResult = .{};
    const status = Bridge.antfly_read_document_client_probe_v1(&database, &layout, &alloc, &receipt);
    if (status != 0) std.debug.print("compiled document reader failed: {s}\n", .{std.mem.sliceTo(&receipt.failure, 0)});
    try std.testing.expectEqual(@as(u32, 0), status);
    try std.testing.expectEqual(@as(i64, 9007199254740993), receipt.value);
    try std.testing.expectEqual(@as(u32, 1), receipt.owner_page);
    try std.testing.expectEqual(@as(u32, 1), receipt.released);
    try std.testing.expectEqual(@as(u32, 4), receipt.guarded);
}

test "opaque read session rejects execution-private columnar and pattern state before export" {
    const provider = @import("read_session_provider");
    const Catalog = local.sql_catalog;
    const Cell = Catalog.Row.Cell;
    const Cells = Catalog.Row.TypedCells;
    const Array = local.sql_array_value.Value;
    const BlockSlice = @FieldType(Cells.Columnar, "blocks");
    const Block = @typeInfo(BlockSlice).pointer.child;
    const blocks: BlockSlice = @as([*]const Block, @ptrFromInt(@alignOf(Block)))[0..1];
    var values = [_]Cell{.{}};
    var row = struct { typed_cells: ?Cells }{ .typed_cells = .{ .layout = @ptrFromInt(@alignOf(Catalog.Row.TypedLayout)), .values = &values, .columnar = .{ .blocks = blocks, .row = 0 } } };
    try std.testing.expectError(error.UnsupportedVersion, provider.validateExportedRow(row));
    row.typed_cells.?.columnar = null;
    values[0].patterns = @ptrFromInt(@alignOf(@typeInfo(@typeInfo(@FieldType(Cell, "patterns")).optional.child).pointer.child));
    values[0].array = @ptrFromInt(@alignOf(Array));
    try std.testing.expectError(error.UnsupportedVersion, provider.validateExportedRow(row));
    values[0] = .{};
    var element = [_]Cell{.{ .patterns = @ptrFromInt(@alignOf(@typeInfo(@typeInfo(@FieldType(Cell, "patterns")).optional.child).pointer.child)) }};
    var array: Array = .{ .element_type = .int64, .dimensions = &.{}, .elements = &element };
    values[0].array = &array;
    try std.testing.expectError(error.UnsupportedVersion, provider.validateExportedRow(row));
    element[0] = .{};
    try provider.validateExportedRow(row);
    var page: local.storage_relational_read_view.View.Page = .{ .arena = undefined, .rows = &.{}, .after = null, .columnar_blocks = blocks };
    try std.testing.expectError(error.UnsupportedVersion, provider.validateExportedPage(page));
    page.columnar_blocks = &.{};
    page.lease = @ptrFromInt(@alignOf(@typeInfo(@typeInfo(@FieldType(@TypeOf(page), "lease")).optional.child).pointer.child));
    try std.testing.expectError(error.UnsupportedVersion, provider.validateExportedPage(page));
    page.lease = null;
    page.retained_pages = @as([*]@TypeOf(page), @ptrFromInt(@alignOf(@TypeOf(page))))[0..1];
    try std.testing.expectError(error.UnsupportedVersion, provider.validateExportedPage(page));
    page.retained_pages = &.{};
    try provider.validateExportedPage(page);
}

test "opaque read session capture fence allocation failure releases both admissions" {
    const provider = @import("read_session_provider");
    const Admission = local.storage_db_snapshot_admission.SnapshotAdmission;
    const Fence = DB.StatementReadFence;
    const Fixture = struct {
        alloc: Allocator,
        primary: Admission = .{},
        replay: Admission = .{},
        backup_requested: bool = false,
        pub fn tryStatementReadFence(self: *@This()) !?Fence {
            var primary = self.primary.tryAcquireCapture() orelse return null;
            const replay = self.replay.tryAcquireCapture() orelse {
                primary.release();
                return null;
            };
            return .{ .primary = primary, .replay = replay };
        }
        pub fn captureRelationalStatementSnapshotAtFence(_: *@This(), _: *const Fence) !DB.RelationalStatementSnapshot {
            return error.TestUnexpectedResult;
        }
        pub fn tryEmbeddedBackupReadFence(self: *@This()) !?Fence {
            self.backup_requested = true;
            return self.tryStatementReadFence();
        }
    };
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var db: Fixture = .{ .alloc = failing.allocator() };
    try std.testing.expectError(error.OutOfMemory, provider.tryFence(&db, false));
    var primary = db.primary.tryAcquireCapture() orelse return error.TestUnexpectedResult;
    primary.release();
    var replay = db.replay.tryAcquireCapture() orelse return error.TestUnexpectedResult;
    replay.release();
    db.alloc = std.testing.allocator;
    const handle = (try provider.tryFence(&db, true)) orelse return error.TestUnexpectedResult;
    try std.testing.expect(db.backup_requested);
    try std.testing.expect(db.primary.tryAcquireCapture() == null);
    try std.testing.expect(db.replay.tryAcquireCapture() == null);
    try std.testing.expect((try provider.tryFence(&db, false)) == null);
    // Reject an incompatible capability before following its poisoned state.
    var bad = handle;
    bad.version = 0;
    bad.context = @ptrFromInt(1);
    try std.testing.expectError(error.UnsupportedVersion, provider.closeFence(bad));
    try provider.closeFence(handle);
    primary = db.primary.tryAcquireCapture() orelse return error.TestUnexpectedResult;
    primary.release();
    replay = db.replay.tryAcquireCapture() orelse return error.TestUnexpectedResult;
    replay.release();
}
