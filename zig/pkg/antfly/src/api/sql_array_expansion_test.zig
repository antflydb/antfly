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
const db = local.storage_db_selected_root.db;
const raft = @import("../raft/mod.zig");
const reads = @import("antfly_source_root").antfly_sources.table_reads;
const Server = @import("http_server.zig").ApiHttpServer;
const Handler = @import("httpx_handler.zig").AntflyApiHandler;

test "httpx SQL typed array expansion matches PostgreSQL through native captures" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("sql-array-expansion");
    defer directory.cleanup();
    var database = try db.DB.open(a, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer database.close();
    const schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"tags":{"type":"sql_array","x-antfly-sql-type":"text","nullable":true}}}}}}
    ;
    try database.setSchemaJson(a, schema);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const tags = (try local.sql_array_text.decodeLeaky(scratch, .text, "{urgent,NULL,vip}", .{})).value;
    const empty = (try local.sql_array_text.decodeLeaky(scratch, .text, "{}", .{})).value;
    const tags_wire = try local.sql_array_wire.toJsonLeaky(scratch, tags, .{});
    const empty_wire = try local.sql_array_wire.toJsonLeaky(scratch, empty, .{});
    try database.batch(.{ .writes = &.{
        .{ .key = "0", .value = try std.json.Stringify.valueAlloc(scratch, .{ .id = @as(i64, 9007199254740993), .tags = tags_wire }, .{}) },
        .{ .key = "1", .value = try std.json.Stringify.valueAlloc(scratch, .{ .id = @as(i64, 9007199254740993), .tags = empty_wire }, .{}) },
        .{ .key = "2", .value = "{\"id\":9007199254740993}" },
    }, .timestamp_ns = 42 });
    var source: @import("sql_parity_sources.zig").Tables(1) = .{
        .records = .{.{ .table_id = 7, .name = "things", .schema_json = schema }},
        .reads = .{reads.BoundTableReadSource.init("things", 7, &database, raft.read_gate.alreadyReadSafeBarrier())},
    };
    var runtime = try db.background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded });
    defer runtime.deinit();
    var server = Server.init(a, .{ .backend_runtime = runtime.ptr() }, .{ .ptr = &source, .vtable = &.{ .status = @TypeOf(source).status, .system_catalog = @TypeOf(source).systemCatalog, .supports_query_definitions = true } }, source.source(), null);
    defer server.deinit();
    var handler: Handler = .{ .api_server = &server };
    try @import("sql_parity_reference.zig").runArrayExpansion(a, &handler);
    try std.testing.expectEqual(@as(usize, 6), source.captures);
    try std.testing.expectEqual(@as(usize, 0), source.lookup_calls.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 0), source.unbounded_reads.load(.monotonic));
}
