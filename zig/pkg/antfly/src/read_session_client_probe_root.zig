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

//! Independent non-test consumer of the production retained-read engine ABI.
const std = @import("std");
const local = @import("antfly_local_sources");
const DB = local.storage_db_db.DB;
const engine = local.storage_engine;
const contract = engine.session_contract;
const TypeContract = @import("antfly_runtime_abi").native_abi.TypeContract;
pub const antfly_sources = @import("source_owner_storage.zig");
pub const capi_dependencies = @import("capi_dependencies.zig");
comptime {
    @export(&probeDocument, .{ .name = "antfly_read_document_client_probe_v1", .visibility = .hidden });
    @export(&probe, .{ .name = "antfly_read_session_client_probe_v1", .visibility = .hidden });
}
fn probe(pointer: ?*anyopaque, layout: *const TypeContract, allocator: *const std.mem.Allocator, output: *contract.ProbeResult) callconv(.c) u32 {
    output.* = .{};
    mark(output, "db_layout");
    if (!layout.matches(TypeContract.of(DB))) return failed(output, error.UnsupportedVersion);
    const raw = pointer orelse return failed(output, error.InvalidArgument);
    if (@intFromPtr(raw) % @alignOf(DB) != 0) return failed(output, error.InvalidArgument);
    const db: *DB = @ptrCast(@alignCast(raw));
    run(db, allocator.*, output) catch |err| return failed(output, err);
    return 0;
}
fn failed(output: *contract.ProbeResult, err: anyerror) u32 {
    const text = @errorName(err);
    const used = std.mem.indexOfScalar(u8, &output.failure, 0) orelse 0;
    const start = @min(used + 1, output.failure.len - 1);
    if (used < output.failure.len) output.failure[used] = ':';
    const len = @min(text.len, output.failure.len - start - 1);
    @memcpy(output.failure[start..][0..len], text[0..len]);
    return 1;
}
fn mark(output: *contract.ProbeResult, text: []const u8) void {
    output.failure = @splat(0);
    @memcpy(output.failure[0..@min(text.len, output.failure.len - 1)], text[0..@min(text.len, output.failure.len - 1)]);
}
fn run(db: *DB, alloc: std.mem.Allocator, out: *contract.ProbeResult) !void {
    mark(out, "mode");
    if (@import("builtin").is_test or !engine.enabled) return error.UnsupportedVersion;
    const descriptor = engine.descriptorForValidation();
    mark(out, "descriptor_version");
    if (descriptor.version != 37) return error.UnsupportedVersion;
    inline for (@typeInfo(engine.Descriptor).@"struct".field_names, @typeInfo(engine.Descriptor).@"struct".field_types) |field, T| {
        if (comptime T == @TypeOf(engine.table_layout)) {
            const actual = @field(descriptor.*, field);
            const expected = @field(engine, field);
            if (!actual.matches(expected)) {
                mark(out, field);
                return error.UnsupportedVersion;
            }
        }
    }
    mark(out, "fence");
    var fence = (try db.tryStatementReadFence()) orelse return error.Backpressured;
    mark(out, "capture");
    var snapshot = db.captureRelationalStatementSnapshotAtFence(&fence) catch |err| {
        fence.release();
        return err;
    };
    fence.release();
    fence.release(); // Public release remains idempotent across the owner boundary.
    var snapshot_open = true;
    defer if (snapshot_open) snapshot.deinit();
    out.schema_version = snapshot.schema_version;
    mark(out, "write");
    try db.batch(.{ .timestamp_ns = 200, .writes = &.{.{ .key = "a", .value = "{\"id\":2}" }} });
    var canceled = std.atomic.Value(bool).init(false);
    mark(out, "open");
    const session = try db.openRelationalReadSessionAtSnapshot(alloc, "", "", .{
        .relational_query_json = "{\"fields\":[\"id\",\"label\"],\"schema_version\":1}",
        .cancellation = local.storage_db_types.CancellationToken.fromAtomic(&canceled),
    }, &snapshot);
    var session_open = true;
    defer if (session_open) session.deinit();
    mark(out, "guard");
    for (0..4) |invalid_case| {
        var invalid = session.handle;
        invalid.context = @ptrFromInt(1);
        if (invalid_case == 0) invalid.version = 0 else if (invalid_case == 1) invalid.kind = .document else if (invalid_case == 2) invalid.context = null else invalid.kind = @fromBackingInt(@intCast(std.math.maxInt(u32)));
        if (contract.call("checkpoint", .relational, invalid, .{invalid})) |_| {
            return error.InvalidArgument;
        } else |err| {
            const expected: anyerror = if (invalid_case == 2) error.InvalidArgument else error.UnsupportedVersion;
            if (err != expected) return err;
            out.guarded += 1;
        }
    }
    mark(out, "close_statement");
    snapshot.deinit();
    snapshot_open = false;
    mark(out, "page");
    var page = try session.nextViewPage(alloc, 1);
    var page_open = true;
    defer if (page_open) page.deinit();
    out.owner_page = @intFromBool(page.owner_release != null);
    if (page.rows.len != 1) return error.InvalidArgument;
    mark(out, "retain");
    var retained = try page.retain(alloc);
    var retained_open = true;
    defer if (retained_open) retained.deinit();
    mark(out, "cancel");
    canceled.store(true, .release);
    if (session.nextViewPage(alloc, 1)) |unexpected| {
        var owned = unexpected;
        owned.deinit();
        return error.InvalidArgument;
    } else |err| {
        if (err != error.Canceled) return err;
        out.canceled = 1;
    }
    mark(out, "close_session");
    session.deinit();
    session_open = false;
    mark(out, "reader_batch");
    try verifyReader(db, alloc, out);
    mark(out, "numeric_array_values");
    try verifyNativeValues(db, alloc);
    mark(out, "release_page");
    page.deinit();
    page_open = false;
    mark(out, "read_retained");
    out.value = (try retained.rows[0].cell("id")).value.integer;
    if (!std.mem.eql(u8, (try retained.rows[0].cell("label")).value.string, "retained across close")) return error.InvalidArgument;
    mark(out, "release_retained");
    retained.deinit();
    retained_open = false;
    out.released = 1;
    out.failure = @splat(0);
}

fn verifyReader(db: *DB, alloc: std.mem.Allocator, out: *contract.ProbeResult) !void {
    try db.batch(.{ .timestamp_ns = 300, .writes = &.{.{ .key = "a", .value = "{\"id\":9007199254740993,\"label\":\"native integer\"}" }} });
    mark(out, "reader_typed_page");
    const Filter = struct {
        fail: bool = false,
        visits: usize = 0,
        fn matches(raw: *anyopaque, _: std.mem.Allocator, _: []const u8, _: @typeInfo(@typeInfo(@FieldType(DB.RelationalRows.RowFilter, "matches")).pointer.child).@"fn".param_types[3].?) anyerror!bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.visits += 1;
            if (self.fail) return error.TestWindowFailed;
            return true;
        }
    };
    var filter: Filter = .{};
    var paged_reader = try db.beginRelationalRows(alloc, .{ .fields = &.{"id"}, .row_filter = .{ .context = &filter, .matches = Filter.matches } });
    var paged_open = true;
    defer if (paged_open) paged_reader.deinit();
    var page = try paged_reader.nextTypedPage(alloc, db.backend_runtime.io(), .{ .rows = 1 });
    defer page.deinit();
    paged_reader.deinit();
    paged_open = false;
    if (page.owner_release == null or page.rows.len != 1 or page.rows[0].typed.?.object.get("id").?.integer != 9007199254740993) return error.InvalidArgument;
    filter.fail = true;
    var failing_reader = try db.beginRelationalRows(alloc, .{ .fields = &.{"id"}, .row_filter = .{ .context = &filter, .matches = Filter.matches } });
    if (failing_reader.nextTypedPage(alloc, null, .{})) |unexpected| {
        var cleanup = unexpected;
        cleanup.deinit();
        failing_reader.deinit();
        return error.InvalidArgument;
    } else |err| {
        failing_reader.deinit();
        if (err != error.TestWindowFailed or filter.visits != 2) return error.InvalidArgument;
    }
    var lease = try db.beginQueryReadLease();
    var lease_open = true;
    defer if (lease_open) lease.release();
    mark(out, "lease_guards");
    for (0..3) |invalid_case| {
        var invalid = lease.handle;
        if (invalid_case == 0) invalid.version = 0 else if (invalid_case == 1) invalid.context = null else invalid.context = @ptrFromInt(1);
        if (DB.QueryReadLease.Protocol.call("reader", invalid, .{ invalid, alloc, @as([]const []const u8, &.{"id"}), @as(u32, 1) })) |_| {
            return error.InvalidArgument;
        } else |err| {
            if (err != (if (invalid_case == 0) error.UnsupportedVersion else error.InvalidArgument)) return err;
        }
    }
    const Predicate = struct {
        fn accept(ptr: *anyopaque, _: []const u8) !bool {
            const calls: *usize = @ptrCast(@alignCast(ptr));
            calls.* += 1;
            return true;
        }
    };
    var calls: usize = 0;
    mark(out, "lease_search");
    var ranked = try lease.search(alloc, .{ .native_key_predicate = .{ .ptr = &calls, .allows = Predicate.accept } });
    defer ranked.result.deinit();
    const original = ranked.request.native_key_predicate orelse return error.InvalidArgument;
    if (original.ptr != @as(*anyopaque, @ptrCast(&calls)) or original.allows != Predicate.accept) return error.InvalidArgument;
    // Native predicates are route-specific (e.g. dense), so match-all need
    // not call one. Captured requests must retain the original callback after
    // the owner's temporary receiver has gone out of scope.
    if (!try original.allows(original.ptr, "a") or calls == 0) return error.InvalidArgument;
    mark(out, "reader_capture");
    var reader = try lease.relationalRows(alloc, &.{ "id", "label" }, 1);
    var reader_open = true;
    defer if (reader_open) reader.deinit();
    lease.release();
    lease_open = false;
    mark(out, "reader_generation_write");
    // Releasing the writer fence must allow a new generation while the
    // independently retained Reader continues reading the captured generation.
    try db.batch(.{ .timestamp_ns = 301, .writes = &.{.{ .key = "a", .value = "{\"id\":7}" }} });
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const layout = try local.sql_catalog.Row.TypedLayout.init(a, &.{ "id", "label" });
    mark(out, "reader_lookup_batch");
    const batch = try reader.lookupSqlBatch(a, &.{ "a", "missing" }, layout, 256 * 1024);
    if (batch.rows.len != 2 or batch.consumed != 2 or batch.rows[1] != null) return error.InvalidArgument;
    const row = batch.rows[0] orelse return error.InvalidArgument;
    if ((try row.typed_cells.?.at(0)).value.integer != 9007199254740993) return error.InvalidArgument;
    mark(out, "reader_lookup_json");
    const json = (try reader.lookupTypedRow(a, "a")) orelse return error.InvalidArgument;
    if (json.typed.?.object.get("id").?.integer != 9007199254740993) return error.InvalidArgument;
    var shared: ?*const local.sql_catalog.Row.TypedLayout = null;
    mark(out, "reader_lookup_native");
    const typed = (try reader.lookupSqlRowWithLayout(a, "a", &shared)) orelse return error.InvalidArgument;
    reader.deinit();
    reader_open = false;
    mark(out, "reader_after_close");
    if (page.rows[0].typed.?.object.get("id").?.integer != 9007199254740993) return error.InvalidArgument;
    // Ranked/point payloads use the caller's arena and outlive reader/lease.
    if ((try typed.typed_cells.?.at(0)).value.integer != 9007199254740993 or (try row.typed_cells.?.at(0)).value.integer != 9007199254740993) return error.InvalidArgument;
}

fn probeDocument(pointer: ?*anyopaque, layout: *const TypeContract, allocator: *const std.mem.Allocator, output: *contract.ProbeResult) callconv(.c) u32 {
    output.* = .{};
    mark(output, "document_reader");
    if (@import("builtin").is_test or !engine.enabled or !layout.matches(TypeContract.of(DB))) return failed(output, error.UnsupportedVersion);
    const raw = pointer orelse return failed(output, error.InvalidArgument);
    if (@intFromPtr(raw) % @alignOf(DB) != 0) return failed(output, error.InvalidArgument);
    const db: *DB = @ptrCast(@alignCast(raw));
    verifyDocumentReader(db, allocator.*, output) catch |err| return failed(output, err);
    return 0;
}
fn verifyDocumentReader(db: *DB, alloc: std.mem.Allocator, out: *contract.ProbeResult) !void {
    const padding = try alloc.alloc(u8, 32 * 1024);
    defer alloc.free(padding);
    @memset(padding, 'p');
    const document = try std.fmt.allocPrint(alloc, "{{\"id\":9007199254740993,\"v\":[0,0],\"padding\":\"{s}\"}}", .{padding});
    defer alloc.free(document);
    mark(out, "document_write");
    try db.batch(.{ .writes = &.{ .{ .key = "a", .value = document }, .{ .key = "b", .value = document } }, .sync_level = .full_index });
    var lease = try db.beginQueryReadLease();
    var lease_open = true;
    defer if (lease_open) lease.release();
    const Predicate = struct {
        fn accept(raw: *anyopaque, _: []const u8) anyerror!bool {
            const count: *usize = @ptrCast(@alignCast(raw));
            count.* += 1;
            return true;
        }
        fn fail(raw: *anyopaque, _: []const u8) anyerror!bool {
            const count: *usize = @ptrCast(@alignCast(raw));
            count.* += 1;
            return error.TestWindowFailed;
        }
    };
    var count: usize = 0;
    mark(out, "document_dense_callback");
    var ranked = try lease.search(alloc, .{ .index_name = "document_vector", .dense = .{ .vector = &.{ 0, 0 }, .k = 1 }, .native_key_predicate = .{ .ptr = &count, .allows = Predicate.accept } });
    defer ranked.result.deinit();
    const retained_hook = ranked.request.native_key_predicate orelse return error.InvalidArgument;
    if (count == 0 or retained_hook.ptr != @as(*anyopaque, @ptrCast(&count)) or retained_hook.allows != Predicate.accept) return error.InvalidArgument;
    count = 0;
    if (lease.search(alloc, .{ .index_name = "document_vector", .dense = .{ .vector = &.{ 0, 0 }, .k = 1 }, .native_key_predicate = .{ .ptr = &count, .allows = Predicate.fail } })) |unexpected| {
        var cleanup = unexpected;
        cleanup.result.deinit();
        return error.InvalidArgument;
    } else |err| if (err != error.TestWindowFailed or count == 0) return error.InvalidArgument;
    mark(out, "document_capture");
    var reader = try lease.documentRows(alloc, 1);
    var reader_open = true;
    defer if (reader_open) reader.deinit();
    lease.release();
    lease_open = false;
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"id\":1}" }} });
    mark(out, "document_guards");
    for (0..4) |invalid_case| {
        var invalid = reader.handle;
        if (invalid_case == 0) invalid.version = 0 else if (invalid_case == 1) invalid.kind = .reader else if (invalid_case == 2) invalid.context = null else invalid.context = @ptrFromInt(1);
        if (contract.call("lookup_document", .document_reader, invalid, .{ invalid, @as([]const u8, "a") })) |_| return error.InvalidArgument else |err| {
            const expected: anyerror = if (invalid_case < 2) error.UnsupportedVersion else error.InvalidArgument;
            if (err != expected) return err;
            out.guarded += 1;
        }
    }
    mark(out, "document_single");
    const one = try reader.lookup("a");
    try verifyDocumentBytes(alloc, one, padding);
    if (reader.lookupBatch(&.{}, 1)) |_| return error.InvalidArgument else |err| if (err != error.InvalidBatch) return err;
    mark(out, "document_batch");
    const batch = try reader.lookupBatch(&.{ "a", "b" }, 128 * 1024);
    if (batch.consumed != 2 or batch.values.len != 2) return error.InvalidArgument;
    for (batch.values) |value| try verifyDocumentBytes(alloc, value orelse return error.InvalidArgument, padding);
    mark(out, "document_projection");
    const projected = try reader.lookupProjectedBatch(&.{ "a", "b" }, &.{"id"}, 4096);
    if (projected.consumed == 0 or projected.consumed > 2 or projected.values.len != projected.consumed) return error.InvalidArgument;
    for (projected.values) |source| switch (source) {
        .fields => |fields| {
            if (fields.len != 1 or !std.mem.eql(u8, fields[0].name, "id") or !std.mem.eql(u8, fields[0].value, "9007199254740993")) return error.InvalidArgument;
        },
        .bytes => return error.InvalidArgument,
    };
    out.owner_page = 1;
    // The existing borrowed-window API requires consumers to own selected
    // cells before page release. Preserve that exact rule across the boundary.
    const copied = try alloc.dupe(u8, projected.values[0].fields[0].value);
    defer alloc.free(copied);
    // LSM may consume one projected row per byte window while a range-capable
    // backend consumes both. Continue from caller order without retaining a
    // borrowed source from the previous operation.
    mark(out, "document_projection_repeat");
    const repeated = try reader.lookupProjectedBatch(&.{"b"}, &.{"id"}, 4096);
    if (repeated.consumed != 1 or repeated.values.len != 1 or repeated.values[0] != .fields or repeated.values[0].fields.len != 1 or !std.mem.eql(u8, repeated.values[0].fields[0].value, copied)) return error.InvalidArgument;
    mark(out, "document_release_page");
    reader.releasePage();
    const after_release = try reader.lookup("b");
    try verifyDocumentBytes(alloc, after_release, padding);
    mark(out, "document_close");
    reader.deinit();
    reader_open = false;
    if (!std.mem.eql(u8, copied, "9007199254740993")) return error.InvalidArgument;
    out.value = try std.fmt.parseInt(i64, copied, 10);
    out.released = 1;
}

fn verifyDocumentBytes(alloc: std.mem.Allocator, bytes: []const u8, padding: []const u8) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, bytes, .{ .parse_numbers = false });
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidArgument;
    const id = parsed.value.object.get("id") orelse return error.InvalidArgument;
    const body = parsed.value.object.get("padding") orelse return error.InvalidArgument;
    const exact = switch (id) {
        .integer => |value| value == 9007199254740993,
        .number_string => |value| std.mem.eql(u8, value, "9007199254740993"),
        else => false,
    };
    if (!exact or body != .string or !std.mem.eql(u8, body.string, padding)) {
        std.debug.print("document snapshot mismatch: id={any}, bytes={d}, prefix={s}\n", .{ id, bytes.len, bytes[0..@min(bytes.len, 80)] });
        return error.InvalidArgument;
    }
}

fn verifyNativeValues(db: *DB, alloc: std.mem.Allocator) !void {
    var reader = try db.beginRelationalRows(alloc, .{ .fields = &.{ "amount", "numbers" }, .primary_lower = .{ .key = "typed-values", .inclusive = true } });
    var open = true;
    defer if (open) reader.deinit();
    var page = try reader.nextTypedPage(alloc, null, .{ .native_cells = true, .rows = 1 });
    defer page.deinit();
    if (page.rows.len != 1 or page.owner_release == null) return error.InvalidArgument;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const point = (try reader.lookupSqlRow(a, "typed-values")) orelse return error.InvalidArgument;
    const layout = try local.sql_catalog.Row.TypedLayout.init(a, &.{ "amount", "numbers" });
    const batch = try reader.lookupSqlBatch(a, &.{"typed-values"}, layout, 1024 * 1024);
    if (batch.consumed != 1 or batch.rows.len != 1) return error.InvalidArgument;
    reader.deinit();
    open = false;
    try verifyNativeCells(a, page.rows[0].typed_cells orelse return error.InvalidArgument);
    try verifyNativeCells(a, point.typed_cells orelse return error.InvalidArgument);
    try verifyNativeCells(a, (batch.rows[0] orelse return error.InvalidArgument).typed_cells orelse return error.InvalidArgument);
}
fn verifyNativeCells(a: std.mem.Allocator, cells: local.sql_catalog.Row.TypedCells) !void {
    if (cells.columnar != null) return error.InvalidArgument;
    const number = (try cells.at(0)).numeric orelse return error.InvalidArgument;
    var context: local.sql_numeric_value.Context = .{ .alloc = a };
    const text = try local.sql_numeric_value.format(&context, number.*);
    if (!std.mem.eql(u8, text, "9007199254740993.2500")) return error.InvalidArgument;
    const array = (try cells.at(1)).array orelse return error.InvalidArgument;
    if (array.element_type != .int64 or array.dimensions.len != 1 or array.dimensions[0].lower != -3 or array.dimensions[0].length != 2 or array.stored == null or array.cardinality() != 2) return error.InvalidArgument;
    if (array.flatCell(0).value.integer != 9007199254740993 or !array.flatCell(1).sql_null) return error.InvalidArgument;
}
