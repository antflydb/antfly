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

//! Executable fixture: builtin.is_test is false, so real archive calls are exercised.
const std = @import("std");
const client = @import("storage_client");
const db_mod = client.capi.db_mod;
const engine = client.storage_engine;
pub fn main(init: std.process.Init) !void {
    const alloc = init.gpa;
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    const root = args.next() orelse return error.MissingProbeDirectory;
    try std.Io.Dir.cwd().deleteTree(init.io, root);
    try std.Io.Dir.cwd().createDirPath(init.io, root);
    const physical = try std.fs.path.join(alloc, &.{ root, "physical" });
    defer alloc.free(physical);
    try std.Io.Dir.cwd().deleteTree(init.io, physical);
    if (!engine.enabled) return error.ProbeRequiresSharedStorage;
    const descriptor = engine.descriptorForValidation();
    engine.validateDescriptor(descriptor) catch |err| {
        inline for (.{ "table_layout", "db_layout", "core_layout", "handle_layout", "registry_layout", "backend_runtime_layout", "resource_manager_layout", "prepared_owner_layout", "prepared_allocator_layout", "embedding_memo_layout", "async_context_layout", "enrichment_runtime_layout", "relationalReader_layout", "relationalSession_layout", "documentSession_layout", "index_manager_layout", "algebraic_index_layout", "docstore_layout", "rewrite_program_layout", "enrichment_set_layout", "backup_snapshot_layout", "resolution_runtime_layout", "promotion_runtime_layout", "pinned_snapshot_layout", "staged_generation_layout", "generation_manager_layout", "native_manifest_layout", "writer_layout" }) |field| {
            const actual = @field(descriptor, field);
            const expected = @field(engine, field);
            std.debug.print("storage ABI {s}: actual={any} expected={any}\n", .{ field, actual, expected });
        }
        for (descriptor.index_manager_fields[0..descriptor.index_manager_field_count], engine.index_manager_fields) |actual, expected| {
            if (!std.mem.eql(u8, std.mem.span(actual.type_name), std.mem.span(expected.type_name)) or actual.offset != expected.offset)
                std.debug.print("index manager field {s}: owner={s} offset={d}; client={s} offset={d}\n", .{ std.mem.span(actual.name), std.mem.span(actual.type_name), actual.offset, std.mem.span(expected.type_name), expected.offset });
        }
        for (descriptor.table_fields[0..descriptor.table_field_count], engine.table_fields) |actual, expected| {
            if (!std.mem.eql(u8, std.mem.span(actual.type_name), std.mem.span(expected.type_name)))
                std.debug.print("method signature {s}: owner={s}; client={s}\n", .{ std.mem.span(actual.name), std.mem.span(actual.type_name), std.mem.span(expected.type_name) });
        }
        for (descriptor.db_fields[0..descriptor.db_field_count], engine.db_fields) |actual, expected| {
            if (actual.offset != expected.offset or actual.size != expected.size or !std.mem.eql(u8, std.mem.span(actual.type_name), std.mem.span(expected.type_name)))
                std.debug.print("DB field {s}: owner offset={d} size={d} type={s}; client offset={d} size={d} type={s}\n", .{ std.mem.span(actual.name), actual.offset, actual.size, std.mem.span(actual.type_name), expected.offset, expected.size, std.mem.span(expected.type_name) });
        }
        for (descriptor.core_fields[0..descriptor.core_field_count], engine.core_fields) |actual, expected| {
            if (actual.offset != expected.offset or actual.size != expected.size or !std.mem.eql(u8, std.mem.span(actual.type_name), std.mem.span(expected.type_name)))
                std.debug.print("Core field {s}: owner offset={d} size={d} type={s}; client offset={d} size={d} type={s}\n", .{ std.mem.span(actual.name), actual.offset, actual.size, std.mem.span(actual.type_name), expected.offset, expected.size, std.mem.span(expected.type_name) });
        }
        inline for (.{ "backend", "resource" }) |kind| {
            for (@field(descriptor, kind ++ "_fields")[0..@field(descriptor, kind ++ "_field_count")], @field(engine, kind ++ "_fields")) |actual, expected| {
                if (actual.offset != expected.offset or actual.size != expected.size or !std.mem.eql(u8, std.mem.span(actual.type_name), std.mem.span(expected.type_name)))
                    std.debug.print("{s} field {s}: owner offset={d} size={d} type={s}; client offset={d} size={d} type={s}\n", .{ kind, std.mem.span(actual.name), actual.offset, actual.size, std.mem.span(actual.type_name), expected.offset, expected.size, std.mem.span(expected.type_name) });
            }
        }
        return err;
    };
    var incompatible = engine.descriptorForValidation().*;
    incompatible.db_layout.size += 1;
    engine.validateDescriptor(&incompatible) catch |err| {
        if (err != error.UnsupportedVersion) return err;
        try exerciseBorrowedRuntime(alloc, root, init.io);
        return exercise(alloc, root, physical, init.io);
    };
    return error.AcceptedIncompatibleStorageLayout;
}
fn exercise(alloc: std.mem.Allocator, root: []const u8, physical: []const u8, io: std.Io) !void {
    const db = try db_mod.DB.openOwned(alloc, physical, .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.closeOwned();
    try db.batch(.{ .writes = &.{.{ .key = "shared-row", .value = "{\"n\":1}" }}, .sync_level = .write });
    const value = (try db.get(alloc, "shared-row")) orelse return error.SharedStorageReadBackFailed;
    defer alloc.free(value);
    if (!std.mem.eql(u8, value, "{\"n\":1}")) return error.SharedStorageReadBackFailed;
    var looked_up = (try db.lookup(alloc, "shared-row", .{})) orelse return error.SharedStorageReadBackFailed;
    defer looked_up.deinit(alloc);
    if (!std.mem.eql(u8, looked_up.json, value)) return error.SharedStorageReadBackFailed;
    var scanned = try db.scan(alloc, "", "", .{ .include_documents = true });
    defer scanned.deinit(alloc);
    if (scanned.documents.len != 1 or try db.primaryDocCount(alloc) != 1) return error.SharedStorageReadBackFailed;
    var result = try db.search(alloc, .{});
    defer result.deinit();
    if (result.hits.len != 1) return error.SharedStorageQueryFailed;
    if (try db_mod.DB.readRestoreStateForPathWithIo(alloc, io, root)) |_| return error.SharedStorageInvalidBatchAccepted;
    const RangeArgs = std.meta.ArgsTuple(@typeInfo(@FieldType(engine.VTable, "portableDecoderBindRange")).pointer.child);
    const range_args: RangeArgs = .{ alloc, db.core.store, .{ .start = "", .end = "" } };
    try engine.call("portableDecoderBindRange", range_args);
    try engine.call("portableDecoderBindRange", range_args);
    try exerciseRelationalLifetime(alloc, root, io);
    try exerciseForeignScan(alloc, db);
    try exerciseReplicatedBatch(alloc, db);
    try db.sync(true);
    try db.reconfigureEnrichmentRuntimePaused(.{});
    var runtime_options: db_mod.OpenOptions = .{ .enrichment = .{} };
    if (try db.createDetachedEnrichmentRuntime(&runtime_options.enrichment.?)) |_| return error.SharedStorageInvalidBatchAccepted;
    const ReplayArgs = std.meta.ArgsTuple(@typeInfo(@FieldType(engine.VTable, "applyDerivedBatchToIndexAsync")).pointer.child);
    const replay_args: ReplayArgs = .{ db.async_context, .{}, .{ .name = "vector", .kind = .dense_vector }, .{} };
    if (try engine.call("applyDerivedBatchToIndexAsync", replay_args)) return error.SharedStorageInvalidBatchAccepted;
    const transaction = try db.beginTransaction(100);
    try db.writeTransaction(transaction, .{ .writes = &.{.{ .key = "transaction-row", .value = "{\"n\":3}" }} });
    try db.commitTransaction(transaction, 101);
    const committed = (try db.get(alloc, "transaction-row")) orelse return error.SharedStorageReadBackFailed;
    defer alloc.free(committed);
    if (!std.mem.eql(u8, committed, "{\"n\":3}")) return error.SharedStorageReadBackFailed;
    try exercisePreparedWorker(alloc, db);
    try exerciseFactoryAggregationBackup(alloc, db, root, physical, io);
    var portable = std.ArrayList(u8).empty;
    defer portable.deinit(alloc);
    try engine.call("portableExport", .{ alloc, db.core.store, &portable });
    if (portable.items.len == 0) return error.SharedStorageReadBackFailed;
    var streamed: std.Io.Writer.Allocating = .init(alloc);
    defer streamed.deinit();
    const ExportOptions = @FieldType(engine.PortableExportOptions, "value");
    try engine.call("portableExportToWriterWithOptions", .{ alloc, db.core.store, &streamed.writer, ExportOptions{} });
    if (!std.mem.eql(u8, portable.items, streamed.written())) return error.SharedStorageReadBackFailed;
    const Failing = struct {
        fn drain(_: *std.Io.Writer, _: []const []const u8, _: usize) std.Io.Writer.Error!usize {
            return error.WriteFailed;
        }
    };
    var failing_writer: std.Io.Writer = .{ .buffer = &.{}, .vtable = &.{ .drain = Failing.drain } };
    if (engine.call("portableExportToWriterWithOptions", .{ alloc, db.core.store, &failing_writer, ExportOptions{} })) |_| {
        return error.SharedStorageErrorIdentityLost;
    } else |err| if (err != error.WriteFailed) return error.SharedStorageErrorIdentityLost;
    const spool_path = try std.fs.path.join(alloc, &.{ root, "native-export-spool" });
    defer alloc.free(spool_path);
    const spool = try std.Io.Dir.cwd().createFile(io, spool_path, .{ .read = true });
    defer spool.close(io);
    streamed.clearRetainingCapacity();
    try engine.call("portableExportToWriterWithOptions", .{ alloc, db.core.store, &streamed.writer, ExportOptions{ .spool = .{ .io = io, .file = spool } } });
    if (!std.mem.eql(u8, portable.items, streamed.written())) return error.SharedStorageReadBackFailed;
    try exerciseForeignPredicate(alloc, db);
    try exerciseForeignHook(db);
    db.batch(.{ .row_policy_install_bundle = "reject-this-private-command" }) catch |err| {
        if (err != error.InvalidBatchRequest) return error.SharedStorageErrorIdentityLost;
        try exerciseServerHandle(alloc, root);
        return exerciseCapi(alloc, root);
    };
    return error.SharedStorageInvalidBatchAccepted;
}
fn exerciseCapi(alloc: std.mem.Allocator, root: []const u8) !void {
    const capi = client.capi;
    const path = try std.fs.path.join(alloc, &.{ root, "public.aflite" });
    defer alloc.free(path);
    const terminated = try alloc.dupeSentinel(u8, path, 0);
    defer alloc.free(terminated);
    var handle: ?*anyopaque = null;
    if (capi.antfly_lite_create(terminated, &handle) != .ok) return error.SharedStorageCapiOpenFailed;
    defer capi.antfly_db_close(handle);
    if (capi.asHandle(handle) == null) return error.SharedStorageRegistryNotShared;
    var out: capi.ApiTypes.Buffer = .{};
    defer capi.antfly_buffer_free(&out);
    const bad = "invalid-json";
    const status = capi.antfly_db_batch_json(handle, .{ .ptr = bad.ptr, .len = bad.len }, &out);
    if (status == .ok) return error.SharedStorageInvalidJsonAccepted;
    if (capi.ApiTypes.lastErrorCode() != status or std.mem.span(capi.ApiTypes.lastErrorDetail()).len == 0)
        return error.SharedStorageErrorStateNotShared;
    std.debug.print("shared storage lifecycle, batch/query, named failure, registry/TLS and incompatible-layout checks passed\n", .{});
}

fn exerciseServerHandle(alloc: std.mem.Allocator, root: []const u8) !void {
    const capi = client.capi;
    const path = try std.fs.path.join(alloc, &.{ root, "server-created" });
    defer alloc.free(path);
    const handle = try alloc.create(capi.Handle);
    errdefer alloc.destroy(handle);
    handle.* = .{ .alloc = alloc, .db = try db_mod.DB.open(alloc, path, .{ .start_optional_runtimes = false, .start_index_workers = false }) };
    errdefer handle.db.close();
    try handle.db.batch(.{ .writes = &.{.{ .key = "from-server", .value = "{\"origin\":\"server\"}" }}, .sync_level = .write });
    const id = try capi.handleRegistry().register(handle);
    defer capi.antfly_db_close(id);
    const key = "from-server";
    var out: capi.ApiTypes.Buffer = .{};
    defer capi.antfly_buffer_free(&out);
    if (capi.antfly_db_lookup_json(id, .{ .ptr = key.ptr, .len = key.len }, &out) != .ok)
        return error.SharedStorageServerHandleRejected;
    if (std.mem.indexOf(u8, out.ptr.?[0..out.len], "server") == null)
        return error.SharedStorageServerHandleReadFailed;
}

fn exerciseForeignHook(db: *db_mod.DB) !void {
    const Callback = struct {
        fn observe(ptr: *anyopaque, _: []const u8) anyerror!void {
            const count: *usize = @ptrCast(@alignCast(ptr));
            count.* += 1;
            return error.TestWindowFailed;
        }
    };
    var count: usize = 0;
    db.batchInternal(.{ .writes = &.{.{ .key = "callback-row", .value = "{}" }}, .sync_level = .write }, null, .{ .committed_batch_effects_observer = .{ .ptr = &count, .apply = Callback.observe } }) catch |err| {
        if (err != error.TestWindowFailed or count != 1) return error.SharedStorageErrorIdentityLost;
        return;
    };
    return error.SharedStorageInvalidBatchAccepted;
}

fn exerciseForeignScan(alloc: std.mem.Allocator, db: *db_mod.DB) !void {
    const Visitor = struct {
        fn visit(ptr: ?*anyopaque, entry: db_mod.types.ScanVisitEntry) anyerror!void {
            const count: *usize = @ptrCast(@alignCast(ptr.?));
            if (!std.mem.eql(u8, entry.id, "shared-row")) return error.SharedStorageReadBackFailed;
            count.* += 1;
        }
        fn fail(ptr: ?*anyopaque, _: db_mod.types.ScanVisitEntry) anyerror!void {
            const count: *usize = @ptrCast(@alignCast(ptr.?));
            count.* += 1;
            return error.TestWindowFailed;
        }
    };
    var count: usize = 0;
    try db.scanVisit(alloc, "", "", .{}, .{ .context = &count, .visit = Visitor.visit });
    if (count != 1) return error.SharedStorageReadBackFailed;
    count = 0;
    db.scanVisit(alloc, "", "", .{}, .{ .context = &count, .visit = Visitor.fail }) catch |err| {
        if (err != error.TestWindowFailed or count != 1) return error.SharedStorageErrorIdentityLost;
        return;
    };
    return error.SharedStorageInvalidBatchAccepted;
}

fn exerciseReplicatedBatch(alloc: std.mem.Allocator, db: *db_mod.DB) !void {
    try db.applyReplicatedBatch(.{ .request = .{ .writes = &.{.{ .key = "replicated-row", .value = "{}" }} }, .receipt = .none }, 1);
    const value = (try db.get(alloc, "replicated-row")) orelse return error.SharedStorageReadBackFailed;
    defer alloc.free(value);
    if (!std.mem.eql(u8, value, "{}")) return error.SharedStorageReadBackFailed;
}

fn exerciseBorrowedRuntime(alloc: std.mem.Allocator, root: []const u8, io: std.Io) !void {
    var runtime = try db_mod.background_runtime.BackendRuntimeHandle.init(alloc, .{ .backend = .manual, .borrowed_io = .{ .general = io } });
    defer runtime.deinit();
    const path = try std.fs.path.join(alloc, &.{ root, "borrowed-runtime" });
    defer alloc.free(path);
    const db = try db_mod.DB.openOwned(alloc, path, .{ .backend_runtime = runtime.ptr(), .start_optional_runtimes = false, .start_index_workers = false });
    defer db.closeOwned();
    try db.batch(.{ .writes = &.{.{ .key = "borrowed-row", .value = "{}" }}, .sync_level = .write });
    const value = (try db.get(alloc, "borrowed-row")) orelse return error.SharedStorageReadBackFailed;
    defer alloc.free(value);
    var options: db_mod.OpenOptions = .{ .enrichment = .{ .enable_without_producers = true } };
    try db.initOptionalRuntimes(&options);
    const enrichment = db.enrichment_runtime orelse return error.SharedStorageReadBackFailed;
    try enrichment.reloadDurableState();
    try enrichment.catchUpUntil(0);
    try enrichment.waitForAppliedWithVisibilityDeadline(0, .{}, null);
    var pinned = try db.core.pinPortableSnapshot();
    defer pinned.deinit();
    const destination = try std.fs.path.join(alloc, &.{ root, "pinned-materialization" });
    defer alloc.free(destination);
    try std.Io.Dir.cwd().createDirPath(io, destination);
    const Receipt = struct {
        calls: usize = 0,
        fail: bool = false,
        fn record(raw: *anyopaque, _: []const u8, size: u64, _: [32]u8) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (size == 0) return error.SharedStorageReadBackFailed;
            if (self.fail) return error.TestWindowFailed;
        }
    };
    var receipt: Receipt = .{};
    if (try pinned.materializeWithSink(alloc, io, destination, .none, .{ .ptr = &receipt, .record_fn = Receipt.record }) == 0 or receipt.calls == 0)
        return error.SharedStorageReadBackFailed;
    receipt.fail = true;
    if (pinned.materializeWithSink(alloc, io, destination, .none, .{ .ptr = &receipt, .record_fn = Receipt.record })) |_| {
        return error.SharedStorageErrorIdentityLost;
    } else |err| if (err != error.TestWindowFailed) return error.SharedStorageErrorIdentityLost;
    _ = try pinned.materializeWithSink(alloc, io, destination, .none, null);
    const Function = @typeInfo(@FieldType(engine.VTable, "restoreSnapshotToLocalDeferredRuntimeRepairWithIoAndCancellation")).pointer.child;
    const Stage = @typeInfo(@typeInfo(Function).@"fn".param_types[0].?).pointer.child;
    const Manager = @typeInfo(@FieldType(Stage, "manager")).pointer.child;
    var manager = Manager.init(alloc);
    defer manager.deinit();
    const restore_path = try std.fs.path.join(alloc, &.{ root, "native-restore" });
    defer alloc.free(restore_path);
    var transition = try manager.beginExclusive(restore_path);
    defer transition.deinit();
    var staged = try transition.beginStaging();
    defer staged.deinit();
    const restore_options: db_mod.OpenOptions = .{ .backend_runtime = runtime.ptr(), .staged_generation = &staged, .start_optional_runtimes = false, .start_index_workers = false };
    _ = try db_mod.DB.restoreSnapshotToLocalDeferredRuntimeRepairWithIoAndCancellation(&staged, alloc, io, destination, staged.path(), restore_options, .none);
    var restored = try db_mod.DB.open(alloc, staged.path(), restore_options);
    defer restored.close();
    const restored_value = (try restored.get(alloc, "borrowed-row")) orelse return error.SharedStorageReadBackFailed;
    defer alloc.free(restored_value);
    if (!std.mem.eql(u8, restored_value, "{}")) return error.SharedStorageReadBackFailed;
}

fn exerciseForeignPredicate(alloc: std.mem.Allocator, db: *db_mod.DB) !void {
    try db.addIndex(.{ .name = "vector", .kind = .dense_vector, .config_json = "{\"field\":\"v\",\"dims\":2}" });
    try db.batch(.{ .writes = &.{.{ .key = "vector-row", .value = "{\"v\":[0,0]}" }}, .sync_level = .full_index });
    const Callback = struct {
        fn allows(ptr: *anyopaque, _: []const u8) anyerror!bool {
            const count: *usize = @ptrCast(@alignCast(ptr));
            count.* += 1;
            return error.TestWindowFailed;
        }
    };
    const Accept = struct {
        fn allows(ptr: *anyopaque, _: []const u8) anyerror!bool {
            const count: *usize = @ptrCast(@alignCast(ptr));
            count.* += 1;
            return true;
        }
    };
    var accepted: usize = 0;
    var captured = try db.searchWithCapturedRequest(alloc, .{ .native_key_predicate = .{ .ptr = &accepted, .allows = Accept.allows } });
    defer captured.result.deinit();
    const retained = captured.request.native_key_predicate orelse return error.SharedStorageErrorIdentityLost;
    if (retained.ptr != @as(*anyopaque, @ptrCast(&accepted)) or retained.allows != Accept.allows)
        return error.SharedStorageErrorIdentityLost;
    if (!try retained.allows(retained.ptr, "vector-row")) return error.SharedStorageErrorIdentityLost;
    var count: usize = 0;
    const snapshot = try db.searchRequestAtCurrentIdentityGeneration(.{ .native_key_predicate = .{ .ptr = &count, .allows = Callback.allows } });
    const predicate = snapshot.native_key_predicate orelse return error.SharedStorageErrorIdentityLost;
    if (predicate.ptr != @as(*anyopaque, @ptrCast(&count)) or predicate.allows != Callback.allows)
        return error.SharedStorageErrorIdentityLost;
    _ = predicate.allows(predicate.ptr, "vector-row") catch |err| {
        if (err != error.TestWindowFailed or count != 1) return error.SharedStorageErrorIdentityLost;
    };
    if (count != 1) return error.SharedStorageErrorIdentityLost;
    count = 0;
    var result = db.search(alloc, .{ .dense = .{ .vector = &.{ 0, 0 }, .k = 1 }, .native_key_predicate = .{ .ptr = &count, .allows = Callback.allows } }) catch |err| {
        if (err != error.TestWindowFailed or count == 0) return error.SharedStorageErrorIdentityLost;
        _ = db.searchDenseProfiled(alloc, .{ .native_key_predicate = .{ .ptr = &count, .allows = Callback.allows } }, .{ .vector = &.{ 0, 0 }, .k = 1 }) catch |profile_error| {
            if (profile_error != error.TestWindowFailed) return error.SharedStorageErrorIdentityLost;
            return;
        };
        return error.SharedStorageInvalidBatchAccepted;
    };
    result.deinit();
    return error.SharedStorageInvalidBatchAccepted;
}

fn exercisePreparedWorker(alloc: std.mem.Allocator, db: *db_mod.DB) !void {
    const Function = @typeInfo(@FieldType(engine.VTable, "batchPrepared")).pointer.child;
    const Args = std.meta.ArgsTuple(Function);
    const Memo = @typeInfo(@FieldType(Args, "4")).pointer.child;
    const Guard = @typeInfo(@FieldType(Args, "5")).pointer.child;
    var memo = Memo.init(alloc);
    defer memo.deinit();
    var guard: Guard = .{ .child = alloc, .io = db.backend_runtime.io().? };
    try db.batchInternalPrepared(.{ .writes = &.{.{ .key = "prepared-row", .value = "{}" }}, .sync_level = .write }, null, .{}, &memo, &guard);
    const value = (try db.get(alloc, "prepared-row")) orelse return error.SharedStorageReadBackFailed;
    defer alloc.free(value);
}

// Exercise the real owner with a client-created filter retained across pages.
// builtin.is_test is false here, unlike direct implementation unit tests.
fn exerciseRelationalLifetime(alloc: std.mem.Allocator, root: []const u8, io: std.Io) !void {
    const path = try std.fs.path.join(alloc, &.{ root, "relational" });
    defer alloc.free(path);
    const db = try db_mod.DB.openOwned(alloc, path, .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.closeOwned();
    try db.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer"}},"additionalProperties":false}}}}
    );
    try db.batch(.{ .writes = &.{ .{ .key = "a", .value = "{\"n\":1}" }, .{ .key = "b", .value = "{\"n\":2}" } }, .sync_level = .write });
    var fence = (try db.tryStatementReadFence()) orelse return error.SharedStorageQueryFailed;
    var cut = try db.captureRelationalStatementSnapshot();
    fence.release();
    defer cut.deinit();
    const Filter = struct {
        fail: bool = false,
        visits: usize = 0,
        fn matches(raw: *anyopaque, _: std.mem.Allocator, _: []const u8, _: @typeInfo(@typeInfo(@FieldType(db_mod.DB.RelationalRows.RowFilter, "matches")).pointer.child).@"fn".param_types[3].?) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.visits += 1;
            if (self.fail) return error.TestWindowFailed;
            return true;
        }
    };
    var filter: Filter = .{};
    var reader = try db.beginRelationalRows(alloc, .{ .fields = &.{"n"}, .row_filter = .{ .context = &filter, .matches = Filter.matches } });
    defer reader.deinit();
    var page = try reader.nextTypedPage(alloc, io, .{ .rows = 1 });
    defer page.deinit();
    if (page.rows.len != 1) return error.SharedStorageQueryFailed;
    var second = try reader.nextTypedPage(alloc, io, .{ .rows = 1 });
    defer second.deinit();
    if (second.rows.len != 1) return error.SharedStorageQueryFailed;
    if (filter.visits != 2) return error.SharedStorageQueryFailed;
    var failing = try db.beginRelationalRows(alloc, .{ .fields = &.{"n"}, .row_filter = .{ .context = &filter, .matches = Filter.matches } });
    defer failing.deinit();
    filter.fail = true;
    if (failing.nextTypedPage(alloc, null, .{})) |unexpected| {
        var cleanup = unexpected;
        cleanup.deinit();
        return error.SharedStorageErrorIdentityLost;
    } else |err| if (err != error.TestWindowFailed) return error.SharedStorageErrorIdentityLost;
    try db.catchUpPendingDerivedReplay();
}

fn exerciseFactoryAggregationBackup(alloc: std.mem.Allocator, database: *db_mod.DB, root: []const u8, physical: []const u8, io: std.Io) !void {
    const write = client.antfly_sources.local_write;
    var enrichment = try write.createManagedDbEnrichments(alloc, "{}", database.backend_runtime, null, null, null, "", null, null);
    defer enrichment.deinit(alloc);
    if (enrichment.generated) return error.SharedStorageInvalidBatchAccepted;
    var result = try database.search(alloc, .{ .include_stored = true });
    defer result.deinit();
    const aggregations = try db_mod.aggregations.computeSearchAggregations(alloc, &.{.{ .name = "total", .type = "sum", .field = "n" }}, result, .{ .index_manager = database.core.index_manager, .doc_store = database.core.store });
    defer db_mod.aggregations.deinitResults(alloc, aggregations);
    if (aggregations.len != 1) return error.NativeAggregationCountMismatch;
    if (!std.mem.eql(u8, aggregations[0].value_json orelse return error.NativeAggregationMissingValue, "4")) {
        std.debug.print("native aggregation hits={d} value={s}\n", .{ result.hits.len, aggregations[0].value_json.? });
        return error.NativeAggregationValueMismatch;
    }
    const destination = try std.fs.path.join(alloc, &.{ root, "native-backup" });
    defer alloc.free(destination);
    var snapshot = try write.prepareNativeBackupShardSnapshot(alloc, database, physical, 1, .{ .backup_root = destination, .backup_id = "probe", .io = io });
    defer snapshot.deinit(alloc);
    if (snapshot.owned_io_receiver == null) return error.NativeBackupReceiverMissing;
    // Preparation has returned. Copy and destruction must retain the same
    // stable receiving I/O rather than borrowing a dead preparation frame.
    const shards = try write.copyPreparedNativeBackupShardSnapshot(alloc, &snapshot);
    defer write.freeBackupShards(alloc, shards);
    if (shards.len != 1 or shards[0].artifact_size_bytes == 0) return error.SharedStorageReadBackFailed;
}
