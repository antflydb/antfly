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

//! End-to-end LSM lifecycle and standby contracts, plus reproducible work counts.
const std = @import("std");
const db_mod = @import("mod.zig");
const rows = @import("relational_rows.zig");
const records = @import("relational_index_records.zig");
const internal = @import("../internal_keys.zig");
const primary_mod = @import("../hot_standby/primary.zig");
const time = @import("antfly_platform").time;
const alloc = std.testing.allocator;

fn mixedScanAllocations(test_alloc: std.mem.Allocator, db: *db_mod.DB) !void {
    var reader = try db.beginRelationalRows(test_alloc, .{ .fields = &.{"payload"} });
    defer reader.deinit();
    var page = try reader.nextPage(test_alloc, null, .{ .rows = 2, .time_ns = std.time.ns_per_s });
    defer page.deinit();
    try std.testing.expectEqual(@as(usize, 2), page.rows.len);
    try std.testing.expectEqual(@as(usize, 2), reader.source_compilations);
}

test "relational index system historical scan evicts bounded snapshot bindings" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("historical-binding-eviction");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false, .primary_backend = .{ .lsm = .{} } });
    defer db.close();
    for (1..11) |version| {
        try install(&db, @intCast(version), false);
        try write(&db, version, 1);
    }
    var reader = try db.beginRelationalRows(alloc, .{ .fields = &.{"id"} });
    defer reader.deinit();
    var count: usize = 0;
    while (true) {
        var page = try reader.nextPage(alloc, std.testing.io, .{ .rows = 1 });
        defer page.deinit();
        count += page.rows.len;
        if (!page.more) break;
    }
    try std.testing.expectEqual(@as(usize, 10), count);
    try std.testing.expectEqual(@as(usize, 10), reader.source_compilations);
    var retained: usize = 0;
    for (reader.bindings) |binding| if (binding != null) {
        retained += 1;
    };
    try std.testing.expectEqual(@as(usize, 8), retained);
    try std.testing.expect(reader.binding_bytes <= 2 * 1024 * 1024);
}

test "relational index system mixed schema scan compiles each snapshot layout once" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("mixed-schema-bindings");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false, .primary_backend = .{ .lsm = .{} } });
    defer db.close();
    try install(&db, 1, false);
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    const writes = try owned.alloc(db_mod.types.BatchWrite, 512);
    for (writes, 0..) |*item, i| item.* = .{ .key = try std.fmt.allocPrint(owned, "{d:0>4}", .{i}), .value = "{\"tenant\":1,\"id\":7,\"payload\":\"old\"}" };
    try db.batch(.{ .writes = writes });
    try install(&db, 2, false);
    const updates = try owned.alloc(db_mod.types.BatchWrite, writes.len / 2);
    for (updates, 0..) |*item, i| item.* = .{ .key = writes[i * 2 + 1].key, .value = "{\"tenant\":1,\"id\":7,\"payload\":\"new\"}" };
    try db.batch(.{ .writes = updates });
    var reader = try db.beginRelationalRows(alloc, .{ .fields = &.{"payload"}, .conditions = &.{.{ .column = "id", .op = .gte, .value = .{ .integer = 7 } }} });
    defer reader.deinit();
    // Live publication must not change either the source layouts or rows in
    // this reader; cache identity is the read snapshot, never a global epoch ID.
    try install(&db, 3, false);
    var count: usize = 0;
    const started = time.monotonicNs();
    while (true) {
        var page = try reader.nextPage(alloc, std.testing.io, .{ .rows = 7 });
        defer page.deinit();
        for (page.rows) |row| {
            try std.testing.expectEqualStrings(if (count % 2 == 0) "{\"payload\":\"old\"}" else "{\"payload\":\"new\"}", row.json);
            count += 1;
        }
        if (!page.more) break;
    }
    try std.testing.expectEqual(@as(usize, 512), count);
    try std.testing.expectEqual(@as(usize, 2), reader.source_compilations);
    try std.testing.expect(reader.source_cache_hits >= 510);
    try std.testing.expect(reader.binding_bytes <= 2 * 1024 * 1024);
    std.debug.print("mixed schema LSM scan: rows={d} compilations={d} hits={d} elapsed_us={d}\n", .{ count, reader.source_compilations, reader.source_cache_hits, (time.monotonicNs() - started) / 1000 });
    try std.testing.checkAllAllocationFailures(alloc, mixedScanAllocations, .{&db});
}

fn expressionKeyAllocations(test_alloc: std.mem.Allocator) !void {
    const schema_mod = @import("../schema.zig");
    const codec = @import("algebraic/relational_row_codec.zig");
    const tuples = @import("relational_index_keys.zig");
    const native = @import("../relational_index.zig");
    const table: schema_mod.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{
        .{ .name = "id", .path = "id", .column_type = .integer },
        .{ .name = "price", .path = "price", .column_type = .integer, .allows_null = true },
        .{ .name = "label", .path = "label", .column_type = .string },
    } };
    var layout = try codec.PhysicalLayout.init(test_alloc, table);
    defer layout.deinit();
    const keys = [_]native.RelationalIndexKey{
        .{ .expression_json = "{\"op\":\"multiply\",\"args\":[{\"op\":\"column\",\"column\":\"price\"},{\"op\":\"literal\",\"type\":\"integer\",\"value\":\"2\"}]}", .result_type = .integer },
        .{ .column = "id", .direction = .desc },
        .{ .expression_json = "{\"op\":\"lower_ascii\",\"args\":[{\"op\":\"column\",\"column\":\"label\"}]}", .result_type = .string },
        .{ .expression_json = "{\"op\":\"literal\",\"type\":\"integer\",\"value\":null}", .result_type = .integer },
    };
    var plan = try tuples.TuplePlan.init(test_alloc, table, &layout, &keys);
    defer plan.deinit();
    var prepared = try @import("document_mapper.zig").PreparedRelationalWrite.init(test_alloc, "a", "{\"id\":7,\"price\":9007199254740993,\"label\":\"ALPHA\"}", null, table, &layout);
    defer prepared.deinit(test_alloc);
    var encoded = try plan.encodeAlloc(test_alloc, try prepared.typedView(table, &layout));
    defer encoded.deinit(test_alloc);
    var bounds = std.ArrayList(u8).empty;
    defer bounds.deinit(test_alloc);
    _ = try plan.appendValues(test_alloc, &bounds, &.{ .{ .integer = 18014398509481986 }, .{ .integer = 7 }, .{ .string = "alpha" }, .null });
    try std.testing.expect(encoded.has_null);
    try std.testing.expectEqualSlices(u8, bounds.items, encoded.bytes);
    const historical: schema_mod.TableSchema = .{ .version = 2, .storage_mode = .relational, .relational_columns = &.{ table.relational_columns[2], table.relational_columns[1], table.relational_columns[0] } };
    var old_layout = try codec.PhysicalLayout.init(test_alloc, historical);
    defer old_layout.deinit();
    var projected = try plan.projectSource(test_alloc, historical, &old_layout);
    defer projected.deinit();
    var old_prepared = try @import("document_mapper.zig").PreparedRelationalWrite.init(test_alloc, "a", "{\"id\":7,\"price\":9007199254740993,\"label\":\"ALPHA\"}", null, historical, &old_layout);
    defer old_prepared.deinit(test_alloc);
    var old_encoded = try projected.encodeAlloc(test_alloc, try old_prepared.typedView(historical, &old_layout));
    defer old_encoded.deinit(test_alloc);
    try std.testing.expectEqualSlices(u8, encoded.bytes, old_encoded.bytes);
    var cover = try @import("relational_index_cover.zig").Plan.init(test_alloc, table, &layout, &keys, &.{"label"});
    defer cover.deinit();
    try std.testing.expectEqual(@as(usize, 2), cover.columns.len);
    try std.testing.expect(cover.contains("id") and cover.contains("label"));
    try std.testing.expect(!cover.contains("price"));
    const payload = try cover.encode(test_alloc, try prepared.typedView(table, &layout));
    defer test_alloc.free(payload);
    _ = try cover.decode(payload);
}

test "relational index system expression keys share typed bounds historical projections and allocation cleanup" {
    try std.testing.checkAllAllocationFailures(alloc, expressionKeyAllocations, .{});
}

test "relational index system expression keys fence declarations dependency changes and aggregate expansion" {
    const schema_mod = @import("../schema.zig");
    const codec = @import("algebraic/relational_row_codec.zig");
    const tuples = @import("relational_index_keys.zig");
    const native = @import("../relational_index.zig");
    const table: schema_mod.TableSchema = .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{.{ .name = "label", .path = "label", .column_type = .string }} };
    var layout = try codec.PhysicalLayout.init(alloc, table);
    defer layout.deinit();
    const expression = "{\"op\":\"column\",\"column\":\"label\"}";
    const key: native.RelationalIndexKey = .{ .expression_json = expression, .result_type = .string };
    for ([_]native.RelationalIndexKey{
        .{},                                .{ .column = "label", .expression_json = expression, .result_type = .string },
        .{ .expression_json = expression }, .{ .column = "label", .result_type = .string },
    }) |invalid| try std.testing.expectError(error.InvalidRelationalIndexDefinition, tuples.TuplePlan.init(alloc, table, &layout, &.{invalid}));
    var plan = try tuples.TuplePlan.init(alloc, table, &layout, &.{ key, key, key });
    defer plan.deinit();
    const incompatible: schema_mod.TableSchema = .{ .version = 2, .storage_mode = .relational, .relational_columns = &.{.{ .name = "label", .path = "different.path", .column_type = .string }} };
    var changed = try codec.PhysicalLayout.init(alloc, incompatible);
    defer changed.deinit();
    try std.testing.expectError(error.RelationalIndexColumnTypeMismatch, plan.projectSource(alloc, incompatible, &changed));
    const wide = try alloc.alloc(u8, @import("../../schema/relational_expression.zig").max_output_bytes);
    defer alloc.free(wide);
    @memset(wide, 'x');
    const json = try std.json.Stringify.valueAlloc(alloc, .{ .label = wide }, .{});
    defer alloc.free(json);
    var prepared = try @import("document_mapper.zig").PreparedRelationalWrite.init(alloc, "a", json, null, table, &layout);
    defer prepared.deinit(alloc);
    var output = std.ArrayList(u8).empty;
    defer output.deinit(alloc);
    try output.appendSlice(alloc, "prefix");
    try std.testing.expectError(error.RelationalExpressionBudgetExceeded, plan.append(alloc, &output, try prepared.typedView(table, &layout)));
    try std.testing.expectEqualStrings("prefix", output.items);
}

const expression_index_schema =
    \\{"version":2,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"tenant_id","keys":[{"column":"tenant"},{"expression":{"op":"multiply","args":[{"op":"column","column":"price"},{"op":"literal","type":"integer","value":"2"}]},"result_type":"integer"},{"column":"id","direction":"desc"}],"include_columns":["price","payload"],"where":[{"column":"tenant","op":"gt","value":0}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant":{"type":"integer"},"id":{"type":"integer"},"price":{"type":"integer"},"payload":{"type":"keyword"}},"additionalProperties":false}}}}
;

fn expressionIndexCount(db: *db_mod.DB, doubled_price: i64) !usize {
    var reader = try db.beginRelationalRows(alloc, .{ .index = "tenant_id", .fields = &.{ "id", "price", "payload" }, .lower = .{ .values = &.{ .{ .integer = 1 }, .{ .integer = doubled_price } } }, .upper = .{ .values = &.{ .{ .integer = 1 }, .{ .integer = doubled_price } } }, .conditions = &.{.{ .column = "tenant", .op = .gt, .value = .{ .integer = 0 } }} });
    defer reader.deinit();
    var count: usize = 0;
    for (0..32) |_| {
        var page = try reader.nextPage(alloc, std.testing.io, .{ .rows = 100, .records = 100 });
        defer page.deinit();
        try std.testing.expectEqual(@as(usize, 0), page.primary_lookups);
        count += page.rows.len;
        if (!page.more) return count;
    }
    return error.RowScanDidNotConverge;
}

/// Simulate a replica whose durable primary checkpoint precedes its local
/// coverage checkpoint. No replicated schema/restore receipt is changed.
fn resetRestoreIndexCoverage(db: *db_mod.DB, failed: bool) !void {
    const jobs = @import("relational_index_jobs.zig");
    var pinned = db.core.relational_indexes.acquire().?;
    defer pinned.deinit();
    const index = pinned.plan.boundIndexes()[0];
    var txn = try db.core.store.beginWriteTxn();
    errdefer txn.abort();
    var progress = try jobs.resetProgress(try jobs.status(&txn, index));
    if (failed) {
        progress.state = .failed;
        progress.failure = .invalid_row;
    }
    const bytes = try progress.encode(alloc);
    defer alloc.free(bytes);
    try txn.put(&jobs.progressKey(index.id()), bytes);
    try txn.commit();
    try db.core.store.sync(true);
}

test "relational index system restore replay projection bounds allocation for wide ordinary batches" {
    const effects = @import("../hot_standby/effects.zig");
    const wide = try alloc.alloc(u8, 1024 * 1024);
    defer alloc.free(wide);
    @memset(wide, '\\'); // escaped payload is larger than either scratch budget
    const payload = try effects.encodeBatchMutationRequestAlloc(alloc, .{ .writes = &.{.{ .key = "wide", .value = wide }} });
    defer alloc.free(payload);
    var scratch: [16 * 1024]u8 = undefined;
    var bounded = std.heap.FixedBufferAllocator.init(&scratch);
    const record: @import("../hot_standby/replication_record.zig").RecordView = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 1, .previous_lsn = 0, .payload = payload };
    try std.testing.expectEqual(null, try effects.decodeRestoreFinishForReplay(bounded.allocator(), record));
}

test "relational index system restore receipts require local coverage through failure and owner restart" {
    const staging = @import("restore_staging.zig");
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    var source_directory = try TestDirectory.init("restore-index-coverage-source");
    defer source_directory.cleanup();
    var target_directory = try TestDirectory.init("restore-index-coverage-target");
    defer target_directory.cleanup();
    var source_options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .start_optional_runtimes = false, .start_index_workers = false };
    const target_options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 3, .shard_id = 4, .range_id = 4 }, .start_optional_runtimes = false, .start_index_workers = false };
    {
        var source = try db_mod.DB.open(alloc, source_directory.path(), source_options);
        defer source.close();
        try source.setSchemaJson(alloc, expression_index_schema);
        try source.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"tenant\":1,\"id\":7,\"price\":10,\"payload\":\"restored\"}" }} });
    }
    source_options.open_mode = .query_readonly;
    var source = try db_mod.DB.open(alloc, source_directory.path(), source_options);
    defer source.close();
    var target = try db_mod.DB.open(alloc, target_directory.path(), target_options);
    defer target.close();
    try target.setSchemaJson(alloc, expression_index_schema);
    const schema_bytes = try @import("../schema.zig").serializeSchema(alloc, target.core.schema.?);
    defer alloc.free(schema_bytes);
    const scope: staging.Scope = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .source_artifact_digest = @splat(3), .source_namespace = source_options.identity_namespace.?, .target_namespace = target_options.identity_namespace.?, .target_schema_digest = staging.digest(schema_bytes) };
    try target.reserveRestoreStagingScoped(alloc, scope);
    try target.beginRestoreStaging(alloc, scope);
    var page = try target.prepareRestoreStagingPage(alloc, scope, &source, 128, .none);
    defer page.deinit();
    // Hidden import pages must not request ordinary serving-table visibility.
    // Exercise the compiled-owner JSON boundary as well as the native request.
    try std.testing.expectEqual(.write, page.batch.?.sync_level);
    const batch_api = @import("../../api/batch.zig");
    const encoded_page = try batch_api.encodeBatchRequest(alloc, page.batch.?);
    defer alloc.free(encoded_page);
    var decoded_page = try batch_api.parseInternalBatchRequest(alloc, encoded_page);
    defer decoded_page.deinit(alloc);
    try std.testing.expectEqual(.write, decoded_page.req.sync_level);
    try target.batchRaftReplicatedApply(decoded_page.req, .{ .index = 1, .term = 1 });
    try std.testing.expectError(error.RestoreStagingInProgress, target.lookup(alloc, "a", .{}));
    {
        var imported = (try target.restoreStagingStatus(alloc)).?;
        defer imported.deinit();
        try std.testing.expectEqual(.imported, imported.value.phase);
        try std.testing.expectEqual(@as(u64, 1), imported.value.rows);
    }

    // A permanent local job failure must not acquire a validated receipt.
    try resetRestoreIndexCoverage(&target, true);
    try std.testing.expectError(error.RestoreProjectionCorrupt, target.finishRestoreStaging(alloc, scope.digest(), .validated));
    try std.testing.expectError(error.RestoreProjectionCorrupt, target.prepareRestoreStagingIndexesStep(alloc, scope.digest()));
    try resetRestoreIndexCoverage(&target, false);
    var standby_gate: @import("../hot_standby/public_gate_state.zig").State = .{};
    standby_gate.role.store(@intFromEnum(@import("../hot_standby/public_gate_state.zig").Role.standby), .release);
    var raft_index: u64 = 2;
    for ([_]staging.Phase{ .validated, .published }) |phase| {
        // Also exercise same-phase receipt retries: a durable validated or
        // published marker is not evidence of replica-local index readiness.
        for (0..2) |trial| {
            try resetRestoreIndexCoverage(&target, false);
            target.close();
            target = try db_mod.DB.open(alloc, target_directory.path(), target_options);
            try std.testing.expectEqual(.building, (try target.relationalIndexBuildStatus("tenant_id")).state);
            try std.testing.expectError(error.IndexRebuilding, target.finishRestoreStaging(alloc, scope.digest(), phase));
            if (trial == 1) {
                target.ha_write_gate = .{ .shared = .{ .state = &standby_gate } };
                try std.testing.expectError(error.HAReadOnlyStandby, target.prepareRestoreStagingIndexesStep(alloc, scope.digest()));
            }
            const request: db_mod.types.BatchRequest = .{ .restore_staging = .{ .finish = .{ .scope = scope.digest(), .phase = phase } } };
            const entry_index = if (trial == 0) raft_index else raft_index - 1;
            var pending: usize = 0;
            for (0..32) |_| {
                target.batchRaftReplicatedApply(request, .{ .index = entry_index, .term = 1 }) catch |err| switch (err) {
                    error.RestoreProjectionCatchUpPending => {
                        pending += 1;
                        continue;
                    },
                    else => return err,
                };
                break;
            } else return error.IndexBuildDidNotConverge;
            try std.testing.expectEqual(.ready, (try target.relationalIndexBuildStatus("tenant_id")).state);
            try std.testing.expect(pending != 0); // one bounded phase per retry
            var progress = (try target.restoreStagingStatus(alloc)).?;
            defer progress.deinit();
            try std.testing.expectEqual(phase, progress.value.phase);
            if (trial == 0) raft_index += 1;
        }
    }
    // The hot-standby LSN fast path has the same obligation as Raft: a replay
    // receipt cannot skip reconstructing this replica's missing local proof.
    const ha_payload = try @import("../hot_standby/effects.zig").encodeBatchMutationRequestAlloc(alloc, .{ .restore_staging = .{ .finish = .{ .scope = scope.digest(), .phase = .published } } });
    defer alloc.free(ha_payload);
    const record: @import("../hot_standby/replication_record.zig").RecordView = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = 1, .previous_lsn = 0, .payload = ha_payload };
    try target.applyHAReplicationRecord(record);
    try resetRestoreIndexCoverage(&target, false);
    target.close();
    target = try db_mod.DB.open(alloc, target_directory.path(), target_options);
    target.ha_write_gate = .{ .shared = .{ .state = &standby_gate } };
    // Superseded entries and an unrelated scope must not perform maintenance
    // against the current generation, even when its local coverage is missing.
    try target.batchRaftReplicatedApply(.{ .restore_staging = .{ .finish = .{ .scope = scope.digest(), .phase = .validated } } }, .{ .index = 2, .term = 1 });
    try std.testing.expectEqual(.building, (try target.relationalIndexBuildStatus("tenant_id")).state);
    try target.batchRaftReplicatedApply(.{ .restore_staging = .{ .finish = .{ .scope = @splat(99), .phase = .published } } }, .{ .index = 3, .term = 1 });
    try std.testing.expectEqual(.building, (try target.relationalIndexBuildStatus("tenant_id")).state);
    var ha_pending: usize = 0;
    for (0..32) |_| {
        target.applyHAReplicationRecord(record) catch |err| switch (err) {
            error.RestoreProjectionCatchUpPending => {
                ha_pending += 1;
                continue;
            },
            else => return err,
        };
        break;
    } else return error.IndexBuildDidNotConverge;
    try std.testing.expectEqual(.ready, (try target.relationalIndexBuildStatus("tenant_id")).state);
    try std.testing.expect(ha_pending != 0);
    target.close();
    target = try db_mod.DB.open(alloc, target_directory.path(), target_options);
    try std.testing.expectEqual(@as(usize, 1), try expressionIndexCount(&target, 20));
}

test "relational index system historical expression failures persist and recover after row correction" {
    const jobs = @import("relational_index_jobs.zig");
    const expressions = @import("../../schema/relational_expression_errors.zig");
    inline for (@typeInfo(expressions.Error).error_set.?) |field| {
        const err = @field(expressions.Error, field.name);
        try std.testing.expectEqual(if (expressions.isInvalidInput(err)) @as(?jobs.Failure, .invalid_row) else null, jobs.classifyRowFailure(err));
    }
    try std.testing.expectEqual(null, jobs.classifyRowFailure(error.OutOfMemory));
    try std.testing.expectEqual(null, jobs.classifyRowFailure(error.Canceled));
    try std.testing.expectEqual(null, jobs.classifyRowFailure(error.Corrupted));
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    var directory = try TestDirectory.init("expression-index-historical-overflow");
    defer directory.cleanup();
    const options: db_mod.OpenOptions = .{ .start_optional_runtimes = false, .start_index_workers = false };
    var db = try db_mod.DB.open(alloc, directory.path(), options);
    defer db.close();
    var initial = try std.json.parseFromSlice(std.json.Value, alloc, expression_index_schema, .{});
    defer initial.deinit();
    try initial.value.object.put(initial.arena.allocator(), "version", .{ .integer = 1 });
    _ = initial.value.object.swapRemove("relational_indexes");
    const schema_json = try std.json.Stringify.valueAlloc(alloc, initial.value, .{});
    defer alloc.free(schema_json);
    try db.setSchemaJson(alloc, schema_json);
    // A valid old row becomes unindexable only when the new expression is
    // installed. The worker must record its physical source hash and fail.
    try db.batch(.{ .writes = &.{.{ .key = "overflow", .value = "{\"tenant\":1,\"id\":7,\"price\":9223372036854775807,\"payload\":\"old\"}" }} });
    const primary_key = try internal.relationalRowKeyAlloc(alloc, "overflow");
    defer alloc.free(primary_key);
    const historical_row = try db.core.store.get(alloc, primary_key);
    defer alloc.free(historical_row);
    try db.setSchemaJson(alloc, expression_index_schema);
    try db.buildRelationalIndexStep("tenant_id", .{});
    const failed = try db.relationalIndexBuildStatus("tenant_id");
    try std.testing.expectEqual(.failed, failed.state);
    try std.testing.expectEqual(.invalid_row, failed.failure);
    db.close();
    db = try db_mod.DB.open(alloc, directory.path(), options);
    try std.testing.expectEqual(.failed, (try db.relationalIndexBuildStatus("tenant_id")).state);
    try db.batch(.{ .writes = &.{.{ .key = "overflow", .value = "{\"tenant\":1,\"id\":7,\"price\":10,\"payload\":\"fixed\"}" }} });
    try std.testing.expect(try db.retryRelationalIndexBuild("tenant_id", failed.generation));
    _ = try ready(&db);
    try std.testing.expectEqual(@as(usize, 1), try expressionIndexCount(&db, 20));
    // Model a repair discovering the same historical source behind an old
    // forward/reverse pair. Both scrub phases must use the identical durable
    // failure rule, rather than retrying deterministic arithmetic forever.
    for ([_]jobs.Phase{ .forward, .reverse }) |phase| {
        {
            var pinned = db.core.relational_indexes.acquire().?;
            defer pinned.deinit();
            const index = pinned.plan.boundIndexes()[0];
            var txn = try db.core.store.beginWriteTxn();
            errdefer txn.abort();
            var progress = try jobs.resetProgress(try jobs.status(&txn, index));
            progress.phase = phase;
            const encoded = try progress.encode(alloc);
            defer alloc.free(encoded);
            try txn.put(primary_key, historical_row);
            try txn.put(&jobs.progressKey(index.id()), encoded);
            try txn.commit();
        }
        try db.buildRelationalIndexStep("tenant_id", .{});
        try std.testing.expectEqual(.failed, (try db.relationalIndexBuildStatus("tenant_id")).state);
        try std.testing.expectEqual(.invalid_row, (try db.relationalIndexBuildStatus("tenant_id")).failure);
        try db.batch(.{ .writes = &.{.{ .key = "overflow", .value = "{\"tenant\":1,\"id\":7,\"price\":10,\"payload\":\"fixed\"}" }} });
        try std.testing.expect(try db.retryRelationalIndexBuild("tenant_id", failed.generation));
        _ = try ready(&db);
        try std.testing.expectEqual(@as(usize, 1), try expressionIndexCount(&db, 20));
    }
}

test "relational index system direct expression lifecycle partial exclusion repair and portable restore" {
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    var directory = try TestDirectory.init("relational-direct-expression-index");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    var initial = try std.json.parseFromSlice(std.json.Value, alloc, expression_index_schema, .{ .parse_numbers = false });
    defer initial.deinit();
    const arena = initial.arena.allocator();
    try initial.value.object.put(arena, "version", .{ .integer = 1 });
    _ = initial.value.object.swapRemove("relational_indexes");
    const first = try std.json.Stringify.valueAlloc(arena, initial.value, .{});
    try db.setSchemaJson(alloc, first);
    try db.batch(.{ .writes = &.{
        .{ .key = "old", .value = "{\"tenant\":1,\"id\":1,\"price\":10,\"payload\":\"old\"}" },
        .{ .key = "excluded", .value = "{\"tenant\":0,\"id\":2,\"price\":9223372036854775807}" },
    } });
    try db.setSchemaJson(alloc, expression_index_schema);
    _ = try ready(&db);
    try std.testing.expectEqual(@as(usize, 1), try expressionIndexCount(&db, 20));
    // Nonmember expressions are never evaluated, even during cold backfill.
    try std.testing.expectError(error.RelationalExpressionOverflow, db.batch(.{ .writes = &.{.{ .key = "excluded", .value = "{\"tenant\":1,\"id\":2,\"price\":9223372036854775807}" }} }));
    try db.batch(.{ .writes = &.{.{ .key = "old", .value = "{\"tenant\":1,\"id\":1,\"price\":20,\"payload\":\"changed\"}" }} });
    try std.testing.expectEqual(@as(usize, 0), try expressionIndexCount(&db, 20));
    try std.testing.expectEqual(@as(usize, 1), try expressionIndexCount(&db, 40));
    const status = try db.relationalIndexBuildStatus("tenant_id");
    const id = records.Id{ .generation = status.generation, .slot = status.slot };
    var reverse = std.ArrayList(u8).empty;
    defer reverse.deinit(alloc);
    try records.appendReverseKey(alloc, &reverse, id, "old");
    const control = @import("relational_index_maintenance_contract.zig");
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(reverse.items, "corrupt reverse");
        const ticket = try (control.Control{ .epoch = 1, .last_request = @splat(1) }).encode(alloc);
        defer alloc.free(ticket);
        try txn.put(&control.controlKey(id), ticket);
        try txn.commit();
    }
    _ = try ready(&db);
    try std.testing.expectEqual(@as(usize, 1), try expressionIndexCount(&db, 40));
    var archive: std.ArrayListUnmanaged(u8) = .empty;
    defer archive.deinit(alloc);
    try @import("../portable_backup.zig").exportPortable(alloc, db.core.store, &archive);
    var restored_directory = try TestDirectory.init("relational-direct-expression-restored");
    defer restored_directory.cleanup();
    var restored = try db_mod.DB.open(alloc, restored_directory.path(), .{ .start_optional_runtimes = false });
    defer restored.close();
    try restored.importPortableIntoEmpty(alloc, archive.items, @import("doc_identity.zig").default_namespace);
    _ = try ready(&restored);
    try std.testing.expectEqual(@as(usize, 1), try expressionIndexCount(&restored, 40));
    const restored_generation = (try restored.relationalIndexBuildStatus("tenant_id")).generation;
    var next_schema = try std.json.parseFromSlice(std.json.Value, alloc, expression_index_schema, .{ .parse_numbers = false });
    defer next_schema.deinit();
    const next_alloc = next_schema.arena.allocator();
    try next_schema.value.object.put(next_alloc, "version", .{ .integer = 3 });
    const unchanged = try std.json.Stringify.valueAlloc(next_alloc, next_schema.value, .{});
    try restored.setSchemaJson(alloc, unchanged);
    try std.testing.expectEqual(restored_generation, (try restored.relationalIndexBuildStatus("tenant_id")).generation);
    try next_schema.value.object.put(next_alloc, "version", .{ .integer = 4 });
    next_schema.value.object.getPtr("relational_indexes").?.array.items[0].object.getPtr("keys").?.array.items[1].object.getPtr("expression").?.object.getPtr("args").?.array.items[1].object.getPtr("value").?.* = .{ .string = "3" };
    const changed = try std.json.Stringify.valueAlloc(next_alloc, next_schema.value, .{});
    try restored.setSchemaJson(alloc, changed);
    try std.testing.expect((try restored.relationalIndexBuildStatus("tenant_id")).generation > restored_generation);
    _ = try ready(&restored);
    try std.testing.expectEqual(@as(usize, 0), try expressionIndexCount(&restored, 40));
    try std.testing.expectEqual(@as(usize, 1), try expressionIndexCount(&restored, 60));
    try restored.batch(.{ .deletes = &.{"old"} });
    try std.testing.expectEqual(@as(usize, 0), try expressionIndexCount(&restored, 60));
}

test "relational index system expression composite cold covering work avoids wide primary probes" {
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    var directory = try TestDirectory.init("relational-expression-key-work");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    var schema_value = try std.json.parseFromSlice(std.json.Value, alloc, expression_index_schema, .{ .parse_numbers = false });
    defer schema_value.deinit();
    const temporary = schema_value.arena.allocator();
    const includes = schema_value.value.object.getPtr("relational_indexes").?.array.items[0].object.getPtr("include_columns").?;
    includes.array.shrinkRetainingCapacity(1);
    const schema_json = try std.json.Stringify.valueAlloc(temporary, schema_value.value, .{});
    try db.setSchemaJson(alloc, schema_json);
    const payload = try temporary.alloc(u8, 4096);
    @memset(payload, 'x');
    const writes = try temporary.alloc(db_mod.types.BatchWrite, 128);
    for (writes, 0..) |*write_item, i| write_item.* = .{
        .key = try std.fmt.allocPrint(temporary, "row:{d}", .{i}),
        .value = try std.json.Stringify.valueAlloc(temporary, .{ .tenant = 1, .id = i, .price = i, .payload = payload }, .{}),
    };
    try db.batch(.{ .writes = writes });
    _ = try ready(&db);
    db.close();
    db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    var probes: [2]usize = undefined;
    for ([_]bool{ false, true }, 0..) |fallback, i| {
        var reader = try db.beginRelationalRows(alloc, .{
            .index = "tenant_id",
            .fields = &.{ "id", "price" },
            .include_primary_digest = fallback,
            .lower = .{ .values = &.{.{ .integer = 1 }} },
            .upper = .{ .values = &.{.{ .integer = 1 }} },
            .conditions = &.{.{ .column = "tenant", .op = .gt, .value = .{ .integer = 0 } }},
        });
        defer reader.deinit();
        var count: usize = 0;
        probes[i] = 0;
        for (0..256) |_| {
            var page = try reader.nextPage(alloc, std.testing.io, .{ .rows = 256, .records = 256 });
            defer page.deinit();
            count += page.rows.len;
            probes[i] += page.primary_lookups;
            if (!page.more) break;
        } else return error.RowScanDidNotConverge;
        try std.testing.expectEqual(writes.len, count);
    }
    try std.testing.expectEqual(@as(usize, 0), probes[0]);
    try std.testing.expectEqual(writes.len, probes[1]);
    std.debug.print("expression-index cold work rows={d} primary_probes={d} fallback_probes={d} unselected_primary_payload_bytes={d}\n", .{ writes.len, probes[0], probes[1], writes.len * payload.len });
}

fn partialPreparationAllocations(test_alloc: std.mem.Allocator) !void {
    const registry = @import("schema_registry.zig");
    const plans = @import("relational_index_plan.zig");
    const partial = @import("relational_index_predicate.zig");
    var schemas = try registry.Registry.initCloned(test_alloc, std.testing.io, .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{
        .{ .name = "id", .path = "id", .column_type = .integer },
        .{ .name = "label", .path = "label", .column_type = .string, .allows_null = true },
    } });
    defer schemas.deinit();
    var view = schemas.acquire().?;
    defer view.release();
    const predicates = [_]@import("../relational_index.zig").UniquePredicate{
        .{ .field = "id", .op = .gt, .value_json = "9007199254740993" },
        .{ .field = "label", .op = .eq, .value_json = "\"alpha\"", .collation = "ci" },
    };
    var plan = try plans.View.init(test_alloc, view, &.{.{ .name = "partial", .generation = 1, .keys = &.{.{ .column = "id" }}, .include_columns = &.{"label"}, .where = &predicates }});
    defer plan.release();
    var equivalent = try partial.Plan.init(test_alloc, view.tableSchema().*, view.physicalLayout(), &.{
        .{ .field = "label", .op = .eq, .value_json = "\"ALPHA\"", .collation = "ci" },
        .{ .field = "id", .op = .gt, .value_json = "\"9007199254740993\"" },
        predicates[0],
    });
    defer equivalent.deinit();
    const compiled = plan.boundIndexes()[0].predicate.?;
    try std.testing.expectEqualSlices(u8, &compiled.identity, &equivalent.identity);
    try std.testing.expect(compiled.impliedBy(equivalent.conditions));
    try std.testing.expect(!compiled.impliedBy(equivalent.conditions[0..1]));
    var different = try partial.Plan.init(test_alloc, view.tableSchema().*, view.physicalLayout(), &.{
        .{ .field = "id", .op = .gt, .value_json = "9007199254740992" }, predicates[1],
    });
    defer different.deinit();
    try std.testing.expect(!std.mem.eql(u8, &compiled.identity, &different.identity));
    try std.testing.expect(!compiled.impliedBy(different.conditions));
    const query = [_]@import("relational_predicate.zig").Plan{
        equivalent.conditions[0], equivalent.conditions[1], equivalent.conditions[2], different.conditions[0],
    };
    var proven: [query.len]bool = undefined;
    try std.testing.expect(compiled.impliedByAndMark(&query, &proven));
    // Stronger membership also certifies the query's weaker numeric bound.
    try std.testing.expectEqualSlices(bool, &.{ true, true, true, true }, &proven);
    var batch = plans.Batch.init(test_alloc, plan);
    defer batch.deinit();
    for ([_][]const u8{
        "{\"id\":9007199254740993,\"label\":\"Alpha\"}",
        "{\"id\":9007199254740994,\"label\":null}",
        "{\"id\":9007199254740994,\"label\":\"Alpha\"}",
    }, 0..) |json, i| {
        var prepared = try @import("document_mapper.zig").PreparedRelationalWrite.init(test_alloc, "a", json, null, view.tableSchema().*, view.physicalLayout());
        defer prepared.deinit(test_alloc);
        _ = try batch.appendPrepared(&prepared);
        const key = try batch.key(i, 0);
        try std.testing.expectEqual(i == 2, key.member);
        if (i < 2) {
            try std.testing.expectEqual(@as(usize, 0), key.bytes.len);
            try std.testing.expectEqual(@as(usize, 0), key.payload.len);
            try std.testing.expectEqual(@as(usize, 0), batch.bytes.items.len);
        }
    }
}

test "relational index system partial canonical typed identities and allocation failures" {
    try std.testing.checkAllAllocationFailures(alloc, partialPreparationAllocations, .{});
}

test "relational index system partial implication hashes wide operands once with bounded merge work" {
    const partial = @import("relational_index_predicate.zig");
    const predicates = @import("relational_predicate.zig");
    var schemas = try @import("schema_registry.zig").Registry.initCloned(alloc, std.testing.io, .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{
        .{ .name = "label", .path = "label", .column_type = .string },
    } });
    defer schemas.deinit();
    var view = schemas.acquire().?;
    defer view.release();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const temporary = arena.allocator();
    const wide = try temporary.alloc(u8, 4096);
    @memset(wide, 'x');
    const definitions = try temporary.alloc(@import("../relational_index.zig").UniquePredicate, partial.max_conditions);
    for (definitions, 0..) |*definition, i| {
        const literal = try std.fmt.allocPrint(temporary, "{s}{d}", .{ wide, i });
        definition.* = .{ .field = "label", .op = .ne, .value_json = try std.json.Stringify.valueAlloc(temporary, literal, .{}) };
    }
    var plan = try partial.Plan.init(alloc, view.tableSchema().*, view.physicalLayout(), definitions);
    defer plan.deinit();
    var query: [partial.max_conditions]predicates.Plan = undefined;
    var expected_bytes: usize = 0;
    for (&query, 0..) |*condition, i| {
        condition.* = plan.conditions[query.len - 1 - i];
        expected_bytes += condition.operand.len;
    }
    var work: partial.ImplicationWork = .{};
    try std.testing.expect(plan.impliedByWithWork(&query, &work));
    try std.testing.expectEqual(query.len, work.query_hashes);
    try std.testing.expectEqual(expected_bytes, work.operand_bytes_hashed);
    try std.testing.expect(work.comparisons <= query.len + plan.conditions.len);
    try std.testing.expect(!plan.impliedBy(query[0 .. query.len - 1]));
    std.debug.print("partial-index implication work conjuncts={d} query_hashes={d} operand_bytes={d} comparisons={d}\n", .{ query.len, work.query_hashes, work.operand_bytes_hashed, work.comparisons });
    // One equality outside every excluded point proves the entire 256-term
    // index predicate without manufacturing/re-hashing 256 query predicates.
    var equality = try predicates.Plan.init(alloc, view.tableSchema().*, view.physicalLayout(), .{
        .column = "label",
        .op = .eq,
        .value = .{ .string = "a" },
    });
    defer equality.deinit();
    work = .{};
    try std.testing.expect(plan.impliedByWithWork(&.{equality}, &work));
    try std.testing.expectEqual(@as(usize, 1), work.query_hashes);
    try std.testing.expectEqual(@as(usize, 1), work.semantic_domains);
    try std.testing.expectEqual(equality.operand.len, work.operand_bytes_hashed);
    var residual = [_]bool{true};
    try std.testing.expect(plan.impliedByAndMark(&.{equality}, &residual));
    try std.testing.expect(!residual[0]);
    std.debug.print("partial-index semantic proof index_conjuncts=256 query_hashes=1 domains=1 residual_retained=true\n", .{});
}

fn installPartial(db: *db_mod.DB, version: u32, indexed: bool) !void {
    const Key = struct { column: []const u8 };
    const Predicate = struct { column: []const u8, op: []const u8, value: i64 };
    const Index = struct { name: []const u8, keys: []const Key, include_columns: []const []const u8, where: []const Predicate };
    const json = try std.json.Stringify.valueAlloc(alloc, .{
        .version = version,
        .storage_mode = "relational",
        .default_type = "row",
        .relational_indexes = @as([]const Index, if (indexed) &.{.{ .name = "tenant_id", .keys = &.{.{ .column = "id" }}, .include_columns = &.{ "tenant", "payload" }, .where = &.{.{ .column = "tenant", .op = "gt", .value = 0 }} }} else &.{}),
        .document_schemas = .{ .row = .{ .schema = .{
            .type = "object",
            .properties = .{ .tenant = .{ .type = "integer" }, .id = .{ .type = "integer" }, .payload = .{ .type = "string" } },
            .additionalProperties = false,
        } } },
    }, .{});
    defer alloc.free(json);
    try db.setSchemaJson(alloc, json);
}

test "relational index system retirement work follows generation size not unrelated table size" {
    const gc = @import("relational_index_gc.zig");
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("generation-local-retirement");
    defer directory.cleanup();
    const options: db_mod.OpenOptions = .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var db = try db_mod.DB.open(alloc, directory.path(), options);
    defer db.close();
    try installPartial(&db, 1, true);
    // Ten thousand genuine primary rows, none belonging to the partial index.
    for (0..40) |batch_number| {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const a = arena.allocator();
        var writes: [250]@import("types.zig").BatchWrite = undefined;
        for (&writes, 0..) |*item, i| item.* = .{
            .key = try std.fmt.allocPrint(a, "doc:{d:0>8}", .{batch_number * writes.len + i}),
            .value = try std.fmt.allocPrint(a, "{{\"tenant\":0,\"id\":{d},\"payload\":\"unchanged\"}}", .{batch_number * writes.len + i}),
        };
        try db.batch(.{ .writes = &writes });
    }
    try installPartial(&db, 2, false);
    var empty_scanned: usize = 0;
    while (try gc.Page.prepare(alloc, std.testing.io, db.core)) |value| {
        var page = value;
        defer page.deinit();
        empty_scanned += page.records_scanned;
        try page.commit(db.core);
    }
    try std.testing.expectEqual(@as(usize, 0), empty_scanned);

    try installPartial(&db, 3, true);
    for (0..5) |i| try write(&db, i, 1);
    const id = blk: {
        var plan = db.core.relational_indexes.acquire().?;
        defer plan.deinit();
        break :blk plan.plan.boundIndexes()[0].id();
    };
    // Lose one forward key; the independently owned locator must still find
    // its reverse. Retirement cannot depend on decoding corrupt tuple values.
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        var cursor = try txn.openCursor();
        const first = (try cursor.seekAtOrAfter(&(try records.forwardPrefix(id)))).?;
        const lost = try alloc.dupe(u8, first.key);
        defer alloc.free(lost);
        cursor.close();
        try txn.delete(lost);
        try txn.commit();
    }
    try installPartial(&db, 4, false);
    var visited: usize = 0;
    var pages: usize = 0;
    while (try gc.Page.prepare(alloc, std.testing.io, db.core)) |value| {
        {
            var page = value;
            defer page.deinit();
            visited += page.records_scanned;
            try page.commit(db.core);
        }
        pages += 1;
        db.close();
        db = try db_mod.DB.open(alloc, directory.path(), options);
    }
    try std.testing.expectEqual(@as(usize, 9), visited);
    try std.testing.expectEqual(@as(usize, 2), pages);
    var key = std.ArrayList(u8).empty;
    defer key.deinit(alloc);
    for (0..5) |i| {
        var name: [64]u8 = undefined;
        key.clearRetainingCapacity();
        try records.appendReverseKey(alloc, &key, id, try std.fmt.bufPrint(&name, "doc:{d:0>8}", .{i}));
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, key.items));
    }
    std.debug.print("\nretirement work: 10000 unrelated rows; empty generation={} visits, 5-row damaged generation={} visits / {} pages (reopen each page)\n", .{ empty_scanned, visited, pages });
}

test "relational index system partial membership discharges uncovered predicates without primary probes" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("relational-partial-cover-proof");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"tenant_id","keys":[{"column":"id"}],"include_columns":["label"],"where":[{"column":"active","op":"eq","value":true}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"active":{"type":"boolean"},"bucket":{"type":"integer"},"label":{"type":"keyword"}},"additionalProperties":false}}}}
    );
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    const writes = try owned.alloc(db_mod.types.BatchWrite, 192);
    for (writes, 0..) |*write_request, i| write_request.* = .{
        .key = try std.fmt.allocPrint(owned, "row:{d:0>3}", .{i}),
        .value = try std.json.Stringify.valueAlloc(owned, .{ .id = i, .active = i < 128, .bucket = i % 2, .label = "member" }, .{}),
    };
    try db.batch(.{ .writes = writes });
    _ = try ready(&db);
    const membership: rows.Condition = .{ .column = "active", .op = .eq, .value = .{ .boolean = true } };
    const cases = [_]struct {
        conditions: []const rows.Condition,
        fields: []const []const u8 = &.{ "id", "label" },
        digest: bool = false,
        expected_rows: usize = 128,
        expected_probes: usize = 0,
    }{
        .{ .conditions = &.{membership} },
        // An extra covered conjunct remains a real residual predicate.
        .{ .conditions = &.{ membership, .{ .column = "label", .op = .eq, .value = .{ .string = "member" } } } },
        .{ .conditions = &.{ membership, .{ .column = "label", .op = .eq, .value = .{ .string = "different" } } }, .expected_rows = 0 },
        // Unproven predicates, requested fields and physical digests still
        // require authoritative primary reads, even with the membership proof.
        .{ .conditions = &.{ membership, .{ .column = "bucket", .op = .eq, .value = .{ .integer = 1 } } }, .expected_rows = 64, .expected_probes = 128 },
        .{ .conditions = &.{membership}, .fields = &.{ "id", "active" }, .expected_probes = 128 },
        .{ .conditions = &.{membership}, .digest = true, .expected_probes = 128 },
    };
    for (cases) |case| {
        var reader = try db.beginRelationalRows(alloc, .{ .index = "tenant_id", .fields = case.fields, .conditions = case.conditions, .include_primary_digest = case.digest });
        defer reader.deinit();
        var page = try reader.nextPage(alloc, std.testing.io, .{ .rows = 256, .records = 1024, .time_ns = std.time.ns_per_s });
        defer page.deinit();
        try std.testing.expect(!page.more);
        try std.testing.expectEqual(case.expected_rows, page.rows.len);
        try std.testing.expectEqual(case.expected_probes, page.primary_lookups);
        try std.testing.expectEqual(if (case.expected_probes == 0) case.expected_rows else @as(usize, 0), page.index_only_rows);
    }
    std.debug.print("partial-index uncovered membership predicate rows=128 primary_probes=0 residual_fallback_probes=128\n", .{});
}

fn partialCount(db: *db_mod.DB) !usize {
    var reader = try db.beginRelationalRows(alloc, .{ .index = "tenant_id", .fields = &.{ "id", "payload" }, .conditions = &.{.{ .column = "tenant", .op = .gt, .value = .{ .integer = 0 } }} });
    defer reader.deinit();
    var page = try reader.nextPage(alloc, std.testing.io, .{ .rows = 100, .records = 100 });
    defer page.deinit();
    try std.testing.expect(!page.more);
    try std.testing.expectEqual(@as(usize, 0), page.primary_lookups);
    return page.rows.len;
}

test "relational index system partial stronger query bounds preserve residual covering semantics" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("relational-partial-interval-proof");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try db.setSchemaJson(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","relational_indexes":[{"name":"scores","keys":[{"column":"id"}],"include_columns":["score"],"where":[{"column":"score","op":"gte","value":10},{"column":"score","op":"lte","value":20}]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"score":{"type":"integer"}},"additionalProperties":false}}}}
    );
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    const writes = try owned.alloc(db_mod.types.BatchWrite, 32);
    for (writes, 0..) |*write_request, i| write_request.* = .{
        .key = try std.fmt.allocPrint(owned, "row:{d:0>3}", .{i}),
        .value = try std.json.Stringify.valueAlloc(owned, .{ .id = i, .score = i }, .{}),
    };
    try db.batch(.{ .writes = writes });
    // Query=>index admits a stronger range, but index=>query does NOT prove
    // its residual. Incorrectly sharing these two proofs would return 11 rows.
    _ = try readyIndex(&db, "scores");
    var reader = try db.beginRelationalRows(alloc, .{ .index = "scores", .fields = &.{"id"}, .conditions = &.{
        .{ .column = "score", .op = .gte, .value = .{ .integer = 12 } },
        .{ .column = "score", .op = .lt, .value = .{ .integer = 18 } },
        .{ .column = "score", .op = .ne, .value = .{ .integer = 5 } },
        .{ .column = "score", .op = .is_not_null },
    } });
    defer reader.deinit();
    var page = try reader.nextPage(alloc, std.testing.io, .{ .rows = 64, .records = 128, .time_ns = std.time.ns_per_s });
    defer page.deinit();
    try std.testing.expect(!page.more);
    try std.testing.expectEqual(@as(usize, 6), page.rows.len);
    try std.testing.expectEqual(@as(usize, 0), page.primary_lookups);
    try std.testing.expectEqual(@as(usize, 6), page.index_only_rows);
    try std.testing.expectError(error.PartialIndexPredicateNotImplied, db.beginRelationalRows(alloc, .{ .index = "scores", .conditions = &.{
        .{ .column = "score", .op = .gte, .value = .{ .integer = 9 } },
        .{ .column = "score", .op = .lte, .value = .{ .integer = 20 } },
    } }));
}

test "relational index system partial historical membership coalescing and covering proofs" {
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    var directory = try TestDirectory.init("relational-partial-membership");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try installPartial(&db, 1, false);
    try db.batch(.{ .writes = &.{
        .{ .key = "yes", .value = "{\"tenant\":1,\"id\":1,\"payload\":\"yes\"}" },
        .{ .key = "no", .value = "{\"tenant\":0,\"id\":2,\"payload\":\"no\"}" },
        .{ .key = "null", .value = "{\"id\":3,\"payload\":\"unknown\"}" },
    } });
    try installPartial(&db, 2, true);
    _ = try ready(&db);
    try std.testing.expectEqual(@as(usize, 1), try partialCount(&db));
    try std.testing.expectError(error.PartialIndexPredicateNotImplied, db.beginRelationalRows(alloc, .{ .index = "tenant_id" }));
    try std.testing.expectError(error.PartialIndexPredicateNotImplied, db.beginRelationalRows(alloc, .{ .index = "tenant_id", .conditions = &.{.{ .column = "tenant", .op = .gte, .value = .{ .integer = 0 } }} }));
    // Historical nonmembers legitimately have no reverse entry. Their first
    // active-layout update adds membership, while old members remove it.
    try db.batch(.{ .writes = &.{
        .{ .key = "yes", .value = "{\"tenant\":0,\"id\":1}" },
        .{ .key = "no", .value = "{\"tenant\":2,\"id\":2}" },
        .{ .key = "null", .value = "{\"tenant\":3,\"id\":3}" },
    } });
    try std.testing.expectEqual(@as(usize, 2), try partialCount(&db));
    try db.batch(.{ .writes = &.{
        .{ .key = "no", .value = "{\"tenant\":9,\"id\":2}" },
        .{ .key = "no", .value = "{\"id\":2}" },
        .{ .key = "yes", .value = "{\"tenant\":9,\"id\":1}" },
        .{ .key = "yes", .value = "{\"tenant\":0,\"id\":1}" },
    } });
    try std.testing.expectEqual(@as(usize, 1), try partialCount(&db));
    try db.batch(.{ .deletes = &.{ "no", "yes" } });
    try std.testing.expectEqual(@as(usize, 1), try partialCount(&db));
    db.close();
    db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    try std.testing.expectEqual(@as(usize, 1), try partialCount(&db));
    try db.batch(.{ .deletes = &.{"null"} });
    try std.testing.expectEqual(@as(usize, 0), try partialCount(&db));
}

test "relational index system partial missing member reverse fails closed and repair scrubs nonmembers" {
    const control = @import("relational_index_maintenance_contract.zig");
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    var directory = try TestDirectory.init("relational-partial-repair");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try installPartial(&db, 1, true);
    try db.batch(.{ .writes = &.{.{ .key = "member", .value = "{\"tenant\":1,\"id\":1}" }} });
    _ = try ready(&db);
    const status = try db.relationalIndexBuildStatus("tenant_id");
    const id = records.Id{ .generation = status.generation, .slot = status.slot };
    var reverse = std.ArrayList(u8).empty;
    defer reverse.deinit(alloc);
    try records.appendReverseKey(alloc, &reverse, id, "member");
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.delete(reverse.items);
        try txn.commit();
    }
    try std.testing.expectError(error.MissingRelationalIndexReverse, db.batch(.{ .writes = &.{.{ .key = "member", .value = "{\"tenant\":0,\"id\":1}" }} }));
    try std.testing.expectError(error.MissingRelationalIndexReverse, db.batch(.{ .deletes = &.{"member"} }));
    const original = (try db.get(alloc, "member")).?;
    defer alloc.free(original);
    try std.testing.expectEqualStrings("{\"tenant\":1,\"id\":1}", original);
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        const ticket = try (control.Control{ .epoch = 1, .last_request = @splat(1) }).encode(alloc);
        defer alloc.free(ticket);
        try txn.put(&control.controlKey(id), ticket);
        try txn.commit();
    }
    _ = try ready(&db);
    const primary = try internal.relationalRowKeyAlloc(alloc, "member");
    defer alloc.free(primary);
    // Simulate a stale derived pair surviving an authoritative nonmember
    // update. Copy canonical bytes from another nonmember, not invalid AROW.
    try db.batch(.{ .writes = &.{.{ .key = "nonmember", .value = "{\"tenant\":0,\"id\":1}" }} });
    const nonmember_key = try internal.relationalRowKeyAlloc(alloc, "nonmember");
    defer alloc.free(nonmember_key);
    const nonmember = try db.core.store.get(alloc, nonmember_key);
    defer alloc.free(nonmember);
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(primary, nonmember);
        try txn.put(reverse.items, "corrupt reverse");
        const ticket = try (control.Control{ .epoch = 2, .last_request = @splat(2) }).encode(alloc);
        defer alloc.free(ticket);
        try txn.put(&control.controlKey(id), ticket);
        try txn.commit();
    }
    _ = try ready(&db);
    try std.testing.expectEqual(@as(usize, 0), try partialCount(&db));
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectError(error.NotFound, read.get(reverse.items));
}

test "relational index system topology clear bounds key-only pages and removes every generation" {
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    const Destination = @import("../../data/storage/db_split_handoff.zig").Destination;
    for ([_]bool{ false, true }) |relational| {
        var directory = try TestDirectory.init("relational-topology-clear");
        defer directory.cleanup();
        var destination = try Destination.init(alloc, .{ .root_dir = directory.path(), .db = .{ .start_optional_runtimes = false } });
        defer destination.deinit();
        const db = destination.db;
        if (relational) try install(db, 1, true);
        // More than one physical page, with wide values to catch accidental
        // value materialization. Binary primary IDs retain ordinary ordering.
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const owned = arena.allocator();
        const payload = try owned.alloc(u8, 16 * 1024);
        @memset(payload, 'x');
        const json = try std.json.Stringify.valueAlloc(owned, .{ .tenant = 1, .id = 7, .payload = payload }, .{});
        const writes = try owned.alloc(db_mod.types.BatchWrite, 300);
        for (writes, 0..) |*item, i| item.* = .{ .key = try std.fmt.allocPrint(owned, "m:{d:0>4}\x00", .{i}), .value = json };
        try db.batch(.{ .writes = writes });
        try db.batch(.{ .writes = &.{ .{ .key = "a", .value = json }, .{ .key = "z", .value = json } } });
        if (relational) {
            _ = try ready(db);
            // Keep a retired generation alongside the new active one. Cleanup
            // must use row-owned reverse entries, not only the current plan.
            try install(db, 2, false);
            try install(db, 3, true);
            _ = try ready(db);
        }
        const before = db.core.nextDerivedSequence();
        // The old all-values range scan needed >4 MiB of caller allocation.
        // Key-only pages handle this corpus with a fixed 128-KiB scratch budget.
        var scratch: [128 * 1024]u8 = undefined;
        var bounded = std.heap.FixedBufferAllocator.init(&scratch);
        try destination.deleteDocsInRange(bounded.allocator(), .{ .start = "m:", .end = "n" });
        try std.testing.expect(db.core.nextDerivedSequence() > before + 1);
        try destination.deleteDocsInRange(alloc, .{ .start = "m:", .end = "n" });
        try std.testing.expectError(error.InvalidRange, destination.deleteDocsInRange(alloc, .{ .start = "z", .end = "a" }));
        // Catalog row_count is deliberately a presence bit, not cardinality.
        try std.testing.expectEqual(@as(u64, 1), db.core.table_catalog.row_count);
        try std.testing.expectEqual(@as(?u64, 2), try @import("range_cardinality.zig").load(alloc, db.core.store));
        try std.testing.expect((try db.get(alloc, writes[0].key)) == null);
        for ([_][]const u8{ "a", "z" }) |key| {
            const value = (try db.get(alloc, key)).?;
            defer alloc.free(value);
            try std.testing.expectEqualStrings(json, value);
        }
        // Verify no native forward or reverse record survives for a removed
        // document, including generations no longer in the active catalog.
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        var cursor = try read.openCursor();
        defer cursor.close();
        var item = try cursor.first();
        while (item) |entry| : (item = try cursor.next()) {
            if (internal.isRelationalIndexReverseKey(entry.key)) {
                const owner = try records.parseReverseKey(entry.key);
                try std.testing.expect(!std.mem.startsWith(u8, owner.document_component, "m:"));
            } else if (records.isForwardKey(entry.key)) {
                const owner = try records.forwardOwnership(entry.key);
                try std.testing.expect(!std.mem.startsWith(u8, owner.document_component, "m:"));
            }
        }
    }
}

test "relational index system replicated merge fences stale attempts and rebuilds range readiness after restart" {
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    var directory = try TestDirectory.init("relational-merge-replay");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try install(&db, 1, true);
    try db.updateRange(.{ .start = "m", .end = "z" });
    try db.batch(.{ .writes = &.{.{ .key = "x", .value = "{\"tenant\":1,\"id\":9}" }} });
    _ = try ready(&db);
    var index: u64 = 0;
    var checkpoint: db_mod.types.MergeReplicationCheckpoint = .{
        .kind = .accept,
        .transition_id = 100,
        .donor_group_id = 101,
        .receiver_group_id = 102,
        .receiver_base_start = "m",
        .receiver_base_end = "z",
        .merged_start = "a",
        .merged_end = "z",
    };
    try applyTopology(&db, &index, .{ .merge_checkpoint = checkpoint });
    // Prior coverage only certified the receiver's old range. A merge may not
    // expose that receipt as readiness for the incoming donor range.
    try std.testing.expectEqual(.building, (try db.relationalIndexBuildStatus("tenant_id")).state);
    try std.testing.expectError(error.RelationalIndexNotReady, db.beginRelationalRows(alloc, .{ .index = "tenant_id" }));
    checkpoint.kind = .begin_copy;
    checkpoint.copy_attempt = .{ .donor_term = 1, .sequence = 50 };
    try applyTopology(&db, &index, .{ .merge_checkpoint = checkpoint });
    const old_copy: db_mod.types.MergeReplicationContext = .{
        .transition_id = 100,
        .donor_group_id = 101,
        .receiver_group_id = 102,
        .identity_namespace = db.core.identity_namespace,
        .copy_attempt = checkpoint.copy_attempt,
    };
    const old_request: db_mod.types.BatchRequest = .{ .merge_replication = old_copy, .writes = &.{.{ .key = "b\x00", .value = "{\"tenant\":1,\"id\":1}" }} };
    try applyTopology(&db, &index, old_request);
    checkpoint.copy_attempt = .{ .donor_term = 2, .sequence = 1 };
    try applyTopology(&db, &index, .{ .merge_checkpoint = checkpoint });
    var winning_copy = old_copy;
    winning_copy.copy_attempt = checkpoint.copy_attempt;
    const winning_request: db_mod.types.BatchRequest = .{ .merge_replication = winning_copy, .writes = &.{.{ .key = "b\x00", .value = "{\"tenant\":1,\"id\":7}" }} };
    try applyTopology(&db, &index, winning_request);
    // Exact Raft replay, then a stale owner's request at a NEW applied index:
    // neither may leave old secondary tuples or change the winning row.
    try db.batchRaftReplicatedApply(winning_request, .{ .term = 7, .index = index });
    try applyTopology(&db, &index, old_request);
    try applyTopology(&db, &index, .{ .merge_replication = old_copy, .deletes = &.{"b\x00"} });
    db.close();
    db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    try std.testing.expectEqual(.building, (try db.relationalIndexBuildStatus("tenant_id")).state);
    checkpoint.kind = .bootstrap_complete;
    checkpoint.bootstrap_applied_index = 20;
    try applyTopology(&db, &index, .{ .merge_checkpoint = checkpoint });
    try applyTopology(&db, &index, .{ .merge_replication = winning_copy, .deletes = &.{"b\x00"} });
    checkpoint.kind = .finalize;
    try applyTopology(&db, &index, .{ .merge_checkpoint = checkpoint });
    _ = try ready(&db);
    try expectIndexRows(&db, &.{ "x", "b\x00" });
    const winner = (try db.get(alloc, "b\x00")).?;
    defer alloc.free(winner);
    try std.testing.expectEqualStrings("{\"tenant\":1,\"id\":7}", winner);
    try db.batch(.{ .deletes = &.{"b\x00"} });
    try expectIndexRows(&db, &.{"x"});
}

fn applyTopology(db: *db_mod.DB, index: *u64, request: db_mod.types.BatchRequest) !void {
    index.* += 1;
    try db.batchRaftReplicatedApply(request, .{ .term = 7, .index = index.* });
}

test "relational index system replicated split checkpoints and sparse deltas preserve native companions across reopen" {
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    var directory = try TestDirectory.init("relational-split-replay");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try install(&db, 1, true);
    _ = try ready(&db);
    const copy: db_mod.types.SplitReplicationContext = .{
        .transition_id = 7001,
        .attempt_epoch = 3,
        .source_group_id = 71,
        .destination_group_id = 72,
        .identity_namespace = db.core.identity_namespace,
        .bootstrap_sequence = 9,
    };
    var control = copy;
    control.operation = .checkpoint;
    control.sequence = 9;
    var checkpoint: db_mod.types.SplitReplicationCheckpoint = .{
        .kind = .destination_begin,
        .transition_id = 7001,
        .attempt_epoch = 3,
        .source_group_id = 71,
        .destination_group_id = 72,
        .range_start = "m",
        .range_end = "",
        .delta_sequence = 9,
    };
    var index: u64 = 0;
    try applyTopology(&db, &index, .{ .split_replication = control, .split_checkpoint = checkpoint });
    try std.testing.expectEqual(.building, (try db.relationalIndexBuildStatus("tenant_id")).state);
    try std.testing.expectError(error.RelationalIndexNotReady, db.beginRelationalRows(alloc, .{ .index = "tenant_id" }));
    const request: db_mod.types.BatchRequest = .{ .split_replication = copy, .writes = &.{ .{ .key = "x\x00", .value = "{\"tenant\":1,\"id\":9}" }, .{ .key = "y", .value = "{\"tenant\":1,\"id\":8}" } } };
    try applyTopology(&db, &index, request);
    try db.batchRaftReplicatedApply(request, .{ .term = 7, .index = index });
    db.close();
    db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    try applyTopology(&db, &index, .{ .split_replication = control, .split_checkpoint = checkpoint });
    checkpoint.kind = .destination_complete;
    try applyTopology(&db, &index, .{ .split_replication = control, .split_checkpoint = checkpoint });
    var delta = copy;
    delta.bootstrap_sequence = null;
    delta.operation = .delta;
    delta.sequence = 20;
    delta.previous_sequence = 9;
    const update: db_mod.types.BatchRequest = .{ .split_replication = delta, .writes = &.{.{ .key = "x\x00", .value = "{\"tenant\":1,\"id\":7}" }}, .deletes = &.{"y"} };
    try applyTopology(&db, &index, update);
    try db.batchRaftReplicatedApply(update, .{ .term = 7, .index = index });
    try applyTopology(&db, &index, update);
    _ = try ready(&db);
    try expectIndexRows(&db, &.{"x\x00"});
    const current = (try db.get(alloc, "x\x00")).?;
    defer alloc.free(current);
    try std.testing.expectEqualStrings("{\"tenant\":1,\"id\":7}", current);
    try std.testing.expect((try db.get(alloc, "y")) == null);
    try std.testing.expectEqual(@as(u64, 20), try db.getSplitDeltaFinalSeq(alloc));
}

test "relational index system same-bound incarnation change fences readiness and maintenance across reopen" {
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    const jobs = @import("relational_index_jobs.zig");
    const maintenance = @import("relational_index_maintenance_contract.zig");
    var directory = try TestDirectory.init("relational-index-incarnation");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .identity_namespace = .{ .table_id = 7, .shard_id = 8, .range_id = 9 } });
    defer db.close();
    try install(&db, 1, true);
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"tenant\":1,\"id\":9}" }} });
    _ = try ready(&db);
    const command: maintenance.Command = blk: {
        var pinned = db.core.relational_indexes.acquire().?;
        defer pinned.deinit();
        const index = pinned.plan.boundIndexes()[0];
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        const proof = try jobs.statusProofWithOwnership(&read, index, try jobs.ownership(&read));
        break :blk .{
            .action = .repair,
            .table_id = db.core.identity_namespace.table_id,
            .owner_group_id = 1,
            .schema_version = 1,
            .index_name = "tenant_id",
            .generation = index.id().generation,
            .slot = index.id().slot,
            .owner = proof.progress.owner,
            .comparison = proof.progress.comparison,
            .expected_progress_digest = proof.digest,
            .expected_maintenance_epoch = proof.maintenance_epoch,
            .routing_key = "",
        };
    };
    var namespace = db.core.identity_namespace;
    namespace.range_id += 1;
    try db.reassignIdentityNamespaceForInternalTransition(namespace);
    for (0..2) |pass| {
        if (pass != 0) {
            db.close();
            db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
        }
        try std.testing.expectEqual(.building, (try db.relationalIndexBuildStatus("tenant_id")).state);
        try std.testing.expectError(error.RelationalIndexNotReady, db.beginRelationalRows(alloc, .{ .index = "tenant_id" }));
        const transaction = try db.beginTransaction(100 + pass * 10);
        try std.testing.expectError(error.PreparedGenerationChanged, db.writeTransaction(transaction, .{ .relational_index_maintenance = command }));
        try db.abortTransaction(transaction, 101 + pass * 10);
    }
    _ = try ready(&db);
    try expectIndexRows(&db, &.{"a"});
}

fn expectIndexRows(db: *db_mod.DB, expected: []const []const u8) !void {
    var reader = try db.beginRelationalRows(alloc, .{ .index = "tenant_id" });
    defer reader.deinit();
    var page = try reader.nextPage(alloc, null, .{});
    defer page.deinit();
    try std.testing.expectEqual(expected.len, page.rows.len);
    for (expected) |key| {
        var count: usize = 0;
        for (page.rows) |row| if (std.mem.eql(u8, key, row.key)) {
            count += 1;
        };
        try std.testing.expectEqual(@as(usize, 1), count);
    }
    const progress = try db.relationalIndexBuildStatus("tenant_id");
    const prefix = try records.forwardPrefix(.{ .generation = progress.generation, .slot = progress.slot });
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    var cursor = try read.openCursor();
    defer cursor.close();
    var item = try cursor.seekAtOrAfter(&prefix);
    var count: usize = 0;
    while (item) |entry| : (item = try cursor.next()) {
        if (!std.mem.startsWith(u8, entry.key, &prefix)) break;
        _ = try records.forwardOwnership(entry.key);
        count += 1;
    }
    // A query may defensively reject stale tuples. Inspect physical counts too
    // so that stale/replayed mutations cannot hide an accumulating orphan leak.
    try std.testing.expectEqual(expected.len, count);
}

test "relational index system topology coverage carries completed CHECK phase and incarnation" {
    const catalog = @import("relational_integrity_catalog.zig");
    const activation = @import("relational_integrity_activation.zig");
    const handoff = @import("relational_integrity_handoff.zig");
    const namespace: @import("doc_identity.zig").Namespace = .{ .table_id = 7, .shard_id = 8, .range_id = 9 };
    for ([_]bool{ false, true }) |checks| {
        var compiled = try catalog.prepareWithChecks(alloc, null, @splat(1), 1, @splat(2), &.{}, if (checks) @splat(3) else @splat(0));
        defer compiled.deinit();
        const raw = try handoff.coverageForRange(alloc, compiled.value, namespace, .{ .start = "m", .end = "z" });
        defer alloc.free(raw);
        const progress = try activation.Progress.decode(raw);
        try std.testing.expectEqual(.enforced, progress.state);
        try std.testing.expectEqual(if (checks) activation.Phase.check else activation.Phase.foreign_key, progress.phase);
        var replacement = namespace;
        replacement.range_id += 1;
        const changed = try handoff.coverageForRange(alloc, compiled.value, replacement, .{ .start = "m", .end = "z" });
        defer alloc.free(changed);
        const new_progress = try activation.Progress.decode(changed);
        try std.testing.expect(!std.mem.eql(u8, &progress.owner, &new_progress.owner));
    }
}

test "relational index system status pins exact epoch and retired corruption cannot block cleanup" {
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    var directory = try TestDirectory.init("relational-index-status");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try install(&db, 1, false);
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"tenant\":1,\"id\":7,\"payload\":\"data\"}" }} });
    try install(&db, 2, true);
    const request = "{\"name\":\"tenant_id\",\"schema_version\":2}";
    var pending = (try db.lookup(alloc, "", .{ .relational_index_status_json = request })).?;
    defer pending.deinit(alloc);
    var status = try std.json.parseFromSlice(@import("relational_index_status_contract.zig").Status, alloc, pending.json, .{});
    defer status.deinit();
    try std.testing.expectEqual(.building, status.value.state);
    try std.testing.expectError(error.PreparedGenerationChanged, db.lookup(alloc, "", .{ .relational_index_status_json = "{\"name\":\"tenant_id\",\"schema_version\":1}" }));
    _ = try ready(&db);
    var covered = (try db.lookup(alloc, "", .{ .relational_index_status_json = request })).?;
    defer covered.deinit(alloc);
    var complete = try std.json.parseFromSlice(@import("relational_index_status_contract.zig").Status, alloc, covered.json, .{});
    defer complete.deinit();
    try std.testing.expectEqual(.ready, complete.value.state);
    var owner_batch = (try db.lookup(alloc, "", .{ .relational_index_status_json = "{\"schema_version\":2}" })).?;
    defer owner_batch.deinit(alloc);
    var batch_status = try std.json.parseFromSlice(@import("relational_index_status_contract.zig").Batch, alloc, owner_batch.json, .{});
    defer batch_status.deinit();
    try std.testing.expectEqual(@as(usize, 1), batch_status.value.statuses.len);
    try std.testing.expectEqualStrings("tenant_id", batch_status.value.statuses[0].name);
    try std.testing.expectEqualDeep(complete.value, batch_status.value.statuses[0].status);
    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Cancelled, db.lookup(alloc, "", .{ .relational_index_status_json = "{\"schema_version\":2}", .cancellation = db_mod.types.CancellationToken.fromAtomic(&canceled) }));
    try std.testing.expectError(error.Timeout, db.lookup(alloc, "", .{ .relational_index_status_json = "{\"schema_version\":2}", .execution_deadline_ns = 0 }));
    const id = records.Id{ .generation = complete.value.generation, .slot = complete.value.slot };
    try install(&db, 3, true);
    const reused = try db.relationalIndexBuildStatus("tenant_id");
    try std.testing.expectEqual(.ready, reused.state);
    try std.testing.expectEqual(id.generation, reused.generation);
    try std.testing.expectEqual(id.slot, reused.slot);
    const prefix = try records.forwardPrefix(id);
    const forward = key: {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        var cursor = try read.openCursor();
        defer cursor.close();
        const item = (try cursor.seekAtOrAfter(&prefix)).?;
        try std.testing.expect(std.mem.startsWith(u8, item.key, &prefix));
        break :key try alloc.dupe(u8, item.key);
    };
    defer alloc.free(forward);
    try install(&db, 4, false);
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(forward, "damaged retired payload");
        try txn.commit();
    }
    for (0..8) |_| if (!try db.collectRelationalIndexGarbageStep()) break;
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectError(error.NotFound, read.get(forward));
}

test "relational index system public typed bounds cursor auth and cross-owner order" {
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    var left_dir = try TestDirectory.init("relational-index-query-left");
    defer left_dir.cleanup();
    var right_dir = try TestDirectory.init("relational-index-query-right");
    defer right_dir.cleanup();
    var left = try db_mod.DB.open(alloc, left_dir.path(), .{ .start_optional_runtimes = false });
    defer left.close();
    var right = try db_mod.DB.open(alloc, right_dir.path(), .{ .start_optional_runtimes = false });
    defer right.close();
    for ([_]*db_mod.DB{ &left, &right }) |db| try install(db, 1, false);
    try left.batch(.{ .writes = &.{
        .{ .key = "a", .value = "{\"tenant\":1,\"id\":9007199254740992,\"payload\":\"visible\"}" },
        .{ .key = "b", .value = "{\"tenant\":2,\"id\":0,\"payload\":\"hidden\"}" },
    } });
    try right.batch(.{ .writes = &.{
        .{ .key = "y", .value = "{\"tenant\":1,\"id\":9007199254740993,\"payload\":\"visible\"}" },
        .{ .key = "z", .value = "{\"tenant\":1,\"id\":9007199254740991,\"payload\":\"hidden\"}" },
    } });
    // One owner observed intermediate create/drop epochs; the other restored
    // directly into the final immutable schema. Physical generations differ,
    // but distributed ordering/continuation must use the logical identity.
    try install(&left, 2, true);
    try install(&left, 3, false);
    for ([_]*db_mod.DB{ &left, &right }) |db| try install(db, 4, true);
    const query = "{\"schema_version\":4,\"index\":\"tenant_id\",\"fields\":[\"id\"],\"lower\":{\"values\":[1]},\"upper\":{\"values\":[1]}}";
    const opts = db_mod.types.ScanOptions{ .include_documents = true, .limit = 2, .relational_query_json = query };
    try std.testing.expectError(error.RelationalIndexNotReady, left.scan(alloc, "", "", opts));
    for ([_]*db_mod.DB{ &left, &right }) |db| _ = try ready(db);
    const left_status = try left.relationalIndexBuildStatus("tenant_id");
    const right_status = try right.relationalIndexBuildStatus("tenant_id");
    try std.testing.expect(left_status.generation != right_status.generation);
    var merge = try @import("../../api/relational_row_merge.zig").Merger.init(alloc, 2);
    defer merge.deinit();
    var after: ?[]u8 = null;
    defer if (after) |value| alloc.free(value);
    for ([_]*db_mod.DB{ &left, &right }) |db| {
        var result = try db.scan(alloc, "", "", opts);
        defer result.deinit(alloc);
        if (db == &right) {
            try std.testing.expectEqualStrings("y", result.hashes[0].id);
            try std.testing.expectEqualStrings("{\"id\":9007199254740993}", result.documents[0].json);
            after = try alloc.dupe(u8, result.hashes[0].relational_cursor.?);
        }
        const encoded = try @import("../../api/local_query_contract.zig").encodeStorageKernelScanNdjson(alloc, result, true);
        defer alloc.free(encoded);
        merge.beginGroup();
        try merge.write(encoded);
        try merge.endGroup();
    }
    const merged = try merge.finishAlloc();
    defer alloc.free(merged);
    try std.testing.expect(std.mem.startsWith(u8, merged, "{\"_id\":\"y\""));
    try std.testing.expect(std.mem.indexOf(u8, merged, "{\"_id\":\"a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, merged, "{\"_id\":\"z\"") == null);
    const resumed_query = try std.fmt.allocPrint(alloc, "{{\"schema_version\":4,\"index\":\"tenant_id\",\"fields\":[],\"after\":\"{s}\"}}", .{after.?});
    defer alloc.free(resumed_query);
    var resumed = try right.scan(alloc, "", "", .{ .include_documents = true, .limit = 2, .relational_query_json = resumed_query });
    defer resumed.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), resumed.hashes.len);
    try std.testing.expectEqualStrings("z", resumed.hashes[0].id);
    var resumed_other_owner = try left.scan(alloc, "", "", .{ .include_documents = true, .limit = 2, .relational_query_json = resumed_query });
    defer resumed_other_owner.deinit(alloc);
    try std.testing.expectEqualStrings("a", resumed_other_owner.hashes[0].id);
    // Authorization sees the typed full row, not the empty caller projection.
    var authorized = try right.scan(alloc, "", "", .{
        .include_documents = true,
        .limit = 2,
        .relational_query_json = "{\"schema_version\":4,\"index\":\"tenant_id\",\"fields\":[]}",
        .filter_query_json = "{\"term\":{\"payload\":\"visible\"}}",
    });
    defer authorized.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), authorized.hashes.len);
    try std.testing.expectEqualStrings("y", authorized.hashes[0].id);
    try std.testing.expectEqualStrings("{}", authorized.documents[0].json);
    try install(&right, 5, false);
    try std.testing.expectError(error.PreparedGenerationChanged, right.scan(alloc, "", "", opts));
    try install(&right, 6, true);
    _ = try ready(&right);
    const old_generation_query = try std.fmt.allocPrint(alloc, "{{\"schema_version\":6,\"index\":\"tenant_id\",\"fields\":[],\"after\":\"{s}\"}}", .{after.?});
    defer alloc.free(old_generation_query);
    try std.testing.expectError(error.PreparedGenerationChanged, right.scan(alloc, "", "", .{ .relational_query_json = old_generation_query }));
}

fn schema(version: u32, indexed: bool) ![]u8 {
    const Key = struct { column: []const u8, direction: []const u8 = "asc" };
    const Index = struct { name: []const u8, keys: []const Key };
    return std.json.Stringify.valueAlloc(alloc, .{
        .version = version,
        .storage_mode = "relational",
        .default_type = "row",
        .relational_indexes = @as([]const Index, if (indexed) &.{.{ .name = "tenant_id", .keys = &.{ .{ .column = "tenant" }, .{ .column = "id", .direction = "desc" } } }} else &.{}),
        .document_schemas = .{ .row = .{ .schema = .{
            .type = "object",
            .properties = .{ .tenant = .{ .type = "integer" }, .id = .{ .type = "integer" }, .payload = .{ .type = "string" } },
            .additionalProperties = false,
        } } },
    }, .{});
}

test "relational index system repair is primary authoritative resumable and control fenced" {
    const jobs = @import("relational_index_jobs.zig");
    const control = @import("relational_index_maintenance_contract.zig");
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    var directory = try TestDirectory.init("relational-index-repair");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    defer db.close();
    try install(&db, 1, true);
    try write(&db, 1, 1);
    _ = try ready(&db);
    const status = try db.relationalIndexBuildStatus("tenant_id");
    const id = records.Id{ .generation = status.generation, .slot = status.slot };
    const prefix = try records.forwardPrefix(id);
    const forward = blk: {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        var cursor = try read.openCursor();
        defer cursor.close();
        break :blk try alloc.dupe(u8, (try cursor.seekAtOrAfter(&prefix)).?.key);
    };
    defer alloc.free(forward);
    var reverse = std.ArrayList(u8).empty;
    defer reverse.deinit(alloc);
    try records.appendReverseKey(alloc, &reverse, id, "doc:00000001");
    var orphan = std.ArrayList(u8).empty;
    defer orphan.deinit(alloc);
    try records.appendReverseKey(alloc, &orphan, id, "absent");
    const stale = try alloc.dupe(u8, forward);
    defer alloc.free(stale);
    stale[records.forward_prefix_len + 1] ^= 1;
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.delete(forward);
        try txn.put(reverse.items, "corrupt reverse");
        try txn.put(orphan.items, "corrupt orphan");
        try txn.put(stale, "corrupt stale value");
        const ticket = try (control.Control{ .epoch = 1, .last_request = @splat(1) }).encode(alloc);
        defer alloc.free(ticket);
        try txn.put(&control.controlKey(id), ticket);
        try txn.commit();
    }
    try std.testing.expectEqual(.building, (try db.relationalIndexBuildStatus("tenant_id")).state);
    var old_page = (try jobs.Page.prepare(alloc, std.testing.io, db.core, "tenant_id", .{ .records = 1 })).?;
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        const ticket = try (control.Control{ .epoch = 2, .last_request = @splat(2) }).encode(alloc);
        defer alloc.free(ticket);
        try txn.put(&control.controlKey(id), ticket);
        try txn.commit();
    }
    try std.testing.expectError(error.PreparedGenerationChanged, old_page.commit(db.core));
    old_page.deinit();
    try db.buildRelationalIndexStep("tenant_id", .{ .records = 1 });
    db.close();
    db = try db_mod.DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false });
    _ = try ready(&db);
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqualStrings("", try read.get(forward));
        _ = try records.reverseTuple(reverse.items, try read.get(reverse.items));
        try std.testing.expectError(error.NotFound, read.get(stale));
        try std.testing.expectError(error.NotFound, read.get(orphan.items));
    }
    try std.testing.expectEqual(@as(usize, 1), (try scan(&db, true)).count);
    // Corrupt authoritative rows stop the generation; derived repair must not
    // conceal bad user data or falsely publish readiness.
    const primary_key = try internal.relationalRowKeyAlloc(alloc, "doc:00000001");
    defer alloc.free(primary_key);
    const primary_value = try db.core.store.get(alloc, primary_key);
    defer alloc.free(primary_value);
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        const damaged = try alloc.dupe(u8, primary_value);
        defer alloc.free(damaged);
        damaged[damaged.len - 1] ^= 1;
        try txn.put(primary_key, damaged);
        const ticket = try (control.Control{ .epoch = 3, .last_request = @splat(3) }).encode(alloc);
        defer alloc.free(ticket);
        try txn.put(&control.controlKey(id), ticket);
        try txn.commit();
    }
    for (0..8) |_| {
        try db.buildRelationalIndexStep("tenant_id", .{});
        if ((try db.relationalIndexBuildStatus("tenant_id")).state == .failed) break;
    }
    try std.testing.expectEqual(.failed, (try db.relationalIndexBuildStatus("tenant_id")).state);
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(primary_key, primary_value);
        try txn.commit();
    }
    try std.testing.expect(try db.retryRelationalIndexBuild("tenant_id", id.generation));
    _ = try ready(&db);
    // A reverse-only orphan must also retire when the catalog becomes empty.
    try install(&db, 2, false);
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.delete(forward);
        try txn.commit();
    }
    for (0..16) |_| if (!try db.collectRelationalIndexGarbageStep()) break;
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expectError(error.NotFound, read.get(reverse.items));
    try std.testing.expectError(error.NotFound, read.get(&control.controlKey(id)));
}

test "relational index system replicated maintenance tolerates divergent local progress and fences replay" {
    const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
    const jobs = @import("relational_index_jobs.zig");
    const control = @import("relational_index_maintenance_contract.zig");
    var first_dir = try TestDirectory.init("relational-maintenance-primary");
    defer first_dir.cleanup();
    var second_dir = try TestDirectory.init("relational-maintenance-replica");
    defer second_dir.cleanup();
    const options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 7, .shard_id = 71, .range_id = 71 }, .start_index_workers = false, .start_optional_runtimes = false };
    var first = try db_mod.DB.open(alloc, first_dir.path(), options);
    defer first.close();
    var second = try db_mod.DB.open(alloc, second_dir.path(), options);
    defer second.close();
    for ([_]*db_mod.DB{ &first, &second }) |db| {
        try install(db, 1, true);
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"tenant\":1,\"id\":7,\"payload\":\"data\"}" }} });
        _ = try ready(db);
    }
    const command = blk: {
        var pinned = first.core.relational_indexes.acquire().?;
        defer pinned.deinit();
        const index = pinned.plan.boundIndexes()[0];
        var txn = try first.core.store.beginWriteTxn();
        errdefer txn.abort();
        var failed = try jobs.status(&txn, index);
        failed.state = .failed;
        failed.failure = .invalid_row;
        const bytes = try failed.encode(alloc);
        defer alloc.free(bytes);
        try txn.put(&jobs.progressKey(index.id()), bytes);
        try txn.commit();
        break :blk control.Command{ .action = .retry, .table_id = 7, .owner_group_id = 71, .schema_version = 1, .index_name = "tenant_id", .generation = index.generation, .slot = index.slot, .owner = failed.owner, .comparison = failed.comparison, .expected_progress_digest = control.progressDigest(bytes), .expected_maintenance_epoch = 0, .routing_key = "" };
    };
    const id = records.Id{ .generation = command.generation, .slot = command.slot };
    // Different replica-local progress MUST NOT affect deterministic replicated
    // prepare: both replicas accept the same desired control and independently rebuild.
    for ([_]*db_mod.DB{ &first, &second }) |db| {
        const txn_id = try db.beginTransaction(1000);
        try db.writeTransaction(txn_id, .{ .relational_index_maintenance = command });
        try db.commitTransaction(txn_id, 2000);
        try std.testing.expectEqual(.building, (try db.relationalIndexBuildStatus("tenant_id")).state);
        _ = try ready(db);
        const repeated = try db.beginTransaction(3000);
        try db.writeTransaction(repeated, .{ .relational_index_maintenance = command });
        try db.commitTransaction(repeated, 4000);
        try std.testing.expectEqual(.ready, (try db.relationalIndexBuildStatus("tenant_id")).state);
        var read = try db.core.store.beginProbeTxn();
        defer read.abort();
        const receipt = try control.readControl(&read, id);
        try std.testing.expectEqual(@as(u64, 1), receipt.epoch);
        try std.testing.expectEqual(command.fingerprint(), receipt.last_request);
    }
    var stale = command;
    stale.expected_progress_digest[0] ^= 1;
    const rejected = try first.beginTransaction(5000);
    try std.testing.expectError(error.PreparedGenerationChanged, first.writeTransaction(rejected, .{ .relational_index_maintenance = stale }));
    try first.abortTransaction(rejected, 6000);
    var wrong_table = command;
    wrong_table.table_id += 1;
    const wrong = try first.beginTransaction(7000);
    try std.testing.expectError(error.PreparedGenerationChanged, first.writeTransaction(wrong, .{ .relational_index_maintenance = wrong_table }));
    try first.abortTransaction(wrong, 8000);
    const value = try (control.Control{ .epoch = 2, .last_request = @splat(1) }).encode(alloc);
    defer alloc.free(value);
    try std.testing.expectError(error.ReservedRelationalIndexMetadataKey, first.batch(.{ .writes = &.{.{ .key = &control.controlKey(id), .value = value }} }));
    // Local workers cannot publish through a pending replicated control intent.
    var repair_command = command;
    repair_command.action = .repair;
    repair_command.expected_maintenance_epoch = 1;
    const repair_txn = try first.beginTransaction(9000);
    try first.writeTransaction(repair_txn, .{ .relational_index_maintenance = repair_command });
    try first.commitTransaction(repair_txn, 10000);
    var pending_page = (try jobs.Page.prepare(alloc, std.testing.io, first.core, "tenant_id", .{ .records = 1 })).?;
    defer pending_page.deinit();
    var obsolete_page = (try jobs.Page.prepare(alloc, std.testing.io, first.core, "tenant_id", .{ .records = 1 })).?;
    defer obsolete_page.deinit();
    repair_command.expected_maintenance_epoch = 2;
    const next_repair = try first.beginTransaction(11000);
    try first.writeTransaction(next_repair, .{ .relational_index_maintenance = repair_command });
    try std.testing.expectError(error.IntentConflict, pending_page.commit(first.core));
    try first.commitTransaction(next_repair, 12000);
    try std.testing.expectError(error.PreparedGenerationChanged, obsolete_page.commit(first.core));
    _ = try ready(&first);
    first.close();
    first = try db_mod.DB.open(alloc, first_dir.path(), options);
    var read = try first.core.store.beginProbeTxn();
    defer read.abort();
    try std.testing.expectEqual(@as(u64, 3), (try control.readControl(&read, id)).epoch);
}

fn install(db: *db_mod.DB, version: u32, indexed: bool) !void {
    const encoded = try schema(version, indexed);
    defer alloc.free(encoded);
    try db.setSchemaJson(alloc, encoded);
}

fn write(db: *db_mod.DB, id: usize, tenant: usize) !void {
    var key: [64]u8 = undefined;
    var value: [160]u8 = undefined;
    try db.batch(.{ .writes = &.{.{
        .key = try std.fmt.bufPrint(&key, "doc:{d:0>8}", .{id}),
        .value = try std.fmt.bufPrint(&value, "{{\"tenant\":{d},\"id\":{d},\"payload\":\"unchanged\"}}", .{ tenant, id }),
    }} });
}

fn ready(db: *db_mod.DB) !usize {
    return readyIndex(db, "tenant_id");
}

fn readyIndex(db: *db_mod.DB, name: []const u8) !usize {
    for (0..2048) |pages| {
        if ((try db.relationalIndexBuildStatus(name)).state == .ready) return pages;
        _ = try db.runRelationalIndexMaintenancePass();
    }
    return error.IndexBuildDidNotConverge;
}

const Scan = struct { count: usize = 0, examined: usize = 0, bytes: usize = 0 };
fn scan(db: *db_mod.DB, indexed: bool) !Scan {
    var reader = try db.beginRelationalRows(alloc, .{
        .index = if (indexed) "tenant_id" else null,
        .lower = if (indexed) .{ .values = &.{.{ .integer = 1 }} } else null,
        .upper = if (indexed) .{ .values = &.{.{ .integer = 1 }} } else null,
        .fields = &.{"id"},
        .conditions = if (indexed) &.{} else &.{.{ .column = "tenant", .op = .eq, .value = .{ .integer = 1 } }},
    });
    defer reader.deinit();
    var result: Scan = .{};
    for (0..4096) |_| {
        var page = try reader.nextPage(alloc, std.testing.io, .{ .rows = 32, .records = 64, .output_bytes = 64 * 1024 });
        defer page.deinit();
        result.count += page.rows.len;
        result.examined += page.records_examined;
        result.bytes += page.output_bytes;
        if (!page.more) return result;
    }
    return error.RowScanDidNotConverge;
}

fn replay(primary: *primary_mod.Primary, replica: *db_mod.DB, next: *u64) !void {
    while (next.* <= primary.lastLsn()) : (next.* += 1) {
        var entry = (try primary.log.entryAt(alloc, next.*)) orelse return error.MissingReplicationRecord;
        defer entry.deinit(alloc);
        try replica.applyHAReplicationRecord(entry.record);
        try replica.applyHAReplicationRecord(entry.record);
    }
}

test "relational index system standby replays schema churn and rebuilds ready generations after restart" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", owned);
    const source_path = try std.fmt.allocPrint(owned, "{s}/source", .{root});
    const replica_path = try std.fmt.allocPrint(owned, "{s}/replica", .{root});
    var primary = try primary_mod.Primary.open(alloc, try std.fmt.allocPrintSentinel(owned, "{s}/log", .{root}, 0), try std.fmt.allocPrintSentinel(owned, "{s}/slots", .{root}, 0), .{ .cluster_id = 71, .table_id = 7, .shard_id = 71, .timeline_id = 1, .epoch = 1 }, .{});
    defer primary.close();
    const options: db_mod.OpenOptions = .{ .primary_backend = .{ .lsm = .{ .flush_threshold = 32 } }, .identity_namespace = .{ .table_id = 7, .shard_id = 71, .range_id = 71 }, .start_index_workers = false, .start_optional_runtimes = false };
    var last_lsn = std.atomic.Value(u64).init(0);
    var failures = std.atomic.Value(u64).init(0);
    var mirrored = options;
    mirrored.ha_async_metadata_mirror = .{ .primary = &primary, .last_lsn = &last_lsn, .failure_count = &failures };
    mirrored.ha_async_batch_mirror = .{ .primary = &primary, .sync_policy = .{ .mode = .async } };
    var source = try db_mod.DB.open(alloc, source_path, mirrored);
    defer source.close();
    var replica = try db_mod.DB.open(alloc, replica_path, options);
    var replica_open = true;
    defer if (replica_open) replica.close();
    var next: u64 = 1;
    try install(&source, 1, false);
    for (0..16) |id| try write(&source, id, id % 4);
    try install(&source, 2, true);
    try replay(&primary, &replica, &next);
    _ = try ready(&source);
    _ = try ready(&replica);
    try std.testing.expectEqual(@as(usize, 4), (try scan(&replica, true)).count);
    const first = try replica.relationalIndexBuildStatus("tenant_id");
    // Desired maintenance is replicated through the same durable outbox as
    // ordinary mutations; replay never copies replica-local build progress.
    const control = @import("relational_index_maintenance_contract.zig");
    const jobs = @import("relational_index_jobs.zig");
    const repair_command = blk: {
        var pinned = source.core.relational_indexes.acquire().?;
        defer pinned.deinit();
        const index = pinned.plan.boundIndexes()[0];
        var read = try source.core.store.beginProbeTxn();
        defer read.abort();
        const proof = try jobs.statusProofWithOwnership(&read, index, try jobs.ownership(&read));
        break :blk control.Command{ .action = .repair, .table_id = 7, .owner_group_id = 71, .schema_version = 2, .index_name = "tenant_id", .generation = index.generation, .slot = index.slot, .owner = proof.progress.owner, .comparison = proof.progress.comparison, .expected_progress_digest = proof.digest, .expected_maintenance_epoch = proof.maintenance_epoch, .routing_key = "" };
    };
    const repair_txn = try source.beginTransaction(1000);
    try source.writeTransaction(repair_txn, .{ .relational_index_maintenance = repair_command });
    try source.commitTransaction(repair_txn, 2000);
    try replay(&primary, &replica, &next);
    try std.testing.expectEqual(.building, (try replica.relationalIndexBuildStatus("tenant_id")).state);
    {
        var read = try replica.core.store.beginProbeTxn();
        defer read.abort();
        const ticket = try control.readControl(&read, .{ .generation = repair_command.generation, .slot = repair_command.slot });
        try std.testing.expectEqual(@as(u64, 1), ticket.epoch);
        try std.testing.expectEqual(repair_command.fingerprint(), ticket.last_request);
    }
    _ = try ready(&source);
    _ = try ready(&replica);
    try write(&source, 1, 2);
    try source.batch(.{ .deletes = &.{"doc:00000005"} });
    try install(&source, 3, false);
    try install(&source, 4, true);
    try replay(&primary, &replica, &next);
    replica.close();
    replica_open = false;
    replica = try db_mod.DB.open(alloc, replica_path, options);
    replica_open = true;
    _ = try ready(&replica);
    const current = try replica.relationalIndexBuildStatus("tenant_id");
    try std.testing.expect(current.generation > first.generation);
    try std.testing.expectEqual(@as(usize, 2), (try scan(&replica, true)).count);
    try std.testing.expectEqual(next - 1, try replica.haAppliedReplicationLsn());
    try std.testing.expectEqual(@as(u64, 0), failures.load(.acquire));
    // The recovered replica's active plan must also serve fresh primary writes;
    // replay completion alone is insufficient evidence of a usable write plan.
    try write(&replica, 99, 1);
    try std.testing.expectEqual(@as(usize, 3), (try scan(&replica, true)).count);
}

test "relational index system primary prefix scans preserve snapshots bounds and failed pages" {
    var directory = try @import("../../common/test_directory.zig").TestDirectory.init("primary-prefix-scan");
    defer directory.cleanup();
    var db = try db_mod.DB.open(alloc, directory.path(), .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    try install(&db, 1, true);
    const keys = [_][]const u8{ "a", "a\x00", "a\x00\xff", "aa", "\xff", "\xff\x00" };
    for (keys) |key| try db.batch(.{ .writes = &.{.{ .key = key, .value = "{\"tenant\":1,\"id\":1}" }} });
    // A leftover companion with no primary must consume only one work unit.
    const orphan = try internal.relationalRowKeyAlloc(alloc, "0-orphan");
    defer alloc.free(orphan);
    orphan[orphan.len - 1] = internal.relational_row_kind + 1;
    try db.core.store.put(orphan, "orphan");
    var reader = try db.beginRelationalRows(alloc, .{ .fields = &.{"id"} });
    defer reader.deinit();
    var bounded = try db.beginRelationalRows(alloc, .{ .fields = &.{"id"}, .primary_lower = .{ .key = "a", .inclusive = false }, .primary_upper = .{ .key = "\xff", .inclusive = true } });
    defer bounded.deinit();
    try db.batch(.{ .writes = &.{ .{ .key = "a", .value = "{\"tenant\":1,\"id\":2}" }, .{ .key = "z", .value = "{\"tenant\":1,\"id\":3}" } }, .deletes = &.{"aa"} });
    var none: [0]u8 = .{};
    var fixed = std.heap.FixedBufferAllocator.init(&none);
    try std.testing.expectError(error.OutOfMemory, reader.nextPage(fixed.allocator(), null, .{ .records = 1 }));
    try std.testing.expectEqual(@as(usize, 0), reader.after.items.len);
    {
        var empty = try reader.nextPage(alloc, null, .{ .records = 1 });
        defer empty.deinit();
        try std.testing.expect(empty.more and empty.rows.len == 0 and empty.records_examined == 1);
    }
    try std.testing.expectError(error.RelationalRowResultTooLarge, reader.nextPage(alloc, null, .{ .output_bytes = 1 }));
    var count: usize = 0;
    var examined: usize = 1;
    while (true) {
        var page = try reader.nextPage(alloc, null, .{ .records = 1 });
        defer page.deinit();
        for (page.rows) |row| {
            try std.testing.expectEqualStrings(keys[count], row.key);
            try std.testing.expectEqualStrings("{\"id\":1}", row.json);
            count += 1;
        }
        examined += page.records_examined;
        if (!page.more) break;
    }
    try std.testing.expectEqual(keys.len, count);
    try std.testing.expectEqual(keys.len + 1, examined);
    var page = try bounded.nextPage(alloc, null, .{ .time_ns = std.time.ns_per_s });
    defer page.deinit();
    try std.testing.expectEqual(@as(usize, 4), page.rows.len);
    for (page.rows, keys[1..5]) |row, key| try std.testing.expectEqualStrings(key, row.key);
}

test "relational index system LSM build work is linear in rows times indexes" {
    const jobs = @import("relational_index_jobs.zig");
    const count = 64;
    for ([_]usize{ 1, 8, 32 }) |index_count| {
        var directory = try @import("../../common/test_directory.zig").TestDirectory.init("index-build-scaling");
        defer directory.cleanup();
        const options: db_mod.OpenOptions = .{ .primary_backend = .{ .lsm = .{ .flush_threshold = 128 } }, .start_index_workers = false, .start_optional_runtimes = false };
        var db = try db_mod.DB.open(alloc, directory.path(), options);
        defer db.close();
        const single = try schema(1, true);
        defer alloc.free(single);
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, single, .{});
        defer parsed.deinit();
        const owned = parsed.arena.allocator();
        const indexes = parsed.value.object.getPtr("relational_indexes").?;
        const template = indexes.array.items[0];
        for (1..index_count) |i| {
            var item = template;
            item.object = try template.object.clone(owned);
            try item.object.put(owned, "name", .{ .string = try std.fmt.allocPrint(owned, "index_{d}", .{i}) });
            try indexes.array.append(item);
        }
        const checked = try std.json.parseFromSliceLeaky(std.json.Value, owned, "[{\"name\":\"nonnegative\",\"column\":\"id\",\"op\":\"gte\",\"value\":0}]", .{});
        try parsed.value.object.put(owned, "checks", checked);
        // object.put can relocate its entries; retain the array value itself.
        const definitions = parsed.value.object.get("relational_indexes").?.array.items;
        const declaration = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
        defer alloc.free(declaration);
        try db.setSchemaJson(alloc, declaration);
        const edge_keys = [_][]const u8{ "a", "a\x00", "a\x00\xff", "aa" };
        for (0..count) |i| {
            if (i < edge_keys.len) {
                try db.batch(.{ .writes = &.{.{ .key = edge_keys[i], .value = "{\"tenant\":1,\"id\":1}" }} });
            } else try write(&db, i, i % 4);
        }
        // Reopen against persisted LSM data. Every unrelated generation is
        // populated before measuring even the first target's verification.
        db.close();
        db = try db_mod.DB.open(alloc, directory.path(), options);
        const started = time.monotonicNs();
        var examined: usize = 0;
        for (definitions) |item| {
            const name = item.object.get("name").?.string;
            var target_examined: usize = 0;
            for (0..256) |_| {
                var page = (try jobs.Page.prepare(alloc, std.testing.io, db.core, name, .{ .records = 7 })) orelse break;
                defer page.deinit();
                target_examined += page.records_examined;
                try page.commit(db.core);
            } else return error.IndexBuildDidNotConverge;
            try std.testing.expectEqual(@as(usize, 3 * count), target_examined);
            try std.testing.expectEqual(.ready, (try db.relationalIndexBuildStatus(name)).state);
            examined += target_examined;
        }
        try std.testing.expectEqual(3 * count * index_count, examined);
        const build_elapsed = time.monotonicNs() - started;
        var primary_rows: usize = 0;
        var primary_records: usize = 0;
        {
            var primary_reader = try db.beginRelationalRows(alloc, .{ .fields = &.{"id"} });
            defer primary_reader.deinit();
            while (true) {
                var page = try primary_reader.nextPage(alloc, std.testing.io, .{ .rows = 1, .records = 1 });
                defer page.deinit();
                primary_rows += page.rows.len;
                primary_records += page.records_examined;
                if (!page.more) break;
            }
        }
        try std.testing.expectEqual(count, primary_rows);
        try std.testing.expectEqual(count, primary_records);
        const checks = @import("relational_constraint_jobs.zig");
        var check_records: usize = 0;
        while (try checks.Page.prepare(alloc, std.testing.io, db.core, .{ .records = 7 })) |prepared| {
            {
                var page = prepared;
                defer page.deinit();
                check_records += page.records_examined;
                try page.commit(db.core);
            }
            // CHECK continuation is durable and resumes after the whole family.
            db.close();
            db = try db_mod.DB.open(alloc, directory.path(), options);
        }
        try std.testing.expectEqual(count, check_records);
        for (0..256) |_| {
            _ = try db.runRelationalIndexMaintenancePass();
            if (!db.relational_index_maintenance_sweep.isPending()) break;
        } else return error.MaintenanceDidNotBecomeIdle;
        try std.testing.expect(!try db.runRelationalIndexMaintenancePass() or index_count > 16);
        std.debug.print("LSM primary/CHECK scans indexes={d} rows={d} primary_records={d} check_records={d}\n", .{ index_count, count, primary_records, check_records });
        std.debug.print("LSM generation-local build indexes={d} rows={d} examined={d} elapsed_us={d}\n", .{ index_count, count, examined, build_elapsed / 1000 });
    }
}

test "relational index system LSM write rebuild query and churn work benchmark" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const path = try std.fmt.allocPrint(alloc, "{s}/lsm", .{root});
    defer alloc.free(path);
    var db = try db_mod.DB.open(alloc, path, .{ .primary_backend = .{ .lsm = .{ .flush_threshold = 128 } }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    const count = 256;
    try install(&db, 1, false);
    const start = time.monotonicNs();
    for (0..count) |id| try write(&db, id, id % 16);
    const write_ns = time.monotonicNs() - start;
    const build_start = time.monotonicNs();
    try install(&db, 2, true);
    const build_pages = try ready(&db);
    const build_ns = time.monotonicNs() - build_start;
    const primary_scan = try scan(&db, false);
    const index_scan = try scan(&db, true);
    try std.testing.expectEqual(@as(usize, count / 16), index_scan.count);
    try std.testing.expectEqual(primary_scan.count, index_scan.count);
    try std.testing.expect(index_scan.examined < primary_scan.examined);
    const churn_start = time.monotonicNs();
    for (0..4) |cycle| {
        for (0..64) |id| try write(&db, id, (id + cycle) % 16);
        try install(&db, @intCast(3 + cycle * 2), false);
        try install(&db, @intCast(4 + cycle * 2), true);
        _ = try ready(&db);
        for (0..2048) |_| {
            if (!try db.runRelationalIndexMaintenancePass()) break;
        } else return error.IndexGarbageDidNotConverge;
    }
    const churn_ns = time.monotonicNs() - churn_start;
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    var cursor = try read.openCursor();
    defer cursor.close();
    var forward: usize = 0;
    var reverse: usize = 0;
    var logical_bytes: usize = 0;
    var item = try cursor.seekAtOrAfter("");
    while (item) |entry| : (item = try cursor.next()) {
        if (records.isForwardKey(entry.key)) forward += 1;
        if (internal.isRelationalIndexReverseKey(entry.key)) reverse += 1;
        logical_bytes += entry.key.len + entry.value.len;
    }
    try std.testing.expectEqual(@as(usize, count), forward);
    try std.testing.expectEqual(@as(usize, count), reverse);
    const backend = db.core.primary_store_owner.lsmBackend() orelse return error.ExpectedLsmBackend;
    const usage = try backend.measurePhysicalUsage();
    const after = try scan(&db, true);
    try std.testing.expect(after.examined <= index_scan.examined + 1);
    std.debug.print("relational-index benchmark rows={} write_ns={} build_ns={} build_pages={} primary_records={} index_records={} churn_ns={} forward={} reverse={} logical_bytes={} active_sst_bytes={} obsolete_bytes={} wal_bytes={}\n", .{ count, write_ns, build_ns, build_pages, primary_scan.examined, index_scan.examined, churn_ns, forward, reverse, logical_bytes, usage.active_sst_bytes, usage.obsolete_file_bytes, usage.wal_retained_bytes });
}
