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

const std = @import("std");
const foreign_mod = @import("../foreign/mod.zig");
const table_writes_api = @import("../api/table_writes.zig");
const metadata_api = @import("api.zig");
const metadata_mod = @import("mod.zig");
const metadata_server = @import("server.zig");
const metadata_table_workflow = @import("table_workflow.zig");
const metadata_reconciler = @import("reconciler.zig");
const metadata_table_manager = @import("table_manager.zig");
const metadata_transition_state = @import("transition_state.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const tables_api = @import("../api/tables.zig");
const db_mod = @import("../storage/db/mod.zig");
const backend_types = @import("../storage/backend_types.zig");
const secrets = @import("../common/secrets.zig");
const pattern_filter = @import("../search/pattern_filter.zig");

const Allocator = std.mem.Allocator;
// CDC checkpoints are external resume positions. Only publish progress after
// the applied row is visible through query/index paths.
const cdc_apply_sync_level = db_mod.types.SyncLevel.full_index;

fn classifyReplicationError(err: anyerror) []const u8 {
    return switch (err) {
        error.ReplicationExactCutoverRequired,
        error.ForeignAuthFailed,
        error.ForeignTableNotFound,
        error.ForeignReplicationSlotMissing,
        error.UnknownColumn,
        error.InvalidQueryRequest,
        error.UnsupportedReplicationStreaming,
        => "terminal",
        else => "retryable",
    };
}

pub const BackfillSummary = struct {
    rows_applied: usize = 0,
    batches_applied: usize = 0,
    final_offset: usize = 0,
};

pub const SnapshotBackfillRunner = struct {
    alloc: Allocator,
    registry: *foreign_mod.Registry,
    write_source: table_writes_api.TableWriteSource,
    secret_store: ?*secrets.FileStore = null,
    batch_size: usize = 256,

    pub fn runTableSource(
        self: *SnapshotBackfillRunner,
        status_sink: anytype,
        table: metadata_table_manager.TableRecord,
        source_ordinal: u32,
    ) !BackfillSummary {
        return try self.runTableSourceFromOffset(status_sink, table, source_ordinal, 0);
    }

    pub fn runTableSourceFromOffset(
        self: *SnapshotBackfillRunner,
        status_sink: anytype,
        table: metadata_table_manager.TableRecord,
        source_ordinal: u32,
        start_offset: usize,
    ) !BackfillSummary {
        return try self.runTableSourceFromStatus(status_sink, table, source_ordinal, start_offset, null);
    }

    pub fn runTableSourceFromStatus(
        self: *SnapshotBackfillRunner,
        status_sink: anytype,
        table: metadata_table_manager.TableRecord,
        source_ordinal: u32,
        start_offset: usize,
        existing_status: ?metadata_table_manager.ReplicationSourceStatusRecord,
    ) !BackfillSummary {
        var progress_offset = start_offset;
        return self.runTableSourceInner(status_sink, table, source_ordinal, start_offset, existing_status, &progress_offset) catch |err| {
            var parsed = parseReplicationSourceConfig(self.alloc, table.name, table.replication_sources_json, source_ordinal) catch return err;
            defer parsed.deinit(self.alloc);

            callUpsertStatus(status_sink, .{
                .table_id = table.table_id,
                .source_ordinal = source_ordinal,
                .source_kind = parsed.type_name,
                .external_table = parsed.postgres_table,
                .slot_name = parsed.slot_name,
                .publication_name = parsed.publication_name,
                .phase = "failed",
                .checkpoint = checkpointSlice(progress_offset),
                .snapshot_offset = @intCast(progress_offset),
                .stream_checkpoint = "",
                .last_error = @errorName(err),
                .failure_class = classifyReplicationError(err),
                .lag_records = 0,
                .lag_millis = 0,
                .updated_at_ms = nowMillis(),
            }) catch {};
            return err;
        };
    }

    fn runTableSourceInner(
        self: *SnapshotBackfillRunner,
        status_sink: anytype,
        table: metadata_table_manager.TableRecord,
        source_ordinal: u32,
        start_offset: usize,
        existing_status: ?metadata_table_manager.ReplicationSourceStatusRecord,
        progress_offset: *usize,
    ) !BackfillSummary {
        var parsed = try parseReplicationSourceConfig(self.alloc, table.name, table.replication_sources_json, source_ordinal);
        defer parsed.deinit(self.alloc);

        var resolved_dsn = try secrets.resolveReferenceWithGenerationOwned(self.alloc, self.secret_store, parsed.dsn);
        defer resolved_dsn.deinit(self.alloc);
        std.log.info(
            "metadata cdc snapshot dsn resolved table={s} source={d} secret_generation={d} secret_source={s}",
            .{ table.name, source_ordinal, resolved_dsn.cacheGeneration(), @tagName(resolved_dsn.source) },
        );

        const phase_snapshot = "snapshot";
        const phase_complete = "cutover_prepared";
        const config = foreign_mod.Config{
            .kind = .postgres,
            .dsn = try self.alloc.dupe(u8, resolved_dsn.value),
        };
        var source = try self.registry.create(self.alloc, config);
        defer source.deinit(self.alloc);

        const persisted_prepared_checkpoint = if (existing_status) |status|
            if (status.prepared_checkpoint.len > 0) status.prepared_checkpoint else null
        else
            null;
        const persisted_cutover_mode = if (existing_status) |status|
            if (status.cutover_mode.len > 0) status.cutover_mode else null
        else
            null;
        if (parsed.require_exact_cutover) {
            if (persisted_cutover_mode) |mode| {
                if (!std.mem.eql(u8, mode, "exported_snapshot")) return error.ReplicationExactCutoverRequired;
            }
        }

        var prepare_params = foreign_mod.ReplicationPollParams{
            .table = try self.alloc.dupe(u8, parsed.postgres_table),
            .slot_name = try self.alloc.dupe(u8, parsed.slot_name),
            .publication_name = try self.alloc.dupe(u8, parsed.publication_name),
            .filter_query_json = if (parsed.publication_filter_json) |value| try self.alloc.dupe(u8, value) else null,
        };
        defer prepare_params.deinit(self.alloc);
        var exact_cutover_rejected = false;
        var prepared_snapshot = if (start_offset == 0 and persisted_prepared_checkpoint == null)
            source.beginPreparedReplicationSnapshot(self.alloc, prepare_params) catch |err| switch (err) {
                error.UnsupportedExactCutover => blk: {
                    if (parsed.require_exact_cutover) return error.ReplicationExactCutoverRequired;
                    exact_cutover_rejected = true;
                    break :blk null;
                },
                else => return err,
            }
        else
            null;
        defer if (prepared_snapshot) |*prepared| prepared.deinit(self.alloc);

        var prepare_result: foreign_mod.ReplicationPrepareResult = undefined;
        if (prepared_snapshot == null and persisted_prepared_checkpoint == null) {
            prepare_result = try source.prepareReplication(self.alloc, prepare_params);
            if (parsed.require_exact_cutover and prepare_result.slot_existed) {
                return error.ReplicationExactCutoverRequired;
            }
        }
        defer if (prepared_snapshot == null and persisted_prepared_checkpoint == null) prepare_result.deinit(self.alloc);
        var snapshot_reader = if (prepared_snapshot == null)
            source.beginSnapshotQuery(self.alloc) catch |err| switch (err) {
                error.UnsupportedConsistentSnapshot => null,
                else => return err,
            }
        else
            null;
        defer if (snapshot_reader) |*reader| reader.deinit(self.alloc);
        const prepared_checkpoint = if (persisted_prepared_checkpoint) |checkpoint|
            checkpoint
        else if (prepared_snapshot) |prepared|
            prepared.checkpoint
        else
            prepare_result.checkpoint;
        const cutover_mode = if (persisted_cutover_mode) |mode|
            mode
        else if (prepared_snapshot != null)
            "exported_snapshot"
        else if (prepare_result.slot_existed)
            "slot_resumed"
        else
            "slot_first";

        try callUpsertStatus(status_sink, .{
            .table_id = table.table_id,
            .source_ordinal = source_ordinal,
            .source_kind = parsed.type_name,
            .external_table = parsed.postgres_table,
            .cutover_mode = cutover_mode,
            .slot_name = parsed.slot_name,
            .publication_name = parsed.publication_name,
            .phase = phase_snapshot,
            .checkpoint = checkpointSlice(start_offset),
            .snapshot_offset = @intCast(start_offset),
            .prepared_checkpoint = prepared_checkpoint,
            .stream_checkpoint = "",
            .last_error = "",
            .failure_class = "",
            .lag_records = 0,
            .lag_millis = 0,
            .consecutive_failures = 0,
            .last_source_commit_at_ms = 0,
            .last_success_at_ms = 0,
            .last_change_applied_at_ms = 0,
            .updated_at_ms = nowMillis(),
        });

        var summary: BackfillSummary = .{ .final_offset = start_offset };
        progress_offset.* = start_offset;
        const snapshot_order_field = try deriveSnapshotOrderFieldAlloc(self.alloc, parsed.key_template);
        defer if (snapshot_order_field) |field| self.alloc.free(field);

        var bulk_ingest_tables: std.ArrayListUnmanaged([]const u8) = .empty;
        defer bulk_ingest_tables.deinit(self.alloc);
        try beginBackfillBulkIngestWindow(self.alloc, self.write_source, table.name, parsed, &bulk_ingest_tables);
        errdefer abortBackfillBulkIngestWindow(self.write_source, bulk_ingest_tables.items);

        while (true) {
            var params = foreign_mod.QueryParams{
                .table = try self.alloc.dupe(u8, parsed.postgres_table),
                .filter_query_json = if (parsed.publication_filter_json) |value| try self.alloc.dupe(u8, value) else null,
                .limit = self.batch_size,
                .offset = summary.final_offset,
                .order_by = if (snapshot_order_field) |field| blk: {
                    const order_by = try self.alloc.alloc(foreign_mod.SortField, 1);
                    order_by[0] = .{ .field = try self.alloc.dupe(u8, field), .desc = false };
                    break :blk order_by;
                } else &.{},
            };
            defer params.deinit(self.alloc);
            var result = if (prepared_snapshot) |prepared|
                try prepared.reader.query(self.alloc, params)
            else if (snapshot_reader) |reader|
                try reader.query(self.alloc, params)
            else
                try source.query(self.alloc, params);
            defer result.deinit(self.alloc);

            if (result.rows.len == 0) break;

            if (parsed.has_update_transforms or parsed.has_routes) {
                for (result.rows) |row| {
                    const empty_checkpoint = try self.alloc.dupe(u8, "");
                    defer self.alloc.free(empty_checkpoint);
                    _ = try applyReplicationChange(self.alloc, self.write_source, table.name, .{
                        .op = .insert,
                        .checkpoint = empty_checkpoint,
                        .row = row,
                    }, parsed);
                }
            } else {
                const writes = try rowsToBatchWritesAlloc(self.alloc, result.rows, parsed.key_template);
                defer freeBatchWritesOwned(self.alloc, writes);

                try applyReplicationBatchRequired(self.alloc, self.write_source, table.name, .{
                    .writes = writes,
                    .sync_level = cdc_apply_sync_level,
                });
            }

            summary.rows_applied += result.rows.len;
            summary.batches_applied += 1;
            summary.final_offset += result.rows.len;
            progress_offset.* = summary.final_offset;

            try callUpsertStatus(status_sink, .{
                .table_id = table.table_id,
                .source_ordinal = source_ordinal,
                .source_kind = parsed.type_name,
                .external_table = parsed.postgres_table,
                .cutover_mode = cutover_mode,
                .slot_name = parsed.slot_name,
                .publication_name = parsed.publication_name,
                .phase = phase_snapshot,
                .checkpoint = checkpointSlice(summary.final_offset),
                .snapshot_offset = @intCast(summary.final_offset),
                .prepared_checkpoint = prepared_checkpoint,
                .stream_checkpoint = "",
                .last_error = "",
                .failure_class = "",
                .lag_records = 0,
                .lag_millis = 0,
                .consecutive_failures = 0,
                .last_source_commit_at_ms = 0,
                .last_success_at_ms = nowMillis(),
                .last_change_applied_at_ms = nowMillis(),
                .updated_at_ms = nowMillis(),
            });

            if (result.rows.len < self.batch_size) break;
        }

        try finishBackfillBulkIngestWindow(self.alloc, self.write_source, bulk_ingest_tables.items);
        bulk_ingest_tables.clearRetainingCapacity();

        try callUpsertStatus(status_sink, .{
            .table_id = table.table_id,
            .source_ordinal = source_ordinal,
            .source_kind = parsed.type_name,
            .external_table = parsed.postgres_table,
            .cutover_mode = cutover_mode,
            .slot_name = parsed.slot_name,
            .publication_name = parsed.publication_name,
            .phase = phase_complete,
            .checkpoint = checkpointSlice(summary.final_offset),
            .snapshot_offset = @intCast(summary.final_offset),
            .prepared_checkpoint = prepared_checkpoint,
            .stream_checkpoint = "",
            .last_error = "",
            .failure_class = "",
            .lag_records = 0,
            .lag_millis = 0,
            .consecutive_failures = 0,
            .last_source_commit_at_ms = 0,
            .last_success_at_ms = if (summary.rows_applied > 0) nowMillis() else 0,
            .last_change_applied_at_ms = if (summary.rows_applied > 0) nowMillis() else 0,
            .updated_at_ms = nowMillis(),
        });

        return summary;
    }
};

fn beginBackfillBulkIngestWindow(
    alloc: Allocator,
    write_source: table_writes_api.TableWriteSource,
    table_name: []const u8,
    parsed: ParsedReplicationSourceConfig,
    opened_tables: *std.ArrayListUnmanaged([]const u8),
) !void {
    errdefer {
        abortBackfillBulkIngestWindow(write_source, opened_tables.items);
        opened_tables.clearRetainingCapacity();
    }

    if (parsed.routes.len == 0) {
        try beginBackfillBulkIngestTable(alloc, write_source, table_name, opened_tables);
        return;
    }

    for (parsed.routes) |route| {
        if (backfillBulkIngestTableOpened(opened_tables.items, route.target_table)) continue;
        try beginBackfillBulkIngestTable(alloc, write_source, route.target_table, opened_tables);
    }
}

fn beginBackfillBulkIngestTable(
    alloc: Allocator,
    write_source: table_writes_api.TableWriteSource,
    table_name: []const u8,
    opened_tables: *std.ArrayListUnmanaged([]const u8),
) !void {
    if ((try write_source.beginBulkIngest(alloc, table_name)) == null) return;
    try opened_tables.append(alloc, table_name);
}

fn finishBackfillBulkIngestWindow(
    alloc: Allocator,
    write_source: table_writes_api.TableWriteSource,
    opened_tables: []const []const u8,
) !void {
    for (opened_tables) |table_name| {
        _ = try write_source.finishBulkIngest(alloc, table_name, .{
            .compact = false,
            .max_deferred_l0_runs = 64,
        });
    }
}

fn abortBackfillBulkIngestWindow(
    write_source: table_writes_api.TableWriteSource,
    opened_tables: []const []const u8,
) void {
    for (opened_tables) |table_name| write_source.abortBulkIngest(table_name);
}

fn backfillBulkIngestTableOpened(opened_tables: []const []const u8, table_name: []const u8) bool {
    for (opened_tables) |opened| {
        if (std.mem.eql(u8, opened, table_name)) return true;
    }
    return false;
}

pub const BackfillRoundSummary = struct {
    tables_considered: usize = 0,
    sources_considered: usize = 0,
    sources_started: usize = 0,
    sources_resumed: usize = 0,
    sources_skipped_complete: usize = 0,
    sources_completed: usize = 0,
};

pub const StreamSummary = struct {
    changes_applied: usize = 0,
    writes_applied: usize = 0,
    deletes_applied: usize = 0,
    last_checkpoint_len: usize = 0,
};

const ReplicationApplySummary = struct {
    writes_applied: usize = 0,
    deletes_applied: usize = 0,
};

pub const StreamingRoundSummary = struct {
    tables_considered: usize = 0,
    sources_considered: usize = 0,
    sources_started: usize = 0,
    sources_resumed: usize = 0,
    sources_skipped_pending_snapshot: usize = 0,
    sources_polled: usize = 0,
    changes_applied: usize = 0,
};

pub const SnapshotBackfillCoordinator = struct {
    alloc: Allocator,
    runner: SnapshotBackfillRunner,

    pub fn runRound(self: *SnapshotBackfillCoordinator, service: anytype) !BackfillRoundSummary {
        const Service = switch (@typeInfo(@TypeOf(service))) {
            .pointer => |pointer| pointer.child,
            else => @TypeOf(service),
        };

        var readiness_snapshot = try metadata_api.captureSnapshot(self.alloc, service);
        defer metadata_api.freeSnapshot(self.alloc, service, &readiness_snapshot);

        const tables = try service.listProjectedTables(self.alloc);
        defer service.freeProjectedTables(self.alloc, tables);
        const statuses = if (@hasDecl(Service, "listProjectedReplicationSourceStatuses"))
            try service.listProjectedReplicationSourceStatuses(self.alloc)
        else
            &[_]metadata_table_manager.ReplicationSourceStatusRecord{};
        defer if (@hasDecl(Service, "freeProjectedReplicationSourceStatuses") and statuses.len > 0) {
            service.freeProjectedReplicationSourceStatuses(self.alloc, statuses);
        };

        var summary: BackfillRoundSummary = .{};
        for (tables) |table| {
            const source_count = try countReplicationSourcesJson(self.alloc, table.replication_sources_json);
            if (source_count == 0) continue;
            const table_ready = tableReadyForReplication(&readiness_snapshot, table.table_id);
            const has_existing_status = tableHasReplicationSourceStatus(statuses, table.table_id);
            if (!table_ready and !has_existing_status) {
                std.log.info("metadata cdc snapshot skip table not ready table={s}", .{table.name});
                continue;
            }
            summary.tables_considered += 1;

            for (0..source_count) |ordinal| {
                summary.sources_considered += 1;
                const existing = findReplicationSourceStatus(statuses, table.table_id, @intCast(ordinal));
                if (existing) |record| {
                    if (std.mem.eql(u8, record.phase, "snapshot_complete") or phaseAllowsStreaming(record.phase)) {
                        summary.sources_skipped_complete += 1;
                        continue;
                    }
                    if (std.mem.eql(u8, record.phase, "failed") and std.mem.eql(u8, record.failure_class, "terminal")) {
                        summary.sources_skipped_complete += 1;
                        continue;
                    }
                }

                const start_offset = if (existing) |record| snapshotOffsetForStatus(record) else 0;
                if (start_offset > 0) {
                    summary.sources_resumed += 1;
                } else {
                    summary.sources_started += 1;
                }

                _ = self.runner.runTableSourceFromStatus(service, table, @intCast(ordinal), start_offset, existing) catch |err| {
                    if (std.mem.eql(u8, classifyReplicationError(err), "terminal")) {
                        std.log.warn(
                            "metadata cdc snapshot source terminal error table={s} source={d} err={s}",
                            .{ table.name, ordinal, @errorName(err) },
                        );
                        continue;
                    }
                    return err;
                };
                summary.sources_completed += 1;
            }
        }

        return summary;
    }
};

pub const StreamingReplicationRunner = struct {
    alloc: Allocator,
    registry: *foreign_mod.Registry,
    write_source: table_writes_api.TableWriteSource,
    secret_store: ?*secrets.FileStore = null,
    batch_size: usize = 256,

    pub fn runTableSourceFromCheckpoint(
        self: *StreamingReplicationRunner,
        status_sink: anytype,
        table: metadata_table_manager.TableRecord,
        source_ordinal: u32,
        snapshot_offset: usize,
        cutover_mode: []const u8,
        resume_checkpoint: ?[]const u8,
        existing_status: ?metadata_table_manager.ReplicationSourceStatusRecord,
    ) !StreamSummary {
        var progress_checkpoint = std.ArrayListUnmanaged(u8).empty;
        defer progress_checkpoint.deinit(self.alloc);
        if (resume_checkpoint) |checkpoint| try progress_checkpoint.appendSlice(self.alloc, checkpoint);

        return self.runTableSourceInner(status_sink, table, source_ordinal, snapshot_offset, cutover_mode, resume_checkpoint, existing_status, &progress_checkpoint) catch |err| {
            var parsed = parseReplicationSourceConfig(self.alloc, table.name, table.replication_sources_json, source_ordinal) catch return err;
            defer parsed.deinit(self.alloc);
            const prior_failures = if (existing_status) |status|
                if (std.mem.eql(u8, status.phase, "streaming_failed")) status.consecutive_failures else 0
            else
                0;
            const last_success_at_ms = if (existing_status) |status| status.last_success_at_ms else 0;
            const last_change_applied_at_ms = if (existing_status) |status| status.last_change_applied_at_ms else 0;
            const last_source_commit_at_ms = if (existing_status) |status| status.last_source_commit_at_ms else 0;
            const lag_millis = if (existing_status) |status| status.lag_millis else 0;
            const prepared_checkpoint = if (existing_status) |status| status.prepared_checkpoint else "";

            callUpsertStatus(status_sink, .{
                .table_id = table.table_id,
                .source_ordinal = source_ordinal,
                .source_kind = parsed.type_name,
                .external_table = parsed.postgres_table,
                .cutover_mode = cutover_mode,
                .slot_name = parsed.slot_name,
                .publication_name = parsed.publication_name,
                .phase = "streaming_failed",
                .checkpoint = progress_checkpoint.items,
                .snapshot_offset = @intCast(snapshot_offset),
                .prepared_checkpoint = prepared_checkpoint,
                .stream_checkpoint = progress_checkpoint.items,
                .last_error = @errorName(err),
                .failure_class = classifyReplicationError(err),
                .lag_records = 0,
                .lag_millis = lag_millis,
                .consecutive_failures = prior_failures + 1,
                .last_source_commit_at_ms = last_source_commit_at_ms,
                .last_success_at_ms = last_success_at_ms,
                .last_change_applied_at_ms = last_change_applied_at_ms,
                .updated_at_ms = nowMillis(),
            }) catch {};
            return err;
        };
    }

    fn runTableSourceInner(
        self: *StreamingReplicationRunner,
        status_sink: anytype,
        table: metadata_table_manager.TableRecord,
        source_ordinal: u32,
        snapshot_offset: usize,
        cutover_mode: []const u8,
        resume_checkpoint: ?[]const u8,
        existing_status: ?metadata_table_manager.ReplicationSourceStatusRecord,
        progress_checkpoint: *std.ArrayListUnmanaged(u8),
    ) !StreamSummary {
        var parsed = try parseReplicationSourceConfig(self.alloc, table.name, table.replication_sources_json, source_ordinal);
        defer parsed.deinit(self.alloc);

        var resolved_dsn = try secrets.resolveReferenceWithGenerationOwned(self.alloc, self.secret_store, parsed.dsn);
        defer resolved_dsn.deinit(self.alloc);
        std.log.info(
            "metadata cdc stream dsn resolved table={s} source={d} secret_generation={d} secret_source={s}",
            .{ table.name, source_ordinal, resolved_dsn.cacheGeneration(), @tagName(resolved_dsn.source) },
        );

        const config = foreign_mod.Config{
            .kind = .postgres,
            .dsn = try self.alloc.dupe(u8, resolved_dsn.value),
        };
        var source = try self.registry.create(self.alloc, config);
        defer source.deinit(self.alloc);

        var params = foreign_mod.ReplicationPollParams{
            .table = try self.alloc.dupe(u8, parsed.postgres_table),
            .slot_name = try self.alloc.dupe(u8, parsed.slot_name),
            .publication_name = try self.alloc.dupe(u8, parsed.publication_name),
            .filter_query_json = if (parsed.publication_filter_json) |value| try self.alloc.dupe(u8, value) else null,
            .checkpoint = if (resume_checkpoint) |value| try self.alloc.dupe(u8, value) else null,
            .limit = self.batch_size,
        };
        defer params.deinit(self.alloc);

        const prior_last_change_applied_at_ms = if (existing_status) |status| status.last_change_applied_at_ms else 0;
        const prior_last_source_commit_at_ms = if (existing_status) |status| status.last_source_commit_at_ms else 0;
        const prior_lag_millis = if (existing_status) |status| status.lag_millis else 0;
        const prepared_checkpoint = if (existing_status) |status|
            if (status.prepared_checkpoint.len > 0) status.prepared_checkpoint else if (resume_checkpoint) |value| value else ""
        else if (resume_checkpoint) |value|
            value
        else
            "";

        std.log.info(
            "metadata cdc stream poll begin table={s} source={d} snapshot_offset={d} resume_checkpoint_len={d}",
            .{ table.name, source_ordinal, snapshot_offset, if (resume_checkpoint) |value| value.len else 0 },
        );
        var result = try source.pollChanges(self.alloc, params);
        defer result.deinit(self.alloc);
        std.log.info(
            "metadata cdc stream poll end table={s} source={d} changes={d} lag={d}",
            .{ table.name, source_ordinal, result.changes.len, result.lag_records },
        );

        var summary: StreamSummary = .{};

        if (result.changes.len == 0) {
            if (result.checkpoint.len > 0) {
                progress_checkpoint.clearRetainingCapacity();
                try progress_checkpoint.appendSlice(self.alloc, result.checkpoint);
            }
            try callUpsertStatus(status_sink, .{
                .table_id = table.table_id,
                .source_ordinal = source_ordinal,
                .source_kind = parsed.type_name,
                .external_table = parsed.postgres_table,
                .cutover_mode = cutover_mode,
                .slot_name = parsed.slot_name,
                .publication_name = parsed.publication_name,
                .phase = "streaming",
                .checkpoint = progress_checkpoint.items,
                .snapshot_offset = @intCast(snapshot_offset),
                .prepared_checkpoint = prepared_checkpoint,
                .stream_checkpoint = progress_checkpoint.items,
                .last_error = "",
                .failure_class = "",
                .lag_records = result.lag_records,
                .lag_millis = if (result.lag_millis > 0) result.lag_millis else prior_lag_millis,
                .consecutive_failures = 0,
                .last_source_commit_at_ms = prior_last_source_commit_at_ms,
                .last_success_at_ms = nowMillis(),
                .last_change_applied_at_ms = prior_last_change_applied_at_ms,
                .updated_at_ms = nowMillis(),
            });
            return summary;
        }

        for (result.changes) |change| {
            const apply_summary = try applyReplicationChange(self.alloc, self.write_source, table.name, change, parsed);
            progress_checkpoint.clearRetainingCapacity();
            try progress_checkpoint.appendSlice(self.alloc, change.checkpoint);
            const applied_at_ms = nowMillis();
            const applied_lag_ms: u64 = if (change.commit_timestamp_ms > 0)
                @intCast(@max(@as(i64, 0), @as(i64, @intCast(applied_at_ms)) - @as(i64, @intCast(change.commit_timestamp_ms))))
            else if (result.lag_millis > 0) result.lag_millis else 0;

            summary.changes_applied += 1;
            summary.last_checkpoint_len = change.checkpoint.len;
            summary.writes_applied += apply_summary.writes_applied;
            summary.deletes_applied += apply_summary.deletes_applied;

            try callUpsertStatus(status_sink, .{
                .table_id = table.table_id,
                .source_ordinal = source_ordinal,
                .source_kind = parsed.type_name,
                .external_table = parsed.postgres_table,
                .cutover_mode = cutover_mode,
                .slot_name = parsed.slot_name,
                .publication_name = parsed.publication_name,
                .phase = "streaming",
                .checkpoint = progress_checkpoint.items,
                .snapshot_offset = @intCast(snapshot_offset),
                .prepared_checkpoint = prepared_checkpoint,
                .stream_checkpoint = progress_checkpoint.items,
                .last_error = "",
                .failure_class = "",
                .lag_records = if (change.lag_records > 0) change.lag_records else result.lag_records,
                .lag_millis = applied_lag_ms,
                .consecutive_failures = 0,
                .last_source_commit_at_ms = change.commit_timestamp_ms,
                .last_success_at_ms = nowMillis(),
                .last_change_applied_at_ms = applied_at_ms,
                .updated_at_ms = nowMillis(),
            });
        }

        return summary;
    }
};

pub const StreamingReplicationCoordinator = struct {
    alloc: Allocator,
    runner: StreamingReplicationRunner,

    pub fn runRound(self: *StreamingReplicationCoordinator, service: anytype) !StreamingRoundSummary {
        const Service = switch (@typeInfo(@TypeOf(service))) {
            .pointer => |pointer| pointer.child,
            else => @TypeOf(service),
        };

        var readiness_snapshot = try metadata_api.captureSnapshot(self.alloc, service);
        defer metadata_api.freeSnapshot(self.alloc, service, &readiness_snapshot);

        const tables = try service.listProjectedTables(self.alloc);
        defer service.freeProjectedTables(self.alloc, tables);
        const statuses = if (@hasDecl(Service, "listProjectedReplicationSourceStatuses"))
            try service.listProjectedReplicationSourceStatuses(self.alloc)
        else
            &[_]metadata_table_manager.ReplicationSourceStatusRecord{};
        defer if (@hasDecl(Service, "freeProjectedReplicationSourceStatuses") and statuses.len > 0) {
            service.freeProjectedReplicationSourceStatuses(self.alloc, statuses);
        };

        var summary: StreamingRoundSummary = .{};
        for (tables) |table| {
            const source_count = try countReplicationSourcesJson(self.alloc, table.replication_sources_json);
            if (source_count == 0) continue;
            const table_ready = tableReadyForReplication(&readiness_snapshot, table.table_id);
            const status_allows_streaming = tableHasStreamingEligibleStatus(statuses, table.table_id);
            if (!table_ready and !status_allows_streaming) {
                std.log.info("metadata cdc stream skip table not ready table={s}", .{table.name});
                continue;
            }
            summary.tables_considered += 1;

            for (0..source_count) |ordinal| {
                summary.sources_considered += 1;
                const existing = findReplicationSourceStatus(statuses, table.table_id, @intCast(ordinal)) orelse {
                    std.log.info(
                        "metadata cdc stream skip pending snapshot table={s} source={d} reason=no_status",
                        .{ table.name, ordinal },
                    );
                    summary.sources_skipped_pending_snapshot += 1;
                    continue;
                };
                if (!phaseAllowsStreaming(existing.phase)) {
                    std.log.info(
                        "metadata cdc stream skip pending snapshot table={s} source={d} phase={s}",
                        .{ table.name, ordinal, existing.phase },
                    );
                    summary.sources_skipped_pending_snapshot += 1;
                    continue;
                }

                const snapshot_offset = snapshotOffsetForStatus(existing);
                const resume_checkpoint = checkpointForStreaming(existing);
                std.log.info(
                    "metadata cdc stream consider table={s} source={d} phase={s} snapshot_offset={d} checkpoint_len={d}",
                    .{ table.name, ordinal, existing.phase, snapshot_offset, if (resume_checkpoint) |value| value.len else 0 },
                );
                if (resume_checkpoint) |_| summary.sources_resumed += 1 else summary.sources_started += 1;

                const stream_summary = self.runner.runTableSourceFromCheckpoint(
                    service,
                    table,
                    @intCast(ordinal),
                    snapshot_offset,
                    existing.cutover_mode,
                    resume_checkpoint,
                    existing,
                ) catch |err| {
                    if (std.mem.eql(u8, classifyReplicationError(err), "terminal")) {
                        std.log.warn(
                            "metadata cdc stream source terminal error table={s} source={d} err={s}",
                            .{ table.name, ordinal, @errorName(err) },
                        );
                    }
                    return err;
                };
                summary.sources_polled += 1;
                summary.changes_applied += stream_summary.changes_applied;
            }
        }

        return summary;
    }
};

const ParsedReplicationSourceConfig = struct {
    type_name: []u8,
    dsn: []u8,
    postgres_table: []u8,
    key_template: []u8,
    slot_name: []u8,
    publication_name: []u8,
    require_exact_cutover: bool = false,
    on_update_json: ?[]u8 = null,
    on_delete_json: ?[]u8 = null,
    publication_filter_json: ?[]u8 = null,
    routes: []ParsedReplicationRouteConfig = &.{},
    has_update_transforms: bool = false,
    has_delete_transforms: bool = false,
    delete_document_on_delete: bool = false,
    has_routes: bool = false,

    fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.type_name);
        alloc.free(self.dsn);
        alloc.free(self.postgres_table);
        alloc.free(self.key_template);
        alloc.free(self.slot_name);
        alloc.free(self.publication_name);
        if (self.on_update_json) |value| alloc.free(value);
        if (self.on_delete_json) |value| alloc.free(value);
        if (self.publication_filter_json) |value| alloc.free(value);
        for (self.routes) |*route| route.deinit(alloc);
        if (self.routes.len > 0) alloc.free(self.routes);
        self.* = undefined;
    }
};

const ParsedReplicationRouteConfig = struct {
    target_table: []u8,
    where_json: ?[]u8 = null,
    key_template: ?[]u8 = null,
    on_update_json: ?[]u8 = null,
    on_delete_json: ?[]u8 = null,
    delete_document_on_delete: bool = false,

    fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.target_table);
        if (self.where_json) |value| alloc.free(value);
        if (self.key_template) |value| alloc.free(value);
        if (self.on_update_json) |value| alloc.free(value);
        if (self.on_delete_json) |value| alloc.free(value);
        self.* = undefined;
    }
};

fn parseReplicationSourceConfig(
    alloc: Allocator,
    table_name: []const u8,
    replication_sources_json: []const u8,
    source_ordinal: u32,
) !ParsedReplicationSourceConfig {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, replication_sources_json, .{});
    defer parsed.deinit();

    if (parsed.value != .array) return error.InvalidReplicationSourceConfig;
    if (source_ordinal >= parsed.value.array.items.len) return error.UnknownReplicationSource;

    const source = parsed.value.array.items[source_ordinal];
    if (source != .object) return error.InvalidReplicationSourceConfig;

    const type_name = try parseRequiredStringFieldAlloc(alloc, source, "type");
    errdefer alloc.free(type_name);
    if (!std.mem.eql(u8, type_name, "postgres")) return error.UnsupportedReplicationSource;

    const dsn = try parseRequiredStringFieldAlloc(alloc, source, "dsn");
    errdefer alloc.free(dsn);
    const postgres_table = try parseRequiredStringFieldAlloc(alloc, source, "postgres_table");
    errdefer alloc.free(postgres_table);

    const key_template = if (source.object.get("key_template")) |value| blk: {
        if (value == .null) break :blk try alloc.dupe(u8, "");
        break :blk try parseStringValueAlloc(alloc, value);
    } else try alloc.dupe(u8, "");
    errdefer alloc.free(key_template);
    const slot_name = if (source.object.get("slot_name")) |value| blk: {
        if (value == .null) break :blk try deriveSlotNameAlloc(alloc, table_name, postgres_table);
        break :blk try parseStringValueAlloc(alloc, value);
    } else try deriveSlotNameAlloc(alloc, table_name, postgres_table);
    errdefer alloc.free(slot_name);
    const publication_name = if (source.object.get("publication_name")) |value| blk: {
        if (value == .null) break :blk try derivePublicationNameAlloc(alloc, table_name, postgres_table);
        break :blk try parseStringValueAlloc(alloc, value);
    } else try derivePublicationNameAlloc(alloc, table_name, postgres_table);
    errdefer alloc.free(publication_name);
    const require_exact_cutover = if (source.object.get("require_exact_cutover")) |value| blk: {
        switch (value) {
            .null => break :blk false,
            .bool => |enabled| break :blk enabled,
            else => return error.InvalidReplicationSourceConfig,
        }
    } else false;
    const routes: []ParsedReplicationRouteConfig = if (source.object.get("routes")) |value| blk: {
        if (value == .null) break :blk &.{};
        break :blk try parseReplicationRoutesAlloc(alloc, value);
    } else &.{};

    var out: ParsedReplicationSourceConfig = .{
        .type_name = type_name,
        .dsn = dsn,
        .postgres_table = postgres_table,
        .key_template = key_template,
        .slot_name = slot_name,
        .publication_name = publication_name,
        .require_exact_cutover = require_exact_cutover,
        .on_update_json = if (source.object.get("on_update")) |value|
            if (value == .null) null else try std.json.Stringify.valueAlloc(alloc, value, .{})
        else
            null,
        .on_delete_json = if (source.object.get("on_delete")) |value|
            if (value == .null) null else try std.json.Stringify.valueAlloc(alloc, value, .{})
        else
            null,
        .publication_filter_json = if (source.object.get("publication_filter")) |value|
            if (value == .null) null else try std.json.Stringify.valueAlloc(alloc, value, .{})
        else
            null,
        .routes = routes,
        .has_update_transforms = hasNonEmptyArrayField(source, "on_update"),
        .has_delete_transforms = hasNonEmptyArrayField(source, "on_delete"),
        .delete_document_on_delete = arrayContainsDeleteDocumentOp(source, "on_delete"),
        .has_routes = routes.len > 0,
    };
    errdefer out.deinit(alloc);
    return out;
}

fn parseReplicationRoutesAlloc(alloc: Allocator, value: std.json.Value) ![]ParsedReplicationRouteConfig {
    if (value != .array) return error.InvalidReplicationSourceConfig;
    if (value.array.items.len == 0) return &.{};

    const routes = try alloc.alloc(ParsedReplicationRouteConfig, value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (routes[0..initialized]) |*route| route.deinit(alloc);
        alloc.free(routes);
    }

    for (value.array.items, 0..) |item, i| {
        if (item != .object) return error.InvalidReplicationSourceConfig;
        routes[i] = .{
            .target_table = try parseRequiredStringFieldAlloc(alloc, item, "target_table"),
            .where_json = if (item.object.get("where")) |where_value|
                if (where_value == .null) null else try std.json.Stringify.valueAlloc(alloc, where_value, .{})
            else
                null,
            .key_template = if (item.object.get("key_template")) |key_value|
                if (key_value == .null) null else try parseStringValueAlloc(alloc, key_value)
            else
                null,
            .on_update_json = if (item.object.get("on_update")) |update_value|
                if (update_value == .null) null else try std.json.Stringify.valueAlloc(alloc, update_value, .{})
            else
                null,
            .on_delete_json = if (item.object.get("on_delete")) |delete_value|
                if (delete_value == .null) null else try std.json.Stringify.valueAlloc(alloc, delete_value, .{})
            else
                null,
            .delete_document_on_delete = arrayContainsDeleteDocumentOp(item, "on_delete"),
        };
        initialized += 1;
    }

    return routes;
}

fn hasNonEmptyArrayField(value: std.json.Value, field: []const u8) bool {
    const child = value.object.get(field) orelse return false;
    return child == .array and child.array.items.len > 0;
}

fn arrayContainsDeleteDocumentOp(value: std.json.Value, field: []const u8) bool {
    const child = value.object.get(field) orelse return false;
    if (child != .array) return false;
    for (child.array.items) |item| {
        if (item != .object) continue;
        const op_value = item.object.get("op") orelse continue;
        if (op_value != .string) continue;
        if (std.mem.eql(u8, op_value.string, "$delete_document")) return true;
    }
    return false;
}

fn parseRequiredStringFieldAlloc(alloc: Allocator, value: std.json.Value, field: []const u8) ![]u8 {
    const child = value.object.get(field) orelse return error.InvalidReplicationSourceConfig;
    return try parseStringValueAlloc(alloc, child);
}

fn parseStringValueAlloc(alloc: Allocator, value: std.json.Value) ![]u8 {
    return switch (value) {
        .string => |text| try alloc.dupe(u8, text),
        else => error.InvalidReplicationSourceConfig,
    };
}

fn rowsToBatchWritesAlloc(
    alloc: Allocator,
    rows: []const std.json.Value,
    key_template: []const u8,
) ![]db_mod.types.BatchWrite {
    const writes = try alloc.alloc(db_mod.types.BatchWrite, rows.len);
    var initialized: usize = 0;
    errdefer {
        for (writes[0..initialized]) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        alloc.free(writes);
    }

    for (rows, 0..) |row, i| {
        writes[i] = .{
            .key = try renderDocumentKeyAlloc(alloc, row, key_template),
            .value = try std.json.Stringify.valueAlloc(alloc, row, .{}),
        };
        initialized += 1;
    }

    return writes;
}

fn applyReplicationBatchRequired(
    alloc: Allocator,
    write_source: table_writes_api.TableWriteSource,
    table_name: []const u8,
    req: db_mod.types.BatchRequest,
) !void {
    if ((try write_source.batch(alloc, table_name, req)) == null) {
        return error.UnsupportedReplicationRoute;
    }
}

fn applyReplicationChange(
    alloc: Allocator,
    write_source: table_writes_api.TableWriteSource,
    table_name: []const u8,
    change: foreign_mod.ReplicationChange,
    parsed: ParsedReplicationSourceConfig,
) !ReplicationApplySummary {
    if (parsed.routes.len > 0) {
        return try applyReplicationChangeRouted(alloc, write_source, change, parsed);
    }

    switch (change.op) {
        .insert, .update => {
            const row = change.row orelse return error.InvalidReplicationSourceRow;
            const key = try renderReplicationChangeKeyAlloc(alloc, change, parsed.key_template);
            defer alloc.free(key);
            const transforms = if (parsed.on_update_json) |on_update_json|
                try resolveConfiguredTransformOpsAlloc(alloc, on_update_json, row)
            else
                try autoSetTransformOpsAlloc(alloc, row);
            defer freeTransformOpsOwned(alloc, transforms);
            const doc_transforms = [_]db_mod.types.DocumentTransform{.{
                .key = key,
                .operations = transforms,
                .upsert = true,
            }};
            try applyReplicationBatchRequired(alloc, write_source, table_name, .{
                .transforms = doc_transforms[0..],
                .sync_level = cdc_apply_sync_level,
            });
            return .{ .writes_applied = 1 };
        },
        .delete => {
            const key = try renderReplicationChangeKeyAlloc(alloc, change, parsed.key_template);
            defer alloc.free(key);
            if (parsed.delete_document_on_delete or change.row == null) {
                const deletes = [_][]const u8{key};
                try applyReplicationBatchRequired(alloc, write_source, table_name, .{
                    .deletes = deletes[0..],
                    .sync_level = cdc_apply_sync_level,
                });
                return .{ .deletes_applied = 1 };
            } else {
                const transforms = if (parsed.on_delete_json) |on_delete_json|
                    try resolveConfiguredTransformOpsAlloc(alloc, on_delete_json, change.row.?)
                else if (parsed.on_update_json) |on_update_json|
                    try deriveUnsetTransformOpsFromUpdateAlloc(alloc, on_update_json, parsed.key_template)
                else
                    try autoUnsetTransformOpsAlloc(alloc, change.row.?, parsed.key_template);
                defer freeTransformOpsOwned(alloc, transforms);
                const doc_transforms = [_]db_mod.types.DocumentTransform{.{
                    .key = key,
                    .operations = transforms,
                    .upsert = false,
                }};
                try applyReplicationBatchRequired(alloc, write_source, table_name, .{
                    .transforms = doc_transforms[0..],
                    .sync_level = cdc_apply_sync_level,
                });
                return .{ .deletes_applied = 1 };
            }
        },
    }
}

fn applyReplicationChangeRouted(
    alloc: Allocator,
    write_source: table_writes_api.TableWriteSource,
    change: foreign_mod.ReplicationChange,
    parsed: ParsedReplicationSourceConfig,
) !ReplicationApplySummary {
    const row = change.row orelse return error.InvalidReplicationSourceRow;
    var summary: ReplicationApplySummary = .{};

    for (parsed.routes) |route| {
        const key = try renderRouteChangeKeyAlloc(alloc, change, parsed, route);
        defer alloc.free(key);
        if (!(try routeMatchesRow(alloc, route, key, row))) continue;

        switch (change.op) {
            .insert, .update => {
                const transforms = if (route.on_update_json) |on_update_json|
                    try resolveConfiguredTransformOpsAlloc(alloc, on_update_json, row)
                else
                    try autoSetTransformOpsAlloc(alloc, row);
                defer freeTransformOpsOwned(alloc, transforms);
                const doc_transforms = [_]db_mod.types.DocumentTransform{.{
                    .key = key,
                    .operations = transforms,
                    .upsert = true,
                }};
                try applyReplicationBatchRequired(alloc, write_source, route.target_table, .{
                    .transforms = doc_transforms[0..],
                    .sync_level = cdc_apply_sync_level,
                });
                summary.writes_applied += 1;
            },
            .delete => {
                if (route.delete_document_on_delete) {
                    const deletes = [_][]const u8{key};
                    try applyReplicationBatchRequired(alloc, write_source, route.target_table, .{
                        .deletes = deletes[0..],
                        .sync_level = cdc_apply_sync_level,
                    });
                    summary.deletes_applied += 1;
                } else {
                    const transforms = if (route.on_delete_json) |on_delete_json|
                        try resolveConfiguredTransformOpsAlloc(alloc, on_delete_json, row)
                    else if (route.on_update_json) |on_update_json|
                        try deriveUnsetTransformOpsFromUpdateAlloc(alloc, on_update_json, route.key_template orelse parsed.key_template)
                    else
                        try autoUnsetTransformOpsAlloc(alloc, row, route.key_template orelse parsed.key_template);
                    defer freeTransformOpsOwned(alloc, transforms);
                    const doc_transforms = [_]db_mod.types.DocumentTransform{.{
                        .key = key,
                        .operations = transforms,
                        .upsert = false,
                    }};
                    try applyReplicationBatchRequired(alloc, write_source, route.target_table, .{
                        .transforms = doc_transforms[0..],
                        .sync_level = cdc_apply_sync_level,
                    });
                    summary.deletes_applied += 1;
                }
            },
        }
    }

    return summary;
}

fn renderRouteChangeKeyAlloc(
    alloc: Allocator,
    change: foreign_mod.ReplicationChange,
    parsed: ParsedReplicationSourceConfig,
    route: ParsedReplicationRouteConfig,
) ![]u8 {
    if (route.key_template == null) {
        return try renderReplicationChangeKeyAlloc(alloc, change, parsed.key_template);
    }
    const row = change.row orelse return error.InvalidReplicationSourceRow;
    return try renderDocumentKeyAlloc(alloc, row, route.key_template.?);
}

fn routeMatchesRow(
    alloc: Allocator,
    route: ParsedReplicationRouteConfig,
    key: []const u8,
    row: std.json.Value,
) !bool {
    if (route.where_json) |where_json| {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, where_json, .{});
        defer parsed.deinit();
        var normalized = try normalizeRouteFilterQueryAlloc(alloc, parsed.value);
        defer foreign_mod.deinitJsonValue(alloc, &normalized);
        return try pattern_filter.jsonDocMatchesPatternFilter(alloc, key, row, normalized);
    }
    return true;
}

fn normalizeRouteFilterQueryAlloc(alloc: Allocator, value: std.json.Value) !std.json.Value {
    if (value != .object) return try cloneJsonValueAllocLocal(alloc, value);

    if (value.object.get("conjuncts")) |conjuncts| {
        if (conjuncts != .array) return error.InvalidReplicationSourceConfig;
        var items = std.json.Array.init(alloc);
        errdefer {
            for (items.items) |*item| foreign_mod.deinitJsonValue(alloc, item);
            items.deinit();
        }
        for (conjuncts.array.items) |item| try items.append(try normalizeRouteFilterQueryAlloc(alloc, item));
        var obj = std.json.ObjectMap.empty;
        errdefer {
            var it = obj.iterator();
            while (it.next()) |entry| {
                alloc.free(@constCast(entry.key_ptr.*));
                foreign_mod.deinitJsonValue(alloc, entry.value_ptr);
            }
            obj.deinit(alloc);
        }
        try obj.put(alloc, try alloc.dupe(u8, "conjuncts"), .{ .array = items });
        return .{ .object = obj };
    }

    if (value.object.get("disjuncts")) |disjuncts| {
        if (disjuncts != .array) return error.InvalidReplicationSourceConfig;
        var items = std.json.Array.init(alloc);
        errdefer {
            for (items.items) |*item| foreign_mod.deinitJsonValue(alloc, item);
            items.deinit();
        }
        for (disjuncts.array.items) |item| try items.append(try normalizeRouteFilterQueryAlloc(alloc, item));
        var obj = std.json.ObjectMap.empty;
        errdefer {
            var it = obj.iterator();
            while (it.next()) |entry| {
                alloc.free(@constCast(entry.key_ptr.*));
                foreign_mod.deinitJsonValue(alloc, entry.value_ptr);
            }
            obj.deinit(alloc);
        }
        try obj.put(alloc, try alloc.dupe(u8, "disjuncts"), .{ .array = items });
        return .{ .object = obj };
    }

    if (value.object.get("bool")) |bool_query| {
        if (bool_query != .object) return error.InvalidReplicationSourceConfig;
        var inner = std.json.ObjectMap.empty;
        errdefer {
            var it = inner.iterator();
            while (it.next()) |entry| {
                alloc.free(@constCast(entry.key_ptr.*));
                foreign_mod.deinitJsonValue(alloc, entry.value_ptr);
            }
            inner.deinit(alloc);
        }
        inline for (.{ "must", "filter", "should", "must_not" }) |field_name| {
            if (bool_query.object.get(field_name)) |items_value| {
                if (items_value != .array) return error.InvalidReplicationSourceConfig;
                var items = std.json.Array.init(alloc);
                errdefer {
                    for (items.items) |*item| foreign_mod.deinitJsonValue(alloc, item);
                    items.deinit();
                }
                for (items_value.array.items) |item| try items.append(try normalizeRouteFilterQueryAlloc(alloc, item));
                try inner.put(alloc, try alloc.dupe(u8, field_name), .{ .array = items });
            }
        }
        return .{ .object = blk: {
            var obj = std.json.ObjectMap.empty;
            try obj.put(alloc, try alloc.dupe(u8, "bool"), .{ .object = inner });
            break :blk obj;
        } };
    }

    if (value.object.get("field")) |field_value| {
        if (field_value != .string) return error.InvalidReplicationSourceConfig;
        inline for (.{ "term", "match", "prefix", "wildcard", "regexp" }) |operator_name| {
            if (value.object.get(operator_name)) |operator_value| {
                var outer = std.json.ObjectMap.empty;
                errdefer {
                    var it = outer.iterator();
                    while (it.next()) |entry| {
                        alloc.free(@constCast(entry.key_ptr.*));
                        foreign_mod.deinitJsonValue(alloc, entry.value_ptr);
                    }
                    outer.deinit(alloc);
                }
                var inner = std.json.ObjectMap.empty;
                errdefer {
                    var it = inner.iterator();
                    while (it.next()) |entry| {
                        alloc.free(@constCast(entry.key_ptr.*));
                        foreign_mod.deinitJsonValue(alloc, entry.value_ptr);
                    }
                    inner.deinit(alloc);
                }
                try inner.put(alloc, try alloc.dupe(u8, field_value.string), try cloneJsonValueAllocLocal(alloc, operator_value));
                try outer.put(alloc, try alloc.dupe(u8, operator_name), .{ .object = inner });
                return .{ .object = outer };
            }
        }
    }

    return try cloneJsonValueAllocLocal(alloc, value);
}

test "metadata replication route bool filter clauses are required with must clauses" {
    const alloc = std.testing.allocator;

    var active = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"tenant":"acme","status":"active"}
    , .{});
    defer active.deinit();
    var inactive = try std.json.parseFromSlice(std.json.Value, alloc,
        \\{"tenant":"acme","status":"inactive"}
    , .{});
    defer inactive.deinit();

    const route = ParsedReplicationRouteConfig{
        .target_table = @constCast("target"[0..]),
        .where_json = @constCast(
            \\{"bool":{"must":[{"term":{"tenant":"acme"}}],"filter":[{"term":{"status":"active"}}]}}
        [0..]),
    };

    try std.testing.expect(try routeMatchesRow(alloc, route, "doc:active", active.value));
    try std.testing.expect(!(try routeMatchesRow(alloc, route, "doc:inactive", inactive.value)));
}

fn resolveConfiguredTransformOpsAlloc(
    alloc: Allocator,
    transforms_json: []const u8,
    row: std.json.Value,
) ![]db_mod.types.TransformOp {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, transforms_json, .{});
    defer parsed.deinit();
    if (parsed.value != .array) return error.InvalidReplicationSourceConfig;

    var out = std.ArrayListUnmanaged(db_mod.types.TransformOp).empty;
    errdefer freeTransformOpsOwned(alloc, out.items);

    for (parsed.value.array.items) |item| {
        if (item != .object) return error.InvalidReplicationSourceConfig;
        const op_value = item.object.get("op") orelse return error.InvalidReplicationSourceConfig;
        if (op_value != .string) return error.InvalidReplicationSourceConfig;
        const op_name = op_value.string;

        if (std.mem.eql(u8, op_name, "$delete_document")) continue;
        if (std.mem.eql(u8, op_name, "$merge")) {
            const merge_value = item.object.get("value") orelse return error.InvalidReplicationSourceConfig;
            var resolved = try resolveTransformValueAlloc(alloc, merge_value, row);
            defer foreign_mod.deinitJsonValue(alloc, &resolved);
            if (resolved != .object) return error.InvalidReplicationSourceConfig;

            var it = resolved.object.iterator();
            while (it.next()) |entry| {
                try out.append(alloc, .{
                    .op = .set,
                    .path = try alloc.dupe(u8, entry.key_ptr.*),
                    .value_json = try std.json.Stringify.valueAlloc(alloc, entry.value_ptr.*, .{}),
                });
            }
            continue;
        }

        const path_value = item.object.get("path") orelse return error.InvalidReplicationSourceConfig;
        if (path_value != .string) return error.InvalidReplicationSourceConfig;
        const transform_op = try mapTransformOpType(op_name);

        if (transform_op == .unset or transform_op == .current_date) {
            try out.append(alloc, .{
                .op = transform_op,
                .path = try alloc.dupe(u8, path_value.string),
                .value_json = null,
            });
            continue;
        }

        const value = item.object.get("value") orelse return error.InvalidReplicationSourceConfig;
        const value_json = try resolveTransformValueJsonAlloc(alloc, value, row);
        errdefer alloc.free(value_json);
        try out.append(alloc, .{
            .op = transform_op,
            .path = try alloc.dupe(u8, path_value.string),
            .value_json = value_json,
        });
    }

    return try out.toOwnedSlice(alloc);
}

fn deriveUnsetTransformOpsFromUpdateAlloc(
    alloc: Allocator,
    on_update_json: []const u8,
    key_template: []const u8,
) ![]db_mod.types.TransformOp {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, on_update_json, .{});
    defer parsed.deinit();
    if (parsed.value != .array) return error.InvalidReplicationSourceConfig;

    var out = std.ArrayListUnmanaged(db_mod.types.TransformOp).empty;
    errdefer freeTransformOpsOwned(alloc, out.items);
    for (parsed.value.array.items) |item| {
        if (item != .object) return error.InvalidReplicationSourceConfig;
        const op_value = item.object.get("op") orelse return error.InvalidReplicationSourceConfig;
        if (op_value != .string) return error.InvalidReplicationSourceConfig;
        if (!std.mem.eql(u8, op_value.string, "$set")) continue;
        const path_value = item.object.get("path") orelse return error.InvalidReplicationSourceConfig;
        if (path_value != .string) return error.InvalidReplicationSourceConfig;
        if (keyTemplateContainsField(key_template, path_value.string)) continue;
        try out.append(alloc, .{
            .op = .unset,
            .path = try alloc.dupe(u8, path_value.string),
            .value_json = null,
        });
    }
    return try out.toOwnedSlice(alloc);
}

fn mapTransformOpType(op_name: []const u8) !db_mod.types.TransformOpType {
    if (std.mem.eql(u8, op_name, "$set")) return .set;
    if (std.mem.eql(u8, op_name, "$unset")) return .unset;
    if (std.mem.eql(u8, op_name, "$inc")) return .inc;
    if (std.mem.eql(u8, op_name, "$push")) return .push;
    if (std.mem.eql(u8, op_name, "$pull")) return .pull;
    if (std.mem.eql(u8, op_name, "$addToSet")) return .add_to_set;
    if (std.mem.eql(u8, op_name, "$pop")) return .pop;
    if (std.mem.eql(u8, op_name, "$mul")) return .mul;
    if (std.mem.eql(u8, op_name, "$min")) return .min;
    if (std.mem.eql(u8, op_name, "$max")) return .max;
    if (std.mem.eql(u8, op_name, "$currentDate")) return .current_date;
    if (std.mem.eql(u8, op_name, "$rename")) return .rename;
    return error.UnsupportedReplicationTransform;
}

fn resolveTransformValueJsonAlloc(
    alloc: Allocator,
    value: std.json.Value,
    row: std.json.Value,
) ![]u8 {
    var resolved = try resolveTransformValueAlloc(alloc, value, row);
    defer foreign_mod.deinitJsonValue(alloc, &resolved);
    return try std.json.Stringify.valueAlloc(alloc, resolved, .{});
}

fn resolveTransformValueAlloc(
    alloc: Allocator,
    value: std.json.Value,
    row: std.json.Value,
) !std.json.Value {
    return switch (value) {
        .string => |text| try resolveTransformStringValueAlloc(alloc, text, row),
        else => try cloneJsonValueAllocLocal(alloc, value),
    };
}

fn resolveTransformStringValueAlloc(
    alloc: Allocator,
    text: []const u8,
    row: std.json.Value,
) !std.json.Value {
    const first_start = std.mem.indexOf(u8, text, "{{") orelse return .{ .string = try alloc.dupe(u8, text) };
    const first_end = std.mem.indexOfPos(u8, text, first_start + 2, "}}") orelse return .{ .string = try alloc.dupe(u8, text) };
    const field = std.mem.trim(u8, text[first_start + 2 .. first_end], &std.ascii.whitespace);

    if (first_start == 0 and first_end + 2 == text.len and std.mem.indexOfPos(u8, text, first_end + 2, "{{") == null) {
        return try cloneJsonValueAllocLocal(alloc, try lookupRowField(row, field));
    }

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var cursor: usize = 0;
    while (cursor < text.len) {
        const start_opt = std.mem.indexOfPos(u8, text, cursor, "{{");
        if (start_opt == null) {
            try out.appendSlice(alloc, text[cursor..]);
            break;
        }
        const start = start_opt.?;
        try out.appendSlice(alloc, text[cursor..start]);
        const end = std.mem.indexOfPos(u8, text, start + 2, "}}") orelse return error.InvalidReplicationSourceConfig;
        const ref = std.mem.trim(u8, text[start + 2 .. end], &std.ascii.whitespace);
        try appendJsonValueText(alloc, &out, try lookupRowField(row, ref));
        cursor = end + 2;
    }
    return .{ .string = try out.toOwnedSlice(alloc) };
}

fn cloneJsonValueAllocLocal(alloc: Allocator, value: std.json.Value) !std.json.Value {
    return switch (value) {
        .null => .null,
        .bool => |v| .{ .bool = v },
        .integer => |v| .{ .integer = v },
        .float => |v| .{ .float = v },
        .number_string => |v| .{ .number_string = try alloc.dupe(u8, v) },
        .string => |v| .{ .string = try alloc.dupe(u8, v) },
        .array => |arr| blk: {
            var out = std.json.Array.init(alloc);
            errdefer {
                for (out.items) |*item| foreign_mod.deinitJsonValue(alloc, item);
                out.deinit();
            }
            for (arr.items) |item| try out.append(try cloneJsonValueAllocLocal(alloc, item));
            break :blk .{ .array = out };
        },
        .object => |obj| blk: {
            var out = std.json.ObjectMap.empty;
            errdefer {
                var it = out.iterator();
                while (it.next()) |entry| {
                    alloc.free(@constCast(entry.key_ptr.*));
                    foreign_mod.deinitJsonValue(alloc, entry.value_ptr);
                }
                out.deinit(alloc);
            }
            var it = obj.iterator();
            while (it.next()) |entry| {
                try out.put(alloc, try alloc.dupe(u8, entry.key_ptr.*), try cloneJsonValueAllocLocal(alloc, entry.value_ptr.*));
            }
            break :blk .{ .object = out };
        },
    };
}

fn autoSetTransformOpsAlloc(alloc: Allocator, row: std.json.Value) ![]db_mod.types.TransformOp {
    if (row != .object) return error.InvalidReplicationSourceRow;
    const count = row.object.count();
    const ops = try alloc.alloc(db_mod.types.TransformOp, count);
    var initialized: usize = 0;
    errdefer freeTransformOpsOwned(alloc, ops[0..initialized]);

    var it = row.object.iterator();
    while (it.next()) |entry| {
        ops[initialized] = .{
            .op = .set,
            .path = try alloc.dupe(u8, entry.key_ptr.*),
            .value_json = try std.json.Stringify.valueAlloc(alloc, entry.value_ptr.*, .{}),
        };
        initialized += 1;
    }
    return ops[0..initialized];
}

fn autoUnsetTransformOpsAlloc(
    alloc: Allocator,
    row: std.json.Value,
    key_template: []const u8,
) ![]db_mod.types.TransformOp {
    if (row != .object) return error.InvalidReplicationSourceRow;
    var ops = std.ArrayListUnmanaged(db_mod.types.TransformOp).empty;
    errdefer freeTransformOpsOwned(alloc, ops.items);

    var it = row.object.iterator();
    while (it.next()) |entry| {
        if (keyTemplateContainsField(key_template, entry.key_ptr.*)) continue;
        try ops.append(alloc, .{
            .op = .unset,
            .path = try alloc.dupe(u8, entry.key_ptr.*),
            .value_json = null,
        });
    }
    return try ops.toOwnedSlice(alloc);
}

fn tableHasReplicationSourceStatus(
    records: []const metadata_table_manager.ReplicationSourceStatusRecord,
    table_id: u64,
) bool {
    for (records) |record| {
        if (record.table_id == table_id) return true;
    }
    return false;
}

fn keyTemplateContainsField(key_template: []const u8, field: []const u8) bool {
    if (key_template.len == 0) {
        return std.mem.eql(u8, field, "_id") or std.mem.eql(u8, field, "id");
    }
    if (std.mem.indexOf(u8, key_template, "{{") == null) {
        return std.mem.eql(u8, key_template, field);
    }

    var cursor: usize = 0;
    while (cursor < key_template.len) {
        const start = std.mem.indexOfPos(u8, key_template, cursor, "{{") orelse break;
        const end = std.mem.indexOfPos(u8, key_template, start + 2, "}}") orelse break;
        const template_field = std.mem.trim(u8, key_template[start + 2 .. end], &std.ascii.whitespace);
        if (std.mem.eql(u8, template_field, field)) return true;
        cursor = end + 2;
    }
    return false;
}

fn tableReadyForReplication(snapshot: *const metadata_api.AdminSnapshot, table_id: u64) bool {
    if (snapshot.ranges.len == 0 and snapshot.merged_group_statuses.len == 0) return true;
    var range_count: usize = 0;
    for (snapshot.ranges) |range| {
        if (range.table_id != table_id) continue;
        range_count += 1;
        if (!groupReadyForReplication(snapshot, range.group_id)) return false;
    }
    return range_count > 0;
}

fn groupReadyForReplication(snapshot: *const metadata_api.AdminSnapshot, group_id: u64) bool {
    const status = findMergedGroupStatus(snapshot, group_id) orelse return groupReadyForReplicationWithoutHealthStatus(snapshot, group_id);
    if (status.updated_at_millis == 0) return false;
    if (!status.leader_known) return false;
    if (status.joint_consensus) return false;
    if (status.transition_pending) return false;
    if (status.replay_required and !status.replay_caught_up) return false;
    if (status.restore_pending) return false;
    return groupHasExpectedHealthyPlacement(snapshot, group_id, status);
}

fn findMergedGroupStatus(
    snapshot: *const metadata_api.AdminSnapshot,
    group_id: u64,
) ?metadata_reconciler.MergedGroupStatus {
    for (snapshot.merged_group_statuses) |status| {
        if (status.group_id == group_id) return status;
    }
    return null;
}

fn groupReadyForReplicationWithoutHealthStatus(
    snapshot: *const metadata_api.AdminSnapshot,
    group_id: u64,
) bool {
    const expected = countPlacementIntentsForGroup(snapshot.placement_intents, group_id);
    if (expected == 0) return false;
    const readiness = metadata_transition_state.readinessForGroup(
        group_id,
        snapshot.split_transitions,
        snapshot.merge_transitions,
    );
    if (readiness.transition_pending) return false;
    if (readiness.replay_required and !readiness.replay_caught_up) return false;
    for (snapshot.restore_progresses) |progress| {
        if (progress.group_id == group_id) return false;
    }
    return true;
}

fn groupHasExpectedHealthyPlacement(
    snapshot: *const metadata_api.AdminSnapshot,
    group_id: u64,
    status: metadata_reconciler.MergedGroupStatus,
) bool {
    const expected = countPlacementIntentsForGroup(snapshot.placement_intents, group_id);
    if (status.voter_count_known) {
        if (expected > 0 and status.voter_count != expected) return false;
        return status.healthy_voter_reports >= status.voter_count;
    }
    if (expected == 0) return true;
    return countHealthyStoresReportingGroup(snapshot.stores, group_id) >= expected;
}

fn countPlacementIntentsForGroup(intents: anytype, group_id: u64) u16 {
    var count: u16 = 0;
    for (intents) |intent| {
        if (intent.record.group_id == group_id) count +|= 1;
    }
    return count;
}

fn countHealthyStoresReportingGroup(stores: []const metadata_table_manager.StoreRecord, group_id: u64) usize {
    var count: usize = 0;
    for (stores) |store| {
        if (!store.live) continue;
        if (!std.mem.eql(u8, store.health_class, "healthy")) continue;
        for (store.group_statuses) |group_status| {
            if (group_status.group_id != group_id) continue;
            count += 1;
            break;
        }
    }
    return count;
}

fn freeTransformOpsOwned(alloc: Allocator, ops: []const db_mod.types.TransformOp) void {
    for (ops) |op| {
        alloc.free(@constCast(op.path));
        if (op.value_json) |value_json| alloc.free(value_json);
    }
    if (ops.len > 0) alloc.free(ops);
}

fn renderReplicationChangeKeyAlloc(
    alloc: Allocator,
    change: foreign_mod.ReplicationChange,
    key_template: []const u8,
) ![]u8 {
    if (change.key) |key| return try alloc.dupe(u8, key);
    const row = change.row orelse return error.InvalidReplicationSourceRow;
    return try renderDocumentKeyAlloc(alloc, row, key_template);
}

fn renderDocumentKeyAlloc(alloc: Allocator, row: std.json.Value, key_template: []const u8) ![]u8 {
    if (row != .object) return error.InvalidReplicationSourceRow;

    if (key_template.len == 0) {
        return try renderFieldKeyAlloc(alloc, row, "_id", true);
    }

    if (std.mem.indexOf(u8, key_template, "{{") == null) {
        return try renderFieldKeyAlloc(alloc, row, key_template, false);
    }

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    var cursor: usize = 0;
    while (cursor < key_template.len) {
        const start_opt = std.mem.indexOfPos(u8, key_template, cursor, "{{");
        if (start_opt == null) {
            try out.appendSlice(alloc, key_template[cursor..]);
            break;
        }
        const start = start_opt.?;
        try out.appendSlice(alloc, key_template[cursor..start]);
        const end = std.mem.indexOfPos(u8, key_template, start + 2, "}}") orelse return error.InvalidReplicationSourceConfig;
        const field = std.mem.trim(u8, key_template[start + 2 .. end], &std.ascii.whitespace);
        try appendJsonValueText(alloc, &out, try lookupRowField(row, field));
        cursor = end + 2;
    }

    return try out.toOwnedSlice(alloc);
}

fn renderFieldKeyAlloc(alloc: Allocator, row: std.json.Value, field: []const u8, allow_id_fallback: bool) ![]u8 {
    if (lookupRowField(row, field)) |value| {
        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(alloc);
        try appendJsonValueText(alloc, &out, value);
        return try out.toOwnedSlice(alloc);
    } else |_| {
        if (allow_id_fallback and !std.mem.eql(u8, field, "id")) {
            return try renderFieldKeyAlloc(alloc, row, "id", false);
        }
        return error.InvalidReplicationSourceRow;
    }
}

fn lookupRowField(row: std.json.Value, field_path: []const u8) !std.json.Value {
    var current = row;
    var start: usize = 0;
    while (true) {
        const end = std.mem.indexOfPos(u8, field_path, start, ".") orelse field_path.len;
        if (current != .object) return error.InvalidReplicationSourceRow;
        current = current.object.get(field_path[start..end]) orelse return error.InvalidReplicationSourceRow;
        if (end == field_path.len) return current;
        start = end + 1;
    }
}

fn appendJsonValueText(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: std.json.Value) !void {
    switch (value) {
        .string => |text| try out.appendSlice(alloc, text),
        .integer => |number| {
            const text = try std.fmt.allocPrint(alloc, "{d}", .{number});
            defer alloc.free(text);
            try out.appendSlice(alloc, text);
        },
        .float => |number| {
            const text = try std.fmt.allocPrint(alloc, "{d}", .{number});
            defer alloc.free(text);
            try out.appendSlice(alloc, text);
        },
        .bool => |flag| try out.appendSlice(alloc, if (flag) "true" else "false"),
        .number_string => |text| try out.appendSlice(alloc, text),
        else => return error.InvalidReplicationSourceRow,
    }
}

fn deriveSlotNameAlloc(alloc: Allocator, table_name: []const u8, postgres_table: []const u8) ![]u8 {
    const sanitized_table = try sanitizePostgresIdentifierAlloc(alloc, try alloc.dupe(u8, postgres_table), 63);
    defer alloc.free(sanitized_table);
    const raw = if (std.mem.indexOfScalar(u8, sanitized_table, '_') != null)
        try std.fmt.allocPrint(alloc, "antfly_postgres_{s}", .{sanitized_table})
    else
        try std.fmt.allocPrint(alloc, "antfly_postgres_{s}_{s}", .{ postgres_table, table_name });
    return try sanitizePostgresIdentifierAlloc(alloc, raw, 63);
}

fn derivePublicationNameAlloc(alloc: Allocator, table_name: []const u8, postgres_table: []const u8) ![]u8 {
    const sanitized_table = try sanitizePostgresIdentifierAlloc(alloc, try alloc.dupe(u8, postgres_table), 63);
    defer alloc.free(sanitized_table);
    const raw = if (std.mem.indexOfScalar(u8, sanitized_table, '_') != null)
        try std.fmt.allocPrint(alloc, "antfly_pub_postgres_{s}", .{sanitized_table})
    else
        try std.fmt.allocPrint(alloc, "antfly_pub_postgres_{s}_{s}", .{ postgres_table, table_name });
    return try sanitizePostgresIdentifierAlloc(alloc, raw, 63);
}

fn sanitizePostgresIdentifierAlloc(alloc: Allocator, raw_owned: []u8, max_len: usize) ![]u8 {
    defer alloc.free(raw_owned);
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    for (raw_owned) |byte| {
        if (out.items.len >= max_len) break;
        if (std.ascii.isAlphanumeric(byte) or byte == '_') {
            try out.append(alloc, std.ascii.toLower(byte));
        } else {
            try out.append(alloc, '_');
        }
    }
    return try out.toOwnedSlice(alloc);
}

fn freeBatchWritesOwned(alloc: Allocator, writes: []const db_mod.types.BatchWrite) void {
    for (writes) |write| {
        alloc.free(@constCast(write.key));
        alloc.free(@constCast(write.value));
    }
    if (writes.len > 0) alloc.free(writes);
}

fn callUpsertStatus(status_sink: anytype, record: metadata_table_manager.ReplicationSourceStatusRecord) !void {
    try status_sink.upsertReplicationSourceStatus(record);
}

fn findReplicationSourceStatus(
    records: []const metadata_table_manager.ReplicationSourceStatusRecord,
    table_id: u64,
    source_ordinal: u32,
) ?metadata_table_manager.ReplicationSourceStatusRecord {
    for (records) |record| {
        if (record.table_id == table_id and record.source_ordinal == source_ordinal) return record;
    }
    return null;
}

fn tableHasStreamingEligibleStatus(
    records: []const metadata_table_manager.ReplicationSourceStatusRecord,
    table_id: u64,
) bool {
    for (records) |record| {
        if (record.table_id != table_id) continue;
        if (phaseAllowsStreaming(record.phase)) return true;
    }
    return false;
}

fn checkpointSlice(offset: usize) []const u8 {
    return std.fmt.bufPrint(&checkpoint_buf, "snapshot_offset:{d}", .{offset}) catch "snapshot_offset:0";
}

fn phaseAllowsStreaming(phase: []const u8) bool {
    return std.mem.eql(u8, phase, "snapshot_complete") or
        std.mem.eql(u8, phase, "cutover_prepared") or
        std.mem.eql(u8, phase, "streaming") or
        std.mem.eql(u8, phase, "streaming_failed");
}

fn checkpointForStreaming(record: metadata_table_manager.ReplicationSourceStatusRecord) ?[]const u8 {
    if (record.stream_checkpoint.len > 0) return record.stream_checkpoint;
    if (record.prepared_checkpoint.len > 0) return record.prepared_checkpoint;
    if (std.mem.eql(u8, record.phase, "snapshot_complete") or std.mem.eql(u8, record.phase, "cutover_prepared")) return null;
    if (record.checkpoint.len == 0) return null;
    return record.checkpoint;
}

fn parseSnapshotOffset(checkpoint: []const u8) usize {
    const prefix = "snapshot_offset:";
    if (!std.mem.startsWith(u8, checkpoint, prefix)) return 0;
    return std.fmt.parseInt(usize, checkpoint[prefix.len..], 10) catch 0;
}

fn snapshotOffsetForStatus(record: metadata_table_manager.ReplicationSourceStatusRecord) usize {
    if (record.snapshot_offset > 0) return @intCast(record.snapshot_offset);
    return parseSnapshotOffset(record.checkpoint);
}

fn deriveSnapshotOrderFieldAlloc(alloc: Allocator, key_template: []const u8) !?[]u8 {
    if (key_template.len == 0) return null;

    if (std.mem.indexOf(u8, key_template, "{{") == null) {
        if (!isSimpleSnapshotOrderField(key_template)) return null;
        return try alloc.dupe(u8, key_template);
    }

    if (!std.mem.startsWith(u8, key_template, "{{") or !std.mem.endsWith(u8, key_template, "}}")) return null;
    const inner = std.mem.trim(u8, key_template[2 .. key_template.len - 2], &std.ascii.whitespace);
    if (!isSimpleSnapshotOrderField(inner)) return null;
    return try alloc.dupe(u8, inner);
}

fn isSimpleSnapshotOrderField(field: []const u8) bool {
    if (field.len == 0) return false;
    if (!(std.ascii.isAlphabetic(field[0]) or field[0] == '_')) return false;
    for (field[1..]) |ch| {
        if (!(std.ascii.isAlphanumeric(ch) or ch == '_')) return false;
    }
    return true;
}

fn countReplicationSourcesJson(alloc: Allocator, replication_sources_json: []const u8) !usize {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, replication_sources_json, .{});
    defer parsed.deinit();
    if (parsed.value != .array) return error.InvalidReplicationSourceConfig;
    return parsed.value.array.items.len;
}

threadlocal var checkpoint_buf: [64]u8 = undefined;

fn nowMillis() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.REALTIME, &ts))) {
        .SUCCESS => {},
        else => return 0,
    }
    const sec: u64 = @intCast(@max(ts.sec, 0));
    const nsec: u64 = @intCast(@max(ts.nsec, 0));
    return sec * std.time.ms_per_s + @divTrunc(nsec, std.time.ns_per_ms);
}

test "metadata replication derives stable snapshot order field from simple key template" {
    const alloc = std.testing.allocator;

    const plain = try deriveSnapshotOrderFieldAlloc(alloc, "id");
    defer if (plain) |field| alloc.free(field);
    try std.testing.expectEqualStrings("id", plain.?);

    const wrapped = try deriveSnapshotOrderFieldAlloc(alloc, "{{ user_id }}");
    defer if (wrapped) |field| alloc.free(field);
    try std.testing.expectEqualStrings("user_id", wrapped.?);

    const composite = try deriveSnapshotOrderFieldAlloc(alloc, "{{tenant_id}}:{{user_id}}");
    defer if (composite) |field| alloc.free(field);
    try std.testing.expect(composite == null);

    const nested = try deriveSnapshotOrderFieldAlloc(alloc, "{{profile.id}}");
    defer if (nested) |field| alloc.free(field);
    try std.testing.expect(nested == null);
}

test "metadata replication backfill applies postgres snapshot rows through bound write source" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-metadata-replication-backfill";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    const FakeExecutor = struct {
        const Parent = @This();

        var prepare_calls: usize = 0;
        var last_prepare_slot_name: ?[]u8 = null;
        var last_prepare_publication_name: ?[]u8 = null;
        var saw_order_by_id: bool = false;
        var snapshot_begin_calls: usize = 0;
        var snapshot_query_calls: usize = 0;

        const SnapshotSession = struct {
            fn destroy(ptr: *anyopaque, inner_alloc: Allocator) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                inner_alloc.destroy(self);
            }

            fn query(_: *anyopaque, inner_alloc: Allocator, prepared: foreign_mod.PreparedQuery) !foreign_mod.QueryResult {
                var owned = prepared;
                defer owned.deinit(inner_alloc);
                Parent.snapshot_query_calls += 1;
                Parent.saw_order_by_id = std.mem.indexOf(u8, owned.sql_text, "ORDER BY \"id\" ASC") != null;

                const limit = parseSqlSuffixUsize(owned.sql_text, " LIMIT ") orelse 0;
                const offset = parseSqlSuffixUsize(owned.sql_text, " OFFSET ") orelse 0;

                const all_rows = [_][]const u8{
                    "{\"id\":\"doc:1\",\"name\":\"alpha\"}",
                    "{\"id\":\"doc:2\",\"name\":\"beta\"}",
                };
                if (offset >= all_rows.len) return .{ .rows = &.{}, .total = all_rows.len };

                const count = @min(limit, all_rows.len - offset);
                const rows = try inner_alloc.alloc(std.json.Value, count);
                for (0..count) |i| {
                    rows[i] = try std.json.parseFromSliceLeaky(std.json.Value, inner_alloc, all_rows[offset + i], .{});
                }
                return .{ .rows = rows, .total = all_rows.len };
            }
        };

        fn query(_: *anyopaque, inner_alloc: Allocator, _: []const u8, prepared: foreign_mod.PreparedQuery) !foreign_mod.QueryResult {
            var owned = prepared;
            defer owned.deinit(inner_alloc);
            saw_order_by_id = std.mem.indexOf(u8, owned.sql_text, "ORDER BY \"id\" ASC") != null;

            const limit = parseSqlSuffixUsize(owned.sql_text, " LIMIT ") orelse 0;
            const offset = parseSqlSuffixUsize(owned.sql_text, " OFFSET ") orelse 0;

            const all_rows = [_][]const u8{
                "{\"id\":\"doc:1\",\"name\":\"alpha\"}",
                "{\"id\":\"doc:2\",\"name\":\"beta\"}",
            };
            if (offset >= all_rows.len) return .{ .rows = &.{}, .total = all_rows.len };

            const count = @min(limit, all_rows.len - offset);
            const rows = try inner_alloc.alloc(std.json.Value, count);
            for (0..count) |i| {
                rows[i] = try std.json.parseFromSliceLeaky(std.json.Value, inner_alloc, all_rows[offset + i], .{});
            }
            return .{ .rows = rows, .total = all_rows.len };
        }

        fn statistics(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !foreign_mod.TableStatistics {
            return .{ .row_count = 2, .size_bytes = 64 };
        }

        fn beginSnapshotQuery(_: *anyopaque, inner_alloc: Allocator, _: []const u8) !foreign_mod.PostgresQueryExecutor.SnapshotQuery {
            Parent.snapshot_begin_calls += 1;
            const session = try inner_alloc.create(SnapshotSession);
            session.* = .{};
            return .{
                .ptr = session,
                .vtable = &.{
                    .deinit = SnapshotSession.destroy,
                    .query = SnapshotSession.query,
                },
            };
        }

        fn prepareReplication(_: *anyopaque, inner_alloc: Allocator, _: []const u8, params: foreign_mod.ReplicationPollParams) !foreign_mod.ReplicationPrepareResult {
            prepare_calls += 1;
            if (last_prepare_slot_name) |slot_name| inner_alloc.free(slot_name);
            if (last_prepare_publication_name) |publication_name| inner_alloc.free(publication_name);
            last_prepare_slot_name = if (params.slot_name) |slot_name| try inner_alloc.dupe(u8, slot_name) else null;
            last_prepare_publication_name = if (params.publication_name) |publication_name| try inner_alloc.dupe(u8, publication_name) else null;
            return .{ .checkpoint = try inner_alloc.dupe(u8, "lsn:prepared") };
        }

        fn discoverColumns(_: *anyopaque, inner_alloc: Allocator, _: []const u8, _: []const u8) ![]foreign_mod.Column {
            const columns = try inner_alloc.alloc(foreign_mod.Column, 2);
            columns[0] = .{
                .name = try inner_alloc.dupe(u8, "id"),
                .data_type = try inner_alloc.dupe(u8, "text"),
                .nullable = false,
            };
            columns[1] = .{
                .name = try inner_alloc.dupe(u8, "name"),
                .data_type = try inner_alloc.dupe(u8, "text"),
                .nullable = false,
            };
            return columns;
        }
    };
    FakeExecutor.prepare_calls = 0;
    FakeExecutor.last_prepare_slot_name = null;
    FakeExecutor.last_prepare_publication_name = null;
    FakeExecutor.saw_order_by_id = false;
    FakeExecutor.snapshot_begin_calls = 0;
    FakeExecutor.snapshot_query_calls = 0;
    defer {
        if (FakeExecutor.last_prepare_slot_name) |slot_name| alloc.free(slot_name);
        if (FakeExecutor.last_prepare_publication_name) |publication_name| alloc.free(publication_name);
    }

    var registry = foreign_mod.Registry{};
    defer registry.deinit(alloc);
    try foreign_mod.registerPostgresExecutor(alloc, &registry, .{
        .ptr = undefined,
        .vtable = &.{
            .query = FakeExecutor.query,
            .statistics = FakeExecutor.statistics,
            .discover_columns = FakeExecutor.discoverColumns,
            .begin_snapshot_query = FakeExecutor.beginSnapshotQuery,
            .prepare_replication = FakeExecutor.prepareReplication,
        },
    });

    var write_source = table_writes_api.BoundTableWriteSource.init("docs", &db);

    const StatusSink = struct {
        alloc: Allocator,
        records: std.ArrayListUnmanaged(metadata_table_manager.ReplicationSourceStatusRecord) = .empty,

        fn deinit(self: *@This()) void {
            for (self.records.items) |record| metadata_table_manager.freeReplicationSourceStatus(self.alloc, record);
            self.records.deinit(self.alloc);
        }

        fn upsertReplicationSourceStatus(self: *@This(), record: metadata_table_manager.ReplicationSourceStatusRecord) !void {
            try self.records.append(self.alloc, try metadata_table_manager.cloneReplicationSourceStatus(self.alloc, record));
        }
    };

    var status_sink: StatusSink = .{ .alloc = alloc };
    defer status_sink.deinit();

    var runner = SnapshotBackfillRunner{
        .alloc = alloc,
        .registry = &registry,
        .write_source = write_source.source(),
        .batch_size = 1,
    };

    const summary = try runner.runTableSource(&status_sink, .{
        .table_id = 11,
        .name = "docs",
        .replication_sources_json = "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\",\"key_template\":\"id\"}]",
    }, 0);

    try std.testing.expectEqual(@as(usize, 2), summary.rows_applied);
    try std.testing.expectEqual(@as(usize, 2), summary.batches_applied);
    try std.testing.expectEqual(@as(usize, 2), summary.final_offset);
    try std.testing.expect(status_sink.records.items.len >= 3);
    try std.testing.expectEqualStrings("cutover_prepared", status_sink.records.items[status_sink.records.items.len - 1].phase);
    try std.testing.expectEqualStrings("snapshot_offset:2", status_sink.records.items[status_sink.records.items.len - 1].checkpoint);
    try std.testing.expectEqual(@as(u64, 2), status_sink.records.items[status_sink.records.items.len - 1].snapshot_offset);
    try std.testing.expectEqualStrings("lsn:prepared", status_sink.records.items[status_sink.records.items.len - 1].prepared_checkpoint);
    try std.testing.expectEqualStrings("slot_first", status_sink.records.items[status_sink.records.items.len - 1].cutover_mode);
    try std.testing.expectEqualStrings("antfly_postgres_users_docs", status_sink.records.items[status_sink.records.items.len - 1].slot_name);
    try std.testing.expectEqualStrings("antfly_pub_postgres_users_docs", status_sink.records.items[status_sink.records.items.len - 1].publication_name);
    try std.testing.expect(status_sink.records.items[status_sink.records.items.len - 1].updated_at_ms > 0);
    try std.testing.expectEqual(@as(usize, 1), FakeExecutor.prepare_calls);
    try std.testing.expectEqual(@as(usize, 1), FakeExecutor.snapshot_begin_calls);
    try std.testing.expectEqual(@as(usize, 3), FakeExecutor.snapshot_query_calls);
    try std.testing.expect(FakeExecutor.saw_order_by_id);
    try std.testing.expectEqualStrings("antfly_postgres_users_docs", FakeExecutor.last_prepare_slot_name.?);
    try std.testing.expectEqualStrings("antfly_pub_postgres_users_docs", FakeExecutor.last_prepare_publication_name.?);

    var result_one = (try db.lookup(alloc, "doc:1", .{})).?;
    defer result_one.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, result_one.json, "\"alpha\"") != null);

    var result_two = (try db.lookup(alloc, "doc:2", .{})).?;
    defer result_two.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, result_two.json, "\"beta\"") != null);
}

test "metadata replication backfill prefers prepared exact cutover snapshot when supported" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-metadata-replication-prepared-snapshot";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    const FakeExecutor = struct {
        const Parent = @This();

        var prepare_calls: usize = 0;
        var exact_cutover_calls: usize = 0;
        var snapshot_query_calls: usize = 0;

        const SnapshotSession = struct {
            fn destroy(ptr: *anyopaque, inner_alloc: Allocator) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                inner_alloc.destroy(self);
            }

            fn query(_: *anyopaque, inner_alloc: Allocator, prepared: foreign_mod.PreparedQuery) !foreign_mod.QueryResult {
                var owned = prepared;
                defer owned.deinit(inner_alloc);
                Parent.snapshot_query_calls += 1;

                const limit = parseSqlSuffixUsize(owned.sql_text, " LIMIT ") orelse 0;
                const offset = parseSqlSuffixUsize(owned.sql_text, " OFFSET ") orelse 0;
                const all_rows = [_][]const u8{
                    "{\"id\":\"doc:1\",\"name\":\"alpha\"}",
                    "{\"id\":\"doc:2\",\"name\":\"beta\"}",
                };
                if (offset >= all_rows.len) return .{ .rows = &.{}, .total = all_rows.len };
                const count = @min(limit, all_rows.len - offset);
                const rows = try inner_alloc.alloc(std.json.Value, count);
                for (0..count) |i| {
                    rows[i] = try std.json.parseFromSliceLeaky(std.json.Value, inner_alloc, all_rows[offset + i], .{});
                }
                return .{ .rows = rows, .total = all_rows.len };
            }
        };

        fn query(_: *anyopaque, inner_alloc: Allocator, _: []const u8, prepared: foreign_mod.PreparedQuery) !foreign_mod.QueryResult {
            var owned = prepared;
            defer owned.deinit(inner_alloc);
            return .{ .rows = &.{}, .total = 0 };
        }

        fn statistics(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !foreign_mod.TableStatistics {
            return .{ .row_count = 2, .size_bytes = 64 };
        }

        fn beginPreparedReplicationSnapshot(
            _: *anyopaque,
            inner_alloc: Allocator,
            _: []const u8,
            _: foreign_mod.ReplicationPollParams,
        ) !foreign_mod.PostgresQueryExecutor.PreparedReplicationSnapshot {
            Parent.exact_cutover_calls += 1;
            const session = try inner_alloc.create(SnapshotSession);
            session.* = .{};
            return .{
                .checkpoint = try inner_alloc.dupe(u8, "lsn:exact"),
                .snapshot_query = .{
                    .ptr = session,
                    .vtable = &.{
                        .deinit = SnapshotSession.destroy,
                        .query = SnapshotSession.query,
                    },
                },
            };
        }

        fn prepareReplication(_: *anyopaque, inner_alloc: Allocator, _: []const u8, _: foreign_mod.ReplicationPollParams) !foreign_mod.ReplicationPrepareResult {
            Parent.prepare_calls += 1;
            return .{ .checkpoint = try inner_alloc.dupe(u8, "lsn:fallback") };
        }

        fn discoverColumns(_: *anyopaque, inner_alloc: Allocator, _: []const u8, _: []const u8) ![]foreign_mod.Column {
            const columns = try inner_alloc.alloc(foreign_mod.Column, 1);
            columns[0] = .{
                .name = try inner_alloc.dupe(u8, "id"),
                .data_type = try inner_alloc.dupe(u8, "text"),
                .nullable = false,
            };
            return columns;
        }
    };
    FakeExecutor.prepare_calls = 0;
    FakeExecutor.exact_cutover_calls = 0;
    FakeExecutor.snapshot_query_calls = 0;

    var registry = foreign_mod.Registry{};
    defer registry.deinit(alloc);
    try foreign_mod.registerPostgresExecutor(alloc, &registry, .{
        .ptr = undefined,
        .vtable = &.{
            .query = FakeExecutor.query,
            .statistics = FakeExecutor.statistics,
            .discover_columns = FakeExecutor.discoverColumns,
            .begin_prepared_replication_snapshot = FakeExecutor.beginPreparedReplicationSnapshot,
            .prepare_replication = FakeExecutor.prepareReplication,
        },
    });

    var write_source = table_writes_api.BoundTableWriteSource.init("docs", &db);

    const StatusSink = struct {
        alloc: Allocator,
        records: std.ArrayListUnmanaged(metadata_table_manager.ReplicationSourceStatusRecord) = .empty,

        fn deinit(self: *@This()) void {
            for (self.records.items) |record| metadata_table_manager.freeReplicationSourceStatus(self.alloc, record);
            self.records.deinit(self.alloc);
        }

        fn upsertReplicationSourceStatus(self: *@This(), record: metadata_table_manager.ReplicationSourceStatusRecord) !void {
            try self.records.append(self.alloc, try metadata_table_manager.cloneReplicationSourceStatus(self.alloc, record));
        }
    };

    var status_sink: StatusSink = .{ .alloc = alloc };
    defer status_sink.deinit();

    var runner = SnapshotBackfillRunner{
        .alloc = alloc,
        .registry = &registry,
        .write_source = write_source.source(),
        .batch_size = 1,
    };

    _ = try runner.runTableSource(&status_sink, .{
        .table_id = 11,
        .name = "docs",
        .replication_sources_json = "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\",\"key_template\":\"id\"}]",
    }, 0);

    try std.testing.expectEqual(@as(usize, 1), FakeExecutor.exact_cutover_calls);
    try std.testing.expectEqual(@as(usize, 0), FakeExecutor.prepare_calls);
    try std.testing.expectEqual(@as(usize, 3), FakeExecutor.snapshot_query_calls);
    try std.testing.expectEqualStrings("lsn:exact", status_sink.records.items[status_sink.records.items.len - 1].prepared_checkpoint);
    try std.testing.expectEqualStrings("exported_snapshot", status_sink.records.items[status_sink.records.items.len - 1].cutover_mode);
}

test "metadata replication backfill applies configured update transforms" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-metadata-replication-backfill-transforms";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    const FakeExecutor = struct {
        fn query(_: *anyopaque, inner_alloc: Allocator, _: []const u8, prepared: foreign_mod.PreparedQuery) !foreign_mod.QueryResult {
            var owned = prepared;
            defer owned.deinit(inner_alloc);
            const rows = try inner_alloc.alloc(std.json.Value, 1);
            rows[0] = try std.json.parseFromSliceLeaky(std.json.Value, inner_alloc, "{\"id\":\"doc:7\",\"user_name\":\"Alice\",\"profile\":{\"city\":\"sf\"}}", .{});
            return .{ .rows = rows, .total = 1 };
        }

        fn statistics(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !foreign_mod.TableStatistics {
            return .{ .row_count = 1, .size_bytes = 64 };
        }

        fn prepareReplication(_: *anyopaque, inner_alloc: Allocator, _: []const u8, _: foreign_mod.ReplicationPollParams) !foreign_mod.ReplicationPrepareResult {
            return .{ .checkpoint = try inner_alloc.dupe(u8, "lsn:prepared") };
        }

        fn discoverColumns(_: *anyopaque, inner_alloc: Allocator, _: []const u8, _: []const u8) ![]foreign_mod.Column {
            const columns = try inner_alloc.alloc(foreign_mod.Column, 1);
            columns[0] = .{
                .name = try inner_alloc.dupe(u8, "id"),
                .data_type = try inner_alloc.dupe(u8, "text"),
                .nullable = false,
            };
            return columns;
        }
    };

    var registry = foreign_mod.Registry{};
    defer registry.deinit(alloc);
    try foreign_mod.registerPostgresExecutor(alloc, &registry, .{
        .ptr = undefined,
        .vtable = &.{
            .query = FakeExecutor.query,
            .statistics = FakeExecutor.statistics,
            .prepare_replication = FakeExecutor.prepareReplication,
            .discover_columns = FakeExecutor.discoverColumns,
        },
    });

    var write_source = table_writes_api.BoundTableWriteSource.init("docs", &db);
    const StatusSink = struct {
        fn upsertReplicationSourceStatus(_: *@This(), _: metadata_table_manager.ReplicationSourceStatusRecord) !void {}
    };
    var status_sink = StatusSink{};

    var runner = SnapshotBackfillRunner{
        .alloc = alloc,
        .registry = &registry,
        .write_source = write_source.source(),
        .batch_size = 8,
    };

    const summary = try runner.runTableSource(&status_sink, .{
        .table_id = 11,
        .name = "docs",
        .replication_sources_json = "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\",\"key_template\":\"id\",\"on_update\":[{\"op\":\"$set\",\"path\":\"name\",\"value\":\"{{user_name}}\"},{\"op\":\"$merge\",\"value\":\"{{profile}}\"}]}]",
    }, 0);

    try std.testing.expectEqual(@as(usize, 1), summary.rows_applied);
    var result = (try db.lookup(alloc, "doc:7", .{})).?;
    defer result.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, result.json, "\"name\":\"Alice\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.json, "\"city\":\"sf\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.json, "\"user_name\"") == null);
}

test "metadata replication backfill routes matching snapshot rows to target tables" {
    const alloc = std.testing.allocator;

    const FakeExecutor = struct {
        fn query(_: *anyopaque, inner_alloc: Allocator, _: []const u8, prepared: foreign_mod.PreparedQuery) !foreign_mod.QueryResult {
            var owned = prepared;
            defer owned.deinit(inner_alloc);
            const rows = try inner_alloc.alloc(std.json.Value, 2);
            rows[0] = try std.json.parseFromSliceLeaky(std.json.Value, inner_alloc, "{\"id\":\"r-1\",\"name\":\"Alice\",\"tier\":\"premium\"}", .{});
            rows[1] = try std.json.parseFromSliceLeaky(std.json.Value, inner_alloc, "{\"id\":\"r-2\",\"name\":\"Bob\",\"tier\":\"free\"}", .{});
            return .{ .rows = rows, .total = 2 };
        }

        fn statistics(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !foreign_mod.TableStatistics {
            return .{ .row_count = 2, .size_bytes = 64 };
        }

        fn prepareReplication(_: *anyopaque, inner_alloc: Allocator, _: []const u8, _: foreign_mod.ReplicationPollParams) !foreign_mod.ReplicationPrepareResult {
            return .{ .checkpoint = try inner_alloc.dupe(u8, "lsn:prepared") };
        }

        fn discoverColumns(_: *anyopaque, inner_alloc: Allocator, _: []const u8, _: []const u8) ![]foreign_mod.Column {
            const columns = try inner_alloc.alloc(foreign_mod.Column, 1);
            columns[0] = .{
                .name = try inner_alloc.dupe(u8, "id"),
                .data_type = try inner_alloc.dupe(u8, "text"),
                .nullable = false,
            };
            return columns;
        }
    };

    const CaptureWriteSource = struct {
        const CapturedCall = struct {
            table_name: []u8,
            key: []u8,
            op_paths: [][]u8 = &.{},
        };

        alloc: Allocator,
        calls: std.ArrayListUnmanaged(CapturedCall) = .empty,
        begun_tables: std.ArrayListUnmanaged([]u8) = .empty,
        finished_tables: std.ArrayListUnmanaged([]u8) = .empty,
        aborted_tables: std.ArrayListUnmanaged([]u8) = .empty,

        fn deinit(self: *@This()) void {
            for (self.calls.items) |call| {
                self.alloc.free(call.table_name);
                self.alloc.free(call.key);
                for (call.op_paths) |path| self.alloc.free(path);
                if (call.op_paths.len > 0) self.alloc.free(call.op_paths);
            }
            self.calls.deinit(self.alloc);
            for (self.begun_tables.items) |table_name| self.alloc.free(table_name);
            self.begun_tables.deinit(self.alloc);
            for (self.finished_tables.items) |table_name| self.alloc.free(table_name);
            self.finished_tables.deinit(self.alloc);
            for (self.aborted_tables.items) |table_name| self.alloc.free(table_name);
            self.aborted_tables.deinit(self.alloc);
        }

        fn source(self: *@This()) table_writes_api.TableWriteSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .batch = batch,
                    .begin_bulk_ingest = beginBulkIngest,
                    .finish_bulk_ingest = finishBulkIngest,
                    .abort_bulk_ingest = abortBulkIngest,
                },
            };
        }

        fn batch(ptr: *anyopaque, _: Allocator, table_name: []const u8, req: db_mod.types.BatchRequest) !?void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            for (req.transforms) |doc_transform| {
                const op_paths = try self.alloc.alloc([]u8, doc_transform.operations.len);
                var initialized: usize = 0;
                errdefer {
                    for (op_paths[0..initialized]) |path| self.alloc.free(path);
                    self.alloc.free(op_paths);
                }
                for (doc_transform.operations) |op| {
                    op_paths[initialized] = try self.alloc.dupe(u8, op.path);
                    initialized += 1;
                }
                try self.calls.append(self.alloc, .{
                    .table_name = try self.alloc.dupe(u8, table_name),
                    .key = try self.alloc.dupe(u8, doc_transform.key),
                    .op_paths = op_paths,
                });
            }
            return {};
        }

        fn beginBulkIngest(ptr: *anyopaque, _: Allocator, table_name: []const u8) !?void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.begun_tables.append(self.alloc, try self.alloc.dupe(u8, table_name));
            return {};
        }

        fn finishBulkIngest(
            ptr: *anyopaque,
            _: Allocator,
            table_name: []const u8,
            options: backend_types.BulkIngestFinishOptions,
        ) !?void {
            try std.testing.expect(!options.compact);
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.finished_tables.append(self.alloc, try self.alloc.dupe(u8, table_name));
            return {};
        }

        fn abortBulkIngest(ptr: *anyopaque, table_name: []const u8) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.aborted_tables.append(self.alloc, self.alloc.dupe(u8, table_name) catch return) catch return;
        }
    };

    var registry = foreign_mod.Registry{};
    defer registry.deinit(alloc);
    try foreign_mod.registerPostgresExecutor(alloc, &registry, .{
        .ptr = undefined,
        .vtable = &.{
            .query = FakeExecutor.query,
            .statistics = FakeExecutor.statistics,
            .prepare_replication = FakeExecutor.prepareReplication,
            .discover_columns = FakeExecutor.discoverColumns,
        },
    });

    var capture = CaptureWriteSource{ .alloc = alloc };
    defer capture.deinit();

    const StatusSink = struct {
        fn upsertReplicationSourceStatus(_: *@This(), _: metadata_table_manager.ReplicationSourceStatusRecord) !void {}
    };
    var status_sink = StatusSink{};

    var runner = SnapshotBackfillRunner{
        .alloc = alloc,
        .registry = &registry,
        .write_source = capture.source(),
        .batch_size = 8,
    };

    const summary = try runner.runTableSource(&status_sink, .{
        .table_id = 11,
        .name = "cdc_routes_router",
        .replication_sources_json =
        \\[{
        \\  "type":"postgres",
        \\  "dsn":"postgres://db",
        \\  "postgres_table":"users",
        \\  "key_template":"id",
        \\  "routes":[
        \\    {
        \\      "target_table":"premium_users",
        \\      "where":{"term":"premium","field":"tier"},
        \\      "on_update":[{"op":"$set","path":"name","value":"{{name}}"}]
        \\    },
        \\    {
        \\      "target_table":"free_users",
        \\      "where":{"term":"free","field":"tier"}
        \\    }
        \\  ]
        \\}]
        ,
    }, 0);

    try std.testing.expectEqual(@as(usize, 2), summary.rows_applied);
    try std.testing.expectEqual(@as(usize, 2), capture.calls.items.len);
    try std.testing.expectEqualStrings("premium_users", capture.calls.items[0].table_name);
    try std.testing.expectEqualStrings("r-1", capture.calls.items[0].key);
    try std.testing.expectEqual(@as(usize, 1), capture.calls.items[0].op_paths.len);
    try std.testing.expectEqualStrings("name", capture.calls.items[0].op_paths[0]);
    try std.testing.expectEqualStrings("free_users", capture.calls.items[1].table_name);
    try std.testing.expectEqualStrings("r-2", capture.calls.items[1].key);
    try std.testing.expectEqual(@as(usize, 2), capture.begun_tables.items.len);
    try std.testing.expectEqualStrings("premium_users", capture.begun_tables.items[0]);
    try std.testing.expectEqualStrings("free_users", capture.begun_tables.items[1]);
    try std.testing.expectEqual(@as(usize, 2), capture.finished_tables.items.len);
    try std.testing.expectEqualStrings("premium_users", capture.finished_tables.items[0]);
    try std.testing.expectEqualStrings("free_users", capture.finished_tables.items[1]);
    try std.testing.expectEqual(@as(usize, 0), capture.aborted_tables.items.len);
}

test "metadata replication source parser derives slot and publication names like Go" {
    const alloc = std.testing.allocator;

    var parsed = try parseReplicationSourceConfig(
        alloc,
        "docs",
        "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"Users-Prod\",\"key_template\":\"id\"}]",
        0,
    );
    defer parsed.deinit(alloc);

    try std.testing.expectEqualStrings("antfly_postgres_users_prod", parsed.slot_name);
    try std.testing.expectEqualStrings("antfly_pub_postgres_users_prod", parsed.publication_name);
    try std.testing.expect(!parsed.has_delete_transforms);
    try std.testing.expect(!parsed.delete_document_on_delete);
}

test "metadata replication source parser preserves explicit slot and publication names" {
    const alloc = std.testing.allocator;

    var parsed = try parseReplicationSourceConfig(
        alloc,
        "docs",
        "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\",\"slot_name\":\"custom_slot\",\"publication_name\":\"custom_pub\",\"on_delete\":[{\"op\":\"$unset\",\"path\":\"name\"}]}]",
        0,
    );
    defer parsed.deinit(alloc);

    try std.testing.expectEqualStrings("custom_slot", parsed.slot_name);
    try std.testing.expectEqualStrings("custom_pub", parsed.publication_name);
    try std.testing.expect(parsed.has_delete_transforms);
    try std.testing.expect(!parsed.delete_document_on_delete);
}

test "metadata replication source parser accepts null optional fields" {
    const alloc = std.testing.allocator;
    var parsed = try parseReplicationSourceConfig(
        alloc,
        "docs",
        "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\",\"key_template\":null,\"slot_name\":null,\"publication_name\":null,\"on_update\":null,\"on_delete\":null,\"publication_filter\":null,\"routes\":null}]",
        0,
    );
    defer parsed.deinit(alloc);

    try std.testing.expectEqualStrings("", parsed.key_template);
    try std.testing.expectEqualStrings("antfly_postgres_users_docs", parsed.slot_name);
    try std.testing.expectEqualStrings("antfly_pub_postgres_users_docs", parsed.publication_name);
    try std.testing.expect(parsed.on_update_json == null);
    try std.testing.expect(parsed.on_delete_json == null);
    try std.testing.expect(parsed.publication_filter_json == null);
    try std.testing.expectEqual(@as(usize, 0), parsed.routes.len);
    try std.testing.expect(!parsed.has_update_transforms);
    try std.testing.expect(!parsed.has_delete_transforms);
    try std.testing.expect(!parsed.delete_document_on_delete);
}

test "metadata replication source parser detects delete document op" {
    const alloc = std.testing.allocator;

    var parsed = try parseReplicationSourceConfig(
        alloc,
        "docs",
        "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\",\"on_delete\":[{\"op\":\"$delete_document\"}]}]",
        0,
    );
    defer parsed.deinit(alloc);

    try std.testing.expect(parsed.delete_document_on_delete);
}

test "metadata replication backfill coordinator resumes and then skips completed sources" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-metadata-replication-backfill-coordinator";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    const FakeExecutor = struct {
        fn query(_: *anyopaque, inner_alloc: Allocator, _: []const u8, prepared: foreign_mod.PreparedQuery) !foreign_mod.QueryResult {
            var owned = prepared;
            defer owned.deinit(inner_alloc);

            const limit = parseSqlSuffixUsize(owned.sql_text, " LIMIT ") orelse 0;
            const offset = parseSqlSuffixUsize(owned.sql_text, " OFFSET ") orelse 0;

            const all_rows = [_][]const u8{
                "{\"id\":\"doc:1\",\"name\":\"alpha\"}",
                "{\"id\":\"doc:2\",\"name\":\"beta\"}",
            };
            if (offset >= all_rows.len) return .{ .rows = &.{}, .total = all_rows.len };

            const count = @min(limit, all_rows.len - offset);
            const rows = try inner_alloc.alloc(std.json.Value, count);
            for (0..count) |i| {
                rows[i] = try std.json.parseFromSliceLeaky(std.json.Value, inner_alloc, all_rows[offset + i], .{});
            }
            return .{ .rows = rows, .total = all_rows.len };
        }

        fn statistics(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !foreign_mod.TableStatistics {
            return .{ .row_count = 2, .size_bytes = 64 };
        }

        fn discoverColumns(_: *anyopaque, inner_alloc: Allocator, _: []const u8, _: []const u8) ![]foreign_mod.Column {
            const columns = try inner_alloc.alloc(foreign_mod.Column, 1);
            columns[0] = .{
                .name = try inner_alloc.dupe(u8, "id"),
                .data_type = try inner_alloc.dupe(u8, "text"),
                .nullable = false,
            };
            return columns;
        }
    };

    var registry = foreign_mod.Registry{};
    defer registry.deinit(alloc);
    try foreign_mod.registerPostgresExecutor(alloc, &registry, .{
        .ptr = undefined,
        .vtable = &.{
            .query = FakeExecutor.query,
            .statistics = FakeExecutor.statistics,
            .discover_columns = FakeExecutor.discoverColumns,
        },
    });

    var write_source = table_writes_api.BoundTableWriteSource.init("docs", &db);

    const FakeService = struct {
        alloc: Allocator,
        table: metadata_table_manager.TableRecord,
        statuses: std.ArrayListUnmanaged(metadata_table_manager.ReplicationSourceStatusRecord) = .empty,

        pub fn deinit(self: *@This()) void {
            for (self.statuses.items) |record| metadata_table_manager.freeReplicationSourceStatus(self.alloc, record);
            self.statuses.deinit(self.alloc);
        }

        pub fn metadataStatus(_: *@This()) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }

        pub fn listProjectedTables(self: *@This(), inner_alloc: Allocator) ![]metadata_table_manager.TableRecord {
            const out = try inner_alloc.alloc(metadata_table_manager.TableRecord, 1);
            out[0] = .{
                .table_id = self.table.table_id,
                .name = try inner_alloc.dupe(u8, self.table.name),
                .replication_sources_json = try inner_alloc.dupe(u8, self.table.replication_sources_json),
            };
            return out;
        }

        pub fn freeProjectedTables(_: *@This(), inner_alloc: Allocator, records: []metadata_table_manager.TableRecord) void {
            for (records) |record| {
                inner_alloc.free(record.name);
                inner_alloc.free(record.replication_sources_json);
            }
            inner_alloc.free(records);
        }

        pub fn listProjectedRanges(_: *@This(), inner_alloc: Allocator) ![]metadata_table_manager.RangeRecord {
            const out = try inner_alloc.alloc(metadata_table_manager.RangeRecord, 1);
            out[0] = .{
                .group_id = 7001,
                .table_id = 22,
                .start_key = "",
                .end_key = null,
            };
            return out;
        }

        pub fn freeProjectedRanges(_: *@This(), inner_alloc: Allocator, records: []metadata_table_manager.RangeRecord) void {
            inner_alloc.free(records);
        }

        pub fn listProjectedStores(_: *@This(), inner_alloc: Allocator) ![]metadata_table_manager.StoreRecord {
            return try inner_alloc.alloc(metadata_table_manager.StoreRecord, 0);
        }

        pub fn freeProjectedStores(_: *@This(), inner_alloc: Allocator, records: []metadata_table_manager.StoreRecord) void {
            inner_alloc.free(records);
        }

        pub fn listProjectedPlacementIntents(_: *@This(), inner_alloc: Allocator) ![]raft_reconciler.PlacementIntent {
            return try inner_alloc.alloc(raft_reconciler.PlacementIntent, 0);
        }

        pub fn freeProjectedPlacementIntents(_: *@This(), inner_alloc: Allocator, records: []raft_reconciler.PlacementIntent) void {
            inner_alloc.free(records);
        }

        pub fn listProjectedSplitTransitions(_: *@This(), inner_alloc: Allocator) ![]metadata_transition_state.SplitTransitionRecord {
            return try inner_alloc.alloc(metadata_transition_state.SplitTransitionRecord, 0);
        }

        pub fn freeProjectedSplitTransitions(_: *@This(), inner_alloc: Allocator, records: []metadata_transition_state.SplitTransitionRecord) void {
            inner_alloc.free(records);
        }

        pub fn listProjectedMergeTransitions(_: *@This(), inner_alloc: Allocator) ![]metadata_transition_state.MergeTransitionRecord {
            return try inner_alloc.alloc(metadata_transition_state.MergeTransitionRecord, 0);
        }

        pub fn freeProjectedMergeTransitions(_: *@This(), inner_alloc: Allocator, records: []metadata_transition_state.MergeTransitionRecord) void {
            inner_alloc.free(records);
        }

        pub fn listProjectedReplicationSourceStatuses(self: *@This(), inner_alloc: Allocator) ![]metadata_table_manager.ReplicationSourceStatusRecord {
            const out = try inner_alloc.alloc(metadata_table_manager.ReplicationSourceStatusRecord, self.statuses.items.len);
            for (self.statuses.items, 0..) |record, i| {
                out[i] = try metadata_table_manager.cloneReplicationSourceStatus(inner_alloc, record);
            }
            return out;
        }

        pub fn freeProjectedReplicationSourceStatuses(_: *@This(), inner_alloc: Allocator, records: []metadata_table_manager.ReplicationSourceStatusRecord) void {
            for (records) |record| metadata_table_manager.freeReplicationSourceStatus(inner_alloc, record);
            if (records.len > 0) inner_alloc.free(records);
        }

        pub fn upsertReplicationSourceStatus(self: *@This(), record: metadata_table_manager.ReplicationSourceStatusRecord) !void {
            for (self.statuses.items) |*existing| {
                if (existing.table_id == record.table_id and existing.source_ordinal == record.source_ordinal) {
                    metadata_table_manager.freeReplicationSourceStatus(self.alloc, existing.*);
                    existing.* = try metadata_table_manager.cloneReplicationSourceStatus(self.alloc, record);
                    return;
                }
            }
            try self.statuses.append(self.alloc, try metadata_table_manager.cloneReplicationSourceStatus(self.alloc, record));
        }
    };

    var service: FakeService = .{
        .alloc = alloc,
        .table = .{
            .table_id = 22,
            .name = "docs",
            .replication_sources_json = "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\",\"key_template\":\"id\"}]",
        },
    };
    defer service.deinit();

    try service.upsertReplicationSourceStatus(.{
        .table_id = 22,
        .source_ordinal = 0,
        .source_kind = "postgres",
        .external_table = "users",
        .phase = "snapshot",
        .checkpoint = "snapshot_offset:1",
        .snapshot_offset = 1,
        .cutover_mode = "exported_snapshot",
        .prepared_checkpoint = "lsn:resume",
        .last_error = "",
        .lag_records = 0,
        .updated_at_ms = 1,
    });

    var coordinator = SnapshotBackfillCoordinator{
        .alloc = alloc,
        .runner = .{
            .alloc = alloc,
            .registry = &registry,
            .write_source = write_source.source(),
            .batch_size = 1,
        },
    };

    const first = try coordinator.runRound(&service);
    try std.testing.expectEqual(@as(usize, 1), first.tables_considered);
    try std.testing.expectEqual(@as(usize, 1), first.sources_considered);
    try std.testing.expectEqual(@as(usize, 1), first.sources_resumed);
    try std.testing.expectEqual(@as(usize, 1), first.sources_completed);
    try std.testing.expectEqualStrings("cutover_prepared", service.statuses.items[0].phase);
    try std.testing.expectEqualStrings("snapshot_offset:2", service.statuses.items[0].checkpoint);
    try std.testing.expectEqualStrings("lsn:resume", service.statuses.items[0].prepared_checkpoint);
    try std.testing.expectEqualStrings("exported_snapshot", service.statuses.items[0].cutover_mode);

    const second = try coordinator.runRound(&service);
    try std.testing.expectEqual(@as(usize, 1), second.tables_considered);
    try std.testing.expectEqual(@as(usize, 1), second.sources_considered);
    try std.testing.expectEqual(@as(usize, 1), second.sources_skipped_complete);
    try std.testing.expectEqual(@as(usize, 0), second.sources_completed);
}

test "metadata replication backfill marks existing-slot fallback as slot_resumed" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-metadata-replication-slot-resumed";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    const FakeExecutor = struct {
        fn query(_: *anyopaque, inner_alloc: Allocator, _: []const u8, prepared: foreign_mod.PreparedQuery) !foreign_mod.QueryResult {
            var owned = prepared;
            defer owned.deinit(inner_alloc);
            const rows = try inner_alloc.alloc(std.json.Value, 1);
            rows[0] = try std.json.parseFromSliceLeaky(std.json.Value, inner_alloc, "{\"id\":\"doc:1\",\"name\":\"alpha\"}", .{});
            return .{ .rows = rows, .total = 1 };
        }

        fn statistics(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !foreign_mod.TableStatistics {
            return .{ .row_count = 1, .size_bytes = 32 };
        }

        fn prepareReplication(_: *anyopaque, inner_alloc: Allocator, _: []const u8, _: foreign_mod.ReplicationPollParams) !foreign_mod.ReplicationPrepareResult {
            return .{
                .checkpoint = try inner_alloc.dupe(u8, "lsn:resumed"),
                .slot_existed = true,
            };
        }

        fn discoverColumns(_: *anyopaque, inner_alloc: Allocator, _: []const u8, _: []const u8) ![]foreign_mod.Column {
            const columns = try inner_alloc.alloc(foreign_mod.Column, 1);
            columns[0] = .{
                .name = try inner_alloc.dupe(u8, "id"),
                .data_type = try inner_alloc.dupe(u8, "text"),
                .nullable = false,
            };
            return columns;
        }
    };

    var registry = foreign_mod.Registry{};
    defer registry.deinit(alloc);
    try foreign_mod.registerPostgresExecutor(alloc, &registry, .{
        .ptr = undefined,
        .vtable = &.{
            .query = FakeExecutor.query,
            .statistics = FakeExecutor.statistics,
            .discover_columns = FakeExecutor.discoverColumns,
            .prepare_replication = FakeExecutor.prepareReplication,
        },
    });

    var write_source = table_writes_api.BoundTableWriteSource.init("docs", &db);

    const StatusSink = struct {
        alloc: Allocator,
        records: std.ArrayListUnmanaged(metadata_table_manager.ReplicationSourceStatusRecord) = .empty,

        fn deinit(self: *@This()) void {
            for (self.records.items) |record| metadata_table_manager.freeReplicationSourceStatus(self.alloc, record);
            self.records.deinit(self.alloc);
        }

        fn upsertReplicationSourceStatus(self: *@This(), record: metadata_table_manager.ReplicationSourceStatusRecord) !void {
            try self.records.append(self.alloc, try metadata_table_manager.cloneReplicationSourceStatus(self.alloc, record));
        }
    };

    var status_sink: StatusSink = .{ .alloc = alloc };
    defer status_sink.deinit();

    var runner = SnapshotBackfillRunner{
        .alloc = alloc,
        .registry = &registry,
        .write_source = write_source.source(),
        .batch_size = 8,
    };

    _ = try runner.runTableSource(&status_sink, .{
        .table_id = 11,
        .name = "docs",
        .replication_sources_json = "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\",\"key_template\":\"id\"}]",
    }, 0);

    try std.testing.expectEqualStrings("slot_resumed", status_sink.records.items[status_sink.records.items.len - 1].cutover_mode);
    try std.testing.expectEqualStrings("lsn:resumed", status_sink.records.items[status_sink.records.items.len - 1].prepared_checkpoint);
}

test "metadata replication backfill rejects existing-slot fallback when exact cutover is required" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-metadata-replication-exact-cutover-required";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    const FakeExecutor = struct {
        fn query(_: *anyopaque, inner_alloc: Allocator, _: []const u8, prepared: foreign_mod.PreparedQuery) !foreign_mod.QueryResult {
            var owned = prepared;
            defer owned.deinit(inner_alloc);
            return .{ .rows = &.{}, .total = 0 };
        }

        fn statistics(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !foreign_mod.TableStatistics {
            return .{ .row_count = 1, .size_bytes = 32 };
        }

        fn prepareReplication(_: *anyopaque, inner_alloc: Allocator, _: []const u8, _: foreign_mod.ReplicationPollParams) !foreign_mod.ReplicationPrepareResult {
            return .{
                .checkpoint = try inner_alloc.dupe(u8, "lsn:existing"),
                .slot_existed = true,
            };
        }

        fn discoverColumns(_: *anyopaque, inner_alloc: Allocator, _: []const u8, _: []const u8) ![]foreign_mod.Column {
            const columns = try inner_alloc.alloc(foreign_mod.Column, 1);
            columns[0] = .{
                .name = try inner_alloc.dupe(u8, "id"),
                .data_type = try inner_alloc.dupe(u8, "text"),
                .nullable = false,
            };
            return columns;
        }
    };

    var registry = foreign_mod.Registry{};
    defer registry.deinit(alloc);
    try foreign_mod.registerPostgresExecutor(alloc, &registry, .{
        .ptr = undefined,
        .vtable = &.{
            .query = FakeExecutor.query,
            .statistics = FakeExecutor.statistics,
            .discover_columns = FakeExecutor.discoverColumns,
            .prepare_replication = FakeExecutor.prepareReplication,
        },
    });

    var write_source = table_writes_api.BoundTableWriteSource.init("docs", &db);

    const StatusSink = struct {
        alloc: Allocator,
        records: std.ArrayListUnmanaged(metadata_table_manager.ReplicationSourceStatusRecord) = .empty,

        fn deinit(self: *@This()) void {
            for (self.records.items) |record| metadata_table_manager.freeReplicationSourceStatus(self.alloc, record);
            self.records.deinit(self.alloc);
        }

        fn latest(self: *@This()) metadata_table_manager.ReplicationSourceStatusRecord {
            return self.records.items[self.records.items.len - 1];
        }

        fn upsertReplicationSourceStatus(self: *@This(), record: metadata_table_manager.ReplicationSourceStatusRecord) !void {
            try self.records.append(self.alloc, try metadata_table_manager.cloneReplicationSourceStatus(self.alloc, record));
        }
    };

    var status_sink: StatusSink = .{ .alloc = alloc };
    defer status_sink.deinit();

    var runner = SnapshotBackfillRunner{
        .alloc = alloc,
        .registry = &registry,
        .write_source = write_source.source(),
        .batch_size = 8,
    };

    try std.testing.expectError(error.ReplicationExactCutoverRequired, runner.runTableSource(&status_sink, .{
        .table_id = 11,
        .name = "docs",
        .replication_sources_json = "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\",\"key_template\":\"id\",\"require_exact_cutover\":true}]",
    }, 0));

    try std.testing.expectEqualStrings("failed", status_sink.latest().phase);
    try std.testing.expectEqualStrings("terminal", status_sink.latest().failure_class);
    try std.testing.expectEqualStrings("ReplicationExactCutoverRequired", status_sink.latest().last_error);
}

test "metadata replication stream applies insert update and delete through bound write source" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-metadata-replication-stream";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    _ = try db.batch(.{
        .writes = &[_]db_mod.types.BatchWrite{
            .{ .key = "doc:2", .value = "{\"id\":\"doc:2\",\"name\":\"stale\"}" },
        },
    });

    const StreamCtx = struct {
        served: bool = false,
    };

    const StreamSource = struct {
        ctx: *StreamCtx,
        alloc: Allocator,

        fn destroy(ptr: *anyopaque, _: Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.alloc.destroy(self);
        }

        fn query(_: *anyopaque, inner_alloc: Allocator, _: foreign_mod.QueryParams) !foreign_mod.QueryResult {
            return .{ .rows = try inner_alloc.alloc(std.json.Value, 0), .total = 0 };
        }

        fn statistics(_: *anyopaque, _: []const u8) !foreign_mod.TableStatistics {
            return .{ .row_count = 0, .size_bytes = 0 };
        }

        fn pollChanges(ptr: *anyopaque, inner_alloc: Allocator, _: foreign_mod.ReplicationPollParams) !foreign_mod.ReplicationPollResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.ctx.served) return .{ .changes = &.{}, .lag_records = 0 };
            self.ctx.served = true;

            const changes = try inner_alloc.alloc(foreign_mod.ReplicationChange, 3);
            changes[0] = .{
                .op = .insert,
                .checkpoint = try inner_alloc.dupe(u8, "lsn:1"),
                .row = try std.json.parseFromSliceLeaky(std.json.Value, inner_alloc, "{\"id\":\"doc:1\",\"name\":\"alpha\"}", .{}),
                .commit_timestamp_ms = 1000,
            };
            changes[1] = .{
                .op = .update,
                .checkpoint = try inner_alloc.dupe(u8, "lsn:2"),
                .row = try std.json.parseFromSliceLeaky(std.json.Value, inner_alloc, "{\"id\":\"doc:1\",\"name\":\"beta\"}", .{}),
                .commit_timestamp_ms = 1001,
            };
            changes[2] = .{
                .op = .delete,
                .checkpoint = try inner_alloc.dupe(u8, "lsn:3"),
                .row = try std.json.parseFromSliceLeaky(std.json.Value, inner_alloc, "{\"id\":\"doc:2\",\"name\":\"stale\"}", .{}),
                .commit_timestamp_ms = 1002,
            };
            return .{ .changes = changes, .lag_records = 4, .lag_millis = 40 };
        }

        fn factory(ctx_ptr: *anyopaque, inner_alloc: Allocator, config: foreign_mod.Config) !foreign_mod.Source {
            var owned_config = config;
            defer owned_config.deinit(inner_alloc);
            const self = try inner_alloc.create(@This());
            self.* = .{
                .ctx = @ptrCast(@alignCast(ctx_ptr)),
                .alloc = inner_alloc,
            };
            return .{
                .ptr = self,
                .vtable = &.{
                    .deinit = destroy,
                    .query = query,
                    .statistics = statistics,
                    .poll_changes = pollChanges,
                },
            };
        }
    };

    var stream_ctx = StreamCtx{};
    var registry = foreign_mod.Registry{};
    defer registry.deinit(alloc);
    try registry.registerWithContext(alloc, .postgres, &stream_ctx, StreamSource.factory, null);

    var write_source = table_writes_api.BoundTableWriteSource.init("docs", &db);

    const StatusSink = struct {
        alloc: Allocator,
        records: std.ArrayListUnmanaged(metadata_table_manager.ReplicationSourceStatusRecord) = .empty,

        fn deinit(self: *@This()) void {
            for (self.records.items) |record| metadata_table_manager.freeReplicationSourceStatus(self.alloc, record);
            self.records.deinit(self.alloc);
        }

        fn upsertReplicationSourceStatus(self: *@This(), record: metadata_table_manager.ReplicationSourceStatusRecord) !void {
            try self.records.append(self.alloc, try metadata_table_manager.cloneReplicationSourceStatus(self.alloc, record));
        }
    };

    var status_sink: StatusSink = .{ .alloc = alloc };
    defer status_sink.deinit();

    var runner = StreamingReplicationRunner{
        .alloc = alloc,
        .registry = &registry,
        .write_source = write_source.source(),
    };

    const summary = try runner.runTableSourceFromCheckpoint(&status_sink, .{
        .table_id = 11,
        .name = "docs",
        .replication_sources_json = "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\",\"key_template\":\"id\"}]",
    }, 0, 2, "slot_first", null, null);

    try std.testing.expectEqual(@as(usize, 3), summary.changes_applied);
    try std.testing.expectEqual(@as(usize, 2), summary.writes_applied);
    try std.testing.expectEqual(@as(usize, 1), summary.deletes_applied);
    try std.testing.expect(status_sink.records.items.len >= 3);
    try std.testing.expectEqualStrings("streaming", status_sink.records.items[status_sink.records.items.len - 1].phase);
    try std.testing.expectEqualStrings("lsn:3", status_sink.records.items[status_sink.records.items.len - 1].checkpoint);
    try std.testing.expectEqualStrings("lsn:3", status_sink.records.items[status_sink.records.items.len - 1].stream_checkpoint);
    try std.testing.expectEqual(@as(u64, 2), status_sink.records.items[status_sink.records.items.len - 1].snapshot_offset);
    try std.testing.expectEqualStrings("antfly_postgres_users_docs", status_sink.records.items[status_sink.records.items.len - 1].slot_name);
    try std.testing.expectEqualStrings("antfly_pub_postgres_users_docs", status_sink.records.items[status_sink.records.items.len - 1].publication_name);
    try std.testing.expectEqual(@as(u64, 4), status_sink.records.items[status_sink.records.items.len - 1].lag_records);
    try std.testing.expect(status_sink.records.items[status_sink.records.items.len - 1].lag_millis > 0);
    try std.testing.expectEqual(@as(u64, 1002), status_sink.records.items[status_sink.records.items.len - 1].last_source_commit_at_ms);

    var result_one = (try db.lookup(alloc, "doc:1", .{})).?;
    defer result_one.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, result_one.json, "\"beta\"") != null);
    var result_two = (try db.lookup(alloc, "doc:2", .{})).?;
    defer result_two.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, result_two.json, "\"id\":\"doc:2\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result_two.json, "\"name\"") == null);
}

test "metadata replication stream delete document op removes the full document" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-metadata-replication-stream-delete-document";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    _ = try db.batch(.{
        .writes = &[_]db_mod.types.BatchWrite{
            .{ .key = "doc:2", .value = "{\"id\":\"doc:2\",\"name\":\"stale\"}" },
        },
    });

    const StreamSource = struct {
        alloc: Allocator,

        fn destroy(ptr: *anyopaque, _: Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.alloc.destroy(self);
        }

        fn query(_: *anyopaque, inner_alloc: Allocator, _: foreign_mod.QueryParams) !foreign_mod.QueryResult {
            return .{ .rows = try inner_alloc.alloc(std.json.Value, 0), .total = 0 };
        }

        fn statistics(_: *anyopaque, _: []const u8) !foreign_mod.TableStatistics {
            return .{ .row_count = 0, .size_bytes = 0 };
        }

        fn pollChanges(_: *anyopaque, inner_alloc: Allocator, params: foreign_mod.ReplicationPollParams) !foreign_mod.ReplicationPollResult {
            _ = params;
            const changes = try inner_alloc.alloc(foreign_mod.ReplicationChange, 1);
            changes[0] = .{
                .op = .delete,
                .checkpoint = try inner_alloc.dupe(u8, "lsn:7"),
                .row = try std.json.parseFromSliceLeaky(std.json.Value, inner_alloc, "{\"id\":\"doc:2\",\"name\":\"stale\"}", .{}),
            };
            return .{ .changes = changes, .lag_records = 0 };
        }

        fn factory(_: *anyopaque, inner_alloc: Allocator, config: foreign_mod.Config) !foreign_mod.Source {
            var owned_config = config;
            defer owned_config.deinit(inner_alloc);
            const self = try inner_alloc.create(@This());
            self.* = .{ .alloc = inner_alloc };
            return .{
                .ptr = self,
                .vtable = &.{
                    .deinit = destroy,
                    .query = query,
                    .statistics = statistics,
                    .poll_changes = pollChanges,
                },
            };
        }
    };

    var registry = foreign_mod.Registry{};
    defer registry.deinit(alloc);
    var sentinel: u8 = 0;
    try registry.registerWithContext(alloc, .postgres, &sentinel, StreamSource.factory, null);

    var write_source = table_writes_api.BoundTableWriteSource.init("docs", &db);

    const StatusSink = struct {
        fn upsertReplicationSourceStatus(_: *@This(), _: metadata_table_manager.ReplicationSourceStatusRecord) !void {}
    };
    var status_sink = StatusSink{};

    var runner = StreamingReplicationRunner{
        .alloc = alloc,
        .registry = &registry,
        .write_source = write_source.source(),
    };

    _ = try runner.runTableSourceFromCheckpoint(&status_sink, .{
        .table_id = 11,
        .name = "docs",
        .replication_sources_json = "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\",\"key_template\":\"id\",\"on_delete\":[{\"op\":\"$delete_document\"}]}]",
    }, 0, 2, "slot_first", null, null);

    try std.testing.expect((try db.lookup(alloc, "doc:2", .{})) == null);
}

test "metadata replication stream routes matching rows to target tables" {
    const alloc = std.testing.allocator;

    const StreamCtx = struct {
        served: bool = false,
    };

    const StreamSource = struct {
        ctx: *StreamCtx,
        alloc: Allocator,

        fn destroy(ptr: *anyopaque, _: Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.alloc.destroy(self);
        }

        fn query(_: *anyopaque, inner_alloc: Allocator, _: foreign_mod.QueryParams) !foreign_mod.QueryResult {
            return .{ .rows = try inner_alloc.alloc(std.json.Value, 0), .total = 0 };
        }

        fn statistics(_: *anyopaque, _: []const u8) !foreign_mod.TableStatistics {
            return .{ .row_count = 0, .size_bytes = 0 };
        }

        fn pollChanges(ptr: *anyopaque, inner_alloc: Allocator, _: foreign_mod.ReplicationPollParams) !foreign_mod.ReplicationPollResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.ctx.served) return .{ .changes = &.{}, .lag_records = 0 };
            self.ctx.served = true;

            const changes = try inner_alloc.alloc(foreign_mod.ReplicationChange, 4);
            changes[0] = .{
                .op = .insert,
                .checkpoint = try inner_alloc.dupe(u8, "lsn:20"),
                .row = try std.json.parseFromSliceLeaky(std.json.Value, inner_alloc, "{\"id\":\"r-1\",\"name\":\"Alice\",\"tier\":\"premium\"}", .{}),
            };
            changes[1] = .{
                .op = .update,
                .checkpoint = try inner_alloc.dupe(u8, "lsn:21"),
                .row = try std.json.parseFromSliceLeaky(std.json.Value, inner_alloc, "{\"id\":\"r-2\",\"name\":\"Bob\",\"tier\":\"free\"}", .{}),
            };
            changes[2] = .{
                .op = .update,
                .checkpoint = try inner_alloc.dupe(u8, "lsn:22"),
                .row = try std.json.parseFromSliceLeaky(std.json.Value, inner_alloc, "{\"id\":\"r-3\",\"name\":\"Eve\",\"tier\":\"enterprise\"}", .{}),
            };
            changes[3] = .{
                .op = .delete,
                .checkpoint = try inner_alloc.dupe(u8, "lsn:23"),
                .row = try std.json.parseFromSliceLeaky(std.json.Value, inner_alloc, "{\"id\":\"r-1\",\"name\":\"Alice\",\"tier\":\"premium\"}", .{}),
            };
            return .{ .changes = changes, .lag_records = 0 };
        }

        fn factory(ctx_ptr: *anyopaque, inner_alloc: Allocator, config: foreign_mod.Config) !foreign_mod.Source {
            var owned_config = config;
            defer owned_config.deinit(inner_alloc);
            const self = try inner_alloc.create(@This());
            self.* = .{
                .ctx = @ptrCast(@alignCast(ctx_ptr)),
                .alloc = inner_alloc,
            };
            return .{
                .ptr = self,
                .vtable = &.{
                    .deinit = destroy,
                    .query = query,
                    .statistics = statistics,
                    .poll_changes = pollChanges,
                },
            };
        }
    };

    const CaptureWriteSource = struct {
        const CapturedCall = struct {
            table_name: []u8,
            key: []u8,
            upsert: bool,
            delete_document: bool,
            op_paths: [][]u8 = &.{},
        };

        alloc: Allocator,
        calls: std.ArrayListUnmanaged(CapturedCall) = .empty,

        fn deinit(self: *@This()) void {
            for (self.calls.items) |call| {
                self.alloc.free(call.table_name);
                self.alloc.free(call.key);
                for (call.op_paths) |path| self.alloc.free(path);
                if (call.op_paths.len > 0) self.alloc.free(call.op_paths);
            }
            self.calls.deinit(self.alloc);
        }

        fn source(self: *@This()) table_writes_api.TableWriteSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .batch = batch,
                },
            };
        }

        fn batch(ptr: *anyopaque, _: Allocator, table_name: []const u8, req: db_mod.types.BatchRequest) !?void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (req.transforms.len > 0) {
                for (req.transforms) |doc_transform| {
                    const op_paths = try self.alloc.alloc([]u8, doc_transform.operations.len);
                    errdefer {
                        for (op_paths[0..doc_transform.operations.len]) |path| self.alloc.free(path);
                        self.alloc.free(op_paths);
                    }
                    for (doc_transform.operations, 0..) |op, i| {
                        op_paths[i] = try self.alloc.dupe(u8, op.path);
                    }
                    try self.calls.append(self.alloc, .{
                        .table_name = try self.alloc.dupe(u8, table_name),
                        .key = try self.alloc.dupe(u8, doc_transform.key),
                        .upsert = doc_transform.upsert,
                        .delete_document = false,
                        .op_paths = op_paths,
                    });
                }
            }
            if (req.deletes.len > 0) {
                for (req.deletes) |key| {
                    try self.calls.append(self.alloc, .{
                        .table_name = try self.alloc.dupe(u8, table_name),
                        .key = try self.alloc.dupe(u8, key),
                        .upsert = false,
                        .delete_document = true,
                    });
                }
            }
            return {};
        }
    };

    var stream_ctx = StreamCtx{};
    var registry = foreign_mod.Registry{};
    defer registry.deinit(alloc);
    try registry.registerWithContext(alloc, .postgres, &stream_ctx, StreamSource.factory, null);

    var capture = CaptureWriteSource{ .alloc = alloc };
    defer capture.deinit();

    const StatusSink = struct {
        fn upsertReplicationSourceStatus(_: *@This(), _: metadata_table_manager.ReplicationSourceStatusRecord) !void {}
    };
    var status_sink = StatusSink{};

    var runner = StreamingReplicationRunner{
        .alloc = alloc,
        .registry = &registry,
        .write_source = capture.source(),
    };

    const summary = try runner.runTableSourceFromCheckpoint(&status_sink, .{
        .table_id = 11,
        .name = "cdc_routes_router",
        .replication_sources_json =
        \\[{
        \\  "type":"postgres",
        \\  "dsn":"postgres://db",
        \\  "postgres_table":"users",
        \\  "key_template":"id",
        \\  "on_update":[{"op":"$set","path":"ignored","value":"true"}],
        \\  "routes":[
        \\    {
        \\      "target_table":"premium_users",
        \\      "where":{"term":"premium","field":"tier"},
        \\      "on_update":[{"op":"$set","path":"name","value":"{{name}}"}],
        \\      "on_delete":[{"op":"$delete_document"}]
        \\    },
        \\    {
        \\      "target_table":"free_users",
        \\      "where":{"term":"free","field":"tier"}
        \\    }
        \\  ]
        \\}]
        ,
    }, 0, 2, "slot_first", null, null);

    try std.testing.expectEqual(@as(usize, 4), summary.changes_applied);
    try std.testing.expectEqual(@as(usize, 2), summary.writes_applied);
    try std.testing.expectEqual(@as(usize, 1), summary.deletes_applied);
    try std.testing.expectEqual(@as(usize, 3), capture.calls.items.len);

    try std.testing.expectEqualStrings("premium_users", capture.calls.items[0].table_name);
    try std.testing.expectEqualStrings("r-1", capture.calls.items[0].key);
    try std.testing.expect(capture.calls.items[0].upsert);
    try std.testing.expectEqual(@as(usize, 1), capture.calls.items[0].op_paths.len);
    try std.testing.expectEqualStrings("name", capture.calls.items[0].op_paths[0]);

    try std.testing.expectEqualStrings("free_users", capture.calls.items[1].table_name);
    try std.testing.expectEqualStrings("r-2", capture.calls.items[1].key);
    try std.testing.expect(capture.calls.items[1].upsert);
    try std.testing.expectEqual(@as(usize, 3), capture.calls.items[1].op_paths.len);

    try std.testing.expectEqualStrings("premium_users", capture.calls.items[2].table_name);
    try std.testing.expectEqualStrings("r-1", capture.calls.items[2].key);
    try std.testing.expect(capture.calls.items[2].delete_document);
}

test "metadata replication stream applies configured update and derived delete transforms" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-metadata-replication-stream-transforms";

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    _ = try db.batch(.{
        .writes = &[_]db_mod.types.BatchWrite{
            .{ .key = "doc:9", .value = "{\"id\":\"doc:9\",\"name\":\"before\",\"age\":7,\"stale\":true}" },
        },
    });

    const StreamCtx = struct {
        calls: usize = 0,
    };

    const StreamSource = struct {
        ctx: *StreamCtx,
        alloc: Allocator,

        fn destroy(ptr: *anyopaque, _: Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.alloc.destroy(self);
        }

        fn query(_: *anyopaque, inner_alloc: Allocator, _: foreign_mod.QueryParams) !foreign_mod.QueryResult {
            return .{ .rows = try inner_alloc.alloc(std.json.Value, 0), .total = 0 };
        }

        fn statistics(_: *anyopaque, _: []const u8) !foreign_mod.TableStatistics {
            return .{ .row_count = 0, .size_bytes = 0 };
        }

        fn pollChanges(ptr: *anyopaque, inner_alloc: Allocator, _: foreign_mod.ReplicationPollParams) !foreign_mod.ReplicationPollResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.ctx.calls += 1;
            if (self.ctx.calls == 1) {
                const changes = try inner_alloc.alloc(foreign_mod.ReplicationChange, 1);
                changes[0] = .{
                    .op = .update,
                    .checkpoint = try inner_alloc.dupe(u8, "lsn:10"),
                    .row = try std.json.parseFromSliceLeaky(std.json.Value, inner_alloc, "{\"id\":\"doc:9\",\"name\":\"after\",\"profile\":{\"city\":\"sf\"}}", .{}),
                };
                return .{ .changes = changes, .lag_records = 0 };
            }
            const changes = try inner_alloc.alloc(foreign_mod.ReplicationChange, 1);
            changes[0] = .{
                .op = .delete,
                .checkpoint = try inner_alloc.dupe(u8, "lsn:11"),
                .row = try std.json.parseFromSliceLeaky(std.json.Value, inner_alloc, "{\"id\":\"doc:9\",\"name\":\"after\",\"profile\":{\"city\":\"sf\"}}", .{}),
            };
            return .{ .changes = changes, .lag_records = 0 };
        }

        fn factory(ctx_ptr: *anyopaque, inner_alloc: Allocator, config: foreign_mod.Config) !foreign_mod.Source {
            var owned_config = config;
            defer owned_config.deinit(inner_alloc);
            const self = try inner_alloc.create(@This());
            self.* = .{
                .ctx = @ptrCast(@alignCast(ctx_ptr)),
                .alloc = inner_alloc,
            };
            return .{
                .ptr = self,
                .vtable = &.{
                    .deinit = destroy,
                    .query = query,
                    .statistics = statistics,
                    .poll_changes = pollChanges,
                },
            };
        }
    };

    var ctx = StreamCtx{};
    var registry = foreign_mod.Registry{};
    defer registry.deinit(alloc);
    try registry.registerWithContext(alloc, .postgres, &ctx, StreamSource.factory, null);

    var write_source = table_writes_api.BoundTableWriteSource.init("docs", &db);
    const StatusSink = struct {
        fn upsertReplicationSourceStatus(_: *@This(), _: metadata_table_manager.ReplicationSourceStatusRecord) !void {}
    };
    var status_sink = StatusSink{};

    var runner = StreamingReplicationRunner{
        .alloc = alloc,
        .registry = &registry,
        .write_source = write_source.source(),
    };

    _ = try runner.runTableSourceFromCheckpoint(&status_sink, .{
        .table_id = 11,
        .name = "docs",
        .replication_sources_json = "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\",\"key_template\":\"id\",\"on_update\":[{\"op\":\"$set\",\"path\":\"name\",\"value\":\"{{name}}\"},{\"op\":\"$merge\",\"value\":\"{{profile}}\"}],\"on_delete\":[{\"op\":\"$unset\",\"path\":\"name\"},{\"op\":\"$unset\",\"path\":\"city\"}]}]",
    }, 0, 2, "slot_first", null, null);

    var after_update = (try db.lookup(alloc, "doc:9", .{})).?;
    defer after_update.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, after_update.json, "\"after\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, after_update.json, "\"city\":\"sf\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, after_update.json, "\"stale\":true") != null);

    _ = try runner.runTableSourceFromCheckpoint(&status_sink, .{
        .table_id = 11,
        .name = "docs",
        .replication_sources_json = "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\",\"key_template\":\"id\",\"on_update\":[{\"op\":\"$set\",\"path\":\"name\",\"value\":\"{{name}}\"},{\"op\":\"$merge\",\"value\":\"{{profile}}\"}],\"on_delete\":[{\"op\":\"$unset\",\"path\":\"name\"},{\"op\":\"$unset\",\"path\":\"city\"}]}]",
    }, 0, 2, "slot_first", "lsn:10", null);

    var after_delete = (try db.lookup(alloc, "doc:9", .{})).?;
    defer after_delete.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, after_delete.json, "\"name\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, after_delete.json, "\"city\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, after_delete.json, "\"stale\":true") != null);
}

test "metadata replication stream coordinator waits for snapshot completion and resumes checkpoints" {
    const alloc = std.testing.allocator;

    const StreamCtx = struct {
        poll_calls: usize = 0,
        checkpoints: std.ArrayListUnmanaged([]u8) = .empty,

        fn deinit(self: *@This(), inner_alloc: Allocator) void {
            for (self.checkpoints.items) |checkpoint| inner_alloc.free(checkpoint);
            self.checkpoints.deinit(inner_alloc);
        }
    };

    const StreamSource = struct {
        ctx: *StreamCtx,
        alloc: Allocator,

        fn destroy(ptr: *anyopaque, _: Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.alloc.destroy(self);
        }

        fn query(_: *anyopaque, inner_alloc: Allocator, _: foreign_mod.QueryParams) !foreign_mod.QueryResult {
            return .{ .rows = try inner_alloc.alloc(std.json.Value, 0), .total = 0 };
        }

        fn statistics(_: *anyopaque, _: []const u8) !foreign_mod.TableStatistics {
            return .{ .row_count = 0, .size_bytes = 0 };
        }

        fn pollChanges(ptr: *anyopaque, inner_alloc: Allocator, params: foreign_mod.ReplicationPollParams) !foreign_mod.ReplicationPollResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.ctx.poll_calls += 1;
            try self.ctx.checkpoints.append(inner_alloc, if (params.checkpoint) |checkpoint| try inner_alloc.dupe(u8, checkpoint) else try inner_alloc.dupe(u8, ""));

            if (self.ctx.poll_calls == 1) {
                const changes = try inner_alloc.alloc(foreign_mod.ReplicationChange, 1);
                changes[0] = .{
                    .op = .insert,
                    .checkpoint = try inner_alloc.dupe(u8, "lsn:5"),
                    .row = try std.json.parseFromSliceLeaky(std.json.Value, inner_alloc, "{\"id\":\"doc:1\",\"name\":\"alpha\"}", .{}),
                };
                return .{ .changes = changes, .lag_records = 0 };
            }
            return .{ .changes = &.{}, .lag_records = 0 };
        }

        fn factory(ctx_ptr: *anyopaque, inner_alloc: Allocator, config: foreign_mod.Config) !foreign_mod.Source {
            var owned_config = config;
            defer owned_config.deinit(inner_alloc);
            const self = try inner_alloc.create(@This());
            self.* = .{
                .ctx = @ptrCast(@alignCast(ctx_ptr)),
                .alloc = inner_alloc,
            };
            return .{
                .ptr = self,
                .vtable = &.{
                    .deinit = destroy,
                    .query = query,
                    .statistics = statistics,
                    .poll_changes = pollChanges,
                },
            };
        }
    };

    var registry = foreign_mod.Registry{};
    defer registry.deinit(alloc);
    var stream_ctx = StreamCtx{};
    defer stream_ctx.deinit(alloc);
    try registry.registerWithContext(alloc, .postgres, &stream_ctx, StreamSource.factory, null);

    const NullSource = struct {
        var sentinel: u8 = 0;

        fn batch(_: *anyopaque, _: Allocator, _: []const u8, _: db_mod.types.BatchRequest) !?void {
            return {};
        }
    };

    const FakeService = struct {
        alloc: Allocator,
        table: metadata_table_manager.TableRecord,
        statuses: std.ArrayListUnmanaged(metadata_table_manager.ReplicationSourceStatusRecord) = .empty,

        pub fn deinit(self: *@This()) void {
            for (self.statuses.items) |record| metadata_table_manager.freeReplicationSourceStatus(self.alloc, record);
            self.statuses.deinit(self.alloc);
        }

        pub fn metadataStatus(_: *@This()) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }

        pub fn listProjectedTables(self: *@This(), inner_alloc: Allocator) ![]metadata_table_manager.TableRecord {
            const out = try inner_alloc.alloc(metadata_table_manager.TableRecord, 1);
            out[0] = .{
                .table_id = self.table.table_id,
                .name = try inner_alloc.dupe(u8, self.table.name),
                .replication_sources_json = try inner_alloc.dupe(u8, self.table.replication_sources_json),
            };
            return out;
        }

        pub fn freeProjectedTables(_: *@This(), inner_alloc: Allocator, records: []metadata_table_manager.TableRecord) void {
            for (records) |record| {
                inner_alloc.free(record.name);
                inner_alloc.free(record.replication_sources_json);
            }
            inner_alloc.free(records);
        }

        pub fn listProjectedRanges(_: *@This(), inner_alloc: Allocator) ![]metadata_table_manager.RangeRecord {
            return try inner_alloc.alloc(metadata_table_manager.RangeRecord, 0);
        }

        pub fn freeProjectedRanges(_: *@This(), inner_alloc: Allocator, records: []metadata_table_manager.RangeRecord) void {
            inner_alloc.free(records);
        }

        pub fn listProjectedStores(_: *@This(), inner_alloc: Allocator) ![]metadata_table_manager.StoreRecord {
            return try inner_alloc.alloc(metadata_table_manager.StoreRecord, 0);
        }

        pub fn freeProjectedStores(_: *@This(), inner_alloc: Allocator, records: []metadata_table_manager.StoreRecord) void {
            inner_alloc.free(records);
        }

        pub fn listProjectedPlacementIntents(_: *@This(), inner_alloc: Allocator) ![]raft_reconciler.PlacementIntent {
            return try inner_alloc.alloc(raft_reconciler.PlacementIntent, 0);
        }

        pub fn freeProjectedPlacementIntents(_: *@This(), inner_alloc: Allocator, records: []raft_reconciler.PlacementIntent) void {
            inner_alloc.free(records);
        }

        pub fn listProjectedSplitTransitions(_: *@This(), inner_alloc: Allocator) ![]metadata_transition_state.SplitTransitionRecord {
            return try inner_alloc.alloc(metadata_transition_state.SplitTransitionRecord, 0);
        }

        pub fn freeProjectedSplitTransitions(_: *@This(), inner_alloc: Allocator, records: []metadata_transition_state.SplitTransitionRecord) void {
            inner_alloc.free(records);
        }

        pub fn listProjectedMergeTransitions(_: *@This(), inner_alloc: Allocator) ![]metadata_transition_state.MergeTransitionRecord {
            return try inner_alloc.alloc(metadata_transition_state.MergeTransitionRecord, 0);
        }

        pub fn freeProjectedMergeTransitions(_: *@This(), inner_alloc: Allocator, records: []metadata_transition_state.MergeTransitionRecord) void {
            inner_alloc.free(records);
        }

        pub fn listProjectedReplicationSourceStatuses(self: *@This(), inner_alloc: Allocator) ![]metadata_table_manager.ReplicationSourceStatusRecord {
            const out = try inner_alloc.alloc(metadata_table_manager.ReplicationSourceStatusRecord, self.statuses.items.len);
            for (self.statuses.items, 0..) |record, i| {
                out[i] = try metadata_table_manager.cloneReplicationSourceStatus(inner_alloc, record);
            }
            return out;
        }

        pub fn freeProjectedReplicationSourceStatuses(_: *@This(), inner_alloc: Allocator, records: []metadata_table_manager.ReplicationSourceStatusRecord) void {
            for (records) |record| metadata_table_manager.freeReplicationSourceStatus(inner_alloc, record);
            if (records.len > 0) inner_alloc.free(records);
        }

        pub fn upsertReplicationSourceStatus(self: *@This(), record: metadata_table_manager.ReplicationSourceStatusRecord) !void {
            for (self.statuses.items) |*existing| {
                if (existing.table_id == record.table_id and existing.source_ordinal == record.source_ordinal) {
                    metadata_table_manager.freeReplicationSourceStatus(self.alloc, existing.*);
                    existing.* = try metadata_table_manager.cloneReplicationSourceStatus(self.alloc, record);
                    return;
                }
            }
            try self.statuses.append(self.alloc, try metadata_table_manager.cloneReplicationSourceStatus(self.alloc, record));
        }
    };

    var service: FakeService = .{
        .alloc = alloc,
        .table = .{
            .table_id = 22,
            .name = "docs",
            .replication_sources_json = "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\",\"key_template\":\"id\"}]",
        },
    };
    defer service.deinit();

    try service.upsertReplicationSourceStatus(.{
        .table_id = 22,
        .source_ordinal = 0,
        .source_kind = "postgres",
        .external_table = "users",
        .slot_name = "antfly_postgres_users_docs",
        .publication_name = "antfly_pub_postgres_users_docs",
        .phase = "cutover_prepared",
        .checkpoint = "snapshot_offset:2",
        .snapshot_offset = 2,
        .prepared_checkpoint = "lsn:prepared",
        .last_error = "",
        .lag_records = 0,
        .updated_at_ms = 1,
    });

    var coordinator = StreamingReplicationCoordinator{
        .alloc = alloc,
        .runner = .{
            .alloc = alloc,
            .registry = &registry,
            .write_source = .{
                .ptr = &NullSource.sentinel,
                .vtable = &.{
                    .batch = NullSource.batch,
                },
            },
        },
    };

    const first = try coordinator.runRound(&service);
    try std.testing.expectEqual(@as(usize, 1), first.tables_considered);
    try std.testing.expectEqual(@as(usize, 1), first.sources_considered);
    try std.testing.expectEqual(@as(usize, 0), first.sources_started);
    try std.testing.expectEqual(@as(usize, 1), first.sources_resumed);
    try std.testing.expectEqual(@as(usize, 1), first.sources_polled);
    try std.testing.expectEqual(@as(usize, 1), first.changes_applied);
    try std.testing.expectEqual(@as(usize, 1), stream_ctx.poll_calls);
    try std.testing.expectEqualStrings("lsn:prepared", stream_ctx.checkpoints.items[0]);
    try std.testing.expectEqualStrings("streaming", service.statuses.items[0].phase);
    try std.testing.expectEqualStrings("lsn:5", service.statuses.items[0].checkpoint);
    try std.testing.expectEqualStrings("lsn:prepared", service.statuses.items[0].prepared_checkpoint);
    try std.testing.expectEqualStrings("lsn:5", service.statuses.items[0].stream_checkpoint);
    try std.testing.expectEqual(@as(u64, 2), service.statuses.items[0].snapshot_offset);

    const second = try coordinator.runRound(&service);
    try std.testing.expectEqual(@as(usize, 1), second.sources_resumed);
    try std.testing.expectEqual(@as(usize, 1), second.sources_polled);
    try std.testing.expectEqual(@as(usize, 2), stream_ctx.poll_calls);
    try std.testing.expectEqualStrings("lsn:5", stream_ctx.checkpoints.items[1]);
    try std.testing.expectEqualStrings("lsn:prepared", service.statuses.items[0].prepared_checkpoint);
    try std.testing.expectEqualStrings("lsn:5", service.statuses.items[0].stream_checkpoint);
}

test "metadata replication stream coordinator recovers after transient polling failure" {
    const alloc = std.testing.allocator;

    const StreamCtx = struct {
        poll_calls: usize = 0,
    };

    const StreamSource = struct {
        ctx: *StreamCtx,
        alloc: Allocator,

        fn destroy(ptr: *anyopaque, _: Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.alloc.destroy(self);
        }

        fn query(_: *anyopaque, inner_alloc: Allocator, _: foreign_mod.QueryParams) !foreign_mod.QueryResult {
            return .{ .rows = try inner_alloc.alloc(std.json.Value, 0), .total = 0 };
        }

        fn statistics(_: *anyopaque, _: []const u8) !foreign_mod.TableStatistics {
            return .{ .row_count = 0, .size_bytes = 0 };
        }

        fn pollChanges(ptr: *anyopaque, inner_alloc: Allocator, params: foreign_mod.ReplicationPollParams) !foreign_mod.ReplicationPollResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = params;
            self.ctx.poll_calls += 1;
            if (self.ctx.poll_calls == 1) return error.ForeignQueryFailed;

            const changes = try inner_alloc.alloc(foreign_mod.ReplicationChange, 1);
            changes[0] = .{
                .op = .insert,
                .checkpoint = try inner_alloc.dupe(u8, "lsn:9"),
                .row = try std.json.parseFromSliceLeaky(std.json.Value, inner_alloc, "{\"id\":\"doc:1\",\"name\":\"recovered\"}", .{}),
                .lag_records = 2,
                .commit_timestamp_ms = 1005,
            };
            return .{ .changes = changes, .lag_records = 2, .lag_millis = 25 };
        }

        fn factory(ctx_ptr: *anyopaque, inner_alloc: Allocator, config: foreign_mod.Config) !foreign_mod.Source {
            var owned_config = config;
            defer owned_config.deinit(inner_alloc);
            const self = try inner_alloc.create(@This());
            self.* = .{
                .ctx = @ptrCast(@alignCast(ctx_ptr)),
                .alloc = inner_alloc,
            };
            return .{
                .ptr = self,
                .vtable = &.{
                    .deinit = destroy,
                    .query = query,
                    .statistics = statistics,
                    .poll_changes = pollChanges,
                },
            };
        }
    };

    const NullSource = struct {
        var sentinel: u8 = 0;

        fn batch(_: *anyopaque, _: Allocator, _: []const u8, _: db_mod.types.BatchRequest) !?void {
            return {};
        }
    };

    const StatusSink = struct {
        alloc: Allocator,
        records: std.ArrayListUnmanaged(metadata_table_manager.ReplicationSourceStatusRecord) = .empty,

        fn deinit(self: *@This()) void {
            for (self.records.items) |record| metadata_table_manager.freeReplicationSourceStatus(self.alloc, record);
            self.records.deinit(self.alloc);
        }

        fn latest(self: *@This()) metadata_table_manager.ReplicationSourceStatusRecord {
            return self.records.items[self.records.items.len - 1];
        }

        fn upsertReplicationSourceStatus(self: *@This(), record: metadata_table_manager.ReplicationSourceStatusRecord) !void {
            for (self.records.items) |*existing| {
                if (existing.table_id == record.table_id and existing.source_ordinal == record.source_ordinal) {
                    metadata_table_manager.freeReplicationSourceStatus(self.alloc, existing.*);
                    existing.* = try metadata_table_manager.cloneReplicationSourceStatus(self.alloc, record);
                    return;
                }
            }
            try self.records.append(self.alloc, try metadata_table_manager.cloneReplicationSourceStatus(self.alloc, record));
        }
    };

    const FakeService = struct {
        alloc: Allocator,
        statuses: *StatusSink,

        pub fn metadataStatus(_: *@This()) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }

        pub fn listProjectedTables(_: *@This(), inner_alloc: Allocator) ![]metadata_table_manager.TableRecord {
            const out = try inner_alloc.alloc(metadata_table_manager.TableRecord, 1);
            out[0] = .{
                .table_id = 22,
                .name = try inner_alloc.dupe(u8, "docs"),
                .replication_sources_json = try inner_alloc.dupe(u8, "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\",\"key_template\":\"id\"}]"),
            };
            return out;
        }

        pub fn freeProjectedTables(_: *@This(), inner_alloc: Allocator, records: []metadata_table_manager.TableRecord) void {
            for (records) |record| {
                inner_alloc.free(record.name);
                inner_alloc.free(record.replication_sources_json);
            }
            inner_alloc.free(records);
        }

        pub fn listProjectedRanges(_: *@This(), inner_alloc: Allocator) ![]metadata_table_manager.RangeRecord {
            return try inner_alloc.alloc(metadata_table_manager.RangeRecord, 0);
        }

        pub fn freeProjectedRanges(_: *@This(), inner_alloc: Allocator, records: []metadata_table_manager.RangeRecord) void {
            inner_alloc.free(records);
        }

        pub fn listProjectedStores(_: *@This(), inner_alloc: Allocator) ![]metadata_table_manager.StoreRecord {
            return try inner_alloc.alloc(metadata_table_manager.StoreRecord, 0);
        }

        pub fn freeProjectedStores(_: *@This(), inner_alloc: Allocator, records: []metadata_table_manager.StoreRecord) void {
            inner_alloc.free(records);
        }

        pub fn listProjectedPlacementIntents(_: *@This(), inner_alloc: Allocator) ![]raft_reconciler.PlacementIntent {
            return try inner_alloc.alloc(raft_reconciler.PlacementIntent, 0);
        }

        pub fn freeProjectedPlacementIntents(_: *@This(), inner_alloc: Allocator, records: []raft_reconciler.PlacementIntent) void {
            inner_alloc.free(records);
        }

        pub fn listProjectedSplitTransitions(_: *@This(), inner_alloc: Allocator) ![]metadata_transition_state.SplitTransitionRecord {
            return try inner_alloc.alloc(metadata_transition_state.SplitTransitionRecord, 0);
        }

        pub fn freeProjectedSplitTransitions(_: *@This(), inner_alloc: Allocator, records: []metadata_transition_state.SplitTransitionRecord) void {
            inner_alloc.free(records);
        }

        pub fn listProjectedMergeTransitions(_: *@This(), inner_alloc: Allocator) ![]metadata_transition_state.MergeTransitionRecord {
            return try inner_alloc.alloc(metadata_transition_state.MergeTransitionRecord, 0);
        }

        pub fn freeProjectedMergeTransitions(_: *@This(), inner_alloc: Allocator, records: []metadata_transition_state.MergeTransitionRecord) void {
            inner_alloc.free(records);
        }

        pub fn listProjectedReplicationSourceStatuses(self: *@This(), inner_alloc: Allocator) ![]metadata_table_manager.ReplicationSourceStatusRecord {
            const out = try inner_alloc.alloc(metadata_table_manager.ReplicationSourceStatusRecord, self.statuses.records.items.len);
            for (self.statuses.records.items, 0..) |record, i| {
                out[i] = try metadata_table_manager.cloneReplicationSourceStatus(inner_alloc, record);
            }
            return out;
        }

        pub fn freeProjectedReplicationSourceStatuses(_: *@This(), inner_alloc: Allocator, records: []metadata_table_manager.ReplicationSourceStatusRecord) void {
            for (records) |record| metadata_table_manager.freeReplicationSourceStatus(inner_alloc, record);
            if (records.len > 0) inner_alloc.free(records);
        }

        pub fn upsertReplicationSourceStatus(self: *@This(), record: metadata_table_manager.ReplicationSourceStatusRecord) !void {
            try self.statuses.upsertReplicationSourceStatus(record);
        }
    };

    var registry = foreign_mod.Registry{};
    defer registry.deinit(alloc);
    var stream_ctx = StreamCtx{};
    try registry.registerWithContext(alloc, .postgres, &stream_ctx, StreamSource.factory, null);

    var status_sink: StatusSink = .{ .alloc = alloc };
    defer status_sink.deinit();
    try status_sink.upsertReplicationSourceStatus(.{
        .table_id = 22,
        .source_ordinal = 0,
        .source_kind = "postgres",
        .external_table = "users",
        .slot_name = "antfly_postgres_users_docs",
        .publication_name = "antfly_pub_postgres_users_docs",
        .phase = "cutover_prepared",
        .checkpoint = "snapshot_offset:1",
        .snapshot_offset = 1,
        .prepared_checkpoint = "lsn:prepared",
        .cutover_mode = "exported_snapshot",
        .last_error = "",
        .lag_records = 0,
        .updated_at_ms = 1,
    });

    var service = FakeService{ .alloc = alloc, .statuses = &status_sink };
    var coordinator = StreamingReplicationCoordinator{
        .alloc = alloc,
        .runner = .{
            .alloc = alloc,
            .registry = &registry,
            .write_source = .{
                .ptr = &NullSource.sentinel,
                .vtable = &.{
                    .batch = NullSource.batch,
                },
            },
        },
    };

    try std.testing.expectError(error.ForeignQueryFailed, coordinator.runRound(&service));
    try std.testing.expectEqualStrings("streaming_failed", status_sink.latest().phase);
    try std.testing.expect(status_sink.latest().last_error.len > 0);
    try std.testing.expectEqualStrings("retryable", status_sink.latest().failure_class);
    try std.testing.expectEqualStrings("lsn:prepared", status_sink.latest().prepared_checkpoint);

    const recovery = try coordinator.runRound(&service);
    try std.testing.expectEqual(@as(usize, 1), recovery.sources_resumed);
    try std.testing.expectEqual(@as(usize, 1), recovery.sources_polled);
    try std.testing.expectEqual(@as(usize, 1), recovery.changes_applied);
    try std.testing.expectEqualStrings("streaming", status_sink.latest().phase);
    try std.testing.expectEqualStrings("lsn:9", status_sink.latest().stream_checkpoint);
    try std.testing.expectEqualStrings("", status_sink.latest().last_error);
    try std.testing.expectEqualStrings("", status_sink.latest().failure_class);
    try std.testing.expectEqual(@as(u64, 2), status_sink.latest().lag_records);
    try std.testing.expect(status_sink.latest().lag_millis > 0);
    try std.testing.expectEqual(@as(u64, 1005), status_sink.latest().last_source_commit_at_ms);
}

test "metadata replication stream coordinator marks missing slot as terminal failure" {
    const alloc = std.testing.allocator;

    const StreamSource = struct {
        alloc: Allocator,

        fn destroy(ptr: *anyopaque, _: Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.alloc.destroy(self);
        }

        fn query(_: *anyopaque, inner_alloc: Allocator, _: foreign_mod.QueryParams) !foreign_mod.QueryResult {
            return .{ .rows = try inner_alloc.alloc(std.json.Value, 0), .total = 0 };
        }

        fn statistics(_: *anyopaque, _: []const u8) !foreign_mod.TableStatistics {
            return .{ .row_count = 0, .size_bytes = 0 };
        }

        fn pollChanges(_: *anyopaque, _: Allocator, _: foreign_mod.ReplicationPollParams) !foreign_mod.ReplicationPollResult {
            return error.ForeignReplicationSlotMissing;
        }

        fn factory(_: *anyopaque, inner_alloc: Allocator, config: foreign_mod.Config) !foreign_mod.Source {
            var owned_config = config;
            defer owned_config.deinit(inner_alloc);
            const self = try inner_alloc.create(@This());
            self.* = .{ .alloc = inner_alloc };
            return .{
                .ptr = self,
                .vtable = &.{
                    .deinit = destroy,
                    .query = query,
                    .statistics = statistics,
                    .poll_changes = pollChanges,
                },
            };
        }
    };

    const NullSource = struct {
        var sentinel: u8 = 0;

        fn batch(_: *anyopaque, _: Allocator, _: []const u8, _: db_mod.types.BatchRequest) !?void {
            return {};
        }
    };

    const StatusSink = struct {
        alloc: Allocator,
        records: std.ArrayListUnmanaged(metadata_table_manager.ReplicationSourceStatusRecord) = .empty,

        fn deinit(self: *@This()) void {
            for (self.records.items) |record| metadata_table_manager.freeReplicationSourceStatus(self.alloc, record);
            self.records.deinit(self.alloc);
        }

        fn latest(self: *@This()) metadata_table_manager.ReplicationSourceStatusRecord {
            return self.records.items[self.records.items.len - 1];
        }

        fn upsertReplicationSourceStatus(self: *@This(), record: metadata_table_manager.ReplicationSourceStatusRecord) !void {
            for (self.records.items) |*existing| {
                if (existing.table_id == record.table_id and existing.source_ordinal == record.source_ordinal) {
                    metadata_table_manager.freeReplicationSourceStatus(self.alloc, existing.*);
                    existing.* = try metadata_table_manager.cloneReplicationSourceStatus(self.alloc, record);
                    return;
                }
            }
            try self.records.append(self.alloc, try metadata_table_manager.cloneReplicationSourceStatus(self.alloc, record));
        }
    };

    const FakeService = struct {
        statuses: *StatusSink,

        pub fn metadataStatus(_: *@This()) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }

        pub fn listProjectedTables(_: *@This(), inner_alloc: Allocator) ![]metadata_table_manager.TableRecord {
            const out = try inner_alloc.alloc(metadata_table_manager.TableRecord, 1);
            out[0] = .{
                .table_id = 22,
                .name = try inner_alloc.dupe(u8, "docs"),
                .replication_sources_json = try inner_alloc.dupe(u8, "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\",\"key_template\":\"id\"}]"),
            };
            return out;
        }

        pub fn freeProjectedTables(_: *@This(), inner_alloc: Allocator, records: []metadata_table_manager.TableRecord) void {
            for (records) |record| {
                inner_alloc.free(record.name);
                inner_alloc.free(record.replication_sources_json);
            }
            inner_alloc.free(records);
        }

        pub fn listProjectedRanges(_: *@This(), inner_alloc: Allocator) ![]metadata_table_manager.RangeRecord {
            return try inner_alloc.alloc(metadata_table_manager.RangeRecord, 0);
        }

        pub fn freeProjectedRanges(_: *@This(), inner_alloc: Allocator, records: []metadata_table_manager.RangeRecord) void {
            inner_alloc.free(records);
        }

        pub fn listProjectedStores(_: *@This(), inner_alloc: Allocator) ![]metadata_table_manager.StoreRecord {
            return try inner_alloc.alloc(metadata_table_manager.StoreRecord, 0);
        }

        pub fn freeProjectedStores(_: *@This(), inner_alloc: Allocator, records: []metadata_table_manager.StoreRecord) void {
            inner_alloc.free(records);
        }

        pub fn listProjectedPlacementIntents(_: *@This(), inner_alloc: Allocator) ![]raft_reconciler.PlacementIntent {
            return try inner_alloc.alloc(raft_reconciler.PlacementIntent, 0);
        }

        pub fn freeProjectedPlacementIntents(_: *@This(), inner_alloc: Allocator, records: []raft_reconciler.PlacementIntent) void {
            inner_alloc.free(records);
        }

        pub fn listProjectedSplitTransitions(_: *@This(), inner_alloc: Allocator) ![]metadata_transition_state.SplitTransitionRecord {
            return try inner_alloc.alloc(metadata_transition_state.SplitTransitionRecord, 0);
        }

        pub fn freeProjectedSplitTransitions(_: *@This(), inner_alloc: Allocator, records: []metadata_transition_state.SplitTransitionRecord) void {
            inner_alloc.free(records);
        }

        pub fn listProjectedMergeTransitions(_: *@This(), inner_alloc: Allocator) ![]metadata_transition_state.MergeTransitionRecord {
            return try inner_alloc.alloc(metadata_transition_state.MergeTransitionRecord, 0);
        }

        pub fn freeProjectedMergeTransitions(_: *@This(), inner_alloc: Allocator, records: []metadata_transition_state.MergeTransitionRecord) void {
            inner_alloc.free(records);
        }

        pub fn listProjectedReplicationSourceStatuses(self: *@This(), inner_alloc: Allocator) ![]metadata_table_manager.ReplicationSourceStatusRecord {
            const out = try inner_alloc.alloc(metadata_table_manager.ReplicationSourceStatusRecord, self.statuses.records.items.len);
            for (self.statuses.records.items, 0..) |record, i| {
                out[i] = try metadata_table_manager.cloneReplicationSourceStatus(inner_alloc, record);
            }
            return out;
        }

        pub fn freeProjectedReplicationSourceStatuses(_: *@This(), inner_alloc: Allocator, records: []metadata_table_manager.ReplicationSourceStatusRecord) void {
            for (records) |record| metadata_table_manager.freeReplicationSourceStatus(inner_alloc, record);
            if (records.len > 0) inner_alloc.free(records);
        }

        pub fn upsertReplicationSourceStatus(self: *@This(), record: metadata_table_manager.ReplicationSourceStatusRecord) !void {
            try self.statuses.upsertReplicationSourceStatus(record);
        }
    };

    var registry = foreign_mod.Registry{};
    defer registry.deinit(alloc);
    try registry.registerWithContext(alloc, .postgres, undefined, StreamSource.factory, null);

    var status_sink: StatusSink = .{ .alloc = alloc };
    defer status_sink.deinit();
    try status_sink.upsertReplicationSourceStatus(.{
        .table_id = 22,
        .source_ordinal = 0,
        .source_kind = "postgres",
        .external_table = "users",
        .slot_name = "antfly_postgres_users_docs",
        .publication_name = "antfly_pub_postgres_users_docs",
        .phase = "streaming",
        .checkpoint = "lsn:10",
        .snapshot_offset = 1,
        .prepared_checkpoint = "lsn:prepared",
        .stream_checkpoint = "lsn:10",
        .cutover_mode = "exported_snapshot",
        .last_error = "",
        .failure_class = "",
        .lag_records = 0,
        .updated_at_ms = 1,
    });

    var service = FakeService{ .statuses = &status_sink };
    var coordinator = StreamingReplicationCoordinator{
        .alloc = alloc,
        .runner = .{
            .alloc = alloc,
            .registry = &registry,
            .write_source = .{
                .ptr = &NullSource.sentinel,
                .vtable = &.{
                    .batch = NullSource.batch,
                },
            },
        },
    };

    try std.testing.expectError(error.ForeignReplicationSlotMissing, coordinator.runRound(&service));
    try std.testing.expectEqualStrings("streaming_failed", status_sink.latest().phase);
    try std.testing.expectEqualStrings("terminal", status_sink.latest().failure_class);
    try std.testing.expectEqualStrings("ForeignReplicationSlotMissing", status_sink.latest().last_error);
    try std.testing.expectEqual(@as(u64, 1), status_sink.latest().consecutive_failures);
    try std.testing.expectEqualStrings("lsn:prepared", status_sink.latest().prepared_checkpoint);
    try std.testing.expectEqualStrings("lsn:10", status_sink.latest().stream_checkpoint);
}

fn parseSqlSuffixUsize(sql_text: []const u8, marker: []const u8) ?usize {
    const start = std.mem.indexOf(u8, sql_text, marker) orelse return null;
    const value_start = start + marker.len;
    var value_end = value_start;
    while (value_end < sql_text.len and std.ascii.isDigit(sql_text[value_end])) : (value_end += 1) {}
    if (value_end == value_start) return null;
    return std.fmt.parseInt(usize, sql_text[value_start..value_end], 10) catch null;
}

test "metadata replication live snapshot and later streaming insert through runner" {
    const alloc = std.testing.allocator;
    const dsn = try testPgDsnAlloc(alloc);
    defer alloc.free(dsn);

    const wal_level = execPsqlScalarAlloc(alloc, dsn, "show wal_level") catch return error.SkipZigTest;
    defer alloc.free(wal_level);
    if (!std.ascii.eqlIgnoreCase(std.mem.trim(u8, wal_level, &std.ascii.whitespace), "logical")) return error.SkipZigTest;

    const path = "/tmp/antfly-metadata-replication-live-runner";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    const suffix = blk: {
        var prng = std.Random.DefaultPrng.init(nowMillis());
        break :blk prng.random().int(u64);
    };
    const source_table = try std.fmt.allocPrint(alloc, "antfly_zig_metadata_cdc_live_{d}", .{suffix});
    defer alloc.free(source_table);
    const slot_name = try std.fmt.allocPrint(alloc, "antfly_zig_metadata_cdc_live_slot_{d}", .{suffix});
    defer alloc.free(slot_name);
    const publication_name = try std.fmt.allocPrint(alloc, "antfly_zig_metadata_cdc_live_pub_{d}", .{suffix});
    defer alloc.free(publication_name);

    const drop_publication_sql = try std.fmt.allocPrint(alloc, "drop publication if exists {s}", .{publication_name});
    defer alloc.free(drop_publication_sql);
    const drop_slot_sql = try std.fmt.allocPrint(
        alloc,
        "select pg_drop_replication_slot('{s}') from pg_replication_slots where slot_name = '{s}' and not active",
        .{ slot_name, slot_name },
    );
    defer alloc.free(drop_slot_sql);
    const drop_table_sql = try std.fmt.allocPrint(alloc, "drop table if exists {s}", .{source_table});
    defer alloc.free(drop_table_sql);
    defer {
        execPsqlCommand(alloc, dsn, drop_publication_sql) catch {};
        execPsqlCommand(alloc, dsn, drop_slot_sql) catch {};
        execPsqlCommand(alloc, dsn, drop_table_sql) catch {};
    }

    const create_table_sql = try std.fmt.allocPrint(
        alloc,
        "create table {s} (id text primary key, name text not null, tier text not null)",
        .{source_table},
    );
    defer alloc.free(create_table_sql);
    const seed_sql = try std.fmt.allocPrint(
        alloc,
        "insert into {s} (id, name, tier) values ('user-1', 'Alice', 'gold')",
        .{source_table},
    );
    defer alloc.free(seed_sql);
    const insert_sql = try std.fmt.allocPrint(
        alloc,
        "insert into {s} (id, name, tier) values ('user-2', 'Bob', 'silver')",
        .{source_table},
    );
    defer alloc.free(insert_sql);

    try execPsqlCommand(alloc, dsn, create_table_sql);
    try execPsqlCommand(alloc, dsn, seed_sql);

    var registry = foreign_mod.Registry{};
    defer registry.deinit(alloc);
    try foreign_mod.registerDefaultPostgresExecutor(alloc, &registry);

    var write_source = table_writes_api.BoundTableWriteSource.init("docs", &db);

    const StatusSink = struct {
        alloc: Allocator,
        records: std.ArrayListUnmanaged(metadata_table_manager.ReplicationSourceStatusRecord) = .empty,

        fn deinit(self: *@This()) void {
            for (self.records.items) |record| metadata_table_manager.freeReplicationSourceStatus(self.alloc, record);
            self.records.deinit(self.alloc);
        }

        fn upsertReplicationSourceStatus(self: *@This(), record: metadata_table_manager.ReplicationSourceStatusRecord) !void {
            for (self.records.items) |*existing| {
                if (existing.table_id == record.table_id and existing.source_ordinal == record.source_ordinal) {
                    metadata_table_manager.freeReplicationSourceStatus(self.alloc, existing.*);
                    existing.* = try metadata_table_manager.cloneReplicationSourceStatus(self.alloc, record);
                    return;
                }
            }
            try self.records.append(self.alloc, try metadata_table_manager.cloneReplicationSourceStatus(self.alloc, record));
        }

        fn latest(self: *@This()) metadata_table_manager.ReplicationSourceStatusRecord {
            return self.records.items[self.records.items.len - 1];
        }
    };

    var status_sink: StatusSink = .{ .alloc = alloc };
    defer status_sink.deinit();

    const table = metadata_table_manager.TableRecord{
        .table_id = 41,
        .name = "docs",
        .replication_sources_json = try std.fmt.allocPrint(
            alloc,
            "[{{\"type\":\"postgres\",\"dsn\":\"{s}\",\"postgres_table\":\"{s}\",\"key_template\":\"id\",\"slot_name\":\"{s}\",\"publication_name\":\"{s}\",\"on_delete\":[{{\"op\":\"$delete_document\"}}]}}]",
            .{ dsn, source_table, slot_name, publication_name },
        ),
    };
    defer alloc.free(table.replication_sources_json);

    var snapshot_runner = SnapshotBackfillRunner{
        .alloc = alloc,
        .registry = &registry,
        .write_source = write_source.source(),
    };
    const snapshot_summary = try snapshot_runner.runTableSource(&status_sink, table, 0);
    try std.testing.expectEqual(@as(usize, 1), snapshot_summary.rows_applied);
    try std.testing.expectEqualStrings("cutover_prepared", status_sink.latest().phase);
    try std.testing.expectEqual(@as(u64, 1), status_sink.latest().snapshot_offset);
    try std.testing.expect(status_sink.latest().prepared_checkpoint.len > 0);

    var user_one = (try db.lookup(alloc, "user-1", .{})).?;
    defer user_one.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, user_one.json, "\"Alice\"") != null);

    var stream_runner = StreamingReplicationRunner{
        .alloc = alloc,
        .registry = &registry,
        .write_source = write_source.source(),
    };
    const first_stream = try stream_runner.runTableSourceFromCheckpoint(&status_sink, table, 0, 1, "slot_first", null, null);
    try std.testing.expectEqual(@as(usize, 0), first_stream.changes_applied);
    try std.testing.expectEqualStrings("streaming", status_sink.latest().phase);

    try execPsqlCommand(alloc, dsn, insert_sql);

    const resume_checkpoint = if (status_sink.latest().stream_checkpoint.len > 0) status_sink.latest().stream_checkpoint else null;
    const second_stream = try stream_runner.runTableSourceFromCheckpoint(&status_sink, table, 0, 1, "slot_first", resume_checkpoint, null);
    try std.testing.expectEqual(@as(usize, 1), second_stream.changes_applied);
    try std.testing.expectEqualStrings("streaming", status_sink.latest().phase);

    var user_two = (try db.lookup(alloc, "user-2", .{})).?;
    defer user_two.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, user_two.json, "\"Bob\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, user_two.json, "\"silver\"") != null);
}

test "metadata http service live snapshot and later streaming insert through hosted rounds" {
    const raft_engine = @import("raft_engine");
    const raft_host_mod = @import("../raft/host.zig");
    const alloc = std.testing.allocator;
    const dsn = try testPgDsnAlloc(alloc);
    defer alloc.free(dsn);

    const wal_level = execPsqlScalarAlloc(alloc, dsn, "show wal_level") catch return error.SkipZigTest;
    defer alloc.free(wal_level);
    if (!std.ascii.eqlIgnoreCase(std.mem.trim(u8, wal_level, &std.ascii.whitespace), "logical")) return error.SkipZigTest;

    const Factory = struct {
        alloc: Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host_mod.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host_mod.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            errdefer self.alloc.free(peers);
            var bootstrap = try raft_host_mod.catalog.runtimeBootstrapFromRecord(self.alloc, record);
            errdefer raft_host_mod.catalog.freeRuntimeBootstrap(self.alloc, &bootstrap);
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = bootstrap,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, inner_alloc: Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            raft_host_mod.catalog.freeRuntimeBootstrap(inner_alloc, &desc.bootstrap);
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/metadata-cdc-hosted-root", .{tmp.sub_path});
    defer alloc.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/metadata-cdc-hosted-catalog.txt", .{tmp.sub_path});
    defer alloc.free(replica_catalog_path);
    const snapshot_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/metadata-cdc-hosted-snapshots", .{tmp.sub_path});
    defer alloc.free(snapshot_root);

    var store = raft_engine.core.MemoryStorage.init(alloc);
    defer store.deinit();
    var factory = Factory{ .alloc = alloc, .store = &store };

    var server = try metadata_server.MetadataServer.init(alloc, .{
        .http = .{
            .http = .{
                .host = .{
                    .local_node_id = 1,
                    .metadata_group_id = 2012,
                    .replica_root_dir = replica_root,
                    .replica_catalog_path = replica_catalog_path,
                },
                .transport = .{
                    .snapshot = .{ .root_dir = snapshot_root },
                },
            },
        },
        .admin_listener = .{},
    }, .{
        .http = .{
            .http = .{
                .http = .{
                    .host = .{
                        .descriptor_factory = factory.iface(),
                    },
                },
            },
        },
    });
    defer server.deinit();
    try server.start();

    _ = try server.svc.ensureMetadataReplica(.{
        .group_id = 2012,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try server.svc.campaignMetadataGroup();
    try server.runRound();

    const suffix = blk: {
        var prng = std.Random.DefaultPrng.init(nowMillis());
        break :blk prng.random().int(u64);
    };
    const source_table = try std.fmt.allocPrint(alloc, "antfly_zig_metadata_cdc_hosted_{d}", .{suffix});
    defer alloc.free(source_table);
    const slot_name = try std.fmt.allocPrint(alloc, "antfly_zig_metadata_cdc_hosted_slot_{d}", .{suffix});
    defer alloc.free(slot_name);
    const publication_name = try std.fmt.allocPrint(alloc, "antfly_zig_metadata_cdc_hosted_pub_{d}", .{suffix});
    defer alloc.free(publication_name);

    const drop_publication_sql = try std.fmt.allocPrint(alloc, "drop publication if exists {s}", .{publication_name});
    defer alloc.free(drop_publication_sql);
    const drop_slot_sql = try std.fmt.allocPrint(
        alloc,
        "select pg_drop_replication_slot('{s}') from pg_replication_slots where slot_name = '{s}' and not active",
        .{ slot_name, slot_name },
    );
    defer alloc.free(drop_slot_sql);
    const drop_table_sql = try std.fmt.allocPrint(alloc, "drop table if exists {s}", .{source_table});
    defer alloc.free(drop_table_sql);
    defer {
        execPsqlCommand(alloc, dsn, drop_publication_sql) catch {};
        execPsqlCommand(alloc, dsn, drop_slot_sql) catch {};
        execPsqlCommand(alloc, dsn, drop_table_sql) catch {};
    }

    const create_table_sql = try std.fmt.allocPrint(
        alloc,
        "create table {s} (id text primary key, name text not null, tier text not null)",
        .{source_table},
    );
    defer alloc.free(create_table_sql);
    const seed_sql = try std.fmt.allocPrint(
        alloc,
        "insert into {s} (id, name, tier) values ('user-1', 'Alice', 'gold')",
        .{source_table},
    );
    defer alloc.free(seed_sql);
    const insert_sql = try std.fmt.allocPrint(
        alloc,
        "insert into {s} (id, name, tier) values ('user-2', 'Bob', 'silver')",
        .{source_table},
    );
    defer alloc.free(insert_sql);

    try execPsqlCommand(alloc, dsn, create_table_sql);
    try execPsqlCommand(alloc, dsn, seed_sql);

    const create_body = try std.fmt.allocPrint(
        alloc,
        \\{{"replication_sources":[{{"type":"postgres","dsn":"{s}","postgres_table":"{s}","key_template":"id","slot_name":"{s}","publication_name":"{s}","on_delete":[{{"op":"$delete_document"}}]}}]}}
    ,
        .{ dsn, source_table, slot_name, publication_name },
    );
    defer alloc.free(create_body);
    var create_req = try tables_api.parseCreateTableRequest(alloc, create_body);
    defer create_req.deinit(alloc);

    const table = tables_api.deriveTableRecord("docs", create_req);
    const ranges = try tables_api.deriveInitialRanges(alloc, table);
    defer {
        for (ranges) |record| metadata_table_manager.freeRange(alloc, record);
        alloc.free(ranges);
    }

    var workflow = metadata_table_workflow.TableWorkflow.init(alloc);
    defer workflow.deinit();
    try workflow.setPlacementCandidates(&.{1});
    _ = try workflow.createTableWithRanges(server.svc, table, ranges);

    const group_id = ranges[0].group_id;
    const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root, group_id);
    defer alloc.free(db_path);

    var rounds: usize = 0;
    while (rounds < 64) : (rounds += 1) {
        server.svc.cdc_next_round_at_ms = 0;
        try server.runRound();

        var check_db = try db_mod.DB.open(alloc, db_path, .{
            .open_mode = .query_readonly,
        });
        defer check_db.close();
        if (try check_db.lookup(alloc, "user-1", .{})) |found| {
            var user_one = found;
            defer user_one.deinit(alloc);
            try std.testing.expect(std.mem.indexOf(u8, user_one.json, "\"Alice\"") != null);
            break;
        }
    }
    try std.testing.expect(rounds < 64);

    rounds = 0;
    while (rounds < 64) : (rounds += 1) {
        server.svc.cdc_next_round_at_ms = 0;
        try server.runRound();

        const statuses = try server.svc.listProjectedReplicationSourceStatuses(alloc);
        defer server.svc.freeProjectedReplicationSourceStatuses(alloc, statuses);
        if (statuses.len == 0) continue;
        if (std.mem.eql(u8, statuses[0].phase, "cutover_prepared")) break;
    }
    try std.testing.expect(rounds < 64);

    try execPsqlCommand(alloc, dsn, insert_sql);

    rounds = 0;
    while (rounds < 64) : (rounds += 1) {
        server.svc.cdc_next_round_at_ms = 0;
        try server.runRound();

        var check_db = try db_mod.DB.open(alloc, db_path, .{
            .open_mode = .query_readonly,
        });
        defer check_db.close();
        if (try check_db.lookup(alloc, "user-2", .{})) |found| {
            var user_two = found;
            defer user_two.deinit(alloc);
            try std.testing.expect(std.mem.indexOf(u8, user_two.json, "\"Bob\"") != null);
            try std.testing.expect(std.mem.indexOf(u8, user_two.json, "\"silver\"") != null);
            return;
        }
    }
    return error.TestExpectedEqual;
}

fn testPsqlBin() ?[]const u8 {
    if (std.c.getenv("ANTFLY_TEST_PSQL_BIN")) |value_z| return std.mem.span(value_z);
    return "/opt/homebrew/opt/postgresql@18/bin/psql";
}

fn testPgDsnAlloc(alloc: Allocator) ![]u8 {
    if (std.c.getenv("ANTFLY_TEST_PG_DSN")) |value_z| {
        return try alloc.dupe(u8, std.mem.span(value_z));
    }
    if (std.c.getenv("PG_DSN")) |value_z| {
        return try alloc.dupe(u8, std.mem.span(value_z));
    }
    return try alloc.dupe(u8, "postgres://localhost:5432/postgres?sslmode=disable");
}

fn execPsqlCommand(alloc: Allocator, dsn: []const u8, sql_text: []const u8) !void {
    const psql_bin = testPsqlBin() orelse return error.FileNotFound;
    const result = try std.process.run(alloc, std.testing.io, .{
        .argv = &.{ psql_bin, dsn, "-v", "ON_ERROR_STOP=1", "-c", sql_text },
    });
    defer alloc.free(result.stdout);
    defer alloc.free(result.stderr);
    switch (result.term) {
        .exited => |code| if (code != 0) return error.ChildProcessFailure,
        else => return error.ChildProcessFailure,
    }
}

fn execPsqlScalarAlloc(alloc: Allocator, dsn: []const u8, sql_text: []const u8) ![]u8 {
    const psql_bin = testPsqlBin() orelse return error.FileNotFound;
    const result = try std.process.run(alloc, std.testing.io, .{
        .argv = &.{ psql_bin, dsn, "-tAc", sql_text },
    });
    defer alloc.free(result.stderr);
    switch (result.term) {
        .exited => |code| {
            if (code != 0) {
                alloc.free(result.stdout);
                return error.ChildProcessFailure;
            }
        },
        else => {
            alloc.free(result.stdout);
            return error.ChildProcessFailure;
        },
    }
    return result.stdout;
}
