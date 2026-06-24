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

const ha_replication = @import("ha_replication.zig");
const docstore_mod = @import("../docstore.zig");
const metadata_table_manager = @import("../../metadata/table_manager.zig");
const relational_store_mod = @import("relational_store.zig");
const schema_api_mod = @import("../../schema/mod.zig");
const schema_mod = @import("../schema.zig");

const Allocator = std.mem.Allocator;

pub const local_schema_json_key = "\x00\x00__metadata__:schema_json";
pub const local_lite_sql_table_record_json_key = "\x00\x00__metadata__:lite_sql_table_record_json";

pub const ApplyTableSchemaOptions = struct {
    persist_local_schema_json: bool = true,
    reload_algebraic_schema_configs: bool = true,
};

pub const SchemaRewriteJobExecutionResult = struct {
    report: relational_store_mod.RowRewriteReport,
    progress_row_key: []u8,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.progress_row_key);
        self.* = undefined;
    }

    pub fn finishRequest(self: @This(), job: metadata_table_manager.SchemaRewriteJobRecord) metadata_table_manager.SchemaRewriteJobFinishRequest {
        return .{
            .job_id = job.job_id,
            .lease_owner = job.lease_owner,
            .completed_row_count = self.report.scanned_rows,
            .progress_row_key = self.progress_row_key,
        };
    }
};

pub const SchemaRewriteJobDrainOptions = struct {
    worker_id: []const u8 = "antfly-schema-rewrite-worker",
    group_id: ?u64 = null,
    now_ms: ?u64 = null,
    lease_ttl_ms: u64 = 60_000,
    max_jobs: usize = 1,
};

pub fn Impl(comptime DB: type) type {
    return struct {
        const Self = @This();

        pub fn setSchema(self: *DB, table_schema: schema_mod.TableSchema) !void {
            try ha_replication.enforceWriteGateOptional(self.ha_write_gate);
            try Self.preflightMetadataSyncCommit(self);
            try Self.validateRuntimeSchemaFeatureLevel(table_schema);
            try self.core.setSchema(table_schema);
            Self.refreshRuntimeSideEffects(self);
            try Self.mirrorSchemaMetadataCommit(self, table_schema);
        }

        /// Apply table metadata schema JSON to the DB runtime and all schema-derived
        /// local artifacts. This is the single production entry point for table
        /// schema application so write-cache reconciliation, metadata provisioning,
        /// and crash recovery keep algebraic sidecars in the same lifecycle state.
        pub fn applyTableSchemaJson(
            self: *DB,
            alloc: Allocator,
            schema_json: []const u8,
            options: ApplyTableSchemaOptions,
        ) !void {
            if (schema_json.len == 0) return;
            if (self.open_mode == .query_readonly or self.open_mode == .status_only) return error.ReadOnly;
            try ha_replication.enforceWriteGateOptional(self.ha_write_gate);
            try Self.preflightMetadataSyncCommit(self);

            var parsed_schema = try schema_api_mod.parseValidatedTableSchema(alloc, schema_json);
            defer parsed_schema.deinit(alloc);

            const runtime_schema = try schema_api_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
            defer schema_mod.freeSchema(alloc, runtime_schema);

            self.core.lockApply();
            defer self.core.unlockApply();

            try self.schemaRuntimeValidateTableSchemaCompatibilityLocked(alloc, runtime_schema);
            if (options.reload_algebraic_schema_configs) {
                try Self.stageAlgebraicSchemaConfigsPending(self, schema_json);
            }
            try self.schemaRuntimeMigrateRelationalRowsForSchemaTransitionLocked(alloc, runtime_schema);
            try self.schemaRuntimeMigrateRelationalConstraintsForSchemaTransitionLocked(alloc, runtime_schema);
            if (options.persist_local_schema_json) {
                try Self.setSchemaWithLocalSchemaJson(self, runtime_schema, schema_json);
            } else {
                try Self.setSchema(self, runtime_schema);
            }
            if (options.reload_algebraic_schema_configs) {
                try Self.completePendingAlgebraicSchemaRebuilds(self);
            }
        }

        /// Refresh schema-derived algebraic index configs for callers that have
        /// already applied the runtime schema. This remains a structural mutation:
        /// config swaps, sidecar clears, and rebuild replay are serialized with
        /// normal apply work just like `applyTableSchemaJson`.
        pub fn reloadAlgebraicSchemaConfigs(self: *DB, schema_json: []const u8) !void {
            if (schema_json.len == 0) return;
            if (self.open_mode == .query_readonly or self.open_mode == .status_only) return error.ReadOnly;

            self.core.lockApply();
            defer self.core.unlockApply();

            try self.core.index_manager.reloadAlgebraicSchemaConfigs(self.core.store, schema_json);
        }

        pub fn stageAlgebraicSchemaConfigsPending(self: *DB, schema_json: []const u8) !void {
            try self.core.index_manager.stageAlgebraicSchemaConfigsPending(self.core.store, schema_json);
        }

        pub fn completePendingAlgebraicSchemaRebuilds(self: *DB) !void {
            try self.core.index_manager.completePendingAlgebraicSchemaRebuilds(self.core.store);
        }

        pub fn setSchemaWithLocalSchemaJson(self: *DB, table_schema: schema_mod.TableSchema, schema_json: []const u8) !void {
            try Self.validateRuntimeSchemaFeatureLevel(table_schema);
            const metadata_puts = [_]schema_mod.SchemaMetadataPut{.{
                .key = local_schema_json_key,
                .value = schema_json,
            }};
            try self.core.setSchemaWithMetadata(table_schema, metadata_puts[0..]);
            Self.refreshRuntimeSideEffects(self);
            try Self.mirrorSchemaMetadataCommit(self, table_schema);
        }

        pub fn setSchemaWithLocalLiteSqlTableRecordJson(
            self: *DB,
            table_schema: schema_mod.TableSchema,
            schema_json: []const u8,
            table_record_json: []const u8,
        ) !void {
            try Self.validateRuntimeSchemaFeatureLevel(table_schema);
            const metadata_puts = [_]schema_mod.SchemaMetadataPut{
                .{
                    .key = local_schema_json_key,
                    .value = schema_json,
                },
                .{
                    .key = local_lite_sql_table_record_json_key,
                    .value = table_record_json,
                },
            };
            try self.core.setSchemaWithMetadata(table_schema, metadata_puts[0..]);
            Self.refreshRuntimeSideEffects(self);
            try Self.mirrorSchemaMetadataCommit(self, table_schema);
        }

        pub fn setSchemaJson(self: *DB, alloc: Allocator, schema_json: []const u8) !void {
            try self.applyTableSchemaJson(alloc, schema_json, .{});
        }

        pub fn getSchemaJson(self: *DB, alloc: Allocator) !?[]u8 {
            return try self.core.getStoreValue(alloc, local_schema_json_key);
        }

        pub fn applyLiteSqlTableRecord(self: *DB, alloc: Allocator, table: metadata_table_manager.TableRecord) !void {
            if (table.schema_json.len == 0) return error.InvalidSchemaUpdateRequest;
            if (self.open_mode == .query_readonly or self.open_mode == .status_only) return error.ReadOnly;
            try ha_replication.enforceWriteGateOptional(self.ha_write_gate);
            try Self.preflightMetadataSyncCommit(self);

            const table_record_json = try std.json.Stringify.valueAlloc(alloc, table, .{});
            defer alloc.free(table_record_json);

            var parsed_schema = try schema_api_mod.parseValidatedTableSchema(alloc, table.schema_json);
            defer parsed_schema.deinit(alloc);

            const runtime_schema = try schema_api_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
            defer schema_mod.freeSchema(alloc, runtime_schema);

            self.core.lockApply();
            defer self.core.unlockApply();

            try self.schemaRuntimeValidateTableSchemaCompatibilityLocked(alloc, runtime_schema);
            try Self.stageAlgebraicSchemaConfigsPending(self, table.schema_json);
            try self.schemaRuntimeMigrateRelationalRowsForSchemaTransitionLocked(alloc, runtime_schema);
            try self.schemaRuntimeMigrateRelationalConstraintsForSchemaTransitionLocked(alloc, runtime_schema);
            try Self.setSchemaWithLocalLiteSqlTableRecordJson(self, runtime_schema, table.schema_json, table_record_json);
            try Self.completePendingAlgebraicSchemaRebuilds(self);
        }

        pub fn getLiteSqlTableRecordAlloc(self: *DB, alloc: Allocator) !?metadata_table_manager.TableRecord {
            const raw = (try self.core.getStoreValue(alloc, local_lite_sql_table_record_json_key)) orelse return null;
            defer alloc.free(raw);
            var parsed = try std.json.parseFromSlice(metadata_table_manager.TableRecord, alloc, raw, .{ .allocate = .alloc_always });
            defer parsed.deinit();
            return try metadata_table_manager.cloneTable(alloc, parsed.value);
        }

        pub fn drainSchemaRewriteJobsForIdle(
            self: *DB,
            alloc: Allocator,
            service: anytype,
            options: SchemaRewriteJobDrainOptions,
        ) !usize {
            if (options.max_jobs == 0) return 0;
            const ServiceType = @TypeOf(service);
            const ServiceDeclType = switch (@typeInfo(ServiceType)) {
                .pointer => |pointer| pointer.child,
                else => ServiceType,
            };
            if (comptime !@hasDecl(ServiceDeclType, "listProjectedSchemaRewriteJobs") or
                !@hasDecl(ServiceDeclType, "beginSchemaRewriteJob") or
                !@hasDecl(ServiceDeclType, "finishSchemaRewriteJob") or
                !@hasDecl(ServiceDeclType, "invalidateSchemaRewriteJob"))
            {
                return error.UnsupportedOperation;
            }

            const now_ms = options.now_ms orelse @divTrunc(self.schemaRuntimeCurrentTimeNs(), std.time.ns_per_ms);
            const lease_expires_at_ms = now_ms +| options.lease_ttl_ms;
            if (options.worker_id.len == 0 or lease_expires_at_ms <= now_ms) return error.InvalidSchemaRewriteJobLease;

            const jobs = try service.listProjectedSchemaRewriteJobs(alloc);
            defer if (comptime @hasDecl(ServiceDeclType, "freeProjectedSchemaRewriteJobs")) {
                service.freeProjectedSchemaRewriteJobs(alloc, jobs);
            } else {
                for (jobs) |job| metadata_table_manager.freeSchemaRewriteJob(alloc, job);
                alloc.free(jobs);
            };

            var progressed: usize = 0;
            for (jobs) |job| {
                if (progressed >= options.max_jobs) break;
                if (options.group_id) |group_id| {
                    if (job.group_id != group_id) continue;
                }
                if (!schemaRewriteJobClaimableForDrain(job, now_ms)) continue;

                service.beginSchemaRewriteJob(.{
                    .job_id = job.job_id,
                    .lease_owner = options.worker_id,
                    .now_ms = now_ms,
                    .lease_expires_at_ms = lease_expires_at_ms,
                }) catch |err| switch (err) {
                    error.SchemaRewriteJobClaimBusy,
                    error.SchemaRewriteJobNotDeclared,
                    error.UnknownSchemaRewriteJob,
                    => continue,
                    else => return err,
                };

                var running_job = job;
                running_job.state = metadata_table_manager.schema_rewrite_running;
                running_job.lease_owner = options.worker_id;
                running_job.lease_expires_at_ms = lease_expires_at_ms;

                var result = Self.executeClaimedSchemaRewriteJob(self, alloc, running_job) catch |err| {
                    try service.invalidateSchemaRewriteJob(.{
                        .job_id = running_job.job_id,
                        .lease_owner = options.worker_id,
                        .last_error = @errorName(err),
                    });
                    progressed += 1;
                    continue;
                };
                defer result.deinit(alloc);
                try service.finishSchemaRewriteJob(result.finishRequest(running_job));
                progressed += 1;
            }
            return progressed;
        }

        pub fn executeClaimedSchemaRewriteJob(
            self: *DB,
            alloc: Allocator,
            job: metadata_table_manager.SchemaRewriteJobRecord,
        ) !SchemaRewriteJobExecutionResult {
            if (self.open_mode == .query_readonly or self.open_mode == .status_only) return error.ReadOnly;
            const validate_constraints = std.mem.eql(u8, job.action, "validate") and std.mem.eql(u8, job.reason, "constraints");
            const rewrite_rows = std.mem.eql(u8, job.action, "rewrite") and std.mem.eql(u8, job.reason, "row_images");
            if (!validate_constraints and !rewrite_rows) return error.InvalidSchemaRewriteJob;
            if (!std.mem.eql(u8, job.state, metadata_table_manager.schema_rewrite_running)) return error.SchemaRewriteJobNotRunning;
            if (job.lease_owner.len == 0) return error.SchemaRewriteJobLeaseMismatch;
            const has_expression_rewrite = job.expression != null or job.target_column.len != 0;
            const has_row_rewrite = job.rewrite_renames.len != 0 or job.rewrite_drops.len != 0;
            const has_full_row_rewrite = job.full_row_rewrite;
            if (validate_constraints) {
                if (has_expression_rewrite or has_row_rewrite or has_full_row_rewrite) return error.InvalidSchemaRewriteJob;
            } else if (has_expression_rewrite) {
                if (job.target_column.len == 0 or job.expression == null) return error.InvalidSchemaRewriteExpression;
                if (has_row_rewrite or has_full_row_rewrite) return error.InvalidSchemaRewriteExpression;
            } else if (has_row_rewrite and has_full_row_rewrite) {
                return error.InvalidSchemaRewriteExpression;
            } else if (!has_row_rewrite and !has_full_row_rewrite) {
                return error.InvalidSchemaRewriteExpression;
            }
            const local_range = self.getRange();
            if (!std.mem.eql(u8, job.start_row_key, local_range.start) or !optionalStringsEqual(job.end_row_key, if (local_range.end.len == 0) null else local_range.end)) {
                return error.SchemaRewriteJobRangeMismatch;
            }

            self.core.lockApply();
            defer self.core.unlockApply();

            const local_schema_json = self.core.store.get(alloc, local_schema_json_key) catch |err| switch (err) {
                error.NotFound => return error.InvalidSchemaRewriteGeneration,
                else => return err,
            };
            defer alloc.free(local_schema_json);
            const current_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(local_schema_json);
            if (job.schema_generation != current_generation) return error.InvalidSchemaRewriteGeneration;

            const runtime_schema = self.core.schema orelse return error.InvalidSchemaUpdateRequest;
            if (runtime_schema.storage_mode != .relational or runtime_schema.relational_columns.len == 0) return error.InvalidSchemaUpdateRequest;
            const column_index_policy = relational_store_mod.ColumnIndexPolicy.fromColumns(runtime_schema.relational_columns);

            if (validate_constraints) {
                const report = try self.schemaRuntimeValidateRelationalSchemaConstraintsForJobLocked(alloc, runtime_schema, local_range.start, local_range.end);
                const progress_row_key = try alloc.dupe(u8, local_range.end);
                return .{ .report = report, .progress_row_key = progress_row_key };
            }

            if (has_expression_rewrite) {
                const expression = job.expression.?;
                const target_column = relationalRowsFindColumn(runtime_schema.relational_columns, job.target_column) orelse return error.InvalidSchemaRewriteExpression;
                try self.schemaRuntimeValidateRelationalRowsExpressionAgainstSchema(runtime_schema, expression);

                const rows = try relational_store_mod.scanRowsAlloc(alloc, self.core.store, local_range.start, local_range.end);
                defer relational_store_mod.freeRows(alloc, rows);

                var report: relational_store_mod.RowRewriteReport = .{ .scanned_rows = @intCast(rows.len) };
                var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
                defer writes.deinit(alloc);
                var deletes = std.ArrayListUnmanaged([]const u8).empty;
                defer deletes.deinit(alloc);
                var owned_keys = std.ArrayListUnmanaged([]u8).empty;
                defer {
                    for (owned_keys.items) |key| alloc.free(key);
                    owned_keys.deinit(alloc);
                }
                var owned_values = std.ArrayListUnmanaged([]u8).empty;
                defer {
                    for (owned_values.items) |value| alloc.free(value);
                    owned_values.deinit(alloc);
                }

                for (rows) |row| {
                    const rewritten = try self.schemaRuntimeSchemaRewriteRowValueAlloc(alloc, row.row_value, target_column, expression);
                    if (rewritten) |new_row| {
                        var new_row_owned = true;
                        errdefer if (new_row_owned) alloc.free(new_row);
                        try relational_store_mod.appendUpsertWithColumnIndexPolicy(
                            alloc,
                            self.core.store,
                            &writes,
                            &deletes,
                            &owned_keys,
                            &owned_values,
                            row.doc_key,
                            new_row,
                            column_index_policy,
                        );
                        try owned_values.append(alloc, new_row);
                        new_row_owned = false;
                        report.rewritten_rows += 1;
                    } else {
                        report.unchanged_rows += 1;
                    }
                }

                if (writes.items.len > 0 or deletes.items.len > 0) try self.core.store.putBatch(writes.items, deletes.items);
                const progress_row_key = try alloc.dupe(u8, local_range.end);
                return .{ .report = report, .progress_row_key = progress_row_key };
            }

            if (has_full_row_rewrite) {
                const rows = try relational_store_mod.scanRowsAlloc(alloc, self.core.store, local_range.start, local_range.end);
                defer relational_store_mod.freeRows(alloc, rows);

                var report: relational_store_mod.RowRewriteReport = .{ .scanned_rows = @intCast(rows.len) };
                var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
                defer writes.deinit(alloc);
                var deletes = std.ArrayListUnmanaged([]const u8).empty;
                defer deletes.deinit(alloc);
                var owned_keys = std.ArrayListUnmanaged([]u8).empty;
                defer {
                    for (owned_keys.items) |key| alloc.free(key);
                    owned_keys.deinit(alloc);
                }
                var owned_values = std.ArrayListUnmanaged([]u8).empty;
                defer {
                    for (owned_values.items) |value| alloc.free(value);
                    owned_values.deinit(alloc);
                }

                for (rows) |row| {
                    const new_row = try alloc.dupe(u8, row.row_value);
                    var new_row_owned = true;
                    errdefer if (new_row_owned) alloc.free(new_row);
                    try relational_store_mod.appendUpsertWithColumnIndexPolicy(
                        alloc,
                        self.core.store,
                        &writes,
                        &deletes,
                        &owned_keys,
                        &owned_values,
                        row.doc_key,
                        new_row,
                        column_index_policy,
                    );
                    try owned_values.append(alloc, new_row);
                    new_row_owned = false;
                    report.rewritten_rows += 1;
                }

                if (writes.items.len > 0 or deletes.items.len > 0) try self.core.store.putBatch(writes.items, deletes.items);
                const progress_row_key = try alloc.dupe(u8, local_range.end);
                return .{ .report = report, .progress_row_key = progress_row_key };
            }

            const renames = try alloc.alloc(relational_store_mod.RowRewriteRename, job.rewrite_renames.len);
            defer alloc.free(renames);
            for (job.rewrite_renames, 0..) |rename, i| {
                renames[i] = .{ .old_path = rename.old_path, .new_path = rename.new_path };
            }
            const plan = relational_store_mod.RowRewritePlan{
                .renames = renames,
                .drops = job.rewrite_drops,
            };
            const report = try relational_store_mod.rewriteRowsInRangeWithColumnIndexPolicy(
                alloc,
                self.core.store,
                plan,
                local_range.start,
                local_range.end,
                column_index_policy,
            );
            const progress_row_key = try alloc.dupe(u8, local_range.end);
            return .{ .report = report, .progress_row_key = progress_row_key };
        }

        pub fn validateRuntimeSchemaFeatureLevel(table_schema: schema_mod.TableSchema) !void {
            if (table_schema.storage_mode != .relational) return;
            for (table_schema.foreign_keys) |foreign_key| {
                if (foreign_key.validation_state == .validating or foreign_key.validation_state == .invalid) return error.InvalidSchemaUpdateRequest;
            }
        }

        pub fn refreshRuntimeSideEffects(self: *DB) void {
            const maybe_relational_columns = self.schemaRuntimeRelationalColumnsForStore();
            const relational_columns = maybe_relational_columns orelse &.{};
            const relational_base_rows = maybe_relational_columns != null;
            self.async_context.relational_base_rows = relational_base_rows;
            if (self.ttl_cleanup_context) |ctx| ctx.batch.relational_base_rows = relational_base_rows;
            if (self.enrichment_runtime) |runtime| runtime.setRelationalBaseRows(relational_base_rows);
            if (self.transaction_recovery_identity_context) |ctx| {
                ctx.relational_base_rows = relational_base_rows;
                ctx.relational_columns = relational_columns;
            }
        }

        fn preflightMetadataSyncCommit(self: *DB) !void {
            const resources = self.core.batchExecutionResources();
            try ha_replication.preflightMirrorSyncCommit(resources.log_mutex, self.ha_async_metadata_mirror);
        }

        fn mirrorSchemaMetadataCommit(self: *DB, table_schema: schema_mod.TableSchema) !void {
            const resources = self.core.batchExecutionResources();
            try ha_replication.mirrorSchemaMetadataCommit(self.alloc, resources.log_mutex, self.ha_async_metadata_mirror, table_schema);
        }
    };
}

fn schemaRewriteJobClaimableForDrain(job: metadata_table_manager.SchemaRewriteJobRecord, now_ms: u64) bool {
    if (std.mem.eql(u8, job.state, metadata_table_manager.schema_rewrite_declared)) return true;
    return std.mem.eql(u8, job.state, metadata_table_manager.schema_rewrite_running) and
        job.lease_expires_at_ms != 0 and
        job.lease_expires_at_ms <= now_ms;
}

fn relationalRowsFindColumn(columns: []const schema_mod.RelationalColumn, name: []const u8) ?schema_mod.RelationalColumn {
    for (columns) |column| {
        if (std.mem.eql(u8, column.path, name) or std.mem.eql(u8, column.name, name)) return column;
    }
    return null;
}

fn optionalStringsEqual(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    return std.mem.eql(u8, a.?, b.?);
}
