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
const internal_keys = @import("../internal_keys.zig");
const mapper = @import("document_mapper.zig");
const metadata_table_manager = @import("../../metadata/table_manager.zig");
const relational_row_codec = @import("algebraic/relational_row_codec.zig");
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

            try Self.validateTableSchemaCompatibilityLocked(self, alloc, runtime_schema);
            if (options.reload_algebraic_schema_configs) {
                try Self.stageAlgebraicSchemaConfigsPending(self, schema_json);
            }
            try Self.migrateRelationalRowsForSchemaTransitionLocked(self, alloc, runtime_schema);
            try Self.migrateRelationalConstraintsForSchemaTransitionLocked(self, alloc, runtime_schema);
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

            try Self.validateTableSchemaCompatibilityLocked(self, alloc, runtime_schema);
            try Self.stageAlgebraicSchemaConfigsPending(self, table.schema_json);
            try Self.migrateRelationalRowsForSchemaTransitionLocked(self, alloc, runtime_schema);
            try Self.migrateRelationalConstraintsForSchemaTransitionLocked(self, alloc, runtime_schema);
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

        pub fn validateTableSchemaCompatibilityLocked(self: *DB, alloc: Allocator, next_schema: schema_mod.TableSchema) !void {
            if (self.core.schema) |current_schema| {
                return validateRuntimeTableSchemaTransition(current_schema, next_schema);
            }

            const durable_schema = try schema_mod.loadSchema(self.core.store, alloc);
            defer if (durable_schema) |loaded| schema_mod.freeSchema(alloc, loaded);
            if (durable_schema) |current_schema| {
                return validateRuntimeTableSchemaTransition(current_schema, next_schema);
            }

            try Self.validateFirstTableSchemaApplyAgainstExistingRows(self, alloc, next_schema);
        }

        pub fn migrateRelationalRowsForSchemaTransitionLocked(self: *DB, alloc: Allocator, next_schema: schema_mod.TableSchema) !void {
            if (self.core.schema) |current_schema| {
                return try Self.migrateRelationalRowsFromCurrentLocked(self, alloc, current_schema, next_schema);
            }

            const durable_schema = try schema_mod.loadSchema(self.core.store, alloc);
            defer if (durable_schema) |loaded| schema_mod.freeSchema(alloc, loaded);
            if (durable_schema) |current_schema| {
                return try Self.migrateRelationalRowsFromCurrentLocked(self, alloc, current_schema, next_schema);
            }
        }

        pub fn migrateRelationalConstraintsForSchemaTransitionLocked(self: *DB, alloc: Allocator, next_schema: schema_mod.TableSchema) !void {
            if (self.core.schema) |current_schema| {
                return try Self.migrateRelationalConstraintsFromCurrentLocked(self, alloc, current_schema, next_schema);
            }

            const durable_schema = try schema_mod.loadSchema(self.core.store, alloc);
            defer if (durable_schema) |loaded| schema_mod.freeSchema(alloc, loaded);
            if (durable_schema) |current_schema| {
                return try Self.migrateRelationalConstraintsFromCurrentLocked(self, alloc, current_schema, next_schema);
            }
        }

        fn migrateRelationalRowsFromCurrentLocked(
            self: *DB,
            alloc: Allocator,
            current_schema: schema_mod.TableSchema,
            next_schema: schema_mod.TableSchema,
        ) !void {
            if (current_schema.storage_mode != .relational or next_schema.storage_mode != .relational) return;

            var sets = std.ArrayListUnmanaged(relational_store_mod.RowRewriteSet).empty;
            defer sets.deinit(alloc);
            var owned_rows = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_rows.items) |row| alloc.free(row);
                owned_rows.deinit(alloc);
            }
            var decoded_rows = std.ArrayListUnmanaged(relational_row_codec.Row).empty;
            defer {
                for (decoded_rows.items) |*row| row.deinit(alloc);
                decoded_rows.deinit(alloc);
            }

            var generated_columns = std.ArrayListUnmanaged(schema_mod.RelationalColumn).empty;
            defer generated_columns.deinit(alloc);

            for (next_schema.relational_columns) |column| {
                if (relationalColumnPathExists(current_schema.relational_columns, column.path)) continue;
                if (try relationalColumnHasUniqueDroppedRenameSource(current_schema.relational_columns, next_schema.relational_columns, column)) continue;
                if (column.generated != null) {
                    try generated_columns.append(alloc, column);
                    continue;
                }
                const default_value = column.default_value orelse continue;
                if (default_value.kind != .literal) return error.InvalidSchemaUpdateRequest;
                if (column.nullable and jsonLiteralIsNull(default_value.value_json)) continue;
                const row_value = try relationalDefaultColumnRowValueAlloc(alloc, column, default_value.value_json);
                errdefer alloc.free(row_value);
                try owned_rows.append(alloc, row_value);
                var decoded = try relational_row_codec.deserialize(alloc, row_value);
                errdefer decoded.deinit(alloc);
                if (decoded.cells.len != 1) return error.InvalidSchemaUpdateRequest;
                try decoded_rows.append(alloc, decoded);
                const stable_decoded = &decoded_rows.items[decoded_rows.items.len - 1];
                try sets.append(alloc, .{
                    .cell = stable_decoded.cells[0],
                    .only_if_missing = true,
                });
            }

            const column_index_policy = relational_store_mod.ColumnIndexPolicy.fromColumns(next_schema.relational_columns);
            if (sets.items.len != 0) {
                _ = try relational_store_mod.rewriteRowsInRangeWithColumnIndexPolicy(
                    alloc,
                    self.core.store,
                    .{ .sets = sets.items },
                    self.getRange().start,
                    self.getRange().end,
                    column_index_policy,
                );
            }
            if (generated_columns.items.len != 0) {
                try Self.backfillRelationalGeneratedColumnsLocked(self, alloc, generated_columns.items, column_index_policy);
            }
        }

        fn backfillRelationalGeneratedColumnsLocked(
            self: *DB,
            alloc: Allocator,
            generated_columns: []const schema_mod.RelationalColumn,
            column_index_policy: relational_store_mod.ColumnIndexPolicy,
        ) !void {
            const rows = try relational_store_mod.scanRowsAlloc(alloc, self.core.store, self.getRange().start, self.getRange().end);
            defer relational_store_mod.freeRows(alloc, rows);

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
                const row_json = mapper.materializeRelationalRowValueAlloc(alloc, row.row_value) catch return error.InvalidRowsRequest;
                defer alloc.free(row_json);
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, row_json, .{}) catch return error.InvalidRowsRequest;
                defer parsed.deinit();
                if (parsed.value != .object) return error.InvalidRowsRequest;

                var row_sets = std.ArrayListUnmanaged(relational_store_mod.RowRewriteSet).empty;
                defer row_sets.deinit(alloc);
                var row_owned_values = std.ArrayListUnmanaged([]u8).empty;
                defer {
                    for (row_owned_values.items) |value| alloc.free(value);
                    row_owned_values.deinit(alloc);
                }
                var row_decoded = std.ArrayListUnmanaged(relational_row_codec.Row).empty;
                defer {
                    for (row_decoded.items) |*decoded| decoded.deinit(alloc);
                    row_decoded.deinit(alloc);
                }

                for (generated_columns) |column| {
                    const generated = column.generated orelse continue;
                    const value_json = self.schemaRuntimeRelationalRowsGeneratedColumnValueJsonAlloc(alloc, parsed.value, generated) catch return error.InvalidRowsRequest;
                    defer alloc.free(value_json);
                    const generated_row = try relationalDefaultColumnRowValueAlloc(alloc, column, value_json);
                    errdefer alloc.free(generated_row);
                    try row_owned_values.append(alloc, generated_row);
                    var decoded = try relational_row_codec.deserialize(alloc, generated_row);
                    errdefer decoded.deinit(alloc);
                    if (decoded.cells.len != 1) return error.InvalidSchemaUpdateRequest;
                    try row_decoded.append(alloc, decoded);
                    const stable_decoded = &row_decoded.items[row_decoded.items.len - 1];
                    try row_sets.append(alloc, .{
                        .cell = stable_decoded.cells[0],
                        .only_if_missing = true,
                    });
                }

                const rewritten = try relational_store_mod.rewriteRowValueWithPlanAlloc(alloc, row.row_value, .{ .sets = row_sets.items });
                if (rewritten) |new_row| {
                    var new_row_owned = true;
                    errdefer if (new_row_owned) alloc.free(new_row);
                    try relational_store_mod.appendUpsertWithColumnIndexPolicy(alloc, self.core.store, &writes, &deletes, &owned_keys, &owned_values, row.doc_key, new_row, column_index_policy);
                    try owned_values.append(alloc, new_row);
                    new_row_owned = false;
                }
            }

            if (writes.items.len > 0 or deletes.items.len > 0) try self.core.store.putBatch(writes.items, deletes.items);
        }

        fn migrateRelationalConstraintsFromCurrentLocked(
            self: *DB,
            alloc: Allocator,
            current_schema: schema_mod.TableSchema,
            next_schema: schema_mod.TableSchema,
        ) !void {
            if (current_schema.storage_mode != .relational or next_schema.storage_mode != .relational) return;

            var unique_to_build = std.ArrayListUnmanaged(schema_mod.UniqueConstraint).empty;
            defer unique_to_build.deinit(alloc);
            var unique_to_delete = std.ArrayListUnmanaged(schema_mod.UniqueConstraint).empty;
            defer unique_to_delete.deinit(alloc);
            var foreign_keys_to_build = std.ArrayListUnmanaged(schema_mod.ForeignKey).empty;
            defer foreign_keys_to_build.deinit(alloc);
            var foreign_keys_to_delete = std.ArrayListUnmanaged(schema_mod.ForeignKey).empty;
            defer foreign_keys_to_delete.deinit(alloc);
            var checks_to_validate = std.ArrayListUnmanaged(schema_mod.RelationalCheck).empty;
            defer checks_to_validate.deinit(alloc);

            for (next_schema.unique_constraints) |constraint| {
                if (findUniqueConstraintByName(current_schema.unique_constraints, constraint.name) == null) {
                    try unique_to_build.append(alloc, constraint);
                }
            }
            for (current_schema.unique_constraints) |constraint| {
                if (findUniqueConstraintByName(next_schema.unique_constraints, constraint.name) == null) {
                    try unique_to_delete.append(alloc, constraint);
                }
            }
            for (next_schema.foreign_keys) |foreign_key| {
                if (findForeignKeyByName(current_schema.foreign_keys, foreign_key.name)) |current_foreign_key| {
                    if (current_foreign_key.validation_state != .enforced and foreign_key.validation_state == .enforced) {
                        try foreign_keys_to_build.append(alloc, foreign_key);
                    }
                } else if (foreign_key.validation_state == .enforced) {
                    try foreign_keys_to_build.append(alloc, foreign_key);
                }
            }
            for (current_schema.foreign_keys) |foreign_key| {
                if (findForeignKeyByName(next_schema.foreign_keys, foreign_key.name) == null) {
                    try foreign_keys_to_delete.append(alloc, foreign_key);
                }
            }
            for (next_schema.checks) |check| {
                if (findRelationalCheckByName(current_schema.checks, check.name)) |current_check| {
                    if (current_check.validation_state != .enforced and check.validation_state == .enforced) {
                        try checks_to_validate.append(alloc, check);
                    }
                } else if (check.validation_state == .enforced) {
                    try checks_to_validate.append(alloc, check);
                }
            }

            if (unique_to_build.items.len > 0) {
                try relational_store_mod.rebuildUniqueConstraintRowsInRange(
                    alloc,
                    self.core.store,
                    next_schema.relational_columns,
                    next_schema.periods,
                    unique_to_build.items,
                    self.getRange().start,
                    self.getRange().end,
                );
            }
            if (foreign_keys_to_build.items.len > 0) {
                const validate_report = try relational_store_mod.reconcileForeignKeyRefsInRangeWithPrimaryKey(
                    alloc,
                    self.core.store,
                    next_schema.default_type,
                    next_schema.relational_columns,
                    next_schema.periods,
                    foreign_keys_to_build.items,
                    next_schema.primary_key,
                    next_schema.unique_constraints,
                    self.getRange().start,
                    self.getRange().end,
                    .validate,
                );
                try self.schemaRuntimeRecordForeignKeyIntegrityProgressLocked(alloc, .validate, null, self.getRange().start, self.getRange().end, validate_report);
                if (validate_report.missing_parent_rows != 0) return error.ForeignKeyViolation;

                const repair_report = try relational_store_mod.reconcileForeignKeyRefsInRangeWithPrimaryKey(
                    alloc,
                    self.core.store,
                    next_schema.default_type,
                    next_schema.relational_columns,
                    next_schema.periods,
                    foreign_keys_to_build.items,
                    next_schema.primary_key,
                    next_schema.unique_constraints,
                    self.getRange().start,
                    self.getRange().end,
                    .repair,
                );
                try self.schemaRuntimeRecordForeignKeyIntegrityProgressLocked(alloc, .repair, null, self.getRange().start, self.getRange().end, repair_report);
                if (repair_report.missing_parent_rows != 0) return error.ForeignKeyViolation;
            }
            if (checks_to_validate.items.len > 0) {
                try self.schemaRuntimeValidateRelationalChecksInRangeLocked(
                    alloc,
                    checks_to_validate.items,
                    self.getRange().start,
                    self.getRange().end,
                );
            }
            if (foreign_keys_to_delete.items.len > 0) {
                try relational_store_mod.deleteForeignKeyRefRows(alloc, self.core.store, foreign_keys_to_delete.items);
            }
            if (unique_to_delete.items.len > 0) {
                try relational_store_mod.deleteUniqueConstraintRows(alloc, self.core.store, unique_to_delete.items);
            }
        }

        fn schemaRewriteRowValueAlloc(
            self: *DB,
            alloc: Allocator,
            row_value: []const u8,
            target_column: schema_mod.RelationalColumn,
            expression: schema_mod.RelationalRowsExpression,
        ) !?[]u8 {
            const row_json = mapper.materializeRelationalRowValueAlloc(alloc, row_value) catch return error.InvalidRowsRequest;
            defer alloc.free(row_json);
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, row_json, .{}) catch return error.InvalidRowsRequest;
            defer parsed.deinit();
            if (parsed.value != .object) return error.InvalidRowsRequest;

            const value_json = self.schemaRuntimeRelationalRowsExpressionValueJsonAlloc(alloc, parsed.value, expression) catch return error.InvalidRowsRequest;
            defer alloc.free(value_json);
            var parsed_value = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidRowsRequest;
            defer parsed_value.deinit();

            if (parsed_value.value == .null) {
                if (!target_column.nullable) return error.InvalidRowsRequest;
                return try relational_store_mod.rewriteRowValueWithPlanAlloc(alloc, row_value, .{ .drops = &.{target_column.path} });
            }

            const target_row = try relationalDefaultColumnRowValueAlloc(alloc, target_column, value_json);
            defer alloc.free(target_row);
            var decoded = try relational_row_codec.deserialize(alloc, target_row);
            defer decoded.deinit(alloc);
            if (decoded.cells.len != 1) return error.InvalidRowsRequest;
            return try relational_store_mod.rewriteRowValueWithPlanAlloc(alloc, row_value, .{ .sets = &.{.{ .cell = decoded.cells[0] }} });
        }

        fn validateFirstTableSchemaApplyAgainstExistingRows(self: *DB, alloc: Allocator, next_schema: schema_mod.TableSchema) !void {
            const existing_mode = try detectExistingPhysicalStorageMode(alloc, self.core.store);
            switch (existing_mode) {
                .empty => return,
                .document => if (next_schema.storage_mode != .document) return error.InvalidSchemaUpdateRequest,
                .relational => if (next_schema.storage_mode != .relational) return error.InvalidSchemaUpdateRequest,
                .mixed => return error.InvalidSchemaUpdateRequest,
            }
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
                    const rewritten = try Self.schemaRewriteRowValueAlloc(self, alloc, row.row_value, target_column, expression);
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

fn validateRuntimeTableSchemaTransition(current_schema: schema_mod.TableSchema, next_schema: schema_mod.TableSchema) !void {
    if (current_schema.storage_mode != next_schema.storage_mode) return error.InvalidSchemaUpdateRequest;
    if (next_schema.storage_mode != .relational) return;
    try validateRelationalColumnCatalogTransition(current_schema.relational_columns, next_schema.relational_columns);
    if (!schema_mod.primaryKeyCatalogsEqual(current_schema.primary_key, next_schema.primary_key)) {
        return error.InvalidSchemaUpdateRequest;
    }
    if (!schema_mod.relationalPeriodCatalogsEqual(current_schema.periods, next_schema.periods)) {
        return error.InvalidSchemaUpdateRequest;
    }
    try validateConstraintCatalogTransition(current_schema, next_schema);
}

fn validateRelationalColumnCatalogTransition(current_columns: []const schema_mod.RelationalColumn, next_columns: []const schema_mod.RelationalColumn) !void {
    for (next_columns) |column| {
        if (findRelationalColumnByPath(current_columns, column.path)) |current_column| {
            if (!schema_mod.relationalColumnCatalogsEqual(&.{current_column.*}, &.{column})) {
                return error.InvalidSchemaUpdateRequest;
            }
            continue;
        }
        if (try relationalColumnHasUniqueDroppedRenameSource(current_columns, next_columns, column)) continue;
        try validateNewRelationalColumnTransition(current_columns, next_columns, column);
    }
}

fn findRelationalColumnByPath(columns: []const schema_mod.RelationalColumn, path: []const u8) ?*const schema_mod.RelationalColumn {
    for (columns) |*column| {
        if (std.mem.eql(u8, column.path, path)) return column;
    }
    return null;
}

fn relationalColumnPathExists(columns: []const schema_mod.RelationalColumn, path: []const u8) bool {
    return findRelationalColumnByPath(columns, path) != null;
}

fn relationalColumnHasUniqueDroppedRenameSource(
    current_columns: []const schema_mod.RelationalColumn,
    next_columns: []const schema_mod.RelationalColumn,
    target_column: schema_mod.RelationalColumn,
) !bool {
    var source_index: ?usize = null;
    for (current_columns, 0..) |current_column, index| {
        if (relationalColumnPathExists(next_columns, current_column.path)) continue;
        if (!schema_mod.relationalColumnDefinitionsEqual(current_column, target_column)) continue;
        if (source_index != null) return error.InvalidSchemaUpdateRequest;
        source_index = index;
    }
    const matched_source_index = source_index orelse return false;

    var use_count: usize = 0;
    for (next_columns) |next_column| {
        if (relationalColumnPathExists(current_columns, next_column.path)) continue;
        if (schema_mod.relationalColumnDefinitionsEqual(current_columns[matched_source_index], next_column)) {
            use_count += 1;
        }
    }
    if (use_count != 1) return error.InvalidSchemaUpdateRequest;
    return true;
}

fn validateNewRelationalColumnTransition(
    current_columns: []const schema_mod.RelationalColumn,
    next_columns: []const schema_mod.RelationalColumn,
    column: schema_mod.RelationalColumn,
) !void {
    if (column.generated) |generated| {
        try validateAppendedGeneratedColumnTransition(current_columns, next_columns, generated);
        return;
    }
    if (column.default_value) |default_value| {
        if (default_value.kind != .literal) return error.InvalidSchemaUpdateRequest;
        return;
    }
    if (!column.nullable) return error.InvalidSchemaUpdateRequest;
}

fn relationalColumnMatchesGeneratedSource(column: schema_mod.RelationalColumn, field: []const u8, normalized: []const u8) bool {
    return std.mem.eql(u8, field, column.name) or
        std.mem.eql(u8, field, column.path) or
        std.mem.eql(u8, normalized, column.name) or
        std.mem.eql(u8, normalized, column.path);
}

fn validateGeneratedColumnBackfillSource(
    current_columns: []const schema_mod.RelationalColumn,
    next_columns: []const schema_mod.RelationalColumn,
    field: []const u8,
) !void {
    const normalized = if (std.mem.startsWith(u8, field, "/")) field[1..] else field;
    for (current_columns) |column| {
        if (relationalColumnMatchesGeneratedSource(column, field, normalized)) {
            return;
        }
    }
    for (next_columns) |column| {
        if (relationalColumnPathExists(current_columns, column.path)) continue;
        if (!relationalColumnMatchesGeneratedSource(column, field, normalized)) continue;
        if (column.generated != null) return error.InvalidSchemaUpdateRequest;
        const default_value = column.default_value orelse return error.InvalidSchemaUpdateRequest;
        if (default_value.kind != .literal) return error.InvalidSchemaUpdateRequest;
        if (jsonLiteralIsNull(default_value.value_json)) return error.InvalidSchemaUpdateRequest;
        return;
    }
    return error.InvalidSchemaUpdateRequest;
}

fn validateAppendedGeneratedColumnTransition(
    current_columns: []const schema_mod.RelationalColumn,
    next_columns: []const schema_mod.RelationalColumn,
    generated: schema_mod.RelationalGeneratedValue,
) !void {
    switch (generated.op) {
        .lower, .upper, .md5 => {
            const field = generated.field orelse return error.InvalidSchemaUpdateRequest;
            try validateGeneratedColumnBackfillSource(current_columns, next_columns, field);
        },
        .concat, .concat_ws => {
            if (generated.fields.len == 0) return error.InvalidSchemaUpdateRequest;
            for (generated.fields) |field| {
                try validateGeneratedColumnBackfillSource(current_columns, next_columns, field);
            }
        },
        .expression => {
            const expression = generated.expression orelse return error.InvalidSchemaUpdateRequest;
            try validateGeneratedColumnExpressionBackfillSources(current_columns, next_columns, expression);
        },
    }
}

fn validateGeneratedColumnExpressionBackfillSources(
    current_columns: []const schema_mod.RelationalColumn,
    next_columns: []const schema_mod.RelationalColumn,
    expression: schema_mod.RelationalRowsExpression,
) error{InvalidSchemaUpdateRequest}!void {
    if (expression.kind == .field) {
        try validateGeneratedColumnBackfillSource(current_columns, next_columns, expression.field);
    }
    for (expression.operands) |operand| try validateGeneratedColumnExpressionBackfillSources(current_columns, next_columns, operand);
    for (expression.case_branches) |branch| {
        try validateGeneratedColumnExpressionConditionBackfillSources(current_columns, next_columns, branch.when);
        try validateGeneratedColumnExpressionBackfillSources(current_columns, next_columns, branch.then);
    }
    for (expression.case_else) |case_else| try validateGeneratedColumnExpressionBackfillSources(current_columns, next_columns, case_else);
}

fn validateGeneratedColumnExpressionConditionBackfillSources(
    current_columns: []const schema_mod.RelationalColumn,
    next_columns: []const schema_mod.RelationalColumn,
    condition: schema_mod.RelationalRowsExpressionCondition,
) error{InvalidSchemaUpdateRequest}!void {
    try validateGeneratedColumnExpressionBackfillSources(current_columns, next_columns, condition.lhs);
    for (condition.rhs) |rhs| try validateGeneratedColumnExpressionBackfillSources(current_columns, next_columns, rhs);
}

fn validateConstraintCatalogTransition(current_schema: schema_mod.TableSchema, next_schema: schema_mod.TableSchema) !void {
    for (current_schema.unique_constraints) |constraint| {
        if (findUniqueConstraintByName(next_schema.unique_constraints, constraint.name)) |next_constraint| {
            if (!uniqueConstraintsEqual(constraint, next_constraint)) return error.InvalidSchemaUpdateRequest;
        }
    }
    for (next_schema.unique_constraints) |constraint| {
        if (findUniqueConstraintByName(current_schema.unique_constraints, constraint.name)) |current_constraint| {
            if (!uniqueConstraintsEqual(current_constraint, constraint)) return error.InvalidSchemaUpdateRequest;
        }
    }
    for (current_schema.foreign_keys) |foreign_key| {
        if (findForeignKeyByName(next_schema.foreign_keys, foreign_key.name)) |next_foreign_key| {
            if (!foreignKeysSameDefinition(foreign_key, next_foreign_key)) return error.InvalidSchemaUpdateRequest;
        }
    }
    for (next_schema.foreign_keys) |foreign_key| {
        if (findForeignKeyByName(current_schema.foreign_keys, foreign_key.name)) |current_foreign_key| {
            if (!foreignKeysSameDefinition(current_foreign_key, foreign_key)) return error.InvalidSchemaUpdateRequest;
        }
    }
    for (current_schema.checks) |check| {
        if (findRelationalCheckByName(next_schema.checks, check.name)) |next_check| {
            if (!schema_mod.relationalChecksEqual(check, next_check)) return error.InvalidSchemaUpdateRequest;
        }
    }
    for (next_schema.checks) |check| {
        if (findRelationalCheckByName(current_schema.checks, check.name)) |current_check| {
            if (!schema_mod.relationalChecksEqual(current_check, check)) return error.InvalidSchemaUpdateRequest;
        }
    }
}

fn relationalDefaultColumnRowValueAlloc(
    alloc: Allocator,
    column: schema_mod.RelationalColumn,
    value_json: []const u8,
) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('{');
    try writer.print("{f}:", .{std.json.fmt(column.path, .{})});
    try writer.writeAll(value_json);
    try writer.writeByte('}');
    const json = try out.toOwnedSlice();
    defer alloc.free(json);
    const columns = [_]schema_mod.RelationalColumn{column};
    return try mapper.buildRelationalRowValueAlloc(alloc, json, columns[0..]);
}

fn jsonLiteralIsNull(value_json: []const u8) bool {
    return std.mem.eql(u8, std.mem.trim(u8, value_json, " \t\r\n"), "null");
}

const ExistingPhysicalStorageMode = enum {
    empty,
    document,
    relational,
    mixed,
};

fn detectExistingPhysicalStorageMode(alloc: Allocator, store: anytype) !ExistingPhysicalStorageMode {
    const lower = [_]u8{internal_keys.user_namespace};
    const upper = [_]u8{internal_keys.user_namespace + 1};
    const rows = try store.scanRange(alloc, lower[0..], upper[0..]);
    defer docstore_mod.DocStore.freeResults(alloc, rows);

    var saw_document = false;
    var saw_relational = false;
    for (rows) |entry| {
        if (internal_keys.isPrimaryDocumentKey(entry.key)) {
            saw_document = true;
        } else if (internal_keys.isRelationalRowKey(entry.key)) {
            saw_relational = true;
        }
        if (saw_document and saw_relational) return .mixed;
    }

    if (saw_document) return .document;
    if (saw_relational) return .relational;
    return .empty;
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

fn findUniqueConstraintByName(unique_constraints: []const schema_mod.UniqueConstraint, name: []const u8) ?schema_mod.UniqueConstraint {
    for (unique_constraints) |constraint| {
        if (std.mem.eql(u8, constraint.name, name)) return constraint;
    }
    return null;
}

fn findForeignKeyByName(foreign_keys: []const schema_mod.ForeignKey, name: []const u8) ?schema_mod.ForeignKey {
    for (foreign_keys) |foreign_key| {
        if (std.mem.eql(u8, foreign_key.name, name)) return foreign_key;
    }
    return null;
}

fn findRelationalCheckByName(checks: []const schema_mod.RelationalCheck, name: []const u8) ?schema_mod.RelationalCheck {
    for (checks) |check| {
        if (std.mem.eql(u8, check.name, name)) return check;
    }
    return null;
}

fn uniqueConstraintsEqual(a: schema_mod.UniqueConstraint, b: schema_mod.UniqueConstraint) bool {
    return std.mem.eql(u8, a.name, b.name) and
        stringSlicesEqual(a.columns, b.columns) and
        uniqueExpressionSlicesEqual(a.expressions, b.expressions) and
        optionalStringsEqual(a.without_overlaps_period, b.without_overlaps_period) and
        uniquePredicateSlicesEqual(a.where, b.where) and
        relationalRowsExpressionConditionSlicesEqual(a.where_expressions, b.where_expressions);
}

fn uniqueExpressionSlicesEqual(a: []const schema_mod.UniqueExpression, b: []const schema_mod.UniqueExpression) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (left.op != right.op) return false;
        if (!std.mem.eql(u8, left.field, right.field)) return false;
        if (left.expression == null or right.expression == null) {
            if (left.expression != null or right.expression != null) return false;
        } else if (!relationalRowsExpressionEqual(left.expression.?, right.expression.?)) return false;
    }
    return true;
}

fn uniquePredicateSlicesEqual(a: []const schema_mod.UniquePredicate, b: []const schema_mod.UniquePredicate) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (left.op != right.op) return false;
        if (!std.mem.eql(u8, left.field, right.field)) return false;
        if (!optionalStringsEqual(left.value_json, right.value_json)) return false;
    }
    return true;
}

fn relationalRowsExpressionConditionSlicesEqual(
    a: []const schema_mod.RelationalRowsExpressionCondition,
    b: []const schema_mod.RelationalRowsExpressionCondition,
) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (!relationalRowsExpressionConditionEqual(left, right)) return false;
    }
    return true;
}

fn foreignKeysSameDefinition(a: schema_mod.ForeignKey, b: schema_mod.ForeignKey) bool {
    return std.mem.eql(u8, a.name, b.name) and
        stringSlicesEqual(a.child_columns, b.child_columns) and
        optionalStringsEqual(a.child_period, b.child_period) and
        std.mem.eql(u8, a.parent_table, b.parent_table) and
        stringSlicesEqual(a.parent_columns, b.parent_columns) and
        optionalStringsEqual(a.parent_period, b.parent_period) and
        a.on_delete == b.on_delete and
        a.on_update == b.on_update and
        a.timing == b.timing and
        a.deferrable == b.deferrable and
        a.match == b.match;
}

fn stringSlicesEqual(a: []const []const u8, b: []const []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (!std.mem.eql(u8, left, right)) return false;
    }
    return true;
}

fn relationalRowsExpressionConditionEqual(
    lhs: schema_mod.RelationalRowsExpressionCondition,
    rhs: schema_mod.RelationalRowsExpressionCondition,
) bool {
    if (lhs.op != rhs.op or lhs.rhs.len != rhs.rhs.len) return false;
    if (!relationalRowsExpressionEqual(lhs.lhs, rhs.lhs)) return false;
    for (lhs.rhs, rhs.rhs) |lhs_rhs, rhs_rhs| {
        if (!relationalRowsExpressionEqual(lhs_rhs, rhs_rhs)) return false;
    }
    return true;
}

fn relationalRowsExpressionEqual(
    lhs: schema_mod.RelationalRowsExpression,
    rhs: schema_mod.RelationalRowsExpression,
) bool {
    if (lhs.kind != rhs.kind or
        lhs.field_source != rhs.field_source or
        lhs.json_as_text != rhs.json_as_text or
        lhs.cast_type != rhs.cast_type or
        !std.mem.eql(u8, lhs.field, rhs.field) or
        !std.mem.eql(u8, lhs.value_json, rhs.value_json) or
        !std.mem.eql(u8, lhs.json_path, rhs.json_path) or
        lhs.operands.len != rhs.operands.len or
        lhs.case_branches.len != rhs.case_branches.len or
        lhs.case_else.len != rhs.case_else.len)
    {
        return false;
    }
    for (lhs.operands, rhs.operands) |lhs_operand, rhs_operand| {
        if (!relationalRowsExpressionEqual(lhs_operand, rhs_operand)) return false;
    }
    for (lhs.case_branches, rhs.case_branches) |lhs_branch, rhs_branch| {
        if (!relationalRowsExpressionConditionEqual(lhs_branch.when, rhs_branch.when)) return false;
        if (!relationalRowsExpressionEqual(lhs_branch.then, rhs_branch.then)) return false;
    }
    for (lhs.case_else, rhs.case_else) |lhs_fallback, rhs_fallback| {
        if (!relationalRowsExpressionEqual(lhs_fallback, rhs_fallback)) return false;
    }
    return true;
}
