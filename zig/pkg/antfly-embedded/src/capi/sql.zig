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

//! Database catalog SQL binding for Lite/C. Reuses the public compiler and executor;
//! this is not a second catalog, coordinator, or SQL implementation.
const std = @import("std");
const storage_root = @import("antfly_storage_root");
const dependencies = (if (@hasDecl(storage_root, "runtime_impl")) storage_root.runtime_impl else storage_root).capi_dependencies;
const catalog = dependencies.sql_catalog;
const ast = dependencies.sql_ast;
pub const compiler = dependencies.sql_compiler;
pub const runtime = dependencies.sql_runtime;

pub fn Adapter(comptime native: type) type {
    return struct {
        const Self = @This();
        const DB = native.db.DB;
        const types = native.db.types;
        const integrity = dependencies.api_relational_integrity_commit;
        const reads = dependencies.api_table_read_source;
        const contract = dependencies.api_distributed_txn_contract;
        const topology = dependencies.common_topology_records;
        transaction: ?*@import("sql_session.zig").Session = null,
        handle: ?*@import("handles.zig").Handle = null,
        db: *DB,
        table_name: []const u8,
        read_only: bool = false,
        outcome_transaction_id: ?types.TxnId = null,

        pub fn backend(self: *Self) catalog.Backend {
            return .{ .execution_io = self.db.backend_runtime.io(), .ptr = self, .supports_search_relations = true, .predicate_only_mutations = true, .vtable = &.{ .resolve_conflict_owners = resolveConflictOwners, .generate_row_id = generateRowId, .resolve = resolve, .scan = scan, .open_scan = open, .open_statement = openStatement, .mutate = mutate, .mutate_prepared = mutate, .prepare_mutations = prepareMutations, .checkpoint = checkpoint, .ddl = ddl } };
        }

        fn selected(self: *Self, name: []const u8) !Self {
            if (self.handle) |handle| return .{ .transaction = self.transaction, .handle = handle, .db = try @import("tables.zig").get(handle, name), .table_name = name, .read_only = self.read_only };
            if (!std.mem.eql(u8, name, self.table_name)) return error.UndefinedTable;
            return self.*;
        }

        fn ddl(ptr: *anyopaque, alloc: std.mem.Allocator, request: catalog.Ddl) !catalog.DdlOutcome {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (self.read_only) return error.SqlReadOnlyTransaction;
            const handle = self.handle orelse return error.UnsupportedSqlExecution;
            if (self.transaction) |session| if (session.active) return error.UnsupportedSqlExecution;
            switch (request) {
                .create_table => |create| {
                    if (create.name.database != null or (create.name.namespace != null and !std.mem.eql(u8, create.name.namespace.?, "public")) or create.tablespace != null) return error.UnsupportedSqlExecution;
                    @import("tables.zig").create(handle, create.name.table, create.schema_json, create.if_not_exists) catch |err| switch (err) {
                        error.SqlDdlPending => return .{ .mutation_outcome = .committed_pending },
                        else => return err,
                    };
                },
                .drop_table => |drop| {
                    if (drop.table.database != null or (drop.table.namespace != null and !std.mem.eql(u8, drop.table.namespace.?, "public"))) return error.UnsupportedSqlExecution;
                    @import("tables.zig").drop(handle, drop.table.table, drop.if_exists) catch |err| switch (err) {
                        error.SqlDdlPending => return .{ .mutation_outcome = .committed_pending },
                        else => return err,
                    };
                },
                .catalog_ddl => |change| return self.alterSchema(alloc, change),
                else => return error.UnsupportedSqlExecution,
            }
            return .{};
        }

        fn ownsIndex(db: *DB, alloc: std.mem.Allocator, name: []const u8) !bool {
            const bytes = (try db.getSchemaJson(alloc)) orelse return false;
            const schema = try std.json.parseFromSliceLeaky(std.json.Value, alloc, bytes, .{});
            const indexes = schema.object.get("relational_indexes") orelse return false;
            for (indexes.array.items) |index| if (std.mem.eql(u8, index.object.get("name").?.string, name)) return true;
            return false;
        }

        fn alterSchema(self: *Self, alloc: std.mem.Allocator, ddl_request: ast.CatalogDdl) !catalog.DdlOutcome {
            const handle = self.handle orelse return error.UnsupportedSqlExecution;
            const tables = @import("tables.zig");
            try tables.requireDatabase(handle);
            if (ddl_request.kind != .table or ddl_request.action != .alter_schema or ddl_request.schema_change == null) return error.UnsupportedSqlExecution;
            var ddl_value = ddl_request;
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const a = arena.allocator();
            if (ddl_value.name.table.len == 0) {
                if (ddl_value.schema_change.? != .drop_index) return error.InvalidSqlSyntax;
                const name = ddl_value.schema_change.?.drop_index;
                var found: ?[]const u8 = if (try ownsIndex(&handle.db, a, name)) "default" else null;
                var iterator = handle.embedded_tables.valueIterator();
                while (iterator.next()) |table| if (try ownsIndex(&table.*.db, a, name)) {
                    if (found != null) return error.SqlAmbiguousIndex;
                    found = table.*.name;
                };
                ddl_value.name.table = found orelse {
                    if (ddl_value.conditional) return .{};
                    return error.SqlIndexNotFound;
                };
            }
            if (ddl_value.name.database != null or (ddl_value.name.namespace != null and !std.mem.eql(u8, ddl_value.name.namespace.?, "public"))) return error.UndefinedTable;
            const db = try tables.get(handle, ddl_value.name.table);
            const bytes = (try db.getSchemaJson(a)) orelse return error.UnsupportedSqlExecution;
            const parsed = try native.public_api.tables.parseValidatedTableSchema(a, bytes);
            const schema = try native.public_api.tables.deriveRuntimeTableSchema(a, parsed);
            if (schema.storage_mode != .relational) return error.UnsupportedSqlShape;
            var candidate = try std.json.parseFromSliceLeaky(std.json.Value, a, bytes, .{ .parse_numbers = false });
            if (!try dependencies.sql_schema_ddl.apply(a, &candidate, ddl_value)) return .{};
            const validation = ddl_value.schema_change.? == .validate_constraint;
            const version = if (validation) schema.version else std.math.add(u32, schema.version, 1) catch return error.SqlProgramLimitExceeded;
            try candidate.object.put(a, "version", .{ .integer = version });
            const proposed = try std.json.Stringify.valueAlloc(a, candidate, .{});
            var target = try self.selected(ddl_value.name.table);
            defer self.outcome_transaction_id = target.outcome_transaction_id;
            const metadata = try target.localCatalog(a, schema.version);
            const proposed_schema = try native.public_api.tables.parseValidatedTableSchema(a, proposed);
            const validator = dependencies.schema_relational_foreign_key_target;
            for (try proposed_schema.relationalForeignKeyDefinitions(a)) |fk| {
                const parent = for (metadata.table) |record| {
                    if (std.mem.eql(u8, record.name, fk.parent_table)) break record;
                } else return error.UndefinedTable;
                try validator.validate(a, proposed, parent.name, if (parent.table_id == db.core.identity_namespace.table_id) proposed else parent.schema_json);
            }
            for (metadata.table) |record| {
                const child_bytes = if (record.table_id == db.core.identity_namespace.table_id) proposed else record.schema_json;
                const child = try native.public_api.tables.parseValidatedTableSchema(a, child_bytes);
                for (try child.relationalForeignKeyDefinitions(a)) |fk| if (std.mem.eql(u8, fk.parent_table, ddl_value.name.table)) {
                    validator.validate(a, child_bytes, ddl_value.name.table, proposed) catch |err| switch (err) {
                        error.ForeignKeyTargetNotUnique, error.ForeignKeyTypeMismatch, error.ForeignKeyPartialSupportIndexRequired => return error.SqlDependentConstraint,
                        else => return err,
                    };
                };
            }
            // Receipt ownership precedes publication, including all allocations.
            var receipt: catalog.DdlReceipt = .{
                .database = try alloc.dupe(u8, "default"),
                .namespace = try alloc.dupe(u8, "public"),
                .table = try alloc.dupe(u8, ddl_value.name.table),
                .table_id = try std.fmt.allocPrint(alloc, "{d}", .{db.core.identity_namespace.table_id}),
                .schema_version = version,
                .state = .pending,
            };
            if (validation) {
                @import("sql_ddl.zig").retry(&target, version) catch |err| switch (err) {
                    error.SqlDdlPending => return .{ .mutation_outcome = .committed_pending, .receipt = receipt },
                    error.SqlMutationOutcomeUnknown => {
                        receipt.state = .admission_unknown;
                        return .{ .mutation_outcome = null, .receipt = receipt };
                    },
                    else => return err,
                };
            } else {
                const published_fk = @import("sql_fk.zig").publish(&target, a, proposed, schema.version, false) catch |err| switch (err) {
                    error.SqlDdlPending => return .{ .mutation_outcome = .committed_pending, .receipt = receipt },
                    error.SqlMutationOutcomeUnknown => {
                        receipt.state = .admission_unknown;
                        return .{ .mutation_outcome = null, .receipt = receipt };
                    },
                    else => return err,
                };
                if (!published_fk) {
                    const retired = @import("sql_ddl.zig").publish(&target, a, proposed, schema.version) catch |err| switch (err) {
                        error.SqlDdlPending => return .{ .mutation_outcome = .committed_pending, .receipt = receipt },
                        else => return err,
                    };
                    if (!retired) db.compareAndSetSchemaJson(a, proposed, schema.version) catch |err| switch (err) {
                        error.DurabilityOutcomeUnknown => {
                            receipt.state = .admission_unknown;
                            return .{ .mutation_outcome = null, .receipt = receipt };
                        },
                        else => return err,
                    };
                }
            }
            if (ddl_value.schema_change.? == .create_index) {
                target.db.ensureRelationalIndexesReady(&.{ddl_value.schema_change.?.create_index.name}, .none, native.platform_time.monotonicNs() +| 5 * std.time.ns_per_s) catch |err| switch (err) {
                    // A failed derived build belongs in the invalid receipt;
                    // ddlState below reads its authoritative diagnostic state.
                    error.RelationalIndexNotReady => {},
                    else => return .{ .mutation_outcome = .committed_pending, .receipt = receipt },
                };
            }
            @import("sql_ddl.zig").activate(&target, version) catch return .{ .mutation_outcome = .committed_pending, .receipt = receipt };
            receipt.state = target.ddlState(a, ddl_value.schema_change.?, proposed, version) catch return .{ .mutation_outcome = .committed_pending, .receipt = receipt };
            return .{ .mutation_outcome = switch (receipt.state) {
                .ready => .committed,
                .invalid => .committed_repair_required,
                else => .committed_pending,
            }, .receipt = receipt };
        }

        fn ddlState(self: *Self, alloc: std.mem.Allocator, change: ast.SchemaChange, schema_json: []const u8, version: u32) !catalog.DdlReceipt.State {
            // Coordinated coverage is authoritative for UNIQUE, FK and CHECK
            // activation. The local CHECK-only job cannot describe other DDL.
            var state: catalog.DdlReceipt.State = .ready;
            if (try dependencies.api_relational_integrity_commit.requiresActivation(alloc, schema_json)) {
                const response = (try self.db.lookup(alloc, "", .{ .relational_activation_json = "{}" })) orelse return error.ConstraintActivationUnavailable;
                const status = try std.json.parseFromSliceLeaky(struct { schema_version: u32, state: dependencies.storage_db_relational_integrity_activation_contract.State }, alloc, response.json, .{ .ignore_unknown_fields = true });
                if (status.schema_version != version) return error.PreparedGenerationChanged;
                state = switch (status.state) {
                    .enforced => .ready,
                    .invalid => .invalid,
                    .validating => .pending,
                };
            }
            if (change == .create_index) {
                const index = try self.db.relationalIndexBuildStatus(change.create_index.name);
                if (index.state == .failed) return .invalid;
                if (index.state != .ready and state != .invalid) return .pending;
            }
            return state;
        }

        pub const LocalCatalog = struct {
            table: []const topology.TableRecord,
            range: []const topology.RangeRecord,
        };

        pub fn localCatalog(self: *Self, alloc: std.mem.Allocator, version: u32) !LocalCatalog {
            const count = if (self.handle) |handle| handle.embedded_tables.count() + 1 else 1;
            const tables = try alloc.alloc(topology.TableRecord, count);
            const ranges = try alloc.alloc(topology.RangeRecord, count);
            var used: usize = 0;
            try self.appendMetadata(alloc, self.db, self.table_name, version, tables, ranges, &used);
            if (self.handle) |handle| {
                if (self.db != &handle.db) try self.appendMetadata(alloc, &handle.db, "default", null, tables, ranges, &used);
                var iterator = handle.embedded_tables.valueIterator();
                while (iterator.next()) |entry| {
                    if (&entry.*.db == self.db) continue;
                    try self.appendMetadata(alloc, &entry.*.db, entry.*.name, null, tables, ranges, &used);
                }
            }
            return .{ .table = tables[0..used], .range = ranges[0..used] };
        }

        fn appendMetadata(self: *Self, alloc: std.mem.Allocator, db: *DB, name: []const u8, expected: ?u32, tables: []topology.TableRecord, ranges: []topology.RangeRecord, used: *usize) !void {
            _ = self;
            const range = db.core.byteRange();
            const identity = db.core.identity_namespace;
            if (range.start.len != 0 or range.end.len != 0) return error.UnsupportedSqlExecution;
            const json = (try db.getSchemaJson(alloc)) orelse {
                if (expected != null) return error.IntegrityCatalogUnavailable;
                return;
            };
            if (identity.table_id == 0) return error.CoordinatedConstraintsRequireTableIdentity;
            const parsed = try native.public_api.tables.parseValidatedTableSchema(alloc, json);
            if (expected) |version| if (parsed.version != version) return error.PreparedGenerationChanged;
            tables[used.*] = .{ .table_id = identity.table_id, .name = name, .schema_json = json };
            ranges[used.*] = .{ .group_id = identity.shard_id, .range_id = identity.range_id, .table_id = identity.table_id, .start_key = "", .doc_identity_shard_id = identity.shard_id, .doc_identity_range_id = identity.range_id };
            used.* += 1;
        }

        pub fn localSource(self: *Self) reads.TableReadSource {
            return .{ .ptr = self, .vtable = &.{ .lookup = integrityLookup, .scan = integrityScan, .query = integrityQuery } };
        }

        fn integrityLookup(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, key: []const u8, options: types.LookupOptions, _: dependencies.raft_read_gate.ReadConsistency) !?reads.LookupResponse {
            const adapter_owner: *Self = @ptrCast(@alignCast(ptr));
            var selected_adapter = try adapter_owner.selected(name);
            const self = &selected_adapter;
            const result = (try self.db.lookup(alloc, key, options)) orelse return null;
            errdefer alloc.free(result.json);
            const internal = options.relational_integrity_catalog or options.relational_integrity_jobs_json.len != 0 or options.relational_index_status_json.len != 0 or options.relational_activation_json.len != 0;
            return .{ .json = result.json, .version = if (internal) 0 else result.version orelse try self.db.getTimestamp(alloc, key), .expected_content_digest = result.expected_content_digest };
        }

        fn integrityScan(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, from: []const u8, to: []const u8, options: types.ScanOptions, _: dependencies.raft_read_gate.ReadConsistency) !?reads.ScanResponse {
            const adapter_owner: *Self = @ptrCast(@alignCast(ptr));
            var selected_adapter = try adapter_owner.selected(name);
            const self = &selected_adapter;
            var result = try self.db.scan(alloc, from, to, options);
            defer result.deinit(alloc);
            return .{ .ndjson = try dependencies.api_local_query_contract.encodeStorageKernelScanNdjson(alloc, result, options.include_documents) };
        }

        fn integrityQuery(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: types.SearchRequest, _: dependencies.raft_read_gate.ReadConsistency) !?dependencies.api_query_response.QueryResponse {
            return error.UnsupportedSqlExecution;
        }

        fn resolveConflictOwners(ptr: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, columns: []const []const u8, expressions: []const catalog.ConflictExpression, conditions: []const catalog.Condition, mutations: []const catalog.Mutation) ![]const catalog.ConflictOwner {
            const adapter_owner: *Self = @ptrCast(@alignCast(ptr));
            var selected_adapter = try adapter_owner.selected(table.physical_name);
            const self = &selected_adapter;
            if (self.read_only) return error.SqlReadOnlyTransaction;
            if (!std.mem.eql(u8, table.physical_name, self.table_name)) return error.UndefinedTable;
            if (columns.len == 0 and expressions.len == 0) {
                const json = (try self.db.getSchemaJson(alloc)) orelse return error.IntegrityCatalogUnavailable;
                const parsed = try native.public_api.tables.parseValidatedTableSchema(alloc, json);
                if (parsed.version != table.schema_version) return error.PreparedGenerationChanged;
                if (!try integrity.requiresCoordination(alloc, json)) {
                    const output = try alloc.alloc(catalog.ConflictOwner, mutations.len);
                    @memset(output, .{ .key = null, .identity = null, .guard = null, .primary_only = true });
                    return output;
                }
            }
            const metadata = try self.localCatalog(alloc, table.schema_version);
            const writes = try dependencies.sql_mutation_images.writes(types.BatchWrite, alloc, mutations);
            const target_predicates = try dependencies.sql_conflict_predicate.toNative(alloc, conditions);
            const keys = try dependencies.sql_conflict_predicate.expressionsToNative(alloc, expressions);
            const owners = try integrity.resolveConflictOwners(alloc, self.localSource(), metadata.table, metadata.range, self.table_name, table.schema_version, columns, keys, target_predicates, writes, if (self.transaction) |session| if (session.active) try session.commitRequests(alloc) else &.{} else &.{}, .{});
            const output = try alloc.alloc(catalog.ConflictOwner, owners.len);
            for (owners, output) |*owner, *out| out.* = .{ .key = owner.key, .identity = owner.identity, .identities = owner.identities, .guard = owner };
            return output;
        }

        fn commitCoordinated(self: *Self, request: contract.TableCommitRequest) !catalog.MutationOutcome {
            const io = self.db.backend_runtime.io() orelse return error.UnsupportedSqlExecution;
            const now: u64 = @intCast(@max(0, std.Io.Clock.real.now(io).nanoseconds));
            const txn_id = try self.db.beginTransaction(now);
            self.outcome_transaction_id = txn_id;
            self.db.writeTransaction(txn_id, .{
                .schema_version = request.schema_version,
                .relational_schema_version = request.relational_schema_version,
                .relational_integrity_generation_set = request.relational_integrity_generation_set,
                .relational_repair = request.relational_repair,
                .writes = request.writes,
                .deletes = request.deletes,
                .predicates = request.predicates,
                .integrity_commands = request.integrity_commands,
            }) catch |err| {
                self.db.abortTransaction(txn_id, now +| 1) catch return error.SqlMutationOutcomeUnknown;
                self.outcome_transaction_id = null;
                return if (err == error.VersionConflict) error.SqlWriteConflict else err;
            };
            // Only durable status can classify a failure after commit starts.
            // Never abort/replay a possibly published mutation.
            self.db.commitTransaction(txn_id, now +| 1) catch |err| {
                const status = self.db.getTransactionStatus(txn_id) catch return error.SqlMutationOutcomeUnknown;
                if (status == .committed) return switch (err) {
                    error.EnrichmentWorkerFailed => .committed_repair_required,
                    else => .committed_pending,
                };
                if (status == .aborted) {
                    self.outcome_transaction_id = null;
                    return error.SqlWriteConflict;
                }
                return error.SqlMutationOutcomeUnknown;
            };
            self.outcome_transaction_id = null;
            return .committed;
        }

        fn generateRowId(ptr: *anyopaque, alloc: std.mem.Allocator) ![]const u8 {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return dependencies.storage_row_identity.generate(alloc, self.db.backend_runtime.io() orelse return error.UnsupportedSqlExecution);
        }

        const Statement = struct {
            alloc: std.mem.Allocator,
            cursors: []catalog.Cursor,
            fn close(ptr: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                for (self.cursors) |cursor| cursor.close(cursor.ptr);
                self.alloc.free(self.cursors);
                self.alloc.destroy(self);
            }
        };

        fn openStatement(ptr: *anyopaque, alloc: std.mem.Allocator, requests: []const catalog.StatementScan) !catalog.StatementRead {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (requests.len == 0 or requests.len > 64) return error.SqlProgramLimitExceeded;
            const statement = try alloc.create(Statement);
            errdefer alloc.destroy(statement);
            const cursors = try alloc.alloc(catalog.Cursor, requests.len);
            errdefer alloc.free(cursors);
            var initialized: usize = 0;
            errdefer for (cursors[0..initialized]) |cursor| cursor.close(cursor.ptr);
            // All aliases share this handle's capture interval. Independent
            // cursors then retain their snapshots after writers are released.
            const io = self.db.backend_runtime.io() orelse return error.SqlStatementReadUnavailable;
            const deadline = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(5));
            var search_arena = std.heap.ArenaAllocator.init(alloc);
            defer search_arena.deinit();
            const prepared = try search_arena.allocator().alloc(?native.public_api.query.OwnedQueryRequest, requests.len);
            @memset(prepared, null);
            defer for (prepared) |*request| if (request.*) |*owned| owned.deinit(search_arena.allocator());
            for (requests, prepared) |request, *out| if (request.request.search) |search| {
                var selected_adapter = try self.selected(request.table.physical_name);
                out.* = try selected_adapter.prepareSearch(search_arena.allocator(), search);
            };
            var owners: [64]*DB = undefined;
            var owner_count: usize = 0;
            for (requests) |request| {
                const selected_adapter = try self.selected(request.table.physical_name);
                const exists = for (owners[0..owner_count]) |db| {
                    if (db == selected_adapter.db) break true;
                } else false;
                if (!exists) {
                    owners[owner_count] = selected_adapter.db;
                    owner_count += 1;
                }
            }
            std.mem.sort(*DB, owners[0..owner_count], {}, struct {
                fn less(_: void, a: *DB, b: *DB) bool {
                    return a.core.identity_namespace.table_id < b.core.identity_namespace.table_id;
                }
            }.less);
            var fences: [64]DB.StatementReadFence = undefined;
            var held: usize = 0;
            defer for (fences[0..held]) |*fence| fence.release();
            while (held != owner_count) {
                errdefer {
                    for (fences[0..held]) |*fence| fence.release();
                    held = 0;
                }
                if (try owners[held].tryStatementReadFence()) |fence| {
                    fences[held] = fence;
                    held += 1;
                    continue;
                }
                for (fences[0..held]) |*fence| fence.release();
                held = 0;
                if (std.Io.Clock.awake.now(io).nanoseconds >= deadline.nanoseconds) return error.SqlStatementReadUnavailable;
                try io.sleep(.fromMilliseconds(1), .awake);
            }
            for (requests, prepared, cursors) |request, *search, *cursor| {
                if (search.*) |*owned| {
                    var selected_adapter = try self.selected(request.table.physical_name);
                    cursor.* = try selected_adapter.openSearch(alloc, request.table, request.request, owned.req);
                } else cursor.* = (try open(ptr, alloc, request.table, request.request)) orelse return error.SqlStatementSnapshotRequired;
                initialized += 1;
            }
            statement.* = .{ .alloc = alloc, .cursors = cursors };
            return .{ .ptr = statement, .cursors = cursors, .close = Statement.close };
        }

        const SemanticResolver = struct {
            adapter: *Self,
            fn resolveDense(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, index: []const u8, text: []const u8, template: ?[]const u8, limit: u32) anyerror!types.DenseKnnQuery {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                if (template != null) return error.UnsupportedQueryRequest;
                const handle = self.adapter.handle orelse return error.UnsupportedSqlExecution;
                const configs = try self.adapter.db.listIndexes(alloc);
                defer types.freeIndexConfigs(alloc, configs);
                var json: std.json.Value = .{ .object = .empty };
                for (configs) |config| {
                    const normalized = try @import("db.zig").liteManagedEmbeddingIndexConfigJson(alloc, config.kind, config.config_json);
                    try json.object.put(alloc, config.name, try std.json.parseFromSliceLeaky(std.json.Value, alloc, normalized, .{}));
                }
                const bytes = try std.json.Stringify.valueAlloc(alloc, json, .{});
                var managed = try @import("handles.zig").managed_embedder.ManagedEmbedder.initFromIndexesJsonWithOptions(alloc, bytes, .{ .antfly_provider = handle.liteAntflyProvider() });
                defer managed.deinit();
                return .{ .vector = try managed.embedQuery(alloc, index, text), .k = limit };
            }
        };

        fn prepareSearch(self: *Self, alloc: std.mem.Allocator, search: anytype) !native.public_api.query.OwnedQueryRequest {
            if (self.transaction) |session| if (session.active) {
                for (session.entries.items) |entry| if (std.mem.eql(u8, entry.table.physical_name, self.table_name)) return error.SqlSearchUncommittedWrites;
            };
            const text = search.request_text orelse return error.InvalidSqlParameters;
            const trimmed = std.mem.trim(u8, text, " \t\r\n");
            const bytes = if (std.mem.startsWith(u8, trimmed, "{")) trimmed else try std.json.Stringify.valueAlloc(alloc, .{ .full_text_search = .{ .query = text } }, .{});
            var resolver = SemanticResolver{ .adapter = self };
            var body = std.json.parseFromSliceLeaky(std.json.Value, alloc, bytes, .{}) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSqlParameters;
            if (body != .object) return error.InvalidSqlParameters;
            if (search.limit) |limit| try body.object.put(alloc, "limit", .{ .integer = limit });
            const canonical = try std.json.Stringify.valueAlloc(alloc, body, .{});
            var owned = native.public_api.query.parsePublicQueryRequest(alloc, .{ .ptr = &resolver, .vtable = &.{ .resolve_dense_query = SemanticResolver.resolveDense } }, self.table_name, canonical) catch |err| return if (native.public_api.query.isPublicQueryValidationError(err)) error.InvalidSqlParameters else err;
            errdefer owned.deinit(alloc);
            if (owned.req.limit == 0 or owned.req.limit > 10000) return error.SqlProgramLimitExceeded;
            owned.req.include_stored = true;
            owned.req.fields = &.{};
            owned.req.include_all_fields = true;
            return owned;
        }

        const SearchCursor = struct {
            alloc: std.mem.Allocator,
            arena: std.heap.ArenaAllocator,
            rows: []catalog.Row,
            position: usize = 0,
            fn next(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32) !catalog.Page {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                const end = @min(self.rows.len, self.position + limit);
                const rows = self.rows[self.position..end];
                self.position = end;
                return .{ .rows = rows, .after = if (end < self.rows.len) try std.fmt.allocPrint(alloc, "{d}", .{end}) else null };
            }
            fn close(ptr: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.arena.deinit();
                self.alloc.destroy(self);
            }
        };

        fn openSearch(self: *Self, alloc: std.mem.Allocator, table: catalog.Table, request: catalog.Scan, search: types.SearchRequest) !catalog.Cursor {
            // Statement capture fences already exclude primary mutation on
            // every participating table. The native lease also fences search
            // index/schema access while ranking and hydrating these hits.
            var lease = try self.db.beginQueryReadLease();
            defer lease.release();
            var view = self.db.core.acquireSchemaView() orelse return error.PreparedGenerationChanged;
            defer view.release();
            if (view.version() != table.schema_version) return error.PreparedGenerationChanged;
            var captured = try lease.search(alloc, search);
            defer captured.result.deinit();
            const cursor = try alloc.create(SearchCursor);
            errdefer alloc.destroy(cursor);
            var arena = std.heap.ArenaAllocator.init(alloc);
            errdefer arena.deinit();
            const a = arena.allocator();
            const rows = try a.alloc(catalog.Row, captured.result.hits.len);
            var fields: std.ArrayList([]const u8) = .empty;
            for (request.fields) |field| if (!std.mem.eql(u8, field, "_id") and !std.mem.eql(u8, field, "score") and !std.mem.eql(u8, field, "_highlights")) {
                try fields.append(a, field);
            };
            var typed_reader: ?DB.RelationalRows.Reader = if (table.storage_mode == .relational)
                try lease.relationalRows(alloc, fields.items, table.schema_version)
            else
                null;
            defer if (typed_reader) |*reader| reader.deinit();
            for (captured.result.hits, rows) |hit, *out| {
                const typed = if (typed_reader) |*reader| (try reader.lookupTypedRow(a, hit.id)) orelse return error.SqlStatementReadUnavailable else null;
                var document = if (typed) |row| row.typed orelse return error.InvalidSqlBackendResponse else if (hit.source_value) |value| try types.cloneJsonValue(a, value) else if (hit.stored_data) |data| try std.json.parseFromSliceLeaky(std.json.Value, a, data, .{ .parse_numbers = false }) else return error.InvalidSqlBackendResponse;
                if (document != .object) return error.InvalidSqlBackendResponse;
                try document.object.put(a, "score", if (hit.score) |score| .{ .float = score } else .null);
                if (try native.public_api.query.highlightsJsonValue(a, hit.highlights)) |highlights| {
                    const encoded = try std.json.Stringify.valueAlloc(a, highlights, .{});
                    try document.object.put(a, "_highlights", try std.json.parseFromSliceLeaky(std.json.Value, a, encoded, .{ .parse_numbers = false }));
                } else _ = document.object.swapRemove("_highlights");
                if (typed) |row| {
                    // Native typed cells are already projected and owned. Add
                    // only search metadata, keeping their SQL-null bitmap.
                    var projected: std.json.ObjectMap = .empty;
                    const nulls = try a.alloc(bool, request.fields.len);
                    for (request.fields) |field| {
                        if (std.mem.eql(u8, field, "_id")) continue;
                        try projected.put(a, field, document.object.get(field) orelse .null);
                        nulls[projected.count() - 1] = if (std.mem.eql(u8, field, "score")) hit.score == null else if (std.mem.eql(u8, field, "_highlights")) !document.object.contains("_highlights") else if (document.object.getIndex(field)) |index| row.sql_nulls.?[index] else true;
                    }
                    out.* = .{ .id = row.key, .version = row.version, .value = .{ .object = projected }, .sql_nulls = nulls[0..projected.count()] };
                } else out.* = try dependencies.sql_document_row.projectValue(a, table, hit.id, 0, document, request.fields);
            }
            cursor.* = .{ .alloc = alloc, .arena = arena, .rows = rows };
            return .{ .ptr = cursor, .next = SearchCursor.next, .close = SearchCursor.close };
        }

        fn resolve(ptr: *anyopaque, alloc: std.mem.Allocator, name: ast.Name, action: catalog.Action) !catalog.Table {
            const adapter_owner: *Self = @ptrCast(@alignCast(ptr));
            if (name.database != null or (name.namespace != null and !std.mem.eql(u8, name.namespace.?, "public"))) return error.UndefinedTable;
            var selected_adapter = try adapter_owner.selected(name.table);
            const self = &selected_adapter;
            if (self.read_only and action != .read) return error.SqlReadOnlyTransaction;
            const json = (try self.db.getSchemaJson(alloc)) orelse return error.UnsupportedSqlExecution;
            defer alloc.free(json);
            var scratch = std.heap.ArenaAllocator.init(alloc);
            defer scratch.deinit();
            const parsed = try native.public_api.tables.parseValidatedTableSchema(scratch.allocator(), json);
            const schema = try native.public_api.tables.deriveRuntimeTableSchema(scratch.allocator(), parsed);
            if (schema.storage_mode == .document) {
                return .{ .id = self.db.core.identity_namespace.table_id, .physical_name = try alloc.dupe(u8, self.table_name), .schema_version = schema.version, .storage_mode = .document, .columns = try dependencies.sql_document_row.deriveColumns(alloc, parsed) };
            }
            const columns = try alloc.alloc(catalog.Column, schema.relational_columns.len);
            for (schema.relational_columns, columns) |column, *out| {
                var generated = false;
                if (parsed.generated_columns) |items| {
                    if (items.value != .array) return error.InvalidSqlBackendResponse;
                    for (items.value.array.items) |item| {
                        const generated_name = if (item == .object) item.object.get("column") else null;
                        if (generated_name) |value| if (value == .string and std.mem.eql(u8, value.string, column.name)) {
                            generated = true;
                        };
                    }
                }
                out.* = .{ .name = try alloc.dupe(u8, column.name), .path = try alloc.dupe(u8, column.path), .nullable = !column.required or column.allows_null, .generated = generated, .type = dependencies.sql_document_row.relationalType(parsed, column.name, switch (column.column_type) {
                    .string => .string,
                    .integer => .integer,
                    .number => .number,
                    .boolean => .boolean,
                    .datetime => .datetime,
                    .json => .json,
                    else => return error.UnsupportedSqlExecution,
                }) };
            }
            return .{ .id = self.db.core.identity_namespace.table_id, .physical_name = try alloc.dupe(u8, self.table_name), .schema_version = schema.version, .columns = columns };
        }

        const Cursor = struct {
            session: *DB.RelationalReadSession,
            alloc: std.mem.Allocator,
            fn next(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32) !catalog.Page {
                const self: *Cursor = @ptrCast(@alignCast(ptr));
                var page = try self.session.nextTypedPage(alloc, null, .{ .rows = limit, .output_bytes = 16 * 1024 * 1024 });
                errdefer page.deinit();
                const owned = page.arena.allocator();
                const rows = try owned.alloc(catalog.Row, page.rows.len);
                for (page.rows, rows) |row, *out| out.* = .{ .id = row.key, .version = row.version, .value = row.typed orelse return error.InvalidSqlBackendResponse, .sql_nulls = row.sql_nulls, .expected_content_digest = row.expected_content_digest };
                const after = if (page.more) try owned.dupe(u8, self.session.reader.after.items) else null;
                return .{ .rows = rows, .after = after, .owned_arena = page.arena };
            }
            fn close(ptr: *anyopaque) void {
                const self: *Cursor = @ptrCast(@alignCast(ptr));
                self.session.deinit();
                self.alloc.destroy(self);
            }
        };

        const DocumentCursor = struct {
            session: *DB.DocumentReadSession,
            alloc: std.mem.Allocator,
            fn next(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32) !catalog.Page {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                var page = try self.session.next(alloc, limit);
                errdefer page.deinit();
                const rows = try page.arena.allocator().alloc(catalog.Row, page.rows.len);
                for (page.rows, rows) |row, *out| out.* = .{ .id = row.id, .version = row.version, .value = row.value, .sql_nulls = row.sql_nulls, .expected_content_digest = row.expected_content_digest, .document = row.document };
                return .{ .rows = rows, .after = page.after, .owned_arena = page.arena };
            }
            fn close(ptr: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.session.deinit();
                self.alloc.destroy(self);
            }
        };

        fn openNative(ptr: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, request: catalog.Scan) !?catalog.Cursor {
            const adapter_owner: *Self = @ptrCast(@alignCast(ptr));
            var selected_adapter = try adapter_owner.selected(table.physical_name);
            const self = &selected_adapter;
            var scratch = std.heap.ArenaAllocator.init(alloc);
            defer scratch.deinit();
            const temporary = scratch.allocator();
            const conditions = try temporary.alloc(types.RelationalRowQuery.Condition, request.conditions.len);
            for (request.conditions, conditions) |condition, *out| out.* = .{ .column = condition.column, .value = condition.value, .op = switch (condition.op) {
                .neq => .ne,
                inline else => |tag| @field(@FieldType(types.RelationalRowQuery.Condition, "op"), @tagName(tag)),
            } };
            const from = request.primary_key orelse request.after orelse "";
            const to = if (request.primary_key) |key| try std.mem.concat(temporary, u8, &.{ key, "\x00" }) else "";
            if (table.storage_mode == .document) {
                const session = try self.db.openDocumentReadSession(alloc, from, to, .{
                    .sql_document_preimage = request.include_document,
                    .include_content_hashes = request.include_primary_digest,
                    .inclusive_from = request.primary_key != null,
                    .exclusive_to = true,
                    .limit = request.limit,
                    .relational_query = .{ .page_bytes = 256 * 1024, .fields = request.fields, .conditions = conditions, .schema_version = table.schema_version },
                });
                errdefer session.deinit();
                const cursor = try alloc.create(DocumentCursor);
                cursor.* = .{ .alloc = alloc, .session = session };
                return .{ .ptr = cursor, .next = DocumentCursor.next, .close = DocumentCursor.close };
            }
            const session = try self.db.openRelationalReadSession(alloc, from, to, .{
                .include_content_hashes = request.include_primary_digest,
                .inclusive_from = request.primary_key != null,
                .exclusive_to = true,
                .limit = request.limit,
                .relational_query = .{ .page_bytes = 256 * 1024, .fields = request.fields, .conditions = conditions, .schema_version = table.schema_version, .auto_index = request.primary_key == null and !request.primary_order and self.transaction == null },
            });
            errdefer session.deinit();
            const cursor = try alloc.create(Cursor);
            cursor.* = .{ .session = session, .alloc = alloc };
            return .{ .ptr = cursor, .next = Cursor.next, .close = Cursor.close };
        }

        fn open(ptr: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, request: catalog.Scan) !?catalog.Cursor {
            if (request.search != null) return error.SqlStatementSnapshotRequired;
            const self: *Self = @ptrCast(@alignCast(ptr));
            const native_cursor = (try openNative(ptr, alloc, table, request)) orelse return null;
            errdefer native_cursor.close(native_cursor.ptr);
            if (self.transaction) |session| if (session.active) return try @import("sql_overlay.zig").open(alloc, native_cursor, session, table, request);
            return native_cursor;
        }

        fn scan(ptr: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, request: catalog.Scan) !catalog.Page {
            const cursor = (try open(ptr, alloc, table, request)).?;
            defer cursor.close(cursor.ptr);
            return cursor.next(cursor.ptr, alloc, request.limit);
        }

        fn prepareMutations(ptr: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, input: []const catalog.Mutation) ![]const catalog.Mutation {
            const adapter_owner: *Self = @ptrCast(@alignCast(ptr));
            var selected_adapter = try adapter_owner.selected(table.physical_name);
            const self = &selected_adapter;
            if (self.read_only) return error.SqlReadOnlyTransaction;
            const images = dependencies.sql_mutation_images;
            const writes = try images.writes(types.BatchWrite, alloc, input);
            if (writes.len == 0) return input;
            if (table.storage_mode == .document) {
                const session = try self.db.openDocumentReadSession(alloc, writes[0].key, writes[0].key, .{ .limit = 1, .relational_query = .{ .fields = &.{}, .schema_version = table.schema_version } });
                defer session.deinit();
                return images.merge(alloc, input, try session.normalizeRows(alloc, writes));
            }
            const session = try self.db.openRelationalReadSession(alloc, writes[0].key, writes[0].key, .{
                .limit = 1,
                .relational_query = .{ .fields = &.{}, .schema_version = table.schema_version },
            });
            defer session.deinit();
            const normalized = try session.normalizeRows(alloc, writes);
            return images.merge(alloc, input, normalized);
        }

        fn mutate(ptr: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
            const adapter_owner: *Self = @ptrCast(@alignCast(ptr));
            var selected_adapter = try adapter_owner.selected(table.physical_name);
            const self = &selected_adapter;
            defer adapter_owner.outcome_transaction_id = self.outcome_transaction_id;
            if (self.transaction) |session| if (session.active) return session.stage(table, mutations);
            if (self.read_only) return error.SqlReadOnlyTransaction;
            self.outcome_transaction_id = null;
            // The executor provides a commit-scoped arena. A second arena
            // would retain superseded serialization buffers in its parent.
            const temporary = alloc;
            var writes: std.ArrayList(types.BatchWrite) = .empty;
            var deletes: std.ArrayList([]const u8) = .empty;
            const predicates = try temporary.alloc(types.TransactionVersionPredicate, mutations.len);
            for (mutations, predicates) |mutation, *predicate| {
                predicate.* = .{ .key = mutation.key, .expected_version = mutation.expected_version, .expected_content_digest = mutation.expected_content_digest, .unique_absence = mutation.unique_absence };
                if (mutation.predicate_only) continue;
                if (mutation.row) |row| {
                    try writes.append(temporary, .{ .key = mutation.key, .value = try std.json.Stringify.valueAlloc(temporary, row, .{}), .json_null_fields = if (table.storage_mode == .document) &.{} else mutation.json_null_fields });
                } else try deletes.append(temporary, mutation.key);
            }
            const request: types.BatchRequest = .{ .writes = writes.items, .deletes = deletes.items, .predicates = predicates, .schema_version = table.schema_version, .relational_schema_version = if (table.storage_mode == .relational) table.schema_version else null };
            var prepared: ?integrity.Prepared = null;
            defer if (prepared) |*value| value.deinit();
            const schema_json = (try self.db.getSchemaJson(temporary)) orelse return error.IntegrityCatalogUnavailable;
            if (try integrity.requiresCoordination(temporary, schema_json)) {
                const metadata = try self.localCatalog(temporary, table.schema_version);
                var generation: ?[32]u8 = null;
                var commands: std.ArrayList(@typeInfo(@FieldType(types.BatchRequest, "integrity_commands")).pointer.child) = .empty;
                for (mutations) |mutation| if (mutation.conflict_guard) |proof| {
                    const owner: *const integrity.ConflictOwner = @ptrCast(@alignCast(proof));
                    if (generation) |prior| if (!std.mem.eql(u8, &prior, &owner.generation_set)) return error.PreparedGenerationChanged;
                    generation = owner.generation_set;
                    try commands.appendSlice(temporary, owner.guards);
                };
                const input_writes = try temporary.alloc(types.TransactionWrite, writes.items.len);
                for (writes.items, input_writes) |write, *out| out.* = .{ .key = write.key, .value = write.value, .json_null_fields = write.json_null_fields };
                const input: contract.TableCommitRequest = .{ .table_name = self.table_name, .writes = input_writes, .deletes = deletes.items, .predicates = predicates, .schema_version = table.schema_version, .relational_schema_version = table.schema_version, .relational_integrity_generation_set = generation, .integrity_commands = commands.items };
                prepared = try integrity.prepareWithRepairAdmission(temporary, self.localSource(), metadata.table, metadata.range, &.{input}, self.repairAdmission(), .{});
                if (self.handle) |handle| return @import("sql_commit.zig").commit(handle, prepared.?.tables, &self.outcome_transaction_id);
                if (prepared.?.tables.len != 1 or !std.mem.eql(u8, prepared.?.tables[0].table_name, self.table_name)) return error.UnsupportedSqlExecution;
                return self.commitCoordinated(prepared.?.tables[0]);
            }
            self.db.batch(request) catch |err| switch (err) {
                error.VersionConflict => return error.SqlWriteConflict,
                error.CommitVisibilityNotSatisfied, error.EnrichmentWaitCanceled, error.EnrichmentWaitTimeout, error.EnrichmentRetryInProgress, error.CommitPropagationIncomplete => return .committed_pending,
                error.EnrichmentWorkerFailed => return .committed_repair_required,
                else => return err,
            };
            return .committed;
        }

        fn checkpoint(_: *anyopaque) !void {}

        pub fn repairAdmission(self: *Self) dependencies.api_relational_integrity_commit.RepairAdmission {
            return .{ .ptr = self, .eligible = repairEligible };
        }

        fn repairEligible(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, version: u32) !bool {
            const self: *Self = @ptrCast(@alignCast(ptr));
            var selected_adapter = try self.selected(name);
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const a = arena.allocator();
            const response = (try selected_adapter.db.lookup(a, "", .{ .relational_activation_json = "{}" })) orelse return false;
            const status = try std.json.parseFromSliceLeaky(struct { schema_version: u32, state: dependencies.storage_db_relational_integrity_activation_contract.State, failure: []const u8 }, a, response.json, .{ .ignore_unknown_fields = true });
            if (status.schema_version != version) return error.PreparedGenerationChanged;
            return status.state == .invalid or (status.state == .validating and std.mem.eql(u8, status.failure, "ForeignKeyParentMissing"));
        }
    };
}
