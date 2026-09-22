// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Single-handle SQL binding for Lite/C. Reuses the public compiler and executor;
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
        db: *DB,
        table_name: []const u8,
        read_only: bool = false,
        outcome_transaction_id: ?types.TxnId = null,

        pub fn backend(self: *Self) catalog.Backend {
            return .{ .ptr = self, .predicate_only_mutations = true, .vtable = &.{ .resolve_conflict_owners = resolveConflictOwners, .generate_row_id = generateRowId, .resolve = resolve, .scan = scan, .open_scan = open, .open_statement = openStatement, .mutate = mutate, .prepare_mutations = prepareMutations, .checkpoint = checkpoint } };
        }

        const LocalCatalog = struct {
            table: [1]topology.TableRecord,
            range: [1]topology.RangeRecord,
        };

        fn localCatalog(self: *Self, alloc: std.mem.Allocator, version: u32) !LocalCatalog {
            // A Lite handle is an integrity owner only when its actual durable
            // identity and full byte range cover every possible claim route.
            const range = self.db.core.byteRange();
            const identity = self.db.core.identity_namespace;
            if (range.start.len != 0 or range.end.len != 0) return error.UnsupportedSqlExecution;
            if (identity.table_id == 0) return error.CoordinatedConstraintsRequireTableIdentity;
            const json = (try self.db.getSchemaJson(alloc)) orelse return error.IntegrityCatalogUnavailable;
            const parsed = try native.public_api.tables.parseValidatedTableSchema(alloc, json);
            if (parsed.version != version) return error.PreparedGenerationChanged;
            return .{
                .table = .{.{ .table_id = identity.table_id, .name = self.table_name, .schema_json = json }},
                .range = .{.{ .group_id = identity.shard_id, .range_id = identity.range_id, .table_id = identity.table_id, .start_key = "", .doc_identity_shard_id = identity.shard_id, .doc_identity_range_id = identity.range_id }},
            };
        }

        fn localSource(self: *Self) reads.TableReadSource {
            return .{ .ptr = self, .vtable = &.{ .lookup = integrityLookup, .scan = integrityScan, .query = integrityQuery } };
        }

        fn integrityLookup(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, key: []const u8, options: types.LookupOptions, _: dependencies.raft_read_gate.ReadConsistency) !?reads.LookupResponse {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (!std.mem.eql(u8, name, self.table_name)) return error.UnsupportedSqlExecution;
            const result = (try self.db.lookup(alloc, key, options)) orelse return null;
            errdefer alloc.free(result.json);
            const internal = options.relational_integrity_catalog or options.relational_integrity_jobs_json.len != 0 or options.relational_index_status_json.len != 0 or options.relational_activation_json.len != 0;
            return .{ .json = result.json, .version = if (internal) 0 else result.version orelse try self.db.getTimestamp(alloc, key), .expected_content_digest = result.expected_content_digest };
        }

        fn integrityScan(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8, from: []const u8, to: []const u8, options: types.ScanOptions, _: dependencies.raft_read_gate.ReadConsistency) !?reads.ScanResponse {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (!std.mem.eql(u8, name, self.table_name)) return error.UnsupportedSqlExecution;
            var result = try self.db.scan(alloc, from, to, options);
            defer result.deinit(alloc);
            return .{ .ndjson = try dependencies.api_local_query_contract.encodeStorageKernelScanNdjson(alloc, result, options.include_documents) };
        }

        fn integrityQuery(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: types.SearchRequest, _: dependencies.raft_read_gate.ReadConsistency) !?dependencies.api_query_response.QueryResponse {
            return error.UnsupportedSqlExecution;
        }

        fn resolveConflictOwners(ptr: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, columns: []const []const u8, mutations: []const catalog.Mutation) ![]const catalog.ConflictOwner {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (self.read_only) return error.SqlReadOnlyTransaction;
            if (!std.mem.eql(u8, table.physical_name, self.table_name)) return error.UndefinedTable;
            if (columns.len == 0) {
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
            const owners = try integrity.resolveConflictOwners(alloc, self.localSource(), &metadata.table, &metadata.range, self.table_name, table.schema_version, columns, writes, &.{}, .{});
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
            for (requests) |request| {
                if (!std.mem.eql(u8, request.table.physical_name, self.table_name)) return error.UndefinedTable;
            }
            const statement = try alloc.create(Statement);
            errdefer alloc.destroy(statement);
            const cursors = try alloc.alloc(catalog.Cursor, requests.len);
            errdefer alloc.free(cursors);
            var initialized: usize = 0;
            errdefer for (cursors[0..initialized]) |cursor| cursor.close(cursor.ptr);
            // All aliases share this handle's capture interval. Independent
            // cursors then retain their snapshots after writers are released.
            var fence = (try self.db.tryStatementReadFence()) orelse return error.SqlStatementSnapshotRequired;
            defer fence.release();
            for (requests, cursors) |request, *cursor| {
                cursor.* = (try open(ptr, alloc, request.table, request.request)) orelse return error.SqlStatementSnapshotRequired;
                initialized += 1;
            }
            statement.* = .{ .alloc = alloc, .cursors = cursors };
            return .{ .ptr = statement, .cursors = cursors, .close = Statement.close };
        }

        fn resolve(ptr: *anyopaque, alloc: std.mem.Allocator, name: ast.Name, action: catalog.Action) !catalog.Table {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (name.database != null or name.namespace != null or !std.mem.eql(u8, name.table, self.table_name)) return error.UndefinedTable;
            if (self.read_only and action != .read) return error.SqlReadOnlyTransaction;
            const json = (try self.db.getSchemaJson(alloc)) orelse return error.UnsupportedSqlExecution;
            defer alloc.free(json);
            var scratch = std.heap.ArenaAllocator.init(alloc);
            defer scratch.deinit();
            const parsed = try native.public_api.tables.parseValidatedTableSchema(scratch.allocator(), json);
            const schema = try native.public_api.tables.deriveRuntimeTableSchema(scratch.allocator(), parsed);
            if (schema.storage_mode == .document) {
                return .{ .id = 1, .physical_name = self.table_name, .schema_version = schema.version, .storage_mode = .document, .columns = try dependencies.sql_document_row.deriveColumns(alloc, parsed) };
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
                out.* = .{ .name = try alloc.dupe(u8, column.name), .path = try alloc.dupe(u8, column.path), .nullable = !column.required or column.allows_null, .generated = generated, .type = switch (column.column_type) {
                    .string => .string,
                    .integer => .integer,
                    .number => .number,
                    .boolean => .boolean,
                    .datetime => .datetime,
                    .json => .json,
                    else => return error.UnsupportedSqlExecution,
                } };
            }
            return .{ .id = 1, .physical_name = self.table_name, .schema_version = schema.version, .columns = columns };
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

        fn open(ptr: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, request: catalog.Scan) !?catalog.Cursor {
            const self: *Self = @ptrCast(@alignCast(ptr));
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
                    .relational_query = .{ .fields = request.fields, .conditions = conditions, .schema_version = table.schema_version },
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
                .relational_query = .{ .fields = request.fields, .conditions = conditions, .schema_version = table.schema_version, .auto_index = request.primary_key == null and !request.primary_order },
            });
            errdefer session.deinit();
            const cursor = try alloc.create(Cursor);
            cursor.* = .{ .session = session, .alloc = alloc };
            return .{ .ptr = cursor, .next = Cursor.next, .close = Cursor.close };
        }

        fn scan(ptr: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, request: catalog.Scan) !catalog.Page {
            const cursor = (try open(ptr, alloc, table, request)).?;
            defer cursor.close(cursor.ptr);
            return cursor.next(cursor.ptr, alloc, request.limit);
        }

        fn prepareMutations(ptr: *anyopaque, alloc: std.mem.Allocator, table: catalog.Table, input: []const catalog.Mutation) ![]const catalog.Mutation {
            const self: *Self = @ptrCast(@alignCast(ptr));
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
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (self.read_only) return error.SqlReadOnlyTransaction;
            self.outcome_transaction_id = null;
            var scratch = std.heap.ArenaAllocator.init(alloc);
            defer scratch.deinit();
            const temporary = scratch.allocator();
            var writes: std.ArrayList(types.BatchWrite) = .empty;
            var deletes: std.ArrayList([]const u8) = .empty;
            const predicates = try temporary.alloc(types.TransactionVersionPredicate, mutations.len);
            for (mutations, predicates) |mutation, *predicate| {
                predicate.* = .{ .key = mutation.key, .expected_version = mutation.expected_version, .expected_content_digest = mutation.expected_content_digest };
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
                prepared = try integrity.prepareWithCoverage(temporary, self.localSource(), &metadata.table, &metadata.range, &.{input});
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
    };
}
