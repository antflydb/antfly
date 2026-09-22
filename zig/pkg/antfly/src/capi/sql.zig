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
        db: *DB,
        table_name: []const u8,
        read_only: bool = false,

        pub fn backend(self: *Self) catalog.Backend {
            return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = scan, .open_scan = open, .open_statement = openStatement, .mutate = mutate, .prepare_mutations = prepareMutations, .checkpoint = checkpoint } };
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
                if (action != .read) return error.UnsupportedSqlExecution;
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
                for (page.rows, rows) |row, *out| out.* = .{ .id = row.key, .version = row.version, .value = row.typed orelse return error.InvalidSqlBackendResponse, .sql_nulls = row.sql_nulls };
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
                for (page.rows, rows) |row, *out| out.* = .{ .id = row.id, .version = row.version, .value = row.value, .sql_nulls = row.sql_nulls };
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
            var scratch = std.heap.ArenaAllocator.init(alloc);
            defer scratch.deinit();
            const temporary = scratch.allocator();
            var writes: std.ArrayList(types.BatchWrite) = .empty;
            var deletes: std.ArrayList([]const u8) = .empty;
            const predicates = try temporary.alloc(types.TransactionVersionPredicate, mutations.len);
            for (mutations, predicates) |mutation, *predicate| {
                predicate.* = .{ .key = mutation.key, .expected_version = mutation.expected_version };
                if (mutation.row) |row| {
                    try writes.append(temporary, .{ .key = mutation.key, .value = try std.json.Stringify.valueAlloc(temporary, row, .{}), .json_null_fields = mutation.json_null_fields });
                } else try deletes.append(temporary, mutation.key);
            }
            self.db.batch(.{ .writes = writes.items, .deletes = deletes.items, .predicates = predicates, .relational_schema_version = table.schema_version }) catch |err| switch (err) {
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
